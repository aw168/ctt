let BOT_TOKEN;
let GROUP_ID;
let MAX_MESSAGES_PER_MINUTE;
const CURRENT_VERSION = "v1.4.0";
const VERSION_CHECK_URL = "https://raw.githubusercontent.com/iawooo/tz/main/CFTeleTrans/tag.md";
const UPDATE_INFO_URL = "https://raw.githubusercontent.com/iawooo/tz/main/CFTeleTrans/admin.md";
let lastCleanupTime = 0;
let lastCacheCleanupTime = 0;
const CLEANUP_INTERVAL = 24 * 60 * 60 * 1000;
const CACHE_CLEANUP_INTERVAL = 1 * 60 * 60 * 1000;
let isInitialized = false;

// 使用更高效的消息去重机制
const MESSAGE_DEDUP_SIZE = 5000; // 减小内存占用
const CALLBACK_DEDUP_SIZE = 1000;

class TimedSet {
  constructor(maxSize, ttl = 300000) { // 5分钟TTL
    this.maxSize = maxSize;
    this.ttl = ttl;
    this.data = new Map(); // key -> timestamp
  }

  has(key) {
    const timestamp = this.data.get(key);
    if (!timestamp) return false;
    
    const now = Date.now();
    if (now - timestamp > this.ttl) {
      this.data.delete(key);
      return false;
    }
    
    return true;
  }

  add(key) {
    const now = Date.now();
    
    // 清理过期数据
    if (this.data.size >= this.maxSize) {
      this.cleanup(now);
    }
    
    this.data.set(key, now);
  }

  cleanup(now = Date.now()) {
    let deletedCount = 0;
    for (const [key, timestamp] of this.data.entries()) {
      if (now - timestamp > this.ttl) {
        this.data.delete(key);
        deletedCount++;
      }
    }
    
    // 如果清理后仍然太大，删除最老的条目
    if (this.data.size >= this.maxSize) {
      const entries = Array.from(this.data.entries()).sort((a, b) => a[1] - b[1]);
      const toDelete = entries.slice(0, Math.floor(this.maxSize * 0.3));
      for (const [key] of toDelete) {
        this.data.delete(key);
      }
    }
    
    if (deletedCount > 0) {
      console.log(`清理了 ${deletedCount} 个过期的去重记录`);
    }
  }
}

const processedMessages = new TimedSet(MESSAGE_DEDUP_SIZE);
const processedCallbacks = new TimedSet(CALLBACK_DEDUP_SIZE);

const settingsCache = new Map([
  ['verification_enabled', null],
  ['user_raw_enabled', null]
]);
class LRUCache {
  constructor(maxSize) {
    this.maxSize = maxSize;
    this.cache = new Map();
    this.lastAccess = new Map();
    this._cleanupInProgress = false;
  }

  get(key) {
    const value = this.cache.get(key);
    if (value !== undefined) {
      // 移动到最后（最近使用）
      this.cache.delete(key);
      this.cache.set(key, value);
      this.lastAccess.set(key, Date.now());
    }
    return value;
  }

  set(key, value) {
    // 如果正在清理，等待完成
    if (this._cleanupInProgress) {
      return;
    }

    // 如果已存在，更新值
    if (this.cache.has(key)) {
      this.cache.delete(key);
    } else if (this.cache.size >= this.maxSize) {
      // 删除最旧的条目
      const firstKey = this.cache.keys().next().value;
      this.cache.delete(firstKey);
      this.lastAccess.delete(firstKey);
    }

    this.cache.set(key, value);
    this.lastAccess.set(key, Date.now());
  }

  clear() {
    this.cache.clear();
    this.lastAccess.clear();
  }

  cleanStale(maxAge = 3600000) {
    if (this._cleanupInProgress) {
      return;
    }

    this._cleanupInProgress = true;
    try {
    const now = Date.now();
      const keysToDelete = [];

    for (const [key, lastAccessTime] of this.lastAccess.entries()) {
      if (now - lastAccessTime > maxAge) {
          keysToDelete.push(key);
        }
      }

      for (const key of keysToDelete) {
        this.cache.delete(key);
        this.lastAccess.delete(key);
      }

      if (keysToDelete.length > 0) {
        console.log(`LRU缓存清理了 ${keysToDelete.length} 个过期条目`);
      }
    } finally {
      this._cleanupInProgress = false;
    }
  }

  size() {
    return this.cache.size;
  }
}
const userInfoCache = new LRUCache(1000);
const topicIdCache = new LRUCache(1000);
const userStateCache = new LRUCache(1000);
const messageRateCache = new LRUCache(1000);
export default {
  async fetch(request, env) {
    BOT_TOKEN = env.BOT_TOKEN_ENV || null;
    GROUP_ID = env.GROUP_ID_ENV || null;
    MAX_MESSAGES_PER_MINUTE = env.MAX_MESSAGES_PER_MINUTE_ENV ? parseInt(env.MAX_MESSAGES_PER_MINUTE_ENV) : 40;
if (!env.D1) {
      return new Response('Server configuration error: D1 database is not bound', { status: 500 });
    }
if (!isInitialized) {
      await initialize(env.D1, request);
      isInitialized = true;
    }
async function handleRequest(request) {
      if (!BOT_TOKEN || !GROUP_ID) {
        return new Response('Server configuration error: Missing required environment variables', { status: 500 });
      }
      const url = new URL(request.url);
      if (url.pathname === '/webhook') {
        try {
          const update = await request.json();
          await handleUpdate(update);
          return new Response('OK');
        } catch (error) {
          return new Response('Bad Request', { status: 400 });
        }
      } else if (url.pathname === '/registerWebhook') {
        return await registerWebhook(request);
      } else if (url.pathname === '/unRegisterWebhook') {
        return await unRegisterWebhook();
      } else if (url.pathname === '/checkTables') {
        await checkAndRepairTables(env.D1);
        return new Response('Database tables checked and repaired', { status: 200 });
      }
      return new Response('Not Found', { status: 404 });
    }
async function initialize(d1, request) {
      await Promise.all([
        checkAndRepairTables(d1),
        autoRegisterWebhook(request),
        checkBotPermissions(),
        cleanExpiredVerificationCodes(d1),
        cleanupCreatingTopics(d1),
        setupPeriodicCleanup(d1),
        preloadSettings(d1)
      ]);
      setTimeout(async () => {
        try {
          await cleanupInvalidTopics(d1);
        } catch (error) {
          console.error(`延迟清理无效话题失败: ${error.message}`);
        }
      }, 10000);
    }
async function preloadSettings(d1) {
      try {
        console.log('预加载常用设置...');
        const settingsResult = await d1.prepare('SELECT key, value FROM settings WHERE key IN (?, ?)')
          .bind('verification_enabled', 'user_raw_enabled')
          .all();
        if (settingsResult.results && settingsResult.results.length > 0) {
          for (const row of settingsResult.results) {
            settingsCache.set(row.key, row.value === 'true');
          }
        }
        console.log('设置预加载完成');
      } catch (error) {
        console.error(`预加载设置失败: ${error.message}`);
      }
    }
async function autoRegisterWebhook(request) {
      const webhookUrl = `${new URL(request.url).origin}/webhook`;
      await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/setWebhook`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ url: webhookUrl }),
      });
    }
async function checkBotPermissions() {
      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getChat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: GROUP_ID })
      });
      const data = await response.json();
      if (!data.ok) {
        throw new Error(`Failed to access group: ${data.description}`);
      }
      const memberResponse = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getChatMember`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          chat_id: GROUP_ID,
          user_id: (await getBotId())
        })
      });
      const memberData = await memberResponse.json();
      if (!memberData.ok) {
        throw new Error(`Failed to get bot member status: ${memberData.description}`);
      }
    }
async function getBotId() {
      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getMe`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({})
      });
      const data = await response.json();
      if (!data.ok) throw new Error(`Failed to get bot ID: ${data.description}`);
      return data.result.id;
    }
async function checkAndRepairTables(d1) {
      const expectedTables = {
        user_states: {
          columns: {
            chat_id: 'TEXT PRIMARY KEY',
            is_blocked: 'BOOLEAN DEFAULT FALSE',
            is_verified: 'BOOLEAN DEFAULT FALSE',
            verified_expiry: 'INTEGER',
            verification_code: 'TEXT',
            code_expiry: 'INTEGER',
            last_verification_message_id: 'TEXT',
            is_first_verification: 'BOOLEAN DEFAULT TRUE',
            is_rate_limited: 'BOOLEAN DEFAULT FALSE',
            is_verifying: 'BOOLEAN DEFAULT FALSE'
          }
        },
        message_rates: {
          columns: {
            chat_id: 'TEXT PRIMARY KEY',
            message_count: 'INTEGER DEFAULT 0',
            window_start: 'INTEGER',
            start_count: 'INTEGER DEFAULT 0',
            start_window_start: 'INTEGER'
          }
        },
        chat_topic_mappings: {
          columns: {
            chat_id: 'TEXT PRIMARY KEY',
            topic_id: 'TEXT NOT NULL',
            created_at: 'INTEGER DEFAULT (strftime("%s", "now"))',
            status: 'TEXT DEFAULT "active"'
          }
        },
        settings: {
          columns: {
            key: 'TEXT PRIMARY KEY',
            value: 'TEXT'
          }
        },
        topic_creation_locks: {
          columns: {
            chat_id: 'TEXT PRIMARY KEY',
            lock_time: 'INTEGER NOT NULL',
            process_id: 'TEXT',
            status: 'TEXT DEFAULT "creating"'
          }
        }
      };

      for (const [tableName, structure] of Object.entries(expectedTables)) {
        const tableInfo = await d1.prepare(
          `SELECT sql FROM sqlite_master WHERE type='table' AND name=?`
        ).bind(tableName).first();

        if (!tableInfo) {
          await createTable(d1, tableName, structure);
          continue;
        }

        const columnsResult = await d1.prepare(
          `PRAGMA table_info(${tableName})`
        ).all();

        const currentColumns = new Map(
          columnsResult.results.map(col => [col.name, {
            type: col.type,
            notnull: col.notnull,
            dflt_value: col.dflt_value
          }])
        );

        for (const [colName, colDef] of Object.entries(structure.columns)) {
          if (!currentColumns.has(colName)) {
            const columnParts = colDef.split(' ');
            const addColumnSQL = `ALTER TABLE ${tableName} ADD COLUMN ${colName} ${columnParts.slice(1).join(' ')}`;
            await d1.exec(addColumnSQL);
          }
        }

        // 创建必要的索引
        if (tableName === 'settings') {
          await d1.exec('CREATE INDEX IF NOT EXISTS idx_settings_key ON settings (key)');
        } else if (tableName === 'chat_topic_mappings') {
          await d1.exec('CREATE INDEX IF NOT EXISTS idx_topic_id ON chat_topic_mappings (topic_id)');
          await d1.exec('CREATE INDEX IF NOT EXISTS idx_chat_status ON chat_topic_mappings (chat_id, status)');
        } else if (tableName === 'topic_creation_locks') {
          await d1.exec('CREATE INDEX IF NOT EXISTS idx_lock_time ON topic_creation_locks (lock_time)');
        }
      }

      // 插入默认设置
      await Promise.all([
        d1.prepare('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)')
          .bind('verification_enabled', 'true').run(),
        d1.prepare('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)')
          .bind('user_raw_enabled', 'true').run()
      ]);

      // 清理过期的创建锁
      await cleanupExpiredLocks(d1);

      settingsCache.set('verification_enabled', (await getSetting('verification_enabled', d1)) === 'true');
      settingsCache.set('user_raw_enabled', (await getSetting('user_raw_enabled', d1)) === 'true');
    }
async function createTable(d1, tableName, structure) {
      const columnsDef = Object.entries(structure.columns)
        .map(([name, def]) => `${name} ${def}`)
        .join(', ');
      const createSQL = `CREATE TABLE ${tableName} (${columnsDef})`;
      await d1.exec(createSQL);
    }
async function cleanExpiredVerificationCodes(d1) {
      const now = Date.now();
      if (now - lastCleanupTime < CLEANUP_INTERVAL) {
        return;
      }
      const nowSeconds = Math.floor(now / 1000);
      const expiredCodes = await d1.prepare(
        'SELECT chat_id FROM user_states WHERE code_expiry IS NOT NULL AND code_expiry < ?'
      ).bind(nowSeconds).all();
      if (expiredCodes.results.length > 0) {
        await d1.batch(
          expiredCodes.results.map(({ chat_id }) =>
            d1.prepare(
              'UPDATE user_states SET verification_code = NULL, code_expiry = NULL, is_verifying = FALSE WHERE chat_id = ?'
            ).bind(chat_id)
          )
        );
      }
      lastCleanupTime = now;
    }
async function cleanupCreatingTopics(d1) {
      try {
    // 清理遗留的creating状态记录
        const creatingTopics = await d1.prepare('SELECT chat_id FROM chat_topic_mappings WHERE topic_id = ?')
          .bind('creating')
          .all();

        if (creatingTopics.results.length > 0) {
          console.log(`清理 ${creatingTopics.results.length} 个遗留的临时话题标记`);
      
      await d1.batch([
        d1.prepare('DELETE FROM chat_topic_mappings WHERE topic_id = ?').bind('creating'),
        d1.prepare('DELETE FROM topic_creation_locks WHERE status = ?').bind('creating')
      ]);
      
          for (const row of creatingTopics.results) {
            topicIdCache.set(row.chat_id, undefined);
          }
        }

    // 清理过期的时间戳设置（向后兼容）
        await d1.prepare('DELETE FROM settings WHERE key LIKE ?')
          .bind('creating_timestamp_%')
          .run();
      
      } catch (error) {
        console.error(`清理临时话题标记时出错: ${error.message}`);
      }
    }
async function cleanupInvalidTopics(d1) {
      try {
        const topicMappings = await d1.prepare('SELECT chat_id, topic_id FROM chat_topic_mappings WHERE topic_id != ?')
          .bind('creating')
          .all();
        if (!topicMappings.results || topicMappings.results.length === 0) {
          return;
        }
        console.log(`检查 ${topicMappings.results.length} 个话题ID是否有效`);
        let invalidCount = 0;
        const batchSize = 5;
        for (let i = 0; i < topicMappings.results.length; i += batchSize) {
          const batch = topicMappings.results.slice(i, i + batchSize);
          for (const mapping of batch) {
            try {
              const isValid = await validateTopic(mapping.topic_id, true);
              if (!isValid) {
                console.log(`话题ID ${mapping.topic_id} 无效，将从数据库删除`);
                await d1.prepare('DELETE FROM chat_topic_mappings WHERE chat_id = ?')
                  .bind(mapping.chat_id)
                  .run();
                topicIdCache.set(mapping.chat_id, undefined);
                invalidCount++;
              }
            } catch (error) {
              console.error(`验证话题ID ${mapping.topic_id} 时出错: ${error.message}`);
            }
          }
          if (i + batchSize < topicMappings.results.length) {
            await new Promise(resolve => setTimeout(resolve, 500));
          }
        }
        if (invalidCount > 0) {
          console.log(`已清理 ${invalidCount} 个无效话题ID`);
        } else {
          console.log(`所有话题ID有效`);
        }
      } catch (error) {
        console.error(`清理无效话题ID时出错: ${error.message}`);
      }
    }
async function handleUpdate(update) {
      if (update.message) {
        const messageId = update.message.message_id.toString();
        const chatId = update.message.chat.id.toString();
        const messageKey = `${chatId}:${messageId}`;
        if (processedMessages.has(messageKey)) {
          return;
        }
        processedMessages.add(messageKey);
        if (processedMessages.size > MESSAGE_DEDUP_SIZE) {
          processedMessages.cleanup();
        }
        await onMessage(update.message);
      } else if (update.callback_query) {
        await onCallbackQuery(update.callback_query);
      }
    }
async function onMessage(message) {
      const chatId = message.chat.id.toString();
      const text = message.text || '';
      const messageId = message.message_id;
  
      if (chatId === GROUP_ID) {
        const topicId = message.message_thread_id;
        if (topicId) {
          const privateChatId = await getPrivateChatId(topicId);
          if (privateChatId && text === '/admin') {
            await sendAdminPanel(chatId, topicId, privateChatId, messageId);
            return;
          }
          if (privateChatId && text.startsWith('/reset_user')) {
            await handleResetUser(chatId, topicId, text);
            return;
          }
          if (privateChatId) {
            await forwardMessageToPrivateChat(privateChatId, message);
          }
        }
        return;
      }

  // 使用原子操作获取或创建用户状态
  let userState = await getOrCreateUserState(chatId);
  
if (userState.is_blocked) {
        await sendMessageToUser(chatId, "您已被拉黑，无法发送消息。请联系管理员解除拉黑。");
        return;
      }

  const verificationEnabled = settingsCache.get('verification_enabled') ?? 
    ((await getSetting('verification_enabled', env.D1)) === 'true');
    
if (!verificationEnabled) {
    // 验证功能关闭，直接处理消息
      } else {
        const nowSeconds = Math.floor(Date.now() / 1000);
        const isVerified = userState.is_verified;
        const isFirstVerification = userState.is_first_verification;
        const isRateLimited = await checkMessageRate(chatId);
        const isVerifying = userState.is_verifying || false;

if (!isVerified || (isRateLimited && !isFirstVerification)) {
          if (isVerifying) {
        // 用户正在验证中，检查验证码状态
        const verificationResult = await checkVerificationStatus(chatId);
        if (verificationResult.needNewCode) {
              await sendMessageToUser(chatId, '验证码已过期，正在为您发送新的验证码...');
          await resetVerificationState(chatId);
try {
                await handleVerification(chatId, 0);
              } catch (verificationError) {
                console.error(`发送新验证码失败: ${verificationError.message}`);
                    await sendMessageToUser(chatId, '发送验证码失败，请发送任意消息重试');
              }
              return;
            } else {
              await sendMessageToUser(chatId, `请完成验证后发送消息"${text || '您的具体信息'}"。`);
            }
            return;
          }
      
          await sendMessageToUser(chatId, `请完成验证后发送消息"${text || '您的具体信息'}"。`);
          await handleVerification(chatId, messageId);
          return;
        }
      }

if (text === '/start') {
    await handleStartCommand(chatId);
    return;
  }

  // 处理普通消息
  await handleRegularMessage(chatId, text, message);
}
async function getOrCreateUserState(chatId) {
  let userState = userStateCache.get(chatId);
  if (userState !== undefined) {
    return userState;
  }

  userState = await env.D1.prepare('SELECT is_blocked, is_first_verification, is_verified, verified_expiry, is_verifying FROM user_states WHERE chat_id = ?')
    .bind(chatId)
    .first();

  if (!userState) {
    userState = { is_blocked: false, is_first_verification: true, is_verified: false, verified_expiry: null, is_verifying: false };
    await env.D1.prepare('INSERT INTO user_states (chat_id, is_blocked, is_first_verification, is_verified, is_verifying) VALUES (?, ?, ?, ?, ?)')
      .bind(chatId, false, true, false, false)
      .run();
  }

  userStateCache.set(chatId, userState);
  return userState;
}
async function handleStartCommand(chatId) {
  try {
    if (await checkStartCommandRate(chatId)) {
      await sendMessageToUser(chatId, "➡️您发送 /start 命令过于频繁，请稍后再试！如果您已经在聊天中，无需重复发送 /start 命令。");
      return;
    }

    const existingTopicId = await getExistingTopicId(chatId);
    if (existingTopicId) {
      const successMessage = await getVerificationSuccessMessage();
      await sendMessageToUser(chatId, `${successMessage}\n➡️您已经在聊天中，无需重复发送 /start 命令。`);
      return;
    }

    const userInfo = await getUserInfo(chatId);
    if (!userInfo) {
      await sendMessageToUser(chatId, "无法获取用户信息，请稍后再试。");
      return;
    }

    const successMessage = await getVerificationSuccessMessage();
    await sendMessageToUser(chatId, `${successMessage}\n你好，欢迎使用私聊机器人，现在发送信息吧！`);
    
    let topicId = null;
    let retries = 3;
    let error = null;
    
    while (retries > 0 && !topicId) {
      try {
        topicId = await ensureUserTopic(chatId, userInfo);
        if (topicId) break;
      } catch (err) {
        error = err;
        console.error(`创建话题失败，剩余重试次数: ${retries-1}, 错误: ${err.message}`);
        retries--;
        if (retries > 0) {
          await new Promise(resolve => setTimeout(resolve, 1000));
        }
      }
    }

    if (!topicId) {
      console.error(`为用户 ${chatId} 创建话题失败，已达到最大重试次数`);
      await sendMessageToUser(chatId, "创建聊天话题失败，请稍后再试或联系管理员。");
      throw error || new Error("创建话题失败，未知原因");
    }
  } catch (error) {
    console.error(`处理 /start 命令时出错: ${error.message}`);
  }
}

async function handleRegularMessage(chatId, text, message) {
  const userInfo = await getUserInfo(chatId);
  if (!userInfo) {
    await sendMessageToUser(chatId, "无法获取用户信息，请稍后再试或联系管理员。");
    return;
  }

  let topicId = await ensureUserTopic(chatId, userInfo);
  if (!topicId) {
    await sendMessageToUser(chatId, "无法创建话题，请稍后再试或联系管理员。");
    return;
  }

  const isTopicValid = await validateTopic(topicId, false);
  if (!isTopicValid) {
    console.log(`用户 ${chatId} 的话题 ${topicId} 无效，将重新创建`);
    await env.D1.prepare('UPDATE chat_topic_mappings SET status = ? WHERE chat_id = ?')
      .bind('inactive', chatId)
      .run();
    topicIdCache.set(chatId, undefined);
    
    topicId = await ensureUserTopic(chatId, userInfo);
    if (!topicId) {
      await sendMessageToUser(chatId, "无法重新创建话题，请稍后再试或联系管理员。");
      return;
    }
  }

  const userName = userInfo.username || `User_${chatId}`;
  const nickname = userInfo.nickname || userName;

  if (text) {
    const formattedMessage = `${nickname}:\n${text}`;
    await sendMessageToTopic(topicId, formattedMessage);
  } else {
    await copyMessageToTopic(topicId, message);
  }
}

    try {
      return await handleRequest(request);
    } catch (error) {
      return new Response('Internal Server Error', { status: 500 });
    }
  }
};

async function handleAdminCallback(callbackQuery, chatId, topicId, action, privateChatId, messageId) {
  const senderId = callbackQuery.from.id.toString();
  const isAdmin = await checkIfAdmin(senderId);
  
  if (!isAdmin) {
    await sendMessageToTopic(topicId, '只有管理员可以使用此功能。');
    await sendAdminPanel(chatId, topicId, privateChatId, messageId);
    return;
  }

  if (action === 'block') {
    let state = userStateCache.get(privateChatId);
    if (state === undefined) {
      state = await env.D1.prepare('SELECT is_blocked FROM user_states WHERE chat_id = ?')
        .bind(privateChatId)
        .first() || { is_blocked: false };
    }
    state.is_blocked = true;
    userStateCache.set(privateChatId, state);
    await env.D1.prepare('INSERT OR REPLACE INTO user_states (chat_id, is_blocked) VALUES (?, ?)')
      .bind(privateChatId, true)
      .run();
    await sendMessageToTopic(topicId, `用户 ${privateChatId} 已被拉黑，消息将不再转发。`);
    
  } else if (action === 'unblock') {
    let state = userStateCache.get(privateChatId);
    if (state === undefined) {
      state = await env.D1.prepare('SELECT is_blocked, is_first_verification FROM user_states WHERE chat_id = ?')
        .bind(privateChatId)
        .first() || { is_blocked: false, is_first_verification: true };
    }
    state.is_blocked = false;
    state.is_first_verification = true;
    userStateCache.set(privateChatId, state);
    await env.D1.prepare('INSERT OR REPLACE INTO user_states (chat_id, is_blocked, is_first_verification) VALUES (?, ?, ?)')
      .bind(privateChatId, false, true)
      .run();
    await sendMessageToTopic(topicId, `用户 ${privateChatId} 已解除拉黑，消息将继续转发。`);
    
  } else if (action === 'toggle_verification') {
    const currentState = (await getSetting('verification_enabled', env.D1)) === 'true';
    const newState = !currentState;
    await setSetting('verification_enabled', newState.toString());
    await sendMessageToTopic(topicId, `验证码功能已${newState ? '开启' : '关闭'}。`);
    
  } else if (action === 'check_blocklist') {
    const blockedUsers = await env.D1.prepare('SELECT chat_id FROM user_states WHERE is_blocked = ?')
      .bind(true)
      .all();
    const blockList = blockedUsers.results.length > 0 
      ? blockedUsers.results.map(row => row.chat_id).join('\n')
      : '当前没有被拉黑的用户。';
    await sendMessageToTopic(topicId, `黑名单列表：\n${blockList}`);
    
  } else if (action === 'toggle_user_raw') {
    const currentState = (await getSetting('user_raw_enabled', env.D1)) === 'true';
    const newState = !currentState;
    await setSetting('user_raw_enabled', newState.toString());
    await sendMessageToTopic(topicId, `用户端 Raw 链接已${newState ? '开启' : '关闭'}。`);
    
  } else if (action === 'delete_user') {
    userStateCache.set(privateChatId, undefined);
    messageRateCache.set(privateChatId, undefined);
    topicIdCache.set(privateChatId, undefined);
    await env.D1.batch([
      env.D1.prepare('DELETE FROM user_states WHERE chat_id = ?').bind(privateChatId),
      env.D1.prepare('DELETE FROM message_rates WHERE chat_id = ?').bind(privateChatId),
      env.D1.prepare('DELETE FROM chat_topic_mappings WHERE chat_id = ?').bind(privateChatId),
      env.D1.prepare('DELETE FROM topic_creation_locks WHERE chat_id = ?').bind(privateChatId)
    ]);
    await sendMessageToTopic(topicId, `用户 ${privateChatId} 的状态、消息记录和话题映射已删除，用户需重新发起会话。`);
    
  } else if (action === 'show_update') {
    try {
      const hasUpdate = await hasNewVersion();
      if (hasUpdate) {
        const updateInfo = await getUpdateInfo();
        const remoteVersion = await getRemoteVersion();
        const updateMessage = `🔄 检测到新版本！\n\n当前版本: ${CURRENT_VERSION}\n最新版本: ${remoteVersion}\n\n${updateInfo}\n\n请访问GitHub项目更新: https://github.com/iawooo/ctt`;
        await sendMessageToTopic(topicId, updateMessage);
      } else {
        await sendMessageToTopic(topicId, `当前已是最新版本 ${CURRENT_VERSION}，无需更新。`);
      }
    } catch (error) {
      console.error(`显示更新信息失败: ${error.message}`);
      await sendMessageToTopic(topicId, `获取更新信息失败，请稍后再试或直接访问GitHub项目: https://github.com/iawooo/ctt`);
    }
  } else if (action === 'check_update') {
    await sendMessageToTopic(topicId, `正在检查更新...`);
    try {
      const hasUpdate = await hasNewVersion();
      if (hasUpdate) {
        const updateInfo = await getUpdateInfo();
        const remoteVersion = await getRemoteVersion();
        const updateMessage = `🔄 检测到新版本！\n\n当前版本: ${CURRENT_VERSION}\n最新版本: ${remoteVersion}\n\n${updateInfo}\n\n请访问GitHub项目更新: https://github.com/iawooo/ctt`;
        await sendMessageToTopic(topicId, updateMessage);
      } else {
        await sendMessageToTopic(topicId, `当前已是最新版本 ${CURRENT_VERSION}，无需更新。`);
      }
    } catch (error) {
      console.error(`检查更新失败: ${error.message}`);
      await sendMessageToTopic(topicId, `检查更新失败，请稍后再试: ${error.message}`);
    }
  } else {
    await sendMessageToTopic(topicId, `未知操作：${action}`);
  }

  await sendAdminPanel(chatId, topicId, privateChatId, messageId);
}

// Telegram API调用限制器
class TelegramRateLimiter {
  constructor() {
    this.calls = [];
    this.maxCallsPerSecond = 30;
    this.maxCallsPerMinute = 1000;
  }

  async waitForSlot() {
    const now = Date.now();
    
    // 清理1秒前的调用记录
    this.calls = this.calls.filter(time => now - time < 1000);
    
    // 检查每秒限制
    if (this.calls.length >= this.maxCallsPerSecond) {
      const oldestCall = this.calls[0];
      const waitTime = 1000 - (now - oldestCall);
      if (waitTime > 0) {
        await new Promise(resolve => setTimeout(resolve, waitTime));
        return this.waitForSlot();
      }
    }

    // 记录此次调用
    this.calls.push(now);
  }
}

const telegramRateLimiter = new TelegramRateLimiter();

async function fetchWithRetry(url, options, retries = 3, initialBackoff = 1000) {
  for (let i = 0; i < retries; i++) {
    try {
      // 等待速率限制
      await telegramRateLimiter.waitForSlot();

      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 10000); // 增加超时时间
      
      const response = await fetch(url, { 
        ...options, 
        signal: controller.signal,
        headers: {
          ...options.headers,
          'User-Agent': 'Telegram Bot Worker/1.0'
        }
      });
      
      clearTimeout(timeoutId);

      if (response.ok) {
        return response;
      }

      if (response.status === 429) {
        const retryAfter = response.headers.get('Retry-After') || 5;
        const delay = Math.max(parseInt(retryAfter) * 1000, 1000);
        console.log(`Rate limited. Waiting for ${delay}ms before retry.`);
        await new Promise(resolve => setTimeout(resolve, delay));
        continue;
      }

      if (response.status >= 500) {
        // 服务器错误，可以重试
        const errorText = await response.text();
        console.log(`Server error ${response.status}: ${errorText}`);
        throw new Error(`Server error ${response.status}: ${errorText}`);
      }

      // 客户端错误，不重试
      const errorText = await response.text();
      throw new Error(`Request failed with status ${response.status}: ${errorText}`);

    } catch (error) {
      if (i === retries - 1) throw error;
      
      // 指数退避 + 随机抖动
      const exponentialWait = initialBackoff * Math.pow(2, i);
      const jitter = Math.random() * 0.3 * exponentialWait;
      const waitTime = Math.floor(exponentialWait + jitter);
      
      console.log(`Request to ${url} failed (attempt ${i+1}/${retries}). Retrying in ${waitTime}ms. Error: ${error.message}`);
      await new Promise(resolve => setTimeout(resolve, waitTime));
    }
  }
  
  throw new Error(`Failed to fetch ${url} after ${retries} retries`);
}

async function setSetting(key, value) {
  await env.D1.prepare('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)')
    .bind(key, value)
    .run();
  if (key === 'verification_enabled') {
    settingsCache.set('verification_enabled', value === 'true');
    if (value === 'false') {
      await env.D1.prepare('UPDATE user_states SET is_verified = ?, verified_expiry = NULL, is_verifying = ?, verification_code = NULL, code_expiry = NULL, is_first_verification = ? WHERE chat_id NOT IN (SELECT chat_id FROM user_states WHERE is_blocked = TRUE)')
        .bind(true, false, false)
        .run();
      userStateCache.clear();
    }
  } else if (key === 'user_raw_enabled') {
    settingsCache.set('user_raw_enabled', value === 'true');
  }
}

async function onCallbackQuery(callbackQuery) {
  const chatId = callbackQuery.message.chat.id.toString();
  const topicId = callbackQuery.message.message_thread_id;
  const data = callbackQuery.data;
  const messageId = callbackQuery.message.message_id;
  const callbackKey = `${chatId}:${callbackQuery.id}`;

  if (processedCallbacks.has(callbackKey)) {
    return;
  }
  processedCallbacks.add(callbackKey);

  const parts = data.split('_');
  let action;
  let privateChatId;

  if (data.startsWith('verify_')) {
    action = 'verify';
    privateChatId = parts[1];
  } else if (data.startsWith('toggle_verification_')) {
    action = 'toggle_verification';
    privateChatId = parts.slice(2).join('_');
  } else if (data.startsWith('toggle_user_raw_')) {
    action = 'toggle_user_raw';
    privateChatId = parts.slice(3).join('_');
  } else if (data.startsWith('check_blocklist_')) {
    action = 'check_blocklist';
    privateChatId = parts.slice(2).join('_');
  } else if (data.startsWith('block_')) {
    action = 'block';
    privateChatId = parts.slice(1).join('_');
  } else if (data.startsWith('unblock_')) {
    action = 'unblock';
    privateChatId = parts.slice(1).join('_');
  } else if (data.startsWith('delete_user_')) {
    action = 'delete_user';
    privateChatId = parts.slice(2).join('_');
  } else if (data.startsWith('show_update_')) {
    action = 'show_update';
    privateChatId = parts.slice(2).join('_');
  } else if (data.startsWith('check_update_')) {
    action = 'check_update';
    privateChatId = parts.slice(2).join('_');
  } else {
    action = data;
    privateChatId = '';
  }

  if (action === 'verify') {
    await handleVerificationCallback(callbackQuery, chatId, data, messageId);
  } else {
    await handleAdminCallback(callbackQuery, chatId, topicId, action, privateChatId, messageId);
  }

await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/answerCallbackQuery`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          callback_query_id: callbackQuery.id
        })
      });
    }

async function handleVerificationCallback(callbackQuery, chatId, data, messageId) {
  const [, userChatId, selectedAnswer, result] = data.split('_');
  if (userChatId !== chatId) {
    return;
  }

  // 原子性验证状态检查和更新
  const verificationResult = await atomicVerificationCheck(chatId, selectedAnswer);
  
  if (verificationResult.expired) {
    await sendMessageToUser(chatId, '验证码已过期，正在为您发送新的验证码...');
    await resetVerificationState(chatId);
    
    try {
      await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          chat_id: chatId,
          message_id: messageId
        })
      });
    } catch (error) {
      console.log(`删除过期验证按钮失败: ${error.message}`);
    }

    try {
      await handleVerification(chatId, 0);
    } catch (verificationError) {
      console.error(`发送新验证码失败: ${verificationError.message}`);
      await sendMessageToUser(chatId, '发送验证码失败，请发送任意消息重试');
    }
    return;
  }

  if (verificationResult.success) {
    await completeVerification(chatId);
    const successMessage = await getVerificationSuccessMessage();
    await sendMessageToUser(chatId, `${successMessage}\n你好，欢迎使用私聊机器人！现在可以发送消息了。`);
    
    const userInfo = await getUserInfo(chatId);
    await ensureUserTopic(chatId, userInfo);
  } else {
    await sendMessageToUser(chatId, '验证失败，请重新尝试。');
    await handleVerification(chatId, messageId);
  }

  await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      chat_id: chatId,
      message_id: messageId
    })
  });
}

async function atomicVerificationCheck(chatId, selectedAnswer) {
  const nowSeconds = Math.floor(Date.now() / 1000);
  
  // 原子性检查验证状态
  const verificationState = await env.D1.prepare(`
    SELECT verification_code, code_expiry, is_verifying 
    FROM user_states 
    WHERE chat_id = ?
  `).bind(chatId).first();

  if (!verificationState) {
    return { expired: true, success: false };
  }

  const { verification_code, code_expiry, is_verifying } = verificationState;

  // 检查是否过期
  if (!verification_code || !code_expiry || nowSeconds > code_expiry || !is_verifying) {
    return { expired: true, success: false };
  }

  // 检查答案是否正确
  const isCorrect = selectedAnswer === verification_code;
  
  return { expired: false, success: isCorrect };
}

async function completeVerification(chatId) {
  const nowSeconds = Math.floor(Date.now() / 1000);
  
  // 原子性更新验证状态
  await env.D1.batch([
    env.D1.prepare(`
      UPDATE user_states 
      SET is_verified = ?, verified_expiry = NULL, verification_code = NULL, 
          code_expiry = NULL, last_verification_message_id = NULL, 
          is_first_verification = ?, is_verifying = ? 
      WHERE chat_id = ?
    `).bind(true, false, false, chatId),
    
    env.D1.prepare(`
      UPDATE message_rates 
      SET message_count = ?, window_start = ? 
      WHERE chat_id = ?
    `).bind(0, nowSeconds * 1000, chatId)
  ]);

  // 更新缓存
  const verificationState = await env.D1.prepare(`
    SELECT is_verified, verified_expiry, verification_code, code_expiry, 
           last_verification_message_id, is_first_verification, is_verifying 
    FROM user_states 
    WHERE chat_id = ?
  `).bind(chatId).first();
  
  userStateCache.set(chatId, verificationState);
  
  const rateData = { message_count: 0, window_start: nowSeconds * 1000 };
  messageRateCache.set(chatId, rateData);
}

async function handleVerification(chatId, messageId) {
      try {
        let userState = userStateCache.get(chatId);
        if (userState === undefined) {
          userState = await env.D1.prepare('SELECT is_blocked, is_first_verification, is_verified, verified_expiry, is_verifying FROM user_states WHERE chat_id = ?')
            .bind(chatId)
            .first();
          if (!userState) {
            userState = { is_blocked: false, is_first_verification: true, is_verified: false, verified_expiry: null, is_verifying: false };
          }
          userStateCache.set(chatId, userState);
        }

userState.verification_code = null;
        userState.code_expiry = null;
        userState.is_verifying = true;
        userStateCache.set(chatId, userState);
        await env.D1.prepare('UPDATE user_states SET verification_code = NULL, code_expiry = NULL, is_verifying = ? WHERE chat_id = ?')
          .bind(true, chatId)
          .run();

const lastVerification = userState.last_verification_message_id || (await env.D1.prepare('SELECT last_verification_message_id FROM user_states WHERE chat_id = ?')
          .bind(chatId)
          .first())?.last_verification_message_id;

if (lastVerification) {
          try {
            await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                chat_id: chatId,
                message_id: lastVerification
              })
            });
          } catch (deleteError) {
            console.log(`删除上一条验证消息失败: ${deleteError.message}`);
          }

userState.last_verification_message_id = null;
          userStateCache.set(chatId, userState);
          await env.D1.prepare('UPDATE user_states SET last_verification_message_id = NULL WHERE chat_id = ?')
            .bind(chatId)
            .run();
        }

await sendVerification(chatId);
      } catch (error) {
        console.error(`处理验证过程失败: ${error.message}`);
        try {
          await env.D1.prepare('UPDATE user_states SET is_verifying = FALSE WHERE chat_id = ?')
            .bind(chatId)
            .run();
          let currentState = userStateCache.get(chatId);
          if (currentState) {
            currentState.is_verifying = false;
            userStateCache.set(chatId, currentState);
          }
        } catch (resetError) {
          console.error(`重置用户验证状态失败: ${resetError.message}`);
        }
        throw error;
      }
    }

async function sendVerification(chatId) {
      try {
        const num1 = Math.floor(Math.random() * 10);
        const num2 = Math.floor(Math.random() * 10);
        const operation = Math.random() > 0.5 ? '+' : '-';
        const correctResult = operation === '+' ? num1 + num2 : num1 - num2;

const options = new Set([correctResult]);
        while (options.size < 4) {
          const wrongResult = correctResult + Math.floor(Math.random() * 5) - 2;
          if (wrongResult !== correctResult) options.add(wrongResult);
        }
        const optionArray = Array.from(options).sort(() => Math.random() - 0.5);

const buttons = optionArray.map(option => ({
          text: `(${option})`,
          callback_data: `verify_${chatId}_${option}_${option === correctResult ? 'correct' : 'wrong'}`
        }));

const question = `请计算：${num1} ${operation} ${num2} = ?（点击下方按钮完成验证，勿重复点击！）`;
        const nowSeconds = Math.floor(Date.now() / 1000);
        const codeExpiry = nowSeconds + 300;

let userState = userStateCache.get(chatId);
        if (userState === undefined) {
          userState = { verification_code: correctResult.toString(), code_expiry: codeExpiry, last_verification_message_id: null, is_verifying: true };
        } else {
          userState.verification_code = correctResult.toString();
          userState.code_expiry = codeExpiry;
          userState.last_verification_message_id = null;
          userState.is_verifying = true;
        }
        userStateCache.set(chatId, userState);

const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: chatId,
            text: question,
            reply_markup: { inline_keyboard: [buttons] }
          })
        });
        const data = await response.json();
        if (data.ok) {
          userState.last_verification_message_id = data.result.message_id.toString();
          userStateCache.set(chatId, userState);
          await env.D1.prepare('UPDATE user_states SET verification_code = ?, code_expiry = ?, last_verification_message_id = ?, is_verifying = ? WHERE chat_id = ?')
            .bind(correctResult.toString(), codeExpiry, data.result.message_id.toString(), true, chatId)
            .run();
        } else {
          throw new Error(`Telegram API 返回错误: ${data.description || '未知错误'}`);
        }
      } catch (error) {
        console.error(`发送验证码失败: ${error.message}`);
        throw error;
      }
    }

async function checkIfAdmin(userId) {
      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getChatMember`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          chat_id: GROUP_ID,
          user_id: userId
        })
      });
      const data = await response.json();
      return data.ok && (data.result.status === 'administrator' || data.result.status === 'creator');
    }

async function getUserInfo(chatId) {
      let userInfo = userInfoCache.get(chatId);
      if (userInfo !== undefined) {
        return userInfo;
      }

const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getChat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: chatId })
      });
      const data = await response.json();
      if (!data.ok) {
        userInfo = {
          id: chatId,
          username: `User_${chatId}`,
          nickname: `User_${chatId}`
        };
      } else {
        const result = data.result;
        const nickname = result.first_name
          ? `${result.first_name}${result.last_name ? ` ${result.last_name}` : ''}`.trim()
          : result.username || `User_${chatId}`;
        userInfo = {
          id: result.id || chatId,
          username: result.username || `User_${chatId}`,
          nickname: nickname
        };
      }

userInfoCache.set(chatId, userInfo);
      return userInfo;
    }

async function getExistingTopicId(chatId) {
  // 首先检查缓存
      let topicId = topicIdCache.get(chatId);
  if (topicId !== undefined && topicId !== 'creating') {
    // 验证缓存的话题ID是否有效
    const isValid = await validateTopic(topicId, true);
    if (isValid) {
          return topicId;
        }
    // 缓存无效，清理
        topicIdCache.set(chatId, undefined);
      }

  // 从数据库查询，只查询活跃状态的话题
  const result = await env.D1.prepare(`
    SELECT topic_id FROM chat_topic_mappings 
    WHERE chat_id = ? AND topic_id != 'creating' AND status = 'active'
  `).bind(chatId).first();
  
  if (result && result.topic_id) {
    // 验证数据库中的话题ID
    const isValid = await validateTopic(result.topic_id, true);
    if (isValid) {
        topicId = result.topic_id;
        topicIdCache.set(chatId, topicId);
        return topicId;
    } else {
      // 数据库中的话题ID无效，标记为非活跃
      console.log(`话题ID ${result.topic_id} 无效，标记为非活跃`);
      await env.D1.prepare(`
        UPDATE chat_topic_mappings 
        SET status = 'inactive' 
        WHERE chat_id = ?
      `).bind(chatId).run();
    }
  }

return null;
    }

async function createForumTopic(topicName, userName, nickname, userId) {
      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/createForumTopic`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: GROUP_ID, name: `${nickname}` })
      });
      const data = await response.json();
      if (!data.ok) throw new Error(`Failed to create forum topic: ${data.description}`);
      const topicId = data.result.message_thread_id;

const now = new Date();
      const formattedTime = now.toISOString().replace('T', ' ').substring(0, 19);
      const notificationContent = await getNotificationContent();
      const pinnedMessage = `昵称: ${nickname}\n用户名: @${userName}\nUserID: ${userId}\n发起时间: ${formattedTime}\n\n${notificationContent}`;
      const messageResponse = await sendMessageToTopic(topicId, pinnedMessage);
      const messageId = messageResponse.result.message_id;
      await pinMessage(topicId, messageId);

return topicId;
    }

async function saveTopicId(chatId, topicId) {
  try {
    // 使用 UPSERT 操作确保原子性
    await env.D1.prepare(`
      INSERT OR REPLACE INTO chat_topic_mappings 
      (chat_id, topic_id, created_at, status) 
      VALUES (?, ?, strftime('%s', 'now'), 'active')
    `).bind(chatId, topicId).run();
    
    // 更新缓存
topicIdCache.set(chatId, topicId);
    
    console.log(`话题ID已保存: ${chatId} -> ${topicId}`);
  } catch (error) {
    console.error(`保存话题ID失败: ${error.message}`);
    throw error;
    }
}

async function getPrivateChatId(topicId) {
  // 首先检查缓存
  for (const [chatId, tid] of topicIdCache.cache) {
    if (tid === topicId) return chatId;
  }
  
  // 从数据库查询，只查询活跃状态的话题
  const mapping = await env.D1.prepare(`
    SELECT chat_id FROM chat_topic_mappings 
    WHERE topic_id = ? AND status = 'active'
  `).bind(topicId).first();
  
  if (mapping) {
    // 更新缓存
    topicIdCache.set(mapping.chat_id, topicId);
    return mapping.chat_id;
  }
  
  return null;
}

async function sendMessageToTopic(topicId, text) {
      if (!text.trim()) {
        throw new Error('Message text is empty');
      }

const requestBody = {
        chat_id: GROUP_ID,
        text: text,
        message_thread_id: topicId
      };
      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestBody)
      });
      const data = await response.json();
      if (!data.ok) {
        throw new Error(`Failed to send message to topic ${topicId}: ${data.description}`);
      }
      return data;
    }

async function copyMessageToTopic(topicId, message) {
      const requestBody = {
        chat_id: GROUP_ID,
        from_chat_id: message.chat.id,
        message_id: message.message_id,
        message_thread_id: topicId,
        disable_notification: true
      };
      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/copyMessage`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestBody)
      });
      const data = await response.json();
      if (!data.ok) {
        throw new Error(`Failed to copy message to topic ${topicId}: ${data.description}`);
      }
    }

async function pinMessage(topicId, messageId) {
      const requestBody = {
        chat_id: GROUP_ID,
        message_id: messageId,
        message_thread_id: topicId
      };
      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/pinChatMessage`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestBody)
      });
      const data = await response.json();
      if (!data.ok) {
        throw new Error(`Failed to pin message: ${data.description}`);
      }
    }

async function forwardMessageToPrivateChat(privateChatId, message) {
      const requestBody = {
        chat_id: privateChatId,
        from_chat_id: message.chat.id,
        message_id: message.message_id,
        disable_notification: true
      };
      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/copyMessage`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestBody)
      });
      const data = await response.json();
      if (!data.ok) {
        throw new Error(`Failed to forward message to private chat: ${data.description}`);
      }
    }

async function sendMessageToUser(chatId, text) {
      const requestBody = { chat_id: chatId, text: text };
      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestBody)
      });
      const data = await response.json();
      if (!data.ok) {
        throw new Error(`Failed to send message to user: ${data.description}`);
      }
    }

async function registerWebhook(request) {
      const webhookUrl = `${new URL(request.url).origin}/webhook`;
      const response = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/setWebhook`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ url: webhookUrl })
      }).then(r => r.json());
      return new Response(response.ok ? 'Webhook set successfully' : JSON.stringify(response, null, 2));
    }

async function unRegisterWebhook() {
      const response = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/setWebhook`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ url: '' })
      }).then(r => r.json());
      return new Response(response.ok ? 'Webhook removed' : JSON.stringify(response, null, 2));
    }

async function setupPeriodicCleanup(d1) {
      await performCacheCleanup();
  await cleanupExpiredLocks(d1);
  
setInterval(async () => {
        try {
          await performCacheCleanup();
      await cleanupExpiredLocks(d1);
        } catch (error) {
      console.error(`定期清理失败: ${error.message}`);
        }
      }, CACHE_CLEANUP_INTERVAL);
    }

async function performCacheCleanup() {
      const now = Date.now();
      if (now - lastCacheCleanupTime < CACHE_CLEANUP_INTERVAL) {
        return;
      }

console.log('执行缓存清理...');
userInfoCache.cleanStale(3 * 60 * 60 * 1000);
      topicIdCache.cleanStale(3 * 60 * 60 * 1000);
      userStateCache.cleanStale(3 * 60 * 60 * 1000);
      messageRateCache.cleanStale(3 * 60 * 60 * 1000);

lastCacheCleanupTime = now;
      console.log('缓存清理完成');
    }

async function hasNewVersion() {
      try {
        const remoteVersion = await getRemoteVersion();
const normalizedRemote = remoteVersion.toLowerCase().replace(/\s+/g, '');
        const normalizedCurrent = CURRENT_VERSION.toLowerCase().replace(/\s+/g, '');
console.log(`版本比较详情:`);
        console.log(`- 当前版本(原始): "${CURRENT_VERSION}"`);
        console.log(`- 远程版本(原始): "${remoteVersion}"`);
        console.log(`- 当前版本(规范化): "${normalizedCurrent}"`);
        console.log(`- 远程版本(规范化): "${normalizedRemote}"`);
        console.log(`- 是否需要更新: ${normalizedRemote !== normalizedCurrent}`);
return normalizedRemote !== normalizedCurrent;
      } catch (error) {
        console.error(`版本比较失败: ${error.message}`);
        return false;
      }
    }

async function getRemoteVersion() {
      try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 3000);
const cacheBuster = `?t=${Date.now()}`;
        const response = await fetch(`${VERSION_CHECK_URL}${cacheBuster}`, { 
          signal: controller.signal, 
          cache: 'no-store',
          headers: {
            'Cache-Control': 'no-cache, no-store, must-revalidate',
            'Pragma': 'no-cache',
            'Expires': '0'
          }
        });
        clearTimeout(timeoutId);

if (!response.ok) {
          console.error(`获取远程版本失败: ${response.status}`);
          return CURRENT_VERSION;
        }

const versionText = await response.text();
        return versionText.trim();
      } catch (error) {
        console.error(`获取远程版本异常: ${error.message}`);
        return CURRENT_VERSION;
      }
    }

async function getUpdateInfo() {
      try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 3000);
const cacheBuster = `?t=${Date.now()}`;
        const response = await fetch(`${UPDATE_INFO_URL}${cacheBuster}`, { 
          signal: controller.signal, 
          cache: 'no-store',
          headers: {
            'Cache-Control': 'no-cache, no-store, must-revalidate',
            'Pragma': 'no-cache',
            'Expires': '0'
          }
        });
        clearTimeout(timeoutId);

if (!response.ok) {
          console.error(`获取更新信息失败: ${response.status}`);
          return "获取更新信息失败，请直接访问项目仓库查看。";
        }

const updateText = await response.text();
        return updateText.trim() || "有新版本可用，请访问GitHub项目查看详情。";
      } catch (error) {
        console.error(`获取更新信息异常: ${error.message}`);
        return "获取更新信息发生错误，请直接访问项目仓库。";
      }
    }

async function batchUpdateUserStates(d1, operations, batchSize = 50) {
      const batches = [];
      for (let i = 0; i < operations.length; i += batchSize) {
        batches.push(operations.slice(i, i + batchSize));
      }

for (const batch of batches) {
        await d1.batch(batch);
      }
    }

async function cleanupExpiredLocks(d1) {
  try {
    const lockTimeout = 30000; // 30秒
    const cutoffTime = Date.now() - lockTimeout;
    
    // 清理过期的创建锁
    const expiredLocks = await d1.prepare(`
      SELECT chat_id FROM topic_creation_locks 
      WHERE lock_time < ?
    `).bind(cutoffTime).all();
    
    if (expiredLocks.results && expiredLocks.results.length > 0) {
      console.log(`清理 ${expiredLocks.results.length} 个过期的创建锁`);
      
      await d1.batch([
        d1.prepare(`DELETE FROM topic_creation_locks WHERE lock_time < ?`).bind(cutoffTime),
        d1.prepare(`DELETE FROM chat_topic_mappings WHERE topic_id = 'creating' AND chat_id IN (
          SELECT chat_id FROM topic_creation_locks WHERE lock_time < ?
        )`).bind(cutoffTime)
      ]);
      
      // 清理缓存
      for (const row of expiredLocks.results) {
        topicIdCache.set(row.chat_id, undefined);
      }
    }
    
    // 清理过期的时间戳设置
    await d1.prepare(`DELETE FROM settings WHERE key LIKE 'creating_timestamp_%'`).run();
    
    } catch (error) {
    console.error(`清理过期锁失败: ${error.message}`);
  }
}

// 智能缓存预热器
class CachePreloader {
  constructor() {
    this.preloadQueue = new Set();
    this.preloadInProgress = new Map();
  }

  async preloadUserData(chatId) {
    if (this.preloadInProgress.has(chatId)) {
      return;
    }

    this.preloadInProgress.set(chatId, true);
    
    try {
      // 并行预加载用户相关数据
      await Promise.all([
        this.preloadUserState(chatId),
        this.preloadUserInfo(chatId),
        this.preloadTopicId(chatId),
        this.preloadMessageRate(chatId)
      ]);
    } catch (error) {
      console.error(`预加载用户数据失败 ${chatId}: ${error.message}`);
    } finally {
      this.preloadInProgress.delete(chatId);
    }
  }

  async preloadUserState(chatId) {
    if (userStateCache.get(chatId) === undefined) {
      try {
        const userState = await env.D1.prepare('SELECT * FROM user_states WHERE chat_id = ?')
          .bind(chatId)
          .first();
        if (userState) {
          userStateCache.set(chatId, userState);
        }
      } catch (error) {
        console.error(`预加载用户状态失败: ${error.message}`);
      }
    }
  }

  async preloadUserInfo(chatId) {
    if (userInfoCache.get(chatId) === undefined) {
      try {
        await getUserInfo(chatId);
      } catch (error) {
        console.error(`预加载用户信息失败: ${error.message}`);
      }
    }
  }

  async preloadTopicId(chatId) {
    if (topicIdCache.get(chatId) === undefined) {
      try {
        await getExistingTopicId(chatId);
      } catch (error) {
        console.error(`预加载话题ID失败: ${error.message}`);
      }
    }
  }

  async preloadMessageRate(chatId) {
    if (messageRateCache.get(chatId) === undefined) {
      try {
        const data = await env.D1.prepare('SELECT * FROM message_rates WHERE chat_id = ?')
          .bind(chatId)
          .first();
        if (data) {
          messageRateCache.set(chatId, data);
        }
      } catch (error) {
        console.error(`预加载消息频率失败: ${error.message}`);
      }
    }
  }
}

// 死锁检测器
class DeadlockDetector {
  constructor() {
    this.resourceWaiters = new Map(); // resource -> Set<chatId>
    this.waitingFor = new Map(); // chatId -> resource
    this.timeouts = new Map(); // chatId -> timeout
  }

  startWaiting(chatId, resource, timeoutMs = 30000) {
    // 检测死锁
    if (this.detectDeadlock(chatId, resource)) {
      console.warn(`检测到潜在死锁: ${chatId} 等待 ${resource}`);
      this.breakDeadlock(chatId, resource);
      return false;
    }

    // 记录等待关系
    if (!this.resourceWaiters.has(resource)) {
      this.resourceWaiters.set(resource, new Set());
    }
    this.resourceWaiters.get(resource).add(chatId);
    this.waitingFor.set(chatId, resource);

    // 设置超时
    const timeoutId = setTimeout(() => {
      this.stopWaiting(chatId);
      console.warn(`等待超时: ${chatId} 等待 ${resource}`);
    }, timeoutMs);
    this.timeouts.set(chatId, timeoutId);

    return true;
  }

  stopWaiting(chatId) {
    const resource = this.waitingFor.get(chatId);
    if (resource) {
      const waiters = this.resourceWaiters.get(resource);
      if (waiters) {
        waiters.delete(chatId);
        if (waiters.size === 0) {
          this.resourceWaiters.delete(resource);
        }
      }
      this.waitingFor.delete(chatId);
    }

    const timeoutId = this.timeouts.get(chatId);
    if (timeoutId) {
      clearTimeout(timeoutId);
      this.timeouts.delete(chatId);
    }
  }

  detectDeadlock(chatId, resource) {
    // 简单的循环等待检测
    const visited = new Set();
    let current = resource;

    while (current && !visited.has(current)) {
      visited.add(current);
      
      // 查找持有当前资源的用户
      for (const [holderId, heldResource] of this.waitingFor.entries()) {
        if (heldResource === current) {
          // 如果持有者在等待chatId需要的资源，形成循环
          if (holderId === chatId) {
            return true;
          }
          current = `user_${holderId}`;
          break;
        }
      }
      
      if (current === `user_${chatId}`) {
        return true;
      }
    }

    return false;
  }

  breakDeadlock(chatId, resource) {
    // 简单的死锁破除：取消最新的等待
    console.log(`破除死锁: 取消 ${chatId} 对 ${resource} 的等待`);
    this.stopWaiting(chatId);
  }

  getWaitingStats() {
    return {
      totalWaiters: this.waitingFor.size,
      resources: this.resourceWaiters.size,
      details: Array.from(this.resourceWaiters.entries()).map(([resource, waiters]) => ({
        resource,
        waiters: Array.from(waiters)
      }))
    };
  }
}

// 消息批处理器
class MessageBatcher {
  constructor(batchSize = 10, flushInterval = 1000) {
    this.batchSize = batchSize;
    this.flushInterval = flushInterval;
    this.batches = new Map(); // topicId -> messages[]
    this.timers = new Map(); // topicId -> timer
  }

  addMessage(topicId, message) {
    if (!this.batches.has(topicId)) {
      this.batches.set(topicId, []);
    }

    const batch = this.batches.get(topicId);
    batch.push(message);

    // 如果达到批处理大小，立即发送
    if (batch.length >= this.batchSize) {
      this.flushBatch(topicId);
    } else {
      // 设置定时器
      this.setFlushTimer(topicId);
    }
  }

  setFlushTimer(topicId) {
    if (this.timers.has(topicId)) {
      clearTimeout(this.timers.get(topicId));
    }

    const timer = setTimeout(() => {
      this.flushBatch(topicId);
    }, this.flushInterval);

    this.timers.set(topicId, timer);
  }

  async flushBatch(topicId) {
    const batch = this.batches.get(topicId);
    if (!batch || batch.length === 0) {
      return;
    }

    // 清理定时器
    if (this.timers.has(topicId)) {
      clearTimeout(this.timers.get(topicId));
      this.timers.delete(topicId);
    }

    try {
      // 合并消息内容
      const combinedMessage = batch.map(msg => msg.text).join('\n---\n');
      
      if (combinedMessage.length > 4000) {
        // 如果消息过长，分批发送
        for (const message of batch) {
          await sendMessageToTopic(topicId, message.text);
          await new Promise(resolve => setTimeout(resolve, 100)); // 防止过快发送
        }
      } else {
        // 发送合并消息
        await sendMessageToTopic(topicId, combinedMessage);
      }

      console.log(`批量发送 ${batch.length} 条消息到话题 ${topicId}`);
    } catch (error) {
      console.error(`批量发送失败: ${error.message}`);
      // 回退到单独发送
      for (const message of batch) {
        try {
          await sendMessageToTopic(topicId, message.text);
        } catch (sendError) {
          console.error(`单独发送失败: ${sendError.message}`);
        }
      }
    } finally {
      this.batches.delete(topicId);
    }
  }

  async flushAll() {
    const topicIds = Array.from(this.batches.keys());
    await Promise.all(topicIds.map(id => this.flushBatch(id)));
  }
}

// 全局实例
const cachePreloader = new CachePreloader();
const deadlockDetector = new DeadlockDetector();
const messageBatcher = new MessageBatcher();
