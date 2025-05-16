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
const processedMessages = new Set();
const processedCallbacks = new Set();
const topicCreationLocks = new Map();
const settingsCache = new Map([
  ['verification_enabled', null],
  ['user_raw_enabled', null]
]);
class LRUCache {
  constructor(maxSize) {
    this.maxSize = maxSize;
    this.cache = new Map();
    this.lastAccess = new Map();
  }
  get(key) {
    const value = this.cache.get(key);
    if (value !== undefined) {
      this.cache.delete(key);
      this.cache.set(key, value);
      this.lastAccess.set(key, Date.now());
    }
    return value;
  }
  set(key, value) {
    if (this.cache.size >= this.maxSize) {
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
    const now = Date.now();
    for (const [key, lastAccessTime] of this.lastAccess.entries()) {
      if (now - lastAccessTime > maxAge) {
        this.cache.delete(key);
        this.lastAccess.delete(key);
      }
    }
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
            topic_id: 'TEXT NOT NULL'
          }
        },
        settings: {
          columns: {
            key: 'TEXT PRIMARY KEY',
            value: 'TEXT'
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
        if (tableName === 'settings') {
          await d1.exec('CREATE INDEX IF NOT EXISTS idx_settings_key ON settings (key)');
        }
      }
      await Promise.all([
        d1.prepare('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)')
          .bind('verification_enabled', 'true').run(),
        d1.prepare('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)')
          .bind('user_raw_enabled', 'true').run()
      ]);
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
        const creatingTopics = await d1.prepare('SELECT chat_id FROM chat_topic_mappings WHERE topic_id = ?')
          .bind('creating')
          .all();
        if (creatingTopics.results.length > 0) {
          console.log(`清理 ${creatingTopics.results.length} 个遗留的临时话题标记`);
          await d1.prepare('DELETE FROM chat_topic_mappings WHERE topic_id = ?')
            .bind('creating')
            .run();
          for (const row of creatingTopics.results) {
            topicIdCache.set(row.chat_id, undefined);
          }
        }
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
        if (processedMessages.size > 10000) {
          processedMessages.clear();
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
let userState = userStateCache.get(chatId);
      if (userState === undefined) {
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
      }
if (userState.is_blocked) {
        await sendMessageToUser(chatId, "您已被拉黑，无法发送消息。请联系管理员解除拉黑。");
        return;
      }
const verificationEnabled = (await getSetting('verification_enabled', env.D1)) === 'true';
if (!verificationEnabled) {
      } else {
        const nowSeconds = Math.floor(Date.now() / 1000);
        const isVerified = userState.is_verified;
        const isFirstVerification = userState.is_first_verification;
        const isRateLimited = await checkMessageRate(chatId);
        const isVerifying = userState.is_verifying || false;
if (!isVerified || (isRateLimited && !isFirstVerification)) {
          if (isVerifying) {
            const storedCode = await env.D1.prepare('SELECT verification_code, code_expiry FROM user_states WHERE chat_id = ?')
              .bind(chatId)
              .first();
const nowSeconds = Math.floor(Date.now() / 1000);
            const isCodeExpired = !storedCode?.verification_code || !storedCode?.code_expiry || nowSeconds > storedCode.code_expiry;
if (isCodeExpired) {
              await sendMessageToUser(chatId, '验证码已过期，正在为您发送新的验证码...');
              await env.D1.prepare('UPDATE user_states SET verification_code = NULL, code_expiry = NULL, is_verifying = FALSE WHERE chat_id = ?')
                .bind(chatId)
                .run();
              userStateCache.set(chatId, { ...userState, verification_code: null, code_expiry: null, is_verifying: false });
try {
                const lastVerification = await env.D1.prepare('SELECT last_verification_message_id FROM user_states WHERE chat_id = ?')
                  .bind(chatId)
                  .first();
if (lastVerification?.last_verification_message_id) {
                  try {
                    await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
                      method: 'POST',
                      headers: { 'Content-Type': 'application/json' },
                      body: JSON.stringify({
                        chat_id: chatId,
                        message_id: lastVerification.last_verification_message_id
                      })
                    });
                  } catch (deleteError) {
                    console.log(`删除旧验证消息失败: ${deleteError.message}`);
                  }
await env.D1.prepare('UPDATE user_states SET last_verification_message_id = NULL WHERE chat_id = ?')
                    .bind(chatId)
                    .run();
                }
              } catch (error) {
                console.log(`查询旧验证消息失败: ${error.message}`);
              }
try {
                await handleVerification(chatId, 0);
              } catch (verificationError) {
                console.error(`发送新验证码失败: ${verificationError.message}`);
                setTimeout(async () => {
                  try {
                    await handleVerification(chatId, 0);
                  } catch (retryError) {
                    console.error(`重试发送验证码仍失败: ${retryError.message}`);
                    await sendMessageToUser(chatId, '发送验证码失败，请发送任意消息重试');
                  }
                }, 1000);
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
        return;
      }
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
const isTopicValid = await validateTopic(topicId, false); // 使用完整验证
      if (!isTopicValid) {
        console.log(`用户 ${chatId} 的话题 ${topicId} 无效，将重新创建`);
        await env.D1.prepare('DELETE FROM chat_topic_mappings WHERE chat_id = ?').bind(chatId).run();
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
async function validateTopic(topicId, quickCheck = false) {
      if (!topicId || topicId === 'creating') {
        return false;
      }
      try {
        // 快速检查模式只验证话题ID是否有效，不会发送测试消息
        if (quickCheck) {
          const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getChat`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              chat_id: GROUP_ID
            })
          }, 1); // 仅重试1次，加快返回速度
if (!response.ok) {
            console.error(`快速验证话题ID失败: 无法访问目标群组`);
            return false;
          }
// 话题ID存在即视为有效，不做进一步验证
          return true;
        }
// 完整验证包括发送测试消息
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: GROUP_ID,
            message_thread_id: topicId,
            text: "您有新消息！",
            disable_notification: true
          })
        });
        const data = await response.json();
        if (data.ok) {
          await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              chat_id: GROUP_ID,
              message_id: data.result.message_id
            })
          });
          return true;
        }
        return false;
      } catch (error) {
        console.error(`验证话题ID ${topicId} 失败: ${error.message}`);
        return false;
      }
    }
async function ensureUserTopic(chatId, userInfo) {
      // 在真正创建话题之前，先检查是否有有效话题ID
             const cachedTopicId = topicIdCache.get(chatId);
       if (cachedTopicId && cachedTopicId !== 'creating') {
         try {
           // 快速验证话题是否可用
           const isValid = await validateTopic(cachedTopicId, true);
           if (isValid) {
             return cachedTopicId;
           } else {
             // 无效话题ID从缓存中删除
             console.log(`缓存中话题ID ${cachedTopicId} 无效，将重新创建`);
             topicIdCache.set(chatId, undefined);
           }
         } catch (error) {
           console.error(`验证缓存话题ID失败: ${error.message}`);
           // 出错不立即清除缓存，继续检查数据库
         }
       }
// 全局同步锁，确保同一用户的话题创建请求串行处理
      let lock = topicCreationLocks.get(chatId);
      if (!lock) {
        lock = Promise.resolve();
        topicCreationLocks.set(chatId, lock);
      }
// 创建新锁
      const newLock = (async () => {
        try {
          // 等待前一个锁完成
          await lock;
// 再次从数据库查询最新状态
          const dbTopicInfo = await env.D1.prepare('SELECT topic_id FROM chat_topic_mappings WHERE chat_id = ?')
            .bind(chatId)
            .first();
// 如果数据库中已有有效话题ID，优先使用
                     if (dbTopicInfo && dbTopicInfo.topic_id !== 'creating') {
             try {
               // 验证话题是否可用
               const isValid = await validateTopic(dbTopicInfo.topic_id, true);
               if (isValid) {
                 topicIdCache.set(chatId, dbTopicInfo.topic_id);
                 return dbTopicInfo.topic_id;
               } else {
                 // 无效话题ID需要从数据库删除
                 console.log(`数据库中话题ID ${dbTopicInfo.topic_id} 无效，将删除并重新创建`);
                 await env.D1.prepare('DELETE FROM chat_topic_mappings WHERE chat_id = ?')
                   .bind(chatId)
                   .run();
               }
             } catch (error) {
               console.error(`验证数据库话题ID失败: ${error.message}`);
               // 验证失败也继续执行创建流程
             }
           }
// 清除任何临时标记
          if (dbTopicInfo && dbTopicInfo.topic_id === 'creating') {
            // 检查创建标记是否过期（5分钟内的临时标记视为有效，可能是其他进程正在创建）
            const nowTimestamp = Date.now();
            // 尝试获取创建时间，如果能查询到
            const createTimeRecord = await env.D1.prepare('SELECT value FROM settings WHERE key = ?')
              .bind(`creating_timestamp_${chatId}`)
              .first();
let shouldClear = true;
            if (createTimeRecord && createTimeRecord.value) {
              const createTime = parseInt(createTimeRecord.value);
              const timeDiff = nowTimestamp - createTime;
// 如果标记时间在5分钟内，等待其他进程完成创建
              if (timeDiff < 5 * 60 * 1000) {
                console.log(`话题正在由其他进程创建中(${Math.floor(timeDiff/1000)}秒前)，等待完成...`);
                // 等待短暂时间后再次检查
                await new Promise(resolve => setTimeout(resolve, 2000));
// 再次检查是否创建完成
                const recheckResult = await env.D1.prepare('SELECT topic_id FROM chat_topic_mappings WHERE chat_id = ?')
                  .bind(chatId)
                  .first();
if (recheckResult && recheckResult.topic_id !== 'creating') {
                  // 已创建完成，直接返回
                  topicIdCache.set(chatId, recheckResult.topic_id);
                  return recheckResult.topic_id;
                }
// 创建超时，清除锁继续创建
                shouldClear = true;
              }
            }
if (shouldClear) {
              console.log(`清理过期的临时话题标记: ${chatId}`);
              try {
                await env.D1.batch([
                  env.D1.prepare('DELETE FROM chat_topic_mappings WHERE chat_id = ? AND topic_id = ?')
                    .bind(chatId, 'creating'),
                  env.D1.prepare('DELETE FROM settings WHERE key = ?')
                    .bind(`creating_timestamp_${chatId}`)
                ]);
              } catch (clearError) {
                console.error(`清理临时标记失败: ${clearError.message}`);
                // 继续执行，不阻断流程
              }
            }
          }
// 使用事务确保原子性操作
          try {
            // 1. 先尝试插入临时标记
            const insertOp = await env.D1.prepare('INSERT OR IGNORE INTO chat_topic_mappings (chat_id, topic_id) VALUES (?, ?)')
              .bind(chatId, 'creating')
              .run();
// 2. 记录创建开始时间
            await env.D1.prepare('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)')
              .bind(`creating_timestamp_${chatId}`, Date.now().toString())
              .run();
// 检查是否真的获取到了锁（插入成功）
            if (insertOp.changes === 0) {
              // 插入失败，可能其他进程已经在创建中
              console.log(`无法获取话题创建锁，可能其他进程正在处理: ${chatId}`);
// 等待一段时间后检查结果
              await new Promise(resolve => setTimeout(resolve, 3000));
// 再次查询结果
              const checkAgain = await env.D1.prepare('SELECT topic_id FROM chat_topic_mappings WHERE chat_id = ?')
                .bind(chatId)
                .first();
if (checkAgain && checkAgain.topic_id !== 'creating') {
                // 另一个进程已创建完成
                topicIdCache.set(chatId, checkAgain.topic_id);
                return checkAgain.topic_id;
              } else {
                // 创建仍未完成，返回null等待下次请求重试
                console.log(`等待话题创建超时: ${chatId}`);
                return null;
              }
            }
// 成功获取创建锁，开始创建话题
            console.log(`为用户 ${chatId} 创建新话题...`);
try {
              // 创建话题
              const userName = userInfo.username || `User_${chatId}`;
              const nickname = userInfo.nickname || userName;
              const topicId = await createForumTopic(nickname, userName, nickname, userInfo.id || chatId);
// 更新数据库，使用条件更新确保原子性
              const updateResult = await env.D1.prepare('UPDATE chat_topic_mappings SET topic_id = ? WHERE chat_id = ? AND topic_id = ?')
                .bind(topicId, chatId, 'creating')
                .run();
// 清理临时时间戳
              await env.D1.prepare('DELETE FROM settings WHERE key = ?')
                .bind(`creating_timestamp_${chatId}`)
                .run();
if (updateResult.changes === 0) {
                // 更新失败，再次检查是否已被其他进程更新
                const finalCheck = await env.D1.prepare('SELECT topic_id FROM chat_topic_mappings WHERE chat_id = ?')
                  .bind(chatId)
                  .first();
if (finalCheck && finalCheck.topic_id !== 'creating') {
                  // 使用数据库中已存在的值
                  console.log(`话题ID已被其他进程更新为 ${finalCheck.topic_id}`);
                  topicIdCache.set(chatId, finalCheck.topic_id);
                  return finalCheck.topic_id;
                } else {
                  // 强制更新数据库
                  console.log(`强制更新话题ID: ${chatId} -> ${topicId}`);
                  await env.D1.prepare('INSERT OR REPLACE INTO chat_topic_mappings (chat_id, topic_id) VALUES (?, ?)')
                    .bind(chatId, topicId)
                    .run();
                }
              }
// 更新缓存并返回结果
              topicIdCache.set(chatId, topicId);
              return topicId;
            } catch (createError) {
              // 创建失败，清理临时标记
              console.error(`创建话题失败: ${createError.message}`);
              await env.D1.batch([
                env.D1.prepare('DELETE FROM chat_topic_mappings WHERE chat_id = ? AND topic_id = ?')
                  .bind(chatId, 'creating'),
                env.D1.prepare('DELETE FROM settings WHERE key = ?')
                  .bind(`creating_timestamp_${chatId}`)
              ]);
topicIdCache.set(chatId, undefined);
              throw createError;
            }
          } catch (txError) {
            // 事务错误，最后检查一次
            console.error(`话题创建事务失败: ${txError.message}`);
const emergencyCheck = await env.D1.prepare('SELECT topic_id FROM chat_topic_mappings WHERE chat_id = ?')
              .bind(chatId)
              .first();
if (emergencyCheck && emergencyCheck.topic_id !== 'creating') {
              // 使用现有的话题ID
              topicIdCache.set(chatId, emergencyCheck.topic_id);
              return emergencyCheck.topic_id;
            }
// 清理所有临时状态
            await env.D1.batch([
              env.D1.prepare('DELETE FROM chat_topic_mappings WHERE chat_id = ? AND topic_id = ?')
                .bind(chatId, 'creating'),
              env.D1.prepare('DELETE FROM settings WHERE key = ?')
                .bind(`creating_timestamp_${chatId}`)
            ]);
topicIdCache.set(chatId, undefined);
            throw txError;
          }
        } finally {
          // 清理锁
          if (topicCreationLocks.get(chatId) === newLock) {
            topicCreationLocks.delete(chatId);
          }
        }
      })();
// 更新全局锁
      topicCreationLocks.set(chatId, newLock);
      return newLock;
    }
async function handleResetUser(chatId, topicId, text) {
      const senderId = chatId;
      const isAdmin = await checkIfAdmin(senderId);
      if (!isAdmin) {
        await sendMessageToTopic(topicId, '只有管理员可以使用此功能。');
        return;
      }
const parts = text.split(' ');
      if (parts.length !== 2) {
        await sendMessageToTopic(topicId, '用法：/reset_user <chat_id>');
        return;
      }
const targetChatId = parts[1];
      await env.D1.batch([
        env.D1.prepare('DELETE FROM user_states WHERE chat_id = ?').bind(targetChatId),
        env.D1.prepare('DELETE FROM message_rates WHERE chat_id = ?').bind(targetChatId),
        env.D1.prepare('DELETE FROM chat_topic_mappings WHERE chat_id = ?').bind(targetChatId)
      ]);
      userStateCache.set(targetChatId, undefined);
      messageRateCache.set(targetChatId, undefined);
      topicIdCache.set(targetChatId, undefined);
      await sendMessageToTopic(topicId, `用户 ${targetChatId} 的状态已重置。`);
    }
async function sendAdminPanel(chatId, topicId, privateChatId, messageId, forceCheckUpdate = true) {
      try {
        const verificationEnabled = (await getSetting('verification_enabled', env.D1)) === 'true';
        const userRawEnabled = (await getSetting('user_raw_enabled', env.D1)) === 'true';
let hasUpdate = false;
        try {
          if (forceCheckUpdate) {
            console.log("强制检查更新...");
            hasUpdate = await hasNewVersion();
          } else {
            hasUpdate = await hasNewVersion();
          }
        } catch (error) {
          console.error(`版本检测失败: ${error.message}`);
        }
const buttons = [
          [
            { text: '拉黑用户', callback_data: `block_${privateChatId}` },
            { text: '解除拉黑', callback_data: `unblock_${privateChatId}` }
          ],
          [
            { text: verificationEnabled ? '关闭验证码' : '开启验证码', callback_data: `toggle_verification_${privateChatId}` },
            { text: '查询黑名单', callback_data: `check_blocklist_${privateChatId}` }
          ],
          [
            { text: userRawEnabled ? '关闭用户Raw' : '开启用户Raw', callback_data: `toggle_user_raw_${privateChatId}` },
            { text: 'GitHub项目', url: 'https://github.com/iawooo/ctt' }
          ],
          [
            { text: '删除用户', callback_data: `delete_user_${privateChatId}` },
            { text: '检查更新', callback_data: `check_update_${privateChatId}` }
          ]
        ];
if (hasUpdate) {
          buttons.push([
            { text: '🔄 有新版本可用', callback_data: `show_update_${privateChatId}` }
          ]);
        }
const adminMessage = '管理员面板：请选择操作' + (hasUpdate ? '\n🔄 检测到新版本！' : '');
        await Promise.all([
          fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              chat_id: chatId,
              message_thread_id: topicId,
              text: adminMessage,
              reply_markup: { inline_keyboard: buttons }
            })
          }),
          fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              chat_id: chatId,
              message_id: messageId
            })
          })
        ]);
      } catch (error) {
        console.error(`发送管理面板失败: ${error.message}`);
        try {
          await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              chat_id: chatId,
              message_thread_id: topicId,
              text: '管理员面板加载失败，请稍后再试。'
            })
          });
        } catch (sendError) {
          console.error(`发送简化面板也失败: ${sendError.message}`);
        }
      }
    }
async function getVerificationSuccessMessage() {
      const userRawEnabled = (await getSetting('user_raw_enabled', env.D1)) === 'true';
      if (!userRawEnabled) return '验证成功！您现在可以与我聊天。';
const response = await fetch('https://raw.githubusercontent.com/iawooo/tz/refs/heads/main/CFTeleTrans/start.md');
      if (!response.ok) return '验证成功！您现在可以与我聊天。';
      const message = await response.text();
      return message.trim() || '验证成功！您现在可以与我聊天。';
    }
async function getNotificationContent() {
      const response = await fetch('https://raw.githubusercontent.com/iawooo/tz/main/CFTeleTrans/notification.md');
      if (!response.ok) return '';
      const content = await response.text();
      return content.trim() || '';
    }
async function checkStartCommandRate(chatId) {
      const now = Date.now();
      const window = 5 * 60 * 1000;
      const maxStartsPerWindow = 1;
let data = messageRateCache.get(chatId);
      if (data === undefined) {
        data = await env.D1.prepare('SELECT start_count, start_window_start FROM message_rates WHERE chat_id = ?')
          .bind(chatId)
          .first();
        if (!data) {
          data = { start_count: 0, start_window_start: now };
          await env.D1.prepare('INSERT INTO message_rates (chat_id, start_count, start_window_start) VALUES (?, ?, ?)')
            .bind(chatId, data.start_count, data.start_window_start)
            .run();
        }
        messageRateCache.set(chatId, data);
      }
if (now - data.start_window_start > window) {
        data.start_count = 1;
        data.start_window_start = now;
        await env.D1.prepare('UPDATE message_rates SET start_count = ?, start_window_start = ? WHERE chat_id = ?')
          .bind(data.start_count, data.start_window_start, chatId)
          .run();
      } else {
        data.start_count += 1;
        await env.D1.prepare('UPDATE message_rates SET start_count = ? WHERE chat_id = ?')
          .bind(data.start_count, chatId)
          .run();
      }
messageRateCache.set(chatId, data);
return data.start_count > maxStartsPerWindow;
    }
async function checkMessageRate(chatId) {
      const now = Date.now();
      const window = 60 * 1000;
let data = messageRateCache.get(chatId);
      if (data === undefined) {
        data = await env.D1.prepare('SELECT message_count, window_start FROM message_rates WHERE chat_id = ?')
          .bind(chatId)
          .first();
        if (!data) {
          data = { message_count: 0, window_start: now };
          await env.D1.prepare('INSERT INTO message_rates (chat_id, message_count, window_start) VALUES (?, ?, ?)')
            .bind(chatId, data.message_count, data.window_start)
            .run();
        }
        messageRateCache.set(chatId, data);
      }
if (now - data.window_start > window) {
        data.message_count = 1;
        data.window_start = now;
      } else {
        data.message_count += 1;
      }
messageRateCache.set(chatId, data);
      await env.D1.prepare('UPDATE message_rates SET message_count = ?, window_start = ? WHERE chat_id = ?')
        .bind(data.message_count, data.window_start, chatId)
        .run();
      return data.message_count > MAX_MESSAGES_PER_MINUTE;
    }
async function getSetting(key, d1) {
      const result = await d1.prepare('SELECT value FROM settings WHERE key = ?')
        .bind(key)
        .first();
      return result?.value || null;
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
        const [, userChatId, selectedAnswer, result] = data.split('_');
        if (userChatId !== chatId) {
          return;
        }
let verificationState = userStateCache.get(chatId);
        if (verificationState === undefined) {
          verificationState = await env.D1.prepare('SELECT verification_code, code_expiry, is_verifying FROM user_states WHERE chat_id = ?')
            .bind(chatId)
            .first();
          if (!verificationState) {
            verificationState = { verification_code: null, code_expiry: null, is_verifying: false };
          }
          userStateCache.set(chatId, verificationState);
        }
const storedCode = verificationState.verification_code;
        const codeExpiry = verificationState.code_expiry;
        const nowSeconds = Math.floor(Date.now() / 1000);
if (!storedCode || (codeExpiry && nowSeconds > codeExpiry)) {
          await sendMessageToUser(chatId, '验证码已过期，正在为您发送新的验证码...');
          await env.D1.prepare('UPDATE user_states SET verification_code = NULL, code_expiry = NULL, is_verifying = FALSE WHERE chat_id = ?')
            .bind(chatId)
            .run();
          userStateCache.set(chatId, { ...verificationState, verification_code: null, code_expiry: null, is_verifying: false });
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
            setTimeout(async () => {
              try {
                await handleVerification(chatId, 0);
              } catch (retryError) {
                console.error(`重试发送验证码仍失败: ${retryError.message}`);
                await sendMessageToUser(chatId, '发送验证码失败，请发送任意消息重试');
              }
            }, 1000);
          }
          return;
        }
if (result === 'correct') {
          await env.D1.prepare('UPDATE user_states SET is_verified = ?, verified_expiry = NULL, verification_code = NULL, code_expiry = NULL, last_verification_message_id = NULL, is_first_verification = ?, is_verifying = ? WHERE chat_id = ?')
            .bind(true, false, false, chatId)
            .run();
          verificationState = await env.D1.prepare('SELECT is_verified, verified_expiry, verification_code, code_expiry, last_verification_message_id, is_first_verification, is_verifying FROM user_states WHERE chat_id = ?')
            .bind(chatId)
            .first();
          userStateCache.set(chatId, verificationState);
let rateData = await env.D1.prepare('SELECT message_count, window_start FROM message_rates WHERE chat_id = ?')
            .bind(chatId)
            .first() || { message_count: 0, window_start: nowSeconds * 1000 };
          rateData.message_count = 0;
          rateData.window_start = nowSeconds * 1000;
          messageRateCache.set(chatId, rateData);
          await env.D1.prepare('UPDATE message_rates SET message_count = ?, window_start = ? WHERE chat_id = ?')
            .bind(0, nowSeconds * 1000, chatId)
            .run();
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
      } else {
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
            env.D1.prepare('DELETE FROM chat_topic_mappings WHERE chat_id = ?').bind(privateChatId)
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
await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/answerCallbackQuery`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          callback_query_id: callbackQuery.id
        })
      });
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
      let topicId = topicIdCache.get(chatId);
      if (topicId !== undefined) {
        if (topicId !== 'creating') {
          return topicId;
        }
        topicIdCache.set(chatId, undefined);
      }
const result = await env.D1.prepare('SELECT topic_id FROM chat_topic_mappings WHERE chat_id = ?')
        .bind(chatId)
        .first();
if (result && result.topic_id !== 'creating') {
        topicId = result.topic_id;
        topicIdCache.set(chatId, topicId);
        return topicId;
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
      const existingMapping = await env.D1.prepare('SELECT topic_id FROM chat_topic_mappings WHERE chat_id = ?')
        .bind(chatId)
        .first();
if (existingMapping) {
        if (existingMapping.topic_id !== 'creating') {
          topicIdCache.set(chatId, existingMapping.topic_id);
          return;
        }
        await env.D1.prepare('UPDATE chat_topic_mappings SET topic_id = ? WHERE chat_id = ?')
          .bind(topicId, chatId)
          .run();
      } else {
        await env.D1.prepare('INSERT INTO chat_topic_mappings (chat_id, topic_id) VALUES (?, ?)')
          .bind(chatId, topicId)
          .run();
      }
topicIdCache.set(chatId, topicId);
    }
async function getPrivateChatId(topicId) {
      for (const [chatId, tid] of topicIdCache.cache) if (tid === topicId) return chatId;
      const mapping = await env.D1.prepare('SELECT chat_id FROM chat_topic_mappings WHERE topic_id = ?')
        .bind(topicId)
        .first();
      return mapping?.chat_id || null;
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
async function fetchWithRetry(url, options, retries = 3, initialBackoff = 1000) {
      for (let i = 0; i < retries; i++) {
        try {
          const controller = new AbortController();
          const timeoutId = setTimeout(() => controller.abort(), 5000);
          const response = await fetch(url, { ...options, signal: controller.signal });
          clearTimeout(timeoutId);
          if (response.ok) {
            return response;
          }
          if (response.status === 429) {
            const retryAfter = response.headers.get('Retry-After') || 5;
            const delay = parseInt(retryAfter) * 1000;
            console.log(`Rate limited. Waiting for ${delay}ms before retry.`);
            await new Promise(resolve => setTimeout(resolve, delay));
            continue;
          }
          throw new Error(`Request failed with status ${response.status}: ${await response.text()}`);
        } catch (error) {
          if (i === retries - 1) throw error;
          const exponentialWait = initialBackoff * Math.pow(2, i);
          const jitter = Math.random() * 0.3 * exponentialWait;
          const waitTime = Math.floor(exponentialWait + jitter);
          console.log(`Request to ${url} failed (attempt ${i+1}/${retries}). Retrying in ${waitTime}ms. Error: ${error.message}`);
          await new Promise(resolve => setTimeout(resolve, waitTime));
        }
      }
      throw new Error(`Failed to fetch ${url} after ${retries} retries`);
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
setInterval(async () => {
        try {
          await performCacheCleanup();
        } catch (error) {
          console.error(`定期缓存清理失败: ${error.message}`);
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
try {
      return await handleRequest(request);
    } catch (error) {
      return new Response('Internal Server Error', { status: 500 });
    }
  }
};
