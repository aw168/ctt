let BOT_TOKEN;
let GROUP_ID;
let MAX_MESSAGES_PER_MINUTE;

// 添加当前版本常量
const CURRENT_VERSION = "v1.3.0";
// 更新链接，移除已过期的token
const VERSION_CHECK_URL = "https://raw.githubusercontent.com/iawooo/tz/main/CFTeleTrans/tag.md";
const UPDATE_INFO_URL = "https://raw.githubusercontent.com/iawooo/tz/main/CFTeleTrans/admin.md";

let lastCleanupTime = 0;
let lastCacheCleanupTime = 0;
const CLEANUP_INTERVAL = 24 * 60 * 60 * 1000; // 24 小时
const CACHE_CLEANUP_INTERVAL = 1 * 60 * 60 * 1000; // 1 小时
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
    this.lastAccess = new Map(); // 记录每个键的最后访问时间
  }
  get(key) {
    const value = this.cache.get(key);
    if (value !== undefined) {
      this.cache.delete(key);
      this.cache.set(key, value);
      this.lastAccess.set(key, Date.now()); // 更新访问时间
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
    this.lastAccess.set(key, Date.now()); // 记录访问时间
  }
  clear() {
    this.cache.clear();
    this.lastAccess.clear();
  }
  
  // 新增: 清理指定时间前未访问的项
  cleanStale(maxAge = 3600000) { // 默认1小时
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

// 添加分布式锁机制
const CREATION_LOCKS = new Map();

// 添加全局序列化队列，强制所有话题创建任务串行执行
const TOPIC_CREATION_QUEUE = [];
let TOPIC_CREATION_RUNNING = false;

// 串行执行话题创建任务的函数
async function runTopicCreationQueue() {
  if (TOPIC_CREATION_RUNNING) return;
  
  TOPIC_CREATION_RUNNING = true;
  try {
    while (TOPIC_CREATION_QUEUE.length > 0) {
      const task = TOPIC_CREATION_QUEUE.shift();
      try {
        const result = await task.fn(...task.args);
        task.resolve(result);
      } catch (error) {
        task.reject(error);
      }
    }
  } finally {
    TOPIC_CREATION_RUNNING = false;
  }
}

// 将话题创建任务添加到队列
function enqueueTopicCreation(fn, ...args) {
  return new Promise((resolve, reject) => {
    TOPIC_CREATION_QUEUE.push({ fn, args, resolve, reject });
    // 启动队列处理（如果尚未运行）
    if (!TOPIC_CREATION_RUNNING) {
      runTopicCreationQueue();
    }
  });
}

// 添加时区转换函数，转换为北京时间
function convertToBeijingTime(date) {
  // 创建一个新的Date对象，避免修改原始对象
  const beijingDate = new Date(date);
  // 将UTC时间调整为北京时间 (UTC+8)
  beijingDate.setHours(beijingDate.getHours() + 8);
  // 格式化为 YYYY-MM-DD HH:MM:SS
  return beijingDate.toISOString().replace('T', ' ').substring(0, 19);
}

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
        // 预加载失败不影响主流程
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
        // 清理创建锁中过期的项（超过5分钟的锁视为异常）
        const now = Date.now();
        const lockTimeout = 5 * 60 * 1000; // 5分钟
        
        for (const [key, timestamp] of CREATION_LOCKS.entries()) {
          if (now - timestamp > lockTimeout) {
            console.log(`清理过期的创建锁: ${key}`);
            CREATION_LOCKS.delete(key);
          }
        }
        
        // 查找所有标记为正在创建的话题映射
        const creatingTopics = await d1.prepare('SELECT chat_id FROM chat_topic_mappings WHERE topic_id = ?')
          .bind('creating')
          .all();
        
        if (creatingTopics.results.length > 0) {
          console.log(`清理 ${creatingTopics.results.length} 个遗留的临时话题标记`);
          
          // 删除所有临时标记
          await d1.prepare('DELETE FROM chat_topic_mappings WHERE topic_id = ?')
            .bind('creating')
            .run();
          
          // 从缓存中清除这些用户的话题ID
          for (const row of creatingTopics.results) {
            topicIdCache.set(row.chat_id, undefined);
          }
        }
        
        // 查找最近24小时内创建但无消息的话题（可能是孤儿话题）
        // 注意：这需要Telegram API的额外支持，这里只是概念示例
        // 实际上Telegram API可能不支持列出所有话题或检查话题消息量
        try {
          console.log("检查孤儿话题...");
          // 此处应添加实际检测和清理逻辑，但需要Telegram API支持
          // 如果Telegram API不支持，可以考虑维护一个自己的话题活跃度记录
        } catch (orphanError) {
          console.error(`检查孤儿话题时出错: ${orphanError.message}`);
        }
      } catch (error) {
        console.error(`清理临时话题标记时出错: ${error.message}`);
        // 继续执行，不中断初始化流程
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
        // 验证码关闭时，所有用户都可以直接发送消息
      } else {
        const nowSeconds = Math.floor(Date.now() / 1000);
        // 修改验证检查逻辑，只检查是否验证过，不再检查过期时间
        const isVerified = userState.is_verified;
        const isFirstVerification = userState.is_first_verification;
        const isRateLimited = await checkMessageRate(chatId);
        const isVerifying = userState.is_verifying || false;

        // 只有未验证或达到频率限制时才需要验证
        if (!isVerified || (isRateLimited && !isFirstVerification)) {
          if (isVerifying) {
            // 检查验证码是否已过期
            const storedCode = await env.D1.prepare('SELECT verification_code, code_expiry FROM user_states WHERE chat_id = ?')
              .bind(chatId)
              .first();
            
            const nowSeconds = Math.floor(Date.now() / 1000);
            const isCodeExpired = !storedCode?.verification_code || !storedCode?.code_expiry || nowSeconds > storedCode.code_expiry;
            
            if (isCodeExpired) {
              // 如果验证码已过期，重新发送验证码
              await sendMessageToUser(chatId, '验证码已过期，正在为您发送新的验证码...');
              await env.D1.prepare('UPDATE user_states SET verification_code = NULL, code_expiry = NULL, is_verifying = FALSE WHERE chat_id = ?')
                .bind(chatId)
                .run();
              userStateCache.set(chatId, { ...userState, verification_code: null, code_expiry: null, is_verifying: false });
              
              // 删除旧的验证消息（如果存在）
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
                    // 删除失败仍继续处理
                  }
                  
                  await env.D1.prepare('UPDATE user_states SET last_verification_message_id = NULL WHERE chat_id = ?')
                    .bind(chatId)
                    .run();
                }
              } catch (error) {
                console.log(`查询旧验证消息失败: ${error.message}`);
                // 即使出错也继续处理
              }
              
              // 立即发送新的验证码
              try {
                await handleVerification(chatId, 0);
              } catch (verificationError) {
                console.error(`发送新验证码失败: ${verificationError.message}`);
                // 如果发送验证码失败，则再次尝试
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

          // 先检查是否已有话题
          const existingTopicId = await getExistingTopicId(chatId);
          if (existingTopicId) {
            console.log(`用户 ${chatId} 已有话题ID: ${existingTopicId}，验证是否有效`);
            
            // 验证话题是否有效
            const isTopicValid = await validateTopic(existingTopicId);
            if (isTopicValid) {
              const successMessage = await getVerificationSuccessMessage();
              await sendMessageToUser(chatId, `${successMessage}\n➡️您已经在聊天中，无需重复发送 /start 命令。`);
              return;
            } else {
              console.log(`用户 ${chatId} 的话题 ${existingTopicId} 已失效，将重新创建`);
              // 删除数据库中的旧映射
              await env.D1.prepare('DELETE FROM chat_topic_mappings WHERE chat_id = ?')
                .bind(chatId)
                .run();
              topicIdCache.set(chatId, undefined);
              // 继续创建新话题
            }
          }

          // 获取用户信息
          const userInfo = await getUserInfo(chatId);
          if (!userInfo) {
            await sendMessageToUser(chatId, "无法获取用户信息，请稍后再试。");
            return;
          }

          // 发送欢迎消息
          const successMessage = await getVerificationSuccessMessage();
          await sendMessageToUser(chatId, `${successMessage}\n你好，欢迎使用私聊机器人，现在发送信息吧！`);
          
          // 创建话题，添加重试机制
          let topicId = null;
          let retries = 3;
          let error = null;
          
          console.log(`开始为用户 ${chatId} 创建话题，最多尝试 ${retries} 次`);
          
          while (retries > 0 && !topicId) {
            try {
              // 检查是否在此期间已创建话题
              const checkTopicId = await getExistingTopicId(chatId);
              if (checkTopicId) {
                console.log(`重试前检查到用户 ${chatId} 已有话题ID: ${checkTopicId}`);
                
                // 验证话题是否有效
                const isCheckValid = await validateTopic(checkTopicId);
                if (isCheckValid) {
                  topicId = checkTopicId;
                  break;
                } else {
                  console.log(`检测到的话题 ${checkTopicId} 无效，将继续创建`);
                  // 删除无效映射
                  await env.D1.prepare('DELETE FROM chat_topic_mappings WHERE chat_id = ?')
                    .bind(chatId)
                    .run();
                  topicIdCache.set(chatId, undefined);
                }
              }
              
              topicId = await ensureUserTopic(chatId, userInfo);
              if (topicId) {
                console.log(`为用户 ${chatId} 成功创建话题: ${topicId}`);
                break;
              }
            } catch (err) {
              error = err;
              console.error(`创建话题失败，剩余重试次数: ${retries-1}, 错误: ${err.message}`);
              retries--;
              // 短暂延迟后重试
              if (retries > 0) {
                console.log(`等待1秒后重试为用户 ${chatId} 创建话题`);
                await new Promise(resolve => setTimeout(resolve, 1000));
              }
            }
          }
          
          if (!topicId) {
            console.error(`为用户 ${chatId} 创建话题失败，已达到最大重试次数`);
            await sendMessageToUser(chatId, "创建聊天话题失败，请稍后再试或联系管理员。");
            throw error || new Error("创建话题失败，未知原因");
          }
        
          // 验证创建的话题
          console.log(`验证为用户 ${chatId} 创建的话题 ${topicId}`);
          const isTopicValid = await validateTopic(topicId);
          if (!isTopicValid) {
            console.error(`为用户 ${chatId} 创建的话题 ${topicId} 验证失败`);
            await sendMessageToUser(chatId, "创建的聊天话题暂时无法验证，请发送任意消息再次尝试。");
            
            // 不自动删除话题ID映射，让用户下次消息能重试
            // 但尝试删除无效的话题防止垃圾数据堆积
            await deleteForumTopic(topicId);
          } else {
            console.log(`为用户 ${chatId} 创建的话题 ${topicId} 验证成功`);
          }
        } catch (error) {
          console.error(`处理 /start 命令时出错: ${error.message}`);
          // 不向用户发送错误信息，因为已经在上面处理过了
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

      // 确保话题有效
      console.log(`为用户 ${chatId} 验证话题 ${topicId} 是否有效`);
      const isTopicValid = await validateTopic(topicId);
      if (!isTopicValid) {
        console.log(`话题 ${topicId} 无效，尝试删除并重新创建`);
        
        // 先尝试删除无效的话题
        await deleteForumTopic(topicId);
        
        // 无论删除成功与否，都清理数据库映射
        await env.D1.prepare('DELETE FROM chat_topic_mappings WHERE chat_id = ?').bind(chatId).run();
        topicIdCache.set(chatId, undefined);
        
        // 等待一小段时间，确保Telegram服务器处理完成
        await new Promise(resolve => setTimeout(resolve, 500));
        
        // 尝试重新创建话题
        console.log(`为用户 ${chatId} 重新创建话题`);
        topicId = await ensureUserTopic(chatId, userInfo);
        if (!topicId) {
          await sendMessageToUser(chatId, "无法重新创建话题，请稍后再试或联系管理员。");
          return;
        }
        
        // 再次验证新创建的话题
        const isNewTopicValid = await validateTopic(topicId);
        if (!isNewTopicValid) {
          console.error(`用户 ${chatId} 的新话题 ${topicId} 仍然无法验证，但仍会尝试发送消息`);
          // 不再删除话题映射，允许下次消息时重试
          // 不再立即返回，而是尝试发送消息
        }
      }

      const userName = userInfo.username || `User_${chatId}`;
      const nickname = userInfo.nickname || userName;

      // 尝试发送消息，即使话题验证失败也尝试
      try {
        if (text) {
          const formattedMessage = `${nickname}:\n${text}`;
          await sendMessageToTopic(topicId, formattedMessage);
        } else {
          await copyMessageToTopic(topicId, message);
        }
      } catch (sendError) {
        console.error(`发送消息到话题 ${topicId} 失败: ${sendError.message}`);
        
        // 如果发送失败，检查是否是话题不存在的问题
        if (sendError.message && (
            sendError.message.includes("Bad Request") ||
            sendError.message.includes("chat not found") ||
            sendError.message.includes("message thread not found") ||
            sendError.message.includes("Thread")
          )) {
          console.log(`话题 ${topicId} 已失效，启动强制重建流程`);
          
          // 强制删除现有映射 - 关键修复：确保所有数据库记录都被清除
          try {
            await env.D1.prepare('DELETE FROM chat_topic_mappings WHERE chat_id = ?')
              .bind(chatId)
              .run();
            console.log(`删除数据库映射成功`);
          } catch (dbError) {
            console.error(`删除数据库映射失败: ${dbError.message}`);
          }
          
          // 清空缓存并重新标记为重建状态
          topicIdCache.set(chatId, 'rebuilding');
          
          // 直接重建话题 - 绕过队列直接调用API
          try {
            console.log(`开始为用户 ${chatId} 强制重建话题`);
            
            // 减少等待时间
            await new Promise(resolve => setTimeout(resolve, 100));
            
            // 直接调用Telegram API创建话题
            const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/createForumTopic`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                chat_id: GROUP_ID,
                name: nickname,
                icon_color: 9367192
              })
            }, 2, 300); // 减少重试次数和超时时间
            
            // 快速处理响应
            if (!response.ok) {
              throw new Error(`直接创建话题API调用失败: ${await response.text()}`);
            }
            
            const data = await response.json();
            if (!data.ok) {
              throw new Error(`API返回非成功状态: ${data.description || '未知错误'}`);
            }
            
            const newTopicId = data.result.message_thread_id;
            console.log(`为用户 ${chatId} 强制重建话题成功，新ID: ${newTopicId}`);
            
            // 立即更新缓存
            topicIdCache.set(chatId, newTopicId);
            
            // 并行执行数据库操作 - 不等待完成
            (async function() {
              try {
                await env.D1.prepare('INSERT INTO chat_topic_mappings (chat_id, topic_id) VALUES (?, ?)')
                  .bind(chatId, newTopicId)
                  .run();
              } catch (dbError) {
                console.error(`异步保存话题ID到数据库失败: ${dbError.message}`);
                setTimeout(async () => {
                  try {
                    await env.D1.prepare('INSERT INTO chat_topic_mappings (chat_id, topic_id) VALUES (?, ?)')
                      .bind(chatId, newTopicId)
                      .run();
                  } catch (retryError) {}
                }, 1000);
              }
            })();
            
            // 先发送欢迎消息并置顶，确保它是第一条消息
            try {
              const now = new Date();
              const formattedTime = convertToBeijingTime(now); // 使用北京时间
              const notificationContent = await getNotificationContent();
              const pinnedMessage = `昵称: ${nickname}\n用户名: @${userName}\nUserID: ${chatId}\n重建时间: ${formattedTime}\n\n${notificationContent}`;
              
              // 发送欢迎消息
              const welcomeResponse = await sendMessageToTopic(newTopicId, pinnedMessage);
              
              // 并行操作 - 立即发送用户原始消息，不等待置顶完成
              (async function() {
                try {
                  if (text) {
                    const formattedMessage = `${nickname}:\n${text}`;
                    await sendMessageToTopic(newTopicId, formattedMessage);
                  } else if (message) {
                    await copyMessageToTopic(newTopicId, message);
                  }
                } catch (resendError) {
                  console.error(`消息重发到新话题失败: ${resendError.message}`);
                }
              })();
              
              // 处理置顶，这部分可以在后台完成，不阻塞主流程
              if (welcomeResponse && welcomeResponse.ok && welcomeResponse.result) {
                const welcomeMessageId = welcomeResponse.result.message_id;
                setTimeout(async () => {
                  try {
                    await pinMessage(newTopicId, welcomeMessageId);
                  } catch (pinError) {}
                }, 100);
              }
            } catch (welcomeError) {
              console.error(`发送欢迎消息失败: ${welcomeError.message}`);
              // 如果欢迎消息失败，仍尝试发送用户消息
              try {
                if (text) {
                  const formattedMessage = `${nickname}:\n${text}`;
                  await sendMessageToTopic(newTopicId, formattedMessage);
                } else if (message) {
                  await copyMessageToTopic(newTopicId, message);
                }
              } catch (resendError) {}
            }
            
            return; // 早期返回，不执行后续的备用逻辑
          } catch (directError) {
            console.error(`直接创建话题失败: ${directError.message}`);
            topicIdCache.set(chatId, undefined);
            
            // 精简备用流程
            try {
              // 直接执行最小化的创建流程
              const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/createForumTopic`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                  chat_id: GROUP_ID,
                  name: nickname,
                  icon_color: 9367192
                })
              }, 1, 300);
              
              if (response.ok) {
                const data = await response.json();
                if (data.ok) {
                  const finalTopicId = data.result.message_thread_id;
                  topicIdCache.set(chatId, finalTopicId);
                  
                  // 并行发送欢迎消息和更新数据库
                  Promise.all([
                    (async () => {
                      const now = new Date();
                      const formattedTime = convertToBeijingTime(now); // 使用北京时间
                      const notificationContent = await getNotificationContent();
                      const pinnedMessage = `昵称: ${nickname}\n用户名: @${userName}\nUserID: ${chatId}\n重建时间: ${formattedTime}\n\n${notificationContent}`;
                      
                      const welcomeResult = await sendMessageToTopic(finalTopicId, pinnedMessage);
                      if (welcomeResult && welcomeResult.ok) {
                        setTimeout(() => pinMessage(finalTopicId, welcomeResult.result.message_id), 100);
                      }
                    })(),
                    env.D1.prepare('INSERT INTO chat_topic_mappings (chat_id, topic_id) VALUES (?, ?)')
                      .bind(chatId, finalTopicId)
                      .run()
                      .catch(() => {})
                  ]).catch(() => {});
                  
                  // 立即发送用户消息
                  if (text) {
                    const formattedMessage = `${nickname}:\n${text}`;
                    sendMessageToTopic(finalTopicId, formattedMessage).catch(() => {});
                  }
                }
              }
            } catch (finalError) {}
          }
        } else {
          // 其他类型的发送错误
          console.error(`其他类型的消息发送错误: ${sendError.message}`);
        }
      }
    }

    async function validateTopic(topicId) {
      try {
        console.log(`验证话题ID: ${topicId}...`);
        
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
          const deleteResponse = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              chat_id: GROUP_ID,
              message_id: data.result.message_id
            })
          });
          
          const deleteData = await deleteResponse.json();
          if (!deleteData.ok) {
            console.warn(`验证话题ID: ${topicId} - 无法删除测试消息: ${deleteData.description || '未知错误'}`);
          }
          
          console.log(`话题ID: ${topicId} 有效`);
          return true;
        }
        
        console.error(`验证话题ID: ${topicId} 失败 - ${data.description || '未知错误'}`);
        return false;
      } catch (error) {
        console.error(`验证话题ID: ${topicId} 时发生异常: ${error.message}`);
        return false;
      }
    }

    async function ensureUserTopic(chatId, userInfo) {
      try {
        // 先检查现有话题
        const existingTopicId = await getExistingTopicId(chatId);
        
        if (existingTopicId) {
          console.log(`[确保话题] 用户 ${chatId} 已有话题ID: ${existingTopicId}`);
          
          // 验证现有话题是否有效
          const isValid = await validateTopic(existingTopicId);
          if (isValid) {
            return existingTopicId;
          } else {
            // 如果话题无效（可能已在群组中被删除），则在数据库中标记为已删除
            console.log(`[确保话题] 用户 ${chatId} 的话题 ${existingTopicId} 已在群组中被删除，将重新创建`);
            
            // 删除数据库中的旧映射
            await env.D1.prepare('DELETE FROM chat_topic_mappings WHERE chat_id = ?')
              .bind(chatId)
              .run();
            
            // 清除缓存
            topicIdCache.set(chatId, undefined);
            
            // 继续下面的逻辑创建新话题
          }
        }
        
        // 如果缓存中有"creating"状态，等待一段时间看是否能找到已创建的话题
        if (topicIdCache.get(chatId) === 'creating') {
          console.log(`[确保话题] 检测到用户 ${chatId} 的话题正在创建中，等待...`);
          
          // 等待一段时间后重新检查
          for (let i = 0; i < 5; i++) {
            await new Promise(resolve => setTimeout(resolve, 500));
            
            const checkResult = await getExistingTopicId(chatId);
            if (checkResult) {
              console.log(`[确保话题] 等待后发现用户 ${chatId} 已有话题ID: ${checkResult}`);
              
              // 验证是否有效
              const isStillValid = await validateTopic(checkResult);
              if (isStillValid) {
                return checkResult;
              } else {
                // 如果无效，清除后创建新话题
                console.log(`[确保话题] 等待期间发现的话题 ${checkResult} 无效，将重新创建`);
                await env.D1.prepare('DELETE FROM chat_topic_mappings WHERE chat_id = ?')
                  .bind(chatId)
                  .run();
                topicIdCache.set(chatId, undefined);
                break;
              }
            }
          }
          
          console.log(`[确保话题] 用户 ${chatId} 的话题创建等待超时或无效，将尝试重新创建`);
          topicIdCache.set(chatId, undefined); // 清除可能卡住的状态
        }
        
        // 标记为正在创建中
        topicIdCache.set(chatId, 'creating');
        
        // 准备用户信息
        const userName = userInfo.username || `User_${chatId}`;
        const nickname = userInfo.nickname || userName;
        
        try {
          // 创建话题
          const topicId = await createForumTopic(nickname, userName, nickname, userInfo.id || chatId);
          console.log(`[确保话题] 为用户 ${chatId} 创建话题成功: ${topicId}`);
          
          // 创建成功后再次验证
          const isNewTopicValid = await validateTopic(topicId);
          if (!isNewTopicValid) {
            console.error(`[确保话题] 新创建的话题 ${topicId} 验证失败，这可能是暂时性问题`);
            // 尽管验证失败，我们仍然返回话题ID，让用户可以尝试使用
            // 如果真的无效，下次消息会再次触发创建
          }
          
          return topicId;
        } catch (error) {
          console.error(`[确保话题] 创建话题失败: ${error.message}`);
          topicIdCache.set(chatId, undefined); // 清除创建中状态
          throw error;
        }
      } catch (error) {
        console.error(`[确保话题] 主流程异常: ${error.message}`);
        // 确保在异常情况下清除缓存
        topicIdCache.set(chatId, undefined);
        throw error;
      }
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
        
        // 增加try-catch，确保版本检测失败不影响面板显示
        let hasUpdate = false;
        try {
          if (forceCheckUpdate) {
            console.log("强制检查更新...");
            // 清除缓存，确保获取最新版本
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
        
        // 如果有新版本，添加更新信息按钮
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
        // 尝试发送简化版面板
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
      const window = 5 * 60 * 1000; // 5分钟窗口
      const maxStartsPerWindow = 1; // 每个窗口最多允许1次 /start 命令

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

      // 如果窗口已过期，重置计数
      if (now - data.start_window_start > window) {
        data.start_count = 1; // 设置为1因为当前请求也算一次
        data.start_window_start = now;
        await env.D1.prepare('UPDATE message_rates SET start_count = ?, start_window_start = ? WHERE chat_id = ?')
          .bind(data.start_count, data.start_window_start, chatId)
          .run();
      } else {
        // 窗口内增加计数
        data.start_count += 1;
        await env.D1.prepare('UPDATE message_rates SET start_count = ? WHERE chat_id = ?')
          .bind(data.start_count, chatId)
          .run();
      }

      // 更新缓存
      messageRateCache.set(chatId, data);
      
      // 返回是否超出限制
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
          // 关闭验证码时将所有未拉黑用户标记为已验证，不设置过期时间
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
          
          // 删除旧的验证消息
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
            // 即使删除失败也继续处理
          }
          
          // 立即发送新的验证码
          try {
            await handleVerification(chatId, 0);
          } catch (verificationError) {
            console.error(`发送新验证码失败: ${verificationError.message}`);
            // 如果发送验证码失败，则再次尝试
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
          // 移除过期时间设置，让验证永久有效
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
            // 继续处理，即使删除失败
          }
          
          userState.last_verification_message_id = null;
          userStateCache.set(chatId, userState);
          await env.D1.prepare('UPDATE user_states SET last_verification_message_id = NULL WHERE chat_id = ?')
            .bind(chatId)
            .run();
        }

        // 确保发送验证码
        await sendVerification(chatId);
      } catch (error) {
        console.error(`处理验证过程失败: ${error.message}`);
        // 重置用户状态以防卡住
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
        throw error; // 向上传递错误以便调用方处理
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
        throw error; // 向上传递错误以便调用方处理
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
      try {
        // 检查是否处于重建模式
        const cachedId = topicIdCache.get(chatId);
        if (cachedId === 'rebuilding') {
          console.log(`[获取话题] 用户 ${chatId} 正在重建话题中，返回null允许创建`);
          return null;
        }
        
        if (cachedId && cachedId !== 'creating') {
          // 二次验证缓存中的话题ID是否真实存在于数据库中
          const verifyCache = await env.D1.prepare(`
            SELECT topic_id FROM chat_topic_mappings 
            WHERE chat_id = ? AND topic_id = ?
            LIMIT 1
          `).bind(chatId, cachedId).first();
          
          if (verifyCache) {
            return cachedId;
          } else {
            console.log(`[获取话题] 缓存中的话题ID ${cachedId} 在数据库中未找到，清除缓存`);
            topicIdCache.set(chatId, undefined);
          }
        }
        
        // 查询数据库中的有效映射
        const result = await env.D1.prepare(`
          SELECT topic_id FROM chat_topic_mappings 
          WHERE chat_id = ? AND topic_id != 'creating' AND topic_id != 'rebuilding'
          LIMIT 1
        `).bind(chatId).first();
        
        if (result && result.topic_id) {
          topicIdCache.set(chatId, result.topic_id); // 更新缓存
          return result.topic_id;
        }
        
        return null; // 没有找到有效话题ID
      } catch (error) {
        console.error(`[获取话题] 查询话题ID失败: ${error.message}`);
        return null;
      }
    }

    // 修改创建话题的核心逻辑，解决重建问题
    async function _createForumTopicInternal(topicName, userName, nickname, userId) {
      try {
        // 确认当前处于重建模式时跳过检查已有话题的步骤
        const isRebuilding = topicIdCache.get(userId) === 'rebuilding';
        
        if (!isRebuilding) {
          // 正常模式：首先检查数据库是否已存在有效映射
          const existingTopic = await env.D1.prepare(`
            SELECT topic_id FROM chat_topic_mappings 
            WHERE chat_id = ? AND topic_id != 'creating' AND topic_id != 'rebuilding'
            LIMIT 1
          `).bind(userId).first();
          
          if (existingTopic && existingTopic.topic_id) {
            console.log(`[串行化] 用户 ${userId} 已有话题ID: ${existingTopic.topic_id}`);
            topicIdCache.set(userId, existingTopic.topic_id);
            return existingTopic.topic_id;
          }
        } else {
          console.log(`[串行化] 用户 ${userId} 处于重建模式，跳过检查现有话题`);
        }
        
        // 清理任何可能的临时状态
        await env.D1.prepare(`
          DELETE FROM chat_topic_mappings 
          WHERE chat_id = ? AND (topic_id = 'creating' OR topic_id = 'rebuilding')
        `).bind(userId).run();
        
        // 创建临时标记
        try {
          await env.D1.prepare(`
            INSERT INTO chat_topic_mappings (chat_id, topic_id) 
            VALUES (?, 'creating')
          `).bind(userId).run();
        } catch (insertError) {
          console.log(`[串行化] 用户 ${userId} 临时标记创建失败: ${insertError.message}`);
          
          // 等待一小段时间，然后检查是否已创建完成
          await new Promise(resolve => setTimeout(resolve, 1000));
          
          const checkAfterInsertFail = await env.D1.prepare(`
            SELECT topic_id FROM chat_topic_mappings 
            WHERE chat_id = ? AND topic_id != 'creating' AND topic_id != 'rebuilding'
            LIMIT 1
          `).bind(userId).first();
          
          if (checkAfterInsertFail && checkAfterInsertFail.topic_id) {
            return checkAfterInsertFail.topic_id;
          }
          
          throw new Error('临时标记创建失败，需要重试');
        }
        
        console.log(`[串行化] 开始${isRebuilding ? '重建' : '创建'}用户 ${userId} 的话题: ${topicName}`);
        
        // 创建论坛话题 (API调用)
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/createForumTopic`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: GROUP_ID,
            name: topicName,
            icon_color: 9367192
          })
        }, 3, 1000);
        
        // 处理API响应
        if (!response.ok) {
          const errorData = await response.json();
          const errorMsg = errorData.description || '未知错误';
          console.error(`[串行化] 创建话题API调用失败: ${errorMsg}`);
          
          // 删除临时标记
          await env.D1.prepare(`
            DELETE FROM chat_topic_mappings 
            WHERE chat_id = ? AND topic_id = 'creating'
          `).bind(userId).run();
          
          throw new Error(`话题${isRebuilding ? '重建' : '创建'}失败: ${errorMsg}`);
        }
        
        const data = await response.json();
        const topicId = data.result.message_thread_id;
        console.log(`[串行化] 话题${isRebuilding ? '重建' : '创建'}成功，ID: ${topicId}`);
        
        // 更新数据库
        await env.D1.prepare(`
          DELETE FROM chat_topic_mappings WHERE chat_id = ? AND (topic_id = 'creating' OR topic_id = 'rebuilding')
        `).bind(userId).run();
        
        await env.D1.prepare(`
          INSERT INTO chat_topic_mappings (chat_id, topic_id) VALUES (?, ?)
        `).bind(userId, topicId).run();
        
        console.log(`[串行化] 话题ID已保存到数据库 - 用户: ${userId}, 话题ID: ${topicId}`);
        
        // 更新缓存
        topicIdCache.set(userId, topicId);
        
        // 发送欢迎消息
        try {
          const now = new Date();
          const formattedTime = convertToBeijingTime(now); // 使用北京时间
          const notificationContent = await getNotificationContent();
          const pinnedMessage = `昵称: ${nickname}\n用户名: @${userName}\nUserID: ${userId}\n${isRebuilding ? '重建' : '发起'}时间: ${formattedTime}\n\n${notificationContent}`;
          
          const messageResponse = await sendMessageToTopic(topicId, pinnedMessage);
          if (messageResponse && messageResponse.ok) {
            const messageId = messageResponse.result.message_id;
            try {
              await pinMessage(topicId, messageId);
            } catch (pinError) {
              console.error(`[串行化] 消息置顶失败: ${pinError.message}`);
            }
          }
        } catch (messageError) {
          console.error(`[串行化] 发送欢迎消息失败: ${messageError.message}`);
          // 不影响主流程
        }
        
        return topicId;
      } catch (error) {
        console.error(`[串行化] ${topicIdCache.get(userId) === 'rebuilding' ? '重建' : '创建'}话题内部错误: ${error.message}`);
        
        // 清理临时标记
        try {
          await env.D1.prepare(`
            DELETE FROM chat_topic_mappings 
            WHERE chat_id = ? AND (topic_id = 'creating' OR topic_id = 'rebuilding')
          `).bind(userId).run();
        } catch (cleanupError) {
          console.error(`[串行化] 清理临时标记失败: ${cleanupError.message}`);
        }
        
        throw error;
      }
    }

    // 修改 createForumTopic 函数，使用队列机制实现真正的串行处理
    async function createForumTopic(topicName, userName, nickname, userId) {
      try {
        // 检查是否处于重建模式
        const isRebuilding = topicIdCache.get(userId) === 'rebuilding';
        
        // 只有在非重建模式下才检查现有话题
        if (!isRebuilding) {
          // 先检查缓存
          const cachedTopicId = topicIdCache.get(userId);
          if (cachedTopicId && cachedTopicId !== 'creating' && cachedTopicId !== 'rebuilding') {
            console.log(`[队列] 缓存命中，用户 ${userId} 已有话题ID: ${cachedTopicId}`);
            return cachedTopicId;
          }
          
          // 快速检查数据库
          const quickCheck = await env.D1.prepare(`
            SELECT topic_id FROM chat_topic_mappings 
            WHERE chat_id = ? AND topic_id != 'creating' AND topic_id != 'rebuilding'
            LIMIT 1
          `).bind(userId).first();
          
          if (quickCheck && quickCheck.topic_id) {
            console.log(`[队列] 数据库查询，用户 ${userId} 已有话题ID: ${quickCheck.topic_id}`);
            topicIdCache.set(userId, quickCheck.topic_id);
            return quickCheck.topic_id;
          }
        } else {
          console.log(`[队列] 用户 ${userId} 处于重建模式，跳过检查现有话题`);
        }
        
        // 确保重建模式下直接进入创建流程
        console.log(`[队列] 将用户 ${userId} 的话题${isRebuilding ? '重建' : '创建'}任务加入队列`);
        
        // 创建前先清理旧记录 - 重要解决方案：确保在重建时彻底清除旧记录
        if (isRebuilding) {
          try {
            await env.D1.prepare('DELETE FROM chat_topic_mappings WHERE chat_id = ?')
              .bind(userId)
              .run();
            console.log(`[队列] 成功清理用户 ${userId} 的所有话题映射记录`);
          } catch (cleanError) {
            console.error(`[队列] 清理用户 ${userId} 旧记录失败: ${cleanError.message}`);
          }
        }
        
        // 将任务添加到队列，确保串行执行
        return await enqueueTopicCreation(_createForumTopicInternal, topicName, userName, nickname, userId);
      } catch (error) {
        console.error(`[队列] 话题${isRebuilding ? '重建' : '创建'}任务失败: ${error.message}`);
        
        // 清除缓存
        topicIdCache.set(userId, undefined);
        
        throw error;
      }
    }

    async function saveTopicId(chatId, topicId) {
      try {
        console.log(`保存话题ID - 用户: ${chatId}, 话题ID: ${topicId}`);
        
        // 先检查是否已存在映射
        const existingMapping = await env.D1.prepare('SELECT topic_id FROM chat_topic_mappings WHERE chat_id = ?')
          .bind(chatId)
          .first();
        
        if (existingMapping) {
          // 如果存在且不是临时标记，则不更新
          if (existingMapping.topic_id !== 'creating' && existingMapping.topic_id === topicId) {
            console.log(`话题映射已存在且匹配，无需更新 - 用户: ${chatId}, 话题ID: ${topicId}`);
            topicIdCache.set(chatId, topicId);
            return;
          }
          
          // 如果是临时标记或不同的话题ID，则更新
          console.log(`更新话题映射 - 用户: ${chatId}, 旧话题ID: ${existingMapping.topic_id}, 新话题ID: ${topicId}`);
          
          const updateResult = await env.D1.prepare('UPDATE chat_topic_mappings SET topic_id = ? WHERE chat_id = ?')
            .bind(topicId, chatId)
            .run();
          
          if (updateResult.error) {
            console.error(`更新话题映射失败 - 用户: ${chatId}, 错误: ${updateResult.error}`);
          }
        } else {
          // 如果不存在，则插入
          console.log(`创建新话题映射 - 用户: ${chatId}, 话题ID: ${topicId}`);
          
          const insertResult = await env.D1.prepare('INSERT INTO chat_topic_mappings (chat_id, topic_id) VALUES (?, ?)')
            .bind(chatId, topicId)
            .run();
          
          if (insertResult.error) {
            console.error(`插入话题映射失败 - 用户: ${chatId}, 错误: ${insertResult.error}`);
          }
        }
        
        // 更新缓存
        topicIdCache.set(chatId, topicId);
        console.log(`话题映射已保存并缓存 - 用户: ${chatId}, 话题ID: ${topicId}`);
      } catch (error) {
        console.error(`保存话题ID时出错 - 用户: ${chatId}, 错误: ${error.message}`);
        // 出错时也要尝试更新缓存，确保后续操作能正常进行
        topicIdCache.set(chatId, topicId);
      }
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

    // 添加删除话题函数
    async function deleteForumTopic(topicId) {
      try {
        console.log(`尝试删除话题: ${topicId}`);
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteForumTopic`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: GROUP_ID,
            message_thread_id: topicId
          })
        });
        
        const data = await response.json();
        if (data.ok) {
          console.log(`成功删除话题: ${topicId}`);
          return true;
        } else {
          console.error(`删除话题失败: ${topicId}, 错误: ${data.description || '未知错误'}`);
          return false;
        }
      } catch (error) {
        console.error(`删除话题时发生异常: ${error.message}`);
        return false;
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
          
          // 特殊处理 429 Too Many Requests
          if (response.status === 429) {
            const retryAfter = response.headers.get('Retry-After') || 5;
            const delay = parseInt(retryAfter) * 1000;
            console.log(`Rate limited. Waiting for ${delay}ms before retry.`);
            await new Promise(resolve => setTimeout(resolve, delay));
            continue;
          }
          
          // 其他错误状态码
          throw new Error(`Request failed with status ${response.status}: ${await response.text()}`);
        } catch (error) {
          if (i === retries - 1) throw error;
          
          // 计算指数退避时间，并添加随机抖动
          const exponentialWait = initialBackoff * Math.pow(2, i);
          const jitter = Math.random() * 0.3 * exponentialWait; // 添加30%以内的随机抖动
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

    // 新增：设置定期清理任务
    async function setupPeriodicCleanup(d1) {
      // 立即执行一次清理
      await performCacheCleanup();
      
      // 设置定期执行
      setInterval(async () => {
        try {
          await performCacheCleanup();
        } catch (error) {
          console.error(`定期缓存清理失败: ${error.message}`);
        }
      }, CACHE_CLEANUP_INTERVAL);
    }

    // 执行缓存清理
    async function performCacheCleanup() {
      const now = Date.now();
      if (now - lastCacheCleanupTime < CACHE_CLEANUP_INTERVAL) {
        return;
      }
      
      console.log('执行缓存清理...');
      
      // 清理超过3小时未访问的缓存项
      userInfoCache.cleanStale(3 * 60 * 60 * 1000);
      topicIdCache.cleanStale(3 * 60 * 60 * 1000);
      userStateCache.cleanStale(3 * 60 * 60 * 1000);
      messageRateCache.cleanStale(3 * 60 * 60 * 1000);
      
      // 更新最后清理时间
      lastCacheCleanupTime = now;
      console.log('缓存清理完成');
    }

    // 批量更新用户状态
    async function batchUpdateUserStates(d1, operations, batchSize = 50) {
      const batches = [];
      for (let i = 0; i < operations.length; i += batchSize) {
        batches.push(operations.slice(i, i + batchSize));
      }
      
      for (const batch of batches) {
        await d1.batch(batch);
      }
    }

    // 获取远程版本信息
    async function getRemoteVersion() {
      try {
        // 添加超时控制
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 3000); // 3秒超时
        
        // 添加随机参数破坏缓存
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
          return CURRENT_VERSION; // 如果获取失败，返回当前版本，防止误报更新
        }
        
        const versionText = await response.text();
        return versionText.trim(); // 去除可能的空白字符
      } catch (error) {
        console.error(`获取远程版本异常: ${error.message}`);
        return CURRENT_VERSION; // 如果出现异常，返回当前版本
      }
    }

    // 获取更新信息
    async function getUpdateInfo() {
      try {
        // 添加超时控制
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 3000); // 3秒超时
        
        // 添加随机参数破坏缓存
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

    // 检查是否有新版本
    async function hasNewVersion() {
      try {
        // 每次都重新获取，不使用缓存
        const remoteVersion = await getRemoteVersion();
        
        // 规范化版本字符串 - 去除所有空白字符和转为小写
        const normalizedRemote = remoteVersion.toLowerCase().replace(/\s+/g, '');
        const normalizedCurrent = CURRENT_VERSION.toLowerCase().replace(/\s+/g, '');
        
        console.log(`版本比较详情:`);
        console.log(`- 当前版本(原始): "${CURRENT_VERSION}"`);
        console.log(`- 远程版本(原始): "${remoteVersion}"`);
        console.log(`- 当前版本(规范化): "${normalizedCurrent}"`);
        console.log(`- 远程版本(规范化): "${normalizedRemote}"`);
        console.log(`- 是否需要更新: ${normalizedRemote !== normalizedCurrent}`);
        
        // 如果版本不同，则需要更新
        return normalizedRemote !== normalizedCurrent;
      } catch (error) {
        console.error(`版本比较失败: ${error.message}`);
        return false; // 如果发生错误，返回false表示没有新版本
      }
    }

    // 在文件适当位置添加删除话题函数
    try {
      return await handleRequest(request);
    } catch (error) {
      return new Response('Internal Server Error', { status: 500 });
    }
  }
};
