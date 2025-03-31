// @ts-check - Enable type checking for this file
let BOT_TOKEN;
let GROUP_ID;
let MAX_MESSAGES_PER_MINUTE;

let lastCleanupTime = 0;
const CLEANUP_INTERVAL = 24 * 60 * 60 * 1000; // 24 小时
let isInitialized = false;
const processedMessages = new Set();
const processedCallbacks = new Set(); // 新增：记录已处理的回调
const settingsCache = new Map([
  ['verification_enabled', null],
  ['user_raw_enabled', null]
]);

/**
 * @typedef {import('@cloudflare/workers-types').D1Database} D1Database
 * @typedef {import('@cloudflare/workers-types').Fetcher} Fetcher
 * @typedef {import('@cloudflare/workers-types').Request} CfRequest
 * @typedef {import('@cloudflare/workers-types').ExecutionContext} ExecutionContext
 * @typedef {{
 *   BOT_TOKEN_ENV?: string;
 *   GROUP_ID_ENV?: string;
 *   MAX_MESSAGES_PER_MINUTE_ENV?: string;
 *   D1?: D1Database;
 * }} Env
 */

class LRUCache {
  /** @param {number} maxSize */
  constructor(maxSize) {
    this.maxSize = maxSize;
    this.cache = new Map();
  }
  /** @param {any} key */
  get(key) {
    const value = this.cache.get(key);
    if (value !== undefined) {
      this.cache.delete(key);
      this.cache.set(key, value);
    }
    return value;
  }
  /**
   * @param {any} key
   * @param {any} value
   */
  set(key, value) {
    if (this.cache.size >= this.maxSize) {
      const firstKey = this.cache.keys().next().value;
      this.cache.delete(firstKey);
    }
    this.cache.set(key, value);
  }
}

const userInfoCache = new LRUCache(1000); // 最大 1000 条
const topicIdCache = new LRUCache(1000);
const userStateCache = new LRUCache(1000);
const messageRateCache = new LRUCache(1000);

export default {
  /**
   * @param {CfRequest} request
   * @param {Env} env
   * @param {ExecutionContext} ctx
   * @returns {Promise<Response>}
   */
  async fetch(request, env, ctx) {
    BOT_TOKEN = env.BOT_TOKEN_ENV || null;
    GROUP_ID = env.GROUP_ID_ENV || null;
    MAX_MESSAGES_PER_MINUTE = env.MAX_MESSAGES_PER_MINUTE_ENV ? parseInt(env.MAX_MESSAGES_PER_MINUTE_ENV) : 40;

    if (!env.D1) {
      console.error('D1 database is not bound');
      return new Response('Server configuration error: D1 database is not bound', { status: 500 });
    }

    if (!isInitialized) {
      // 使用 ctx.waitUntil 确保初始化在后台运行，不阻塞首次请求的响应
      ctx.waitUntil(initialize(env.D1, request));
      isInitialized = true; // 立即标记为已初始化（开始初始化）
    }

    // 定期在后台运行清理任务
    ctx.waitUntil(cleanExpiredVerificationCodes(env.D1));


    /**
     * @param {CfRequest} req
     * @returns {Promise<Response>}
     */
    async function handleRequest(req) {
      if (!BOT_TOKEN || !GROUP_ID) {
        console.error('Missing required environment variables');
        return new Response('Server configuration error: Missing required environment variables', { status: 500 });
      }

      const url = new URL(req.url);
      if (url.pathname === '/webhook') {
        if (req.method !== 'POST') {
          return new Response('Method Not Allowed', { status: 405 });
        }
        try {
          const update = await req.json();
          // 不等待 handleUpdate，让它在后台运行
          ctx.waitUntil(handleUpdate(update));
          // 立即返回 OK 给 Telegram
          return new Response('OK');
        } catch (error) {
          console.error('Error parsing request or handling update:', error);
          return new Response('Bad Request', { status: 400 });
        }
      } else if (url.pathname === '/registerWebhook') {
        return await registerWebhook(req);
      } else if (url.pathname === '/unRegisterWebhook') {
        return await unRegisterWebhook();
      } else if (url.pathname === '/checkTables') {
        await checkAndRepairTables(env.D1);
        return new Response('Database tables checked and repaired', { status: 200 });
      }
      return new Response('Not Found', { status: 404 });
    }

    /**
     * @param {D1Database} d1
     * @param {CfRequest} initialRequest
     */
    async function initialize(d1, initialRequest) {
      console.log("Initializing worker...");
      await Promise.all([
        checkAndRepairTables(d1),
        autoRegisterWebhook(initialRequest),
        checkBotPermissions(),
        // 可以在初始化时也运行一次清理
        // cleanExpiredVerificationCodes(d1) // ctx.waitUntil 已经调用
      ]);
       console.log("Worker initialization complete.");
    }

    /**
     * @param {CfRequest} request
     */
    async function autoRegisterWebhook(request) {
      const webhookUrl = `${new URL(request.url).origin}/webhook`;
      console.log(`Attempting webhook auto-registration: ${webhookUrl}`);
      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/setWebhook`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            url: webhookUrl,
            allowed_updates: ["message", "callback_query"] // 明确指定需要的更新类型
          }),
        });
        const data = await response.json();
        if (!response.ok || !data.ok) {
          console.error('Webhook auto-registration failed:', JSON.stringify(data || {status: response.status}, null, 2));
        } else {
          console.log('Webhook auto-registered successfully.');
        }
      } catch (error) {
        console.error('Error during webhook auto-registration:', error);
      }
    }

    async function checkBotPermissions() {
      console.log("Checking bot permissions...");
      try {
        const chatResponse = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getChat`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ chat_id: GROUP_ID })
        });
        const chatData = await chatResponse.json();
        if (!chatData.ok) {
          console.error(`Failed to access group ${GROUP_ID}: ${chatData.description}. Is GROUP_ID correct and bot a member?`);
          return; // 如果无法访问群组，则停止检查
        }

        const botId = await getBotId();
         if (!botId) {
             console.error("Could not determine Bot ID. Cannot check permissions.");
             return;
         }

        const memberResponse = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getChatMember`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: GROUP_ID,
            user_id: botId
          })
        });
        const memberData = await memberResponse.json();
        if (!memberData.ok) {
          console.error(`Failed to get bot member status in group ${GROUP_ID}: ${memberData.description}. Is bot an admin?`);
          return; // 如果无法获取成员状态，则停止
        }

        const result = memberData.result;
        const isAdmin = result.status === 'administrator' || result.status === 'creator';
        console.log(`Bot status in group ${GROUP_ID}: ${result.status}`);

        // 检查关键权限 (与原始代码一致)
        const canSendMessages = result.can_send_messages === true;
        const canPostMessages = result.can_post_messages === true; // 注意：原始代码为 !== false，改为 === true 更明确
        const canManageTopics = result.can_manage_topics === true; // 注意：原始代码为 !== false，改为 === true 更明确
        // 补充检查原始代码隐含需要的权限
        const canDeleteMessages = result.can_delete_messages === true;
        const canPinMessages = result.can_pin_messages === true;

        let missingPermissions = [];
        if (!canSendMessages) missingPermissions.push("Send Messages");
        // if (!canPostMessages) missingPermissions.push("Post Messages (in channels)"); // Post 通常用于频道
        if (!canManageTopics) missingPermissions.push("Manage Topics");
        if (!canDeleteMessages) missingPermissions.push("Delete Messages");
        if (!canPinMessages) missingPermissions.push("Pin Messages");

        if (missingPermissions.length > 0) {
          console.error('-------------------------------------------------');
          console.error('PERMISSION WARNING: Bot lacks necessary permissions!');
          console.error(`Group ID: ${GROUP_ID}`);
          console.error(`Missing: ${missingPermissions.join(', ')}`);
          if (!isAdmin) console.error("Hint: Bot is not an administrator in the group.");
          console.error('Please grant the bot Administrator rights with these permissions.');
          console.error('-------------------------------------------------');
        } else {
          console.log('Bot has necessary permissions checked.');
           if (!isAdmin) console.warn("Warning: Bot has permissions but is not an administrator. Granting admin rights is recommended.");
        }
      } catch (error) {
        console.error('Error checking bot permissions:', error);
      }
    }

    async function getBotId() {
      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getMe`);
        const data = await response.json();
        if (!data.ok) {
            console.error(`Failed to get bot ID: ${data.description}`);
            return null;
        }
        return data.result.id;
      } catch (error) {
          console.error("Exception fetching bot ID:", error);
          return null;
      }
    }

    /** @param {D1Database} d1 */
    async function checkAndRepairTables(d1) {
        console.log("Checking/Repairing DB tables...");
      try {
        // 保持原始表格结构定义
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
              is_rate_limited: 'BOOLEAN DEFAULT FALSE', // 保持原始字段
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
          let tableExists = false;
          try {
            const tableInfo = await d1.prepare(
              `SELECT sql FROM sqlite_master WHERE type='table' AND name=?`
            ).bind(tableName).first();

            if (!tableInfo) {
              console.warn(`Table '${tableName}' not found. Creating...`);
              await createTable(d1, tableName, structure);
              tableExists = true;
              if (tableName === 'settings') {
                  await d1.exec('CREATE INDEX IF NOT EXISTS idx_settings_key ON settings (key)');
              }
              continue;
            } else {
                tableExists = true;
            }

            const columnsResult = await d1.prepare(
              `PRAGMA table_info(${tableName})`
            ).all();

             if (!columnsResult || !columnsResult.results) {
                console.error(`Could not get column info for table '${tableName}'. Skipping column check.`);
                continue;
            }

            const currentColumns = new Map(
              columnsResult.results.map(col => [col.name.toLowerCase(), { // Use lowercase for comparison
                type: col.type.toUpperCase(),
                notnull: col.notnull,
                dflt_value: col.dflt_value
              }])
            );

            for (const [colName, colDef] of Object.entries(structure.columns)) {
              if (!currentColumns.has(colName.toLowerCase())) {
                console.warn(`Column '${colName}' missing in table '${tableName}'. Adding...`);
                const columnParts = colDef.trim().split(/\s+/);
                const colType = columnParts[0];
                const constraints = columnParts.slice(1).join(' ');
                const addColumnSQL = `ALTER TABLE ${tableName} ADD COLUMN ${colName} ${colType} ${constraints}`;
                try {
                    await d1.exec(addColumnSQL);
                    console.log(`Column '${colName}' added to '${tableName}'.`);
                } catch (addColumnError) {
                     console.error(`Failed to add column '${colName}' to '${tableName}':`, addColumnError);
                     // Consider more robust error handling if needed
                }
              }
            }

            if (tableName === 'settings') {
              const indexInfo = await d1.prepare(
                  `SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=? AND name='idx_settings_key'`
              ).bind(tableName).first();
              if (!indexInfo) {
                  console.warn(`Index 'idx_settings_key' missing for 'settings'. Creating...`);
                  await d1.exec('CREATE INDEX IF NOT EXISTS idx_settings_key ON settings (key)');
              }
            }
          } catch (error) {
            console.error(`Error checking/repairing table '${tableName}':`, error);
            if (!tableExists || error.message.includes("no such table")) {
                console.warn(`Attempting to recreate table '${tableName}' due to error.`);
                try {
                    await d1.exec(`DROP TABLE IF EXISTS ${tableName}`);
                    await createTable(d1, tableName, structure);
                    if (tableName === 'settings') await d1.exec('CREATE INDEX IF NOT EXISTS idx_settings_key ON settings (key)');
                    console.log(`Table '${tableName}' recreated.`);
                } catch (recreateError) {
                    console.error(`FATAL: Failed to recreate table '${tableName}':`, recreateError);
                    throw recreateError;
                }
            } else {
                 console.error(`Could not automatically repair '${tableName}'. Manual check needed.`);
            }
          }
        }

        // 保持原始的默认设置插入逻辑
        await Promise.all([
          d1.prepare('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)')
            .bind('verification_enabled', 'true').run(),
          d1.prepare('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)')
            .bind('user_raw_enabled', 'true').run()
        ]);

        // 加载设置到缓存 (保持原始逻辑)
        await loadSettingsToCache(d1); // 使用辅助函数加载
         console.log("DB table check/repair complete.");

      } catch (error) {
        console.error('Error in checkAndRepairTables:', error);
        throw error; // 抛出错误以停止初始化（如果需要）
      }
    }

     /** @param {D1Database} d1 */
    async function loadSettingsToCache(d1) {
        try {
            const results = await d1.prepare('SELECT key, value FROM settings').all();
            if (results?.results) {
                 settingsCache.set('verification_enabled', (results.results.find(r => r.key === 'verification_enabled')?.value || 'true') === 'true');
                 settingsCache.set('user_raw_enabled', (results.results.find(r => r.key === 'user_raw_enabled')?.value || 'true') === 'true');
                 // console.log("Settings loaded into cache:", settingsCache);
            } else {
                 console.warn("No settings found in DB. Using defaults.");
                 settingsCache.set('verification_enabled', true);
                 settingsCache.set('user_raw_enabled', true);
            }
        } catch (error) {
            console.error('Error loading settings into cache:', error);
            settingsCache.set('verification_enabled', true); // Fallback
            settingsCache.set('user_raw_enabled', true); // Fallback
        }
    }

    /**
     * @param {D1Database} d1
     * @param {string} tableName
     * @param {{ columns: Record<string, string> }} structure
     */
    async function createTable(d1, tableName, structure) {
      // 保持原始的建表逻辑
      const columnsDef = Object.entries(structure.columns)
        .map(([name, def]) => `"${name}" ${def}`) // 引用名称
        .join(', ');
      const createSQL = `CREATE TABLE "${tableName}" (${columnsDef});`;
      console.log(`Executing SQL: ${createSQL}`);
      await d1.exec(createSQL);
    }

    /** @param {D1Database} d1 */
    async function cleanExpiredVerificationCodes(d1) {
      const now = Date.now();
      // 保持原始的清理间隔检查
      if (now - lastCleanupTime < CLEANUP_INTERVAL) {
        return;
      }
       console.log("Running cleanup for expired verification codes...");

      try {
        const nowSeconds = Math.floor(now / 1000);
        // 保持原始的查询逻辑
        const expiredCodes = await d1.prepare(
          'SELECT chat_id FROM user_states WHERE code_expiry IS NOT NULL AND code_expiry < ?'
        ).bind(nowSeconds).all();

        if (expiredCodes.results && expiredCodes.results.length > 0) {
          const idsToClean = expiredCodes.results.map(({ chat_id }) => chat_id);
           console.log(`Found ${idsToClean.length} expired codes to clean.`);
          // 保持原始的批量更新逻辑
          await d1.batch(
            idsToClean.map(chat_id =>
              d1.prepare(
                'UPDATE user_states SET verification_code = NULL, code_expiry = NULL, is_verifying = FALSE WHERE chat_id = ?'
              ).bind(chat_id)
            )
          );
           console.log("Cleaned expired verification states.");
           // 清理缓存
           idsToClean.forEach(chat_id => {
               const cachedState = userStateCache.get(chat_id);
               if (cachedState) {
                   cachedState.verification_code = null;
                   cachedState.code_expiry = null;
                   cachedState.is_verifying = false;
                   userStateCache.set(chat_id, cachedState);
               }
           });
        }
        lastCleanupTime = now; // 更新清理时间
      } catch (error) {
        console.error('Error cleaning expired verification codes:', error);
      }
    }

    /** @param {any} update */
    async function handleUpdate(update) {
      // 保持原始的 update 处理逻辑
      if (update.message) {
        const message = update.message;
        const messageId = message.message_id?.toString();
        const chatId = message.chat?.id?.toString();

        if (!messageId || !chatId) {
            console.warn("Received message without message_id or chat_id");
            return;
        }

        const messageKey = `${chatId}:${messageId}`;
        if (processedMessages.has(messageKey)) {
          return; // Skip duplicate
        }
        processedMessages.add(messageKey);
        if (processedMessages.size > 10000) { // Cleanup old entries
          const oldestKeys = Array.from(processedMessages).slice(0, 5000);
          oldestKeys.forEach(key => processedMessages.delete(key));
        }

        await onMessage(message); // Await message processing

      } else if (update.callback_query) {
        const callbackQuery = update.callback_query;
        const callbackId = callbackQuery.id;
        const chatId = callbackQuery.message?.chat?.id?.toString();
        const messageId = callbackQuery.message?.message_id?.toString();

        if (!callbackId || !chatId || !messageId) {
            console.warn("Received callback_query without id, chat_id or message_id");
            return;
        }

        const callbackKey = `${chatId}:${messageId}:${callbackId}`; // Specific key
        if (processedCallbacks.has(callbackKey)) {
           ctx.waitUntil(answerCallbackQuery(callbackId).catch(()=>{})); // Answer duplicate anyway
           return; // Skip duplicate
        }
        processedCallbacks.add(callbackKey);
        if (processedCallbacks.size > 5000) { // Cleanup old entries
            const oldestKeys = Array.from(processedCallbacks).slice(0, 2500);
            oldestKeys.forEach(key => processedCallbacks.delete(key));
        }

        await onCallbackQuery(callbackQuery); // Await callback processing
      }
    }

    /** @param {import('@cloudflare/workers-types').TelegramMessage} message */
    async function onMessage(message) {
      const chatId = message.chat.id.toString();
      const text = message.text || '';
      const messageId = message.message_id;
      const fromUserId = message.from?.id?.toString(); // Original code didn't use this here, but good practice

      // --- Group Message Handling (保持原始逻辑) ---
      if (chatId === GROUP_ID) {
        const topicId = message.message_thread_id?.toString(); // Ensure string
        if (topicId) { // Message is in a topic
          const privateChatId = await getPrivateChatId(topicId);
          // 检查 /admin 命令 (需要管理员权限)
          if (privateChatId && text === '/admin') {
            const senderIsAdmin = fromUserId ? await checkIfAdmin(fromUserId) : false; // Check if sender is admin
            if (senderIsAdmin) {
                await sendAdminPanel(chatId, topicId, privateChatId, messageId);
            } else {
                 // Optional: Notify non-admin?
                 // await sendMessageToTopic(topicId, '只有管理员可以使用 /admin 命令。');
            }
            return; // Handled /admin command
          }
          // 检查 /reset_user 命令 (需要管理员权限)
          if (privateChatId && text.startsWith('/reset_user')) {
             const senderIsAdmin = fromUserId ? await checkIfAdmin(fromUserId) : false;
             if (senderIsAdmin) {
                 // ** 注意：原始代码 handleResetUser 只接受 chatId, topicId, text **
                 // ** 为了兼容原始签名，不传递 privateChatId，在 handleResetUser 内部重新获取 **
                 await handleResetUser(chatId, topicId, text);
             } else {
                  // Optional: Notify non-admin?
             }
             return; // Handled /reset_user command
          }
           // 检查 /delete_user 命令 (需要管理员权限)
           if (privateChatId && text.startsWith('/delete_user')) { // 假设这个命令存在于你的回调逻辑中
                const senderIsAdmin = fromUserId ? await checkIfAdmin(fromUserId) : false;
                if (senderIsAdmin) {
                     await handleDeleteUser(chatId, topicId, privateChatId); // 使用这个辅助函数
                }
                return;
           }


          // 转发群内消息到私聊 (如果 privateChatId 存在)
          if (privateChatId) {
            // 只有在 privateChatId 存在时才转发
            await forwardMessageToPrivateChat(privateChatId, message);
          }
        }
        // 如果消息不在 topic 中，或者 /admin, /reset_user 等命令未匹配，则忽略群内其他消息
        return;
      }

      // --- Private Message Handling ---

      // 获取用户状态 (保持原始逻辑)
      let userState = userStateCache.get(chatId);
      if (userState === undefined) {
         try {
            // 选择所有字段以兼容原始代码访问的任何字段
            userState = await env.D1.prepare('SELECT * FROM user_states WHERE chat_id = ?')
              .bind(chatId)
              .first();
            if (!userState) {
              // 保持原始的插入逻辑
              await env.D1.prepare('INSERT INTO user_states (chat_id, is_blocked, is_first_verification, is_verified, is_verifying) VALUES (?, ?, ?, ?, ?)')
                .bind(chatId, false, true, false, false)
                .run();
              // 创建默认状态对象 (与原始代码一致)
              userState = { chat_id: chatId, is_blocked: false, is_first_verification: true, is_verified: false, verified_expiry: null, verification_code: null, code_expiry: null, last_verification_message_id: null, is_rate_limited: false, is_verifying: false };
            }
            userStateCache.set(chatId, userState);
         } catch (dbError) {
            console.error(`DB Error fetching/creating user state for ${chatId}:`, dbError);
            await sendMessageToUser(chatId, "服务器内部错误，请稍后再试。");
            return;
         }
      }

      // 1. Check if blocked (保持原始逻辑)
      const isBlocked = userState.is_blocked || false;
      if (isBlocked) {
        await sendMessageToUser(chatId, "您已被拉黑，无法发送消息。请联系管理员解除拉黑。");
        return;
      }

      // 2. Handle /start (保持原始逻辑)
      if (text === '/start') {
        if (await checkStartCommandRate(chatId)) {
          await sendMessageToUser(chatId, "您发送 /start 命令过于频繁，请稍后再试！");
          return;
        }

        const verificationEnabled = settingsCache.get('verification_enabled');
        const isFirstVerification = userState.is_first_verification; // 使用已加载的状态

        if (verificationEnabled && isFirstVerification) {
          await sendMessageToUser(chatId, "你好，欢迎使用私聊机器人，请完成验证以开始使用！");
          await handleVerification(chatId, messageId); // 启动验证
        } else {
           // 检查是否已验证或验证是否过期 (与原始逻辑略有不同，但更健壮)
           const nowSeconds = Math.floor(Date.now() / 1000);
           const isCurrentlyVerified = userState.is_verified && userState.verified_expiry && nowSeconds < userState.verified_expiry;
           if(verificationEnabled && !isCurrentlyVerified) {
                await sendMessageToUser(chatId, "请先完成验证。");
                await handleVerification(chatId, messageId);
           } else {
               // 已验证或无需验证
               const successMessage = await getVerificationSuccessMessage();
               await sendMessageToUser(chatId, `${successMessage}\n你好，欢迎使用私聊机器人，现在发送信息吧！`);
           }
        }
        return; // /start 处理完毕
      }

      // 3. Check verification & rate limit for regular messages (保持原始逻辑)
      const verificationEnabled = settingsCache.get('verification_enabled');
      const nowSeconds = Math.floor(Date.now() / 1000);
      const isVerified = userState.is_verified && userState.verified_expiry && nowSeconds < userState.verified_expiry;
      // 重新获取 is_verifying 状态以确保最新 (保持原始逻辑)
       const latestState = await env.D1.prepare('SELECT is_verifying FROM user_states WHERE chat_id = ?').bind(chatId).first();
       const isVerifying = latestState?.is_verifying || userState.is_verifying || false; // Fallback to cache

      if (verificationEnabled && !isVerified) {
        if (isVerifying) {
          // 保持原始提示
          await sendMessageToUser(chatId, `请完成验证后发送消息“${text || '您的具体信息'}”。`);
          return; // 已经在验证中，不重复启动
        }
        // 保持原始提示
        await sendMessageToUser(chatId, `请完成验证后发送消息“${text || '您的具体信息'}”。`);
        await handleVerification(chatId, messageId); // 启动验证
        return; // 停止处理此消息
      }

       // 4. Check Rate Limit (保持原始逻辑)
       const isRateLimited = await checkMessageRate(chatId);
       if (isRateLimited) {
           // console.log(`User ${chatId} rate limited.`); // 可以取消注释以进行调试
           // 可以在此处添加给用户的提示，但原始代码没有，所以保持不变
           // await sendMessageToUser(chatId, "消息发送过于频繁，请稍候。");
           return;
       }


      // --- ***** BUG FIX AREA START ***** ---
      // 5. Forward verified message to topic
      try {
          // 先获取用户信息
          const userInfo = await getUserInfo(chatId);
          const userName = userInfo.username || `User_${chatId}`;
          const nickname = userInfo.nickname || userName;
          const topicName = nickname;

          // **关键修复：** 首先检查现有 Topic ID (缓存或数据库)
          let topicId = await getExistingTopicId(chatId);

          // **如果不存在，则创建新的**
          if (!topicId) {
              console.log(`No existing topic found for ${chatId}. Creating new one...`);
              // 创建 Topic (createForumTopic 返回数字 ID)
              const numericTopicId = await createForumTopic(topicName, userName, nickname, userInfo.id || chatId);
              topicId = numericTopicId.toString(); // 转换为字符串
              // 保存新的映射关系 (保存到数据库并更新缓存)
              await saveTopicId(chatId, topicId);
              console.log(`Created and saved topic ${topicId} for ${chatId}.`);
          } else {
               // console.log(`Found existing topic ${topicId} for ${chatId}.`);
          }

          // 确保 topicId 是字符串类型以便后续使用
          const topicIdStr = topicId.toString();

          // --- 保持原始的转发逻辑 ---
          try {
              if (text) {
                  // 保持原始格式
                  const formattedMessage = `${nickname}:\n${text}`;
                  await sendMessageToTopic(topicIdStr, formattedMessage);
              } else {
                  // 复制非文本消息
                  await copyMessageToTopic(topicIdStr, message);
              }
          } catch (error) {
              // 保持原始的 topic 关闭/删除错误处理和重试逻辑
               if (error.message && (error.message.includes('Request failed with status 400') || error.message.includes('message thread not found') || error.message.includes("topic closed"))) {
                   console.warn(`Forwarding to topic ${topicIdStr} failed (likely closed/deleted). Recreating for ${chatId}.`);
                   const newNumericTopicId = await createForumTopic(topicName, userName, nickname, userInfo.id || chatId);
                   const newTopicIdStr = newNumericTopicId.toString();
                   await saveTopicId(chatId, newTopicIdStr); // 更新映射

                   // Retry forwarding
                   if (text) {
                       const formattedMessage = `${nickname}:\n${text}`;
                       await sendMessageToTopic(newTopicIdStr, formattedMessage);
                   } else {
                       await copyMessageToTopic(newTopicIdStr, message);
                   }
                   console.log(`Retried forwarding to new topic ${newTopicIdStr} for ${chatId}.`);
               } else {
                   // 抛出其他转发错误
                   throw error;
               }
          }
          // --- 结束保持原始转发逻辑 ---

      } catch (error) {
        // 保持原始的外部 catch 块
        console.error(`Error handling message from chatId ${chatId}:`, error);
        await sendMessageToTopic(null, `无法转发用户 ${chatId} 的消息：${error.message}`); // 发送到主群
        await sendMessageToUser(chatId, "消息转发失败，请稍后再试或联系管理员。");
      }
      // --- ***** BUG FIX AREA END ***** ---
    }


    /**
     * @param {string} chatId - Group ID (== GROUP_ID)
     * @param {string} topicId - Topic ID string
     * @param {string} text - Command text like "/reset_user 12345"
     */
    async function handleResetUser(chatId, topicId, text) {
       // 保持原始逻辑，从文本中解析目标用户ID
       const parts = text.split(' ');
       if (parts.length !== 2 || !/^\d+$/.test(parts[1])) { // 检查格式和 ID 是否为数字
         await sendMessageToTopic(topicId, '用法：/reset_user <user_chat_id>');
         return;
       }
       const targetChatId = parts[1]; // 从命令文本获取目标ID

       // 在函数内部获取 privateChatId 以确认命令是否针对当前 topic 的用户 (可选但推荐)
        const currentTopicUser = await getPrivateChatId(topicId);
        if (currentTopicUser !== targetChatId) {
             await sendMessageToTopic(topicId, `警告：您正在尝试重置用户 ${targetChatId}，但这与当前话题用户 (${currentTopicUser}) 不符。请在正确的用户话题下执行此命令，或确认用户ID无误。如果确认，请忽略此消息。`);
             // 可以选择在这里 return 阻止跨话题重置，或者允许管理员重置任意用户
             // return; // 取消注释以阻止跨话题重置
        }


      try {
        // 保持原始的批量删除逻辑
        await env.D1.batch([
          env.D1.prepare('DELETE FROM user_states WHERE chat_id = ?').bind(targetChatId),
          env.D1.prepare('DELETE FROM message_rates WHERE chat_id = ?').bind(targetChatId)
        ]);
        // 保持原始的缓存清理逻辑
        userStateCache.set(targetChatId, undefined);
        messageRateCache.set(targetChatId, undefined);
        console.log(`Admin reset state for user ${targetChatId} via command in topic ${topicId}.`);
        await sendMessageToTopic(topicId, `用户 ${targetChatId} 的状态已重置。`); // 保持原始消息

      } catch (error) {
        console.error(`Error resetting user ${targetChatId}:`, error);
        await sendMessageToTopic(topicId, `重置用户 ${targetChatId} 失败：${error.message}`); // 保持原始消息
      }
    }

     /**
     * @param {string} adminChatId - Group ID
     * @param {string} topicId - Topic ID string
     * @param {string} userChatId - User's private chat ID derived from topic
     */
     async function handleDeleteUser(adminChatId, topicId, userChatId) {
        // 这是根据回调函数推断的，原始 onMessage 中没有直接调用
         if (!userChatId) {
             await sendMessageToTopic(topicId, 'Error: Could not find associated user for deletion.');
             return;
         }
         try {
             await env.D1.batch([
                 env.D1.prepare('DELETE FROM user_states WHERE chat_id = ?').bind(userChatId),
                 env.D1.prepare('DELETE FROM message_rates WHERE chat_id = ?').bind(userChatId),
                 env.D1.prepare('DELETE FROM chat_topic_mappings WHERE chat_id = ?').bind(userChatId)
             ]);
             userStateCache.set(userChatId, undefined);
             messageRateCache.set(userChatId, undefined);
             topicIdCache.set(userChatId, undefined);
             userInfoCache.set(userChatId, undefined);
             console.log(`Admin deleted user ${userChatId} data via topic ${topicId}.`);
             await sendMessageToTopic(topicId, `用户 ${userChatId} 的所有数据已删除。`);
         } catch (error) {
              console.error(`Error deleting user ${userChatId}:`, error);
              await sendMessageToTopic(topicId, `删除用户 ${userChatId} 失败: ${error.message}`);
         }
     }


    /**
     * @param {string} chatId - Group Chat ID (GROUP_ID)
     * @param {string} topicId - Topic Thread ID string
     * @param {string} privateChatId - User's Private Chat ID
     * @param {number | string} originalMessageId - ID of the /admin message to delete
     */
    async function sendAdminPanel(chatId, topicId, privateChatId, originalMessageId) {
      // 保持原始的管理员面板发送逻辑
      try {
        const verificationEnabled = settingsCache.get('verification_enabled');
        const userRawEnabled = settingsCache.get('user_raw_enabled');
         let isBlocked = false;
         const userState = userStateCache.get(privateChatId) || await env.D1.prepare('SELECT is_blocked FROM user_states WHERE chat_id = ?').bind(privateChatId).first();
         if (userState) isBlocked = userState.is_blocked || false;

        // 保持原始按钮定义
        const buttons = [
          [
            { text: isBlocked ? '解除拉黑' : '拉黑用户', callback_data: `${isBlocked ? 'unblock' : 'block'}_${privateChatId}` },
            // { text: '解除拉黑', callback_data: `unblock_${privateChatId}` } // 原始代码有两个按钮，这里根据状态合并为一个
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
            { text: '删除用户', callback_data: `delete_user_${privateChatId}` }
          ]
        ];

        const adminMessage = '管理员面板：请选择操作'; // 保持原始消息

        // 保持原始的发送和删除逻辑
        // 发送新面板
        await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              chat_id: chatId,
              message_thread_id: topicId,
              text: adminMessage,
              reply_markup: { inline_keyboard: buttons }
            })
          });
        // 删除旧的 /admin 消息
        if (originalMessageId) {
             ctx.waitUntil(fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                  chat_id: chatId,
                  message_id: originalMessageId
                })
              }).catch(error => console.error(`Non-critical: Error deleting original /admin message ${originalMessageId}:`, error)));
        }
      } catch (error) {
        console.error(`Error sending admin panel to chatId ${chatId}, topicId ${topicId}:`, error);
         try { await sendMessageToTopic(topicId, "发送管理面板时出错。"); } catch {}
      }
    }

    // --- 其他辅助函数保持不变 ---

    async function getVerificationSuccessMessage() {
      // 保持原始逻辑
      const userRawEnabled = settingsCache.get('user_raw_enabled');
      const defaultMessage = '验证成功！您现在可以与客服聊天。';
      if (!userRawEnabled) return defaultMessage;

      try {
        const response = await fetch('https://raw.githubusercontent.com/iawooo/ctt/main/CFTeleTrans/start.md', { headers: { 'Cache-Control': 'no-cache' }});
        if (!response.ok) {
             console.warn(`Failed to fetch start.md (${response.status}). Using default.`);
             // 原始代码没有 fallback 到 README，所以这里也不加
             return defaultMessage;
        }
        const message = await response.text();
        return message.trim() || defaultMessage;
      } catch (error) {
        console.error("Error fetching verification success message:", error);
        return defaultMessage;
      }
    }

    async function getNotificationContent() {
      // 保持原始逻辑
      const defaultNotification = ''; // 原始代码是空字符串
      try {
        const response = await fetch('https://raw.githubusercontent.com/iawooo/ctt/main/CFTeleTrans/notification.md', { headers: { 'Cache-Control': 'no-cache' } });
        if (!response.ok) {
             console.warn(`Failed to fetch notification.md (${response.status}). Using default (empty).`);
             return defaultNotification;
        }
        const content = await response.text();
        return content.trim() || defaultNotification;
      } catch (error) {
        console.error("Error fetching notification content:", error);
        return defaultNotification;
      }
    }

    /** @param {string} chatId */
    async function checkStartCommandRate(chatId) {
      // 保持原始逻辑
      const now = Date.now();
      const window = 5 * 60 * 1000;
      const maxStartsPerWindow = 1; // 原始代码是1

      let data = messageRateCache.get(chatId);
      let dbNeedsUpdate = false;
      if (data === undefined) {
        const dbData = await env.D1.prepare('SELECT start_count, start_window_start FROM message_rates WHERE chat_id = ?').bind(chatId).first();
        data = { ...(messageRateCache.get(chatId)), start_count: dbData?.start_count || 0, start_window_start: dbData?.start_window_start || 0 };
      }

      if (now - (data.start_window_start || 0) > window) {
        data.start_count = 1;
        data.start_window_start = now;
        dbNeedsUpdate = true;
      } else {
        data.start_count = (data.start_count || 0) + 1;
        dbNeedsUpdate = true;
      }
      messageRateCache.set(chatId, data);

      if (dbNeedsUpdate) {
           ctx.waitUntil(
               env.D1.prepare( // Use original INSERT OR REPLACE/UPDATE logic
                   'INSERT INTO message_rates (chat_id, start_count, start_window_start, message_count, window_start) VALUES (?, ?, ?, COALESCE((SELECT message_count FROM message_rates WHERE chat_id = ?), 0), COALESCE((SELECT window_start FROM message_rates WHERE chat_id = ?), 0)) ON CONFLICT(chat_id) DO UPDATE SET start_count = excluded.start_count, start_window_start = excluded.start_window_start'
               ).bind(chatId, data.start_count, data.start_window_start, chatId, chatId).run()
               .catch(dbError => console.error(`DB Error updating start rate for ${chatId}:`, dbError))
           );
       }
      return data.start_count > maxStartsPerWindow;
    }

    /** @param {string} chatId */
    async function checkMessageRate(chatId) {
      // 保持原始逻辑
      const now = Date.now();
      const window = 60 * 1000;

      let data = messageRateCache.get(chatId);
      let dbNeedsUpdate = false;
      if (data === undefined) {
        const dbData = await env.D1.prepare('SELECT message_count, window_start FROM message_rates WHERE chat_id = ?').bind(chatId).first();
        data = { ...(messageRateCache.get(chatId)), message_count: dbData?.message_count || 0, window_start: dbData?.window_start || 0 };
      }

      if (now - (data.window_start || 0) > window) {
        data.message_count = 1;
        data.window_start = now;
        dbNeedsUpdate = true;
      } else {
        data.message_count = (data.message_count || 0) + 1;
        dbNeedsUpdate = true;
      }
      messageRateCache.set(chatId, data);

       if (dbNeedsUpdate) {
           ctx.waitUntil(
               env.D1.prepare( // Use original INSERT OR REPLACE/UPDATE logic
                   'INSERT INTO message_rates (chat_id, message_count, window_start, start_count, start_window_start) VALUES (?, ?, ?, COALESCE((SELECT start_count FROM message_rates WHERE chat_id = ?), 0), COALESCE((SELECT start_window_start FROM message_rates WHERE chat_id = ?), 0)) ON CONFLICT(chat_id) DO UPDATE SET message_count = excluded.message_count, window_start = excluded.window_start'
               ).bind(chatId, data.message_count, data.window_start, chatId, chatId).run()
               .catch(dbError => console.error(`DB Error updating message rate for ${chatId}:`, dbError))
           );
       }
      return data.message_count > MAX_MESSAGES_PER_MINUTE;
    }

    /**
     * @param {string} key
     * @param {D1Database} d1
     * @returns {Promise<string | null>}
     */
    async function getSetting(key, d1) {
      // 保持原始逻辑: cache -> db -> default? (原始代码隐含了默认值)
       const cachedValue = settingsCache.get(key);
       // 注意：原始缓存检查可能未正确处理 boolean false
       if (cachedValue !== null && cachedValue !== undefined) {
           return String(cachedValue); // 返回字符串形式
       }

      try {
        const result = await d1.prepare('SELECT value FROM settings WHERE key = ?').bind(key).first();
        const dbValue = result?.value || null;

        if (dbValue !== null) {
            // Update cache with DB value
            if (key === 'verification_enabled' || key === 'user_raw_enabled') {
                settingsCache.set(key, dbValue === 'true');
            } else {
                settingsCache.set(key, dbValue);
            }
            return dbValue; // Return DB value
        } else {
            // DB returned null, apply default and cache it
            const defaultValue = (key === 'verification_enabled' || key === 'user_raw_enabled') ? true : null;
            const defaultStringValue = (key === 'verification_enabled' || key === 'user_raw_enabled') ? 'true' : null;
            settingsCache.set(key, defaultValue); // Cache the default boolean/null
            return defaultStringValue; // Return string default
        }
      } catch (error) {
        console.error(`Error getting setting ${key}:`, error);
        // Return default on error
        const defaultValue = (key === 'verification_enabled' || key === 'user_raw_enabled') ? 'true' : null;
        // Optionally cache the default on error? settingsCache.set(key, defaultValue === 'true');
        return defaultValue;
      }
    }

    /**
     * @param {string} key
     * @param {string} value
     */
    async function setSetting(key, value) {
      // 保持原始逻辑
      try {
        await env.D1.prepare('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)')
          .bind(key, value)
          .run();
        // Update cache
        if (key === 'verification_enabled' || key === 'user_raw_enabled') {
          settingsCache.set(key, value === 'true');
        } else {
            settingsCache.set(key, value);
        }
      } catch (error) {
        console.error(`Error setting ${key} to ${value}:`, error);
        throw error;
      }
    }

    /** @param {import('@cloudflare/workers-types').TelegramCallbackQuery} callbackQuery */
    async function onCallbackQuery(callbackQuery) {
        // --- 基本信息提取 (保持原始逻辑) ---
        const callbackQueryId = callbackQuery.id;
        const userId = callbackQuery.from.id.toString(); // 点击者
        const data = callbackQuery.data;
        const message = callbackQuery.message;
        if (!message) {
            ctx.waitUntil(answerCallbackQuery(callbackQueryId).catch(()=>{}));
            return;
        }
        const chatId = message.chat.id.toString(); // 消息所在聊天
        const messageId = message.message_id;
        const topicId = message.message_thread_id?.toString(); // 话题 ID

        // --- 尽早应答 Callback (保持原始逻辑) ---
        ctx.waitUntil(answerCallbackQuery(callbackQueryId).catch(err => console.warn(`Non-critical: Failed to answer callback ${callbackQueryId}: ${err}`)));

        // --- 解析 Action 和 Target (保持原始逻辑) ---
        const parts = data.split('_');
        let action = parts[0];
        let privateChatId = null; // 用于管理员操作的目标用户 ID

        // 保持原始的 action/privateChatId 解析逻辑
        if (data.startsWith('verify_')) {
            action = 'verify';
            privateChatId = parts[1]; // verify_{userChatId}_...
        } else if (data.startsWith('toggle_verification_')) {
            action = 'toggle_verification';
            privateChatId = parts.slice(2).join('_'); // toggle_verification_{adminContextId}
        } else if (data.startsWith('toggle_user_raw_')) {
            action = 'toggle_user_raw';
            privateChatId = parts.slice(3).join('_'); // toggle_user_raw_{adminContextId}
        } else if (data.startsWith('check_blocklist_')) {
            action = 'check_blocklist';
            privateChatId = parts.slice(2).join('_'); // check_blocklist_{adminContextId}
        } else if (data.startsWith('block_')) {
            action = 'block';
            privateChatId = parts.slice(1).join('_'); // block_{targetUserId}
        } else if (data.startsWith('unblock_')) {
            action = 'unblock';
            privateChatId = parts.slice(1).join('_'); // unblock_{targetUserId}
        } else if (data.startsWith('delete_user_')) {
            action = 'delete_user';
            privateChatId = parts.slice(2).join('_'); // delete_user_{targetUserId}
        } else {
            action = data; // 如果不匹配特定模式，则将整个 data 视为 action
            privateChatId = null; // 无法确定目标用户
        }
         // 修正：确保 privateChatId 确实是目标用户的 ID
         const targetUserId = privateChatId; // 将解析出的 ID 赋给 targetUserId

      // --- 处理 Action (保持原始逻辑结构) ---
      try {
        // --- 处理验证回调 ---
        if (action === 'verify') {
          const [, userChatId, selectedAnswer, result] = parts; // 重新解构以获取 userChatId
          // 安全检查：确保是目标用户点击
           if (userId !== userChatId) {
                console.warn(`Verification click mismatch: Clicker ${userId}, Target ${userChatId}`);
                return;
            }

           // 删除验证消息 (保持原始逻辑)
           ctx.waitUntil(fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
                method: 'POST', headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ chat_id: userChatId, message_id: messageId })
            }).catch(deleteError => console.warn(`Non-critical: Failed to delete verification msg ${messageId}: ${deleteError}`)));


          // 检查验证码状态 (保持原始逻辑)
          let verificationState = userStateCache.get(userChatId);
          if (verificationState === undefined) {
             verificationState = await env.D1.prepare('SELECT verification_code, code_expiry, is_verifying FROM user_states WHERE chat_id = ?').bind(userChatId).first();
             userStateCache.set(userChatId, verificationState); // 缓存结果
          }
          const storedCode = verificationState?.verification_code;
          const codeExpiry = verificationState?.code_expiry;
          const nowSeconds = Math.floor(Date.now() / 1000);

          if (!storedCode || (codeExpiry && nowSeconds > codeExpiry)) {
            await sendMessageToUser(userChatId, '验证码已过期，请重新发送消息以获取新验证码。'); // 保持原消息
            // 清理过期状态 (保持原始逻辑)
            ctx.waitUntil(env.D1.prepare('UPDATE user_states SET verification_code = NULL, code_expiry = NULL, is_verifying = FALSE WHERE chat_id = ?').bind(userChatId).run().catch(()=>{}));
            if (verificationState) {
                verificationState.verification_code = null;
                verificationState.code_expiry = null;
                verificationState.is_verifying = false;
                userStateCache.set(userChatId, verificationState);
            }
            return;
          }

          // 处理结果 (保持原始逻辑)
          if (result === 'correct') {
            const verifiedExpiry = nowSeconds + 3600 * 24; // 原始代码是1天
            await env.D1.prepare('UPDATE user_states SET is_verified = TRUE, verified_expiry = ?, verification_code = NULL, code_expiry = NULL, last_verification_message_id = NULL, is_first_verification = FALSE, is_verifying = FALSE WHERE chat_id = ?')
              .bind(verifiedExpiry, userChatId)
              .run();

             // 更新缓存 (保持原始逻辑)
             const updatedState = { ...(userStateCache.get(userChatId)), is_verified: true, verified_expiry: verifiedExpiry, verification_code: null, code_expiry: null, last_verification_message_id: null, is_first_verification: false, is_verifying: false };
             userStateCache.set(userChatId, updatedState);

            // 重置速率限制 (保持原始逻辑)
            ctx.waitUntil(env.D1.prepare('UPDATE message_rates SET message_count = 0 WHERE chat_id = ?').bind(userChatId).run().catch(()=>{}));
            let rateData = messageRateCache.get(userChatId);
            if (rateData) { rateData.message_count = 0; messageRateCache.set(userChatId, rateData); }

            // 发送成功消息 (保持原始逻辑)
            const successMessage = await getVerificationSuccessMessage();
            await sendMessageToUser(userChatId, `${successMessage}\n你好，欢迎使用私聊机器人！现在可以发送消息了。`);
          } else {
            await sendMessageToUser(userChatId, '验证失败，请重新尝试。'); // 保持原消息
            await handleVerification(userChatId, null); // 重新发起验证
          }

        // --- 处理管理员回调 ---
        } else {
          // 检查是否为管理员 (保持原始逻辑)
          const isAdmin = await checkIfAdmin(userId);
          if (!isAdmin) {
            await answerCallbackQuery(callbackQueryId, { text: "只有管理员可以使用此功能。", show_alert: true }); // 保持原消息
            return;
          }

           // 确保目标用户 ID 存在 (使用前面解析的 targetUserId)
           if (!targetUserId && ['block', 'unblock', 'delete_user'].includes(action)) {
                console.error(`Admin action '${action}' needs target user ID, but got null from data: ${data}`);
                await sendMessageToTopic(topicId, `操作 '${action}' 失败: 缺少目标用户 ID。`);
                return;
            }

           let refreshAdminPanel = true; // 默认刷新面板
           // privateChatId 在这里应该等于 targetUserId
           const panelContextId = targetUserId || await getPrivateChatId(topicId); // 面板需要一个用户 ID 上下文

          // 处理具体管理员操作 (保持原始逻辑)
          if (action === 'block') {
            // 使用 targetUserId
            await env.D1.prepare('INSERT OR REPLACE INTO user_states (chat_id, is_blocked) VALUES (?, TRUE)').bind(targetUserId).run();
            userStateCache.set(targetUserId, { ...(userStateCache.get(targetUserId)), is_blocked: true });
            await sendMessageToTopic(topicId, `用户 ${targetUserId} 已被拉黑，消息将不再转发。`); // 保持原消息
          } else if (action === 'unblock') {
            // 使用 targetUserId
            await env.D1.prepare('UPDATE user_states SET is_blocked = FALSE, is_first_verification = TRUE, is_verified = FALSE WHERE chat_id = ?').bind(targetUserId).run();
            userStateCache.set(targetUserId, { ...(userStateCache.get(targetUserId)), is_blocked: false, is_first_verification: true, is_verified: false });
            await sendMessageToTopic(topicId, `用户 ${targetUserId} 已解除拉黑，消息将继续转发。`); // 保持原消息
          } else if (action === 'toggle_verification') {
            const currentState = settingsCache.get('verification_enabled');
            const newState = !currentState;
            await setSetting('verification_enabled', newState.toString());
            await sendMessageToTopic(topicId, `验证码功能已${newState ? '开启' : '关闭'}。`); // 保持原消息
          } else if (action === 'check_blocklist') {
            const blockedResult = await env.D1.prepare('SELECT chat_id FROM user_states WHERE is_blocked = TRUE').all();
            const blockedIds = blockedResult?.results?.map(r => r.chat_id) || [];
            const blockListText = blockedIds.length > 0 ? blockedIds.join('\n') : '当前没有被拉黑的用户。';
            await sendMessageToTopic(topicId, `黑名单列表：\n${blockListText}`); // 保持原消息
            refreshAdminPanel = false; // 查询类不刷新面板
          } else if (action === 'toggle_user_raw') {
            const currentState = settingsCache.get('user_raw_enabled');
            const newState = !currentState;
            await setSetting('user_raw_enabled', newState.toString());
            await sendMessageToTopic(topicId, `用户端 Raw 链接已${newState ? '开启' : '关闭'}。`); // 保持原消息
          } else if (action === 'delete_user') {
             // 使用 targetUserId
             await handleDeleteUser(chatId, topicId, targetUserId); // 调用辅助函数
             refreshAdminPanel = false; // 删除后不刷新面板
          } else {
            await sendMessageToTopic(topicId, `未知操作：${action}`); // 保持原消息
            refreshAdminPanel = false;
          }

          // 刷新管理员面板 (保持原始逻辑)
          if (refreshAdminPanel && topicId && panelContextId) {
             // 删除旧面板
             ctx.waitUntil(fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
                 method: 'POST', headers: { 'Content-Type': 'application/json' },
                 body: JSON.stringify({ chat_id: chatId, message_id: messageId })
             }).catch(deleteError => console.warn(`Non-critical: Failed to delete old admin panel ${messageId}: ${deleteError}`)));
             // 发送新面板
             await sendAdminPanel(chatId, topicId, panelContextId, null);
          }
        }

      } catch (error) {
        console.error(`Error processing callback query ${data} from user ${userId}:`, error);
        // 保持原始错误处理
        if (topicId && chatId === GROUP_ID) { // Admin command error
            await sendMessageToTopic(topicId, `处理操作 ${action} 失败：${error.message}`);
        } else if (action === 'verify') { // Verification error
             try { await sendMessageToUser(userId, "验证处理时发生错误，请重试。"); } catch {}
        }
      }
    }


    async function handleVerification(chatId, messageId) {
      // 保持原始的 handleVerification 逻辑
       let userState = userStateCache.get(chatId);
       if (userState === undefined) {
         userState = await env.D1.prepare('SELECT is_blocked, is_first_verification, is_verified, verified_expiry, is_verifying, last_verification_message_id FROM user_states WHERE chat_id = ?')
           .bind(chatId)
           .first();
         if (!userState) {
              // This case should ideally be handled by onMessage creating the state
              console.error(`State not found during handleVerification for ${chatId}`);
              await sendMessageToUser(chatId, "无法开始验证，请重试 /start");
              return;
         }
         userStateCache.set(chatId, userState);
       }

       // 保持原始的清理旧状态和设置为验证中的逻辑
       // userState.verification_code = null; // 不在这里清除，在发送挑战前清除
       // userState.code_expiry = null;
       userState.is_verifying = true;
       userStateCache.set(chatId, userState);
       // 先更新数据库状态为 is_verifying = TRUE
       await env.D1.prepare('UPDATE user_states SET is_verifying = TRUE, verification_code = NULL, code_expiry = NULL WHERE chat_id = ?')
           .bind(chatId)
           .run();


      // 保持原始的删除旧验证消息逻辑
      const lastVerification = userState.last_verification_message_id; // Use already fetched state
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
        } catch (error) {
          console.warn("Non-critical: Error deleting old verification message:", error.message);
        }
        // 清理数据库和缓存中的旧 ID
        userState.last_verification_message_id = null;
        userStateCache.set(chatId, userState);
        ctx.waitUntil(env.D1.prepare('UPDATE user_states SET last_verification_message_id = NULL WHERE chat_id = ?').bind(chatId).run().catch(()=>{}));
      }

      await sendVerificationChallenge(chatId); // 调用发送挑战函数
    }

    async function sendVerificationChallenge(chatId) {
      // 保持原始的 sendVerification (已重命名为 sendVerificationChallenge) 逻辑
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
        text: `(${option})`, // 保持原始按钮文本
        callback_data: `verify_${chatId}_${option}_${option === correctResult ? 'correct' : 'wrong'}`
      }));

      const question = `请计算：${num1} ${operation} ${num2} = ?（点击下方按钮完成验证）`; // 保持原问题
      const nowSeconds = Math.floor(Date.now() / 1000);
      const codeExpiry = nowSeconds + 300; // 5分钟
      const correctCodeString = correctResult.toString();

      // 获取当前状态以更新 (确保 is_verifying=true 已被设置)
       let userState = userStateCache.get(chatId);
       if (!userState) {
            console.error(`State lost before sending challenge for ${chatId}`);
            return; // 无法继续
       }

       // --- 在发送前存储验证码和有效期 ---
       try {
            userState.verification_code = correctCodeString;
            userState.code_expiry = codeExpiry;
            userState.last_verification_message_id = null; // 清空旧ID
            userStateCache.set(chatId, userState);

            await env.D1.prepare('UPDATE user_states SET verification_code = ?, code_expiry = ?, last_verification_message_id = NULL WHERE chat_id = ?')
               .bind(correctCodeString, codeExpiry, chatId)
               .run();
       } catch (dbError) {
            console.error(`DB error saving verification details for ${chatId}:`, dbError);
            // 如果存储失败，需要回滚 is_verifying 状态吗？
            userState.is_verifying = false;
            userState.verification_code = null;
            userState.code_expiry = null;
            userStateCache.set(chatId, userState);
            ctx.waitUntil(env.D1.prepare('UPDATE user_states SET is_verifying = FALSE, verification_code = NULL, code_expiry = NULL WHERE chat_id = ?').bind(chatId).run().catch(()=>{}));
            await sendMessageToUser(chatId, "存储验证信息时出错，请重试。");
            return;
       }
        // --- 结束存储 ---


      try {
        // 发送验证消息 (保持原始逻辑)
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

        // 如果发送成功，记录消息 ID (保持原始逻辑)
        if (data.ok && data.result?.message_id) {
          const sentMessageId = data.result.message_id.toString();
          userState.last_verification_message_id = sentMessageId;
          userStateCache.set(chatId, userState);
          await env.D1.prepare('UPDATE user_states SET last_verification_message_id = ? WHERE chat_id = ?')
            .bind(sentMessageId, chatId)
            .run();
        } else {
             // 发送失败，回滚 is_verifying
             console.error(`Failed to send verification message to ${chatId}: ${data.description}`);
             userState.is_verifying = false;
             userState.verification_code = null;
             userState.code_expiry = null;
             userStateCache.set(chatId, userState);
             ctx.waitUntil(env.D1.prepare('UPDATE user_states SET is_verifying = FALSE, verification_code = NULL, code_expiry = NULL WHERE chat_id = ?').bind(chatId).run().catch(()=>{}));
             await sendMessageToUser(chatId, "发送验证消息失败，请稍后重试。");
        }
      } catch (error) {
        console.error("Error sending verification message:", error);
         // 发送失败，回滚 is_verifying
         userState.is_verifying = false;
         userState.verification_code = null;
         userState.code_expiry = null;
         userStateCache.set(chatId, userState);
         ctx.waitUntil(env.D1.prepare('UPDATE user_states SET is_verifying = FALSE, verification_code = NULL, code_expiry = NULL WHERE chat_id = ?').bind(chatId).run().catch(()=>{}));
         await sendMessageToUser(chatId, "发送验证消息时发生网络错误，请重试。");
      }
    }

    async function checkIfAdmin(userId) {
      // 保持原始逻辑
      if (!userId) return false;
      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getChatMember`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ chat_id: GROUP_ID, user_id: userId })
        });
        const data = await response.json();
        return data.ok && (data.result.status === 'administrator' || data.result.status === 'creator');
      } catch (error) {
        console.error(`Error checking admin status for user ${userId}:`, error);
        return false;
      }
    }

    async function getUserInfo(chatId) {
       // 保持原始逻辑
      let userInfo = userInfoCache.get(chatId);
      if (userInfo === undefined) {
        try {
          const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getChat`, {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ chat_id: chatId })
          });
          const data = await response.json();
          if (!data.ok) {
            userInfo = { id: chatId, username: `User_${chatId}`, nickname: `User_${chatId}` };
          } else {
            const result = data.result;
            const nickname = result.first_name
              ? `${result.first_name}${result.last_name ? ` ${result.last_name}` : ''}`.trim()
              : result.username || `User_${chatId}`;
            userInfo = {
              id: result.id?.toString() || chatId,
              username: result.username || `User_${chatId}`,
              nickname: nickname
            };
          }
          userInfoCache.set(chatId, userInfo);
        } catch (error) {
          console.error(`Error fetching user info for chatId ${chatId}:`, error);
          userInfo = { id: chatId, username: `User_${chatId}`, nickname: `User_${chatId}` };
          userInfoCache.set(chatId, userInfo);
        }
      }
      return userInfo;
    }

    async function getExistingTopicId(chatId) {
      // 保持原始逻辑
      let topicId = topicIdCache.get(chatId);
      if (topicId === undefined) {
        try {
             const mapping = await env.D1.prepare('SELECT topic_id FROM chat_topic_mappings WHERE chat_id = ?').bind(chatId).first();
             topicId = mapping?.topic_id || null; // 使用 null 表示未找到
             if (topicId) topicIdCache.set(chatId, topicId);
        } catch(dbError) {
             console.error(`DB Error getting topic ID for ${chatId}:`, dbError);
             topicId = null;
        }
      }
      return topicId;
    }

    async function createForumTopic(topicName, userName, nickname, userId) {
      // 保持原始逻辑
      try {
        const createResponse = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/createForumTopic`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ chat_id: GROUP_ID, name: topicName.substring(0, 128) })
        });
        const createData = await createResponse.json();
        if (!createData.ok) throw new Error(`Failed to create forum topic: ${createData.description}`);
        const topicId = createData.result.message_thread_id; // Numeric ID
        const topicIdStr = topicId.toString();

        const now = new Date();
        const formattedTime = now.toISOString().replace('T', ' ').substring(0, 19);
        const notificationContent = await getNotificationContent();
        // 保持原始 pinned message 格式
        const pinnedMessage = `昵称: ${nickname}\n用户名: @${userName}\nUserID: ${userId}\n发起时间: ${formattedTime}\n\n${notificationContent}`;
        const messageResponse = await sendMessageToTopic(topicIdStr, pinnedMessage);
        if (messageResponse.ok && messageResponse.result?.message_id) {
            const messageId = messageResponse.result.message_id;
             // Pinning in topic not supported directly, pins globally
            ctx.waitUntil(pinMessage(topicIdStr, messageId).catch(()=>{}));
        }
        return topicId; // 返回数字 ID
      } catch (error) {
        console.error(`Error creating forum topic for user ${userId}:`, error);
        throw error;
      }
    }

    async function saveTopicId(chatId, topicId) {
      // 保持原始逻辑 (topicId 传入时应为 string)
      topicIdCache.set(chatId, topicId);
      try {
        await env.D1.prepare('INSERT OR REPLACE INTO chat_topic_mappings (chat_id, topic_id) VALUES (?, ?)')
          .bind(chatId, topicId)
          .run();
      } catch (error) {
        console.error(`Error saving topic ID ${topicId} for chatId ${chatId}:`, error);
        topicIdCache.set(chatId, undefined); // Invalidate cache on error
        throw error;
      }
    }

    async function getPrivateChatId(topicId) {
      // 保持原始逻辑 (topicId 传入时应为 string)
      for (const [chatId, tid] of topicIdCache.cache) if (tid === topicId) return chatId;
      try {
        const mapping = await env.D1.prepare('SELECT chat_id FROM chat_topic_mappings WHERE topic_id = ?').bind(topicId).first();
         const privateChatId = mapping?.chat_id || null;
         if(privateChatId) topicIdCache.set(privateChatId, topicId); // Populate forward cache
        return privateChatId;
      } catch (error) {
        console.error(`Error fetching private chat ID for topicId ${topicId}:`, error);
        return null; // 原始代码抛出错误，这里改为返回 null
      }
    }

    async function sendMessageToTopic(topicId, text, parseMode = null) {
      // 保持原始逻辑 (topicId 传入时应为 string 或 null)
       if (!text || text.trim() === '') return { ok: false, description: 'Empty text' };
       const MAX_LENGTH = 4096;
       if (text.length > MAX_LENGTH) text = text.substring(0, MAX_LENGTH - 10) + "...(截断)";

      try {
        const requestBody = {
          chat_id: GROUP_ID, text: text,
          ...(topicId && { message_thread_id: topicId }),
          ...(parseMode && { parse_mode: parseMode }),
        };
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(requestBody)
        });
        const data = await response.json();
        if (!data.ok) {
          // 原始代码抛出错误，保持一致
          throw new Error(`Failed to send message to topic: ${data.description} (chat_id: ${GROUP_ID}, topic_id: ${topicId})`);
        }
        return data; // 返回成功响应
      } catch (error) {
        console.error(`Error sending message to topic ${topicId}:`, error);
        throw error; // 重新抛出以被上层捕获
      }
    }

    async function copyMessageToTopic(topicId, message) {
      // 保持原始逻辑 (topicId 传入时应为 string)
      try {
        const requestBody = {
          chat_id: GROUP_ID, from_chat_id: message.chat.id,
          message_id: message.message_id, message_thread_id: topicId,
          disable_notification: true
        };
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/copyMessage`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(requestBody)
        });
        const data = await response.json();
        if (!data.ok) {
           // 原始代码抛出错误，保持一致
           throw new Error(`Failed to copy message to topic: ${data.description} (chat_id: ${GROUP_ID}, from_chat_id: ${message.chat.id}, message_id: ${message.message_id}, topic_id: ${topicId})`);
        }
        // 成功时不返回任何内容
      } catch (error) {
        console.error(`Error copying message to topic ${topicId}:`, error);
        throw error; // 重新抛出以被上层捕获
      }
    }

    async function pinMessage(topicId, messageId) {
       // 保持原始逻辑 (topicId 仅用于日志)
      try {
        const requestBody = { chat_id: GROUP_ID, message_id: messageId }; // no message_thread_id
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/pinChatMessage`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(requestBody)
        });
        const data = await response.json();
        if (!data.ok) {
            // 原始代码抛出错误，保持一致
            throw new Error(`Failed to pin message: ${data.description}`);
        }
      } catch (error) {
        console.error(`Error pinning message ${messageId} in group ${GROUP_ID} (assoc topic ${topicId}):`, error);
        throw error; // 重新抛出
      }
    }

    async function forwardMessageToPrivateChat(privateChatId, message) {
      // 保持原始逻辑
      try {
        const requestBody = {
          chat_id: privateChatId, from_chat_id: message.chat.id,
          message_id: message.message_id, disable_notification: true
        };
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/copyMessage`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(requestBody)
        });
        const data = await response.json();
        if (!data.ok) {
          // 原始代码抛出错误，保持一致
          throw new Error(`Failed to forward message to private chat: ${data.description} (chat_id: ${privateChatId}, from_chat_id: ${message.chat.id}, message_id: ${message.message_id})`);
        }
      } catch (error) {
        console.error(`Error forwarding message to private chat ${privateChatId}:`, error);
        throw error; // 重新抛出
      }
    }

    async function sendMessageToUser(chatId, text) {
      // 保持原始逻辑
      if (!text || text.trim() === '') return;
      const MAX_LENGTH = 4096;
      if (text.length > MAX_LENGTH) text = text.substring(0, MAX_LENGTH - 10) + "...(截断)";

      try {
        const requestBody = { chat_id: chatId, text: text };
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(requestBody)
        });
        const data = await response.json();
        if (!data.ok) {
            // 原始代码抛出错误，保持一致
            throw new Error(`Failed to send message to user: ${data.description}`);
        }
      } catch (error) {
        console.error(`Error sending message to user ${chatId}:`, error);
        throw error; // 重新抛出
      }
    }

    async function fetchWithRetry(url, options = {}, retries = 3, backoff = 500) {
      // 保持原始逻辑
      let lastError;
      for (let i = 0; i < retries; i++) {
         let timeoutId;
        try {
          const controller = new AbortController();
          timeoutId = setTimeout(() => controller.abort(), 10000); // 10秒超时
          const response = await fetch(url, { ...options, signal: controller.signal });
          clearTimeout(timeoutId);

          if (response.ok) return response; // 成功则返回

          if (response.status === 429) { // 处理限速
            const retryAfter = parseInt(response.headers.get('Retry-After') || '5', 10);
            const delay = Math.max(retryAfter * 1000, backoff);
            console.warn(`Rate limited (429), retrying after ${delay}ms...`);
            await new Promise(resolve => setTimeout(resolve, delay));
            lastError = new Error(`Rate limited (429)`);
            continue;
          }
           if (response.status >= 500) { // 处理服务器错误
                console.warn(`Server error (${response.status}), retrying after ${backoff}ms...`);
                await new Promise(resolve => setTimeout(resolve, backoff));
                lastError = new Error(`Server error (${response.status})`);
                backoff *= 2;
                continue;
           }

          // 对于其他 4xx 错误，不重试，直接返回响应让调用者处理
           console.warn(`Non-retryable client error (${response.status}) for ${url}`);
           return response; // 返回非 OK 但非重试的响应

        } catch (error) {
           clearTimeout(timeoutId); // 确保超时被清除
           lastError = error;
           if (error.name === 'AbortError') {
               console.error(`Fetch aborted (timeout) attempt ${i + 1}/${retries} for ${url}`);
           } else {
               console.error(`Fetch error attempt ${i + 1}/${retries} for ${url}:`, error.message);
           }
           if (i === retries - 1) throw lastError; // 最后一次尝试失败则抛出
           await new Promise(resolve => setTimeout(resolve, backoff));
           backoff *= 2; // 指数退避
        }
      }
       throw lastError || new Error(`Fetch failed after ${retries} retries`); // Fallback throw
    }

    async function registerWebhook(request) {
      // 保持原始逻辑
      const webhookUrl = `${new URL(request.url).origin}/webhook`;
      const response = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/setWebhook`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ url: webhookUrl, allowed_updates: ["message", "callback_query"] }) // 保持 allowed_updates
      }).then(r => r.json());
      return new Response(response.ok ? 'Webhook set successfully' : JSON.stringify(response, null, 2));
    }

    async function unRegisterWebhook() {
      // 保持原始逻辑
      const response = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/setWebhook`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ url: '' })
      }).then(r => r.json());
      return new Response(response.ok ? 'Webhook removed' : JSON.stringify(response, null, 2));
    }

    // --- Main Request Handling (保持原始逻辑) ---
    try {
      return await handleRequest(request);
    } catch (error) {
      console.error('Unhandled error in fetch handler:', error, error.stack);
       // 保持原始的错误报告逻辑
       if (BOT_TOKEN && GROUP_ID) {
           ctx.waitUntil(sendMessageToTopic(null, `🚨 WORKER ERROR 🚨\n${error.message}`).catch(()=>{}));
       }
      return new Response('Internal Server Error', { status: 500 });
    }
  }
};

/**
 * 保持原始的 answerCallbackQuery 辅助函数
 * @param {string} callbackQueryId
 * @param {{ text?: string; show_alert?: boolean }} [options={}]
 */
async function answerCallbackQuery(callbackQueryId, options = {}) {
    if (!BOT_TOKEN) return;
    try {
        // 使用原始的 fetch (不带 retry)
        const response = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/answerCallbackQuery`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ callback_query_id: callbackQueryId, ...options })
        });
         if (!response.ok) {
             const data = await response.json().catch(() => ({}));
             console.warn(`Non-critical: Failed to answer callback ${callbackQueryId}: ${data.description || response.status}`);
        }
    } catch (error) {
        console.warn(`Non-critical: Exception answering callback ${callbackQueryId}:`, error);
    }
}
