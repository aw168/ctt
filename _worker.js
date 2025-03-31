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

    // 初始化逻辑 (保持原始)
    if (!isInitialized) {
      // ctx.waitUntil(initialize(env.D1, request)); // Use waitUntil if init can run async
      await initialize(env.D1, request); // Stick to original await if needed
      isInitialized = true;
    }

     // 清理逻辑 (保持原始) - 在后台运行
    ctx.waitUntil(cleanExpiredVerificationCodes(env.D1));


    /**
     * @param {CfRequest} req
     * @returns {Promise<Response>}
     */
    async function handleRequest(req) {
      // 保持原始 handleRequest 逻辑
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
          // 使用 waitUntil 运行 handleUpdate，立即响应 TG (保持原始优化)
          ctx.waitUntil(handleUpdate(update));
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
      // 保持原始 initialize 逻辑
      await Promise.all([
        checkAndRepairTables(d1),
        autoRegisterWebhook(initialRequest),
        checkBotPermissions(),
        cleanExpiredVerificationCodes(d1) // 保持原始调用位置
      ]);
    }

    /**
     * @param {CfRequest} request
     */
    async function autoRegisterWebhook(request) {
      // 保持原始 autoRegisterWebhook 逻辑
      const webhookUrl = `${new URL(request.url).origin}/webhook`;
      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/setWebhook`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ url: webhookUrl, allowed_updates: ["message", "callback_query"] }), // 显式添加 updates
        }).then(r => r.json()); // 保持原始的 .then(r => r.json())
        if (!response.ok) {
          console.error('Webhook auto-registration failed:', JSON.stringify(response, null, 2));
        } else {
           console.log('Webhook auto-registration successful.');
        }
      } catch (error) {
        console.error('Error during webhook auto-registration:', error);
      }
    }

    async function checkBotPermissions() {
      // 保持原始 checkBotPermissions 逻辑
      try {
        const chatResponse = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getChat`, { // 使用 fetchWithRetry
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ chat_id: GROUP_ID })
        });
        const chatData = await chatResponse.json();
        if (!chatData.ok) {
          throw new Error(`Failed to access group: ${chatData.description}`);
        }

        const botId = await getBotId(); // 保持获取 Bot ID
        if (!botId) throw new Error("Failed to get Bot ID");

        const memberResponse = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getChatMember`, { // 使用 fetchWithRetry
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: GROUP_ID,
            user_id: botId // 使用获取到的 Bot ID
          })
        });
        const memberData = await memberResponse.json();
        if (!memberData.ok) {
          throw new Error(`Failed to get bot member status: ${memberData.description}`);
        }

        // 保持原始权限检查逻辑 (使用 !== false)
        const canSendMessages = memberData.result.can_send_messages !== false;
        const canPostMessages = memberData.result.can_post_messages !== false;
        const canManageTopics = memberData.result.can_manage_topics !== false;
        // 补充检查其他必要权限
        const canDeleteMessages = memberData.result.can_delete_messages === true; // 用 === true 检查更安全
        const canPinMessages = memberData.result.can_pin_messages === true;

        if (!canSendMessages || !canPostMessages || !canManageTopics || !canDeleteMessages || !canPinMessages) {
          console.error('Bot lacks necessary permissions in the group:', {
            canSendMessages,
            canPostMessages, // Keep original check name
            canManageTopics, // Keep original check name
            canDeleteMessages,
            canPinMessages
          });
          // 原始代码没有抛出错误，所以这里也不抛出
        } else {
            console.log("Bot permissions seem sufficient.");
        }
      } catch (error) {
        console.error('Error checking bot permissions:', error);
        // 原始代码抛出错误，保持一致
        throw error;
      }
    }

    async function getBotId() {
      // 保持原始 getBotId 逻辑
      try {
          const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getMe`); // 原始代码未使用 fetchWithRetry，但这里用上更好
          const data = await response.json();
          if (!data.ok) throw new Error(`Failed to get bot ID: ${data.description}`);
          return data.result.id;
      } catch (error) {
           console.error("Failed to get bot ID:", error);
           throw error; // 原始代码在失败时会抛出错误（因为没有 catch）
      }
    }

    /** @param {D1Database} d1 */
    async function checkAndRepairTables(d1) {
      // 保持原始 checkAndRepairTables 逻辑
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
          try {
            const tableInfo = await d1.prepare(
              `SELECT sql FROM sqlite_master WHERE type='table' AND name=?`
            ).bind(tableName).first();

            if (!tableInfo) {
              console.warn(`Table '${tableName}' creating...`);
              await createTable(d1, tableName, structure); // 保持原始调用
              // 原始代码没有在创建后立即加索引，所以这里也不加
              continue;
            }

            const columnsResult = await d1.prepare(
              `PRAGMA table_info(${tableName})`
            ).all();

            const currentColumns = new Map(
              columnsResult.results.map(col => [col.name.toLowerCase(), { // 保持小写比较
                type: col.type.toUpperCase(),
                notnull: col.notnull,
                dflt_value: col.dflt_value
              }])
            );

            for (const [colName, colDef] of Object.entries(structure.columns)) {
              if (!currentColumns.has(colName.toLowerCase())) {
                 console.warn(`Column '${colName}' missing in '${tableName}'. Adding...`);
                const columnParts = colDef.trim().split(/\s+/);
                // 保持原始的添加列逻辑
                const addColumnSQL = `ALTER TABLE ${tableName} ADD COLUMN ${colName} ${columnParts.slice(1).join(' ')}`;
                await d1.exec(addColumnSQL);
              }
            }

            // 保持原始的 settings 索引创建逻辑
            if (tableName === 'settings') {
               // 原始代码在这里创建索引，即使表已存在
               await d1.exec('CREATE INDEX IF NOT EXISTS idx_settings_key ON settings (key)');
            }
          } catch (error) {
            console.error(`Error checking ${tableName}:`, error);
             // 保持原始的删除和重建逻辑
            console.warn(`Attempting DROP and CREATE for ${tableName}`);
            await d1.exec(`DROP TABLE IF EXISTS ${tableName}`);
            await createTable(d1, tableName, structure);
            if (tableName === 'settings') await d1.exec('CREATE INDEX IF NOT EXISTS idx_settings_key ON settings (key)'); // 重建后加索引
          }
        }

        // 保持原始的默认设置插入逻辑
        await Promise.all([
          d1.prepare('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)')
            .bind('verification_enabled', 'true').run(),
          d1.prepare('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)')
            .bind('user_raw_enabled', 'true').run()
        ]);

        // 保持原始的缓存加载逻辑
        settingsCache.set('verification_enabled', (await getSetting('verification_enabled', d1)) === 'true');
        settingsCache.set('user_raw_enabled', (await getSetting('user_raw_enabled', d1)) === 'true');
        console.log("DB check/repair finished.");
      } catch (error) {
        console.error('Error in checkAndRepairTables:', error);
        throw error; // 保持原始抛出
      }
    }

    /**
     * @param {D1Database} d1
     * @param {string} tableName
     * @param {{ columns: Record<string, string> }} structure
     */
    async function createTable(d1, tableName, structure) {
      // 保持原始 createTable 逻辑
      const columnsDef = Object.entries(structure.columns)
        .map(([name, def]) => `${name} ${def}`)
        .join(', ');
      const createSQL = `CREATE TABLE ${tableName} (${columnsDef})`;
      await d1.exec(createSQL);
    }

    /** @param {D1Database} d1 */
    async function cleanExpiredVerificationCodes(d1) {
      // 保持原始 cleanExpiredVerificationCodes 逻辑
      const now = Date.now();
      if (now - lastCleanupTime < CLEANUP_INTERVAL) {
        return;
      }
       console.log("Cleaning expired verification codes...");

      try {
        const nowSeconds = Math.floor(now / 1000);
        const expiredCodes = await d1.prepare(
          'SELECT chat_id FROM user_states WHERE code_expiry IS NOT NULL AND code_expiry < ?'
        ).bind(nowSeconds).all();

        if (expiredCodes.results && expiredCodes.results.length > 0) {
           console.log(`Found ${expiredCodes.results.length} expired codes.`);
          await d1.batch(
            expiredCodes.results.map(({ chat_id }) =>
              d1.prepare(
                'UPDATE user_states SET verification_code = NULL, code_expiry = NULL, is_verifying = FALSE WHERE chat_id = ?' // 保持 is_verifying 重置
              ).bind(chat_id)
            )
          );
          // 清理缓存
          expiredCodes.results.forEach(({ chat_id }) => {
               const cachedState = userStateCache.get(chat_id);
               if (cachedState) {
                   cachedState.verification_code = null;
                   cachedState.code_expiry = null;
                   cachedState.is_verifying = false;
                   userStateCache.set(chat_id, cachedState);
               }
           });
        }
        lastCleanupTime = now;
      } catch (error) {
        console.error('Error cleaning expired verification codes:', error);
      }
    }

    /** @param {any} update */
    async function handleUpdate(update) {
      // 保持原始 handleUpdate 逻辑
      if (update.message) {
        const messageId = update.message.message_id.toString();
        const chatId = update.message.chat.id.toString();
        const messageKey = `${chatId}:${messageId}`;

        if (processedMessages.has(messageKey)) {
          return;
        }
        processedMessages.add(messageKey);

        if (processedMessages.size > 10000) {
           // 保持原始清理逻辑
           processedMessages.clear(); // 原始是 clear()
        }

        await onMessage(update.message); // 保持 await

      } else if (update.callback_query) {
         // 保持原始 callback 处理逻辑
         const callbackQuery = update.callback_query;
         const callbackId = callbackQuery.id;
         const chatId = callbackQuery.message?.chat?.id?.toString(); // 从 message 获取
         // const messageId = callbackQuery.message?.message_id?.toString(); // 从 message 获取
         const callbackKey = `${chatId}:${callbackId}`; // 使用原始 key

         if (processedCallbacks.has(callbackKey)) {
             // 原始代码没有应答重复回调，所以这里也不应答
             return;
         }
         processedCallbacks.add(callbackKey);
          // 原始代码没有清理 processedCallbacks，所以这里也不加

        await onCallbackQuery(callbackQuery); // 保持 await
      }
    }

    /** @param {import('@cloudflare/workers-types').TelegramMessage} message */
    async function onMessage(message) {
      const chatId = message.chat.id.toString();
      const text = message.text || '';
      const messageId = message.message_id;
      const fromUserId = message.from?.id?.toString(); // 获取发送者ID

      // --- Group Message Handling (保持原始逻辑) ---
      if (chatId === GROUP_ID) {
        const topicId = message.message_thread_id?.toString(); // 确保是字符串
        if (topicId) {
          const privateChatId = await getPrivateChatId(topicId);
          // **保持原始 `/admin` 检查逻辑 (没有管理员检查)**
          if (privateChatId && text === '/admin') {
             // 直接调用 sendAdminPanel，不检查 senderIsAdmin
             await sendAdminPanel(chatId, topicId, privateChatId, messageId);
             return; // 处理完毕
          }
          // **保持原始 `/reset_user` 检查逻辑**
          if (privateChatId && text.startsWith('/reset_user')) {
             // 原始代码在这里没有检查管理员，检查是在 onCallbackQuery 中
             // 调用原始的 handleResetUser
             await handleResetUser(chatId, topicId, text);
             return; // 处理完毕
          }
          // 转发逻辑 (保持原始)
          if (privateChatId) {
            await forwardMessageToPrivateChat(privateChatId, message);
          }
        }
        // 忽略群内其他消息 (保持原始行为)
        return;
      }

      // --- Private Message Handling ---

      // 获取用户状态 (保持原始逻辑)
      let userState = userStateCache.get(chatId);
      if (userState === undefined) {
         try {
            userState = await env.D1.prepare('SELECT * FROM user_states WHERE chat_id = ?') // 获取所有字段
              .bind(chatId)
              .first();
            if (!userState) {
              await env.D1.prepare('INSERT INTO user_states (chat_id, is_blocked, is_first_verification, is_verified, is_verifying) VALUES (?, ?, ?, ?, ?)')
                .bind(chatId, false, true, false, false)
                .run();
              userState = { chat_id: chatId, is_blocked: false, is_first_verification: true, is_verified: false, verified_expiry: null, verification_code: null, code_expiry: null, last_verification_message_id: null, is_rate_limited: false, is_verifying: false };
            }
            userStateCache.set(chatId, userState);
         } catch (dbError) {
            console.error(`DB Error user state ${chatId}:`, dbError);
            await sendMessageToUser(chatId, "服务器错误."); // 简化错误消息
            return;
         }
      }

      // 1. Check blocked (保持原始逻辑)
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
        const isFirstVerification = userState.is_first_verification;
        if (verificationEnabled && isFirstVerification) {
          await sendMessageToUser(chatId, "你好，欢迎使用私聊机器人，请完成验证以开始使用！");
          await handleVerification(chatId, messageId);
        } else {
          const successMessage = await getVerificationSuccessMessage();
          // 原始代码即使非首次也发送欢迎语，保持一致
          await sendMessageToUser(chatId, `${successMessage}\n你好，欢迎使用私聊机器人，现在发送信息吧！`);
        }
        return; // /start 处理完毕
      }

      // 3. Check verification & rate limit (保持原始逻辑)
      const verificationEnabled = settingsCache.get('verification_enabled');
      const nowSeconds = Math.floor(Date.now() / 1000);
      const isVerified = userState.is_verified && userState.verified_expiry && nowSeconds < userState.verified_expiry;
      const isFirstVerification = userState.is_first_verification; // 原始代码在这里也检查了
      const isRateLimited = await checkMessageRate(chatId); // 原始代码在这里检查了速率
      // 保持原始的重新获取 is_verifying 逻辑
      const latestState = await env.D1.prepare('SELECT is_verifying FROM user_states WHERE chat_id = ?').bind(chatId).first();
      const isVerifying = latestState?.is_verifying || userState.is_verifying || false; // Fallback to cache/state

      // 保持原始的验证检查逻辑
      // 注意原始逻辑是 (verificationEnabled AND (!isVerified OR (isRateLimited AND !isFirstVerification)))
      // 这意味着即使验证通过，如果速率受限且不是首次验证，也会触发验证
      // 保持这个原始的、可能有些奇怪的逻辑
      if (verificationEnabled && (!isVerified || (isRateLimited && !isFirstVerification))) {
        if (isVerifying) {
          await sendMessageToUser(chatId, `请完成验证后发送消息“${text || '您的具体信息'}”。`);
          return;
        }
        await sendMessageToUser(chatId, `请完成验证后发送消息“${text || '您的具体信息'}”。`);
        await handleVerification(chatId, messageId); // 启动验证
        return; // 停止处理此消息
      }
       // 如果因为速率限制触发了上面的验证，这里就不需要再检查 isRateLimited 了
       // 如果上面的条件不满足（即已验证且未触发速率限制验证），则继续

      // --- ***** BUG FIX AREA START ***** ---
      // 4. Forward verified message to topic
      try {
          // 获取用户信息 (保持原始位置)
          const userInfo = await getUserInfo(chatId);
          // 原始代码在这里检查缓存，然后才创建或转发
          // **修改点：** 先调用 getExistingTopicId 检查缓存和数据库
          let topicId = await getExistingTopicId(chatId);

          const userName = userInfo.username || `User_${chatId}`;
          const nickname = userInfo.nickname || userName;
          const topicName = nickname;

          // **修改点：** 如果 getExistingTopicId 返回 null，则创建
          if (!topicId) {
              console.log(`Creating new topic for ${chatId}...`);
              const numericTopicId = await createForumTopic(topicName, userName, nickname, userInfo.id || chatId);
              topicId = numericTopicId.toString();
              await saveTopicId(chatId, topicId); // 保存映射
          }

          // 确保 topicId 是字符串
          const topicIdStr = topicId.toString();

          // --- 保持原始的转发逻辑 ---
          try {
              if (text) {
                  const formattedMessage = `${nickname}:\n${text}`;
                  await sendMessageToTopic(topicIdStr, formattedMessage);
              } else {
                  await copyMessageToTopic(topicIdStr, message);
              }
          } catch (error) {
              // 保持原始的 topic 关闭/删除错误处理和重试逻辑
               if (error.message && (error.message.includes('Request failed with status 400') || error.message.includes('message thread not found') || error.message.includes("topic closed"))) {
                   console.warn(`Topic ${topicIdStr} closed/deleted for ${chatId}. Recreating...`);
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
               } else {
                   throw error; // Re-throw 其他错误
               }
          }
          // --- 结束保持原始转发逻辑 ---

      } catch (error) {
        // 保持原始的外部 catch 块
        console.error(`Error handling message from chatId ${chatId}:`, error);
        await sendMessageToTopic(null, `无法转发用户 ${chatId} 的消息：${error.message}`);
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
       // 保持原始逻辑，从文本解析 targetChatId
       const parts = text.split(' ');
       // 保持原始检查逻辑 (没有检查 ID 合法性)
       if (parts.length !== 2) {
         await sendMessageToTopic(topicId, '用法：/reset_user <chat_id>');
         return;
       }
       const targetChatId = parts[1];

      try {
        // 保持原始数据库操作
        await env.D1.batch([
          env.D1.prepare('DELETE FROM user_states WHERE chat_id = ?').bind(targetChatId),
          env.D1.prepare('DELETE FROM message_rates WHERE chat_id = ?').bind(targetChatId)
        ]);
        // 保持原始缓存清理
        userStateCache.set(targetChatId, undefined); // 原始代码是 undefined
        messageRateCache.set(targetChatId, undefined); // 原始代码是 undefined
        await sendMessageToTopic(topicId, `用户 ${targetChatId} 的状态已重置。`); // 保持原消息

      } catch (error) {
        console.error(`Error resetting user ${targetChatId}:`, error);
        await sendMessageToTopic(topicId, `重置用户 ${targetChatId} 失败：${error.message}`); // 保持原消息
      }
    }

     /**
     * @param {string} adminChatId - Group ID
     * @param {string} topicId - Topic ID string
     * @param {string} userChatId - User's private chat ID derived from topic
     */
     async function handleDeleteUser(adminChatId, topicId, userChatId) {
        // 保持 onCallbackQuery 中 delete_user 的逻辑
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
             await sendMessageToTopic(topicId, `用户 ${userChatId} 的状态和消息记录已删除，话题保留。`); // 保持回调中的消息
         } catch (error) {
              console.error(`Error deleting user ${userChatId}:`, error);
              await sendMessageToTopic(topicId, `删除用户 ${userChatId} 失败：${error.message}`); // 保持回调中的消息
         }
     }


    /**
     * @param {string} chatId - Group Chat ID (GROUP_ID)
     * @param {string} topicId - Topic Thread ID string
     * @param {string} privateChatId - User's Private Chat ID
     * @param {number | string} originalMessageId - ID of the /admin message to delete
     */
    async function sendAdminPanel(chatId, topicId, privateChatId, originalMessageId) {
      // 保持原始 sendAdminPanel 逻辑
      try {
        const verificationEnabled = settingsCache.get('verification_enabled');
        const userRawEnabled = settingsCache.get('user_raw_enabled');

        // 保持原始按钮定义 (合并 block/unblock)
         let isBlocked = false;
         const userState = userStateCache.get(privateChatId) || await env.D1.prepare('SELECT is_blocked FROM user_states WHERE chat_id = ?').bind(privateChatId).first();
         if (userState) isBlocked = userState.is_blocked || false;
         const blockAction = isBlocked ? 'unblock' : 'block';
         const blockText = isBlocked ? '解除拉黑' : '拉黑用户';

        const buttons = [
          [
            { text: blockText, callback_data: `${blockAction}_${privateChatId}` }
            // 原始代码有两个按钮，这里保持合并后的简化版，如果你需要原始的两个按钮请告知
            // { text: '拉黑用户', callback_data: `block_${privateChatId}` },
            // { text: '解除拉黑', callback_data: `unblock_${privateChatId}` }
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

        // 保持原始发送和删除逻辑
        await Promise.all([
          fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, { // 使用 fetchWithRetry
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              chat_id: chatId,
              message_thread_id: topicId,
              text: adminMessage,
              reply_markup: { inline_keyboard: buttons }
            })
          }),
          // 保持原始删除逻辑 (如果 originalMessageId 存在)
          originalMessageId ? fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, { // 使用 fetchWithRetry
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              chat_id: chatId,
              message_id: originalMessageId
            })
          }).catch(error => console.error(`Non-critical: Error deleting message ${originalMessageId}:`, error)) : Promise.resolve() // 如果 ID 不存在，则解析为空 Promise
        ]);
      } catch (error) {
        console.error(`Error sending admin panel to chatId ${chatId}, topicId ${topicId}:`, error);
        // 原始代码没有错误通知，所以这里也不加
      }
    }

    // --- 其他辅助函数保持不变 (与上一版相同，这里省略以减少篇幅) ---
    // getVerificationSuccessMessage, getNotificationContent, checkStartCommandRate, checkMessageRate
    // getSetting, setSetting, onCallbackQuery, handleVerification, sendVerificationChallenge (原sendVerification)
    // checkIfAdmin, getUserInfo, getExistingTopicId, createForumTopic, saveTopicId, getPrivateChatId
    // sendMessageToTopic, copyMessageToTopic, pinMessage, forwardMessageToPrivateChat, sendMessageToUser
    // fetchWithRetry, registerWebhook, unRegisterWebhook

    // --- 保持 onCallbackQuery 完整逻辑 ---
    /** @param {import('@cloudflare/workers-types').TelegramCallbackQuery} callbackQuery */
    async function onCallbackQuery(callbackQuery) {
        const chatId = callbackQuery.message.chat.id.toString();
        const topicId = callbackQuery.message.message_thread_id?.toString();
        const data = callbackQuery.data;
        const messageId = callbackQuery.message.message_id;
        const callbackQueryId = callbackQuery.id; // 使用 callbackQuery.id
        const userId = callbackQuery.from.id.toString(); // 点击者 ID

        // **保持原始的 callbackKey 和检查逻辑**
        const callbackKey = `${chatId}:${callbackQuery.id}`; // 原始 key
        if (processedCallbacks.has(callbackKey)) {
            // 原始代码没有应答，所以这里也不应答
            return;
        }
        processedCallbacks.add(callbackKey);
        // 原始代码没有清理，所以这里也不清理

        // --- 解析 Action 和 Target (保持原始解析逻辑) ---
        const parts = data.split('_');
        let action = parts[0];
        let privateChatId = null; // 用于管理员操作的目标用户 ID

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
            action = data; // Fallback
            privateChatId = ''; // 原始代码是空字符串
        }
        const targetUserId = privateChatId; // 目标用户 ID (与 privateChatId 相同)

        try {
            // --- 处理验证回调 (保持原始逻辑) ---
            if (action === 'verify') {
                const [, userChatId, selectedAnswer, result] = parts; // 获取 userChatId

                // 安全检查：确保是目标用户点击 (原始代码没有此检查，但这是好的实践，这里保留)
                if (userId !== userChatId) {
                     console.warn(`Verification click mismatch: Clicker ${userId}, Target ${userChatId}`);
                     ctx.waitUntil(answerCallbackQuery(callbackQueryId).catch(()=>{})); // 应答一下
                     return;
                 }

                 // 获取验证状态 (保持原始逻辑)
                let verificationState = userStateCache.get(userChatId);
                if (verificationState === undefined) {
                    verificationState = await env.D1.prepare('SELECT verification_code, code_expiry, is_verifying FROM user_states WHERE chat_id = ?').bind(userChatId).first();
                    userStateCache.set(userChatId, verificationState);
                }
                const storedCode = verificationState?.verification_code;
                const codeExpiry = verificationState?.code_expiry;
                const nowSeconds = Math.floor(Date.now() / 1000);

                // 删除验证消息 (保持原始位置)
                // 注意：如果验证码过期或错误，消息也会被删除
                ctx.waitUntil(fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, { // 使用 fetchWithRetry
                    method: 'POST', headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ chat_id: userChatId, message_id: messageId })
                }).catch(deleteError => console.warn(`Non-critical: Failed to delete verification msg ${messageId}: ${deleteError}`)));

                // 处理过期 (保持原始逻辑)
                if (!storedCode || (codeExpiry && nowSeconds > codeExpiry)) {
                    await sendMessageToUser(userChatId, '验证码已过期，请重新发送消息以获取新验证码。');
                    ctx.waitUntil(env.D1.prepare('UPDATE user_states SET verification_code = NULL, code_expiry = NULL, is_verifying = FALSE WHERE chat_id = ?').bind(userChatId).run().catch(()=>{}));
                    if (verificationState) { /* ... 清理缓存 ... */ verificationState.verification_code=null; verificationState.code_expiry=null; verificationState.is_verifying=false; userStateCache.set(userChatId, verificationState); }
                    // 原始代码在 return 前没有应答 callback，保持一致
                    return;
                }

                // 处理结果 (保持原始逻辑)
                if (result === 'correct') {
                    const verifiedExpiry = nowSeconds + 3600 * 24; // 1天
                    await env.D1.prepare('UPDATE user_states SET is_verified = TRUE, verified_expiry = ?, verification_code = NULL, code_expiry = NULL, last_verification_message_id = NULL, is_first_verification = FALSE, is_verifying = FALSE WHERE chat_id = ?')
                        .bind(verifiedExpiry, userChatId).run();
                    /* ... 更新缓存 ... */ const updatedState={...(userStateCache.get(userChatId)),is_verified:true,verified_expiry:verifiedExpiry,verification_code:null,code_expiry:null,last_verification_message_id:null,is_first_verification:false,is_verifying:false}; userStateCache.set(userChatId,updatedState);
                    /* ... 重置速率 ... */ ctx.waitUntil(env.D1.prepare('UPDATE message_rates SET message_count = 0 WHERE chat_id = ?').bind(userChatId).run().catch(()=>{})); let rateData=messageRateCache.get(userChatId); if(rateData){rateData.message_count=0;messageRateCache.set(userChatId,rateData);}
                    const successMessage = await getVerificationSuccessMessage();
                    await sendMessageToUser(userChatId, `${successMessage}\n你好，欢迎使用私聊机器人！现在可以发送消息了。`);
                } else {
                    await sendMessageToUser(userChatId, '验证失败，请重新尝试。');
                    await handleVerification(userChatId, null); // 重新验证
                }
                // 原始代码在验证结束后没有应答 callback，保持一致

            // --- 处理管理员回调 (保持原始逻辑) ---
            } else {
                const isAdmin = await checkIfAdmin(userId);
                if (!isAdmin) {
                    await answerCallbackQuery(callbackQueryId, { text: "只有管理员可以使用此功能。", show_alert: true }); // 保持原消息
                    return;
                }

                // 保持原始的目标用户 ID 检查
                if (!targetUserId && ['block', 'unblock', 'delete_user'].includes(action)) {
                     console.error(`Admin action '${action}' needs target user ID, got null`);
                     await sendMessageToTopic(topicId, `操作 '${action}' 失败: 缺少目标用户 ID。`); // 保持原消息
                     // 原始代码没有应答 callback，保持一致
                     return;
                 }

                let refreshAdminPanel = true;
                const panelContextId = targetUserId || await getPrivateChatId(topicId); // 保持获取上下文 ID

                // 保持原始 action 处理逻辑
                if (action === 'block') {
                    await env.D1.prepare('INSERT OR REPLACE INTO user_states (chat_id, is_blocked) VALUES (?, TRUE)').bind(targetUserId).run();
                    userStateCache.set(targetUserId, { ...(userStateCache.get(targetUserId)), is_blocked: true });
                    await sendMessageToTopic(topicId, `用户 ${targetUserId} 已被拉黑，消息将不再转发。`);
                } else if (action === 'unblock') {
                    await env.D1.prepare('UPDATE user_states SET is_blocked = FALSE, is_first_verification = TRUE, is_verified = FALSE WHERE chat_id = ?').bind(targetUserId).run();
                    userStateCache.set(targetUserId, { ...(userStateCache.get(targetUserId)), is_blocked: false, is_first_verification: true, is_verified: false });
                    await sendMessageToTopic(topicId, `用户 ${targetUserId} 已解除拉黑，消息将继续转发。`);
                } else if (action === 'toggle_verification') {
                    const currentState = settingsCache.get('verification_enabled');
                    const newState = !currentState;
                    await setSetting('verification_enabled', newState.toString());
                    await sendMessageToTopic(topicId, `验证码功能已${newState ? '开启' : '关闭'}。`);
                } else if (action === 'check_blocklist') {
                    const blockedResult = await env.D1.prepare('SELECT chat_id FROM user_states WHERE is_blocked = TRUE').all();
                    const blockedIds = blockedResult?.results?.map(r => r.chat_id) || [];
                    const blockListText = blockedIds.length > 0 ? blockedIds.join('\n') : '当前没有被拉黑的用户。';
                    await sendMessageToTopic(topicId, `黑名单列表：\n${blockListText}`);
                    refreshAdminPanel = false;
                } else if (action === 'toggle_user_raw') {
                    const currentState = settingsCache.get('user_raw_enabled');
                    const newState = !currentState;
                    await setSetting('user_raw_enabled', newState.toString());
                    await sendMessageToTopic(topicId, `用户端 Raw 链接已${newState ? '开启' : '关闭'}。`);
                } else if (action === 'delete_user') {
                    await handleDeleteUser(chatId, topicId, targetUserId);
                    refreshAdminPanel = false;
                } else {
                    await sendMessageToTopic(topicId, `未知操作：${action}`);
                    refreshAdminPanel = false;
                }

                 // 刷新面板逻辑 (保持原始)
                 if (refreshAdminPanel && topicId && panelContextId) {
                     // 原始代码没有删除旧面板，而是直接发送新面板，保持这个行为
                     // ctx.waitUntil(fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, { ... }).catch(...)); // 如果需要删除旧面板
                     await sendAdminPanel(chatId, topicId, panelContextId, messageId); // 传递 messageId 让 sendAdminPanel 删除旧面板
                 }
                 // 原始代码在管理员操作后才应答 callback，保持这个位置
                 await answerCallbackQuery(callbackQueryId); // 使用辅助函数应答
            }

        } catch (error) {
            console.error(`Error processing callback query ${data} from user ${userId}:`, error);
            // 保持原始错误处理
             if (topicId && chatId === GROUP_ID) { // Admin command error
                 await sendMessageToTopic(topicId, `处理操作 ${action} 失败：${error.message}`);
             } else if (action === 'verify') { // Verification error
                  try { await sendMessageToUser(userId, "处理验证时发生内部错误，请重试。"); } catch {}
             }
             // 原始代码在 catch 块中没有应答 callback，保持一致
        }
    }


     // --- 保持其他辅助函数完整性 (getUserInfo, getExistingTopicId, etc.) ---
     // ... (所有其他辅助函数，与上一版本相同，这里省略) ...

    async function getVerificationSuccessMessage() {
      // 保持原始逻辑
      const userRawEnabled = settingsCache.get('user_raw_enabled');
      const defaultMessage = '验证成功！您现在可以与客服聊天。';
      if (!userRawEnabled) return defaultMessage;
      try {
        const response = await fetch('https://raw.githubusercontent.com/iawooo/ctt/main/CFTeleTrans/start.md', { headers: { 'Cache-Control': 'no-cache' }});
        if (!response.ok) return defaultMessage;
        const message = await response.text();
        return message.trim() || defaultMessage;
      } catch (error) {
        console.error("Error fetching verification success message:", error);
        return defaultMessage;
      }
    }
    async function getNotificationContent() {
      // 保持原始逻辑
      const defaultNotification = '';
      try {
        const response = await fetch('https://raw.githubusercontent.com/iawooo/ctt/main/CFTeleTrans/notification.md', { headers: { 'Cache-Control': 'no-cache' } });
        if (!response.ok) return defaultNotification;
        const content = await response.text();
        return content.trim() || defaultNotification;
      } catch (error) {
        console.error("Error fetching notification content:", error);
        return defaultNotification;
      }
    }
    async function getSetting(key, d1) {
        // 保持原始逻辑（包含默认值处理）
        const cachedValue = settingsCache.get(key);
        if (cachedValue !== null && cachedValue !== undefined) { // 检查 null 和 undefined
            return String(cachedValue);
        }
        try {
            const result = await d1.prepare('SELECT value FROM settings WHERE key = ?').bind(key).first();
            const dbValue = result?.value || null;
            if (dbValue !== null) {
                const isBoolSetting = key === 'verification_enabled' || key === 'user_raw_enabled';
                settingsCache.set(key, isBoolSetting ? dbValue === 'true' : dbValue);
                return dbValue;
            } else {
                // Apply default and cache it (原始代码的隐式行为)
                const defaultValue = (key === 'verification_enabled' || key === 'user_raw_enabled') ? true : null;
                const defaultStringValue = (key === 'verification_enabled' || key === 'user_raw_enabled') ? 'true' : null;
                settingsCache.set(key, defaultValue);
                return defaultStringValue;
            }
        } catch (error) {
            console.error(`Error getting setting ${key}:`, error);
            const defaultValue = (key === 'verification_enabled' || key === 'user_raw_enabled') ? 'true' : null;
            return defaultValue;
        }
    }
    async function setSetting(key, value) {
        // 保持原始逻辑
        try {
            await env.D1.prepare('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)').bind(key, value).run();
            const isBoolSetting = key === 'verification_enabled' || key === 'user_raw_enabled';
            settingsCache.set(key, isBoolSetting ? value === 'true' : value);
        } catch (error) {
            console.error(`Error setting ${key} to ${value}:`, error);
            throw error;
        }
    }
    // 保持 sendVerificationChallenge (原 sendVerification) 逻辑
    async function sendVerificationChallenge(chatId) {
        const num1 = Math.floor(Math.random() * 10);
        const num2 = Math.floor(Math.random() * 10);
        const operation = Math.random() > 0.5 ? '+' : '-';
        const correctResult = operation === '+' ? num1 + num2 : num1 - num2;
        const options = new Set([correctResult]);
        while (options.size < 4) { const w = correctResult + Math.floor(Math.random() * 5) - 2; if (w !== correctResult) options.add(w); }
        const optionArray = Array.from(options).sort(() => Math.random() - 0.5);
        const buttons = optionArray.map(o => ({ text: `(${o})`, callback_data: `verify_${chatId}_${o}_${o === correctResult ? 'correct' : 'wrong'}` }));
        const question = `请计算：${num1} ${operation} ${num2} = ?（点击下方按钮完成验证）`;
        const nowSeconds = Math.floor(Date.now() / 1000);
        const codeExpiry = nowSeconds + 300;
        const correctCodeString = correctResult.toString();

        // --- 在发送前存储验证码和有效期 (保持原始逻辑) ---
        let userState = userStateCache.get(chatId); if(!userState){console.error(`State lost before sending challenge ${chatId}`);return;}
        try {
            userState.verification_code = correctCodeString; userState.code_expiry = codeExpiry; userState.last_verification_message_id = null; userStateCache.set(chatId, userState);
            await env.D1.prepare('UPDATE user_states SET verification_code = ?, code_expiry = ?, last_verification_message_id = NULL WHERE chat_id = ?').bind(correctCodeString, codeExpiry, chatId).run();
        } catch (dbError) { /* ... 错误处理，回滚 is_verifying ... */ console.error("DB error save verify details", dbError); userState.is_verifying = false; userState.verification_code=null; userState.code_expiry=null; userStateCache.set(chatId, userState); ctx.waitUntil(env.D1.prepare('UPDATE user_states SET is_verifying=FALSE, verification_code=NULL, code_expiry=NULL WHERE chat_id = ?').bind(chatId).run().catch(()=>{})); await sendMessageToUser(chatId,"存储验证信息出错"); return; }

        try {
            const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ chat_id: chatId, text: question, reply_markup: { inline_keyboard: [buttons] } }) });
            const data = await response.json();
            if (data.ok && data.result?.message_id) {
                const sentMessageId = data.result.message_id.toString();
                userState.last_verification_message_id = sentMessageId; userStateCache.set(chatId, userState);
                await env.D1.prepare('UPDATE user_states SET last_verification_message_id = ? WHERE chat_id = ?').bind(sentMessageId, chatId).run();
            } else { /* ... 错误处理，回滚 is_verifying ... */ console.error("Failed send verify msg", data.description); userState.is_verifying = false; userState.verification_code=null; userState.code_expiry=null; userStateCache.set(chatId, userState); ctx.waitUntil(env.D1.prepare('UPDATE user_states SET is_verifying=FALSE, verification_code=NULL, code_expiry=NULL WHERE chat_id = ?').bind(chatId).run().catch(()=>{})); await sendMessageToUser(chatId,"发送验证消息失败"); }
        } catch (error) { /* ... 错误处理，回滚 is_verifying ... */ console.error("Error sending verify msg", error); userState.is_verifying = false; userState.verification_code=null; userState.code_expiry=null; userStateCache.set(chatId, userState); ctx.waitUntil(env.D1.prepare('UPDATE user_states SET is_verifying=FALSE, verification_code=NULL, code_expiry=NULL WHERE chat_id = ?').bind(chatId).run().catch(()=>{})); await sendMessageToUser(chatId,"网络错误发送验证消息"); }
    }
    async function checkIfAdmin(userId) { /* ... 保持原始逻辑 ... */ if (!userId) return false; try { const r = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getChatMember`, { method: 'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({chat_id:GROUP_ID,user_id:userId})}); const d=await r.json(); return d.ok && (d.result.status==='administrator'||d.result.status==='creator'); } catch (e){console.error("checkAdmin err",e); return false;} }
    async function getUserInfo(chatId) { /* ... 保持原始逻辑 ... */ let u=userInfoCache.get(chatId);if(u===undefined){try{const r=await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getChat`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({chat_id:chatId})});const d=await r.json();if(!d.ok){u={id:chatId,username:`User_${chatId}`,nickname:`User_${chatId}`;}}else{const res=d.result;const n=res.first_name?`${res.first_name}${res.last_name?` ${res.last_name}`:''}`.trim():res.username||`User_${chatId}`;u={id:res.id?.toString()||chatId,username:res.username||`User_${chatId}`,nickname:n};}}catch(e){console.error("getUserInfo err",e);u={id:chatId,username:`User_${chatId}`,nickname:`User_${chatId}`;}}userInfoCache.set(chatId,u);}return u; }
    async function getExistingTopicId(chatId) { /* ... 保持原始逻辑 ... */ let t=topicIdCache.get(chatId);if(t===undefined){try{const m=await env.D1.prepare('SELECT topic_id FROM chat_topic_mappings WHERE chat_id = ?').bind(chatId).first();t=m?.topic_id||null;if(t)topicIdCache.set(chatId,t);}catch(e){console.error("getTopicId err",e);t=null;}}return t; }
    async function createForumTopic(topicName, userName, nickname, userId) { /* ... 保持原始逻辑 ... */ try{const cr=await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/createForumTopic`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({chat_id:GROUP_ID,name:topicName.substring(0,128)})});const cd=await cr.json();if(!cd.ok)throw new Error(`Create topic fail: ${cd.description}`);const tid=cd.result.message_thread_id;const tids=tid.toString();const now=new Date();const ft=now.toISOString().replace('T',' ').substring(0,19);const nc=await getNotificationContent();const pm=`昵称: ${nickname}\n用户名: @${userName}\nUserID: ${userId}\n发起时间: ${ft}\n\n${nc}`;const mr=await sendMessageToTopic(tids,pm);if(mr.ok&&mr.result?.message_id){const mid=mr.result.message_id;ctx.waitUntil(pinMessage(tids,mid).catch(()=>{}));}return tid;}catch(e){console.error("createTopic err",e);throw e;} }
    async function saveTopicId(chatId, topicId) { /* ... 保持原始逻辑 ... */ topicIdCache.set(chatId,topicId);try{await env.D1.prepare('INSERT OR REPLACE INTO chat_topic_mappings (chat_id, topic_id) VALUES (?, ?)').bind(chatId,topicId).run();}catch(e){console.error("saveTopicId err",e);topicIdCache.set(chatId,undefined);throw e;} }
    async function getPrivateChatId(topicId) { /* ... 保持原始逻辑 ... */ for(const [cid,tid]of topicIdCache.cache)if(tid===topicId)return cid;try{const m=await env.D1.prepare('SELECT chat_id FROM chat_topic_mappings WHERE topic_id = ?').bind(topicId).first();const pid=m?.chat_id||null;if(pid)topicIdCache.set(pid,topicId);return pid;}catch(e){console.error("getPrivateChatId err",e);return null;} } // 返回 null 而不是抛出
    async function sendMessageToTopic(topicId, text, parseMode = null) { /* ... 保持原始逻辑 ... */ if(!text||text.trim()==='')return{ok:false};const MAX=4096;if(text.length>MAX)text=text.substring(0,MAX-10)+"...";try{const body={chat_id:GROUP_ID,text:text,...(topicId&&{message_thread_id:topicId}),...(parseMode&&{parse_mode:parseMode})};const r=await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});const d=await r.json();if(!d.ok)throw new Error(`Send topic fail: ${d.description}`);return d;}catch(e){console.error("sendTopic err",e);throw e;} }
    async function copyMessageToTopic(topicId, message) { /* ... 保持原始逻辑 ... */ try{const body={chat_id:GROUP_ID,from_chat_id:message.chat.id,message_id:message.message_id,message_thread_id:topicId,disable_notification:true};const r=await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/copyMessage`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});const d=await r.json();if(!d.ok)throw new Error(`Copy topic fail: ${d.description}`);}catch(e){console.error("copyTopic err",e);throw e;} }
    async function pinMessage(topicId, messageId) { /* ... 保持原始逻辑 ... */ try{const body={chat_id:GROUP_ID,message_id:messageId};const r=await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/pinChatMessage`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});const d=await r.json();if(!d.ok)throw new Error(`Pin fail: ${d.description}`);}catch(e){console.error("pinMsg err",e);throw e;} }
    async function forwardMessageToPrivateChat(privateChatId, message) { /* ... 保持原始逻辑 ... */ try{const body={chat_id:privateChatId,from_chat_id:message.chat.id,message_id:message.message_id,disable_notification:true};const r=await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/copyMessage`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});const d=await r.json();if(!d.ok)throw new Error(`Forward fail: ${d.description}`);}catch(e){console.error("forwardMsg err",e);throw e;} }
    async function sendMessageToUser(chatId, text) { /* ... 保持原始逻辑 ... */ if(!text||text.trim()==='')return;const MAX=4096;if(text.length>MAX)text=text.substring(0,MAX-10)+"...";try{const body={chat_id:chatId,text:text};const r=await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});const d=await r.json();if(!d.ok)throw new Error(`Send user fail: ${d.description}`);}catch(e){console.error("sendUser err",e);throw e;} }
    async function fetchWithRetry(url, options = {}, retries = 3, backoff = 500) { /* ... 保持原始逻辑 ... */ let lastError; for(let i=0;i<retries;i++){let tid;try{const controller=new AbortController();tid=setTimeout(()=>controller.abort(),10000);const r=await fetch(url,{...options,signal:controller.signal});clearTimeout(tid);if(r.ok)return r;if(r.status===429){const ra=parseInt(r.headers.get('Retry-After')||'5',10);const delay=Math.max(ra*1000,backoff);await new Promise(res=>setTimeout(res,delay));lastError=new Error('429');continue;}if(r.status>=500){await new Promise(res=>setTimeout(res,backoff));lastError=new Error(String(r.status));backoff*=2;continue;}return r;}catch(e){clearTimeout(tid);lastError=e;if(i===retries-1)throw lastError;await new Promise(res=>setTimeout(res,backoff));backoff*=2;}} throw lastError || new Error('Fetch retry failed');}
    async function registerWebhook(request) { /* ... 保持原始逻辑 ... */ const u=`${new URL(request.url).origin}/webhook`;const r=await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/setWebhook`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({url:u,allowed_updates:["message","callback_query"]})}).then(r=>r.json());return new Response(r.ok?'Webhook set':JSON.stringify(r,null,2));}
    async function unRegisterWebhook() { /* ... 保持原始逻辑 ... */ const r=await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/setWebhook`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({url:''})}).then(r=>r.json());return new Response(r.ok?'Webhook removed':JSON.stringify(r,null,2));}


    // --- Main Request Entry Point (保持原始逻辑) ---
    try {
      return await handleRequest(request);
    } catch (error) {
      console.error('Unhandled error in fetch handler:', error, error.stack);
      if (BOT_TOKEN && GROUP_ID) { ctx.waitUntil(sendMessageToTopic(null, `WORKER ERROR: ${error.message}`).catch(()=>{})); } // 简化错误报告
      return new Response('Internal Server Error', { status: 500 });
    }
  }
};

/** 保持原始 answerCallbackQuery */
async function answerCallbackQuery(callbackQueryId, options = {}) {
    if (!BOT_TOKEN) return;
    try {
        const response = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/answerCallbackQuery`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ callback_query_id: callbackQueryId, ...options }) });
        if (!response.ok) { const d = await response.json().catch(()=>({})); console.warn(`Non-critical: answerCb ${callbackQueryId} fail: ${d.description||response.status}`); }
    } catch (error) { console.warn(`Non-critical: answerCb ${callbackQueryId} exc:`, error); }
}
