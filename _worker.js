// 从环境变量中读取配置
let BOT_TOKEN;
let GROUP_ID;
let MAX_MESSAGES_PER_MINUTE;

// 临时管理员白名单（用于调试）
const ADMIN_WHITELIST = ['YOUR_ADMIN_USER_ID']; // 替换为你的 Telegram 用户 ID

// 全局变量，用于控制清理频率和 webhook 初始化
let lastCleanupTime = 0;
const CLEANUP_INTERVAL = 24 * 60 * 60 * 1000; // 24 小时
let isWebhookInitialized = false; // 用于标记 webhook 是否已初始化
const processedMessages = new Set(); // 用于存储已处理的消息 ID，防止重复处理

// 调试环境变量加载
export default {
  async fetch(request, env) {
    // 加载环境变量
    console.log('BOT_TOKEN_ENV:', env.BOT_TOKEN_ENV || 'undefined');
    console.log('GROUP_ID_ENV:', env.GROUP_ID_ENV || 'undefined');
    console.log('MAX_MESSAGES_PER_MINUTE_ENV:', env.MAX_MESSAGES_PER_MINUTE_ENV || 'undefined');

    if (!env.BOT_TOKEN_ENV) {
      console.error('BOT_TOKEN_ENV is not defined');
      BOT_TOKEN = null;
    } else {
      BOT_TOKEN = env.BOT_TOKEN_ENV;
    }

    if (!env.GROUP_ID_ENV) {
      console.error('GROUP_ID_ENV is not defined');
      GROUP_ID = null;
    } else {
      GROUP_ID = env.GROUP_ID_ENV;
    }

    MAX_MESSAGES_PER_MINUTE = env.MAX_MESSAGES_PER_MINUTE_ENV ? parseInt(env.MAX_MESSAGES_PER_MINUTE_ENV) : 40;

    if (!env.D1) {
      console.error('D1 database is not bound');
      return new Response('Server configuration error: D1 database is not bound', { status: 500 });
    }

    try {
      await checkAndRepairTables(env.D1);
    } catch (error) {
      console.error('Error checking and repairing tables:', error);
      return new Response('Database initialization error', { status: 500 });
    }

    if (!isWebhookInitialized && BOT_TOKEN) {
      try {
        await autoRegisterWebhook(request);
        isWebhookInitialized = true;
      } catch (error) {
        console.error('Error auto-registering webhook:', error);
      }
    }

    // 检查机器人权限
    if (BOT_TOKEN && GROUP_ID) {
      try {
        await checkBotPermissions();
      } catch (error) {
        console.error('Error checking bot permissions:', error);
      }
    }

    try {
      await cleanExpiredVerificationCodes(env.D1);
    } catch (error) {
      console.error('Error cleaning expired verification codes:', error);
    }

    async function handleRequest(request) {
      if (!BOT_TOKEN || !GROUP_ID) {
        console.error('Missing required environment variables');
        return new Response('Server configuration error: Missing required environment variables', { status: 500 });
      }

      const url = new URL(request.url);
      if (url.pathname === '/webhook') {
        try {
          const update = await request.json();
          console.log('Received Telegram update:', JSON.stringify(update, null, 2));
          await handleUpdate(update);
          return new Response('OK');
        } catch (error) {
          console.error('Error parsing request or handling update:', error);
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

    async function autoRegisterWebhook(request) {
      console.log('Attempting to auto-register webhook...');
      const webhookUrl = `${new URL(request.url).origin}/webhook`;
      try {
        const response = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/setWebhook`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ url: webhookUrl }),
        }).then(r => r.json());
        if (response.ok) {
          console.log('Webhook auto-registered successfully');
        } else {
          console.error('Webhook auto-registration failed:', JSON.stringify(response, null, 2));
        }
      } catch (error) {
        console.error('Error during webhook auto-registration:', error);
      }
    }

    async function checkBotPermissions() {
      console.log(`Checking bot permissions in group ${GROUP_ID}...`);
      try {
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

        const canSendMessages = memberData.result.can_send_messages !== false;
        const canPostMessages = memberData.result.can_post_messages !== false;
        if (!canSendMessages || !canPostMessages) {
          console.error('Bot lacks permission to send messages in the group');
        }
        console.log('Bot permissions checked successfully');
      } catch (error) {
        console.error('Error checking bot permissions:', error);
        throw error;
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
      try {
        console.log('Checking and repairing database tables...');
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
              is_first_verification: 'BOOLEAN DEFAULT FALSE',
              is_rate_limited: 'BOOLEAN DEFAULT FALSE'
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
              console.log(`Table ${tableName} not found, creating...`);
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
                console.log(`Adding missing column ${colName} to ${tableName}`);
                const columnParts = colDef.split(' ');
                const addColumnSQL = `ALTER TABLE ${tableName} ADD COLUMN ${colName} ${columnParts.slice(1).join(' ')}`;
                await d1.exec(addColumnSQL);
              }
            }

            console.log(`Table ${tableName} checked and verified`);
          } catch (error) {
            console.error(`Error checking ${tableName}:`, error);
            console.log(`Attempting to recreate ${tableName}...`);
            await d1.exec(`DROP TABLE IF EXISTS ${tableName}`);
            await createTable(d1, tableName, structure);
          }
        }

        await d1.prepare('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)')
          .bind('verification_enabled', 'true').run();
        await d1.prepare('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)')
          .bind('user_raw_enabled', 'true').run();

        console.log('Database tables check and repair completed');
      } catch (error) {
        console.error('Error in checkAndRepairTables:', error);
        throw error;
      }
    }

    async function createTable(d1, tableName, structure) {
      const columnsDef = Object.entries(structure.columns)
        .map(([name, def]) => `${name} ${def}`)
        .join(', ');
      const createSQL = `CREATE TABLE ${tableName} (${columnsDef})`;
      await d1.exec(createSQL);
      console.log(`Table ${tableName} created successfully`);
    }

    async function cleanExpiredVerificationCodes(d1) {
      const now = Date.now();
      if (now - lastCleanupTime < CLEANUP_INTERVAL) {
        return;
      }

      console.log('Running task to clean expired verification codes...');
      try {
        const nowSeconds = Math.floor(now / 1000);
        const expiredCodes = await d1.prepare(
          'SELECT chat_id FROM user_states WHERE code_expiry IS NOT NULL AND code_expiry < ?'
        ).bind(nowSeconds).all();

        if (expiredCodes.results.length > 0) {
          console.log(`Found ${expiredCodes.results.length} expired verification codes. Cleaning up...`);
          await d1.batch(
            expiredCodes.results.map(({ chat_id }) =>
              d1.prepare(
                'UPDATE user_states SET verification_code = NULL, code_expiry = NULL WHERE chat_id = ?'
              ).bind(chat_id)
            )
          );
        } else {
          console.log('No expired verification codes found.');
        }
        lastCleanupTime = now;
      } catch (error) {
        console.error('Error cleaning expired verification codes:', error);
      }
    }

    async function handleUpdate(update) {
      if (update.message) {
        const messageId = update.message.message_id.toString();
        const chatId = update.message.chat.id.toString();
        const messageKey = `${chatId}:${messageId}`;
        
        if (processedMessages.has(messageKey)) {
          console.log(`Message ${messageKey} already processed, skipping...`);
          return;
        }
        processedMessages.add(messageKey);
        
        if (processedMessages.size > 10000) {
          processedMessages.clear();
        }

        console.log(`Handling message update for chatId ${chatId}:`, JSON.stringify(update.message, null, 2));
        await onMessage(update.message);
      } else if (update.callback_query) {
        console.log(`Handling callback query update for chatId ${update.callback_query.message.chat.id}:`, JSON.stringify(update.callback_query, null, 2));
        await onCallbackQuery(update.callback_query);
      } else {
        console.log('Received unknown update type:', JSON.stringify(update, null, 2));
      }
    }

    async function onMessage(message) {
      const chatId = message.chat.id.toString();
      const text = message.text || '';
      const messageId = message.message_id;

      console.log(`Processing message from chatId ${chatId}: ${text}`);

      if (chatId === GROUP_ID) {
        const topicId = message.message_thread_id;
        if (topicId) {
          const privateChatId = await getPrivateChatId(topicId);
          if (privateChatId && text === '/admin') {
            console.log(`Admin panel requested by chatId ${chatId} in topic ${topicId}, userId: ${message.from.id}`);
            await sendAdminPanel(chatId, topicId, privateChatId, messageId);
            return;
          }
          if (privateChatId && text.startsWith('/reset_user')) {
            console.log(`Reset user command received for chatId ${chatId} in topic ${topicId}`);
            await handleResetUser(chatId, topicId, text);
            return;
          }
          if (privateChatId) {
            console.log(`Forwarding message from group to private chatId ${privateChatId}`);
            await forwardMessageToPrivateChat(privateChatId, message);
          }
        }
        return;
      }

      // 检查用户状态，如果不存在则初始化
      let userState = await env.D1.prepare('SELECT is_blocked, is_first_verification, is_verified, verified_expiry FROM user_states WHERE chat_id = ?')
        .bind(chatId)
        .first();

      if (!userState) {
        console.log(`No user state found for chatId ${chatId}, initializing with default values...`);
        await env.D1.prepare('INSERT INTO user_states (chat_id, is_blocked, is_first_verification, is_verified) VALUES (?, ?, ?, ?)')
          .bind(chatId, false, true, false)
          .run();
        userState = { is_blocked: false, is_first_verification: true, is_verified: false, verified_expiry: null };
      }

      const isBlocked = userState.is_blocked || false;
      console.log(`User ${chatId} is_blocked status: ${isBlocked}`);

      if (isBlocked) {
        console.log(`User ${chatId} is blocked, ignoring message.`);
        await sendMessageToUser(chatId, "您已被拉黑，无法发送消息。请联系管理员解除拉黑。");
        return;
      }

      if (text === '/start') {
        console.log(`Processing /start command for chatId ${chatId}`);

        if (await checkStartCommandRate(chatId)) {
          console.log(`User ${chatId} exceeded /start command rate limit, ignoring.`);
          await sendMessageToUser(chatId, "您发送 /start 命令过于频繁，请稍后再试！");
          return;
        }

        const verificationEnabled = (await getSetting('verification_enabled')) === 'true';
        const isFirstVerification = userState?.is_first_verification || true;

        if (verificationEnabled && isFirstVerification) {
          console.log(`First verification required for chatId ${chatId}, sending verification...`);
          await sendMessageToUser(chatId, "你好，欢迎使用私聊机器人，请完成验证以开始使用！");
          await handleVerification(chatId, messageId);
        } else {
          console.log(`No verification required for chatId ${chatId}, sending welcome message...`);
          const successMessage = await getVerificationSuccessMessage();
          await sendMessageToUser(chatId, `${successMessage}\n你好，欢迎使用私聊机器人，现在发送信息吧！`);
        }
        return;
      }

      // 检查验证状态
      const verificationEnabled = (await getSetting('verification_enabled')) === 'true';
      const nowSeconds = Math.floor(Date.now() / 1000);
      const isVerified = userState.is_verified && userState.verified_expiry && nowSeconds < userState.verified_expiry;

      if (verificationEnabled && !isVerified) {
        console.log(`User ${chatId} is not verified, requiring verification.`);
        await sendMessageToUser(chatId, "您尚未完成验证，请完成验证后发送消息。");
        await handleVerification(chatId, messageId);
        return;
      }

      if (verificationEnabled && await checkMessageRate(chatId)) {
        console.log(`User ${chatId} exceeded message rate limit, requiring verification.`);
        await env.D1.prepare('UPDATE user_states SET is_rate_limited = ? WHERE chat_id = ?')
          .bind(true, chatId)
          .run();
        const messageContent = text || '非文本消息';
        await sendMessageToUser(chatId, `无法转发的信息：${messageContent}\n信息过于频繁，请完成验证后发送信息`);
        await handleVerification(chatId, messageId);
        return;
      }

      // 转发消息
      try {
        console.log(`Fetching user info for chatId ${chatId}`);
        const userInfo = await getUserInfo(chatId);
        const userName = userInfo.username || userInfo.first_name;
        const nickname = `${userInfo.first_name} ${userInfo.last_name || ''}`.trim();
        const topicName = `${nickname}`;
        console.log(`User info for chatId ${chatId}: username=${userName}, nickname=${nickname}`);

        let topicId = await getExistingTopicId(chatId);
        if (!topicId || !(await checkTopicExists(topicId))) {
          console.log(`No valid topic found for chatId ${chatId}, creating new topic...`);
          topicId = await createForumTopic(topicName, userName, nickname, userInfo.id);
          await saveTopicId(chatId, topicId);
          console.log(`Created topic ${topicId} for chatId ${chatId}`);
        } else {
          console.log(`Found existing topic ${topicId} for chatId ${chatId}`);
        }

        if (text) {
          const formattedMessage = `*${nickname}:*\n------------------------------------------------\n\n${text}`;
          console.log(`Sending text message to topic ${topicId}: ${formattedMessage}`);
          await sendMessageToTopic(topicId, formattedMessage);
        } else {
          console.log(`Copying non-text message to topic ${topicId}`);
          await copyMessageToTopic(topicId, message);
        }
        console.log(`Message from chatId ${chatId} successfully forwarded to topic ${topicId}`);
      } catch (error) {
        console.error(`Error handling message from chatId ${chatId}:`, error);
        await sendMessageToTopic(null, `无法转发用户 ${chatId} 的消息：${error.message}`);
        await sendMessageToUser(chatId, "消息转发失败，请稍后再试或联系管理员。");
      }
    }

    async function checkTopicExists(topicId) {
      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: GROUP_ID,
            message_thread_id: topicId,
            text: 'Checking topic existence...'
          })
        });
        const data = await response.json();
        if (data.ok) {
          // 如果发送成功，删除测试消息
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
        console.error(`Topic ${topicId} does not exist or is inaccessible:`, error);
        return false;
      }
    }

    async function handleResetUser(chatId, topicId, text) {
      const senderId = chatId; // 假设 chatId 是发送者的 ID
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
      try {
        await env.D1.batch([
          env.D1.prepare('DELETE FROM user_states WHERE chat_id = ?').bind(targetChatId),
          env.D1.prepare('DELETE FROM message_rates WHERE chat_id = ?').bind(targetChatId)
          // 不删除 chat_topic_mappings，以保留话题
        ]);
        await sendMessageToTopic(topicId, `用户 ${targetChatId} 的状态已重置。`);
      } catch (error) {
        console.error(`Error resetting user ${targetChatId}:`, error);
        await sendMessageToTopic(topicId, `重置用户 ${targetChatId} 失败：${error.message}`);
      }
    }

    async function sendAdminPanel(chatId, topicId, privateChatId, messageId) {
      try {
        const [verificationEnabled, userRawEnabled] = await Promise.all([
          getSetting('verification_enabled') === 'true',
          getSetting('user_raw_enabled') === 'true'
        ]);
        console.log(`Sending admin panel - verificationEnabled: ${verificationEnabled}, userRawEnabled: ${userRawEnabled}`);

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
            { text: 'GitHub项目', callback_data: `github_${privateChatId}` }
          ],
          [
            { text: '删除用户', callback_data: `delete_user_${privateChatId}` }
          ]
        ];

        const adminMessage = '管理员面板：请选择操作';
        const apiCalls = [
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
        ];

        await Promise.all(apiCalls);
        console.log(`Admin panel sent successfully to chatId ${chatId}, topicId ${topicId}`);
      } catch (error) {
        console.error(`Error sending admin panel to chatId ${chatId}, topicId ${topicId}:`, error);
      }
    }

    async function getVerificationSuccessMessage() {
      const userRawEnabled = (await getSetting('user_raw_enabled')) === 'true';
      if (!userRawEnabled) return '验证成功！您现在可以与客服聊天。';

      try {
        const response = await fetch('https://raw.githubusercontent.com/iawooo/ctt/refs/heads/main/CFTeleTrans/start.md');
        if (!response.ok) throw new Error(`Failed to fetch start.md: ${response.statusText}`);
        const message = await response.text();
        return message.trim() || '验证成功！您现在可以与客服聊天。';
      } catch (error) {
        console.error("Error fetching verification success message:", error);
        return '验证成功！您现在可以与客服聊天。';
      }
    }

    async function getNotificationContent() {
      try {
        const response = await fetch('https://raw.githubusercontent.com/iawooo/ctt/refs/heads/main/CFTeleTrans/notification.md');
        if (!response.ok) throw new Error(`Failed to fetch notification.md: ${response.statusText}`);
        const content = await response.text();
        return content.trim() || '';
      } catch (error) {
        console.error("Error fetching notification content:", error);
        return '';
      }
    }

    async function checkStartCommandRate(chatId) {
      const now = Date.now();
      const window = 5 * 60 * 1000;
      const maxStartsPerWindow = 1;

      const rateData = await env.D1.prepare('SELECT start_count, start_window_start FROM message_rates WHERE chat_id = ?')
        .bind(chatId)
        .first();
      let data = rateData ? { count: rateData.start_count, start: rateData.start_window_start } : { count: 0, start: now };

      if (now - data.start > window) {
        data.count = 1;
        data.start = now;
      } else {
        data.count += 1;
      }

      await env.D1.prepare('INSERT OR REPLACE INTO message_rates (chat_id, start_count, start_window_start) VALUES (?, ?, ?)')
        .bind(chatId, data.count, data.start)
        .run();

      return data.count > maxStartsPerWindow;
    }

    async function checkMessageRate(chatId) {
      const now = Date.now();
      const window = 60 * 1000;

      const rateData = await env.D1.prepare('SELECT message_count, window_start FROM message_rates WHERE chat_id = ?')
        .bind(chatId)
        .first();
      let data = rateData ? { count: rateData.message_count, start: rateData.window_start } : { count: 0, start: now };

      if (now - data.start > window) {
        data.count = 1;
        data.start = now;
      } else {
        data.count += 1;
      }

      await env.D1.prepare('INSERT OR REPLACE INTO message_rates (chat_id, message_count, window_start) VALUES (?, ?, ?)')
        .bind(chatId, data.count, data.start)
        .run();

      return data.count > MAX_MESSAGES_PER_MINUTE;
    }

    async function getSetting(key) {
      try {
        const result = await env.D1.prepare('SELECT value FROM settings WHERE key = ?')
          .bind(key)
          .first();
        return result?.value || null;
      } catch (error) {
        console.error(`Error getting setting ${key}:`, error);
        throw error;
      }
    }

    async function setSetting(key, value) {
      try {
        await env.D1.prepare('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)')
          .bind(key, value)
          .run();
        console.log(`Setting ${key} updated to ${value}`);
      } catch (error) {
        console.error(`Error setting ${key} to ${value}:`, error);
        throw error;
      }
    }

    async function onCallbackQuery(callbackQuery) {
      const chatId = callbackQuery.message.chat.id.toString();
      const topicId = callbackQuery.message.message_thread_id;
      const data = callbackQuery.data;
      const messageId = callbackQuery.message.message_id;

      console.log(`Processing callback query from chatId ${chatId}, topicId ${topicId}: ${data}`);

      // 提取 action 和 privateChatId
      // 修改解析逻辑，确保正确提取完整的 action
      const parts = data.split('_');
      let action;
      let privateChatId;

      if (data.startsWith('verify_')) {
        action = 'verify';
        privateChatId = parts[1]; // verify_<chatId>_<option>_<result>
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
      } else if (data.startsWith('github_')) {
        action = 'github';
        privateChatId = parts.slice(1).join('_');
      } else if (data.startsWith('delete_user_')) {
        action = 'delete_user';
        privateChatId = parts.slice(2).join('_');
      } else {
        action = data;
        privateChatId = '';
      }

      console.log(`Parsed action: ${action}, privateChatId: ${privateChatId}`);

      try {
        if (action === 'verify') {
          const [, userChatId, selectedAnswer, result] = data.split('_');
          if (userChatId !== chatId) {
            console.log(`ChatId mismatch: expected ${userChatId}, got ${chatId}`);
            return;
          }

          const verificationState = await env.D1.prepare('SELECT verification_code, code_expiry FROM user_states WHERE chat_id = ?')
            .bind(chatId)
            .first();
          const storedCode = verificationState?.verification_code;
          const codeExpiry = verificationState?.code_expiry;
          const nowSeconds = Math.floor(Date.now() / 1000);

          if (!storedCode || (codeExpiry && nowSeconds > codeExpiry)) {
            await sendMessageToUser(chatId, '验证码已过期，请重新发送消息以获取新验证码。');
            return;
          }

          if (result === 'correct') {
            const verifiedExpiry = nowSeconds + 3600;
            await env.D1.prepare('UPDATE user_states SET is_verified = ?, verified_expiry = ?, verification_code = NULL, code_expiry = NULL, last_verification_message_id = NULL WHERE chat_id = ?')
              .bind(true, verifiedExpiry, chatId)
              .run();

            const userState = await env.D1.prepare('SELECT is_first_verification, is_rate_limited FROM user_states WHERE chat_id = ?')
              .bind(chatId)
              .first();
            const isFirstVerification = userState?.is_first_verification || false;
            const isRateLimited = userState?.is_rate_limited || false;

            const successMessage = await getVerificationSuccessMessage();
            await sendMessageToUser(chatId, `${successMessage}\n你好，欢迎使用私聊机器人！现在可以发送消息了。`);

            if (isFirstVerification) {
              await env.D1.prepare('UPDATE user_states SET is_first_verification = ? WHERE chat_id = ?')
                .bind(false, chatId)
                .run();
            }

            if (isRateLimited) {
              await env.D1.prepare('UPDATE user_states SET is_rate_limited = ? WHERE chat_id = ?')
                .bind(false, chatId)
                .run();
            }
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
            console.log(`User ${senderId} is not an admin, ignoring callback.`);
            await sendMessageToTopic(topicId, '只有管理员可以使用此功能。');
            await sendAdminPanel(chatId, topicId, privateChatId, messageId);
            return;
          }

          console.log(`Processing admin action: ${action} for privateChatId ${privateChatId}`);

          // 检查话题是否有效，如果无效则重新创建
          if (!(await checkTopicExists(topicId))) {
            console.log(`Topic ${topicId} is invalid, attempting to recreate for privateChatId ${privateChatId}`);
            const userInfo = await getUserInfo(privateChatId);
            const userName = userInfo.username || userInfo.first_name;
            const nickname = `${userInfo.first_name} ${userInfo.last_name || ''}`.trim();
            const topicName = `${nickname}`;
            topicId = await createForumTopic(topicName, userName, nickname, privateChatId);
            await saveTopicId(privateChatId, topicId);
            console.log(`Recreated topic ${topicId} for privateChatId ${privateChatId}`);
          }

          if (action === 'block') {
            console.log(`Blocking user ${privateChatId}`);
            await env.D1.prepare('INSERT OR REPLACE INTO user_states (chat_id, is_blocked) VALUES (?, ?)')
              .bind(privateChatId, true)
              .run();
            const updatedState = await env.D1.prepare('SELECT is_blocked FROM user_states WHERE chat_id = ?')
              .bind(privateChatId)
              .first();
            console.log(`After blocking, user ${privateChatId} is_blocked status: ${updatedState?.is_blocked}`);
            await sendMessageToTopic(topicId, `用户 ${privateChatId} 已被拉黑，消息将不再转发。`);
          } else if (action === 'unblock') {
            console.log(`Unblocking user ${privateChatId}`);
            await env.D1.prepare('INSERT OR REPLACE INTO user_states (chat_id, is_blocked, is_first_verification) VALUES (?, ?, ?)')
              .bind(privateChatId, false, true)
              .run();
            const updatedState = await env.D1.prepare('SELECT is_blocked FROM user_states WHERE chat_id = ?')
              .bind(privateChatId)
              .first();
            console.log(`After unblocking, user ${privateChatId} is_blocked status: ${updatedState?.is_blocked}`);
            await sendMessageToTopic(topicId, `用户 ${privateChatId} 已解除拉黑，消息将继续转发。`);
          } else if (action === 'toggle_verification') {
            console.log(`Toggling verification for privateChatId ${privateChatId}`);
            const currentState = (await getSetting('verification_enabled')) === 'true';
            const newState = !currentState;
            await setSetting('verification_enabled', newState.toString());
            await sendMessageToTopic(topicId, `验证码功能已${newState ? '开启' : '关闭'}。`);
          } else if (action === 'check_blocklist') {
            console.log(`Checking blocklist for privateChatId ${privateChatId}`);
            const blockedUsers = await env.D1.prepare('SELECT chat_id FROM user_states WHERE is_blocked = ?')
              .bind(true)
              .all();
            console.log(`Blocked users query result: ${JSON.stringify(blockedUsers.results)}`);
            const blockList = blockedUsers.results.length > 0 
              ? blockedUsers.results.map(row => row.chat_id).join('\n')
              : '当前没有被拉黑的用户。';
            await sendMessageToTopic(topicId, `黑名单列表：\n${blockList}`);
          } else if (action === 'toggle_user_raw') {
            console.log(`Toggling user raw for privateChatId ${privateChatId}`);
            const currentState = (await getSetting('user_raw_enabled')) === 'true';
            const newState = !currentState;
            await setSetting('user_raw_enabled', newState.toString());
            await sendMessageToTopic(topicId, `用户端 Raw 链接已${newState ? '开启' : '关闭'}。`);
          } else if (action === 'github') {
            console.log(`Sending GitHub link for privateChatId ${privateChatId}`);
            await sendMessageToTopic(topicId, '访问我的 GitHub 项目：https://github.com/YOUR_GITHUB_USERNAME/YOUR_REPO');
          } else if (action === 'delete_user') {
            console.log(`Deleting user ${privateChatId}`);
            try {
              await env.D1.batch([
                env.D1.prepare('DELETE FROM user_states WHERE chat_id = ?').bind(privateChatId),
                env.D1.prepare('DELETE FROM message_rates WHERE chat_id = ?').bind(privateChatId)
                // 不删除 chat_topic_mappings，以保留话题
              ]);
              await sendMessageToTopic(topicId, `用户 ${privateChatId} 的状态和消息记录已删除，话题保留。`);
            } catch (error) {
              console.error(`Error deleting user ${privateChatId}:`, error);
              await sendMessageToTopic(topicId, `删除用户 ${privateChatId} 失败：${error.message}`);
            }
          } else {
            console.log(`Unknown action: ${action}`);
            await sendMessageToTopic(topicId, `未知操作：${action}`);
          }

          // 重新获取最新状态并更新管理员面板
          const [verificationEnabled, userRawEnabled] = await Promise.all([
            getSetting('verification_enabled') === 'true',
            getSetting('user_raw_enabled') === 'true'
          ]);
          console.log(`Updated states - verificationEnabled: ${verificationEnabled}, userRawEnabled: ${userRawEnabled}`);
          await sendAdminPanel(chatId, topicId, privateChatId, messageId);
        }

        // 响应回调查询
        await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/answerCallbackQuery`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            callback_query_id: callbackQuery.id
          })
        });
        console.log(`Callback query ${callbackQuery.id} processed successfully`);
      } catch (error) {
        console.error(`Error processing callback query ${data}:`, error);
        await sendMessageToTopic(topicId, `处理操作 ${action} 失败：${error.message}`);
      }
    }

    async function handleVerification(chatId, messageId) {
      await env.D1.prepare('UPDATE user_states SET verification_code = NULL, code_expiry = NULL WHERE chat_id = ?')
        .bind(chatId)
        .run();

      const lastVerification = await env.D1.prepare('SELECT last_verification_message_id FROM user_states WHERE chat_id = ?')
        .bind(chatId)
        .first();
      const lastVerificationMessageId = lastVerification?.last_verification_message_id;

      if (lastVerificationMessageId) {
        try {
          await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              chat_id: chatId,
              message_id: lastVerificationMessageId
            })
          });
        } catch (error) {
          console.error("Error deleting old verification message:", error);
        }
        await env.D1.prepare('UPDATE user_states SET last_verification_message_id = NULL WHERE chat_id = ?')
          .bind(chatId)
          .run();
      }

      await sendVerification(chatId);
    }

    async function sendVerification(chatId) {
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

      const question = `请计算：${num1} ${operation} ${num2} = ?（点击下方按钮完成验证）`;
      const nowSeconds = Math.floor(Date.now() / 1000);
      const codeExpiry = nowSeconds + 300;

      await env.D1.prepare('INSERT OR REPLACE INTO user_states (chat_id, verification_code, code_expiry) VALUES (?, ?, ?)')
        .bind(chatId, correctResult.toString(), codeExpiry)
        .run();

      try {
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
          await env.D1.prepare('UPDATE user_states SET last_verification_message_id = ? WHERE chat_id = ?')
            .bind(data.result.message_id.toString(), chatId)
            .run();
        } else {
          console.error(`Failed to send verification message: ${data.description}`);
        }
      } catch (error) {
        console.error("Error sending verification message:", error);
      }
    }

    async function checkIfAdmin(userId) {
      if (ADMIN_WHITELIST.includes(userId)) return true;

      try {
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
      } catch (error) {
        console.error(`Error checking admin status for user ${userId}:`, error);
        return false;
      }
    }

    async function getUserInfo(chatId) {
      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getChat`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ chat_id: chatId })
        });
        const data = await response.json();
        if (!data.ok) throw new Error(`Failed to get user info: ${data.description}`);
        return data.result;
      } catch (error) {
        console.error(`Error fetching user info for chatId ${chatId}:`, error);
        throw error;
      }
    }

    async function getExistingTopicId(chatId) {
      try {
        const mapping = await env.D1.prepare('SELECT topic_id FROM chat_topic_mappings WHERE chat_id = ?')
          .bind(chatId)
          .first();
        return mapping?.topic_id || null;
      } catch (error) {
        console.error(`Error fetching topic ID for chatId ${chatId}:`, error);
        throw error;
      }
    }

    async function createForumTopic(topicName, userName, nickname, userId) {
      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/createForumTopic`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ chat_id: GROUP_ID, name: topicName })
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
      } catch (error) {
        console.error(`Error creating forum topic for user ${userId}:`, error);
        throw error;
      }
    }

    async function saveTopicId(chatId, topicId) {
      try {
        await env.D1.prepare('INSERT OR REPLACE INTO chat_topic_mappings (chat_id, topic_id) VALUES (?, ?)')
          .bind(chatId, topicId)
          .run();
      } catch (error) {
        console.error(`Error saving topic ID ${topicId} for chatId ${chatId}:`, error);
        throw error;
      }
    }

    async function getPrivateChatId(topicId) {
      try {
        const mapping = await env.D1.prepare('SELECT chat_id FROM chat_topic_mappings WHERE topic_id = ?')
          .bind(topicId)
          .first();
        return mapping?.chat_id || null;
      } catch (error) {
        console.error(`Error fetching private chat ID for topicId ${topicId}:`, error);
        throw error;
      }
    }

    async function sendMessageToTopic(topicId, text) {
      if (!text.trim()) {
        console.error(`Failed to send message to topic: message text is empty`);
        throw new Error('Message text is empty');
      }

      try {
        const requestBody = {
          chat_id: GROUP_ID,
          text: text,
          message_thread_id: topicId,
          parse_mode: 'Markdown'
        };
        console.log(`Sending message to topic ${topicId} with body:`, JSON.stringify(requestBody));
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(requestBody)
        });
        const data = await response.json();
        if (!data.ok) {
          console.error(`Failed to send message to topic ${topicId}: ${data.description}`);
          throw new Error(`Failed to send message to topic: ${data.description}`);
        }
        return data;
      } catch (error) {
        console.error(`Error sending message to topic ${topicId}:`, error);
        throw error;
      }
    }

    async function copyMessageToTopic(topicId, message) {
      try {
        const requestBody = {
          chat_id: GROUP_ID,
          from_chat_id: message.chat.id,
          message_id: message.message_id,
          message_thread_id: topicId
        };
        console.log(`Copying message to topic ${topicId} with body:`, JSON.stringify(requestBody));
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/copyMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(requestBody)
        });
        const data = await response.json();
        if (!data.ok) {
          console.error(`Failed to copy message to topic ${topicId}: ${data.description}`);
          throw new Error(`Failed to copy message to topic: ${data.description}`);
        }
      } catch (error) {
        console.error(`Error copying message to topic ${topicId}:`, error);
        throw error;
      }
    }

    async function pinMessage(topicId, messageId) {
      try {
        const requestBody = {
          chat_id: GROUP_ID,
          message_id: messageId,
          message_thread_id: topicId
        };
        console.log(`Pinning message ${messageId} in topic ${topicId} with body:`, JSON.stringify(requestBody));
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/pinChatMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(requestBody)
        });
        const data = await response.json();
        if (!data.ok) {
          console.error(`Failed to pin message ${messageId} in topic ${topicId}: ${data.description}`);
          throw new Error(`Failed to pin message: ${data.description}`);
        }
      } catch (error) {
        console.error(`Error pinning message ${messageId} in topic ${topicId}:`, error);
        throw error;
      }
    }

    async function forwardMessageToPrivateChat(privateChatId, message) {
      try {
        const requestBody = {
          chat_id: privateChatId,
          from_chat_id: message.chat.id,
          message_id: message.message_id
        };
        console.log(`Forwarding message to private chat ${privateChatId} with body:`, JSON.stringify(requestBody));
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/copyMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(requestBody)
        });
        const data = await response.json();
        if (!data.ok) {
          console.error(`Failed to forward message to private chat ${privateChatId}: ${data.description}`);
          throw new Error(`Failed to forward message to private chat: ${data.description}`);
        }
      } catch (error) {
        console.error(`Error forwarding message to private chat ${privateChatId}:`, error);
        throw error;
      }
    }

    async function sendMessageToUser(chatId, text) {
      try {
        const requestBody = { chat_id: chatId, text: text };
        console.log(`Sending message to user ${chatId} with body:`, JSON.stringify(requestBody));
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(requestBody)
        });
        const data = await response.json();
        if (!data.ok) {
          console.error(`Failed to send message to user ${chatId}: ${data.description}`);
          throw new Error(`Failed to send message to user: ${data.description}`);
        }
      } catch (error) {
        console.error(`Error sending message to user ${chatId}:`, error);
        throw error;
      }
    }

    async function fetchWithRetry(url, options, retries = 5, backoff = 2000) {
      for (let i = 0; i < retries; i++) {
        try {
          const response = await fetch(url, options);
          if (response.ok) return response;
          if (response.status === 429) {
            const retryAfter = response.headers.get('Retry-After');
            const delay = retryAfter ? parseInt(retryAfter) * 1000 : backoff * Math.pow(2, i);
            console.log(`Rate limited, retrying after ${delay}ms (attempt ${i + 1}/${retries})`);
            await new Promise(resolve => setTimeout(resolve, delay));
          } else {
            throw new Error(`Request failed with status ${response.status}: ${response.statusText}`);
          }
        } catch (error) {
          if (i === retries - 1) throw error;
          await new Promise(resolve => setTimeout(resolve, backoff * Math.pow(2, i)));
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

    try {
      return await handleRequest(request);
    } catch (error) {
      console.error('Unhandled error in fetch handler:', error);
      return new Response('Internal Server Error', { status: 500 });
    }
  }
};
