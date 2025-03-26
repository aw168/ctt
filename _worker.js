// 从环境变量中读取配置
let BOT_TOKEN;
let GROUP_ID;
let MAX_MESSAGES_PER_MINUTE;

// 全局变量，用于控制清理频率和 webhook 初始化
let lastCleanupTime = 0;
const CLEANUP_INTERVAL = 24 * 60 * 60 * 1000; // 24 小时
let isWebhookInitialized = false; // 用于标记 webhook 是否已初始化

// 调试环境变量加载
export default {
  async fetch(request, env) {
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

    // 检查 D1 绑定
    if (!env.D1) {
      console.error('D1 database is not bound');
      return new Response('Server configuration error: D1 database is not bound', { status: 500 });
    }

    // 在每次部署时自动检查和修复数据库表
    await checkAndRepairTables(env.D1);

    // 自动注册 webhook（仅在首次启动时执行）
    if (!isWebhookInitialized && BOT_TOKEN) {
      await autoRegisterWebhook(request);
      isWebhookInitialized = true; // 标记为已初始化，避免重复注册
    }

    // 清理过期的验证码缓存（基于时间间隔）
    await cleanExpiredVerificationCodes(env.D1);

    // 主处理函数
    async function handleRequest(request) {
      if (!BOT_TOKEN || !GROUP_ID) {
        console.error('Missing required environment variables');
        return new Response('Server configuration error: Missing required environment variables', { status: 500 });
      }

      const url = new URL(request.url);
      if (url.pathname === '/webhook') {
        try {
          const update = await request.json();
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
          settings: { // 新增设置表
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

        // 初始化默认设置
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
          for (const { chat_id } of expiredCodes.results) {
            await d1.prepare(
              'UPDATE user_states SET verification_code = NULL, code_expiry = NULL WHERE chat_id = ?'
            ).bind(chat_id).run();
            console.log(`Cleaned expired verification code for chat_id: ${chat_id}`);
          }
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
          if (privateChatId) {
            await forwardMessageToPrivateChat(privateChatId, message);
          }
        }
        return;
      }

      const userState = await env.D1.prepare('SELECT is_blocked, is_first_verification FROM user_states WHERE chat_id = ?')
        .bind(chatId)
        .first();
      const isBlocked = userState ? userState.is_blocked : false;
      if (isBlocked) {
        console.log(`User ${chatId} is blocked, ignoring message.`);
        return;
      }

      const isFirstVerification = userState ? userState.is_first_verification : true;
      const verificationEnabled = await getSetting('verification_enabled') === 'true';

      if (text === '/start') {
        console.log(`Received /start command from ${chatId}, processing without forwarding...`);
        if (await checkStartCommandRate(chatId)) {
          console.log(`User ${chatId} exceeded /start command rate limit, ignoring.`);
          await sendMessageToUser(chatId, "您发送 /start 命令过于频繁，请稍后再试！");
          return;
        }

        if (!userState) {
          await env.D1.prepare('INSERT INTO user_states (chat_id, is_first_verification) VALUES (?, ?)')
            .bind(chatId, true)
            .run();
        }

        if (verificationEnabled && isFirstVerification) {
          await sendMessageToUser(chatId, "你好，欢迎使用私聊机器人，请完成验证以开始使用！");
          await handleVerification(chatId, messageId);
        } else {
          const successMessage = await getVerificationSuccessMessage();
          await sendMessageToUser(chatId, `${successMessage}\n你好，欢迎使用私聊机器人，现在发送信息吧！`);
        }
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

      try {
        const userInfo = await getUserInfo(chatId);
        const userName = userInfo.username || userInfo.first_name;
        const nickname = `${userInfo.first_name} ${userInfo.last_name || ''}`.trim();
        const topicName = `${nickname}`;

        let topicId = await getExistingTopicId(chatId);
        if (!topicId) {
          topicId = await createForumTopic(topicName, userName, nickname, userInfo.id);
          await saveTopicId(chatId, topicId);
        }

        if (text) {
          const formattedMessage = `*${nickname}:*\n------------------------------------------------\n\n${text}`;
          await sendMessageToTopic(topicId, formattedMessage);
        } else {
          await copyMessageToTopic(topicId, message);
        }
      } catch (error) {
        console.error(`Error handling message from chatId ${chatId}:`, error);
      }
    }

    async function sendAdminPanel(chatId, topicId, privateChatId, messageId) {
      const verificationEnabled = await getSetting('verification_enabled') === 'true';
      const userRawEnabled = await getSetting('user_raw_enabled') === 'true';

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
        ]
      ];

      const adminMessage = await getAdminRawContent();
      try {
        await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: chatId,
            message_thread_id: topicId,
            text: adminMessage || '管理员面板：请选择操作',
            reply_markup: {
              inline_keyboard: buttons
            }
          })
        });
        await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: chatId,
            message_id: messageId
          })
        });
      } catch (error) {
        console.error('Error sending admin panel:', error);
      }
    }

    async function getAdminRawContent() {
      try {
        const response = await fetch('https://raw.githubusercontent.com/YOUR_GITHUB_USERNAME/YOUR_REPO/main/admin_panel.md');
        if (!response.ok) throw new Error(`Failed to fetch admin_panel.md: ${response.statusText}`);
        const content = await response.text();
        return content.trim() || '管理员面板：请选择操作';
      } catch (error) {
        console.error('Error fetching admin raw content:', error);
        return '管理员面板：请选择操作';
      }
    }

    async function getVerificationSuccessMessage() {
      const userRawEnabled = await getSetting('user_raw_enabled') === 'true';
      if (!userRawEnabled) return '验证成功！您现在可以与客服聊天。';

      try {
        const response = await fetch('https://raw.githubusercontent.com/iawooo/ctt/refs/heads/main/CFTeleTrans/start.md');
        if (!response.ok) throw new Error(`Failed to fetch start.md: ${response.statusText}`);
        const message = await response.text();
        const trimmedMessage = message.trim();
        return trimmedMessage || '验证成功！您现在可以与客服聊天。';
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
      const window = 5 * 60 * 1000; // 5 分钟窗口
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

      await env.D1.prepare('UPDATE message_rates SET start_count = ?, start_window_start = ? WHERE chat_id = ?')
        .bind(data.count, data.start, chatId)
        .run();

      console.log(`User ${chatId} /start command count: ${data.count}/${maxStartsPerWindow} in last 5 minutes`);
      return data.count > maxStartsPerWindow;
    }

    async function checkMessageRate(chatId) {
      const now = Date.now();
      const window = 60 * 1000; // 1 分钟窗口

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

      await env.D1.prepare('UPDATE message_rates SET message_count = ?, window_start = ? WHERE chat_id = ?')
        .bind(data.count, data.start, chatId)
        .run();

      console.log(`User ${chatId} message count: ${data.count}/${MAX_MESSAGES_PER_MINUTE} in last minute`);
      return data.count > MAX_MESSAGES_PER_MINUTE;
    }

    async function getSetting(key) {
      const result = await env.D1.prepare('SELECT value FROM settings WHERE key = ?')
        .bind(key)
        .first();
      return result ? result.value : null;
    }

    async function setSetting(key, value) {
      await env.D1.prepare('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)')
        .bind(key, value)
        .run();
    }

    async function onCallbackQuery(callbackQuery) {
      const chatId = callbackQuery.message.chat.id.toString();
      const topicId = callbackQuery.message.message_thread_id;
      const data = callbackQuery.data;
      const messageId = callbackQuery.message.message_id;

      if (data.startsWith('verify_')) {
        const [, userChatId, selectedAnswer, result] = data.split('_');
        if (userChatId !== chatId) return;

        const verificationState = await env.D1.prepare('SELECT verification_code, code_expiry FROM user_states WHERE chat_id = ?')
          .bind(chatId)
          .first();
        const storedCode = verificationState ? verificationState.verification_code : null;
        const codeExpiry = verificationState ? verificationState.code_expiry : null;
        const nowSeconds = Math.floor(Date.now() / 1000);
        if (!storedCode || (codeExpiry && nowSeconds > codeExpiry)) {
          await sendMessageToUser(chatId, '验证码已过期，请重新发送消息以获取新验证码。');
          return;
        }

        if (result === 'correct') {
          const verifiedExpiry = nowSeconds + 3600; // 1 小时有效期，仅用于记录
          await env.D1.prepare('UPDATE user_states SET is_verified = ?, verified_expiry = ?, verification_code = NULL, code_expiry = NULL, last_verification_message_id = NULL WHERE chat_id = ?')
            .bind(true, verifiedExpiry, chatId)
            .run();

          const userState = await env.D1.prepare('SELECT is_first_verification, is_rate_limited FROM user_states WHERE chat_id = ?')
            .bind(chatId)
            .first();
          const isFirstVerification = userState ? userState.is_first_verification : false;
          const isRateLimited = userState ? userState.is_rate_limited : false;

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
      } else if (data.startsWith('block_') || data.startsWith('unblock_') || data.startsWith('toggle_verification_') || 
                 data.startsWith('check_blocklist_') || data.startsWith('toggle_user_raw_') || data.startsWith('github_')) {
        const senderId = callbackQuery.from.id.toString();
        const isAdmin = await checkIfAdmin(senderId);
        if (!isAdmin) {
          await sendMessageToTopic(topicId, '只有管理员可以使用此功能。');
          return;
        }

        const privateChatId = data.split('_')[1];
        if (data.startsWith('block_')) {
          await env.D1.prepare('INSERT OR REPLACE INTO user_states (chat_id, is_blocked) VALUES (?, ?)')
            .bind(privateChatId, true)
            .run();
          await sendMessageToTopic(topicId, `用户 ${privateChatId} 已被拉黑，消息将不再转发。`);
        } else if (data.startsWith('unblock_')) {
          await env.D1.prepare('UPDATE user_states SET is_blocked = ? WHERE chat_id = ?')
            .bind(false, privateChatId)
            .run();
          await sendMessageToTopic(topicId, `用户 ${privateChatId} 已解除拉黑，消息将继续转发。`);
        } else if (data.startsWith('toggle_verification_')) {
          const currentState = await getSetting('verification_enabled') === 'true';
          const newState = !currentState;
          await setSetting('verification_enabled', newState.toString());
          await sendMessageToTopic(topicId, `验证码功能已${newState ? '开启' : '关闭'}。`);
        } else if (data.startsWith('check_blocklist_')) {
          const blockedUsers = await env.D1.prepare('SELECT chat_id FROM user_states WHERE is_blocked = ?')
            .bind(true)
            .all();
          const blockList = blockedUsers.results.length > 0 
            ? blockedUsers.results.map(row => row.chat_id).join('\n')
            : '当前没有被拉黑的用户。';
          await sendMessageToTopic(topicId, `黑名单列表：\n${blockList}`);
        } else if (data.startsWith('toggle_user_raw_')) {
          const currentState = await getSetting('user_raw_enabled') === 'true';
          const newState = !currentState;
          await setSetting('user_raw_enabled', newState.toString());
          await sendMessageToTopic(topicId, `用户端 Raw 链接已${newState ? '开启' : '关闭'}。`);
        } else if (data.startsWith('github_')) {
          await sendMessageToTopic(topicId, '访问我的 GitHub 项目：https://github.com/YOUR_GITHUB_USERNAME/YOUR_REPO');
        }

        await sendAdminPanel(chatId, topicId, privateChatId, messageId); // 刷新面板
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
      await env.D1.prepare('UPDATE user_states SET verification_code = NULL, code_expiry = NULL WHERE chat_id = ?')
        .bind(chatId)
        .run();

      const lastVerification = await env.D1.prepare('SELECT last_verification_message_id FROM user_states WHERE chat_id = ?')
        .bind(chatId)
        .first();
      const lastVerificationMessageId = lastVerification ? lastVerification.last_verification_message_id : null;
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

      const options = new Set();
      options.add(correctResult);
      while (options.size < 4) {
        const wrongResult = correctResult + Math.floor(Math.random() * 5) - 2;
        if (wrongResult !== correctResult) {
          options.add(wrongResult);
        }
      }
      const optionArray = Array.from(options).sort(() => Math.random() - 0.5);

      const buttons = optionArray.map((option) => ({
        text: `(${option})`,
        callback_data: `verify_${chatId}_${option}_${option === correctResult ? 'correct' : 'wrong'}`
      }));

      const question = `请计算：${num1} ${operation} ${num2} = ?（点击下方按钮完成验证）`;
      const nowSeconds = Math.floor(Date.now() / 1000);
      const codeExpiry = nowSeconds + 300; // 5 分钟有效期
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
            reply_markup: {
              inline_keyboard: [buttons]
            }
          })
        });
        const data = await response.json();
        if (!data.ok) {
          console.error(`Failed to send verification message: ${data.description}`);
          return;
        }
        await env.D1.prepare('UPDATE user_states SET last_verification_message_id = ? WHERE chat_id = ?')
          .bind(data.result.message_id.toString(), chatId)
          .run();
      } catch (error) {
        console.error("Error sending verification message:", error);
      }
    }

    async function checkIfAdmin(userId) {
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
        if (!data.ok) {
          console.error(`Failed to check admin status: ${data.description}`);
          return false;
        }
        const status = data.result.status;
        return status === 'administrator' || status === 'creator';
      } catch (error) {
        console.error("Error checking admin status:", error);
        return false;
      }
    }

    async function getUserInfo(chatId) {
      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getChat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: chatId })
      });
      const data = await response.json();
      if (!data.ok) {
        throw new Error(`Failed to get user info: ${data.description}`);
      }
      return data.result;
    }

    async function getExistingTopicId(chatId) {
      const mapping = await env.D1.prepare('SELECT topic_id FROM chat_topic_mappings WHERE chat_id = ?')
        .bind(chatId)
        .first();
      return mapping ? mapping.topic_id : null;
    }

    async function createForumTopic(topicName, userName, nickname, userId) {
      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/createForumTopic`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: GROUP_ID, name: topicName })
      });
      const data = await response.json();
      if (!data.ok) {
        throw new Error(`Failed to create forum topic: ${data.description}`);
      }
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
      await env.D1.prepare('INSERT OR REPLACE INTO chat_topic_mappings (chat_id, topic_id) VALUES (?, ?)')
        .bind(chatId, topicId)
        .run();
    }

    async function getPrivateChatId(topicId) {
      const mapping = await env.D1.prepare('SELECT chat_id FROM chat_topic_mappings WHERE topic_id = ?')
        .bind(topicId)
        .first();
      return mapping ? mapping.chat_id : null;
    }

    async function sendMessageToTopic(topicId, text) {
      console.log("Sending message to topic:", topicId, text);
      if (!text.trim()) {
        console.error(`Failed to send message to topic: message text is empty`);
        return;
      }

      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: GROUP_ID,
            text: text,
            message_thread_id: topicId,
            parse_mode: 'Markdown'
          })
        });
        const data = await response.json();
        if (!data.ok) {
          console.error(`Failed to send message to topic: ${data.description}`);
        }
        return data;
      } catch (error) {
        console.error("Error sending message to topic:", error);
      }
    }

    async function copyMessageToTopic(topicId, message) {
      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/copyMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: GROUP_ID,
            from_chat_id: message.chat.id,
            message_id: message.message_id,
            message_thread_id: topicId
          })
        });
        const data = await response.json();
        if (!data.ok) {
          console.error(`Failed to copy message to topic: ${data.description}`);
        }
      } catch (error) {
        console.error("Error copying message to topic:", error);
      }
    }

    async function pinMessage(topicId, messageId) {
      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/pinChatMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: GROUP_ID,
            message_id: messageId,
            message_thread_id: topicId
          })
        });
        const data = await response.json();
        if (!data.ok) {
          console.error(`Failed to pin message: ${data.description}`);
        }
      } catch (error) {
        console.error("Error pinning message:", error);
      }
    }

    async function forwardMessageToPrivateChat(privateChatId, message) {
      console.log("Forwarding message to private chat:", privateChatId, message);
      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/copyMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: privateChatId,
            from_chat_id: message.chat.id,
            message_id: message.message_id
          })
        });
        const data = await response.json();
        if (!data.ok) {
          console.error(`Failed to forward message to private chat: ${data.description}`);
        }
      } catch (error) {
        console.error("Error forwarding message:", error);
      }
    }

    async function sendMessageToUser(chatId, text) {
      console.log("Sending message to user:", chatId, text);
      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ chat_id: chatId, text: text })
        });
        const data = await response.json();
        if (!data.ok) {
          console.error(`Failed to send message to user: ${data.description}`);
        }
      } catch (error) {
        console.error("Error sending message to user:", error);
      }
    }

    async function fetchWithRetry(url, options, retries = 3, backoff = 1000) {
      for (let i = 0; i < retries; i++) {
        try {
          const response = await fetch(url, options);
          if (response.ok) {
            return response;
          } else if (response.status === 429) {
            const retryAfter = response.headers.get('Retry-After');
            const delay = retryAfter ? parseInt(retryAfter) * 1000 : backoff * Math.pow(2, i);
            await new Promise(resolve => setTimeout(resolve, delay));
          } else {
            console.error(`Request failed with status ${response.status}: ${response.statusText}`);
            throw new Error(`Request failed with status ${response.status}: ${response.statusText}`);
          }
        } catch (error) {
          if (i === retries - 1) {
            console.error(`Failed to fetch ${url} after ${retries} retries:`, error);
            throw error;
          }
        }
      }
      throw new Error(`Failed to fetch ${url} after ${retries} retries`);
    }

    async function registerWebhook(request) {
      console.log('BOT_TOKEN in registerWebhook:', BOT_TOKEN);
      const webhookUrl = `${new URL(request.url).origin}/webhook`;
      const response = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/setWebhook`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ url: webhookUrl })
      }).then(r => r.json());
      return new Response(response.ok ? 'Webhook set successfully' : JSON.stringify(response, null, 2));
    }

    async function unRegisterWebhook() {
      console.log('BOT_TOKEN in unRegisterWebhook:', BOT_TOKEN);
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
