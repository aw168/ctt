// 从环境变量中读取配置
let BOT_TOKEN;
let GROUP_ID;
let MAX_MESSAGES_PER_MINUTE;

// 全局变量，用于控制清理频率和 webhook 初始化
let lastCleanupTime = 0;
const CLEANUP_INTERVAL = 24 * 60 * 60 * 1000; // 24 小时
let isWebhookInitialized = false; // 用于标记 webhook 是否已初始化
const processedMessages = new Set(); // 用于存储已处理的消息 ID，防止重复处理
const processingCallbacks = new Set(); // 用于存储正在处理的 callback_query.id

// 缓存 settings 表中的常用值
const settingsCache = {
  verification_enabled: null,
  user_raw_enabled: null
};

// 缓存 chat_topic_mappings 表中的话题映射
const topicCache = new Map();

// 缓存 user_states 表中的用户状态
const userStateCache = new Map();

// 缓存 message_rates 表中的消息频率
const messageRateCache = new Map();

// 缓存用户信息
const userInfoCache = new Map();

// 缓存管理员状态
const adminCache = new Map();

// 缓存 admin.md 内容
let adminPanelMessageCache = '';

export default {
  async fetch(request, env) {
    // 加载环境变量
    if (!env.BOT_TOKEN_ENV) {
      BOT_TOKEN = null;
      return new Response('Server configuration error: BOT_TOKEN_ENV is not set', { status: 500 });
    } else {
      BOT_TOKEN = env.BOT_TOKEN_ENV;
    }

    if (!env.GROUP_ID_ENV) {
      GROUP_ID = null;
      return new Response('Server configuration error: GROUP_ID_ENV is not set', { status: 500 });
    } else {
      GROUP_ID = env.GROUP_ID_ENV;
    }

    MAX_MESSAGES_PER_MINUTE = env.MAX_MESSAGES_PER_MINUTE_ENV ? parseInt(env.MAX_MESSAGES_PER_MINUTE_ENV) : 40;

    if (!env.D1) {
      return new Response('Server configuration error: D1 database is not bound', { status: 500 });
    }

    try {
      await checkAndRepairTables(env.D1);
    } catch (error) {
      return new Response('Database initialization error', { status: 500 });
    }

    // 初始化 adminPanelMessageCache
    if (!adminPanelMessageCache) {
      try {
        const response = await fetch('https://raw.githubusercontent.com/iawooo/ctt/refs/heads/main/CFTeleTrans/admin.md');
        if (response.ok) {
          adminPanelMessageCache = await response.text();
          adminPanelMessageCache = adminPanelMessageCache.trim() || '';
        } else {
          adminPanelMessageCache = '';
        }
      } catch (error) {
        adminPanelMessageCache = '';
      }
    }

    if (!isWebhookInitialized && BOT_TOKEN) {
      try {
        await autoRegisterWebhook(request);
        isWebhookInitialized = true;
      } catch (error) {}
    }

    if (BOT_TOKEN && GROUP_ID) {
      try {
        await checkBotPermissions();
      } catch (error) {}
    }

    try {
      await cleanExpiredVerificationCodes(env.D1);
    } catch (error) {}

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

    async function autoRegisterWebhook(request) {
      const webhookUrl = `${new URL(request.url).origin}/webhook`;
      await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/setWebhook`, {
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
      if (!data.ok) return;

      const memberResponse = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getChatMember`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          chat_id: GROUP_ID,
          user_id: (await getBotId())
        })
      });
    }

    async function getBotId() {
      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getMe`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({})
      });
      const data = await response.json();
      if (!data.ok) throw new Error('Failed to get bot ID');
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

        if (tableName === 'chat_topic_mappings') {
          await d1.exec('CREATE INDEX IF NOT EXISTS idx_chat_topic_mappings_chat_id ON chat_topic_mappings (chat_id)');
        }

        if (tableName === 'user_states') {
          await d1.exec('CREATE INDEX IF NOT EXISTS idx_user_states_chat_id ON user_states (chat_id)');
        }
      }

      // 初始化 settings 表并缓存值
      await d1.prepare('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)')
        .bind('verification_enabled', 'true').run();
      await d1.prepare('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)')
        .bind('user_raw_enabled', 'true').run();

      settingsCache.verification_enabled = (await getSetting('verification_enabled', d1)) === 'true';
      settingsCache.user_raw_enabled = (await getSetting('user_raw_enabled', d1)) === 'true';

      // 初始化 topicCache
      const topicMappings = await d1.prepare('SELECT chat_id, topic_id FROM chat_topic_mappings').all();
      for (const { chat_id, topic_id } of topicMappings.results) {
        topicCache.set(chat_id, topic_id);
      }

      // 初始化 userStateCache
      const userStates = await d1.prepare('SELECT chat_id, is_blocked, is_verified, verified_expiry, is_first_verification, is_rate_limited FROM user_states').all();
      for (const state of userStates.results) {
        userStateCache.set(state.chat_id, {
          is_blocked: state.is_blocked || false,
          is_verified: state.is_verified || false,
          verified_expiry: state.verified_expiry || null,
          is_first_verification: state.is_first_verification || false,
          is_rate_limited: state.is_rate_limited || false
        });
      }

      // 初始化 messageRateCache
      const messageRates = await d1.prepare('SELECT chat_id, message_count, window_start, start_count, start_window_start FROM message_rates').all();
      for (const rate of messageRates.results) {
        messageRateCache.set(rate.chat_id, {
          message_count: rate.message_count || 0,
          window_start: rate.window_start || Date.now(),
          start_count: rate.start_count || 0,
          start_window_start: rate.start_window_start || Date.now()
        });
      }
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
              'UPDATE user_states SET verification_code = NULL, code_expiry = NULL WHERE chat_id = ?'
            ).bind(chat_id)
          )
        );

        for (const { chat_id } of expiredCodes.results) {
          const state = userStateCache.get(chat_id);
          if (state) {
            state.verification_code = null;
            state.code_expiry = null;
          }
        }
      }
      lastCleanupTime = now;
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
        }
        return;
      }

      // 从缓存中获取用户状态
      let userState = userStateCache.get(chatId);
      if (!userState) {
        userState = {
          is_blocked: false,
          is_verified: false,
          verified_expiry: null,
          is_first_verification: true,
          is_rate_limited: false
        };
        env.D1.prepare('INSERT INTO user_states (chat_id, is_blocked, is_first_verification, is_verified) VALUES (?, ?, ?, ?)')
          .bind(chatId, false, true, false)
          .run();
        userStateCache.set(chatId, userState);
      }

      if (userState.is_blocked) {
        await sendMessageToUser(chatId, "您已被拉黑，无法发送消息。请联系管理员解除拉黑。");
        return;
      }

      if (text === '/start') {
        if (await checkStartCommandRate(chatId)) {
          await sendMessageToUser(chatId, "您发送 /start 命令过于频繁，请稍后再试！");
          return;
        }

        const verificationEnabled = settingsCache.verification_enabled;
        const isFirstVerification = userState.is_first_verification;

        if (verificationEnabled && isFirstVerification) {
          await sendMessageToUser(chatId, "你好，欢迎使用私聊机器人，请完成验证以开始使用！");
          await handleVerification(chatId, messageId);
        } else {
          const successMessage = await getVerificationSuccessMessage();
          await sendMessageToUser(chatId, `${successMessage}\n你好，欢迎使用私聊机器人，现在发送信息吧！`);
        }
        return;
      }

      // 检查验证状态
      const verificationEnabled = settingsCache.verification_enabled;
      const nowSeconds = Math.floor(Date.now() / 1000);
      const isVerified = userState.is_verified && userState.verified_expiry && nowSeconds < userState.verified_expiry;

      if (verificationEnabled && !isVerified) {
        await sendMessageToUser(chatId, "您尚未完成验证，请完成验证后发送消息。");
        await handleVerification(chatId, messageId);
        return;
      }

      if (verificationEnabled && await checkMessageRate(chatId)) {
        userState.is_rate_limited = true;
        env.D1.prepare('UPDATE user_states SET is_rate_limited = ? WHERE chat_id = ?')
          .bind(true, chatId)
          .run();
        const messageContent = text || '非文本消息';
        await sendMessageToUser(chatId, `无法转发的信息：${messageContent}\n信息过于频繁，请完成验证后发送信息`);
        await handleVerification(chatId, messageId);
        return;
      }

      // 并行获取用户信息和话题 ID
      const [userInfo, topicId] = await Promise.all([
        getUserInfo(chatId),
        getExistingTopicId(chatId)
      ]);

      const userName = userInfo.username || userInfo.first_name;
      const nickname = `${userInfo.first_name} ${userInfo.last_name || ''}`.trim();
      const topicName = `${nickname}`;

      // 格式化消息内容
      let formattedMessage = '';
      if (text) {
        formattedMessage = `*${nickname}:*\n------------------------------------------------\n\n${text}`;
      } else if (message.photo) {
        const caption = message.caption || '无描述';
        formattedMessage = `*${nickname}:*\n------------------------------------------------\n\n发送了一张图片\n描述: ${caption}`;
      } else if (message.document) {
        const fileName = message.document.file_name || '未知文件';
        formattedMessage = `*${nickname}:*\n------------------------------------------------\n\n发送了一个文件\n文件名: ${fileName}`;
      } else if (message.video) {
        const caption = message.caption || '无描述';
        formattedMessage = `*${nickname}:*\n------------------------------------------------\n\n发送了一个视频\n描述: ${caption}`;
      } else {
        formattedMessage = `*${nickname}:*\n------------------------------------------------\n\n发送了一条非文本消息`;
      }

      let finalTopicId = topicId;
      let shouldCreateTopic = !finalTopicId;

      // 转发消息
      if (shouldCreateTopic) {
        finalTopicId = await createForumTopic(topicName, userName, nickname, userInfo.id);
        saveTopicId(chatId, finalTopicId); // 异步保存，不等待
      }
      await sendMessageToTopic(finalTopicId, formattedMessage);
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
      userStateCache.delete(targetChatId);
      messageRateCache.delete(targetChatId);
      topicCache.delete(targetChatId);
      await sendMessageToTopic(topicId, `用户 ${targetChatId} 的状态已重置。`);
    }

    async function getAdminPanelMessage() {
      return adminPanelMessageCache;
    }

    async function sendAdminPanel(chatId, topicId, privateChatId, messageId) {
      const verificationEnabled = settingsCache.verification_enabled;
      const userRawEnabled = settingsCache.user_raw_enabled;

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
          { text: '删除用户', callback_data: `delete_user_${privateChatId}` }
        ]
      ];

      const adminMessageContent = await getAdminPanelMessage();
      const adminMessage = adminMessageContent ? `${adminMessageContent}\n\n管理员面板：请选择操作` : '管理员面板：请选择操作';

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

      await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          chat_id: chatId,
          message_id: messageId
        })
      });
    }

    async function getVerificationSuccessMessage() {
      const userRawEnabled = settingsCache.user_raw_enabled;
      if (!userRawEnabled) return '验证成功！您现在可以与客服聊天。';

      try {
        const response = await fetch('https://raw.githubusercontent.com/iawooo/ctt/refs/heads/main/CFTeleTrans/start.md');
        if (!response.ok) return '验证成功！您现在可以与客服聊天。';
        const message = await response.text();
        return message.trim() || '验证成功！您现在可以与客服聊天。';
      } catch (error) {
        return '验证成功！您现在可以与客服聊天。';
      }
    }

    async function checkStartCommandRate(chatId) {
      const now = Date.now();
      const window = 5 * 60 * 1000;
      const maxStartsPerWindow = 1;

      let rateData = messageRateCache.get(chatId) || { start_count: 0, start_window_start: now };
      if (now - rateData.start_window_start > window) {
        rateData.start_count = 1;
        rateData.start_window_start = now;
      } else {
        rateData.start_count += 1;
      }

      messageRateCache.set(chatId, rateData);
      env.D1.prepare('INSERT OR REPLACE INTO message_rates (chat_id, start_count, start_window_start) VALUES (?, ?, ?)')
        .bind(chatId, rateData.start_count, rateData.start_window_start)
        .run();

      return rateData.start_count > maxStartsPerWindow;
    }

    async function checkMessageRate(chatId) {
      const now = Date.now();
      const window = 60 * 1000;

      let rateData = messageRateCache.get(chatId) || { message_count: 0, window_start: now };
      if (now - rateData.window_start > window) {
        rateData.message_count = 1;
        rateData.window_start = now;
      } else {
        rateData.message_count += 1;
      }

      messageRateCache.set(chatId, rateData);
      env.D1.prepare('INSERT OR REPLACE INTO message_rates (chat_id, message_count, window_start) VALUES (?, ?, ?)')
        .bind(chatId, rateData.message_count, rateData.window_start)
        .run();

      return rateData.message_count > MAX_MESSAGES_PER_MINUTE;
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
        settingsCache.verification_enabled = value === 'true';
      } else if (key === 'user_raw_enabled') {
        settingsCache.user_raw_enabled = value === 'true';
      }
    }

    async function onCallbackQuery(callbackQuery) {
      const callbackId = callbackQuery.id;
      
      // 检查是否已经在处理相同的 callback_query
      if (processingCallbacks.has(callbackId)) {
        await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/answerCallbackQuery`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            callback_query_id: callbackId,
            text: '正在处理，请稍候...',
            show_alert: false
          })
        });
        return;
      }

      // 添加到处理中的集合
      processingCallbacks.add(callbackId);

      try {
        const chatId = callbackQuery.message.chat.id.toString();
        let topicId = callbackQuery.message.message_thread_id;
        const data = callbackQuery.data;
        const messageId = callbackQuery.message.message_id;

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
        } else {
          action = data;
          privateChatId = '';
        }

        if (action === 'verify') {
          const [, userChatId, selectedAnswer, result] = data.split('_');
          if (userChatId !== chatId) {
            return;
          }

          const verificationState = await env.D1.prepare('SELECT verification_code, code_expiry FROM user_states WHERE chat_id = ?')
            .bind(chatId)
            .first();
          const storedCode = verificationState?.verification_code;
          const code_expiry = verificationState?.code_expiry;
          const nowSeconds = Math.floor(Date.now() / 1000);

          if (!storedCode || (code_expiry && nowSeconds > code_expiry)) {
            await sendMessageToUser(chatId, '验证码已过期，请重新发送消息以获取新验证码。');
            return;
          }

          if (result === 'correct') {
            const verifiedExpiry = nowSeconds + 3600;
            await env.D1.prepare('UPDATE user_states SET is_verified = ?, verified_expiry = ?, verification_code = NULL, code_expiry = NULL, last_verification_message_id = NULL WHERE chat_id = ?')
              .bind(true, verifiedExpiry, chatId)
              .run();

            const userState = userStateCache.get(chatId) || {};
            userState.is_verified = true;
            userState.verified_expiry = verifiedExpiry;
            userState.verification_code = null;
            userState.code_expiry = null;

            const isFirstVerification = userState.is_first_verification || false;
            const isRateLimited = userState.is_rate_limited || false;

            const successMessage = await getVerificationSuccessMessage();
            await sendMessageToUser(chatId, `${successMessage}\n你好，欢迎使用私聊机器人！现在可以发送消息了。`);

            if (isFirstVerification) {
              userState.is_first_verification = false;
              await env.D1.prepare('UPDATE user_states SET is_first_verification = ? WHERE chat_id = ?')
                .bind(false, chatId)
                .run();
            }

            if (isRateLimited) {
              userState.is_rate_limited = false;
              await env.D1.prepare('UPDATE user_states SET is_rate_limited = ? WHERE chat_id = ?')
                .bind(false, chatId)
                .run();
            }

            userStateCache.set(chatId, userState);
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

          const userInfo = await getUserInfo(privateChatId);
          const userName = userInfo.username || userInfo.first_name;
          const nickname = `${userInfo.first_name} ${userInfo.last_name || ''}`.trim();

          if (action === 'block') {
            const userState = userStateCache.get(privateChatId) || {};
            userState.is_blocked = true;
            userStateCache.set(privateChatId, userState);
            await env.D1.prepare('INSERT OR REPLACE INTO user_states (chat_id, is_blocked) VALUES (?, ?)')
              .bind(privateChatId, true)
              .run();
            await sendMessageToTopic(topicId, `用户 ${privateChatId} 已被拉黑，消息将不再转发。`);
          } else if (action === 'unblock') {
            const userState = userStateCache.get(privateChatId) || {};
            userState.is_blocked = false;
            userState.is_first_verification = true;
            userStateCache.set(privateChatId, userState);
            await env.D1.prepare('INSERT OR REPLACE INTO user_states (chat_id, is_blocked, is_first_verification) VALUES (?, ?, ?)')
              .bind(privateChatId, false, true)
              .run();
            await sendMessageToTopic(topicId, `用户 ${privateChatId} 已解除拉黑，消息将继续转发。`);
          } else if (action === 'toggle_verification') {
            const currentState = settingsCache.verification_enabled;
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
            const currentState = settingsCache.user_raw_enabled;
            const newState = !currentState;
            await setSetting('user_raw_enabled', newState.toString());
            await sendMessageToTopic(topicId, `用户端 Raw 链接已${newState ? '开启' : '关闭'}。`);
          } else if (action === 'delete_user') {
            await env.D1.batch([
              env.D1.prepare('DELETE FROM user_states WHERE chat_id = ?').bind(privateChatId),
              env.D1.prepare('DELETE FROM message_rates WHERE chat_id = ?').bind(privateChatId),
              env.D1.prepare('DELETE FROM chat_topic_mappings WHERE chat_id = ?').bind(privateChatId)
            ]);
            userStateCache.delete(privateChatId);
            messageRateCache.delete(privateChatId);
            topicCache.delete(privateChatId);
            await sendMessageToTopic(topicId, `用户 ${privateChatId} 的状态和消息记录已删除，话题保留。`);
          } else {
            await sendMessageToTopic(topicId, `未知操作：${action}`);
          }

          await sendAdminPanel(chatId, topicId, privateChatId, messageId);
        }

        await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/answerCallbackQuery`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            callback_query_id: callbackId,
            text: '操作完成！',
            show_alert: false
          })
        });
      } finally {
        // 无论成功或失败，都移除处理中的 callback_query
        processingCallbacks.delete(callbackId);
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
        await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: chatId,
            message_id: lastVerificationMessageId
          })
        });
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
      }
    }

    async function checkIfAdmin(userId) {
      if (adminCache.has(userId)) {
        return adminCache.get(userId);
      }

      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getChatMember`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          chat_id: GROUP_ID,
          user_id: userId
        })
      });
      const data = await response.json();
      const isAdmin = data.ok && (data.result.status === 'administrator' || data.result.status === 'creator');
      adminCache.set(userId, isAdmin);
      return isAdmin;
    }

    async function getUserInfo(chatId) {
      if (userInfoCache.has(chatId)) {
        return userInfoCache.get(chatId);
      }

      const defaultUserInfo = { first_name: "Unknown", last_name: "", username: "unknown", id: chatId };
      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getChat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: chatId })
      });
      const data = await response.json();
      if (data.ok) {
        userInfoCache.set(chatId, data.result);
        return data.result;
      }
      return defaultUserInfo;
    }

    async function getExistingTopicId(chatId) {
      return topicCache.get(chatId) || null;
    }

    async function createForumTopic(topicName, userName, nickname, userId) {
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
      const pinnedMessage = `昵称: ${nickname}\n用户名: @${userName}\nUserID: ${userId}\n发起时间: ${formattedTime}\n\n通知内容：请及时处理用户消息。`;
      const messageResponse = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          chat_id: GROUP_ID,
          message_thread_id: topicId,
          text: pinnedMessage,
          parse_mode: 'Markdown'
        })
      });
      const messageData = await messageResponse.json();
      if (messageData.ok) {
        const messageId = messageData.result.message_id;
        await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/pinChatMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: GROUP_ID,
            message_id: messageId,
            message_thread_id: topicId
          })
        });
      }

      return topicId;
    }

    async function saveTopicId(chatId, topicId) {
      topicCache.set(chatId, topicId);
      env.D1.prepare('INSERT OR REPLACE INTO chat_topic_mappings (chat_id, topic_id) VALUES (?, ?)')
        .bind(chatId, topicId)
        .run();
    }

    async function getPrivateChatId(topicId) {
      const chatId = [...topicCache.entries()].find(([_, tid]) => tid === topicId)?.[0];
      if (chatId) {
        return chatId;
      }

      const mapping = await env.D1.prepare('SELECT chat_id FROM chat_topic_mappings WHERE topic_id = ?')
        .bind(topicId)
        .first();
      const result = mapping?.chat_id || null;
      if (result) {
        topicCache.set(result, topicId);
      }
      return result;
    }

    async function sendMessageToTopic(topicId, text) {
      if (!text.trim()) return;

      const requestBody = {
        chat_id: GROUP_ID,
        text: text,
        message_thread_id: topicId,
        parse_mode: 'Markdown'
      };
      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestBody)
      });
      const data = await response.json();
      if (!data.ok) throw new Error(`Failed to send message to topic: ${data.description}`);
      return data;
    }

    async function sendMessageToUser(chatId, text) {
      const requestBody = { chat_id: chatId, text: text };
      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestBody)
      });
      const data = await response.json();
      if (!data.ok) throw new Error(`Failed to send message to user: ${data.description}`);
    }

    async function fetchWithRetry(url, options, retries = 2, backoff = 500) {
      for (let i = 0; i <= retries; i++) {
        try {
          const controller = new AbortController();
          const timeoutId = setTimeout(() => controller.abort(), 500); // 缩短超时时间到 500ms
          const response = await fetch(url, { ...options, signal: controller.signal });
          clearTimeout(timeoutId);

          if (response.ok) {
            return response;
          }
          if (response.status === 429) {
            const retryAfter = response.headers.get('Retry-After');
            const delay = retryAfter ? parseInt(retryAfter) * 1000 : backoff * Math.pow(2, i);
            await new Promise(resolve => setTimeout(resolve, delay));
          } else {
            throw new Error(`Request failed with status ${response.status}`);
          }
        } catch (error) {
          if (i === retries) throw error;
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
      return new Response('Internal Server Error', { status: 500 });
    }
  }
};
