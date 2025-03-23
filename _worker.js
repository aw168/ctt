// 从环境变量中读取配置
let BOT_TOKEN;
let GROUP_ID;
let MAX_MESSAGES_PER_MINUTE;

// 调试环境变量加载
export default {
  async fetch(request, env) {
    // 调试环境变量
    console.log('BOT_TOKEN_ENV:', env.BOT_TOKEN_ENV || 'undefined');
    console.log('GROUP_ID_ENV:', env.GROUP_ID_ENV || 'undefined');
    console.log('MAX_MESSAGES_PER_MINUTE_ENV:', env.MAX_MESSAGES_PER_MINUTE_ENV || 'undefined');

    // 设置环境变量
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
      console.error('D1 database is not bound. Please check your Worker bindings in the Cloudflare dashboard.');
      return new Response('Server configuration error: D1 database is not bound', { status: 500 });
    }

    // 自动初始化数据库表
    try {
      await initializeDatabase(env.D1);
    } catch (error) {
      console.error('Failed to initialize database:', error.message);
      return new Response(`Server configuration error: Failed to initialize database - ${error.message}`, { status: 500 });
    }

    // 主处理函数
    async function handleRequest(request) {
      // 检查环境变量是否加载
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
      }
      return new Response('Not Found', { status: 404 });
    }

    // 数据库初始化函数
    async function initializeDatabase(d1) {
      try {
        console.log('Initializing database...');

        // 清空数据库：删除所有表
        console.log('Dropping existing tables...');
        try {
          await d1.exec('DROP TABLE IF EXISTS user_states');
          console.log('Dropped user_states table');
        } catch (dropError) {
          console.warn('Error dropping user_states table:', dropError.message);
        }

        try {
          await d1.exec('DROP TABLE IF EXISTS message_rates');
          console.log('Dropped message_rates table');
        } catch (dropError) {
          console.warn('Error dropping message_rates table:', dropError.message);
        }

        try {
          await d1.exec('DROP TABLE IF EXISTS chat_topic_mappings');
          console.log('Dropped chat_topic_mappings table');
        } catch (dropError) {
          console.warn('Error dropping chat_topic_mappings table:', dropError.message);
        }

        // 创建新表
        console.log('Creating new tables...');

        // 创建 user_states 表（单行格式，避免换行问题）
        await d1.exec('CREATE TABLE user_states (chat_id TEXT PRIMARY KEY, is_verified BOOLEAN DEFAULT FALSE, verified_expiry INTEGER, verification_code TEXT, code_expiry INTEGER, is_blocked BOOLEAN DEFAULT FALSE, is_rate_limited BOOLEAN DEFAULT FALSE, is_first_verification BOOLEAN DEFAULT TRUE, last_verification_message_id TEXT, last_verification_time INTEGER, verification_timestamp TEXT)');
        console.log('user_states table created');

        // 创建 message_rates 表
        await d1.exec('CREATE TABLE message_rates (chat_id TEXT PRIMARY KEY, message_count INTEGER DEFAULT 0, window_start INTEGER)');
        console.log('message_rates table created');

        // 创建 chat_topic_mappings 表
        await d1.exec('CREATE TABLE chat_topic_mappings (chat_id TEXT PRIMARY KEY, topic_id INTEGER UNIQUE)');
        console.log('chat_topic_mappings table created');

        console.log('Database tables initialized successfully');
      } catch (error) {
        console.error('Error initializing database:', error.message);
        throw new Error(`Database initialization failed: ${error.message}`);
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
          if (privateChatId) {
            if (text.startsWith('/block') || text.startsWith('/unblock') || text.startsWith('/checkblock')) {
              await handleAdminCommand(message, topicId, privateChatId);
              return;
            }
            await forwardMessageToPrivateChat(privateChatId, message);
          }
        }
        return;
      }

      const userState = await env.D1.prepare('SELECT is_blocked FROM user_states WHERE chat_id = ?')
        .bind(chatId)
        .first();
      const isBlocked = userState ? userState.is_blocked : false;
      if (isBlocked) {
        console.log(`User ${chatId} is blocked, ignoring message.`);
        return;
      }

      const verificationState = await env.D1.prepare('SELECT is_verified, verified_expiry FROM user_states WHERE chat_id = ?')
        .bind(chatId)
        .first();
      let isVerified = verificationState ? verificationState.is_verified : false;
      const verifiedExpiry = verificationState ? verificationState.verified_expiry : null;
      const nowSeconds = Math.floor(Date.now() / 1000);
      if (verifiedExpiry && nowSeconds > verifiedExpiry) {
        await env.D1.prepare('UPDATE user_states SET is_verified = ?, verified_expiry = NULL WHERE chat_id = ?')
          .bind(false, chatId)
          .run();
        isVerified = false;
      }
      console.log(`User ${chatId} verification status: ${isVerified}`);
      if (!isVerified) {
        const messageContent = text || '非文本消息';
        await sendMessageToUser(chatId, `无法转发的信息：${messageContent}\n无法发送，请完成验证`);
        await handleVerification(chatId, messageId);
        return;
      }

      if (await checkMessageRate(chatId)) {
        console.log(`User ${chatId} exceeded message rate limit, resetting verification.`);
        await env.D1.prepare('UPDATE user_states SET is_verified = ?, verified_expiry = NULL, is_rate_limited = ? WHERE chat_id = ?')
          .bind(false, true, chatId)
          .run();
        const messageContent = text || '非文本消息';
        await sendMessageToUser(chatId, `无法转发的信息：${messageContent}\n信息过于频繁，请完成验证后发送信息`);
        await handleVerification(chatId, messageId);
        return;
      }

      if (text === '/start') {
        await env.D1.prepare('INSERT OR REPLACE INTO user_states (chat_id, is_first_verification) VALUES (?, ?)')
          .bind(chatId, true)
          .run();
        const verificationStateAgain = await env.D1.prepare('SELECT is_verified, verified_expiry FROM user_states WHERE chat_id = ?')
          .bind(chatId)
          .first();
        const isVerifiedAgain = verificationStateAgain ? verificationStateAgain.is_verified : false;
        const verifiedExpiryAgain = verificationStateAgain ? verificationStateAgain.verified_expiry : null;
        if (verifiedExpiryAgain && nowSeconds > verifiedExpiryAgain) {
          await env.D1.prepare('UPDATE user_states SET is_verified = ?, verified_expiry = NULL WHERE chat_id = ?')
            .bind(false, chatId)
            .run();
        }
        if (isVerifiedAgain && (!verifiedExpiryAgain || nowSeconds <= verifiedExpiryAgain)) {
          const successMessage = await getVerificationSuccessMessage();
          await sendMessageToUser(chatId, `${successMessage}\n你好，欢迎使用私聊机器人，现在发送信息吧！`);
        } else {
          await sendMessageToUser(chatId, "你好，欢迎使用私聊机器人，现在发送信息吧！");
          await handleVerification(chatId, messageId);
        }
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

    async function handleAdminCommand(message, topicId, privateChatId) {
      const text = message.text;
      const senderId = message.from.id.toString();

      const isAdmin = await checkIfAdmin(senderId);
      if (!isAdmin) {
        await sendMessageToTopic(topicId, '只有管理员可以使用此命令。');
        return;
      }

      if (text === '/block') {
        await env.D1.prepare('INSERT OR REPLACE INTO user_states (chat_id, is_blocked) VALUES (?, ?)')
          .bind(privateChatId, true)
          .run();
        await sendMessageToTopic(topicId, `用户 ${privateChatId} 已被拉黑，消息将不再转发。`);
      } else if (text === '/unblock') {
        await env.D1.prepare('UPDATE user_states SET is_blocked = ? WHERE chat_id = ?')
          .bind(false, privateChatId)
          .run();
        await sendMessageToTopic(topicId, `用户 ${privateChatId} 已解除拉黑，消息将继续转发。`);
      } else if (text === '/checkblock') {
        const userState = await env.D1.prepare('SELECT is_blocked FROM user_states WHERE chat_id = ?')
          .bind(privateChatId)
          .first();
        const isBlocked = userState ? userState.is_blocked : false;
        const status = isBlocked ? '是' : '否';
        await sendMessageToTopic(topicId, `用户 ${privateChatId} 是否在黑名单中：${status}`);
      }
    }

    async function checkIfAdmin(userId) {
      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getChatMember`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: GROUP_ID,
            user_id: userId,
          }),
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

    async function handleVerification(chatId, messageId) {
      const nowSeconds = Math.floor(Date.now() / 1000);

      // 检查是否在冷却时间内（防止重复触发）
      const userState = await env.D1.prepare('SELECT last_verification_time FROM user_states WHERE chat_id = ?')
        .bind(chatId)
        .first();
      const lastVerificationTime = userState ? userState.last_verification_time : 0;
      const cooldownSeconds = 10; // 冷却时间 10 秒
      if (lastVerificationTime && (nowSeconds - lastVerificationTime) < cooldownSeconds) {
        console.log(`User ${chatId} is in verification cooldown, skipping...`);
        await sendMessageToUser(chatId, '请稍后再试，验证请求过于频繁。');
        return;
      }

      // 删除旧的验证消息（如果存在）
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
              message_id: lastVerificationMessageId,
            }),
          });
        } catch (error) {
          console.error("Error deleting old verification message:", error);
        }
        await env.D1.prepare('UPDATE user_states SET last_verification_message_id = NULL WHERE chat_id = ?')
          .bind(chatId)
          .run();
      }

      // 更新最后验证时间
      await env.D1.prepare('UPDATE user_states SET last_verification_time = ? WHERE chat_id = ?')
        .bind(nowSeconds, chatId)
        .run();

      // 生成并发送新的验证问题
      await sendVerification(chatId);
    }

    async function sendVerification(chatId) {
      // 生成新的随机数学问题
      const num1 = Math.floor(Math.random() * 10);
      const num2 = Math.floor(Math.random() * 10);
      const operation = Math.random() > 0.5 ? '+' : '-';
      const correctResult = operation === '+' ? num1 + num2 : num1 - num2;

      // 生成选项
      const options = new Set();
      options.add(correctResult);
      while (options.size < 4) {
        const wrongResult = correctResult + Math.floor(Math.random() * 5) - 2;
        if (wrongResult !== correctResult) {
          options.add(wrongResult);
        }
      }
      const optionArray = Array.from(options).sort(() => Math.random() - 0.5);

      // 创建按钮
      const timestamp = Date.now().toString();
      const buttons = optionArray.map((option) => ({
        text: `(${option})`,
        callback_data: `verify_${chatId}_${option}_${option === correctResult ? 'correct' : 'wrong'}_${timestamp}`,
      }));

      // 构造验证问题
      const question = `请计算：${num1} ${operation} ${num2} = ?（点击下方按钮完成验证）`;
      const nowSeconds = Math.floor(Date.now() / 1000);
      const codeExpiry = nowSeconds + 300; // 验证码有效期 5 分钟

      // 更新数据库中的验证码、过期时间和时间戳
      await env.D1.prepare('INSERT OR REPLACE INTO user_states (chat_id, verification_code, code_expiry, verification_timestamp) VALUES (?, ?, ?, ?)')
        .bind(chatId, correctResult.toString(), codeExpiry, timestamp)
        .run();

      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: chatId,
            text: question,
            reply_markup: {
              inline_keyboard: [buttons],
            },
          }),
        });
        const data = await response.json();
        if (!data.ok) {
          console.error(`Failed to send verification message: ${data.description}`);
          return;
        }
        // 保存新的验证消息 ID
        await env.D1.prepare('UPDATE user_states SET last_verification_message_id = ? WHERE chat_id = ?')
          .bind(data.result.message_id.toString(), chatId)
          .run();
      } catch (error) {
        console.error("Error sending verification message:", error);
      }
    }

    async function getVerificationSuccessMessage() {
      try {
        const response = await fetch('https://raw.githubusercontent.com/iawooo/ctt/refs/heads/main/CFTeleTrans/start.md');
        if (!response.ok) {
          throw new Error(`Failed to fetch fraud.db: ${response.statusText}`);
        }
        const message = await response.text();
        const trimmedMessage = message.trim();
        if (!trimmedMessage) {
          throw new Error('fraud.db content is empty');
        }
        return trimmedMessage;
      } catch (error) {
        console.error("Error fetching verification success message:", error);
        return '验证成功！您现在可以与客服聊天。';
      }
    }

    async function getNotificationContent() {
      try {
        const response = await fetch('https://raw.githubusercontent.com/iawooo/ctt/refs/heads/main/CFTeleTrans/notification.md');
        if (!response.ok) {
          throw new Error(`Failed to fetch notification.txt: ${response.statusText}`);
        }
        const content = await response.text();
        const trimmedContent = content.trim();
        if (!trimmedContent) {
          throw new Error('notification.txt content is empty');
        }
        return trimmedContent;
      } catch (error) {
        console.error("Error fetching notification content:", error);
        return '';
      }
    }

    async function onCallbackQuery(callbackQuery) {
      const chatId = callbackQuery.message.chat.id.toString();
      const data = callbackQuery.data;
      const messageId = callbackQuery.message.message_id;

      if (!data.startsWith('verify_')) return;

      const [, userChatId, selectedAnswer, result, timestamp] = data.split('_');
      if (userChatId !== chatId) return;

      // 获取验证码、过期时间和时间戳
      const verificationState = await env.D1.prepare('SELECT verification_code, code_expiry, verification_timestamp FROM user_states WHERE chat_id = ?')
        .bind(chatId)
        .first();
      const storedCode = verificationState ? verificationState.verification_code : null;
      const codeExpiry = verificationState ? verificationState.code_expiry : null;
      const storedTimestamp = verificationState ? verificationState.verification_timestamp : null;
      const nowSeconds = Math.floor(Date.now() / 1000);

      // 调试日志
      console.log(`Chat ID: ${chatId}, Selected Answer: ${selectedAnswer}, Result: ${result}, Timestamp: ${timestamp}`);
      console.log(`Stored Code: ${storedCode}, Code Expiry: ${codeExpiry}, Stored Timestamp: ${storedTimestamp}, Now: ${nowSeconds}`);

      // 检查时间戳是否匹配（确保处理的是最新的验证请求）
      if (!storedTimestamp || storedTimestamp !== timestamp) {
        await sendMessageToUser(chatId, '此验证已失效，请重新发送消息以获取新验证码。');
        // 删除旧的验证消息
        try {
          await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              chat_id: chatId,
              message_id: messageId,
            }),
          });
        } catch (error) {
          console.error("Error deleting verification message:", error);
        }
        // 响应回调查询
        await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/answerCallbackQuery`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            callback_query_id: callbackQuery.id,
          }),
        });
        return;
      }

      // 检查验证码是否存在且未过期
      if (!storedCode || (codeExpiry && nowSeconds > codeExpiry)) {
        await sendMessageToUser(chatId, '验证码已过期，请重新发送消息以获取新验证码。');
        // 删除旧的验证消息
        try {
          await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              chat_id: chatId,
              message_id: messageId,
            }),
          });
        } catch (error) {
          console.error("Error deleting verification message:", error);
        }
        // 响应回调查询
        await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/answerCallbackQuery`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            callback_query_id: callbackQuery.id,
          }),
        });
        return;
      }

      // 检查答案是否正确
      if (result === 'correct' && selectedAnswer === storedCode) {
        // 答案正确，更新验证状态
        const verifiedExpiry = nowSeconds + 3600; // 验证有效期 1 小时
        await env.D1.prepare('UPDATE user_states SET is_verified = ?, verified_expiry = ?, verification_code = NULL, code_expiry = NULL, last_verification_message_id = NULL, last_verification_time = NULL, verification_timestamp = NULL WHERE chat_id = ?')
          .bind(true, verifiedExpiry, chatId)
          .run();

        const userState = await env.D1.prepare('SELECT is_first_verification, is_rate_limited FROM user_states WHERE chat_id = ?')
          .bind(chatId)
          .first();
        const isFirstVerification = userState ? userState.is_first_verification : false;
        const isRateLimited = userState ? userState.is_rate_limited : false;

        if (isFirstVerification) {
          const successMessage = await getVerificationSuccessMessage();
          await sendMessageToUser(chatId, `${successMessage}\n你好，欢迎使用私聊机器人！`);
          await env.D1.prepare('UPDATE user_states SET is_first_verification = ? WHERE chat_id = ?')
            .bind(false, chatId)
            .run();
        } else {
          await sendMessageToUser(chatId, '验证成功！请重新发送您的消息');
        }

        if (isRateLimited) {
          await env.D1.prepare('UPDATE user_states SET is_rate_limited = ? WHERE chat_id = ?')
            .bind(false, chatId)
            .run();
        }
      } else {
        // 答案错误，重新生成验证问题
        await sendMessageToUser(chatId, '验证失败，请重新尝试。');
        await handleVerification(chatId, messageId);
      }

      // 删除旧的验证消息
      try {
        await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: chatId,
            message_id: messageId,
          }),
        });
      } catch (error) {
        console.error("Error deleting verification message:", error);
      }

      // 响应回调查询
      await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/answerCallbackQuery`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          callback_query_id: callbackQuery.id,
        }),
      });
    }

    async function checkMessageRate(chatId) {
      const key = chatId;
      const now = Date.now();
      const window = 60 * 1000;

      const rateData = await env.D1.prepare('SELECT message_count, window_start FROM message_rates WHERE chat_id = ?')
        .bind(key)
        .first();
      let data = rateData ? { count: rateData.message_count, start: rateData.window_start } : { count: 0, start: now };

      if (now - data.start > window) {
        data.count = 1;
        data.start = now;
      } else {
        data.count += 1;
      }

      await env.D1.prepare('INSERT OR REPLACE INTO message_rates (chat_id, message_count, window_start) VALUES (?, ?, ?)')
        .bind(key, data.count, data.start)
        .run();

      console.log(`User ${chatId} message count: ${data.count}/${MAX_MESSAGES_PER_MINUTE} in last minute`);
      return data.count > MAX_MESSAGES_PER_MINUTE;
    }

    async function getUserInfo(chatId) {
      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getChat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: chatId }),
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
        body: JSON.stringify({ chat_id: GROUP_ID, name: topicName }),
      });
      const data = await response.json();
      if (!data.ok) {
        throw new Error(`Failed to create forum topic: ${data.description}`);
      }
      const topicId = data.result.message_thread_id;

      const now = new Date();
      const formattedTime = now.toISOString().replace('T', ' ').substring(0, 19);

      const notificationContent = await getNotificationContent();

      const pinnedMessage = `昵称: ${nickname}\n用户名: ${userName}\nUserID: ${userId}\n发起时间: ${formattedTime}\n\n${notificationContent}`;
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
            parse_mode: 'Markdown',
          }),
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
            message_thread_id: topicId,
          }),
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
            message_thread_id: topicId,
          }),
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
            message_id: message.message_id,
          }),
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
          body: JSON.stringify({ chat_id: chatId, text: text }),
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
        body: JSON.stringify({ url: webhookUrl }),
      }).then(r => r.json());
      return new Response(response.ok ? 'Webhook set successfully' : JSON.stringify(response, null, 2));
    }

    async function unRegisterWebhook() {
      console.log('BOT_TOKEN in unRegisterWebhook:', BOT_TOKEN);
      const response = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/setWebhook`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ url: '' }),
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
