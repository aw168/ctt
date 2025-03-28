let BOT_TOKEN;
let GROUP_ID;
let MAX_MESSAGES_PER_MINUTE = 40;

let lastCleanupTime = 0;
const CLEANUP_INTERVAL = 24 * 60 * 60 * 1000; // 24 小时
let isInitialized = false;

// 内存缓存
const userInfoCache = new Map();
const adminCache = new Map();
const topicIdCache = new Map();
const userStateCache = new Map();
const settingsCache = {
  verification_enabled: null,
  user_raw_enabled: null
};

export default {
  async fetch(request, env) {
    // 一次性初始化
    if (!isInitialized) {
      BOT_TOKEN = env.BOT_TOKEN_ENV || null;
      GROUP_ID = env.GROUP_ID_ENV || null;
      MAX_MESSAGES_PER_MINUTE = env.MAX_MESSAGES_PER_MINUTE_ENV ? parseInt(env.MAX_MESSAGES_PER_MINUTE_ENV) : 40;

      if (!BOT_TOKEN || !GROUP_ID || !env.D1) {
        console.error('Missing required environment variables or D1 binding');
        return new Response('Server configuration error', { status: 500 });
      }

      // 初始化数据库表
      await checkAndRepairTables(env.D1);

      // 预加载 settings
      settingsCache.verification_enabled = (await getSetting('verification_enabled', env.D1)) === 'true';
      settingsCache.user_raw_enabled = (await getSetting('user_raw_enabled', env.D1)) === 'true';

      // 预加载话题映射
      const mappings = await env.D1.prepare('SELECT chat_id, topic_id FROM chat_topic_mappings').all();
      mappings.results.forEach(({ chat_id, topic_id }) => topicIdCache.set(chat_id, topic_id));

      isInitialized = true;
    }

    async function handleRequest(request) {
      const url = new URL(request.url);
      if (url.pathname === '/webhook') {
        try {
          const update = await request.json();
          await handleUpdate(update);
          return new Response('OK');
        } catch (error) {
          console.error('Error handling update:', error);
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

      // 处理群组消息
      if (chatId === GROUP_ID) {
        const topicId = message.message_thread_id;
        if (topicId) {
          const privateChatId = await getPrivateChatId(topicId);
          if (privateChatId) {
            if (text === '/admin') {
              await sendAdminPanel(chatId, topicId, privateChatId, messageId);
              return;
            }
            if (text.startsWith('/reset_user')) {
              await handleResetUser(chatId, topicId, text);
              return;
            }
            // 转发群组消息到私聊
            await forwardMessageToPrivateChat(privateChatId, message);
          } else {
            console.error(`No private chat ID found for topicId ${topicId}`);
            await sendMessageToTopic(topicId, `无法找到对应的私聊用户，topicId: ${topicId}`);
          }
        } else {
          console.error('No topic ID found for group message');
          await sendMessageToTopic(null, '群组消息缺少话题 ID，无法转发');
        }
        return;
      }

      // 获取用户状态（优先从内存读取）
      const userStateKey = `${chatId}:state`;
      let userState = userStateCache.get(userStateKey);
      if (!userState) {
        const userData = await env.D1.prepare(
          'SELECT is_blocked, is_first_verification, is_verified, verified_expiry, message_count, window_start, start_count, start_window_start, verification_code, code_expiry, last_verification_message_id, rate_limit_verified, rate_limit_window_start FROM users WHERE chat_id = ?'
        ).bind(chatId).first();

        userState = userData || {
          is_blocked: false,
          is_first_verification: true,
          is_verified: false,
          verified_expiry: null,
          message_count: 0,
          window_start: Date.now(),
          start_count: 0,
          start_window_start: Date.now(),
          verification_code: null,
          code_expiry: null,
          last_verification_message_id: null,
          rate_limit_verified: false,
          rate_limit_window_start: Date.now()
        };
        userStateCache.set(userStateKey, userState);
      }

      const isBlocked = userState.is_blocked || false;
      if (isBlocked) {
        await sendMessageToUser(chatId, "您已被拉黑，无法发送消息。请联系管理员解除拉黑。");
        return;
      }

      if (text === '/start') {
        const now = Date.now();
        const window = 5 * 60 * 1000;
        const maxStartsPerWindow = 1;

        let startCount = userState.start_count || 0;
        let startWindowStart = userState.start_window_start || now;

        if (now - startWindowStart > window) {
          startCount = 1;
          startWindowStart = now;
        } else {
          startCount += 1;
        }

        userState.start_count = startCount;
        userState.start_window_start = startWindowStart;
        userStateCache.set(userStateKey, userState);

        await env.D1.prepare(
          'INSERT INTO users (chat_id, start_count, start_window_start) VALUES (?, ?, ?) ON CONFLICT(chat_id) DO UPDATE SET start_count = ?, start_window_start = ?'
        ).bind(chatId, startCount, startWindowStart, startCount, startWindowStart).run();

        if (startCount > maxStartsPerWindow) {
          await sendMessageToUser(chatId, "您发送 /start 命令过于频繁，请稍后再试！");
          return;
        }

        const verificationEnabled = settingsCache.verification_enabled;
        const isFirstVerification = userState.is_first_verification;

        if (verificationEnabled && isFirstVerification) {
          console.log(`Chat ${chatId}: Triggering first-time verification`);
          await sendMessageToUser(chatId, "你好，欢迎使用私聊机器人，请完成验证以开始使用！");
          await handleVerification(chatId, messageId);
          await sendMessageToUser(chatId, `请验证通过后重新发送“${text}”`);
        } else {
          const successMessage = await getVerificationSuccessMessage();
          await sendMessageToUser(chatId, `${successMessage}\n你好，欢迎使用私聊机器人，现在发送信息吧！`);
        }
        return;
      }

      const verificationEnabled = settingsCache.verification_enabled;
      const nowSeconds = Math.floor(Date.now() / 1000);
      const isVerified = userState.is_verified && userState.verified_expiry && nowSeconds < userState.verified_expiry;
      const isFirstVerification = userState.is_first_verification;

      // 速率限制（每分钟限制）
      const now = Date.now();
      const window = 60 * 1000;
      let messageCount = userState.message_count || 0;
      let windowStart = userState.window_start || now;
      let rateLimitVerified = userState.rate_limit_verified || false;
      let rateLimitWindowStart = userState.rate_limit_window_start || now;

      // 检查速率限制窗口是否重置
      if (now - windowStart > window) {
        console.log(`Chat ${chatId}: Rate limit window reset, resetting rate_limit_verified`);
        messageCount = 1;
        windowStart = now;
        rateLimitVerified = false;
        rateLimitWindowStart = now;
      } else {
        messageCount += 1;
      }

      userState.message_count = messageCount;
      userState.window_start = windowStart;
      userState.rate_limit_verified = rateLimitVerified;
      userState.rate_limit_window_start = rateLimitWindowStart;
      userStateCache.set(userStateKey, userState);

      await env.D1.prepare(
        'INSERT INTO users (chat_id, message_count, window_start, rate_limit_verified, rate_limit_window_start) VALUES (?, ?, ?, ?, ?) ON CONFLICT(chat_id) DO UPDATE SET message_count = ?, window_start = ?, rate_limit_verified = ?, rate_limit_window_start = ?'
      ).bind(chatId, messageCount, windowStart, rateLimitVerified ? 1 : 0, rateLimitWindowStart, messageCount, windowStart, rateLimitVerified ? 1 : 0, rateLimitWindowStart).run();

      const isRateLimited = messageCount > MAX_MESSAGES_PER_MINUTE;

      // 验证码触发逻辑：未通过初次验证或（达到速率限制且未通过速率限制验证）时触发验证码
      if (verificationEnabled && (!isVerified || (isRateLimited && !rateLimitVerified))) {
        console.log(`Chat ${chatId}: Triggering verification - isVerified: ${isVerified}, isRateLimited: ${isRateLimited}, rateLimitVerified: ${rateLimitVerified}`);
        // 如果已有验证码且未过期，直接提示验证
        if (userState.verification_code && userState.code_expiry && nowSeconds < userState.code_expiry) {
          await sendMessageToUser(chatId, "请验证上方验证码后再发送信息。");
          await sendMessageToUser(chatId, `请验证通过后重新发送“${text}”`);
        } else {
          // 否则生成新的验证码
          await handleVerification(chatId, messageId);
          await sendMessageToUser(chatId, `请验证通过后重新发送“${text}”`);
        }
        return;
      }

      try {
        // 并行获取用户信息和话题 ID
        const [userInfo, topicId] = await Promise.all([
          getUserInfo(chatId),
          getTopicId(chatId)
        ]);

        const userName = userInfo.username || `User_${chatId}`;
        const nickname = userInfo.nickname || userName;
        const topicName = nickname;

        let finalTopicId = topicId;
        if (!finalTopicId) {
          console.log(`Chat ${chatId}: No topic ID found, creating new topic`);
          finalTopicId = await createForumTopic(topicName, userName, nickname, userInfo.id || chatId);
          if (!finalTopicId) {
            throw new Error('Failed to create forum topic');
          }
          topicIdCache.set(chatId, finalTopicId);
          await env.D1.prepare('INSERT OR REPLACE INTO chat_topic_mappings (chat_id, topic_id) VALUES (?, ?)')
            .bind(chatId, finalTopicId)
            .run();
        }

        // 验证话题 ID 是否有效
        if (!finalTopicId) {
          throw new Error('Invalid topic ID after creation');
        }

        console.log(`Chat ${chatId}: Using topic ID ${finalTopicId} for message forwarding`);

        // 记录消息内容
        console.log(`Chat ${chatId}: Forwarding message - Text: ${text || 'No text'}, Message ID: ${messageId}`);

        // 发送消息
        const formattedMessage = text ? `${nickname}:\n${text}` : null;
        await (formattedMessage ? sendMessageToTopic(finalTopicId, formattedMessage) : copyMessageToTopic(finalTopicId, message));
      } catch (error) {
        console.error(`Error handling message from chatId ${chatId}:`, error);
        if (error.message.includes('429')) {
          await sendMessageToUser(chatId, `消息“${text}”转发失败：消息发送过于频繁，请稍后再试。`);
          await sendMessageToTopic(null, `无法转发用户 ${chatId} 的消息：Telegram API 速率限制 (429)`);
        } else {
          await sendMessageToUser(chatId, `消息“${text}”转发失败：${error.message}，请稍后再试或联系管理员。`);
          await sendMessageToTopic(null, `无法转发用户 ${chatId} 的消息：${error.message}`);
        }
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
      try {
        await env.D1.prepare('DELETE FROM users WHERE chat_id = ?').bind(targetChatId).run();
        userStateCache.delete(`${targetChatId}:state`);
        await sendMessageToTopic(topicId, `用户 ${targetChatId} 的状态已重置。`);
      } catch (error) {
        console.error(`Error resetting user ${targetChatId}:`, error);
        await sendMessageToTopic(topicId, `重置用户 ${targetChatId} 失败：${error.message}`);
      }
    }

    async function sendAdminPanel(chatId, topicId, privateChatId, messageId) {
      try {
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

        const adminMessage = '管理员面板：请选择操作';
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
        console.error(`Error sending admin panel to chatId ${chatId}, topicId ${topicId}:`, error);
        await sendMessageToTopic(topicId, `发送管理员面板失败：${error.message}`);
      }
    }

    async function getVerificationSuccessMessage() {
      const userRawEnabled = settingsCache.user_raw_enabled;
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
        // 如果关闭验证码，重置所有用户的验证状态
        if (value === 'false') {
          await env.D1.prepare('UPDATE users SET is_verified = ?, verified_expiry = NULL, rate_limit_verified = ?').bind(false, false).run();
          userStateCache.forEach((userState, userStateKey) => {
            userState.is_verified = false;
            userState.verified_expiry = null;
            userState.rate_limit_verified = false;
            userStateCache.set(userStateKey, userState);
          });
        }
      } else if (key === 'user_raw_enabled') {
        settingsCache.user_raw_enabled = value === 'true';
      }
    }

    async function onCallbackQuery(callbackQuery) {
      const chatId = callbackQuery.message.chat.id.toString();
      const topicId = callbackQuery.message.message_thread_id;
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

      try {
        if (action === 'verify') {
          const [, userChatId, selectedAnswer, result] = data.split('_');
          if (userChatId !== chatId) {
            return;
          }

          const userStateKey = `${chatId}:state`;
          const userState = userStateCache.get(userStateKey) || {};
          const storedCode = userState.verification_code;
          const codeExpiry = userState.code_expiry;
          const nowSeconds = Math.floor(Date.now() / 1000);

          if (!storedCode || (codeExpiry && nowSeconds > codeExpiry)) {
            await sendMessageToUser(chatId, '验证码已过期，请重新发送消息以获取新验证码。');
            return;
          }

          if (result === 'correct') {
            const verifiedExpiry = nowSeconds + 3600 * 24;
            const isFirstVerification = userState.is_first_verification;

            if (isFirstVerification) {
              console.log(`Chat ${chatId}: First-time verification passed`);
              userState.is_verified = true;
              userState.verified_expiry = verifiedExpiry;
              userState.is_first_verification = false;
            } else {
              console.log(`Chat ${chatId}: Rate limit verification passed`);
              userState.rate_limit_verified = true;
            }

            userState.verification_code = null;
            userState.code_expiry = null;
            userState.last_verification_message_id = null;
            userStateCache.set(userStateKey, userState);

            await env.D1.prepare(
              'UPDATE users SET is_verified = ?, verified_expiry = ?, verification_code = NULL, code_expiry = NULL, last_verification_message_id = NULL, is_first_verification = ?, rate_limit_verified = ? WHERE chat_id = ?'
            ).bind(
              userState.is_verified ? 1 : 0,
              userState.verified_expiry,
              userState.is_first_verification ? 1 : 0,
              userState.rate_limit_verified ? 1 : 0,
              chatId
            ).run();

            const successMessage = await getVerificationSuccessMessage();
            await sendMessageToUser(chatId, `${successMessage}\n你好，欢迎使用私聊机器人！现在可以发送消息了。`);
          } else {
            await sendMessageToUser(chatId, '验证失败，请重新尝试。');
            await handleVerification(chatId, messageId);
          }

          // 删除验证码消息，忽略 400 错误
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
            if (error.message.includes('400')) {
              console.warn(`Failed to delete verification message (likely already deleted): ${error.message}`);
            } else {
              throw error;
            }
          }
        } else {
          const senderId = callbackQuery.from.id.toString();
          const isAdmin = await checkIfAdmin(senderId);
          if (!isAdmin) {
            await sendMessageToTopic(topicId, '只有管理员可以使用此功能。');
            await sendAdminPanel(chatId, topicId, privateChatId, messageId);
            return;
          }

          if (action === 'block') {
            const userStateKey = `${privateChatId}:state`;
            const userState = userStateCache.get(userStateKey) || {};
            userState.is_blocked = true;
            userStateCache.set(userStateKey, userState);

            await env.D1.prepare('UPDATE users SET is_blocked = ? WHERE chat_id = ?')
              .bind(true, privateChatId)
              .run();
            await sendMessageToTopic(topicId, `用户 ${privateChatId} 已被拉黑，消息将不再转发。`);
          } else if (action === 'unblock') {
            const userStateKey = `${privateChatId}:state`;
            const userState = userStateCache.get(userStateKey) || {};
            userState.is_blocked = false;
            userState.is_first_verification = true;
            userStateCache.set(userStateKey, userState);

            await env.D1.prepare('UPDATE users SET is_blocked = ?, is_first_verification = ? WHERE chat_id = ?')
              .bind(false, true, privateChatId)
              .run();
            await sendMessageToTopic(topicId, `用户 ${privateChatId} 已解除拉黑，消息将继续转发。`);
          } else if (action === 'toggle_verification') {
            const currentState = settingsCache.verification_enabled;
            const newState = !currentState;
            await setSetting('verification_enabled', newState.toString());
            await sendMessageToTopic(topicId, `验证码功能已${newState ? '开启' : '关闭'}。`);
          } else if (action === 'check_blocklist') {
            const blockedUsers = await env.D1.prepare('SELECT chat_id FROM users WHERE is_blocked = ?')
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
            try {
              await env.D1.prepare('DELETE FROM users WHERE chat_id = ?').bind(privateChatId).run();
              userStateCache.delete(`${privateChatId}:state`);
              await sendMessageToTopic(topicId, `用户 ${privateChatId} 的状态和消息记录已删除，话题保留。`);
            } catch (error) {
              console.error(`Error deleting user ${privateChatId}:`, error);
              await sendMessageToTopic(topicId, `删除用户 ${privateChatId} 失败：${error.message}`);
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
      } catch (error) {
        console.error(`Error processing callback query ${data}:`, error);
        await sendMessageToTopic(topicId, `处理操作 ${action} 失败：${error.message}`);
      }
    }

    async function handleVerification(chatId, messageId) {
      const userStateKey = `${chatId}:state`;
      const userState = userStateCache.get(userStateKey) || {};
      userState.verification_code = null;
      userState.code_expiry = null;
      userStateCache.set(userStateKey, userState);

      await env.D1.prepare('UPDATE users SET verification_code = NULL, code_expiry = NULL WHERE chat_id = ?')
        .bind(chatId)
        .run();

      const lastVerificationMessageId = userState.last_verification_message_id;
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
          if (error.message.includes('400')) {
            console.warn(`Failed to delete old verification message (likely already deleted): ${error.message}`);
          } else {
            console.error("Error deleting old verification message:", error);
          }
        }
        userState.last_verification_message_id = null;
        userStateCache.set(userStateKey, userState);
        await env.D1.prepare('UPDATE users SET last_verification_message_id = NULL WHERE chat_id = ?')
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

      const userStateKey = `${chatId}:state`;
      const userState = userStateCache.get(userStateKey) || {};
      userState.verification_code = correctResult.toString();
      userState.code_expiry = codeExpiry;
      userStateCache.set(userStateKey, userState);

      await env.D1.prepare('UPDATE users SET verification_code = ?, code_expiry = ? WHERE chat_id = ?')
        .bind(correctResult.toString(), codeExpiry, chatId)
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
          userState.last_verification_message_id = data.result.message_id.toString();
          userStateCache.set(userStateKey, userState);
          await env.D1.prepare('UPDATE users SET last_verification_message_id = ? WHERE chat_id = ?')
            .bind(data.result.message_id.toString(), chatId)
            .run();
        }
      } catch (error) {
        console.error("Error sending verification message:", error);
      }
    }

    async function checkIfAdmin(userId) {
      if (adminCache.has(userId)) {
        return adminCache.get(userId);
      }

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
        const isAdmin = data.ok && (data.result.status === 'administrator' || data.result.status === 'creator');
        adminCache.set(userId, isAdmin);
        setTimeout(() => adminCache.delete(userId), 60 * 60 * 1000); // 缓存 1 小时
        return isAdmin;
      } catch (error) {
        console.error(`Error checking admin status for user ${userId}:`, error);
        return false;
      }
    }

    async function getUserInfo(chatId) {
      if (userInfoCache.has(chatId)) {
        return userInfoCache.get(chatId);
      }

      const userInfo = {
        id: chatId,
        username: `User_${chatId}`,
        nickname: `User_${chatId}`
      };

      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getChat`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ chat_id: chatId })
        });
        const data = await response.json();
        if (data.ok) {
          const result = data.result;
          const nickname = result.first_name
            ? `${result.first_name}${result.last_name ? ` ${result.last_name}` : ''}`.trim()
            : result.username || `User_${chatId}`;
          userInfo.id = result.id || chatId;
          userInfo.username = result.username || `User_${chatId}`;
          userInfo.nickname = nickname;
        }
      } catch (error) {
        console.error(`Error fetching user info for chatId ${chatId}:`, error);
      }

      userInfoCache.set(chatId, userInfo);
      setTimeout(() => userInfoCache.delete(chatId), 24 * 60 * 60 * 1000); // 缓存 24 小时
      return userInfo;
    }

    async function getTopicId(chatId) {
      if (topicIdCache.has(chatId)) {
        const cachedTopicId = topicIdCache.get(chatId);
        console.log(`Chat ${chatId}: Found topic ID ${cachedTopicId} in cache`);
        return cachedTopicId;
      }
      const mapping = await env.D1.prepare('SELECT topic_id FROM chat_topic_mappings WHERE chat_id = ?')
        .bind(chatId)
        .first();
      const topicId = mapping?.topic_id || null;
      if (topicId) {
        console.log(`Chat ${chatId}: Found topic ID ${topicId} in database`);
        topicIdCache.set(chatId, topicId);
      } else {
        console.log(`Chat ${chatId}: No topic ID found in database`);
      }
      return topicId;
    }

    async function createForumTopic(topicName, userName, nickname, userId) {
      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/createForumTopic`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ chat_id: GROUP_ID, name: topicName })
        });
        const data = await response.json();
        if (!data.ok) {
          console.error(`Failed to create forum topic for user ${userId}: ${data.description}`);
          throw new Error(`Failed to create forum topic: ${data.description}`);
        }
        const topicId = data.result.message_thread_id;
        console.log(`Chat ${userId}: Created new topic ID ${topicId}`);

        const now = new Date();
        const formattedTime = now.toISOString().replace('T', ' ').substring(0, 19);
        const notificationContent = await getNotificationContent();
        const pinnedMessage = `昵称: ${nickname}\n用户名: @${userName}\nUserID: ${userId}\n发起时间: ${formattedTime}\n\n${notificationContent}`;
        const messageResponse = await sendMessageToTopic(topicId, pinnedMessage);
        await pinMessage(topicId, messageResponse.result.message_id);

        return topicId;
      } catch (error) {
        console.error(`Error creating forum topic for user ${userId}:`, error);
        throw error;
      }
    }

    async function forwardMessageToPrivateChat(privateChatId, message) {
      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/copyMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: privateChatId,
            from_chat_id: message.chat.id,
            message_id: message.message_id,
            disable_notification: true
          })
        });
        const data = await response.json();
        if (!data.ok) throw new Error(`Failed to forward message to private chat: ${data.description}`);
      } catch (error) {
        console.error(`Error forwarding message to private chat ${privateChatId}:`, error);
        if (error.message.includes('429')) {
          throw new Error('Telegram API 速率限制 (429)');
        } else {
          throw error;
        }
      }
    }

    async function sendMessageToTopic(topicId, text) {
      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: GROUP_ID,
            text: text,
            message_thread_id: topicId
          })
        });
        const data = await response.json();
        if (!data.ok) {
          console.error(`Failed to send message to topic ${topicId}: ${data.description}`);
          throw new Error(`Failed to send message to topic: ${data.description}`);
        }
        return data;
      } catch (error) {
        console.error(`Error in sendMessageToTopic for topic ${topicId}:`, error);
        throw error;
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
            disable_notification: true
          })
        });
        const data = await response.json();
        if (!data.ok) {
          console.error(`Failed to copy message to topic ${topicId}: ${data.description}`);
          throw new Error(`Failed to copy message to topic: ${data.description}`);
        }
      } catch (error) {
        console.error(`Error in copyMessageToTopic for topic ${topicId}:`, error);
        throw error;
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
          console.error(`Failed to pin message in topic ${topicId}: ${data.description}`);
          throw new Error(`Failed to pin message: ${data.description}`);
        }
      } catch (error) {
        console.error(`Error in pinMessage for topic ${topicId}:`, error);
        throw error;
      }
    }

    async function sendMessageToUser(chatId, text) {
      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: chatId,
            text: text
          })
        });
        const data = await response.json();
        if (!data.ok) throw new Error(`Failed to send message to user: ${data.description}`);
      } catch (error) {
        console.error(`Error sending message to user ${chatId}:`, error);
        if (error.message.includes('429')) {
          throw new Error('Telegram API 速率限制 (429)');
        } else {
          throw error;
        }
      }
    }

    async function getPrivateChatId(topicId) {
      for (const [chatId, cachedTopicId] of topicIdCache.entries()) {
        if (cachedTopicId === topicId.toString()) {
          return chatId;
        }
      }
      const mapping = await env.D1.prepare('SELECT chat_id FROM chat_topic_mappings WHERE topic_id = ?')
        .bind(topicId)
        .first();
      const chatId = mapping?.chat_id || null;
      if (chatId) {
        topicIdCache.set(chatId, topicId.toString());
      }
      return chatId;
    }

    async function fetchWithRetry(url, options) {
      const maxRetries = 3;
      let lastError;

      for (let i = 0; i <= maxRetries; i++) {
        try {
          const response = await fetch(url, options);
          if (!response.ok) {
            if (response.status === 429) {
              await new Promise(resolve => setTimeout(resolve, 5000 * (i + 1)));
              if (i === maxRetries) {
                throw new Error('Request failed with status 429');
              }
              continue;
            }
            throw new Error(`Request failed with status ${response.status}`);
          }
          return response;
        } catch (error) {
          lastError = error;
          if (i === maxRetries) break;
          await new Promise(resolve => setTimeout(resolve, 1000 * (i + 1)));
        }
      }
      throw lastError;
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

    async function checkAndRepairTables(d1) {
      const expectedTables = {
        users: {
          columns: {
            chat_id: 'TEXT PRIMARY KEY',
            is_blocked: 'BOOLEAN DEFAULT FALSE',
            is_verified: 'BOOLEAN DEFAULT FALSE',
            verified_expiry: 'INTEGER',
            verification_code: 'TEXT',
            code_expiry: 'INTEGER',
            last_verification_message_id: 'TEXT',
            is_first_verification: 'BOOLEAN DEFAULT TRUE',
            message_count: 'INTEGER DEFAULT 0',
            window_start: 'INTEGER',
            start_count: 'INTEGER DEFAULT 0',
            start_window_start: 'INTEGER',
            rate_limit_verified: 'BOOLEAN DEFAULT FALSE',
            rate_limit_window_start: 'INTEGER'
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
          const columnsDef = Object.entries(structure.columns)
            .map(([name, def]) => `${name} ${def}`)
            .join(', ');
          const createSQL = `CREATE TABLE ${tableName} (${columnsDef})`;
          await d1.exec(createSQL);
        } else {
          const columns = Object.keys(structure.columns);
          const existingColumns = await d1.prepare(
            `PRAGMA table_info(${tableName})`
          ).all().then(res => res.results.map(col => col.name));

          for (const column of columns) {
            if (!existingColumns.includes(column)) {
              const def = structure.columns[column];
              await d1.exec(`ALTER TABLE ${tableName} ADD COLUMN ${column} ${def.split(' ').slice(1).join(' ')}`);
            }
          }
        }

        if (tableName === 'chat_topic_mappings') {
          await d1.exec('CREATE INDEX IF NOT EXISTS idx_chat_topic_mappings_chat_id ON chat_topic_mappings (chat_id)');
          await d1.exec('CREATE INDEX IF NOT EXISTS idx_chat_topic_mappings_topic_id ON chat_topic_mappings (topic_id)');
        }
        if (tableName === 'settings') {
          await d1.exec('CREATE INDEX IF NOT EXISTS idx_settings_key ON settings (key)');
        }
      }

      await d1.prepare('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)')
        .bind('verification_enabled', 'true').run();
      await d1.prepare('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)')
        .bind('user_raw_enabled', 'true').run();
    }

    async function cleanExpiredVerificationCodes() {
      const now = Date.now();
      if (now - lastCleanupTime < CLEANUP_INTERVAL) {
        return;
      }

      const nowSeconds = Math.floor(now / 1000);
      const expiredCodes = await env.D1.prepare(
        'SELECT chat_id FROM users WHERE code_expiry IS NOT NULL AND code_expiry < ?'
      ).bind(nowSeconds).all();

      if (expiredCodes.results.length > 0) {
        await env.D1.batch(
          expiredCodes.results.map(({ chat_id }) =>
            env.D1.prepare(
              'UPDATE users SET verification_code = NULL, code_expiry = NULL WHERE chat_id = ?'
            ).bind(chat_id)
          )
        );
        expiredCodes.results.forEach(({ chat_id }) => {
          const userStateKey = `${chat_id}:state`;
          const userState = userStateCache.get(userStateKey);
          if (userState) {
            userState.verification_code = null;
            userState.code_expiry = null;
            userStateCache.set(userStateKey, userState);
          }
        });
      }
      lastCleanupTime = now;
    }

    await cleanExpiredVerificationCodes();

    try {
      return await handleRequest(request);
    } catch (error) {
      console.error('Unhandled error in fetch handler:', error);
      return new Response('Internal Server Error', { status: 500 });
    }
  }
};
