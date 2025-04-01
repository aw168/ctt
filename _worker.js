let BOT_TOKEN;
let GROUP_ID;
let MAX_MESSAGES_PER_MINUTE;

let lastCleanupTime = 0;
const CLEANUP_INTERVAL = 24 * 60 * 60 * 1000; // 24 小时
let isInitialized = false;
const processedMessages = new Set();
const processedCallbacks = new Set();

// 用于实现锁机制的 Map
const topicCreationLocks = new Map();

const settingsCache = new Map([
  ['verification_enabled', null],
  ['user_raw_enabled', null]
]);

class LRUCache {
  constructor(maxSize) {
    this.maxSize = maxSize;
    this.cache = new Map();
  }
  get(key) {
    const value = this.cache.get(key);
    if (value !== undefined) {
      this.cache.delete(key);
      this.cache.set(key, value);
    }
    return value;
  }
  set(key, value) {
    if (this.cache.size >= this.maxSize) {
      const firstKey = this.cache.keys().next().value;
      this.cache.delete(firstKey);
    }
    this.cache.set(key, value);
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
      console.error('D1 database is not bound');
      return new Response('Server configuration error: D1 database is not bound', { status: 500 });
    }

    if (!isInitialized) {
      await initialize(env.D1, request);
      isInitialized = true;
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

    async function initialize(d1, request) {
      await Promise.all([
        checkAndRepairTables(d1),
        autoRegisterWebhook(request),
        checkBotPermissions(),
        cleanExpiredVerificationCodes(d1)
      ]);
    }

    async function autoRegisterWebhook(request) {
      const webhookUrl = `${new URL(request.url).origin}/webhook`;
      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/setWebhook`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ url: webhookUrl }),
        }).then(r => r.json());
        if (!response.ok) {
          console.error('Webhook auto-registration failed:', JSON.stringify(response, null, 2));
        }
      } catch (error) {
        console.error('Error during webhook auto-registration:', error);
      }
    }

    async function checkBotPermissions() {
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
        const canManageTopics = memberData.result.can_manage_topics !== false;
        if (!canSendMessages || !canPostMessages || !canManageTopics) {
          console.error('Bot lacks necessary permissions in the group:', {
            canSendMessages,
            canPostMessages,
            canManageTopics
          });
        }
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
          try {
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
          } catch (error) {
            console.error(`Error checking ${tableName}:`, error);
            await d1.exec(`DROP TABLE IF EXISTS ${tableName}`);
            await createTable(d1, tableName, structure);
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
    }

    async function cleanExpiredVerificationCodes(d1) {
      const now = Date.now();
      if (now - lastCleanupTime < CLEANUP_INTERVAL) {
        return;
      }

      try {
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

      console.log(`Processing message from chatId ${chatId}: ${text}`);

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

      // 强制从数据库读取最新的用户状态
      let userState = await env.D1.prepare('SELECT is_blocked, is_first_verification, is_verified, verified_expiry, is_verifying FROM user_states WHERE chat_id = ?')
        .bind(chatId)
        .first();
      if (!userState) {
        await env.D1.prepare('INSERT INTO user_states (chat_id, is_blocked, is_first_verification, is_verified, is_verifying) VALUES (?, ?, ?, ?, ?)')
          .bind(chatId, false, true, false, false)
          .run();
        userState = { is_blocked: false, is_first_verification: true, is_verified: false, verified_expiry: null, is_verifying: false };
      }
      userStateCache.set(chatId, userState);
      console.log(`User state for chatId ${chatId}: ${JSON.stringify(userState)}`);

      const isBlocked = userState.is_blocked || false;
      if (isBlocked) {
        await sendMessageToUser(chatId, "您已被拉黑，无法发送消息。请联系管理员解除拉黑。");
        console.log(`User ${chatId} is blocked, message not forwarded`);
        return;
      }

      const verificationEnabled = (await getSetting('verification_enabled', env.D1)) === 'true';
      console.log(`Verification enabled: ${verificationEnabled} for chatId ${chatId}`);

      // 如果验证码功能关闭，跳过所有验证逻辑
      if (!verificationEnabled) {
        console.log(`Verification is disabled, skipping verification checks for chatId ${chatId}`);
      } else {
        const nowSeconds = Math.floor(Date.now() / 1000);
        const isVerified = userState.is_verified && userState.verified_expiry && nowSeconds < userState.verified_expiry;
        const isFirstVerification = userState.is_first_verification;
        const isRateLimited = await checkMessageRate(chatId);
        const isVerifying = userState.is_verifying || false;

        console.log(`User ${chatId} verification status: isVerified=${isVerified}, isFirstVerification=${isFirstVerification}, isRateLimited=${isRateLimited}, isVerifying=${isVerifying}`);

        if (!isVerified || (isRateLimited && !isFirstVerification)) {
          if (isVerifying) {
            await sendMessageToUser(chatId, `请完成验证后发送消息“${text || '您的具体信息'}”。`);
            console.log(`User ${chatId} is verifying, message not forwarded`);
            return;
          }
          await sendMessageToUser(chatId, `请完成验证后发送消息“${text || '您的具体信息'}”。`);
          await handleVerification(chatId, messageId);
          console.log(`User ${chatId} needs verification, message not forwarded`);
          return;
        }
      }

      if (text === '/start') {
        if (await checkStartCommandRate(chatId)) {
          await sendMessageToUser(chatId, "您发送 /start 命令过于频繁，请稍后再试！");
          console.log(`User ${chatId} sent /start too frequently`);
          return;
        }

        const successMessage = await getVerificationSuccessMessage();
        await sendMessageToUser(chatId, `${successMessage}\n你好，欢迎使用私聊机器人，现在发送信息吧！`);
        const userInfo = await getUserInfo(chatId);
        await ensureUserTopic(chatId, userInfo);
        console.log(`User ${chatId} sent /start, welcome message sent`);
        return;
      }

      try {
        const userInfo = await getUserInfo(chatId);
        if (!userInfo) {
          throw new Error(`Failed to fetch user info for chatId ${chatId}`);
        }
        console.log(`User info for chatId ${chatId}: ${JSON.stringify(userInfo)}`);

        let topicId = await ensureUserTopic(chatId, userInfo);
        if (!topicId) {
          throw new Error(`Failed to ensure topic for chatId ${chatId}`);
        }

        const isTopicValid = await validateTopic(topicId);
        if (!isTopicValid) {
          console.log(`Topic ${topicId} is invalid for chatId ${chatId}, recreating topic...`);
          await env.D1.prepare('DELETE FROM chat_topic_mappings WHERE chat_id = ?').bind(chatId).run();
          topicIdCache.set(chatId, undefined);
          topicId = await ensureUserTopic(chatId, userInfo);
          if (!topicId) {
            throw new Error(`Failed to recreate topic for chatId ${chatId}`);
          }
        }

        const userName = userInfo.username || `User_${chatId}`;
        const nickname = userInfo.nickname || userName;

        try {
          if (text) {
            const formattedMessage = `${nickname}:\n${text}`;
            await sendMessageToTopic(topicId, formattedMessage);
            console.log(`Message forwarded to topic ${topicId} for chatId ${chatId}: ${formattedMessage}`);
          } else {
            await copyMessageToTopic(topicId, message);
            console.log(`Non-text message forwarded to topic ${topicId} for chatId ${chatId}`);
          }
        } catch (error) {
          console.error(`Error sending message to topic ${topicId}:`, error);
          throw error;
        }
      } catch (error) {
        console.error(`Error handling message from chatId ${chatId}:`, error);
        await sendMessageToTopic(null, `无法转发用户 ${chatId} 的消息：${error.message}`);
        await sendMessageToUser(chatId, "消息转发失败，请稍后再试或联系管理员。");
      }
    }

    async function validateTopic(topicId) {
      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: GROUP_ID,
            message_thread_id: topicId,
            text: "测试消息，用于验证话题有效性",
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
          console.log(`Topic ${topicId} is valid`);
          return true;
        } else {
          console.log(`Topic ${topicId} is invalid: ${data.description}`);
          return false;
        }
      } catch (error) {
        console.error(`Error validating topic ${topicId}:`, error);
        return false;
      }
    }

    async function ensureUserTopic(chatId, userInfo) {
      let lock = topicCreationLocks.get(chatId);
      if (!lock) {
        lock = Promise.resolve();
        topicCreationLocks.set(chatId, lock);
      }

      try {
        await lock;

        let topicId = await getExistingTopicId(chatId, 3, 1000); // 重试 3 次，每次间隔 1 秒
        if (topicId) {
          console.log(`Topic ${topicId} already exists for chatId ${chatId}, using existing topic.`);
          return topicId;
        }

        const newLock = (async () => {
          console.log(`Creating new topic for chatId ${chatId}...`);
          const userName = userInfo.username || `User_${chatId}`;
          const nickname = userInfo.nickname || userName;
          topicId = await createForumTopic(nickname, userName, nickname, userInfo.id || chatId);
          await saveTopicId(chatId, topicId);
          console.log(`Created topic ${topicId} for chatId ${chatId}`);
          return topicId;
        })();

        topicCreationLocks.set(chatId, newLock);
        return await newLock;
      } catch (error) {
        console.error(`Error ensuring topic for chatId ${chatId}:`, error);
        throw error;
      } finally {
        if (topicCreationLocks.get(chatId) === lock) {
          topicCreationLocks.delete(chatId);
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
        await env.D1.batch([
          env.D1.prepare('DELETE FROM user_states WHERE chat_id = ?').bind(targetChatId),
          env.D1.prepare('DELETE FROM message_rates WHERE chat_id = ?').bind(targetChatId),
          env.D1.prepare('DELETE FROM chat_topic_mappings WHERE chat_id = ?').bind(targetChatId)
        ]);
        userStateCache.set(targetChatId, undefined);
        messageRateCache.set(targetChatId, undefined);
        topicIdCache.set(targetChatId, undefined);
        await sendMessageToTopic(topicId, `用户 ${targetChatId} 的状态已重置。`);
      } catch (error) {
        console.error(`Error resetting user ${targetChatId}:`, error);
        await sendMessageToTopic(topicId, `重置用户 ${targetChatId} 失败：${error.message}`);
      }
    }

    async function sendAdminPanel(chatId, topicId, privateChatId, messageId) {
      try {
        const verificationEnabled = (await getSetting('verification_enabled', env.D1)) === 'true';
        const userRawEnabled = (await getSetting('user_raw_enabled', env.D1)) === 'true';

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
          }).catch(error => console.error(`Error deleting message ${messageId}:`, error))
        ]);
      } catch (error) {
        console.error(`Error sending admin panel to chatId ${chatId}, topicId ${topicId}:`, error);
      }
    }

    async function getVerificationSuccessMessage() {
      const userRawEnabled = (await getSetting('user_raw_enabled', env.D1)) === 'true';
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
      try {
        const result = await d1.prepare('SELECT value FROM settings WHERE key = ?')
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
        if (key === 'verification_enabled') {
          settingsCache.set('verification_enabled', value === 'true');
          if (value === 'false') {
            const nowSeconds = Math.floor(Date.now() / 1000);
            const verifiedExpiry = nowSeconds + 3600 * 24;
            await env.D1.prepare('UPDATE user_states SET is_verified = ?, verified_expiry = ?, is_verifying = ?, verification_code = NULL, code_expiry = NULL, is_first_verification = ?')
              .bind(true, verifiedExpiry, false, false)
              .run();
            userStateCache.clear(); // 清除缓存，确保后续读取最新状态
            console.log('Verification disabled, set all users to verified and cleared userStateCache');
          } else {
            console.log('Verification enabled, user states unchanged');
          }
        } else if (key === 'user_raw_enabled') {
          settingsCache.set('user_raw_enabled', value === 'true');
        }
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

          let verificationState = await env.D1.prepare('SELECT verification_code, code_expiry, is_verifying FROM user_states WHERE chat_id = ?')
            .bind(chatId)
            .first();
          if (!verificationState) {
            verificationState = { verification_code: null, code_expiry: null, is_verifying: false };
          }
          userStateCache.set(chatId, verificationState);

          const storedCode = verificationState.verification_code;
          const codeExpiry = verificationState.code_expiry;
          const nowSeconds = Math.floor(Date.now() / 1000);

          if (!storedCode || (codeExpiry && nowSeconds > codeExpiry)) {
            await sendMessageToUser(chatId, '验证码已过期，请重新发送消息以获取新验证码。');
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
            } catch (deleteError) {
              console.error(`Error deleting expired verification message: ${deleteError.message}`);
            }
            return;
          }

          if (result === 'correct') {
            const verifiedExpiry = nowSeconds + 3600 * 24;
            await env.D1.prepare('UPDATE user_states SET is_verified = ?, verified_expiry = ?, verification_code = NULL, code_expiry = NULL, last_verification_message_id = NULL, is_first_verification = ?, is_verifying = ? WHERE chat_id = ?')
              .bind(true, verifiedExpiry, false, false, chatId)
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

          try {
            await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                chat_id: chatId,
                message_id: messageId
              })
            });
          } catch (deleteError) {
            console.error(`Error deleting verification message: ${deleteError.message}`);
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
            let state = await env.D1.prepare('SELECT is_blocked FROM user_states WHERE chat_id = ?')
              .bind(privateChatId)
              .first() || { is_blocked: false };
            state.is_blocked = true;
            userStateCache.set(privateChatId, state);
            await env.D1.prepare('INSERT OR REPLACE INTO user_states (chat_id, is_blocked) VALUES (?, ?)')
              .bind(privateChatId, true)
              .run();
            await sendMessageToTopic(topicId, `用户 ${privateChatId} 已被拉黑，消息将不再转发。`);
          } else if (action === 'unblock') {
            let state = await env.D1.prepare('SELECT is_blocked, is_first_verification FROM user_states WHERE chat_id = ?')
              .bind(privateChatId)
              .first() || { is_blocked: false, is_first_verification: true };
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
            settingsCache.set('verification_enabled', newState);
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
            settingsCache.set('user_raw_enabled', newState);
            await sendMessageToTopic(topicId, `用户端 Raw 链接已${newState ? '开启' : '关闭'}。`);
          } else if (action === 'delete_user') {
            try {
              userStateCache.set(privateChatId, undefined);
              messageRateCache.set(privateChatId, undefined);
              topicIdCache.set(privateChatId, undefined);
              await env.D1.batch([
                env.D1.prepare('DELETE FROM user_states WHERE chat_id = ?').bind(privateChatId),
                env.D1.prepare('DELETE FROM message_rates WHERE chat_id = ?').bind(privateChatId),
                env.D1.prepare('DELETE FROM chat_topic_mappings WHERE chat_id = ?').bind(privateChatId)
              ]);
              await sendMessageToTopic(topicId, `用户 ${privateChatId} 的状态、消息记录和话题映射已删除，用户需重新发起会话。`);
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
        } catch (error) {
          console.error("Error deleting old verification message:", error);
        }
        userState.last_verification_message_id = null;
        userStateCache.set(chatId, userState);
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
          userStateCache.set(chatId, userState);
          await env.D1.prepare('UPDATE user_states SET verification_code = ?, code_expiry = ?, last_verification_message_id = ?, is_verifying = ? WHERE chat_id = ?')
            .bind(correctResult.toString(), codeExpiry, data.result.message_id.toString(), true, chatId)
            .run();
        }
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
        return data.ok && (data.result.status === 'administrator' || data.result.status === 'creator');
      } catch (error) {
        console.error(`Error checking admin status for user ${userId}:`, error);
        return false;
      }
    }

    async function getUserInfo(chatId) {
      let userInfo = userInfoCache.get(chatId);
      if (userInfo !== undefined) {
        return userInfo;
      }

      try {
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
      } catch (error) {
        console.error(`Error fetching user info for chatId ${chatId}:`, error);
        userInfo = {
          id: chatId,
          username: `User_${chatId}`,
          nickname: `User_${chatId}`
        };
      }

      userInfoCache.set(chatId, userInfo);
      return userInfo;
    }

    async function getExistingTopicId(chatId, retries = 3, delay = 1000) {
      let topicId = topicIdCache.get(chatId);
      if (topicId !== undefined) {
        console.log(`Found topicId ${topicId} in cache for chatId ${chatId}`);
        return topicId;
      }

      for (let i = 0; i < retries; i++) {
        try {
          const result = await env.D1.prepare('SELECT topic_id FROM chat_topic_mappings WHERE chat_id = ?')
            .bind(chatId)
            .first();
          topicId = result?.topic_id || null;
          if (topicId) {
            console.log(`Found topicId ${topicId} in database for chatId ${chatId} on attempt ${i + 1}`);
            topicIdCache.set(chatId, topicId);
            return topicId;
          } else {
            console.log(`No topicId found in database for chatId ${chatId} on attempt ${i + 1}`);
          }
        } catch (error) {
          console.error(`Error fetching topicId for chatId ${chatId} from database on attempt ${i + 1}:`, error);
        }

        if (i < retries - 1) {
          console.log(`Retrying to fetch topicId for chatId ${chatId} after ${delay}ms...`);
          await new Promise(resolve => setTimeout(resolve, delay));
        }
      }

      console.log(`Failed to fetch topicId for chatId ${chatId} after ${retries} attempts`);
      return null;
    }

    async function createForumTopic(topicName, userName, nickname, userId) {
      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/createForumTopic`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ chat_id: GROUP_ID, name: `CTTBOT: ${nickname}` })
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
        topicIdCache.set(chatId, topicId);
        console.log(`Saved topicId ${topicId} for chatId ${chatId} to database and cache`);
      } catch (error) {
        console.error(`Error saving topicId ${topicId} for chatId ${chatId}:`, error);
        throw error;
      }
    }

    async function getPrivateChatId(topicId) {
      for (const [chatId, tid] of topicIdCache.cache) if (tid === topicId) return chatId;
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
        throw new Error('Message text is empty');
      }

      try {
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
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/pinChatMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(requestBody)
        });
        const data = await response.json();
        if (!data.ok) {
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
      } catch (error) {
        console.error(`Error forwarding message to private chat ${privateChatId}:`, error);
        throw error;
      }
    }

    async function sendMessageToUser(chatId, text) {
      try {
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
      } catch (error) {
        console.error(`Error sending message to user ${chatId}:`, error);
        throw error;
      }
    }

    async function fetchWithRetry(url, options, retries = 3, backoff = 1000) {
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
            await new Promise(resolve => setTimeout(resolve, delay));
            continue;
          }
          throw new Error(`Request failed with status ${response.status}: ${await response.text()}`);
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
