// Global variables and constants
let BOT_TOKEN;
let GROUP_ID;
let MAX_MESSAGES_PER_MINUTE;

const PROCESSED_MESSAGE_TTL = 60 * 60 * 1000; // 1 hour in milliseconds

// In-memory stores for frequently accessed data
const processedMessages = new Map(); // Tracks processed message IDs to prevent duplicates
const processedCallbacks = new Map(); // Tracks processed callback query IDs

const settingsCache = new Map([ // Cache for global settings
  ['verification_enabled', null],
  ['user_raw_enabled', null]
]);

// Simple LRU Cache implementation
class LRUCache {
  constructor(maxSize) {
    this.maxSize = maxSize;
    this.cache = new Map();
  }
  get(key) {
    const value = this.cache.get(key);
    if (value !== undefined) {
      // Move accessed item to the end to mark as recently used
      this.cache.delete(key);
      this.cache.set(key, value);
    }
    return value;
  }
  set(key, value) {
    if (this.cache.has(key)) {
      // If key exists, delete it first to update its position
      this.cache.delete(key);
    }
    if (this.cache.size >= this.maxSize) {
      // Evict the least recently used item (first key in map iteration)
      const firstKey = this.cache.keys().next().value;
      this.cache.delete(firstKey);
    }
    this.cache.set(key, value);
  }
  delete(key) {
    this.cache.delete(key);
  }
}

// Initialize LRU caches for user-specific data
const userInfoCache = new LRUCache(1000); // Cache user profile info
const topicIdCache = new LRUCache(1000); // Cache chat_id -> topic_id mappings
const userStateCache = new LRUCache(1000); // Cache user states (blocked, verified, etc.)
const messageRateCache = new LRUCache(1000); // Cache message rate limiting data

let isInitialized = false; // Flag to ensure initialization runs only once

export default {
  /**
   * Main Cloudflare Worker entry point.
   * Handles incoming requests.
   */
  async fetch(request, env) {
    // Load environment variables
    BOT_TOKEN = env.BOT_TOKEN_ENV || null;
    GROUP_ID = env.GROUP_ID_ENV || null;
    MAX_MESSAGES_PER_MINUTE = env.MAX_MESSAGES_PER_MINUTE_ENV ? parseInt(env.MAX_MESSAGES_PER_MINUTE_ENV) : 40;

    // Ensure D1 binding is present
    if (!env.D1) {
      console.error('D1 database is not bound');
      return new Response('Server configuration error: D1 database is not bound', { status: 500 });
    }

    // Run initialization tasks on the first request
    if (!isInitialized) {
      await initialize(env.D1, request);
      isInitialized = true;
    }

    // Route the request based on the URL path
    return await handleRequest(request, env.D1);
  }
};

// --- Request Handling ---

/**
 * Routes incoming requests to the appropriate handler function.
 */
async function handleRequest(request, d1) {
  if (!BOT_TOKEN || !GROUP_ID) {
    console.error('Missing required environment variables');
    return new Response('Server configuration error: Missing required environment variables', { status: 500 });
  }

  const url = new URL(request.url);
  try {
    if (url.pathname === '/webhook') {
      const update = await request.json();
      await handleUpdate(update, d1);
      return new Response('OK');
    } else if (url.pathname === '/registerWebhook') {
      return await registerWebhook(request);
    } else if (url.pathname === '/unRegisterWebhook') {
      return await unRegisterWebhook();
    } else if (url.pathname === '/checkTables') {
      await checkAndRepairTables(d1);
      return new Response('Database tables checked and repaired', { status: 200 });
    }
    return new Response('Not Found', { status: 404 });
  } catch (error) {
    console.error(`Error handling request for ${url.pathname}:`, error);
    if (error instanceof SyntaxError) { // Handle JSON parsing errors specifically
      return new Response('Bad Request: Invalid JSON', { status: 400 });
    }
    return new Response('Internal Server Error', { status: 500 });
  }
}

/**
 * Processes incoming updates from Telegram (messages or callback queries).
 */
async function handleUpdate(update, d1) {
  if (update.message) {
    const messageId = update.message.message_id.toString();
    const chatId = update.message.chat.id.toString();
    const messageKey = `${chatId}:${messageId}`;

    if (isMessageProcessed(messageKey)) {
      console.log(`Skipping already processed message: ${messageKey}`);
      return;
    }
    addProcessedMessage(messageKey);
    await onMessage(update.message, d1);

  } else if (update.callback_query) {
    const chatId = update.callback_query.message.chat.id.toString();
    const callbackId = update.callback_query.id;
    const callbackKey = `${chatId}:${callbackId}`;

    if (isCallbackProcessed(callbackKey)) {
      console.log(`Skipping already processed callback: ${callbackKey}`);
      return;
    }
    addProcessedCallback(callbackKey);
    await onCallbackQuery(update.callback_query, d1);
  }
}

// --- Initialization and Setup ---

/**
 * Performs initial setup tasks when the worker starts.
 */
async function initialize(d1, request) {
  console.log("Initializing worker...");
  try {
    await Promise.all([
      checkAndRepairTables(d1),
      autoRegisterWebhook(request),
      checkBotPermissions(),
      cleanExpiredVerificationCodes(d1) // Initial cleanup
    ]);
    // Start periodic cleanup task (run every hour)
    setInterval(() => {
        cleanupProcessedMessages();
        cleanupProcessedCallbacks();
        cleanExpiredVerificationCodes(d1);
    }, 60 * 60 * 1000);
    console.log("Worker initialization complete.");
  } catch (error) {
    console.error("Initialization failed:", error);
    // Depending on the error, you might want to prevent the worker from starting
  }
}

/**
 * Automatically registers the webhook URL with Telegram.
 */
async function autoRegisterWebhook(request) {
  const webhookUrl = `${new URL(request.url).origin}/webhook`;
  console.log(`Attempting to auto-register webhook: ${webhookUrl}`);
  try {
    const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/setWebhook`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url: webhookUrl }),
    }).then(r => r.json());
    if (!response.ok) {
      console.error('Webhook auto-registration failed:', JSON.stringify(response, null, 2));
    } else {
      console.log('Webhook auto-registration successful.');
    }
  } catch (error) {
    console.error('Error during webhook auto-registration:', error);
  }
}

/**
 * Checks if the bot has the necessary permissions in the target group.
 */
async function checkBotPermissions() {
  console.log("Checking bot permissions...");
  try {
    // Check if bot can access the group chat info
    const chatResponse = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getChat`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: GROUP_ID })
    });
    const chatData = await chatResponse.json();
    if (!chatData.ok) {
      throw new Error(`Failed to access group: ${chatData.description}`);
    }
    console.log("Group access confirmed.");

    // Check bot's member status and permissions within the group
    const botId = await getBotId();
    const memberResponse = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getChatMember`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: GROUP_ID, user_id: botId })
    });
    const memberData = await memberResponse.json();
    if (!memberData.ok) {
      throw new Error(`Failed to get bot member status: ${memberData.description}`);
    }
    console.log("Bot member status retrieved.");

    // Verify required permissions
    const result = memberData.result;
    const permissions = {
        canSendMessages: result.can_send_messages !== false,
        canPostMessages: result.can_post_messages !== false, // Important for topics
        canManageTopics: result.can_manage_topics !== false, // Needed to create/manage topics
        canPinMessages: result.can_pin_messages !== false, // Needed for pinning info
        canDeleteMessages: result.can_delete_messages !== false // Useful for admin commands
    };

    const missingPermissions = Object.entries(permissions)
                                    .filter(([_, value]) => !value)
                                    .map(([key, _]) => key);

    if (missingPermissions.length > 0) {
      console.error('Bot lacks necessary permissions in the group:', missingPermissions);
    } else {
      console.log("Bot has necessary permissions.");
    }

  } catch (error) {
    console.error('Error checking bot permissions:', error);
    // Depending on severity, might want to throw or handle differently
  }
}

/**
 * Retrieves the bot's own user ID.
 */
async function getBotId() {
  try {
    const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getMe`);
    const data = await response.json();
    if (!data.ok) throw new Error(`Failed to get bot ID: ${data.description}`);
    return data.result.id;
  } catch (error) {
    console.error("Error fetching bot ID:", error);
    throw error; // Re-throw as this is critical
  }
}

// --- Database Management ---

/**
 * Checks if database tables exist and have the correct schema, repairing if necessary.
 */
async function checkAndRepairTables(d1) {
  console.log("Checking and repairing database tables...");
  try {
    const expectedTables = {
      // Defines the expected structure for each table
      user_states: {
        columns: {
          chat_id: 'TEXT PRIMARY KEY',
          is_blocked: 'BOOLEAN DEFAULT FALSE',
          is_verified: 'BOOLEAN DEFAULT FALSE',
          verified_expiry: 'INTEGER', // Store as Unix timestamp (seconds)
          verification_code: 'TEXT',
          code_expiry: 'INTEGER', // Store as Unix timestamp (seconds)
          last_verification_message_id: 'TEXT',
          is_first_verification: 'BOOLEAN DEFAULT TRUE',
          is_rate_limited: 'BOOLEAN DEFAULT FALSE', // Consider if still needed
          is_verifying: 'BOOLEAN DEFAULT FALSE'
        }
      },
      message_rates: {
        columns: {
          chat_id: 'TEXT PRIMARY KEY',
          message_count: 'INTEGER DEFAULT 0',
          window_start: 'INTEGER', // Store as Unix timestamp (milliseconds)
          start_count: 'INTEGER DEFAULT 0',
          start_window_start: 'INTEGER' // Store as Unix timestamp (milliseconds)
        }
      },
      chat_topic_mappings: {
        columns: {
          chat_id: 'TEXT PRIMARY KEY',
          topic_id: 'TEXT NOT NULL'
        }
        // Consider adding an index on topic_id if getPrivateChatId is frequent
        // indices: { idx_topic_id: 'ON chat_topic_mappings (topic_id)' }
      },
      settings: {
        columns: {
          key: 'TEXT PRIMARY KEY',
          value: 'TEXT'
        },
        indices: { idx_settings_key: 'ON settings (key)' } // Index for faster lookups
      }
    };

    for (const [tableName, structure] of Object.entries(expectedTables)) {
      try {
        // Check if table exists
        const tableInfo = await d1.prepare(
          `SELECT sql FROM sqlite_master WHERE type='table' AND name=?`
        ).bind(tableName).first();

        if (!tableInfo) {
          console.log(`Table ${tableName} not found. Creating...`);
          await createTable(d1, tableName, structure);
          continue; // Move to the next table
        }

        // Check and add missing columns
        const columnsResult = await d1.prepare(
          `PRAGMA table_info(${tableName})`
        ).all();

        const currentColumns = new Set(columnsResult.results.map(col => col.name));

        for (const [colName, colDef] of Object.entries(structure.columns)) {
          if (!currentColumns.has(colName)) {
            console.log(`Adding missing column ${colName} to table ${tableName}...`);
            const columnParts = colDef.split(' ');
            // Ensure proper syntax for ADD COLUMN
            const addColumnSQL = `ALTER TABLE ${tableName} ADD COLUMN ${colName} ${columnParts.slice(0).join(' ')}`;
            await d1.exec(addColumnSQL);
          }
        }

        // Check and create missing indices
        if (structure.indices) {
          for (const [indexName, indexDef] of Object.entries(structure.indices)) {
             const indexInfo = await d1.prepare(
               `SELECT name FROM sqlite_master WHERE type='index' AND name=?`
             ).bind(indexName).first();
             if (!indexInfo) {
                console.log(`Creating missing index ${indexName} on table ${tableName}...`);
                await d1.exec(`CREATE INDEX IF NOT EXISTS ${indexName} ${indexDef}`);
             }
          }
        }

      } catch (error) {
        console.error(`Error checking/repairing table ${tableName}:`, error, `Attempting to recreate table.`);
        // If altering fails, drop and recreate (potential data loss warning)
        await d1.exec(`DROP TABLE IF EXISTS ${tableName}`);
        await createTable(d1, tableName, structure);
      }
    }

    // Ensure default settings exist
    await Promise.all([
      d1.prepare('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)')
        .bind('verification_enabled', 'true').run(),
      d1.prepare('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)')
        .bind('user_raw_enabled', 'true').run()
    ]);

    // Pre-populate settings cache
    settingsCache.set('verification_enabled', (await getSetting('verification_enabled', d1)) === 'true');
    settingsCache.set('user_raw_enabled', (await getSetting('user_raw_enabled', d1)) === 'true');
    console.log("Database tables checked/repaired successfully.");

  } catch (error) {
    console.error('Fatal error during database table check/repair:', error);
    throw error; // Initialization cannot proceed
  }
}

/**
 * Creates a new table with the specified structure.
 */
async function createTable(d1, tableName, structure) {
  const columnsDef = Object.entries(structure.columns)
    .map(([name, def]) => `${name} ${def}`)
    .join(', ');
  const createTableSQL = `CREATE TABLE ${tableName} (${columnsDef})`;
  await d1.exec(createTableSQL);
  console.log(`Table ${tableName} created.`);

  // Create indices if defined
  if (structure.indices) {
    for (const [indexName, indexDef] of Object.entries(structure.indices)) {
      console.log(`Creating index ${indexName} on table ${tableName}...`);
      await d1.exec(`CREATE INDEX IF NOT EXISTS ${indexName} ${indexDef}`);
    }
  }
}

/**
 * Periodically cleans up expired verification codes from the database.
 */
async function cleanExpiredVerificationCodes(d1) {
  console.log("Cleaning expired verification codes...");
  try {
    const nowSeconds = Math.floor(Date.now() / 1000);
    // Select chat_ids with expired codes
    const expiredCodes = await d1.prepare(
      'SELECT chat_id FROM user_states WHERE code_expiry IS NOT NULL AND code_expiry < ? AND is_verifying = TRUE'
    ).bind(nowSeconds).all();

    if (expiredCodes.results && expiredCodes.results.length > 0) {
      console.log(`Found ${expiredCodes.results.length} expired verification codes to clean.`);
      const chatIdsToClean = expiredCodes.results.map(({ chat_id }) => chat_id);

      // Create batch update statements
      const batchUpdates = chatIdsToClean.map(chat_id =>
        d1.prepare(
          'UPDATE user_states SET verification_code = NULL, code_expiry = NULL, is_verifying = FALSE WHERE chat_id = ?'
        ).bind(chat_id)
      );

      await d1.batch(batchUpdates);

      // Invalidate cache for cleaned users
      chatIdsToClean.forEach(chatId => userStateCache.delete(chatId));

      console.log(`Cleaned ${chatIdsToClean.length} expired verification states.`);
    } else {
      console.log("No expired verification codes found.");
    }
  } catch (error) {
    console.error('Error cleaning expired verification codes:', error);
  }
}


// --- Message and Callback Handling Logic ---

/**
 * Handles incoming messages from users.
 */
async function onMessage(message, d1) {
  const chatId = message.chat.id.toString();
  const text = message.text || '';
  const messageId = message.message_id;

  // Messages from the target group (likely admin replies)
  if (chatId === GROUP_ID) {
    const topicId = message.message_thread_id;
    if (topicId) { // Only handle messages within topics
      const privateChatId = await getPrivateChatId(topicId, d1);
      if (privateChatId) {
          // Handle admin commands within a topic
          if (text === '/admin') {
            await sendAdminPanel(chatId, topicId, privateChatId, messageId);
            return;
          }
          if (text.startsWith('/reset_user')) {
            await handleResetUser(chatId, topicId, text, d1);
            return;
          }
          // Forward admin's message to the user's private chat
          await forwardMessageToPrivateChat(privateChatId, message);
      } else {
        console.warn(`Received message in topic ${topicId} but couldn't find corresponding private chat ID.`);
      }
    }
    return; // Ignore messages not in topics or without a mapped user
  }

  // Messages from private chats (users)
  try {
    let userState = await getUserState(chatId, d1); // Use unified getter

    if (userState.is_blocked) {
      await sendMessageToUser(chatId, "您已被拉黑，无法发送消息。请联系管理员解除拉黑。");
      return;
    }

    // Handle /start command
    if (text === '/start') {
      if (await checkStartCommandRate(chatId, d1)) {
        await sendMessageToUser(chatId, "您发送 /start 命令过于频繁，请稍后再试！");
        return;
      }

      const verificationEnabled = settingsCache.get('verification_enabled');
      if (verificationEnabled && userState.is_first_verification) {
        await sendMessageToUser(chatId, "你好，欢迎使用私聊机器人，请完成验证以开始使用！");
        await handleVerification(chatId, messageId, d1);
      } else {
        const successMessage = await getVerificationSuccessMessage();
        await sendMessageToUser(chatId, `${successMessage}\n你好，欢迎使用私聊机器人，现在发送信息吧！`);
        // Mark first verification as done if they bypassed it
        if (userState.is_first_verification) {
           await d1.prepare('UPDATE user_states SET is_first_verification = FALSE WHERE chat_id = ?').bind(chatId).run();
           userState.is_first_verification = false;
           userStateCache.set(chatId, userState);
        }
      }
      return;
    }

    // Verification and Rate Limiting Checks
    const verificationEnabled = settingsCache.get('verification_enabled');
    const isVerified = isUserVerified(userState);
    const isRateLimited = await checkMessageRate(chatId, d1);

    if (verificationEnabled && (!isVerified || (isRateLimited && !userState.is_first_verification))) {
      if (userState.is_verifying) {
        await sendMessageToUser(chatId, `请先完成当前的验证。`);
        return;
      }
      // Use a more generic message if rate limited or needs verification
      const reason = !isVerified ? "验证" : "发送消息过于频繁，需要重新验证";
      await sendMessageToUser(chatId, `请完成${reason}后才能发送消息。`);
      await handleVerification(chatId, messageId, d1);
      return;
    }

    // Forward message to the appropriate topic in the group
    const userInfo = await getUserInfo(chatId);
    const nickname = userInfo.nickname || `User_${chatId}`;
    const topicName = nickname; // Use nickname for topic name

    let topicId = await getOrCreateTopicForUser(chatId, topicName, userInfo, d1);

    try {
      if (text) {
        const formattedMessage = `${nickname}:\n${text}`;
        await sendMessageToTopic(topicId, formattedMessage);
      } else {
        // Forward non-text messages (photos, files, etc.)
        await copyMessageToTopic(topicId, message);
      }
    } catch (error) {
      // Handle topic not found error (e.g., manually deleted by admin)
      if (error.message.includes('Request failed with status 400') || error.message.includes('chat not found') || error.message.includes('thread not found')) {
        console.warn(`Topic ${topicId} for user ${chatId} seems invalid. Recreating...`);
        topicIdCache.delete(chatId); // Clear cache
        topicId = await createForumTopic(topicName, userInfo.username, nickname, chatId, d1); // Recreate
        await saveTopicId(chatId, topicId, d1); // Save new ID

        // Retry sending/copying the message
        if (text) {
          const formattedMessage = `${nickname}:\n${text}`;
          await sendMessageToTopic(topicId, formattedMessage);
        } else {
          await copyMessageToTopic(topicId, message);
        }
      } else {
        throw error; // Re-throw other errors
      }
    }
  } catch (error) {
    console.error(`Error handling message from chatId ${chatId}:`, error);
    // Attempt to notify admin in the general topic (if applicable) or log
    try {
       await sendMessageToTopic(null, `无法处理用户 ${chatId} 的消息：${error.message}`); // Send to general if topicId is null/invalid
    } catch (adminNotifyError) {
        console.error("Failed to send error notification to admin:", adminNotifyError);
    }
    await sendMessageToUser(chatId, "处理您的消息时遇到错误，请稍后再试或联系管理员。");
  }
}

/**
 * Handles callback queries (button presses).
 */
async function onCallbackQuery(callbackQuery, d1) {
  const message = callbackQuery.message;
  const chatId = message.chat.id.toString(); // Can be user's private chat or the group chat
  const topicId = message.message_thread_id; // Only present if callback is in a group topic
  const data = callbackQuery.data; // Data string from the button
  const messageId = message.message_id;
  const callbackQueryId = callbackQuery.id;

  // Helper function to answer the callback query
  const answerCallback = async (text = '') => {
    try {
      await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/answerCallbackQuery`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ callback_query_id: callbackQueryId, text: text })
      });
    } catch (error) {
      console.error(`Failed to answer callback query ${callbackQueryId}:`, error);
    }
  };

  try {
    // --- Verification Callbacks (from User's Private Chat) ---
    if (data.startsWith('verify_')) {
        const [, userChatId, selectedAnswer, result] = data.split('_');
        // Ensure the callback is from the correct user
        if (userChatId !== chatId) {
            console.warn(`Verification callback mismatch: Expected ${userChatId}, got ${chatId}`);
            await answerCallback('无效的操作。');
            return;
        }

        const { verificationState, isExpired } = await getVerificationState(chatId, d1);

        if (isExpired) {
            await sendMessageToUser(chatId, '验证码已过期或已被使用，请重新发送消息以获取新验证码。');
            await clearVerificationState(chatId, d1); // Clean up DB and cache
            // Attempt to delete the expired verification message
            try { await deleteMessage(chatId, messageId); } catch (e) { /* Ignore deletion error */ }
            await answerCallback('验证码已过期。');
            return;
        }

        // Process verification result
        if (result === 'correct') {
            await completeVerification(chatId, d1); // Update DB and cache
            const successMessage = await getVerificationSuccessMessage();
            await sendMessageToUser(chatId, `${successMessage}\n你好，欢迎使用私聊机器人！现在可以发送消息了。`);
            await answerCallback('验证成功！');
        } else {
            await sendMessageToUser(chatId, '验证失败，请重新尝试。');
            await handleVerification(chatId, messageId, d1); // Send a new verification challenge
            await answerCallback('验证失败。');
        }

        // Delete the verification message after interaction
        try { await deleteMessage(chatId, messageId); } catch (e) { /* Ignore deletion error */ }

    // --- Admin Panel Callbacks (from Group Chat Topic) ---
    } else {
        const senderId = callbackQuery.from.id.toString();
        const isAdmin = await checkIfAdmin(senderId);
        if (!isAdmin) {
            await answerCallback('您没有执行此操作的权限。');
            return;
        }

        // Extract action and target user ID from callback data
        const parts = data.split('_');
        let action = parts[0];
        let targetChatId = ''; // The user the admin is acting upon

        // Determine action and target based on data pattern
        if (data.startsWith('toggle_verification_') || data.startsWith('toggle_user_raw_') || data.startsWith('check_blocklist_') || data.startsWith('delete_user_')) {
            action = parts.slice(0, -1).join('_'); // e.g., toggle_verification
            targetChatId = parts[parts.length - 1];
        } else if (data.startsWith('block_') || data.startsWith('unblock_')) {
            action = parts[0]; // block or unblock
            targetChatId = parts.slice(1).join('_');
        } else {
            // Handle unknown actions or potentially malformed data
            console.warn(`Received unknown admin callback action: ${data}`);
            await answerCallback('未知操作。');
            return;
        }

        let responseText = ''; // Text to show the admin in the callback answer

        // --- Perform Admin Actions ---
        if (action === 'block') {
            await setUserBlockedStatus(targetChatId, true, d1);
            await sendMessageToTopic(topicId, `用户 ${targetChatId} 已被拉黑。`);
            responseText = `用户 ${targetChatId} 已拉黑`;
        } else if (action === 'unblock') {
            await setUserBlockedStatus(targetChatId, false, d1);
            await sendMessageToTopic(topicId, `用户 ${targetChatId} 已解除拉黑。`);
            responseText = `用户 ${targetChatId} 已解封`;
        } else if (action === 'toggle_verification') {
            const currentState = settingsCache.get('verification_enabled');
            const newState = !currentState;
            await setSetting('verification_enabled', newState.toString(), d1);
            await sendMessageToTopic(topicId, `验证码功能已${newState ? '开启' : '关闭'}。`);
            responseText = `验证码已 ${newState ? '开启' : '关闭'}`;
        } else if (action === 'check_blocklist') {
            const blockedUsers = await d1.prepare('SELECT chat_id FROM user_states WHERE is_blocked = TRUE').all();
            const blockList = blockedUsers.results && blockedUsers.results.length > 0
                ? blockedUsers.results.map(row => row.chat_id).join('\n')
                : '当前没有被拉黑的用户。';
            await sendMessageToTopic(topicId, `黑名单列表：\n${blockList}`);
            responseText = '黑名单已发送';
        } else if (action === 'toggle_user_raw') {
            const currentState = settingsCache.get('user_raw_enabled');
            const newState = !currentState;
            await setSetting('user_raw_enabled', newState.toString(), d1);
            await sendMessageToTopic(topicId, `用户端 Raw 链接已${newState ? '开启' : '关闭'}。`);
            responseText = `用户端 Raw 已 ${newState ? '开启' : '关闭'}`;
        } else if (action === 'delete_user') {
            await deleteUser(targetChatId, d1);
            await sendMessageToTopic(topicId, `用户 ${targetChatId} 的状态和消息记录已删除。`);
            responseText = `用户 ${targetChatId} 已删除`;
        } else {
            responseText = '未知操作';
            console.warn(`Unhandled admin action: ${action}`);
        }

        // Update the admin panel message with fresh buttons/state
        await sendAdminPanel(chatId, topicId, targetChatId, messageId);
        await answerCallback(responseText);
    }
  } catch (error) {
    console.error(`Error processing callback query ${data}:`, error);
    // Attempt to notify the user or admin depending on the context
    if (topicId) { // Likely an admin action error
        try { await sendMessageToTopic(topicId, `处理操作 ${data} 失败：${error.message}`); } catch (e) {}
    } else { // Likely a user verification error
        try { await sendMessageToUser(chatId, `处理您的操作时遇到错误，请重试。`); } catch (e) {}
    }
    await answerCallback('处理失败，请重试。'); // Generic error to user/admin
  }
}


// --- Verification Logic ---

/**
 * Initiates the verification process for a user.
 */
async function handleVerification(chatId, originalMessageId, d1) {
    try {
        let userState = await getUserState(chatId, d1);

        // If already verifying, potentially resend or notify
        if (userState.is_verifying) {
            // Optional: Check if the last verification message still exists or has expired
            console.log(`User ${chatId} is already in verification process.`);
            // You might want to resend if the previous message was deleted or expired
            // For now, just return to avoid sending multiple challenges at once
            return;
        }

        // Clear any previous verification message
        if (userState.last_verification_message_id) {
            try {
                await deleteMessage(chatId, userState.last_verification_message_id);
            } catch (error) {
                // Log error but continue, message might already be deleted
                console.warn(`Could not delete previous verification message ${userState.last_verification_message_id} for ${chatId}:`, error.message);
            }
        }

        // Set user state to verifying *before* sending the message
        await d1.prepare('UPDATE user_states SET is_verifying = TRUE, verification_code = NULL, code_expiry = NULL, last_verification_message_id = NULL WHERE chat_id = ?')
                 .bind(chatId)
                 .run();
        userState.is_verifying = true;
        userState.verification_code = null;
        userState.code_expiry = null;
        userState.last_verification_message_id = null;
        userStateCache.set(chatId, userState); // Update cache

        await sendVerificationChallenge(chatId, d1);

    } catch (error) {
        console.error(`Error initiating verification for ${chatId}:`, error);
        await sendMessageToUser(chatId, "启动验证过程时出错，请稍后重试。");
    }
}

/**
 * Sends the verification challenge (math question) to the user.
 */
async function sendVerificationChallenge(chatId, d1) {
  const num1 = Math.floor(Math.random() * 10);
  const num2 = Math.floor(Math.random() * 10);
  // Ensure the result is not negative for subtraction to keep it simple
  const operation = (Math.random() > 0.5 && num1 >= num2) ? '-' : '+';
  const correctResult = operation === '+' ? num1 + num2 : num1 - num2;

  // Generate distractors, ensuring they are unique and different from the correct answer
  const options = new Set([correctResult]);
  while (options.size < 4) {
    // Generate wrong answers close to the correct one
    const wrongResult = correctResult + (Math.floor(Math.random() * 5) - 2); // Range -2 to +2
    if (wrongResult !== correctResult) {
      options.add(wrongResult);
    }
  }
  // Shuffle the options
  const optionArray = Array.from(options).sort(() => Math.random() - 0.5);

  // Create inline keyboard buttons
  const buttons = optionArray.map(option => ({
    text: `(${option})`, // Display the option number
    // Format: verify_CHATID_SELECTEDANSWER_RESULT(correct/wrong)
    callback_data: `verify_${chatId}_${option}_${option === correctResult ? 'correct' : 'wrong'}`
  }));

  const question = `请计算： ${num1} ${operation} ${num2} = ? （请在 5 分钟内点击下方按钮完成验证）`;
  const nowSeconds = Math.floor(Date.now() / 1000);
  const codeExpiry = nowSeconds + 300; // 5 minutes expiry

  try {
    // Send the message with the question and buttons
    const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        chat_id: chatId,
        text: question,
        reply_markup: { inline_keyboard: [buttons] } // Buttons are in a single row
      })
    });
    const data = await response.json();

    if (data.ok) {
      const sentMessageId = data.result.message_id.toString();
      // Store the correct answer, expiry, and message ID in the database
      await d1.prepare('UPDATE user_states SET verification_code = ?, code_expiry = ?, last_verification_message_id = ?, is_verifying = TRUE WHERE chat_id = ?')
               .bind(correctResult.toString(), codeExpiry, sentMessageId, chatId)
               .run();

      // Update cache
      let userState = userStateCache.get(chatId) || await fetchUserState(chatId, d1);
      userState.verification_code = correctResult.toString();
      userState.code_expiry = codeExpiry;
      userState.last_verification_message_id = sentMessageId;
      userState.is_verifying = true; // Ensure this is set
      userStateCache.set(chatId, userState);

    } else {
      throw new Error(`Failed to send verification message: ${data.description}`);
    }
  } catch (error) {
    console.error("Error sending verification challenge:", error);
    // If sending fails, reset the verifying state
    await clearVerificationState(chatId, d1, false); // Don't clear code/expiry if they existed
    await sendMessageToUser(chatId, "发送验证消息时出错，请稍后重试。");
  }
}

/**
 * Gets the current verification state for a user, checking expiry.
 */
async function getVerificationState(chatId, d1) {
    let verificationState = userStateCache.get(chatId);
    if (!verificationState) {
        verificationState = await d1.prepare('SELECT verification_code, code_expiry, is_verifying FROM user_states WHERE chat_id = ?')
                                  .bind(chatId)
                                  .first();
        if (verificationState) {
            userStateCache.set(chatId, verificationState);
        }
    }

    if (!verificationState || !verificationState.is_verifying || !verificationState.code_expiry) {
        return { verificationState: null, isExpired: true }; // Not verifying or no expiry set
    }

    const nowSeconds = Math.floor(Date.now() / 1000);
    const isExpired = nowSeconds > verificationState.code_expiry;

    return { verificationState, isExpired };
}

/**
 * Updates user state upon successful verification.
 */
async function completeVerification(chatId, d1) {
    const nowSeconds = Math.floor(Date.now() / 1000);
    const verifiedExpiry = nowSeconds + (3600 * 24); // 24-hour verification validity

    // Update database: set verified, clear verification details, mark first verification done
    await d1.prepare('UPDATE user_states SET is_verified = TRUE, verified_expiry = ?, verification_code = NULL, code_expiry = NULL, last_verification_message_id = NULL, is_first_verification = FALSE, is_verifying = FALSE WHERE chat_id = ?')
             .bind(verifiedExpiry, chatId)
             .run();

    // Update cache
    let userState = userStateCache.get(chatId) || await fetchUserState(chatId, d1); // Fetch fresh state if not cached
    userState.is_verified = true;
    userState.verified_expiry = verifiedExpiry;
    userState.verification_code = null;
    userState.code_expiry = null;
    userState.last_verification_message_id = null;
    userState.is_first_verification = false;
    userState.is_verifying = false;
    userStateCache.set(chatId, userState);

    // Reset message rate limit count after successful verification
    await d1.prepare('UPDATE message_rates SET message_count = 0 WHERE chat_id = ?')
             .bind(chatId)
             .run();
    let rateData = messageRateCache.get(chatId);
    if (rateData) {
      rateData.message_count = 0;
      messageRateCache.set(chatId, rateData);
    }
}

/**
 * Clears verification-related fields in the database and cache.
 */
async function clearVerificationState(chatId, d1, clearVerifyingFlag = true) {
    try {
        const updateStmt = clearVerifyingFlag
            ? 'UPDATE user_states SET verification_code = NULL, code_expiry = NULL, last_verification_message_id = NULL, is_verifying = FALSE WHERE chat_id = ?'
            : 'UPDATE user_states SET verification_code = NULL, code_expiry = NULL, last_verification_message_id = NULL WHERE chat_id = ? AND is_verifying = TRUE'; // Only clear if verifying

        await d1.prepare(updateStmt).bind(chatId).run();

        // Update cache
        let userState = userStateCache.get(chatId);
        if (userState) {
            userState.verification_code = null;
            userState.code_expiry = null;
            userState.last_verification_message_id = null;
            if (clearVerifyingFlag) {
                userState.is_verifying = false;
            }
            userStateCache.set(chatId, userState);
        }
        console.log(`Cleared verification state for ${chatId}.`);
    } catch (error) {
        console.error(`Error clearing verification state for ${chatId}:`, error);
    }
}

// --- User and Topic Management ---

/**
 * Retrieves user state from cache or database. Creates record if non-existent.
 */
async function getUserState(chatId, d1) {
  let userState = userStateCache.get(chatId);
  if (!userState) {
    userState = await fetchUserState(chatId, d1); // Fetches or creates in DB
    userStateCache.set(chatId, userState);
  }
  return userState;
}

/**
 * Fetches user state from DB or creates a default entry.
 */
async function fetchUserState(chatId, d1) {
  try {
    let userState = await d1.prepare(
        'SELECT chat_id, is_blocked, is_verified, verified_expiry, verification_code, code_expiry, last_verification_message_id, is_first_verification, is_rate_limited, is_verifying FROM user_states WHERE chat_id = ?'
      ).bind(chatId).first();

    if (!userState) {
      console.log(`No user state found for ${chatId}, creating default entry.`);
      const defaultState = {
          chat_id: chatId,
          is_blocked: false,
          is_verified: false,
          verified_expiry: null,
          verification_code: null,
          code_expiry: null,
          last_verification_message_id: null,
          is_first_verification: true,
          is_rate_limited: false,
          is_verifying: false
      };
      await d1.prepare(
        'INSERT INTO user_states (chat_id, is_blocked, is_verified, verified_expiry, verification_code, code_expiry, last_verification_message_id, is_first_verification, is_rate_limited, is_verifying) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
        ).bind(...Object.values(defaultState)).run();
        return defaultState; // Return the newly created state
    }
    return userState; // Return the fetched state
  } catch (error) {
    console.error(`Error fetching/creating user state for chatId ${chatId}:`, error);
    // Return a default safe state in case of DB error
    return { is_blocked: false, is_first_verification: true, is_verified: false, verified_expiry: null, is_verifying: false };
  }
}

/**
 * Checks if a user is currently considered verified based on state and expiry.
 */
function isUserVerified(userState) {
    if (!userState || !userState.is_verified || !userState.verified_expiry) {
        return false;
    }
    const nowSeconds = Math.floor(Date.now() / 1000);
    return nowSeconds < userState.verified_expiry;
}


/**
 * Sets the blocked status for a user in the database and cache.
 */
async function setUserBlockedStatus(chatId, isBlocked, d1) {
  try {
    // Update DB first
    await d1.prepare('INSERT OR REPLACE INTO user_states (chat_id, is_blocked) VALUES (?, ?) ON CONFLICT(chat_id) DO UPDATE SET is_blocked = excluded.is_blocked')
             .bind(chatId, isBlocked)
             .run();

    // Update cache after successful DB operation
    let state = userStateCache.get(chatId);
    if (!state) {
        state = await fetchUserState(chatId, d1); // Fetch if not in cache
    }
    state.is_blocked = isBlocked;
    userStateCache.set(chatId, state);
    console.log(`User ${chatId} blocked status set to: ${isBlocked}`);
  } catch (error) {
      console.error(`Error setting blocked status for ${chatId}:`, error);
      // Should we re-throw or just log? Depends on desired behavior on failure.
  }
}

/**
 * Deletes a user's state and rate limit records.
 */
async function deleteUser(chatId, d1) {
  try {
    await d1.batch([
      d1.prepare('DELETE FROM user_states WHERE chat_id = ?').bind(chatId),
      d1.prepare('DELETE FROM message_rates WHERE chat_id = ?').bind(chatId)
      // Optionally: d1.prepare('DELETE FROM chat_topic_mappings WHERE chat_id = ?').bind(chatId) // If you want to remove topic mapping too
    ]);
    // Clear caches
    userStateCache.delete(chatId);
    messageRateCache.delete(chatId);
    // topicIdCache.delete(chatId); // If deleting mapping

    console.log(`Deleted user data for ${chatId}.`);
  } catch (error) {
    console.error(`Error deleting user ${chatId}:`, error);
    throw error; // Re-throw to indicate failure
  }
}


/**
 * Retrieves user information (username, nickname) from cache or Telegram API.
 */
async function getUserInfo(chatId) {
  let userInfo = userInfoCache.get(chatId);
  if (userInfo === undefined) {
    try {
      console.log(`Fetching user info for ${chatId} from API...`);
      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getChat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: chatId })
      });
      const data = await response.json();
      if (!data.ok) {
        // Handle cases where the bot can't get chat info (e.g., user blocked the bot)
        console.warn(`Failed to get chat info for ${chatId}: ${data.description}. Using default info.`);
        userInfo = { id: chatId, username: `User_${chatId}`, nickname: `User_${chatId}` };
      } else {
        const result = data.result;
        const firstName = result.first_name || '';
        const lastName = result.last_name || '';
        // Construct nickname, fallback to username or generic ID
        let nickname = `${firstName} ${lastName}`.trim();
        if (!nickname) {
            nickname = result.username ? `@${result.username}` : `User_${chatId}`;
        }

        userInfo = {
          id: result.id ? result.id.toString() : chatId, // Ensure ID is string
          username: result.username || `User_${chatId}`,
          nickname: nickname
        };
      }
      userInfoCache.set(chatId, userInfo);
    } catch (error) {
      console.error(`Error fetching user info for chatId ${chatId}:`, error);
      // Provide default info on error
      userInfo = { id: chatId, username: `User_${chatId}`, nickname: `User_${chatId}` };
      userInfoCache.set(chatId, userInfo); // Cache default info to avoid repeated API calls on error
    }
  }
  return userInfo;
}

/**
 * Gets the existing topic ID for a user from cache or database.
 */
async function getExistingTopicId(chatId, d1) {
  let topicId = topicIdCache.get(chatId);
  if (topicId === undefined) {
    try {
        const result = await d1.prepare('SELECT topic_id FROM chat_topic_mappings WHERE chat_id = ?')
                              .bind(chatId)
                              .first();
        topicId = result?.topic_id || null;
        if (topicId) {
            topicIdCache.set(chatId, topicId);
        }
    } catch (error) {
        console.error(`Error fetching existing topic ID for ${chatId}:`, error);
        topicId = null; // Assume no topic exists on error
    }
  }
  return topicId;
}

/**
 * Gets or creates a forum topic for a user.
 */
async function getOrCreateTopicForUser(chatId, topicName, userInfo, d1) {
    let topicId = await getExistingTopicId(chatId, d1);
    if (!topicId) {
        console.log(`No existing topic found for ${chatId}. Creating new topic...`);
        topicId = await createForumTopic(topicName, userInfo.username, userInfo.nickname, chatId, d1);
        await saveTopicId(chatId, topicId, d1);
    }
    return topicId;
}


/**
 * Creates a new forum topic in the group for a user.
 */
async function createForumTopic(topicName, userName, nickname, userId, d1) {
  try {
    console.log(`Creating forum topic "${topicName}" for user ${userId}...`);
    const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/createForumTopic`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      // Use user's nickname as the topic name
      body: JSON.stringify({ chat_id: GROUP_ID, name: topicName })
    });
    const data = await response.json();
    if (!data.ok) {
        // Handle specific errors like "topic limit exceeded" if possible
      throw new Error(`Failed to create forum topic: ${data.description}`);
    }
    const topicId = data.result.message_thread_id;
    console.log(`Created topic ${topicId} for user ${userId}.`);

    // Send and pin an initial message with user info and rules/notification
    const now = new Date();
    const formattedTime = now.toISOString().replace('T', ' ').substring(0, 19);
    const notificationContent = await getNotificationContent(); // Fetch custom content
    const pinnedMessageText = `👤 **用户:** ${nickname}\n🆔 **ID:** ${userId}\n🏷️ **用户名:** @${userName}\n⏰ **时间:** ${formattedTime}\n\n${notificationContent}`;

    try {
        const messageResponse = await sendMessageToTopic(topicId, pinnedMessageText);
        const messageId = messageResponse.result.message_id;
        await pinMessage(topicId, messageId);
        console.log(`Pinned initial message ${messageId} in topic ${topicId}.`);
    } catch(pinError) {
        console.error(`Failed to send or pin initial message in topic ${topicId}:`, pinError);
        // Continue even if pinning fails, topic is still created
    }

    return topicId;
  } catch (error) {
    console.error(`Error creating forum topic for user ${userId}:`, error);
    throw error; // Re-throw to indicate failure
  }
}

/**
 * Saves the mapping between a user's chat ID and their topic ID.
 */
async function saveTopicId(chatId, topicId, d1) {
  topicIdCache.set(chatId, topicId); // Update cache first
  try {
    await d1.prepare('INSERT OR REPLACE INTO chat_topic_mappings (chat_id, topic_id) VALUES (?, ?)')
             .bind(chatId, topicId)
             .run();
    console.log(`Saved topic mapping: ${chatId} -> ${topicId}`);
  } catch (error) {
    console.error(`Error saving topic ID ${topicId} for chatId ${chatId}:`, error);
    // Consider cache invalidation on error?
    throw error;
  }
}

/**
 * Retrieves the private chat ID associated with a group topic ID.
 */
async function getPrivateChatId(topicId, d1) {
  // Check cache first (more efficient for active users)
  for (const [chatId, cachedTopicId] of topicIdCache.cache.entries()) {
    if (cachedTopicId === topicId) {
      return chatId;
    }
  }
  // If not in cache, query the database
  try {
    const mapping = await d1.prepare('SELECT chat_id FROM chat_topic_mappings WHERE topic_id = ?')
                           .bind(topicId)
                           .first();
    if (mapping?.chat_id) {
        topicIdCache.set(mapping.chat_id, topicId); // Add to cache if found in DB
        return mapping.chat_id;
    }
    return null; // Not found
  } catch (error) {
    console.error(`Error fetching private chat ID for topicId ${topicId}:`, error);
    return null; // Return null on error
  }
}

// --- Rate Limiting ---

/**
 * Checks and updates the message rate limit for a user.
 */
async function checkMessageRate(chatId, d1) {
  const now = Date.now(); // Use milliseconds for finer granularity
  const window = 60 * 1000; // 1 minute window

  let data = messageRateCache.get(chatId);
  if (data === undefined) {
    // Fetch only relevant fields if possible
    data = await d1.prepare('SELECT message_count, window_start FROM message_rates WHERE chat_id = ?')
                   .bind(chatId)
                   .first() || { message_count: 0, window_start: now };
    messageRateCache.set(chatId, data);
  }

  if (now - data.window_start > window) {
    // Window expired, reset count and start time
    data.message_count = 1;
    data.window_start = now;
  } else {
    // Increment count within the window
    data.message_count += 1;
  }

  messageRateCache.set(chatId, data); // Update cache

  // Update database (could potentially be debounced or batched for performance)
  await d1.prepare('INSERT OR REPLACE INTO message_rates (chat_id, message_count, window_start) VALUES (?, ?, ?)')
           .bind(chatId, data.message_count, data.window_start)
           .run();

  return data.message_count > MAX_MESSAGES_PER_MINUTE;
}

/**
 * Checks and updates the /start command rate limit for a user.
 */
async function checkStartCommandRate(chatId, d1) {
  const now = Date.now();
  const window = 5 * 60 * 1000; // 5 minute window
  const maxStartsPerWindow = 1; // Allow only one /start in 5 mins

  let data = messageRateCache.get(chatId);
  if (data === undefined) {
    // Fetch only relevant fields
    data = await d1.prepare('SELECT start_count, start_window_start FROM message_rates WHERE chat_id = ?')
                   .bind(chatId)
                   .first() || { start_count: 0, start_window_start: now };
    messageRateCache.set(chatId, data);
  }

  if (now - data.start_window_start > window) {
    data.start_count = 1;
    data.start_window_start = now;
  } else {
    data.start_count += 1;
  }

  messageRateCache.set(chatId, data); // Update cache

  // Update database
  await d1.prepare('INSERT OR REPLACE INTO message_rates (chat_id, start_count, start_window_start) VALUES (?, ?, ?)')
           .bind(chatId, data.start_count, data.start_window_start)
           .run();

  return data.start_count > maxStartsPerWindow;
}


// --- Settings Management ---

/**
 * Retrieves a setting value from the database.
 */
async function getSetting(key, d1) {
  try {
    const result = await d1.prepare('SELECT value FROM settings WHERE key = ?')
                           .bind(key)
                           .first();
    return result?.value || null;
  } catch (error) {
    console.error(`Error getting setting ${key}:`, error);
    // Return default or throw? Depends on how critical the setting is.
    // Returning null might be safer for non-critical settings.
    return null;
  }
}

/**
 * Updates a setting value in the database and cache.
 */
async function setSetting(key, value, d1) {
  try {
    await d1.prepare('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)')
             .bind(key, value)
             .run();
    // Update cache immediately
    if (settingsCache.has(key)) {
        settingsCache.set(key, value === 'true'); // Assuming boolean settings
    }
    console.log(`Setting ${key} updated to ${value}.`);
  } catch (error) {
    console.error(`Error setting ${key} to ${value}:`, error);
    throw error; // Re-throw as setting changes might be critical
  }
}

// --- Telegram API Interaction Helpers ---

/**
 * Sends a text message to a specific user.
 */
async function sendMessageToUser(chatId, text) {
  if (!text || !text.trim()) {
      console.warn(`Attempted to send empty message to user ${chatId}`);
      return;
  }
  try {
    const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: chatId, text: text })
    });
    const data = await response.json();
    if (!data.ok) {
      // Handle specific errors like "bot was blocked by the user"
      if (data.description.includes("bot was blocked by the user")) {
          console.log(`User ${chatId} blocked the bot. Marking as blocked.`);
          // Optionally mark user as blocked in DB here
          // await setUserBlockedStatus(chatId, true, /* need d1 instance */);
      } else {
          throw new Error(`Failed to send message to user ${chatId}: ${data.description}`);
      }
    }
  } catch (error) {
    console.error(`Error sending message to user ${chatId}:`, error);
    // Don't re-throw here usually, as failure to send a notification shouldn't stop main flow
  }
}

/**
 * Sends a text message to a specific topic within the group.
 */
async function sendMessageToTopic(topicId, text) {
  if (!text || !text.trim()) {
    console.warn(`Attempted to send empty message to topic ${topicId}`);
    return; // Don't throw, just prevent sending empty messages
  }
  if (!topicId) {
      console.warn("Attempted to send message to null/undefined topicId. Sending to general chat (if applicable).");
      // Decide if sending to general chat without topicId is intended
      // If not, return or throw an error here.
  }

  try {
    const requestBody = {
      chat_id: GROUP_ID,
      text: text,
      // Only include message_thread_id if it's valid
      ...(topicId && { message_thread_id: topicId })
    };
    const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(requestBody)
    });
    const data = await response.json();
    if (!data.ok) {
      throw new Error(`Failed to send message to topic ${topicId || 'general'}: ${data.description} (chat_id: ${GROUP_ID})`);
    }
    return data; // Return the response data (contains message_id)
  } catch (error) {
    console.error(`Error sending message to topic ${topicId || 'general'}:`, error);
    throw error; // Re-throw to indicate failure
  }
}

/**
 * Copies a message (text, photo, etc.) to a specific topic.
 */
async function copyMessageToTopic(topicId, message) {
  if (!topicId) {
      console.error(`Cannot copy message to null/undefined topicId for message ${message.message_id}`);
      throw new Error("Invalid topic ID for copying message.");
  }
  try {
    const requestBody = {
      chat_id: GROUP_ID,
      from_chat_id: message.chat.id,
      message_id: message.message_id,
      message_thread_id: topicId,
      disable_notification: true // Usually disable notifications for copies
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
    return data;
  } catch (error) {
    console.error(`Error copying message ${message.message_id} to topic ${topicId}:`, error);
    throw error;
  }
}

/**
 * Forwards (copies) a message from the group topic to the user's private chat.
 */
async function forwardMessageToPrivateChat(privateChatId, message) {
  try {
    const requestBody = {
      chat_id: privateChatId,
      from_chat_id: message.chat.id, // Should be GROUP_ID
      message_id: message.message_id,
      disable_notification: false // Notifications might be desired here
    };
    const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/copyMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(requestBody)
    });
    const data = await response.json();
    if (!data.ok) {
        // Handle cases like user blocking the bot
      if (data.description.includes("bot was blocked by the user") || data.description.includes("user is deactivated")) {
          console.warn(`Could not forward message to ${privateChatId}: ${data.description}. Marking as blocked.`);
          // Optionally block user here
      } else {
          throw new Error(`Failed to forward message to private chat ${privateChatId}: ${data.description}`);
      }
    }
  } catch (error) {
    console.error(`Error forwarding message ${message.message_id} to private chat ${privateChatId}:`, error);
    // Don't re-throw, failure to forward shouldn't break everything
  }
}

/**
 * Pins a message within a specific group topic.
 */
async function pinMessage(topicId, messageId) {
   if (!topicId) {
      console.warn(`Cannot pin message ${messageId} in null/undefined topicId.`);
      return;
   }
  try {
    const requestBody = {
      chat_id: GROUP_ID,
      message_id: messageId,
      message_thread_id: topicId // Required for pinning in topics
    };
    const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/pinChatMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(requestBody)
    });
    const data = await response.json();
    if (!data.ok) {
      throw new Error(`Failed to pin message ${messageId} in topic ${topicId}: ${data.description}`);
    }
  } catch (error) {
    console.error(`Error pinning message ${messageId} in topic ${topicId}:`, error);
    // Don't re-throw, pinning failure is often non-critical
  }
}

/**
 * Deletes a message in a given chat.
 */
async function deleteMessage(chatId, messageId) {
    try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                chat_id: chatId,
                message_id: messageId
            })
        });
        const data = await response.json();
        // Ignore "message to delete not found" or "message can't be deleted" errors
        if (!data.ok && !data.description.includes("not found") && !data.description.includes("can't be deleted")) {
            throw new Error(`Failed to delete message ${messageId} in chat ${chatId}: ${data.description}`);
        }
        return data.ok; // Return true if deletion was successful or ignored known errors
    } catch (error) {
        console.error(`Error deleting message ${messageId} in chat ${chatId}:`, error);
        throw error; // Re-throw other errors
    }
}


/**
 * Wrapper for fetch with retry logic, timeout, and specific error handling.
 */
async function fetchWithRetry(url, options = {}, retries = 3, backoff = 1000) {
  for (let i = 0; i < retries; i++) {
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 5000); // 5 second timeout

      const response = await fetch(url, { ...options, signal: controller.signal });
      clearTimeout(timeoutId);

      if (response.ok) {
        return response; // Success
      }

      // Handle rate limiting specifically
      if (response.status === 429) {
        const retryAfterHeader = response.headers.get('Retry-After');
        const retryAfterSeconds = retryAfterHeader ? parseInt(retryAfterHeader, 10) : 5; // Default 5s
        const delay = Math.min(retryAfterSeconds * 1000, 60000); // Cap delay at 60s
        console.warn(`Rate limited (429) on ${url}. Retrying after ${delay / 1000} seconds...`);
        await new Promise(resolve => setTimeout(resolve, delay));
        continue; // Retry immediately after delay
      }

      // Handle other non-OK statuses
      const errorBody = await response.text(); // Try to get error details
      throw new Error(`Request failed: ${response.status} ${response.statusText} on ${url}. Body: ${errorBody}`);

    } catch (error) {
      if (error.name === 'AbortError') {
          console.error(`Request timed out for ${url} after 5 seconds.`);
      } else {
          console.error(`Fetch attempt ${i + 1} for ${url} failed:`, error.message);
      }

      if (i === retries - 1) {
        console.error(`Final attempt failed for ${url}.`);
        throw error; // Throw error after last retry
      }

      // Exponential backoff for retries
      const delay = backoff * Math.pow(2, i) + Math.random() * 1000; // Add jitter
      console.log(`Retrying in ${Math.round(delay / 1000)} seconds...`);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
  // Should not be reachable if retries > 0, but satisfies TS/linting
  throw new Error(`Failed to fetch ${url} after ${retries} retries`);
}

// --- Webhook Management Endpoints ---

/**
 * Registers the webhook URL manually via a request.
 */
async function registerWebhook(request) {
  const webhookUrl = `${new URL(request.url).origin}/webhook`;
  try {
      const response = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/setWebhook`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ url: webhookUrl })
      }).then(r => r.json());
      return new Response(JSON.stringify(response, null, 2), {
          headers: { 'Content-Type': 'application/json' },
          status: response.ok ? 200 : 400
      });
  } catch (error) {
      console.error("Error registering webhook:", error);
      return new Response(JSON.stringify({ ok: false, description: error.message }), {
          headers: { 'Content-Type': 'application/json' },
          status: 500
      });
  }
}

/**
 * Unregisters (removes) the webhook manually via a request.
 */
async function unRegisterWebhook() {
   try {
      const response = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/setWebhook`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ url: '' }) // Empty URL removes the webhook
      }).then(r => r.json());
      return new Response(JSON.stringify(response, null, 2), {
          headers: { 'Content-Type': 'application/json' },
          status: response.ok ? 200 : 400
      });
   } catch (error) {
      console.error("Error unregistering webhook:", error);
      return new Response(JSON.stringify({ ok: false, description: error.message }), {
          headers: { 'Content-Type': 'application/json' },
          status: 500
      });
   }
}

// --- Utility Functions ---

/**
 * Checks if a given user ID belongs to an administrator or creator of the group.
 */
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
    return data.ok && ['administrator', 'creator'].includes(data.result.status);
  } catch (error) {
    console.error(`Error checking admin status for user ${userId}:`, error);
    return false; // Assume not admin on error
  }
}

/**
 * Fetches content from a specified URL (e.g., for start/notification messages).
 */
async function fetchContentFromURL(url, fallback) {
    try {
        const response = await fetch(url); // Use default fetch, maybe add timeout
        if (!response.ok) {
            throw new Error(`Failed to fetch ${url}: ${response.status} ${response.statusText}`);
        }
        const text = await response.text();
        return text.trim() || fallback;
    } catch (error) {
        console.error(`Error fetching content from ${url}:`, error);
        return fallback;
    }
}

/**
 * Gets the verification success message, potentially fetching from external URL.
 */
async function getVerificationSuccessMessage() {
  const fallbackMessage = '验证成功！您现在可以与客服聊天。';
  const userRawEnabled = settingsCache.get('user_raw_enabled');
  if (!userRawEnabled) return fallbackMessage;

  const url = 'https://raw.githubusercontent.com/iawooo/ctt/refs/heads/main/CFTeleTrans/start.md';
  return await fetchContentFromURL(url, fallbackMessage);
}

/**
 * Gets the notification content to pin in new topics, potentially fetching from external URL.
 */
async function getNotificationContent() {
  const fallbackContent = ''; // Default to empty if fetch fails
  const url = 'https://raw.githubusercontent.com/iawooo/ctt/refs/heads/main/CFTeleTrans/notification.md';
  return await fetchContentFromURL(url, fallbackContent);
}


// --- Processed Message/Callback Cleanup (Called Periodically) ---

function cleanupProcessedMessages() {
    const now = Date.now();
    let deletedCount = 0;
    for (const [key, timestamp] of processedMessages.entries()) {
        if (now - timestamp > PROCESSED_MESSAGE_TTL) {
            processedMessages.delete(key);
            deletedCount++;
        }
    }
    if (deletedCount > 0) {
        console.log(`Cleaned up ${deletedCount} old processed message entries.`);
    }
}

function cleanupProcessedCallbacks() {
    const now = Date.now();
    let deletedCount = 0;
    for (const [key, timestamp] of processedCallbacks.entries()) {
        if (now - timestamp > PROCESSED_MESSAGE_TTL) {
            processedCallbacks.delete(key);
            deletedCount++;
        }
    }
     if (deletedCount > 0) {
        console.log(`Cleaned up ${deletedCount} old processed callback entries.`);
    }
}
