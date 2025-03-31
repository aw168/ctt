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
      // Use waitUntil for initialization that doesn't need to block the response
      ctx.waitUntil(initialize(env.D1, request));
      isInitialized = true; // Set immediately, assuming init starts
    }

    // Schedule background tasks like cleanup if not already scheduled
     // Run cleanup periodically in the background
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
          // Don't await, let it run in background after returning OK
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
      // Run initialization tasks concurrently
      await Promise.all([
        checkAndRepairTables(d1),
        autoRegisterWebhook(initialRequest),
        checkBotPermissions(),
        // Initial cleanup run can also be part of init
        // cleanExpiredVerificationCodes(d1) // Already called via waitUntil in fetch
      ]);
    }

     /**
     * @param {CfRequest} request
     */
    async function autoRegisterWebhook(request) {
      const webhookUrl = `${new URL(request.url).origin}/webhook`;
      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/setWebhook`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
              url: webhookUrl,
              allowed_updates: ["message", "callback_query"] // Specify desired updates
            }),
        });
        const data = await response.json(); // Always parse JSON response
        if (!response.ok || !data.ok) {
          console.error('Webhook auto-registration failed:', JSON.stringify(data || { status: response.status }, null, 2));
        } else {
          console.log('Webhook auto-registration successful.');
        }
      } catch (error) {
        console.error('Error during webhook auto-registration:', error);
      }
    }

    async function checkBotPermissions() {
      try {
        const chatResponse = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getChat`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ chat_id: GROUP_ID })
        });
        const chatData = await chatResponse.json();
        if (!chatData.ok) {
          console.error(`Failed to access group ${GROUP_ID}: ${chatData.description}. Is GROUP_ID correct and bot a member?`);
          // Consider throwing or returning early if group access fails
          // throw new Error(`Failed to access group: ${chatData.description}`);
          return; // Stop permission check if group not accessible
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
          console.error(`Failed to get bot member status for bot ${botId} in group ${GROUP_ID}: ${memberData.description}. Is bot an admin?`);
          // throw new Error(`Failed to get bot member status: ${memberData.description}`);
          return; // Stop if bot member status fails
        }

        const result = memberData.result;
        // Simplified check: Does it have *some* admin rights? Essential ones checked below.
        const isAdmin = result.status === 'administrator' || result.status === 'creator';
        console.log(`Bot Status in group ${GROUP_ID}: ${result.status}`);


        // Check specific essential permissions
        const canSendMessages = result.can_send_messages === true; // Essential for any interaction
        const canManageTopics = result.can_manage_topics === true; // Essential for creating topics
        const canDeleteMessages = result.can_delete_messages === true; // Needed for /admin cleanup etc.
        const canPinMessages = result.can_pin_messages === true; // Needed for pinning info message

        let missingPermissions = [];
        if (!canSendMessages) missingPermissions.push("Send Messages");
        if (!canManageTopics) missingPermissions.push("Manage Topics");
        if (!canDeleteMessages) missingPermissions.push("Delete Messages");
        if (!canPinMessages) missingPermissions.push("Pin Messages");


        if (missingPermissions.length > 0) {
          console.error('-------------------------------------------------');
          console.error('PERMISSION WARNING: Bot lacks ESSENTIAL permissions!');
          console.error(`Group ID: ${GROUP_ID}`);
          console.error(`Missing: ${missingPermissions.join(', ')}`);
          if (!isAdmin) console.error("Hint: Bot is not an administrator in the group.");
          console.error('Please grant the bot Administrator rights with these permissions.');
          console.error('-------------------------------------------------');
        } else {
            console.log('Bot has essential permissions (Send Messages, Manage Topics, Delete Messages, Pin Messages).');
            if (!isAdmin) console.warn("Warning: Bot has permissions but is not an administrator. Granting full admin rights is recommended.");
        }

      } catch (error) {
        console.error('Error checking bot permissions:', error);
        // throw error; // Decide if this should halt initialization
      }
    }

    async function getBotId() {
      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getMe`);
        const data = await response.json();
        if (!data.ok) {
            console.error(`Failed to get bot ID: ${data.description}`);
            return null; // Return null on failure
        }
        return data.result.id;
      } catch (error) {
          console.error("Exception during getBotId:", error);
          return null;
      }
    }

    /** @param {D1Database} d1 */
    async function checkAndRepairTables(d1) {
      console.log("Checking/Repairing DB Tables..."); // Log start
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
              is_rate_limited: 'BOOLEAN DEFAULT FALSE', // Keep if used, check logic
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
              console.log(`Table '${tableName}' not found. Creating...`);
              await createTable(d1, tableName, structure);
              tableExists = true; // Now it exists
              // If table is newly created, no need to check columns
              if (tableName === 'settings') {
                 console.log(`Creating index for newly created 'settings' table...`);
                 await d1.exec('CREATE INDEX IF NOT EXISTS idx_settings_key ON settings (key)');
              }
              continue; // Skip column check for new table
            } else {
                tableExists = true;
            }

            // Check columns if table existed
            const columnsResult = await d1.prepare(
              `PRAGMA table_info(${tableName})`
            ).all();

            if (!columnsResult || !columnsResult.results) {
                console.error(`Failed to get column info for existing table '${tableName}'. Skipping column check.`);
                continue;
            }

            const currentColumns = new Map(
              columnsResult.results.map(col => [col.name.toLowerCase(), { // Compare lowercase
                type: col.type.toUpperCase(),
                notnull: col.notnull,
                dflt_value: col.dflt_value // Keep original case if needed later
              }])
            );

            for (const [colName, colDef] of Object.entries(structure.columns)) {
              if (!currentColumns.has(colName.toLowerCase())) {
                console.log(`Column '${colName}' missing in table '${tableName}'. Adding...`);
                // Basic split for type and constraints
                const columnParts = colDef.trim().split(/\s+/);
                const colType = columnParts[0]; // e.g., TEXT, INTEGER, BOOLEAN
                const constraints = columnParts.slice(1).join(' '); // e.g., PRIMARY KEY, DEFAULT FALSE
                const addColumnSQL = `ALTER TABLE ${tableName} ADD COLUMN ${colName} ${colType} ${constraints}`;
                try {
                    await d1.exec(addColumnSQL);
                     console.log(`Column '${colName}' added.`);
                } catch (addColumnError) {
                    console.error(`Failed to add column '${colName}' to '${tableName}':`, addColumnError);
                    // Decide if this requires table recreation or just logging
                }
              }
              // Optional: Add type/constraint checking here if needed
            }

            // Ensure index exists on settings table (check even if table existed)
            if (tableName === 'settings') {
              const indexInfo = await d1.prepare(
                  `SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=? AND name='idx_settings_key'`
              ).bind(tableName).first();
              if (!indexInfo) {
                  console.log(`Index 'idx_settings_key' missing for 'settings'. Creating...`);
                  await d1.exec('CREATE INDEX IF NOT EXISTS idx_settings_key ON settings (key)');
              }
            }

          } catch (error) {
            console.error(`Error checking/repairing table '${tableName}':`, error);
            // Attempt recovery only if the error suggests structural issues
             if (!tableExists || error.message.includes("no such table") || error.message.includes("syntax error")) {
                 console.warn(`Attempting to recreate table '${tableName}' due to error.`);
                 try {
                     await d1.exec(`DROP TABLE IF EXISTS ${tableName}`);
                     await createTable(d1, tableName, structure);
                     console.log(`Table '${tableName}' recreated.`);
                      if (tableName === 'settings') {
                         await d1.exec('CREATE INDEX IF NOT EXISTS idx_settings_key ON settings (key)');
                      }
                 } catch (recreateError) {
                     console.error(`FATAL: Failed to recreate table '${tableName}':`, recreateError);
                     throw recreateError; // Stop initialization if recreation fails
                 }
             } else {
                 console.error(`Could not automatically repair table '${tableName}'. Manual intervention might be needed.`);
             }
          }
        }

        // Ensure default settings exist (run after all tables are checked/created)
        console.log("Ensuring default settings records exist...");
        await Promise.all([
          d1.prepare('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)')
            .bind('verification_enabled', 'true').run(),
          d1.prepare('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)')
            .bind('user_raw_enabled', 'true').run()
        ]);

        // Load settings into cache AFTER ensuring tables/records exist
        await loadSettingsToCache(d1); // Make this a separate function
        console.log("DB Check/Repair Finished.");

      } catch (error) {
        console.error('Critical error during checkAndRepairTables:', error);
        // Depending on the severity, you might want to prevent the worker from proceeding
         throw error;
      }
    }

     /**
     * @param {D1Database} d1
     */
    async function loadSettingsToCache(d1) {
        try {
            const results = await d1.prepare('SELECT key, value FROM settings').all();
            if (results?.results) {
                 settingsCache.set('verification_enabled', (results.results.find(r => r.key === 'verification_enabled')?.value || 'true') === 'true');
                 settingsCache.set('user_raw_enabled', (results.results.find(r => r.key === 'user_raw_enabled')?.value || 'true') === 'true');
                  console.log("Settings loaded into cache:", settingsCache);
            } else {
                 console.warn("Failed to load settings from DB or no settings found. Using defaults.");
                  settingsCache.set('verification_enabled', true); // Default
                  settingsCache.set('user_raw_enabled', true); // Default
            }
        } catch (error) {
            console.error('Error loading settings into cache:', error);
            // Fallback to defaults if loading fails critically
            settingsCache.set('verification_enabled', true);
            settingsCache.set('user_raw_enabled', true);
        }
    }


    /**
     * @param {D1Database} d1
     * @param {string} tableName
     * @param {{ columns: Record<string, string> }} structure
     */
    async function createTable(d1, tableName, structure) {
      const columnsDef = Object.entries(structure.columns)
        .map(([name, def]) => `"${name}" ${def}`) // Quote names
        .join(', ');
      const createSQL = `CREATE TABLE "${tableName}" (${columnsDef});`;
       console.log(`Executing SQL: ${createSQL}`); // Log SQL
      await d1.exec(createSQL);
    }

    /** @param {D1Database} d1 */
    async function cleanExpiredVerificationCodes(d1) {
      const now = Date.now();
      // Avoid running too frequently if called multiple times rapidly
      if (now - lastCleanupTime < 60 * 1000) { // Run at most once per minute
        return;
      }

      console.log("Checking for expired verification codes..."); // Log start
      try {
        const nowSeconds = Math.floor(now / 1000);
        // Select IDs of users whose code has expired
        const expiredCodes = await d1.prepare(
          'SELECT chat_id FROM user_states WHERE code_expiry IS NOT NULL AND code_expiry < ?'
        ).bind(nowSeconds).all();

        if (expiredCodes.results && expiredCodes.results.length > 0) {
          const idsToClean = expiredCodes.results.map(({ chat_id }) => chat_id);
          console.log(`Found ${idsToClean.length} expired codes. Cleaning for IDs: ${idsToClean.join(', ')}`);

          // Batch update to clear expired state
          await d1.batch(
            idsToClean.map(chat_id =>
              d1.prepare(
                // Also reset is_verifying flag
                'UPDATE user_states SET verification_code = NULL, code_expiry = NULL, is_verifying = FALSE WHERE chat_id = ?'
              ).bind(chat_id)
            )
          );
           console.log(`Cleaned ${idsToClean.length} expired verification states.`);

           // Clear relevant cache entries
           idsToClean.forEach(chat_id => {
               const cachedState = userStateCache.get(chat_id);
               if (cachedState) {
                    cachedState.verification_code = null;
                    cachedState.code_expiry = null;
                    cachedState.is_verifying = false;
                    // Keep other state fields like is_blocked etc.
                    userStateCache.set(chat_id, cachedState);
               }
           });

        }
        // else { console.log("No expired codes found."); }

        lastCleanupTime = now; // Update time after successful check/cleanup
      } catch (error) {
        console.error('Error cleaning expired verification codes:', error);
        // Don't update lastCleanupTime on error, so it retries sooner
      }
    }

    /** @param {any} update */
    async function handleUpdate(update) {
       // console.log("Handling update:", JSON.stringify(update, null, 2)); // Optional: Log incoming updates
      if (update.message) {
        const message = update.message;
        const messageId = message.message_id?.toString();
        const chatId = message.chat?.id?.toString();

        // Basic check for essential data
        if (!messageId || !chatId) {
            console.warn("Received message update lacking message_id or chat_id:", update);
            return;
        }

        const messageKey = `${chatId}:${messageId}`;

        if (processedMessages.has(messageKey)) {
          // console.log(`Skipping duplicate message: ${messageKey}`);
          return; // Already processed
        }
        processedMessages.add(messageKey);

        // Simple cleanup for processedMessages to prevent unbounded growth
        if (processedMessages.size > 10000) {
          const oldestKeys = Array.from(processedMessages).slice(0, 5000);
          oldestKeys.forEach(key => processedMessages.delete(key));
          console.log("Cleaned processedMessages Set.");
        }

        await onMessage(message); // Await processing

      } else if (update.callback_query) {
         const callbackQuery = update.callback_query;
         const callbackId = callbackQuery.id;
         const chatId = callbackQuery.message?.chat?.id?.toString();
         const messageId = callbackQuery.message?.message_id?.toString(); // ID of the message the button is on

          if (!callbackId || !chatId || !messageId) {
              console.warn("Received callback_query lacking id, chat_id or message_id:", update);
              return;
          }

        // Use a more specific key including callback ID to prevent race conditions
        const callbackKey = `${chatId}:${messageId}:${callbackId}`;

        if (processedCallbacks.has(callbackKey)) {
           // console.log(`Skipping duplicate callback: ${callbackKey}`);
           // Answer the duplicate callback anyway to remove loading state
            ctx.waitUntil(answerCallbackQuery(callbackId).catch(()=>{}));
           return; // Already processing/processed
        }
        processedCallbacks.add(callbackKey);

         // Cleanup processedCallbacks
        if (processedCallbacks.size > 5000) {
             const oldestKeys = Array.from(processedCallbacks).slice(0, 2500);
             oldestKeys.forEach(key => processedCallbacks.delete(key));
              console.log("Cleaned processedCallbacks Set.");
         }

        await onCallbackQuery(callbackQuery); // Await processing
      }
      // else { console.log("Ignoring unhandled update type:", Object.keys(update)); }
    }

    /** @param {import('@cloudflare/workers-types').TelegramMessage} message */
    async function onMessage(message) {
      const chatId = message.chat.id.toString();
      const text = message.text || '';
      const messageId = message.message_id;
      const fromUserId = message.from?.id?.toString(); // User who sent the message

      // --- Group Message Handling ---
      if (chatId === GROUP_ID) {
        const topicId = message.message_thread_id?.toString(); // Ensure string
        const botId = await getBotId();

        // Ignore messages from the bot itself in the group
        if (fromUserId && botId && fromUserId === botId.toString()) {
            return;
        }

        // Handle messages within a topic thread
        if (topicId) {
          const privateChatId = await getPrivateChatId(topicId);
          if (privateChatId) {
              // --- Admin Commands in Topic ---
              const senderIsAdmin = fromUserId ? await checkIfAdmin(fromUserId) : false;

              if (text === '/admin' && senderIsAdmin) {
                  await sendAdminPanel(chatId, topicId, privateChatId, messageId);
                  return; // Handled
              }
              if (text.startsWith('/reset_user') && senderIsAdmin) {
                  await handleResetUser(chatId, topicId, privateChatId, text); // Pass privateChatId
                  return; // Handled
              }
              if (text.startsWith('/delete_user') && senderIsAdmin) {
                   await handleDeleteUser(chatId, topicId, privateChatId); // Pass privateChatId
                   return; // Handled
              }
              if (!senderIsAdmin && (text === '/admin' || text.startsWith('/reset_user') || text.startsWith('/delete_user'))) {
                  // Optional: Notify non-admin they can't use the command
                  // await sendMessageToTopic(topicId, "Only admins can use this command.");
                  return; // Ignore admin command attempts by non-admins
              }

              // --- Forward Admin Message to User ---
              // If message is from an admin (or anyone) in the topic, forward to the associated user
              await forwardMessageToPrivateChat(privateChatId, message);

          } else {
             // console.log(`Message in topic ${topicId}, but no private chat mapping found.`);
             // Optional: Send message to topic indicating unknown user?
          }
        } else {
            // --- Handle Commands in General Group Chat (optional) ---
            // E.g., if an admin needs to run a command not tied to a topic
            // if (text === '/some_global_admin_command' && fromUserId && await checkIfAdmin(fromUserId)) { ... }
        }
        return; // Finish handling for group messages
      }

      // --- Private Message Handling ---

      // Get user state (cache or DB)
      let userState = userStateCache.get(chatId);
      if (userState === undefined) {
         try {
            userState = await env.D1.prepare('SELECT * FROM user_states WHERE chat_id = ?') // Select all fields initially
              .bind(chatId)
              .first();
            if (!userState) {
              // User not found, create basic state
               console.log(`User ${chatId} not found in user_states. Creating entry.`);
              await env.D1.prepare('INSERT INTO user_states (chat_id) VALUES (?)').bind(chatId).run();
              userState = { chat_id: chatId, is_blocked: false, is_first_verification: true, is_verified: false, verified_expiry: null, verification_code: null, code_expiry: null, last_verification_message_id: null, is_rate_limited: false, is_verifying: false };
            }
            userStateCache.set(chatId, userState); // Cache the fetched/new state
         } catch (dbError) {
              console.error(`DB Error fetching user state for ${chatId}:`, dbError);
              await sendMessageToUser(chatId, "An internal error occurred. Please try again later.");
              return; // Stop if we can't get user state
         }
      }

      // 1. Check if blocked
      if (userState.is_blocked) {
        // console.log(`Message from blocked user ${chatId} ignored.`);
        // await sendMessageToUser(chatId, "您已被拉黑，无法发送消息。请联系管理员解除拉黑。"); // Kept original message
        return;
      }

      // 2. Handle /start command
      if (text === '/start') {
        if (await checkStartCommandRate(chatId)) {
          await sendMessageToUser(chatId, "您发送 /start 命令过于频繁，请稍后再试！"); // Kept original message
          return;
        }

        const verificationEnabled = settingsCache.get('verification_enabled');
        const isFirstTime = userState.is_first_verification; // Use DB/cache state

        if (verificationEnabled && isFirstTime) {
          await sendMessageToUser(chatId, "你好，欢迎使用私聊机器人，请完成验证以开始使用！"); // Kept original message
          await handleVerification(chatId, messageId); // Start verification
        } else {
          // Not first time, or verification disabled
           const nowSeconds = Math.floor(Date.now() / 1000);
           const isCurrentlyVerified = userState.is_verified && userState.verified_expiry && nowSeconds < userState.verified_expiry;

           if (verificationEnabled && !isCurrentlyVerified) {
               // Needs verification (again)
               await sendMessageToUser(chatId, "请先完成验证。"); // Simple prompt
               await handleVerification(chatId, messageId);
           } else {
               // Already verified or verification off - send welcome/info message
               const successMessage = await getVerificationSuccessMessage(); // Use the dynamic message
               await sendMessageToUser(chatId, `${successMessage}\n你好，欢迎使用私聊机器人，现在发送信息吧！`); // Kept original message structure
           }
        }
        return; // Handled /start
      }

      // 3. Check verification & rate limit for regular messages
      const verificationEnabled = settingsCache.get('verification_enabled');
      const nowSeconds = Math.floor(Date.now() / 1000);
      // Use state directly from cache/DB object loaded earlier
      const isVerified = userState.is_verified && userState.verified_expiry && nowSeconds < userState.verified_expiry;
      // Fetch latest 'is_verifying' state for accuracy, as it can change rapidly
      const latestDbState = await env.D1.prepare('SELECT is_verifying FROM user_states WHERE chat_id = ?').bind(chatId).first();
      const isVerifying = latestDbState?.is_verifying || userState.is_verifying || false; // Use DB if available, else cached


      if (verificationEnabled && !isVerified) {
          if (isVerifying) {
            // Already verifying, maybe remind them gently?
             // console.log(`User ${chatId} is already verifying. Ignoring message: ${text}`);
             // await sendMessageToUser(chatId, `请先完成当前的验证问题。`); // Optional reminder
            return; // Don't process message, don't start new verification
          }
          // Needs verification, not currently verifying
          const promptText = text ? `“${text.substring(0, 50)}${text.length > 50 ? '...' : ''}”` : "您的具体信息"; // Kept original prompt structure
          await sendMessageToUser(chatId, `请完成验证后发送消息${promptText}。`); // Kept original message
          await handleVerification(chatId, messageId); // Start verification
          return; // Stop processing this message
      }

      // 4. Check rate limit (only if verified or verification disabled)
      if (await checkMessageRate(chatId)) {
         console.log(`User ${chatId} rate limited.`);
         // Avoid spamming rate limit message - maybe use the is_rate_limited flag?
         if (!userState.is_rate_limited) { // Check flag before sending message
             await sendMessageToUser(chatId, "您发送消息过于频繁，请稍候再试。");
             // Set flag (optional, requires DB update logic)
             // userState.is_rate_limited = true; userStateCache.set(chatId, userState); /* ... DB update ... */
         }
         return; // Stop processing
      }
      // else { // Reset flag if needed (optional)
         // if (userState.is_rate_limited) { userState.is_rate_limited = false; userStateCache.set(chatId, userState); /* ... DB update ... */ }
      // }

      // 5. Forward verified message to topic
      try {
         // ***** FIX START *****
         // Fetch user info first
         const userInfo = await getUserInfo(chatId);
         // THEN, check for existing topic ID (cache or DB)
         let topicId = await getExistingTopicId(chatId); // Checks cache & DB

         const userName = userInfo.username || `User_${chatId}`;
         const nickname = userInfo.nickname || userName;
         const topicName = nickname;

         // If no topic found, create one
         if (!topicId) { // Check if topicId is null/undefined/falsy
             topicId = await createForumTopic(topicName, userName, nickname, userInfo.id || chatId);
             await saveTopicId(chatId, topicId.toString()); // Save the new mapping (saves to DB and updates cache)
         }
          // Ensure topicId is string for API calls
          const topicIdStr = topicId.toString();
          // ***** FIX END *****


          // --- Original Forwarding logic ---
          try {
              if (text) {
                  // Keep original format
                  const formattedMessage = `${nickname}:\n${text}`;
                  await sendMessageToTopic(topicIdStr, formattedMessage);
              } else {
                  // Copy non-text messages
                  await copyMessageToTopic(topicIdStr, message);
              }
          } catch (error) {
              // Keep original error handling for closed/deleted topics
              // Use includes for broader matching of potential error messages
               if (error.message && (error.message.includes('400') || error.message.includes('thread not found') || error.message.includes('topic closed'))) {
                   console.warn(`Topic ${topicIdStr} likely closed/deleted for user ${chatId}. Attempting to recreate.`);
                   // Recreate topic using the same user info
                   const newTopicId = await createForumTopic(topicName, userName, nickname, userInfo.id || chatId);
                   await saveTopicId(chatId, newTopicId.toString()); // Save the NEW ID, overwriting old mapping
                   const newTopicIdStr = newTopicId.toString();

                   // Retry forwarding to the new topic
                   if (text) {
                       const formattedMessage = `${nickname}:\n${text}`;
                       await sendMessageToTopic(newTopicIdStr, formattedMessage);
                   } else {
                       await copyMessageToTopic(newTopicIdStr, message);
                   }
                   console.log(`Successfully forwarded message to newly created topic ${newTopicIdStr} for user ${chatId}.`);
               } else {
                   throw error; // Re-throw other unexpected forwarding errors
               }
          }
          // --- End Original Forwarding Logic ---

      } catch (error) {
        // Keep original outer catch block for forwarding issues
        console.error(`Error handling message from chatId ${chatId}:`, error);
        // Attempt to notify admin in general chat (topicId = null)
        await sendMessageToTopic(null, `无法转发用户 ${chatId} 的消息：${error.message}`);
        await sendMessageToUser(chatId, "消息转发失败，请稍后再试或联系管理员。");
      }
    }

    /**
     * @param {string} adminChatId - Group ID
     * @param {string} topicId - Topic ID
     * @param {string} userChatId - User's private chat ID derived from topic
     * @param {string} text - The command text, e.g., "/reset_user 12345"
     */
    async function handleResetUser(adminChatId, topicId, userChatId, text) {
      // userChatId is already derived via getPrivateChatId before calling this
       if (!userChatId) {
           await sendMessageToTopic(topicId, 'Error: Could not find associated user for this topic.');
           return;
       }

       // Optional: Extract target ID from text if command format was different, but here we assume userChatId is the target
       // const parts = text.split(' ');
       // const targetChatId = parts[1] || userChatId; // Example if needed

       const targetChatId = userChatId; // The user associated with this topic IS the target

      try {
        // Batch delete state and rate limits
        await env.D1.batch([
          env.D1.prepare('DELETE FROM user_states WHERE chat_id = ?').bind(targetChatId),
          env.D1.prepare('DELETE FROM message_rates WHERE chat_id = ?').bind(targetChatId)
          // Keep the chat_topic_mappings entry unless explicitly deleted
        ]);

        // Clear caches for the user
        userStateCache.set(targetChatId, undefined); // Force reload on next interaction
        messageRateCache.set(targetChatId, undefined); // Force reload
        // topicIdCache.set(targetChatId, undefined); // Keep topic mapping unless deleted

        console.log(`Admin reset state for user ${targetChatId} via topic ${topicId}`);
        await sendMessageToTopic(topicId, `用户 ${targetChatId} 的状态 (验证, 速率) 已重置. 下次交互时需要重新验证.`); // Confirmation message

      } catch (error) {
        console.error(`Error resetting user ${targetChatId}:`, error);
        await sendMessageToTopic(topicId, `重置用户 ${targetChatId} 失败：${error.message}`);
      }
    }

    /**
     * @param {string} adminChatId - Group ID
     * @param {string} topicId - Topic ID
     * @param {string} userChatId - User's private chat ID derived from topic
     */
     async function handleDeleteUser(adminChatId, topicId, userChatId) {
        if (!userChatId) {
             await sendMessageToTopic(topicId, 'Error: Could not find associated user for deletion.');
             return;
         }
         const targetChatId = userChatId;

         try {
             // Delete ALL records for this user
             await env.D1.batch([
                 env.D1.prepare('DELETE FROM user_states WHERE chat_id = ?').bind(targetChatId),
                 env.D1.prepare('DELETE FROM message_rates WHERE chat_id = ?').bind(targetChatId),
                 env.D1.prepare('DELETE FROM chat_topic_mappings WHERE chat_id = ?').bind(targetChatId) // Also delete topic mapping
             ]);

             // Clear all caches
             userStateCache.set(targetChatId, undefined);
             messageRateCache.set(targetChatId, undefined);
             topicIdCache.set(targetChatId, undefined);
             userInfoCache.set(targetChatId, undefined); // Clear user info too

             console.log(`Admin deleted all data for user ${targetChatId} via topic ${topicId}`);
             await sendMessageToTopic(topicId, `用户 ${targetChatId} 的所有数据 (状态, 速率, 话题关联) 已删除。话题本身保留，管理员可手动关闭或归档。`);

             // Optional: Close the topic (requires permission)
             // try {
             //    await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/closeForumTopic`, { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ chat_id: adminChatId, message_thread_id: topicId }) });
             // } catch (closeError) { console.error("Could not close topic after user deletion:", closeError); }

         } catch (error) {
             console.error(`Error deleting user ${targetChatId}:`, error);
             await sendMessageToTopic(topicId, `删除用户 ${targetChatId} 失败：${error.message}`);
         }
     }


    /**
     * @param {string} chatId - Group Chat ID (GROUP_ID)
     * @param {string} topicId - Topic Thread ID
     * @param {string} privateChatId - User's Private Chat ID
     * @param {number | string} originalMessageId - ID of the /admin message to delete
     */
    async function sendAdminPanel(chatId, topicId, privateChatId, originalMessageId) {
      try {
        // Fetch current settings and user block state
        const verificationEnabled = settingsCache.get('verification_enabled');
        const userRawEnabled = settingsCache.get('user_raw_enabled');
        let isBlocked = false;
         const userState = userStateCache.get(privateChatId) || await env.D1.prepare('SELECT is_blocked FROM user_states WHERE chat_id = ?').bind(privateChatId).first();
        if (userState) {
             isBlocked = userState.is_blocked || false;
             // Update cache if fetched from DB
             if (!userStateCache.get(privateChatId)) userStateCache.set(privateChatId, userState);
         }

        // Determine button text/callback based on state
        const blockAction = isBlocked ? 'unblock' : 'block';
        const blockText = isBlocked ? '解除拉黑' : '拉黑用户';

        const buttons = [
          [ // Row 1: Block/Unblock
            { text: blockText, callback_data: `${blockAction}_${privateChatId}` }
          ],
          [ // Row 2: Toggles & Query
            { text: verificationEnabled ? '关闭验证码' : '开启验证码', callback_data: `toggle_verification_${privateChatId}` }, // privateChatId added for context, action is global
            { text: '查询黑名单', callback_data: `check_blocklist_${privateChatId}` } // privateChatId added for context
          ],
          [ // Row 3: User Raw & Link
            { text: userRawEnabled ? '关闭用户Raw' : '开启用户Raw', callback_data: `toggle_user_raw_${privateChatId}` }, // privateChatId added for context
            { text: 'GitHub项目', url: 'https://github.com/iawooo/ctt' }
          ],
           [ // Row 4: Delete User (replaces Reset User button for direct action)
             // Keep original Delete User button
            { text: '删除用户', callback_data: `delete_user_${privateChatId}` }
            // Consider adding Reset back if needed: { text: '重置用户状态', callback_data: `reset_user_${privateChatId}` }
          ]
        ];

        const adminMessage = `管理员面板 (用户: ${privateChatId})\n请选择操作：`;

        // Send the panel as a new message
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

        // Delete the original "/admin" command message if ID is provided
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
         // Attempt to notify in the topic if panel fails
         try { await sendMessageToTopic(topicId, "发送管理面板时出错。"); } catch {}
      }
    }

    async function getVerificationSuccessMessage() {
      const userRawEnabled = settingsCache.get('user_raw_enabled');
      const defaultMessage = '验证成功！您现在可以与客服聊天。'; // Original default

      if (!userRawEnabled) {
        return defaultMessage;
      }

      try {
        // Try fetching the specific start.md first
        const startUrl = 'https://raw.githubusercontent.com/iawooo/ctt/main/CFTeleTrans/start.md';
        const response = await fetch(startUrl, { headers: { 'Cache-Control': 'no-cache' }}); // Try fresh fetch

        if (!response.ok) {
          console.warn(`Failed to fetch start.md (${response.status}). Trying README.md...`);
           // Fallback to README.md
           const readmeUrl = 'https://raw.githubusercontent.com/iawooo/ctt/main/README.md';
           const readmeResponse = await fetch(readmeUrl, { headers: { 'Cache-Control': 'no-cache' }});
           if (!readmeResponse.ok) {
               console.warn(`Failed to fetch fallback README.md (${readmeResponse.status}). Using default message.`);
               return defaultMessage;
           }
           const readmeMessage = await readmeResponse.text();
           return readmeMessage.trim() || defaultMessage; // Use README content or default
        }

        const message = await response.text();
        return message.trim() || defaultMessage; // Use start.md content or default

      } catch (error) {
        console.error("Error fetching custom verification success message:", error);
        return defaultMessage; // Fallback on network or other errors
      }
    }

    async function getNotificationContent() {
       const defaultNotification = "管理员将会收到通知。"; // Simple default
      try {
        const url = 'https://raw.githubusercontent.com/iawooo/ctt/main/CFTeleTrans/notification.md';
        const response = await fetch(url, { headers: { 'Cache-Control': 'no-cache' } });

        if (!response.ok) {
             console.warn(`Failed to fetch notification.md (${response.status}). Using default notification.`);
             return defaultNotification;
        }
        const content = await response.text();
        return content.trim() || defaultNotification; // Use fetched content or default

      } catch (error) {
        console.error("Error fetching notification content:", error);
        return defaultNotification; // Fallback on error
      }
    }

    /** @param {string} chatId */
    async function checkStartCommandRate(chatId) {
      const now = Date.now();
      const window = 5 * 60 * 1000; // 5 minutes
      const maxStartsPerWindow = 3; // Allow a few starts

      let data = messageRateCache.get(chatId);
      let dbNeedsUpdate = false;

      if (data === undefined) {
        const dbData = await env.D1.prepare('SELECT start_count, start_window_start FROM message_rates WHERE chat_id = ?')
          .bind(chatId)
          .first();
        // Initialize data object, ensuring other potential fields (like message_count) aren't lost if partially cached
        data = { ...(messageRateCache.get(chatId)), start_count: dbData?.start_count || 0, start_window_start: dbData?.start_window_start || 0 };
      }

      if (now - (data.start_window_start || 0) > window) {
        data.start_count = 1;
        data.start_window_start = now;
        dbNeedsUpdate = true; // Needs DB update because window reset
      } else {
        data.start_count = (data.start_count || 0) + 1;
        // Update DB only if count changes (optimization)
        dbNeedsUpdate = true;
      }

      // Update cache immediately
      messageRateCache.set(chatId, data);

       // Update DB if necessary
       if (dbNeedsUpdate) {
           ctx.waitUntil( // Update DB in background
               env.D1.prepare(
                   'INSERT INTO message_rates (chat_id, start_count, start_window_start, message_count, window_start) VALUES (?, ?, ?, COALESCE((SELECT message_count FROM message_rates WHERE chat_id = ?), 0), COALESCE((SELECT window_start FROM message_rates WHERE chat_id = ?), 0)) ON CONFLICT(chat_id) DO UPDATE SET start_count = excluded.start_count, start_window_start = excluded.start_window_start'
               ).bind(chatId, data.start_count, data.start_window_start, chatId, chatId).run()
               .catch(dbError => console.error(`DB Error updating start rate for ${chatId}:`, dbError))
           );
       }

      return data.start_count > maxStartsPerWindow;
    }

    /** @param {string} chatId */
    async function checkMessageRate(chatId) {
      const now = Date.now();
      const window = 60 * 1000; // 1 minute (as per original)

      let data = messageRateCache.get(chatId);
      let dbNeedsUpdate = false;

      if (data === undefined) {
         const dbData = await env.D1.prepare('SELECT message_count, window_start FROM message_rates WHERE chat_id = ?')
           .bind(chatId)
           .first();
          data = { ...(messageRateCache.get(chatId)), message_count: dbData?.message_count || 0, window_start: dbData?.window_start || 0 };
      }


      if (now - (data.window_start || 0) > window) {
        data.message_count = 1;
        data.window_start = now;
        dbNeedsUpdate = true;
      } else {
        data.message_count = (data.message_count || 0) + 1;
        dbNeedsUpdate = true; // Count increased
      }

      // Update cache
      messageRateCache.set(chatId, data);

      // Update DB if necessary
       if (dbNeedsUpdate) {
           ctx.waitUntil( // Update DB in background
                env.D1.prepare(
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
      // Keep original logic: Cache first, then DB
      const cachedValue = settingsCache.get(key);
      // Check explicitly for null, as boolean `false` is a valid cached value
      if (cachedValue !== null) {
          // Return the string representation expected by callers
          return String(cachedValue);
      }

      try {
        const result = await d1.prepare('SELECT value FROM settings WHERE key = ?')
          .bind(key)
          .first();
        const value = result?.value || null; // DB value or null

        // Update cache with DB value (convert known booleans)
         if (value !== null) {
             if (key === 'verification_enabled' || key === 'user_raw_enabled') {
                 settingsCache.set(key, value === 'true');
             } else {
                  settingsCache.set(key, value); // Store others as string
             }
         } else {
             // If DB is null, should we cache null or default? Caching null is safer.
             settingsCache.set(key, null); // Cache the null to avoid repeated DB checks
             // However, the original seemed to default, let's stick to that if null
              if (key === 'verification_enabled') return 'true';
              if (key === 'user_raw_enabled') return 'true';
         }

        return value;

      } catch (error) {
        console.error(`Error getting setting ${key}:`, error);
        // Fallback to defaults on error, consistent with original implicit behavior
         if (key === 'verification_enabled') return 'true';
         if (key === 'user_raw_enabled') return 'true';
         return null;
      }
    }

    /**
     * @param {string} key
     * @param {string} value - Value must be a string for DB
     */
    async function setSetting(key, value) {
      try {
        // Update DB
        await env.D1.prepare('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)')
          .bind(key, value)
          .run();

        // Update Cache (convert to boolean if applicable)
        if (key === 'verification_enabled' || key === 'user_raw_enabled') {
          settingsCache.set(key, value === 'true');
        } else {
           settingsCache.set(key, value); // Store other settings as string
        }
        console.log(`Setting '${key}' updated to '${value}' in DB and cache.`);

      } catch (error) {
        console.error(`Error setting ${key} to ${value}:`, error);
        // Invalidate cache for this key on error? Or leave stale? Leaving stale for now.
        throw error; // Re-throw to indicate failure
      }
    }

    /** @param {import('@cloudflare/workers-types').TelegramCallbackQuery} callbackQuery */
    async function onCallbackQuery(callbackQuery) {
      // --- Basic Info ---
      const callbackQueryId = callbackQuery.id;
      const userId = callbackQuery.from.id.toString(); // User who clicked
      const data = callbackQuery.data;
      const message = callbackQuery.message;
      if (!message) {
          console.warn("Callback query received without associated message:", callbackQuery);
          // Answer anyway to dismiss loading state
          ctx.waitUntil(answerCallbackQuery(callbackQueryId).catch(()=>{}));
          return;
      }
      const chatId = message.chat.id.toString(); // Chat where the message is (group or private)
      const messageId = message.message_id;
      const topicId = message.message_thread_id?.toString(); // Topic ID if it's in a group topic


      // --- Answer Callback Early ---
      ctx.waitUntil(answerCallbackQuery(callbackQueryId).catch(err => console.error(`Non-critical: Failed to answer callback ${callbackQueryId}: ${err}`)));


      // --- Parse Action and Target ---
      const parts = data.split('_');
      const action = parts[0];
      let targetChatId = null; // Usually the user's private chat ID

      // Determine targetChatId based on action convention
      if (action === 'verify') {
          targetChatId = parts[1]; // verify_{userChatId}_{answer}_{result}
      } else if (['block', 'unblock', 'delete', 'reset', 'toggle', 'check'].some(prefix => action.startsWith(prefix))) {
          // Assumes format like action_{targetUserId} or action_subaction_{targetUserId}
          targetChatId = parts[parts.length - 1]; // Heuristic: last part is often the ID
          // Validate if it looks like an ID (numeric string)
          if (!/^\d+$/.test(targetChatId) && !/^-?\d+$/.test(targetChatId)) { // Allow negative IDs for chats/channels
               // If the last part isn't numeric, maybe it's the second to last?
               if (parts.length > 1 && /^\d+$/.test(parts[parts.length - 2])) {
                   targetChatId = parts[parts.length - 2];
               } else {
                   console.warn(`Could not reliably determine targetChatId from callback data: ${data}`);
                   targetChatId = null; // Fallback to null if unsure
               }
          }
      }


      // --- Process Actions ---
      try {
        // --- Verification Action (User in Private Chat clicks button) ---
        if (action === 'verify') {
          // data format: verify_{userChatId}_{selectedAnswer}_{result}
          const [, userChatId, selectedAnswer, result] = parts;

          // Security Check: Ensure the clicker is the intended user
          if (userId !== userChatId) {
              console.warn(`Verification callback user mismatch. Clicker: ${userId}, Target: ${userChatId}`);
              // Answer with an alert? Silently ignore? Ignoring for now.
              // await answerCallbackQuery(callbackQueryId, { text: "Invalid action.", show_alert: true });
              return;
          }

           // Delete the verification message regardless of outcome
           ctx.waitUntil(fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ chat_id: userChatId, message_id: messageId })
            }).catch(deleteError => console.warn(`Non-critical: Error deleting verification msg ${messageId} for ${userChatId}: ${deleteError}`)));


          // Check verification state (code, expiry)
           let verificationState = userStateCache.get(userChatId);
           if (verificationState === undefined) {
              verificationState = await env.D1.prepare('SELECT verification_code, code_expiry FROM user_states WHERE chat_id = ?').bind(userChatId).first();
              // Cache even if null
              userStateCache.set(userChatId, verificationState);
           }

          const storedCode = verificationState?.verification_code;
          const codeExpiry = verificationState?.code_expiry;
          const nowSeconds = Math.floor(Date.now() / 1000);

          // Handle expired code
          if (!storedCode || (codeExpiry && nowSeconds > codeExpiry)) {
            await sendMessageToUser(userChatId, '验证码已过期，请重新发送消息以获取新验证码。'); // Keep original msg
            // Clean up potentially stale verifying state in DB/cache
             ctx.waitUntil(env.D1.prepare('UPDATE user_states SET verification_code = NULL, code_expiry = NULL, is_verifying = FALSE WHERE chat_id = ? AND verification_code IS NOT NULL') // Only update if code was set
                 .bind(userChatId).run().catch(e => console.error("DB Error cleaning expired code state:", e)));
             if (verificationState) {
                 verificationState.verification_code = null;
                 verificationState.code_expiry = null;
                 verificationState.is_verifying = false; // Ensure reset
                 userStateCache.set(userChatId, verificationState);
             }
            return; // Stop processing
          }

          // Process correct / incorrect answer
          if (result === 'correct') {
            const verifiedExpiry = nowSeconds + (3600 * 24 * 7); // 7 days expiry
            // Update DB: verified, expiry, clear verification attempt fields, set not first time, not verifying
            await env.D1.prepare(
                'UPDATE user_states SET is_verified = TRUE, verified_expiry = ?, verification_code = NULL, code_expiry = NULL, last_verification_message_id = NULL, is_first_verification = FALSE, is_verifying = FALSE WHERE chat_id = ?'
                ).bind(verifiedExpiry, userChatId).run();

             // Update Cache
             const updatedState = {
                 ...(userStateCache.get(userChatId) || {}), // Get existing cached fields
                 is_verified: true,
                 verified_expiry: verifiedExpiry,
                 verification_code: null,
                 code_expiry: null,
                 last_verification_message_id: null,
                 is_first_verification: false,
                 is_verifying: false
             };
             userStateCache.set(userChatId, updatedState);


            // Reset message rate limit counter
             ctx.waitUntil(env.D1.prepare('UPDATE message_rates SET message_count = 0 WHERE chat_id = ?').bind(userChatId).run().catch(()=>{}));
             let rateData = messageRateCache.get(userChatId);
             if (rateData) { rateData.message_count = 0; messageRateCache.set(userChatId, rateData); }

            // Send success message
            const successMessage = await getVerificationSuccessMessage();
            await sendMessageToUser(userChatId, `${successMessage}\n你好，欢迎使用私聊机器人！现在可以发送消息了。`); // Keep original msg structure

          } else { // Incorrect answer
            await sendMessageToUser(userChatId, '验证失败，请重新尝试。'); // Keep original msg
            // Send a new challenge immediately
            await handleVerification(userChatId, null); // Pass null messageId as old one is deleted
          }

        // --- Admin Actions (Admin in Group Topic clicks button) ---
        } else {
           // Check if clicker is admin
           const isAdmin = await checkIfAdmin(userId);
           if (!isAdmin) {
              // Notify non-admin clicker
              await answerCallbackQuery(callbackQueryId, { text: "只有管理员可以使用此功能。", show_alert: true }); // Keep original msg
              return;
           }

           // Ensure targetChatId is available for user-specific actions
            if (!targetChatId && ['block', 'unblock', 'delete_user'].includes(action)) {
                console.error(`Admin action '${action}' requires a targetChatId, but it was not found in data: ${data}`);
                await sendMessageToTopic(topicId, `执行 '${action}' 操作失败: 无法识别目标用户。`);
                return;
            }


           let refreshAdminPanel = true; // Refresh panel by default after an action
           let panelPrivateChatId = targetChatId || await getPrivateChatId(topicId); // Get ID for panel context

           // Handle specific admin actions based on callback data prefix
           if (action === 'block') {
               await env.D1.prepare('INSERT OR REPLACE INTO user_states (chat_id, is_blocked) VALUES (?, TRUE)')
                   .bind(targetChatId).run();
               userStateCache.set(targetChatId, { ...(userStateCache.get(targetChatId)), is_blocked: true });
               await sendMessageToTopic(topicId, `用户 ${targetChatId} 已被拉黑。`); // Keep original msg
           } else if (action === 'unblock') {
               await env.D1.prepare('UPDATE user_states SET is_blocked = FALSE, is_first_verification = TRUE, is_verified = FALSE WHERE chat_id = ?')
                   .bind(targetChatId).run();
               userStateCache.set(targetChatId, { ...(userStateCache.get(targetChatId)), is_blocked: false, is_first_verification: true, is_verified: false });
               await sendMessageToTopic(topicId, `用户 ${targetChatId} 已解除拉黑。`); // Keep original msg
           } else if (action === 'toggle_verification') {
               const currentState = settingsCache.get('verification_enabled');
               const newState = !currentState;
               await setSetting('verification_enabled', newState.toString()); // Updates DB & cache
               await sendMessageToTopic(topicId, `验证码功能已${newState ? '开启' : '关闭'}。`); // Keep original msg
           } else if (action === 'check_blocklist') {
               const blockedUsersResult = await env.D1.prepare('SELECT chat_id FROM user_states WHERE is_blocked = TRUE').all();
               const blockedIds = blockedUsersResult?.results?.map(r => r.chat_id) || [];
               const blockListText = blockedIds.length > 0 ? blockedIds.join('\n') : '当前没有被拉黑的用户。';
               await sendMessageToTopic(topicId, `--- 黑名单列表 ---\n${blockListText}`); // Keep original msg format
               refreshAdminPanel = false; // Don't refresh panel for a query
           } else if (action === 'toggle_user_raw') {
               const currentState = settingsCache.get('user_raw_enabled');
               const newState = !currentState;
               await setSetting('user_raw_enabled', newState.toString());
               await sendMessageToTopic(topicId, `用户端 Raw 链接已${newState ? '开启' : '关闭'}。`); // Keep original msg
           } else if (action === 'delete_user') {
               await handleDeleteUser(chatId, topicId, targetChatId); // Reuse existing function
               refreshAdminPanel = false; // Don't refresh panel after deletion
           } else {
               console.warn(`Unhandled admin callback action: ${data}`);
               await sendMessageToTopic(topicId, `未知操作：${action}`); // Keep original msg
               refreshAdminPanel = false;
           }

           // Refresh Admin Panel if needed and context exists
           if (refreshAdminPanel && topicId && panelPrivateChatId) {
              // Delete the old panel message first
              ctx.waitUntil(fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
                   method: 'POST', headers: { 'Content-Type': 'application/json' },
                   body: JSON.stringify({ chat_id: chatId, message_id: messageId })
               }).catch(deleteError => console.warn(`Non-critical: Failed to delete old admin panel ${messageId}: ${deleteError}`)));
               // Send a fresh panel
               await sendAdminPanel(chatId, topicId, panelPrivateChatId, null); // Pass null originalMessageId
           }
        }

      } catch (error) {
        console.error(`Error processing callback query ${data} from user ${userId}:`, error);
         // Attempt to notify admin in topic about the error
         if (topicId && chatId === GROUP_ID) { // Ensure it's an admin action in group
             try {
                 await sendMessageToTopic(topicId, `处理操作 '${action}' 时出错: ${error.message}`);
             } catch (notifyError) {
                 console.error("Also failed to send error notification to topic:", notifyError);
             }
         } else if (action === 'verify' && userId) { // Notify user if verification failed
              try { await sendMessageToUser(userId, "处理验证时发生内部错误，请重试。"); } catch {}
         }
      }
    }

    /**
     * @param {string} chatId - User's private chat ID
     * @param {number | string | null} messageId - Original message ID (can be null)
     */
    async function handleVerification(chatId, messageId) {
       console.log(`Starting verification process for ${chatId}`);
       let userState = userStateCache.get(chatId);
       // Fetch state if not in cache, focusing on needed fields
       if (userState === undefined) {
         userState = await env.D1.prepare('SELECT is_blocked, is_verifying, last_verification_message_id FROM user_states WHERE chat_id = ?')
           .bind(chatId)
           .first();
         if (!userState) { // Should exist if created in onMessage, but handle defensively
              console.error(`Cannot handle verification: User state for ${chatId} not found.`);
              await sendMessageToUser(chatId, "无法开始验证：用户状态未找到。请尝试 /start。");
              return;
         }
         userStateCache.set(chatId, userState);
       }

       // Prevent starting multiple verifications simultaneously
       if (userState.is_verifying) {
           console.log(`User ${chatId} is already verifying. Aborting new request.`);
           // Optional: Check expiry of existing code? Resend if old? For now, just abort.
           return;
       }

       // --- Critical Section: Set Verifying State FIRST ---
       try {
           userState.is_verifying = true;
           userStateCache.set(chatId, userState); // Update cache optimistically
           await env.D1.prepare('UPDATE user_states SET is_verifying = TRUE, verification_code = NULL, code_expiry = NULL WHERE chat_id = ?')
               .bind(chatId)
               .run();
            console.log(`Set is_verifying=TRUE for ${chatId}`);
       } catch (dbError) {
            console.error(`DB Error setting is_verifying for ${chatId}:`, dbError);
            userState.is_verifying = false; // Revert cache on DB error
            userStateCache.set(chatId, userState);
            await sendMessageToUser(chatId, "启动验证时发生错误，请重试。");
            return;
       }
       // --- End Critical Section ---


       // Delete previous verification message if recorded
       const lastVerificationMsgId = userState.last_verification_message_id;
       if (lastVerificationMsgId) {
           console.log(`Deleting old verification message ${lastVerificationMsgId} for ${chatId}`);
           ctx.waitUntil(fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
               method: 'POST', headers: { 'Content-Type': 'application/json' },
               body: JSON.stringify({ chat_id: chatId, message_id: lastVerificationMsgId })
           }).catch(error => {
                // Log only, don't block if deletion fails (message might already be gone)
                console.warn(`Non-critical: Failed to delete old verification msg ${lastVerificationMsgId}: ${error.message}`);
           }));
           // Clear the ID from state after attempting deletion
            userState.last_verification_message_id = null;
            userStateCache.set(chatId, userState); // Update cache
            ctx.waitUntil(env.D1.prepare('UPDATE user_states SET last_verification_message_id = NULL WHERE chat_id = ?').bind(chatId).run().catch(()=>{})); // Update DB async
       }

       // Send the new challenge
       await sendVerificationChallenge(chatId); // Renamed from sendVerification
    }

    /**
     * @param {string} chatId - User's private chat ID
     */
    async function sendVerificationChallenge(chatId) {
      // Generate question (keep original logic)
      const num1 = Math.floor(Math.random() * 10);
      const num2 = Math.floor(Math.random() * 10);
      const operation = Math.random() > 0.5 ? '+' : '-';
      const correctResult = operation === '+' ? num1 + num2 : num1 - num2;

      const options = new Set([correctResult]);
      while (options.size < 4) {
        const offset = Math.floor(Math.random() * 5) - 2; // +/- 2
        const wrongResult = correctResult + offset;
        // Ensure different and non-negative (optional, can simplify)
        if (wrongResult !== correctResult && wrongResult >= 0) {
           options.add(wrongResult);
        } else if (options.size < 3) { // Try slightly larger offset if needed
             options.add(Math.max(0, correctResult + (Math.random() > 0.5 ? 3 : -3)));
        } else { // Fallback
             options.add(Math.max(0, Math.floor(Math.random() * 20)));
        }
      }
      const optionArray = Array.from(options).filter(o => o !== correctResult); // Get wrong options
       optionArray.push(correctResult); // Add correct option
       optionArray.sort(() => Math.random() - 0.5); // Shuffle


      const buttons = optionArray.map(option => ({
        text: ` ${option} `, // Padding
        callback_data: `verify_${chatId}_${option}_${option === correctResult ? 'correct' : 'wrong'}`
      }));

      const question = `请计算：${num1} ${operation} ${num2} = ?（点击下方按钮完成验证）`; // Keep original msg
      const nowSeconds = Math.floor(Date.now() / 1000);
      const codeExpiry = nowSeconds + 300; // 5 minutes expiry
      const correctCodeString = correctResult.toString();


      // --- Critical Section: Store code/expiry BEFORE sending message ---
      let userState = userStateCache.get(chatId); // Get potentially updated state
      if (!userState) { // Should exist from handleVerification, but check again
           console.error(`Cannot send challenge: User state lost for ${chatId}.`);
           // Attempt to revert is_verifying?
           ctx.waitUntil(env.D1.prepare('UPDATE user_states SET is_verifying = FALSE WHERE chat_id = ?').bind(chatId).run().catch(()=>{}));
           return;
      }

      try {
           userState.verification_code = correctCodeString;
           userState.code_expiry = codeExpiry;
           userState.last_verification_message_id = null; // Ensure cleared before getting new ID
           userStateCache.set(chatId, userState); // Update cache

           await env.D1.prepare('UPDATE user_states SET verification_code = ?, code_expiry = ?, last_verification_message_id = NULL WHERE chat_id = ?')
               .bind(correctCodeString, codeExpiry, chatId)
               .run();
            console.log(`Stored verification details for ${chatId}. Code: ${correctCodeString}, Expires: ${codeExpiry}`);
      } catch (dbError) {
            console.error(`DB Error storing verification details for ${chatId}:`, dbError);
             // Revert is_verifying if storing details fails
             userState.is_verifying = false;
             userState.verification_code = null;
             userState.code_expiry = null;
             userStateCache.set(chatId, userState);
             ctx.waitUntil(env.D1.prepare('UPDATE user_states SET is_verifying = FALSE, verification_code = NULL, code_expiry = NULL WHERE chat_id = ?').bind(chatId).run().catch(()=>{}));
             await sendMessageToUser(chatId, "存储验证信息时出错，请重试。");
             return;
      }
      // --- End Critical Section ---


      // Send the message
      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: chatId,
            text: question,
            reply_markup: { inline_keyboard: [buttons] } // Single row
          })
        });
        const data = await response.json();

        if (data.ok && data.result?.message_id) {
          const sentMessageId = data.result.message_id.toString();
          console.log(`Verification message sent to ${chatId}. Message ID: ${sentMessageId}`);
          // --- Update State with Message ID ---
          try {
               userState.last_verification_message_id = sentMessageId;
               userStateCache.set(chatId, userState); // Update cache
               await env.D1.prepare('UPDATE user_states SET last_verification_message_id = ? WHERE chat_id = ?')
                 .bind(sentMessageId, chatId)
                 .run();
          } catch (dbError) {
               console.error(`DB Error saving verification message ID ${sentMessageId} for ${chatId}:`, dbError);
               // Non-fatal, verification can still proceed, but old message might not be deleted next time
          }
          // --- End Update State ---
        } else {
          // Sending failed: Revert is_verifying state
          console.error(`Failed to send verification message to ${chatId}: ${data.description || 'Unknown API error'}`);
          userState.is_verifying = false;
          userState.verification_code = null; // Clear potentially stored code
          userState.code_expiry = null;
          userStateCache.set(chatId, userState);
          ctx.waitUntil(env.D1.prepare('UPDATE user_states SET is_verifying = FALSE, verification_code = NULL, code_expiry = NULL WHERE chat_id = ?').bind(chatId).run().catch(()=>{}));
          await sendMessageToUser(chatId, "抱歉，发送验证消息时出错，请稍后重试。");
        }
      } catch (error) {
        console.error(`Network/fetch error sending verification message to ${chatId}:`, error);
        // Revert state on error
         userState.is_verifying = false;
         userState.verification_code = null;
         userState.code_expiry = null;
         userStateCache.set(chatId, userState);
         ctx.waitUntil(env.D1.prepare('UPDATE user_states SET is_verifying = FALSE, verification_code = NULL, code_expiry = NULL WHERE chat_id = ?').bind(chatId).run().catch(()=>{}));
        try { await sendMessageToUser(chatId, "发送验证时发生网络错误，请重试。"); } catch {}
      }
    }

    /** @param {string} userId */
    async function checkIfAdmin(userId) {
        if (!userId) return false;
        // Simple Admin Cache (Optional, can be basic Map)
        // const adminCache = new Map(); // Define globally or pass if needed
        // if (adminCache.has(userId)) return adminCache.get(userId);

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

            if (data.ok) {
                const status = data.result.status;
                const isAdmin = status === 'administrator' || status === 'creator';
                // adminCache.set(userId, isAdmin); // Update cache
                return isAdmin;
            } else {
                // Log non-OK responses, but treat as non-admin
                // console.warn(`Check admin failed for ${userId}: ${data.description}`);
                // adminCache.set(userId, false); // Cache negative result
                return false;
            }
        } catch (error) {
            console.error(`Exception checking admin status for user ${userId}:`, error);
            // adminCache.set(userId, false); // Cache negative result on error
            return false; // Assume not admin on error
        }
    }

    /** @param {string} chatId */
    async function getUserInfo(chatId) {
      let userInfo = userInfoCache.get(chatId);
      if (userInfo === undefined) {
        // console.log(`User info cache miss for ${chatId}. Fetching...`);
        try {
          const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getChat`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ chat_id: chatId })
          });
          const data = await response.json();

          if (data.ok && data.result) {
            const result = data.result;
            // Keep original nickname logic
            const nickname = result.first_name
              ? `${result.first_name}${result.last_name ? ` ${result.last_name}` : ''}`.trim()
              : result.username || `User_${chatId}`; // Fallback chain

            userInfo = {
              id: result.id?.toString() || chatId, // Ensure string ID
              username: result.username || `User_${chatId}`, // Default username
              nickname: nickname
            };
            // console.log(`Fetched user info for ${chatId}:`, userInfo);
          } else {
             console.warn(`Failed to get user info for ${chatId}: ${data.description || 'API Error'}. Using defaults.`);
             userInfo = { id: chatId, username: `User_${chatId}`, nickname: `User_${chatId}` };
          }
          userInfoCache.set(chatId, userInfo); // Cache success or default

        } catch (error) {
          console.error(`Exception fetching user info for chatId ${chatId}:`, error);
          // Use default on exception
          userInfo = { id: chatId, username: `User_${chatId}`, nickname: `User_${chatId}` };
          userInfoCache.set(chatId, userInfo); // Cache default on error
        }
      }
      return userInfo;
    }

    /**
     * @param {string} chatId
     * @returns {Promise<string | null>} Topic ID string or null
      */
    async function getExistingTopicId(chatId) {
      let topicId = topicIdCache.get(chatId);
      if (topicId === undefined) {
        // console.log(`Topic ID cache miss for ${chatId}. Querying DB...`);
        try {
            // Select only topic_id for efficiency
             const mapping = await env.D1.prepare('SELECT topic_id FROM chat_topic_mappings WHERE chat_id = ?')
              .bind(chatId)
              .first();

             topicId = mapping?.topic_id || null; // Use null if not found

             if (topicId) {
                 // console.log(`Found topic ID ${topicId} for chat ${chatId} in DB. Caching.`);
                 topicIdCache.set(chatId, topicId); // Add to cache if found in DB
             }
             // else { console.log(`No topic ID found for chat ${chatId} in DB.`); }

        } catch (dbError) {
            console.error(`DB Error fetching topic ID for ${chatId}:`, dbError);
            topicId = null; // Return null on DB error
        }
      }
      // else { console.log(`Topic ID cache hit for ${chatId}: ${topicId}`); }
      return topicId; // Returns string ID or null
    }

    /**
     * @param {string} topicName
     * @param {string} userName
     * @param {string} nickname
     * @param {string} userId
     * @returns {Promise<number>} Topic Thread ID (numeric)
      */
    async function createForumTopic(topicName, userName, nickname, userId) {
       console.log(`Creating topic for User ${userId} (Nick: ${nickname}, User: ${userName}). Suggested Name: ${topicName}`);
      try {
        // 1. Create Topic
        const createResponse = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/createForumTopic`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          // Ensure name length is within limits
          body: JSON.stringify({ chat_id: GROUP_ID, name: topicName.substring(0, 128) })
        });
        const createData = await createResponse.json();
        if (!createData.ok || !createData.result?.message_thread_id) {
           console.error(`Failed to create topic: ${createData.description || 'API Error'}`);
           throw new Error(`Failed to create forum topic: ${createData.description || 'Unknown error'}`);
        }
        const topicId = createData.result.message_thread_id; // This is numeric
        const topicIdStr = topicId.toString();
        console.log(`Topic ${topicIdStr} created for user ${userId}. Sending info message...`);

        // 2. Send Info Message (Keep original format)
        const now = new Date();
        const formattedTime = now.toISOString().replace('T', ' ').substring(0, 19); // YYYY-MM-DD HH:MM:SS
        const notificationContent = await getNotificationContent(); // Fetch dynamic part
        // Use simple formatting, avoid Markdown issues unless necessary
        const pinnedMessageText = `昵称: ${nickname}\n` +
                                  `用户名: @${userName}\n` + // Assumes userName exists or is default
                                  `UserID: ${userId}\n` +
                                  `发起时间: ${formattedTime}\n\n` +
                                  `${notificationContent}`;

        const messageResponse = await sendMessageToTopic(topicIdStr, pinnedMessageText); // Send plain text

        // 3. Pin Message (Best effort)
        if (messageResponse.ok && messageResponse.result?.message_id) {
             const messageIdToPin = messageResponse.result.message_id;
             console.log(`Attempting to pin message ${messageIdToPin} in group ${GROUP_ID} (associated with topic ${topicIdStr})`);
             // Pinning within topic is not directly supported, this pins globally in group
             ctx.waitUntil(pinMessage(topicIdStr, messageIdToPin) // Use topicIdStr for context if needed
                 .catch(pinError => console.warn(`Non-critical: Failed to pin message ${messageIdToPin}: ${pinError}`))
             );
        } else {
             console.warn(`Could not get message ID for pinning in topic ${topicIdStr}. Response:`, messageResponse);
        }

        return topicId; // Return numeric ID

      } catch (error) {
        console.error(`Exception in createForumTopic for user ${userId}:`, error);
        // Attempt to notify admin in general chat on failure
         ctx.waitUntil(sendMessageToTopic(null, `CRITICAL: Failed to create topic for user ${userId} (@${userName}). Error: ${error.message}`).catch(()=>{}));
        throw error; // Re-throw so caller knows it failed
      }
    }

    /**
     * @param {string} chatId
     * @param {string} topicId - Topic ID as string
     */
    async function saveTopicId(chatId, topicId) {
      // Update cache first
      topicIdCache.set(chatId, topicId);
      try {
        // Use INSERT OR REPLACE to handle updates if topic is recreated
        await env.D1.prepare('INSERT OR REPLACE INTO chat_topic_mappings (chat_id, topic_id) VALUES (?, ?)')
          .bind(chatId, topicId)
          .run();
        // console.log(`Saved/Updated topic mapping: Chat ${chatId} -> Topic ${topicId}`);
      } catch (error) {
        console.error(`DB Error saving topic ID ${topicId} for chatId ${chatId}:`, error);
        // Invalidate cache if DB save fails to ensure consistency?
        topicIdCache.set(chatId, undefined); // Remove potentially incorrect cache entry
        throw error; // Re-throw
      }
    }

    /**
     * @param {string} topicId - Topic ID as string
     * @returns {Promise<string | null>} Private chat ID string or null
      */
    async function getPrivateChatId(topicId) {
        // Check cache first (less efficient reverse lookup)
        for (const [chatId, cachedTopicId] of topicIdCache.cache.entries()) {
            // Ensure strict comparison (both should be strings or consistently typed)
            if (cachedTopicId?.toString() === topicId?.toString()) {
                // console.log(`Reverse cache hit: Topic ${topicId} -> Chat ${chatId}`);
                return chatId;
            }
        }
       // console.log(`Reverse cache miss for topic ${topicId}. Querying DB...`);

        try {
            // Query DB - Index on topic_id recommended for performance
            const mapping = await env.D1.prepare('SELECT chat_id FROM chat_topic_mappings WHERE topic_id = ?')
            .bind(topicId) // Bind the string topicId
            .first();

            const privateChatId = mapping?.chat_id || null; // string or null

            if (privateChatId) {
                // console.log(`Found private chat ID ${privateChatId} for topic ${topicId} in DB.`);
                // Populate forward cache as well
                topicIdCache.set(privateChatId, topicId);
            }
            // else { console.log(`No private chat ID found for topic ${topicId} in DB.`); }
            return privateChatId;

        } catch (error) {
            console.error(`DB error fetching private chat ID for topicId ${topicId}:`, error);
            return null; // Return null on error
        }
    }

    /**
     * @param {string | null} topicId - Topic ID string, or null for general chat
     * @param {string} text - Message text
     * @param {'MarkdownV2' | 'HTML' | null} [parseMode=null]
     * @returns {Promise<any>} Telegram API response
      */
    async function sendMessageToTopic(topicId, text, parseMode = null) {
      if (!text || text.trim() === '') {
        console.warn("Attempted to send empty message to topic:", topicId);
        return { ok: false, description: 'Message text is empty' };
      }

      const MAX_LENGTH = 4096;
       if (text.length > MAX_LENGTH) {
           console.warn(`Truncating message for topic ${topicId}. Length: ${text.length}`);
           text = text.substring(0, MAX_LENGTH - 10) + "...(截断)";
       }

      try {
        const requestBody = {
          chat_id: GROUP_ID,
          text: text,
          // Only add thread ID if topicId is provided and not null
          ...(topicId && { message_thread_id: topicId.toString() }), // Ensure string
          ...(parseMode && { parse_mode: parseMode }),
          // disable_web_page_preview: true // Optional
        };

        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(requestBody)
        });

        const data = await response.json();
        if (!data.ok) {
           // Log specific errors
           console.error(`TG API Error sending to topic ${topicId || 'General'}: ${data.description} (Code: ${data.error_code})`);
           // Throw error to be handled by caller (e.g., recreate topic logic)
           throw new Error(`Telegram API Error: ${data.description} (Code: ${data.error_code})`);
        }
        return data; // Return success response

      } catch (error) {
        console.error(`Exception sending message to topic ${topicId || 'General'}:`, error);
        // Re-throw the error caught or created
        throw error;
      }
    }

    /**
     * @param {string} topicId - Topic ID string
     * @param {import('@cloudflare/workers-types').TelegramMessage} message - Original message
     * @returns {Promise<void>}
      */
    async function copyMessageToTopic(topicId, message) {
      try {
        const requestBody = {
          chat_id: GROUP_ID,
          from_chat_id: message.chat.id,
          message_id: message.message_id,
          message_thread_id: topicId.toString(), // Ensure string
          // disable_notification: true // Optional
        };
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/copyMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(requestBody)
        });
        const data = await response.json();
        if (!data.ok) {
           console.error(`TG API Error copying message ${message.message_id} to topic ${topicId}: ${data.description} (Code: ${data.error_code})`);
           // Throw error to be handled by caller (e.g., recreate topic logic)
           throw new Error(`Telegram API Error copying message: ${data.description} (Code: ${data.error_code})`);
        }
         // console.log(`Copied message ${message.message_id} to topic ${topicId}`);
      } catch (error) {
        console.error(`Exception copying message ${message.message_id} to topic ${topicId}:`, error);
        throw error; // Re-throw
      }
    }

    /**
     * @param {string} topicId - Topic ID (string) for context, though pinning is global
     * @param {number | string} messageId - Message ID to pin
     * @returns {Promise<void>}
      */
    async function pinMessage(topicId, messageId) {
      try {
        const requestBody = {
          chat_id: GROUP_ID,
          message_id: messageId,
          disable_notification: true
          // No message_thread_id for pinChatMessage
        };
        // console.log(`Attempting global pin for message ${messageId} (assoc. topic ${topicId})`);
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/pinChatMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(requestBody)
        });
        const data = await response.json();
        if (!data.ok) {
           // Log failure but don't throw - pinning is non-essential usually
           console.warn(`Failed to pin message ${messageId}: ${data.description} (Code: ${data.error_code}). Check bot 'Pin Messages' permission.`);
           // throw new Error(`Failed to pin message: ${data.description}`);
        }
        // else { console.log(`Pin request successful for message ${messageId}.`); }
      } catch (error) {
        // Log exception but don't throw
        console.warn(`Exception pinning message ${messageId}:`, error);
        // throw error;
      }
    }

    /**
     * @param {string} privateChatId - Target user's private chat ID
     * @param {import('@cloudflare/workers-types').TelegramMessage} message - Message from group topic
     * @returns {Promise<void>}
      */
    async function forwardMessageToPrivateChat(privateChatId, message) {
      try {
        // Use copyMessage
        const requestBody = {
          chat_id: privateChatId,
          from_chat_id: message.chat.id, // GROUP_ID
          message_id: message.message_id,
          // disable_notification: false // Notify user by default
        };
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/copyMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(requestBody)
        });
        const data = await response.json();
        if (!data.ok) {
           console.error(`TG API Error forwarding message ${message.message_id} to user ${privateChatId}: ${data.description} (Code: ${data.error_code})`);
            // Handle specific errors like bot blocked
            if (data.error_code === 403 && data.description.includes("blocked")) {
                console.warn(`Bot blocked by user ${privateChatId}. Cannot forward message.`);
                // Optional: Automatically block user in DB?
                 // ctx.waitUntil(env.D1.prepare('UPDATE user_states SET is_blocked = TRUE WHERE chat_id = ?').bind(privateChatId).run().catch(()=>{}));
            }
            // Throw error? Or just log? Logging for now.
            // throw new Error(`Failed to forward message to private chat: ${data.description}`);
        }
         // else { console.log(`Forwarded message ${message.message_id} to user ${privateChatId}`); }
      } catch (error) {
        console.error(`Exception forwarding message to private chat ${privateChatId}:`, error);
        // throw error;
      }
    }

    /**
     * @param {string} chatId - User's private chat ID
     * @param {string} text - Message text
     * @returns {Promise<void>}
      */
    async function sendMessageToUser(chatId, text) {
       if (!text || text.trim() === '') return; // Avoid empty

       const MAX_LENGTH = 4096;
       if (text.length > MAX_LENGTH) {
           text = text.substring(0, MAX_LENGTH - 10) + "...(截断)";
       }

      try {
        const requestBody = { chat_id: chatId, text: text };
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(requestBody)
        });
        const data = await response.json();
        if (!data.ok) {
           console.warn(`Failed to send message to user ${chatId}: ${data.description} (Code: ${data.error_code})`);
           // Handle blocked etc. silently maybe
           // throw new Error(`Failed to send message to user: ${data.description}`);
        }
      } catch (error) {
        console.error(`Exception sending message to user ${chatId}:`, error);
        // throw error;
      }
    }

    /**
     * @param {string} url
     * @param {RequestInit} options
     * @param {number} [retries=3]
     * @param {number} [backoff=500]
     * @returns {Promise<Response>}
      */
     async function fetchWithRetry(url, options = {}, retries = 3, backoff = 500) {
         let lastError = null;
         for (let i = 0; i < retries; i++) {
              let timeoutId;
              try {
                  const controller = new AbortController();
                  timeoutId = setTimeout(() => controller.abort(), 10000); // 10s timeout

                  const response = await fetch(url, { ...options, signal: controller.signal });
                  clearTimeout(timeoutId); // Clear timeout on success

                  // Retry on 429 or 5xx
                  if (response.status === 429) {
                      const retryAfter = parseInt(response.headers.get('Retry-After') || '5', 10);
                      const delay = Math.max(retryAfter * 1000, backoff);
                      console.warn(`Rate limited (429) on attempt ${i + 1}/${retries} for ${url}. Retrying after ${delay}ms.`);
                      await new Promise(resolve => setTimeout(resolve, delay));
                      lastError = new Error(`Rate limited (429)`); // Store error info
                      continue; // Next attempt
                  }
                  if (response.status >= 500 && response.status < 600) {
                       console.warn(`Server error (${response.status}) on attempt ${i + 1}/${retries} for ${url}. Retrying after ${backoff}ms.`);
                       await new Promise(resolve => setTimeout(resolve, backoff));
                       lastError = new Error(`Server error (${response.status})`);
                       backoff *= 2; // Exponential backoff
                       continue; // Next attempt
                  }

                  // Not a retryable error, return response (caller handles non-ok logic)
                  return response;

              } catch (error) {
                  clearTimeout(timeoutId); // Clear timeout on error too
                   lastError = error; // Store the error
                   if (error.name === 'AbortError') {
                       console.error(`Fetch aborted (timeout) for ${url} on attempt ${i + 1}/${retries}.`);
                   } else {
                       console.error(`Fetch error for ${url} on attempt ${i + 1}/${retries}:`, error.message);
                   }

                  if (i === retries - 1) {
                       console.error(`Fetch failed permanently after ${retries} attempts for ${url}. Last error: ${lastError.message}`);
                       throw lastError; // Throw the last encountered error
                  }

                  // Wait before retry for network errors/timeouts
                  await new Promise(resolve => setTimeout(resolve, backoff));
                  backoff *= 2;
              }
          }
          // Should not be reached if retries >= 1
           throw lastError || new Error(`Fetch failed after ${retries} retries (reached end unexpectedly)`);
      }


    /** @param {CfRequest} request */
    async function registerWebhook(request) {
       if (!BOT_TOKEN) return new Response("BOT_TOKEN not configured.", { status: 500 });
       const webhookUrl = `${new URL(request.url).origin}/webhook`;
       console.log(`Setting webhook to: ${webhookUrl}`);
       try {
            const response = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/setWebhook`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ url: webhookUrl, allowed_updates: ["message", "callback_query"] })
            });
            const data = await response.json();
            if (!response.ok || !data.ok) {
                return new Response(`Webhook setup failed: ${JSON.stringify(data)}`, { status: response.status });
            }
            return new Response('Webhook set successfully');
       } catch (error) {
            return new Response(`Error setting webhook: ${error.message}`, { status: 500 });
       }
    }

    async function unRegisterWebhook() {
       if (!BOT_TOKEN) return new Response("BOT_TOKEN not configured.", { status: 500 });
       console.log("Removing webhook...");
        try {
            const response = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/setWebhook`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ url: '' }) // Empty URL removes webhook
            });
             const data = await response.json();
             if (!response.ok || !data.ok) {
                 return new Response(`Webhook removal failed: ${JSON.stringify(data)}`, { status: response.status });
             }
            return new Response('Webhook removed');
        } catch (error) {
             return new Response(`Error removing webhook: ${error.message}`, { status: 500 });
        }
    }

    // --- Main Request Entry Point ---
    try {
      return await handleRequest(request);
    } catch (error) {
      console.error('FATAL: Unhandled error in fetch handler:', error, error.stack);
      // Minimal error reporting to admin if possible
       if (BOT_TOKEN && GROUP_ID) {
           ctx.waitUntil(sendMessageToTopic(null, `🚨 WORKER ERROR 🚨\nUnhandled Exception: ${error.message}\nCheck logs!`).catch(()=>{}));
       }
      return new Response('Internal Server Error', { status: 500 });
    }
  }
};


/**
 * Helper to answer callback queries, used by onCallbackQuery
 * @param {string} callbackQueryId
 * @param {{ text?: string; show_alert?: boolean }} [options={}]
 */
async function answerCallbackQuery(callbackQueryId, options = {}) {
    if (!BOT_TOKEN) return; // Need token
    try {
        // Use fetchWithRetry for robustness? Or keep simple? Keep simple for now.
        const response = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/answerCallbackQuery`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                callback_query_id: callbackQueryId,
                ...options
            })
        });
        if (!response.ok) {
             const data = await response.json().catch(() => ({})); // Try to get error details
             console.warn(`Non-critical: Failed to answer callback ${callbackQueryId}: ${data.description || response.status}`);
        }
    } catch (error) {
        console.warn(`Non-critical: Exception answering callback ${callbackQueryId}:`, error);
    }
}
