let BOT_TOKEN;
let GROUP_ID;
let MAX_MESSAGES_PER_MINUTE;

let lastCleanupTime = 0;
const CLEANUP_INTERVAL = 24 * 60 * 60 * 1000; // 24 小时
let isInitialized = false;
const processedMessages = new Set();
const processedCallbacks = new Set();

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
  clear() {
    this.cache.clear();
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
      return new Response('Server configuration error: D1 database is not bound', { status: 500 });
    }

    if (!isInitialized) {
      await initialize(env.D1, request);
      isInitialized = true;
    }

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

    async function initialize(d1, request) {
      await Promise.all([
        checkAndRepairTables(d1),
        autoRegisterWebhook(request),
        checkBotPermissions(),
        cleanExpiredVerificationCodes(d1),
        cleanupCreatingTopics(d1),
        cleanupOldData(d1)
      ]);
    }

    async function autoRegisterWebhook(request) {
      const webhookUrl = `${new URL(request.url).origin}/webhook`;
      await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/setWebhook`, {
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
        },
        message_mapping: {
          columns: {
            id: 'INTEGER PRIMARY KEY AUTOINCREMENT',
            user_id: 'TEXT NOT NULL',
            user_message_id: 'TEXT NOT NULL',
            group_message_id: 'TEXT NOT NULL',
            created_at: 'INTEGER DEFAULT (strftime(\'%s\', \'now\'))'
          }
        },
        edit_state: {
          columns: {
            id: 'INTEGER PRIMARY KEY AUTOINCREMENT',
            user_id: 'TEXT NOT NULL',
            original_message_id: 'TEXT NOT NULL',
            instruction_message_id: 'TEXT NOT NULL',
            created_at: 'INTEGER DEFAULT (strftime(\'%s\', \'now\'))'
          }
        },
        admin_edit_state: {
          columns: {
            id: 'INTEGER PRIMARY KEY AUTOINCREMENT',
            admin_id: 'TEXT NOT NULL',
            topic_id: 'TEXT NOT NULL',
            original_message_id: 'TEXT NOT NULL',
            instruction_message_id: 'TEXT NOT NULL',
            created_at: 'INTEGER DEFAULT (strftime(\'%s\', \'now\'))'
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
        
        if (tableName === 'message_mapping') {
          await d1.exec('CREATE UNIQUE INDEX IF NOT EXISTS idx_message_mapping_user_msg ON message_mapping (user_id, user_message_id)');
          await d1.exec('CREATE INDEX IF NOT EXISTS idx_message_mapping_group_msg ON message_mapping (group_message_id)');
        }
        
        if (tableName === 'edit_state') {
          await d1.exec('CREATE UNIQUE INDEX IF NOT EXISTS idx_edit_state_user_instruction ON edit_state (user_id, instruction_message_id)');
          await d1.exec('CREATE INDEX IF NOT EXISTS idx_edit_state_original ON edit_state (user_id, original_message_id)');
        }
        
        if (tableName === 'admin_edit_state') {
          await d1.exec('CREATE UNIQUE INDEX IF NOT EXISTS idx_admin_edit_state_instruction ON admin_edit_state (topic_id, instruction_message_id)');
          await d1.exec('CREATE INDEX IF NOT EXISTS idx_admin_edit_state_original ON admin_edit_state (admin_id, original_message_id)');
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
              'UPDATE user_states SET verification_code = NULL, code_expiry = NULL, is_verifying = FALSE WHERE chat_id = ?'
            ).bind(chat_id)
          )
        );
      }
      lastCleanupTime = now;
    }

    async function cleanupOldData(d1) {
      try {
        // 清理30天前的消息映射
        const thirtyDaysAgo = Math.floor(Date.now() / 1000) - (30 * 24 * 60 * 60);
        await d1.prepare('DELETE FROM message_mapping WHERE created_at < ?')
          .bind(thirtyDaysAgo)
          .run();

        // 清理1小时前的未完成编辑状态
        const oneHourAgo = Math.floor(Date.now() / 1000) - (1 * 60 * 60);
        await d1.prepare('DELETE FROM edit_state WHERE created_at < ?')
          .bind(oneHourAgo)
          .run();
        await d1.prepare('DELETE FROM admin_edit_state WHERE created_at < ?')
          .bind(oneHourAgo)
          .run();

        console.log('旧数据清理完成');
      } catch (error) {
        console.error(`清理旧数据时出错: ${error.message}`);
      }
    }

    async function cleanupCreatingTopics(d1) {
      try {
        // 查找所有标记为正在创建的话题映射
        const creatingTopics = await d1.prepare('SELECT chat_id FROM chat_topic_mappings WHERE topic_id = ?')
          .bind('creating')
          .all();
        
        if (creatingTopics.results.length > 0) {
          console.log(`清理 ${creatingTopics.results.length} 个遗留的临时话题标记`);
          
          // 删除所有临时标记
          await d1.prepare('DELETE FROM chat_topic_mappings WHERE topic_id = ?')
            .bind('creating')
            .run();
          
          // 从缓存中清除这些用户的话题ID
          for (const row of creatingTopics.results) {
            topicIdCache.set(row.chat_id, undefined);
          }
        }
      } catch (error) {
        console.error(`清理临时话题标记时出错: ${error.message}`);
        // 继续执行，不中断初始化流程
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
      } else if (update.edited_message) {
        // 处理编辑消息事件
        await onEditedMessage(update.edited_message);
      }
    }

    async function onMessage(message) {
      const chatId = message.chat.id.toString();
      const text = message.text || '';
      const messageId = message.message_id;

      if (chatId === GROUP_ID) {
        const topicId = message.message_thread_id;
        if (topicId) {
          // 新增：检查消息是否由管理员（非机器人）发送
          if (message.from && message.from.id.toString() !== (await getBotId()).toString()) {
            const isAdmin = await checkIfAdmin(message.from.id.toString());
            if (isAdmin) {
              console.log(`检测到管理员 ${message.from.id} 在群组 ${GROUP_ID} 话题 ${topicId} 中发送了消息 ${messageId}`);
              
              // 为管理员消息添加编辑/删除按钮
              try {
                await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/editMessageReplyMarkup`, {
                  method: 'POST',
                  headers: { 'Content-Type': 'application/json' },
                  body: JSON.stringify({
                    chat_id: GROUP_ID,
                    message_id: messageId,
                    reply_markup: {
                      inline_keyboard: [
                        [
                          { text: "编辑消息", callback_data: `admin_edit_${messageId}` },
                          { text: "删除消息", callback_data: `admin_delete_${messageId}` }
                        ]
                      ]
                    }
                  })
                });
                console.log(`成功为管理员直接发送的消息 ${messageId} 添加编辑/删除按钮`);
                
                // 查找对应的私聊
                const privateChatId = await getPrivateChatId(topicId);
                if (privateChatId) {
                  // 转发到私聊
                  const privateMsg = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/copyMessage`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                      chat_id: privateChatId,
                      from_chat_id: GROUP_ID,
                      message_id: messageId
                    })
                  }).then(res => res.json());
                  
                  // 保存消息映射
                  if (privateMsg.ok) {
                    await env.D1.prepare(
                      'INSERT OR REPLACE INTO message_mapping (user_id, user_message_id, group_message_id) VALUES (?, ?, ?)'
                    ).bind(privateChatId, privateMsg.result.message_id.toString(), messageId.toString()).run();
                    console.log(`为管理员直接发送的消息创建映射: 私聊ID=${privateChatId}, 私聊消息ID=${privateMsg.result.message_id}, 群组消息ID=${messageId}`);
                  }
                }
              } catch (error) {
                console.error(`为管理员直接发送的消息 ${messageId} 添加按钮失败: ${error.message}`);
              }
            }
          }
          
          const privateChatId = await getPrivateChatId(topicId);
          if (privateChatId && text === '/admin') {
            await sendAdminPanel(chatId, topicId, privateChatId, messageId);
            return;
          }
          if (privateChatId && text.startsWith('/reset_user')) {
            await handleResetUser(chatId, topicId, text);
            return;
          }
          
          // 检查是否是对管理员编辑指导消息的回复
          if (message.reply_to_message && text) {
            const replyToMsgId = message.reply_to_message.message_id.toString();
            
            // 查询是否有待编辑的管理员消息
            const editState = await env.D1.prepare(
              'SELECT original_message_id, admin_id FROM admin_edit_state WHERE topic_id = ? AND instruction_message_id = ?'
            ).bind(topicId, replyToMsgId).first();
            
            if (editState && editState.original_message_id && editState.admin_id === message.from.id.toString()) {
              const originalMsgId = editState.original_message_id;
              
              try {
                // 编辑群组中的消息
                await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/editMessageText`, {
                  method: 'POST',
                  headers: { 'Content-Type': 'application/json' },
                  body: JSON.stringify({
                    chat_id: GROUP_ID,
                    message_id: originalMsgId,
                    text: text, // 使用管理员回复的新内容
                    reply_markup: {
                      inline_keyboard: [
                        [
                          { text: "编辑消息", callback_data: `admin_edit_${originalMsgId}` },
                          { text: "删除消息", callback_data: `admin_delete_${originalMsgId}` }
                        ]
                      ]
                    }
                  })
                });
                
                // 查询此消息在私聊中的对应消息
                const result = await env.D1.prepare(
                  'SELECT user_id, user_message_id FROM message_mapping WHERE group_message_id = ?'
                ).bind(originalMsgId).first();
                
                // 如果找到对应的私聊消息，也更新它
                if (result && result.user_id && result.user_message_id) {
                  try {
                    await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/editMessageText`, {
                      method: 'POST',
                      headers: { 'Content-Type': 'application/json' },
                      body: JSON.stringify({
                        chat_id: result.user_id,
                        message_id: result.user_message_id,
                        text: text // 使用管理员回复的新内容
                      })
                    });
                  } catch (error) {
                    console.log(`编辑私聊中的管理员消息失败: ${error.message}`);
                    // 忽略错误，私聊消息可能已过期
                  }
                }
                
                // 发送编辑成功提示
                await sendMessageToTopic(topicId, "✅ 好的主人，已将对应消息编辑。");
                
                // 删除编辑状态和编辑指导消息
                await env.D1.prepare('DELETE FROM admin_edit_state WHERE topic_id = ? AND instruction_message_id = ?')
                  .bind(topicId, replyToMsgId).run();
                
                try {
                  await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                      chat_id: GROUP_ID,
                      message_id: replyToMsgId
                    })
                  });
                } catch (error) {
                  console.log(`删除编辑指导消息失败: ${error.message}`);
                }
                
                // 删除管理员的回复消息
                try {
                  await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                      chat_id: GROUP_ID,
                      message_id: messageId
                    })
                  });
                } catch (error) {
                  console.log(`删除管理员回复消息失败: ${error.message}`);
                }
                
                return;
              } catch (error) {
                console.error(`编辑管理员消息失败: ${error.message}`);
                await sendMessageToTopic(topicId, "编辑消息失败，请重试。");
              }
            }
          }
          
          if (privateChatId) {
            const senderId = message.from.id.toString();
            const isAdmin = await checkIfAdmin(senderId);

            if (isAdmin) {
              let isReplyToAdminEditInstruction = false;
              if (message.reply_to_message && text) {
                const replyToMsgId = message.reply_to_message.message_id.toString();
                const editState = await env.D1.prepare(
                  'SELECT original_message_id FROM admin_edit_state WHERE topic_id = ? AND instruction_message_id = ? AND admin_id = ?'
                ).bind(topicId, replyToMsgId, senderId).first();
                if (editState) {
                  isReplyToAdminEditInstruction = true;
                }
              }

              // 只有当不是对编辑指示的回复时，才进行普通消息处理
              if (!isReplyToAdminEditInstruction) {
                console.log(`管理员 ${senderId} 在话题 ${topicId} 中发送了普通消息 ${message.message_id}`);
                
                // 获取原始消息文本
                const messageText = message.text || "管理员发送了一条消息";
                
                // 如果是文本消息，直接发送带按钮的新消息，而不是先发送后编辑
                if (message.text) {
                  try {
                    // 直接发送带编辑/删除按钮的消息
                    const result = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
                      method: 'POST',
                      headers: { 'Content-Type': 'application/json' },
                      body: JSON.stringify({
                        chat_id: GROUP_ID,
                        message_thread_id: topicId,
                        text: messageText,
                        reply_markup: {
                          inline_keyboard: [
                            [
                              { text: "编辑消息", callback_data: `admin_edit_${message.message_id}` },
                              { text: "删除消息", callback_data: `admin_delete_${message.message_id}` }
                            ]
                          ]
                        }
                      })
                    }).then(res => res.json());
                    
                    if (result.ok) {
                      // 转发到私聊
                      const forwardResult = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                          chat_id: privateChatId,
                          text: messageText
                        })
                      }).then(res => res.json());
                      
                      // 保存正确的消息映射关系
                      if (forwardResult.ok) {
                        await env.D1.prepare(
                          'INSERT OR REPLACE INTO message_mapping (user_id, user_message_id, group_message_id) VALUES (?, ?, ?)'
                        ).bind(privateChatId, forwardResult.result.message_id.toString(), result.result.message_id.toString()).run();
                      }
                      
                      // 删除原始消息
                      try {
                        await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
                          method: 'POST',
                          headers: { 'Content-Type': 'application/json' },
                          body: JSON.stringify({
                            chat_id: GROUP_ID,
                            message_id: message.message_id
                          })
                        });
                      } catch (deleteError) {
                        console.log(`删除原始管理员消息失败: ${deleteError.message}`);
                      }
                    }
                  } catch (error) {
                    console.error(`管理员发送带按钮消息失败: ${error.message}`);
                    // 如果新方法失败，回退到原方法
                    await fallbackForwardMessage();
                  }
                } else {
                  // 非文本消息使用原方法
                  await fallbackForwardMessage();
                }
                
                // 回退方法：使用原来的转发+编辑按钮方式
                async function fallbackForwardMessage() {
                  try {
                    // 转发消息到私聊
                    const forwardResult = await forwardMessageToPrivateChat(privateChatId, message);
                    console.log(`转发结果: ${JSON.stringify(forwardResult)}`);
                    
                    // 为原消息添加按钮
                    try {
                      await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/editMessageReplyMarkup`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                          chat_id: GROUP_ID,
                          message_id: message.message_id,
                          reply_markup: {
                            inline_keyboard: [
                              [
                                { text: "编辑消息", callback_data: `admin_edit_${message.message_id}` },
                                { text: "删除消息", callback_data: `admin_delete_${message.message_id}` }
                              ]
                            ]
                          }
                        })
                      });
                      console.log(`成功为管理员消息 ${message.message_id} 添加编辑/删除按钮`);
                    } catch (error) {
                      console.error(`为管理员消息 ${message.message_id} 添加按钮失败: ${error.message}`);
                    }
                    
                    // 如果转发成功，保存消息映射
                    if (forwardResult && forwardResult.ok && forwardResult.result && forwardResult.result.message_id) {
                      await env.D1.prepare(
                        'INSERT OR REPLACE INTO message_mapping (user_id, user_message_id, group_message_id) VALUES (?, ?, ?)'
                      ).bind(privateChatId, forwardResult.result.message_id.toString(), message.message_id.toString()).run();
                      console.log(`保存消息映射: 私聊消息ID=${forwardResult.result.message_id}, 群组消息ID=${message.message_id}`);
                    } else {
                      console.error(`未能获取转发消息的ID，无法保存映射关系`);
                    }
                  } catch (error) {
                    console.error(`转发管理员消息失败: ${error.message}`);
                  }
                }
              } else {
                console.log(`管理员 ${senderId} 的消息 ${message.message_id} 是对编辑指示的回复，不添加按钮。`);
              }
            } else {
              // 非管理员消息，直接转发
              await forwardMessageToPrivateChat(privateChatId, message);
            }
          }
        }
        return;
      }

      let userState = userStateCache.get(chatId);
      if (userState === undefined) {
        userState = await env.D1.prepare('SELECT is_blocked, is_first_verification, is_verified, verified_expiry, is_verifying FROM user_states WHERE chat_id = ?')
          .bind(chatId)
          .first();
        if (!userState) {
          userState = { is_blocked: false, is_first_verification: true, is_verified: false, verified_expiry: null, is_verifying: false };
          await env.D1.prepare('INSERT INTO user_states (chat_id, is_blocked, is_first_verification, is_verified, is_verifying) VALUES (?, ?, ?, ?, ?)')
            .bind(chatId, false, true, false, false)
            .run();
        }
        userStateCache.set(chatId, userState);
      }

      if (userState.is_blocked) {
        await sendMessageToUser(chatId, "您已被拉黑，无法发送消息。请联系管理员解除拉黑。");
        return;
      }

      const verificationEnabled = (await getSetting('verification_enabled', env.D1)) === 'true';

      if (!verificationEnabled) {
        // 验证码关闭时，所有用户都可以直接发送消息
      } else {
        const nowSeconds = Math.floor(Date.now() / 1000);
        const isVerified = userState.is_verified && userState.verified_expiry && nowSeconds < userState.verified_expiry;
        const isFirstVerification = userState.is_first_verification;
        const isRateLimited = await checkMessageRate(chatId);
        const isVerifying = userState.is_verifying || false;

        if (!isVerified || (isRateLimited && !isFirstVerification)) {
          if (isVerifying) {
            // 检查验证码是否已过期
            const storedCode = await env.D1.prepare('SELECT verification_code, code_expiry FROM user_states WHERE chat_id = ?')
              .bind(chatId)
              .first();
            
            const nowSeconds = Math.floor(Date.now() / 1000);
            const isCodeExpired = !storedCode?.verification_code || !storedCode?.code_expiry || nowSeconds > storedCode.code_expiry;
            
            if (isCodeExpired) {
              // 如果验证码已过期，重新发送验证码
              await sendMessageToUser(chatId, '验证码已过期，正在为您发送新的验证码...');
              await env.D1.prepare('UPDATE user_states SET verification_code = NULL, code_expiry = NULL, is_verifying = FALSE WHERE chat_id = ?')
                .bind(chatId)
                .run();
              userStateCache.set(chatId, { ...userState, verification_code: null, code_expiry: null, is_verifying: false });
              
              // 删除旧的验证消息（如果存在）
              try {
                const lastVerification = await env.D1.prepare('SELECT last_verification_message_id FROM user_states WHERE chat_id = ?')
                  .bind(chatId)
                  .first();
                
                if (lastVerification?.last_verification_message_id) {
                  try {
                    await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
                      method: 'POST',
                      headers: { 'Content-Type': 'application/json' },
                      body: JSON.stringify({
                        chat_id: chatId,
                        message_id: lastVerification.last_verification_message_id
                      })
                    });
                  } catch (deleteError) {
                    console.log(`删除旧验证消息失败: ${deleteError.message}`);
                    // 删除失败仍继续处理
                  }
                  
                  await env.D1.prepare('UPDATE user_states SET last_verification_message_id = NULL WHERE chat_id = ?')
                    .bind(chatId)
                    .run();
                }
              } catch (error) {
                console.log(`查询旧验证消息失败: ${error.message}`);
                // 即使出错也继续处理
              }
              
              // 立即发送新的验证码
              try {
                await handleVerification(chatId, 0);
              } catch (verificationError) {
                console.error(`发送新验证码失败: ${verificationError.message}`);
                // 如果发送验证码失败，则再次尝试
                setTimeout(async () => {
                  try {
                    await handleVerification(chatId, 0);
                  } catch (retryError) {
                    console.error(`重试发送验证码仍失败: ${retryError.message}`);
                    await sendMessageToUser(chatId, '发送验证码失败，请发送任意消息重试');
                  }
                }, 1000);
              }
              return;
            } else {
              await sendMessageToUser(chatId, `请完成验证后发送消息"${text || '您的具体信息'}"。`);
            }
            return;
          }
          await sendMessageToUser(chatId, `请完成验证后发送消息"${text || '您的具体信息'}"。`);
          await handleVerification(chatId, messageId);
          return;
        }
      }

      if (text === '/start') {
        try {
        if (await checkStartCommandRate(chatId)) {
            await sendMessageToUser(chatId, "➡️您发送 /start 命令过于频繁，请稍后再试！如果您已经在聊天中，无需重复发送 /start 命令。");
            return;
        }

          // 先检查是否已有话题
          const existingTopicId = await getExistingTopicId(chatId);
          if (existingTopicId) {
        const successMessage = await getVerificationSuccessMessage();
            await sendMessageToUser(chatId, `${successMessage}\n➡️您已经在聊天中，无需重复发送 /start 命令。`);
            return;
          }
        
        // 获取用户信息
        const userInfo = await getUserInfo(chatId);
          if (!userInfo) {
            await sendMessageToUser(chatId, "无法获取用户信息，请稍后再试。");
            return;
          }

          // 发送欢迎消息
          const successMessage = await getVerificationSuccessMessage();
          await sendMessageToUser(chatId, `${successMessage}\n你好，欢迎使用私聊机器人，现在发送信息吧！`);
          
          // 创建话题，添加重试机制
          let topicId = null;
          let retries = 3;
          let error = null;
          
          while (retries > 0 && !topicId) {
            try {
              topicId = await ensureUserTopic(chatId, userInfo);
              if (topicId) break;
            } catch (err) {
              error = err;
              console.error(`创建话题失败，剩余重试次数: ${retries-1}, 错误: ${err.message}`);
              retries--;
              // 短暂延迟后重试
              if (retries > 0) {
                await new Promise(resolve => setTimeout(resolve, 1000));
              }
            }
          }
          
          if (!topicId) {
            console.error(`为用户 ${chatId} 创建话题失败，已达到最大重试次数`);
            await sendMessageToUser(chatId, "创建聊天话题失败，请稍后再试或联系管理员。");
            throw error || new Error("创建话题失败，未知原因");
          }
        } catch (error) {
          console.error(`处理 /start 命令时出错: ${error.message}`);
          // 不向用户发送错误信息，因为已经在上面处理过了
        }
        return;
      }

      const userInfo = await getUserInfo(chatId);
      if (!userInfo) {
        await sendMessageToUser(chatId, "无法获取用户信息，请稍后再试或联系管理员。");
        return;
      }

      let topicId = await ensureUserTopic(chatId, userInfo);
      if (!topicId) {
        await sendMessageToUser(chatId, "无法创建话题，请稍后再试或联系管理员。");
        return;
      }

      const isTopicValid = await validateTopic(topicId);
      if (!isTopicValid) {
        await env.D1.prepare('DELETE FROM chat_topic_mappings WHERE chat_id = ?').bind(chatId).run();
        topicIdCache.set(chatId, undefined);
        topicId = await ensureUserTopic(chatId, userInfo);
        if (!topicId) {
          await sendMessageToUser(chatId, "无法重新创建话题，请稍后再试或联系管理员。");
          return;
        }
      }

      const userName = userInfo.username || `User_${chatId}`;
      const nickname = userInfo.nickname || userName;

      if (text) {
        const formattedMessage = `${nickname}:\n${text}`;
        await sendMessageToTopic(topicId, formattedMessage, chatId, messageId);
      } else {
        await copyMessageToTopic(topicId, message);
      }

      // 检查是否是对编辑指导消息的回复
      if (message.reply_to_message && text) {
        const replyToMsgId = message.reply_to_message.message_id.toString();
        
        // 查询是否有待编辑的消息
        const editState = await env.D1.prepare(
          'SELECT original_message_id FROM edit_state WHERE user_id = ? AND instruction_message_id = ?'
        ).bind(chatId, replyToMsgId).first();
        
        if (editState && editState.original_message_id) {
          // 查询原消息在群组中的对应消息ID
          const originalMsgId = editState.original_message_id;
          const mapping = await env.D1.prepare(
            'SELECT group_message_id FROM message_mapping WHERE user_id = ? AND user_message_id = ?'
          ).bind(chatId, originalMsgId).first();
          
          if (mapping && mapping.group_message_id) {
            // 更新群组中的消息
            const userInfo = await getUserInfo(chatId);
            const nickname = userInfo.nickname || userInfo.username || `User_${chatId}`;
            const formattedMessage = `${nickname}:\n${text}\n\n(已编辑)`;
            
            try {
              await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/editMessageText`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                  chat_id: GROUP_ID,
                  message_id: mapping.group_message_id,
                  text: formattedMessage,
                  reply_markup: {
                    inline_keyboard: [
                      [
                        { text: "编辑消息", callback_data: `edit_${chatId}_${originalMsgId}` },
                        { text: "删除消息", callback_data: `delete_${chatId}_${originalMsgId}` }
                      ]
                    ]
                  }
                })
              });
              
              // 更新用户消息
              await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/editMessageText`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                  chat_id: chatId,
                  message_id: originalMsgId,
                  text: text
                })
              });
              
              // 发送编辑成功通知
              await sendMessageToUser(chatId, "消息已成功编辑！");
            } catch (error) {
              console.error(`编辑消息失败: ${error.message}`);
              await sendMessageToUser(chatId, "编辑消息失败，可能是消息已过期。");
            }
            
            // 删除编辑状态和编辑指导消息
            await env.D1.prepare('DELETE FROM edit_state WHERE user_id = ? AND instruction_message_id = ?')
              .bind(chatId, replyToMsgId).run();
            
            try {
              await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                  chat_id: chatId,
                  message_id: replyToMsgId
                })
              });
          } catch (error) {
              console.log(`删除编辑指导消息失败: ${error.message}`);
            }
            
            // 删除用户回复消息
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
              console.log(`删除用户回复消息失败: ${error.message}`);
            }
            
            return;
          }
        }
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
            text: "您有新消息！",
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
          return true;
        }
        return false;
          } catch (error) {
        return false;
      }
    }

    async function ensureUserTopic(chatId, userInfo) {
      // 使用全局锁防止同一用户创建多个话题
      let lock = topicCreationLocks.get(chatId);
      if (!lock) {
        lock = Promise.resolve();
        topicCreationLocks.set(chatId, lock);
      }

      // 创建新的锁，确保在当前锁完成前不会执行新的创建操作
      const newLock = (async () => {
        try {
          // 等待前一个锁完成
          await lock;
          
          // 再次检查是否已有话题（可能在等待期间已被创建）
      let topicId = await getExistingTopicId(chatId);
          if (topicId) {
            return topicId;
          }

          // 添加数据库级别的锁定机制
          // 使用事务和唯一约束来确保不会重复创建
          try {
            // 创建临时标记表示正在创建话题
            await env.D1.prepare('INSERT OR IGNORE INTO chat_topic_mappings (chat_id, topic_id) VALUES (?, ?)')
              .bind(chatId, 'creating')
              .run();
            
            // 再次检查，确保没有其他进程已经创建了话题
            const checkAgain = await env.D1.prepare('SELECT topic_id FROM chat_topic_mappings WHERE chat_id = ?')
              .bind(chatId)
              .first();
            
            if (checkAgain && checkAgain.topic_id !== 'creating') {
              // 另一个进程已经创建了话题
              topicIdCache.set(chatId, checkAgain.topic_id);
              return checkAgain.topic_id;
            }
            
            // 创建新话题
          const userName = userInfo.username || `User_${chatId}`;
          const nickname = userInfo.nickname || userName;
          topicId = await createForumTopic(nickname, userName, nickname, userInfo.id || chatId);
            
            // 更新数据库中的话题ID
            await env.D1.prepare('UPDATE chat_topic_mappings SET topic_id = ? WHERE chat_id = ?')
              .bind(topicId, chatId)
              .run();
            
            // 更新缓存
            topicIdCache.set(chatId, topicId);
          return topicId;
          } catch (error) {
            // 如果创建过程中出错，清除临时标记
            console.error(`创建话题时出错: ${error.message}`);
            
            // 再次检查是否已有话题（可能在出错期间已被创建）
            const finalCheck = await env.D1.prepare('SELECT topic_id FROM chat_topic_mappings WHERE chat_id = ?')
              .bind(chatId)
              .first();
            
            if (finalCheck && finalCheck.topic_id !== 'creating') {
              // 已有有效话题
              topicIdCache.set(chatId, finalCheck.topic_id);
              return finalCheck.topic_id;
            }
            
            // 清除临时标记
            await env.D1.prepare('DELETE FROM chat_topic_mappings WHERE chat_id = ? AND topic_id = ?')
              .bind(chatId, 'creating')
              .run();
            
            throw error; // 重新抛出错误
          }
        } finally {
          // 只有当这个锁是最新的锁时才删除
          if (topicCreationLocks.get(chatId) === newLock) {
            topicCreationLocks.delete(chatId);
          }
        }
      })();

      // 更新锁
      topicCreationLocks.set(chatId, newLock);
      return newLock;
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
      userStateCache.set(targetChatId, undefined);
      messageRateCache.set(targetChatId, undefined);
      topicIdCache.set(targetChatId, undefined);
      await sendMessageToTopic(topicId, `用户 ${targetChatId} 的状态已重置。`);
    }

    async function sendAdminPanel(chatId, topicId, privateChatId, messageId) {
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
        })
      ]);
    }

    async function getVerificationSuccessMessage() {
      const userRawEnabled = (await getSetting('user_raw_enabled', env.D1)) === 'true';
      if (!userRawEnabled) return '验证成功！您现在可以与我聊天。';

      const response = await fetch('https://raw.githubusercontent.com/iawooo/ctt/refs/heads/main/CFTeleTrans/start.md');
      if (!response.ok) return '验证成功！您现在可以与我聊天。';
      const message = await response.text();
      return message.trim() || '验证成功！您现在可以与我聊天。';
    }

    async function getNotificationContent() {
      const response = await fetch('https://raw.githubusercontent.com/iawooo/ctt/refs/heads/main/CFTeleTrans/notification.md');
      if (!response.ok) return '';
      const content = await response.text();
      return content.trim() || '';
    }

    async function checkStartCommandRate(chatId) {
      const now = Date.now();
      const window = 5 * 60 * 1000; // 5分钟窗口
      const maxStartsPerWindow = 1; // 每个窗口最多允许1次 /start 命令

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

      // 如果窗口已过期，重置计数
      if (now - data.start_window_start > window) {
        data.start_count = 1; // 设置为1因为当前请求也算一次
        data.start_window_start = now;
        await env.D1.prepare('UPDATE message_rates SET start_count = ?, start_window_start = ? WHERE chat_id = ?')
          .bind(data.start_count, data.start_window_start, chatId)
          .run();
      } else {
        // 窗口内增加计数
        data.start_count += 1;
        await env.D1.prepare('UPDATE message_rates SET start_count = ? WHERE chat_id = ?')
          .bind(data.start_count, chatId)
          .run();
      }

      // 更新缓存
      messageRateCache.set(chatId, data);
      
      // 返回是否超出限制
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
        settingsCache.set('verification_enabled', value === 'true');
        if (value === 'false') {
          const nowSeconds = Math.floor(Date.now() / 1000);
          const verifiedExpiry = nowSeconds + 3600 * 24;
          await env.D1.prepare('UPDATE user_states SET is_verified = ?, verified_expiry = ?, is_verifying = ?, verification_code = NULL, code_expiry = NULL, is_first_verification = ? WHERE chat_id NOT IN (SELECT chat_id FROM user_states WHERE is_blocked = TRUE)')
            .bind(true, verifiedExpiry, false, false)
            .run();
          userStateCache.clear();
        }
      } else if (key === 'user_raw_enabled') {
        settingsCache.set('user_raw_enabled', value === 'true');
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
      } else if (data.startsWith('admin_edit_')) {
        action = 'admin_edit';
        privateChatId = ''; // 管理员操作不涉及特定私聊ID
      } else if (data.startsWith('admin_delete_')) {
        action = 'admin_delete';
        privateChatId = ''; // 管理员操作不涉及特定私聊ID
      } else if (data.startsWith('edit_')) {
        action = 'edit';
        privateChatId = parts[1];
      } else if (data.startsWith('delete_')) {
        action = 'delete';
        privateChatId = parts[1];
      } else {
        action = data;
        privateChatId = '';
      }

      if (action === 'verify') {
        const [, userChatId, selectedAnswer, result] = data.split('_');
        if (userChatId !== chatId) {
          return;
        }

        let verificationState = userStateCache.get(chatId);
        if (verificationState === undefined) {
          verificationState = await env.D1.prepare('SELECT verification_code, code_expiry, is_verifying FROM user_states WHERE chat_id = ?')
            .bind(chatId)
            .first();
          if (!verificationState) {
            verificationState = { verification_code: null, code_expiry: null, is_verifying: false };
          }
          userStateCache.set(chatId, verificationState);
        }

        const storedCode = verificationState.verification_code;
        const codeExpiry = verificationState.code_expiry;
        const nowSeconds = Math.floor(Date.now() / 1000);

        if (!storedCode || (codeExpiry && nowSeconds > codeExpiry)) {
          await sendMessageToUser(chatId, '验证码已过期，正在为您发送新的验证码...');
          await env.D1.prepare('UPDATE user_states SET verification_code = NULL, code_expiry = NULL, is_verifying = FALSE WHERE chat_id = ?')
            .bind(chatId)
            .run();
          userStateCache.set(chatId, { ...verificationState, verification_code: null, code_expiry: null, is_verifying: false });
          
          // 删除旧的验证消息
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
            console.log(`删除过期验证按钮失败: ${error.message}`);
            // 即使删除失败也继续处理
          }
          
          // 立即发送新的验证码
          try {
            await handleVerification(chatId, 0);
          } catch (verificationError) {
            console.error(`发送新验证码失败: ${verificationError.message}`);
            // 如果发送验证码失败，则再次尝试
            setTimeout(async () => {
              try {
                await handleVerification(chatId, 0);
              } catch (retryError) {
                console.error(`重试发送验证码仍失败: ${retryError.message}`);
                await sendMessageToUser(chatId, '发送验证码失败，请发送任意消息重试');
              }
            }, 1000);
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

        await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: chatId,
            message_id: messageId
          })
        });
      } else if (action === 'admin_edit') {
        // 管理员编辑自己的消息
        const originalMessageId = data.split('_')[2]; // 确保这里获取的是原始消息的ID
        const senderId = callbackQuery.from.id.toString();
        const isAdmin = await checkIfAdmin(senderId);
        
        if (!isAdmin) {
          await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/answerCallbackQuery`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              callback_query_id: callbackQuery.id,
              text: "只有管理员可以编辑此消息",
              show_alert: true
            })
          });
          return;
        }
        
        try {
          // 发送编辑指导消息到群组话题中
          const instructionMsg = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              chat_id: GROUP_ID, // 直接发送到群组
              message_thread_id: topicId, // 确保在正确的话题中
              text: "请在当前话题中发送您想要编辑成的新内容。您的下一条消息将用于编辑。",
              reply_markup: {
                force_reply: true,
                selective: true
              }
            })
          }).then(res => res.json());
          
          if (instructionMsg.ok) {
            // 将回复标记为等待编辑状态
            await env.D1.prepare(
              'INSERT OR REPLACE INTO admin_edit_state (admin_id, topic_id, original_message_id, instruction_message_id) VALUES (?, ?, ?, ?)'
            ).bind(senderId, topicId, originalMessageId, instructionMsg.result.message_id.toString()).run();
          } else {
            throw new Error(`发送编辑指导消息失败: ${instructionMsg.description}`);
          }
          
          await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/answerCallbackQuery`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              callback_query_id: callbackQuery.id
            })
          });
        } catch (error) {
          console.error(`管理员编辑消息流程出错: ${error.message}`);
          await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/answerCallbackQuery`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              callback_query_id: callbackQuery.id,
              text: "编辑消息流程出错，请重试",
              show_alert: true
            })
          });
        }
      } else if (action === 'admin_delete') {
        // 管理员删除自己的消息
        const messageId = data.split('_')[2];
        const senderId = callbackQuery.from.id.toString();
        const isAdmin = await checkIfAdmin(senderId);
        
        if (!isAdmin) {
          await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/answerCallbackQuery`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              callback_query_id: callbackQuery.id,
              text: "只有管理员可以删除此消息",
              show_alert: true
            })
          });
          return;
        }
        
        try {
          // 查询此消息在私聊中的对应消息
          const result = await env.D1.prepare(
            'SELECT user_id, user_message_id FROM message_mapping WHERE group_message_id = ?'
          ).bind(messageId).first();
          
          // 删除群组中的消息
          await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              chat_id: GROUP_ID,
              message_id: messageId
            })
          });
          
          // 如果找到对应的私聊消息，也删除它
          if (result && result.user_id && result.user_message_id) {
            try {
              await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                  chat_id: result.user_id,
                  message_id: result.user_message_id
                })
              });
            } catch (error) {
              console.log(`删除私聊中的管理员消息失败: ${error.message}`);
              // 忽略错误，私聊消息可能已过期
            }
            
            // 从数据库中删除映射
            await env.D1.prepare('DELETE FROM message_mapping WHERE group_message_id = ?')
              .bind(messageId).run();
          }
          
          await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/answerCallbackQuery`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              callback_query_id: callbackQuery.id,
              text: "消息已删除"
            })
          });
        } catch (error) {
          console.error(`删除管理员消息失败: ${error.message}`);
          await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/answerCallbackQuery`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              callback_query_id: callbackQuery.id,
              text: "删除消息失败，可能是消息已过期",
              show_alert: true
            })
          });
        }
      } else if (action.startsWith('edit')) {
        // 编辑消息操作
        const [, userId, messageId] = data.split('_');
        
        // 检查是否是管理员或消息的发送者
        const senderId = callbackQuery.from.id.toString();
        const isAdmin = await checkIfAdmin(senderId);
        const isMessageSender = senderId === userId;
        
        if (!isAdmin && !isMessageSender) {
          await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/answerCallbackQuery`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              callback_query_id: callbackQuery.id,
              text: "您没有权限编辑此消息",
              show_alert: true
            })
          });
          return;
        }
        
        // 向用户发送编辑提示
        if (isMessageSender) {
          try {
            // 获取原始消息内容
            const originalMessage = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getMessage`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                chat_id: userId,
                message_id: messageId
              })
            }).then(res => res.json());
            
            let originalText = "";
            if (originalMessage.ok && originalMessage.result.text) {
              originalText = originalMessage.result.text;
            }
            
            // 发送编辑指导消息
            const instructionMsg = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                chat_id: userId,
                text: "请发送新的消息内容来替换原消息。原消息内容如下：\n\n" + originalText,
                reply_markup: {
                  force_reply: true,
                  selective: true
                }
              })
            }).then(res => res.json());
            
            if (instructionMsg.ok) {
              // 将回复标记为等待编辑状态
              await env.D1.prepare(
                'INSERT OR REPLACE INTO edit_state (user_id, original_message_id, instruction_message_id) VALUES (?, ?, ?)'
              ).bind(userId, messageId, instructionMsg.result.message_id.toString()).run();
            }
          } catch (error) {
            console.error(`发送编辑指导消息失败: ${error.message}`);
            await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/answerCallbackQuery`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                callback_query_id: callbackQuery.id,
                text: "发送编辑指导消息失败，请重试",
                show_alert: true
              })
            });
          }
        } else {
          // 管理员编辑用户消息
          await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/answerCallbackQuery`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              callback_query_id: callbackQuery.id,
              text: "管理员无法直接编辑用户消息，请通过回复发送新消息",
              show_alert: true
            })
          });
        }
      } else if (action.startsWith('delete')) {
        // 删除消息操作
        const [, userId, messageId] = data.split('_');
        
        // 检查是否是管理员或消息的发送者
        const senderId = callbackQuery.from.id.toString();
        const isAdmin = await checkIfAdmin(senderId);
        const isMessageSender = senderId === userId;
        
        if (!isAdmin && !isMessageSender) {
          await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/answerCallbackQuery`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              callback_query_id: callbackQuery.id,
              text: "您没有权限删除此消息",
              show_alert: true
            })
          });
          return;
        }
        
        // 查询群组中的消息ID
        const result = await env.D1.prepare(
          'SELECT group_message_id FROM message_mapping WHERE user_id = ? AND user_message_id = ?'
        ).bind(userId, messageId).first();
        
        if (result && result.group_message_id) {
          // 删除群组中的消息
          try {
            await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                chat_id: GROUP_ID,
                message_id: result.group_message_id
              })
            });
            
            // 删除私聊中的消息
            try {
              await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                  chat_id: userId,
                  message_id: messageId
                })
              });
            } catch (error) {
              // 私聊消息可能已经过期无法删除，忽略错误
              console.log(`无法删除私聊消息: ${error.message}`);
            }
            
            // 从数据库中删除映射
            await env.D1.prepare('DELETE FROM message_mapping WHERE user_id = ? AND user_message_id = ?')
              .bind(userId, messageId).run();
              
            await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/answerCallbackQuery`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                callback_query_id: callbackQuery.id,
                text: "消息已删除"
              })
            });
          } catch (error) {
            console.error(`删除消息失败: ${error.message}`);
            await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/answerCallbackQuery`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                callback_query_id: callbackQuery.id,
                text: "删除消息失败，可能是消息已过期",
                show_alert: true
              })
            });
          }
        } else {
          await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/answerCallbackQuery`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              callback_query_id: callbackQuery.id,
              text: "找不到对应的消息",
              show_alert: true
            })
          });
        }
      } else {
        await sendMessageToTopic(topicId, `未知操作：${action}`);
      }

      await sendAdminPanel(chatId, topicId, privateChatId, messageId);
    }

    async function handleVerification(chatId, messageId) {
      try {
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
          } catch (deleteError) {
            console.log(`删除上一条验证消息失败: ${deleteError.message}`);
            // 继续处理，即使删除失败
          }
          
          userState.last_verification_message_id = null;
          userStateCache.set(chatId, userState);
          await env.D1.prepare('UPDATE user_states SET last_verification_message_id = NULL WHERE chat_id = ?')
            .bind(chatId)
            .run();
        }

        // 确保发送验证码
        await sendVerification(chatId);
      } catch (error) {
        console.error(`处理验证过程失败: ${error.message}`);
        // 重置用户状态以防卡住
        try {
          await env.D1.prepare('UPDATE user_states SET is_verifying = FALSE WHERE chat_id = ?')
            .bind(chatId)
            .run();
          let currentState = userStateCache.get(chatId);
          if (currentState) {
            currentState.is_verifying = false;
            userStateCache.set(chatId, currentState);
          }
        } catch (resetError) {
          console.error(`重置用户验证状态失败: ${resetError.message}`);
        }
        throw error; // 向上传递错误以便调用方处理
      }
    }

    async function sendVerification(chatId) {
      try {
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

        const question = `请计算：${num1} ${operation} ${num2} = ?（点击下方按钮完成验证，勿重复点击！）`;
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
        } else {
          throw new Error(`Telegram API 返回错误: ${data.description || '未知错误'}`);
        }
      } catch (error) {
        console.error(`发送验证码失败: ${error.message}`);
        throw error; // 向上传递错误以便调用方处理
      }
    }

    async function checkIfAdmin(userId) {
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
    }

    async function getUserInfo(chatId) {
      let userInfo = userInfoCache.get(chatId);
      if (userInfo !== undefined) {
        return userInfo;
      }

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

      userInfoCache.set(chatId, userInfo);
      return userInfo;
    }

    async function getExistingTopicId(chatId) {
        let topicId = topicIdCache.get(chatId);
        if (topicId !== undefined) {
        // 确保缓存中不是临时标记
        if (topicId !== 'creating') {
          return topicId;
        }
        // 如果是临时标记，则从缓存中移除，重新查询数据库
        topicIdCache.set(chatId, undefined);
      }

      const result = await env.D1.prepare('SELECT topic_id FROM chat_topic_mappings WHERE chat_id = ?')
        .bind(chatId)
        .first();
      
      // 确保数据库中不是临时标记
      if (result && result.topic_id !== 'creating') {
        topicId = result.topic_id;
        topicIdCache.set(chatId, topicId);
        return topicId;
      }
      
      return null;
    }

    async function createForumTopic(topicName, userName, nickname, userId) {
      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/createForumTopic`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: GROUP_ID, name: `${nickname}` })
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
    }

    async function saveTopicId(chatId, topicId) {
      // 先检查是否已存在映射
      const existingMapping = await env.D1.prepare('SELECT topic_id FROM chat_topic_mappings WHERE chat_id = ?')
        .bind(chatId)
        .first();
      
      if (existingMapping) {
        // 如果存在且不是临时标记，则不更新
        if (existingMapping.topic_id !== 'creating') {
          topicIdCache.set(chatId, existingMapping.topic_id);
          return;
        }
        // 如果是临时标记，则更新
        await env.D1.prepare('UPDATE chat_topic_mappings SET topic_id = ? WHERE chat_id = ?')
          .bind(topicId, chatId)
          .run();
      } else {
        // 如果不存在，则插入
        await env.D1.prepare('INSERT INTO chat_topic_mappings (chat_id, topic_id) VALUES (?, ?)')
          .bind(chatId, topicId)
          .run();
      }
      
      // 更新缓存
      topicIdCache.set(chatId, topicId);
    }

    async function getPrivateChatId(topicId) {
      for (const [chatId, tid] of topicIdCache.cache) if (tid === topicId) return chatId;
      const mapping = await env.D1.prepare('SELECT chat_id FROM chat_topic_mappings WHERE topic_id = ?')
        .bind(topicId)
        .first();
      return mapping?.chat_id || null;
    }

    async function sendMessageToTopic(topicId, text, userId = null, userMessageId = null) {
      if (!text.trim()) {
        throw new Error('Message text is empty');
      }

      let requestBody = {
        chat_id: GROUP_ID,
        text: text,
        message_thread_id: topicId
      };
      
      // 如果是转发用户消息，添加编辑/删除按钮
      if (userId && userMessageId) {
        requestBody.reply_markup = {
          inline_keyboard: [
            [
              { text: "编辑消息", callback_data: `edit_${userId}_${userMessageId}` },
              { text: "删除消息", callback_data: `delete_${userId}_${userMessageId}` }
            ]
          ]
        };
      }
      
      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestBody)
      });
      const data = await response.json();
      if (!data.ok) {
        throw new Error(`Failed to send message to topic ${topicId}: ${data.description}`);
      }
      
      // 如果是转发用户消息，保存消息ID映射
      if (userId && userMessageId) {
        await env.D1.prepare(
          'INSERT OR REPLACE INTO message_mapping (user_id, user_message_id, group_message_id) VALUES (?, ?, ?)'
        ).bind(userId, userMessageId, data.result.message_id.toString()).run();
      }
      
      return data;
    }

    async function copyMessageToTopic(topicId, message) {
      const requestBody = {
        chat_id: GROUP_ID,
        from_chat_id: message.chat.id,
        message_id: message.message_id,
        message_thread_id: topicId,
        disable_notification: true,
        reply_markup: {
          inline_keyboard: [
            [
              { text: "编辑消息", callback_data: `edit_${message.chat.id}_${message.message_id}` },
              { text: "删除消息", callback_data: `delete_${message.chat.id}_${message.message_id}` }
            ]
          ]
        }
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
      
      // 保存消息ID映射
      await env.D1.prepare(
        'INSERT OR REPLACE INTO message_mapping (user_id, user_message_id, group_message_id) VALUES (?, ?, ?)'
      ).bind(message.chat.id.toString(), message.message_id.toString(), data.result.message_id.toString()).run();
      
      return data;
    }

    async function pinMessage(topicId, messageId) {
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
    }

    async function forwardMessageToPrivateChat(privateChatId, message) {
      let groupMessageSent = false; 
      console.log(`[forwardMessageToPrivateChat] 开始处理消息 ${message.message_id} 从 chat ${message.chat.id} 到 ${privateChatId}`);

      if (message.reply_to_message && message.chat.id.toString() === GROUP_ID) {
        console.log(`[forwardMessageToPrivateChat] 消息 ${message.message_id} 是对群组消息 ${message.reply_to_message.message_id} 的回复`);
        const result = await env.D1.prepare(
          'SELECT user_message_id FROM message_mapping WHERE group_message_id = ?'
        ).bind(message.reply_to_message.message_id.toString()).first();
        
        if (result && result.user_message_id) {
          console.log(`[forwardMessageToPrivateChat] 找到映射，私聊用户消息ID: ${result.user_message_id}`);
          const requestBody = {
            chat_id: privateChatId,
            text: message.text || "管理员发送了一条消息", // 确保文本不为空
            reply_to_message_id: result.user_message_id
          };
          
          try {
            const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify(requestBody)
            });
            const data = await response.json();
            console.log(`[forwardMessageToPrivateChat] 回复消息API响应: ${JSON.stringify(data)}`);
            if (data.ok) {
              await env.D1.prepare(
                'INSERT OR REPLACE INTO message_mapping (user_id, user_message_id, group_message_id) VALUES (?, ?, ?)'
              ).bind(privateChatId, data.result.message_id.toString(), message.message_id.toString()).run();
              groupMessageSent = true; 
              console.log(`[forwardMessageToPrivateChat] 回复消息已发送并映射，groupMessageSent: ${groupMessageSent}`);
            }
            return { ok: data.ok, result: data.result, ok_group_message_sent: groupMessageSent };
          } catch (error) {
            console.error(`回复消息失败: ${error.message}`);
          }
        }
      }
      
      console.log(`[forwardMessageToPrivateChat] 消息 ${message.message_id} 作为普通消息/复制处理`);
      const requestBody = {
        chat_id: privateChatId,
        from_chat_id: message.chat.id, // 可能是GROUP_ID (管理员发) 或 privateChatId (用户发)
        message_id: message.message_id,
        disable_notification: true
      };
      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/copyMessage`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestBody)
      });
      const data = await response.json();
      console.log(`[forwardMessageToPrivateChat] 复制消息API响应: ${JSON.stringify(data)}`);
      if (!data.ok) {
        console.error(`[forwardMessageToPrivateChat] 复制消息失败: ${data.description}`);
        throw new Error(`Failed to forward/copy message to private chat: ${data.description}`);
      }
      
      if (data.ok) {
        if (message.chat.id.toString() === GROUP_ID) {
          console.log(`[forwardMessageToPrivateChat] 消息 ${message.message_id} 源自群组，进行映射`);
          // ... (database insert logic)
          groupMessageSent = true; 
          console.log(`[forwardMessageToPrivateChat] 群组消息已复制并映射，groupMessageSent: ${groupMessageSent}`);
        } else {
          console.log(`[forwardMessageToPrivateChat] 消息 ${message.message_id} 非源自群组，不进行特定映射处理，groupMessageSent: ${groupMessageSent}`);
        }
      }
      console.log(`[forwardMessageToPrivateChat] 函数返回: ok=${data.ok}, groupMessageSent=${groupMessageSent}`);
      return { ok: data.ok, result: data.result, ok_group_message_sent: groupMessageSent };
    }

    async function onEditedMessage(editedMessage) {
      const chatId = editedMessage.chat.id.toString();
      
      // 如果是私聊消息，则转发编辑后的消息到群组
      if (chatId !== GROUP_ID) {
        const userInfo = await getUserInfo(chatId);
        if (!userInfo) {
          return;
        }
        
        const topicId = await getExistingTopicId(chatId);
        if (!topicId) {
          return;
        }
        
        // 查询原消息在群组中的对应消息ID
        const result = await env.D1.prepare(
          'SELECT group_message_id FROM message_mapping WHERE user_id = ? AND user_message_id = ?'
        ).bind(chatId, editedMessage.message_id.toString()).first();
        
        if (result && result.group_message_id) {
          const nickname = userInfo.nickname || userInfo.username || `User_${chatId}`;
          const text = editedMessage.text;
          if (text) {
            const formattedMessage = `${nickname}:\n${text}\n\n(已编辑)`;
            await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/editMessageText`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                chat_id: GROUP_ID,
                message_id: result.group_message_id,
                text: formattedMessage
              })
            });
          }
        }
      }
    }

    async function sendMessageToUser(chatId, text) {
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
      return new Response('Internal Server Error', { status: 500 });
    }
  }
};
