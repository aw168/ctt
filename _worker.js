// 从环境变量中读取配置
let BOT_TOKEN;
let GROUP_ID;
let MAX_MESSAGES_PER_MINUTE;
let isInitialized = false;

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

    if (!env.D1) {
      console.error('D1 database is not bound. Please check your Worker environment configuration.');
      return new Response('Server configuration error: D1 database is not bound. Please check your Worker environment configuration.', { status: 500 });
    }

    if (!BOT_TOKEN || !GROUP_ID) {
      console.error('Missing required environment variables');
      return new Response('Server configuration error: Missing required environment variables', { status: 500 });
    }

    if (!isInitialized) {
      console.log('Performing initialization...');
      const dbCheckResult = await checkAndFixTables(env);
      if (!dbCheckResult.success) {
        return new Response(dbCheckResult.message, { status: 500 });
      }

      const webhookResult = await registerWebhook(request, env);
      if (!webhookResult.success) {
        return new Response(webhookResult.message, { status: 500 });
      }

      isInitialized = true;
      console.log('Initialization completed successfully.');
    }

    const url = new URL(request.url);
    if (url.pathname === '/webhook') {
      try {
        const update = await request.json();
        await handleUpdate(update, env);
        return new Response('OK');
      } catch (error) {
        console.error('Error parsing request or handling update:', error);
        return new Response('Bad Request', { status: 400 });
      }
    } else if (url.pathname === '/registerWebhook') {
      return await registerWebhook(request, env);
    } else if (url.pathname === '/unRegisterWebhook') {
      return await unRegisterWebhook(env);
    } else if (url.pathname === '/initTables') {
      return await checkAndFixTables(env);
    }

    return new Response('Not Found', { status: 404 });
  }
};

async function checkAndFixTables(env) {
  console.log('Checking database tables...');
  
  try {
    const tableSchemas = {
      'user_states': 'CREATE TABLE user_states (chat_id TEXT PRIMARY KEY, is_blocked BOOLEAN DEFAULT FALSE, is_verified BOOLEAN DEFAULT FALSE, verified_expiry INTEGER, verification_code TEXT, code_expiry INTEGER, last_verification_message_id TEXT, is_first_verification BOOLEAN DEFAULT FALSE, is_rate_limited BOOLEAN DEFAULT FALSE, last_activity INTEGER, activity_count INTEGER DEFAULT 0, last_verification_time INTEGER)',
      'message_rates': 'CREATE TABLE message_rates (chat_id TEXT PRIMARY KEY, message_count INTEGER DEFAULT 0, window_start INTEGER)',
      'chat_topic_mappings': 'CREATE TABLE chat_topic_mappings (chat_id TEXT PRIMARY KEY, topic_id TEXT NOT NULL)'
    };
    
    console.log('Attempting to query existing tables...');
    const existingTablesResult = await env.D1.prepare(
      `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`
    ).all();
    if (!existingTablesResult.success) {
      console.error('Failed to query existing tables:', existingTablesResult.error);
      throw new Error(`Failed to query existing tables: ${existingTablesResult.error}`);
    }
    const existingTables = existingTablesResult.results.map(row => row.name);
    console.log('Existing tables:', existingTables);
    
    for (const [tableName, createTableSQL] of Object.entries(tableSchemas)) {
      if (!existingTables.includes(tableName)) {
        console.log(`Table ${tableName} does not exist. Creating...`);
        console.log(`Executing SQL: ${createTableSQL}`);
        const createResult = await env.D1.exec(createTableSQL);
        if (createResult.error) {
          console.error(`Failed to create table ${tableName}:`, createResult.error);
          throw new Error(`Failed to create table ${tableName}: ${createResult.error}`);
        }
        console.log(`Table ${tableName} created successfully.`);
      } else {
        console.log(`Table ${tableName} exists. Checking structure...`);
        const tableInfoResult = await env.D1.prepare(`PRAGMA table_info(${tableName})`).all();
        if (!tableInfoResult.success) {
          console.error(`Failed to get table info for ${tableName}:`, tableInfoResult.error);
          throw new Error(`Failed to get table info for ${tableName}: ${tableInfoResult.error}`);
        }
        const columns = tableInfoResult.results;
        
        const expectedColumns = createTableSQL
          .match(/\(([^)]+)\)/)[1]
          .split(',')
          .map(col => col.trim().split(' ')[0]);
        
        const actualColumns = columns.map(col => col.name);
        const missingColumns = expectedColumns.filter(col => !actualColumns.includes(col));
        
        if (missingColumns.length > 0) {
          console.log(`Table ${tableName} is missing columns: ${missingColumns.join(', ')}. Recreating...`);
          await env.D1.exec(`ALTER TABLE ${tableName} RENAME TO ${tableName}_old`);
          console.log(`Executing SQL for recreate: ${createTableSQL}`);
          await env.D1.exec(createTableSQL);
          
          const commonColumns = actualColumns.filter(col => expectedColumns.includes(col));
          if (commonColumns.length > 0) {
            await env.D1.exec(
              `INSERT INTO ${tableName} (${commonColumns.join(', ')}) 
               SELECT ${commonColumns.join(', ')} FROM ${tableName}_old`
            );
          }
          
          await env.D1.exec(`DROP TABLE ${tableName}_old`);
          console.log(`Table ${tableName} recreated and data migrated.`);
        } else {
          console.log(`Table ${tableName} exists and has all required columns.`);
        }
      }
    }
    
    console.log('Database check completed successfully.');
    return { success: true, message: 'Database check completed successfully.' };
  } catch (error) {
    console.error('Error checking/fixing database tables:', error);
    return { success: false, message: `Error checking/fixing database tables: ${error.message}` };
  }
}

async function registerWebhook(request, env) {
  console.log('BOT_TOKEN in registerWebhook:', BOT_TOKEN);
  const webhookUrl = `${new URL(request.url).origin}/webhook`;
  try {
    const response = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/setWebhook`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url: webhookUrl }),
    }).then(r => r.json());
    if (response.ok) {
      return { success: true, message: 'Webhook set successfully' };
    } else {
      console.error('Failed to set webhook:', response.description);
      return { success: false, message: `Failed to set webhook: ${response.description}` };
    }
  } catch (error) {
    console.error('Error registering webhook:', error);
    return { success: false, message: `Error registering webhook: ${error.message}` };
  }
}

async function unRegisterWebhook(env) {
  console.log('BOT_TOKEN in unRegisterWebhook:', BOT_TOKEN);
  try {
    const response = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/setWebhook`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url: '' }),
    }).then(r => r.json());
    if (response.ok) {
      return new Response('Webhook removed', { status: 200 });
    } else {
      console.error('Failed to remove webhook:', response.description);
      return new Response(`Failed to remove webhook: ${response.description}`, { status: 500 });
    }
  } catch (error) {
    console.error('Error unregistering webhook:', error);
    return new Response(`Error unregistering webhook: ${error.message}`, { status: 500 });
  }
}

async function updateUserActivity(chatId, env) {
  const nowSeconds = Math.floor(Date.now() / 1000);
  await env.D1.prepare(`
    INSERT OR REPLACE INTO user_states 
    (chat_id, last_activity, activity_count) 
    VALUES (?, ?, COALESCE((SELECT activity_count FROM user_states WHERE chat_id = ?) + 1, 1))
  `).bind(chatId, nowSeconds, chatId).run();
}

async function handleUpdate(update, env) {
  if (update.message) {
    await onMessage(update.message, env);
  } else if (update.callback_query) {
    await onCallbackQuery(update.callback_query, env);
  }
}

async function onMessage(message, env) {
  const chatId = message.chat.id.toString();
  const text = message.text || '';
  const messageId = message.message_id.toString(); // 确保消息 ID 是字符串

  await updateUserActivity(chatId, env);

  if (chatId === GROUP_ID) {
    const topicId = message.message_thread_id;
    if (topicId) {
      const privateChatId = await getPrivateChatId(topicId, env);
      if (privateChatId) {
        if (text.startsWith('/block') || text.startsWith('/unblock') || text.startsWith('/checkblock') || text.startsWith('/check')) {
          await handleAdminCommand(message, topicId, privateChatId, env);
          return;
        }
        await forwardMessageToPrivateChat(privateChatId, message, env);
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

  const verificationState = await env.D1.prepare('SELECT is_verified, verified_expiry, last_verification_time FROM user_states WHERE chat_id = ?')
    .bind(chatId)
    .first();
  let isVerified = verificationState ? verificationState.is_verified : false;
  const verifiedExpiry = verificationState ? Number(verificationState.verified_expiry) : null;
  const lastVerificationTime = verificationState ? Number(verificationState.last_verification_time) : 0;
  const nowSeconds = Math.floor(Date.now() / 1000);

  if (verifiedExpiry && nowSeconds > verifiedExpiry) {
    console.log(`Verification expired for chatId ${chatId}, resetting is_verified.`);
    await env.D1.prepare('UPDATE user_states SET is_verified = ?, verified_expiry = NULL WHERE chat_id = ?')
      .bind(false, chatId)
      .run();
    isVerified = false;
  }
  console.log(`User ${chatId} verification status: ${isVerified}`);

  if (text === '/start') {
    await env.D1.prepare('INSERT OR REPLACE INTO user_states (chat_id, is_first_verification) VALUES (?, ?)')
      .bind(chatId, true)
      .run();

    const welcomeMessage = await getVerificationSuccessMessage();
    await sendMessageToUser(chatId, `${welcomeMessage}\n你好，欢迎使用私聊机器人，现在发送信息吧！`, env);

    if (!isVerified) {
      await handleVerification(chatId, messageId, env);
    }
    return;
  }

  if (!isVerified) {
    // 添加冷却机制：如果距离上次生成验证码不到 10 秒，则忽略消息
    if (lastVerificationTime && (nowSeconds - lastVerificationTime) < 10) {
      console.log(`Verification generation on cooldown for chatId ${chatId}, last_verification_time: ${lastVerificationTime}, now: ${nowSeconds}`);
      // 不发送任何提示，直接忽略消息，避免用户误操作
      return;
    }

    const messageContent = text || '非文本消息';
    await sendMessageToUser(chatId, `无法转发的信息：${messageContent}\n无法发送，请完成验证`, env);
    await handleVerification(chatId, messageId, env);
    return;
  }

  if (await checkMessageRate(chatId, env)) {
    console.log(`User ${chatId} exceeded message rate limit, resetting verification.`);
    await env.D1.prepare('UPDATE user_states SET is_verified = ?, verified_expiry = NULL, is_rate_limited = ? WHERE chat_id = ?')
      .bind(false, true, chatId)
      .run();
    const messageContent = text || '非文本消息';
    await sendMessageToUser(chatId, `无法转发的信息：${messageContent}\n信息过于频繁，请完成验证后发送信息`, env);
    await handleVerification(chatId, messageId, env);
    return;
  }

  try {
    const userInfo = await getUserInfo(chatId, env);
    const userName = userInfo.username || userInfo.first_name;
    const nickname = `${userInfo.first_name} ${userInfo.last_name || ''}`.trim();
    const topicName = `${nickname}`;

    let topicId = await getExistingTopicId(chatId, env);
    if (!topicId) {
      topicId = await createForumTopic(topicName, userName, nickname, userInfo.id, env);
      await saveTopicId(chatId, topicId, env);
    }

    if (text) {
      const formattedMessage = `*${nickname}:*\n------------------------------------------------\n\n${text}`;
      await sendMessageToTopic(topicId, formattedMessage, env);
    } else {
      await copyMessageToTopic(topicId, message, env);
    }
  } catch (error) {
    console.error(`Error handling message from chatId ${chatId}:`, error);
  }
}

async function handleAdminCommand(message, topicId, privateChatId, env) {
  const text = message.text;
  const senderId = message.from.id.toString();

  const isAdmin = await checkIfAdmin(senderId, env);
  if (!isAdmin) {
    await sendMessageToTopic(topicId, '只有管理员可以使用此命令。', env);
    return;
  }

  if (text === '/block') {
    await env.D1.prepare('INSERT OR REPLACE INTO user_states (chat_id, is_blocked) VALUES (?, ?)')
      .bind(privateChatId, true)
      .run();
    await sendMessageToTopic(topicId, `用户 ${privateChatId} 已被拉黑，消息将不再转发。`, env);
  } else if (text === '/unblock') {
    await env.D1.prepare('UPDATE user_states SET is_blocked = ? WHERE chat_id = ?')
      .bind(false, privateChatId)
      .run();
    await sendMessageToTopic(topicId, `用户 ${privateChatId} 已解除拉黑，消息将继续转发。`, env);
  } else if (text === '/checkblock') {
    const userState = await env.D1.prepare('SELECT is_blocked FROM user_states WHERE chat_id = ?')
      .bind(privateChatId)
      .first();
    const isBlocked = userState ? userState.is_blocked : false;
    const status = isBlocked ? '是' : '否';
    await sendMessageToTopic(topicId, `用户 ${privateChatId} 是否在黑名单中：${status}`, env);
  } else if (text === '/check') {
    // 获取用户状态
    const userState = await env.D1.prepare(`
      SELECT is_blocked, is_verified, verified_expiry, verification_code, code_expiry, 
             last_verification_message_id, is_first_verification, is_rate_limited, 
             last_activity, activity_count, last_verification_time 
      FROM user_states 
      WHERE chat_id = ?
    `).bind(privateChatId).first();

    // 获取消息频率
    const messageRate = await env.D1.prepare(`
      SELECT message_count, window_start 
      FROM message_rates 
      WHERE chat_id = ?
    `).bind(privateChatId).first();

    const nowSeconds = Math.floor(Date.now() / 1000);
    let debugMessage = `用户 ${privateChatId} 状态信息：\n\n`;

    // 用户状态信息
    debugMessage += `**用户状态 (user_states):**\n`;
    if (userState) {
      debugMessage += `- 是否被拉黑 (is_blocked): ${userState.is_blocked ? '是' : '否'}\n`;
      debugMessage += `- 是否已验证 (is_verified): ${userState.is_verified ? '是' : '否'}\n`;
      debugMessage += `- 验证有效期 (verified_expiry): ${userState.verified_expiry || '无'} (${userState.verified_expiry && nowSeconds > userState.verified_expiry ? '已过期' : '未过期'})\n`;
      debugMessage += `- 当前验证码 (verification_code): ${userState.verification_code || '无'}\n`;
      debugMessage += `- 验证码有效期 (code_expiry): ${userState.code_expiry || '无'} (${userState.code_expiry && nowSeconds > userState.code_expiry ? '已过期' : '未过期'})\n`;
      debugMessage += `- 最后验证码消息ID (last_verification_message_id): ${userState.last_verification_message_id || '无'}\n`;
      debugMessage += `- 是否首次验证 (is_first_verification): ${userState.is_first_verification ? '是' : '否'}\n`;
      debugMessage += `- 是否被限流 (is_rate_limited): ${userState.is_rate_limited ? '是' : '否'}\n`;
      debugMessage += `- 最后活动时间 (last_activity): ${userState.last_activity || '无'}\n`;
      debugMessage += `- 活动次数 (activity_count): ${userState.activity_count || 0}\n`;
      debugMessage += `- 最后生成验证码时间 (last_verification_time): ${userState.last_verification_time || '无'}\n`;
    } else {
      debugMessage += `未找到用户状态记录。\n`;
    }

    // 消息频率信息
    debugMessage += `\n**消息频率 (message_rates):**\n`;
    if (messageRate) {
      const windowStart = messageRate.window_start || 0;
      const messageCount = messageRate.message_count || 0;
      const window = 60 * 1000; // 1 分钟窗口
      const windowEnd = windowStart + window;
      const currentTime = Date.now();
      debugMessage += `- 消息计数 (message_count): ${messageCount}\n`;
      debugMessage += `- 窗口开始时间 (window_start): ${windowStart}\n`;
      debugMessage += `- 窗口是否有效: ${currentTime <= windowEnd ? '是' : '否'}\n`;
    } else {
      debugMessage += `未找到消息频率记录。\n`;
    }

    // 当前时间
    debugMessage += `\n**当前时间:** ${nowSeconds} (${new Date(nowSeconds * 1000).toISOString()})\n`;

    await sendMessageToTopic(topicId, debugMessage, env);
  }
}

async function checkIfAdmin(userId, env) {
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

async function handleVerification(chatId, messageId, env) {
  console.log(`Starting verification process for chatId ${chatId}`);
  
  try {
    // 清理旧的验证码状态
    const oldState = await env.D1.prepare('SELECT last_verification_message_id FROM user_states WHERE chat_id = ?')
      .bind(chatId)
      .first();
    const oldMessageId = oldState && oldState.last_verification_message_id ? oldState.last_verification_message_id : null;

    if (oldMessageId) {
      try {
        await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: chatId,
            message_id: oldMessageId,
          }),
        });
        console.log(`Deleted old verification message ${oldMessageId} for chatId ${chatId}`);
      } catch (error) {
        // 忽略删除旧消息的错误，继续处理
        console.error(`Failed to delete old verification message ${oldMessageId} for chatId ${chatId}:`, error);
      }
    }

    // 确保记录生成验证码的时间
    const nowSeconds = Math.floor(Date.now() / 1000);
    
    // 生成并发送新验证码
    await sendVerification(chatId, env);
    
  } catch (error) {
    console.error(`Error in handleVerification for chatId ${chatId}:`, error);
    await sendMessageToUser(chatId, '验证流程出错，请稍后重试或联系管理员。', env);
  }
}

async function sendVerification(chatId, env) {
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
    callback_data: `verify_${chatId}_${option}`,
  }));

  const question = `请计算：${num1} ${operation} ${num2} = ?（点击下方按钮完成验证）`;
  const nowSeconds = Math.floor(Date.now() / 1000);
  const codeExpiry = nowSeconds + 300; // 5 分钟有效期
  const verificationCode = correctResult.toString(); // 确保验证码是字符串
  
  console.log(`Generating verification for chatId ${chatId}: code=${verificationCode}, expiry=${codeExpiry}, now=${nowSeconds}`);

  try {
    // 首先清理旧的验证状态，避免干扰
    await env.D1.prepare('UPDATE user_states SET verification_code = NULL, code_expiry = NULL, last_verification_message_id = NULL WHERE chat_id = ?')
      .bind(chatId)
      .run();
    
    // 插入新的验证码
    await env.D1.prepare('UPDATE user_states SET verification_code = ?, code_expiry = ?, last_verification_time = ? WHERE chat_id = ?')
      .bind(verificationCode, codeExpiry, nowSeconds, chatId)
      .run();
    
    // 验证是否成功写入
    const verificationCheck = await env.D1.prepare('SELECT verification_code, code_expiry FROM user_states WHERE chat_id = ?')
      .bind(chatId)
      .first();
    
    if (!verificationCheck || verificationCheck.verification_code !== verificationCode) {
      console.error(`Failed to store verification code for chatId ${chatId}:`, verificationCheck);
      await sendMessageToUser(chatId, '存储验证码失败，请稍后重试。', env);
      return;
    }
    
    console.log(`Verification code stored successfully for chatId ${chatId}:`, verificationCheck);

    // 发送验证消息
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
    
    const newMessageId = data.result.message_id.toString(); // 确保是字符串
    
    // 更新验证消息ID
    await env.D1.prepare('UPDATE user_states SET last_verification_message_id = ? WHERE chat_id = ?')
      .bind(newMessageId, chatId)
      .run();
    
    // 验证更新是否成功
    const messageIdCheck = await env.D1.prepare('SELECT last_verification_message_id FROM user_states WHERE chat_id = ?')
      .bind(chatId)
      .first();
    
    if (!messageIdCheck || messageIdCheck.last_verification_message_id !== newMessageId) {
      console.error(`Failed to update last_verification_message_id for chatId ${chatId}`);
      // 尝试重新更新
      await env.D1.prepare('UPDATE user_states SET last_verification_message_id = ? WHERE chat_id = ?')
        .bind(newMessageId, chatId)
        .run();
      
      // 再次验证
      const retryCheck = await env.D1.prepare('SELECT last_verification_message_id FROM user_states WHERE chat_id = ?')
        .bind(chatId)
        .first();
      
      if (!retryCheck || retryCheck.last_verification_message_id !== newMessageId) {
        console.error(`Failed to update last_verification_message_id after retry for chatId ${chatId}`);
        await sendMessageToUser(chatId, '验证码发送成功，但系统记录出现问题。如验证失败，请重新发送消息获取新验证码。', env);
      }
    }
    
    console.log(`Verification message sent for chatId ${chatId}, message_id: ${newMessageId}`);
    
  } catch (error) {
    console.error("Error in sendVerification:", error);
    await sendMessageToUser(chatId, '发送验证码过程中出错，请稍后重试。', env);
  }
}

async function getVerificationSuccessMessage() {
  try {
    const response = await fetch('https://raw.githubusercontent.com/iawooo/ctt/refs/heads/main/CFTeleTrans/start.md');
    if (!response.ok) {
      throw new Error(`Failed to fetch start.md: ${response.statusText}`);
    }
    const message = await response.text();
    const trimmedMessage = message.trim();
    if (!trimmedMessage) {
      throw new Error('start.md content is empty');
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
      throw new Error(`Failed to fetch notification.md: ${response.statusText}`);
    }
    const content = await response.text();
    const trimmedContent = content.trim();
    if (!trimmedContent) {
      throw new Error('notification.md content is empty');
    }
    return trimmedContent;
  } catch (error) {
    console.error("Error fetching notification content:", error);
    return '';
  }
}

async function onCallbackQuery(callbackQuery, env) {
  const chatId = callbackQuery.message.chat.id.toString();
  const data = callbackQuery.data;
  const messageId = callbackQuery.message.message_id.toString(); // 确保一开始就是字符串类型

  await updateUserActivity(chatId, env);

  if (!data.startsWith('verify_')) return;

  const [, userChatId, selectedAnswer] = data.split('_');
  if (userChatId !== chatId) return;

  const verificationState = await env.D1.prepare('SELECT verification_code, code_expiry, last_verification_message_id FROM user_states WHERE chat_id = ?')
    .bind(chatId)
    .first();
  const storedCode = verificationState ? verificationState.verification_code : null;
  const codeExpiry = verificationState ? Number(verificationState.code_expiry) : null; // 确保转换为数字
  const storedMessageId = verificationState ? String(verificationState.last_verification_message_id) : null; // 确保转换为字符串
  const nowSeconds = Math.floor(Date.now() / 1000);
  
  console.log(`Checking verification for chatId ${chatId}: storedCode=${storedCode}, codeExpiry=${codeExpiry}, nowSeconds=${nowSeconds}, storedMessageId=${storedMessageId}, messageId=${messageId}`);

  // 确保验证码存在且未过期
  if (!storedCode || !codeExpiry || nowSeconds > codeExpiry) {
    console.log(`Verification expired or not found for chatId ${chatId}`);
    await sendMessageToUser(chatId, '验证码已过期，请重新发送消息以获取新验证码。', env);
    // 清理过期的验证状态
    await env.D1.prepare('UPDATE user_states SET verification_code = NULL, code_expiry = NULL, last_verification_message_id = NULL WHERE chat_id = ?')
      .bind(chatId)
      .run();
    await handleVerification(chatId, messageId, env);
    
    // 回应回调查询
    await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/answerCallbackQuery`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        callback_query_id: callbackQuery.id,
        text: '验证码已过期，已发送新验证码',
        show_alert: true,
      }),
    });
    return;
  }

  // 确保消息ID匹配 - 严格使用字符串比较
  if (storedMessageId && storedMessageId !== messageId) {
    console.log(`Ignoring outdated verification message for chatId ${chatId}, current messageId: ${messageId}, storedMessageId: ${storedMessageId}`);
    await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/answerCallbackQuery`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        callback_query_id: callbackQuery.id,
        text: '此验证码已过期，请使用最新的验证码。',
        show_alert: true,
      }),
    });
    return;
  }

  // 处理验证结果
  let verificationSuccess = false;
  if (selectedAnswer === storedCode) {
    verificationSuccess = true;
    const verifiedExpiry = nowSeconds + 3600; // 1 小时有效期
    
    // 使用事务确保状态更新的一致性
    try {
      await env.D1.prepare('UPDATE user_states SET is_verified = ?, verified_expiry = ?, verification_code = NULL, code_expiry = NULL, last_verification_message_id = NULL, last_verification_time = NULL WHERE chat_id = ?')
        .bind(true, verifiedExpiry, chatId)
        .run();
      
      console.log(`Verification successful for chatId ${chatId}, verified until ${verifiedExpiry}`);
      
      // 验证更新是否成功
      const updatedState = await env.D1.prepare('SELECT is_verified FROM user_states WHERE chat_id = ?')
        .bind(chatId)
        .first();
        
      if (!updatedState || !updatedState.is_verified) {
        console.error(`Failed to update verification status for chatId ${chatId}`);
        await sendMessageToUser(chatId, '验证状态更新失败，请联系管理员。', env);
        verificationSuccess = false;
      }
    } catch (error) {
      console.error(`Database error during verification for chatId ${chatId}:`, error);
      await sendMessageToUser(chatId, '验证处理过程中出错，请稍后重试。', env);
      verificationSuccess = false;
    }
    
    if (verificationSuccess) {
      const userState = await env.D1.prepare('SELECT is_first_verification, is_rate_limited FROM user_states WHERE chat_id = ?')
        .bind(chatId)
        .first();
      const isFirstVerification = userState ? userState.is_first_verification : false;
      const isRateLimited = userState ? userState.is_rate_limited : false;

      if (isFirstVerification) {
        const successMessage = await getVerificationSuccessMessage();
        await sendMessageToUser(chatId, `${successMessage}\n你好，欢迎使用私聊机器人！`, env);
        await env.D1.prepare('UPDATE user_states SET is_first_verification = ? WHERE chat_id = ?')
          .bind(false, chatId)
          .run();
      } else {
        await sendMessageToUser(chatId, '验证成功！请重新发送您的消息', env);
      }

      if (isRateLimited) {
        await env.D1.prepare('UPDATE user_states SET is_rate_limited = ? WHERE chat_id = ?')
          .bind(false, chatId)
          .run();
      }
    }
  } else {
    await sendMessageToUser(chatId, '验证失败，请重新尝试。', env);
    await handleVerification(chatId, messageId, env);
  }

  // 尝试删除验证消息
  try {
    await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        chat_id: chatId,
        message_id: messageId,
      }),
    });
    console.log(`Deleted verification message ${messageId} for chatId ${chatId}`);
  } catch (error) {
    console.error("Error deleting verification message:", error);
    // 忽略删除消息失败，不影响主流程
  }

  // 回应回调查询
  await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/answerCallbackQuery`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      callback_query_id: callbackQuery.id,
      text: verificationSuccess ? '验证成功' : '验证失败',
    }),
  });
}

async function checkUserVerificationState(chatId, env) {
  try {
    const state = await env.D1.prepare(`
      SELECT 
        is_verified, 
        verified_expiry, 
        verification_code, 
        code_expiry, 
        last_verification_message_id,
        last_verification_time
      FROM user_states 
      WHERE chat_id = ?
    `).bind(chatId).first();
    
    if (!state) {
      return { exists: false, message: "用户状态记录不存在" };
    }
    
    const nowSeconds = Math.floor(Date.now() / 1000);
    const verificationStatus = {
      exists: true,
      is_verified: state.is_verified || false,
      verified_expiry: state.verified_expiry || null,
      verification_code: state.verification_code || null,
      code_expiry: state.code_expiry || null,
      last_verification_message_id: state.last_verification_message_id || null,
      last_verification_time: state.last_verification_time || null,
      is_verification_expired: state.code_expiry ? nowSeconds > Number(state.code_expiry) : true,
      is_verified_expired: state.verified_expiry ? nowSeconds > Number(state.verified_expiry) : true
    };
    
    return verificationStatus;
  } catch (error) {
    console.error("Error checking user verification state:", error);
    return { error: error.message };
  }
}

async function checkMessageRate(chatId, env) {
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

async function getUserInfo(chatId, env) {
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

async function getExistingTopicId(chatId, env) {
  const mapping = await env.D1.prepare('SELECT topic_id FROM chat_topic_mappings WHERE chat_id = ?')
    .bind(chatId)
    .first();
  return mapping ? mapping.topic_id : null;
}

async function createForumTopic(topicName, userName, nickname, userId, env) {
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
  const messageResponse = await sendMessageToTopic(topicId, pinnedMessage, env);
  const messageId = messageResponse.result.message_id;
  await pinMessage(topicId, messageId, env);

  return topicId;
}

async function saveTopicId(chatId, topicId, env) {
  await env.D1.prepare('INSERT OR REPLACE INTO chat_topic_mappings (chat_id, topic_id) VALUES (?, ?)')
    .bind(chatId, topicId)
    .run();
}

async function getPrivateChatId(topicId, env) {
  const mapping = await env.D1.prepare('SELECT chat_id FROM chat_topic_mappings WHERE topic_id = ?')
    .bind(topicId)
    .first();
  return mapping ? mapping.chat_id : null;
}

async function sendMessageToTopic(topicId, text, env) {
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

async function copyMessageToTopic(topicId, message, env) {
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

async function pinMessage(topicId, messageId, env) {
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

async function forwardMessageToPrivateChat(privateChatId, message, env) {
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

async function sendMessageToUser(chatId, text, env) {
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
