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
      'user_states': 'CREATE TABLE user_states (chat_id TEXT PRIMARY KEY, is_blocked BOOLEAN DEFAULT FALSE, is_verified BOOLEAN DEFAULT FALSE, verified_expiry INTEGER, verification_code TEXT, code_expiry INTEGER, last_verification_message_id TEXT, is_first_verification BOOLEAN DEFAULT FALSE, is_rate_limited BOOLEAN DEFAULT FALSE, last_activity INTEGER, activity_count INTEGER DEFAULT 0)',
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

async function calculateSessionExpiry(chatId, env) {
  const nowSeconds = Math.floor(Date.now() / 1000);
  const userState = await env.D1.prepare('SELECT activity_count FROM user_states WHERE chat_id = ?')
    .bind(chatId)
    .first();
  
  const activityCount = userState ? userState.activity_count : 0;
  const baseExpiry = 3600; // 1 小时
  const extraHours = Math.min(Math.floor(activityCount / 10), 3);
  return nowSeconds + baseExpiry + (extraHours * 3600);
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
  const messageId = message.message_id;

  await updateUserActivity(chatId, env);

  if (chatId === GROUP_ID) {
    const topicId = message.message_thread_id;
    if (topicId) {
      const privateChatId = await getPrivateChatId(topicId, env);
      if (privateChatId) {
        if (text.startsWith('/block') || text.startsWith('/unblock') || text.startsWith('/checkblock')) {
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

  const verificationState = await env.D1.prepare('SELECT is_verified, verified_expiry FROM user_states WHERE chat_id = ?')
    .bind(chatId)
    .first();
  let isVerified = verificationState ? verificationState.is_verified : false;
  const verifiedExpiry = verificationState ? verificationState.verified_expiry : null;
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
  // 清理旧的验证码状态
  console.log(`Cleaning old verification state for chatId ${chatId}`);
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
          message_id: lastVerificationMessageId,
        }),
      });
      console.log(`Deleted old verification message ${lastVerificationMessageId} for chatId ${chatId}`);
    } catch (error) {
      console.error("Error deleting old verification message:", error);
    }
    await env.D1.prepare('UPDATE user_states SET last_verification_message_id = NULL WHERE chat_id = ?')
      .bind(chatId)
      .run();
  }

  await sendVerification(chatId, env);
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
    callback_data: `verify_${chatId}_${option}_${option === correctResult ? 'correct' : 'wrong'}`,
  }));

  const question = `请计算：${num1} ${operation} ${num2} = ?（点击下方按钮完成验证）`;
  const nowSeconds = Math.floor(Date.now() / 1000);
  const codeExpiry = nowSeconds + 600; // 延长到 10 分钟
  console.log(`Generating verification for chatId ${chatId}: code=${correctResult}, expiry=${codeExpiry}, now=${nowSeconds}`);

  // 插入验证码
  let insertResult;
  try {
    insertResult = await env.D1.prepare('INSERT OR REPLACE INTO user_states (chat_id, verification_code, code_expiry) VALUES (?, ?, ?)')
      .bind(chatId, correctResult.toString(), codeExpiry)
      .run();
    console.log('Insert verification code result:', insertResult);
  } catch (error) {
    console.error(`Failed to insert verification code for chatId ${chatId}:`, error);
    await sendMessageToUser(chatId, '生成验证码失败，请稍后重试。', env);
    return;
  }

  // 验证是否成功写入
  const verificationCheck = await env.D1.prepare('SELECT verification_code, code_expiry FROM user_states WHERE chat_id = ?')
    .bind(chatId)
    .first();
  if (!verificationCheck || verificationCheck.verification_code !== correctResult.toString() || verificationCheck.code_expiry !== codeExpiry) {
    console.error(`Failed to store verification code for chatId ${chatId}:`, verificationCheck);
    await sendMessageToUser(chatId, '存储验证码失败，请稍后重试。', env);
    return;
  }
  console.log(`Verification code stored successfully for chatId ${chatId}:`, verificationCheck);

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
    const updateResult = await env.D1.prepare('UPDATE user_states SET last_verification_message_id = ? WHERE chat_id = ?')
      .bind(data.result.message_id.toString(), chatId)
      .run();
    console.log('Update last_verification_message_id result:', updateResult);
  } catch (error) {
    console.error("Error sending verification message:", error);
    await sendMessageToUser(chatId, '发送验证码失败，请稍后重试。', env);
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
  const messageId = callbackQuery.message.message_id;

  await updateUserActivity(chatId, env);

  if (!data.startsWith('verify_')) return;

  const [, userChatId, selectedAnswer, result] = data.split('_');
  if (userChatId !== chatId) return;

  const verificationState = await env.D1.prepare('SELECT verification_code, code_expiry FROM user_states WHERE chat_id = ?')
    .bind(chatId)
    .first();
  const storedCode = verificationState ? verificationState.verification_code : null;
  const codeExpiry = verificationState ? verificationState.code_expiry : null;
  const nowSeconds = Math.floor(Date.now() / 1000);
  console.log(`Checking verification for chatId ${chatId}: storedCode=${storedCode}, codeExpiry=${codeExpiry}, nowSeconds=${nowSeconds}`);

  if (!storedCode || (codeExpiry && nowSeconds > codeExpiry)) {
    console.log(`Verification expired or not found for chatId ${chatId}`);
    await sendMessageToUser(chatId, '验证码已过期，请重新发送消息以获取新验证码。', env);
    await handleVerification(chatId, messageId, env); // 重新生成验证码
    return;
  }

  if (result === 'correct') {
    const verifiedExpiry = await calculateSessionExpiry(chatId, env);
    const updateResult = await env.D1.prepare('UPDATE user_states SET is_verified = ?, verified_expiry = ?, verification_code = NULL, code_expiry = NULL, last_verification_message_id = NULL WHERE chat_id = ?')
      .bind(true, verifiedExpiry, chatId)
      .run();
    console.log('Update verification status result:', updateResult);

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
  } else {
    await sendMessageToUser(chatId, '验证失败，请重新尝试。', env);
    await handleVerification(chatId, messageId, env);
  }

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

  await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/answerCallbackQuery`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      callback_query_id: callbackQuery.id,
    }),
  });
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
