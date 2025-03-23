// 从环境变量中读取配置
const BOT_TOKEN = typeof BOT_TOKEN_ENV !== 'undefined' ? BOT_TOKEN_ENV : 'YOUR_BOT_TOKEN'; // 从环境变量获取
const GROUP_ID = typeof GROUP_ID_ENV !== 'undefined' ? GROUP_ID_ENV : 'YOUR_GROUP_CHAT_ID'; // 从环境变量获取
const MAX_MESSAGES_PER_MINUTE = typeof MAX_MESSAGES_PER_MINUTE_ENV !== 'undefined' ? parseInt(MAX_MESSAGES_PER_MINUTE_ENV) : 40; // 消息频率限制，默认40条/分钟

addEventListener('fetch', event => {
  event.respondWith(handleRequest(event.request));
});

async function handleRequest(request) {
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

  // 如果消息来自后台群组（客服回复或管理员命令）
  if (chatId === GROUP_ID) {
    const topicId = message.message_thread_id;
    if (topicId) {
      const privateChatId = await getPrivateChatId(topicId);
      if (privateChatId) {
        // 检查是否为管理员命令
        if (text.startsWith('/block') || text.startsWith('/unblock') || text.startsWith('/checkblock')) {
          await handleAdminCommand(message, topicId, privateChatId);
          return;
        }
        // 普通客服回复
        await forwardMessageToPrivateChat(privateChatId, message);
      }
    }
    return;
  }

  // 检查用户是否被拉黑
  const isBlocked = await TOPIC_KV.get(`blocked-${chatId}`);
  if (isBlocked === 'true') {
    console.log(`User ${chatId} is blocked, ignoring message.`);
    return;
  }

  // 检查用户是否已通过验证
  const isVerified = await TOPIC_KV.get(`verified-${chatId}`);
  console.log(`User ${chatId} verification status: ${isVerified}`);
  if (!isVerified || isVerified !== 'true') {
    const messageContent = text || '非文本消息';
    await sendMessageToUser(chatId, `无法转发的信息：${messageContent}\n无法发送，请完成验证`);
    await handleVerification(chatId, messageId);
    return;
  }

  // 检查消息频率（防刷）
  if (await checkMessageRate(chatId)) {
    console.log(`User ${chatId} exceeded message rate limit, resetting verification.`);
    await TOPIC_KV.put(`verified-${chatId}`, 'false', { expirationTtl: 3600 }); // 确保状态持久化
    await TOPIC_KV.put(`rate-limited-${chatId}`, 'true'); // 标记为频率限制触发的验证
    const messageContent = text || '非文本消息';
    await sendMessageToUser(chatId, `无法转发的信息：${messageContent}\n信息过于频繁，请完成验证后发送信息`);
    await handleVerification(chatId, messageId);
    return;
  }

  // 处理客户消息
  if (text === '/start') {
    await TOPIC_KV.put(`first-verification-${chatId}`, 'true'); // 标记为首次验证
    const isVerifiedAgain = await TOPIC_KV.get(`verified-${chatId}`);
    if (isVerifiedAgain === 'true') {
      // 如果已经验证过，直接发送欢迎消息
      const successMessage = await getVerificationSuccessMessage();
      await sendMessageToUser(chatId, `${successMessage}\n你好，欢迎使用私聊机器人，现在发送信息吧！`);
    } else {
      await sendMessageToUser(chatId, "你好，欢迎使用私聊机器人，现在发送信息吧！");
      await handleVerification(chatId, messageId); // 触发验证
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

/**
 * 处理管理员命令
 */
async function handleAdminCommand(message, topicId, privateChatId) {
  const text = message.text;
  const senderId = message.from.id.toString();

  // 检查发送者是否为管理员
  const isAdmin = await checkIfAdmin(senderId);
  if (!isAdmin) {
    await sendMessageToTopic(topicId, '只有管理员可以使用此命令。');
    return;
  }

  if (text === '/block') {
    await TOPIC_KV.put(`blocked-${privateChatId}`, 'true');
    await sendMessageToTopic(topicId, `用户 ${privateChatId} 已被拉黑，消息将不再转发。`);
  } else if (text === '/unblock') {
    await TOPIC_KV.delete(`blocked-${privateChatId}`);
    await sendMessageToTopic(topicId, `用户 ${privateChatId} 已解除拉黑，消息将继续转发。`);
  } else if (text === '/checkblock') {
    const isBlocked = await TOPIC_KV.get(`blocked-${privateChatId}`);
    const status = isBlocked === 'true' ? '是' : '否';
    await sendMessageToTopic(topicId, `用户 ${privateChatId} 是否在黑名单中：${status}`);
  }
}

/**
 * 检查用户是否为管理员
 */
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

/**
 * 处理验证码逻辑（按钮式验证）
 */
async function handleVerification(chatId, messageId) {
  // 清理旧的验证码状态
  await TOPIC_KV.delete(`code-${chatId}`);
  const lastVerificationMessageId = await TOPIC_KV.get(`last-verification-${chatId}`);
  if (lastVerificationMessageId) {
    // 删除旧验证码消息（如果存在）
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
    await TOPIC_KV.delete(`last-verification-${chatId}`);
  }

  // 强制发送新验证码
  await sendVerification(chatId);
}

/**
 * 发送验证码（四个选项，仅显示括号内数字）
 */
async function sendVerification(chatId) {
  const num1 = Math.floor(Math.random() * 10);
  const num2 = Math.floor(Math.random() * 10);
  const operation = Math.random() > 0.5 ? '+' : '-';
  const correctResult = operation === '+' ? num1 + num2 : num1 - num2;

  // 生成四个选项（一个正确答案，三个错误答案）
  const options = new Set();
  options.add(correctResult);
  while (options.size < 4) {
    const wrongResult = correctResult + Math.floor(Math.random() * 5) - 2;
    if (wrongResult !== correctResult) {
      options.add(wrongResult);
    }
  }
  const optionArray = Array.from(options).sort(() => Math.random() - 0.5); // 随机排序

  // 创建按钮，仅显示括号内数字
  const buttons = optionArray.map((option) => ({
    text: `(${option})`,
    callback_data: `verify_${chatId}_${option}_${option === correctResult ? 'correct' : 'wrong'}`,
  }));

  const question = `请计算：${num1} ${operation} ${num2} = ?（点击下方按钮完成验证）`;
  await TOPIC_KV.put(`code-${chatId}`, correctResult.toString(), { expirationTtl: 300 }); // 5分钟有效期

  try {
    const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        chat_id: chatId,
        text: question,
        reply_markup: {
          inline_keyboard: [buttons], // 一行四个按钮
        },
      }),
    });
    const data = await response.json();
    if (!data.ok) {
      console.error(`Failed to send verification message: ${data.description}`);
      return;
    }
    // 存储验证码消息ID，以便后续删除
    await TOPIC_KV.put(`last-verification-${chatId}`, data.result.message_id.toString());
  } catch (error) {
    console.error("Error sending verification message:", error);
  }
}

/**
 * 获取验证成功提示消息（仅在首次验证时使用）
 */
async function getVerificationSuccessMessage() {
  try {
    const response = await fetch('https://raw.githubusercontent.com/iawooo/ctt/refs/heads/main/CFTeleTrans/start.md');
    if (!response.ok) {
      throw new Error(`Failed to fetch fraud.db: ${response.statusText}`);
    }
    const message = await response.text();
    const trimmedMessage = message.trim(); // 移除多余的换行或空格
    if (!trimmedMessage) {
      throw new Error('fraud.db content is empty');
    }
    return trimmedMessage;
  } catch (error) {
    console.error("Error fetching verification success message:", error);
    return '验证成功！您现在可以与客服聊天。'; // 备用提示
  }
}

/**
 * 获取子论坛通知内容
 */
async function getNotificationContent() {
  try {
    const response = await fetch('https://raw.githubusercontent.com/iawooo/ctt/refs/heads/main/CFTeleTrans/notification.md');
    if (!response.ok) {
      throw new Error(`Failed to fetch notification.txt: ${response.statusText}`);
    }
    const content = await response.text();
    const trimmedContent = content.trim(); // 移除多余的换行或空格
    if (!trimmedContent) {
      throw new Error('notification.txt content is empty');
    }
    return trimmedContent;
  } catch (error) {
    console.error("Error fetching notification content:", error);
    return ''; // 如果获取失败，返回空字符串
  }
}

/**
 * 处理按钮点击（验证结果）
 */
async function onCallbackQuery(callbackQuery) {
  const chatId = callbackQuery.message.chat.id.toString();
  const data = callbackQuery.data;
  const messageId = callbackQuery.message.message_id;

  if (!data.startsWith('verify_')) return;

  const [, userChatId, selectedAnswer, result] = data.split('_');
  if (userChatId !== chatId) return;

  const storedCode = await TOPIC_KV.get(`code-${chatId}`);
  if (!storedCode) {
    await sendMessageToUser(chatId, '验证码已过期，请重新发送消息以获取新验证码。');
    return;
  }

  if (result === 'correct') {
    // 确保验证状态持久化
    await TOPIC_KV.put(`verified-${chatId}`, 'true', { expirationTtl: 3600 }); // 设置1小时有效期
    await TOPIC_KV.delete(`code-${chatId}`);
    await TOPIC_KV.delete(`last-verification-${chatId}`);

    // 检查是否为首次验证（/start 触发）
    const isFirstVerification = await TOPIC_KV.get(`first-verification-${chatId}`);
    const isRateLimited = await TOPIC_KV.get(`rate-limited-${chatId}`);

    if (isFirstVerification === 'true') {
      const successMessage = await getVerificationSuccessMessage();
      await sendMessageToUser(chatId, `${successMessage}\n你好，欢迎使用私聊机器人！`);
      await TOPIC_KV.delete(`first-verification-${chatId}`); // 删除首次验证标记
    } else {
      await sendMessageToUser(chatId, '验证成功！请重新发送您的消息');
    }

    // 清除频率限制标记
    if (isRateLimited === 'true') {
      await TOPIC_KV.delete(`rate-limited-${chatId}`);
    }
  } else {
    await sendMessageToUser(chatId, '验证失败，请重新尝试。');
    await handleVerification(chatId, messageId); // 重新触发验证
  }

  // 删除验证码消息
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

  // 回复回调查询
  await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/answerCallbackQuery`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      callback_query_id: callbackQuery.id,
    }),
  });
}

/**
 * 检查消息频率（防刷）
 */
async function checkMessageRate(chatId) {
  const key = `rate-${chatId}`;
  const now = Date.now();
  const window = 60 * 1000; // 1分钟窗口

  const data = await TOPIC_KV.get(key, { type: 'json' }) || { count: 0, start: now };
  if (now - data.start > window) {
    data.count = 1;
    data.start = now;
  } else {
    data.count += 1;
  }

  await TOPIC_KV.put(key, JSON.stringify(data));
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
  const topicId = await TOPIC_KV.get(chatId);
  return topicId;
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

  // 获取 notification.txt 内容
  const notificationContent = await getNotificationContent();

  // 置顶消息中追加 notification.txt 内容
  const pinnedMessage = `昵称: ${nickname}\n用户名: ${userName}\nUserID: ${userId}\n发起时间: ${formattedTime}\n\n${notificationContent}`;
  const messageResponse = await sendMessageToTopic(topicId, pinnedMessage);
  const messageId = messageResponse.result.message_id;
  await pinMessage(topicId, messageId);

  return topicId;
}

async function saveTopicId(chatId, topicId) {
  await TOPIC_KV.put(chatId, topicId);
  await TOPIC_KV.put(topicId, chatId);
}

async function getPrivateChatId(topicId) {
  const privateChatId = await TOPIC_KV.get(topicId);
  return privateChatId;
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
  }
  throw new Error(`Failed to fetch ${url} after ${retries} retries`);
}

async function registerWebhook(request) {
  const webhookUrl = `${new URL(request.url).origin}/webhook`;
  const response = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/setWebhook`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ url: webhookUrl }),
  }).then(r => r.json());
  return new Response(response.ok ? 'Webhook set successfully' : JSON.stringify(response, null, 2));
}

async function unRegisterWebhook() {
  const response = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/setWebhook`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ url: '' }),
  }).then(r => r.json());
  return new Response(response.ok ? 'Webhook removed' : JSON.stringify(response, null, 2));
}
