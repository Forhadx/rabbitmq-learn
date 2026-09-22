const { connect, retry, sendToDeadQueue, getRetryCount, MAX_RETRIES, QUEUE } = require("./rabbitmq");

async function processMessage(content) {
  // Replace with real logic (e.g. send email, write to DB)
  throw new Error("Something went wrong!");
}

async function consumeMail() {
  const { channel } = await connect(); // regular channel, no confirms needed

  channel.consume(QUEUE, async (msg) => {
    if (!msg) return;

    const content    = JSON.parse(msg.content);
    const retryCount = getRetryCount(msg.properties.headers);

    try {
      await processMessage(content);
      channel.ack(msg);
    } catch (err) {
      channel.nack(msg, false, false);

      if (retryCount < MAX_RETRIES - 1) {
        retry(channel, content, retryCount);
      } else {
        sendToDeadQueue(channel, content, retryCount, err.message);
      }
    }
  });
}

consumeMail().catch(console.error);
