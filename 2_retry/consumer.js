const amqp = require("amqplib");
require("dotenv").config();

const MAX_RETRIES = 3;
const RABBITMQ_URL = process.env.RABBITMQ_URL;

const EXCHANGE      = "mail_exchange";
const ROUTING_KEY   = "send_mail";
const QUEUE         = "mail_queue";
const DEAD_QUEUE    = "mail_dead_letter_queue";

async function processMessage(content) {
  // Simulate a processing error
  throw new Error("Something went wrong!");

  // On success you would do real work here, e.g:
  // await sendEmail(content);
}

async function main() {
  const connection = await amqp.connect(RABBITMQ_URL);
  const channel    = await connection.createChannel();

  // Set up exchange and queues
  await channel.assertExchange(EXCHANGE, "direct", { durable: true });
  await channel.assertQueue(QUEUE,       { durable: true });
  await channel.assertQueue(DEAD_QUEUE,  { durable: true });
  await channel.bindQueue(QUEUE, EXCHANGE, ROUTING_KEY);

  console.log(`Waiting for messages... (max retries: ${MAX_RETRIES})`);

  channel.consume(QUEUE, async (msg) => {
    if (!msg) return;

    const content    = JSON.parse(msg.content);
    const retryCount = msg.properties.headers?.["x-retry-count"] ?? 0;

    console.log(`\nReceived (attempt ${retryCount + 1}):`, content);

    try {
      await processMessage(content);

      channel.ack(msg);
      console.log("✓ Message processed successfully");

    } catch (err) {
      console.error(`✗ Error: ${err.message}`);
      channel.nack(msg, false, false); // remove from queue (no requeue)

      const hasRetriesLeft = retryCount < MAX_RETRIES - 1;

      if (hasRetriesLeft) {
        // Re-publish with incremented retry count
        channel.publish(EXCHANGE, ROUTING_KEY, msg.content, {
          persistent: true,
          headers: { "x-retry-count": retryCount + 1 },
        });
        console.log(`↺ Retrying... (${retryCount + 1}/${MAX_RETRIES})`);

      } else {
        // All retries exhausted → send to dead-letter queue
        channel.sendToQueue(DEAD_QUEUE, msg.content, {
          persistent: true,
          headers: { "x-retry-count": retryCount, "x-failed-reason": err.message },
        });
        console.log("✗ Max retries reached. Moved to dead-letter queue.");
      }
    }
  });
}

main().catch(console.error);
