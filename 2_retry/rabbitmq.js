const amqp = require("amqplib");
require("dotenv").config();

const RABBITMQ_URL = process.env.RABBITMQ_URL;

const EXCHANGE    = "mail_exchange";
const ROUTING_KEY = "send_mail";
const QUEUE       = "mail_queue";
const DEAD_QUEUE  = "mail_dead_letter_queue";
const MAX_RETRIES = 3;

async function setup(channel) {
  await channel.assertExchange(EXCHANGE, "direct", { durable: true });
  await channel.assertQueue(QUEUE, { durable: true });
  await channel.assertQueue(DEAD_QUEUE, { durable: true });
  await channel.bindQueue(QUEUE, EXCHANGE, ROUTING_KEY);
}

async function connect() {
  const connection = await amqp.connect(RABBITMQ_URL);
  const channel = await connection.createChannel();
  await setup(channel);
  channel.prefetch(1);
  return { connection, channel };
}

async function connectConfirm() {
  const connection = await amqp.connect(RABBITMQ_URL);
  const channel = await connection.createConfirmChannel();
  await setup(channel);
  return { connection, channel };
}

function publish(channel, content) {
  const ok = channel.publish(EXCHANGE, ROUTING_KEY, Buffer.from(JSON.stringify(content)), {
    persistent: true,
  });
  if (!ok) throw new Error("Publish failed: channel buffer is full");
}

function retry(channel, content, retryCount) {
  channel.publish(EXCHANGE, ROUTING_KEY, Buffer.from(JSON.stringify(content)), {
    persistent: true,
    headers: { "x-retry-count": retryCount + 1 },
  });
}

function getRetryCount(headers) {
  const raw = headers?.["x-retry-count"];
  // amqplib returns header values as { value, type } objects
  return typeof raw === "object" ? raw?.value ?? 0 : raw ?? 0;
}

function sendToDeadQueue(channel, content, retryCount, reason) {
  channel.sendToQueue(DEAD_QUEUE, Buffer.from(JSON.stringify(content)), {
    persistent: true,
    headers: { "x-retry-count": retryCount, "x-failed-reason": reason },
  });
}

module.exports = { connect, connectConfirm, publish, retry, sendToDeadQueue, getRetryCount, MAX_RETRIES, QUEUE };
