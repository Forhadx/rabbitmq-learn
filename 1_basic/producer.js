const amqp = require("amqplib");
require("dotenv").config();

async function sendMail() {
  try {
    let connection = await amqp.connect(process.env.RABBITMQ_URL);
    console.log("RabbitMQ connected");
    let channel = await connection.createChannel();

    const exchange = "mail_exchange";
    const routingKey = "send_mail";
    const queueName = "mail_queue";

    let msg = {
      to: "ASS@gmail.com",
      subject: "Hello from RabbitMQ",
      text: "This is a test message",
    };

    // direct = exchange type, durable = false(if queue is deleted then queue will also deleted), true(if queue is deleted then queue will not be deleted)
    await channel.assertExchange(exchange, "direct", {
      durable: false,
    }); // make exchange
    await channel.assertQueue(queueName, { durable: false }); // make queue
    await channel.bindQueue(queueName, exchange, routingKey); // make binding between exchange and queue with routing key

    channel.publish(exchange, routingKey, Buffer.from(JSON.stringify(msg))); // publish the msg
    console.log(`[x] Sent ${msg}`);

    setTimeout(() => {
      connection.close();
      process.exit(0);
    }, 500);
  } catch (err) {
    console.log(err);
  }
}

sendMail();
