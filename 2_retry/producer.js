const amqp = require("amqplib");
require("dotenv").config();

async function sendMail() {
  try {
    let connection = await amqp.connect(process.env.RABBITMQ_URL);
    console.log("RabbitMQ connected");
    let channel = await connection.createConfirmChannel();

    const exchange = "mail_exchange";
    const routingKey = "send_mail";
    const queueName = "mail_queue";

    let msg = {
      to: "ASS@gmail.com",
      subject: "Hello from RabbitMQ",
      text: "This is a test message",
    };

    await channel.assertExchange(exchange, "direct", {
      durable: true,
    });
    await channel.assertQueue(queueName, { durable: true });
    await channel.bindQueue(queueName, exchange, routingKey);

    channel.publish(exchange, routingKey, Buffer.from(JSON.stringify(msg)), {
      persistent: true,
    });
    console.log(`[x] Sent ${msg}`);

    
    await channel.waitForConfirms(); 
    await channel.close();
    await connection.close();

  } catch (err) {
    console.log(err);
  }
}

sendMail();
