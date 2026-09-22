const amqp = require("amqplib");
require("dotenv").config();

async function sendMail() {
  try {
    let connection = await amqp.connect(process.env.RABBITMQ_URL);
    console.log("RabbitMQ connected");
    let channel = await connection.createChannel();

    const queueName = "mail_queue";

    await channel.assertQueue(queueName, { durable: false }); // make/get queue

    await channel.consume(queueName, (msg) => {
      if (msg !== null) {
        console.log("receive msg: ", JSON.parse(msg.content));
        channel.ack(msg); // message acknowledgment mean i get the msg to inform the queue
      }
    });
  } catch (err) {
    console.log(err);
  }
}

sendMail();
