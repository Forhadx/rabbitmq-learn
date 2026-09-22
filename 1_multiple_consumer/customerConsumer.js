const amqp = require("amqplib");
require("dotenv").config();

async function sendMail() {
  try {
    let connection = await amqp.connect(process.env.RABBITMQ_URL);
    console.log("RabbitMQ connected");
    let channel = await connection.createChannel();

    const customerQueueName = "customer_queue";

    await channel.assertQueue(customerQueueName, { durable: false });

    await channel.consume(customerQueueName, (msg) => {
      if (msg !== null) {
        console.log("customer receive msg: ", JSON.parse(msg.content));
        channel.ack(msg); 
      }
    });
  } catch (err) {
    console.log(err);
  }
}

sendMail();
