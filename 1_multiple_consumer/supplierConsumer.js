const amqp = require("amqplib");
require("dotenv").config();

async function sendMail() {
  try {
    let connection = await amqp.connect(process.env.RABBITMQ_URL);
    console.log("RabbitMQ connected");
    let channel = await connection.createChannel();

    const supplierQueueName = "supplier_queue";

    await channel.assertQueue(supplierQueueName, { durable: false });

    await channel.consume(supplierQueueName, (msg) => {
      if (msg !== null) {
        console.log("supplier receive msg: ", JSON.parse(msg.content));
        channel.ack(msg); 
      }
    });
  } catch (err) {
    console.log(err);
  }
}

sendMail();
