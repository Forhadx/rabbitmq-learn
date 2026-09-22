const amqp = require("amqplib");
require("dotenv").config();

async function sendMail() {
  try {
    let connection = await amqp.connect(process.env.RABBITMQ_URL);
    console.log("RabbitMQ connected");
    let channel = await connection.createChannel();

    const exchange = "mail_exchange";

    const supplierRoutingKey = "supplier_mail";
    const supplierQueueName = "supplier_queue";

    const customerRoutingKey = "customer_mail";
    const customerQueueName = "customer_queue";

    let msg = {
      to: "ASS@gmail.com",
      subject: "Hello from RabbitMQ",
      text: "This is a test message",
    };

    await channel.assertExchange(exchange, "direct", {
      durable: false,
    });

    // for supplier
    await channel.assertQueue(supplierQueueName, { durable: false });
    await channel.bindQueue(supplierQueueName, exchange, supplierRoutingKey);

    // customer
    await channel.assertQueue(customerQueueName, { durable: false });
    await channel.bindQueue(customerQueueName, exchange, customerRoutingKey);

    // publish the msg for supplier
    channel.publish(
      exchange,
      supplierRoutingKey,
      Buffer.from(JSON.stringify(msg))
    );
    console.log(`[x] Sent supplier msg ${msg}`);

    // publish the msg for customer
    channel.publish(
      exchange,
      customerRoutingKey,
      Buffer.from(JSON.stringify(msg))
    );
    console.log(`[x] Sent customer msg ${msg}`);

    setTimeout(() => {
      connection.close();
      process.exit(0);
    }, 500);
  } catch (err) {
    console.log(err);
  }
}

sendMail();
