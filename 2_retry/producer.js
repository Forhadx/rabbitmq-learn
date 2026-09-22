const { connectConfirm, publish } = require("./rabbitmq");

async function sendMail(data) {
  const { connection, channel } = await connectConfirm();

  publish(channel, data);

  await channel.waitForConfirms(); // wait until broker confirms receipt
  await channel.close();
  await connection.close();
}

const mail = {
  to: "user@example.com",
  subject: "Hello from RabbitMQ",
  text: "This is a test message",
};

sendMail(mail).catch(console.error);
