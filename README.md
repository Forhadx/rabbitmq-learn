# RabbitMQ Producer & Consumer

Simple and runnable RabbitMQ producer and consumer examples using Node.js.

## Prerequisites

1. **Start RabbitMQ using Docker Compose**:
   ```bash
   docker compose up -d
   # or using npm script
   npm run docker:up
   ```

2. **Install dependencies**:
   ```bash
   npm install
   ```

## Running the Examples

### 1. Start the Consumer (first)

The consumer listens for messages continuously:

```bash
node consumer.js
```

You should see:

```
Connecting to RabbitMQ...
[*] Waiting for messages in 'my_queue'. To exit press CTRL+C
```

### 2. Send Messages (in another terminal)

Run the producer to send a message:

```bash
node producer.js
```

You should see in the producer terminal:

```
Connecting to RabbitMQ...
[x] Sent: { id: 1673..., text: 'Hello from RabbitMQ!', ... }
Message sent successfully!
```

And in the consumer terminal:

```
[x] Received: {"id":1673...,"text":"Hello from RabbitMQ!",...}
[→] Processing message: { id: 1673..., ... }
[✓] Processing complete for: 1673...
[✓] Message processed and acknowledged
```

### 3. View Queue Messages (without consuming)

Check all pending messages in the queue:

```bash
npm run view-queue
```

This will show you all messages waiting in the queue without removing them. Perfect for debugging!

### 4. Stop the Consumer

Press `CTRL+C` in the consumer terminal to gracefully shut down.

## How It Works

### Producer (`producer.js`)

1. Connects to RabbitMQ
2. Creates/asserts the queue exists
3. Sends a message to the queue
4. Closes the connection
5. Exits

### Consumer (`consumer.js`)

1. Connects to RabbitMQ
2. Creates/asserts the queue exists
3. Continuously listens for messages
4. Processes each message
5. Acknowledges successful processing (or requeues on error)
6. Stays running until manually stopped

## Key Features

- **Durable Queue**: Queue survives RabbitMQ restarts
- **Persistent Messages**: Messages survive RabbitMQ restarts
- **Manual Acknowledgment**: Messages are only removed after successful processing
- **Prefetch**: Consumer processes one message at a time
- **Error Handling**: Failed messages are requeued for retry
- **Graceful Shutdown**: Clean connection closing

## Customization

To send custom messages, modify the `message` object in `producer.js`:

```javascript
const message = {
  id: Date.now(),
  text: "Your custom message",
  data: {
    /* your data */
  },
};
```

To change processing logic, modify the `processMessage` function in `consumer.js`.

## RabbitMQ Management UI

If using RabbitMQ with management plugin, access the UI at:

- URL: http://localhost:15672
- Username: `guest`
- Password: `guest`

You can view queues, messages, connections, and more.
