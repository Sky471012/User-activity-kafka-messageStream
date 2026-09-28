require('dotenv').config();
const { kafka, topic, consumerGroup } = require('./config/kafka');
const { connectDb } = require('./config/db');
const Event = require('./models/Event');

async function run() {
  await connectDb();

  const consumer = kafka.consumer({ groupId: consumerGroup });

  await consumer.connect();
  console.log('Consumer connected');

  await consumer.subscribe({ topic, fromBeginning: true });
  console.log(`Subscribed to topic: ${topic}`);

  await consumer.run({
    eachMessage: async ({ message }) => {
      const event = JSON.parse(message.value.toString());

      console.log('Received event');
      console.log(`User: ${event.userId}`);
      console.log(`Type: ${event.type}`);
      console.log(`Data: ${JSON.stringify(event.data)}`);
      console.log(`Timestamp: ${event.timestamp}`);

      await Event.create(event);
      console.log(`Saved to MongoDB: ${event.type} by ${event.userId}`);
    },
  });
}

run().catch((err) => {
  console.error('Consumer error:', err.message);
  process.exit(1);
});
