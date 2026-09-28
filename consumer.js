require('dotenv').config();
const { kafka, topic, consumerGroup } = require('./config/kafka');
const { connectDb } = require('./config/db');
const Event = require('./models/Event');

async function run() {
  await connectDb();

  const groupId = process.env.CONSUMER_GROUP || consumerGroup;

  const consumer = kafka.consumer({ groupId });

  await consumer.connect();
  console.log('Consumer connected');
  console.log(`Consumer group: ${groupId}`);

  await consumer.subscribe({ topic, fromBeginning: true });
  console.log(`Subscribed to topic: ${topic}`);

  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      const event = JSON.parse(message.value.toString());

      console.log('Received event');
      console.log(`User: ${event.userId}`);
      console.log(`Type: ${event.type}`);
      console.log(`Data: ${JSON.stringify(event.data)}`);
      console.log(`Timestamp: ${event.timestamp}`);
      console.log(`Partition: ${partition} | User: ${event.userId} | Type: ${event.type}`);

      await Event.create(event);
      console.log(`Saved to MongoDB: ${event.type} by ${event.userId}\n`);
    },
  });
}

run().catch((err) => {
  console.error('Consumer error:', err.message);
  process.exit(1);
});
