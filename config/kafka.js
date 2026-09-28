require('dotenv').config();
const { Kafka } = require('kafkajs');

const brokers = (process.env.KAFKA_BROKERS || '')
  .split(',')
  .map((broker) => broker.trim())
  .filter(Boolean);

if (brokers.length === 0) {
  console.error('KAFKA_BROKERS is missing. Copy .env.example to .env.');
  process.exit(1);
}

const kafka = new Kafka({
  clientId: 'user-activity-app',
  brokers,
});

module.exports = {
  kafka,
  topic: process.env.KAFKA_TOPIC || 'user-events',
  consumerGroup: 'activity-consumer-group',
};
