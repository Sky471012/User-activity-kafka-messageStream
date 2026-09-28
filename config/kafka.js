require('dotenv').config();
const { Kafka } = require('kafkajs');

const brokers = (process.env.KAFKA_BROKERS || '')
  .split(',')
  .map((broker) => broker.trim())
  .filter(Boolean);

if (brokers.length === 0) {
  console.error('KAFKA_BROKERS is missing. Copy .env.example to .env and fill it in.');
  process.exit(1);
}

const kafka = new Kafka({
  clientId: 'user-activity-app',
  brokers,
  ssl: true,
  sasl: {
    mechanism: 'plain',
    username: process.env.KAFKA_USERNAME,
    password: process.env.KAFKA_PASSWORD,
  },
});

module.exports = {
  kafka,
  topic: process.env.KAFKA_TOPIC || 'user-events',
  consumerGroup: 'activity-consumer-group',
};
