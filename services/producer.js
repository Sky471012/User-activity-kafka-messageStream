const { kafka, topic } = require('../config/kafka');

const producer = kafka.producer();

async function connectProducer() {
  await producer.connect();
  console.log('Kafka producer connected');
}

async function publishEvent(event) {
  await producer.send({
    topic,
    messages: [
      {
        key: event.userId,
        value: JSON.stringify(event),
      },
    ],
  });
  console.log(`Event published: type=${event.type} userId=${event.userId}`);
}

module.exports = { connectProducer, publishEvent };
