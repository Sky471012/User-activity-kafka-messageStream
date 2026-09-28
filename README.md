# Kafka User Activity Stream

A minimal Node.js learning project that demonstrates the basic Kafka flow:

```
Express API (producer)  →  Kafka topic: user-events  →  Node.js consumer
```

Built to learn Kafka fundamentals — not a production application.

## Stack

- Node.js (JavaScript, no TypeScript)
- Express
- [KafkaJS](https://kafka.js.org/)
- Confluent Cloud (managed Kafka, SASL/SSL)
- dotenv

## Project structure

```
kafka-user-activity/
├── server.js            # Express API + producer startup
├── consumer.js          # Kafka consumer (separate process)
├── config/
│   └── kafka.js         # KafkaJS client, reads .env
├── services/
│   └── producer.js      # connect + publish helpers
├── package.json
├── .env.example         # copy to .env and fill in
└── .gitignore
```

## Setup

```bash
npm install
cp .env.example .env
```

Fill `.env` with your Confluent Cloud values (Cluster -> Topics -> your cluster gives the broker address; API Keys gives the username/password):

```env
KAFKA_BROKERS=pkc-xxxxx.us-east-1.aws.confluent.cloud:9092
KAFKA_USERNAME=your-api-key
KAFKA_PASSWORD=your-api-secret
KAFKA_TOPIC=user-events
PORT=3000
```

`.env` is gitignored. Never commit real credentials.

## Run

Terminal 1 — start the API (producer):

```bash
npm start
```

Expected logs:

```
Kafka producer connected
Server listening on http://localhost:3000
```

Terminal 2 — start the consumer:

```bash
npm run consumer
```

Expected logs:

```
Consumer connected
Subscribed to topic: user-events
```

## Send an event

```bash
curl -X POST http://localhost:3000/events \
  -H "Content-Type: application/json" \
  -d '{"userId":"u101","type":"product_view","data":{"productId":"p501"}}'
```

Success response (HTTP 201):

```json
{
  "success": true,
  "message": "Event published",
  "event": {
    "userId": "u101",
    "type": "product_view",
    "data": { "productId": "p501" },
    "timestamp": "2026-09-28T10:15:00.000Z"
  }
}
```

The consumer prints:

```
Received event
User: u101
Type: product_view
Data: {"productId":"p501"}
Timestamp: 2026-09-28T10:15:00.000Z
```

Missing `userId` or `type` returns HTTP 400.

## How it works

1. `POST /events` validates `userId` and `type`, then builds an event object with a `timestamp`.
2. `services/producer.js` publishes it to the `user-events` topic as JSON, using `userId` as the message key (same user -> same partition -> ordered).
3. `consumer.js` runs in the `activity-consumer-group`, reads messages, and prints them.
4. Nothing is stored anywhere — Kafka is the buffer between the two processes.

## Concepts used here

- **Broker** — the Kafka server storing and serving the data (your Confluent Cloud cluster).
- **Topic** — the named log everyone reads/writes: `user-events`.
- **Producer** — the client publishing events (`server.js` + `services/producer.js`).
- **Consumer** — the client reading events (`consumer.js`).
- **Consumer group** — `activity-consumer-group`; members share the topic so each message is handled once per group.
- **Partition** — a topic is split into ordered partitions for parallelism; the message key decides the partition.
- **Offset** — a message's index inside its partition; the group tracks it so reads can resume and be replayed.

## Troubleshooting

- `KAFKA_BROKERS is missing` — you forgot to create/fill `.env`.
- `Connection error ... ENOTFOUND` — broker address typo or wrong network.
- `SASL authentication failed` — wrong API key/secret, or key belongs to a different cluster.
- Consumer prints nothing — events sent before it subscribed are only seen if `fromBeginning` is true (it is), otherwise start the consumer first and send new events.
