# Kafka User Activity Stream

A minimal Node.js learning project that demonstrates the basic Kafka flow:

```
Client → Express API (producer) → Kafka topic: user-events → Node.js consumer → MongoDB Atlas
```

Built to learn Kafka fundamentals — not a production application.

## Stack

- Node.js (JavaScript, no TypeScript)
- Express
- [KafkaJS](https://kafka.js.org/)
- Local Apache Kafka 4.x (KRaft mode, no ZooKeeper, plaintext `localhost:9092`)
- MongoDB Atlas + Mongoose
- dotenv

## Project structure

```
kafka-user-activity/
├── server.js            # Express API + producer startup
├── consumer.js          # Kafka consumer -> MongoDB (separate process)
├── config/
│   ├── kafka.js         # KafkaJS client, reads .env
│   └── db.js            # Mongoose connection
├── models/
│   └── Event.js         # Event schema (userId, type, data, timestamp)
├── services/
│   └── producer.js      # connect + publish helpers
├── package.json
├── .env.example         # copy to .env and fill in
└── .gitignore
```

## Setup

Prerequisites: local Kafka 4.x running in KRaft mode on `localhost:9092`, with the topic `user-events` created.

```bash
npm install
cp .env.example .env
```

`.env` — Kafka is local (no SSL, no username/password); MongoDB stays in Atlas (Atlas -> Connect -> Drivers gives the URI):

```env
KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC=user-events
MONGODB_URI=mongodb+srv://user:password@cluster0.xxxxx.mongodb.net/user-events
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
MongoDB connected
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
Saved to MongoDB: product_view by u101
```

Missing `userId` or `type` returns HTTP 400.

Query what was saved:

```js
db.events.find({ userId: "u101" })
```

## How it works

1. `POST /events` validates `userId` and `type`, then builds an event object with a `timestamp`.
2. `services/producer.js` publishes it to the `user-events` topic as JSON, using `userId` as the message key (same user -> same partition -> ordered). The API never writes to MongoDB.
3. `consumer.js` connects to MongoDB first, runs in the `activity-consumer-group`, and reads messages.
4. For each message: parse -> log -> `Event.create()` -> log the save. The message is only treated as processed after the save succeeds; if the save throws, the consumer fails loudly instead of pretending it worked.
5. Kafka is the buffer between the API and the database — the API and the DB never talk to each other directly.

## Concepts used here

- **Broker** — the Kafka server storing and serving the data (your local broker at `localhost:9092`, KRaft mode).
- **Topic** — the named log everyone reads/writes: `user-events`.
- **Producer** — the client publishing events (`server.js` + `services/producer.js`).
- **Consumer** — the client reading events (`consumer.js`).
- **Consumer group** — `activity-consumer-group`; members share the topic so each message is handled once per group.
- **Partition** — a topic is split into ordered partitions for parallelism; the message key decides the partition.
- **Offset** — a message's index inside its partition; the group tracks it so reads can resume and be replayed.

## Troubleshooting

- `KAFKA_BROKERS is missing` — you forgot to create/fill `.env`.
- `MONGODB_URI is missing` — same, add the Atlas connection string to `.env`.
- `Connection error ... ECONNREFUSED localhost:9092` — your local Kafka broker is not running (or not on 9092).
- `Client network socket disconnected before secure TLS connection was established` — your `.env` still has Confluent Cloud values/credentials; local Kafka is plaintext, so use `localhost:9092` with empty `KAFKA_USERNAME`/`KAFKA_PASSWORD`.
- `UnknownTopicOrPartition` — create the topic first: `kafka-topics.sh --create --topic user-events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1`.
- `MongooseServerSelectionError` — bad Atlas URI, or your IP is not allowed in Atlas Network Access.
- `Consumer error: ...` after `Saved` was never logged — the MongoDB save failed, so the message was not marked processed. Fix the DB and restart the consumer; it resumes from the last committed offset.
- KafkaJS partitioner warning (`switched default partitioner`) — harmless; it only tells you KafkaJS v2 partitions differently from v1.
- Consumer prints nothing — events sent before it subscribed are only seen if `fromBeginning` is true (it is), otherwise start the consumer first and send new events.
