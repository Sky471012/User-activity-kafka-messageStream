require('dotenv').config();
const express = require('express');
const { connectProducer, publishEvent } = require('./services/producer');

const app = express();
app.use(express.json());

app.post('/events', async (req, res) => {
  const { userId, type, data } = req.body || {};

  if (!userId || !type) {
    return res.status(400).json({
      success: false,
      error: 'userId and type are required',
    });
  }

  const event = {
    userId,
    type,
    data: data || {},
    timestamp: new Date().toISOString(),
  };

  try {
    await publishEvent(event);
    return res.status(201).json({
      success: true,
      message: 'Event published',
      event,
    });
  } catch (err) {
    console.error('Failed to publish event:', err.message);
    return res.status(500).json({
      success: false,
      error: 'Failed to publish event',
    });
  }
});

const PORT = process.env.PORT || 3000;

connectProducer()
  .then(() => {
    app.listen(PORT, () => {
      console.log(`Server listening on http://localhost:${PORT}`);
    });
  })
  .catch((err) => {
    console.error('Could not connect producer to Kafka:', err.message);
    process.exit(1);
  });
