const mongoose = require('mongoose');

const eventSchema = new mongoose.Schema({
  userId: String,
  type: String,
  data: Object,
  timestamp: Date,
});

module.exports = mongoose.model('Event', eventSchema);
