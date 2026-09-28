require('dotenv').config();
const mongoose = require('mongoose');

async function connectDb() {
  const uri = process.env.MONGODB_URI;
  if (!uri) {
    console.error('MONGODB_URI is missing. Add it to .env.');
    process.exit(1);
  }

  await mongoose.connect(uri);
  console.log('MongoDB connected');
}

module.exports = { connectDb };
