// MongoDB Database Helper Module
// This replaces Firestore with MongoDB operations

const { MongoClient, ObjectId } = require('mongodb');

class MongoDB {
  constructor(uri) {
    this.uri = uri;
    this.client = null;
    this.db = null;
    this.ready = false;
  }

  async connect() {
    try {
      this.client = new MongoClient(this.uri);
      await this.client.connect();
      this.db = this.client.db();
      this.ready = true;
      console.log('✅ MongoDB connected successfully');
      return this.db;
    } catch (err) {
      console.error('❌ MongoDB connection failed:', err);
      throw err;
    }
  }

  collection(name) {
    if (!this.ready) {
      throw new Error('Database not connected');
    }
    return this.db.collection(name);
  }

  // Firestore-like API wrappers
  FieldValue = {
    serverTimestamp: () => new Date(),
    arrayUnion: (...elements) => ({ $addToSet: { $each: elements } }),
    arrayRemove: (...elements) => ({ $pull: { $in: elements } }),
  };

  // Helper to convert Firestore-style queries to MongoDB
  static convertTimestamp(value) {
    if (value instanceof Date) return value;
    if (value && value._seconds) return new Date(value._seconds * 1000);
    if (value && value.toDate) return value.toDate();
    return value;
  }
}

module.exports = MongoDB;
