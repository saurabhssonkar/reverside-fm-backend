// kafkaManager.mjs or kafkaManager.js (with "type": "module" in package.json)

import { Kafka, Partitioners, CompressionTypes } from 'kafkajs';
import CONFIG from '../CONFIG.js';
import logger from '../logger.js';

const kafka = new Kafka({
  clientId: CONFIG.kafka.clientId,
  brokers: CONFIG.kafka.brokers,
  maxInFlightRequests: 1,
  compression: CompressionTypes.GZIP,
  retry: { retries: 3 },
  produceMaxBytesPerPartition: 10485760,
  produceBufferMaxSize: 10485760,
});

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner,
  compression: CompressionTypes.GZIP,
  maxInFlightRequests: 1,
  retry: {
    retries: 3,
    maxRetryTime: 30000,
  },
});

const consumer = kafka.consumer({
  groupId: 'video-processor-group',
  maxBytes: 10485760,
  maxBytesPerPartition: 10485760,
  maxWaitTimeInMs: 5000,
});

const admin = kafka.admin();

async function initializeKafka() {
  try {
    await producer.connect();
    await consumer.connect();
    await admin.connect();
    logger.info('Kafka connections established successfully');
  } catch (error) {
    logger.error(`Kafka initialization failed: ${error.message}`);
    throw error;
  }
}

async function createTopicForCamera(cameraId) {
  const topicName = `camera-${cameraId}-stream`;
  try {
    const existingTopics = await admin.listTopics();
    if (!existingTopics.includes(topicName)) {
      await admin.createTopics({
        topics: [{
          topic: topicName,
          numPartitions: 1,
          replicationFactor: 1,
        }],
      });
      logger.info(`Topic created: ${topicName}`);
    }
    return topicName;
  } catch (error) {
    logger.error(`Failed to create topic for camera ${cameraId}: ${error.message}`);
    throw error;
  }
}

async function sendVideoChunk(cameraId, chunkData) {
  const topicName = `camera-stream`;
  try {
    await producer.send({
      topic: topicName,
      messages: [{
        key: `${cameraId}-${Date.now()}`,
        value: JSON.stringify({
          cameraId,
          timestamp: Date.now(),
          chunkData: chunkData.toString('base64'),
          chunkSize: chunkData.length,
        }),
      }],
    });
    logger.info(`Video chunk sent to topic: ${topicName}`);
  } catch (error) {
    logger.error(`Failed to send chunk for camera ${cameraId}: ${error.message}`);
    throw error;
  }
}

async function deleteTopicForCamera(cameraId) {
  const topicName = `camera-${cameraId}-stream`;
  try {
    // Uncomment if deletion is needed
    // await admin.deleteTopics({ topics: [topicName] });
    // logger.info(`Topic deleted: ${topicName}`);
  } catch (error) {
    logger.error(`Failed to delete topic ${topicName}: ${error.message}`);
  }
}

// Export as individual functions
export  {
  initializeKafka,
  createTopicForCamera,
  sendVideoChunk,
  deleteTopicForCamera,
  producer,
  consumer,
  admin,
};
