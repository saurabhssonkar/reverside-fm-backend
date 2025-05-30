import { Kafka,Partitioners, CompressionTypes } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'video-uploader',
  brokers: ['localhost:9092'],
   maxInFlightRequests: 1,
            compression: CompressionTypes.GZIP, // Add compression
            retry: { retries: 3 },
            // Increase message size limits:
            produceMaxBytesPerPartition: 10485760, // 10MB
            produceBufferMaxSize: 10485760 // 10MB
});
const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner,
  compression: CompressionTypes.GZIP,  // Reduces message size
  maxInFlightRequests: 1,
  retry: {
    retries: 3,
    maxRetryTime: 30000
  }
});

export async function sendToKafka(topic, message) {
  console.log("message", message)
  await producer.connect();
  await producer.send({
    topic,
    messages: [{ value: JSON.stringify(message) }]
  });
}
