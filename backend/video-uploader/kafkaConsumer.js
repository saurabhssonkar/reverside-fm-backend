import { Kafka } from 'kafkajs';
import AWS from 'aws-sdk';
// import redis from './redisClient'; // assume you’ve configured redis correctly

const s3 = new AWS.S3(); // properly configured with region, credentials

const kafka = new Kafka({
  clientId: 'video-consumer',
  brokers: ['localhost:9092'],
  requestTimeout: 30000, // Optional
  retry: {
    retries: 5
  }
});

const consumer = kafka.consumer({ groupId: 'video-group' });

export async function startKafkaConsumer() {
  try {
    await consumer.connect();
    console.log('✅ Kafka connected');

    await consumer.subscribe({ topic: 'video-chunks', fromBeginning: false });
    console.log('✅ Subscribed to topic');

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          console.log('📦 Kafka message:');

          const data = JSON.parse(message.value.toString());
          const { uploadId, key, partNumber, chunk } = data;
          // console.log("chunk",chunk)

          if (!uploadId || !key || !chunk || !Number.isInteger(partNumber)) {
            throw new Error('❌ Invalid message format');
          }

          const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
          
          console.log(`📤 ${uploadId} Uploading part ${partNumber} for key ${key}, chunk size: ${buffer.length},`);

          const uploadRes = await s3.uploadPart({
            Bucket: process.env.AWS_S3_BUCKET,
            Key: key,
            UploadId: uploadId,
            PartNumber:  parseInt(partNumber),
            Body: buffer
          }).promise();

          const partInfo = {
            ETag: uploadRes.ETag,
            PartNumber:  parseInt(partNumber), 
          };

        //   await redis.rpush(`${key}-parts`, JSON.stringify(partInfo));
          console.log(`✅ Uploaded part ${partNumber} for key ${key}, ${JSON.stringify(partInfo)}`);
        } catch (err) {
          console.error('❌ Error processing Kafka message:', err);
        }
      }
    });
  } catch (err) {
    console.error('❌ Kafka consumer failed to start:', err);
  }
}
