import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'video-uploader',
  brokers: ['localhost:9092']
});
const producer = kafka.producer({
    allowAutoTopicCreation: true,
    maxInFlightRequests: null,
    idempotent: true,
    metadataMaxAge: 300000
});

export async function sendToKafka(topic, message) {
    // console.log("message" , message)
  await producer.connect();
  await producer.send({
    topic,
    messages: [{ value: JSON.stringify(message) }]
  });
}
