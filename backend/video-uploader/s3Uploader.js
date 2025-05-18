import AWS from 'aws-sdk';
import redis from './redisClient.js';

const s3 = new AWS.S3();

export async function uploadPart({ uploadId, key, partNumber, chunk }) {
    console.log("partNumber",partNumber)
  const res = await s3.uploadPart({
    Bucket: process.env.AWS_S3_BUCKET,
    Key: key,
    UploadId: uploadId,
    PartNumber: partNumber,
    Body: Buffer.from(chunk, 'base64') // decode base64
  }).promise();

  await redis.rpush(`${key}-parts`, JSON.stringify({
    ETag: res.ETag,
    PartNumber: partNumber
  }));
}
