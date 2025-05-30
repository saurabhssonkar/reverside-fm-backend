import AWS from 'aws-sdk';
import CONFIG from '../CONFIG.js';
import logger from '../logger.js';

// Configure AWS S3 client
const s3 = new AWS.S3({
  region: CONFIG.aws.region,
  accessKeyId: CONFIG.aws.accessKeyId,
  secretAccessKey: CONFIG.aws.secretAccessKey
});

const bucketName = CONFIG.aws.bucketName;

 async function initializeMultipartUpload(cameraId, sessionId) {
  const s3Key = `videos/camera-saurabh`;
  try {
    const params = {
      Bucket: bucketName,
      Key: s3Key,
      ContentType: 'video/webm',
      // Metadata can be added here if needed
    };

    const result = await s3.createMultipartUpload(params).promise();
    logger.info(`Multipart upload initialized: ${result.UploadId}`);
    console.log("uploadId", result.UploadId);

    return {
      uploadId: result.UploadId,
      s3Key: s3Key
    };
  } catch (error) {
    logger.error(`Failed to initialize multipart upload: ${error.message}`);
    throw error;
  }
}

 async function uploadPart(uploadId, s3Key, partNumber, chunkData, uploadedParts) {
  console.log("partNumber", partNumber);
  try {
    const params = {
      Bucket: bucketName,
      Key: s3Key,
      PartNumber: partNumber,
      UploadId: uploadId,
      Body: Buffer.from(chunkData, 'base64')
    };

    const result = await s3.uploadPart(params).promise();
    console.log("result", result.ETag);
    uploadedParts.push(result);

    return {
      ETag: result.ETag,
      PartNumber: partNumber
    };
  } catch (error) {
    logger.error(`Failed to upload part ${partNumber}: ${error.message}`);
    throw error;
  }
}

 async function completeMultipartUpload(uploadId, s3Key) {
  try {
    console.log("uploadid", uploadId, s3Key);

    const completeParams = {
      Bucket: bucketName,
      Key: s3Key,
      UploadId: uploadId
    };

    const data = await s3.listParts(completeParams).promise();

    const parts = data.Parts.map(part => ({
      ETag: part.ETag,
      PartNumber: part.PartNumber
    }));

    completeParams.MultipartUpload = { Parts: parts };

    console.log("parts", parts);

    const result = await s3.completeMultipartUpload(completeParams).promise();
    logger.info(`Multipart upload completed: ${result.Location}`);

    return result.Location;
  } catch (error) {
    logger.error(`Failed to complete multipart upload: ${error.message}`);
    throw error;
  }
}

 async function abortMultipartUpload(uploadId, s3Key) {
  try {
    const params = {
      Bucket: bucketName,
      Key: s3Key,
      UploadId: uploadId
    };

    await s3.abortMultipartUpload(params).promise();
    logger.info(`Multipart upload aborted: ${uploadId}`);
  } catch (error) {
    logger.error(`Failed to abort multipart upload: ${error.message}`);
  }
}

export  {
  initializeMultipartUpload,
  uploadPart,
  completeMultipartUpload,
  abortMultipartUpload
}
