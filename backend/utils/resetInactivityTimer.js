export const resetInactivityTimer = async (inactivityTimer,uploadInfo,redis,s3) => {
    clearTimeout(inactivityTimer);
    inactivityTimer = setTimeout(async () => {
      if (uploadInfo) {
        const parts = (await redis.lrange(`${uploadInfo.key}-parts`, 0, -1)).map(p => JSON.parse(p));
        await s3.completeMultipartUpload({
          Bucket: process.env.AWS_S3_BUCKET,
          Key: uploadInfo.key,
          UploadId: uploadInfo.uploadId,
          MultipartUpload: { Parts: parts }
        }).promise();
        console.log('✅ Upload completed for', uploadInfo.key);
        await redis.del(`${socket.id}-upload`);
      }
    }, 5000); // 5s of no data → complete upload
  };
