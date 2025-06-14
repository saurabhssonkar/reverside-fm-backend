import { v4 as uuidv4 } from 'uuid';
import { completeMultipartUpload, initializeMultipartUpload } from '../aws/S3Manager.js';
import logger from '../logger.js';
import { initializeKafka, sendVideoChunk, createTopicForCamera, consumer } from "../kafka/KafkaManager.js"
import { initializeRedis, incrementChunkCount, getStreamMetadata, updateStreamStatus, setStreamMetadata } from "../Redis/RedisManager.js"
import { uploadPart } from '../aws/S3Manager.js';



// In-memory buffer for chunks per camera
const chunkBuffers = new Map();
// Track multipart upload parts
const uploadParts = new Map();
// Track part numbers
const partNumbers = new Map();

export async function initialize() {
    try {
        console.log("@@@@@@@@");
        await initializeKafka();
        await initializeRedis();
        logger.info('Video Stream Processor initialized successfully');

        // Start recovery process for interrupted uploads
        await recoverIncompleteUploads();
    } catch (error) {
        logger.error(`Processor initialization failed: ${error.message}`);
        throw error;
    }
}

export async function startStreamForCamera(cameraId) {
    try {
        const sessionId = uuidv4();

        await createTopicForCamera(cameraId);

        const uploadInfo = await initializeMultipartUpload(cameraId, sessionId);
        console.log("uploadInfo", uploadInfo);

        await setStreamMetadata(cameraId, {
            sessionId,
            startTime: Date.now(),
            status: 'UPLOADING',
            chunkCount: 0,
            uploadId: uploadInfo.uploadId,
            s3Key: uploadInfo.s3Key
        });

        chunkBuffers.set(cameraId, []);
        uploadParts.set(cameraId, []);
        partNumbers.set(cameraId, 1);

        logger.info(`Stream started for camera: ${cameraId}`);
        return { sessionId, status: 'UPLOADING' };
    } catch (error) {
        logger.error(`Failed to start stream for camera ${cameraId}: ${error.message}`);
        throw error;
    }
}

export async function processVideoChunk(cameraId, chunkData) {
  if (!chunkData || !(chunkData instanceof Buffer)) {
        throw new Error(`Invalid chunkData: Expected Buffer, got ${typeof chunkData}`);
    }

    if (chunkData.length > 10485760) {
        throw new Error(`Chunk size ${chunkData.length} exceeds maximum allowed ${MAX_KAFKA_MESSAGE_SIZE}`);
    }
    try {
        await sendVideoChunk(cameraId, chunkData);
        await incrementChunkCount(cameraId);
        logger.info(`Video chunk processed for camera: ${cameraId}`);
    } catch (error) {
        logger.error(`Failed to process chunk for camera ${cameraId}: ${error.message}`);
        throw error;
    }
}

export async function startConsumer(number) {
    let partNumber = 0;
    const uploadedParts = [];

    try {
        console.log("saurabh eachMessage", number++);

        await consumer.subscribe({
            topic: 'camera-stream',
            fromBeginning: true
        });

        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                try {
                    console.log("saurabh eachMessage3", number++);
                    const chunkInfo = JSON.parse(message.value.toString());
                    const cameraId = chunkInfo.cameraId;
                    const metadata = await getStreamMetadata(cameraId);
                    // console.log("metadata", metadata)
                    console.log("chunkInfo", chunkInfo.timestamp)

                    const base64Data = chunkInfo.chunkData;
                    // console.log("base64Data", base64Data)
                    const buffer = Buffer.from(base64Data, 'base64');
                    //   console.log('Chunk size before uploadPart:', Buffer.byteLength(buffer));
                   
                    console.log('Raw message size:', message.value.length);
                    console.log('Raw message:', message.value.toString().substring(0, 200)); // Print first 200 chars

                    partNumber++;
                    await uploadPart(metadata.uploadId, metadata.s3Key, partNumber, buffer, uploadedParts);





                    // await uploadPart(metadata.uploadId, metadata.s3Key, partNumber, chunkInfo.chunkData, uploadedParts);
                    // await completeMultipartUpload(metadata.uploadId, metadata.s3Key)



                    if (!metadata) {
                        logger.warn(`No metadata found for camera: ${cameraId}`);
                        return;
                    }

                    if (metadata.status === 'COMPLETED' || metadata.status === 'FAILED') {
                        return;
                    }

                    if (!chunkBuffers.has(cameraId)) {
                        chunkBuffers.set(cameraId, []);
                    }
                    chunkBuffers.get(cameraId).push(chunkInfo);
                } catch (error) {
                    logger.error(`Error processing message: ${error.message}`);
                }
            }
        });

        logger.info('Kafka consumer started successfully');
    } catch (error) {
        logger.error(`Failed to start consumer: ${error.message}`);
    }
}

export async function uploadChunkIfReady(cameraId) {
    console.log("chunkBuffers", chunkBuffers);
    const chunks = chunkBuffers.get(cameraId) || [];
    console.log("chunks", chunks);
    console.log("chunks.length", chunks.length);

    if (chunks.length >= 10) {
        await uploadChunksAsPart(cameraId, chunks);
        chunkBuffers.set(cameraId, []);
    }
}

export async function uploadChunksAsPart(cameraId, chunks) {
    console.log("cameraId@", chunks);
    try {
        const metadata = await getStreamMetadata(cameraId);
        if (!metadata) return;

        const combinedData = Buffer.concat(
            chunks.map(chunk => Buffer.from(chunk.chunkData, 'base64'))
        );

        const partNumber = partNumbers.get(cameraId) || 1;

        const part = await s3Manager.uploadPart(
            metadata.uploadId,
            metadata.s3Key,
            partNumber,
            combinedData.toString('base64')
        );

        if (!uploadParts.has(cameraId)) {
            uploadParts.set(cameraId, []);
        }
        uploadParts.get(cameraId).push(part);

        partNumbers.set(cameraId, partNumber + 1);

        logger.info(`Uploaded part ${partNumber} for camera: ${cameraId}`);
    } catch (error) {
        logger.error(`Failed to upload chunk part for camera ${cameraId}: ${error.message}`);
        await handleUploadError(cameraId, error);
    }
}

export async function stopStreamForCamera(cameraId) {
    try {
        const metadata = await getStreamMetadata(cameraId);
        console.log("metadata", metadata);
        const parts = uploadParts.get(cameraId) || [];

        // if (parts.length > 0) {
        const uploadUrl = await s3Manager.completeMultipartUpload(
            metadata.uploadId,
            metadata.s3Key,
            parts
        );

        // Update status to completed (optional)
        // await redisManager.updateStreamStatus(cameraId, 'COMPLETED', {
        //     uploadUrl,
        //     endTime: Date.now()
        // });

        logger.info(`Stream completed for camera: ${cameraId}`);
        // }

        // Cleanup (optional)
        // await cleanupStreamResources(cameraId);

    } catch (error) {
        logger.error(`Failed to stop stream for camera ${cameraId}: ${error.message}`);
        await handleUploadError(cameraId, error);
    }
}

export async function handleUploadError(cameraId, error) {
    try {
        await updateStreamStatus(cameraId, 'FAILED', {
            error: error.message,
            failedAt: Date.now()
        });

        const metadata = await getStreamMetadata(cameraId);
        if (metadata && metadata.uploadId) {
            await s3Manager.abortMultipartUpload(metadata.uploadId, metadata.s3Key);
        }

        await cleanupStreamResources(cameraId);

    } catch (cleanupError) {
        logger.error(`Cleanup failed for camera ${cameraId}: ${cleanupError.message}`);
    }
}

export async function cleanupStreamResources(cameraId) {
    try {
        await kafkaManager.deleteTopicForCamera(cameraId);

        chunkBuffers.delete(cameraId);
        uploadParts.delete(cameraId);
        partNumbers.delete(cameraId);

        logger.info(`Resources cleaned up for camera: ${cameraId}`);
    } catch (error) {
        logger.error(`Cleanup failed for camera ${cameraId}: ${error.message}`);
    }
}

export async function recoverIncompleteUploads() {
    try {
        const activeStreams = await redisManager.getActiveStreams();

        for (const stream of activeStreams) {
            const cameraId = stream.cameraId;

            if (stream.status === 'UPLOADING' || stream.status === 'PROCESSING') {
                logger.info(`Recovering incomplete upload for camera: ${cameraId}`);

                chunkBuffers.set(cameraId, []);
                uploadParts.set(cameraId, []);
                partNumbers.set(cameraId, 1);

                await redisManager.updateStreamStatus(cameraId, 'PROCESSING');

                await kafkaManager.createTopicForCamera(cameraId);
            }
        }

        logger.info('Upload recovery process completed');
    } catch (error) {
        logger.error(`Recovery process failed: ${error.message}`);
    }
}

export async function getStreamStatus(cameraId) {
    return await redisManager.getStreamMetadata(cameraId);
}
