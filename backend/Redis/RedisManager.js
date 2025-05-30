import { createClient } from 'redis';
import CONFIG from '../CONFIG.js';
import logger from '../logger.js';

const client = createClient({
    socket: {
        host: CONFIG.redis.host,
        port: CONFIG.redis.port
    }
});

 const  initializeRedis = async () => {
    try {
        await client.connect();
        logger.info('Redis connection established');
    } catch (error) {
        logger.error(`Redis initialization failed: ${error.message}`);
        throw error;
    }
};

 const setStreamMetadata = async (cameraId, metadata) => {
    const key = `stream:${cameraId}:metadata`;
    try {
        await client.hSet(key, {
            cameraId,
            sessionId: metadata.sessionId,
            startTime: metadata.startTime,
            status: metadata.status,
            chunkCount: metadata.chunkCount || 0,
            uploadId: metadata.uploadId || '',
            s3Key: metadata.s3Key || ''
        });
        logger.info(`Stream metadata stored for camera:  ${JSON.stringify(metadata)}`);
    } catch (error) {
        logger.error(`Failed to store metadata for camera ${cameraId}: ${error.message}`);
    }
};

 const getStreamMetadata = async (cameraId) => {
    const key = `stream:${cameraId}:metadata`;
    try {
        const metadata = await client.hGetAll(key);
        return Object.keys(metadata).length > 0 ? metadata : null;
    } catch (error) {
        logger.error(`Failed to get metadata for camera ${cameraId}: ${error.message}`);
        return null;
    }
};

 const updateStreamStatus = async (cameraId, status, additionalData = {}) => {
    const key = `stream:${cameraId}:metadata`;
    try {
        const updateData = { status, lastUpdated: Date.now(), ...additionalData };
        await client.hSet(key, updateData);
        logger.info(`Stream status updated for camera ${cameraId}: ${status}`);
    } catch (error) {
        logger.error(`Failed to update status for camera ${cameraId}: ${error.message}`);
    }
};

 const incrementChunkCount = async (cameraId) => {
    const key = `stream:${cameraId}:metadata`;
    try {
        await client.hIncrBy(key, 'chunkCount', 1);
        console.log("key",key)
    } catch (error) {
        logger.error(`Failed to increment chunk count for camera ${cameraId}: ${error.message}`);
    }
};

 const deleteStreamData = async (cameraId) => {
    const key = `stream:${cameraId}:metadata`;
    try {
        await client.del(key);
        logger.info(`Stream data deleted for camera: ${cameraId}`);
    } catch (error) {
        logger.error(`Failed to delete stream data for camera ${cameraId}: ${error.message}`);
    }
};

  const getActiveStreams = async () => {
    try {
        const keys = await client.keys('stream:*:metadata');
        const activeStreams = [];
        for (const key of keys) {
            const metadata = await client.hGetAll(key);
            if (metadata.status !== 'COMPLETED' && metadata.status !== 'FAILED') {
                activeStreams.push(metadata);
            }
        }
        return activeStreams;
    } catch (error) {
        logger.error(`Failed to get active streams: ${error.message}`);
        return [];
    }
};
export  {
    initializeRedis,
    setStreamMetadata,
    getActiveStreams,
    deleteStreamData,
    updateStreamStatus,
    incrementChunkCount,
    getStreamMetadata
    
    

}
