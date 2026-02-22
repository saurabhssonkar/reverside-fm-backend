// import { createClient } from 'redis';
// import CONFIG from '../CONFIG.js';
// import logger from '../logger.js';

// const client = createClient({
//     socket: {
//         host: CONFIG.redis.host,
//         port: CONFIG.redis.port
//     }
// });

//  const  initializeRedis = async () => {
//     try {
//         await client.connect();
//         logger.info('Redis connection established');
//     } catch (error) {
//         logger.error(`Redis initialization failed: ${error.message}`);
//         throw error;
//     }
// };

//  const setStreamMetadata = async (cameraId, metadata) => {
//     const key = `stream:${cameraId}:metadata`;
//     try {
//         await client.hSet(key, {
//             cameraId,
//             sessionId: metadata.sessionId,
//             startTime: metadata.startTime,
//             status: metadata.status,
//             chunkCount: metadata.chunkCount || 0,
//             uploadId: metadata.uploadId || '',
//             s3Key: metadata.s3Key || ''
//         });
//         logger.info(`Stream metadata stored for camera:  ${JSON.stringify(metadata)}`);
//     } catch (error) {
//         logger.error(`Failed to store metadata for camera ${cameraId}: ${error.message}`);
//     }
// };

//  const getStreamMetadata = async (cameraId) => {
//     const key = `stream:${cameraId}:metadata`;
//     try {
//         const metadata = await client.hGetAll(key);
//         return Object.keys(metadata).length > 0 ? metadata : null;
//     } catch (error) {
//         logger.error(`Failed to get metadata for camera ${cameraId}: ${error.message}`);
//         return null;
//     }
// };

//  const updateStreamStatus = async (cameraId, status, additionalData = {}) => {
//     const key = `stream:${cameraId}:metadata`;
//     try {
//         const updateData = { status, lastUpdated: Date.now(), ...additionalData };
//         await client.hSet(key, updateData);
//         logger.info(`Stream status updated for camera ${cameraId}: ${status}`);
//     } catch (error) {
//         logger.error(`Failed to update status for camera ${cameraId}: ${error.message}`);
//     }
// };

//  const incrementChunkCount = async (cameraId) => {
//     const key = `stream:${cameraId}:metadata`;
//     try {
//         await client.hIncrBy(key, 'chunkCount', 1);
//         console.log("key",key)
//     } catch (error) {
//         logger.error(`Failed to increment chunk count for camera ${cameraId}: ${error.message}`);
//     }
// };

//  const deleteStreamData = async (cameraId) => {
//     const key = `stream:${cameraId}:metadata`;
//     try {
//         await client.del(key);
//         logger.info(`Stream data deleted for camera: ${cameraId}`);
//     } catch (error) {
//         logger.error(`Failed to delete stream data for camera ${cameraId}: ${error.message}`);
//     }
// };

//   const getActiveStreams = async () => {
//     try {
//         const keys = await client.keys('stream:*:metadata');
//         const activeStreams = [];
//         for (const key of keys) {
//             const metadata = await client.hGetAll(key);
//             if (metadata.status !== 'COMPLETED' && metadata.status !== 'FAILED') {
//                 activeStreams.push(metadata);
//             }
//         }
//         return activeStreams;
//     } catch (error) {
//         logger.error(`Failed to get active streams: ${error.message}`);
//         return [];
//     }
// };
// export  {
//     initializeRedis,
//     setStreamMetadata,
//     getActiveStreams,
//     deleteStreamData,
//     updateStreamStatus,
//     incrementChunkCount,
//     getStreamMetadata
    
    

// }

/**
 * Redis/RedisManager.js
 * 
 * REPLACES/UPDATES: tere existing Redis/RedisManager.js
 * 
 * Handles:
 * - Room metadata
 * - Peer tracking
 * - Stream/recording state
 * - Pub/Sub (multi-node ready)
 */

import Redis from 'ioredis';
import CONFIG from '../CONFIG.js';

let redis;
let publisher;
let subscriber;

// ── Init ──────────────────────────────────────────────────────────────────
export async function initRedis() {
  const opts = {
    host: CONFIG.redis.host,
    port: CONFIG.redis.port,
    password: CONFIG.redis.password || undefined,
    retryStrategy: (times) => Math.min(times * 200, 5000),
  };

  redis = new Redis(opts);
  publisher = new Redis(opts);
  subscriber = new Redis(opts);

  redis.on('error', err => console.error('Redis error:', err.message));

  // Test connection
  await redis.ping();
  return redis;
}

export function getRedisClient() {
  if (!redis) throw new Error('Redis not initialized — call initRedis() first');
  return redis;
}

// ── Room Metadata ─────────────────────────────────────────────────────────
export async function setRoomMeta(roomId, meta) {
  await redis.setex(`room:${roomId}:meta`, 86400, JSON.stringify(meta));
}

export async function getRoomMeta(roomId) {
  const d = await redis.get(`room:${roomId}:meta`);
  return d ? JSON.parse(d) : null;
}

export async function deleteRoomMeta(roomId) {
  await redis.del(`room:${roomId}:meta`, `room:${roomId}:peers`);
}

// ── Peer Tracking ─────────────────────────────────────────────────────────
export async function addPeerToRoom(roomId, socketId, info) {
  await redis.hset(`room:${roomId}:peers`, socketId, JSON.stringify({ socketId, ...info }));
  await redis.expire(`room:${roomId}:peers`, 86400);
}

export async function removePeerFromRoom(roomId, socketId) {
  await redis.hdel(`room:${roomId}:peers`, socketId);
}

export async function getRoomPeers(roomId) {
  const data = await redis.hgetall(`room:${roomId}:peers`) || {};
  return Object.values(data).map(p => JSON.parse(p));
}

// ── Stream / Recording State ──────────────────────────────────────────────
export async function setStreamMeta(cameraId, meta) {
  await redis.setex(`stream:${cameraId}`, 3600, JSON.stringify(meta));
}

export async function getStreamMeta(cameraId) {
  const d = await redis.get(`stream:${cameraId}`);
  return d ? JSON.parse(d) : null;
}

export async function updateStreamStatus(cameraId, status, extra = {}) {
  const meta = await getStreamMeta(cameraId);
  if (meta) await setStreamMeta(cameraId, { ...meta, status, ...extra });
}

export async function deleteStreamMeta(cameraId) {
  await redis.del(`stream:${cameraId}`);
}

export async function getActiveStreams() {
  const keys = await redis.keys('stream:*');
  const result = [];
  for (const key of keys) {
    const d = await redis.get(key);
    if (d) {
      const parsed = JSON.parse(d);
      if (['UPLOADING', 'PROCESSING'].includes(parsed.status)) result.push(parsed);
    }
  }
  return result;
}

// ── Pub/Sub (for future multi-node scaling) ───────────────────────────────
export async function publishEvent(channel, message) {
  await publisher.publish(channel, JSON.stringify(message));
}

export async function subscribeToChannel(channel, handler) {
  await subscriber.subscribe(channel);
  subscriber.on('message', (ch, msg) => {
    if (ch === channel) handler(JSON.parse(msg));
  });
}