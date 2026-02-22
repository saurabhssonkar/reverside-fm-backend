/**
 * recording/RecordingService.js
 *
 * Wraps S3 multipart upload lifecycle for per-peer recording sessions.
 *
 * Exports:
 *   startCameraStream(cameraId)    — called once when recording begins
 *   processChunk(cameraId, buffer) — called for each flushed chunk
 *   completeStream(cameraId)       — called when recording ends (completes S3 upload)
 *   abortStream(cameraId)          — called to cancel an in-progress upload
 *   recoverIncompleteUploads()     — called at server boot
 */

import { v4 as uuidv4 } from 'uuid';
import {
  initializeMultipartUpload,
  uploadPart,
  completeMultipartUpload,
  abortMultipartUpload,
} from '../aws/S3Manager.js';
import {
  setStreamMeta,
  getStreamMeta,
  updateStreamStatus,
  getActiveStreams,
} from '../Redis/RedisManager.js';
import logger from '../logger.js';

// In-memory upload state per cameraId
// Map<cameraId, { uploadId, s3Key, partNumber, parts: [] }>
const sessions = new Map();

// ── Start a new recording session ────────────────────────────────────────────
export async function startCameraStream(cameraId) {
  try {
    const sessionId = uuidv4();
    const uploadInfo = await initializeMultipartUpload(cameraId, sessionId);

    await setStreamMeta(cameraId, {
      sessionId,
      startTime: Date.now(),
      status: 'UPLOADING',
      uploadId: uploadInfo.uploadId,
      s3Key: uploadInfo.s3Key,
      chunkCount: 0,
    });

    sessions.set(cameraId, {
      uploadId:   uploadInfo.uploadId,
      s3Key:      uploadInfo.s3Key,
      partNumber: 1,
      parts:      [],
    });

    logger.info(`Recording started for peer: ${cameraId}`);
    return { sessionId, status: 'UPLOADING' };
  } catch (err) {
    logger.error(`startCameraStream failed [${cameraId}]: ${err.message}`);
    throw err;
  }
}

// ── Upload one flushed chunk as an S3 multipart part ─────────────────────────
export async function processChunk(cameraId, buffer) {
  try {
    let session = sessions.get(cameraId);

    // Guard: re-hydrate from Redis if session was lost after restart
    if (!session) {
      const meta = await getStreamMeta(cameraId);
      if (!meta || !meta.uploadId) {
        throw new Error(`No active session for peer ${cameraId}`);
      }
      session = { uploadId: meta.uploadId, s3Key: meta.s3Key, partNumber: 1, parts: [] };
      sessions.set(cameraId, session);
    }

    const part = await uploadPart(
      session.uploadId,
      session.s3Key,
      session.partNumber,
      buffer.toString('base64'),
      session.parts,
    );

    session.parts.push({ ETag: part.ETag, PartNumber: session.partNumber });
    session.partNumber += 1;

    logger.info(`Chunk uploaded [${cameraId}] part ${session.partNumber - 1}`);
  } catch (err) {
    logger.error(`processChunk failed [${cameraId}]: ${err.message}`);
    throw err;
  }
}

// ── Complete the multipart upload ────────────────────────────────────────────
export async function completeStream(cameraId) {
  try {
    const session = sessions.get(cameraId);
    if (!session) {
      logger.warn(`completeStream: no session found for ${cameraId}`);
      return null;
    }

    const url = await completeMultipartUpload(session.uploadId, session.s3Key);
    await updateStreamStatus(cameraId, 'COMPLETED', { uploadUrl: url, endTime: Date.now() });

    sessions.delete(cameraId);
    logger.info(`Recording finalized for peer: ${cameraId}`);
    return url;
  } catch (err) {
    logger.error(`completeStream failed [${cameraId}]: ${err.message}`);
    await abortStream(cameraId);
    throw err;
  }
}

// ── Abort and clean up an in-progress upload ─────────────────────────────────
export async function abortStream(cameraId) {
  try {
    const session = sessions.get(cameraId);
    if (session) {
      await abortMultipartUpload(session.uploadId, session.s3Key);
      sessions.delete(cameraId);
    }
    await updateStreamStatus(cameraId, 'FAILED', { failedAt: Date.now() });
    logger.info(`Upload aborted for peer: ${cameraId}`);
  } catch (err) {
    logger.error(`abortStream failed [${cameraId}]: ${err.message}`);
  }
}

// ── Recover in-progress sessions after a server restart ──────────────────────
export async function recoverIncompleteUploads() {
  try {
    const activeStreams = await getActiveStreams();

    for (const stream of activeStreams) {
      if (stream.status === 'UPLOADING' || stream.status === 'PROCESSING') {
        const { cameraId, uploadId, s3Key } = stream;
        logger.info(`Recovering upload for peer: ${cameraId}`);

        sessions.set(cameraId, { uploadId, s3Key, partNumber: 1, parts: [] });
        await updateStreamStatus(cameraId, 'PROCESSING');
      }
    }

    logger.info('Upload recovery complete');
  } catch (err) {
    logger.error(`recoverIncompleteUploads failed: ${err.message}`);
  }
}
