/**
 * recording/ChunkBuffer.js
 *
 * In-memory per-peer chunk accumulator.
 * Keeps raw ArrayBuffer/Buffer chunks until the threshold is reached,
 * then the caller flushes them to S3 via RecordingService.
 */

// Map<cameraId, { chunks: Buffer[], totalSize: number }>
const buffers = new Map();

export function appendChunk(cameraId, arrayBuffer) {
  const buf = Buffer.isBuffer(arrayBuffer)
    ? arrayBuffer
    : Buffer.from(arrayBuffer);

  if (!buffers.has(cameraId)) {
    buffers.set(cameraId, { chunks: [], totalSize: 0 });
  }

  const entry = buffers.get(cameraId);
  entry.chunks.push(buf);
  entry.totalSize += buf.byteLength;
}

export function getBuffer(cameraId) {
  return buffers.get(cameraId) || { chunks: [], totalSize: 0 };
}

export function resetBuffer(cameraId) {
  buffers.set(cameraId, { chunks: [], totalSize: 0 });
}
