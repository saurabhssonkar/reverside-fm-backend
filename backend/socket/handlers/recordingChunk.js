/**
 * socket/handlers/recordingChunk.js
 */
import { appendChunk, getBuffer, resetBuffer } from '../../recording/ChunkBuffer.js';
import { startCameraStream, processChunk } from '../../recording/RecordingService.js';
import CONFIG from '../../CONFIG.js';

const initializedStreams = new Set();

export async function handleRecordingChunk(socket, arrayBuffer) {
  try {
    const cameraId = socket.id;

    if (!initializedStreams.has(cameraId)) {
      await startCameraStream(cameraId);
      initializedStreams.add(cameraId);
    }

    appendChunk(cameraId, arrayBuffer);
    const { chunks, totalSize } = getBuffer(cameraId);

    if (totalSize >= CONFIG.recording.chunkThreshold) {
      const combined = mergeChunks(chunks, totalSize);
      await processChunk(cameraId, combined);
      resetBuffer(cameraId);
    }
  } catch (err) {
    console.error(`recordingChunk error [${socket.id}]:`, err.message);
  }
}

function mergeChunks(chunks, totalSize) {
  const out = Buffer.alloc(totalSize);
  let offset = 0;
  for (const chunk of chunks) {
    const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    buf.copy(out, offset);
    offset += buf.length;
  }
  return out;
}
