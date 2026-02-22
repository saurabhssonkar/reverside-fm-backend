/**
 * socket/handlers/joinRoom.js
 */
import { getOrCreateRoom, addPeer, getRoomPeerList } from '../RoomManager.js';

export async function handleJoinRoom(socket, peer, { roomId, displayName, password }, callback) {
  try {
    if (!roomId) return callback({ error: 'roomId required' });

    const room = await getOrCreateRoom({ roomId, hostId: socket.id });

    // Password check
    if (room.isLocked && room.password !== (password || '')) {
      return callback({ error: 'Wrong room password' });
    }

    // Capacity check
    if (room.peers.size >= room.maxParticipants) {
      return callback({ error: 'Room is full' });
    }

    peer.roomId = roomId;
    await addPeer(roomId, socket.id, {
      name: displayName || 'Guest',
      isHost: room.peers.size === 0,
    });

    // Join Socket.IO room for .to(roomId) broadcasts
    socket.join(roomId);

    // Tell existing peers someone joined
    socket.to(roomId).emit('peer-joined', {
      socketId: socket.id,
      displayName: displayName || 'Guest',
    });

    callback({
      rtpCapabilities: room.router.rtpCapabilities,
      participants: getRoomPeerList(roomId),
      isHost: room.hostId === socket.id,
    });
  } catch (err) {
    console.error('joinRoom error:', err);
    callback({ error: err.message });
  }
}


/**
 * socket/handlers/createTransport.js
 */
import { getRoom } from '../RoomManager.js';
import { addTransport } from '../PeerManager.js';
import { createWebRtcTransport } from '../../mediasoup/TransportManager.js';

export async function handleCreateTransport(socket, peer, { consumer }, callback) {
  try {
    if (!peer.roomId) throw new Error('Not in a room');
    const room = getRoom(peer.roomId);
    if (!room) throw new Error('Room not found');

    const transport = await createWebRtcTransport(room.router);
    addTransport(socket.id, transport, !!consumer);

    callback({
      params: {
        id: transport.id,
        iceParameters: transport.iceParameters,
        iceCandidates: transport.iceCandidates,
        dtlsParameters: transport.dtlsParameters,
      },
    });
  } catch (err) {
    console.error('createTransport error:', err);
    callback({ params: { error: err.message } });
  }
}


/**
 * socket/handlers/transportConnect.js
 */
import { getProducerTransport, getConsumerTransport } from '../PeerManager.js';

export async function handleTransportConnect(peer, { dtlsParameters, serverConsumerTransportId }) {
  try {
    const transport = serverConsumerTransportId
      ? getConsumerTransport(peer.socketId, serverConsumerTransportId)
      : getProducerTransport(peer.socketId);

    if (!transport) throw new Error('Transport not found');
    await transport.connect({ dtlsParameters });
  } catch (err) {
    console.error('transportConnect error:', err);
  }
}


/**
 * socket/handlers/produce.js
 */
import { getProducerTransport as getProducerT, addProducer } from '../PeerManager.js';
import { getRoom as getR } from '../RoomManager.js';

export async function handleProduce(socket, peer, io, { kind, rtpParameters, appData }, callback) {
  try {
    const transport = getProducerT(socket.id);
    if (!transport) throw new Error('Producer transport not found');

    const producer = await transport.produce({ kind, rtpParameters, appData });
    addProducer(socket.id, producer);

    producer.on('transportclose', () => producer.close());
    producer.on('score', score => socket.emit('producer-score', { producerId: producer.id, score }));

    // Notify ALL other peers in room about this new producer
    socket.to(peer.roomId).emit('new-producer', {
      producerId: producer.id,
      producerSocketId: socket.id,
      kind,
    });

    callback({ id: producer.id, producersExist: peer.producers.size > 0 });
  } catch (err) {
    console.error('produce error:', err);
    callback({ error: err.message });
  }
}


/**
 * socket/handlers/consume.js
 * Also exports: handleGetProducers, handleConsumerResume
 */
import { getConsumerTransport as getConsumerT, addConsumer, removeConsumer, getPeer } from '../PeerManager.js';
import { getRoom as getRoomC } from '../RoomManager.js';

export async function handleConsume(socket, peer, { rtpCapabilities, remoteProducerId, serverConsumerTransportId }, callback) {
  try {
    const room = getRoomC(peer.roomId);
    if (!room) throw new Error('Room not found');

    const transport = getConsumerT(socket.id, serverConsumerTransportId);
    if (!transport) throw new Error('Consumer transport not found');

    if (!room.router.canConsume({ producerId: remoteProducerId, rtpCapabilities })) {
      throw new Error('Cannot consume this producer');
    }

    const consumer = await transport.consume({
      producerId: remoteProducerId,
      rtpCapabilities,
      paused: true, // Client resumes after setting up the track
    });

    addConsumer(socket.id, consumer);

    consumer.on('transportclose', () => removeConsumer(socket.id, consumer.id));
    consumer.on('producerclose', () => {
      socket.emit('producer-closed', { remoteProducerId });
      removeConsumer(socket.id, consumer.id);
    });
    consumer.on('producerpause', () => socket.emit('consumer-paused', { consumerId: consumer.id }));
    consumer.on('producerresume', () => socket.emit('consumer-resumed', { consumerId: consumer.id }));
    consumer.on('score', score => socket.emit('consumer-score', { consumerId: consumer.id, score }));

    callback({
      params: {
        id: consumer.id,
        producerId: remoteProducerId,
        kind: consumer.kind,
        rtpParameters: consumer.rtpParameters,
        serverConsumerId: consumer.id,
      },
    });
  } catch (err) {
    console.error('consume error:', err);
    callback({ params: { error: err.message } });
  }
}

export function handleGetProducers(socket, peer, callback) {
  const room = getRoomC(peer.roomId);
  if (!room) return callback([]);

  const result = [];
  for (const [peerId] of room.peers) {
    if (peerId === socket.id) continue;
    const otherPeer = getPeer(peerId);
    if (!otherPeer) continue;
    for (const [producerId, producer] of otherPeer.producers) {
      result.push({ producerId, peerId, kind: producer.kind });
    }
  }
  callback(result);
}

export async function handleConsumerResume(peer, { serverConsumerId }) {
  try {
    const consumer = peer.consumers.get(serverConsumerId);
    if (consumer) await consumer.resume();
  } catch (err) {
    console.error('consumerResume error:', err);
  }
}


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


/**
 * socket/handlers/peerStatus.js
 */
import { updatePeerStatus } from '../RoomManager.js';

export function handlePeerStatus(socket, peer, io, data) {
  if (!peer.roomId) return;
  const { isAudioMuted, isVideoOff, isScreenSharing } = data;
  updatePeerStatus(peer.roomId, socket.id, { isAudioMuted, isVideoOff, isScreenSharing });
  socket.to(peer.roomId).emit('peer-status-updated', { socketId: socket.id, ...data });
}


/**
 * socket/handlers/iceRestart.js
 */
import { getProducerTransport as getPT } from '../PeerManager.js';

export async function handleIceRestart(peer, _data, callback) {
  try {
    const transport = getPT(peer.socketId);
    if (!transport) throw new Error('Transport not found');
    const iceParameters = await transport.restartIce();
    callback({ iceParameters });
  } catch (err) {
    console.error('iceRestart error:', err);
    callback({ error: err.message });
  }
}