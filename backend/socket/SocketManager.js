/**
 * socket/SocketManager.js
 * 
 * NEW FILE — create this folder in tere project root.
 * 
 * Links every socket event to its handler.
 * Yahi file hai jo sab kuch connect karti hai.
 */

import { createPeer, cleanupPeer } from './PeerManager.js';
import { removePeer } from './RoomManager.js';
import { handleJoinRoom } from './handlers/joinRoom.js';
import { handleCreateTransport } from './handlers/createTransport.js';
import { handleTransportConnect } from './handlers/transportConnect.js';
import { handleProduce } from './handlers/produce.js';
import { handleConsume, handleGetProducers, handleConsumerResume } from './handlers/consume.js';
import { handleRecordingChunk } from './handlers/recordingChunk.js';
import { handlePeerStatus } from './handlers/peerStatus.js';
import { handleIceRestart } from './handlers/iceRestart.js';

export function registerAllSocketHandlers(io) {
  // Namespace — same as tera existing frontend expects
  const ns = io.of('/mediasoup');

  ns.on('connection', (socket) => {
    console.log(`🔌 Connected: ${socket.id}`);

    // 1. Create peer object in memory
    const peer = createPeer(socket.id, socket);

    // 2. Tell client it connected successfully
    socket.emit('connection-success', { socketId: socket.id });

    // ── Core WebRTC signaling ─────────────────────────────────────────
    socket.on('joinRoom',
      (data, cb) => handleJoinRoom(socket, peer, data, cb));

    socket.on('getProducers',
      (cb) => handleGetProducers(socket, peer, cb));

    socket.on('createWebRtcTransport',
      (data, cb) => handleCreateTransport(socket, peer, data, cb));

    // Producer side transport
    socket.on('transport-connect',
      (data) => handleTransportConnect(peer, data));

    // Consumer side transport
    socket.on('transport-recv-connect',
      (data) => handleTransportConnect(peer, data));

    socket.on('transport-produce',
      (data, cb) => handleProduce(socket, peer, io, data, cb));

    socket.on('consume',
      (data, cb) => handleConsume(socket, peer, data, cb));

    socket.on('consumer-resume',
      (data) => handleConsumerResume(peer, data));

    // ── Extra features ────────────────────────────────────────────────
    socket.on('recording-chunk',
      (chunk) => handleRecordingChunk(socket, chunk));

    socket.on('peer-status-update',
      (data) => handlePeerStatus(socket, peer, io, data));

    socket.on('ice-restart',
      (data, cb) => handleIceRestart(peer, data, cb));

    // ── Disconnect ────────────────────────────────────────────────────
    socket.on('disconnect', async (reason) => {
      console.log(`🔌 Disconnected: ${socket.id} — ${reason}`);

      const roomId = peer.roomId;

      // Close all transports/producers/consumers
      cleanupPeer(socket.id);

      if (roomId) {
        // Remove from room, close router if room empty
        const updatedRoom = await removePeer(roomId, socket.id);

        // Notify remaining peers
        socket.to(roomId).emit('peer-left', { socketId: socket.id });

        if (updatedRoom) {
          socket.to(roomId).emit('participants-updated', {
            participants: [...updatedRoom.peers.values()],
          });
        }
      }
    });
  });
}