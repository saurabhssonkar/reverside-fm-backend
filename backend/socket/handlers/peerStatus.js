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
