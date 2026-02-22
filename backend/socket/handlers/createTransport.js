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
        id:             transport.id,
        iceParameters:  transport.iceParameters,
        iceCandidates:  transport.iceCandidates,
        dtlsParameters: transport.dtlsParameters,
      },
    });
  } catch (err) {
    console.error('createTransport error:', err);
    callback({ params: { error: err.message } });
  }
}
