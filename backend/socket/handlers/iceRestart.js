/**
 * socket/handlers/iceRestart.js
 */
import { getProducerTransport } from '../PeerManager.js';

export async function handleIceRestart(peer, _data, callback) {
  try {
    const transport = getProducerTransport(peer.socketId);
    if (!transport) throw new Error('Transport not found');
    const iceParameters = await transport.restartIce();
    callback({ iceParameters });
  } catch (err) {
    console.error('iceRestart error:', err);
    callback({ error: err.message });
  }
}
