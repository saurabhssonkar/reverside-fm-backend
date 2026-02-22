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
