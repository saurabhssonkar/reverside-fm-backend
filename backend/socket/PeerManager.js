/**
 * socket/PeerManager.js
 *
 * Re-exports all peer-management functions from RoomManager.js where the
 * shared `peers` Map lives. This keeps a single source of truth while
 * letting SocketManager import from a dedicated module.
 */

export {
  createPeer,
  getPeer,
  addTransport,
  getProducerTransport,
  getConsumerTransport,
  addProducer,
  addConsumer,
  removeConsumer,
  cleanupPeer,
  getPeers,
} from './RoomManager.js';
