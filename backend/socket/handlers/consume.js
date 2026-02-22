/**
 * socket/handlers/consume.js
 */
import { getConsumerTransport, addConsumer, removeConsumer, getPeer } from '../PeerManager.js';
import { getRoom } from '../RoomManager.js';

export async function handleConsume(
  socket, peer,
  { rtpCapabilities, remoteProducerId, serverConsumerTransportId },
  callback,
) {
  try {
    const room = getRoom(peer.roomId);
    if (!room) throw new Error('Room not found');

    const transport = getConsumerTransport(socket.id, serverConsumerTransportId);
    if (!transport) throw new Error('Consumer transport not found');

    if (!room.router.canConsume({ producerId: remoteProducerId, rtpCapabilities })) {
      throw new Error('Cannot consume this producer');
    }

    const consumer = await transport.consume({
      producerId:      remoteProducerId,
      rtpCapabilities,
      paused: true, // client resumes after track setup
    });

    addConsumer(socket.id, consumer);

    consumer.on('transportclose', () => removeConsumer(socket.id, consumer.id));
    consumer.on('producerclose', () => {
      socket.emit('producer-closed', { remoteProducerId });
      removeConsumer(socket.id, consumer.id);
    });
    consumer.on('producerpause',  () => socket.emit('consumer-paused',  { consumerId: consumer.id }));
    consumer.on('producerresume', () => socket.emit('consumer-resumed', { consumerId: consumer.id }));
    consumer.on('score', score   => socket.emit('consumer-score',   { consumerId: consumer.id, score }));

    callback({
      params: {
        id:               consumer.id,
        producerId:       remoteProducerId,
        kind:             consumer.kind,
        rtpParameters:    consumer.rtpParameters,
        serverConsumerId: consumer.id,
      },
    });
  } catch (err) {
    console.error('consume error:', err);
    callback({ params: { error: err.message } });
  }
}

export function handleGetProducers(socket, peer, callback) {
  const room = getRoom(peer.roomId);
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
