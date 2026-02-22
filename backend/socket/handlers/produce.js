/**
 * socket/handlers/produce.js
 */
import { getProducerTransport, addProducer } from '../PeerManager.js';

export async function handleProduce(socket, peer, io, { kind, rtpParameters, appData }, callback) {
  try {
    const transport = getProducerTransport(socket.id);
    if (!transport) throw new Error('Producer transport not found');

    const producer = await transport.produce({ kind, rtpParameters, appData });
    addProducer(socket.id, producer);

    producer.on('transportclose', () => producer.close());
    producer.on('score', score =>
      socket.emit('producer-score', { producerId: producer.id, score }));

    // Notify all other peers in the room about this new producer
    socket.to(peer.roomId).emit('new-producer', {
      producerId:       producer.id,
      producerSocketId: socket.id,
      kind,
    });

    callback({ id: producer.id, producersExist: peer.producers.size > 0 });
  } catch (err) {
    console.error('produce error:', err);
    callback({ error: err.message });
  }
}
