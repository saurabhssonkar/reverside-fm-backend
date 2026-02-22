// import mediasoup from 'mediasoup';

// export default function mediasoupSetup(io) {
//   // Worker, rooms, and peer management
//   let worker;
//   const rooms = new Map();
//   const peers = new Map();
  
//   // Create Mediasoup worker
//   const createWorker = async () => {
//     worker = await mediasoup.createWorker({
//       rtcMinPort: 3000,
//       rtcMaxPort: 5050,
//     });
//     console.log(`Worker PID: ${worker.pid}`);

//     worker.on('died', () => {
//       console.error('Mediasoup worker has died');
//       setTimeout(() => process.exit(1), 2000);
//     });

//     return worker;
//   };

//   // Media codecs configuration
//   const mediaCodecs = [
//     {
//       kind: 'audio',
//       mimeType: 'audio/opus',
//       clockRate: 48000,
//       channels: 2,
//     },
//     {
//       kind: 'video',
//       mimeType: 'video/VP8',
//       clockRate: 90000,
//       parameters: {
//         'x-google-start-bitrate': 1000,
//       },
//     },
//   ];

//   // Socket.IO namespace for Mediasoup
//   const mediasoupNamespace = io.of('/mediasoup');

//   mediasoupNamespace.on('connection', (socket) => {
//     console.log(`New connection: ${socket.id}`);
    
//     const peer = {
//       socket,
//       roomName: null,
//       transports: new Map(),
//       producers: new Map(),
//       consumers: new Map(),
//       peerDetails: {
//         name: '',
//         isAdmin: false,
//       }
//     };
    
//     peers.set(socket.id, peer);

//     socket.emit('connection-success', { socketId: socket.id });

//     // Handle disconnection
//     socket.on('disconnect', () => {
//       console.log(`Peer disconnected: ${socket.id}`);
//       cleanupPeer(socket.id);
//       peers.delete(socket.id);
//     });

//     // Room management
//     socket.on('joinRoom', async ({ roomName }, callback) => {
//       const router = await getOrCreateRouter(roomName);
//       peer.roomName = roomName;
      
//       // Add peer to room
//       if (!rooms.has(roomName)) {
//         rooms.set(roomName, {
//           router,
//           peers: new Set(),
//         });
//       }
//       rooms.get(roomName).peers.add(socket.id);
      
//       callback({ rtpCapabilities: router.rtpCapabilities });
//     });

//     // Transport management
//     socket.on('createWebRtcTransport', async ({ consumer }, callback) => {
//       try {
//         if (!peer.roomName) throw new Error('Peer not in a room');
        
//         const router = rooms.get(peer.roomName).router;
//         const transport = await createWebRtcTransport(router);
        
//         peer.transports.set(transport.id, {
//           transport,
//           consumer
//         });

//         callback({
//           params: {
//             id: transport.id,
//             iceParameters: transport.iceParameters,
//             iceCandidates: transport.iceCandidates,
//             dtlsParameters: transport.dtlsParameters,
//           }
//         });
//       } catch (error) {
//         console.error('Transport creation error:', error);
//         callback({ error: error.message });
//       }
//     });

//     // Producer management
//     socket.on('transport-produce', async ({ kind, rtpParameters }, callback) => {
//       try {
//         const producerTransport = getProducerTransport(socket.id);
//         const producer = await producerTransport.produce({ kind, rtpParameters });
        
//         peer.producers.set(producer.id, producer);
        
//         // Notify other peers about new producer
//         notifyNewProducer(peer.roomName, socket.id, producer.id);
        
//         callback({
//           id: producer.id,
//           producersExist: Array.from(peer.producers).length > 1
//         });
//       } catch (error) {
//         console.error('Producer creation error:', error);
//         callback({ error: error.message });
//       }
//     });

//     // Consumer management
//     socket.on('consume', async ({ rtpCapabilities, remoteProducerId, serverConsumerTransportId }, callback) => {
//       try {
//         const consumerTransport = peer.transports.get(serverConsumerTransportId)?.transport;
//         if (!consumerTransport) throw new Error('Transport not found');
        
//         const router = rooms.get(peer.roomName).router;
        
//         if (router.canConsume({ producerId: remoteProducerId, rtpCapabilities })) {
//           const consumer = await consumerTransport.consume({
//             producerId: remoteProducerId,
//             rtpCapabilities,
//             paused: true,
//           });
          
//           peer.consumers.set(consumer.id, consumer);
          
//           consumer.on('producerclose', () => {
//             socket.emit('producer-closed', { remoteProducerId });
//             cleanupConsumer(socket.id, consumer.id);
//           });
          
//           callback({
//             params: {
//               id: consumer.id,
//               producerId: remoteProducerId,
//               kind: consumer.kind,
//               rtpParameters: consumer.rtpParameters,
//               serverConsumerId: consumer.id,
//             }
//           });
//         } else {
//           throw new Error('Cannot consume');
//         }
//       } catch (error) {
//         console.error('Consume error:', error);
//         callback({ error: error.message });
//       }
//     });

//     // Recording handling
//     socket.on('recording-chunk', (arrayBuffer) => {
//       const filePath = path.join(__dirname, `recordings/recorded_${socket.id}.webm`);
//       const buffer = Buffer.from(arrayBuffer);
      
//       fs.appendFile(filePath, buffer, (err) => {
//         if (err) console.error('Error saving chunk:', err);
//       });
//     });
//   });

//   // Helper functions
//   async function getOrCreateRouter(roomName) {
//     if (rooms.has(roomName)) return rooms.get(roomName).router;
    
//     const router = await worker.createRouter({ mediaCodecs });
//     console.log(`Created new router for room: ${roomName}`);
//     return router;
//   }

//   async function createWebRtcTransport(router) {
//     const transport = await router.createWebRtcTransport({
//       listenIps: [{ ip: '0.0.0.0', announcedIp: '127.0.0.1' }],
//       enableUdp: true,
//       enableTcp: true,
//       preferUdp: true,
//     });
    
//     transport.on('dtlsstatechange', (state) => {
//       if (state === 'closed') transport.close();
//     });
    
//     transport.on('close', () => {
//       console.log('Transport closed:', transport.id);
//     });
    
//     return transport;
//   }

//   function getProducerTransport(socketId) {
//     const peer = peers.get(socketId);
//     if (!peer) throw new Error('Peer not found');
    
//     for (const [id, transportData] of peer.transports) {
//       if (!transportData.consumer) return transportData.transport;
//     }
    
//     throw new Error('Producer transport not found');
//   }

//   function notifyNewProducer(roomName, socketId, producerId) {
//     if (!rooms.has(roomName)) return;
    
//     const room = rooms.get(roomName);
//     for (const peerId of room.peers) {
//       if (peerId !== socketId) {
//         const peer = peers.get(peerId);
//         peer.socket.emit('new-producer', { producerId });
//       }
//     }
//   }

//   function cleanupPeer(socketId) {
//     const peer = peers.get(socketId);
//     if (!peer) return;
    
//     // Cleanup transports
//     peer.transports.forEach(transportData => {
//       transportData.transport.close();
//     });
    
//     // Cleanup producers
//     peer.producers.forEach(producer => {
//       producer.close();
//     });
    
//     // Cleanup consumers
//     peer.consumers.forEach(consumer => {
//       consumer.close();
//     });
    
//     // Remove from room
//     if (peer.roomName && rooms.has(peer.roomName)) {
//       rooms.get(peer.roomName).peers.delete(socketId);
      
//       // Cleanup room if empty
//       if (rooms.get(peer.roomName).peers.size === 0) {
//         rooms.get(peer.roomName).router.close();
//         rooms.delete(peer.roomName);
//       }
//     }
//   }

//   function cleanupConsumer(socketId, consumerId) {
//     const peer = peers.get(socketId);
//     if (!peer) return;
    
//     const consumer = peer.consumers.get(consumerId);
//     if (consumer) {
//       consumer.close();
//       peer.consumers.delete(consumerId);
//     }
//   }

//   // Initialize worker
//   createWorker();
// }

/**
 * mediasoup/WorkerPool.js
 * 
 * REPLACES: mediasoup/mediasoup-setup.js
 * 
 * Multi-worker pool — 1 worker per CPU core.
 * Routers distributed round-robin across workers.
 */

import mediasoup from 'mediasoup';
import os from 'os';
import CONFIG from '../CONFIG.js';

const workers = [];
let nextWorkerIdx = 0;

// ── Create all workers ────────────────────────────────────────────────────
export async function createWorkerPool() {
  const numCPUs = os.cpus().length;
  console.log(`Creating ${numCPUs} mediasoup workers...`);

  for (let i = 0; i < numCPUs; i++) {
    await spawnWorker(i);
  }

  console.log(`Worker pool ready: ${workers.length} workers, PIDs: ${workers.map(w => w.pid).join(', ')}`);
}

async function spawnWorker(index) {
  // Each worker gets its own port range slice
  const portSlice = 1000;
  const minPort = CONFIG.mediasoup.rtcMinPort + index * portSlice;
  const maxPort = minPort + portSlice - 1;

  const worker = await mediasoup.createWorker({
    logLevel: process.env.NODE_ENV === 'production' ? 'warn' : 'debug',
    rtcMinPort: minPort,
    rtcMaxPort: maxPort,
  });

  console.log(`  Worker[${index}] PID: ${worker.pid} | ports: ${minPort}-${maxPort}`);

  worker.on('died', async () => {
    console.error(`Worker[${index}] PID ${worker.pid} died — restarting in 2s...`);
    const idx = workers.findIndex(w => w === worker);
    if (idx !== -1) workers.splice(idx, 1);

    setTimeout(async () => {
      const newWorker = await spawnWorker(index);
      workers.splice(idx, 0, newWorker);
      console.log(`Worker[${index}] restarted`);
    }, 2000);
  });

  workers.push(worker);
  return worker;
}

// ── Round-robin worker selection ──────────────────────────────────────────
export function getNextWorker() {
  if (workers.length === 0) throw new Error('No mediasoup workers available');
  const worker = workers[nextWorkerIdx % workers.length];
  nextWorkerIdx++;
  return worker;
}

// ── Create a Router on next available worker ──────────────────────────────
export async function createRouter() {
  const worker = getNextWorker();
  const router = await worker.createRouter({
    mediaCodecs: CONFIG.mediasoup.mediaCodecs,
  });
  console.log(`Router ${router.id} created on worker PID ${worker.pid}`);
  return router;
}

export function getWorkers() { return workers; }