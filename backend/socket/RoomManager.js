/**
 * socket/RoomManager.js
 * 
 * Manages rooms in memory + syncs to Redis.
 */

import { createRouter } from '../mediasoup/WorkerPool.js';
import { setRoomMeta, getRoomMeta, deleteRoomMeta, addPeerToRoom, removePeerFromRoom } from '../Redis/RedisManager.js';

const rooms = new Map(); // roomId => { router, peers: Map, hostId, ... }

export async function getOrCreateRoom({ roomId, hostId, maxParticipants = 50, password = null }) {
  if (rooms.has(roomId)) return rooms.get(roomId);

  const router = await createRouter();
  const room = {
    id: roomId,
    router,
    peers: new Map(),
    hostId,
    maxParticipants,
    isLocked: !!password,
    password,
    createdAt: Date.now(),
  };

  rooms.set(roomId, room);
  await setRoomMeta(roomId, {
    id: roomId, hostId, maxParticipants,
    isLocked: room.isLocked, createdAt: room.createdAt, peerCount: 0,
  });

  console.log(`Room created: ${roomId}`);
  return room;
}

export function getRoom(roomId) {
  return rooms.get(roomId) || null;
}

export async function addPeer(roomId, socketId, info) {
  const room = rooms.get(roomId);
  if (!room) throw new Error(`Room not found: ${roomId}`);
  if (room.peers.size >= room.maxParticipants) throw new Error('Room full');

  room.peers.set(socketId, {
    socketId,
    name: info.name || 'Guest',
    isHost: info.isHost || false,
    joinedAt: Date.now(),
    isAudioMuted: false,
    isVideoOff: false,
    isScreenSharing: false,
  });

  await addPeerToRoom(roomId, socketId, info);
  return room;
}

export async function removePeer(roomId, socketId) {
  const room = rooms.get(roomId);
  if (!room) return null;

  room.peers.delete(socketId);
  await removePeerFromRoom(roomId, socketId);

  if (room.peers.size === 0) {
    room.router.close();
    rooms.delete(roomId);
    await deleteRoomMeta(roomId);
    console.log(`Room destroyed: ${roomId}`);
    return null;
  }

  // Reassign host if host left
  if (room.hostId === socketId) {
    const newHostId = [...room.peers.keys()][0];
    room.hostId = newHostId;
    if (room.peers.get(newHostId)) room.peers.get(newHostId).isHost = true;
    console.log(`New host: ${newHostId} in room ${roomId}`);
  }

  return room;
}

export function updatePeerStatus(roomId, socketId, updates) {
  const room = rooms.get(roomId);
  if (!room) return;
  const peer = room.peers.get(socketId);
  if (peer) Object.assign(peer, updates);
}

export function getRoomPeerList(roomId) {
  const room = rooms.get(roomId);
  return room ? [...room.peers.values()] : [];
}

export function getRooms() { return rooms; }


/**
 * socket/PeerManager.js
 * 
 * Per-peer transports, producers, consumers.
 */

const peers = new Map(); // socketId => peer object

export function createPeer(socketId, socket) {
  const peer = {
    socketId,
    socket,
    roomId: null,
    transports: new Map(),  // transportId => { transport, isConsumer }
    producers: new Map(),   // producerId => producer
    consumers: new Map(),   // consumerId => consumer
  };
  peers.set(socketId, peer);
  return peer;
}

export function getPeer(socketId) {
  return peers.get(socketId) || null;
}

export function addTransport(socketId, transport, isConsumer) {
  const peer = peers.get(socketId);
  if (!peer) throw new Error(`Peer not found: ${socketId}`);
  peer.transports.set(transport.id, { transport, isConsumer });
}

export function getProducerTransport(socketId) {
  const peer = peers.get(socketId);
  if (!peer) return null;
  for (const [, d] of peer.transports) {
    if (!d.isConsumer) return d.transport;
  }
  return null;
}

export function getConsumerTransport(socketId, transportId) {
  const peer = peers.get(socketId);
  if (!peer) return null;
  const d = peer.transports.get(transportId);
  return d?.isConsumer ? d.transport : null;
}

export function addProducer(socketId, producer) {
  const peer = peers.get(socketId);
  if (!peer) throw new Error(`Peer not found: ${socketId}`);
  peer.producers.set(producer.id, producer);
}

export function addConsumer(socketId, consumer) {
  const peer = peers.get(socketId);
  if (!peer) throw new Error(`Peer not found: ${socketId}`);
  peer.consumers.set(consumer.id, consumer);
}

export function removeConsumer(socketId, consumerId) {
  const peer = peers.get(socketId);
  if (!peer) return;
  const c = peer.consumers.get(consumerId);
  if (c) { try { c.close(); } catch (_) {} peer.consumers.delete(consumerId); }
}

export function cleanupPeer(socketId) {
  const peer = peers.get(socketId);
  if (!peer) return;
  for (const c of peer.consumers.values()) { try { c.close(); } catch (_) {} }
  for (const p of peer.producers.values()) { try { p.close(); } catch (_) {} }
  for (const { transport } of peer.transports.values()) { try { transport.close(); } catch (_) {} }
  peers.delete(socketId);
  console.log(`Peer cleaned up: ${socketId}`);
}

export function getPeers() { return peers; }