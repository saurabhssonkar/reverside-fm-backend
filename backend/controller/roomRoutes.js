/**
 * controller/roomRoutes.js
 */
import { Router } from 'express';
import jwt from 'jsonwebtoken';
import { v4 as uuidv4 } from 'uuid';
import { getRoom, getRooms, getRoomPeerList } from '../socket/RoomManager.js';
import { getRoomMeta, getRoomPeers } from '../Redis/RedisManager.js';
import CONFIG from '../CONFIG.js';

const router = Router();

// POST /api/rooms/create — generate a room id + JWT for the host
router.post('/create', async (req, res) => {
  try {
    const { displayName, password, maxParticipants } = req.body;
    const roomId = uuidv4().slice(0, 8);
    const token = jwt.sign(
      { roomId, displayName, role: 'host' },
      CONFIG.jwt.secret,
      { expiresIn: CONFIG.jwt.expiry },
    );
    res.json({ roomId, token, joinUrl: `/room/${roomId}` });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/rooms — list all active rooms (in-memory snapshot)
router.get('/', (req, res) => {
  const list = [];
  for (const [roomId, room] of getRooms()) {
    list.push({
      roomId,
      hostId:          room.hostId,
      peerCount:       room.peers.size,
      maxParticipants: room.maxParticipants,
      isLocked:        room.isLocked,
      createdAt:       room.createdAt,
    });
  }
  res.json(list);
});

// GET /api/rooms/:roomId — room detail (in-memory first, Redis fallback)
router.get('/:roomId', async (req, res) => {
  try {
    const { roomId } = req.params;
    const room = getRoom(roomId);
    if (room) {
      return res.json({
        roomId,
        hostId:          room.hostId,
        peerCount:       room.peers.size,
        maxParticipants: room.maxParticipants,
        isLocked:        room.isLocked,
        createdAt:       room.createdAt,
        peers:           getRoomPeerList(roomId),
      });
    }
    const meta = await getRoomMeta(roomId);
    if (!meta) return res.status(404).json({ error: 'Room not found' });
    res.json(meta);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/rooms/:roomId/participants — participant list (Redis-backed)
router.get('/:roomId/participants', async (req, res) => {
  try {
    const peers = await getRoomPeers(req.params.roomId);
    res.json({ participants: peers, count: peers.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

export default router;
