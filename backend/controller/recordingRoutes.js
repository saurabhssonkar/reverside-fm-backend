/**
 * controller/recordingRoutes.js
 */
import { Router } from 'express';
import { completeStream, abortStream } from '../recording/RecordingService.js';
import { getStreamMeta } from '../Redis/RedisManager.js';
// S3 signed URL generation (not yet implemented in S3Manager)

const router = Router();

router.post('/complete', async (req, res) => {
  try {
    const { cameraId } = req.body;
    if (!cameraId) return res.status(400).json({ error: 'cameraId required' });
    const url = await completeStream(cameraId);
    res.json({ status: 'completed', url });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.post('/abort', async (req, res) => {
  try {
    const { cameraId } = req.body;
    if (!cameraId) return res.status(400).json({ error: 'cameraId required' });
    await abortStream(cameraId);
    res.json({ status: 'aborted' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.get('/:cameraId/url', async (req, res) => {
  try {
    const meta = await getStreamMeta(req.params.cameraId);
    if (!meta || meta.status !== 'COMPLETED') {
      return res.status(404).json({ error: 'Recording not complete' });
    }
    // TODO: implement getSignedUrl in S3Manager.js
    res.json({ s3Key: meta.s3Key, uploadUrl: meta.uploadUrl || null });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

export default router;