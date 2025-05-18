import fs from 'fs';
import path from 'path';
import { exec } from 'child_process';
import express from 'express';


export default function recordingRoutes(app, baseDir) {
  const recordingsDir = path.join(baseDir, 'recordings');
  const mergedDir = path.join(baseDir, 'merged');

  // Ensure directories exist
  [recordingsDir, mergedDir].forEach(dir => {
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  });

  // Serve merged files
  app.use('/merged', express.static(mergedDir));

  // Video merging endpoint
  app.post('/merge-videos', async (req, res) => {
    try {
      // In a real app, you'd get these from the request or a database
      const inputPaths = [
        path.join(recordingsDir, 'recorded_czeOFvTo6_bcayUXAAAD.webm'),
        path.join(recordingsDir, 'recorded_GiX2tXRiWJCaKKffAAAB.webm'),
        path.join(recordingsDir, 'recorded_j_8Ryhuw3sMYOU6iAAAB.webm'),
        path.join(recordingsDir, 'recorded_XAQQZRfql3Ztf5qzAAAB.webm')
      ];
      
      // Check all input files exist
      for (const inputPath of inputPaths) {
        if (!fs.existsSync(inputPath)) {
          throw new Error(`Input file not found: ${inputPath}`);
        }
      }

      const outputPath = path.join(mergedDir, 'final_combined.webm');
      const ffmpegCommand = [
        'ffmpeg -y',
        ...inputPaths.map((p, i) => `-i ${p}`),
        `-filter_complex "[0:v][1:v][2:v][3:v]xstack=inputs=4:layout=0_0|w0_0|0_h0|w0_h0[v]"`,
        '-map "[v]"',
        '-c:v libvpx -crf 10 -b:v 4M',
        outputPath
      ].join(' ');

      exec(ffmpegCommand, (error, stdout, stderr) => {
        if (error) {
          console.error('FFmpeg error:', stderr);
          return res.status(500).json({ 
            error: 'Error merging videos', 
            details: stderr 
          });
        }
        
        res.json({ 
          url: `/merged/final_combined.webm`,
          success: true
        });
      });
    } catch (error) {
      console.error('Merge error:', error);
      res.status(400).json({ error: error.message });
    }
  });

  // Additional recording-related routes can be added here
}