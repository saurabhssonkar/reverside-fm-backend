import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const CONFIG = {
  server: {
    port:        parseInt(process.env.PORT || '3000', 10),
    frontendUrl: process.env.FRONTEND_URL || 'https://localhost:5173',
    announcedIp: process.env.ANNOUNCED_IP  || '127.0.0.1',
    sslKey:      process.env.SSL_KEY  || path.join(__dirname, 'server', 'ssl', 'key.pem'),
    sslCert:     process.env.SSL_CERT || path.join(__dirname, 'server', 'ssl', 'cert.pem'),
  },

  mediasoup: {
    rtcMinPort:  parseInt(process.env.RTC_MIN_PORT || '10000', 10),
    rtcMaxPort:  parseInt(process.env.RTC_MAX_PORT || '59999', 10),
    mediaCodecs: [
      {
        kind:      'audio',
        mimeType:  'audio/opus',
        clockRate: 48000,
        channels:  2,
      },
      {
        kind:       'video',
        mimeType:   'video/VP8',
        clockRate:  90000,
        parameters: { 'x-google-start-bitrate': 1000 },
      },
      {
        kind:       'video',
        mimeType:   'video/VP9',
        clockRate:  90000,
        parameters: { 'profile-id': 2, 'x-google-start-bitrate': 1000 },
      },
      {
        kind:       'video',
        mimeType:   'video/h264',
        clockRate:  90000,
        parameters: {
          'packetization-mode':      1,
          'profile-level-id':        '4d0032',
          'level-asymmetry-allowed': 1,
          'x-google-start-bitrate':  1000,
        },
      },
    ],
  },

  redis: {
    host:     process.env.REDIS_HOST     || '127.0.0.1',
    port:     parseInt(process.env.REDIS_PORT || '6379', 10),
    password: process.env.REDIS_PASSWORD || '',
  },

  kafka: {
    clientId: process.env.KAFKA_CLIENT_ID || 'reverside-backend',
    brokers:  (process.env.KAFKA_BROKERS  || 'localhost:9092').split(','),
  },

  aws: {
    region:          process.env.AWS_REGION          || 'us-east-1',
    accessKeyId:     process.env.AWS_ACCESS_KEY_ID   || '',
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '',
    bucketName:      process.env.AWS_S3_BUCKET        || '',
  },

  jwt: {
    secret: process.env.JWT_SECRET || 'change-me-in-production',
    expiry: process.env.JWT_EXPIRY  || '7d',
  },

  recording: {
    // Trigger an S3 multipart upload part when buffered chunks exceed this size (5 MB default)
    chunkThreshold: parseInt(process.env.RECORDING_CHUNK_THRESHOLD || String(5 * 1024 * 1024), 10),
  },
};

export default CONFIG;
