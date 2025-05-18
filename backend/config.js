export default {
    MEDIASOUP: {
      WORKER: {
        RTC_MIN_PORT: 3000,
        RTC_MAX_PORT: 5050,
      },
      WEBRTC_TRANSPORT: {
        LISTEN_IPS: [
          { ip: '0.0.0.0', announcedIp: '127.0.0.1' }
        ],
        ENABLE_UDP: true,
        ENABLE_TCP: true,
        PREFER_UDP: true,
      },
      MEDIA_CODECS: [
        {
          kind: 'audio',
          mimeType: 'audio/opus',
          clockRate: 48000,
          channels: 2,
        },
        {
          kind: 'video',
          mimeType: 'video/VP8',
          clockRate: 90000,
          parameters: {
            'x-google-start-bitrate': 1000,
          },
        },
      ],
    },
    SERVER: {
      PORT: 3000,
      SSL: {
        KEY_PATH: './server/ssl/key.pem',
        CERT_PATH: './server/ssl/cert.pem',
      },
    },
    RECORDINGS: {
      DIR: 'recordings',
      MERGED_DIR: 'merged',
    },
  };