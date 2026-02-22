/**
 * mediasoup/TransportManager.js
 */
import CONFIG from '../CONFIG.js';

export async function createWebRtcTransport(router) {
  const transport = await router.createWebRtcTransport({
    listenIps: [{ ip: '0.0.0.0', announcedIp: CONFIG.server.announcedIp }],
    enableUdp: true,
    enableTcp: true,
    preferUdp: true,
    initialAvailableOutgoingBitrate: 1000000,
  });

  transport.on('dtlsstatechange', state => {
    if (state === 'closed') transport.close();
  });

  return transport;
}
