/**
 * integrating mediasoup server with a node.js application
 */

/* Please follow mediasoup installation requirements */
/* https://mediasoup.org/documentation/v3/mediasoup/installation/ */
import express from 'express'
import https from 'httpolyglot'
import fs from 'fs'
import path from 'path'
import { exec } from 'child_process';
import { fileURLToPath } from 'url';
import { sendToKafka } from './video-uploader/kafkaProducer.js';
import { startKafkaConsumer } from './video-uploader/kafkaConsumer.js';
import redis from './video-uploader/redisClient.js';
import { v4 as uuidv4 } from 'uuid';
// const cors = require('cors');
import { Server } from 'socket.io'
import mediasoup from 'mediasoup'
import { resetInactivityTimer } from './utils/resetInactivityTimer.js'
import AWS from 'aws-sdk';
import dotenv from 'dotenv';
import { initialize, processVideoChunk, startConsumer, startStreamForCamera } from './StreamVideo/VideoStreamProcessor.js';
import { completeMultipartUpload, initializeMultipartUpload } from './aws/S3Manager.js';




const app = express();
dotenv.config()

  const videoChunkBuffers = new Map();


const __dirname = path.resolve()
// startKafkaConsumer();
// const s3 = new AWS.S3();
const s3 = new AWS.S3({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  region: process.env.AWS_REGION,
  httpOptions: {
    timeout: 60000 // 1 minute timeout
  }
});



// app.use(cors());
app.use(express.static('merged')); // serve merged video
app.use(express.json());

const recordingsDir = path.join(__dirname, 'recordings');

// Ensure the recordings folder exists
if (!fs.existsSync(recordingsDir)) {
  fs.mkdirSync(recordingsDir, { recursive: true });
}


// app.get('*', (req, res, next) => {
//   const path = '/sfu/'

//   if (req.path.indexOf(path) == 0 && req.path.length > path.length) return next()

//   res.send(`You need to specify a room name in the path e.g. 'https://127.0.0.1/sfu/room'`)
// })

// app.use('/sfu/:room', express.static(path.join(__dirname, 'public')))

// SSL cert for HTTPS access
const options = {
  key: fs.readFileSync('./server/ssl/key.pem', 'utf-8'),
  cert: fs.readFileSync('./server/ssl/cert.pem', 'utf-8')
}

const httpsServer = https.createServer(options, app)
httpsServer.listen(3000, async () => {
  console.log('listening on port: ' + 3000)

   try {
    await initialize();  // custom function you want to run after server starts
       // initialize socket.io or any other real-time service

    console.log('Server fully initialized.');
  } catch (error) {
    console.error('Initialization error:', error);
    process.exit(1); // optional: stop the process if initialization fails
  }
})

const io = new Server(httpsServer, {
  cors: {
    origin: "http://localhost:8081", // adjust this based on your frontend
    methods: ["GET", "POST"],
    credentials: true,
  }
})

// socket.io namespace (could represent a room?)
const connections = io.of('/mediasoup')

/**
 * Worker
 * |-> Router(s)
 *     |-> Producer Transport(s)
 *         |-> Producer
 *     |-> Consumer Transport(s)
 *         |-> Consumer 
 **/
let worker
let rooms = {}          // { roomName1: { Router, rooms: [ sicketId1, ... ] }, ...}
let peers = {}          // { socketId1: { roomName1, socket, transports = [id1, id2,] }, producers = [id1, id2,] }, consumers = [id1, id2,], peerDetails }, ...}
let transports = []     // [ { socketId1, roomName1, transport, consumer }, ... ]
let producers = []      // [ { socketId1, roomName1, producer, }, ... ]
let consumers = []      // [ { socketId1, roomName1, consumer, }, ... ]

const createWorker = async () => {
  worker = await mediasoup.createWorker({
    rtcMinPort: 3000,
    rtcMaxPort: 5050,
  })
  console.log(`worker pid ${worker.pid}`)

  worker.on('died', error => {
    // This implies something serious happened, so kill the application
    console.error('mediasoup worker has died')
    setTimeout(() => process.exit(1), 2000) // exit in 2 seconds
  })

  return worker
}

// We create a Worker as soon as our application starts
worker = createWorker()

// This is an Array of RtpCapabilities
// https://mediasoup.org/documentation/v3/mediasoup/rtp-parameters-and-capabilities/#RtpCodecCapability
// list of media codecs supported by mediasoup ...
// https://github.com/versatica/mediasoup/blob/v3/src/supportedRtpCapabilities.ts
const mediaCodecs = [
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
]



connections.on('connection', async socket => {
  console.log(socket.id)
  socket.emit('connection-success', {
    socketId: socket.id,
  })

  const removeItems = (items, socketId, type) => {
    items.forEach(item => {
      if (item.socketId === socket.id) {
        item[type].close()
      }
    })
    items = items.filter(item => item.socketId !== socket.id)

    return items
  }

  socket.on('disconnect', async() => {
    // do some cleanup
    console.log('peer disconnected')
    consumers = removeItems(consumers, socket.id, 'consumer')
    producers = removeItems(producers, socket.id, 'producer')
    transports = removeItems(transports, socket.id, 'transport')
    // completeUpload(socket.id).catch(console.error);
    startConsumer(1)

    const { roomName } = peers[socket.id]
    delete peers[socket.id]

    // remove socket from room
    rooms[roomName] = {
      router: rooms[roomName].router,
      peers: rooms[roomName].peers.filter(socketId => socketId !== socket.id)
    }
  })

  socket.on('joinRoom', async ({ roomName }, callback) => {
    // create Router if it does not exist
    // const router1 = rooms[roomName] && rooms[roomName].get('data').router || await createRoom(roomName, socket.id)
    const router1 = await createRoom(roomName, socket.id)

    peers[socket.id] = {
      socket,
      roomName,           // Name for the Router this Peer joined
      transports: [],
      producers: [],
      consumers: [],
      peerDetails: {
        name: '',
        isAdmin: false,   // Is this Peer the Admin?
      }
    }
    console.log("peers",peers)

    // get Router RTP Capabilities
    const rtpCapabilities = router1.rtpCapabilities

    // call callback from the client and send back the rtpCapabilities
    callback({ rtpCapabilities })
  })

  const createRoom = async (roomName, socketId) => {
    // worker.createRouter(options)
    // options = { mediaCodecs, appData }
    // mediaCodecs -> defined above
    // appData -> custom application data - we are not supplying any
    // none of the two are required

    let router1
    let peers = []
    if (rooms[roomName]) {
      router1 = rooms[roomName].router
      peers = rooms[roomName].peers || []
    } else {
      router1 = await worker.createRouter({ mediaCodecs, })
    }

    console.log(`Router ID: ${router1.id}`, peers.length)

    rooms[roomName] = {
      router: router1,
      peers: [...peers, socketId],
    }

    return router1
  }

  // socket.on('createRoom', async (callback) => {
  //   if (router === undefined) {
  //     // worker.createRouter(options)
  //     // options = { mediaCodecs, appData }
  //     // mediaCodecs -> defined above
  //     // appData -> custom application data - we are not supplying any
  //     // none of the two are required
  //     router = await worker.createRouter({ mediaCodecs, })
  //     console.log(`Router ID: ${router.id}`)
  //   }

  //   getRtpCapabilities(callback)
  // })

  // const getRtpCapabilities = (callback) => {
  //   const rtpCapabilities = router.rtpCapabilities

  //   callback({ rtpCapabilities })
  // }

  // Client emits a request to create server side Transport
  // We need to differentiate between the producer and consumer transports
  socket.on('createWebRtcTransport', async ({ consumer }, callback) => {
    // get Room Name from Peer's properties
    const roomName = peers[socket.id].roomName
    // console.log("saurabh",roomName)

    // get Router (Room) object this peer is in based on RoomName
    const router = rooms[roomName].router


    createWebRtcTransport(router).then(
      transport => {
        callback({
          params: {
            id: transport.id,
            iceParameters: transport.iceParameters,
            iceCandidates: transport.iceCandidates,
            dtlsParameters: transport.dtlsParameters,
          }
        })

        // add transport to Peer's properties
        addTransport(transport, roomName, consumer)
      },
      error => {
        console.log(error)
      })
  })

  const addTransport = (transport, roomName, consumer) => {

    transports = [
      ...transports,
      { socketId: socket.id, transport, roomName, consumer, }
    ]

    peers[socket.id] = {
      ...peers[socket.id],
      transports: [
        ...peers[socket.id].transports,
        transport.id,
      ]
    }
  }

  // const addProducer = (producer, roomName) => {
  //   producers = [
  //     ...producers,
  //     { socketId: socket.id, producer, roomName, }
  //   ]
  //   console.log("@@",producers)


  //   peers[socket.id] = {
  //     ...peers[socket.id],
  //     producers: [
  //       ...peers[socket.id].producers,
  //       producer.id,
  //     ]
  //   }
  // }

  const addProducer = (producer, roomName) => {
  // Check if a producer with the same socket ID already exists
  const existingIndex = producers.findIndex(p => p.socketId === socket.id);
  
  if (existingIndex === -1) {
    // If not exists, add new producer
    producers = [
      ...producers,
      { socketId: socket.id, producer, roomName }
    ];
  } else {
    // If exists, update the existing producer
    producers = producers.map((p, index) => 
      index === existingIndex ? { ...p, producer, roomName } : p
    );
  }

  console.log("@@", producers);

  // Update peers object
  peers[socket.id] = {
    ...peers[socket.id],
    producers: [
      ...(peers[socket.id]?.producers || []),
      producer.id,
    ]
  };
};

  const addConsumer = (consumer, roomName) => {
    // add the consumer to the consumers list
    consumers = [
      ...consumers,
      { socketId: socket.id, consumer, roomName, }
    ]

    // add the consumer id to the peers list
    peers[socket.id] = {
      ...peers[socket.id],
      consumers: [
        ...peers[socket.id].consumers,
        consumer.id,
      ]
    }
  }

  socket.on('getProducers', callback => {
    //return all producer transports
    const { roomName } = peers[socket.id]

    let producerList = []
    console.log("####PRODUCER",producers)
        console.log("####SOCKETID",socket.id)

    producers.forEach(producerData => {
      if (producerData.socketId !== socket.id && producerData.roomName === roomName) {
        console.log("NOT INSIDE" ,producerData.socketId, socket.id)
        producerList = [...producerList, producerData.producer.id]
      }
      console.log("OUT SIDE")
    })
    console.log("producerList",producerList)

    // return the producer list back to the client
    callback(producerList)
  })

  const informConsumers = (roomName, socketId, id) => {
    console.log(`just joined, id ${id} ${roomName}, ${socketId}`)
    // A new producer just joined
    // let all consumers to consume this producer
    producers.forEach(producerData => {
      if (producerData.socketId !== socketId && producerData.roomName === roomName) {
        const producerSocket = peers[producerData.socketId].socket
        // use socket to send producer id to producer
        producerSocket.emit('new-producer', { producerId: id })
      }
    })
  }

  const getTransport = (socketId) => {
    const [producerTransport] = transports.filter(transport => transport.socketId === socketId && !transport.consumer)
    return producerTransport.transport
  }

  // see client's socket.emit('transport-connect', ...)
  socket.on('transport-connect', ({ dtlsParameters }) => {
    console.log('DTLS PARAMS... ', { dtlsParameters })

    getTransport(socket.id).connect({ dtlsParameters })
  })

  // see client's socket.emit('transport-produce', ...)
  socket.on('transport-produce', async ({ kind, rtpParameters, appData }, callback) => {
    // call produce based on the prameters from the client
    const producer = await getTransport(socket.id).produce({
      kind,
      rtpParameters,
    })
    console.log("saurabh",producer)

    // add producer to the producers array
    const { roomName } = peers[socket.id]

    addProducer(producer, roomName)

    informConsumers(roomName, socket.id, producer.id)

    console.log('Producer ID: ', producer.id, producer.kind)

    producer.on('transportclose', () => {
      console.log('transport for this producer closed ')
      producer.close()
    })

    // Send back to the client the Producer's id
    callback({
      id: producer.id,
      producersExist: producers.length > 1 ? true : false
    })
  })

  // see client's socket.emit('transport-recv-connect', ...)
  socket.on('transport-recv-connect', async ({ dtlsParameters, serverConsumerTransportId }) => {
    console.log(`DTLS PARAMS: ${dtlsParameters}`)
    const consumerTransport = transports.find(transportData => (
      transportData.consumer && transportData.transport.id == serverConsumerTransportId
    )).transport
    await consumerTransport.connect({ dtlsParameters })
  })

  socket.on('consume', async ({ rtpCapabilities, remoteProducerId, serverConsumerTransportId }, callback) => {
    try {

      const { roomName } = peers[socket.id]
      const router = rooms[roomName].router
      let consumerTransport = transports.find(transportData => (
        transportData.consumer && transportData.transport.id == serverConsumerTransportId
      )).transport

      // check if the router can consume the specified producer
      if (router.canConsume({
        producerId: remoteProducerId,
        rtpCapabilities
      })) {
        // transport can now consume and return a consumer
        const consumer = await consumerTransport.consume({
          producerId: remoteProducerId,
          rtpCapabilities,
          paused: true,
        })

        consumer.on('transportclose', () => {
          console.log('transport close from consumer')
        })

        consumer.on('producerclose', () => {
          console.log('producer of consumer closed')
          socket.emit('producer-closed', { remoteProducerId })

          consumerTransport.close([])
          transports = transports.filter(transportData => transportData.transport.id !== consumerTransport.id)
          consumer.close()
          consumers = consumers.filter(consumerData => consumerData.consumer.id !== consumer.id)
        })

        addConsumer(consumer, roomName)

        // from the consumer extract the following params
        // to send back to the Client
        const params = {
          id: consumer.id,
          producerId: remoteProducerId,
          kind: consumer.kind,
          rtpParameters: consumer.rtpParameters,
          serverConsumerId: consumer.id,
        }

        // send the parameters to the client
        callback({ params })
      }
    } catch (error) {
      console.log(error.message)
      callback({
        params: {
          error: error
        }
      })
    }
  })

  socket.on('consumer-resume', async ({ serverConsumerId }) => {
    console.log('consumer resume')
    const { consumer } = consumers.find(consumerData => consumerData.consumer.id === serverConsumerId)
    await consumer.resume()
  })
  const filePath = path.join(__dirname, `recordings/recorded_${socket.id}.webm`);
  let uploadInfo = null;
  let partNumber = 1;
  const INACTIVITY_TIMEOUT = 30000; // 30 seconds timeout
  let inactivityTimer;
  const partETags = [];



  // socket.on('recording-chunk', async (arrayBuffer) => {

  //   // // You can store this chunk or write it to a file
  //   console.log(`📦 Received chunk (${arrayBuffer.length} bytes)`);

  //   // const buffer = Buffer.from(arrayBuffer);
  //   // fs.appendFile(filePath, buffer, (err) => {
  //   //   if (err){
  //   //     console.error('Error saving chunk:', err);

  //   //   } else{
  //       // console.log(`check`);

  //   //   }
  //   // });

  //   // if (!uploadInfo) {
  //   //   // console.log("saurabh")
  //   //   const fileKey = `recordings/${uuidv4()}.webm`;
  //   //   const res = await s3.createMultipartUpload({
  //   //     Bucket: process.env.AWS_S3_BUCKET,
  //   //     Key: fileKey
  //   //   }).promise();

  //   //   uploadInfo = { uploadId: res.UploadId, key: fileKey };
  //   //   // console.log("uploadInfo",uploadInfo)
  //   //   await redis.set(`${socket.id}-upload`, JSON.stringify(uploadInfo));
  //   //   // console.log('🚀 Multipart upload started:', fileKey);
  //   // }

  //   // const chunk = Buffer.from(arrayBuffer).toString('base64');
  //   // await sendToKafka('video-chunks', {
  //   //   uploadId: uploadInfo.uploadId,
  //   //   key: uploadInfo.key,
  //   //   partNumber,
  //   //   chunk
  //   // });

  //   // console.log(`📤 Sent part ${partNumber}`);
  //   partNumber++;

  //   await resetInactivityTimer(inactivityTimer,uploadInfo,redis,s3); // update timer

  // });

  const initializedStreams = new Set();



socket.on('recording-chunk', async (arrayBuffer) => {
  try {

   const cameraId = "camera1"; // or derive dynamically
    const socketId = socket.id;
    // console.log("socketId",socketId)

    // Initialize only once per camera
    if (!initializedStreams.has(cameraId)) {
      await startStreamForCamera(cameraId);
      initializedStreams.add(cameraId);
    } 

    const bufferState = videoChunkBuffers.get(socketId) || { chunks: [], totalSize: 0 };

    bufferState.chunks.push(arrayBuffer);
    bufferState.totalSize += arrayBuffer.byteLength;

    // Update buffer state in map
    videoChunkBuffers.set(socketId, bufferState);


    // If 5MB or more, combine and send to processVideoChunk
    if (bufferState.totalSize >= 5 * 1024 * 1024) {
      const combinedBuffer = mergeChunks(bufferState.chunks, bufferState.totalSize);
      console.log("Is Buffer:", Buffer.isBuffer(combinedBuffer)); // ✅ should be true

          console.log("Current buffered size:", (bufferState.totalSize / (1024 * 1024)).toFixed(2), "MB");


      // Call your chunk processor
      await processVideoChunk(cameraId, combinedBuffer);
    

      // Reset buffer for this socket
      videoChunkBuffers.set(socketId, { chunks: [], totalSize: 0 });
    }
  
    // Get current upload state or create new one
  
  } catch (error) {
    console.error('❌ Upload error:', error);
    await abortUpload(socket.id);
  }
});



// socket.on('disconnect', () => {
//   console.log('Client disconnected - completing upload');
//   completeUpload(socket.id).catch(console.error);
// });


})

const createWebRtcTransport = async (router) => {
  return new Promise(async (resolve, reject) => {
    try {
      // https://mediasoup.org/documentation/v3/mediasoup/api/#WebRtcTransportOptions
      const webRtcTransport_options = {
        listenIps: [
          {
            ip: '0.0.0.0', // replace with relevant IP address
            announcedIp: '127.0.0.1',
          }
        ],
        enableUdp: true,
        enableTcp: true,
        preferUdp: true,
      }

      // https://mediasoup.org/documentation/v3/mediasoup/api/#router-createWebRtcTransport
      let transport = await router.createWebRtcTransport(webRtcTransport_options)
      console.log(`transport id: ${transport.id}`)

      transport.on('dtlsstatechange', dtlsState => {
        if (dtlsState === 'closed') {
          transport.close()
        }
      })

      transport.on('close', () => {
        console.log('transport closed')
      })

      resolve(transport)

    } catch (error) {
      reject(error)
    }
  })
}



const __filename = fileURLToPath(import.meta.url);



// Make sure the "merged" folder exists
const mergedDir = path.join(__dirname, 'merged');
if (!fs.existsSync(mergedDir)) {
  fs.mkdirSync(mergedDir, { recursive: true });
}
app.use(express.static(mergedDir));


app.post('/merge-videos', async (req, res) => {
  // Hard-coded inputs—replace or parameterize as needed
  const inputPaths = [
    path.join(__dirname, 'recordings/recorded_czeOFvTo6_bcayUXAAAD.webm'),
    path.join(__dirname, 'recordings/recorded_GiX2tXRiWJCaKKffAAAB.webm'),
    path.join(__dirname, 'recordings/recorded_j_8Ryhuw3sMYOU6iAAAB.webm'),
    path.join(__dirname, 'recordings/recorded_XAQQZRfql3Ztf5qzAAAB.webm')
  ];
  const outputPath = path.join(mergedDir, 'final_combined.webm');

  // Explicit 2×2 layout: [0] top-left, [1] top-right, [2] bottom-left, [3] bottom-right
  const ffmpegCommand = [
    'ffmpeg -y',
    `-i ${inputPaths[0]}`,
    `-i ${inputPaths[1]}`,
    `-i ${inputPaths[2]}`,
    `-i ${inputPaths[3]}`,
    `-filter_complex "[0:v][1:v][2:v][3:v]xstack=inputs=4:layout=0_0|w0_0|0_h0|w0_h0[v]"`,
    '-map "[v]"',
    '-c:v libvpx -crf 10 -b:v 4M',
    outputPath
  ].join(' ');

  exec(ffmpegCommand, (error, stdout, stderr) => {
    if (error) {
      console.error('FFmpeg error:', stderr);
      return res.status(500).json({ error: 'Error merging videos', details: stderr });
    }
    console.log('FFmpeg succeeded:', stdout);
    // Serve static from /merged, so URL is:
    res.json({ url: `http://localhost:3000/final_combined.webm` });
  });
});

app.post('/completeupload', async (req, res) => {
  try {
    const { uploadId, s3Key } = req.body;

    if (!uploadId || !s3Key) {
      return res.status(400).json({ error: 'uploadId and s3Key are required' });
    }

    await completeMultipartUpload(uploadId, s3Key);
    res.json({ status: "OK" });

  } catch (e) {
    console.error("Error in /completeupload:", e);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});


// Serve the merged folder publicly


// import express from 'express';
// import https from 'httpolyglot';
// import fs from 'fs';
// import path from 'path';
// import { fileURLToPath } from 'url';
// import { Server } from 'socket.io';
// import mediasoupSetup from './mediasoup/mediasoup-setup.js';
// import recordingRoutes from './controller/recording-routes.js';

// const __dirname = path.dirname(fileURLToPath(import.meta.url));
// const app = express();

// // SSL configuration
// const options = {
//   key: fs.readFileSync('./server/ssl/key.pem', 'utf-8'),
//   cert: fs.readFileSync('./server/ssl/cert.pem', 'utf-8')
// };

// // Create HTTPS server
// const httpsServer = https.createServer(options, app);
// httpsServer.listen(3000, () => {
//   console.log('Server listening on port: 3000');
// });

// // Configure Socket.IO
// const io = new Server(httpsServer, {
//   cors: {
//     origin: "http://localhost:5173",
//     methods: ["GET", "POST"],
//     credentials: true,
//   }
// });

// // Initialize Mediasoup
// mediasoupSetup(io);

// // Setup recording routes
// recordingRoutes(app, __dirname);

// // Serve static files
// app.use(express.static('public'));
// app.use(express.json());

function mergeChunks(chunks, totalSize) {
  const temp = Buffer.alloc(totalSize);
  let offset = 0;
  for (const chunk of chunks) {
    const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    buf.copy(temp, offset);
    offset += buf.length;
  }
  return temp;
}

