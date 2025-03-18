import { Router } from "mediasoup/node/lib/types";
import { WebRtcTransport } from "mediasoup/node/lib/types";
import { Producer } from "mediasoup/node/lib/types";
import { Consumer } from "mediasoup/node/lib/types";
import { Worker } from "mediasoup/node/lib/types";
import { RtpCapabilities } from "mediasoup/node/lib/types";
import { DtlsParameters } from "mediasoup/node/lib/types";
import { Socket } from "socket.io";
import cors from 'cors'

import express from "express";
import http from "http";
import { Server } from "socket.io";
import * as mediasoup from "mediasoup";

interface TransportDirection {
  send: string;
  recv: string;
}

interface CustomSocket extends Socket {
  roomId?: string;
  transports?: TransportDirection;
  producerIds?: string[];
}

interface Room {
  id: string;
  participants: Map<string, string[]>; // socketId -> producerIds
}

const app = express();
const server = http.createServer(app);
const io = new Server(server,{
  cors:{
    origin: "http://localhost:3001",
    methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD", "PATCH"],
    credentials: true,
  }
});

app.use(express.static("public"));
app.use(
  cors({
    origin: "http://localhost:3001",
    methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD","PATCH"],
    credentials: true,
    preflightContinue: false,
    optionsSuccessStatus: 204,
  })
);

const transports = new Map<string, WebRtcTransport>();
const producers = new Map<string, Producer>();
const consumers = new Map<string, Consumer>();
const rooms = new Map<string, Room>();

let worker: Worker;
let router: Router;

async function startMediasoup() {
  worker = await mediasoup.createWorker({
    logLevel: 'warn',
    rtcMinPort: 10000,
    rtcMaxPort: 10100
  });
  
  console.log('Mediasoup worker created');
  
  router = await worker.createRouter({
    mediaCodecs: [
      { 
        kind: "audio", 
        mimeType: "audio/opus", 
        clockRate: 48000, 
        channels: 2 
      },
      { 
        kind: "video", 
        mimeType: "video/VP8", 
        clockRate: 90000 
      },
      { 
        kind: "video", 
        mimeType: "video/H264", 
        clockRate: 90000, 
        parameters: {
          'packetization-mode': 1,
          'profile-level-id': '42e01f',
          'level-asymmetry-allowed': 1
        } 
      }
    ],
  });
  
  console.log('Mediasoup router created');
}

startMediasoup().catch(error => {
  console.error('Failed to start mediasoup:', error);
  process.exit(1);
});

function getOrCreateRoom(roomId: string): Room {
  let room = rooms.get(roomId);
  if (!room) {
    room = { 
      id: roomId, 
      participants: new Map<string, string[]>() 
    };
    rooms.set(roomId, room);
    console.log(`Room created: ${roomId}`);
  }
  return room;
}

function notifyNewProducer(roomId: string, socketId: string, producerId: string): void {
  const room = rooms.get(roomId);
  if (room) {
    room.participants.forEach((_, participantId) => {
      if (participantId !== socketId) {
        io.to(participantId).emit("newProducer", {
          producerId: producerId,
          producerSocketId: socketId
        });
      }
    });
  }
}

io.on("connection", (socket: CustomSocket) => {
  console.log("New client connected:", socket.id);

  socket.on("joinRoom", async (roomId: string, callback: (data: { routerRtpCapabilities: RtpCapabilities }) => void) => {
    socket.join(roomId);
    socket.roomId = roomId;
    
    const room = getOrCreateRoom(roomId);
    room.participants.set(socket.id, []);
    
    console.log(`Socket ${socket.id} joined room ${roomId}`);
    
    callback({ routerRtpCapabilities: router.rtpCapabilities });
    
    room.participants.forEach((producerIds, participantId) => {
      if (participantId !== socket.id && producerIds.length > 0) {
        producerIds.forEach(producerId => {
          socket.emit("newProducer", {
            producerId: producerId,
            producerSocketId: participantId
          });
        });
      }
    });
  });

  socket.on("createTransport", async (direction: 'send' | 'recv', callback: (transportOptions: any) => void) => {
    try {
      const transport = await router.createWebRtcTransport({
        listenIps: [{ ip: "0.0.0.0", announcedIp: "127.0.0.1" }],
        enableUdp: true,
        enableTcp: true,
        preferUdp: true,
        initialAvailableOutgoingBitrate: 1000000,
      });
      
      console.log(`Transport created for ${socket.id} direction: ${direction}`);
      
      transports.set(transport.id, transport);
      
      socket.transports = socket.transports || { send: '', recv: '' };
      socket.transports[direction] = transport.id;
      
      callback({
        id: transport.id,
        iceParameters: transport.iceParameters,
        iceCandidates: transport.iceCandidates,
        dtlsParameters: transport.dtlsParameters,
      });
      
      transport.on('routerclose', () => {
        transport.close();
        transports.delete(transport.id);
      });
    } catch (error) {
      console.error('Error creating transport:', error);
      callback({ error: 'Failed to create transport' });
    }
  });

  socket.on(
    "connectTransport",
    async (
      { transportId, dtlsParameters }: { transportId: string; dtlsParameters: DtlsParameters },
      callback: (error?: { error: string }) => void
    ) => {
      const transport = transports.get(transportId);
      if (!transport) {
        return callback({ error: "Transport not found" });
      }
      
      try {
        await transport.connect({ dtlsParameters });
        console.log(`Transport ${transportId} connected`);
        callback();
      } catch (error) {
        console.error('Error connecting transport:', error);
        callback({ error: 'Failed to connect transport' });
      }
    }
  );

  socket.on(
    "produce",
    async (
      { transportId, kind, rtpParameters }: { transportId: string; kind: "audio" | "video"; rtpParameters: any },
      callback: (data: { id: string } | { error: string }) => void
    ) => {
      const transport = transports.get(transportId);
      if (!transport) {
        return callback({ error: "Transport not found" });
      }
      
      try {
        const producer = await transport.produce({ kind, rtpParameters });
        producers.set(producer.id, producer);
        
        socket.producerIds = socket.producerIds || [];
        socket.producerIds.push(producer.id);
        
        if (socket.roomId) {
          const room = rooms.get(socket.roomId);
          if (room) {
            const participantProducers = room.participants.get(socket.id) || [];
            participantProducers.push(producer.id);
            room.participants.set(socket.id, participantProducers);
            
            notifyNewProducer(socket.roomId, socket.id, producer.id);
          }
        }
        
        console.log(`Producer created: ${producer.id}, kind: ${kind}`);
        
        callback({ id: producer.id });
        
        producer.on('transportclose', () => {
          producer.close();
          producers.delete(producer.id);
        });
      } catch (error) {
        console.error('Error producing:', error);
        callback({ error: 'Failed to produce' });
      }
    }
  );

  socket.on(
    "consume",
    async (
      { transportId, producerId, rtpCapabilities }: 
      { transportId: string; producerId: string; rtpCapabilities: RtpCapabilities },
      callback: (data: any) => void
    ) => {
      const transport = transports.get(transportId);
      if (!transport) {
        return callback({ error: "Transport not found" });
      }
      
      if (!router.canConsume({ producerId, rtpCapabilities })) {
        return callback({ error: "Cannot consume this producer" });
      }
      
      try {
        const consumer = await transport.consume({
          producerId,
          rtpCapabilities,
          paused: true, 
        });
        
        consumers.set(consumer.id, consumer);
        
        console.log(`Consumer created: ${consumer.id} for producer: ${producerId}`);
        
        callback({
          id: consumer.id,
          producerId,
          kind: consumer.kind,
          rtpParameters: consumer.rtpParameters,
        });
        
        consumer.on('producerclose', () => {
          consumer.close();
          consumers.delete(consumer.id);
          socket.emit('producerClosed', { producerId });
        });
      } catch (error) {
        console.error('Error consuming:', error);
        callback({ error: 'Failed to consume' });
      }
    }
  );

  socket.on('resumeConsumer', async (consumerId: string, callback: () => void) => {
    const consumer = consumers.get(consumerId);
    if (consumer) {
      await consumer.resume();
      callback();
    }
  });



socket.on('closeProducer', async ({ producerId }: { producerId: string }) => {
  const producer = producers.get(producerId);
  if (producer) {
    producer.close();
    producers.delete(producerId);
    
    if (socket.roomId) {
      io.to(socket.roomId).emit('producerClosed', { producerId });
    }
    
    if (socket.roomId) {
      const room = rooms.get(socket.roomId);
      if (room) {
        room.participants.forEach((participantProducerIds, participantId) => {
          const index = participantProducerIds.indexOf(producerId);
          if (index !== -1) {
            participantProducerIds.splice(index, 1);
            room.participants.set(participantId, participantProducerIds);
          }
        });
      }
    }
    
    console.log(`Producer ${producerId} closed`);
  }
});

  socket.on("disconnect", () => {
    console.log("Client disconnected:", socket.id);
    
    if (socket.roomId) {
      const room = rooms.get(socket.roomId);
      if (room) {
        room.participants.delete(socket.id);
        
        if (room.participants.size === 0) {
          rooms.delete(socket.roomId);
          console.log(`Room ${socket.roomId} removed as it's empty`);
        } else {
          socket.to(socket.roomId).emit('participantLeft', { socketId: socket.id });
        }
      }
    }
    
    if (socket.transports) {
      if (socket.transports.send) {
        const transport = transports.get(socket.transports.send);
        if (transport) {
          transport.close();
          transports.delete(socket.transports.send);
        }
      }
      if (socket.transports.recv) {
        const transport = transports.get(socket.transports.recv);
        if (transport) {
          transport.close();
          transports.delete(socket.transports.recv);
        }
      }
    }
    
    if (socket.producerIds) {
      socket.producerIds.forEach((producerId: string) => {
        const producer = producers.get(producerId);
        if (producer) {
          producer.close();
          producers.delete(producerId);
        }
      });
    }
  });
});

['SIGINT', 'SIGTERM'].forEach(signal => {
  process.on(signal, () => {
    if (worker) worker.close();
    server.close(() => {
      console.log('Server closed');
      process.exit(0);
    });
  });
});



server.listen(3111, () => {
  console.log("Server is running on port 3111");
});