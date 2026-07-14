"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");

const mediasoup = require("mediasoup");
const Room = require("../lib/Room");

test("creates the WebRTC and SCTP transport contract used by Dialog", async () => {
  const worker = await mediasoup.createWorker({ rtcMinPort: 46000, rtcMaxPort: 46020, logLevel: "warn" });

  try {
    assert.ok(Number.isInteger(worker.pid) && worker.pid > 0);
    const router = await worker.createRouter({
      mediaCodecs: [{ kind: "audio", mimeType: "audio/opus", clockRate: 48000, channels: 2 }]
    });
    const transport = await router.createWebRtcTransport({
      listenIps: [{ ip: "127.0.0.1" }],
      enableUdp: true,
      enableTcp: true,
      preferUdp: true,
      enableSctp: true,
      numSctpStreams: { OS: 1024, MIS: 1024 },
      maxSctpMessageSize: 262144
    });

    assert.ok(transport.iceParameters);
    assert.ok(transport.sctpParameters);
    transport.close();
    router.close();
  } finally {
    worker.close();
  }
});

test("tracks piped producers without accessing mediasoup internals", () => {
  const room = Object.create(Room.prototype);
  room._pipedProducerIdsByRouter = new Map();

  const first = room._getPipedProducerIds("router-1");
  first.add("producer-1");

  assert.equal(room._getPipedProducerIds("router-1"), first);
  assert.ok(room._getPipedProducerIds("router-1").has("producer-1"));
  assert.notEqual(room._getPipedProducerIds("router-2"), first);
});
