const assert = require("node:assert/strict");
const { test } = require("node:test");

const { internals } = require("../run-ghost-runner");

function makeGlb(gltf, bin = null) {
  const json = Buffer.from(JSON.stringify(gltf), "utf8");
  const paddedJsonLength = Math.ceil(json.length / 4) * 4;
  const paddedBinLength = bin ? Math.ceil(bin.length / 4) * 4 : 0;
  const bytes = Buffer.alloc(20 + paddedJsonLength + (bin ? 8 + paddedBinLength : 0), 0);
  bytes.fill(0x20, 20, 20 + paddedJsonLength);
  bytes.writeUInt32LE(0x46546c67, 0);
  bytes.writeUInt32LE(2, 4);
  bytes.writeUInt32LE(bytes.length, 8);
  bytes.writeUInt32LE(paddedJsonLength, 12);
  bytes.writeUInt32LE(0x4e4f534a, 16);
  json.copy(bytes, 20);
  if (bin) {
    const binHeaderOffset = 20 + paddedJsonLength;
    bytes.writeUInt32LE(paddedBinLength, binHeaderOffset);
    bytes.writeUInt32LE(0x004e4942, binHeaderOffset + 4);
    bin.copy(bytes, binHeaderOffset + 8);
  }
  return bytes;
}

function rangedFetch(bytes) {
  return async (_url, init = {}) => {
    const range = init.headers && init.headers.Range;
    if (!range) {
      return new Response(bytes, {
        status: 200,
        headers: { "content-length": String(bytes.length) }
      });
    }

    const match = String(range).match(/^bytes=(\d+)-(\d+)$/);
    assert.ok(match);
    const start = Number(match[1]);
    const end = Math.min(Number(match[2]), bytes.length - 1);
    const body = bytes.subarray(start, end + 1);
    return new Response(body, {
      status: 206,
      headers: {
        "content-length": String(body.length),
        "content-range": `bytes ${start}-${end}/${bytes.length}`
      }
    });
  };
}

function makeLShapedNavmeshGlb() {
  const positions = new Float32Array([
    0, 0, 0,
    1, 0, 0,
    0, 0, 2,
    1, 0, 2,
    0, 0, 3,
    1, 0, 3,
    3, 0, 2,
    3, 0, 3
  ]);
  const indices = new Uint16Array([
    0, 2, 1,
    1, 2, 3,
    2, 4, 3,
    3, 4, 5,
    3, 5, 6,
    6, 5, 7
  ]);
  const positionBytes = Buffer.from(positions.buffer);
  const indexBytes = Buffer.from(indices.buffer);
  const bin = Buffer.concat([positionBytes, indexBytes]);
  const gltf = {
    asset: { version: "2.0" },
    buffers: [{ byteLength: bin.length }],
    bufferViews: [
      { buffer: 0, byteOffset: 0, byteLength: positionBytes.length },
      { buffer: 0, byteOffset: positionBytes.length, byteLength: indexBytes.length }
    ],
    accessors: [
      { bufferView: 0, componentType: 5126, count: positions.length / 3, type: "VEC3" },
      { bufferView: 1, componentType: 5123, count: indices.length, type: "SCALAR" }
    ],
    meshes: [{ primitives: [{ attributes: { POSITION: 0 }, indices: 1, mode: 4 }] }],
    nodes: [
      {
        name: "navMesh",
        mesh: 0,
        extensions: { MOZ_hubs_components: { "nav-mesh": {} } }
      },
      {
        name: "spawbot-start",
        translation: [0.5, 0, 0.5],
        extensions: { MOZ_hubs_components: { waypoint: {} } }
      },
      {
        name: "spawbot-end",
        translation: [2.5, 0, 2.5],
        extensions: { MOZ_hubs_components: { waypoint: {} } }
      }
    ],
    scenes: [{ nodes: [0, 1, 2] }],
    scene: 0
  };
  return makeGlb(gltf, bin);
}

test("only allows same-origin scene URLs by default", () => {
  const policy = internals.createSceneFetchPolicy("https://meta-hubs.org");

  assert.equal(
    internals.validateSceneUrl("/files/scene.bin", policy).toString(),
    "https://meta-hubs.org/files/scene.bin"
  );
  assert.throws(
    () => internals.validateSceneUrl("http://meta-hubs.org/files/scene.bin", policy),
    /scene_url_protocol_not_allowed/
  );
  assert.throws(
    () => internals.validateSceneUrl("https://127.0.0.1/secret", policy),
    /scene_url_host_not_allowed/
  );
  assert.throws(
    () => internals.validateSceneUrl("https://user:password@meta-hubs.org/files/scene.bin", policy),
    /scene_url_credentials_not_allowed/
  );
});

test("permits explicit CDN hosts without weakening the default origin", () => {
  const policy = internals.createSceneFetchPolicy("https://meta-hubs.org", {
    allowedHosts: ["assets.meta-hubs.org"]
  });

  assert.equal(
    internals.validateSceneUrl("https://assets.meta-hubs.org/scene.glb", policy).hostname,
    "assets.meta-hubs.org"
  );
  assert.throws(
    () => internals.validateSceneUrl("https://example.com/scene.glb", policy),
    /scene_url_host_not_allowed/
  );
});

test("loads a bounded GLB JSON chunk", async () => {
  const glb = makeGlb({ asset: { version: "2.0" }, scenes: [{ nodes: [0] }], nodes: [{}] });
  const policy = internals.createSceneFetchPolicy("https://meta-hubs.org", {
    fetchImpl: async () => new Response(glb, { status: 200, headers: { "content-length": glb.length } }),
    maxSceneBytes: 4096,
    maxJsonBytes: 2048
  });

  const result = await internals.fetchGltfJson("https://meta-hubs.org/files/scene.bin", policy);
  assert.equal(result.nodes.length, 1);
});

test("rejects oversized responses before buffering them", async () => {
  const policy = internals.createSceneFetchPolicy("https://meta-hubs.org", {
    fetchImpl: async () =>
      new Response("too large", { status: 200, headers: { "content-length": String(2 * 1024 * 1024) } }),
    maxSceneBytes: 1024,
    maxJsonBytes: 1024
  });

  await assert.rejects(
    internals.fetchGltfJson("https://meta-hubs.org/files/scene.bin", policy),
    /scene_response_too_large/
  );
});

test("revalidates redirect destinations", async () => {
  const policy = internals.createSceneFetchPolicy("https://meta-hubs.org", {
    fetchImpl: async () =>
      new Response(null, { status: 302, headers: { location: "http://169.254.169.254/latest/meta-data" } })
  });

  await assert.rejects(
    internals.fetchGltfJson("https://meta-hubs.org/files/scene.bin", policy),
    /scene_url_protocol_not_allowed|scene_url_host_not_allowed/
  );
});

test("rejects cyclic glTF node graphs without recursive overflow", () => {
  const gltf = { scenes: [{ nodes: [0] }], nodes: [{ children: [1] }, { children: [0] }] };
  const policy = internals.createSceneFetchPolicy("https://meta-hubs.org");

  internals.validateGltfShape(gltf, policy);
  assert.throws(() => internals.computeWorldNodeMatrices(gltf), /gltf_node_cycle/);
});

test("rejects non-finite scene transforms", () => {
  const policy = internals.createSceneFetchPolicy("https://meta-hubs.org");
  const gltf = { scenes: [{ nodes: [0] }], nodes: [{ translation: [0, "Infinity", 0] }] };

  assert.throws(() => internals.validateGltfShape(gltf, policy), /gltf_invalid_transform/);
});

test("extracts named bot waypoints from a valid Spoke scene", () => {
  const gltf = {
    scenes: [{ nodes: [0] }],
    nodes: [
      {
        name: "spawbot-recepcion",
        translation: [2, 0, 3],
        extensions: { MOZ_hubs_components: { waypoint: { canBeSpawnPoint: false } } }
      }
    ]
  };

  const extracted = internals.extractWaypointsAndColliders(gltf);
  assert.equal(extracted.allWaypoints.length, 1);
  assert.equal(extracted.namedSpawbots.length, 1);
  assert.deepEqual(extracted.namedSpawbots[0].position, [2, 0, 3]);
});

test("keeps static mobility instead of silently normalizing it to medium", () => {
  assert.equal(internals.normalizeBotsConfig({ enabled: true, count: 2, mobility: "static" }).mobility, "static");
});

test("blocks navmesh-required bots until a projected navigation point exists", () => {
  const empty = { navPlanner: null, spawnPoints: [], patrolPoints: [] };
  assert.equal(
    internals.navigationReady({ navigationMode: "navmesh_preferred", requireNavmesh: true, waypointData: empty }),
    false
  );

  const plannerWithoutPoints = { ...empty, navPlanner: { findRoute() {} } };
  assert.equal(
    internals.navigationReady({
      navigationMode: "navmesh_preferred",
      requireNavmesh: true,
      waypointData: plannerWithoutPoints
    }),
    false
  );

  assert.equal(
    internals.navigationReady({
      navigationMode: "navmesh_preferred",
      requireNavmesh: true,
      waypointData: { ...plannerWithoutPoints, spawnPoints: [{ name: "spawbot-safe" }] }
    }),
    true
  );
});

test("allows an explicit legacy navigation opt-out without a navmesh", () => {
  const empty = { navPlanner: null, spawnPoints: [], patrolPoints: [] };
  assert.equal(
    internals.navigationReady({ navigationMode: "colliders", requireNavmesh: true, waypointData: empty }),
    true
  );
  assert.equal(
    internals.navigationReady({ navigationMode: "navmesh_preferred", requireNavmesh: false, waypointData: empty }),
    true
  );
});

test("reports desired versus active bots and the blocking reason", () => {
  assert.deepEqual(
    internals.deriveBotRuntimeStatus({
      enabled: true,
      desired: 10,
      active: 3,
      navigationIsReady: true,
      authenticated: true
    }),
    {
      desired: 10,
      active: 3,
      pending: 0,
      navigationReady: true,
      authenticated: true,
      authoritativeSpawnAcks: true,
      ready: false,
      reason: "insufficient_clearance"
    }
  );
  assert.equal(
    internals.deriveBotRuntimeStatus({
      enabled: true,
      desired: 5,
      active: 0,
      navigationIsReady: false,
      authenticated: true
    }).reason,
    "navmesh_unavailable"
  );
  assert.equal(
    internals.deriveBotRuntimeStatus({
      enabled: false,
      desired: 5,
      active: 0,
      navigationIsReady: true,
      authenticated: false
    }).reason,
    "disabled"
  );
  assert.equal(
    internals.deriveBotRuntimeStatus({
      enabled: true,
      desired: 1,
      active: 0,
      pending: 1,
      navigationIsReady: true,
      authenticated: true
    }).reason,
    "spawn_pending"
  );
  assert.equal(
    internals.deriveBotRuntimeStatus({
      enabled: true,
      desired: 1,
      active: 0,
      navigationIsReady: true,
      authenticated: false
    }).reason,
    "unauthenticated"
  );
});

test("requires authenticated self-presence and a matching Reticulum spawn ACK", async () => {
  const presence = {
    state: {
      session: { metas: [{ context: { bot_runner: true } }] }
    }
  };
  assert.equal(internals.presenceHasAuthenticatedBotRunner(presence, "session"), true);
  assert.equal(internals.presenceHasAuthenticatedBotRunner({ state: {} }, "session"), false);

  const payload = { data: { networkId: "room-bot-room-bot-1" } };
  const ackingChannel = {
    push(_event, _payload, timeoutMs) {
      assert.equal(timeoutMs, 5000);
      const callbacks = {};
      const push = {
        receive(kind, callback) {
          callbacks[kind] = callback;
          return push;
        }
      };
      queueMicrotask(() =>
        callbacks.ok({ bot_spawn_accepted: true, network_id: "room-bot-room-bot-1" })
      );
      return push;
    }
  };
  await internals.requestBotSpawn(ackingChannel, payload);

  const mismatchedChannel = {
    push() {
      const callbacks = {};
      const push = {
        receive(kind, callback) {
          callbacks[kind] = callback;
          return push;
        }
      };
      queueMicrotask(() => callbacks.ok({ bot_spawn_accepted: true, network_id: "other" }));
      return push;
    }
  };
  await assert.rejects(internals.requestBotSpawn(mismatchedChannel, payload), /invalid_ack/);
});

test("bounds Featured avatar discovery and keeps only usable unique refs", async () => {
  const discovered = await internals.fetchFeaturedAvatarRefs("https://meta-hubs.org", {
    timeoutMs: 100,
    fetchImpl: async (_url, { signal, redirect }) => {
      assert.ok(signal);
      assert.equal(redirect, "manual");
      return new Response(
        JSON.stringify({
          entries: [
            { gltfs: { avatar: "fullbody-a" }, tags: { tags: ["fullbody"] } },
            { gltfs: { avatar: "fullbody-a" }, tags: { tags: ["rpm"] } },
            { gltfs: { avatar: "upperbody-b" }, tags: { tags: [] } }
          ]
        }),
        { status: 200, headers: { "content-type": "application/json" } }
      );
    }
  });

  assert.deepEqual(discovered.allRefs, ["fullbody-a", "upperbody-b"]);
  assert.deepEqual(discovered.fullbodyRefs, ["fullbody-a"]);
});

test("rejects cross-origin redirects while discovering Featured avatars", async () => {
  let requests = 0;
  await assert.rejects(
    internals.fetchFeaturedAvatarRefs("https://meta-hubs.org", {
      fetchImpl: async (_url, { redirect }) => {
        requests += 1;
        assert.equal(redirect, "manual");
        return new Response(null, {
          status: 302,
          headers: { location: "https://attacker.invalid/featured.json" }
        });
      }
    }),
    /cross_origin_redirect/
  );
  assert.equal(requests, 1);
});

test("rejects oversized and malformed Featured responses", async () => {
  await assert.rejects(
    internals.fetchFeaturedAvatarRefs("https://meta-hubs.org", {
      maxBytes: 32,
      fetchImpl: async () =>
        new Response(JSON.stringify({ entries: [{ gltfs: { avatar: "a".repeat(64) } }] }), {
          status: 200,
          headers: { "content-type": "application/json" }
        })
    }),
    /response_too_large/
  );

  await assert.rejects(
    internals.fetchFeaturedAvatarRefs("https://meta-hubs.org", {
      fetchImpl: async () =>
        new Response(JSON.stringify({ entries: [{ gltfs: {} }] }), {
          status: 200,
          headers: { "content-type": "application/json" }
        })
    }),
    /invalid_ref/
  );

  await assert.rejects(
    internals.fetchFeaturedAvatarRefs("https://meta-hubs.org", {
      fetchImpl: async () =>
        new Response("{}", {
          status: 200,
          headers: { "content-type": "text/html" }
        })
    }),
    /invalid_content_type/
  );
});

test("aborts a stalled Featured avatar request", async () => {
  await assert.rejects(
    internals.fetchFeaturedAvatarRefs("https://meta-hubs.org", {
      timeoutMs: 10,
      fetchImpl: (_url, { signal }) =>
        new Promise((_resolve, reject) => {
          const keepAlive = setInterval(() => {}, 1000);
          signal.addEventListener(
            "abort",
            () => {
              clearInterval(keepAlive);
              reject(signal.reason);
            },
            { once: true }
          );
        })
    }),
    /abort|timeout/i
  );
});

test("retries scene/navmesh work with a small bounded exponential backoff", async () => {
  const delays = [];
  let attempts = 0;
  const result = await internals.retryWithBackoff(
    async () => {
      attempts += 1;
      if (attempts < 3) throw new Error("temporary_navmesh_failure");
      return "ready";
    },
    {
      maxAttempts: 99,
      baseDelayMs: 10,
      maxDelayMs: 20,
      sleep: async delayMs => delays.push(delayMs)
    }
  );

  assert.equal(result, "ready");
  assert.equal(attempts, 3);
  assert.deepEqual(delays, [10, 20]);
});

test("schedules a bounded clean restart when required navigation stays unavailable", () => {
  let scheduledDelay = null;
  let scheduledCallback = null;
  let exitCode = null;

  const timer = internals.scheduleNavigationRecoveryRestart({
    required: true,
    delayMs: 1,
    scheduleFn(callback, delayMs) {
      scheduledCallback = callback;
      scheduledDelay = delayMs;
      return "recovery-timer";
    },
    exitFn(code) {
      exitCode = code;
    }
  });

  assert.equal(timer, "recovery-timer");
  assert.equal(scheduledDelay, 5000);
  scheduledCallback();
  assert.equal(exitCode, 1);
  assert.equal(
    internals.scheduleNavigationRecoveryRestart({ required: false, scheduleFn: () => assert.fail() }),
    null
  );
});

test("projects separated bot spawns and refuses a collapsed navmesh", () => {
  const identityPlanner = { projectPoint: position => ({ position }) };
  const used = [[0, 0, 0]];
  const separated = internals.findSeparatedNavmeshPosition([0, 0, 0], 1, used, identityPlanner);
  assert.ok(separated);
  assert.ok(Math.hypot(separated[0], separated[2]) >= 0.65);

  const collapsedPlanner = { projectPoint: () => ({ position: [0, 0, 0] }) };
  assert.equal(internals.findSeparatedNavmeshPosition([0, 0, 0], 1, used, collapsedPlanner), null);
});

test("rejects crossing or too-close bot routes", () => {
  const otherBots = [
    {
      id: "bot-2",
      position: [1, 0, -1],
      path: { endPos: [1, 0, 1] },
      routePoints: []
    }
  ];

  assert.equal(
    internals.routeMaintainsSeparation(
      [
        [0, 0, 0],
        [2, 0, 0]
      ],
      "bot-1",
      otherBots
    ),
    false
  );
  assert.equal(
    internals.routeMaintainsSeparation(
      [
        [0, 0, 3],
        [2, 0, 3]
      ],
      "bot-1",
      otherBots
    ),
    true
  );
});

test("an invalid commanded waypoint cannot fall through to random patrol", () => {
  let planned = 0;
  const waypoints = [{ name: "spawbot-lobby", position: [1, 0, 1] }];
  const missing = internals.findCommandedWaypointPlan("spawbot-unknown", waypoints, () => {
    planned += 1;
    return [[0, 0, 0], [1, 0, 1]];
  });
  assert.equal(missing, null);
  assert.equal(planned, 0);

  const unreachable = internals.findCommandedWaypointPlan("spawbot-lobby", waypoints, () => {
    planned += 1;
    return null;
  });
  assert.equal(unreachable, null);
  assert.equal(planned, 1);
});

test("loads the GLB navmesh by byte range and finds a route around missing floor", async () => {
  const glb = makeLShapedNavmeshGlb();
  const policy = internals.createSceneFetchPolicy("https://meta-hubs.org", {
    fetchImpl: rangedFetch(glb),
    maxSceneBytes: 8192,
    maxJsonBytes: 4096,
    maxNavmeshTriangles: 32,
    navmeshSnapDistanceM: 1
  });

  const scene = await internals.fetchGltfScene("https://meta-hubs.org/files/navmesh.glb", policy);
  const navMesh = await internals.extractNavMeshGeometry(scene, policy);
  const planner = internals.createNavMeshPlanner(navMesh, policy);
  const extracted = internals.projectWaypointsToNavmesh(
    internals.extractWaypointsAndColliders(scene.gltf),
    planner
  );
  const route = planner.findRoute(
    extracted.namedSpawbots[0].position,
    extracted.namedSpawbots[1].position
  );

  assert.equal(navMesh.triangleCount, 6);
  assert.ok(route.length >= 3);
  assert.deepEqual(route[0].map(value => Number(value.toFixed(2))), [0.5, 0, 0.5]);
  assert.deepEqual(route.at(-1).map(value => Number(value.toFixed(2))), [2.5, 0, 2.5]);
  navMesh.geometry.dispose();
});
