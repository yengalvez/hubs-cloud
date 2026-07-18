const assert = require("node:assert/strict");
const { test } = require("node:test");

const { internals } = require("../run-ghost-runner");
const TEST_PROCESS_GENERATION = "00000000-0000-4000-8000-000000000003";

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
  assert.equal(
    internals.deriveBotRuntimeStatus({
      enabled: false,
      desired: 0,
      active: 0,
      navigationIsReady: false,
      authenticated: false,
      cleanupUncertain: true
    }).reason,
    "spawn_cleanup_uncertain"
  );
});

test("requires authenticated self-presence and a matching Reticulum spawn ACK", async () => {
  const presence = {
    state: {
      session: {
        metas: [{
          context: { bot_runner: true },
          bot_runner_lease_id: "lease-1",
          bot_runner_join_order: 1,
          bot_runner_authority_epoch: 4,
          bot_runner_authoritative: true
        }]
      }
    }
  };
  assert.equal(internals.presenceHasAuthenticatedBotRunner(presence, "session", "lease-1"), true);
  assert.equal(internals.presenceHasAuthenticatedBotRunner({ state: {} }, "session", "lease-1"), false);

  presence.state.new_session = {
    metas: [{
      context: { bot_runner: true },
      bot_runner_lease_id: "lease-2",
      bot_runner_join_order: 2,
      bot_runner_authority_epoch: 4,
      bot_runner_authoritative: false
    }]
  };
  assert.equal(internals.authoritativeBotRunnerLeaseId(presence), "lease-1");
  assert.equal(internals.presenceHasAuthenticatedBotRunner(presence, "new_session", "lease-2"), false);
  delete presence.state.session;
  assert.equal(internals.authoritativeBotRunnerLeaseId(presence), "");
  presence.state.new_session.metas[0].bot_runner_authority_epoch = 5;
  presence.state.new_session.metas[0].bot_runner_authoritative = true;
  assert.equal(internals.authoritativeBotRunnerLeaseId(presence), "lease-2");
  assert.equal(internals.presenceHasAuthenticatedBotRunner(presence, "new_session", "lease-2"), true);

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
        callbacks.ok({
          bot_spawn_accepted: true,
          network_id: "room-bot-room-bot-1",
          bot_runner_authority_epoch: 5
        })
      );
      return push;
    }
  };
  await internals.requestBotSpawn(ackingChannel, payload, 5);

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
  await assert.rejects(internals.requestBotSpawn(mismatchedChannel, payload, 5), /invalid_ack/);

  const staleEpochChannel = {
    push() {
      const callbacks = {};
      const push = {
        receive(kind, callback) {
          callbacks[kind] = callback;
          return push;
        }
      };
      queueMicrotask(() => callbacks.ok({
        bot_spawn_accepted: true,
        network_id: "room-bot-room-bot-1",
        bot_runner_authority_epoch: 4
      }));
      return push;
    }
  };
  await assert.rejects(internals.requestBotSpawn(staleEpochChannel, payload, 5), /invalid_ack/);

  const rejectingChannel = {
    push() {
      const callbacks = {};
      const push = {
        receive(kind, callback) {
          callbacks[kind] = callback;
          return push;
        }
      };
      queueMicrotask(() => callbacks.error({ reason: "bot_spawn_rejected" }));
      return push;
    }
  };
  await assert.rejects(
    internals.requestBotSpawn(rejectingChannel, payload, 5),
    error => error.message === "bot_spawn_rejected" && error.authoritativeRejection === true
  );
});

test("applies bot commands only for the exact current runner authority fence", () => {
  const authorityFence = { leaseId: "lease-current", authorityEpoch: 12 };
  const record = { id: "bot-1", mobility: "medium" };
  const bots = new Map([[record.id, record]]);
  const calls = [];
  const startWalking = (...args) => calls.push(args);
  const exactCommand = {
    type: "bot_command",
    bot_runner_lease_id: "lease-current",
    bot_runner_authority_epoch: 12,
    body: { type: "go_to_waypoint", bot_id: "bot-1", waypoint: "spawbot-stage" }
  };

  assert.equal(
    internals.applyFencedBotCommand(exactCommand, authorityFence, bots, startWalking, 1234),
    true
  );
  assert.deepEqual(calls, [[record, "spawbot-stage", 1234]]);

  const rejectedCommands = [
    { ...exactCommand, bot_runner_authority_epoch: 11 },
    { ...exactCommand, bot_runner_lease_id: "lease-old" },
    { ...exactCommand, bot_runner_authority_epoch: "12" },
    { type: exactCommand.type, body: exactCommand.body }
  ];
  for (const command of rejectedCommands) {
    assert.equal(
      internals.applyFencedBotCommand(command, authorityFence, bots, startWalking, 5678),
      false
    );
  }
  assert.equal(calls.length, 1);
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

test("accepts only managed configs whose fingerprint matches and then ignores hub refresh config", () => {
  const bots = { enabled: true, count: 2, mobility: "static" };
  const fingerprint = internals.managedRunnerConfigFingerprint(bots);
  const accepted = internals.parseManagedConfigMessage({
    type: "bots-config",
    bots,
    fingerprint,
    revision: 7,
    processGeneration: TEST_PROCESS_GENERATION
  });
  assert.equal(accepted.fingerprint, fingerprint);
  assert.equal(accepted.revision, 7);
  assert.equal(accepted.processGeneration, TEST_PROCESS_GENERATION);
  assert.deepEqual(
    { enabled: accepted.bots.enabled, count: accepted.bots.count, mobility: accepted.bots.mobility },
    bots
  );
  assert.equal(
    internals.parseManagedConfigMessage({
      type: "bots-config",
      bots: { ...bots, mobility: "high" },
      fingerprint,
      revision: 7,
      processGeneration: TEST_PROCESS_GENERATION
    }),
    null
  );
  for (const invalid of [
    { revision: 0, processGeneration: TEST_PROCESS_GENERATION },
    { revision: 7, processGeneration: "invalid" },
    { revision: "7", processGeneration: TEST_PROCESS_GENERATION }
  ]) {
    assert.equal(
      internals.parseManagedConfigMessage({ type: "bots-config", bots, fingerprint, ...invalid }),
      null
    );
  }
  assert.equal(internals.shouldApplyHubRefreshConfig(false), true);
  assert.equal(internals.shouldApplyHubRefreshConfig(true), false);
});

test("managed config delivery is monotonic and idempotent across delayed polls", () => {
  const revisionTwo = {
    revision: 2,
    fingerprint: internals.managedRunnerConfigFingerprint({ enabled: true, count: 2, mobility: "high" })
  };
  const state = {
    appliedRevision: 1,
    appliedFingerprint: internals.managedRunnerConfigFingerprint({
      enabled: true,
      count: 1,
      mobility: "static"
    }),
    pendingRevision: 0,
    pendingFingerprint: ""
  };

  assert.equal(internals.managedConfigDeliveryDecision(state, revisionTwo), "accept");
  state.pendingRevision = revisionTwo.revision;
  state.pendingFingerprint = revisionTwo.fingerprint;

  assert.equal(
    internals.managedConfigDeliveryDecision(state, {
      revision: 1,
      fingerprint: state.appliedFingerprint
    }),
    "stale"
  );
  assert.equal(internals.managedConfigDeliveryDecision(state, revisionTwo), "duplicate");
  assert.equal(
    internals.managedConfigDeliveryDecision(state, { revision: 2, fingerprint: "different" }),
    "conflict"
  );
});

test("hub_refresh invalidates stale scene geometry before reconcile and requests a clean reload", () => {
  const policy = internals.createSceneFetchPolicy("https://meta-hubs.org");
  let waypointData = {
    navPlanner: { findRoute() {} },
    spawnPoints: [{ name: "spawbot-old" }],
    patrolPoints: [{ name: "spawbot-old" }],
    allWaypoints: [{ name: "spawbot-old" }],
    colliders: [{ old: true }]
  };
  const events = [];
  let restarts = 0;
  const result = internals.applyHubRefreshSceneChange({
    payload: {
      hubs: [{ scene: { model_url: "/files/new-scene.glb" } }]
    },
    currentSceneUrl: "https://meta-hubs.org/files/old-scene.glb",
    baseUrl: "https://meta-hubs.org",
    policy,
    invalidateNavigation() {
      waypointData = internals.emptyWaypointData();
      events.push("invalidate");
    },
    reconcile() {
      assert.equal(internals.navigationReady({
        navigationMode: "navmesh_preferred",
        requireNavmesh: true,
        waypointData
      }), false);
      assert.deepEqual(waypointData.colliders, []);
      events.push("reconcile");
    },
    publishStatus() {
      events.push("status");
    },
    requestRestart() {
      restarts += 1;
      events.push("restart");
    }
  });

  assert.equal(result.changed, true);
  assert.equal(result.url, "https://meta-hubs.org/files/new-scene.glb");
  assert.deepEqual(events, ["invalidate", "reconcile", "status", "restart"]);
  assert.equal(restarts, 1);

  const unchanged = internals.applyHubRefreshSceneChange({
    payload: { hubs: [{ scene: { model_url: "/files/new-scene.glb" } }] },
    currentSceneUrl: result.url,
    baseUrl: "https://meta-hubs.org",
    policy,
    invalidateNavigation: () => assert.fail("same scene must not invalidate"),
    reconcile: () => assert.fail("same scene must not reconcile"),
    publishStatus: () => assert.fail("same scene must not publish"),
    requestRestart: () => assert.fail("same scene must not restart")
  });
  assert.equal(unchanged.changed, false);

  const staleEvents = [];
  const staleSameUrl = internals.applyHubRefreshSceneChange({
    payload: {
      hubs: [{ scene: { model_url: "/files/new-scene.glb" } }],
      stale_fields: ["scene"]
    },
    currentSceneUrl: result.url,
    baseUrl: "https://meta-hubs.org",
    policy,
    invalidateNavigation: () => staleEvents.push("invalidate"),
    reconcile: () => staleEvents.push("reconcile"),
    publishStatus: () => staleEvents.push("status"),
    requestRestart: () => staleEvents.push("restart")
  });
  assert.equal(staleSameUrl.changed, true);
  assert.deepEqual(staleEvents, ["invalidate", "reconcile", "status", "restart"]);
});

test("acknowledges only the current managed revision and generation before forced status", () => {
  const events = [];
  let current = {
    fingerprint: "latest",
    revision: 7,
    processGeneration: TEST_PROCESS_GENERATION
  };
  const changed = internals.finalizeManagedConfigApplication({
    fingerprint: "latest",
    revision: 7,
    processGeneration: TEST_PROCESS_GENERATION,
    isCurrent: (fingerprint, revision, processGeneration) =>
      current.fingerprint === fingerprint &&
      current.revision === revision &&
      current.processGeneration === processGeneration,
    reconcile() {
      events.push("reconcile");
      return true;
    },
    applyFingerprint(fingerprint, revision, processGeneration) {
      events.push(`apply:${fingerprint}:${revision}:${processGeneration}`);
      current = {};
    },
    acknowledge(fingerprint, revision, processGeneration) {
      events.push(`ack:${fingerprint}:${revision}:${processGeneration}`);
    },
    publishStatus(force) {
      events.push(`status:${force}`);
    }
  });

  assert.equal(changed, true);
  assert.deepEqual(events, [
    "reconcile",
    `apply:latest:7:${TEST_PROCESS_GENERATION}`,
    `ack:latest:7:${TEST_PROCESS_GENERATION}`,
    "status:true"
  ]);

  events.length = 0;
  current = {
    fingerprint: "stale",
    revision: 8,
    processGeneration: TEST_PROCESS_GENERATION
  };
  internals.finalizeManagedConfigApplication({
    fingerprint: "stale",
    revision: 7,
    processGeneration: TEST_PROCESS_GENERATION,
    isCurrent: (fingerprint, revision, processGeneration) =>
      current.fingerprint === fingerprint &&
      current.revision === revision &&
      current.processGeneration === processGeneration,
    reconcile: () => events.push("reconcile"),
    applyFingerprint: () => events.push("apply"),
    acknowledge: () => events.push("ack"),
    publishStatus: () => events.push("status")
  });
  assert.deepEqual(events, ["reconcile"]);
});

test("pending spawn cancellation stays fail-closed across late success, timeout and rejection", () => {
  for (const transition of ["count-down", "disable", "nav-blocked"]) {
    for (const settlement of ["late-success", "timeout", "late-reject"]) {
      const removals = [];
      let restarts = 0;
      let uncertainTransitions = 0;
      const cleanup = internals.createSpawnCleanupController({
        removeNetworkId: networkId => removals.push(networkId),
        requestControlledRestart: () => {
          restarts += 1;
        },
        onUncertain: () => {
          uncertainTransitions += 1;
        }
      });
      const pending = {
        record: { networkId: `room-bot-2-${transition}-${settlement}` }
      };
      const retained = {
        record: { networkId: `room-bot-1-${transition}-${settlement}` }
      };
      const pendingSpawns = new Map([
        ["bot-1", retained],
        ["bot-2", pending]
      ]);

      assert.equal(cleanup.canSpawn(), true);
      internals.cancelPendingSpawnsForTransition({
        pendingSpawns,
        cleanup,
        botIds: transition === "count-down" ? ["bot-2"] : null
      });
      cleanup.observeAmbiguousSettlement(pending, settlement);
      cleanup.observeAmbiguousSettlement(pending, settlement);

      assert.equal(cleanup.reason(), "spawn_cleanup_uncertain");
      assert.equal(cleanup.canSpawn(), false);
      const expectedRemovals =
        transition === "count-down"
          ? [pending.record.networkId]
          : [retained.record.networkId, pending.record.networkId];
      assert.deepEqual(removals, expectedRemovals);
      assert.equal(cleanup.removalAttemptCount(), expectedRemovals.length);
      assert.deepEqual(Array.from(pendingSpawns.keys()), transition === "count-down" ? ["bot-1"] : []);
      assert.equal(uncertainTransitions, 1);
      assert.equal(restarts, 1);
      assert.equal(cleanup.restartRequested(), true);
    }
  }
});

test("count 2 to 1 to 2 cannot reuse a cancelled bot namespace in the same generation", () => {
  const removals = [];
  let restarts = 0;
  const cleanup = internals.createSpawnCleanupController({
    removeNetworkId: networkId => removals.push(networkId),
    requestControlledRestart: () => {
      restarts += 1;
    }
  });
  const generationA = { record: { networkId: "room-bot-room-bot-2" } };

  assert.equal(cleanup.canSpawn(), true);
  cleanup.cancelPending(generationA); // count 2 -> 1
  assert.equal(cleanup.canSpawn(), false); // count 1 -> 2 must remain blocked
  cleanup.observeAmbiguousSettlement(generationA, "late-success");

  assert.deepEqual(removals, ["room-bot-room-bot-2"]);
  assert.equal(restarts, 1);
  assert.equal(cleanup.canSpawn(), false);
});

test("cancelling several pending bots removes each namespace but requests one clean restart", () => {
  const removals = [];
  let restarts = 0;
  const cleanup = internals.createSpawnCleanupController({
    removeNetworkId: networkId => removals.push(networkId),
    requestControlledRestart: () => {
      restarts += 1;
    }
  });

  cleanup.cancelPending({ record: { networkId: "room-bot-room-bot-1" } });
  cleanup.cancelPending({ record: { networkId: "room-bot-room-bot-2" } });

  assert.deepEqual(removals, ["room-bot-room-bot-1", "room-bot-room-bot-2"]);
  assert.equal(restarts, 1);
});

test("three authoritative spawn failures schedule one bounded revalidated restart", () => {
  let callback = null;
  let delay = null;
  let exitCode = null;
  let stillDesired = false;

  assert.equal(
    internals.scheduleSpawnRecoveryRestart({ attempts: 2, scheduleFn: () => assert.fail() }),
    null
  );
  const timer = internals.scheduleSpawnRecoveryRestart({
    attempts: 3,
    delayMs: 1,
    scheduleFn(fn, delayMs) {
      callback = fn;
      delay = delayMs;
      return "spawn-recovery";
    },
    shouldRestart: () => stillDesired,
    exitFn(code) {
      exitCode = code;
    }
  });
  assert.equal(timer, "spawn-recovery");
  assert.equal(delay, 5000);
  callback();
  assert.equal(exitCode, null);

  stillDesired = true;
  callback();
  assert.equal(exitCode, 1);
});

test("scene URL logging strips query parameters and fragments", () => {
  const redacted = internals.redactUrlForLog(
    "https://user:password@assets.example.invalid/scenes/main.glb?token=do-not-log#private"
  );
  assert.equal(redacted, "https://assets.example.invalid/scenes/main.glb");
  assert.equal(redacted.includes("password"), false);
  assert.equal(redacted.includes("token"), false);
  assert.equal(redacted.includes("private"), false);
  assert.equal(internals.redactUrlForLog("not a URL"), "invalid-url");
  assert.equal(internals.errorCodeForLog(new Error("scene_http_403")), "scene_http_403");
  assert.equal(
    internals.errorCodeForLog(new Error("fetch failed for https://example.invalid/?token=secret")),
    "Error"
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

test("rejects oversized or empty navmesh accessors before any byte fetch", async () => {
  let fetches = 0;
  const policy = internals.createSceneFetchPolicy("https://meta-hubs.org", {
    fetchImpl: async () => {
      fetches += 1;
      throw new Error("accessor bytes must not be fetched");
    },
    maxSceneBytes: 64 * 1024 * 1024,
    maxJsonBytes: 4096,
    maxNavmeshTriangles: 32
  });
  const baseGltf = {
    asset: { version: "2.0" },
    buffers: [{ byteLength: 64 * 1024 * 1024 }],
    bufferViews: [{ buffer: 0, byteOffset: 0, byteLength: 64 * 1024 * 1024 }],
    accessors: [{ bufferView: 0, componentType: 5126, count: 97, type: "VEC3" }],
    meshes: [{ primitives: [{ attributes: { POSITION: 0 }, mode: 4 }] }],
    nodes: [
      {
        mesh: 0,
        extensions: { MOZ_hubs_components: { "nav-mesh": {} } }
      }
    ],
    scenes: [{ nodes: [0] }],
    scene: 0
  };
  const sceneFor = gltf => ({
    gltf,
    isGlb: true,
    glbBinStart: 4096,
    glbBinLength: 64 * 1024 * 1024,
    declaredLength: 4096 + 64 * 1024 * 1024,
    fullBuffer: null,
    sceneUrl: new URL("https://meta-hubs.org/files/navmesh.glb")
  });

  await assert.rejects(
    internals.extractNavMeshGeometry(sceneFor(structuredClone(baseGltf)), policy),
    /navmesh_too_many_vertices/
  );

  const empty = structuredClone(baseGltf);
  empty.accessors[0].count = 0;
  await assert.rejects(
    internals.extractNavMeshGeometry(sceneFor(empty), policy),
    /gltf_invalid_accessor/
  );

  const oversizedIndices = structuredClone(baseGltf);
  oversizedIndices.accessors[0].count = 3;
  oversizedIndices.accessors.push({ bufferView: 0, componentType: 5121, count: 99, type: "SCALAR" });
  oversizedIndices.meshes[0].primitives[0].indices = 1;
  await assert.rejects(
    internals.extractNavMeshGeometry(sceneFor(oversizedIndices), policy),
    /navmesh_too_many_triangles/
  );
  assert.equal(fetches, 0);
});
