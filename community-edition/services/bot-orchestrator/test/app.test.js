const assert = require("node:assert/strict");
const { spawnSync } = require("node:child_process");
const { after, before, test } = require("node:test");

process.env.BOT_ACCESS_KEY = "test-access-key-that-is-at-least-32-characters";
process.env.OPENAI_API_KEY = "";
process.env.RUNNER_AUTOSTART = "false";
process.env.CHAT_RATE_LIMIT_MS = "1";
process.env.CHAT_RATE_LIMIT_MAX_REQUESTS = "3";

const { startServer, internals } = require("../app");

let server;
let baseUrl;

before(async () => {
  server = startServer(0);
  if (!server.listening) {
    await new Promise(resolve => server.once("listening", resolve));
  }
  baseUrl = `http://127.0.0.1:${server.address().port}`;
});

after(async () => {
  await new Promise((resolve, reject) => server.close(error => (error ? reject(error) : resolve())));
});

async function post(path, body, accessKey = process.env.BOT_ACCESS_KEY) {
  return fetch(`${baseUrl}${path}`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-ret-bot-access-key": accessKey
    },
    body: JSON.stringify(body)
  });
}

test("refuses to start without a strong internal access key", () => {
  const result = spawnSync(process.execPath, ["-e", "require('./app').startServer(0)"], {
    cwd: require("node:path").join(__dirname, ".."),
    env: { ...process.env, BOT_ACCESS_KEY: "" },
    encoding: "utf8"
  });

  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /BOT_ACCESS_KEY/);
});

test("refuses Chromium autostart because browser runners cannot receive the internal key", () => {
  const result = spawnSync(process.execPath, ["-e", "require('./app').startServer(0)"], {
    cwd: require("node:path").join(__dirname, ".."),
    env: { ...process.env, RUNNER_AUTOSTART: "true", RUNNER_BACKEND: "chromium" },
    encoding: "utf8"
  });

  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /Chromium is diagnostic-only/);
});

test("refuses an unaudited OpenAI model", () => {
  const result = spawnSync(process.execPath, ["-e", "require('./app').startServer(0)"], {
    cwd: require("node:path").join(__dirname, ".."),
    env: { ...process.env, OPENAI_API_KEY: "configured", OPENAI_MODEL: "other-model" },
    encoding: "utf8"
  });

  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /gpt-5-nano/);
});

test("protects internal endpoints and does not expose Express", async () => {
  const unauthorized = await post("/internal/bots/room-config", { hub_sid: "room", bots: {} }, "wrong");
  assert.equal(unauthorized.status, 401);

  const health = await fetch(`${baseUrl}/health`);
  assert.equal(health.status, 200);
  assert.equal(health.headers.get("x-powered-by"), null);
  const healthBody = await health.json();
  assert.equal(healthBody.runner_backend_default, "ghost");
  assert.equal(healthBody.ghost_navigation_require_navmesh, true);
});

test("sends only movement-relevant config changes to an active ghost runner", () => {
  const messages = [];
  const info = {
    backend: "ghost",
    configFingerprint: null,
    pendingConfigFingerprint: null,
    process: {
      connected: true,
      send(message) {
        messages.push(message);
        return true;
      }
    }
  };

  const initial = { enabled: true, count: 2, mobility: "medium", chat_enabled: true, prompt: "Recepción" };
  assert.equal(internals.sendRunnerConfigToProcess(info, initial), true);
  assert.equal(info.desiredBots, 2);
  assert.equal(info.authoritativeSpawnAcks, false);
  assert.equal(messages.length, 1);
  assert.deepEqual(messages[0].bots, { enabled: true, count: 2, mobility: "medium" });

  assert.equal(internals.sendRunnerConfigToProcess(info, { ...initial, prompt: "Otra persona" }), false);
  assert.equal(internals.sendRunnerConfigToProcess(info, { ...initial, chat_enabled: false }), false);
  assert.equal(messages.length, 1);

  assert.equal(internals.sendRunnerConfigToProcess(info, { ...initial, count: 5 }), true);
  assert.equal(internals.sendRunnerConfigToProcess(info, { ...initial, count: 5 }), false);
  assert.equal(internals.sendRunnerConfigToProcess(info, { ...initial, count: 5, mobility: "high" }), true);
  // A rapid rollback must supersede an in-flight newer config, not be mistaken
  // for the previously acknowledged state.
  info.configFingerprint = internals.runnerConfigFingerprint(initial);
  assert.equal(internals.sendRunnerConfigToProcess(info, initial), true);
  assert.equal(messages.length, 4);
});

test("readiness waits for a fresh applied status after a mobility-only config change", () => {
  const initialConfig = { enabled: true, count: 2, mobility: "medium" };
  const nextConfig = { ...initialConfig, mobility: "high" };
  const initialFingerprint = internals.runnerConfigFingerprint(initialConfig);
  const nextFingerprint = internals.runnerConfigFingerprint(nextConfig);
  const info = {
    backend: "ghost",
    configFingerprint: initialFingerprint,
    pendingConfigFingerprint: null,
    desiredBots: 2,
    activeBots: 2,
    authenticated: true,
    authoritativeSpawnAcks: true,
    ready: true,
    navigationStatus: "ready",
    botStatusReason: "ready",
    process: {
      connected: true,
      send() {
        return true;
      }
    }
  };
  const runtimeStatus = configFingerprint => ({
    desired: 2,
    active: 2,
    authenticated: true,
    authoritativeSpawnAcks: true,
    navigationReady: true,
    ready: true,
    reason: "ready",
    configFingerprint
  });

  assert.equal(internals.sendRunnerConfigToProcess(info, nextConfig), true);
  assert.equal(info.pendingConfigFingerprint, nextFingerprint);
  assert.equal(info.ready, false);

  // Even a heartbeat naming the new config cannot make readiness true before
  // the child has acknowledged receipt of that exact fingerprint.
  assert.equal(internals.applyGhostRuntimeStatus(info, runtimeStatus(nextFingerprint)), false);
  assert.equal(info.ready, false);

  assert.equal(internals.acknowledgeRunnerConfig(info, nextFingerprint), true);
  assert.equal(info.pendingConfigFingerprint, null);

  // A same-count heartbeat generated before the behavioral change was applied
  // must remain stale even though desired/active counts still match.
  assert.equal(internals.applyGhostRuntimeStatus(info, runtimeStatus(initialFingerprint)), false);
  assert.equal(info.ready, false);

  assert.equal(internals.applyGhostRuntimeStatus(info, runtimeStatus(nextFingerprint)), true);
  assert.equal(info.ready, true);
});

test("enforces the global ten-bot ceiling even when the environment requests more", () => {
  const result = spawnSync(
    process.execPath,
    [
      "-e",
      "process.stdout.write(JSON.stringify(require('./app').internals.normalizeConfig({ enabled: true, count: 999 })))"
    ],
    {
      cwd: require("node:path").join(__dirname, ".."),
      env: { ...process.env, MAX_BOTS_PER_ROOM: "999" },
      encoding: "utf8"
    }
  );

  assert.equal(result.status, 0, result.stderr);
  assert.equal(JSON.parse(result.stdout).count, 10);
});

test("keeps the diagnostic Chromium runner ceiling at one", () => {
  const result = spawnSync(
    process.execPath,
    ["-e", "process.stdout.write(String(require('./app').internals.maxActiveForBackend('chromium')))"],
    {
      cwd: require("node:path").join(__dirname, ".."),
      env: { ...process.env, MAX_CHROMIUM_ROOMS: "999" },
      encoding: "utf8"
    }
  );

  assert.equal(result.status, 0, result.stderr);
  assert.equal(result.stdout, "1");
});

test("keeps the ghost runner ceiling at ten even when the environment requests more", () => {
  const result = spawnSync(
    process.execPath,
    ["-e", "process.stdout.write(String(require('./app').internals.maxActiveForBackend('ghost')))"],
    {
      cwd: require("node:path").join(__dirname, ".."),
      env: { ...process.env, MAX_ACTIVE_ROOMS: "999" },
      encoding: "utf8"
    }
  );

  assert.equal(result.status, 0, result.stderr);
  assert.equal(result.stdout, "10");
});

test("health cannot mark a runner active without authentication and authoritative spawn ACKs", () => {
  const info = {
    backend: "ghost",
    configFingerprint: "config-1",
    pendingConfigFingerprint: null,
    desiredBots: 2,
    activeBots: 0,
    authenticated: false,
    authoritativeSpawnAcks: false,
    ready: false,
    navigationStatus: "pending",
    botStatusReason: "pending"
  };

  assert.equal(
    internals.applyGhostRuntimeStatus(info, {
      desired: 2,
      active: 2,
      authenticated: true,
      authoritativeSpawnAcks: true,
      navigationReady: true,
      ready: true,
      reason: "ready",
      configFingerprint: "config-1"
    }),
    false
  );
  assert.equal(internals.runnerHealthSnapshot(new Map([["room", info]])).active_hubs.length, 0);

  assert.equal(internals.applyGhostAuthStatus(info, { authenticated: true }), true);
  assert.equal(
    internals.applyGhostRuntimeStatus(info, {
      desired: 2,
      active: 2,
      authenticated: true,
      navigationReady: true,
      ready: true,
      reason: "ready",
      configFingerprint: "config-1"
    }),
    false
  );
  assert.equal(internals.runnerHealthSnapshot(new Map([["room", info]])).active_hubs.length, 0);

  assert.equal(
    internals.applyGhostRuntimeStatus(info, {
      desired: 9,
      active: 2,
      authenticated: true,
      authoritativeSpawnAcks: true,
      navigationReady: true,
      ready: true,
      reason: "ready",
      configFingerprint: "config-1"
    }),
    false
  );
  assert.equal(info.desiredBots, 2);

  assert.equal(
    internals.applyGhostRuntimeStatus(info, {
      desired: 2,
      active: 2,
      authenticated: true,
      authoritativeSpawnAcks: true,
      navigationReady: true,
      ready: true,
      reason: "ready",
      configFingerprint: "config-1"
    }),
    true
  );
  const snapshot = internals.runnerHealthSnapshot(new Map([["room", info]]));
  assert.deepEqual(snapshot.active_hubs, ["room"]);
  assert.equal(snapshot.runner_bots.room.ready, true);

  const stale = internals.runnerHealthSnapshot(
    new Map([["room", info]]),
    info.lastRuntimeStatusAt + 101,
    100
  );
  assert.deepEqual(stale.active_hubs, []);
  assert.equal(stale.runner_bots.room.active, 0);
  assert.equal(stale.runner_bots.room.ready, false);
  assert.equal(stale.runner_bots.room.reason, "stale_runtime_status");
});

test("readiness requires every configured bot room to have a fresh authoritative runner", () => {
  const configs = new Map([
    ["ready-room", { bots: { enabled: true, count: 2 } }],
    ["blocked-room", { bots: { enabled: true, count: 1 } }],
    ["disabled-room", { bots: { enabled: false, count: 10 } }]
  ]);
  const now = Date.now();
  const runners = new Map([
    [
      "ready-room",
      {
        backend: "ghost",
        desiredBots: 2,
        activeBots: 2,
        authenticated: true,
        authoritativeSpawnAcks: true,
        ready: true,
        navigationStatus: "ready",
        botStatusReason: "ready",
        lastRuntimeStatusAt: now
      }
    ],
    [
      "blocked-room",
      {
        backend: "ghost",
        desiredBots: 1,
        activeBots: 0,
        authenticated: true,
        authoritativeSpawnAcks: true,
        ready: false,
        navigationStatus: "blocked",
        botStatusReason: "navmesh_unavailable",
        lastRuntimeStatusAt: now
      }
    ]
  ]);

  const blocked = internals.runnerReadinessSnapshot(configs, runners, now);
  assert.equal(blocked.ok, false);
  assert.deepEqual(blocked.expected_hubs, ["blocked-room", "ready-room"]);
  assert.deepEqual(blocked.unready_hubs, ["blocked-room"]);

  configs.set("blocked-room", { bots: { enabled: false, count: 1 } });
  const ready = internals.runnerReadinessSnapshot(configs, runners, now);
  assert.equal(ready.ok, true);
  assert.deepEqual(ready.unready_hubs, []);
});

test("a stale child exit cannot delete or restart over a replacement runner", () => {
  const oldInfo = { restartDelayMs: 3000, restartTimer: null };
  const replacementInfo = { restartDelayMs: 3000, restartTimer: null };
  const runners = new Map([["room-race", replacementInfo]]);
  const configs = new Map([["room-race", { bots: { enabled: true, count: 1 } }]]);
  let scheduled = 0;
  let started = 0;

  const result = internals.handleRunnerExit("room-race", oldInfo, {
    runners,
    configs,
    clearTimer() {},
    schedule() {
      scheduled += 1;
      return { id: "unexpected" };
    },
    canAutostart: () => true,
    canStart: () => true,
    start() {
      started += 1;
      return true;
    },
    enqueue() {},
    fillQueue() {}
  });

  assert.equal(result, "stale_exit");
  assert.equal(runners.get("room-race"), replacementInfo);
  assert.equal(scheduled, 0);
  assert.equal(started, 0);
});

test("a delayed restart revalidates room configuration before spawning", () => {
  const info = { restartDelayMs: 3000, restartTimer: null };
  const runners = new Map([["room-stop-race", info]]);
  const configs = new Map([["room-stop-race", { bots: { enabled: true, count: 1 } }]]);
  let restartCallback;
  let started = 0;
  let enqueued = 0;

  const result = internals.handleRunnerExit("room-stop-race", info, {
    runners,
    configs,
    clearTimer() {},
    schedule(callback) {
      restartCallback = callback;
      return { id: "restart" };
    },
    canAutostart: () => true,
    canStart: () => true,
    start() {
      started += 1;
      return true;
    },
    enqueue() {
      enqueued += 1;
    },
    fillQueue() {}
  });

  assert.equal(result, "restart_scheduled");
  assert.equal(runners.has("room-stop-race"), false);
  assert.equal(typeof restartCallback, "function");

  configs.delete("room-stop-race");
  restartCallback();

  assert.equal(started, 0);
  assert.equal(enqueued, 0);
  assert.equal(runners.has("room-stop-race"), false);
});

test("keeps chat disabled until Reticulum supplies an enabled room config", async () => {
  const response = await post("/internal/bots/chat", {
    hub_sid: "room-a",
    bot_id: "bot-1",
    requester_id: "account-1",
    message: "hola"
  });

  assert.equal(response.status, 403);
});

test("preserves static mobility and never emits navigation actions for static bots", async () => {
  assert.equal(internals.normalizeConfig({ mobility: "static" }).mobility, "static");

  const config = await post("/internal/bots/room-config", {
    hub_sid: "room-static",
    bots: { enabled: true, count: 1, mobility: "static", chat_enabled: true }
  });
  assert.equal(config.status, 200);

  const response = await post("/internal/bots/chat", {
    hub_sid: "room-static",
    bot_id: "bot-1",
    requester_id: "account-static",
    message: "Ve a spawbot-recepcion",
    context: { waypoints: ["spawbot-recepcion"] }
  });
  const body = await response.json();

  assert.equal(response.status, 200);
  assert.equal(body.action, null);
});

test("returns a privacy-safe fallback without echoing the user message", async () => {
  const config = await post("/internal/bots/room-config", {
    hub_sid: "room-a",
    bots: { enabled: true, count: 2, mobility: "medium", chat_enabled: true, prompt: "Sé amable." }
  });
  assert.equal(config.status, 200);

  const response = await post("/internal/bots/chat", {
    hub_sid: "room-a",
    bot_id: "bot-1",
    requester_id: "account-1",
    message: "mi dato privado es 1234",
    context: { waypoints: ["spawbot-a", "invalid", "spawbot-a"] }
  });
  const body = await response.json();

  assert.equal(response.status, 200);
  assert.equal(body.reply.includes("1234"), false);
  assert.equal(body.action, null);
});

test("rate limits an account across different bots in the same room", async () => {
  const config = await post("/internal/bots/room-config", {
    hub_sid: "room-rate-limit",
    bots: { enabled: true, count: 4, mobility: "medium", chat_enabled: true }
  });
  assert.equal(config.status, 200);

  for (let botNumber = 1; botNumber <= 3; botNumber += 1) {
    const response = await post("/internal/bots/chat", {
      hub_sid: "room-rate-limit",
      bot_id: `bot-${botNumber}`,
      requester_id: "account-rate-limit",
      message: "hola"
    });
    const body = await response.json();
    assert.equal(response.status, 200);
    assert.equal(body.rate_limited, undefined);
    await new Promise(resolve => setTimeout(resolve, 2));
  }

  const limited = await post("/internal/bots/chat", {
    hub_sid: "room-rate-limit",
    bot_id: "bot-4",
    requester_id: "account-rate-limit",
    message: "hola"
  });
  const limitedBody = await limited.json();

  assert.equal(limited.status, 200);
  assert.equal(limitedBody.rate_limited, true);
});

test("builds non-stored OpenAI requests with pseudonymous safety identifiers", () => {
  const request = internals.buildOpenAIRequest({
    hubSid: "room-a",
    botId: "bot-1",
    requesterId: "account-123",
    message: "hola",
    botsConfig: internals.normalizeConfig({
      enabled: true,
      count: 1,
      mobility: "medium",
      chat_enabled: true,
      prompt: "Eres el recepcionista."
    }),
    context: { waypoints: ["spawbot-recepcion"] }
  });

  assert.equal(request.store, false);
  assert.equal(request.safety_identifier.length, 64);
  assert.equal(request.safety_identifier.includes("account-123"), false);
  assert.match(request.input[0].content[0].text, /texto no confiable/);
  assert.equal(request.input[0].content[0].text.includes("Eres el recepcionista."), false);
  const userPayload = JSON.parse(request.input[1].content[0].text);
  assert.equal(userPayload.room_persona, "Eres el recepcionista.");
  assert.equal(userPayload.known_waypoints, undefined);
  assert.equal(userPayload.hub_sid, undefined);
  assert.equal(userPayload.bot_id, undefined);
  assert.equal(request.input[1].content[0].text.includes("account-123"), false);
  assert.equal(request.text.format.type, "json_schema");
  assert.equal(request.text.format.strict, true);
  assert.deepEqual(request.text.format.schema.required, ["reply"]);
  assert.equal(request.text.format.schema.properties.action, undefined);
  assert.equal(request.text.format.schema.additionalProperties, false);
});

test("handles OpenAI refusals without exposing provider text", () => {
  const parsed = internals.parseOpenAIResponsePayload(
    {
      status: "completed",
      output: [
        {
          type: "message",
          content: [{ type: "refusal", refusal: "provider-specific refusal details" }]
        }
      ]
    },
    []
  );

  assert.equal(parsed.reply.includes("provider-specific"), false);
  assert.equal(parsed.action, null);
});

test("never treats model-provided navigation as executable authority", () => {
  const unknown = internals.parseStructuredReply(
    JSON.stringify({ reply: "Voy.", action: { type: "go_to_waypoint", waypoint: "spawbot-secret" } }),
    ["spawbot-lobby"]
  );
  assert.equal(unknown.action, null);

  const known = internals.parseStructuredReply(
    JSON.stringify({ reply: "Voy.", action: { type: "go_to_waypoint", waypoint: "spawbot-lobby" } }),
    ["spawbot-lobby"]
  );
  assert.equal(known.action, null);
});

test("derives movement only from a direct command with an exact allowlisted target", () => {
  const context = { waypoints: ["spawbot-recepcion"] };

  assert.equal(internals.detectWaypointAction("Hola. Salúdame brevemente.", context), null);
  assert.equal(internals.detectWaypointAction("¿Qué es spawbot-recepcion?", context), null);
  assert.equal(internals.detectWaypointAction("No vayas a spawbot-recepcion", context), null);
  assert.equal(internals.detectWaypointAction("Repite: ve a spawbot-recepcion", context), null);
  assert.equal(internals.detectWaypointAction("¿Ve a spawbot-recepcion?", context), null);
  assert.equal(internals.detectWaypointAction("?go to spawbot-recepcion", context), null);
  assert.equal(internals.detectWaypointAction("Ve al infierno", context), null);
  assert.equal(internals.detectWaypointAction("Ve a cualquier otro sitio", context), null);
  assert.equal(
    internals.detectWaypointAction("Ve a recepción", {
      waypoints: ["spawbot-recepcion", "spawbot-escenario"]
    }),
    null
  );
  assert.deepEqual(internals.detectWaypointAction("Ve a spawbot-recepcion", context), {
    type: "go_to_waypoint",
    waypoint: "spawbot-recepcion"
  });
  assert.equal(internals.detectWaypointAction("Dirígete a recepción", context), null);
});

test("removes stale room state after a complete Reticulum configuration snapshot", async () => {
  const originalFetch = global.fetch;

  global.fetch = async () => ({
    ok: true,
    status: 200,
    json: async () => ({ hubs: [] })
  });

  try {
    await internals.syncActiveRoomsFromReticulum();
  } finally {
    global.fetch = originalFetch;
  }

  const health = await fetch(`${baseUrl}/health`);
  const body = await health.json();
  assert.equal(body.rooms, 0);
});
