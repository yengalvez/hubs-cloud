const assert = require("node:assert/strict");
const fs = require("node:fs");
const { spawnSync } = require("node:child_process");
const { randomUUID } = require("node:crypto");
const { EventEmitter } = require("node:events");
const http = require("node:http");
const { after, before, test } = require("node:test");

process.env.BOT_ORCHESTRATOR_ACCESS_KEY = "test-orchestrator-access-key-at-least-32";
process.env.OPENAI_API_KEY = "";
process.env.RUNNER_AUTOSTART = "false";
process.env.CHAT_RATE_LIMIT_MS = "1";
process.env.CHAT_RATE_LIMIT_MAX_REQUESTS = "3";
process.env.POD_NAMESPACE = "hcce";
process.env.ORCHESTRATOR_POD_NAME = "bot-orchestrator-test";
process.env.ORCHESTRATOR_POD_UID = "22222222-2222-4222-8222-222222222222";
process.env.BOT_RUNNER_IMAGE = `registry.invalid/bot-runner@sha256:${"a".repeat(64)}`;

const { startServer, internals } = require("../app");
const { createRunnerGenerationToken } = require("../runner-generation-token");

let server;
let baseUrl;
let nextTestPid = 10_000;

function validRunningGhostProcessState() {
  nextTestPid += 1;
  return {
    backend: "ghost",
    lifecycle: "running",
    spawned: true,
    ipcConnected: true,
    processGeneration: randomUUID(),
    process: { pid: nextTestPid, connected: true, kill: () => true }
  };
}

function validAuthoritativeRunnerState(bots, nowMs, overrides = {}) {
  const desired = bots.enabled ? bots.count : 0;
  return {
    ...validRunningGhostProcessState(),
    configFingerprint: internals.runnerConfigFingerprint(bots),
    configRevision: 1,
    pendingConfigFingerprint: null,
    pendingConfigRevision: null,
    desiredBots: desired,
    activeBots: desired,
    authenticated: true,
    authoritativeSpawnAcks: true,
    navigationStatus: "ready",
    botStatusReason: "ready",
    lastRuntimeStatusAt: nowMs,
    startedAt: Math.max(1, nowMs - 1_000),
    readySince: Math.max(1, nowMs - 1_000),
    ready: true,
    ...overrides
  };
}

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

async function post(path, body, accessKey = process.env.BOT_ORCHESTRATOR_ACCESS_KEY) {
  return fetch(`${baseUrl}${path}`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-ret-bot-orchestrator-access-key": accessKey
    },
    body: JSON.stringify(body)
  });
}

function rawStatus(pathname, headers) {
  return new Promise((resolve, reject) => {
    const request = http.request(
      {
        host: "127.0.0.1",
        port: server.address().port,
        path: pathname,
        method: "GET",
        headers
      },
      response => {
        response.resume();
        response.on("end", () => resolve(response.statusCode));
      }
    );
    request.on("error", reject);
    request.end();
  });
}

async function configureReadyRoom(hubSid, bots) {
  const normalized = internals.normalizeConfig(bots);
  const response = await post("/internal/bots/room-config", {
    hub_sid: hubSid,
    bots: normalized
  });
  assert.equal(response.status, 200);
  assert.equal(internals.seedReadyRoomForTests(hubSid, normalized), true);
  return normalized;
}

test("refuses to start without a strong orchestrator access key", () => {
  const result = spawnSync(process.execPath, ["-e", "require('./app').startServer(0)"], {
    cwd: require("node:path").join(__dirname, ".."),
    env: { ...process.env, BOT_ORCHESTRATOR_ACCESS_KEY: "" },
    encoding: "utf8"
  });

  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /BOT_ORCHESTRATOR_ACCESS_KEY/);
});

test("does not require or retain the master runner access key in the parent", () => {
  const result = spawnSync(
    process.execPath,
    ["-e", "require('./app').internals.validateRuntimeConfiguration()"],
    {
    cwd: require("node:path").join(__dirname, ".."),
    env: { ...process.env, BOT_RUNNER_ACCESS_KEY: "" },
    encoding: "utf8"
    }
  );

  assert.equal(result.status, 0);
});

test("refuses to mount the master runner key into an autostart parent", () => {
  const result = spawnSync(process.execPath, ["-e", "require('./app').startServer(0)"], {
    cwd: require("node:path").join(__dirname, ".."),
    env: {
      ...process.env,
      RUNNER_AUTOSTART: "true",
      BOT_RUNNER_ACCESS_KEY: "forbidden-master-runner-key-at-least-32"
    },
    encoding: "utf8"
  });

  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /must never receive the master runner access key/);
});

test("refuses production endpoint or credential-header redirection", () => {
  for (const [name, value] of [
    ["RET_INTERNAL_ENDPOINT", "https://redirect.invalid"],
    ["RET_INTERNAL_PATH", "/other"],
    ["RET_INTERNAL_ACCESS_HEADER", "authorization"],
    ["OPENAI_ENDPOINT", "https://redirect.invalid/v1/responses"],
    ["OPENAI_MODERATION_ENDPOINT", "https://redirect.invalid/v1/moderations"]
  ]) {
    const result = spawnSync(process.execPath, ["-e", "require('./app').startServer(0)"], {
      cwd: require("node:path").join(__dirname, ".."),
      env: { ...process.env, RUNNER_AUTOSTART: "true", [name]: value },
      encoding: "utf8"
    });

    assert.notEqual(result.status, 0, `${name} must fail closed`);
    assert.match(result.stderr, /audited/);
  }
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

test("refuses a redirected ghost runner script when autostart is enabled", () => {
  const result = spawnSync(process.execPath, ["-e", "require('./app').startServer(0)"], {
    cwd: require("node:path").join(__dirname, ".."),
    env: {
      ...process.env,
      RUNNER_AUTOSTART: "true",
      GHOST_RUNNER_SCRIPT: "/tmp/untrusted-runner.js"
    },
    encoding: "utf8"
  });

  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /script path must match the audited production contract/);
});

test("builds a runner Pod environment allowlist without any parent or master credential", () => {
  const sourceEnvironment = Object.fromEntries(
    internals.GHOST_RUNNER_ENV_KEYS.map(key => [key, `allowed-${key}`])
  );
  sourceEnvironment.BOT_RUNNER_GENERATION_TOKEN = "must-be-generated-per-pod";
  sourceEnvironment.RUNNER_CONTROL_URL = "must-be-generated-per-pod";
  sourceEnvironment.RUNNER_LEASE_HOLDER_ID = "must-be-generated-per-pod";
  sourceEnvironment.RUNNER_POD_UID = "must-be-generated-per-pod";
  sourceEnvironment.RUNNER_PROCESS_GENERATION = "must-be-generated-per-pod";
  sourceEnvironment.OPENAI_API_KEY = "must-not-reach-child";
  sourceEnvironment.BOT_ORCHESTRATOR_ACCESS_KEY = "must-not-reach-child";
  sourceEnvironment.BOT_RUNNER_ACCESS_KEY = "must-not-reach-child";
  sourceEnvironment.RET_INTERNAL_ENDPOINT = "must-not-reach-child";
  sourceEnvironment.NODE_OPTIONS = "must-not-reach-child";
  sourceEnvironment.AUDIT_SENTINEL = "must-not-reach-child";

  const environment = internals.ghostRunnerPodEnvironment(sourceEnvironment);

  for (const generated of [
    "BOT_RUNNER_GENERATION_TOKEN",
    "RUNNER_CONTROL_URL",
    "RUNNER_LEASE_HOLDER_ID",
    "RUNNER_POD_UID",
    "RUNNER_PROCESS_GENERATION"
  ]) {
    assert.equal(Object.hasOwn(environment, generated), false);
  }
  for (const forbidden of [
    "OPENAI_API_KEY",
    "BOT_ORCHESTRATOR_ACCESS_KEY",
    "BOT_RUNNER_ACCESS_KEY",
    "RET_INTERNAL_ENDPOINT",
    "NODE_OPTIONS",
    "AUDIT_SENTINEL"
  ]) {
    assert.equal(Object.hasOwn(environment, forbidden), false);
  }
  assert.equal(environment.GHOST_NAVIGATION_MODE, "allowed-GHOST_NAVIGATION_MODE");
});

test("keeps the ghost allowlist synchronized with every environment value the child consumes", () => {
  const source = fs.readFileSync(require.resolve("../run-ghost-runner.js"), "utf8");
  const consumed = [...source.matchAll(/process\.env\.([A-Z0-9_]+)/g)].map(match => match[1]);

  assert.deepEqual([...new Set(consumed)].sort(), [...internals.GHOST_RUNNER_ENV_KEYS].sort());
});

test("keeps Chromium as a manual diagnostic without runner authority or secret access", () => {
  const source = fs.readFileSync(require.resolve("../run-bot.js"), "utf8");
  const chromiumState = { ...validRunningGhostProcessState(), backend: "chromium" };

  assert.doesNotMatch(source, /--runner|bot_runner/);
  assert.match(source, /delete process\.env\.BOT_ORCHESTRATOR_ACCESS_KEY/);
  assert.match(source, /delete process\.env\.BOT_RUNNER_ACCESS_KEY/);
  assert.match(source, /delete process\.env\.BOT_RUNNER_GENERATION_TOKEN/);
  assert.match(source, /delete process\.env\.OPENAI_API_KEY/);
  assert.match(source, /delete process\.env\.RUNNER_CONTROL_URL/);
  assert.equal(internals.ghostRunnerProcessStateReason(validRunningGhostProcessState()), null);
  assert.equal(internals.ghostRunnerProcessStateReason(chromiumState), "runner_backend_invalid");
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

test("runner control is bearer-authenticated, Pod-UID-bound, and generation-scoped", async () => {
  internals.resetRuntimeStateForTests();
  const hubSid = "runner-control-room";
  const processGeneration = randomUUID();
  const podUid = "33333333-3333-4333-8333-333333333333";
  const token = createRunnerGenerationToken({
    key: process.env.BOT_ORCHESTRATOR_ACCESS_KEY,
    hubSid,
    processGeneration,
    holderId: process.env.ORCHESTRATOR_POD_UID,
    expiresAtSeconds: Math.floor(Date.now() / 1000) + 300
  });
  const message = {
    type: "bots-config",
    bots: { enabled: true, count: 1, mobility: "static" },
    fingerprint: JSON.stringify({ enabled: true, count: 1, mobility: "static" }),
    processGeneration,
    revision: 1
  };
  const info = {
    isolation: "kubernetes_pod",
    leaseHolderId: process.env.ORCHESTRATOR_POD_UID,
    backend: "ghost",
    lifecycle: "starting",
    processGeneration,
    ipcConnected: true,
    process: {
      pid: 1,
      connected: true,
      podUid,
      podReady: false,
      token,
      pendingMessage: message,
      kill: () => true
    }
  };
  assert.equal(internals.setRunnerStateForTests(hubSid, info), true);

  const unauthorized = await fetch(`${baseUrl}/internal/runner/v1/config`);
  assert.equal(unauthorized.status, 401);

  const headers = {
    authorization: `Bearer ${token}`,
    "x-yenhubs-runner-pod-uid": podUid
  };
  const configResponse = await fetch(`${baseUrl}/internal/runner/v1/config`, { headers });
  assert.equal(configResponse.status, 200);
  assert.equal(configResponse.headers.get("cache-control"), "no-store");
  assert.deepEqual((await configResponse.json()).message, message);

  assert.equal(
    await rawStatus("/internal/runner/v1/config", {
      authorization: [`Bearer ${token}`, `Bearer ${token}`],
      "x-yenhubs-runner-pod-uid": podUid
    }),
    401
  );
  assert.equal(
    await rawStatus("/internal/runner/v1/config", {
      authorization: `Bearer ${token}`,
      "x-yenhubs-runner-pod-uid": [podUid, podUid]
    }),
    401
  );

  const statusResponse = await fetch(`${baseUrl}/internal/runner/v1/status`, {
    method: "POST",
    headers: { ...headers, "content-type": "application/json" },
    body: JSON.stringify({
      message: {
        type: "ghost-navigation-status",
        processGeneration,
        ready: false
      }
    })
  });
  assert.equal(statusResponse.status, 204);
  assert.equal(info.navigationStatus, "blocked");

  internals.deleteRunnerStateForTests(hubSid);
});

test("isolated runner authentication carries the real Reticulum lease shape and fence epoch", () => {
  const processGeneration = randomUUID();
  const info = {
    isolation: "kubernetes_pod",
    lifecycle: "starting",
    processGeneration,
    authenticated: false
  };

  assert.equal(
    internals.applyGhostAuthStatus(info, {
      authenticated: true,
      processGeneration
    }),
    false
  );
  assert.equal(info.authenticated, false);

  assert.equal(
    internals.applyGhostAuthStatus(info, {
      authenticated: true,
      processGeneration,
      runnerLeaseId: `${randomUUID()}:17`,
      runnerAuthorityEpoch: 17
    }),
    false
  );
  assert.equal(info.authenticated, false);

  const runnerLeaseId = randomUUID();
  assert.equal(
    internals.applyGhostAuthStatus(info, {
      authenticated: true,
      processGeneration,
      runnerLeaseId,
      runnerAuthorityEpoch: 17
    }),
    true
  );
  assert.equal(info.runnerLeaseId, runnerLeaseId);
  assert.equal(info.runnerAuthorityEpoch, 17);
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
  assert.equal(info.pendingConfigSentAt > 0, true);
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

test("rejects stale A-B-A acknowledgements, statuses and previous child generations", () => {
  const messages = [];
  const info = {
    ...validRunningGhostProcessState(),
    nextConfigRevision: 0,
    configFingerprint: null,
    configRevision: null,
    pendingConfigFingerprint: null,
    pendingConfigRevision: null,
    desiredBots: 0,
    activeBots: 0,
    authenticated: false,
    authoritativeSpawnAcks: false,
    navigationStatus: "ready",
    botStatusReason: "pending",
    ready: false,
    process: {
      pid: nextTestPid,
      connected: true,
      kill: () => true,
      send(message) {
        messages.push(message);
        return true;
      }
    }
  };
  const configA = { enabled: true, count: 1, mobility: "static" };
  const configB = { enabled: true, count: 1, mobility: "high" };

  assert.equal(internals.sendRunnerConfigToProcess(info, configA), true);
  assert.equal(internals.sendRunnerConfigToProcess(info, configB), true);
  assert.equal(internals.sendRunnerConfigToProcess(info, configA), true);
  assert.deepEqual(messages.map(message => message.revision), [1, 2, 3]);
  assert.equal(new Set(messages.map(message => message.processGeneration)).size, 1);

  const [oldA, _configB, currentA] = messages;
  assert.equal(
    internals.acknowledgeRunnerConfig(
      info,
      oldA.fingerprint,
      oldA.revision,
      oldA.processGeneration
    ),
    false
  );
  assert.equal(info.pendingConfigRevision, currentA.revision);

  assert.equal(
    internals.acknowledgeRunnerConfig(
      info,
      currentA.fingerprint,
      currentA.revision,
      currentA.processGeneration
    ),
    true
  );
  assert.equal(
    internals.acknowledgeRunnerConfig(
      info,
      currentA.fingerprint,
      currentA.revision,
      currentA.processGeneration
    ),
    true
  );
  assert.equal(
    internals.applyGhostAuthStatus(info, {
      authenticated: true,
      processGeneration: currentA.processGeneration
    }),
    true
  );

  const statusFor = message => ({
    type: "ghost-runtime-status",
    desired: 1,
    active: 1,
    authenticated: true,
    authoritativeSpawnAcks: true,
    navigationReady: true,
    ready: true,
    reason: "ready",
    configFingerprint: message.fingerprint,
    configRevision: message.revision,
    processGeneration: message.processGeneration
  });

  assert.equal(internals.applyGhostRuntimeStatus(info, statusFor(oldA)), false);
  assert.equal(info.ready, false);

  const staleProcessGeneration = randomUUID();
  const staleChildStatus = {
    ...statusFor(currentA),
    processGeneration: staleProcessGeneration
  };
  assert.equal(internals.applyGhostRuntimeStatus(info, staleChildStatus), false);
  assert.equal(info.ready, false);

  assert.equal(internals.applyGhostRuntimeStatus(info, statusFor(currentA)), true);
  assert.equal(info.ready, true);

  const oldInfo = { ...info, processGeneration: staleProcessGeneration };
  assert.equal(
    internals.handleRunnerIpcMessage("room-aba", oldInfo, staleChildStatus, new Map([["room-aba", info]])),
    false
  );
});

test("malformed and previous-generation IPC cannot refresh authoritative runtime TTL", () => {
  const bots = { enabled: true, count: 1, mobility: "static" };
  const info = validAuthoritativeRunnerState(bots, 100, { lastAnyRuntimeStatusAt: 100 });
  const validStatus = {
    type: "ghost-runtime-status",
    desired: 1,
    active: 1,
    authenticated: true,
    authoritativeSpawnAcks: true,
    navigationReady: true,
    ready: true,
    reason: "ready",
    configFingerprint: info.configFingerprint,
    configRevision: info.configRevision,
    processGeneration: info.processGeneration
  };

  assert.equal(
    internals.applyGhostRuntimeStatus(info, { ...validStatus, configRevision: 999 }, 150),
    false
  );
  assert.equal(info.lastRuntimeStatusAt, 100);

  const lastAnyAfterMalformed = info.lastAnyRuntimeStatusAt;
  assert.equal(
    internals.applyGhostRuntimeStatus(
      info,
      { ...validStatus, processGeneration: randomUUID() },
      175
    ),
    false
  );
  assert.equal(info.lastAnyRuntimeStatusAt, lastAnyAfterMalformed);
  assert.equal(info.lastRuntimeStatusAt, 100);

  const readiness = internals.deriveRunnerBotReadiness(info, { bots }, 201, 100);
  assert.equal(readiness.ready, false);
  assert.equal(readiness.reason, "stale_runtime_status");
});

test("a failed managed-config send remains pending until watchdog recovery", () => {
  let callback;
  const info = {
    ...validRunningGhostProcessState(),
    lifecycle: "running",
    startedAt: Date.now() - 10,
    configFingerprint: null,
    pendingConfigFingerprint: null,
    process: {
      pid: nextTestPid,
      connected: true,
      kill: () => true,
      send(_message, done) {
        callback = done;
        return true;
      }
    }
  };
  const previousWarn = console.warn;
  console.warn = () => {};
  try {
    assert.equal(
      internals.sendRunnerConfigToProcess(info, { enabled: true, count: 1, mobility: "static" }),
      true
    );
    callback(new Error("sensitive transport detail"));
  } finally {
    console.warn = previousWarn;
  }

  assert.equal(typeof info.pendingConfigFingerprint, "string");
  assert.equal(info.pendingConfigFingerprint.length > 0, true);
  assert.equal(info.botStatusReason, "config_send_failed");
  assert.equal(
    internals.runnerRecoveryReason(info, info.pendingConfigSentAt + 2, { configAckTimeoutMs: 1 }),
    "config_ack_timeout"
  );
});

test("a config change invalidates readiness before checking a disconnected IPC channel", () => {
  const initial = { enabled: true, count: 1, mobility: "static" };
  const info = {
    backend: "ghost",
    lifecycle: "running",
    configFingerprint: internals.runnerConfigFingerprint(initial),
    configRevision: 1,
    pendingConfigFingerprint: null,
    pendingConfigRevision: null,
    desiredBots: 1,
    activeBots: 1,
    authenticated: true,
    authoritativeSpawnAcks: true,
    ready: true,
    readySince: 10,
    terminalStatusAt: 20,
    process: { connected: false }
  };

  assert.equal(
    internals.sendRunnerConfigToProcess(info, { ...initial, mobility: "high" }),
    false
  );
  assert.equal(info.ready, false);
  assert.equal(info.activeBots, 0);
  assert.equal(info.authoritativeSpawnAcks, false);
  assert.equal(info.readySince, 0);
  assert.equal(info.botStatusReason, "config_channel_unavailable");
  assert.equal(typeof info.pendingConfigFingerprint, "string");
  assert.equal(info.pendingConfigSentAt > 0, true);
});

test("readiness waits for a fresh applied status after a mobility-only config change", () => {
  const initialConfig = { enabled: true, count: 2, mobility: "medium" };
  const nextConfig = { ...initialConfig, mobility: "high" };
  const initialFingerprint = internals.runnerConfigFingerprint(initialConfig);
  const nextFingerprint = internals.runnerConfigFingerprint(nextConfig);
  const info = {
    backend: "ghost",
    configFingerprint: initialFingerprint,
    configRevision: 1,
    pendingConfigFingerprint: null,
    pendingConfigRevision: null,
    nextConfigRevision: 1,
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
  const runtimeStatus = (configFingerprint, configRevision) => ({
    desired: 2,
    active: 2,
    authenticated: true,
    authoritativeSpawnAcks: true,
    navigationReady: true,
    ready: true,
    reason: "ready",
    configFingerprint,
    configRevision,
    processGeneration: info.processGeneration
  });

  assert.equal(internals.sendRunnerConfigToProcess(info, nextConfig), true);
  assert.equal(info.pendingConfigFingerprint, nextFingerprint);
  const nextRevision = info.pendingConfigRevision;
  assert.equal(info.ready, false);

  // Even a heartbeat naming the new config cannot make readiness true before
  // the child has acknowledged receipt of that exact fingerprint.
  assert.equal(internals.applyGhostRuntimeStatus(info, runtimeStatus(nextFingerprint, nextRevision)), false);
  assert.equal(info.ready, false);

  assert.equal(
    internals.acknowledgeRunnerConfig(info, nextFingerprint, nextRevision, info.processGeneration),
    true
  );
  assert.equal(info.pendingConfigFingerprint, null);
  assert.equal(info.pendingConfigSentAt, 0);

  // A same-count heartbeat generated before the behavioral change was applied
  // must remain stale even though desired/active counts still match.
  assert.equal(internals.applyGhostRuntimeStatus(info, runtimeStatus(initialFingerprint, 1)), false);
  assert.equal(info.ready, false);

  assert.equal(internals.applyGhostRuntimeStatus(info, runtimeStatus(nextFingerprint, nextRevision)), true);
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

test("normalizes explicit prompt boundary whitespace before codepoint and byte caps", () => {
  const boundary = "\u0085\uFEFF\u00A0";
  const prompt = `${boundary}${"😀".repeat(2_000)}${boundary}`;
  const normalized = internals.normalizeConfig({ prompt }).prompt;

  assert.equal(Array.from(normalized).length, 1_500);
  assert.equal(Buffer.byteLength(normalized, "utf8"), 6_000);
  assert.equal(normalized, "😀".repeat(1_500));
  assert.equal(internals.trimBotPromptBoundaryWhitespace("\u0085x\uFEFF"), "x");
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
  const bots = { enabled: true, count: 2, mobility: "medium" };
  const configs = new Map([["room", { bots }]]);
  const fingerprint = internals.runnerConfigFingerprint(bots);
  const info = {
    ...validRunningGhostProcessState(),
    configFingerprint: fingerprint,
    configRevision: 1,
    pendingConfigFingerprint: null,
    pendingConfigRevision: null,
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
      configFingerprint: fingerprint,
      configRevision: 1,
      processGeneration: info.processGeneration
    }),
    false
  );
  assert.equal(internals.runnerHealthSnapshot(new Map([["room", info]]), Date.now(), 15_000, configs).active_hubs.length, 0);

  assert.equal(
    internals.applyGhostAuthStatus(info, {
      authenticated: true,
      processGeneration: info.processGeneration
    }),
    true
  );
  assert.equal(
    internals.applyGhostRuntimeStatus(info, {
      desired: 2,
      active: 2,
      authenticated: true,
      navigationReady: true,
      ready: true,
      reason: "ready",
      configFingerprint: fingerprint,
      configRevision: 1,
      processGeneration: info.processGeneration
    }),
    false
  );
  assert.equal(internals.runnerHealthSnapshot(new Map([["room", info]]), Date.now(), 15_000, configs).active_hubs.length, 0);

  assert.equal(
    internals.applyGhostRuntimeStatus(info, {
      desired: 9,
      active: 2,
      authenticated: true,
      authoritativeSpawnAcks: true,
      navigationReady: true,
      ready: true,
      reason: "ready",
      configFingerprint: fingerprint,
      configRevision: 1,
      processGeneration: info.processGeneration
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
      configFingerprint: fingerprint,
      configRevision: 1,
      processGeneration: info.processGeneration
    }),
    true
  );
  const snapshot = internals.runnerHealthSnapshot(
    new Map([["room", info]]),
    info.lastRuntimeStatusAt,
    15_000,
    configs
  );
  assert.deepEqual(snapshot.active_hubs, ["room"]);
  assert.equal(snapshot.runner_bots.room.ready, true);

  const stale = internals.runnerHealthSnapshot(
    new Map([["room", info]]),
    info.lastRuntimeStatusAt + 101,
    100,
    configs
  );
  assert.deepEqual(stale.active_hubs, []);
  assert.equal(stale.runner_bots.room.active, 0);
  assert.equal(stale.runner_bots.room.ready, false);
  assert.equal(stale.runner_bots.room.reason, "stale_runtime_status");

  info.process = { ...info.process, connected: false };
  const disconnected = internals.runnerHealthSnapshot(
    new Map([["room", info]]),
    info.lastRuntimeStatusAt,
    100,
    configs
  );
  assert.deepEqual(disconnected.active_hubs, []);
  assert.equal(disconnected.runner_bots.room.active, 0);
  assert.equal(disconnected.runner_bots.room.authenticated, false);
  assert.equal(disconnected.runner_bots.room.ready, false);
  assert.equal(disconnected.runner_bots.room.reason, "config_channel_disconnected");
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
        ...validRunningGhostProcessState(),
        configFingerprint: internals.runnerConfigFingerprint(configs.get("ready-room").bots),
        configRevision: 1,
        pendingConfigFingerprint: null,
        pendingConfigRevision: null,
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
        ...validRunningGhostProcessState(),
        configFingerprint: internals.runnerConfigFingerprint(configs.get("blocked-room").bots),
        configRevision: 1,
        pendingConfigFingerprint: null,
        pendingConfigRevision: null,
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

  const snapshot = { seen: true, valid: true, receivedAt: now, ttlMs: 1000 };
  const blocked = internals.runnerReadinessSnapshot(configs, runners, now, 15_000, snapshot);
  assert.equal(blocked.ok, false);
  assert.deepEqual(blocked.expected_hubs, ["blocked-room", "ready-room"]);
  assert.deepEqual(blocked.unready_hubs, ["blocked-room"]);

  configs.set("blocked-room", { bots: { enabled: false, count: 1 } });
  const extra = internals.runnerReadinessSnapshot(configs, runners, now, 15_000, snapshot);
  assert.equal(extra.ok, false);
  assert.deepEqual(extra.unready_hubs, []);
  assert.deepEqual(extra.extra_process_hubs, ["blocked-room"]);

  runners.delete("blocked-room");
  const ready = internals.runnerReadinessSnapshot(configs, runners, now, 15_000, snapshot);
  assert.equal(ready.ok, true);
});

test("readiness fails closed until a fresh full configuration snapshot exists", () => {
  const now = 100_000;
  const pending = internals.runnerReadinessSnapshot(new Map(), new Map(), now, 100, {
    seen: false,
    receivedAt: 0,
    ttlMs: 1000
  });
  assert.equal(pending.ok, false);
  assert.equal(pending.snapshot_reason, "authoritative_snapshot_pending");

  const fresh = internals.runnerReadinessSnapshot(new Map(), new Map(), now, 100, {
    seen: true,
    valid: true,
    receivedAt: now - 100,
    ttlMs: 1000
  });
  assert.equal(fresh.ok, false);
  assert.equal(fresh.snapshot_reason, "ready");

  const stale = internals.runnerReadinessSnapshot(new Map(), new Map(), now, 100, {
    seen: true,
    valid: true,
    receivedAt: now - 1001,
    ttlMs: 1000
  });
  assert.equal(stale.ok, false);
  assert.equal(stale.snapshot_reason, "authoritative_snapshot_stale");
});

test("readiness reports configured-room capacity overflow explicitly", () => {
  const now = 100_000;
  const configs = new Map(
    Array.from({ length: 6 }, (_value, index) => [
      `capacity-room-${index + 1}`,
      { bots: { enabled: true, count: 1 } }
    ])
  );
  const readiness = internals.runnerReadinessSnapshot(configs, new Map(), now, 100, {
    seen: true,
    valid: true,
    receivedAt: now,
    ttlMs: 1000
  });

  assert.equal(readiness.ok, false);
  assert.equal(readiness.capacity_exceeded, true);
  assert.equal(readiness.configured_room_count, 6);
  assert.equal(readiness.max_active_rooms, 5);
});

test("readiness preserves exact hub keys without object-prototype collisions", () => {
  const now = Date.now();
  const hubSid = "__proto__";
  const readiness = internals.runnerReadinessSnapshot(
    new Map([[hubSid, { bots: { enabled: true, count: 1 } }]]),
    new Map([
      [
        hubSid,
        {
          ...validRunningGhostProcessState(),
          configFingerprint: internals.runnerConfigFingerprint({ enabled: true, count: 1 }),
          configRevision: 1,
          pendingConfigFingerprint: null,
          pendingConfigRevision: null,
          desiredBots: 1,
          activeBots: 1,
          authenticated: true,
          authoritativeSpawnAcks: true,
          ready: true,
          navigationStatus: "ready",
          botStatusReason: "ready",
          lastRuntimeStatusAt: now
        }
      ]
    ]),
    now,
    1_000,
    { seen: true, valid: true, receivedAt: now, ttlMs: 1_000 }
  );

  assert.equal(readiness.ok, true);
  assert.deepEqual(Object.keys(readiness.runner_bots), [hubSid]);
  assert.equal(readiness.runner_bots[hubSid].ready, true);
});

test("the real readiness endpoint exposes the fail-closed production contract", async () => {
  internals.resetRuntimeStateForTests();
  const hubSid = "ready-contract-room";
  const publicHubSid = internals.publicHubIdentifier(hubSid);
  const bots = {
    enabled: true,
    count: 1,
    mobility: "static",
    chat_enabled: false,
    prompt: ""
  };
  await internals.syncActiveRoomsFromReticulum({
    fetchImpl: async () => ({
      ok: true,
      status: 200,
      json: async () => ({ hubs: [{ hub_sid: hubSid, bots }] })
    })
  });

  const now = Date.now();
  const fingerprint = internals.runnerConfigFingerprint(bots);
  const runner = {
    ...validRunningGhostProcessState(),
    lifecycle: "starting",
    configFingerprint: fingerprint,
    configRevision: 1,
    pendingConfigFingerprint: null,
    pendingConfigRevision: null,
    desiredBots: 1,
    activeBots: 0,
    authenticated: false,
    authoritativeSpawnAcks: false,
    ready: false,
    navigationStatus: "pending",
    botStatusReason: "pending",
    startedAt: now,
    lastRuntimeStatusAt: 0
  };
  internals.applyGhostAuthStatus(runner, {
    authenticated: true,
    processGeneration: runner.processGeneration
  });
  assert.equal(
    internals.applyGhostRuntimeStatus(
      runner,
      {
        desired: 1,
        active: 1,
        authenticated: true,
        authoritativeSpawnAcks: true,
        navigationReady: true,
        ready: true,
        reason: "ready",
        configFingerprint: fingerprint,
        configRevision: 1,
        processGeneration: runner.processGeneration
      },
      now
    ),
    true
  );
  assert.equal(internals.setRunnerStateForTests(hubSid, runner), true);

  try {
    const response = await fetch(`${baseUrl}/ready`);
    const body = await response.json();
    assert.equal(response.status, 200);
    assert.deepEqual(Object.keys(body).sort(), [
      "active_hubs",
      "authoritative_snapshot_ready",
      "authoritative_snapshot_ttl_ms",
      "capacity_exceeded",
      "configured_room_count",
      "expected_hubs",
      "extra_process_hubs",
      "max_active_rooms",
      "ok",
      "process_hubs",
      "runner_bots",
      "runner_health_ttl_ms",
      "snapshot_age_ms",
      "snapshot_reason",
      "stopping_hubs",
      "unready_hubs"
    ]);
    assert.equal(body.authoritative_snapshot_ready, true);
    assert.equal(body.snapshot_reason, "ready");
    assert.equal(typeof body.snapshot_age_ms, "number");
    assert.ok(body.snapshot_age_ms >= 0);
    assert.ok(body.runner_health_ttl_ms > 0);
    assert.ok(body.authoritative_snapshot_ttl_ms >= body.runner_health_ttl_ms);
    assert.ok(body.snapshot_age_ms <= body.authoritative_snapshot_ttl_ms);
    assert.deepEqual(body.expected_hubs, [publicHubSid]);
    assert.deepEqual(body.process_hubs, [publicHubSid]);
    assert.deepEqual(body.active_hubs, [publicHubSid]);
    assert.deepEqual(body.extra_process_hubs, []);
    assert.deepEqual(body.stopping_hubs, []);
    assert.deepEqual(Object.keys(body.runner_bots), body.expected_hubs);
    assert.deepEqual(body.runner_bots[publicHubSid], {
      desired: 1,
      active: 1,
      authenticated: true,
      authoritative_spawn_acks: true,
      navigation_ready: true,
      config_applied: true,
      ready: true,
      lifecycle: "running",
      reason: "ready"
    });

    const authoritativeRunner = { ...runner, process: { ...runner.process } };
    const runnerVariant = overrides => ({
      ...authoritativeRunner,
      process: { ...authoritativeRunner.process },
      ...overrides
    });
    const assertHttpRunnerBlocked = async (state, reason, lifecycle, expected = {}) => {
      assert.equal(internals.setRunnerStateForTests(hubSid, state), true);
      const blocked = await fetch(`${baseUrl}/ready`);
      const blockedBody = await blocked.json();
      assert.equal(blocked.status, 503);
      assert.equal(blockedBody.ok, false);
      assert.deepEqual(blockedBody.unready_hubs, [publicHubSid]);
      assert.deepEqual(blockedBody.active_hubs, []);
      const observed = blockedBody.runner_bots[publicHubSid];
      assert.equal(typeof observed.navigation_ready, "boolean");
      assert.equal(typeof observed.config_applied, "boolean");
      assert.equal(observed.ready, false);
      assert.equal(observed.lifecycle, lifecycle);
      assert.equal(observed.reason, reason);
      for (const [key, value] of Object.entries(expected)) assert.equal(observed[key], value, key);
      return blockedBody;
    };

    assert.equal(internals.deleteRunnerStateForTests(hubSid), true);
    let blocked = await fetch(`${baseUrl}/ready`);
    let blockedBody = await blocked.json();
    assert.equal(blocked.status, 503);
    assert.deepEqual(blockedBody.process_hubs, []);
    assert.deepEqual(Object.keys(blockedBody.runner_bots), [publicHubSid]);
    assert.equal(blockedBody.runner_bots[publicHubSid].lifecycle, "missing");
    assert.equal(blockedBody.runner_bots[publicHubSid].reason, "runner_missing");
    assert.equal(blockedBody.runner_bots[publicHubSid].navigation_ready, false);
    assert.equal(blockedBody.runner_bots[publicHubSid].config_applied, false);

    const withoutPid = runnerVariant();
    delete withoutPid.process.pid;
    const withoutIpcState = runnerVariant();
    delete withoutIpcState.ipcConnected;
    const withoutProcessConnected = runnerVariant();
    delete withoutProcessConnected.process.connected;
    const withoutSpawnState = runnerVariant();
    delete withoutSpawnState.spawned;
    const withoutConfigFingerprint = runnerVariant();
    delete withoutConfigFingerprint.configFingerprint;
    const staleRunner = runnerVariant({
      lastRuntimeStatusAt: Date.now() - body.runner_health_ttl_ms - 1
    });
    const malformedStates = [
      [{}, "runner_state_invalid", "unknown"],
      [runnerVariant({ process: {} }), "runner_process_invalid", "running"],
      [withoutPid, "runner_process_invalid", "running"],
      [runnerVariant({ process: { pid: 0, connected: true } }), "runner_process_invalid", "running"],
      [runnerVariant({ process: { pid: 1.5, connected: true } }), "runner_process_invalid", "running"],
      [withoutSpawnState, "runner_not_spawned", "running"],
      [runnerVariant({ spawned: false }), "runner_not_spawned", "running"],
      [withoutIpcState, "config_channel_disconnected", "running"],
      [runnerVariant({ ipcConnected: false }), "config_channel_disconnected", "running"],
      [withoutProcessConnected, "config_channel_disconnected", "running"],
      [runnerVariant({ process: { ...authoritativeRunner.process, connected: false } }), "config_channel_disconnected", "running"],
      [runnerVariant({ backend: "chromium" }), "runner_backend_invalid", "running"],
      [runnerVariant({ lifecycle: "starting" }), "runner_starting", "starting"],
      [runnerVariant({ lifecycle: "stopping" }), "runner_stopping", "stopping"],
      [withoutConfigFingerprint, "runner_state_invalid", "running"]
    ];
    for (const [state, reason, lifecycle] of malformedStates) {
      await assertHttpRunnerBlocked(state, reason, lifecycle);
    }
    await assertHttpRunnerBlocked(staleRunner, "stale_runtime_status", "running");

    const inheritedRunner = Object.create(authoritativeRunner);
    await assertHttpRunnerBlocked(inheritedRunner, "runner_state_invalid", "unknown", {
      navigation_ready: false,
      config_applied: false
    });
    const customPrototypeRunner = Object.assign(Object.create({ inherited: true }), runnerVariant());
    await assertHttpRunnerBlocked(customPrototypeRunner, "runner_state_invalid", "unknown", {
      navigation_ready: false,
      config_applied: false
    });
    const lifecycleGetterRunner = runnerVariant();
    Object.defineProperty(lifecycleGetterRunner, "lifecycle", {
      configurable: true,
      enumerable: true,
      get() {
        throw new Error("readiness must not execute lifecycle getters");
      }
    });
    await assertHttpRunnerBlocked(lifecycleGetterRunner, "runner_state_invalid", "unknown", {
      navigation_ready: false,
      config_applied: false
    });

    await assertHttpRunnerBlocked(runnerVariant({ authenticated: false }), "unauthenticated", "running", {
      authenticated: false,
      navigation_ready: true,
      config_applied: true
    });
    await assertHttpRunnerBlocked(
      runnerVariant({ authoritativeSpawnAcks: false }),
      "spawn_ack_missing",
      "running",
      { authoritative_spawn_acks: false, navigation_ready: true, config_applied: true }
    );
    await assertHttpRunnerBlocked(
      runnerVariant({ navigationStatus: "blocked" }),
      "navigation_not_ready",
      "running",
      { navigation_ready: false, config_applied: true }
    );
    await assertHttpRunnerBlocked(
      runnerVariant({ pendingConfigFingerprint: "pending" }),
      "config_pending",
      "running",
      { navigation_ready: true, config_applied: false }
    );
    await assertHttpRunnerBlocked(
      runnerVariant({ configFingerprint: "wrong" }),
      "config_mismatch",
      "running",
      { navigation_ready: true, config_applied: false }
    );
    await assertHttpRunnerBlocked(
      runnerVariant({ desiredBots: 0 }),
      "config_mismatch",
      "running",
      { desired: 0, navigation_ready: true, config_applied: false }
    );
    await assertHttpRunnerBlocked(
      runnerVariant({ activeBots: 0 }),
      "active_bot_count_mismatch",
      "running",
      { active: 0, navigation_ready: true, config_applied: true }
    );
    await assertHttpRunnerBlocked(
      runnerVariant({ botStatusReason: "config_pending" }),
      "config_pending",
      "running",
      { navigation_ready: true, config_applied: true }
    );

    assert.equal(internals.setRunnerStateForTests(hubSid, runnerVariant({ ready: false })), true);
    const uncached = await fetch(`${baseUrl}/ready`);
    const uncachedBody = await uncached.json();
    assert.equal(uncached.status, 200);
    assert.equal(uncachedBody.runner_bots[publicHubSid].ready, true);

    const configMutationRunner = runnerVariant();
    assert.equal(internals.setRunnerStateForTests(hubSid, configMutationRunner), true);
    await internals.syncActiveRoomsFromReticulum({
      fetchImpl: async () => ({
        ok: true,
        status: 200,
        json: async () => ({ hubs: [{ hub_sid: hubSid, bots: { ...bots, mobility: "high" } }] })
      })
    });
    Object.assign(configMutationRunner, authoritativeRunner, {
      process: { ...authoritativeRunner.process },
      pendingConfigFingerprint: null
    });
    await assertHttpRunnerBlocked(configMutationRunner, "config_mismatch", "running", {
      navigation_ready: true,
      config_applied: false
    });
    await internals.syncActiveRoomsFromReticulum({
      fetchImpl: async () => ({
        ok: true,
        status: 200,
        json: async () => ({ hubs: [{ hub_sid: hubSid, bots }] })
      })
    });

    assert.equal(internals.setRunnerStateForTests(hubSid, runnerVariant()), true);
    assert.equal(
      internals.setRunnerStateForTests("ready-contract-extra", {
        ...runnerVariant(),
        lifecycle: "stopping",
        process: { pid: nextTestPid + 1, connected: true }
      }),
      true
    );
    blocked = await fetch(`${baseUrl}/ready`);
    blockedBody = await blocked.json();
    assert.equal(blocked.status, 503);
    const publicExtraHubSid = internals.publicHubIdentifier("ready-contract-extra");
    assert.deepEqual(blockedBody.process_hubs, [publicHubSid, publicExtraHubSid].sort());
    assert.deepEqual(blockedBody.extra_process_hubs, [publicExtraHubSid]);
    assert.deepEqual(blockedBody.stopping_hubs, [publicExtraHubSid]);
    assert.deepEqual(Object.keys(blockedBody.runner_bots), [publicHubSid]);
    assert.equal(JSON.stringify(blockedBody).includes(hubSid), false);
    assert.equal(JSON.stringify(blockedBody).includes("ready-contract-extra"), false);
  } finally {
    internals.resetRuntimeStateForTests();
  }
});

test("runner watchdog bounds config, startup, stale-status and terminal spawn failures", () => {
  const now = 100_000;
  const starting = overrides => ({
    ...validRunningGhostProcessState(),
    lifecycle: "starting",
    spawned: false,
    startedAt: now - 1_000,
    lastRuntimeStatusAt: 0,
    ...overrides
  });
  assert.equal(
    internals.runnerRecoveryReason(
      starting({
        pendingConfigFingerprint: "pending",
        pendingConfigSentAt: now - 101,
      }),
      now,
      { configAckTimeoutMs: 100, startupGraceMs: 10_000 }
    ),
    null
  );
  assert.equal(
    internals.runnerRecoveryReason(
      starting({
        pendingConfigFingerprint: "pending",
        pendingConfigSentAt: now - 10_001,
      }),
      now,
      { configAckTimeoutMs: 100, startupGraceMs: 10_000 }
    ),
    "config_ack_timeout"
  );
  assert.equal(
    internals.runnerRecoveryReason(
      starting({ startedAt: now - 101 }),
      now,
      { startupGraceMs: 100 }
    ),
    "runtime_startup_timeout"
  );
  assert.equal(
    internals.runnerRecoveryReason(
      { ...validRunningGhostProcessState(), startedAt: 1, lastRuntimeStatusAt: now - 101 },
      now,
      { staleRestartMs: 100 }
    ),
    "runtime_status_stale"
  );
  assert.equal(
    internals.runnerRecoveryReason(
      {
        ...validRunningGhostProcessState(),
        startedAt: 1,
        lastRuntimeStatusAt: now,
        botStatusReason: "spawn_rejected",
        terminalStatusAt: now - 101
      },
      now,
      { terminalGraceMs: 100 }
    ),
    "spawn_rejected"
  );
  assert.equal(
    internals.runnerRecoveryReason(
      {
        ...validRunningGhostProcessState(),
        startedAt: 1,
        lastRuntimeStatusAt: now,
        botStatusReason: "spawn_cleanup_uncertain",
        terminalStatusAt: now - 101
      },
      now,
      { terminalGraceMs: 100 }
    ),
    "spawn_cleanup_uncertain"
  );

  const invalidHeartbeat = starting({
    authenticated: true,
    configFingerprint: "expected",
    pendingConfigFingerprint: null,
    desiredBots: 1,
    startedAt: now - 101,
    lastAnyRuntimeStatusAt: 0,
    lastRuntimeStatusAt: 0
  });
  assert.equal(
    internals.applyGhostRuntimeStatus(invalidHeartbeat, {
      authenticated: true,
      authoritativeSpawnAcks: true,
      desired: 1,
      active: 1,
      navigationReady: true,
      ready: true,
      reason: "ready",
      configFingerprint: "wrong",
      configRevision: 1,
      processGeneration: invalidHeartbeat.processGeneration
    }, now),
    false
  );
  assert.equal(invalidHeartbeat.lastAnyRuntimeStatusAt, 0);
  assert.equal(
    internals.runnerRecoveryReason(invalidHeartbeat, now, { startupGraceMs: 100 }),
    "runtime_startup_timeout"
  );

  const info = starting({
    ready: false,
    recoveryRequested: false
  });
  const runners = new Map([["room", info]]);
  const configs = new Map([["room", { bots: { enabled: true, count: 1 } }]]);
  const requested = [];
  const recoveries = internals.superviseRunners(runners, configs, now, {
    requestSignal(_runner, signal) {
      requested.push(signal);
      return true;
    },
    recoveryOptions: { startupGraceMs: 100 }
  });
  assert.deepEqual(recoveries, [
    { hubSid: "room", reason: "runtime_startup_timeout", action: "sigterm", signalSent: true }
  ]);
  assert.deepEqual(requested, ["SIGTERM"]);
  assert.deepEqual(internals.superviseRunners(runners, configs, now + 1000), []);

  configs.set("room", { bots: { enabled: false, count: 1 } });
  assert.deepEqual(internals.superviseRunners(runners, configs, now + 2000), []);
});

test("watchdog rejects future and negative-age runtime, config and terminal clocks immediately", () => {
  const now = 300_000;
  const starting = overrides => ({
    ...validRunningGhostProcessState(),
    lifecycle: "starting",
    spawned: false,
    startedAt: now - 1_000,
    lastRuntimeStatusAt: 0,
    ...overrides
  });
  const cases = [
    [
      starting({ pendingConfigFingerprint: "pending", pendingConfigSentAt: now + 1 }),
      "config_ack_clock_invalid"
    ],
    [
      starting({ pendingConfigFingerprint: "pending", pendingConfigSentAt: -1 }),
      "config_ack_clock_invalid"
    ],
    [
      { ...validRunningGhostProcessState(), lastRuntimeStatusAt: now + 1 },
      "runtime_status_clock_invalid"
    ],
    [
      { ...validRunningGhostProcessState(), lastRuntimeStatusAt: -1 },
      "runtime_status_clock_invalid"
    ],
    [
      {
        ...validRunningGhostProcessState(),
        lastRuntimeStatusAt: now,
        botStatusReason: "spawn_rejected",
        terminalStatusAt: now + 1
      },
      "terminal_status_clock_invalid"
    ],
    [
      {
        ...validRunningGhostProcessState(),
        lastRuntimeStatusAt: now,
        botStatusReason: "spawn_cleanup_uncertain",
        terminalStatusAt: -1
      },
      "terminal_status_clock_invalid"
    ]
  ];

  for (const [info, reason] of cases) {
    assert.equal(internals.runnerRecoveryReason(info, now), reason);
    const signals = [];
    const recoveries = internals.superviseRunners(
      new Map([["clock-room", info]]),
      new Map([["clock-room", { bots: { enabled: true, count: 1 } }]]),
      now,
      {
        requestSignal(_runner, signal) {
          signals.push(signal);
          return true;
        }
      }
    );
    assert.deepEqual(recoveries, [
      { hubSid: "clock-room", reason, action: "sigterm", signalSent: true }
    ]);
    assert.deepEqual(signals, ["SIGTERM"]);
  }
});

test("watchdog sanitizes future termination, TERM and KILL clocks and still completes bounded escalation", () => {
  const now = 400_000;
  const makeStopping = overrides => ({
    ...validRunningGhostProcessState(),
    lifecycle: "stopping",
    botStatusReason: "runtime_status_stale",
    terminationStartedAt: now - 50,
    lastTerminationAttemptAt: now - 10,
    sigkillAttemptedAt: 0,
    supervisorRestartRequested: false,
    ...overrides
  });
  const configs = new Map([["clock-room", { bots: { enabled: true, count: 1 } }]]);

  for (const [overrides, expectedReason, expectedAction] of [
    [{ terminationStartedAt: now + 1 }, "termination_clock_invalid", "sigterm"],
    [{ terminationStartedAt: -1 }, "termination_clock_invalid", "sigterm"],
    [{ lastTerminationAttemptAt: now + 1 }, "term_attempt_clock_invalid", "sigterm"],
    [{ sigkillAttemptedAt: now + 1, terminationStartedAt: now - 100 }, "kill_attempt_clock_invalid", "sigkill"]
  ]) {
    const info = makeStopping(overrides);
    const signals = [];
    const recoveries = internals.superviseRunners(
      new Map([["clock-room", info]]),
      configs,
      now,
      {
        requestSignal(_runner, signal) {
          signals.push(signal);
          return true;
        },
        terminationGraceMs: 100,
        killGraceMs: 50,
        retryIntervalMs: 10
      }
    );
    assert.equal(recoveries[0].reason, expectedReason);
    assert.equal(recoveries[0].action, expectedAction);
    assert.deepEqual(signals, [expectedAction === "sigkill" ? "SIGKILL" : "SIGTERM"]);
  }

  const bounded = makeStopping({
    terminationStartedAt: now + 10_000,
    lastTerminationAttemptAt: now + 10_000,
    sigkillAttemptedAt: now + 10_000
  });
  const runners = new Map([["clock-room", bounded]]);
  const signals = [];
  const options = {
    requestSignal(_runner, signal) {
      signals.push(signal);
      return true;
    },
    terminationGraceMs: 100,
    killGraceMs: 50,
    retryIntervalMs: 10
  };
  assert.equal(internals.superviseRunners(runners, configs, now, options)[0].action, "sigterm");
  assert.equal(internals.superviseRunners(runners, configs, now + 100, options)[0].action, "sigkill");
  assert.deepEqual(internals.superviseRunners(runners, configs, now + 150, options), [
    { hubSid: "clock-room", reason: "termination_unconfirmed", action: "supervisor_restart_required" }
  ]);
  assert.deepEqual(signals, ["SIGTERM", "SIGKILL"]);
});

test("runner restart backoff is bounded per hub and resets after stability", () => {
  const state = new Map();
  const first = internals.nextRunnerRestartDelay("room", state, 1);
  const second = internals.nextRunnerRestartDelay("room", state, 2);
  assert.ok(second > first);
  let last = second;
  for (let i = 0; i < 20; i++) last = internals.nextRunnerRestartDelay("room", state, 3 + i);
  assert.ok(last <= 300_000);
  internals.resetRunnerRestartBackoff("room", state);
  assert.equal(internals.nextRunnerRestartDelay("room", state, 100), first);
});

test("watchdog retries failed termination, escalates once to SIGKILL and then requests supervisor restart", () => {
  const now = 100_000;
  const info = {
    ...validRunningGhostProcessState(),
    startedAt: now - 1000,
    lastRuntimeStatusAt: 0,
    ready: false
  };
  const runners = new Map([["room", info]]);
  const configs = new Map([["room", { bots: { enabled: true, count: 1 } }]]);
  const signals = [];
  const options = {
    requestSignal(_runner, signal) {
      signals.push(signal);
      throw new Error("transport failure");
    },
    recoveryOptions: { startupGraceMs: 100 },
    terminationGraceMs: 100,
    killGraceMs: 50,
    retryIntervalMs: 10
  };

  assert.deepEqual(internals.superviseRunners(runners, configs, now, options), [
    { hubSid: "room", reason: "runtime_startup_timeout", action: "sigterm", signalSent: false }
  ]);
  assert.deepEqual(internals.superviseRunners(runners, configs, now + 10, options), [
    { hubSid: "room", reason: "runtime_startup_timeout", action: "sigterm", signalSent: false }
  ]);
  assert.deepEqual(internals.superviseRunners(runners, configs, now + 100, options), [
    { hubSid: "room", reason: "termination_grace_exceeded", action: "sigkill", signalSent: false }
  ]);
  assert.deepEqual(internals.superviseRunners(runners, configs, now + 150, options), [
    { hubSid: "room", reason: "termination_unconfirmed", action: "supervisor_restart_required" }
  ]);
  assert.deepEqual(signals, ["SIGTERM", "SIGTERM", "SIGKILL"]);
  assert.deepEqual(internals.superviseRunners(runners, configs, now + 1000, options), []);
});

test("watchdog escalates when kill reports success but the child never exits", () => {
  const now = 200_000;
  const info = {
    ...validRunningGhostProcessState(),
    startedAt: now - 1_000,
    lastRuntimeStatusAt: 0,
    ready: false
  };
  const runners = new Map([["ignored-signal-room", info]]);
  const configs = new Map([["ignored-signal-room", { bots: { enabled: true, count: 1 } }]]);
  const signals = [];
  const options = {
    requestSignal(_runner, signal) {
      signals.push(signal);
      return true;
    },
    recoveryOptions: { startupGraceMs: 100 },
    terminationGraceMs: 100,
    killGraceMs: 50,
    retryIntervalMs: 10
  };

  assert.equal(internals.superviseRunners(runners, configs, now, options)[0].action, "sigterm");
  assert.equal(internals.superviseRunners(runners, configs, now + 10, options)[0].action, "sigterm");
  assert.equal(internals.superviseRunners(runners, configs, now + 100, options)[0].action, "sigkill");
  assert.deepEqual(internals.superviseRunners(runners, configs, now + 150, options), [
    {
      hubSid: "ignored-signal-room",
      reason: "termination_unconfirmed",
      action: "supervisor_restart_required"
    }
  ]);
  assert.deepEqual(signals, ["SIGTERM", "SIGTERM", "SIGKILL"]);
});

test("watchdog never resets backoff from cached readiness after status becomes stale", () => {
  const now = 40_000;
  const bots = { enabled: true, count: 1, mobility: "medium" };
  const config = { bots };
  const info = validAuthoritativeRunnerState(bots, 1, { readySince: 1, startedAt: 1 });
  const resets = [];
  const recoveries = internals.superviseRunners(
    new Map([["room", info]]),
    new Map([["room", config]]),
    now,
    {
      resetBackoff: hubSid => resets.push(hubSid),
      requestSignal: () => true,
      healthTtlMs: 100,
      recoveryOptions: { staleRestartMs: 100 }
    }
  );
  assert.deepEqual(resets, []);
  assert.equal(recoveries[0].reason, "runtime_status_stale");

  const disconnected = validAuthoritativeRunnerState(bots, now - 10, {
    readySince: 1,
    startedAt: 1
  });
  disconnected.process = { ...disconnected.process, connected: false };
  const disconnectedRecovery = internals.superviseRunners(
    new Map([["disconnected-room", disconnected]]),
    new Map([["disconnected-room", config]]),
    now,
    { resetBackoff: hubSid => resets.push(hubSid), requestSignal: () => true, healthTtlMs: 100 }
  );
  assert.deepEqual(resets, []);
  assert.equal(disconnectedRecovery[0].reason, "config_channel_disconnected");

  const fresh = validAuthoritativeRunnerState(bots, now - 10, {
    ready: false,
    readySince: 1,
    startedAt: 1
  });
  internals.superviseRunners(
    new Map([["fresh-room", fresh]]),
    new Map([["fresh-room", config]]),
    now,
    { resetBackoff: hubSid => resets.push(hubSid), healthTtlMs: 100 }
  );
  assert.deepEqual(resets, ["fresh-room"]);
});

test("watchdog classifies malformed states and recovers signalable versus unsignalable runners once", () => {
  const now = 50_000;
  const baseline = {
    ...validRunningGhostProcessState(),
    ready: true,
    readySince: now - 1_000,
    lastRuntimeStatusAt: now,
    startedAt: now - 1_000
  };
  const variant = overrides => ({
    ...baseline,
    process: { ...baseline.process },
    ...overrides
  });
  const missingPid = variant();
  delete missingPid.process.pid;
  const missingIpcState = variant();
  delete missingIpcState.ipcConnected;
  const missingProcessConnected = variant();
  delete missingProcessConnected.process.connected;
  const missingSpawnState = variant();
  delete missingSpawnState.spawned;
  const missingLifecycle = variant();
  delete missingLifecycle.lifecycle;
  const startingWithoutClock = variant({ lifecycle: "starting", spawned: false });
  delete startingWithoutClock.startedAt;
  const classificationCases = [
    [null, "runner_state_missing"],
    [{}, "runner_state_invalid"],
    [variant({ backend: "chromium" }), "runner_backend_invalid"],
    [variant({ lifecycle: "invalid" }), "runner_lifecycle_invalid"],
    [missingLifecycle, "runner_lifecycle_invalid"],
    [startingWithoutClock, "runner_start_clock_invalid"],
    [variant({ lifecycle: "starting", process: undefined }), "runner_process_invalid"]
  ];
  for (const [info, reason] of classificationCases) {
    assert.equal(internals.runnerRecoveryReason(info, now), reason);
  }

  const signalableCases = [
    [variant({ backend: "chromium" }), "runner_backend_invalid"],
    [variant({ lifecycle: "invalid" }), "runner_lifecycle_invalid"],
    [missingSpawnState, "runner_not_spawned"],
    [variant({ spawned: false }), "runner_not_spawned"],
    [missingIpcState, "config_channel_disconnected"],
    [variant({ ipcConnected: false }), "config_channel_disconnected"],
    [missingProcessConnected, "config_channel_disconnected"],
    [variant({ process: { ...baseline.process, connected: false } }), "config_channel_disconnected"]
  ];

  for (const [info, reason] of signalableCases) {
    const signals = [];
    const recoveries = internals.superviseRunners(
      new Map([["malformed-room", info]]),
      new Map([["malformed-room", { bots: { enabled: true, count: 1 } }]]),
      now,
      {
        requestSignal(_runner, signal) {
          signals.push(signal);
          return true;
        }
      }
    );
    assert.deepEqual(recoveries, [
      { hubSid: "malformed-room", reason, action: "sigterm", signalSent: true }
    ]);
    assert.equal(info.lifecycle, "stopping");
    assert.equal(info.ready, false);
    assert.deepEqual(signals, ["SIGTERM"]);
  }

  const noKill = variant();
  delete noKill.process.kill;
  const unsignalableCases = [
    [null, "runner_state_missing"],
    [{}, "runner_state_invalid"],
    [variant({ process: undefined }), "runner_process_invalid"],
    [variant({ process: {} }), "runner_process_invalid"],
    [missingPid, "runner_process_invalid"],
    [variant({ process: { pid: -1, connected: true } }), "runner_process_invalid"],
    [variant({ process: { pid: 2.5, connected: true } }), "runner_process_invalid"],
    [noKill, "runner_process_unsignalable"]
  ];
  for (const [info, reason] of unsignalableCases) {
    const runners = new Map([["unsignalable-room", info]]);
    let recoveryCalls = 0;
    const recoveries = internals.superviseRunners(
      runners,
      new Map([["unsignalable-room", { bots: { enabled: true, count: 1 } }]]),
      now,
      {
        requestSignal() {
          assert.fail("an unsignalable process must never be signalled");
        },
        recoverUnsignalable(hubSid, staleInfo, { runners: currentRunners }) {
          recoveryCalls += 1;
          assert.equal(currentRunners.get(hubSid), staleInfo);
          currentRunners.delete(hubSid);
          return "restart_scheduled";
        }
      }
    );
    assert.deepEqual(recoveries, [
      { hubSid: "unsignalable-room", reason, action: "restart_scheduled", signalSent: false }
    ]);
    assert.equal(recoveryCalls, 1);
    assert.equal(runners.size, 0);
    assert.deepEqual(internals.superviseRunners(runners, new Map(), now + 1), []);
    assert.equal(recoveryCalls, 1);
  }
});

test("unsignalable recovery schedules one forced ghost replacement and one backoff step", () => {
  const hubSid = "recover-missing-runner";
  const malformed = null;
  const runners = new Map([[hubSid, malformed]]);
  const configs = new Map([[hubSid, { bots: { enabled: true, count: 1 } }]]);
  const restartTimers = new Map();
  const generations = new Map();
  let scheduledCallback;
  let backoffCalls = 0;
  let starts = 0;

  const dependencies = {
    runners,
    configs,
    restartTimers,
    generations,
    canAutostart: () => true,
    restartDelayForHub(requestedHubSid) {
      assert.equal(requestedHubSid, hubSid);
      backoffCalls += 1;
      return 0;
    },
    schedule(callback) {
      scheduledCallback = callback;
      return { scheduled: true };
    },
    start(requestedHubSid) {
      assert.equal(requestedHubSid, hubSid);
      starts += 1;
      return true;
    },
    enqueue() {
      assert.fail("a successful forced ghost start must not enqueue");
    },
    fillQueue() {}
  };

  assert.equal(
    internals.recoverUnsignalableRunner(hubSid, malformed, dependencies),
    "restart_scheduled"
  );
  assert.equal(runners.has(hubSid), false);
  assert.equal(restartTimers.has(hubSid), true);
  assert.equal(backoffCalls, 1);
  assert.equal(
    internals.recoverUnsignalableRunner(hubSid, malformed, dependencies),
    "stale_state"
  );
  assert.equal(backoffCalls, 1);
  scheduledCallback();
  assert.equal(starts, 1);
  assert.equal(restartTimers.has(hubSid), false);
  assert.equal(generations.has(hubSid), false);
});

test("stopRunner is idempotent and keeps a runner unready until confirmed exit", () => {
  const now = 1000;
  const signals = [];
  const info = {
    backend: "ghost",
    lifecycle: "running",
    ready: true,
    readySince: 1,
    lastRuntimeStatusAt: now,
    process: { kill: () => true }
  };
  const runners = new Map([["room", info]]);
  const restartTimers = new Map();
  const generations = new Map();
  const backoff = new Map([["room", { failures: 3 }]]);

  const state = internals.stopRunner("room", {
    runners,
    restartTimers,
    generations,
    backoff,
    dequeue() {},
    requestSignal(_runner, signal) {
      signals.push(signal);
      return true;
    },
    nowMs: now
  });
  assert.equal(state, "stopping");
  assert.equal(runners.get("room"), info);
  assert.equal(info.lifecycle, "stopping");
  assert.deepEqual(signals, ["SIGTERM"]);
  assert.equal(backoff.has("room"), false);

  const repeated = internals.stopRunner("room", {
    runners,
    restartTimers,
    generations,
    backoff,
    dequeue() {},
    requestSignal(_runner, signal) {
      signals.push(signal);
      return true;
    },
    nowMs: now + 1
  });
  assert.equal(repeated, "stopping");
  assert.deepEqual(signals, ["SIGTERM"]);

  const readiness = internals.runnerReadinessSnapshot(
    new Map(),
    runners,
    now,
    100,
    { seen: true, valid: true, receivedAt: now, ttlMs: 1000 }
  );
  assert.equal(readiness.ok, false);
  assert.deepEqual(readiness.extra_process_hubs, ["room"]);
  assert.deepEqual(readiness.stopping_hubs, ["room"]);

  let restartCallback;
  let starts = 0;
  const configs = new Map([["room", { bots: { enabled: true, count: 1 } }]]);
  assert.equal(starts, 0);
  assert.equal(
    internals.handleRunnerExit("room", info, {
      runners,
      configs,
      restartTimers,
      generations,
      canAutostart: () => true,
      schedule(callback) {
        restartCallback = callback;
        return { id: "restart-after-exit" };
      },
      start() {
        starts += 1;
        return true;
      },
      fillQueue() {}
    }),
    "restart_scheduled"
  );
  assert.equal(runners.has("room"), false);
  assert.equal(starts, 0);
  restartCallback();
  assert.equal(starts, 1);
});

test("a post-spawn child error waits for exit while a spawn failure may finish immediately", () => {
  let finished = 0;
  const running = {
    process: { pid: 123 },
    spawned: true,
    lifecycle: "running",
    ready: true
  };
  assert.equal(
    internals.handleRunnerProcessError(running, {
      child: running.process,
      finishChild: () => {
        finished += 1;
      },
      requestSignal: () => true,
      nowMs: 1000
    }),
    "awaiting_exit"
  );
  assert.equal(finished, 0);
  assert.equal(running.lifecycle, "stopping");

  const failedSpawn = { process: {}, spawned: false, lifecycle: "starting" };
  assert.equal(
    internals.handleRunnerProcessError(failedSpawn, {
      child: failedSpawn.process,
      finishChild: () => {
        finished += 1;
      }
    }),
    "spawn_failed"
  );
  assert.equal(finished, 1);
});

test("an IPC disconnect after a process error updates the reason without signalling twice", () => {
  const signals = [];
  const info = {
    backend: "ghost",
    process: { pid: 123 },
    ipcConnected: true,
    spawned: true,
    lifecycle: "running",
    ready: true
  };
  const requestSignal = (_runner, signal) => {
    signals.push(signal);
    return true;
  };

  internals.handleRunnerProcessError(info, { requestSignal, nowMs: 1_000 });
  internals.handleRunnerIpcDisconnect(info, { requestSignal, nowMs: 1_001 });

  assert.equal(info.lifecycle, "stopping");
  assert.equal(info.botStatusReason, "config_channel_disconnected");
  assert.deepEqual(signals, ["SIGTERM"]);
});

test("IPC disconnect fails closed, signals once and replaces only after one confirmed exit", () => {
  const child = new EventEmitter();
  child.connected = true;
  child.pid = 123;
  const signals = [];
  child.kill = signal => {
    signals.push(signal);
    return true;
  };

  const now = Date.now();
  const info = {
    backend: "ghost",
    process: child,
    ipcConnected: true,
    spawned: true,
    lifecycle: "running",
    desiredBots: 1,
    activeBots: 1,
    authenticated: true,
    authoritativeSpawnAcks: true,
    ready: true,
    readySince: now - 100,
    lastRuntimeStatusAt: now,
    botStatusReason: "ready"
  };
  const runners = new Map([["ipc-room", info]]);
  const configs = new Map([["ipc-room", { bots: { enabled: true, count: 1 } }]]);
  const restartTimers = new Map();
  const generations = new Map();
  let restartCallback = null;
  let exits = 0;
  let starts = 0;

  internals.attachRunnerLifecycleHandlers("ipc-room", info, child, {
    runners,
    warn() {},
    finishRunner(hubSid, exitedInfo) {
      exits += 1;
      return internals.handleRunnerExit(hubSid, exitedInfo, {
        runners,
        configs,
        restartTimers,
        generations,
        canAutostart: () => true,
        restartDelayForHub: () => 0,
        schedule(callback) {
          restartCallback = callback;
          return { id: "ipc-restart" };
        },
        start() {
          starts += 1;
          return true;
        },
        enqueue() {},
        fillQueue() {}
      });
    }
  });

  child.connected = false;
  child.emit("disconnect");
  assert.equal(info.lifecycle, "stopping");
  assert.equal(info.botStatusReason, "config_channel_disconnected");
  assert.equal(info.ready, false);
  assert.equal(info.activeBots, 0);
  assert.equal(info.authenticated, false);
  assert.deepEqual(signals, ["SIGTERM"]);
  assert.equal(runners.get("ipc-room"), info);
  assert.equal(starts, 0);
  assert.equal(
    internals.runnerReadinessSnapshot(configs, runners, now, 100, {
      seen: true,
      valid: true,
      receivedAt: now,
      ttlMs: 1_000
    }).ok,
    false
  );

  child.emit("error", new Error("arrived after disconnect"));
  child.emit("disconnect");
  assert.deepEqual(signals, ["SIGTERM"]);
  assert.equal(exits, 0);

  child.emit("exit", 1, null);
  child.emit("close", 1, null);
  assert.equal(exits, 1);
  assert.equal(runners.has("ipc-room"), false);
  assert.equal(starts, 0);
  assert.equal(typeof restartCallback, "function");

  restartCallback();
  assert.equal(starts, 1);
  restartCallback();
  assert.equal(starts, 1);
});

test("restart timers are generation-scoped and cancelled callbacks cannot revive a room", () => {
  const runners = new Map();
  const configs = new Map([["room", { bots: { enabled: true, count: 1 } }]]);
  const restartTimers = new Map();
  const generations = new Map();
  const callbacks = [];
  let starts = 0;
  const options = {
    runners,
    configs,
    restartTimers,
    generations,
    schedule(callback) {
      callbacks.push(callback);
      return { id: callbacks.length };
    },
    clearTimer() {},
    canAutostart: () => true,
    start() {
      starts += 1;
      return true;
    },
    enqueue() {},
    fillQueue() {}
  };

  internals.scheduleRunnerRestart("room", 10, options);
  internals.cancelRunnerRestart("room", {
    restartTimers,
    generations,
    clearTimer() {}
  });
  callbacks[0]();
  assert.equal(starts, 0);

  internals.scheduleRunnerRestart("room", 10, options);
  callbacks[1]();
  assert.equal(starts, 1);
  assert.equal(generations.size, 0);
  callbacks[0]();
  assert.equal(starts, 1);
});

test("cancelled restart generations do not retain historical hub ids", () => {
  const restartTimers = new Map();
  const generations = new Map();

  for (let i = 0; i < 1_000; i++) {
    internals.cancelRunnerRestart(`historical-room-${i}`, {
      restartTimers,
      generations,
      clearTimer() {}
    });
  }

  assert.equal(generations.size, 0);
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

test("chat fails closed until the authoritative snapshot and exact runner are ready", async () => {
  internals.resetRuntimeStateForTests();
  const config = await post("/internal/bots/room-config", {
    hub_sid: "room-chat-pending",
    bots: { enabled: true, count: 1, mobility: "medium", chat_enabled: true }
  });
  assert.equal(config.status, 200);

  const response = await post("/internal/bots/chat", {
    hub_sid: "room-chat-pending",
    bot_id: "bot-1",
    requester_id: "account-pending",
    message: "hola"
  });
  assert.equal(response.status, 503);
  assert.deepEqual(await response.json(), { error: "bot service unavailable" });
  internals.resetRuntimeStateForTests();
});

test("room-config admission is atomic at the deployed room ceiling and reopens after stop", async () => {
  internals.resetRuntimeStateForTests();

  for (let index = 1; index <= 5; index += 1) {
    const accepted = await post("/internal/bots/room-config", {
      hub_sid: "admission-room-" + index,
      bots: { enabled: true, count: 1, mobility: "static", chat_enabled: false }
    });
    assert.equal(accepted.status, 200);
  }

  const rejected = await post("/internal/bots/room-config", {
    hub_sid: "admission-room-6",
    bots: { enabled: true, count: 1, mobility: "static", chat_enabled: false }
  });
  assert.equal(rejected.status, 409);
  assert.deepEqual(await rejected.json(), {
    error: "configured_room_limit_exceeded",
    max_configured_rooms: 5
  });

  const stopped = await post("/internal/bots/room-stop", { hub_sid: "admission-room-1" });
  assert.equal(stopped.status, 200);

  const replacement = await post("/internal/bots/room-config", {
    hub_sid: "admission-room-6",
    bots: { enabled: true, count: 1, mobility: "static", chat_enabled: false }
  });
  assert.equal(replacement.status, 200);
  internals.resetRuntimeStateForTests();
});

test("preserves static mobility and never emits navigation actions for static bots", async () => {
  assert.equal(internals.normalizeConfig({ mobility: "static" }).mobility, "static");

  await configureReadyRoom("room-static", {
    enabled: true,
    count: 1,
    mobility: "static",
    chat_enabled: true
  });

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
  await configureReadyRoom("room-a", {
    enabled: true,
    count: 2,
    mobility: "medium",
    chat_enabled: true,
    prompt: "Sé amable."
  });

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
  await configureReadyRoom("room-rate-limit", {
    enabled: true,
    count: 4,
    mobility: "medium",
    chat_enabled: true
  });

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

test("full snapshots validate atomically and malformed payloads preserve prior state", async () => {
  internals.resetRuntimeStateForTests();
  const validRoom = {
    hub_sid: "room-existing",
    bots: { enabled: true, count: 1, mobility: "static", chat_enabled: false, prompt: "" }
  };

  await internals.syncActiveRoomsFromReticulum({
    fetchImpl: async () => ({ ok: true, status: 200, json: async () => ({ hubs: [validRoom] }) })
  });

  let calls = 0;
  await internals.syncActiveRoomsFromReticulum({
    fetchImpl: async () => {
      calls += 1;
      return {
        ok: true,
        status: 200,
        json: async () =>
          calls === 1
            ? {
                hubs: [
                  {
                    hub_sid: "room-new",
                    bots: { enabled: true, count: 1, mobility: "medium", chat_enabled: false, prompt: "" }
                  },
                  { hub_sid: "broken", bots: { enabled: true, count: "many" } }
                ]
              }
            : { hubs: [] }
      };
    }
  });

  const health = await (await fetch(`${baseUrl}/health`)).json();
  const readiness = await (await fetch(`${baseUrl}/ready`)).json();
  assert.equal(health.rooms, 1);
  assert.equal(health.authoritative_snapshot_valid, false);
  assert.deepEqual(readiness.expected_hubs, [internals.publicHubIdentifier("room-existing")]);
  assert.equal(JSON.stringify(readiness).includes("room-existing"), false);
  assert.equal(readiness.authoritative_snapshot_ready, false);
  assert.equal(readiness.snapshot_reason, "authoritative_snapshot_invalid");
});

test("configured-room snapshots are byte-bounded and reject more than ten rooms atomically", async () => {
  const room = index => ({
    hub_sid: `bounded-room-${index}`,
    bots: { enabled: true, count: 1, mobility: "static", chat_enabled: false, prompt: "" }
  });
  assert.throws(
    () => internals.parseRoomSnapshot({ hubs: Array.from({ length: 11 }, (_value, index) => room(index)) }),
    /configured_room_limit_exceeded/
  );
  await assert.rejects(
    internals.fetchRoomSnapshot(
      "http://ret:4001/api-internal/v1/hubs/configured_with_bots",
      {},
      async () => ({
        ok: true,
        status: 200,
        headers: { get: name => (name === "content-length" ? "200000" : null) },
        json: async () => ({ hubs: [] })
      })
    ),
    /room_snapshot_response_too_large/
  );

  internals.resetRuntimeStateForTests();
  await internals.syncActiveRoomsFromReticulum({
    fetchImpl: async () => ({ ok: true, status: 200, json: async () => ({ hubs: [room("existing")] }) })
  });
  let calls = 0;
  await internals.syncActiveRoomsFromReticulum({
    fetchImpl: async () => {
      calls += 1;
      return {
        ok: true,
        status: 200,
        json: async () => ({
          hubs: calls === 1 ? Array.from({ length: 11 }, (_value, index) => room(index)) : []
        })
      };
    }
  });
  const readiness = await (await fetch(`${baseUrl}/ready`)).json();
  assert.deepEqual(readiness.expected_hubs, [internals.publicHubIdentifier("bounded-room-existing")]);
  assert.equal(JSON.stringify(readiness).includes("bounded-room-existing"), false);
  assert.equal(readiness.authoritative_snapshot_ready, false);
  assert.equal(readiness.snapshot_reason, "configured_room_limit_exceeded");
});

test("room synchronization uses only the parent orchestrator credential", async () => {
  internals.resetRuntimeStateForTests();
  let observedRequest;

  await internals.syncActiveRoomsFromReticulum({
    fetchImpl: async (url, options) => {
      observedRequest = { url, headers: options.headers };
      return { ok: true, status: 200, json: async () => ({ hubs: [] }) };
    }
  });

  assert.equal(observedRequest.url, "http://ret:4001/api-internal/v1/hubs/configured_with_bots");
  assert.deepEqual(observedRequest.headers, {
    "x-ret-bot-orchestrator-access-key": process.env.BOT_ORCHESTRATOR_ACCESS_KEY
  });
});

test("a newer POST supersedes an in-flight snapshot and coalesces one trailing full sync", async () => {
  internals.resetRuntimeStateForTests();
  let resolveFirstJson;
  let calls = 0;
  const finalRooms = [
    {
      hub_sid: "room-new-a",
      bots: { enabled: true, count: 1, mobility: "static", chat_enabled: false, prompt: "" }
    },
    {
      hub_sid: "room-new-b",
      bots: { enabled: true, count: 2, mobility: "medium", chat_enabled: false, prompt: "" }
    }
  ];
  const fetchImpl = async () => {
    calls += 1;
    if (calls === 1) {
      return {
        ok: true,
        status: 200,
        json: () => new Promise(resolve => {
          resolveFirstJson = resolve;
        })
      };
    }
    return { ok: true, status: 200, json: async () => ({ hubs: finalRooms }) };
  };

  const sync = internals.syncActiveRoomsFromReticulum({ fetchImpl });
  while (!resolveFirstJson) await new Promise(resolve => setImmediate(resolve));

  const firstPost = await post("/internal/bots/room-config", {
    hub_sid: "room-new-a",
    bots: finalRooms[0].bots
  });
  const secondPost = await post("/internal/bots/room-config", {
    hub_sid: "room-new-b",
    bots: finalRooms[1].bots
  });
  assert.equal(firstPost.status, 200);
  assert.equal(secondPost.status, 200);

  resolveFirstJson({ hubs: [] });
  await sync;

  assert.equal(calls, 2);
  const health = await (await fetch(`${baseUrl}/health`)).json();
  const readinessResponse = await fetch(`${baseUrl}/ready`);
  const readiness = await readinessResponse.json();
  assert.equal(health.rooms, 2);
  assert.equal(health.authoritative_snapshot_valid, true);
  assert.equal(readinessResponse.status, 503);
  assert.equal(readiness.authoritative_snapshot_ready, true);
  assert.deepEqual(
    readiness.expected_hubs,
    [internals.publicHubIdentifier("room-new-a"), internals.publicHubIdentifier("room-new-b")]
  );
  assert.equal(JSON.stringify(readiness).includes("room-new-a"), false);
  assert.equal(JSON.stringify(readiness).includes("room-new-b"), false);
});

test("room-stop during an in-flight snapshot cannot be resurrected by the stale response", async () => {
  internals.resetRuntimeStateForTests();
  const configured = {
    hub_sid: "room-to-stop",
    bots: { enabled: true, count: 1, mobility: "static", chat_enabled: false, prompt: "" }
  };
  await internals.syncActiveRoomsFromReticulum({
    fetchImpl: async () => ({ ok: true, status: 200, json: async () => ({ hubs: [configured] }) })
  });

  let resolveFirstJson;
  let calls = 0;
  const sync = internals.syncActiveRoomsFromReticulum({
    fetchImpl: async () => {
      calls += 1;
      if (calls === 1) {
        return {
          ok: true,
          status: 200,
          json: () => new Promise(resolve => {
            resolveFirstJson = resolve;
          })
        };
      }
      return { ok: true, status: 200, json: async () => ({ hubs: [] }) };
    }
  });
  while (!resolveFirstJson) await new Promise(resolve => setImmediate(resolve));

  const stopped = await post("/internal/bots/room-stop", { hub_sid: "room-to-stop" });
  assert.equal(stopped.status, 200);
  resolveFirstJson({ hubs: [configured] });
  await sync;

  assert.equal(calls, 2);
  assert.equal((await (await fetch(`${baseUrl}/health`)).json()).rooms, 0);
  const readiness = await fetch(`${baseUrl}/ready`);
  assert.equal(readiness.status, 503);
  assert.deepEqual((await readiness.json()).expected_hubs, []);
});

test("a fallback cannot overwrite a newer POST and readiness waits for the trailing full snapshot", async () => {
  internals.resetRuntimeStateForTests();
  const room = {
    hub_sid: "fallback-race-room",
    bots: { enabled: true, count: 3, mobility: "high", chat_enabled: false, prompt: "" }
  };
  let resolveFallbackJson;
  let calls = 0;
  const sync = internals.syncActiveRoomsFromReticulum({
    fetchImpl: async () => {
      calls += 1;
      if (calls === 1) return { ok: false, status: 503, json: async () => ({}) };
      if (calls === 2) {
        return {
          ok: true,
          status: 200,
          json: () => new Promise(resolve => {
            resolveFallbackJson = resolve;
          })
        };
      }
      return { ok: true, status: 200, json: async () => ({ hubs: [room] }) };
    }
  });
  while (!resolveFallbackJson) await new Promise(resolve => setImmediate(resolve));

  const updated = await post("/internal/bots/room-config", { hub_sid: room.hub_sid, bots: room.bots });
  assert.equal(updated.status, 200);
  const pending = await fetch(`${baseUrl}/ready`);
  assert.equal(pending.status, 503);
  assert.equal((await pending.json()).authoritative_snapshot_ready, false);
  resolveFallbackJson({ hubs: [] });
  await sync;

  assert.equal(calls, 3);
  const readiness = await fetch(`${baseUrl}/ready`);
  const body = await readiness.json();
  assert.equal(readiness.status, 503);
  assert.equal(body.authoritative_snapshot_ready, true);
  assert.deepEqual(body.expected_hubs, [internals.publicHubIdentifier(room.hub_sid)]);
  assert.equal(JSON.stringify(body).includes(room.hub_sid), false);
});

test("room prompt validation uses the same Unicode codepoint and UTF-8 byte bounds", () => {
  const acceptedPrompt = "😀".repeat(1000);
  const parsed = internals.parseRoomSnapshot({
    hubs: [
      {
        hub_sid: "unicode-room",
        bots: {
          enabled: true,
          count: 1,
          mobility: "static",
          chat_enabled: true,
          prompt: acceptedPrompt
        }
      }
    ]
  });
  assert.equal(parsed.get("unicode-room").bots.prompt, acceptedPrompt);

  assert.throws(
    () => internals.parseRoomSnapshot({
      hubs: [
        {
          hub_sid: "unicode-room",
          bots: {
            enabled: true,
            count: 1,
            mobility: "static",
            chat_enabled: true,
            prompt: "😀".repeat(1501)
          }
        }
      ]
    }),
    /invalid_room_snapshot_config/
  );
  const truncated = internals.normalizeConfig({ prompt: "😀".repeat(2000) }).prompt;
  assert.equal(Array.from(truncated).length, 1500);
  assert.equal(Buffer.byteLength(truncated, "utf8"), 6000);
  assert.equal(internals.normalizeConfig({ prompt: "x".repeat(1_000_000) }).prompt, "");
});

test("readiness invalidates immediately after a known full-sync failure and only a new full snapshot restores it", async () => {
  internals.resetRuntimeStateForTests();
  const pending = await fetch(`${baseUrl}/ready`);
  assert.equal(pending.status, 503);
  assert.equal((await pending.json()).snapshot_reason, "authoritative_snapshot_pending");

  let calls = 0;
  await internals.syncActiveRoomsFromReticulum({
    fetchImpl: async () => {
      calls += 1;
      return calls === 1
        ? { ok: false, status: 503, json: async () => ({}) }
        : { ok: true, status: 200, json: async () => ({ hubs: [] }) };
    }
  });
  assert.equal((await fetch(`${baseUrl}/ready`)).status, 503);

  await internals.syncActiveRoomsFromReticulum({
    fetchImpl: async () => ({ ok: true, status: 200, json: async () => ({ hubs: [] }) })
  });
  const emptyRestored = await fetch(`${baseUrl}/ready`);
  assert.equal(emptyRestored.status, 503);
  assert.equal((await emptyRestored.json()).authoritative_snapshot_ready, true);

  calls = 0;
  await internals.syncActiveRoomsFromReticulum({
    fetchImpl: async () => {
      calls += 1;
      return calls === 1
        ? { ok: false, status: 503, json: async () => ({}) }
        : { ok: true, status: 200, json: async () => ({ hubs: [] }) };
    }
  });
  const invalidated = await fetch(`${baseUrl}/ready`);
  assert.equal(invalidated.status, 503);
  const invalidatedBody = await invalidated.json();
  assert.equal(invalidatedBody.authoritative_snapshot_ready, false);
  assert.equal(invalidatedBody.snapshot_reason, "authoritative_snapshot_sync_failed");

  await internals.syncActiveRoomsFromReticulum({
    fetchImpl: async () => ({ ok: true, status: 200, json: async () => ({ hubs: [] }) })
  });
  const restored = await fetch(`${baseUrl}/ready`);
  assert.equal(restored.status, 503);
  assert.equal((await restored.json()).authoritative_snapshot_ready, true);
});

test("room synchronization has a timeout and suppresses overlapping runs", async () => {
  internals.resetRuntimeStateForTests();
  let resolveFetch;
  let calls = 0;
  const fetchImpl = () => {
    calls += 1;
    return new Promise(resolve => {
      resolveFetch = resolve;
    });
  };
  const first = internals.syncActiveRoomsFromReticulum({ fetchImpl });
  const second = internals.syncActiveRoomsFromReticulum({ fetchImpl });
  assert.equal(first, second);
  assert.equal(calls, 1);
  resolveFetch({ ok: true, status: 200, json: async () => ({ hubs: [] }) });
  await first;

  await assert.rejects(
    internals.fetchRoomSnapshot(
      "http://ret.invalid/snapshot",
      {},
      (_url, { signal }) =>
        new Promise((_resolve, reject) => {
          signal.addEventListener("abort", () => reject(Object.assign(new Error("aborted"), { name: "AbortError" })));
        }),
      10
    ),
    error => error && error.name === "AbortError"
  );
});
