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

test("protects internal endpoints and does not expose Express", async () => {
  const unauthorized = await post("/internal/bots/room-config", { hub_sid: "room", bots: {} }, "wrong");
  assert.equal(unauthorized.status, 401);

  const health = await fetch(`${baseUrl}/health`);
  assert.equal(health.status, 200);
  assert.equal(health.headers.get("x-powered-by"), null);
  assert.equal((await health.json()).runner_backend_default, "ghost");
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
  assert.equal(userPayload.hub_sid, undefined);
  assert.equal(userPayload.bot_id, undefined);
  assert.equal(request.input[1].content[0].text.includes("account-123"), false);
  assert.equal(request.text.format.type, "json_schema");
  assert.equal(request.text.format.strict, true);
  assert.deepEqual(request.text.format.schema.required, ["reply", "action"]);
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

test("only accepts navigation actions for a known spawbot waypoint", () => {
  const unknown = internals.parseStructuredReply(
    JSON.stringify({ reply: "Voy.", action: { type: "go_to_waypoint", waypoint: "spawbot-secret" } }),
    ["spawbot-lobby"]
  );
  assert.equal(unknown.action, null);

  const known = internals.parseStructuredReply(
    JSON.stringify({ reply: "Voy.", action: { type: "go_to_waypoint", waypoint: "spawbot-lobby" } }),
    ["spawbot-lobby"]
  );
  assert.deepEqual(known.action, { type: "go_to_waypoint", waypoint: "spawbot-lobby" });
});

test("does not infer movement from substrings or a waypoint mention without an instruction", () => {
  const context = { waypoints: ["spawbot-recepcion"] };

  assert.equal(internals.detectWaypointAction("Hola. Salúdame brevemente.", context), null);
  assert.equal(internals.detectWaypointAction("¿Qué es spawbot-recepcion?", context), null);
  assert.deepEqual(internals.detectWaypointAction("Ve a spawbot-recepcion", context), {
    type: "go_to_waypoint",
    waypoint: "spawbot-recepcion"
  });
  assert.deepEqual(internals.detectWaypointAction("Dirígete a recepción", context), {
    type: "go_to_waypoint",
    waypoint: "spawbot-recepcion"
  });
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
