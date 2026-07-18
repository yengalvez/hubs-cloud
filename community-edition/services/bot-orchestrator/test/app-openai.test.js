const assert = require("node:assert/strict");
const { after, before, beforeEach, test } = require("node:test");

process.env.BOT_ORCHESTRATOR_ACCESS_KEY = "test-orchestrator-access-key-at-least-32";
process.env.OPENAI_API_KEY = "test-openai-key";
process.env.OPENAI_ENDPOINT = "https://provider.test/v1/responses";
process.env.OPENAI_MODERATION_ENDPOINT = "https://provider.test/v1/moderations";
process.env.RUNNER_AUTOSTART = "false";
process.env.CHAT_RATE_LIMIT_MS = "1";
process.env.CHAT_RATE_LIMIT_MAX_REQUESTS = "20";
process.env.OPENAI_TOTAL_BUDGET_MS = "200";

const nativeFetch = global.fetch;
const { startServer, internals } = require("../app");

const moderationResults = [];
const modelPayloads = [];
const providerCalls = [];
const providerDelays = [];
let server;
let baseUrl;

function jsonResponse(payload) {
  return new Response(JSON.stringify(payload), {
    status: 200,
    headers: { "content-type": "application/json" }
  });
}

async function waitForProvider(options) {
  const delayMs = providerDelays.shift() || 0;
  if (!delayMs) return;
  await new Promise((resolve, reject) => {
    const timer = setTimeout(resolve, delayMs);
    const abort = () => {
      clearTimeout(timer);
      const error = new Error("provider request aborted");
      error.name = "AbortError";
      reject(error);
    };
    if (options.signal?.aborted) abort();
    else options.signal?.addEventListener("abort", abort, { once: true });
  });
}

global.fetch = async (input, options = {}) => {
  const url = typeof input === "string" ? input : input.url;

  if (url === process.env.OPENAI_MODERATION_ENDPOINT) {
    assert.ok(moderationResults.length > 0, "unexpected moderation request");
    providerCalls.push({ kind: "moderation", body: JSON.parse(options.body) });
    await waitForProvider(options);
    const result = moderationResults.shift();
    return jsonResponse(result && typeof result === "object" ? result : { results: [{ flagged: result }] });
  }

  if (url === process.env.OPENAI_ENDPOINT) {
    assert.ok(modelPayloads.length > 0, "unexpected model request");
    providerCalls.push({ kind: "response", body: JSON.parse(options.body) });
    await waitForProvider(options);
    return jsonResponse(modelPayloads.shift());
  }

  throw new Error(`unexpected fetch target: ${url}`);
};

before(async () => {
  server = startServer(0);
  if (!server.listening) {
    await new Promise(resolve => server.once("listening", resolve));
  }
  baseUrl = `http://127.0.0.1:${server.address().port}`;
});

beforeEach(() => {
  internals.resetRuntimeStateForTests();
  moderationResults.length = 0;
  modelPayloads.length = 0;
  providerCalls.length = 0;
  providerDelays.length = 0;
});

after(async () => {
  global.fetch = nativeFetch;
  await new Promise((resolve, reject) => server.close(error => (error ? reject(error) : resolve())));
});

async function post(path, body) {
  return nativeFetch(`${baseUrl}${path}`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-ret-bot-orchestrator-access-key": process.env.BOT_ORCHESTRATOR_ACCESS_KEY
    },
    body: JSON.stringify(body)
  });
}

async function configureRoom(hubSid, mobility = "medium") {
  const bots = { enabled: true, count: 1, mobility, chat_enabled: true };
  const response = await post("/internal/bots/room-config", {
    hub_sid: hubSid,
    bots
  });
  assert.equal(response.status, 200);
  assert.equal(internals.seedReadyRoomForTests(hubSid, bots), true);
}

function completedModelReply(reply, action) {
  return {
    status: "completed",
    output_text: JSON.stringify({ reply, ...(action === undefined ? {} : { action }) })
  };
}

test("never calls the provider while authoritative bot readiness is pending", async () => {
  const configured = await post("/internal/bots/room-config", {
    hub_sid: "room-provider-pending",
    bots: { enabled: true, count: 1, mobility: "medium", chat_enabled: true }
  });
  assert.equal(configured.status, 200);

  const response = await post("/internal/bots/chat", {
    hub_sid: "room-provider-pending",
    bot_id: "bot-1",
    requester_id: "account-provider-pending",
    message: "hola"
  });

  assert.equal(response.status, 503);
  assert.deepEqual(await response.json(), { error: "bot service unavailable" });
  assert.deepEqual(providerCalls, []);
});

test("returns a generic 2xx response without echo when input moderation blocks", async () => {
  const secret = "private-input-secret-8472";
  await configureRoom("room-input-moderated");
  moderationResults.push(true);

  const response = await post("/internal/bots/chat", {
    hub_sid: "room-input-moderated",
    bot_id: "bot-1",
    requester_id: "account-input-moderated",
    message: `contenido bloqueado ${secret}`,
    context: { waypoints: ["spawbot-lobby"] }
  });
  const body = await response.json();

  assert.equal(response.status, 200);
  assert.equal(body.moderated, true);
  assert.equal(body.action, null);
  assert.equal(JSON.stringify(body).includes(secret), false);
  assert.match(body.reply, /otra pregunta/i);
  assert.deepEqual(
    providerCalls.map(call => call.kind),
    ["moderation"]
  );
});

test("fails closed when a successful moderation response omits the boolean decision", async () => {
  await configureRoom("room-malformed-moderation");
  moderationResults.push({});

  const response = await post("/internal/bots/chat", {
    hub_sid: "room-malformed-moderation",
    bot_id: "bot-1",
    requester_id: "account-malformed-moderation",
    message: "Ve a spawbot-lobby",
    context: { waypoints: ["spawbot-lobby"] }
  });
  const body = await response.json();

  assert.equal(response.status, 200);
  assert.match(body.reply, /no está disponible temporalmente/i);
  assert.equal(body.action, null);
  assert.deepEqual(
    providerCalls.map(call => call.kind),
    ["moderation"]
  );
});

test("returns a generic 2xx response without echo when output moderation blocks", async () => {
  const secret = "provider-output-secret-3921";
  await configureRoom("room-output-moderated");
  moderationResults.push(false, true);
  modelPayloads.push(
    completedModelReply(secret, { type: "go_to_waypoint", waypoint: "spawbot-lobby" })
  );

  const response = await post("/internal/bots/chat", {
    hub_sid: "room-output-moderated",
    bot_id: "bot-1",
    requester_id: "account-output-moderated",
    message: "hola",
    context: { waypoints: ["spawbot-lobby"] }
  });
  const body = await response.json();

  assert.equal(response.status, 200);
  assert.equal(body.moderated, true);
  assert.equal(body.action, null);
  assert.equal(JSON.stringify(body).includes(secret), false);
  assert.match(body.reply, /otra pregunta/i);
  assert.deepEqual(
    providerCalls.map(call => call.kind),
    ["moderation", "response", "moderation"]
  );
});

test("ignores an executable action injected by the model without user movement intent", async () => {
  await configureRoom("room-model-action");
  moderationResults.push(false, false);
  modelPayloads.push(
    completedModelReply("Hola.", { type: "go_to_waypoint", waypoint: "spawbot-lobby" })
  );

  const response = await post("/internal/bots/chat", {
    hub_sid: "room-model-action",
    bot_id: "bot-1",
    requester_id: "account-model-action",
    message: "Solo salúdame brevemente.",
    context: { waypoints: ["spawbot-lobby"] }
  });
  const body = await response.json();

  assert.equal(response.status, 200);
  assert.equal(body.reply, "Hola.");
  assert.equal(body.action, null);
});

test("uses the deterministic user target instead of a conflicting model action", async () => {
  await configureRoom("room-user-action");
  moderationResults.push(false, false);
  modelPayloads.push(
    completedModelReply("Voy.", { type: "go_to_waypoint", waypoint: "spawbot-secret" })
  );

  const response = await post("/internal/bots/chat", {
    hub_sid: "room-user-action",
    bot_id: "bot-1",
    requester_id: "account-user-action",
    message: "Ve a spawbot-lobby",
    context: { waypoints: ["spawbot-lobby", "spawbot-secret"] }
  });
  const body = await response.json();

  assert.equal(response.status, 200);
  assert.deepEqual(body.action, { type: "go_to_waypoint", waypoint: "spawbot-lobby" });
});

test("shares one provider deadline across moderation, model and output moderation", async () => {
  await configureRoom("room-provider-deadline");
  moderationResults.push(false, false);
  modelPayloads.push(completedModelReply("Respuesta tardía."));
  providerDelays.push(90, 90, 90);

  const startedAt = Date.now();
  const response = await post("/internal/bots/chat", {
    hub_sid: "room-provider-deadline",
    bot_id: "bot-1",
    requester_id: "account-provider-deadline",
    message: "hola",
    context: { waypoints: ["spawbot-lobby"] }
  });
  const elapsedMs = Date.now() - startedAt;
  const body = await response.json();

  assert.equal(response.status, 200);
  assert.match(body.reply, /no está disponible temporalmente/i);
  assert.equal(body.action, null);
  assert.ok(elapsedMs < 500, `shared provider deadline took ${elapsedMs}ms`);
  assert.deepEqual(
    providerCalls.map(call => call.kind),
    ["moderation", "response", "moderation"]
  );
});
