const express = require("express");
const { spawn } = require("child_process");

const app = express();
app.use(express.json({ limit: "1mb" }));

const PORT = Number(process.env.PORT || 5001);
const BOT_ACCESS_KEY = process.env.BOT_ACCESS_KEY || "";
const RUNNER_AUTOSTART = process.env.RUNNER_AUTOSTART === "true";
const RUNNER_SCRIPT = process.env.RUNNER_SCRIPT || "";
const HUBS_BASE_URL = process.env.HUBS_BASE_URL || "https://meta-hubs.org/hub.html";
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const OPENAI_MODEL = process.env.OPENAI_MODEL || "gpt-5-nano";
const OPENAI_ENDPOINT = process.env.OPENAI_ENDPOINT || "https://api.openai.com/v1/responses";
const OPENAI_TIMEOUT_MS = Number(process.env.OPENAI_TIMEOUT_MS || 9_000);
const MAX_ACTIVE_ROOMS = parsePositiveInt(process.env.MAX_ACTIVE_ROOMS, 1);
const MAX_BOTS_PER_ROOM = parsePositiveInt(process.env.MAX_BOTS_PER_ROOM, 5);
const CHAT_RATE_LIMIT_MS = parsePositiveInt(process.env.CHAT_RATE_LIMIT_MS, 700);

const roomConfigs = new Map();
const roomRunners = new Map();
const queuedRunnerHubs = [];
const lastChatAt = new Map();

function parsePositiveInt(raw, fallback) {
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.floor(parsed);
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function trimReply(reply) {
  if (typeof reply !== "string") return "";
  return reply.trim().slice(0, 500);
}

function normalizeWaypointName(value) {
  if (typeof value !== "string") return "";
  return value.trim().toLowerCase();
}

function sanitizeKnownWaypoints(context) {
  const source = context && Array.isArray(context.waypoints) ? context.waypoints : [];
  const unique = new Set();

  for (let i = 0; i < source.length; i++) {
    const waypoint = normalizeWaypointName(source[i]);
    if (!waypoint) continue;
    if (!waypoint.startsWith("spawbot-")) continue;
    unique.add(waypoint);
  }

  return Array.from(unique);
}

function sanitizeAction(action, knownWaypoints) {
  if (!action || typeof action !== "object") return null;
  if (action.type !== "go_to_waypoint") return null;

  const waypoint = normalizeWaypointName(action.waypoint);
  if (!waypoint || !waypoint.startsWith("spawbot-")) return null;

  if (knownWaypoints.length > 0 && !knownWaypoints.includes(waypoint)) {
    return null;
  }

  return {
    type: "go_to_waypoint",
    waypoint
  };
}

function extractFirstJsonObject(text) {
  if (typeof text !== "string") return null;
  const source = text.trim();

  let start = -1;
  let depth = 0;
  let inString = false;
  let escaped = false;

  for (let i = 0; i < source.length; i++) {
    const char = source[i];

    if (escaped) {
      escaped = false;
      continue;
    }

    if (char === "\\") {
      escaped = true;
      continue;
    }

    if (char === "\"") {
      inString = !inString;
      continue;
    }

    if (inString) continue;

    if (char === "{") {
      if (depth === 0) start = i;
      depth += 1;
    } else if (char === "}") {
      depth -= 1;
      if (depth === 0 && start >= 0) {
        return source.slice(start, i + 1);
      }
      if (depth < 0) return null;
    }
  }

  return null;
}

function extractOutputText(responsePayload) {
  if (typeof responsePayload?.output_text === "string" && responsePayload.output_text.trim()) {
    return responsePayload.output_text.trim();
  }

  const output = Array.isArray(responsePayload?.output) ? responsePayload.output : [];

  for (let i = 0; i < output.length; i++) {
    const item = output[i];
    const content = Array.isArray(item?.content) ? item.content : [];

    for (let j = 0; j < content.length; j++) {
      const block = content[j];
      if (typeof block?.text === "string" && block.text.trim()) {
        return block.text.trim();
      }
    }
  }

  return "";
}

function parseStructuredReply(responseText, knownWaypoints) {
  const jsonPayload = extractFirstJsonObject(responseText);
  if (!jsonPayload) return null;

  try {
    const parsed = JSON.parse(jsonPayload);
    const reply = trimReply(parsed.reply);
    if (!reply) return null;

    return {
      reply,
      action: sanitizeAction(parsed.action, knownWaypoints)
    };
  } catch (_err) {
    return null;
  }
}

function normalizeMobility(value) {
  if (value === "low" || value === "medium" || value === "high") return value;
  return "medium";
}

function normalizeConfig(input) {
  const source = input || {};
  const count = Number(source.count || 0);

  return {
    enabled: !!source.enabled,
    count: Number.isFinite(count) ? clamp(Math.floor(count), 0, MAX_BOTS_PER_ROOM) : 0,
    mobility: normalizeMobility(source.mobility),
    chat_enabled: !!source.chat_enabled
  };
}

function detectWaypointAction(message, context) {
  if (!message || typeof message !== "string") return null;

  const text = message.toLowerCase();
  const knownWaypoints = sanitizeKnownWaypoints(context);
  const spawbotMatch = text.match(/spawbot-[a-z0-9_-]+/);
  if (spawbotMatch) {
    return sanitizeAction(
      {
        type: "go_to_waypoint",
        waypoint: spawbotMatch[0]
      },
      knownWaypoints
    );
  }

  if ((text.includes("move") || text.includes("ve") || text.includes("go")) && knownWaypoints.length) {
    return {
      type: "go_to_waypoint",
      waypoint: knownWaypoints[Math.floor(Math.random() * knownWaypoints.length)]
    };
  }

  return null;
}

function mobilityReply(config) {
  switch (config.mobility) {
    case "low":
      return "I am in low mobility mode. I will mostly stay idle.";
    case "high":
      return "I am in high mobility mode. I will move frequently.";
    default:
      return "I am in medium mobility mode. I alternate between idle and walking.";
  }
}

function deterministicResponse({ message, botId, botsConfig, context }) {
  let reply = `${botId}: I received "${message}".`;
  if (message.toLowerCase().includes("mobility") || message.toLowerCase().includes("movilidad")) {
    reply = mobilityReply(botsConfig);
  }

  return {
    reply,
    action: detectWaypointAction(message, context)
  };
}

async function callOpenAI({ hubSid, botId, message, botsConfig, context }) {
  if (!OPENAI_API_KEY) return null;

  const knownWaypoints = sanitizeKnownWaypoints(context);
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), OPENAI_TIMEOUT_MS);

  try {
    const systemPrompt = [
      "You are a room bot in a social 3D metaverse.",
      "Reply in one short sentence.",
      "Return ONLY strict JSON: {\"reply\": string, \"action\": null|{\"type\":\"go_to_waypoint\",\"waypoint\":\"spawbot-*\"}}.",
      "Never include markdown.",
      "Set action to null unless the user asks to move/go to a spawbot waypoint."
    ].join(" ");

    const userPayload = {
      hub_sid: hubSid,
      bot_id: botId,
      mobility: botsConfig.mobility,
      message: message.slice(0, 600),
      known_waypoints: knownWaypoints
    };

    const requestBody = {
      model: OPENAI_MODEL,
      max_output_tokens: 320,
      reasoning: { effort: "low" },
      text: { verbosity: "low" },
      input: [
        {
          role: "system",
          content: [{ type: "input_text", text: systemPrompt }]
        },
        {
          role: "user",
          content: [{ type: "input_text", text: JSON.stringify(userPayload) }]
        }
      ]
    };

    const response = await fetch(OPENAI_ENDPOINT, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify(requestBody),
      signal: controller.signal
    });

    if (!response.ok) {
      const errorBody = await response.text();
      throw new Error(`openai_status_${response.status}:${errorBody.slice(0, 200)}`);
    }

    const responsePayload = await response.json();
    const responseText = extractOutputText(responsePayload);
    if (!responseText) {
      throw new Error("openai_empty_output");
    }

    const parsed = parseStructuredReply(responseText, knownWaypoints);
    if (!parsed) {
      throw new Error("openai_invalid_json_output");
    }

    return parsed;
  } finally {
    clearTimeout(timeout);
  }
}

function chatRateLimited(hubSid, botId) {
  const key = `${hubSid}:${botId}`;
  const now = Date.now();
  const previous = lastChatAt.get(key) || 0;

  if (now - previous < CHAT_RATE_LIMIT_MS) {
    return true;
  }

  lastChatAt.set(key, now);
  return false;
}

function runnerStateForHub(hubSid) {
  if (roomRunners.has(hubSid)) return "running";
  if (queuedRunnerHubs.includes(hubSid)) return "queued_capacity";
  return "stopped";
}

function canAutostartRunners() {
  return RUNNER_AUTOSTART && !!RUNNER_SCRIPT;
}

function canStartMoreRunners() {
  return roomRunners.size < MAX_ACTIVE_ROOMS;
}

function enqueueHub(hubSid) {
  if (!queuedRunnerHubs.includes(hubSid)) {
    queuedRunnerHubs.push(hubSid);
  }
}

function dequeueHub(hubSid) {
  const index = queuedRunnerHubs.indexOf(hubSid);
  if (index >= 0) {
    queuedRunnerHubs.splice(index, 1);
  }
}

function fillQueuedRunnerSlots() {
  if (!canAutostartRunners()) return;

  while (queuedRunnerHubs.length > 0 && canStartMoreRunners()) {
    const hubSid = queuedRunnerHubs.shift();
    const room = roomConfigs.get(hubSid);
    if (!room || !room.bots.enabled || room.bots.count <= 0) continue;

    const started = startRunner(hubSid);
    if (!started) {
      queuedRunnerHubs.unshift(hubSid);
      break;
    }
  }
}

function stopRunner(hubSid) {
  dequeueHub(hubSid);
  const info = roomRunners.get(hubSid);
  if (!info) return;

  roomRunners.delete(hubSid);
  clearTimeout(info.restartTimer);

  if (info.process && !info.process.killed) {
    info.process.kill("SIGTERM");
  }
}

function startRunner(hubSid) {
  if (!canAutostartRunners()) return false;
  if (roomRunners.has(hubSid)) return true;
  if (!canStartMoreRunners()) return false;

  const args = [RUNNER_SCRIPT, "--url", HUBS_BASE_URL, "--room", hubSid, "--runner"];
  const child = spawn("node", args, { stdio: "inherit" });

  const info = {
    process: child,
    restartDelayMs: 3000,
    restartTimer: null
  };
  roomRunners.set(hubSid, info);

  child.on("exit", () => {
    roomRunners.delete(hubSid);
    clearTimeout(info.restartTimer);

    const room = roomConfigs.get(hubSid);
    if (room && room.bots && room.bots.enabled && room.bots.count > 0 && canAutostartRunners()) {
      if (canStartMoreRunners()) {
        info.restartTimer = setTimeout(() => {
          const restarted = startRunner(hubSid);
          if (!restarted) {
            enqueueHub(hubSid);
          }
          fillQueuedRunnerSlots();
        }, info.restartDelayMs);
      } else {
        enqueueHub(hubSid);
      }
    }

    fillQueuedRunnerSlots();
  });

  return true;
}

function ensureRunnerState(hubSid) {
  const room = roomConfigs.get(hubSid);

  if (!room || !room.bots.enabled || room.bots.count <= 0) {
    stopRunner(hubSid);
    return "stopped";
  }

  if (roomRunners.has(hubSid)) {
    dequeueHub(hubSid);
    return "running";
  }

  if (!canAutostartRunners()) {
    dequeueHub(hubSid);
    return "stopped";
  }

  if (canStartMoreRunners()) {
    const started = startRunner(hubSid);
    if (started) return "running";
  }

  enqueueHub(hubSid);
  return "queued_capacity";
}

function authorized(req) {
  if (!BOT_ACCESS_KEY) return true;
  return req.get("x-ret-bot-access-key") === BOT_ACCESS_KEY;
}

function authMiddleware(req, res, next) {
  if (!authorized(req)) {
    res.status(401).json({ error: "unauthorized" });
    return;
  }

  next();
}

app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    rooms: roomConfigs.size,
    active_rooms: roomRunners.size,
    queued_rooms: queuedRunnerHubs.length,
    max_active_rooms: MAX_ACTIVE_ROOMS,
    max_bots_per_room: MAX_BOTS_PER_ROOM,
    llm_enabled: !!OPENAI_API_KEY,
    model: OPENAI_MODEL,
    active_hubs: Array.from(roomRunners.keys()),
    queued_hubs: [...queuedRunnerHubs]
  });
});

app.post("/internal/bots/room-config", authMiddleware, (req, res) => {
  const hubSid = req.body && req.body.hub_sid;
  const bots = normalizeConfig(req.body && req.body.bots);

  if (!hubSid || typeof hubSid !== "string") {
    res.status(400).json({ error: "hub_sid is required" });
    return;
  }

  roomConfigs.set(hubSid, {
    bots,
    updatedAt: Date.now()
  });

  const runnerState = ensureRunnerState(hubSid);
  fillQueuedRunnerSlots();

  res.json({ ok: true, hub_sid: hubSid, bots, runner_state: runnerState });
});

app.post("/internal/bots/room-stop", authMiddleware, (req, res) => {
  const hubSid = req.body && req.body.hub_sid;

  if (!hubSid || typeof hubSid !== "string") {
    res.status(400).json({ error: "hub_sid is required" });
    return;
  }

  roomConfigs.delete(hubSid);
  stopRunner(hubSid);
  fillQueuedRunnerSlots();

  res.json({ ok: true, hub_sid: hubSid, runner_state: "stopped" });
});

app.post("/internal/bots/chat", authMiddleware, async (req, res) => {
  const body = req.body || {};
  const hubSid = body.hub_sid;
  const botId = body.bot_id;
  const message = typeof body.message === "string" ? body.message.trim() : "";
  const context = body.context && typeof body.context === "object" ? body.context : {};

  if (!hubSid || typeof hubSid !== "string") {
    res.status(400).json({ error: "hub_sid is required" });
    return;
  }

  if (!botId || typeof botId !== "string") {
    res.status(400).json({ error: "bot_id is required" });
    return;
  }

  if (!message) {
    res.status(400).json({ error: "message is required" });
    return;
  }

  const roomConfig = roomConfigs.get(hubSid);
  const botsConfig =
    (roomConfig && roomConfig.bots) ||
    normalizeConfig({
      enabled: true,
      count: 1,
      mobility: "medium",
      chat_enabled: true
    });

  if (chatRateLimited(hubSid, botId)) {
    res.json({
      reply: "Please wait a moment before sending another message.",
      action: null,
      rate_limited: true
    });
    return;
  }

  const fallback = deterministicResponse({ message, botId, botsConfig, context });

  if (!OPENAI_API_KEY) {
    res.json(fallback);
    return;
  }

  try {
    const response = await callOpenAI({ hubSid, botId, message, botsConfig, context });
    if (!response || !response.reply) {
      res.json(fallback);
      return;
    }

    if (!response.action) {
      response.action = detectWaypointAction(message, context);
    }

    res.json(response);
  } catch (error) {
    console.warn("OpenAI bot chat failed. Falling back to deterministic response.", error.message);
    res.json(fallback);
  }
});

app.listen(PORT, () => {
  console.log(`bot-orchestrator listening on :${PORT}`);
});
