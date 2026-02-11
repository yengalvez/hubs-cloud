const express = require("express");
const { spawn } = require("child_process");

const app = express();
app.use(express.json({ limit: "1mb" }));

const PORT = Number(process.env.PORT || 5001);
const BOT_ACCESS_KEY = process.env.BOT_ACCESS_KEY || "";
const RUNNER_AUTOSTART = process.env.RUNNER_AUTOSTART === "true";
const RUNNER_SCRIPT = process.env.RUNNER_SCRIPT || "";
const HUBS_BASE_URL = process.env.HUBS_BASE_URL || "https://meta-hubs.org/hub.html";

const roomConfigs = new Map();
const roomRunners = new Map();

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

function normalizeMobility(value) {
  if (value === "low" || value === "medium" || value === "high") return value;
  return "medium";
}

function normalizeConfig(input) {
  const source = input || {};
  const count = Number(source.count || 0);

  return {
    enabled: !!source.enabled,
    count: Number.isFinite(count) ? Math.max(0, Math.min(10, Math.floor(count))) : 0,
    mobility: normalizeMobility(source.mobility),
    chat_enabled: !!source.chat_enabled
  };
}

function detectWaypointAction(message, context) {
  if (!message || typeof message !== "string") return null;

  const text = message.toLowerCase();
  const match = text.match(/spawbot-[a-z0-9_-]+/);
  if (match) {
    return {
      type: "go_to_waypoint",
      waypoint: match[0]
    };
  }

  const knownWaypoints = (context && Array.isArray(context.waypoints) && context.waypoints) || [];
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

function runnerKey(hubSid) {
  return `runner:${hubSid}`;
}

function canAutostartRunners() {
  return RUNNER_AUTOSTART && !!RUNNER_SCRIPT;
}

function stopRunner(hubSid) {
  const key = runnerKey(hubSid);
  const info = roomRunners.get(key);
  if (!info) return;

  roomRunners.delete(key);
  clearTimeout(info.restartTimer);

  if (info.process && !info.process.killed) {
    info.process.kill("SIGTERM");
  }
}

function startRunner(hubSid) {
  if (!canAutostartRunners()) return;

  const key = runnerKey(hubSid);
  if (roomRunners.has(key)) return;

  const args = [RUNNER_SCRIPT, "--url", HUBS_BASE_URL, "--room", hubSid, "--runner"];
  const child = spawn("node", args, { stdio: "inherit" });

  const info = {
    process: child,
    restartDelayMs: 3000,
    restartTimer: null
  };
  roomRunners.set(key, info);

  child.on("exit", () => {
    const room = roomConfigs.get(hubSid);
    roomRunners.delete(key);

    if (room && room.bots && room.bots.enabled && room.bots.count > 0 && canAutostartRunners()) {
      info.restartTimer = setTimeout(() => startRunner(hubSid), info.restartDelayMs);
    }
  });
}

app.get("/health", (_req, res) => {
  res.json({ ok: true, rooms: roomConfigs.size });
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

  if (bots.enabled && bots.count > 0) {
    startRunner(hubSid);
  } else {
    stopRunner(hubSid);
  }

  res.json({ ok: true, hub_sid: hubSid, bots });
});

app.post("/internal/bots/room-stop", authMiddleware, (req, res) => {
  const hubSid = req.body && req.body.hub_sid;

  if (!hubSid || typeof hubSid !== "string") {
    res.status(400).json({ error: "hub_sid is required" });
    return;
  }

  roomConfigs.delete(hubSid);
  stopRunner(hubSid);
  res.json({ ok: true, hub_sid: hubSid });
});

app.post("/internal/bots/chat", authMiddleware, (req, res) => {
  const body = req.body || {};
  const hubSid = body.hub_sid;
  const botId = body.bot_id;
  const message = typeof body.message === "string" ? body.message.trim() : "";
  const context = body.context || {};

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
  const botsConfig = (roomConfig && roomConfig.bots) || {
    enabled: true,
    count: 1,
    mobility: "medium",
    chat_enabled: true
  };

  let reply = `${botId}: I received \"${message}\".`;
  if (message.toLowerCase().includes("mobility") || message.toLowerCase().includes("movilidad")) {
    reply = mobilityReply(botsConfig);
  }

  const action = detectWaypointAction(message, context);

  res.json({
    reply,
    action
  });
});

app.listen(PORT, () => {
  console.log(`bot-orchestrator listening on :${PORT}`);
});
