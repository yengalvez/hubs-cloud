#!/usr/bin/env node
const doc = `
Usage:
    ./run-ghost-runner.js [options]
Options:
    -h --help            Show this screen
    -u --url=<url>       URL [default: https://meta-hubs.org]
    -r --room=<room>     Room id
    --runner             Enable room bot-runner mode for this process
`;

const docopt = require("docopt").docopt;
const { mat4, vec3, quat } = require("gl-matrix");

// Node 20+ has fetch globally, but keep this as a sanity check.
if (typeof fetch !== "function") {
  // eslint-disable-next-line no-console
  console.error("This runner requires Node.js with global fetch (Node 18+).");
  process.exit(1);
}

function log(...objs) {
  // eslint-disable-next-line no-console
  console.log([new Date().toISOString()].concat(objs).join(" "));
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function normalizeAngleDeg(deg) {
  const n = Number(deg) || 0;
  return ((n % 360) + 360) % 360;
}

function safeJsonParse(text) {
  try {
    return JSON.parse(text);
  } catch (_err) {
    return null;
  }
}

function parseVec3Like(value, fallback = [0, 0, 0]) {
  if (Array.isArray(value) && value.length >= 3) {
    return [Number(value[0]) || 0, Number(value[1]) || 0, Number(value[2]) || 0];
  }
  if (value && typeof value === "object") {
    return [Number(value.x) || 0, Number(value.y) || 0, Number(value.z) || 0];
  }
  return fallback;
}

function parseQuatLike(value, fallback = [0, 0, 0, 1]) {
  if (Array.isArray(value) && value.length >= 4) {
    return [Number(value[0]) || 0, Number(value[1]) || 0, Number(value[2]) || 0, Number(value[3]) || 1];
  }
  if (value && typeof value === "object") {
    return [Number(value.x) || 0, Number(value.y) || 0, Number(value.z) || 0, Number(value.w) || 1];
  }
  return fallback;
}

function parseSocketUrl(baseUrl) {
  const url = new URL(baseUrl);
  const wsProto = url.protocol === "https:" ? "wss:" : "ws:";
  return `${wsProto}//${url.host}/socket`;
}

async function loadPhoenix() {
  try {
    // eslint-disable-next-line global-require
    return require("phoenix");
  } catch (err) {
    // Support ESM-only builds.
    // eslint-disable-next-line no-console
    console.warn("phoenix require failed, falling back to dynamic import:", err.message);
    const mod = await import("phoenix");
    return mod.default || mod;
  }
}

async function fetchJson(url, opts = {}) {
  const res = await fetch(url, opts);
  if (!res.ok) throw new Error(`http_${res.status}`);
  return res.json();
}

async function fetchDateHeaderMs(url) {
  const res = await fetch(url, { method: "HEAD", cache: "no-cache" });
  const date = res.headers.get("date");
  if (!date) throw new Error("missing_date_header");
  const ms = new Date(date).getTime();
  if (!Number.isFinite(ms)) throw new Error("invalid_date_header");
  return ms;
}

async function computeTimeOffsetMs(baseUrl) {
  const precision = 1000;
  const clientSent = Date.now();
  const serverReceived = (await fetchDateHeaderMs(baseUrl)) + precision / 2;
  const clientReceived = Date.now();
  const serverTime = serverReceived + (clientReceived - clientSent) / 2;
  return serverTime - clientReceived;
}

function createTimekeeper(baseUrl) {
  let averageOffset = 0;
  let initialized = false;
  let lastNow = 0;

  const update = async () => {
    const offset = await computeTimeOffsetMs(baseUrl);
    if (!initialized) {
      averageOffset = offset;
      initialized = true;
    } else {
      // Gentle smoothing.
      averageOffset += (offset - averageOffset) * 0.2;
    }
  };

  // Prime it quickly, then refresh periodically.
  const prime = async () => {
    for (let i = 0; i < 3; i++) {
      try {
        // eslint-disable-next-line no-await-in-loop
        await update();
      } catch (err) {
        log("time offset update failed:", err.message);
      }
    }
    setInterval(() => {
      update().catch(err => log("time offset update failed:", err.message));
    }, 5 * 60 * 1000);
  };

  const nowMs = () => {
    let now = Date.now() + (initialized ? averageOffset : 0);
    if (!Number.isFinite(now)) now = Date.now();
    if (now < lastNow) now = lastNow;
    lastNow = now;
    return now;
  };

  return { prime, nowMs };
}

function isAbsoluteUrl(value) {
  try {
    // eslint-disable-next-line no-new
    new URL(value);
    return true;
  } catch {
    return false;
  }
}

function resolveUrl(baseUrl, maybeRelative) {
  if (!maybeRelative) return "";
  if (isAbsoluteUrl(maybeRelative)) return maybeRelative;
  return new URL(maybeRelative, baseUrl).toString();
}

function isGlb(buffer) {
  if (!buffer || buffer.byteLength < 4) return false;
  const u8 = new Uint8Array(buffer, 0, 4);
  return u8[0] === 0x67 && u8[1] === 0x6c && u8[2] === 0x54 && u8[3] === 0x46; // "glTF"
}

function parseGlbJson(buffer) {
  const view = new DataView(buffer);
  if (view.byteLength < 20) throw new Error("glb_too_small");
  const chunkLength = view.getUint32(12, true);
  const chunkType = view.getUint32(16, true);
  // JSON chunk type
  if (chunkType !== 0x4e4f534a) throw new Error("glb_missing_json_chunk");
  const jsonStart = 20;
  const jsonEnd = jsonStart + chunkLength;
  if (jsonEnd > view.byteLength) throw new Error("glb_incomplete_json_chunk");
  const jsonBytes = new Uint8Array(buffer, jsonStart, chunkLength);
  const jsonText = new TextDecoder("utf-8").decode(jsonBytes);
  const parsed = safeJsonParse(jsonText);
  if (!parsed) throw new Error("glb_invalid_json");
  return parsed;
}

async function fetchMaybeRange(url, maxInitialBytes = 256 * 1024) {
  const res = await fetch(url, {
    headers: { Range: `bytes=0-${maxInitialBytes - 1}` }
  });

  // Some servers ignore Range and return 200 with full body.
  if (res.status !== 206) {
    return {
      status: res.status,
      buffer: await res.arrayBuffer(),
      ranged: false
    };
  }

  return {
    status: res.status,
    buffer: await res.arrayBuffer(),
    ranged: true
  };
}

async function fetchGltfJson(sceneUrl) {
  // Try to minimize startup time by fetching only the GLB JSON chunk via range requests.
  const first = await fetchMaybeRange(sceneUrl);

  if (!first.buffer || first.buffer.byteLength === 0) throw new Error("scene_empty");

  if (!isGlb(first.buffer)) {
    // Not a GLB: fetch full and parse as text JSON.
    const fullRes = await fetch(sceneUrl);
    const text = await fullRes.text();
    const parsed = safeJsonParse(text);
    if (!parsed) throw new Error("gltf_invalid_json");
    return parsed;
  }

  // GLB: parse header + JSON chunk length from the partial buffer.
  const view = new DataView(first.buffer);
  if (view.byteLength < 20) {
    // Too small even for header; fetch full.
    const full = await (await fetch(sceneUrl)).arrayBuffer();
    return parseGlbJson(full);
  }

  const chunkLength = view.getUint32(12, true);
  const needed = 20 + chunkLength;

  if (needed <= view.byteLength) {
    return parseGlbJson(first.buffer);
  }

  // JSON chunk doesn't fit into our partial buffer; fetch a larger range, capped.
  const cap = 2 * 1024 * 1024;
  if (!first.ranged || needed > cap) {
    const full = await (await fetch(sceneUrl)).arrayBuffer();
    return parseGlbJson(full);
  }

  const secondRes = await fetch(sceneUrl, { headers: { Range: `bytes=0-${needed - 1}` } });
  if (secondRes.status !== 206) {
    const full = await (await fetch(sceneUrl)).arrayBuffer();
    return parseGlbJson(full);
  }

  const secondBuf = await secondRes.arrayBuffer();
  return parseGlbJson(secondBuf);
}

function nodeLocalMatrix(node) {
  const out = mat4.create();
  if (node && Array.isArray(node.matrix) && node.matrix.length === 16) {
    // glTF matrices are column-major, same as gl-matrix.
    for (let i = 0; i < 16; i++) out[i] = Number(node.matrix[i]) || 0;
    return out;
  }

  const t = parseVec3Like(node && node.translation, [0, 0, 0]);
  const r = parseQuatLike(node && node.rotation, [0, 0, 0, 1]);
  const s = parseVec3Like(node && node.scale, [1, 1, 1]);

  const qt = quat.fromValues(r[0], r[1], r[2], r[3]);
  const vt = vec3.fromValues(t[0], t[1], t[2]);
  const vs = vec3.fromValues(s[0], s[1], s[2]);
  mat4.fromRotationTranslationScale(out, qt, vt, vs);
  return out;
}

function computeWorldNodeMatrices(gltf) {
  const nodes = Array.isArray(gltf.nodes) ? gltf.nodes : [];
  const scenes = Array.isArray(gltf.scenes) ? gltf.scenes : [];
  const sceneIndex = Number.isFinite(gltf.scene) ? gltf.scene : 0;
  const rootScene = scenes[sceneIndex] || scenes[0] || null;
  const roots = (rootScene && Array.isArray(rootScene.nodes) ? rootScene.nodes : []).filter(n => Number.isFinite(n));

  const world = new Array(nodes.length);
  const visited = new Array(nodes.length).fill(false);

  const dfs = (nodeIndex, parentWorld) => {
    if (!Number.isFinite(nodeIndex) || nodeIndex < 0 || nodeIndex >= nodes.length) return;
    const node = nodes[nodeIndex];
    const local = nodeLocalMatrix(node);
    const w = mat4.create();
    mat4.multiply(w, parentWorld, local);
    world[nodeIndex] = w;
    visited[nodeIndex] = true;

    const children = Array.isArray(node && node.children) ? node.children : [];
    for (let i = 0; i < children.length; i++) {
      dfs(children[i], w);
    }
  };

  const identity = mat4.create();
  for (let i = 0; i < roots.length; i++) {
    dfs(roots[i], identity);
  }

  // Some exports may have nodes not reachable from scene roots; compute them as identity-based.
  for (let i = 0; i < nodes.length; i++) {
    if (!visited[i]) {
      dfs(i, identity);
    }
  }

  return world;
}

function getHubsComponents(node) {
  if (!node || typeof node !== "object") return null;
  const ext = node.extensions;
  if (!ext || typeof ext !== "object") return null;
  // Spoke/Hubs scenes typically export glTF nodes with MOZ_hubs_components.
  // Keep HUBS_components as a fallback for older exports.
  return (
    ext.MOZ_hubs_components ||
    ext["MOZ_hubs_components"] ||
    ext.HUBS_components ||
    ext["HUBS_components"] ||
    null
  );
}

function extractWaypointsAndColliders(gltf) {
  const nodes = Array.isArray(gltf.nodes) ? gltf.nodes : [];
  const worldMats = computeWorldNodeMatrices(gltf);

  const allWaypoints = [];
  const spawnFlagPoints = [];
  const namedSpawbots = [];
  const colliders = [];

  const tmpPos = vec3.create();

  for (let i = 0; i < nodes.length; i++) {
    const node = nodes[i];
    const world = worldMats[i];
    if (!world) continue;

    const name = (node && typeof node.name === "string" && node.name.trim()) ? node.name.trim() : `node-${i}`;
    const lowerName = name.toLowerCase();

    const comps = getHubsComponents(node) || {};
    const waypoint = comps.waypoint || null;
    const spawnPoint = comps["spawn-point"] || comps.spawn_point || null;

    const isWaypoint = !!waypoint || !!spawnPoint;
    if (isWaypoint) {
      vec3.transformMat4(tmpPos, vec3.fromValues(0, 0, 0), world);
      const point = {
        name,
        position: [tmpPos[0], tmpPos[1], tmpPos[2]],
        waypoint
      };
      allWaypoints.push(point);

      const canBeSpawnPoint = !!(waypoint && waypoint.canBeSpawnPoint) || !!spawnPoint;
      if (canBeSpawnPoint) spawnFlagPoints.push(point);
      if (lowerName.startsWith("spawbot-")) namedSpawbots.push(point);
    }

    const boxCollider = comps["box-collider"] || null;
    if (boxCollider) {
      // The exported collider is a unit box scaled by `scale` (full extents), offset by position, rotated by Euler.
      const localT = parseVec3Like(boxCollider.position, [0, 0, 0]);
      const localRDeg = parseVec3Like(boxCollider.rotation, [0, 0, 0]);
      const localS = parseVec3Like(boxCollider.scale, [1, 1, 1]);

      const local = mat4.create();
      const q = quat.create();
      quat.fromEuler(q, localRDeg[0], localRDeg[1], localRDeg[2]);
      mat4.fromRotationTranslationScale(
        local,
        q,
        vec3.fromValues(localT[0], localT[1], localT[2]),
        vec3.fromValues(localS[0], localS[1], localS[2])
      );

      const worldCollider = mat4.create();
      mat4.multiply(worldCollider, world, local);

      const inv = mat4.create();
      if (!mat4.invert(inv, worldCollider)) continue;

      colliders.push({
        name: `${name}-box-collider`,
        world: worldCollider,
        inv
      });
    }
  }

  return {
    allWaypoints,
    spawnFlagPoints,
    namedSpawbots,
    colliders
  };
}

function pickSpawnAndPatrolPoints(points) {
  const all = points.allWaypoints;
  const spawnFlag = points.spawnFlagPoints;
  const spawbots = points.namedSpawbots;

  const spawnPoints = spawbots.length ? spawbots : spawnFlag.length ? spawnFlag : all;
  const patrolPoints =
    spawbots.length >= 2 ? spawbots : all.length >= 2 ? all : spawnFlag.length >= 2 ? spawnFlag : [];

  return { spawnPoints, patrolPoints };
}

function segmentIntersectsUnitAabb(p0, p1) {
  // AABB [-0.5, 0.5] in each axis.
  const min = -0.5;
  const max = 0.5;

  const d = [p1[0] - p0[0], p1[1] - p0[1], p1[2] - p0[2]];
  let tmin = 0;
  let tmax = 1;

  for (let axis = 0; axis < 3; axis++) {
    const p = p0[axis];
    const dir = d[axis];

    if (Math.abs(dir) < 1e-8) {
      if (p < min || p > max) return null;
      continue;
    }

    const invD = 1 / dir;
    let t1 = (min - p) * invD;
    let t2 = (max - p) * invD;
    if (t1 > t2) {
      const tmp = t1;
      t1 = t2;
      t2 = tmp;
    }
    tmin = Math.max(tmin, t1);
    tmax = Math.min(tmax, t2);
    if (tmax < tmin) return null;
  }

  return { tEnter: tmin, tExit: tmax };
}

function isPathClearWithColliders(colliders, from, to, endpointEpsilonM = 0.1) {
  if (!colliders || colliders.length === 0) return true;
  const fromV = [from[0], from[1] + 0.2, from[2]];
  const toV = [to[0], to[1] + 0.2, to[2]];

  const segLen = Math.hypot(toV[0] - fromV[0], toV[1] - fromV[1], toV[2] - fromV[2]);
  if (segLen <= endpointEpsilonM * 2) return true;

  const p0 = vec3.fromValues(fromV[0], fromV[1], fromV[2]);
  const p1 = vec3.fromValues(toV[0], toV[1], toV[2]);
  const tmp0 = vec3.create();
  const tmp1 = vec3.create();

  for (let i = 0; i < colliders.length; i++) {
    const c = colliders[i];
    vec3.transformMat4(tmp0, p0, c.inv);
    vec3.transformMat4(tmp1, p1, c.inv);

    const hit = segmentIntersectsUnitAabb(tmp0, tmp1);
    if (!hit) continue;

    const tHit = clamp(Math.max(hit.tEnter, 0), 0, 1);
    const d = tHit * segLen;
    if (d > endpointEpsilonM && d < segLen - endpointEpsilonM) {
      return false;
    }
  }

  return true;
}

function normalizeBotsConfig(bots) {
  const source = bots && typeof bots === "object" ? bots : {};
  const rawCount = Number(source.count || 0);
  const mobility = source.mobility === "low" || source.mobility === "high" ? source.mobility : "medium";
  return {
    enabled: !!source.enabled,
    count: Number.isFinite(rawCount) ? clamp(Math.floor(rawCount), 0, 10) : 0,
    mobility,
    chat_enabled: !!source.chat_enabled
  };
}

const MOBILITY_BEHAVIOR = {
  low: { speedMps: 0.45, idleMinMs: 8000, idleMaxMs: 22000 },
  medium: { speedMps: 0.75, idleMinMs: 4500, idleMaxMs: 14000 },
  high: { speedMps: 1.05, idleMinMs: 2500, idleMaxMs: 8000 }
};

function randomIdleDurationMs(mobility) {
  const behavior = MOBILITY_BEHAVIOR[mobility] || MOBILITY_BEHAVIOR.medium;
  const range = behavior.idleMaxMs - behavior.idleMinMs;
  return behavior.idleMinMs + Math.floor(Math.random() * Math.max(range, 1));
}

function initialIdleDurationMs(mobility) {
  if (mobility === "low") return 2000 + Math.floor(Math.random() * 3000);
  if (mobility === "high") return 800 + Math.floor(Math.random() * 1000);
  return 1200 + Math.floor(Math.random() * 1300);
}

function separateNearbyPosition(basePos, botIndex, usedPositions) {
  const pos = [basePos[0], basePos[1], basePos[2]];
  if (botIndex === 0) return pos;

  let conflicts = 0;
  for (let i = 0; i < usedPositions.length; i++) {
    const other = usedPositions[i];
    const dx = other[0] - pos[0];
    const dz = other[2] - pos[2];
    if (dx * dx + dz * dz < 0.36) conflicts += 1;
  }
  if (!conflicts) return pos;

  const angle = botIndex * ((Math.PI * 2) / 6);
  const spreadRadius = 0.8 + Math.min(conflicts, 2) * 0.2;
  pos[0] += Math.cos(angle) * spreadRadius;
  pos[2] += Math.sin(angle) * spreadRadius;
  return pos;
}

function botIndexFromId(botId) {
  return Math.max(Number(String(botId).replace("bot-", "")) - 1, 0);
}

async function fetchFeaturedAvatarRefs(baseUrl) {
  const url = new URL("/api/v1/media/search", baseUrl);
  url.searchParams.set("source", "avatar_listings");
  url.searchParams.set("filter", "featured");

  const res = await fetchJson(url.toString());
  const entries = Array.isArray(res && res.entries) ? res.entries : [];

  const allRefs = [];
  const fullbodyRefs = [];

  for (let i = 0; i < entries.length; i++) {
    const entry = entries[i];
    const ref = entry && entry.gltfs && entry.gltfs.avatar;
    if (!ref) continue;
    allRefs.push(ref);
    const tags = ((entry && entry.tags && entry.tags.tags) || []).map(t => String(t).toLowerCase());
    const isFullbody = tags.includes("fullbody") || tags.includes("rpm");
    if (isFullbody) fullbodyRefs.push(ref);
  }

  const uniq = arr => Array.from(new Set(arr));
  return { allRefs: uniq(allRefs), fullbodyRefs: uniq(fullbodyRefs) };
}

function pickAvatarId(botId, avatarRefs, fullbodyRefs, rotationOffset) {
  const refs = fullbodyRefs.length ? fullbodyRefs : avatarRefs;
  if (!refs.length) return "";
  const index = (botIndexFromId(botId) + rotationOffset) % refs.length;
  return refs[index];
}

function buildNetworkId(hubSid, botId) {
  // Keep it stable across restarts, and avoid characters that could be problematic in selectors/ids.
  return `room-bot-${hubSid}-${botId}`;
}

function buildBotPathFreeze(pos, yawDeg, nowMs) {
  return {
    sx: pos[0],
    sy: pos[1],
    sz: pos[2],
    ex: pos[0],
    ey: pos[1],
    ez: pos[2],
    t0: nowMs,
    dur: 0,
    yaw0: yawDeg,
    yaw1: yawDeg
  };
}

function buildBotPathSegment(path) {
  return {
    sx: path.startPos[0],
    sy: path.startPos[1],
    sz: path.startPos[2],
    ex: path.endPos[0],
    ey: path.endPos[1],
    ez: path.endPos[2],
    t0: path.t0,
    dur: path.dur,
    yaw0: path.yaw0,
    yaw1: path.yaw1
  };
}

function sendNaf(channel, payload) {
  channel.push("naf", payload);
}

function sendNafr(channel, payload) {
  channel.push("nafr", { naf: JSON.stringify(payload) });
}

function createEntityPayload({ networkId, owner, creator, lastOwnerTime, components, isFirstSync }) {
  return {
    dataType: "u",
    data: {
      networkId,
      owner,
      creator,
      lastOwnerTime,
      template: "#remote-bot-avatar",
      persistent: false,
      parent: null,
      components,
      isFirstSync: !!isFirstSync
    }
  };
}

function updateEntityPayload({ networkId, owner, creator, lastOwnerTime, components }) {
  return {
    dataType: "u",
    data: {
      networkId,
      owner,
      creator,
      lastOwnerTime,
      template: "#remote-bot-avatar",
      persistent: false,
      parent: null,
      components
    }
  };
}

function removeEntityPayload(networkId) {
  return { dataType: "r", data: { networkId } };
}

(async () => {
  const options = docopt(doc);

  const baseUrl = options["--url"] || "https://meta-hubs.org";
  const hubSid = options["--room"];
  if (!hubSid) {
    log("Missing --room");
    process.exit(1);
  }

  const botAccessKey = process.env.BOT_ACCESS_KEY || "";
  const raycastMode = (process.env.GHOST_RAYCAST_MODE || "spoke_colliders").trim().toLowerCase();
  const pathStartDelayMs = Number(process.env.PATH_START_DELAY_MS || 450);
  const minWalkDurationMs = Number(process.env.MIN_WALK_DURATION_MS || 600);

  const wsPkg = require("ws");
  global.WebSocket = wsPkg; // required for phoenix in Node

  const phoenix = await loadPhoenix();
  const Socket = phoenix.Socket;
  const Presence = phoenix.Presence;

  if (!Socket || !Presence) {
    log("Failed to load phoenix Socket/Presence.");
    process.exit(1);
  }

  const socketUrl = parseSocketUrl(baseUrl);
  log("Ghost runner socket:", socketUrl, "hub:", hubSid);

  const timekeeper = createTimekeeper(baseUrl);
  timekeeper.prime().catch(() => {});

  // The phoenix client library expects to run in a browser and may reference a `global` object.
  // Passing `transport` avoids accessing `global.WebSocket` in Node environments.
  const socket = new Socket(socketUrl, { timeout: 20000, transport: wsPkg });
  socket.connect();

  socket.onError(() => {
    log("Phoenix socket error, exiting for orchestrator restart.");
    process.exit(1);
  });
  socket.onClose(() => {
    log("Phoenix socket closed, exiting for orchestrator restart.");
    process.exit(1);
  });

  const joinParams = {
    profile: { displayName: "bot-runner", avatarId: "" },
    context: { mobile: false, embed: false, hmd: false, bot_runner: true }
  };
  if (botAccessKey) {
    joinParams.bot_access_key = botAccessKey;
  }

  const channel = socket.channel(`hub:${hubSid}`, joinParams);

  const joinData = await new Promise((resolve, reject) => {
    channel
      .join()
      .receive("ok", resolve)
      .receive("error", err => reject(new Error(err && err.reason ? err.reason : "join_failed")))
      .receive("timeout", () => reject(new Error("join_timeout")));
  });

  const hubs = Array.isArray(joinData && joinData.hubs) ? joinData.hubs : [];
  const hub = hubs[0] || null;
  const sessionId = (joinData && joinData.session_id) || "";
  if (!hub || !sessionId) {
    log("Join succeeded but did not return hub/session_id; exiting.");
    process.exit(1);
  }

  log("Joined hub:", hubSid, "session:", sessionId);

  let botsConfig = normalizeBotsConfig((hub.user_data && hub.user_data.bots) || {});

  // Keep these cached and refresh periodically.
  let waypointData = { spawnPoints: [], patrolPoints: [], allWaypoints: [], colliders: [] };
  let avatarRefs = [];
  let fullbodyRefs = [];
  let avatarRotationOffset = Math.floor(Math.random() * 1000);

  const bots = new Map();
  const reservedTargets = new Map();
  const knownOccupants = new Set();

  const reconcileBots = nowMs => {
    if (!botsConfig.enabled || botsConfig.count <= 0) {
      if (bots.size > 0) {
        bots.forEach(record => {
          sendNaf(channel, removeEntityPayload(record.networkId));
        });
        bots.clear();
        reservedTargets.clear();
      }
      return;
    }

    const desired = clamp(botsConfig.count, 0, 10);

    // Remove extra bots.
    for (let i = desired + 1; i <= 10; i++) {
      const botId = `bot-${i}`;
      const record = bots.get(botId);
      if (!record) continue;
      reservedTargets.delete(record.reservedTargetName);
      sendNaf(channel, removeEntityPayload(record.networkId));
      bots.delete(botId);
    }

    // Add missing bots.
    const usedPositions = Array.from(bots.values()).map(r => r.position);
    for (let i = 1; i <= desired; i++) {
      const botId = `bot-${i}`;
      if (bots.has(botId)) continue;

      const index = botIndexFromId(botId);
      const spawnPoints = waypointData.spawnPoints.length ? waypointData.spawnPoints : waypointData.patrolPoints;
      const spawn = spawnPoints.length ? spawnPoints[index % spawnPoints.length] : null;
      const basePos = spawn ? spawn.position : [0, 0, 0];
      const pos = separateNearbyPosition(basePos, index, usedPositions);
      usedPositions.push(pos);

      const avatarId = pickAvatarId(botId, avatarRefs, fullbodyRefs, avatarRotationOffset);
      const yaw = Math.random() * 360;
      const lastOwnerTime = timekeeper.nowMs();
      const networkId = buildNetworkId(hubSid, botId);

      const path = buildBotPathFreeze(pos, normalizeAngleDeg(yaw), nowMs);
      const info = { botId, avatarId, displayName: botId, isBot: true };

      sendNaf(
        channel,
        createEntityPayload({
          networkId,
          owner: sessionId,
          creator: sessionId,
          lastOwnerTime,
          isFirstSync: true,
          components: { 0: path, 1: info }
        })
      );

      bots.set(botId, {
        id: botId,
        networkId,
        lastOwnerTime,
        position: pos,
        homePosition: [pos[0], pos[1], pos[2]],
        yawDeg: normalizeAngleDeg(yaw),
        state: "idle",
        stateEndsAt: nowMs + initialIdleDurationMs(botsConfig.mobility),
        mobility: botsConfig.mobility,
        destination: null,
        reservedTargetName: null,
        path: null
      });
    }

    // Update mobility on existing bots.
    bots.forEach(record => {
      record.mobility = botsConfig.mobility;
    });
  };

  const updateRecordPositionFromPath = (record, nowMs) => {
    const path = record.path;
    if (!path) return;
    const dur = Math.max(0, Number(path.dur) || 0);
    const t0 = Number(path.t0) || 0;
    let alpha = 1;
    if (dur > 0) {
      alpha = nowMs <= t0 ? 0 : (nowMs - t0) / dur;
      alpha = clamp(alpha, 0, 1);
    }
    record.position[0] = path.startPos[0] + (path.endPos[0] - path.startPos[0]) * alpha;
    record.position[1] = path.startPos[1] + (path.endPos[1] - path.startPos[1]) * alpha;
    record.position[2] = path.startPos[2] + (path.endPos[2] - path.startPos[2]) * alpha;
  };

  const releaseReservation = record => {
    if (record && record.reservedTargetName && reservedTargets.get(record.reservedTargetName) === record.id) {
      reservedTargets.delete(record.reservedTargetName);
    }
    record.reservedTargetName = null;
  };

  const reserveTarget = (record, targetName) => {
    if (!record || !targetName) return;
    releaseReservation(record);
    record.reservedTargetName = targetName;
    reservedTargets.set(targetName, record.id);
  };

  const pickPatrolPoint = (record, excludeName) => {
    const points = waypointData.patrolPoints;
    if (!points.length) return null;
    const botId = record.id;

    const candidates = points.filter(p => {
      if (!p || !p.name) return false;
      if (p.name === excludeName) return false;
      const owner = reservedTargets.get(p.name);
      if (owner && owner !== botId) return false;
      const dx = p.position[0] - record.position[0];
      const dz = p.position[2] - record.position[2];
      return dx * dx + dz * dz > 0.04;
    });

    const source = candidates.length ? candidates : points.filter(p => p && p.name !== excludeName);
    if (!source.length) return null;

    // Prefer reachable points if raycast is enabled.
    const shuffled = source.slice();
    for (let i = shuffled.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      const tmp = shuffled[i];
      shuffled[i] = shuffled[j];
      shuffled[j] = tmp;
    }

    const maxAttempts = Math.min(8, shuffled.length);
    for (let i = 0; i < maxAttempts; i++) {
      const point = shuffled[i];
      if (!point) continue;
      if (raycastMode === "spoke_colliders") {
        if (!isPathClearWithColliders(waypointData.colliders, record.position, point.position)) continue;
      }
      return point;
    }

    return source[Math.floor(Math.random() * source.length)];
  };

  const startWalking = (record, desiredWaypointName, nowMs) => {
    updateRecordPositionFromPath(record, nowMs);

    let target = null;
    if (desiredWaypointName) {
      const desired = String(desiredWaypointName).trim().toLowerCase();
      target = waypointData.allWaypoints.find(p => (p.name || "").trim().toLowerCase() === desired) || null;
      if (target && raycastMode === "spoke_colliders") {
        if (!isPathClearWithColliders(waypointData.colliders, record.position, target.position)) {
          log(`[ghost] Commanded waypoint blocked, skipping: ${desired}`);
          target = null;
        }
      }
    }

    if (!target) {
      target = pickPatrolPoint(record, record.destination && record.destination.name);
    }

    if (!target) {
      // Wander near home if we have no patrol points.
      const angle = Math.random() * Math.PI * 2;
      const radius = 0.8 + Math.random() * 1.2;
      target = {
        name: "__wander__",
        position: [record.homePosition[0] + Math.cos(angle) * radius, record.position[1], record.homePosition[2] + Math.sin(angle) * radius]
      };
    }

    if (target.name && target.name !== "__wander__") {
      reserveTarget(record, target.name);
    } else {
      releaseReservation(record);
    }

    const behavior = MOBILITY_BEHAVIOR[record.mobility] || MOBILITY_BEHAVIOR.medium;
    const startPos = [record.position[0], record.position[1], record.position[2]];
    const destination = separateNearbyPosition(target.position, botIndexFromId(record.id), []);
    const endPos = [destination[0], destination[1], destination[2]];

    const dx = endPos[0] - startPos[0];
    const dz = endPos[2] - startPos[2];
    const distance = Math.hypot(dx, dz);
    if (distance <= 0.08) {
      record.state = "idle";
      record.destination = null;
      record.path = null;
      record.stateEndsAt = nowMs + 800;
      return;
    }

    const speedMps = Math.max(0.05, Number(behavior.speedMps) || 0.75);
    const durMs = Math.max(minWalkDurationMs, (distance / speedMps) * 1000);
    const t0 = nowMs + pathStartDelayMs;

    const desiredYaw = normalizeAngleDeg((Math.atan2(dx, dz) * 180) / Math.PI);
    const yaw0 = normalizeAngleDeg(record.yawDeg);
    const yaw1 = desiredYaw;

    record.state = "walk";
    record.destination = { name: target.name, position: endPos };
    record.path = { startPos, endPos, t0, dur: durMs, yaw0, yaw1 };
    record.stateEndsAt = t0 + durMs;
    record.yawDeg = yaw1;

    sendNafr(
      channel,
      updateEntityPayload({
        networkId: record.networkId,
        owner: sessionId,
        creator: sessionId,
        lastOwnerTime: record.lastOwnerTime,
        components: { 0: buildBotPathSegment(record.path) }
      })
    );
  };

  const setIdle = (record, nowMs) => {
    updateRecordPositionFromPath(record, nowMs);

    record.state = "idle";
    record.destination = null;
    releaseReservation(record);
    record.path = null;
    record.stateEndsAt = nowMs + randomIdleDurationMs(record.mobility);

    sendNafr(
      channel,
      updateEntityPayload({
        networkId: record.networkId,
        owner: sessionId,
        creator: sessionId,
        lastOwnerTime: record.lastOwnerTime,
        components: { 0: buildBotPathFreeze(record.position, record.yawDeg, nowMs) }
      })
    );
  };

  const broadcastFullSync = nowMs => {
    bots.forEach(record => {
      const pathData = record.path ? buildBotPathSegment(record.path) : buildBotPathFreeze(record.position, record.yawDeg, nowMs);
      const info = { botId: record.id, avatarId: pickAvatarId(record.id, avatarRefs, fullbodyRefs, avatarRotationOffset), displayName: record.id, isBot: true };

      sendNaf(
        channel,
        createEntityPayload({
          networkId: record.networkId,
          owner: sessionId,
          creator: sessionId,
          lastOwnerTime: record.lastOwnerTime,
          isFirstSync: true,
          components: { 0: pathData, 1: info }
        })
      );
    });
  };

  // Handle commands from bot chat.
  channel.on("message", payload => {
    if (!payload || payload.type !== "bot_command") return;
    const body = payload.body;
    if (!body || typeof body !== "object") return;
    const botId = body.bot_id || body.botId;
    if (!botId) return;
    const record = bots.get(botId);
    if (!record) return;
    if (body.type === "go_to_waypoint" && body.waypoint) {
      startWalking(record, String(body.waypoint), timekeeper.nowMs());
    }
  });

  // Update config on hub refresh (room settings).
  channel.on("hub_refresh", payload => {
    const refreshedHub = payload && Array.isArray(payload.hubs) ? payload.hubs[0] : null;
    if (!refreshedHub || !refreshedHub.user_data) return;
    const nextConfig = normalizeBotsConfig((refreshedHub.user_data && refreshedHub.user_data.bots) || {});
    botsConfig = nextConfig;
  });

  // Presence / late joiners.
  const presence = new Presence(channel);
  presence.onSync(() => {
    const keys = presence.list(key => key) || [];
    for (let i = 0; i < keys.length; i++) {
      const k = keys[i];
      if (!k || k === sessionId) continue;
      if (knownOccupants.has(k)) continue;
      knownOccupants.add(k);
      // Broadcast a full sync for immediate visibility.
      broadcastFullSync(timekeeper.nowMs());
    }
  });

  // Fetch scene waypoints/colliders and featured avatars.
  const sceneUrl = hub && hub.scene && hub.scene.model_url ? resolveUrl(baseUrl, hub.scene.model_url) : "";
  if (!sceneUrl) {
    log("No scene model_url found in hub payload. Bots will use origin fallback.");
  } else {
    log("Scene URL:", sceneUrl);
  }

  const initScenePromise = sceneUrl
    ? fetchGltfJson(sceneUrl)
        .then(gltf => {
          const extracted = extractWaypointsAndColliders(gltf);
          const points = pickSpawnAndPatrolPoints(extracted);
          waypointData = {
            ...points,
            allWaypoints: extracted.allWaypoints,
            colliders: extracted.colliders
          };
          if (raycastMode === "spoke_colliders" && (!waypointData.colliders || waypointData.colliders.length === 0)) {
            log("No box-colliders found in scene. Raycast fallback -> allow.");
          }
          log(
            `Waypoints: all=${waypointData.allWaypoints.length} spawn=${waypointData.spawnPoints.length} patrol=${waypointData.patrolPoints.length} colliders=${waypointData.colliders.length}`
          );
        })
        .catch(err => {
          log("Failed to load/parse scene glTF. Bots will use origin fallback.", err.message);
        })
    : Promise.resolve();

  const initAvatarsPromise = fetchFeaturedAvatarRefs(baseUrl)
    .then(({ allRefs, fullbodyRefs: fb }) => {
      avatarRefs = allRefs;
      fullbodyRefs = fb;
      avatarRotationOffset = Math.floor(Math.random() * 1000);
      log(`Featured avatars: total=${avatarRefs.length} fullbody=${fullbodyRefs.length}`);
    })
    .catch(err => {
      log("Failed to fetch featured avatars:", err.message);
    });

  await Promise.all([initScenePromise, initAvatarsPromise]);

  // Main loop.
  let lastConfigRefreshAt = 0;
  let lastFeaturedRefreshAt = 0;
  const CONFIG_REFRESH_INTERVAL_MS = 3000;
  const FEATURED_REFRESH_INTERVAL_MS = 60000;

  const tick = () => {
    const now = timekeeper.nowMs();

    // Reconcile bots periodically so config changes take effect.
    if (now - lastConfigRefreshAt >= CONFIG_REFRESH_INTERVAL_MS) {
      lastConfigRefreshAt = now;
      reconcileBots(now);
    }

    if (now - lastFeaturedRefreshAt >= FEATURED_REFRESH_INTERVAL_MS) {
      lastFeaturedRefreshAt = now;
      fetchFeaturedAvatarRefs(baseUrl)
        .then(({ allRefs, fullbodyRefs: fb }) => {
          avatarRefs = allRefs;
          fullbodyRefs = fb;
        })
        .catch(() => {});
    }

    bots.forEach(record => {
      updateRecordPositionFromPath(record, now);
      if (record.state === "idle") {
        if (now >= record.stateEndsAt) startWalking(record, null, now);
      } else if (record.state === "walk") {
        if (now >= record.stateEndsAt) setIdle(record, now);
      }
    });
  };

  reconcileBots(timekeeper.nowMs());
  broadcastFullSync(timekeeper.nowMs());

  const interval = setInterval(tick, 100);

  const shutdown = signal => {
    log(`Received ${signal}, shutting down ghost runner.`);
    clearInterval(interval);
    try {
      bots.forEach(record => {
        sendNaf(channel, removeEntityPayload(record.networkId));
      });
    } catch (_err) {}
    try {
      channel.leave();
    } catch (_err) {}
    try {
      socket.disconnect();
    } catch (_err) {}
    process.exit(0);
  };

  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT", () => shutdown("SIGINT"));
})();
