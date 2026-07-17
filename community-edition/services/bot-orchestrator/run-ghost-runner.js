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
const THREE = require("three");
const { Pathfinding } = require("three-pathfinding");

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

function finiteNumber(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function normalizeAngleDeg(deg) {
  const n = finiteNumber(deg);
  return ((n % 360) + 360) % 360;
}

function safeJsonParse(text) {
  try {
    return JSON.parse(text);
  } catch (_err) {
    return null;
  }
}

async function retryWithBackoff(
  operation,
  {
    maxAttempts = 3,
    baseDelayMs = 500,
    maxDelayMs = 2000,
    sleep = delayMs => new Promise(resolve => setTimeout(resolve, delayMs)),
    onRetry = () => {}
  } = {}
) {
  const boundedAttempts = clamp(Math.floor(Number(maxAttempts) || 1), 1, 5);
  const boundedBaseDelay = clamp(Math.floor(Number(baseDelayMs) || 0), 0, 5000);
  const boundedMaxDelay = clamp(Math.floor(Number(maxDelayMs) || 0), boundedBaseDelay, 10_000);

  let lastError;
  for (let attempt = 1; attempt <= boundedAttempts; attempt += 1) {
    try {
      // eslint-disable-next-line no-await-in-loop
      return await operation(attempt);
    } catch (error) {
      lastError = error;
      if (attempt >= boundedAttempts) break;
      const delayMs = Math.min(boundedBaseDelay * 2 ** (attempt - 1), boundedMaxDelay);
      onRetry(error, attempt, delayMs);
      // eslint-disable-next-line no-await-in-loop
      await sleep(delayMs);
    }
  }
  throw lastError;
}

function scheduleNavigationRecoveryRestart({
  required,
  delayMs = 30_000,
  scheduleFn = setTimeout,
  exitFn = code => process.exit(code)
}) {
  if (!required) return null;
  const boundedDelayMs = clamp(Math.floor(Number(delayMs) || 30_000), 5_000, 300_000);
  return scheduleFn(() => exitFn(1), boundedDelayMs);
}

function scheduleSpawnRecoveryRestart({
  attempts,
  delayMs = 5_000,
  scheduleFn = setTimeout,
  shouldRestart = () => true,
  exitFn = code => process.exit(code)
}) {
  if (!Number.isFinite(attempts) || attempts < 3) return null;
  const boundedDelayMs = clamp(Math.floor(Number(delayMs) || 5_000), 5_000, 60_000);
  return scheduleFn(() => {
    if (shouldRestart()) exitFn(1);
  }, boundedDelayMs);
}

function redactUrlForLog(value) {
  try {
    const url = new URL(value);
    url.username = "";
    url.password = "";
    url.search = "";
    url.hash = "";
    return url.toString();
  } catch (_error) {
    return "invalid-url";
  }
}

function errorCodeForLog(error) {
  const message = error && typeof error.message === "string" ? error.message : "";
  if (
    /^(?:http_\d{3}|missing_date_header|invalid_date_header|(?:scene|gltf|glb|navmesh|featured|bot_spawn|authenticated)_[a-z0-9_]+|required_navmesh_unavailable)$/.test(
      message
    )
  ) {
    return message;
  }
  const name = error && typeof error.name === "string" ? error.name : "";
  return /^[A-Za-z][A-Za-z0-9]{0,39}$/.test(name) ? name : "Error";
}

function parseVec3Like(value, fallback = [0, 0, 0]) {
  if (Array.isArray(value) && value.length >= 3) {
    return [
      finiteNumber(value[0], fallback[0]),
      finiteNumber(value[1], fallback[1]),
      finiteNumber(value[2], fallback[2])
    ];
  }
  if (value && typeof value === "object") {
    return [
      finiteNumber(value.x, fallback[0]),
      finiteNumber(value.y, fallback[1]),
      finiteNumber(value.z, fallback[2])
    ];
  }
  return fallback;
}

function parseQuatLike(value, fallback = [0, 0, 0, 1]) {
  if (Array.isArray(value) && value.length >= 4) {
    return [
      finiteNumber(value[0], fallback[0]),
      finiteNumber(value[1], fallback[1]),
      finiteNumber(value[2], fallback[2]),
      finiteNumber(value[3], fallback[3])
    ];
  }
  if (value && typeof value === "object") {
    return [
      finiteNumber(value.x, fallback[0]),
      finiteNumber(value.y, fallback[1]),
      finiteNumber(value.z, fallback[2]),
      finiteNumber(value.w, fallback[3])
    ];
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
    console.warn("phoenix require failed, falling back to dynamic import:", errorCodeForLog(err));
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
        log("time offset update failed:", errorCodeForLog(err));
      }
    }
    setInterval(() => {
      update().catch(err => log("time offset update failed:", errorCodeForLog(err)));
    }, 5 * 60 * 1000).unref();
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

function parsePositiveInteger(value, fallback, min, max) {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed)) return fallback;
  return clamp(parsed, min, max);
}

function isLoopbackHostname(hostname) {
  const normalized = String(hostname || "").toLowerCase().replace(/^\[|\]$/g, "");
  return normalized === "localhost" || normalized === "127.0.0.1" || normalized === "::1";
}

function createSceneFetchPolicy(baseUrl, overrides = {}) {
  const base = new URL(baseUrl);
  const allowHttp =
    overrides.allowHttp !== undefined
      ? !!overrides.allowHttp
      : process.env.GHOST_SCENE_ALLOW_HTTP === "true" ||
        (base.protocol === "http:" && isLoopbackHostname(base.hostname));
  const configuredHosts =
    overrides.allowedHosts !== undefined ? overrides.allowedHosts : process.env.GHOST_SCENE_ALLOWED_HOSTS || "";
  const extraHosts = (Array.isArray(configuredHosts) ? configuredHosts : String(configuredHosts).split(","))
    .map(value => String(value).trim().toLowerCase())
    .filter(Boolean);

  return {
    base,
    allowedHosts: new Set([base.host.toLowerCase(), ...extraHosts]),
    allowHttp,
    maxRedirects: parsePositiveInteger(overrides.maxRedirects, 3, 0, 5),
    timeoutMs: parsePositiveInteger(
      overrides.timeoutMs || process.env.GHOST_SCENE_FETCH_TIMEOUT_MS,
      10_000,
      1_000,
      60_000
    ),
    maxSceneBytes: parsePositiveInteger(
      overrides.maxSceneBytes || process.env.GHOST_SCENE_MAX_BYTES,
      64 * 1024 * 1024,
      1024,
      256 * 1024 * 1024
    ),
    maxJsonBytes: parsePositiveInteger(
      overrides.maxJsonBytes || process.env.GHOST_SCENE_MAX_JSON_BYTES,
      4 * 1024 * 1024,
      1024,
      16 * 1024 * 1024
    ),
    maxNodes: parsePositiveInteger(
      overrides.maxNodes || process.env.GHOST_SCENE_MAX_NODES,
      50_000,
      1,
      250_000
    ),
    maxEdges: parsePositiveInteger(
      overrides.maxEdges || process.env.GHOST_SCENE_MAX_EDGES,
      200_000,
      1,
      1_000_000
    ),
    maxNavmeshTriangles: parsePositiveInteger(
      overrides.maxNavmeshTriangles || process.env.GHOST_NAVMESH_MAX_TRIANGLES,
      50_000,
      1,
      200_000
    ),
    maxRoutePoints: parsePositiveInteger(
      overrides.maxRoutePoints || process.env.GHOST_NAVMESH_MAX_ROUTE_POINTS,
      64,
      2,
      256
    ),
    navmeshSnapDistanceM: clamp(
      finiteNumber(overrides.navmeshSnapDistanceM || process.env.GHOST_NAVMESH_MAX_SNAP_DISTANCE_M, 3),
      0.1,
      20
    ),
    fetchImpl: overrides.fetchImpl || fetch
  };
}

function validateSceneUrl(value, policy) {
  const url = new URL(value, policy.base);
  const protocolAllowed = url.protocol === "https:" || (url.protocol === "http:" && policy.allowHttp);

  if (!protocolAllowed) throw new Error("scene_url_protocol_not_allowed");
  if (url.username || url.password) throw new Error("scene_url_credentials_not_allowed");
  if (!policy.allowedHosts.has(url.host.toLowerCase())) throw new Error("scene_url_host_not_allowed");

  return url;
}

async function fetchWithScenePolicy(value, init, policy) {
  let current = validateSceneUrl(value, policy);

  for (let redirectCount = 0; redirectCount <= policy.maxRedirects; redirectCount++) {
    const response = await policy.fetchImpl(current.toString(), {
      ...init,
      redirect: "manual",
      signal: AbortSignal.timeout(policy.timeoutMs)
    });

    if (![301, 302, 303, 307, 308].includes(response.status)) {
      return response;
    }

    const location = response.headers.get("location");
    if (!location) throw new Error("scene_redirect_missing_location");
    if (redirectCount === policy.maxRedirects) throw new Error("scene_redirect_limit");
    if (response.body && typeof response.body.cancel === "function") await response.body.cancel();
    current = validateSceneUrl(new URL(location, current).toString(), policy);
  }

  throw new Error("scene_redirect_limit");
}

async function readResponseBodyLimited(response, maxBytes) {
  const contentLength = Number.parseInt(response.headers.get("content-length") || "", 10);
  if (Number.isFinite(contentLength) && contentLength > maxBytes) throw new Error("scene_response_too_large");

  if (!response.body || typeof response.body.getReader !== "function") {
    const buffer = await response.arrayBuffer();
    if (buffer.byteLength > maxBytes) throw new Error("scene_response_too_large");
    return buffer;
  }

  const reader = response.body.getReader();
  const chunks = [];
  let total = 0;

  while (true) {
    // eslint-disable-next-line no-await-in-loop
    const { value, done } = await reader.read();
    if (done) break;
    total += value.byteLength;
    if (total > maxBytes) {
      await reader.cancel("scene_response_too_large");
      throw new Error("scene_response_too_large");
    }
    chunks.push(value);
  }

  const merged = new Uint8Array(total);
  let offset = 0;
  chunks.forEach(chunk => {
    merged.set(chunk, offset);
    offset += chunk.byteLength;
  });
  return merged.buffer;
}

function isGlb(buffer) {
  if (!buffer || buffer.byteLength < 4) return false;
  const u8 = new Uint8Array(buffer, 0, 4);
  return u8[0] === 0x67 && u8[1] === 0x6c && u8[2] === 0x54 && u8[3] === 0x46; // "glTF"
}

function validateGltfShape(gltf, policy) {
  if (!gltf || typeof gltf !== "object" || Array.isArray(gltf)) throw new Error("gltf_invalid_root");
  const nodes = Array.isArray(gltf.nodes) ? gltf.nodes : [];
  if (nodes.length > policy.maxNodes) throw new Error("gltf_too_many_nodes");

  let edgeCount = 0;
  nodes.forEach(node => {
    if (!node || typeof node !== "object" || Array.isArray(node)) throw new Error("gltf_invalid_node");
    const transforms = [
      [node.matrix, 16],
      [node.translation, 3],
      [node.rotation, 4],
      [node.scale, 3]
    ];
    transforms.forEach(([transform, expectedLength]) => {
      if (transform === undefined) return;
      if (
        !Array.isArray(transform) ||
        transform.length !== expectedLength ||
        transform.some(value => !Number.isFinite(Number(value)))
      ) {
        throw new Error("gltf_invalid_transform");
      }
    });

    const children = Array.isArray(node && node.children) ? node.children : [];
    edgeCount += children.length;
    if (edgeCount > policy.maxEdges) throw new Error("gltf_too_many_edges");
    children.forEach(child => {
      if (!Number.isInteger(child) || child < 0 || child >= nodes.length) throw new Error("gltf_invalid_child_index");
    });
  });

  return gltf;
}

function parseGlbJson(buffer, policy) {
  const view = new DataView(buffer);
  if (view.byteLength < 20) throw new Error("glb_too_small");
  if (view.getUint32(0, true) !== 0x46546c67) throw new Error("glb_invalid_magic");
  if (view.getUint32(4, true) !== 2) throw new Error("glb_unsupported_version");
  const declaredLength = view.getUint32(8, true);
  if (declaredLength < 20) throw new Error("glb_invalid_length");
  if (declaredLength > policy.maxSceneBytes) throw new Error("scene_response_too_large");
  const chunkLength = view.getUint32(12, true);
  if (chunkLength > policy.maxJsonBytes) throw new Error("gltf_json_too_large");
  const chunkType = view.getUint32(16, true);
  // JSON chunk type
  if (chunkType !== 0x4e4f534a) throw new Error("glb_missing_json_chunk");
  const jsonStart = 20;
  const jsonEnd = jsonStart + chunkLength;
  if (jsonEnd > declaredLength) throw new Error("glb_invalid_length");
  if (jsonEnd > view.byteLength) throw new Error("glb_incomplete_json_chunk");
  const jsonBytes = new Uint8Array(buffer, jsonStart, chunkLength);
  const jsonText = new TextDecoder("utf-8").decode(jsonBytes);
  const parsed = safeJsonParse(jsonText);
  if (!parsed) throw new Error("glb_invalid_json");
  return validateGltfShape(parsed, policy);
}

async function fetchMaybeRange(url, policy, maxInitialBytes = 256 * 1024) {
  const res = await fetchWithScenePolicy(url, {
    headers: { Range: `bytes=0-${maxInitialBytes - 1}` }
  }, policy);
  if (res.status !== 200 && res.status !== 206) throw new Error(`scene_http_${res.status}`);

  // Some servers ignore Range and return 200 with full body.
  if (res.status !== 206) {
    return {
      status: res.status,
      buffer: await readResponseBodyLimited(res, policy.maxSceneBytes),
      ranged: false
    };
  }

  return {
    status: res.status,
    buffer: await readResponseBodyLimited(res, maxInitialBytes),
    ranged: true
  };
}

function parseGlbSceneDescriptor(buffer, sceneUrl, policy) {
  const gltf = parseGlbJson(buffer, policy);
  const view = new DataView(buffer);
  const declaredLength = view.getUint32(8, true);
  const jsonChunkLength = view.getUint32(12, true);
  const binHeaderStart = 20 + jsonChunkLength;

  if (declaredLength === binHeaderStart) {
    return {
      gltf,
      sceneUrl,
      isGlb: true,
      declaredLength,
      glbBinStart: null,
      glbBinLength: 0,
      fullBuffer: buffer.byteLength >= declaredLength ? buffer : null
    };
  }

  if (binHeaderStart + 8 > declaredLength) throw new Error("glb_invalid_bin_chunk");
  if (binHeaderStart + 8 > view.byteLength) throw new Error("glb_incomplete_bin_header");

  const binLength = view.getUint32(binHeaderStart, true);
  const binType = view.getUint32(binHeaderStart + 4, true);
  if (binType !== 0x004e4942) throw new Error("glb_missing_bin_chunk");

  const glbBinStart = binHeaderStart + 8;
  if (glbBinStart + binLength > declaredLength) throw new Error("glb_invalid_bin_chunk");

  return {
    gltf,
    sceneUrl,
    isGlb: true,
    declaredLength,
    glbBinStart,
    glbBinLength: binLength,
    fullBuffer: buffer.byteLength >= declaredLength ? buffer : null
  };
}

async function fetchGltfScene(sceneUrl, policy) {
  // Try to minimize startup time by fetching only the GLB JSON chunk via range requests.
  const first = await fetchMaybeRange(sceneUrl, policy);

  if (!first.buffer || first.buffer.byteLength === 0) throw new Error("scene_empty");

  if (!isGlb(first.buffer)) {
    const jsonBuffer = first.ranged
      ? await readResponseBodyLimited(await fetchWithScenePolicy(sceneUrl, {}, policy), policy.maxJsonBytes)
      : first.buffer;
    if (jsonBuffer.byteLength > policy.maxJsonBytes) throw new Error("gltf_json_too_large");
    const text = new TextDecoder("utf-8").decode(new Uint8Array(jsonBuffer));
    const parsed = safeJsonParse(text);
    if (!parsed) throw new Error("gltf_invalid_json");
    return {
      gltf: validateGltfShape(parsed, policy),
      sceneUrl,
      isGlb: false,
      declaredLength: jsonBuffer.byteLength,
      glbBinStart: null,
      glbBinLength: 0,
      fullBuffer: jsonBuffer
    };
  }

  // GLB: parse header + JSON chunk length from the partial buffer.
  const view = new DataView(first.buffer);
  if (view.byteLength < 20) throw new Error("glb_too_small");
  if (view.getUint32(4, true) !== 2) throw new Error("glb_unsupported_version");
  const declaredLength = view.getUint32(8, true);
  if (declaredLength < 20) throw new Error("glb_invalid_length");
  if (declaredLength > policy.maxSceneBytes) throw new Error("scene_response_too_large");

  const chunkLength = view.getUint32(12, true);
  if (chunkLength > policy.maxJsonBytes) throw new Error("gltf_json_too_large");
  const jsonEnd = 20 + chunkLength;
  if (jsonEnd > declaredLength) throw new Error("glb_invalid_length");
  const needed = declaredLength > jsonEnd ? jsonEnd + 8 : jsonEnd;
  if (needed > declaredLength) throw new Error("glb_invalid_length");

  if (needed <= view.byteLength) {
    return parseGlbSceneDescriptor(first.buffer, sceneUrl, policy);
  }

  if (!first.ranged) throw new Error("glb_incomplete_json_chunk");

  const secondRes = await fetchWithScenePolicy(
    sceneUrl,
    { headers: { Range: `bytes=0-${needed - 1}` } },
    policy
  );
  if (secondRes.status !== 200 && secondRes.status !== 206) throw new Error(`scene_http_${secondRes.status}`);

  const secondBuf = await readResponseBodyLimited(
    secondRes,
    secondRes.status === 206 ? needed : policy.maxSceneBytes
  );
  return parseGlbSceneDescriptor(secondBuf, sceneUrl, policy);
}

async function fetchGltfJson(sceneUrl, policy) {
  return (await fetchGltfScene(sceneUrl, policy)).gltf;
}

async function fetchSceneByteRange(scene, start, length, policy) {
  if (!scene || !scene.isGlb || scene.glbBinStart === null) throw new Error("navmesh_requires_glb_bin");
  if (!Number.isInteger(start) || !Number.isInteger(length) || start < 0 || length <= 0) {
    throw new Error("gltf_invalid_buffer_range");
  }

  const end = start + length;
  if (end > scene.declaredLength) throw new Error("gltf_buffer_range_out_of_bounds");
  if (
    start < scene.glbBinStart ||
    end > scene.glbBinStart + scene.glbBinLength
  ) {
    throw new Error("gltf_buffer_range_out_of_bounds");
  }

  if (scene.fullBuffer) {
    return scene.fullBuffer.slice(start, end);
  }

  const response = await fetchWithScenePolicy(
    scene.sceneUrl,
    { headers: { Range: `bytes=${start}-${end - 1}` } },
    policy
  );
  if (response.status !== 200 && response.status !== 206) throw new Error(`scene_http_${response.status}`);

  const buffer = await readResponseBodyLimited(
    response,
    response.status === 206 ? length : policy.maxSceneBytes
  );

  if (response.status === 206) {
    if (buffer.byteLength !== length) throw new Error("gltf_incomplete_buffer_range");
    return buffer;
  }

  if (end > buffer.byteLength) throw new Error("gltf_incomplete_buffer_range");
  return buffer.slice(start, end);
}

const ACCESSOR_COMPONENTS = {
  SCALAR: 1,
  VEC2: 2,
  VEC3: 3,
  VEC4: 4,
  MAT2: 4,
  MAT3: 9,
  MAT4: 16
};

const COMPONENT_BYTES = {
  5120: 1,
  5121: 1,
  5122: 2,
  5123: 2,
  5125: 4,
  5126: 4
};

function readComponent(view, byteOffset, componentType) {
  switch (componentType) {
    case 5120:
      return view.getInt8(byteOffset);
    case 5121:
      return view.getUint8(byteOffset);
    case 5122:
      return view.getInt16(byteOffset, true);
    case 5123:
      return view.getUint16(byteOffset, true);
    case 5125:
      return view.getUint32(byteOffset, true);
    case 5126:
      return view.getFloat32(byteOffset, true);
    default:
      throw new Error("gltf_unsupported_component_type");
  }
}

function accessorMetadata(scene, accessorIndex) {
  const gltf = scene.gltf;
  const accessors = Array.isArray(gltf.accessors) ? gltf.accessors : [];
  const bufferViews = Array.isArray(gltf.bufferViews) ? gltf.bufferViews : [];
  const accessor = accessors[accessorIndex];
  if (!accessor || typeof accessor !== "object") throw new Error("gltf_invalid_accessor");
  if (accessor.sparse) throw new Error("gltf_sparse_accessor_not_supported");
  if (!Number.isInteger(accessor.bufferView)) throw new Error("gltf_accessor_without_buffer_view");

  const bufferView = bufferViews[accessor.bufferView];
  if (!bufferView || typeof bufferView !== "object") throw new Error("gltf_invalid_buffer_view");
  if ((bufferView.buffer || 0) !== 0) throw new Error("gltf_external_buffer_not_supported");

  const itemSize = ACCESSOR_COMPONENTS[accessor.type];
  const componentBytes = COMPONENT_BYTES[accessor.componentType];
  const count = Number(accessor.count);
  if (!itemSize || !componentBytes || !Number.isSafeInteger(count) || count <= 0) {
    throw new Error("gltf_invalid_accessor");
  }

  const viewByteOffset = Number(bufferView.byteOffset || 0);
  const viewByteLength = Number(bufferView.byteLength);
  const accessorByteOffset = Number(accessor.byteOffset || 0);
  const elementBytes = itemSize * componentBytes;
  const byteStride = Number(bufferView.byteStride || elementBytes);
  if (
    !Number.isInteger(viewByteOffset) ||
    !Number.isInteger(viewByteLength) ||
    !Number.isInteger(accessorByteOffset) ||
    !Number.isInteger(byteStride) ||
    viewByteOffset < 0 ||
    viewByteLength <= 0 ||
    accessorByteOffset < 0 ||
    byteStride < elementBytes ||
    byteStride > 252
  ) {
    throw new Error("gltf_invalid_buffer_view");
  }

  const rangeByteLength = (count - 1) * byteStride + elementBytes;
  const requiredBytes = accessorByteOffset + rangeByteLength;
  if (
    !Number.isSafeInteger(rangeByteLength) ||
    !Number.isSafeInteger(requiredBytes) ||
    requiredBytes > viewByteLength ||
    !Number.isSafeInteger(count * itemSize)
  ) {
    throw new Error("gltf_accessor_out_of_bounds");
  }

  return {
    accessor,
    count,
    itemSize,
    componentBytes,
    componentType: accessor.componentType,
    byteStride,
    rangeByteOffset: viewByteOffset + accessorByteOffset,
    rangeByteLength
  };
}

async function readAccessor(scene, accessorIndex, policy, cache = new Map(), limits = {}) {
  const metadata = accessorMetadata(scene, accessorIndex);
  if (Number.isSafeInteger(limits.maxCount) && metadata.count > limits.maxCount) {
    throw new Error("navmesh_accessor_count_exceeded");
  }

  const cacheKey = `${metadata.rangeByteOffset}:${metadata.rangeByteLength}`;
  let buffer = cache.get(cacheKey);
  if (!buffer) {
    if (
      limits.byteBudget &&
      limits.byteBudget.used + metadata.rangeByteLength > limits.byteBudget.maximum
    ) {
      throw new Error("navmesh_accessor_byte_budget_exceeded");
    }
    const absoluteStart = scene.glbBinStart + metadata.rangeByteOffset;
    buffer = await fetchSceneByteRange(scene, absoluteStart, metadata.rangeByteLength, policy);
    cache.set(cacheKey, buffer);
    if (limits.byteBudget) limits.byteBudget.used += metadata.rangeByteLength;
  }

  const dataView = new DataView(buffer);
  const values = new Float64Array(metadata.count * metadata.itemSize);
  for (let itemIndex = 0; itemIndex < metadata.count; itemIndex++) {
    const elementOffset = itemIndex * metadata.byteStride;
    for (let componentIndex = 0; componentIndex < metadata.itemSize; componentIndex++) {
      values[itemIndex * metadata.itemSize + componentIndex] = readComponent(
        dataView,
        elementOffset + componentIndex * metadata.componentBytes,
        metadata.componentType
      );
    }
  }

  return {
    values,
    count: metadata.count,
    itemSize: metadata.itemSize,
    componentType: metadata.componentType
  };
}

function nodeLocalMatrix(node) {
  const out = mat4.create();
  if (node && Array.isArray(node.matrix) && node.matrix.length === 16) {
    // glTF matrices are column-major, same as gl-matrix.
    for (let i = 0; i < 16; i++) out[i] = Number(node.matrix[i]);
    return out;
  }

  const t = parseVec3Like(node && node.translation, [0, 0, 0]);
  const r = parseQuatLike(node && node.rotation, [0, 0, 0, 1]);
  const s = parseVec3Like(node && node.scale, [1, 1, 1]);

  const qt = quat.fromValues(r[0], r[1], r[2], r[3]);
  if (quat.squaredLength(qt) < 1e-12) throw new Error("gltf_invalid_rotation");
  quat.normalize(qt, qt);
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
  const state = new Uint8Array(nodes.length); // 0=unseen, 1=visiting, 2=complete

  const traverse = (rootIndex, parentWorld) => {
    const stack = [{ nodeIndex: rootIndex, parentWorld, exiting: false }];

    while (stack.length) {
      const frame = stack.pop();
      const { nodeIndex } = frame;
      if (!Number.isInteger(nodeIndex) || nodeIndex < 0 || nodeIndex >= nodes.length) {
        throw new Error("gltf_invalid_child_index");
      }

      if (frame.exiting) {
        state[nodeIndex] = 2;
        continue;
      }
      if (state[nodeIndex] === 1) throw new Error("gltf_node_cycle");
      if (state[nodeIndex] === 2) continue;

      const node = nodes[nodeIndex];
      const local = nodeLocalMatrix(node);
      const w = mat4.create();
      mat4.multiply(w, frame.parentWorld, local);
      world[nodeIndex] = w;
      state[nodeIndex] = 1;
      stack.push({ nodeIndex, parentWorld: frame.parentWorld, exiting: true });

      const children = Array.isArray(node && node.children) ? node.children : [];
      for (let i = children.length - 1; i >= 0; i--) {
        stack.push({ nodeIndex: children[i], parentWorld: w, exiting: false });
      }
    }
  };

  const identity = mat4.create();
  for (let i = 0; i < roots.length; i++) {
    traverse(roots[i], identity);
  }

  // Some exports may have nodes not reachable from scene roots; compute them as identity-based.
  for (let i = 0; i < nodes.length; i++) {
    if (state[i] === 0) {
      traverse(i, identity);
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

async function extractNavMeshGeometry(scene, policy) {
  if (!scene || !scene.isGlb || scene.glbBinStart === null) return null;

  const gltf = scene.gltf;
  const nodes = Array.isArray(gltf.nodes) ? gltf.nodes : [];
  const meshes = Array.isArray(gltf.meshes) ? gltf.meshes : [];
  const worldMatrices = computeWorldNodeMatrices(gltf);
  const bufferCache = new Map();
  const positions = [];
  const indices = [];
  const accessorIndexes = new Set();
  const maximumAccessorCount = policy.maxNavmeshTriangles * 3;
  const byteBudget = {
    used: 0,
    maximum: Math.min(policy.maxSceneBytes, Math.max(64 * 1024, maximumAccessorCount * 64))
  };
  let navmeshPrimitiveCount = 0;
  let vertexCount = 0;
  let triangleCount = 0;

  for (let nodeIndex = 0; nodeIndex < nodes.length; nodeIndex++) {
    const node = nodes[nodeIndex];
    const components = getHubsComponents(node);
    const hasNavMesh =
      !!components &&
      (Object.prototype.hasOwnProperty.call(components, "nav-mesh") ||
        Object.prototype.hasOwnProperty.call(components, "nav_mesh"));
    if (!hasNavMesh || !Number.isInteger(node.mesh)) continue;

    const mesh = meshes[node.mesh];
    const primitives = mesh && Array.isArray(mesh.primitives) ? mesh.primitives : [];
    const world = worldMatrices[nodeIndex];
    if (!world) continue;

    for (let primitiveIndex = 0; primitiveIndex < primitives.length; primitiveIndex++) {
      const primitive = primitives[primitiveIndex];
      if (!primitive || (primitive.mode !== undefined && primitive.mode !== 4)) continue;

      const positionAccessorIndex = primitive.attributes && primitive.attributes.POSITION;
      if (!Number.isInteger(positionAccessorIndex)) continue;

      navmeshPrimitiveCount += 1;
      if (navmeshPrimitiveCount > Math.min(policy.maxNavmeshTriangles, 4096)) {
        throw new Error("navmesh_too_many_primitives");
      }

      const positionMetadata = accessorMetadata(scene, positionAccessorIndex);
      if (positionMetadata.itemSize !== 3 || positionMetadata.componentType !== 5126) {
        throw new Error("navmesh_position_accessor_invalid");
      }
      vertexCount += positionMetadata.count;
      if (vertexCount > maximumAccessorCount) throw new Error("navmesh_too_many_vertices");

      let primitiveTriangleCount;
      let indexMetadata = null;
      if (Number.isInteger(primitive.indices)) {
        indexMetadata = accessorMetadata(scene, primitive.indices);
        if (
          indexMetadata.itemSize !== 1 ||
          ![5121, 5123, 5125].includes(indexMetadata.componentType) ||
          indexMetadata.count % 3 !== 0
        ) {
          throw new Error("navmesh_index_accessor_invalid");
        }
        primitiveTriangleCount = indexMetadata.count / 3;
        accessorIndexes.add(primitive.indices);
      } else {
        if (positionMetadata.count % 3 !== 0) {
          throw new Error("navmesh_nonindexed_triangle_count_invalid");
        }
        primitiveTriangleCount = positionMetadata.count / 3;
      }
      triangleCount += primitiveTriangleCount;
      if (triangleCount > policy.maxNavmeshTriangles) throw new Error("navmesh_too_many_triangles");
      accessorIndexes.add(positionAccessorIndex);
      if (accessorIndexes.size > 4096) throw new Error("navmesh_too_many_accessors");

      // eslint-disable-next-line no-await-in-loop
      const positionAccessor = await readAccessor(scene, positionAccessorIndex, policy, bufferCache, {
        maxCount: maximumAccessorCount,
        byteBudget
      });

      let primitiveIndices;
      if (indexMetadata) {
        // eslint-disable-next-line no-await-in-loop
        const indexAccessor = await readAccessor(scene, primitive.indices, policy, bufferCache, {
          maxCount: maximumAccessorCount,
          byteBudget
        });
        primitiveIndices = indexAccessor.values;
      } else {
        primitiveIndices = Array.from({ length: positionAccessor.count }, (_value, index) => index);
      }

      const baseVertex = positions.length / 3;
      const tmpPosition = vec3.create();
      for (let vertexIndex = 0; vertexIndex < positionAccessor.count; vertexIndex++) {
        const offset = vertexIndex * 3;
        vec3.set(
          tmpPosition,
          positionAccessor.values[offset],
          positionAccessor.values[offset + 1],
          positionAccessor.values[offset + 2]
        );
        vec3.transformMat4(tmpPosition, tmpPosition, world);
        positions.push(tmpPosition[0], tmpPosition[1], tmpPosition[2]);
      }

      for (let index = 0; index < primitiveIndices.length; index++) {
        const localIndex = Number(primitiveIndices[index]);
        if (!Number.isInteger(localIndex) || localIndex < 0 || localIndex >= positionAccessor.count) {
          throw new Error("navmesh_index_out_of_bounds");
        }
        indices.push(baseVertex + localIndex);
      }
    }
  }

  if (!triangleCount || !positions.length || !indices.length) return null;

  const geometry = new THREE.BufferGeometry();
  geometry.setAttribute("position", new THREE.Float32BufferAttribute(positions, 3));
  geometry.setIndex(indices);
  return { geometry, triangleCount };
}

function createNavMeshPlanner(navMesh, policy) {
  if (!navMesh || !navMesh.geometry || !navMesh.triangleCount) return null;

  const zoneId = "ghost-navmesh";
  const pathfinder = new Pathfinding();
  const zoneData = Pathfinding.createZone(navMesh.geometry);
  if (!zoneData || !Array.isArray(zoneData.groups) || !zoneData.groups.length) return null;
  pathfinder.setZoneData(zoneId, zoneData);

  const triangles = [];
  for (let group = 0; group < zoneData.groups.length; group++) {
    const polygons = zoneData.groups[group] || [];
    for (let polygonIndex = 0; polygonIndex < polygons.length; polygonIndex++) {
      const polygon = polygons[polygonIndex];
      const a = zoneData.vertices[polygon.vertexIds[0]];
      const b = zoneData.vertices[polygon.vertexIds[1]];
      const c = zoneData.vertices[polygon.vertexIds[2]];
      if (!a || !b || !c) continue;
      triangles.push({ group, triangle: new THREE.Triangle(a, b, c) });
    }
  }

  const projectPoint = position => {
    if (!Array.isArray(position) || position.length < 3 || !triangles.length) return null;
    const source = new THREE.Vector3(
      finiteNumber(position[0]),
      finiteNumber(position[1]),
      finiteNumber(position[2])
    );
    const candidate = new THREE.Vector3();
    const closest = new THREE.Vector3();
    let closestGroup = null;
    let closestDistanceSq = Number.POSITIVE_INFINITY;

    for (let index = 0; index < triangles.length; index++) {
      const entry = triangles[index];
      entry.triangle.closestPointToPoint(source, candidate);
      const distanceSq = candidate.distanceToSquared(source);
      if (distanceSq >= closestDistanceSq) continue;
      closestDistanceSq = distanceSq;
      closestGroup = entry.group;
      closest.copy(candidate);
    }

    if (closestGroup === null || Math.sqrt(closestDistanceSq) > policy.navmeshSnapDistanceM) return null;
    return {
      group: closestGroup,
      position: [closest.x, closest.y, closest.z],
      vector: closest.clone(),
      distance: Math.sqrt(closestDistanceSq)
    };
  };

  const findRoute = (from, to) => {
    const start = projectPoint(from);
    const target = projectPoint(to);
    if (!start || !target || start.group !== target.group) return null;

    let path;
    try {
      path = pathfinder.findPath(start.vector, target.vector, zoneId, start.group);
    } catch (_error) {
      return null;
    }
    if (!Array.isArray(path)) return null;

    const route = [start.position];
    for (let index = 0; index < path.length; index++) {
      const point = path[index];
      if (!point) continue;
      const next = [point.x, point.y, point.z];
      const previous = route[route.length - 1];
      if (Math.hypot(next[0] - previous[0], next[1] - previous[1], next[2] - previous[2]) <= 0.01) {
        continue;
      }
      route.push(next);
      if (route.length > policy.maxRoutePoints) return null;
    }

    const last = route[route.length - 1];
    if (
      Math.hypot(
        target.position[0] - last[0],
        target.position[1] - last[1],
        target.position[2] - last[2]
      ) > 0.01
    ) {
      route.push(target.position);
    }

    return route.length >= 2 ? route : null;
  };

  return {
    triangleCount: navMesh.triangleCount,
    groupCount: zoneData.groups.length,
    projectPoint,
    findRoute
  };
}

function projectWaypointsToNavmesh(points, planner) {
  if (!planner) return points;

  const projectedByPoint = new Map();
  const project = point => {
    if (projectedByPoint.has(point)) return projectedByPoint.get(point);
    const projected = planner.projectPoint(point.position);
    const next = projected
      ? {
          ...point,
          position: projected.position,
          navGroup: projected.group
        }
      : null;
    projectedByPoint.set(point, next);
    return next;
  };
  const projectList = list => list.map(project).filter(Boolean);

  return {
    ...points,
    allWaypoints: projectList(points.allWaypoints),
    spawnFlagPoints: projectList(points.spawnFlagPoints),
    namedSpawbots: projectList(points.namedSpawbots)
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
  const mobility = ["static", "low", "medium", "high"].includes(source.mobility) ? source.mobility : "medium";
  return {
    enabled: !!source.enabled,
    count: Number.isFinite(rawCount) ? clamp(Math.floor(rawCount), 0, 10) : 0,
    mobility,
    chat_enabled: !!source.chat_enabled
  };
}

function managedRunnerConfigFingerprint(bots) {
  const normalized = normalizeBotsConfig(bots);
  return JSON.stringify({
    enabled: normalized.enabled,
    count: normalized.count,
    mobility: normalized.mobility
  });
}

function validManagedConfigRevision(value) {
  return Number.isSafeInteger(value) && value > 0;
}

function validRunnerProcessGeneration(value) {
  return typeof value === "string" &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
}

function finalizeManagedConfigApplication({
  fingerprint,
  revision,
  processGeneration,
  isCurrent = () => true,
  reconcile,
  applyFingerprint,
  acknowledge,
  publishStatus
}) {
  const changed = reconcile();
  if (
    !fingerprint ||
    !validManagedConfigRevision(revision) ||
    !validRunnerProcessGeneration(processGeneration) ||
    !isCurrent(fingerprint, revision, processGeneration)
  ) {
    return changed;
  }

  applyFingerprint(fingerprint, revision, processGeneration);
  acknowledge(fingerprint, revision, processGeneration);
  publishStatus(true);
  return changed;
}

function createSpawnCleanupController({
  removeNetworkId = () => {},
  requestControlledRestart = () => {},
  onUncertain = () => {}
} = {}) {
  const removalAttempts = new Set();
  let uncertain = false;
  let restartRequested = false;

  const enterUncertainState = pending => {
    const networkId = pending && pending.record && pending.record.networkId;
    if (typeof networkId === "string" && networkId && !removalAttempts.has(networkId)) {
      removalAttempts.add(networkId);
      try {
        removeNetworkId(networkId);
      } catch (_error) {}
    }

    if (!uncertain) {
      uncertain = true;
      onUncertain();
    }
    if (!restartRequested) {
      restartRequested = true;
      try {
        requestControlledRestart();
      } catch (_error) {}
    }
    return true;
  };

  return {
    cancelPending: enterUncertainState,
    observeAmbiguousSettlement: enterUncertainState,
    canSpawn: () => !uncertain,
    isUncertain: () => uncertain,
    reason: () => (uncertain ? "spawn_cleanup_uncertain" : null),
    removalAttemptCount: () => removalAttempts.size,
    restartRequested: () => restartRequested
  };
}

function cancelPendingSpawnsForTransition({ pendingSpawns, cleanup, botIds = null }) {
  const ids = Array.isArray(botIds) ? botIds : Array.from(pendingSpawns.keys());
  let cancelled = false;
  ids.forEach(botId => {
    const pending = pendingSpawns.get(botId);
    if (!pending) return;
    pendingSpawns.delete(botId);
    cleanup.cancelPending(pending);
    cancelled = true;
  });
  return cancelled;
}

function parseManagedConfigMessage(message) {
  if (
    !message ||
    message.type !== "bots-config" ||
    typeof message.fingerprint !== "string" ||
    !validManagedConfigRevision(message.revision) ||
    !validRunnerProcessGeneration(message.processGeneration)
  ) {
    return null;
  }
  const bots = normalizeBotsConfig(message.bots);
  const fingerprint = managedRunnerConfigFingerprint(bots);
  if (!fingerprint || message.fingerprint !== fingerprint) return null;
  return {
    bots,
    fingerprint,
    revision: message.revision,
    processGeneration: message.processGeneration
  };
}

function shouldApplyHubRefreshConfig(managedConfigReceived) {
  return managedConfigReceived !== true;
}

function emptyWaypointData() {
  return {
    spawnPoints: [],
    patrolPoints: [],
    allWaypoints: [],
    colliders: [],
    navPlanner: null
  };
}

function resolveHubSceneState(hub, baseUrl, policy, { requireSceneField = false } = {}) {
  if (!hub || typeof hub !== "object") return { observed: false, url: "", rejected: false };
  if (requireSceneField && !Object.prototype.hasOwnProperty.call(hub, "scene")) {
    return { observed: false, url: "", rejected: false };
  }
  const rawSceneUrl = hub.scene && hub.scene.model_url
    ? resolveUrl(baseUrl, hub.scene.model_url)
    : "";
  if (!rawSceneUrl) return { observed: true, url: "", rejected: false };
  try {
    return { observed: true, url: validateSceneUrl(rawSceneUrl, policy).toString(), rejected: false };
  } catch (_error) {
    return { observed: true, url: "", rejected: true };
  }
}

function applyHubRefreshSceneChange({
  payload,
  currentSceneUrl,
  currentSceneRejected = false,
  baseUrl,
  policy,
  invalidateNavigation,
  reconcile,
  publishStatus,
  requestRestart
}) {
  const refreshedHub = payload && Array.isArray(payload.hubs) ? payload.hubs[0] : null;
  const markedStale = !!(
    payload &&
    Array.isArray(payload.stale_fields) &&
    payload.stale_fields.includes("scene")
  );
  let next = resolveHubSceneState(refreshedHub, baseUrl, policy, { requireSceneField: true });
  if (markedStale && !next.observed) {
    next = { observed: true, url: currentSceneUrl, rejected: currentSceneRejected };
  }
  if (
    !next.observed ||
    (!markedStale && next.url === currentSceneUrl && next.rejected === currentSceneRejected)
  ) {
    return { changed: false, ...next };
  }

  // Invalidate before reconciliation so no spawn, route or status can reuse
  // geometry from the superseded scene, even while the clean restart is pending.
  invalidateNavigation(next);
  reconcile();
  publishStatus();
  requestRestart(next);
  return { changed: true, ...next };
}

function navigationReady({ navigationMode, requireNavmesh, waypointData }) {
  if (navigationMode !== "navmesh_preferred" || !requireNavmesh) return true;
  if (!waypointData || !waypointData.navPlanner) return false;

  return !!(
    (Array.isArray(waypointData.spawnPoints) && waypointData.spawnPoints.length) ||
    (Array.isArray(waypointData.patrolPoints) && waypointData.patrolPoints.length)
  );
}

function deriveBotRuntimeStatus({
  enabled,
  desired,
  active,
  navigationIsReady,
  authenticated,
  pending = 0,
  spawnRejected = false,
  cleanupUncertain = false
}) {
  const boundedDesired = enabled ? clamp(Number(desired) || 0, 0, 10) : 0;
  const boundedActive = clamp(Number(active) || 0, 0, 10);
  const boundedPending = clamp(Number(pending) || 0, 0, 10);
  let reason = "ready";
  if (cleanupUncertain) reason = "spawn_cleanup_uncertain";
  else if (!enabled || boundedDesired === 0) reason = "disabled";
  else if (authenticated !== true) reason = "unauthenticated";
  else if (!navigationIsReady) reason = "navmesh_unavailable";
  else if (boundedActive === boundedDesired) reason = "ready";
  else if (boundedPending > 0) reason = "spawn_pending";
  else if (spawnRejected) reason = "spawn_rejected";
  else reason = "insufficient_clearance";

  return {
    desired: boundedDesired,
    active: boundedActive,
    pending: boundedPending,
    navigationReady: !!navigationIsReady,
    authenticated: authenticated === true,
    authoritativeSpawnAcks: true,
    ready: reason === "ready",
    reason
  };
}

const MOBILITY_BEHAVIOR = {
  static: { speedMps: 0, idleMinMs: Number.POSITIVE_INFINITY, idleMaxMs: Number.POSITIVE_INFINITY },
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
  if (mobility === "static") return Number.POSITIVE_INFINITY;
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

function positionHasClearance(position, usedPositions, minimumDistanceM = 0.65) {
  const minimumDistanceSq = minimumDistanceM * minimumDistanceM;
  return usedPositions.every(other => {
    const dx = other[0] - position[0];
    const dz = other[2] - position[2];
    return dx * dx + dz * dz >= minimumDistanceSq;
  });
}

function findSeparatedNavmeshPosition(basePos, botIndex, usedPositions, navPlanner) {
  if (!navPlanner || typeof navPlanner.projectPoint !== "function") return null;

  const candidates = [[basePos[0], basePos[1], basePos[2]]];
  for (let ring = 1; ring <= 3; ring++) {
    const radius = 0.7 * ring;
    for (let step = 0; step < 8; step++) {
      const angle = (step + botIndex * 0.5) * (Math.PI / 4);
      candidates.push([
        basePos[0] + Math.cos(angle) * radius,
        basePos[1],
        basePos[2] + Math.sin(angle) * radius
      ]);
    }
  }

  for (const candidate of candidates) {
    const projected = navPlanner.projectPoint(candidate);
    if (projected && positionHasClearance(projected.position, usedPositions)) {
      return [projected.position[0], projected.position[1], projected.position[2]];
    }
  }

  return null;
}

function pointSegmentDistanceSq2D(point, start, end) {
  const dx = end[0] - start[0];
  const dz = end[2] - start[2];
  const lengthSq = dx * dx + dz * dz;
  if (lengthSq === 0) {
    const px = point[0] - start[0];
    const pz = point[2] - start[2];
    return px * px + pz * pz;
  }
  const t = clamp(((point[0] - start[0]) * dx + (point[2] - start[2]) * dz) / lengthSq, 0, 1);
  const px = point[0] - (start[0] + dx * t);
  const pz = point[2] - (start[2] + dz * t);
  return px * px + pz * pz;
}

function orientation2D(a, b, c) {
  return (b[0] - a[0]) * (c[2] - a[2]) - (b[2] - a[2]) * (c[0] - a[0]);
}

function pointOnSegment2D(point, start, end) {
  const epsilon = 1e-9;
  return (
    Math.abs(orientation2D(start, end, point)) <= epsilon &&
    point[0] >= Math.min(start[0], end[0]) - epsilon &&
    point[0] <= Math.max(start[0], end[0]) + epsilon &&
    point[2] >= Math.min(start[2], end[2]) - epsilon &&
    point[2] <= Math.max(start[2], end[2]) + epsilon
  );
}

function segmentsDistanceSq2D(a0, a1, b0, b1) {
  const ab0 = orientation2D(a0, a1, b0);
  const ab1 = orientation2D(a0, a1, b1);
  const ba0 = orientation2D(b0, b1, a0);
  const ba1 = orientation2D(b0, b1, a1);
  const properIntersection =
    ((ab0 > 0 && ab1 < 0) || (ab0 < 0 && ab1 > 0)) &&
    ((ba0 > 0 && ba1 < 0) || (ba0 < 0 && ba1 > 0));
  if (
    properIntersection ||
    pointOnSegment2D(b0, a0, a1) ||
    pointOnSegment2D(b1, a0, a1) ||
    pointOnSegment2D(a0, b0, b1) ||
    pointOnSegment2D(a1, b0, b1)
  ) {
    return 0;
  }

  return Math.min(
    pointSegmentDistanceSq2D(a0, b0, b1),
    pointSegmentDistanceSq2D(a1, b0, b1),
    pointSegmentDistanceSq2D(b0, a0, a1),
    pointSegmentDistanceSq2D(b1, a0, a1)
  );
}

function routeMaintainsSeparation(route, recordId, records, minimumDistanceM = 0.55) {
  if (!Array.isArray(route) || route.length < 2) return false;
  const minimumDistanceSq = minimumDistanceM * minimumDistanceM;

  for (const other of records) {
    if (!other || other.id === recordId || !Array.isArray(other.position)) continue;
    const otherRoute = [[...other.position]];
    if (other.path && Array.isArray(other.path.endPos)) otherRoute.push([...other.path.endPos]);
    if (Array.isArray(other.routePoints)) otherRoute.push(...other.routePoints.map(point => [...point]));
    if (otherRoute.length === 1) otherRoute.push([...otherRoute[0]]);

    for (let i = 1; i < route.length; i++) {
      for (let j = 1; j < otherRoute.length; j++) {
        if (segmentsDistanceSq2D(route[i - 1], route[i], otherRoute[j - 1], otherRoute[j]) < minimumDistanceSq) {
          return false;
        }
      }
    }
  }

  return true;
}

function findCommandedWaypointPlan(desiredWaypointName, allWaypoints, planRoute) {
  if (!desiredWaypointName || !Array.isArray(allWaypoints) || typeof planRoute !== "function") return null;
  const desired = String(desiredWaypointName).trim().toLowerCase();
  const target = allWaypoints.find(point => (point.name || "").trim().toLowerCase() === desired) || null;
  if (!target) return null;
  const route = planRoute(target);
  return route ? { target, route } : null;
}

function botIndexFromId(botId) {
  return Math.max(Number(String(botId).replace("bot-", "")) - 1, 0);
}

async function readFeaturedResponseTextLimited(response, maxBytes) {
  const declaredLength = Number(response.headers.get("content-length"));
  if (Number.isFinite(declaredLength) && declaredLength > maxBytes) {
    throw new Error("featured_response_too_large");
  }

  if (!response.body || typeof response.body.getReader !== "function") {
    const body = new Uint8Array(await response.arrayBuffer());
    if (body.byteLength > maxBytes) throw new Error("featured_response_too_large");
    return new TextDecoder().decode(body);
  }

  const reader = response.body.getReader();
  const chunks = [];
  let totalBytes = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      totalBytes += value.byteLength;
      if (totalBytes > maxBytes) {
        await reader.cancel("featured_response_too_large");
        throw new Error("featured_response_too_large");
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }

  const body = new Uint8Array(totalBytes);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return new TextDecoder().decode(body);
}

async function fetchFeaturedAvatarRefs(
  baseUrl,
  {
    fetchImpl = fetch,
    timeoutMs = parsePositiveInteger(process.env.GHOST_FEATURED_FETCH_TIMEOUT_MS, 4000, 250, 30_000),
    maxBytes = parsePositiveInteger(process.env.GHOST_FEATURED_MAX_BYTES, 524_288, 1024, 4_194_304),
    maxRedirects = parsePositiveInteger(process.env.GHOST_FEATURED_MAX_REDIRECTS, 2, 1, 5),
    maxEntries = parsePositiveInteger(process.env.GHOST_FEATURED_MAX_ENTRIES, 256, 1, 1024),
    maxRefs = parsePositiveInteger(process.env.GHOST_FEATURED_MAX_REFS, 128, 1, 512)
  } = {}
) {
  const base = new URL(baseUrl);
  if (!/^https?:$/.test(base.protocol) || base.username || base.password) {
    throw new Error("featured_base_url_invalid");
  }

  let url = new URL("/api/v1/media/search", base);
  url.searchParams.set("source", "avatar_listings");
  url.searchParams.set("filter", "featured");

  const signal = AbortSignal.timeout(timeoutMs);
  let response;
  let redirectCount = 0;
  while (true) {
    response = await fetchImpl(url.toString(), { signal, redirect: "manual" });
    if (![301, 302, 303, 307, 308].includes(response.status)) break;

    if (redirectCount >= maxRedirects) throw new Error("featured_too_many_redirects");
    const location = response.headers.get("location");
    if (!location) throw new Error("featured_redirect_missing_location");
    const nextUrl = new URL(location, url);
    if (nextUrl.origin !== base.origin || nextUrl.username || nextUrl.password) {
      throw new Error("featured_cross_origin_redirect");
    }
    url = nextUrl;
    redirectCount += 1;
  }

  if (!response.ok) throw new Error(`http_${response.status}`);
  const contentType = response.headers.get("content-type") || "";
  if (!/(?:^application\/json\b|^[^;\s]+\/[^;\s]+\+json\b)/i.test(contentType)) {
    throw new Error("featured_invalid_content_type");
  }

  const raw = await readFeaturedResponseTextLimited(response, maxBytes);
  let payload;
  try {
    payload = JSON.parse(raw);
  } catch (_error) {
    throw new Error("featured_invalid_json");
  }
  if (!payload || typeof payload !== "object" || Array.isArray(payload) || !Array.isArray(payload.entries)) {
    throw new Error("featured_invalid_schema");
  }
  if (payload.entries.length > maxEntries) throw new Error("featured_too_many_entries");
  const entries = payload.entries;

  const allRefs = [];
  const fullbodyRefs = [];
  const seenRefs = new Set();

  for (let i = 0; i < entries.length; i++) {
    const entry = entries[i];
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
      throw new Error("featured_invalid_entry");
    }
    if (!entry.gltfs || typeof entry.gltfs !== "object" || Array.isArray(entry.gltfs)) {
      throw new Error("featured_invalid_entry");
    }
    const ref = entry.gltfs.avatar;
    if (
      typeof ref !== "string" ||
      !ref.trim() ||
      ref.length > 2048 ||
      /[\u0000-\u001f\u007f]/.test(ref)
    ) {
      throw new Error("featured_invalid_ref");
    }
    const tagContainer = entry.tags;
    if (
      tagContainer !== undefined &&
      (!tagContainer ||
        typeof tagContainer !== "object" ||
        Array.isArray(tagContainer) ||
        !Array.isArray(tagContainer.tags))
    ) {
      throw new Error("featured_invalid_tags");
    }
    const rawTags = tagContainer ? tagContainer.tags : [];
    if (
      rawTags.length > 32 ||
      rawTags.some(tag => typeof tag !== "string" || !tag.trim() || tag.length > 64)
    ) {
      throw new Error("featured_invalid_tags");
    }
    const normalizedRef = ref.trim();
    if (seenRefs.has(normalizedRef)) continue;
    if (seenRefs.size >= maxRefs) throw new Error("featured_too_many_refs");
    seenRefs.add(normalizedRef);
    allRefs.push(normalizedRef);
    const tags = rawTags.map(tag => tag.toLowerCase());
    const isFullbody = tags.includes("fullbody") || tags.includes("rpm");
    if (isFullbody) fullbodyRefs.push(normalizedRef);
  }

  return { allRefs, fullbodyRefs };
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

function requestBotSpawn(channel, payload, expectedAuthorityEpoch, timeoutMs = 5000) {
  const expectedNetworkId = payload && payload.data && payload.data.networkId;
  if (!expectedNetworkId) return Promise.reject(new Error("bot_spawn_invalid_network_id"));
  if (!Number.isSafeInteger(expectedAuthorityEpoch) || expectedAuthorityEpoch <= 0) {
    return Promise.reject(new Error("bot_spawn_invalid_authority_epoch"));
  }

  return new Promise((resolve, reject) => {
    try {
      channel
        .push("naf", payload, timeoutMs)
        .receive("ok", response => {
          if (
            !response ||
            response.bot_spawn_accepted !== true ||
            response.network_id !== expectedNetworkId ||
            response.bot_runner_authority_epoch !== expectedAuthorityEpoch
          ) {
            reject(new Error("bot_spawn_invalid_ack"));
            return;
          }
          resolve(response);
        })
        .receive("error", response => {
          const error = new Error((response && response.reason) || "bot_spawn_rejected");
          // A Phoenix error reply is authoritative: Reticulum rejected the
          // first sync, so this namespace was not created and may be retried.
          error.authoritativeRejection = true;
          reject(error);
        })
        .receive("timeout", () => reject(new Error("bot_spawn_ack_timeout")));
    } catch (error) {
      reject(error);
    }
  });
}

function authoritativeBotRunnerLease(presence) {
  if (!presence || !presence.state || typeof presence.state !== "object") return null;
  const candidates = [];
  for (const [sessionId, entry] of Object.entries(presence.state)) {
    const metas = entry && Array.isArray(entry.metas) ? entry.metas : [];
    for (const meta of metas) {
      const leaseId = meta && meta.bot_runner_lease_id;
      const authorityEpoch = meta && meta.bot_runner_authority_epoch;
      if (
        meta &&
        meta.context &&
        meta.context.bot_runner === true &&
        meta.bot_runner_authoritative === true &&
        typeof leaseId === "string" &&
        leaseId &&
        Number.isSafeInteger(authorityEpoch) &&
        authorityEpoch > 0
      ) {
        candidates.push([authorityEpoch, String(sessionId), leaseId]);
      }
    }
  }
  candidates.sort((left, right) => {
    if (left[0] !== right[0]) return right[0] - left[0];
    const sessionOrder = left[1].localeCompare(right[1]);
    return sessionOrder || left[2].localeCompare(right[2]);
  });
  return candidates.length
    ? { authorityEpoch: candidates[0][0], leaseId: candidates[0][2] }
    : null;
}

function authoritativeBotRunnerLeaseId(presence) {
  const authority = authoritativeBotRunnerLease(presence);
  return authority ? authority.leaseId : "";
}

function presenceHasAuthenticatedBotRunner(presence, sessionId, leaseId) {
  if (!sessionId || typeof leaseId !== "string" || !leaseId) return false;
  const entry = presence && presence.state && presence.state[sessionId];
  const metas = entry && Array.isArray(entry.metas) ? entry.metas : [];
  const ownsLease = metas.some(
    meta =>
      meta &&
      meta.context &&
      meta.context.bot_runner === true &&
      meta.bot_runner_lease_id === leaseId
  );
  return ownsLease && authoritativeBotRunnerLeaseId(presence) === leaseId;
}

function authenticatedRunnerAuthorityEpoch(presence, sessionId, leaseId) {
  if (!presenceHasAuthenticatedBotRunner(presence, sessionId, leaseId)) return 0;
  const authority = authoritativeBotRunnerLease(presence);
  return authority && authority.leaseId === leaseId ? authority.authorityEpoch : 0;
}

function waitForAuthenticatedRunnerPresence(presence, sessionId, leaseId, timeoutMs = 5000) {
  const initialEpoch = authenticatedRunnerAuthorityEpoch(presence, sessionId, leaseId);
  if (initialEpoch) return Promise.resolve(initialEpoch);

  return new Promise((resolve, reject) => {
    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      reject(new Error("authenticated_bot_runner_presence_timeout"));
    }, timeoutMs);

    presence.onSync(() => {
      const authorityEpoch = authenticatedRunnerAuthorityEpoch(presence, sessionId, leaseId);
      if (settled || !authorityEpoch) return;
      settled = true;
      clearTimeout(timer);
      resolve(authorityEpoch);
    });
  });
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

async function main() {
  const options = docopt(doc);

  const baseUrl = options["--url"] || "https://meta-hubs.org";
  const sceneFetchPolicy = createSceneFetchPolicy(baseUrl);
  const hubSid = options["--room"];
  if (!hubSid) {
    log("Missing --room");
    process.exit(1);
  }

  const runnerProcessGeneration = process.env.RUNNER_PROCESS_GENERATION || "";
  if (!validRunnerProcessGeneration(runnerProcessGeneration)) {
    throw new Error("runner_process_generation_missing");
  }

  const botAccessKey = process.env.BOT_RUNNER_ACCESS_KEY || "";
  const raycastMode = (process.env.GHOST_RAYCAST_MODE || "spoke_colliders").trim().toLowerCase();
  const navigationMode =
    (process.env.GHOST_NAVIGATION_MODE || "navmesh_preferred").trim().toLowerCase() === "colliders"
      ? "colliders"
      : "navmesh_preferred";
  const requireNavmesh =
    (process.env.GHOST_NAVIGATION_REQUIRE_NAVMESH || "true").trim().toLowerCase() !== "false";
  const pathStartDelayMs = Number(process.env.PATH_START_DELAY_MS || 450);
  const minWalkDurationMs = Number(process.env.MIN_WALK_DURATION_MS || 600);
  const minRouteSegmentDurationMs = Number(process.env.MIN_ROUTE_SEGMENT_DURATION_MS || 150);
  const fullSyncBurstRepeats = clamp(Number(process.env.GHOST_FULL_SYNC_BURST_REPEATS || 6), 1, 20);
  const fullSyncBurstIntervalMs = Math.max(250, Number(process.env.GHOST_FULL_SYNC_BURST_INTERVAL_MS || 750));
  const fullSyncBurstInitialDelayMs = Math.max(
    0,
    Number(process.env.GHOST_FULL_SYNC_BURST_INITIAL_DELAY_MS || 250)
  );
  let botsConfig = null;
  let botsConfigDirty = false;
  let pendingManagedConfigFingerprint = "";
  let pendingManagedConfigRevision = 0;
  let appliedManagedConfigFingerprint = "";
  let appliedManagedConfigRevision = 0;
  let managedConfigReceived = false;

  const handleManagedConfig = message => {
    if (!message || message.type !== "bots-config") return;
    const managed = parseManagedConfigMessage(message);
    if (!managed) {
      log("Rejected managed bot config because its fingerprint did not match the normalized payload.");
      return;
    }
    if (managed.processGeneration !== runnerProcessGeneration) {
      log("Rejected managed bot config for a different runner process generation.");
      return;
    }
    botsConfig = managed.bots;
    botsConfigDirty = true;
    pendingManagedConfigFingerprint = managed.fingerprint;
    pendingManagedConfigRevision = managed.revision;
    managedConfigReceived = true;
  };
  process.on("message", handleManagedConfig);

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
  channel.onError(() => {
    log("Phoenix channel error, exiting for orchestrator restart.");
    process.exit(1);
  });
  channel.onClose(() => {
    log("Phoenix channel closed, exiting for orchestrator restart.");
    process.exit(1);
  });

  const joinData = await new Promise((resolve, reject) => {
    channel
      .join()
      .receive("ok", resolve)
      .receive("error", err => {
        socket.disconnect();
        reject(new Error(err && err.reason ? err.reason : "join_failed"));
      })
      .receive("timeout", () => {
        socket.disconnect();
        reject(new Error("join_timeout"));
      });
  });

  const hubs = Array.isArray(joinData && joinData.hubs) ? joinData.hubs : [];
  const hub = hubs[0] || null;
  const sessionId = (joinData && joinData.session_id) || "";
  const runnerLeaseId = (joinData && joinData.bot_runner_lease_id) || "";
  if (!hub || !sessionId || !runnerLeaseId) {
    log("Join succeeded but did not return hub/session_id/bot_runner_lease_id; exiting.");
    process.exit(1);
  }
  if (joinData.bot_runner !== true) {
    socket.disconnect();
    throw new Error("join_did_not_authenticate_bot_runner");
  }

  const presence = new Presence(channel);
  let runnerAuthorityEpoch;
  try {
    runnerAuthorityEpoch = await waitForAuthenticatedRunnerPresence(
      presence,
      sessionId,
      runnerLeaseId,
      5000
    );
  } catch (error) {
    socket.disconnect();
    throw error;
  }
  if (process.connected && typeof process.send === "function") {
    try {
      process.send({
        type: "ghost-auth-status",
        authenticated: true,
        processGeneration: runnerProcessGeneration
      });
    } catch (_error) {
      socket.disconnect();
      throw new Error("ghost_auth_status_ipc_failed");
    }
  }

  log("Joined hub as authenticated bot runner:", hubSid, "session:", sessionId);

  if (!botsConfig) {
    botsConfig = normalizeBotsConfig((hub.user_data && hub.user_data.bots) || {});
  }

  // Keep these cached and refresh periodically.
  let waypointData = emptyWaypointData();
  let sceneGeneration = 0;
  let sceneRestartScheduled = false;
  let activeSceneState = resolveHubSceneState(hub, baseUrl, sceneFetchPolicy);
  let avatarRefs = [];
  let fullbodyRefs = [];
  let avatarRotationOffset = Math.floor(Math.random() * 1000);

  const bots = new Map();
  const pendingSpawns = new Map();
  const spawnAttempts = new Map();
  const spawnRetryAt = new Map();
  const reservedTargets = new Map();
  const knownOccupants = new Set();
  const fullSyncTimers = new Set();
  const spawnRecoveryTimers = new Map();
  let lastRuntimeStatusFingerprint = "";
  let lastRuntimeStatusSentAt = 0;
  let publishRuntimeStatus = () => {};

  const spawnCleanup = createSpawnCleanupController({
    removeNetworkId: networkId => sendNaf(channel, removeEntityPayload(networkId)),
    onUncertain: () => publishRuntimeStatus(true),
    requestControlledRestart: () => {
      const timer = setTimeout(() => {
        fullSyncTimers.delete(timer);
        log("Spawn cleanup could not be confirmed; restarting ghost runner for a clean namespace.");
        process.exit(1);
      }, 250);
      fullSyncTimers.add(timer);
    }
  });

  publishRuntimeStatus = (force = false) => {
    const status = {
      ...deriveBotRuntimeStatus({
        enabled: !!botsConfig.enabled,
        desired: botsConfig.count,
        active: bots.size,
        pending: pendingSpawns.size,
        authenticated: presenceHasAuthenticatedBotRunner(presence, sessionId, runnerLeaseId),
        spawnRejected: Array.from(spawnAttempts.values()).some(attempts => attempts >= 3),
        cleanupUncertain: spawnCleanup.isUncertain(),
        navigationIsReady: navigationReady({ navigationMode, requireNavmesh, waypointData })
      }),
      configFingerprint: appliedManagedConfigFingerprint,
      configRevision: appliedManagedConfigRevision,
      processGeneration: runnerProcessGeneration
    };
    const fingerprint = JSON.stringify(status);
    const now = Date.now();
    if (!force && fingerprint === lastRuntimeStatusFingerprint && now - lastRuntimeStatusSentAt < 5000) return;
    lastRuntimeStatusFingerprint = fingerprint;
    lastRuntimeStatusSentAt = now;
    if (process.connected && typeof process.send === "function") {
      try {
        process.send({ type: "ghost-runtime-status", ...status });
      } catch (_err) {}
    }
  };

  const scheduleTimeout = (fn, delayMs) => {
    const timer = setTimeout(() => {
      fullSyncTimers.delete(timer);
      fn();
    }, delayMs);
    fullSyncTimers.add(timer);
    return timer;
  };

  const clearSpawnRecovery = botId => {
    const timer = spawnRecoveryTimers.get(botId);
    if (!timer) return;
    clearTimeout(timer);
    fullSyncTimers.delete(timer);
    spawnRecoveryTimers.delete(botId);
  };

  const clearAllSpawnRecoveries = () => {
    Array.from(spawnRecoveryTimers.keys()).forEach(clearSpawnRecovery);
  };

  const scheduleSpawnRecovery = (botId, botNumber, attempts) => {
    if (spawnRecoveryTimers.has(botId)) return;
    const timer = scheduleSpawnRecoveryRestart({
      attempts,
      delayMs: process.env.GHOST_SPAWN_RECOVERY_RESTART_MS || 5_000,
      scheduleFn: scheduleTimeout,
      shouldRestart: () => {
        spawnRecoveryTimers.delete(botId);
        return (
          botsConfig.enabled &&
          botsConfig.count >= botNumber &&
          (spawnAttempts.get(botId) || 0) >= 3
        );
      },
      exitFn: code => {
        log("Authoritative spawn retries were exhausted; restarting ghost runner for a clean retry.");
        process.exit(code);
      }
    });
    if (timer) spawnRecoveryTimers.set(botId, timer);
  };

  const cancelPendingSpawn = botId => {
    return cancelPendingSpawnsForTransition({
      pendingSpawns,
      cleanup: spawnCleanup,
      botIds: [botId]
    });
  };

  const cancelAllPendingSpawns = () =>
    cancelPendingSpawnsForTransition({ pendingSpawns, cleanup: spawnCleanup });

  const reconcileBots = nowMs => {
    let changed = false;

    if (!botsConfig.enabled || botsConfig.count <= 0) {
      clearAllSpawnRecoveries();
      changed = cancelAllPendingSpawns() || changed;
      spawnAttempts.clear();
      spawnRetryAt.clear();
      if (bots.size > 0) {
        bots.forEach(record => {
          sendNaf(channel, removeEntityPayload(record.networkId));
        });
        bots.clear();
        reservedTargets.clear();
        changed = true;
      }
      publishRuntimeStatus();
      return changed;
    }

    if (!navigationReady({ navigationMode, requireNavmesh, waypointData })) {
      clearAllSpawnRecoveries();
      changed = cancelAllPendingSpawns() || changed;
      spawnAttempts.clear();
      spawnRetryAt.clear();
      if (bots.size > 0) {
        bots.forEach(record => sendNaf(channel, removeEntityPayload(record.networkId)));
        bots.clear();
        reservedTargets.clear();
        changed = true;
      }
      publishRuntimeStatus();
      return changed;
    }

    const desired = clamp(botsConfig.count, 0, 10);

    // Remove extra bots.
    for (let i = desired + 1; i <= 10; i++) {
      const botId = `bot-${i}`;
      clearSpawnRecovery(botId);
      changed = cancelPendingSpawn(botId) || changed;
      spawnAttempts.delete(botId);
      spawnRetryAt.delete(botId);
      const record = bots.get(botId);
      if (!record) continue;
      reservedTargets.delete(record.reservedTargetName);
      sendNaf(channel, removeEntityPayload(record.networkId));
      bots.delete(botId);
      changed = true;
    }

    // Once an accepted spawn may have outlived our bookkeeping, no namespace
    // can be reused safely in this process. The controlled exit will establish
    // a fresh authenticated session before reconciliation resumes.
    if (!spawnCleanup.canSpawn()) {
      publishRuntimeStatus(true);
      return changed;
    }

    // Add missing bots.
    const usedPositions = [
      ...Array.from(bots.values()).map(record => record.position),
      ...Array.from(pendingSpawns.values()).map(pending => pending.record.position)
    ];
    for (let i = 1; i <= desired; i++) {
      const botId = `bot-${i}`;
      if (bots.has(botId) || pendingSpawns.has(botId)) continue;
      if ((spawnRetryAt.get(botId) || 0) > Date.now()) continue;
      if ((spawnAttempts.get(botId) || 0) >= 3) continue;

      const index = botIndexFromId(botId);
      const spawnPoints = waypointData.spawnPoints.length ? waypointData.spawnPoints : waypointData.patrolPoints;
      const spawn = spawnPoints.length ? spawnPoints[index % spawnPoints.length] : null;
      const basePos = spawn ? spawn.position : [0, 0, 0];
      const pos = waypointData.navPlanner
        ? findSeparatedNavmeshPosition(basePos, index, usedPositions, waypointData.navPlanner)
        : separateNearbyPosition(basePos, index, usedPositions);
      if (!pos) {
        log(`[ghost] No separated navmesh spawn is available for ${botId}; leaving it absent.`);
        continue;
      }
      usedPositions.push(pos);

      const avatarId = pickAvatarId(botId, avatarRefs, fullbodyRefs, avatarRotationOffset);
      const yaw = Math.random() * 360;
      const lastOwnerTime = timekeeper.nowMs();
      const networkId = buildNetworkId(hubSid, botId);

      const path = buildBotPathFreeze(pos, normalizeAngleDeg(yaw), nowMs);
      const info = { botId, avatarId, displayName: botId, isBot: true };

      const payload = createEntityPayload({
        networkId,
        owner: sessionId,
        creator: sessionId,
        lastOwnerTime,
        isFirstSync: true,
        components: { 0: path, 1: info }
      });
      const record = {
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
        path: null,
        routePoints: []
      };
      const pending = { payload, record };
      const attempt = (spawnAttempts.get(botId) || 0) + 1;
      spawnAttempts.set(botId, attempt);
      pendingSpawns.set(botId, pending);

      requestBotSpawn(channel, payload, runnerAuthorityEpoch)
        .then(() => {
          if (pendingSpawns.get(botId) !== pending) {
            spawnCleanup.observeAmbiguousSettlement(pending);
            return;
          }
          pendingSpawns.delete(botId);

          const stillDesired =
            botsConfig.enabled &&
            botsConfig.count >= i &&
            navigationReady({ navigationMode, requireNavmesh, waypointData });
          if (!stillDesired) {
            spawnCleanup.observeAmbiguousSettlement(pending);
            publishRuntimeStatus(true);
            return;
          }

          spawnAttempts.delete(botId);
          spawnRetryAt.delete(botId);
          clearSpawnRecovery(botId);
          record.mobility = botsConfig.mobility;
          record.stateEndsAt = timekeeper.nowMs() + initialIdleDurationMs(record.mobility);
          bots.set(botId, record);
          publishRuntimeStatus();
        })
        .catch(error => {
          const isCurrent = pendingSpawns.get(botId) === pending;
          if (isCurrent) pendingSpawns.delete(botId);
          if (isCurrent && error && error.authoritativeRejection === true && spawnCleanup.canSpawn()) {
            if (attempt < 3) {
              spawnRetryAt.set(botId, Date.now() + Math.min(1000 * 2 ** (attempt - 1), 4000));
            } else {
              scheduleSpawnRecovery(botId, i, attempt);
            }
            log(`[ghost] Spawn was rejected for ${botId} (attempt ${attempt}/3):`, errorCodeForLog(error));
            publishRuntimeStatus(true);
            return;
          }
          // A rejected, malformed or missing reply can race an accepted first
          // sync when this promise was cancelled or the reply was ambiguous.
          // Without an authoritative removal ACK, retrying this
          // deterministic network id in the same session would create an ABA
          // ambiguity. Tear down the whole runner generation instead.
          spawnCleanup.observeAmbiguousSettlement(pending);
          log(`[ghost] Spawn ACK rejected for ${botId} (attempt ${attempt}/3):`, errorCodeForLog(error));
          publishRuntimeStatus(true);
        });

      changed = true;
    }

    // Update mobility on existing bots.
    bots.forEach(record => {
      if (record.mobility === botsConfig.mobility) return;

      const previousMobility = record.mobility;
      updateRecordPositionFromPath(record, nowMs);
      record.mobility = botsConfig.mobility;
      changed = true;

      if (record.mobility === "static") {
        setIdle(record, nowMs);
      } else if (previousMobility === "static") {
        record.stateEndsAt = nowMs + initialIdleDurationMs(record.mobility);
      }
    });

    publishRuntimeStatus();
    return changed;
  };

  const reconcileManagedConfig = nowMs => {
    botsConfigDirty = false;
    const configFingerprint = pendingManagedConfigFingerprint;
    const configRevision = pendingManagedConfigRevision;
    return finalizeManagedConfigApplication({
      fingerprint: configFingerprint,
      revision: configRevision,
      processGeneration: runnerProcessGeneration,
      isCurrent: (fingerprint, revision, processGeneration) =>
        processGeneration === runnerProcessGeneration &&
        pendingManagedConfigFingerprint === fingerprint &&
        pendingManagedConfigRevision === revision,
      reconcile: () => reconcileBots(nowMs),
      applyFingerprint: (fingerprint, revision) => {
        appliedManagedConfigFingerprint = fingerprint;
        appliedManagedConfigRevision = revision;
        pendingManagedConfigFingerprint = "";
        pendingManagedConfigRevision = 0;
      },
      acknowledge: (fingerprint, revision, processGeneration) => {
        if (process.connected && typeof process.send === "function") {
          try {
            process.send({
              type: "bots-config-applied",
              fingerprint,
              revision,
              processGeneration
            });
          } catch (_error) {
            log("Failed to acknowledge applied managed bot config.");
          }
        }
      },
      // The status emitted inside reconcileBots carried the previous applied
      // fingerprint. ACK first so the parent can accept this forced status as
      // proof of the exact configuration that was just reconciled.
      publishStatus: force => publishRuntimeStatus(force)
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

  const planRouteToPoint = (record, point) => {
    if (!record || !point) return null;

    let route = null;

    if (waypointData.navPlanner) {
      route = waypointData.navPlanner.findRoute(record.position, point.position);
    } else {
      if (navigationMode === "navmesh_preferred" && requireNavmesh) return null;
      if (
        raycastMode === "spoke_colliders" &&
        !isPathClearWithColliders(waypointData.colliders, record.position, point.position)
      ) {
        return null;
      }
      route = [
        [record.position[0], record.position[1], record.position[2]],
        [point.position[0], point.position[1], point.position[2]]
      ];
    }

    return routeMaintainsSeparation(route, record.id, bots.values()) ? route : null;
  };

  const pickPatrolPlan = (record, excludeName) => {
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
      const route = planRouteToPoint(record, point);
      if (route) return { target: point, route };
    }

    return null;
  };

  const setIdle = (record, nowMs) => {
    updateRecordPositionFromPath(record, nowMs);

    record.state = "idle";
    record.destination = null;
    releaseReservation(record);
    record.path = null;
    record.routePoints = [];
    record.routeUsesNavmesh = false;
    record.stateEndsAt =
      record.mobility === "static" ? Number.POSITIVE_INFINITY : nowMs + randomIdleDurationMs(record.mobility);

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

  const startNextRouteSegment = (record, nowMs, segmentStartAt) => {
    const behavior = MOBILITY_BEHAVIOR[record.mobility] || MOBILITY_BEHAVIOR.medium;
    const speedMps = Math.max(0.05, Number(behavior.speedMps) || 0.75);

    while (record.routePoints.length) {
      const endPos = record.routePoints.shift();
      const startPos = [record.position[0], record.position[1], record.position[2]];
      const dx = endPos[0] - startPos[0];
      const dy = endPos[1] - startPos[1];
      const dz = endPos[2] - startPos[2];
      const distance = Math.hypot(dx, dy, dz);

      if (distance <= 0.02) {
        record.position = [endPos[0], endPos[1], endPos[2]];
        continue;
      }

      const minimumDuration = record.routeUsesNavmesh ? minRouteSegmentDurationMs : minWalkDurationMs;
      const durMs = Math.max(minimumDuration, (distance / speedMps) * 1000);
      const t0 = Number.isFinite(segmentStartAt) ? segmentStartAt : nowMs + pathStartDelayMs;
      const desiredYaw =
        Math.hypot(dx, dz) > 0.001
          ? normalizeAngleDeg((Math.atan2(dx, dz) * 180) / Math.PI)
          : normalizeAngleDeg(record.yawDeg);
      const yaw0 = normalizeAngleDeg(record.yawDeg);

      record.state = "walk";
      record.path = { startPos, endPos, t0, dur: durMs, yaw0, yaw1: desiredYaw };
      record.stateEndsAt = t0 + durMs;
      record.yawDeg = desiredYaw;

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
      return true;
    }

    return false;
  };

  const startWalking = (record, desiredWaypointName, nowMs) => {
    updateRecordPositionFromPath(record, nowMs);

    if (record.mobility === "static") {
      setIdle(record, nowMs);
      return;
    }

    let plan = null;
    if (desiredWaypointName) {
      const desired = String(desiredWaypointName).trim().toLowerCase();
      plan = findCommandedWaypointPlan(
        desired,
        waypointData.allWaypoints,
        target => planRouteToPoint(record, target)
      );
      if (!plan) {
        log(`[ghost] Commanded waypoint is not reachable, skipping: ${desired}`);
        setIdle(record, nowMs);
        return;
      }
    }

    if (!plan) {
      plan = pickPatrolPlan(record, record.destination && record.destination.name);
    }

    if (!plan && !waypointData.navPlanner) {
      const angle = Math.random() * Math.PI * 2;
      const radius = 0.8 + Math.random() * 1.2;
      const target = {
        name: "__wander__",
        position: [
          record.homePosition[0] + Math.cos(angle) * radius,
          record.position[1],
          record.homePosition[2] + Math.sin(angle) * radius
        ]
      };
      const route = planRouteToPoint(record, target);
      if (route) plan = { target, route };
    }

    if (!plan || !Array.isArray(plan.route) || plan.route.length < 2) {
      setIdle(record, nowMs);
      return;
    }

    if (plan.target.name && plan.target.name !== "__wander__") {
      reserveTarget(record, plan.target.name);
    } else {
      releaseReservation(record);
    }

    record.destination = {
      name: plan.target.name,
      position: [...plan.route[plan.route.length - 1]]
    };
    record.path = null;
    record.routePoints = plan.route.slice(1).map(point => [point[0], point[1], point[2]]);
    record.routeUsesNavmesh = !!waypointData.navPlanner;

    if (!startNextRouteSegment(record, nowMs, nowMs + pathStartDelayMs)) {
      setIdle(record, nowMs);
    }
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

  const scheduleFullSyncBurst = reason => {
    if (!bots.size) return;

    log(
      `[ghost] full sync burst scheduled reason=${reason} bots=${bots.size} repeats=${fullSyncBurstRepeats} intervalMs=${fullSyncBurstIntervalMs}`
    );

    for (let i = 0; i < fullSyncBurstRepeats; i++) {
      const delayMs = fullSyncBurstInitialDelayMs + i * fullSyncBurstIntervalMs;
      scheduleTimeout(() => {
        if (!bots.size) return;
        broadcastFullSync(timekeeper.nowMs());
      }, delayMs);
    }
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
    if (record.mobility === "static") return;
    if (body.type === "go_to_waypoint" && body.waypoint) {
      startWalking(record, String(body.waypoint), timekeeper.nowMs());
    }
  });

  // Update config on hub refresh (room settings).
  channel.on("hub_refresh", payload => {
    const refreshedHub = payload && Array.isArray(payload.hubs) ? payload.hubs[0] : null;
    const sceneChange = applyHubRefreshSceneChange({
      payload,
      currentSceneUrl: activeSceneState.url,
      currentSceneRejected: activeSceneState.rejected,
      baseUrl,
      policy: sceneFetchPolicy,
      invalidateNavigation(next) {
        sceneGeneration += 1;
        activeSceneState = next;
        waypointData = emptyWaypointData();
        botsConfigDirty = true;
      },
      reconcile: () => reconcileBots(timekeeper.nowMs()),
      publishStatus: () => publishRuntimeStatus(true),
      requestRestart() {
        if (sceneRestartScheduled) return;
        sceneRestartScheduled = true;
        scheduleTimeout(() => {
          log("Published scene changed; restarting ghost runner before loading new navigation geometry.");
          process.exit(1);
        }, 250);
      }
    });
    if (sceneChange.changed) {
      log("Published scene changed; invalidated the cached navigation geometry.");
    }

    if (!shouldApplyHubRefreshConfig(managedConfigReceived)) return;
    const userData = refreshedHub && refreshedHub.user_data;
    if (!userData || typeof userData !== "object" || !Object.prototype.hasOwnProperty.call(userData, "bots")) return;
    botsConfig = normalizeBotsConfig(userData.bots || {});
    botsConfigDirty = true;
  });

  // Presence / late joiners. Losing the authenticated self-presence is fatal:
  // the orchestrator will restart and reauthenticate before another spawn.
  presence.onSync(() => {
    if (!presenceHasAuthenticatedBotRunner(presence, sessionId, runnerLeaseId)) {
      log("Authenticated bot-runner presence was lost; exiting.");
      process.exit(1);
      return;
    }
    const keys = presence.list(key => key) || [];
    const currentOccupants = new Set();

    for (let i = 0; i < keys.length; i++) {
      const k = keys[i];
      if (!k || k === sessionId) continue;
      currentOccupants.add(k);
      if (knownOccupants.has(k)) continue;
      knownOccupants.add(k);
      // A single first-sync can land before late joiners are fully ready to instantiate templates.
      // Burst a few guaranteed first-syncs so bots materialize reliably without relying on a heavy Chromium client.
      scheduleFullSyncBurst(`late-join:${k}`);
    }

    // Drop occupants that left so reconnects trigger a new full sync.
    knownOccupants.forEach(k => {
      if (!currentOccupants.has(k)) {
        knownOccupants.delete(k);
      }
    });
  });

  // Fetch scene waypoints/colliders and featured avatars.
  const sceneUrl = activeSceneState.url;
  const sceneUrlRejected = activeSceneState.rejected;
  const initialSceneGeneration = sceneGeneration;
  if (sceneUrlRejected) {
    log(
      requireNavmesh
        ? "Rejected scene model_url. Navmesh-required bots will remain blocked."
        : "Rejected scene model_url. Bots will use origin fallback."
    );
  }
  if (!sceneUrl && !sceneUrlRejected) {
    log(
      requireNavmesh
        ? "No scene model_url found. Navmesh-required bots will remain blocked."
        : "No scene model_url found in hub payload. Bots will use origin fallback."
    );
  } else {
    log("Scene URL:", redactUrlForLog(sceneUrl));
  }

  const initScenePromise = sceneUrl
    ? retryWithBackoff(
        () =>
          fetchGltfScene(sceneUrl, sceneFetchPolicy).then(async scene => {
          const extracted = extractWaypointsAndColliders(scene.gltf);
          let navPlanner = null;

          if (navigationMode === "navmesh_preferred") {
            try {
              const navMesh = await extractNavMeshGeometry(scene, sceneFetchPolicy);
              navPlanner = createNavMeshPlanner(navMesh, sceneFetchPolicy);
              if (navMesh && navMesh.geometry) navMesh.geometry.dispose();
            } catch (error) {
              log(
                requireNavmesh
                  ? "Navmesh rejected. Navmesh-required bots will remain blocked."
                  : "Navmesh rejected. Falling back to collider/direct waypoint movement.",
                errorCodeForLog(error)
              );
            }
          }

          let navigable = projectWaypointsToNavmesh(extracted, navPlanner);
          if (navPlanner && extracted.allWaypoints.length && !navigable.allWaypoints.length) {
            log("No waypoints could be projected to the navmesh. Blocking navmesh-required bots.");
            navPlanner = null;
            navigable = extracted;
          }
          const points = pickSpawnAndPatrolPoints(navigable);
          const nextWaypointData = {
            ...points,
            allWaypoints: navigable.allWaypoints,
            colliders: extracted.colliders,
            navPlanner
          };
          if (initialSceneGeneration !== sceneGeneration) {
            throw new Error("scene_refresh_superseded");
          }
          waypointData = nextWaypointData;
          if (
            !navPlanner &&
            raycastMode === "spoke_colliders" &&
            (!waypointData.colliders || waypointData.colliders.length === 0)
          ) {
            log("No box-colliders found in scene. Raycast fallback -> allow.");
          }
          if (navPlanner) {
            log(
              `Navmesh ready: triangles=${navPlanner.triangleCount} groups=${navPlanner.groupCount} mode=${navigationMode}`
            );
          } else if (navigationMode === "navmesh_preferred") {
            log(
              requireNavmesh
                ? "No valid navmesh found. Navmesh-required bots are blocked."
                : "No valid navmesh found. Falling back to collider/direct waypoint movement."
            );
          }
          log(
            `Waypoints: all=${waypointData.allWaypoints.length} spawn=${waypointData.spawnPoints.length} patrol=${waypointData.patrolPoints.length} colliders=${waypointData.colliders.length}`
          );
          if (
            requireNavmesh &&
            !navigationReady({ navigationMode, requireNavmesh, waypointData })
          ) {
            throw new Error("required_navmesh_unavailable");
          }
        }),
        {
          maxAttempts: 3,
          baseDelayMs: 500,
          maxDelayMs: 2000,
          onRetry: (error, attempt, delayMs) =>
            log(
              `Scene/navmesh attempt ${attempt}/3 failed; retrying in ${delayMs}ms:`,
              errorCodeForLog(error)
            )
        }
      )
        .catch(err => {
          log(
            requireNavmesh
              ? "Failed to load/parse scene glTF. Navmesh-required bots will remain blocked."
              : "Failed to load/parse scene glTF. Bots will use origin fallback.",
            errorCodeForLog(err)
          );
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
      log("Failed to fetch featured avatars:", errorCodeForLog(err));
    });

  await Promise.all([initScenePromise, initAvatarsPromise]);

  if (requireNavmesh && !navigationReady({ navigationMode, requireNavmesh, waypointData })) {
    scheduleNavigationRecoveryRestart({
      required: true,
      delayMs: process.env.GHOST_NAVIGATION_RECOVERY_RESTART_MS || 30_000,
      scheduleFn: scheduleTimeout,
      exitFn: code => {
        log("Required navmesh is still unavailable; restarting ghost runner for a clean retry.");
        process.exit(code);
      }
    });
  }

  if (process.connected && typeof process.send === "function") {
    try {
      process.send({
        type: "ghost-navigation-status",
        processGeneration: runnerProcessGeneration,
        ready: navigationReady({ navigationMode, requireNavmesh, waypointData }),
        required: requireNavmesh,
        mode: navigationMode
      });
    } catch (_err) {}
  }

  // Main loop.
  let lastConfigRefreshAt = 0;
  let lastFeaturedRefreshAt = 0;
  const CONFIG_REFRESH_INTERVAL_MS = 3000;
  const FEATURED_REFRESH_INTERVAL_MS = 60000;

  const tick = () => {
    const now = timekeeper.nowMs();

    // Reconcile bots periodically so config changes take effect.
    if (botsConfigDirty || now - lastConfigRefreshAt >= CONFIG_REFRESH_INTERVAL_MS) {
      lastConfigRefreshAt = now;
      const changed = reconcileManagedConfig(now);
      if (changed) {
        scheduleFullSyncBurst("reconcile");
      }
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
        if (record.mobility !== "static" && now >= record.stateEndsAt) startWalking(record, null, now);
      } else if (record.state === "walk") {
        if (now >= record.stateEndsAt) {
          const nextSegmentStartAt = record.stateEndsAt;
          if (!record.routePoints.length || !startNextRouteSegment(record, now, nextSegmentStartAt)) {
            setIdle(record, now);
          }
        }
      }
    });
  };

  const initialChanged = reconcileManagedConfig(timekeeper.nowMs());
  if (initialChanged) {
    scheduleFullSyncBurst("startup");
  }
  broadcastFullSync(timekeeper.nowMs());

  const interval = setInterval(tick, 100);

  const shutdown = signal => {
    log(`Received ${signal}, shutting down ghost runner.`);
    clearInterval(interval);
    fullSyncTimers.forEach(timer => clearTimeout(timer));
    fullSyncTimers.clear();
    try {
      pendingSpawns.forEach(pending => {
        sendNaf(channel, removeEntityPayload(pending.record.networkId));
      });
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
}

if (require.main === module) {
  main().catch(err => {
    log("Ghost runner failed:", errorCodeForLog(err));
    process.exitCode = 1;
  });
}

module.exports = {
  main,
  internals: {
    authoritativeBotRunnerLeaseId,
    applyHubRefreshSceneChange,
    cancelPendingSpawnsForTransition,
    computeWorldNodeMatrices,
    createSpawnCleanupController,
    createNavMeshPlanner,
    createSceneFetchPolicy,
    deriveBotRuntimeStatus,
    emptyWaypointData,
    extractNavMeshGeometry,
    extractWaypointsAndColliders,
    fetchFeaturedAvatarRefs,
    fetchGltfJson,
    fetchGltfScene,
    finalizeManagedConfigApplication,
    findCommandedWaypointPlan,
    findSeparatedNavmeshPosition,
    errorCodeForLog,
    managedRunnerConfigFingerprint,
    navigationReady,
    normalizeBotsConfig,
    parseManagedConfigMessage,
    parseGlbJson,
    presenceHasAuthenticatedBotRunner,
    projectWaypointsToNavmesh,
    readAccessor,
    requestBotSpawn,
    resolveHubSceneState,
    redactUrlForLog,
    retryWithBackoff,
    routeMaintainsSeparation,
    scheduleNavigationRecoveryRestart,
    scheduleSpawnRecoveryRestart,
    shouldApplyHubRefreshConfig,
    validateGltfShape,
    validateSceneUrl
  }
};
