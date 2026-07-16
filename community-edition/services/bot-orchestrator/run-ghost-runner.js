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

async function readAccessor(scene, accessorIndex, policy, cache = new Map()) {
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
  if (!itemSize || !componentBytes || !Number.isInteger(count) || count < 0) {
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
    byteStride < elementBytes
  ) {
    throw new Error("gltf_invalid_buffer_view");
  }

  const requiredBytes = count === 0 ? 0 : accessorByteOffset + (count - 1) * byteStride + elementBytes;
  if (requiredBytes > viewByteLength) throw new Error("gltf_accessor_out_of_bounds");

  const cacheKey = `${viewByteOffset}:${viewByteLength}`;
  let buffer = cache.get(cacheKey);
  if (!buffer) {
    const absoluteStart = scene.glbBinStart + viewByteOffset;
    buffer = await fetchSceneByteRange(scene, absoluteStart, viewByteLength, policy);
    cache.set(cacheKey, buffer);
  }

  const dataView = new DataView(buffer);
  const values = new Float64Array(count * itemSize);
  for (let itemIndex = 0; itemIndex < count; itemIndex++) {
    const elementOffset = accessorByteOffset + itemIndex * byteStride;
    for (let componentIndex = 0; componentIndex < itemSize; componentIndex++) {
      values[itemIndex * itemSize + componentIndex] = readComponent(
        dataView,
        elementOffset + componentIndex * componentBytes,
        accessor.componentType
      );
    }
  }

  return {
    values,
    count,
    itemSize,
    componentType: accessor.componentType
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

      // eslint-disable-next-line no-await-in-loop
      const positionAccessor = await readAccessor(scene, positionAccessorIndex, policy, bufferCache);
      if (positionAccessor.itemSize !== 3 || positionAccessor.componentType !== 5126) {
        throw new Error("navmesh_position_accessor_invalid");
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

      let primitiveIndices;
      if (Number.isInteger(primitive.indices)) {
        // eslint-disable-next-line no-await-in-loop
        const indexAccessor = await readAccessor(scene, primitive.indices, policy, bufferCache);
        if (
          indexAccessor.itemSize !== 1 ||
          ![5121, 5123, 5125].includes(indexAccessor.componentType) ||
          indexAccessor.count % 3 !== 0
        ) {
          throw new Error("navmesh_index_accessor_invalid");
        }
        primitiveIndices = indexAccessor.values;
      } else {
        if (positionAccessor.count % 3 !== 0) throw new Error("navmesh_nonindexed_triangle_count_invalid");
        primitiveIndices = Array.from({ length: positionAccessor.count }, (_value, index) => index);
      }

      triangleCount += primitiveIndices.length / 3;
      if (triangleCount > policy.maxNavmeshTriangles) throw new Error("navmesh_too_many_triangles");

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

async function main() {
  const options = docopt(doc);

  const baseUrl = options["--url"] || "https://meta-hubs.org";
  const sceneFetchPolicy = createSceneFetchPolicy(baseUrl);
  const hubSid = options["--room"];
  if (!hubSid) {
    log("Missing --room");
    process.exit(1);
  }

  const botAccessKey = process.env.BOT_ACCESS_KEY || "";
  const raycastMode = (process.env.GHOST_RAYCAST_MODE || "spoke_colliders").trim().toLowerCase();
  const navigationMode =
    (process.env.GHOST_NAVIGATION_MODE || "navmesh_preferred").trim().toLowerCase() === "colliders"
      ? "colliders"
      : "navmesh_preferred";
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

  const handleManagedConfig = message => {
    if (!message || message.type !== "bots-config") return;
    botsConfig = normalizeBotsConfig(message.bots);
    botsConfigDirty = true;
    if (process.connected && typeof process.send === "function") {
      try {
        process.send({
          type: "bots-config-applied",
          fingerprint: typeof message.fingerprint === "string" ? message.fingerprint : ""
        });
      } catch (error) {
        log("Failed to acknowledge managed bot config:", error.message);
      }
    }
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

  if (!botsConfig) {
    botsConfig = normalizeBotsConfig((hub.user_data && hub.user_data.bots) || {});
  }

  // Keep these cached and refresh periodically.
  let waypointData = {
    spawnPoints: [],
    patrolPoints: [],
    allWaypoints: [],
    colliders: [],
    navPlanner: null
  };
  let avatarRefs = [];
  let fullbodyRefs = [];
  let avatarRotationOffset = Math.floor(Math.random() * 1000);

  const bots = new Map();
  const reservedTargets = new Map();
  const knownOccupants = new Set();
  const fullSyncTimers = new Set();

  const scheduleTimeout = (fn, delayMs) => {
    const timer = setTimeout(() => {
      fullSyncTimers.delete(timer);
      fn();
    }, delayMs);
    fullSyncTimers.add(timer);
    return timer;
  };

  const reconcileBots = nowMs => {
    let changed = false;

    if (!botsConfig.enabled || botsConfig.count <= 0) {
      if (bots.size > 0) {
        bots.forEach(record => {
          sendNaf(channel, removeEntityPayload(record.networkId));
        });
        bots.clear();
        reservedTargets.clear();
        changed = true;
      }
      return changed;
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
      changed = true;
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
      let pos = separateNearbyPosition(basePos, index, usedPositions);
      if (waypointData.navPlanner) {
        const projected = waypointData.navPlanner.projectPoint(pos);
        pos = projected ? projected.position : [basePos[0], basePos[1], basePos[2]];
      }
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
        path: null,
        routePoints: []
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

    return changed;
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

    if (waypointData.navPlanner) {
      return waypointData.navPlanner.findRoute(record.position, point.position);
    }

    if (
      raycastMode === "spoke_colliders" &&
      !isPathClearWithColliders(waypointData.colliders, record.position, point.position)
    ) {
      return null;
    }

    return [
      [record.position[0], record.position[1], record.position[2]],
      [point.position[0], point.position[1], point.position[2]]
    ];
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
      const target =
        waypointData.allWaypoints.find(p => (p.name || "").trim().toLowerCase() === desired) || null;
      const route = target ? planRouteToPoint(record, target) : null;
      if (target && route) {
        plan = { target, route };
      } else {
        log(`[ghost] Commanded waypoint is not reachable, skipping: ${desired}`);
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
    const userData = refreshedHub && refreshedHub.user_data;
    if (!userData || typeof userData !== "object" || !Object.prototype.hasOwnProperty.call(userData, "bots")) return;
    const nextConfig = normalizeBotsConfig(userData.bots || {});
    botsConfig = nextConfig;
    botsConfigDirty = true;
  });

  // Presence / late joiners.
  const presence = new Presence(channel);
  presence.onSync(() => {
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
  const rawSceneUrl = hub && hub.scene && hub.scene.model_url ? resolveUrl(baseUrl, hub.scene.model_url) : "";
  let sceneUrl = "";
  let sceneUrlRejected = false;
  if (rawSceneUrl) {
    try {
      sceneUrl = validateSceneUrl(rawSceneUrl, sceneFetchPolicy).toString();
    } catch (err) {
      sceneUrlRejected = true;
      log("Rejected scene model_url. Bots will use origin fallback.", err.message);
    }
  }
  if (!sceneUrl && !sceneUrlRejected) {
    log("No scene model_url found in hub payload. Bots will use origin fallback.");
  } else {
    log("Scene URL:", sceneUrl);
  }

  const initScenePromise = sceneUrl
    ? fetchGltfScene(sceneUrl, sceneFetchPolicy)
        .then(async scene => {
          const extracted = extractWaypointsAndColliders(scene.gltf);
          let navPlanner = null;

          if (navigationMode === "navmesh_preferred") {
            try {
              const navMesh = await extractNavMeshGeometry(scene, sceneFetchPolicy);
              navPlanner = createNavMeshPlanner(navMesh, sceneFetchPolicy);
              if (navMesh && navMesh.geometry) navMesh.geometry.dispose();
            } catch (error) {
              log("Navmesh rejected. Falling back to collider/direct waypoint movement.", error.message);
            }
          }

          let navigable = projectWaypointsToNavmesh(extracted, navPlanner);
          if (navPlanner && extracted.allWaypoints.length && !navigable.allWaypoints.length) {
            log("No waypoints could be projected to the navmesh. Disabling navmesh for this scene.");
            navPlanner = null;
            navigable = extracted;
          }
          const points = pickSpawnAndPatrolPoints(navigable);
          waypointData = {
            ...points,
            allWaypoints: navigable.allWaypoints,
            colliders: extracted.colliders,
            navPlanner
          };
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
            log("No valid navmesh found. Falling back to collider/direct waypoint movement.");
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
    if (botsConfigDirty || now - lastConfigRefreshAt >= CONFIG_REFRESH_INTERVAL_MS) {
      botsConfigDirty = false;
      lastConfigRefreshAt = now;
      const changed = reconcileBots(now);
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

  const initialChanged = reconcileBots(timekeeper.nowMs());
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
    log("Ghost runner failed:", err && err.message ? err.message : "unknown_error");
    process.exitCode = 1;
  });
}

module.exports = {
  main,
  internals: {
    computeWorldNodeMatrices,
    createNavMeshPlanner,
    createSceneFetchPolicy,
    extractNavMeshGeometry,
    extractWaypointsAndColliders,
    fetchGltfJson,
    fetchGltfScene,
    normalizeBotsConfig,
    parseGlbJson,
    projectWaypointsToNavmesh,
    readAccessor,
    validateGltfShape,
    validateSceneUrl
  }
};
