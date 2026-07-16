const assert = require("node:assert/strict");
const { test } = require("node:test");

const { internals } = require("../run-ghost-runner");

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
