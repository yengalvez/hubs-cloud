const assert = require("node:assert/strict");
const { test } = require("node:test");

const { internals } = require("../run-ghost-runner");

function makeGlb(gltf) {
  const json = Buffer.from(JSON.stringify(gltf), "utf8");
  const paddedLength = Math.ceil(json.length / 4) * 4;
  const bytes = Buffer.alloc(20 + paddedLength, 0x20);
  bytes.writeUInt32LE(0x46546c67, 0);
  bytes.writeUInt32LE(2, 4);
  bytes.writeUInt32LE(bytes.length, 8);
  bytes.writeUInt32LE(paddedLength, 12);
  bytes.writeUInt32LE(0x4e4f534a, 16);
  json.copy(bytes, 20);
  return bytes;
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
