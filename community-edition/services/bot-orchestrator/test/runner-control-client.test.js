const assert = require("node:assert/strict");
const { test } = require("node:test");

const { createRunnerControlClient, validControlConfiguration } = require("../runner-control-client");

const generation = "11111111-1111-4111-8111-111111111111";

test("runner control accepts only the local service or its exact namespace-qualified cluster address", () => {
  const base = {
    token: "v1.payload.signature",
    podUid: "22222222-2222-4222-8222-222222222222",
    processGeneration: generation
  };
  for (const controlUrl of [
    "http://bot-orchestrator:5001",
    "http://bot-orchestrator.hcce.svc.cluster.local:5001",
    "http://bot-orchestrator.client-2.svc.cluster.local:5001",
    `http://bot-orchestrator.${"a".repeat(63)}.svc.cluster.local:5001`
  ]) assert.equal(validControlConfiguration({ ...base, controlUrl }), true, controlUrl);
  for (const controlUrl of [
    undefined, null, {}, "http://other.hcce.svc.cluster.local:5001",
    "https://bot-orchestrator.hcce.svc.cluster.local:5001",
    "http://bot-orchestrator.hcce.svc.cluster.local:5002",
    "http://bot-orchestrator.hcce.svc.cluster.local:5001/",
    "http://bot-orchestrator.hcce.svc.cluster.local:5001?x=1",
    "http://bot-orchestrator.hcce.svc.cluster.local:5001#x",
    "http://user@bot-orchestrator.hcce.svc.cluster.local:5001",
    "http://bot-orchestrator.hcce.svc.cluster.local.evil:5001",
    "http://bot-orchestrator.-hcce.svc.cluster.local:5001",
    "http://bot-orchestrator.hcce-.svc.cluster.local:5001",
    "http://bot-orchestrator.HCCE.svc.cluster.local:5001",
    "http://bot-orchestrator.hcce.other.svc.cluster.local:5001",
    `http://bot-orchestrator.${"a".repeat(64)}.svc.cluster.local:5001`,
    "http://bot-orchestrator.hcce.svc.cluster.local:5001\n"
  ]) assert.equal(validControlConfiguration({ ...base, controlUrl }), false, String(controlUrl));
});

test("runner control sends the scoped credential and Pod UID only in headers", async () => {
  const requests = [];
  const client = createRunnerControlClient({
    controlUrl: "http://bot-orchestrator:5001",
    token: "v1.payload.signature",
    podUid: "22222222-2222-4222-8222-222222222222",
    processGeneration: generation,
    fetchImpl: async (url, options) => {
      requests.push({ url, options });
      if (url.endsWith("/config")) {
        return new Response(
          JSON.stringify({ ok: true, process_generation: generation, message: null }),
          { status: 200, headers: { "content-type": "application/json" } }
        );
      }
      return new Response(null, { status: 204 });
    }
  });

  assert.equal(await client.fetchConfig(), null);
  assert.equal(await client.publishStatus({ type: "ghost-navigation-status", processGeneration: generation }), true);
  assert.equal(requests.length, 2);
  assert.equal(requests[0].url.includes("v1.payload.signature"), false);
  assert.equal(requests[0].options.headers.authorization, "Bearer v1.payload.signature");
  assert.equal(
    requests[0].options.headers["x-yenhubs-runner-pod-uid"],
    "22222222-2222-4222-8222-222222222222"
  );
  assert.equal(requests[0].options.redirect, "error");
  assert.equal(requests[0].options.cache, "no-store");
});

test("runner control rejects redirects, oversized responses, and stale generation payloads", async () => {
  const base = {
    controlUrl: "http://bot-orchestrator:5001",
    token: "v1.payload.signature",
    podUid: "22222222-2222-4222-8222-222222222222",
    processGeneration: generation
  };

  const stale = createRunnerControlClient({
    ...base,
    fetchImpl: async () =>
      new Response(JSON.stringify({ ok: true, process_generation: "stale", message: null }), {
        status: 200
      })
  });
  await assert.rejects(stale.fetchConfig(), /config_invalid/);

  const oversized = createRunnerControlClient({
    ...base,
    fetchImpl: async () =>
      new Response("{}", { status: 200, headers: { "content-length": "999999" } })
  });
  await assert.rejects(oversized.fetchConfig(), /too_large/);

  const oversizedStream = createRunnerControlClient({
    ...base,
    fetchImpl: async () => new Response("x".repeat(32 * 1024 + 1), { status: 200 })
  });
  await assert.rejects(oversizedStream.fetchConfig(), /too_large/);

  const statusClient = createRunnerControlClient({
    ...base,
    fetchImpl: async () => new Response(null, { status: 204 })
  });
  await assert.rejects(
    statusClient.publishStatus({
      type: "ghost-runtime-status",
      processGeneration: generation,
      padding: "x".repeat(32 * 1024)
    }),
    /status_too_large/
  );

  assert.throws(
    () => createRunnerControlClient({ ...base, controlUrl: "https://redirect.invalid" }),
    /configuration_invalid/
  );
});
