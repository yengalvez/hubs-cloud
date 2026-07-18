const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { EventEmitter } = require("node:events");
const { test } = require("node:test");

const {
  KubernetesApi,
  KubernetesRunnerManager,
  MANAGED_BY_LABEL,
  ROOM_KEY_LABEL
} = require("../kubernetes-runner-manager");
const {
  createRunnerGenerationToken,
  verifyRunnerGenerationToken
} = require("../runner-generation-token");

const key = "test-orchestrator-generation-key-at-least-32";
const generation = "11111111-1111-4111-8111-111111111111";
const ownerUid = "22222222-2222-4222-8222-222222222222";

class FakeApi {
  constructor() {
    this.calls = [];
    this.pods = new Map();
  }

  async request(method, path, body) {
    this.calls.push({ method, path, body });
    if (method === "POST") {
      const pod = structuredClone(body);
      pod.metadata.uid = "33333333-3333-4333-8333-333333333333";
      pod.status = {
        phase: "Running",
        containerStatuses: [{ name: "bot-runner", ready: true }]
      };
      this.pods.set(pod.metadata.name, pod);
      return structuredClone(pod);
    }
    if (method === "GET" && path.includes("?labelSelector=")) {
      return {
        apiVersion: "v1",
        kind: "PodList",
        items: [...this.pods.values()].map(pod => structuredClone(pod))
      };
    }
    if (method === "GET") {
      const pod = this.pods.get(decodeURIComponent(path.split("/").at(-1)));
      if (!pod) throw Object.assign(new Error("not found"), { status: 404 });
      return structuredClone(pod);
    }
    if (method === "DELETE") {
      const name = decodeURIComponent(path.split("/").at(-1));
      this.pods.delete(name);
      return { status: "Success" };
    }
    throw new Error("unexpected fake API request");
  }
}

function manager(api = new FakeApi(), overrides = {}) {
  return new KubernetesRunnerManager({
    api,
    namespace: "hcce",
    ownerPodName: "bot-orchestrator-abc",
    ownerPodUid: ownerUid,
    runnerImage: `registry.invalid/bot-runner@sha256:${"a".repeat(64)}`,
    hubsBaseUrl: "https://hubs.example.test",
    controlUrl: "http://bot-orchestrator:5001",
    credentialKey: key,
    runnerEnvironment: {
      GHOST_NAVIGATION_MODE: "navmesh_preferred",
      GHOST_NAVIGATION_REQUIRE_NAVMESH: "true"
    },
    tokenTtlSeconds: 3600,
    maxActiveRooms: 2,
    now: () => 2_000_000_000_000,
    sleep: async () => {},
    tokenFactory: input => createRunnerGenerationToken({ key, ...input }),
    ...overrides
  });
}

test("creates one hardened, bounded, room-hashed runner Pod without parent secrets", async () => {
  const api = new FakeApi();
  const podManager = manager(api);
  const handle = podManager.create("private-room-sid", generation);
  await new Promise(resolve => handle.once("spawn", resolve));

  const create = api.calls.find(call => call.method === "POST");
  const pod = create.body;
  const container = pod.spec.containers[0];
  const env = Object.fromEntries(container.env.map(entry => [entry.name, entry]));

  assert.match(pod.metadata.name, /^bot-runner-[0-9a-f]{16}-[0-9a-f]{8}$/);
  assert.equal(JSON.stringify(pod.metadata.labels).includes("private-room-sid"), false);
  assert.equal(pod.metadata.labels[MANAGED_BY_LABEL], "bot-orchestrator");
  assert.match(pod.metadata.labels[ROOM_KEY_LABEL], /^[0-9a-f]{20}$/);
  assert.deepEqual(pod.metadata.ownerReferences, [
    {
      apiVersion: "v1",
      kind: "Pod",
      name: "bot-orchestrator-abc",
      uid: ownerUid,
      controller: false,
      blockOwnerDeletion: false
    }
  ]);
  assert.equal(pod.spec.serviceAccountName, "bot-runner");
  assert.equal(pod.spec.automountServiceAccountToken, false);
  assert.deepEqual(pod.spec.imagePullSecrets, [{ name: "bot-images-pull" }]);
  assert.equal(pod.spec.hostPID, false);
  assert.equal(pod.spec.shareProcessNamespace, false);
  assert.equal(container.securityContext.readOnlyRootFilesystem, true);
  assert.equal(container.securityContext.allowPrivilegeEscalation, false);
  assert.deepEqual(container.securityContext.capabilities, { drop: ["ALL"] });
  assert.deepEqual(container.resources, {
    requests: { cpu: "25m", memory: "128Mi" },
    limits: { cpu: "500m", memory: "512Mi" }
  });
  assert.ok(env.BOT_RUNNER_GENERATION_TOKEN.value.startsWith("v1."));
  assert.equal(env.RUNNER_POD_UID.valueFrom.fieldRef.fieldPath, "metadata.uid");
  for (const forbidden of ["BOT_ORCHESTRATOR_ACCESS_KEY", "BOT_RUNNER_ACCESS_KEY", "OPENAI_API_KEY"]) {
    assert.equal(Object.hasOwn(env, forbidden), false);
  }
  assert.equal(handle.podUid, "33333333-3333-4333-8333-333333333333");
  assert.equal(handle.podReady, true);
});

test("a same-room replacement embeds a fresh exact generation credential", () => {
  const podManager = manager();
  const replacementGeneration = "44444444-4444-4444-8444-444444444444";
  const first = podManager.identity("replacement-generation-room", generation);
  const replacement = podManager.identity(
    "replacement-generation-room",
    replacementGeneration
  );

  assert.notEqual(replacement.name, first.name);
  assert.notEqual(replacement.token, first.token);

  const replacementClaims = verifyRunnerGenerationToken(replacement.token, key, {
    hubSid: "replacement-generation-room",
    processGeneration: replacementGeneration,
    holderId: ownerUid,
    nowSeconds: Math.floor(2_000_000_000_000 / 1000)
  });

  assert.equal(replacementClaims.process_generation, replacementGeneration);
  assert.equal(replacementClaims.holder_id, ownerUid);
});

test("accepts only the exact immutable Pod contract", async () => {
  const api = new FakeApi();
  const podManager = manager(api);
  const handle = podManager.create("contract-room", generation);
  await new Promise(resolve => handle.once("spawn", resolve));
  const pod = api.pods.get(handle.name);

  assert.equal(podManager.podMatchesHandle(structuredClone(pod), handle), true);

  const apiNormalizedDefaults = structuredClone(pod);
  delete apiNormalizedDefaults.spec.hostNetwork;
  delete apiNormalizedDefaults.spec.hostPID;
  delete apiNormalizedDefaults.spec.hostIPC;
  assert.equal(podManager.podMatchesHandle(apiNormalizedDefaults, handle), true);

  const wrongImage = structuredClone(pod);
  wrongImage.spec.containers[0].image = `registry.invalid/other@sha256:${"b".repeat(64)}`;
  assert.equal(podManager.podMatchesHandle(wrongImage, handle), false);

  const weakenedSecurity = structuredClone(pod);
  weakenedSecurity.spec.containers[0].securityContext.readOnlyRootFilesystem = false;
  assert.equal(podManager.podMatchesHandle(weakenedSecurity, handle), false);

  const injectedSecret = structuredClone(pod);
  injectedSecret.spec.containers[0].env.push({ name: "OPENAI_API_KEY", value: "forbidden" });
  assert.equal(podManager.podMatchesHandle(injectedSecret, handle), false);

  const extraIdentityLabel = structuredClone(pod);
  extraIdentityLabel.metadata.labels["yenhubs.org/hub-sid"] = "contract-room";
  assert.equal(podManager.podMatchesHandle(extraIdentityLabel, handle), false);
});

test("rejects unsafe manager configuration and sensitive runner environment", () => {
  assert.throws(
    () => manager(new FakeApi(), { hubsBaseUrl: "http://hubs.example.test" }),
    /configuration_missing/
  );
  assert.throws(
    () => manager(new FakeApi(), { runnerEnvironment: { OPENAI_API_KEY: "forbidden" } }),
    /environment_invalid/
  );
});

test("deletion is UID-preconditioned and repeated stop is harmless", async () => {
  const api = new FakeApi();
  const podManager = manager(api);
  const handle = podManager.create("room", generation);
  await new Promise(resolve => handle.once("spawn", resolve));

  const exit = new Promise(resolve => handle.once("exit", resolve));
  assert.equal(handle.kill("SIGTERM"), true);
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(handle.finished, false);
  await podManager.reconcile();
  assert.equal(await exit, 143);
  assert.equal(handle.kill("SIGTERM"), false);

  const deletion = api.calls.find(call => call.method === "DELETE");
  assert.deepEqual(deletion.body.preconditions, { uid: "33333333-3333-4333-8333-333333333333" });
  assert.equal(deletion.body.gracePeriodSeconds, 10);
});

test("an accepted DELETE is not terminal while the Pod is still observable", async () => {
  class DelayedDeleteApi extends FakeApi {
    async request(method, requestPath, body) {
      if (method === "DELETE") {
        this.calls.push({ method, path: requestPath, body });
        return { status: "Success" };
      }
      return super.request(method, requestPath, body);
    }
  }

  const api = new DelayedDeleteApi();
  const podManager = manager(api);
  const handle = podManager.create("slow-delete-room", generation);
  await new Promise(resolve => handle.once("spawn", resolve));
  const exit = new Promise(resolve => handle.once("exit", resolve));

  handle.kill("SIGTERM");
  await new Promise(resolve => setImmediate(resolve));
  await podManager.reconcile();
  assert.equal(handle.finished, false);

  api.pods.delete(handle.name);
  await podManager.reconcile();
  assert.equal(await exit, 143);
});

test("a UID conflict during deletion stays non-terminal until exact absence is confirmed", async () => {
  class UidConflictApi extends FakeApi {
    async request(method, path, body) {
      if (method === "DELETE") {
        this.calls.push({ method, path, body });
        throw Object.assign(new Error("uid precondition failed"), { status: 409 });
      }
      return super.request(method, path, body);
    }
  }

  const api = new UidConflictApi();
  const podManager = manager(api);
  const handle = podManager.create("replacement-room", generation);
  await new Promise(resolve => handle.once("spawn", resolve));
  handle.on("error", () => {});

  const exit = new Promise(resolve => handle.once("exit", resolve));
  assert.equal(handle.kill("SIGTERM"), true);
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(handle.finished, false);
  api.pods.clear();
  await podManager.reconcile();
  assert.equal(await exit, 143);
  assert.equal(handle.finished, true);
});

test("restart reconciliation deletes every unadopted managed Pod and enforces the room ceiling", async () => {
  const api = new FakeApi();
  const orphan = manager(api).podDocument(manager(api).identity("orphan-room", generation));
  orphan.metadata.uid = "44444444-4444-4444-8444-444444444444";
  api.pods.set(orphan.metadata.name, orphan);

  const podManager = manager(api, { maxActiveRooms: 1 });
  await podManager.cleanupOrphans();
  assert.equal(api.pods.size, 0);

  const active = podManager.create("one", generation);
  assert.ok(active);
  assert.equal(podManager.create("two", "55555555-5555-4555-8555-555555555555"), null);
});

test("cleanup and reconciliation fail closed on malformed Kubernetes Pod lists", async () => {
  class MalformedListApi extends FakeApi {
    constructor(response) {
      super();
      this.response = response;
    }

    async request(method, requestPath, body) {
      if (method === "GET" && requestPath.includes("?labelSelector=")) {
        this.calls.push({ method, path: requestPath, body });
        return this.response;
      }
      return super.request(method, requestPath, body);
    }
  }

  for (const response of [null, {}, { kind: "Status", items: [] }, { kind: "PodList" }]) {
    await assert.rejects(manager(new MalformedListApi(response)).cleanupOrphans(), /runner_pod_list_invalid/);
    await assert.rejects(manager(new MalformedListApi(response)).reconcile(), /runner_pod_list_invalid/);
  }
});

test("a create that settles after an empty LIST is never mistaken for a confirmed exit", async () => {
  class DeferredCreateApi extends FakeApi {
    async request(method, requestPath, body) {
      if (method !== "POST") return super.request(method, requestPath, body);
      this.calls.push({ method, path: requestPath, body });
      return new Promise(resolve => {
        this.resolveCreate = () => {
          const pod = structuredClone(body);
          pod.metadata.uid = "66666666-6666-4666-8666-666666666666";
          pod.status = { phase: "Pending", containerStatuses: [] };
          this.pods.set(pod.metadata.name, pod);
          resolve(structuredClone(pod));
        };
      });
    }
  }

  const api = new DeferredCreateApi();
  const podManager = manager(api);
  const handle = podManager.create("deferred-room", generation);
  await podManager.reconcile();
  assert.equal(handle.finished, false);
  assert.equal(handle.createSettled, false);

  const spawned = new Promise(resolve => handle.once("spawn", resolve));
  api.resolveCreate();
  await spawned;
  assert.equal(handle.finished, false);
  assert.equal(handle.podUid, "66666666-6666-4666-8666-666666666666");
});

test("a stop requested during create waits for late-Pod deletion and confirmed absence", async () => {
  class DeferredCreateApi extends FakeApi {
    async request(method, requestPath, body) {
      if (method !== "POST") return super.request(method, requestPath, body);
      this.calls.push({ method, path: requestPath, body });
      return new Promise(resolve => {
        this.resolveCreate = () => {
          const pod = structuredClone(body);
          pod.metadata.uid = "77777777-7777-4777-8777-777777777777";
          pod.status = { phase: "Pending", containerStatuses: [] };
          this.pods.set(pod.metadata.name, pod);
          resolve(structuredClone(pod));
        };
      });
    }
  }

  const api = new DeferredCreateApi();
  const podManager = manager(api);
  const handle = podManager.create("cancelled-create-room", generation);
  const exit = new Promise(resolve => handle.once("exit", resolve));
  handle.kill("SIGTERM");
  assert.equal(handle.finished, false);

  api.resolveCreate();
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(handle.finished, false);
  assert.ok(api.calls.some(call => call.method === "DELETE"));
  await podManager.reconcile();
  assert.equal(await exit, 143);
});

test("Kubernetes API rereads rotated projected ServiceAccount tokens", async () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "yenhubs-kube-api-"));
  const tokenPath = path.join(directory, "token");
  const caPath = path.join(directory, "ca.crt");
  fs.writeFileSync(tokenPath, "first-token\n", { mode: 0o600 });
  fs.writeFileSync(caPath, "test-ca\n", { mode: 0o600 });
  const observed = [];
  const requestImpl = (options, callback) => {
    observed.push(options.headers.authorization);
    const request = new EventEmitter();
    request.setTimeout = () => {};
    request.write = () => {};
    request.destroy = error => request.emit("error", error);
    request.end = () => {
      const response = new EventEmitter();
      response.statusCode = 200;
      callback(response);
      queueMicrotask(() => response.emit("end"));
    };
    return request;
  };

  try {
    const api = new KubernetesApi({
      host: "127.0.0.1",
      port: 443,
      tokenPath,
      caPath,
      requestImpl
    });
    await api.request("GET", "/api/v1/namespaces/hcce/pods");
    fs.writeFileSync(tokenPath, "second-token\n", { mode: 0o600 });
    await api.request("GET", "/api/v1/namespaces/hcce/pods");
    assert.deepEqual(observed, ["Bearer first-token", "Bearer second-token"]);
  } finally {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

test("Kubernetes API uses a total deadline and rejects truncated response bodies", async () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "yenhubs-kube-deadline-"));
  const tokenPath = path.join(directory, "token");
  const caPath = path.join(directory, "ca.crt");
  fs.writeFileSync(tokenPath, "test-token\n", { mode: 0o600 });
  fs.writeFileSync(caPath, "test-ca\n", { mode: 0o600 });

  try {
    let deadlineCallback;
    let destroyed = false;
    const slowApi = new KubernetesApi({
      host: "127.0.0.1",
      port: 443,
      tokenPath,
      caPath,
      scheduleTimeout(callback) {
        deadlineCallback = callback;
        return "deadline";
      },
      cancelTimeout() {},
      requestImpl(_options, callback) {
        const request = new EventEmitter();
        request.write = () => {};
        request.destroy = error => {
          destroyed = true;
          request.emit("error", error);
        };
        request.end = () => {
          const response = new EventEmitter();
          response.statusCode = 200;
          callback(response);
          response.emit("data", Buffer.from('{"kind":'));
          response.emit("data", Buffer.from('"PodList"'));
        };
        return request;
      }
    });
    const slowRequest = slowApi.request("GET", "/api/v1/namespaces/hcce/pods");
    deadlineCallback();
    await assert.rejects(slowRequest, /kubernetes_request_timeout/);
    assert.equal(destroyed, true);

    const truncatedApi = new KubernetesApi({
      host: "127.0.0.1",
      port: 443,
      tokenPath,
      caPath,
      scheduleTimeout: () => "deadline",
      cancelTimeout() {},
      requestImpl(_options, callback) {
        const request = new EventEmitter();
        request.write = () => {};
        request.destroy = error => request.emit("error", error);
        request.end = () => {
          const response = new EventEmitter();
          response.statusCode = 200;
          callback(response);
          queueMicrotask(() => {
            response.emit("data", Buffer.from('{"kind":"PodList"'));
            response.emit("aborted");
          });
        };
        return request;
      }
    });
    await assert.rejects(
      truncatedApi.request("GET", "/api/v1/namespaces/hcce/pods"),
      /kubernetes_response_aborted/
    );
  } finally {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});
