const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { EventEmitter } = require("node:events");
const { test } = require("node:test");

const {
  KubernetesApi,
  KubernetesRunnerManager,
  MAX_GUARD_PODS,
  MAX_GUARD_START_COUNT,
  MIN_GUARD_FENCE_RESERVE,
  MANAGED_BY_LABEL,
  PARENT_NAME_ANNOTATION,
  PARENT_NAMESPACE_ANNOTATION,
  PARENT_UID_ANNOTATION,
  ROOM_KEY_LABEL,
  RUNNER_PROTOCOL_LABEL,
  RUNNER_PROTOCOL_VALUE
} = require("../kubernetes-runner-manager");
const {
  createRunnerGenerationToken,
  verifyRunnerGenerationToken
} = require("../runner-generation-token");

const key = "test-orchestrator-generation-key-at-least-32";
const generation = "11111111-1111-4111-8111-111111111111";
const ownerUid = "22222222-2222-4222-8222-222222222222";
const recoveryEpoch = "44444444-4444-4444-8444-444444444444";

class FakeApi {
  constructor() {
    this.calls = [];
    this.pods = new Map();
    this.resourceVersion = 0;
    this.uidCounter = 0;
    this.requestTimeoutMs = 10_000;
  }

  nextUid() {
    this.uidCounter += 1;
    return `00000000-0000-4000-8000-${String(this.uidCounter).padStart(12, "0")}`;
  }

  async request(method, path, body, options) {
    this.calls.push({ method, path, body, options });
    if (method === "POST") {
      if (this.pods.has(body.metadata.name)) {
        throw Object.assign(new Error("already exists"), { status: 409 });
      }
      const pod = structuredClone(body);
      pod.metadata.uid = this.nextUid();
      pod.metadata.resourceVersion = String(++this.resourceVersion);
      pod.status = pod.metadata.labels.app === "bot-runner"
        ? { phase: "Running", containerStatuses: [{ name: "bot-runner", ready: true }] }
        : { phase: "Pending", containerStatuses: [] };
      this.pods.set(pod.metadata.name, pod);
      return structuredClone(pod);
    }
    const url = new URL(path, "https://kubernetes.invalid");
    if (method === "GET" && url.pathname.endsWith("/pods")) {
      const selector = url.searchParams.get("labelSelector");
      const requirements = selector
        ? selector.split(",").map(requirement => requirement.split("="))
        : [];
      const items = [...this.pods.values()].filter(pod =>
        requirements.every(([name, value]) => pod?.metadata?.labels?.[name] === value)
      );
      return {
        apiVersion: "v1",
        kind: "PodList",
        metadata: { resourceVersion: String(++this.resourceVersion) },
        items: items.map(pod => structuredClone(pod))
      };
    }
    if (method === "PATCH") {
      const name = decodeURIComponent(url.pathname.split("/").at(-1));
      const pod = this.pods.get(name);
      if (!pod) throw Object.assign(new Error("not found"), { status: 404 });
      const tests = Object.fromEntries(body.filter(operation => operation.op === "test")
        .map(operation => [operation.path, operation.value]));
      if (
        tests["/metadata/uid"] !== pod.metadata.uid ||
        tests["/metadata/resourceVersion"] !== pod.metadata.resourceVersion ||
        tests["/metadata/annotations/yenhubs.org~1intent-state"] !==
          pod.metadata.annotations?.["yenhubs.org/intent-state"]
      ) {
        throw Object.assign(new Error("patch conflict"), { status: 409 });
      }
      pod.metadata.annotations["yenhubs.org/intent-state"] = "armed";
      pod.metadata.resourceVersion = String(++this.resourceVersion);
      return structuredClone(pod);
    }
    if (method === "GET") {
      const pod = this.pods.get(decodeURIComponent(url.pathname.split("/").at(-1)));
      if (!pod) throw Object.assign(new Error("not found"), { status: 404 });
      return structuredClone(pod);
    }
    if (method === "DELETE") {
      const name = decodeURIComponent(url.pathname.split("/").at(-1));
      const pod = this.pods.get(name);
      if (
        pod &&
        (body?.preconditions?.uid !== pod.metadata.uid ||
         (body?.preconditions?.resourceVersion !== undefined &&
          body.preconditions.resourceVersion !== pod.metadata.resourceVersion))
      ) {
        throw Object.assign(new Error("uid precondition failed"), { status: 409 });
      }
      this.pods.delete(name);
      return { status: "Success" };
    }
    throw new Error("unexpected fake API request");
  }
}

class AmbiguousCreateApi extends FakeApi {
  constructor() {
    super();
    this.requestTimeoutMs = 1_000;
    this.ambiguousDocument = null;
  }

  async request(method, requestPath, body, options) {
    if (method !== "POST" || body?.metadata?.labels?.app !== "bot-runner") {
      return super.request(method, requestPath, body, options);
    }
    this.calls.push({ method, path: requestPath, body });
    this.ambiguousDocument = structuredClone(body);
    throw new Error("kubernetes_request_timeout");
  }

  commitLate(uid = "cccccccc-cccc-4ccc-8ccc-cccccccccccc") {
    if (this.pods.has(this.ambiguousDocument.metadata.name)) return false;
    const pod = structuredClone(this.ambiguousDocument);
    pod.metadata.uid = uid;
    pod.metadata.resourceVersion = String(++this.resourceVersion);
    pod.status = { phase: "Pending", containerStatuses: [] };
    this.pods.set(pod.metadata.name, pod);
    return pod;
  }
}

function manager(api = new FakeApi(), overrides = {}) {
  const podManager = new KubernetesRunnerManager({
    api,
    namespace: "hcce-bot-runners",
    parentNamespace: "hcce",
    ownerPodName: "bot-orchestrator-abc",
    ownerPodUid: ownerUid,
    runnerImage: `registry.invalid/bot-runner@sha256:${"a".repeat(64)}`,
    hubsBaseUrl: "https://hubs.example.test",
    controlUrl: "http://bot-orchestrator.hcce.svc.cluster.local:5001",
    credentialKey: key,
    runnerEnvironment: {
      GHOST_NAVIGATION_MODE: "navmesh_preferred",
      GHOST_NAVIGATION_REQUIRE_NAVMESH: "true"
    },
    tokenTtlSeconds: 3600,
    maxActiveRooms: 2,
    now: () => 2_000_000_000_000,
    sleep: async () => {},
    tokenFactory: input => createRunnerGenerationToken({ key, recoveryEpoch, ...input }),
    ...overrides
  });
  return podManager;
}

test("creates one hardened, bounded, room-hashed runner Pod without parent secrets", async () => {
  const api = new FakeApi();
  const podManager = manager(api);
  const handle = podManager.create("private-room-sid", generation);
  await new Promise(resolve => handle.once("spawn", resolve));

  const intentCreate = api.calls.find(call =>
    call.method === "POST" && call.body?.metadata?.labels?.app === "bot-runner-intent"
  );
  const create = api.calls.find(call =>
    call.method === "POST" && call.body?.metadata?.labels?.app === "bot-runner"
  );
  assert.ok(intentCreate, "a durable intent must be created before the runner");
  assert.equal(create.path.endsWith("?timeout=5s"), true);
  const pod = create.body;
  const container = pod.spec.containers[0];
  assert.equal(pod.metadata.labels[RUNNER_PROTOCOL_LABEL], RUNNER_PROTOCOL_VALUE);
  const env = Object.fromEntries(container.env.map(entry => [entry.name, entry]));

  assert.match(pod.metadata.name, /^bot-runner-[0-9a-f]{16}-[0-9a-f]{8}$/);
  assert.equal(JSON.stringify(pod.metadata.labels).includes("private-room-sid"), false);
  assert.equal(pod.metadata.labels[MANAGED_BY_LABEL], "bot-orchestrator");
  assert.match(pod.metadata.labels[ROOM_KEY_LABEL], /^[0-9a-f]{20}$/);
  assert.equal(Object.hasOwn(pod.metadata, "ownerReferences"), false);
  assert.deepEqual(pod.metadata.annotations, {
    "yenhubs.org/expires-at": "2033-05-18T04:33:20.000Z",
    [PARENT_NAMESPACE_ANNOTATION]: "hcce",
    [PARENT_NAME_ANNOTATION]: "bot-orchestrator-abc",
    [PARENT_UID_ANNOTATION]: ownerUid
  });
  assert.equal(pod.spec.serviceAccountName, "bot-runner");
  assert.equal(pod.spec.automountServiceAccountToken, false);
  assert.deepEqual(pod.spec.imagePullSecrets, [{ name: "bot-images-pull" }]);
  assert.equal(pod.spec.hostPID, false);
  assert.equal(pod.spec.shareProcessNamespace, false);
  assert.equal(container.securityContext.readOnlyRootFilesystem, true);
  assert.equal(container.securityContext.allowPrivilegeEscalation, false);
  assert.deepEqual(container.securityContext.appArmorProfile, { type: "RuntimeDefault" });
  assert.equal(container.imagePullPolicy, "Always");
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
  assert.equal(handle.podUid, api.pods.get(handle.name).metadata.uid);
  assert.equal(handle.podReady, true);
  assert.ok(
    api.calls.findIndex(call => call === intentCreate) < api.calls.findIndex(call => call === create)
  );
  const arm = api.calls.find(call => call.method === "PATCH");
  assert.equal(arm.options.contentType, "application/json-patch+json");
  assert.ok(api.calls.indexOf(arm) < api.calls.indexOf(create));
});

test("pre-AUD078 runner, intent, and fence shapes without the durable marker are rejected", () => {
  const podManager = manager();
  const identity = podManager.identity("private-room-sid", generation);
  const runner = podManager.podDocument(identity);
  runner.metadata.uid = "runner-uid";
  runner.metadata.resourceVersion = "1";
  assert.doesNotThrow(() => podManager.runnerRecordFromPod(runner));
  delete runner.metadata.labels[RUNNER_PROTOCOL_LABEL];
  assert.throws(() => podManager.runnerRecordFromPod(runner), /runner_pod_contract_invalid/);

  for (const type of ["intent", "fence"]) {
    const guard = podManager.guardPodDocument(identity, type);
    guard.metadata.uid = `${type}-uid`;
    guard.metadata.resourceVersion = "2";
    guard.status = { phase: "Pending" };
    assert.doesNotThrow(() => podManager.guardRecordFromPod(guard, type));
    delete guard.metadata.labels[RUNNER_PROTOCOL_LABEL];
    assert.throws(() => podManager.guardRecordFromPod(guard, type), /runner_guard_contract_invalid/);
  }
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
    recoveryEpoch,
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

  const extraAnnotation = structuredClone(pod);
  extraAnnotation.metadata.annotations["container.apparmor.security.beta.kubernetes.io/bot-runner"] =
    "unconfined";
  assert.equal(podManager.podMatchesHandle(extraAnnotation, handle), false);

  const secretMount = structuredClone(pod);
  secretMount.spec.volumes[0] = { name: "runner-tmp", secret: { secretName: "bot-images-pull" } };
  assert.equal(podManager.podMatchesHandle(secretMount, handle), false);

  for (const mutate of [
    value => { value.spec.runtimeClassName = "unsafe"; },
    value => { value.spec.resourceClaims = [{ name: "device" }]; },
    value => { value.spec.hostAliases = [{ ip: "127.0.0.1", hostnames: ["api.openai.com"] }]; },
    value => { value.spec.dnsConfig = { nameservers: ["203.0.113.1"] }; },
    value => { value.spec.overhead = { cpu: "1" }; },
    value => { value.spec.containers[0].volumeDevices = [{ name: "runner-tmp", devicePath: "/dev/x" }]; }
  ]) {
    const changed = structuredClone(pod);
    mutate(changed);
    assert.equal(podManager.podMatchesHandle(changed, handle), false);
  }
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
  assert.throws(
    () => manager(new FakeApi(), { namespace: "hcce" }),
    /configuration_missing/
  );
  assert.throws(
    () => manager(new FakeApi(), { createServerTimeoutSeconds: 0 }),
    /create_server_timeout_invalid/
  );
  assert.throws(
    () => manager(new FakeApi(), { createServerTimeoutSeconds: 31 }),
    /create_server_timeout_invalid/
  );
  const slowApi = new FakeApi();
  slowApi.requestTimeoutMs = 30_001;
  assert.throws(() => manager(slowApi), /api_request_timeout_invalid/);
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

  const deletion = api.calls.find(call =>
    call.method === "DELETE" &&
    decodeURIComponent(new URL(call.path, "https://kubernetes.invalid").pathname.split("/").at(-1)) ===
      handle.name
  );
  assert.deepEqual(deletion.body.preconditions, { uid: handle.podUid });
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
      if (method === "GET" && new URL(requestPath, "https://kubernetes.invalid").pathname.endsWith("/pods")) {
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
      if (method !== "POST" || body?.metadata?.labels?.app !== "bot-runner") {
        return super.request(method, requestPath, body);
      }
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
  while (!api.resolveCreate) await new Promise(resolve => setImmediate(resolve));
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
      if (method !== "POST" || body?.metadata?.labels?.app !== "bot-runner") {
        return super.request(method, requestPath, body);
      }
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
  while (!api.resolveCreate) await new Promise(resolve => setImmediate(resolve));
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

test("an ambiguous runner CREATE is resolved only by a durable same-name fence", async () => {
  const api = new AmbiguousCreateApi();
  const podManager = manager(api, { createServerTimeoutSeconds: 1 });
  const handle = podManager.create("ambiguous-create-room", generation);
  await new Promise(resolve => handle.once("error", resolve));

  assert.equal(handle.finished, false);
  assert.equal(podManager.createIntents.get(handle.name).runnerPostIssued, true);
  assert.equal(
    api.calls.filter(call =>
      call.method === "POST" && call.body?.metadata?.labels?.app === "bot-runner"
    ).length,
    1
  );

  const target = [{ name: handle.name, uid: null }];
  const fenced = await podManager.confirmRoomStopped("ambiguous-create-room", target);
  assert.equal(fenced.terminal, false);
  assert.equal(fenced.managedRoomPods, 0);
  assert.equal(fenced.fenced, true);
  assert.equal(handle.finished, true);
  assert.equal(api.pods.get(handle.name).metadata.labels.app, "bot-runner-fence");
  assert.equal((await podManager.confirmRoomStopped("ambiguous-create-room", target)).terminal, true);
  assert.deepEqual(podManager.guardCapacitySnapshot(), {
    observed: true,
    intents: 0,
    fences: 1,
    total: 1,
    warning: false,
    warning_threshold: 60,
    start_limit: 80,
    reserve: 20,
    quota: 100
  });
  assert.equal(
    api.commitLate(),
    false,
    "a delayed runner proposal after the terminal ACK must conflict with the fence"
  );
});

test("a runner that wins the fence race is deleted by UID before fence retry", async () => {
  class RunnerWinsFenceApi extends AmbiguousCreateApi {
    async request(method, requestPath, body, options) {
      if (
        method === "POST" &&
        body?.metadata?.labels?.app === "bot-runner-fence" &&
        !this.runnerWonFenceRace
      ) {
        this.calls.push({ method, path: requestPath, body, options });
        this.runnerWonFenceRace = this.commitLate("dddddddd-dddd-4ddd-8ddd-dddddddddddd");
        throw Object.assign(new Error("already exists"), { status: 409 });
      }
      return super.request(method, requestPath, body, options);
    }
  }

  const api = new RunnerWinsFenceApi();
  const podManager = manager(api);
  const handle = podManager.create("fence-race-room", generation);
  await new Promise(resolve => handle.once("error", resolve));
  const result = await podManager.confirmRoomStopped("fence-race-room", [
    { name: handle.name, uid: null }
  ]);

  assert.equal(result.terminal, false);
  assert.equal(api.pods.get(handle.name).metadata.labels.app, "bot-runner-fence");
  assert.ok(api.calls.some(call =>
    call.method === "DELETE" &&
    call.body?.preconditions?.uid === "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
  ));
  assert.equal(
    api.calls.filter(call =>
      call.method === "POST" && call.body?.metadata?.labels?.app === "bot-runner"
    ).length,
    1,
    "an ambiguous runner CREATE is never retried"
  );
});

test("fence creation waits for an accepted runner DELETE to become observable", async () => {
  class DelayedFenceDeleteApi extends AmbiguousCreateApi {
    async request(method, requestPath, body, options) {
      const name = decodeURIComponent(
        new URL(requestPath, "https://kubernetes.invalid").pathname.split("/").at(-1)
      );
      if (
        method === "DELETE" &&
        name === this.ambiguousDocument?.metadata?.name &&
        this.pods.get(name)?.metadata?.labels?.app === "bot-runner"
      ) {
        this.calls.push({ method, path: requestPath, body, options });
        this.delayedDeleteName = name;
        return { status: "Success" };
      }
      return super.request(method, requestPath, body, options);
    }
  }

  const api = new DelayedFenceDeleteApi();
  let retryWaits = 0;
  const podManager = manager(api, {
    sleep: async () => {
      retryWaits += 1;
      if (
        api.delayedDeleteName &&
        api.pods.get(api.delayedDeleteName)?.metadata?.labels?.app === "bot-runner"
      ) {
        api.pods.delete(api.delayedDeleteName);
        api.delayedDeleteName = null;
      }
    }
  });
  const handle = podManager.create("delayed-fence-delete-room", generation);
  await new Promise(resolve => handle.once("error", resolve));
  assert.ok(api.commitLate("dddddddd-dddd-4ddd-8ddd-ddddddddddde"));

  const result = await podManager.confirmRoomStopped("delayed-fence-delete-room", [
    { name: handle.name, uid: null }
  ]);
  assert.equal(result.terminal, false);
  assert.ok(retryWaits >= 1, "fence reconciliation must yield between DELETE retries");
  assert.equal(api.pods.get(handle.name).metadata.labels.app, "bot-runner-fence");
});

test("restart fences every armed intent before readiness without a time assumption", async () => {
  const api = new AmbiguousCreateApi();
  const firstManager = manager(api);
  const handle = firstManager.create("restart-ambiguous-room", generation);
  await new Promise(resolve => handle.once("error", resolve));
  firstManager.close();

  const restartedManager = manager(api);
  const cleanup = await restartedManager.cleanupOrphans();
  assert.equal(cleanup.managed, 0);
  assert.equal(api.pods.get(handle.name).metadata.labels.app, "bot-runner-fence");
  assert.equal(
    api.commitLate("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"),
    false,
    "the fence installed after restart must reject a proposal that materializes later"
  );
  assert.equal(
    [...api.pods.values()].filter(pod => pod.metadata.labels.app === "bot-runner").length,
    0
  );
});

test("restart reconciliation preserves the fence after deleting a runner from its stale LIST", async () => {
  const api = new AmbiguousCreateApi();
  const firstManager = manager(api);
  const handle = firstManager.create("restart-reconcile-race", generation);
  await new Promise(resolve => handle.once("error", resolve));
  const lateRunner = api.commitLate("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeef");
  assert.ok(lateRunner);
  firstManager.close();

  const restartedManager = manager(api);
  await restartedManager.reconcile();

  const target = api.pods.get(handle.name);
  assert.equal(target.metadata.labels.app, "bot-runner-fence");
  const targetDeletes = api.calls.filter(call =>
    call.method === "DELETE" &&
    decodeURIComponent(
      new URL(call.path, "https://kubernetes.invalid").pathname.split("/").at(-1)
    ) === handle.name
  );
  assert.deepEqual(targetDeletes.map(call => call.body.preconditions.uid), [lateRunner.metadata.uid]);
});

test("a stop interleaved with the final pre-arm inventory emits no PATCH or runner POST", async () => {
  class DeferredArmInventoryApi extends FakeApi {
    async request(method, requestPath, body, options) {
      const url = new URL(requestPath, "https://kubernetes.invalid");
      if (method === "GET" && url.pathname.endsWith("/pods")) {
        this.listCount = (this.listCount || 0) + 1;
        if (this.listCount === 2) {
          return new Promise(resolve => {
            this.releaseArmInventory = async () => resolve(
              super.request(method, requestPath, body, options)
            );
          });
        }
      }
      return super.request(method, requestPath, body, options);
    }
  }

  const api = new DeferredArmInventoryApi();
  const podManager = manager(api);
  const handle = podManager.create("arm-stop-race", generation);
  handle.on("error", () => {});
  while (!api.releaseArmInventory) await new Promise(resolve => setImmediate(resolve));
  handle.kill("SIGTERM");
  await api.releaseArmInventory();
  await new Promise(resolve => setImmediate(resolve));

  assert.equal(api.calls.some(call => call.method === "PATCH"), false);
  assert.equal(api.calls.some(call =>
    call.method === "POST" && call.body?.metadata?.labels?.app === "bot-runner"
  ), false);
  assert.equal((await podManager.confirmRoomStopped("arm-stop-race")).terminal, false);
  assert.equal((await podManager.confirmRoomStopped("arm-stop-race")).terminal, true);
});

test("one complete empty inventory is terminal because arm-and-POST is serialized and durable", async () => {
  const api = new FakeApi();
  const firstManager = manager(api);

  const first = await firstManager.confirmRoomStopped("empty-room");
  assert.equal(first.terminal, true);
  assert.equal(first.targetAbsent, true);
  assert.equal(first.managedRoomPods, 0);

  const roomListCalls = api.calls.filter(call =>
    call.method === "GET" && new URL(call.path, "https://kubernetes.invalid").pathname.endsWith("/pods")
  );
  assert.equal(roomListCalls.length, 1);
  assert.equal(roomListCalls.some(call => call.path.includes("empty-room")), false);

  const restartedManager = manager(api);
  assert.equal((await restartedManager.confirmRoomStopped("empty-room")).terminal, true);
});

test("room stop deletes visible, terminal, unknown and ABA runner Pods by observed UID before ACK", async () => {
  const api = new FakeApi();
  const podManager = manager(api);
  const handle = podManager.create("stop-proof-room", generation);
  await new Promise(resolve => handle.once("spawn", resolve));
  const originalUid = handle.podUid;

  const replacement = structuredClone(api.pods.get(handle.name));
  replacement.metadata.uid = "88888888-8888-4888-8888-888888888888";
  replacement.status.phase = "Succeeded";
  api.pods.set(handle.name, replacement);
  handle.podUid = replacement.metadata.uid;

  const unknownIdentity = podManager.identity(
    "stop-proof-room",
    "99999999-9999-4999-8999-999999999999"
  );
  const unknown = podManager.podDocument(unknownIdentity);
  unknown.metadata.uid = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
  unknown.status = { phase: "Succeeded", containerStatuses: [] };
  api.pods.set(unknown.metadata.name, unknown);

  const pending = await podManager.confirmRoomStopped("stop-proof-room", [
    { name: handle.name, uid: originalUid }
  ]);
  assert.equal(pending.terminal, false);
  assert.equal(pending.managedRoomPods, 2);

  const deletions = api.calls.filter(call =>
    call.method === "DELETE" &&
    !decodeURIComponent(
      new URL(call.path, "https://kubernetes.invalid").pathname.split("/").at(-1)
    ).startsWith("bot-intent-")
  );
  assert.deepEqual(
    new Set(deletions.map(call => call.body.preconditions.uid)),
    new Set([replacement.metadata.uid, unknown.metadata.uid])
  );
  assert.equal(api.pods.size, 0);
  assert.equal((await podManager.confirmRoomStopped("stop-proof-room", [{ name: handle.name, uid: originalUid }])).terminal, true);
});

test("an accepted room DELETE remains pending while the same Pod is observable", async () => {
  class DelayedRoomDeleteApi extends FakeApi {
    async request(method, requestPath, body) {
      const name = decodeURIComponent(
        new URL(requestPath, "https://kubernetes.invalid").pathname.split("/").at(-1)
      );
      if (method === "DELETE" && this.pods.get(name)?.metadata?.labels?.app === "bot-runner") {
        this.calls.push({ method, path: requestPath, body });
        return { status: "Success" };
      }
      return super.request(method, requestPath, body);
    }
  }

  const api = new DelayedRoomDeleteApi();
  const podManager = manager(api);
  const handle = podManager.create("delayed-room-stop", generation);
  await new Promise(resolve => handle.once("spawn", resolve));

  const first = await podManager.confirmRoomStopped("delayed-room-stop", [
    { name: handle.name, uid: handle.podUid }
  ]);
  const second = await podManager.confirmRoomStopped("delayed-room-stop", [
    { name: handle.name, uid: handle.podUid }
  ]);
  assert.equal(first.terminal, false);
  assert.equal(second.terminal, false);
  assert.equal(second.managedRoomPods, 1);

  api.pods.delete(handle.name);
  assert.equal((await podManager.confirmRoomStopped("delayed-room-stop", [{ name: handle.name, uid: handle.podUid }])).terminal, true);
});

test("room stop rejects incomplete or paginated Kubernetes LIST evidence", async () => {
  class IncompleteRoomListApi extends FakeApi {
    constructor(metadata) {
      super();
      this.metadata = metadata;
    }

    async request(method, requestPath, body) {
      if (method === "GET" && new URL(requestPath, "https://kubernetes.invalid").pathname.endsWith("/pods")) {
        this.calls.push({ method, path: requestPath, body });
        return { apiVersion: "v1", kind: "PodList", metadata: this.metadata, items: [] };
      }
      return super.request(method, requestPath, body);
    }
  }

  for (const metadata of [undefined, {}, { resourceVersion: "" }, { resourceVersion: "1", continue: "next" }, { resourceVersion: "1", remainingItemCount: 1 }]) {
    await assert.rejects(
      manager(new IncompleteRoomListApi(metadata)).confirmRoomStopped("incomplete-room"),
      /runner_pod_list_incomplete/
    );
  }
});

test("intent and fence guards have an exact inert non-executable contract", async () => {
  const api = new FakeApi();
  const podManager = manager(api);
  const identity = podManager.identity("guard-shape-room", generation);
  const intentDocument = podManager.guardPodDocument(identity, "intent");
  const intent = await api.request("POST", podManager.createPath(), intentDocument);
  const intentRecord = podManager.guardRecordFromPod(intent, "intent");

  assert.equal(intentRecord.state, "unarmed");
  assert.equal(intent.metadata.name, `bot-intent-${identity.name}`);
  assert.equal(JSON.stringify(intent).includes("guard-shape-room"), false);
  assert.equal(intent.spec.serviceAccountName, "bot-runner-guard");
  assert.equal(intent.spec.automountServiceAccountToken, false);
  assert.deepEqual(intent.spec.imagePullSecrets, []);
  assert.equal(intent.spec.schedulerName, "yenhubs-guard-scheduler");
  assert.deepEqual(intent.spec.schedulingGates, [{ name: "yenhubs.org/guard" }]);
  assert.equal(
    intent.spec.containers[0].image,
    `registry.invalid/yenhubs/non-executable-guard@sha256:${"0".repeat(64)}`
  );
  assert.equal(intent.spec.containers[0].imagePullPolicy, "Never");
  assert.deepEqual(intent.spec.containers[0].command, ["/bin/false"]);
  assert.deepEqual(intent.spec.containers[0].env, []);
  assert.deepEqual(intent.spec.containers[0].resources, {});

  const fence = await api.request("POST", podManager.createPath(), podManager.guardPodDocument(identity, "fence"));
  assert.equal(podManager.guardRecordFromPod(fence, "fence").name, identity.name);
  assert.equal(fence.metadata.name, identity.name);
});

test("a terminating or terminal fence is never accepted as durable stop proof", async () => {
  for (const mutate of [
    pod => { pod.metadata.deletionTimestamp = "2026-07-19T00:00:00.000Z"; },
    pod => { pod.metadata.deletionGracePeriodSeconds = 0; },
    pod => { pod.status.phase = "Succeeded"; },
    pod => { pod.status.phase = "Failed"; }
  ]) {
    const api = new FakeApi();
    const podManager = manager(api);
    const identity = podManager.identity("invalid-fence-room", generation);
    const fence = await api.request(
      "POST",
      podManager.createPath(),
      podManager.guardPodDocument(identity, "fence")
    );
    mutate(fence);
    api.pods.set(identity.name, fence);
    await assert.rejects(
      podManager.confirmRoomStopped("invalid-fence-room", [{ name: identity.name, uid: null }]),
      /runner_guard_contract_invalid/
    );
  }

  const api = new FakeApi();
  const podManager = manager(api);
  const identity = podManager.identity("phase-not-assigned-fence-room", generation);
  const fence = await api.request(
    "POST",
    podManager.createPath(),
    podManager.guardPodDocument(identity, "fence")
  );
  delete fence.status;
  assert.equal(
    podManager.guardRecordFromPod(fence, "fence").name,
    identity.name,
    "the exact scheduling gate and fake scheduler are inert before phase assignment"
  );
});

test("startup deletes unarmed intents by UID and a delayed CAS arm cannot recreate them", async () => {
  const api = new FakeApi();
  const podManager = manager(api);
  const identity = podManager.identity("unarmed-restart-room", generation);
  const intent = await api.request(
    "POST",
    podManager.createPath(),
    podManager.guardPodDocument(identity, "intent")
  );
  const oldResourceVersion = intent.metadata.resourceVersion;

  await podManager.cleanupOrphans();
  assert.equal(api.pods.size, 0);
  await assert.rejects(
    api.request(
      "PATCH",
      podManager.podsPath(`/${intent.metadata.name}`),
      [
        { op: "test", path: "/metadata/uid", value: intent.metadata.uid },
        { op: "test", path: "/metadata/resourceVersion", value: oldResourceVersion },
        {
          op: "replace",
          path: "/metadata/annotations/yenhubs.org~1intent-state",
          value: "armed"
        }
      ],
      { contentType: "application/json-patch+json" }
    ),
    error => error?.status === 404
  );
});

test("full namespace inventory rejects an unknown or malformed guard without deleting it", async () => {
  const api = new FakeApi();
  api.pods.set("unknown-guard", {
    apiVersion: "v1",
    kind: "Pod",
    metadata: {
      name: "unknown-guard",
      namespace: "hcce-bot-runners",
      uid: "ffffffff-ffff-4fff-8fff-ffffffffffff",
      resourceVersion: "1",
      labels: { app: "unexpected" }
    },
    spec: {}
  });
  const podManager = manager(api);

  await assert.rejects(podManager.cleanupOrphans(), /runner_managed_pod_type_invalid/);
  await assert.rejects(podManager.confirmRoomStopped("unknown-room"), /runner_managed_pod_type_invalid/);
  assert.equal(api.pods.has("unknown-guard"), true);
  assert.equal(api.calls.some(call => call.method === "DELETE"), false);
});

test("guard inventory reserves twenty BestEffort slots before authorizing a runner", async () => {
  const api = new FakeApi();
  const podManager = manager(api, { maxActiveRooms: 10 });
  const warnings = [];
  podManager.on("guard-capacity-warning", snapshot => warnings.push(snapshot));
  for (let index = 0; index < 80; index += 1) {
    const prefix = index.toString(16).padStart(8, "0");
    const guardGeneration = `${prefix}-0000-4000-8000-${prefix.padEnd(12, "0")}`;
    const identity = podManager.identity(`guard-capacity-${index}`, guardGeneration);
    await api.request("POST", podManager.createPath(), podManager.guardPodDocument(identity, "fence"));
  }
  api.calls.length = 0;

  const handle = podManager.create("capacity-reserved-room", generation);
  await new Promise(resolve => handle.once("error", resolve));
  assert.equal(handle.finished, true);
  assert.equal(MAX_GUARD_PODS, 100);
  assert.equal(MIN_GUARD_FENCE_RESERVE, 20);
  assert.equal(MAX_GUARD_START_COUNT, 80);
  assert.deepEqual(podManager.guardCapacitySnapshot(), {
    observed: true,
    intents: 0,
    fences: 80,
    total: 80,
    warning: true,
    warning_threshold: 60,
    start_limit: 80,
    reserve: 20,
    quota: 100
  });
  assert.equal(warnings.length, 1);
  assert.equal(
    api.calls.some(call => call.method === "POST"),
    false,
    "no intent or runner may consume the fence reserve"
  );
});

test("healthy runner cycles leave zero durable guards and capacity warning starts at sixty", async () => {
  const api = new FakeApi();
  const podManager = manager(api);
  for (let index = 0; index < 3; index += 1) {
    const serial = String(index + 10).padStart(12, "0");
    const cycleGeneration = `33333333-3333-4333-8333-${serial}`;
    const handle = podManager.create(`healthy-cycle-${index}`, cycleGeneration);
    await new Promise(resolve => handle.once("spawn", resolve));
    const exit = new Promise(resolve => handle.once("exit", resolve));
    handle.kill("SIGTERM");
    await podManager.reconcile();
    await exit;
  }
  await podManager.completeManagedInventory();
  assert.deepEqual(podManager.guardCapacitySnapshot(), {
    observed: true,
    intents: 0,
    fences: 0,
    total: 0,
    warning: false,
    warning_threshold: 60,
    start_limit: 80,
    reserve: 20,
    quota: 100
  });

  const warnings = [];
  podManager.on("guard-capacity-warning", snapshot => warnings.push(snapshot));
  for (let index = 0; index < 60; index += 1) {
    const serial = String(index + 200).padStart(12, "0");
    const identity = podManager.identity(
      `warning-capacity-${index}`,
      `55555555-5555-4555-8555-${serial}`
    );
    await api.request("POST", podManager.createPath(), podManager.guardPodDocument(identity, "fence"));
  }
  await podManager.completeManagedInventory();
  assert.equal(podManager.guardCapacitySnapshot().total, 60);
  assert.equal(podManager.guardCapacitySnapshot().warning, true);
  assert.equal(warnings.length, 1);
  await podManager.completeManagedInventory();
  assert.equal(warnings.length, 1, "a stable warning inventory does not spam duplicate transitions");
});

test("a reservation lost after persisting an unarmed intent deletes it and finishes safely", async () => {
  class CapacityRaceApi extends FakeApi {
    async request(method, requestPath, body, options) {
      const url = new URL(requestPath, "https://kubernetes.invalid");
      const name = decodeURIComponent(url.pathname.split("/").at(-1));
      if (
        method === "DELETE" &&
        this.pods.get(name)?.metadata?.labels?.app === "bot-runner-intent" &&
        !this.allowIntentDelete
      ) {
        this.calls.push({ method, path: requestPath, body, options });
        return { status: "Success" };
      }
      if (method === "GET" && url.pathname.endsWith("/pods")) {
        this.fullLists = (this.fullLists || 0) + 1;
        if (this.fullLists === 2) {
          for (let index = 0; index < 80; index += 1) {
            const serial = String(index + 100).padStart(12, "0");
            const identity = this.documentFactory.identity(
              `capacity-race-${index}`,
              `33333333-3333-4333-8333-${serial}`
            );
            const fence = this.documentFactory.guardPodDocument(identity, "fence");
            fence.metadata.uid = this.nextUid();
            fence.metadata.resourceVersion = String(++this.resourceVersion);
            fence.status = { phase: "Pending", containerStatuses: [] };
            this.pods.set(fence.metadata.name, fence);
          }
        }
      }
      return super.request(method, requestPath, body, options);
    }
  }

  const api = new CapacityRaceApi();
  const podManager = manager(api);
  api.documentFactory = podManager;
  const handle = podManager.create("capacity-race-room", generation);
  const error = new Promise(resolve => handle.once("error", resolve));
  const exit = new Promise(resolve => handle.once("exit", resolve));

  assert.match((await error).message, /runner_guard_capacity_reserved/);
  assert.equal(handle.finished, false, "an accepted DELETE is not exact absence proof");
  assert.equal(api.pods.has(`bot-intent-${handle.name}`), true);
  api.allowIntentDelete = true;
  await podManager.reconcile();
  assert.equal(await exit, 137);
  assert.equal(handle.finished, true);
  assert.equal(api.pods.has(`bot-intent-${handle.name}`), false);
  assert.equal(api.calls.some(call =>
    call.method === "POST" && call.body?.metadata?.labels?.app === "bot-runner"
  ), false);
});

test("a runner POST conflict with an exact fence leaves no armed intent after reconciliation", async () => {
  const api = new FakeApi();
  const podManager = manager(api);
  const identity = podManager.identity("preexisting-fence-room", generation);
  await api.request("POST", podManager.createPath(), podManager.guardPodDocument(identity, "fence"));

  const handle = podManager.create("preexisting-fence-room", generation);
  const error = new Promise(resolve => handle.once("error", resolve));
  const exit = new Promise(resolve => handle.once("exit", resolve));
  assert.match((await error).message, /runner_pod_create_fenced/);
  assert.equal(await exit, 1);
  assert.equal(handle.finished, true);
  assert.equal(api.pods.has(`bot-intent-${handle.name}`), true);

  await podManager.reconcile();
  assert.equal(api.pods.has(`bot-intent-${handle.name}`), false);
  assert.equal(api.pods.get(handle.name).metadata.labels.app, "bot-runner-fence");
});

test("a complete LIST clears local intent state after a lost successful DELETE response", async () => {
  class LostIntentDeleteResponseApi extends FakeApi {
    async request(method, requestPath, body, options) {
      const name = decodeURIComponent(
        new URL(requestPath, "https://kubernetes.invalid").pathname.split("/").at(-1)
      );
      if (
        method === "DELETE" &&
        this.pods.get(name)?.metadata?.labels?.app === "bot-runner-intent" &&
        !this.lostDeleteResponse
      ) {
        await super.request(method, requestPath, body, options);
        this.lostDeleteResponse = true;
        this.failIntentReadOnce = true;
        throw new Error("kubernetes_request_timeout");
      }
      if (method === "GET" && this.failIntentReadOnce && name.startsWith("bot-intent-")) {
        this.calls.push({ method, path: requestPath, body, options });
        this.failIntentReadOnce = false;
        throw new Error("kubernetes_request_timeout");
      }
      return super.request(method, requestPath, body, options);
    }
  }

  const api = new LostIntentDeleteResponseApi();
  const podManager = manager(api);
  podManager.on("fatal", () => {});
  const handle = podManager.create("lost-intent-delete-room", generation);
  handle.on("error", () => {});
  await new Promise(resolve => handle.once("spawn", resolve));
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(api.pods.has(`bot-intent-${handle.name}`), false);
  assert.equal(podManager.createIntents.has(handle.name), true);

  await podManager.reconcile();
  assert.equal(podManager.createIntents.has(handle.name), false);
  assert.equal(api.pods.get(handle.name).metadata.labels.app, "bot-runner");
});

test("a permanent fence remains valid across a runner image repository update", async () => {
  const api = new FakeApi();
  const oldManager = manager(api, {
    runnerImage: `old-registry.invalid/team/bot-runner@sha256:${"b".repeat(64)}`
  });
  const identity = oldManager.identity("future-update-room", generation);
  const fence = await api.request(
    "POST",
    oldManager.createPath(),
    oldManager.guardPodDocument(identity, "fence")
  );
  api.calls.length = 0;

  const updatedManager = manager(api, {
    runnerImage: `new-registry.invalid/other/bot-runner@sha256:${"c".repeat(64)}`
  });
  await updatedManager.cleanupOrphans();
  assert.equal(api.pods.get(identity.name).metadata.uid, fence.metadata.uid);
  assert.equal(api.calls.some(call => call.method === "DELETE"), false);
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
