const assert = require("node:assert/strict");
const test = require("node:test");

const {
  GENERATION_LABEL,
  INTENT_STATE_ANNOTATION,
  MANAGED_BY_LABEL,
  MANAGED_BY_VALUE,
  ROOM_KEY_LABEL,
  RUNNER_APP_LABEL,
  RUNNER_NAMESPACE,
  RUNNER_PROTOCOL_LABEL,
  RUNNER_PROTOCOL_VALUE,
  guardPodDocumentForIdentity,
  runnerPodName
} = require("../services/bot-orchestrator/kubernetes-runner-manager");
const {
  completeRunnerNamespaceInventory,
  reconcileRunnerNamespace
} = require("./runner-guard-reconciliation");

const identity = Object.freeze({
  roomKey: "abcabcabcabcabcabcab",
  processGeneration: "11111111-1111-4111-8111-111111111111",
  get name() {
    return runnerPodName(this.roomKey, this.processGeneration);
  }
});

function guard(type, state = "unarmed", uid = `${type}-uid`, resourceVersion = "1") {
  const pod = guardPodDocumentForIdentity(identity, type, RUNNER_NAMESPACE);
  pod.metadata.uid = uid;
  pod.metadata.resourceVersion = resourceVersion;
  if (type === "intent") pod.metadata.annotations[INTENT_STATE_ANNOTATION] = state;
  pod.status = { phase: "Pending" };
  return pod;
}

function runner(uid = "runner-uid", resourceVersion = "1") {
  return {
    apiVersion: "v1",
    kind: "Pod",
    metadata: {
      name: identity.name,
      namespace: RUNNER_NAMESPACE,
      uid,
      resourceVersion,
      labels: {
        app: RUNNER_APP_LABEL,
        [MANAGED_BY_LABEL]: MANAGED_BY_VALUE,
        [RUNNER_PROTOCOL_LABEL]: RUNNER_PROTOCOL_VALUE,
        [ROOM_KEY_LABEL]: identity.roomKey,
        [GENERATION_LABEL]: identity.processGeneration
      }
    },
    spec: { containers: [{ name: "bot-runner" }] },
    status: { phase: "Pending" }
  };
}

function fakeApi(initialPods, { loseFenceCreate = false, conflictAndArm = false } = {}) {
  const pods = new Map(initialPods.map(pod => [pod.metadata.name, structuredClone(pod)]));
  const calls = [];
  let resourceVersion = 10;
  let lost = loseFenceCreate;
  let conflict = conflictAndArm;
  return {
    pods,
    calls,
    async listPods() {
      calls.push(["list"]);
      return {
        apiVersion: "v1",
        kind: "PodList",
        metadata: { resourceVersion: String(resourceVersion++) },
        items: [...pods.values()].map(value => structuredClone(value))
      };
    },
    async getPod(name) {
      calls.push(["get", name]);
      return pods.has(name) ? structuredClone(pods.get(name)) : null;
    },
    async createPod(document) {
      calls.push(["create", document.metadata.name]);
      if (pods.has(document.metadata.name)) {
        const error = new Error("conflict");
        error.status = 409;
        throw error;
      }
      const created = structuredClone(document);
      created.metadata.uid = "fence-uid";
      created.metadata.resourceVersion = String(resourceVersion++);
      created.status = { phase: "Pending" };
      pods.set(created.metadata.name, created);
      if (lost) {
        lost = false;
        throw new Error("lost_success");
      }
      return structuredClone(created);
    },
    async deletePodByUid(pod, options = {}) {
      calls.push(["delete", pod.metadata.name, pod.metadata.uid, options]);
      const current = pods.get(pod.metadata.name);
      if (!current) return;
      if (conflict && pod.metadata.name.startsWith("bot-intent-")) {
        conflict = false;
        current.metadata.annotations[INTENT_STATE_ANNOTATION] = "armed";
        current.metadata.resourceVersion = String(resourceVersion++);
        const error = new Error("resource_version_conflict");
        error.status = 409;
        throw error;
      }
      if (current.metadata.uid !== pod.metadata.uid) throw new Error("uid_mismatch");
      if (
        options.requireResourceVersion &&
        current.metadata.resourceVersion !== pod.metadata.resourceVersion
      ) {
        throw new Error("resource_version_mismatch");
      }
      pods.delete(pod.metadata.name);
    },
    async sleep() {}
  };
}

test("armed intent causally replaces a racing runner with a permanent fence", async () => {
  const api = fakeApi([guard("intent", "armed"), runner()]);
  const result = await reconcileRunnerNamespace(api, { retryDelayMs: 0 });
  assert.equal(result.runners, 0);
  assert.equal(result.intents, 0);
  assert.equal(result.fences, 1);
  assert.equal(api.pods.has(identity.name), true);
  assert.equal(api.pods.get(identity.name).metadata.labels.app, "bot-runner-fence");
  const fenceUid = api.pods.get(identity.name).metadata.uid;
  await assert.rejects(api.createPod(runner("late-runner-uid")), error => error?.status === 409);
  assert.equal(api.pods.get(identity.name).metadata.uid, fenceUid);
  assert.equal(api.pods.get(identity.name).metadata.labels.app, "bot-runner-fence");
  assert.deepEqual(
    api.calls.filter(call => call[0] === "delete").map(call => call.slice(1, 3)),
    [[identity.name, "runner-uid"], [`bot-intent-${identity.name}`, "intent-uid"]]
  );
});

test("a lost successful fence POST is resolved only by the next exact GET", async () => {
  const api = fakeApi([guard("intent", "armed")], { loseFenceCreate: true });
  await reconcileRunnerNamespace(api, { retryDelayMs: 0 });
  assert.equal(api.pods.get(identity.name).metadata.labels.app, "bot-runner-fence");
  const createIndex = api.calls.findIndex(call => call[0] === "create");
  assert.ok(createIndex >= 0);
  assert.ok(api.calls.slice(createIndex + 1).some(call =>
    call[0] === "get" && call[1] === identity.name
  ));
});

test("an unarmed UID+resourceVersion delete loses safely to a concurrent arm", async () => {
  const api = fakeApi([guard("intent")], { conflictAndArm: true });
  await reconcileRunnerNamespace(api, { retryDelayMs: 0 });
  assert.equal(api.pods.get(identity.name).metadata.labels.app, "bot-runner-fence");
  const intentDeletes = api.calls.filter(call =>
    call[0] === "delete" && call[1] === `bot-intent-${identity.name}`
  );
  assert.equal(intentDeletes.length, 2);
  assert.deepEqual(intentDeletes[0][3], { requireResourceVersion: true });
  assert.deepEqual(intentDeletes[1][3], { requireResourceVersion: true });
});

test("when unarmed intent deletion wins, a delayed parent arm PATCH has no target", async () => {
  const api = fakeApi([guard("intent")]);
  await reconcileRunnerNamespace(api, { retryDelayMs: 0 });
  assert.equal(api.pods.has(`bot-intent-${identity.name}`), false);
  const delayedArmPatchCommits = api.pods.has(`bot-intent-${identity.name}`);
  assert.equal(delayedArmPatchCommits, false);
  const deletion = api.calls.find(call => call[0] === "delete");
  assert.deepEqual(deletion[3], { requireResourceVersion: true });
});

test("existing permanent fences survive reconciliation", async () => {
  const api = fakeApi([guard("fence")]);
  const result = await reconcileRunnerNamespace(api, { retryDelayMs: 0 });
  assert.equal(result.fences, 1);
  assert.equal(api.calls.some(call => call[0] === "delete"), false);
});

test("unknown, malformed, paginated, and partial inventories fail closed", async t => {
  const cases = [
    {
      name: "unknown",
      list: {
        kind: "PodList", metadata: { resourceVersion: "1" },
        items: [{ metadata: { name: "unknown", labels: { app: "other" } } }]
      },
      error: /runner_namespace_unknown_pod/
    },
    {
      name: "malformed",
      list: {
        kind: "PodList", metadata: { resourceVersion: "1" },
        items: [{ ...guard("fence"), spec: { containers: [] } }]
      },
      error: /runner_namespace_pod_contract_invalid/
    },
    {
      name: "paginated",
      list: { kind: "PodList", metadata: { resourceVersion: "1", continue: "token" }, items: [] },
      error: /runner_pod_list_incomplete/
    },
    {
      name: "remaining",
      list: {
        kind: "PodList", metadata: { resourceVersion: "1", remainingItemCount: 1 }, items: []
      },
      error: /runner_pod_list_incomplete/
    }
  ];
  for (const fixture of cases) {
    await t.test(fixture.name, async () => {
      let deletes = 0;
      const api = {
        async listPods() { return structuredClone(fixture.list); },
        async deletePodByUid() { deletes += 1; },
        async sleep() {}
      };
      await assert.rejects(
        reconcileRunnerNamespace(api, { maxAttempts: 1, retryDelayMs: 0 }),
        fixture.error
      );
      assert.equal(deletes, 0);
    });
  }
});

test("complete inventory rejects duplicate guard identities", () => {
  const first = guard("fence", "fenced", "one", "1");
  const second = guard("fence", "fenced", "two", "2");
  assert.throws(() => completeRunnerNamespaceInventory({
    kind: "PodList",
    metadata: { resourceVersion: "3" },
    items: [first, second]
  }), /runner_namespace_identity_ambiguous/);
});
