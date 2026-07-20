const assert = require("node:assert/strict");
const crypto = require("node:crypto");
const test = require("node:test");

const {
  CUTOVER_JOURNAL_DOMAIN,
  CUTOVER_JOURNAL_FINALIZER,
  advanceCutoverJournalTransition,
  canonicalJson,
  classifyCutoverJournalPrefix,
  createCutoverJournal,
  cutoverJournalConfigMap,
  liveDeploymentMatchesNormalizedTarget,
  liveDeploymentMatchesJournalTarget,
  liveObjectIsUnencumbered,
  liveResourceMatchesTarget,
  parseExactCutoverJournalConfigMap,
  parseStructurallyExactCutoverJournalConfigMap,
  sha256Canonical,
  verifyCutoverJournal
} = require("./cutover-journal");

const KEY = Buffer.alloc(32, 7);
const CONTEXT = "do-ams3-yenhubs";
const NAMESPACE = "hcce";
const NAMESPACE_UID = "namespace-uid";
const ISSUED_AT = "2026-07-19T12:00:00.000Z";
const NOW = Date.parse("2026-07-20T12:00:00.000Z");
const OPERATION_ID = "12345678-1234-4234-8234-123456789abc";
const AUTHORIZATION = {
  schemaVersion: 1,
  profileId: "yenhubs-process-local-credential-rotation-v1",
  verifiedAud065ReportSha256: "c".repeat(64),
  hmacSha256: "d".repeat(64)
};

function resource(apiVersion, kind, name, spec, namespace = undefined) {
  return {
    apiVersion,
    kind,
    metadata: {
      name,
      ...(namespace ? { namespace } : {}),
      labels: { "yenhubs.org/contract": "aud078" },
      annotations: { "yenhubs.org/protocol": "durable-v2" }
    },
    spec
  };
}

const TARGET_POLICY = resource(
  "admissionregistration.k8s.io/v1",
  "ValidatingAdmissionPolicy",
  "bot-orchestrator-fence-protocol.yenhubs.org",
  { failurePolicy: "Fail", validations: [{ expression: "false" }] }
);
const TARGET_JOURNAL_POLICY = resource(
  "admissionregistration.k8s.io/v1",
  "ValidatingAdmissionPolicy",
  "yenhubs-runner-cutover-journal-v2",
  { failurePolicy: "Fail", validations: [{ expression: "false" }] }
);
const TARGET_JOURNAL_BINDING = resource(
  "admissionregistration.k8s.io/v1",
  "ValidatingAdmissionPolicyBinding",
  "yenhubs-runner-cutover-journal-v2",
  { policyName: "yenhubs-runner-cutover-journal-v2", validationActions: ["Deny"] }
);
const TARGET_BINDING = resource(
  "admissionregistration.k8s.io/v1",
  "ValidatingAdmissionPolicyBinding",
  "bot-orchestrator-fence-protocol.yenhubs.org",
  { policyName: "bot-orchestrator-fence-protocol.yenhubs.org", validationActions: ["Deny"] }
);
const TARGET_DEPLOYMENT = resource(
  "apps/v1",
  "Deployment",
  "bot-orchestrator",
  { replicas: 0, strategy: { type: "Recreate" } },
  NAMESPACE
);
const TARGET_HASHES = {
  journalPolicy: sha256Canonical(TARGET_JOURNAL_POLICY),
  journalBinding: sha256Canonical(TARGET_JOURNAL_BINDING),
  parentPolicy: sha256Canonical(TARGET_POLICY),
  parentBinding: sha256Canonical(TARGET_BINDING),
  parentDeployment: sha256Canonical(TARGET_DEPLOYMENT)
};
const MANIFEST_SHA256 = "a".repeat(64);

function baselineDeployment() {
  return {
    apiVersion: "apps/v1",
    kind: "Deployment",
    metadata: {
      name: "bot-orchestrator",
      namespace: NAMESPACE,
      uid: "deployment-uid",
      resourceVersion: "100"
    },
    spec: { replicas: 1 }
  };
}

function makeJournal(mode = "pristine-cutover") {
  return createCutoverJournal({
    mode,
    operationId: OPERATION_ID,
    authorization: mode === "clean-install" ? null : AUTHORIZATION,
    expectedKubeContext: CONTEXT,
    namespace: NAMESPACE,
    namespaceUid: NAMESPACE_UID,
    baselineDeployment: mode === "clean-install"
      ? null
      : { name: "bot-orchestrator", uid: "deployment-uid", resourceVersion: "100" },
    manifestSha256: MANIFEST_SHA256,
    targetHashes: TARGET_HASHES,
    issuedAt: ISSUED_AT
  }, KEY);
}

function verification(overrides = {}) {
  return {
    key: KEY,
    expectedKubeContext: CONTEXT,
    namespace: NAMESPACE,
    namespaceUid: NAMESPACE_UID,
    manifestSha256: MANIFEST_SHA256,
    targetHashes: TARGET_HASHES,
    now: () => NOW,
    ...overrides
  };
}

function addServerIdentity(value, uid, resourceVersion) {
  const live = structuredClone(value);
  live.metadata.uid = uid;
  live.metadata.resourceVersion = resourceVersion;
  live.metadata.creationTimestamp = "2026-07-19T12:00:00Z";
  return live;
}

test("journal uses one domain-separated HMAC and remains valid after the receipt freshness window", () => {
  const journal = makeJournal();
  const unsigned = structuredClone(journal);
  delete unsigned.hmacSha256;
  const independentlyComputed = crypto.createHmac("sha256", KEY)
    .update(CUTOVER_JOURNAL_DOMAIN, "utf8")
    .update(canonicalJson(unsigned), "utf8")
    .digest("hex");
  assert.equal(journal.hmacSha256, independentlyComputed);
  assert.equal(verifyCutoverJournal(journal, verification()), journal);

  const tampered = structuredClone(journal);
  tampered.baselineDeployment.resourceVersion = "101";
  assert.throws(() => verifyCutoverJournal(tampered, verification()), /hmac_invalid/);
  assert.throws(() => verifyCutoverJournal(journal, verification({ key: crypto.randomBytes(32) })), /hmac_invalid/);
  assert.throws(() => verifyCutoverJournal(journal, verification({ expectedKubeContext: "other" })), /binding_invalid/);
  assert.throws(() => verifyCutoverJournal(journal, verification({
    targetHashes: { ...TARGET_HASHES, parentPolicy: "b".repeat(64) }
  })), /binding_invalid/);

  const future = createCutoverJournal({
    mode: unsigned.mode,
    operationId: unsigned.operationId,
    authorization: AUTHORIZATION,
    expectedKubeContext: unsigned.expectedKubeContext,
    namespace: unsigned.namespace.name,
    namespaceUid: unsigned.namespace.uid,
    baselineDeployment: unsigned.baselineDeployment,
    manifestSha256: unsigned.manifestSha256,
    targetHashes: unsigned.targetHashes,
    issuedAt: "2026-07-20T12:01:00.000Z"
  }, KEY);
  assert.throws(() => verifyCutoverJournal(future, verification()), /issued_in_future/);

  const differentReceipt = createCutoverJournal({
    mode: unsigned.mode,
    operationId: unsigned.operationId,
    authorization: { ...AUTHORIZATION, verifiedAud065ReportSha256: "e".repeat(64) },
    expectedKubeContext: unsigned.expectedKubeContext,
    namespace: unsigned.namespace.name,
    namespaceUid: unsigned.namespace.uid,
    baselineDeployment: unsigned.baselineDeployment,
    manifestSha256: unsigned.manifestSha256,
    targetHashes: unsigned.targetHashes,
    issuedAt: unsigned.issuedAt
  }, KEY);
  assert.notEqual(differentReceipt.authorizationSha256, journal.authorizationSha256);
  assert.notEqual(differentReceipt.hmacSha256, journal.hmacSha256);

  assert.throws(() => verifyCutoverJournal(journal, verification({
    namespaceUid: "recreated-namespace-uid"
  })), /binding_invalid/, "a journal cannot be replayed into a recreated Namespace");
  for (const field of Object.keys(TARGET_HASHES)) {
    assert.throws(() => verifyCutoverJournal(journal, verification({
      targetHashes: { ...TARGET_HASHES, [field]: "f".repeat(64) }
    })), /binding_invalid/);
  }
  assert.throws(() => verifyCutoverJournal(journal, verification({
    manifestSha256: "f".repeat(64)
  })), /binding_invalid/);
  for (const field of ["authorizationSha256", "operationId"]) {
    const changed = structuredClone(journal);
    changed[field] = field === "operationId"
      ? "87654321-4321-4321-8321-cba987654321"
      : "f".repeat(64);
    assert.throws(() => verifyCutoverJournal(changed, verification()), /hmac_invalid/);
  }
});

test("journal ConfigMap is immutable, canonical, namespaced, and rejects user metadata", () => {
  const journal = makeJournal();
  const desired = cutoverJournalConfigMap(journal);
  const live = addServerIdentity(desired, "configmap-uid", "5");
  assert.deepEqual(live.metadata.finalizers, [CUTOVER_JOURNAL_FINALIZER]);
  assert.deepEqual(parseExactCutoverJournalConfigMap(live, verification()), journal);

  for (const mutate of [
    value => { delete value.metadata.uid; },
    value => { delete value.metadata.resourceVersion; },
    value => { value.metadata.generateName = "forged-"; },
    value => { value.metadata.deletionTimestamp = "2026-07-19T12:01:00Z"; },
    value => { value.metadata.deletionGracePeriodSeconds = 0; },
    value => { value.immutable = false; },
    value => { value.metadata.labels = { forged: "true" }; },
    value => { value.metadata.annotations = { forged: "true" }; },
    value => { value.metadata.ownerReferences = [{ uid: "owner" }]; },
    value => { delete value.metadata.finalizers; },
    value => { value.metadata.finalizers.push("unapproved.example/finalizer"); },
    value => { value.data["extra"] = "value"; },
    value => { value.data["journal.json"] = `${value.data["journal.json"]}\n`; }
  ]) {
    const changed = structuredClone(live);
    mutate(changed);
    assert.throws(() => parseExactCutoverJournalConfigMap(changed, verification()));
  }
});

test("durable journal parsing is update-friendly but remains bound to the active Namespace UID", () => {
  const journal = makeJournal();
  const live = addServerIdentity(cutoverJournalConfigMap(journal), "configmap-uid", "5");
  const durableVerification = {
    namespace: NAMESPACE,
    namespaceUid: NAMESPACE_UID,
    allowFutureIssuedAt: true,
    now: () => Date.parse("2025-01-01T00:00:00.000Z")
  };
  assert.deepEqual(
    parseStructurallyExactCutoverJournalConfigMap(live, durableVerification),
    journal,
    "an old first-cutover journal remains valid after context aliases, manifests and image hashes change"
  );
  assert.throws(
    () => parseStructurallyExactCutoverJournalConfigMap(live, {
      ...durableVerification,
      namespaceUid: "recreated-namespace-uid"
    }),
    /binding_invalid/
  );
  const malformed = structuredClone(live);
  const malformedJournal = JSON.parse(malformed.data["journal.json"]);
  malformedJournal.targetHashes.parentDeployment = "not-a-sha256";
  malformed.data["journal.json"] = canonicalJson(malformedJournal);
  assert.throws(
    () => parseStructurallyExactCutoverJournalConfigMap(malformed, durableVerification),
    /contract_invalid/
  );
});

test("only exact P0 through P6 prefixes are accepted", () => {
  const journal = makeJournal();
  const baseline = baselineDeployment();
  const policy = addServerIdentity(TARGET_POLICY, "policy-uid", "10");
  const binding = addServerIdentity(TARGET_BINDING, "binding-uid", "20");
  const journalPolicy = addServerIdentity(TARGET_JOURNAL_POLICY, "journal-policy-uid", "6");
  const journalBinding = addServerIdentity(TARGET_JOURNAL_BINDING, "journal-binding-uid", "7");
  const target = addServerIdentity(TARGET_DEPLOYMENT, "deployment-uid", "101");
  const classify = overrides => classifyCutoverJournalPrefix({
    journal,
    journalPolicy: null,
    journalBinding: null,
    policy: null,
    binding: null,
    deployment: baseline,
    targetJournalPolicy: TARGET_JOURNAL_POLICY,
    targetJournalBinding: TARGET_JOURNAL_BINDING,
    targetPolicy: TARGET_POLICY,
    targetBinding: TARGET_BINDING,
    targetDeployment: TARGET_DEPLOYMENT,
    ...overrides
  });
  assert.equal(classify({}), "P0");
  assert.equal(classify({ journalPolicy }), "P1");
  assert.equal(classify({ journalPolicy, journalBinding }), "P2");
  assert.equal(classify({ journalPolicy, journalBinding, policy }), "P3");
  assert.equal(classify({ journalPolicy, journalBinding, policy, binding }), "P4");
  assert.equal(classify({ journalPolicy, journalBinding, policy, binding, deployment: target }), "P5");
  assert.equal(classify({
    journalPolicy,
    journalBinding,
    policy,
    binding,
    deployment: target,
    journalFenceObserved: true,
    parentFenceObserved: true,
    parentQuiesced: true
  }), "P6");

  assert.throws(() => classify({ journalBinding }), /non_prefix/);
  assert.throws(() => classify({ policy }), /non_prefix/);
  assert.throws(() => classify({ journalPolicy, journalBinding, binding }), /non_prefix/);
  assert.throws(() => classify({
    journalPolicy, journalBinding, policy, deployment: target
  }), /non_prefix/);
  assert.throws(() => classify({
    journalPolicy, journalBinding, policy, binding, deployment: null
  }), /deployment_not_exact/);
  const broadened = structuredClone(policy);
  broadened.spec.validations[0].expression = "true";
  assert.throws(() => classify({
    journalPolicy, journalBinding, policy: broadened
  }), /policy_not_exact/);
  for (const mutate of [
    value => { value.metadata.deletionTimestamp = "2026-07-19T12:01:00Z"; },
    value => { value.metadata.deletionGracePeriodSeconds = 0; },
    value => { value.metadata.finalizers = ["keep"]; },
    value => { value.metadata.ownerReferences = [{ uid: "owner" }]; },
    value => { value.metadata.generateName = "forged-"; },
    value => { delete value.metadata.uid; },
    value => { delete value.metadata.resourceVersion; }
  ]) {
    const terminating = structuredClone(policy);
    mutate(terminating);
    assert.throws(() => classify({
      journalPolicy, journalBinding, policy: terminating
    }), /policy_not_exact/);
  }
  const terminatingTarget = structuredClone(target);
  terminatingTarget.metadata.deletionTimestamp = "2026-07-19T12:01:00Z";
  assert.throws(() => classify({
    journalPolicy, journalBinding, policy, binding, deployment: terminatingTarget
  }), /deployment_not_exact/);
  const replacedBaseline = baselineDeployment();
  replacedBaseline.metadata.uid = "replacement-uid";
  assert.throws(() => classify({ deployment: replacedBaseline }), /non_prefix/);

  const recreatedTarget = structuredClone(target);
  recreatedTarget.metadata.uid = "recreated-deployment-uid";
  assert.equal(liveDeploymentMatchesNormalizedTarget(recreatedTarget, TARGET_DEPLOYMENT), true);
  assert.equal(liveDeploymentMatchesJournalTarget(recreatedTarget, TARGET_DEPLOYMENT, journal), false);
  assert.throws(() => classify({
    journalPolicy,
    journalBinding,
    policy,
    binding,
    deployment: recreatedTarget
  }), /deployment_not_exact/);
  assert.equal(
    liveDeploymentMatchesJournalTarget(recreatedTarget, TARGET_DEPLOYMENT, makeJournal("clean-install")),
    true,
    "a clean CREATE has no historical Deployment UID to preserve"
  );
});

test("every durable fence object rejects terminating or encumbered metadata", () => {
  const objects = [
    addServerIdentity(TARGET_JOURNAL_POLICY, "journal-policy-uid", "1"),
    addServerIdentity(TARGET_JOURNAL_BINDING, "journal-binding-uid", "2"),
    addServerIdentity(TARGET_POLICY, "parent-policy-uid", "3"),
    addServerIdentity(TARGET_BINDING, "parent-binding-uid", "4"),
    addServerIdentity(TARGET_DEPLOYMENT, "deployment-uid", "5")
  ];
  for (const [index, object] of objects.entries()) {
    assert.equal(liveObjectIsUnencumbered(object), true, `durable object ${index}`);
    for (const mutate of [
      value => { value.metadata.deletionTimestamp = ISSUED_AT; },
      value => { value.metadata.deletionGracePeriodSeconds = 0; },
      value => { value.metadata.generateName = "replacement-"; },
      value => { value.metadata.ownerReferences = [{ uid: "owner" }]; },
      value => { value.metadata.finalizers = ["blocked.example/finalizer"]; },
      value => { delete value.metadata.uid; },
      value => { delete value.metadata.resourceVersion; }
    ]) {
      const changed = structuredClone(object);
      mutate(changed);
      assert.equal(liveObjectIsUnencumbered(changed), false, `durable object ${index}`);
    }
  }
});

function transitionHarness({ mode = "pristine-cutover", lostResponse = null, hardFailure = null } = {}) {
  const journal = makeJournal(mode);
  const desiredJournal = cutoverJournalConfigMap(journal);
  const state = {
    namespaceUid: NAMESPACE_UID,
    journalConfigMap: null,
    journalPolicy: null,
    journalBinding: null,
    policy: null,
    binding: null,
    deployment: mode === "clean-install" ? null : baselineDeployment(),
    parentFenceObserved: false,
    journalFenceObserved: false,
    parentQuiesced: false
  };
  const calls = [];
  const mutate = (name, action) => {
    calls.push(name);
    if (hardFailure === name) throw new Error("409 Conflict");
    action();
    if (lostResponse === name) throw new Error("transport closed after server commit");
  };
  const isJournalExact = value => {
    try {
      return parseExactCutoverJournalConfigMap(value, verification()).operationId ===
        journal.operationId;
    } catch (_error) {
      return false;
    }
  };
  return {
    state,
    calls,
    options: {
      journal,
      journalConfigMap: desiredJournal,
      targetPolicy: TARGET_POLICY,
      targetJournalPolicy: TARGET_JOURNAL_POLICY,
      targetJournalBinding: TARGET_JOURNAL_BINDING,
      targetBinding: TARGET_BINDING,
      targetDeployment: TARGET_DEPLOYMENT,
      readState: async () => structuredClone(state),
      validateState: async observed => {
        if (observed.namespaceUid !== NAMESPACE_UID) {
          throw new Error("runner_cutover_journal_namespace_replaced");
        }
      },
      isJournalExact,
      createJournal: async value => mutate("journal", () => {
        state.journalConfigMap = addServerIdentity(value, "journal-uid", "1");
      }),
      createJournalPolicy: async value => mutate("journal-policy", () => {
        state.journalPolicy = addServerIdentity(value, "journal-policy-uid", "2");
      }),
      waitJournalPolicyObserved: async () => {},
      createJournalBinding: async value => mutate("journal-binding", () => {
        state.journalBinding = addServerIdentity(value, "journal-binding-uid", "3");
      }),
      waitJournalFenceObserved: async () => { state.journalFenceObserved = true; },
      createPolicy: async value => mutate("parent-policy", () => {
        state.policy = addServerIdentity(value, "policy-uid", "4");
      }),
      waitPolicyObserved: async () => {},
      createBinding: async value => mutate("parent-binding", () => {
        state.binding = addServerIdentity(value, "binding-uid", "5");
      }),
      waitParentFenceObserved: async () => { state.parentFenceObserved = true; },
      writeDeployment: async (value, baseline) => mutate("deployment", () => {
        if (mode === "pristine-cutover") {
          assert.equal(baseline.resourceVersion, "100");
        } else {
          assert.equal(baseline, null);
        }
        state.deployment = addServerIdentity(value, "deployment-uid", "101");
      }),
      waitParentQuiesced: async () => { state.parentQuiesced = true; }
    }
  };
}

test("clean and pristine transitions converge after an ambiguous/lost response at every POST or PUT", async () => {
  for (const mode of ["clean-install", "pristine-cutover"]) {
    for (const lostResponse of [
      "journal", "journal-policy", "journal-binding", "parent-policy", "parent-binding", "deployment"
    ]) {
      const harness = transitionHarness({ mode, lostResponse });
      assert.equal(await advanceCutoverJournalTransition(harness.options), "P6");
      assert.equal(harness.calls.filter(value => value === lostResponse).length, 1);
      assert.ok(liveResourceMatchesTarget(harness.state.deployment, TARGET_DEPLOYMENT));
    }
  }
});

test("a crash after every committed prefix resumes without repeating or skipping a mutation", async () => {
  for (const crashAfter of [
    "journal", "journal-policy", "journal-binding", "parent-policy", "parent-binding", "deployment"
  ]) {
    const harness = transitionHarness();
    await assert.rejects(() => advanceCutoverJournalTransition({
      ...harness.options,
      afterStep: async step => {
        if (step === crashAfter) throw new Error(`simulated_crash_after_${step}`);
      }
    }), new RegExp(`simulated_crash_after_${crashAfter}`));
    const beforeResume = [...harness.calls];
    assert.equal(await advanceCutoverJournalTransition(harness.options), "P6");
    assert.equal(harness.calls.filter(value => value === crashAfter).length, 1);
    for (const completed of beforeResume) {
      assert.equal(harness.calls.filter(value => value === completed).length, 1);
    }
  }
});

test("409 or transport failure without an exact live result fails closed", async () => {
  for (const hardFailure of [
    "journal", "journal-policy", "journal-binding", "parent-policy", "parent-binding", "deployment"
  ]) {
    const harness = transitionHarness({ hardFailure });
    await assert.rejects(
      () => advanceCutoverJournalTransition(harness.options),
      /failed_and_live_state_not_exact:409 Conflict/
    );
  }
});

test("missing journal with a staged policy is rejected instead of inferred as progress", async () => {
  const harness = transitionHarness();
  harness.state.journalPolicy = addServerIdentity(TARGET_JOURNAL_POLICY, "journal-policy-uid", "2");
  await assert.rejects(
    () => advanceCutoverJournalTransition(harness.options),
    /journal_missing_after_staged_mutation/
  );
});

test("server-normalized Deployment defaults and one controller revision annotation converge exactly", () => {
  const normalized = structuredClone(TARGET_DEPLOYMENT);
  normalized.spec.progressDeadlineSeconds = 600;
  normalized.spec.revisionHistoryLimit = 10;
  normalized.spec.template = {
    metadata: { labels: { app: "bot-orchestrator" } },
    spec: { restartPolicy: "Always", containers: [{ name: "bot-orchestrator", image: "example" }] }
  };
  const live = addServerIdentity(normalized, "deployment-uid", "101");
  live.metadata.annotations["deployment.kubernetes.io/revision"] = "1";
  assert.equal(liveDeploymentMatchesNormalizedTarget(live, normalized), true);
  const invalidRevision = structuredClone(live);
  invalidRevision.metadata.annotations["deployment.kubernetes.io/revision"] = "0";
  assert.equal(liveDeploymentMatchesNormalizedTarget(invalidRevision, normalized), false);
  const additive = structuredClone(live);
  additive.metadata.annotations["unapproved"] = "true";
  assert.equal(liveDeploymentMatchesNormalizedTarget(additive, normalized), false);
});

test("drift or journal deletion at every durable prefix fails before the next mutation", async () => {
  const cases = [
    ["journal", state => { state.journalConfigMap.metadata.deletionTimestamp = ISSUED_AT; }],
    ["journal-policy", state => { state.journalPolicy.spec.failurePolicy = "Ignore"; }],
    ["journal-binding", state => { state.journalConfigMap.metadata.deletionTimestamp = ISSUED_AT; }],
    ["parent-policy", state => { state.policy.spec.failurePolicy = "Ignore"; }],
    ["parent-binding", state => { state.binding.spec.validationActions = ["Warn"]; }],
    ["deployment", state => { state.deployment.metadata.deletionTimestamp = ISSUED_AT; }]
  ];
  for (const [phase, mutate] of cases) {
    const harness = transitionHarness();
    await assert.rejects(() => advanceCutoverJournalTransition({
      ...harness.options,
      afterStep: async step => {
        if (step === phase) {
          mutate(harness.state);
          throw new Error(`crash_with_drift_${phase}`);
        }
      }
    }), new RegExp(`crash_with_drift_${phase}`));
    const mutationsBeforeResume = harness.calls.length;
    await assert.rejects(() => advanceCutoverJournalTransition(harness.options));
    assert.equal(harness.calls.length, mutationsBeforeResume);
  }
});

test("Namespace replacement after every mutation fails closed, including clean Deployment CREATE", async () => {
  for (const mode of ["pristine-cutover", "clean-install"]) {
    for (const phase of [
      "journal", "journal-policy", "journal-binding", "parent-policy", "parent-binding", "deployment"
    ]) {
      const harness = transitionHarness({ mode });
      await assert.rejects(() => advanceCutoverJournalTransition({
        ...harness.options,
        afterStep: async step => {
          if (step === phase) {
            harness.state.namespaceUid = "recreated-namespace-uid";
            throw new Error(`crash_after_namespace_replacement_${phase}`);
          }
        }
      }), new RegExp(`crash_after_namespace_replacement_${phase}`));
      const mutationsBeforeResume = harness.calls.length;
      await assert.rejects(
        () => advanceCutoverJournalTransition(harness.options),
        /namespace_replaced/
      );
      assert.equal(harness.calls.length, mutationsBeforeResume);
    }
  }
});

test("a validly signed replacement operationId is rejected before mutation", async () => {
  const harness = transitionHarness();
  await assert.rejects(() => advanceCutoverJournalTransition({
    ...harness.options,
    afterStep: async step => {
      if (step === "journal") throw new Error("crash_after_journal");
    }
  }), /crash_after_journal/);
  const replacement = createCutoverJournal({
    mode: "pristine-cutover",
    operationId: "87654321-4321-4321-8321-cba987654321",
    authorization: AUTHORIZATION,
    expectedKubeContext: CONTEXT,
    namespace: NAMESPACE,
    namespaceUid: NAMESPACE_UID,
    baselineDeployment: { name: "bot-orchestrator", uid: "deployment-uid", resourceVersion: "100" },
    manifestSha256: MANIFEST_SHA256,
    targetHashes: TARGET_HASHES,
    issuedAt: ISSUED_AT
  }, KEY);
  harness.state.journalConfigMap = addServerIdentity(
    cutoverJournalConfigMap(replacement),
    "replacement-journal-uid",
    "2"
  );
  const calls = harness.calls.length;
  await assert.rejects(() => advanceCutoverJournalTransition(harness.options), /live_object_not_exact/);
  assert.equal(harness.calls.length, calls);
});
