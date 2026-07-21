const assert = require("node:assert/strict");
const crypto = require("node:crypto");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const test = require("node:test");

const {
  PROFILE_CLOUD_COMMIT,
  PROFILE_ID,
  PROFILE_SHA256,
  CLEAN_INSTALL_ANNOTATION,
  CLEAN_INSTALL_VALUE,
  canonicalJson,
  executeCutoverPreflight,
  executeCutoverRevalidation,
  readPrivateCutoverAttestation,
  readPrivateCutoverKey,
  verifyCleanInstallCutoverGate,
  verifyPristineLegacyCutoverGate,
  verifyProcessLocalCutoverAttestation
} = require("./process-local-cutover");

const NOW = Date.parse("2026-07-19T12:00:00.000Z");
const CONTEXT = "do-ams3-yenhubs";
const NAMESPACE = "hcce";

function unsignedAttestation(overrides = {}) {
  return {
    schemaVersion: 1,
    profileId: PROFILE_ID,
    profileSha256: PROFILE_SHA256,
    cloudSourceCommit: PROFILE_CLOUD_COMMIT,
    historicalResourceCount: 42,
    requiredDeploymentCount: 12,
    requiredImagePairCount: 13,
    runnerMode: "process-local",
    expectedKubeContext: CONTEXT,
    namespace: NAMESPACE,
    namespaceUid: "namespace-uid",
    capturedAt: "2026-07-19T11:59:00.000Z",
    verifiedAud065ReportSha256: "a".repeat(64),
    botOrchestratorDeployment: {
      name: "bot-orchestrator",
      uid: "deployment-uid",
      resourceVersion: "100",
      processLocalExact: true
    },
    validator: {
      name: "yenhubs-redacted-rollout-contract",
      status: "passed"
    },
    ...overrides
  };
}

function signedAttestation(key, overrides = {}) {
  const attestation = unsignedAttestation(overrides);
  return {
    ...attestation,
    hmacSha256: crypto.createHmac("sha256", key)
      .update(canonicalJson(attestation), "utf8")
      .digest("hex")
  };
}

function liveNamespace() {
  return {
    apiVersion: "v1",
    kind: "Namespace",
    metadata: { name: NAMESPACE, uid: "namespace-uid", resourceVersion: "50" },
    spec: { finalizers: ["kubernetes"] },
    status: { phase: "Active" }
  };
}

function liveDeployment() {
  return {
    apiVersion: "apps/v1",
    kind: "Deployment",
    metadata: {
      name: "bot-orchestrator",
      namespace: NAMESPACE,
      uid: "deployment-uid",
      resourceVersion: "100",
      annotations: { "cluster-autoscaler.kubernetes.io/safe-to-evict": "true" }
    },
    spec: { replicas: 1 }
  };
}

function exactGateInput(key, overrides = {}) {
  return {
    attestation: signedAttestation(key),
    key,
    namespace: NAMESPACE,
    expectedKubeContext: CONTEXT,
    liveNamespace: liveNamespace(),
    liveDeployment: liveDeployment(),
    runnerNamespace: null,
    isolatedResources: {
      parentServiceAccount: null,
      parentRole: null,
      parentRoleBinding: null,
      runnerAdmissionPolicy: null,
      runnerAdmissionBinding: null,
      runnerProtocolPolicy: null,
      runnerProtocolBinding: null,
      cutoverJournalPolicy: null,
      cutoverJournalBinding: null,
      parentFencePolicy: null,
      parentFenceBinding: null,
      recoveryOperationFencePolicy: null,
      recoveryOperationFenceBinding: null
    },
    parentPodList: {
      apiVersion: "v1",
      kind: "PodList",
      metadata: { resourceVersion: "200" },
      items: [{
        metadata: { name: "reticulum", labels: { app: "reticulum" } },
        spec: { serviceAccountName: "default" }
      }]
    },
    authority: {
      parent: { create: false, delete: false, patch: false },
      runner: { create: false, delete: false, patch: false }
    },
    now: () => NOW,
    ...overrides
  };
}

test("authenticated pristine cutover receipt binds the AUD065 report and exact live identities", () => {
  const key = crypto.randomBytes(32);
  const result = verifyPristineLegacyCutoverGate(exactGateInput(key));
  assert.deepEqual(result, {
    namespaceUid: "namespace-uid",
    deploymentUid: "deployment-uid",
    deploymentResourceVersion: "100",
    parentPodListResourceVersion: "200"
  });
});

test("receipt tampering, wrong keys, stale evidence, context drift, and live ABA fail closed", () => {
  const key = crypto.randomBytes(32);
  const exact = exactGateInput(key);
  const tampered = structuredClone(exact.attestation);
  tampered.verifiedAud065ReportSha256 = "b".repeat(64);
  assert.throws(() => verifyPristineLegacyCutoverGate({ ...exact, attestation: tampered }), /hmac_invalid/);
  assert.throws(() => verifyPristineLegacyCutoverGate({
    ...exact,
    key: crypto.randomBytes(32)
  }), /hmac_invalid/);

  const stale = signedAttestation(key, { capturedAt: "2026-07-19T11:50:00.000Z" });
  assert.throws(() => verifyPristineLegacyCutoverGate({ ...exact, attestation: stale }), /stale/);
  assert.throws(() => verifyPristineLegacyCutoverGate({
    ...exact,
    expectedKubeContext: "other-context"
  }), /contract_invalid/);
  assert.throws(() => verifyPristineLegacyCutoverGate({
    ...exact,
    liveDeployment: {
      ...liveDeployment(),
      metadata: { ...liveDeployment().metadata, resourceVersion: "101" }
    }
  }), /live_binding_changed/);
});

test("the live pristine gate requires legacy annotations, zero control plane, zero candidates, and zero authority", () => {
  const key = crypto.randomBytes(32);
  const exact = exactGateInput(key);
  const isolated = { ...exact.isolatedResources, parentRole: { kind: "Role" } };
  assert.throws(() => verifyPristineLegacyCutoverGate({
    ...exact, isolatedResources: isolated
  }), /isolated_control_plane_present/);
  for (const resourceName of [
    "recoveryOperationFencePolicy",
    "recoveryOperationFenceBinding"
  ]) {
    assert.throws(() => verifyPristineLegacyCutoverGate({
      ...exact,
      isolatedResources: {
        ...exact.isolatedResources,
        [resourceName]: { metadata: { name: "recovery-operation-pod-fence.yenhubs.org" } }
      }
    }), /isolated_control_plane_present/);
  }
  assert.throws(() => verifyPristineLegacyCutoverGate({
    ...exact, runnerNamespace: { kind: "Namespace" }
  }), /runner_namespace_present/);
  assert.throws(() => verifyPristineLegacyCutoverGate({
    ...exact,
    liveDeployment: {
      ...liveDeployment(),
      metadata: {
        ...liveDeployment().metadata,
        annotations: { "yenhubs.org/runner-activation-phase": "active" }
      }
    }
  }), /live_binding_changed|not_legacy/);
  assert.throws(() => verifyPristineLegacyCutoverGate({
    ...exact,
    parentPodList: {
      ...exact.parentPodList,
      items: [{
        metadata: { name: "candidate", labels: { app: "bot-runner" } },
        spec: { serviceAccountName: "default" }
      }]
    }
  }), /parent_runner_candidate_present/);
  assert.throws(() => verifyPristineLegacyCutoverGate({
    ...exact,
    authority: {
      ...exact.authority,
      runner: { ...exact.authority.runner, patch: true }
    }
  }), /runner_authority_present/);
  for (const mutate of [
    value => { value.metadata.deletionTimestamp = "2026-07-19T12:00:00Z"; },
    value => { value.metadata.deletionGracePeriodSeconds = 0; },
    value => { value.metadata.generateName = "hcce-"; },
    value => { value.metadata.finalizers = ["keep"]; },
    value => { value.status.phase = "Terminating"; }
  ]) {
    const namespace = liveNamespace();
    mutate(namespace);
    assert.throws(() => verifyPristineLegacyCutoverGate({
      ...exact,
      liveNamespace: namespace
    }), /live_binding_changed/);
  }
  const missingIdentity = { ...exact.isolatedResources };
  delete missingIdentity.parentFenceBinding;
  assert.throws(() => verifyPristineLegacyCutoverGate({
    ...exact, isolatedResources: missingIdentity
  }), /isolated_control_plane_present/);
  assert.throws(() => verifyPristineLegacyCutoverGate({
    ...exact, isolatedResources: { ...exact.isolatedResources, extra: null }
  }), /isolated_control_plane_present/);
  assert.throws(() => verifyPristineLegacyCutoverGate({
    ...exact, isolatedResources: []
  }), /isolated_control_plane_present/);
  for (const parentPodList of [
    { kind: "PodList", items: [] },
    { kind: "PodList", metadata: { resourceVersion: "1", continue: "next" }, items: [] },
    { kind: "PodList", metadata: { resourceVersion: "1", remainingItemCount: 1 }, items: [] }
  ]) {
    assert.throws(() => verifyPristineLegacyCutoverGate({
      ...exact, parentPodList
    }), /runner_pod_list_incomplete/);
  }
});

test("behavioral preflight failures perform zero cluster mutations", () => {
  const key = crypto.randomBytes(32);
  const mutations = [];
  const mutationAdapter = {
    apply: (...args) => mutations.push(["apply", ...args]),
    create: (...args) => mutations.push(["create", ...args]),
    patch: (...args) => mutations.push(["patch", ...args]),
    delete: (...args) => mutations.push(["delete", ...args]),
    acquireLease: (...args) => mutations.push(["lease", ...args])
  };
  void mutationAdapter;
  const execute = ({ targetActivation = "bootstrap", verifyPristineEvidence }) =>
    executeCutoverPreflight({
      targetActivation,
      readParentNamespace: () => liveNamespace(),
      readParentDeployment: () => liveDeployment(),
      readDurableParentPolicyObserved: () => false,
      verifyPristineEvidence
    });

  for (const verifyPristineEvidence of [
    () => { throw new Error("process_local_cutover_attestation_path_required"); },
    () => {
      const input = exactGateInput(key);
      input.attestation.verifiedAud065ReportSha256 = "b".repeat(64);
      verifyPristineLegacyCutoverGate(input);
    },
    () => verifyPristineLegacyCutoverGate(exactGateInput(key, {
      runnerNamespace: { kind: "Namespace" }
    })),
    () => verifyPristineLegacyCutoverGate(exactGateInput(key, {
      authority: {
        parent: { create: false, delete: false, patch: false },
        runner: { create: true, delete: false, patch: false }
      }
    }))
  ]) {
    assert.throws(() => execute({ verifyPristineEvidence }));
    assert.equal(mutations.length, 0);
  }

  for (const targetActivation of ["admission", "active"]) {
    let evidenceReads = 0;
    assert.throws(() => execute({
      targetActivation,
      verifyPristineEvidence: () => { evidenceReads += 1; }
    }), /requires_pristine_bootstrap_target/);
    assert.equal(evidenceReads, 0);
    assert.equal(mutations.length, 0);
  }
  // Emergency refence uses the same non-bootstrap classification: it cannot
  // turn a pristine legacy baseline into an implicit first cutover.
  assert.throws(() => execute({
    targetActivation: "active",
    verifyPristineEvidence: () => {}
  }), /requires_pristine_bootstrap_target/);
  assert.equal(mutations.length, 0);

  for (const capabilityError of [
    "process_local_cutover_key_path_required",
    "process_local_cutover_key_must_be_private_regular_file"
  ]) {
    assert.throws(() => executeCutoverPreflight({
      targetActivation: "bootstrap",
      readParentNamespace: () => null,
      readParentDeployment: () => assert.fail("a missing Namespace has no Deployment read"),
      readDurableParentPolicyObserved: () => false,
      verifyPristineEvidence: () => {},
      verifyCleanCapability: () => { throw new Error(capabilityError); }
    }), new RegExp(capabilityError));
    assert.equal(mutations.length, 0, "a bad clean key cannot create the Namespace or Lease");
  }
});

test("clean install is revalidated by Namespace UID and supports only marker-bound bootstrap retry", () => {
  const cleanNamespace = liveNamespace();
  cleanNamespace.metadata.annotations = {
    [CLEAN_INSTALL_ANNOTATION]: CLEAN_INSTALL_VALUE
  };
  const base = exactGateInput(crypto.randomBytes(32));
  const cleanEvidence = {
    namespace: NAMESPACE,
    expectedNamespaceUid: "namespace-uid",
    liveNamespace: cleanNamespace,
    liveDeployment: null,
    runnerNamespace: null,
    isolatedResources: base.isolatedResources,
    parentPodList: base.parentPodList,
    authority: base.authority
  };
  assert.equal(
    verifyCleanInstallCutoverGate(cleanEvidence).namespaceUid,
    "namespace-uid"
  );
  assert.throws(() => verifyCleanInstallCutoverGate({
    ...cleanEvidence,
    expectedNamespaceUid: "replaced-uid"
  }), /namespace_or_deployment_changed/);
  const terminatingNamespace = structuredClone(cleanNamespace);
  terminatingNamespace.metadata.deletionTimestamp = "2026-07-19T12:00:00Z";
  terminatingNamespace.status.phase = "Terminating";
  assert.throws(() => verifyCleanInstallCutoverGate({
    ...cleanEvidence,
    liveNamespace: terminatingNamespace
  }), /namespace_or_deployment_changed/);
  assert.throws(() => verifyCleanInstallCutoverGate({
    ...cleanEvidence,
    liveDeployment: liveDeployment()
  }), /namespace_or_deployment_changed/);

  let cleanReads = 0;
  const classification = executeCutoverPreflight({
    targetActivation: "bootstrap",
    readParentNamespace: () => structuredClone(cleanNamespace),
    readParentDeployment: () => null,
    readDurableParentPolicyObserved: () => false,
    verifyPristineEvidence: () => assert.fail("receipt is not used for a marked clean retry"),
    verifyCleanEvidence: uid => {
      cleanReads += 1;
      verifyCleanInstallCutoverGate({ ...cleanEvidence, expectedNamespaceUid: uid });
    }
  });
  assert.equal(classification, "clean-install-resume");
  assert.equal(cleanReads, 1);
  assert.throws(() => executeCutoverPreflight({
    targetActivation: "active",
    readParentNamespace: () => structuredClone(cleanNamespace),
    readParentDeployment: () => null,
    readDurableParentPolicyObserved: () => false,
    verifyPristineEvidence: () => {},
    verifyCleanEvidence: () => {}
  }), /clean_install_requires_bootstrap_target/);
});

test("private evidence readers reject broad modes and symlinks", t => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "cutover-evidence-"));
  fs.chmodSync(directory, 0o700);
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const key = crypto.randomBytes(32);
  const keyPath = path.join(directory, "cutover.key");
  const receiptPath = path.join(directory, "cutover.json");
  fs.writeFileSync(keyPath, key, { mode: 0o600 });
  fs.writeFileSync(receiptPath, `${JSON.stringify(signedAttestation(key))}\n`, { mode: 0o600 });
  assert.deepEqual(readPrivateCutoverKey(keyPath), key);
  assert.equal(readPrivateCutoverAttestation(receiptPath).profileId, PROFILE_ID);

  fs.chmodSync(receiptPath, 0o644);
  assert.throws(() => readPrivateCutoverAttestation(receiptPath), /private_regular_file/);
  fs.chmodSync(receiptPath, 0o600);
  const link = path.join(directory, "link.json");
  fs.symlinkSync(receiptPath, link);
  assert.throws(() => readPrivateCutoverAttestation(link), /private_regular_file|unreadable/);
});

test("preflight is structurally before namespace/Lease mutation and the under-Lease gate precedes bootstrap mutation", () => {
  const source = fs.readFileSync(path.resolve(__dirname, "index.js"), "utf8");
  const main = source.slice(source.indexOf("async function main()"));
  const preflight = main.indexOf("verifyPristineLegacyCutoverPreflight");
  const namespaceMutation = main.indexOf("ensureFoundationalNamespaceForLease");
  const leaseMutation = main.indexOf("acquireOperationLeaseGuard");
  assert.ok(preflight >= 0 && preflight < namespaceMutation && preflight < leaseMutation);

  const bootstrap = source.slice(
    source.indexOf("async function applyBootstrap()"),
    source.indexOf("async function applyAdmission()")
  );
  const firstCutover = bootstrap.indexOf("performFirstCutoverFenceTransition");
  const firstManifest = bootstrap.indexOf("applyManifest()");
  assert.ok(firstCutover >= 0 && firstCutover < firstManifest);

  const underLease = source.slice(
    source.indexOf("async function applyUnderOperationLease"),
    source.indexOf("async function main()")
  );
  assert.ok(
    underLease.indexOf("verifyCutoverPreflightUnderLease") <
      underLease.indexOf("applyBootstrap")
  );

  const normalPreflight = source.slice(
    source.indexOf("function verifyPristineLegacyCutoverPreflight"),
    source.indexOf("function verifyEmergencyRefencePreflight")
  );
  assert.ok(
    normalPreflight.indexOf("loadPrivateCutoverKey") >= 0,
    "clean install must load the private key inside the read-only preflight"
  );
});

test("under-Lease revalidation is behavioral for pristine, clean/resume, and exact fence evidence", () => {
  const key = crypto.randomBytes(32);
  let pristineCalls = 0;
  assert.equal(executeCutoverRevalidation({
    classification: "pristine-cutover",
    verifyPristineEvidence: () => {
      pristineCalls += 1;
      verifyPristineLegacyCutoverGate(exactGateInput(key));
    }
  }), true);
  assert.equal(pristineCalls, 1);

  const cleanNamespace = liveNamespace();
  cleanNamespace.metadata.annotations = { [CLEAN_INSTALL_ANNOTATION]: CLEAN_INSTALL_VALUE };
  const cleanBase = exactGateInput(key);
  for (const classification of ["clean-install", "clean-install-resume"]) {
    let cleanCalls = 0;
    assert.equal(executeCutoverRevalidation({
      classification,
      verifyCleanEvidence: () => {
        cleanCalls += 1;
        verifyCleanInstallCutoverGate({
          namespace: NAMESPACE,
          expectedNamespaceUid: "namespace-uid",
          liveNamespace: cleanNamespace,
          liveDeployment: null,
          runnerNamespace: null,
          isolatedResources: cleanBase.isolatedResources,
          parentPodList: cleanBase.parentPodList,
          authority: cleanBase.authority
        });
      }
    }), true);
    assert.equal(cleanCalls, 1);
  }
  assert.throws(() => executeCutoverRevalidation({
    classification: "clean-install-resume",
    verifyCleanEvidence: () => verifyCleanInstallCutoverGate({
      namespace: NAMESPACE,
      expectedNamespaceUid: "replaced-uid",
      liveNamespace: cleanNamespace,
      liveDeployment: null,
      runnerNamespace: null,
      isolatedResources: cleanBase.isolatedResources,
      parentPodList: cleanBase.parentPodList,
      authority: cleanBase.authority
    })
  }), /namespace_or_deployment_changed/);

  const fenceEvidence = {
    namespace: { uid: "namespace-uid", resourceVersion: "10", specSha256: "a".repeat(64) },
    deployment: { uid: "deployment-uid", resourceVersion: "20", specSha256: "b".repeat(64) },
    policy: { uid: "policy-uid", resourceVersion: "30", specSha256: "c".repeat(64) },
    binding: { uid: "binding-uid", resourceVersion: "40", specSha256: "d".repeat(64) }
  };
  assert.equal(executeCutoverRevalidation({
    classification: "fence-aware",
    expectedFenceEvidence: fenceEvidence,
    readFenceEvidence: () => structuredClone(fenceEvidence)
  }), true);
  for (const mutate of [
    value => { value.namespace.uid = "replacement"; },
    value => { value.deployment.resourceVersion = "21"; },
    value => { value.policy.specSha256 = "e".repeat(64); },
    value => { value.binding.uid = "replacement"; }
  ]) {
    const changed = structuredClone(fenceEvidence);
    mutate(changed);
    assert.throws(() => executeCutoverRevalidation({
      classification: "fence-aware",
      expectedFenceEvidence: fenceEvidence,
      readFenceEvidence: () => changed
    }), /fence_state_changed/);
  }
  assert.throws(() => executeCutoverRevalidation({
    classification: "fence-aware",
    expectedFenceEvidence: fenceEvidence,
    readFenceEvidence: () => null
  }), /fence_state_changed/, "an active durable-to-legacy race must fail before mutation");
  for (const invalid of [
    { classification: "pristine-cutover" },
    { classification: "clean-install" },
    { classification: "fence-aware", expectedFenceEvidence: fenceEvidence },
    { classification: "unknown" }
  ]) {
    assert.throws(() => executeCutoverRevalidation(invalid));
  }
});

test("direct attestation verifier rejects an unauthenticated self-assertion", () => {
  const key = crypto.randomBytes(32);
  const unsigned = unsignedAttestation();
  assert.throws(() => verifyProcessLocalCutoverAttestation(unsigned, {
    key,
    namespace: NAMESPACE,
    expectedKubeContext: CONTEXT,
    liveNamespace: liveNamespace(),
    liveDeployment: liveDeployment(),
    now: () => NOW
  }), /shape_invalid/);
});
