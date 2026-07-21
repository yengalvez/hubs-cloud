const { execFileSync, spawn, spawnSync } = require("node:child_process");
const { createHash, randomUUID } = require("node:crypto");
const { readFileSync } = require("node:fs");
const path = require("node:path");
const { isDeepStrictEqual } = require("node:util");
const utils = require("../utils");
const {
  KubernetesRunnerManager,
  guardPodDocumentForIdentity,
  requireCompletePodList
} = require("../services/bot-orchestrator/kubernetes-runner-manager");
const {
  completeRunnerNamespaceInventory,
  reconcileRunnerNamespace
} = require("./runner-guard-reconciliation");
const {
  canonicalJson,
  activeCutoverNamespace,
  executeCutoverPreflight,
  executeCutoverRevalidation,
  readPrivateCutoverAttestation,
  readPrivateCutoverKey,
  verifyCleanInstallCutoverGate,
  verifyJournalCutoverIsolationGate,
  verifyPristineLegacyCutoverGate
} = require("./process-local-cutover");
const {
  CUTOVER_JOURNAL_NAME,
  advanceCutoverJournalTransition,
  classifyCutoverJournalPrefix,
  createCutoverJournal,
  cutoverJournalConfigMap,
  liveDeploymentMatchesJournalTarget,
  liveDeploymentMatchesNormalizedTarget,
  liveObjectIsUnencumbered,
  liveResourceMatchesTarget,
  parseExactCutoverJournalConfigMap,
  parseStructurallyExactCutoverJournalConfigMap,
  sha256Canonical
} = require("./cutover-journal");
const {
  collectLiveRunnerControlPlane,
  verifyLiveRunnerControlPlane
} = require("./live-runner-control-plane");
const {
  PodWatchEvidence,
  ReplicaSetWatchEvidence,
  completeWatchListResourceVersion,
  forbiddenPod,
  namespacedListItemWithTypeMeta,
  namespacedWatchObjectIsValid,
  podListRawPath,
  podWatchRawPath,
  replicaSetListRawPath,
  replicaSetWatchRawPath
} = require("./pod-quiescence-watch");
const {
  replaceWithBookmarkedSuccessors,
  startEvidenceWatchProcess,
  stopEvidenceWatches,
  waitForBookmarkedWatchBoundary,
  withOwnedEvidenceWatches
} = require("./watch-evidence-process");
const {
  KubectlLeaseClient,
  MUTATION_TIMEOUT_MS,
  OperationLease,
  runLeaseGuardedMutation
} = require("./operation-lease");
const {
  effectiveRbacReviewSpecs,
  selfSubjectRulesReviewRequest,
  verifyEffectiveRbacReviews
} = require("./effective-rbac");
const {
  ADMISSION_POLICY_NAME,
  CUTOVER_JOURNAL_POLICY_NAME,
  PARENT_FENCE_POLICY_NAME,
  RECOVERY_OPERATION_FENCE_POLICY_NAME,
  RUNNER_PROTOCOL_POLICY_NAME,
  RECOVERY_CONSUMERS,
  RECOVERY_EPOCH_ANNOTATION,
  RECOVERY_LOCK_NAME,
  RECOVERY_PHASE_ANNOTATION,
  RUNNER_NAMESPACE,
  admissionPolicyIsObserved,
  applyResourcesSequentially,
  decideApplyMode,
  exactAdmissionBinding,
  exactCutoverJournalBinding,
  exactParentFenceBinding,
  exactRecoveryOperationFenceBinding,
  exactRecoveryOperationFencePolicy,
  exactRunnerProtocolBinding,
  exactDeploymentDesiredState,
  exactFoundationalNamespace,
  exactRecoveryOperationLock,
  parentFencePolicyProtectsLiveOrTarget,
  parentIsQuiesced,
  recoveryConsumerReplicaSets,
  recoveryConsumerReplicaSetsAreStopped,
  recoveryConsumersAreQuiesced,
  recoveryOperationFenceNamespaceSelector,
  retryBestEffortFenceAttempt,
  readActivationPlanText,
  runBestEffortFenceSteps,
  uniqueRunnerPods
} = require("./runner-activation");

const manifestPath = path.resolve(process.env.HCCE_MANIFEST_PATH || "hcce.yaml");
const manifestBytes = readFileSync(manifestPath);
const manifestText = manifestBytes.toString("utf8");
const manifestSha256 = createHash("sha256").update(manifestBytes).digest("hex");
const config = utils.readConfig(process.env.HCCE_INPUT_VALUES_PATH);
const parentNamespace = config.Namespace;
const plan = readActivationPlanText(manifestText);
const waitTimeoutMs = 180_000;
const stablePodAbsenceWindowMs = 61_000;
const stableWatchAcquisitionTimeoutMs = waitTimeoutMs * 3;
const kubectlReadTimeoutMs = 30_000;
const watchServerTimeoutSeconds = 600;
const watchProcessGraceSeconds = 10;
const maxWatchBufferBytes = 4 * 1024 * 1024;
const kubectlContext = String(process.env.KUBECTL_CONTEXT || "");
const leaseHeartbeatPath = path.resolve(__dirname, "operation-lease.js");
const manifestVerifierPath = path.resolve(__dirname, "../generate_script/verify-generated-manifest.js");
let operationLeaseGuard = null;
let failClosedRefenceRequired = false;
let recoveryLockIdentityGuard = null;
let pristineLegacyCutoverRequired = false;
let cutoverPreflightClassification = null;
let cutoverNamespaceUid = null;
let cutoverFenceEvidence = null;
let cutoverKey = null;
let cutoverAttestation = null;
let cutoverBaselineEvidence = null;
let cutoverJournal = null;

function requirePinnedKubectlContext() {
  if (!kubectlContext || kubectlContext !== kubectlContext.trim() || /[\u0000-\u001f\u007f]/u.test(kubectlContext)) {
    throw new Error("KUBECTL_CONTEXT_must_be_one_exact_nonempty_context");
  }
  const current = execFileSync("kubectl", ["config", "current-context"], {
    encoding: "utf8",
    timeout: kubectlReadTimeoutMs
  }).trim();
  if (current !== kubectlContext) throw new Error("KUBECTL_CONTEXT_does_not_match_current_context");
}

function requestedCommandMode(argv) {
  if (argv.length === 0) return "apply";
  if (argv.length === 1 && argv[0] === "--emergency-refence") return "emergency-refence";
  throw new Error("apply_arguments_invalid");
}

function verifyManifestBeforeClusterMutation() {
  const result = spawnSync(
    process.execPath,
    [manifestVerifierPath, "--stdin"],
    {
      input: manifestBytes,
      stdio: ["pipe", "inherit", "inherit"],
      timeout: MUTATION_TIMEOUT_MS
    }
  );
  if (result.status !== 0) throw new Error(`generated_manifest_verification_failed:${result.status}`);
}

function contextArgs(args) {
  return ["--context", kubectlContext, ...args];
}

function runLeaseGuardedRead(read) {
  return operationLeaseGuard
    ? runLeaseGuardedMutation(assertOperationLeaseHeld, read)
    : read();
}

function startLeaseHeartbeat(guard) {
  guard.expectedHeartbeatStop = false;
  guard.heartbeatLost = false;
  const child = spawn(
    process.execPath,
    [leaseHeartbeatPath, kubectlContext, parentNamespace, guard.lease.holder, String(process.pid)],
    { stdio: "ignore" }
  );
  guard.heartbeat = child;
  child.once("error", () => {
    if (!guard.expectedHeartbeatStop) guard.heartbeatLost = true;
  });
  child.once("exit", () => {
    if (!guard.expectedHeartbeatStop) guard.heartbeatLost = true;
  });
}

function acquireOperationLeaseGuard() {
  const holder = `cloud-apply:${randomUUID()}`;
  const lease = new OperationLease(
    new KubectlLeaseClient({ context: kubectlContext }),
    { namespace: parentNamespace, holder }
  );
  lease.acquire();
  const guard = {
    lease,
    heartbeat: null,
    heartbeatLost: false,
    expectedHeartbeatStop: false
  };
  startLeaseHeartbeat(guard);
  return guard;
}

function assertOperationLeaseProcessHealthy() {
  if (!operationLeaseGuard) throw new Error("operation_serialization_lease_not_acquired");
  const child = operationLeaseGuard.heartbeat;
  if (
    operationLeaseGuard.heartbeatLost ||
    !child ||
    child.exitCode !== null ||
    child.signalCode !== null
  ) {
    operationLeaseGuard.heartbeatLost = true;
    throw new Error("operation_serialization_lease_heartbeat_lost");
  }
}

function assertOperationLeaseHeld() {
  assertOperationLeaseProcessHealthy();
  try {
    operationLeaseGuard.lease.assertFreshForMutation();
  } catch (_error) {
    operationLeaseGuard.heartbeatLost = true;
    throw new Error("operation_serialization_lease_lost");
  }
  assertOperationLeaseProcessHealthy();
}

async function stopLeaseHeartbeat(guard) {
  const child = guard?.heartbeat;
  if (!child) return;
  guard.expectedHeartbeatStop = true;
  if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
  await new Promise(resolve => {
    if (child.exitCode !== null || child.signalCode !== null) {
      resolve();
      return;
    }
    let settled = false;
    let timeout;
    const finish = () => {
      if (settled) return;
      settled = true;
      if (timeout) clearTimeout(timeout);
      resolve();
    };
    child.once("exit", finish);
    timeout = setTimeout(() => {
      if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
      finish();
    }, 5_000);
  });
  guard.heartbeat = null;
}

async function recoverOperationLeaseForFailClosedCleanup() {
  if (!operationLeaseGuard) throw new Error("operation_serialization_cleanup_without_lease");
  await stopLeaseHeartbeat(operationLeaseGuard);
  operationLeaseGuard.lease.acquire();
  startLeaseHeartbeat(operationLeaseGuard);
  assertOperationLeaseHeld();
}

async function releaseOperationLeaseGuard() {
  if (!operationLeaseGuard) return;
  const guard = operationLeaseGuard;
  await stopLeaseHeartbeat(guard);
  if (guard.lease.release() !== true) throw new Error("operation_lease_release_not_owned");
  operationLeaseGuard = null;
}

function kubectlJson(args) {
  return runLeaseGuardedRead(() => JSON.parse(execFileSync(
    "kubectl",
    contextArgs(args),
    { encoding: "utf8", timeout: kubectlReadTimeoutMs }
  )));
}

function kubectlOptionalJson(args) {
  return runLeaseGuardedRead(() => {
    const result = spawnSync(
      "kubectl",
      contextArgs(args),
      { encoding: "utf8", timeout: kubectlReadTimeoutMs }
    );
    if (result.status === 0) return JSON.parse(result.stdout);
    if (`${result.stderr || ""}`.includes("NotFound")) return null;
    throw new Error(`kubectl_read_failed:${result.status}`);
  });
}

function kubectlAbsentOnlyJson(args, identity) {
  return runLeaseGuardedRead(() => {
    const result = spawnSync(
      "kubectl",
      contextArgs(args),
      { encoding: "utf8", timeout: kubectlReadTimeoutMs }
    );
    if (result.status === 0) return JSON.parse(result.stdout);
    const diagnostic = `${result.stdout || ""}\n${result.stderr || ""}`;
    if (result.status === 1 && /\(NotFound\)|"reason"\s*:\s*"NotFound"/u.test(diagnostic)) {
      return null;
    }
    throw new Error(`kubectl_absence_read_failed:${identity}:${result.status}`);
  });
}

function foundationalNamespaceResource() {
  const resource = plan.resources.find(value =>
    value?.apiVersion === "v1" &&
    value?.kind === "Namespace" &&
    value?.metadata?.name === parentNamespace
  );
  if (!resource) throw new Error("generated_manifest_parent_namespace_missing");
  return resource;
}

async function ensureFoundationalNamespaceForLease() {
  const expected = foundationalNamespaceResource();
  let live = kubectlOptionalJson(["get", "namespace", parentNamespace, "-o", "json"]);
  if (live === null) {
    const applied = spawnSync(
      "kubectl",
      contextArgs(["--request-timeout=30s", "apply", "-f", "-"]),
      {
        input: JSON.stringify(expected),
        stdio: ["pipe", "inherit", "inherit"],
        timeout: MUTATION_TIMEOUT_MS
      }
    );
    if (applied.status !== 0) throw new Error(`foundational_namespace_apply_failed:${applied.status}`);
    const deadline = Date.now() + 30_000;
    while (Date.now() < deadline) {
      live = kubectlOptionalJson(["get", "namespace", parentNamespace, "-o", "json"]);
      if (exactFoundationalNamespace(live, expected)) return live;
      await sleep(250);
    }
    throw new Error("foundational_namespace_not_exact_or_active");
  }
  const identityOnly = {
    apiVersion: "v1",
    kind: "Namespace",
    metadata: { name: parentNamespace }
  };
  if (!exactFoundationalNamespace(live, identityOnly)) {
    throw new Error("existing_parent_namespace_not_active");
  }
  return live;
}

function sleep(milliseconds) {
  return new Promise(resolve => setTimeout(resolve, milliseconds));
}

async function waitFor(label, predicate) {
  const deadline = Date.now() + waitTimeoutMs;
  let lastError;
  while (Date.now() < deadline) {
    assertOperationLeaseProcessHealthy();
    let matched = false;
    try {
      matched = predicate();
    } catch (error) {
      lastError = error;
    }
    if (matched) {
      assertOperationLeaseHeld();
      return;
    }
    await sleep(1_000);
  }
  throw new Error(`${label}_timeout${lastError ? `:${lastError.message}` : ""}`);
}

function liveParentIsQuiesced() {
  const deployment = kubectlJson([
    "-n", parentNamespace, "get", "deployment", "bot-orchestrator", "-o", "json"
  ]);
  const pods = kubectlJson(["-n", parentNamespace, "get", "pods", "-o", "json"]);
  return parentIsQuiesced(deployment, pods);
}

function liveState() {
  const deployment = kubectlOptionalJson([
    "-n", parentNamespace, "get", "deployment", "bot-orchestrator", "-o", "json"
  ]);
  return {
    activationPhase:
      deployment?.metadata?.annotations?.["yenhubs.org/runner-activation-phase"] || "legacy",
    recoveryPhase: deployment?.metadata?.annotations?.[RECOVERY_PHASE_ANNOTATION] || "legacy",
    recoveryEpoch: deployment?.metadata?.annotations?.[RECOVERY_EPOCH_ANNOTATION] || "legacy-absent"
  };
}

function liveAdmissionCoreIsObserved() {
  const policy = kubectlJson([
    "get", "validatingadmissionpolicy", ADMISSION_POLICY_NAME, "-o", "json"
  ]);
  const binding = kubectlJson([
    "get", "validatingadmissionpolicybinding", ADMISSION_POLICY_NAME, "-o", "json"
  ]);
  return admissionPolicyIsObserved(policy) &&
    exactAdmissionBinding(binding) &&
    liveRunnerProtocolAdmissionIsObserved() &&
    liveCutoverJournalAdmissionIsObserved() &&
    liveParentFenceAdmissionIsObserved();
}

function liveAdmissionIsObserved() {
  return liveAdmissionCoreIsObserved() &&
    liveRecoveryOperationFenceAdmissionIsObserved({
      active: plan.recoveryPhase === "restore-fence"
    });
}

function recoveryAdmissionPolicy() {
  const policy = structuredClone(plan.resources.find(resource =>
    resource?.apiVersion === "admissionregistration.k8s.io/v1" &&
    resource?.kind === "ValidatingAdmissionPolicy" &&
    resource?.metadata?.name === ADMISSION_POLICY_NAME
  ));
  if (!policy) throw new Error("generated_manifest_runner_admission_policy_missing");
  const variables = new Map((policy.spec?.variables || []).map(variable => [variable?.name, variable]));
  const activation = variables.get("activationPhase");
  const recovery = variables.get("recoveryPhase");
  if (!activation || !recovery) throw new Error("generated_manifest_runner_recovery_variables_missing");
  activation.expression = "'bootstrap'";
  recovery.expression = "'active'";
  return policy;
}

function liveRecoveryAdmissionIsObserved() {
  const policy = kubectlJson([
    "get", "validatingadmissionpolicy", ADMISSION_POLICY_NAME, "-o", "json"
  ]);
  const binding = kubectlJson([
    "get", "validatingadmissionpolicybinding", ADMISSION_POLICY_NAME, "-o", "json"
  ]);
  return admissionPolicyIsObserved(policy) &&
    exactAdmissionBinding(binding) &&
    liveRunnerProtocolAdmissionIsObserved() &&
    liveCutoverJournalAdmissionIsObserved() &&
    liveRecoveryOperationFencePolicyIsObserved() &&
    isDeepStrictEqual(policy?.spec, recoveryAdmissionPolicy().spec);
}

function generatedRunnerProtocolPolicy() {
  const policy = plan.resources.find(resource =>
    resource?.apiVersion === "admissionregistration.k8s.io/v1" &&
    resource?.kind === "ValidatingAdmissionPolicy" &&
    resource?.metadata?.name === RUNNER_PROTOCOL_POLICY_NAME
  );
  if (!policy) throw new Error("generated_manifest_runner_protocol_policy_missing");
  return policy;
}

function liveRunnerProtocolAdmissionIsObserved() {
  const policy = kubectlAbsentOnlyJson([
    "get", "validatingadmissionpolicy", RUNNER_PROTOCOL_POLICY_NAME, "-o", "json"
  ], "runner-protocol-policy");
  const binding = kubectlAbsentOnlyJson([
    "get", "validatingadmissionpolicybinding", RUNNER_PROTOCOL_POLICY_NAME, "-o", "json"
  ], "runner-protocol-binding");
  return policy !== null && binding !== null &&
    admissionPolicyIsObserved(policy) &&
    exactRunnerProtocolBinding(binding) &&
    isDeepStrictEqual(policy?.spec, generatedRunnerProtocolPolicy().spec);
}

function generatedParentFencePolicy() {
  const policy = plan.resources.find(resource =>
    resource?.apiVersion === "admissionregistration.k8s.io/v1" &&
    resource?.kind === "ValidatingAdmissionPolicy" &&
    resource?.metadata?.name === PARENT_FENCE_POLICY_NAME
  );
  if (!policy) throw new Error("generated_manifest_parent_fence_policy_missing");
  return policy;
}

function generatedRecoveryOperationFencePolicy() {
  const policy = plan.resources.find(resource =>
    resource?.apiVersion === "admissionregistration.k8s.io/v1" &&
    resource?.kind === "ValidatingAdmissionPolicy" &&
    resource?.metadata?.name === RECOVERY_OPERATION_FENCE_POLICY_NAME
  );
  if (!policy) throw new Error("generated_manifest_recovery_operation_fence_policy_missing");
  return policy;
}

function generatedRecoveryOperationFenceBinding() {
  const binding = plan.resources.find(resource =>
    resource?.apiVersion === "admissionregistration.k8s.io/v1" &&
    resource?.kind === "ValidatingAdmissionPolicyBinding" &&
    resource?.metadata?.name === RECOVERY_OPERATION_FENCE_POLICY_NAME
  );
  if (!binding) throw new Error("generated_manifest_recovery_operation_fence_binding_missing");
  return binding;
}

function recoveryOperationFenceBinding(active) {
  const binding = structuredClone(generatedRecoveryOperationFenceBinding());
  binding.spec.matchResources.namespaceSelector = recoveryOperationFenceNamespaceSelector(
    parentNamespace,
    { active }
  );
  return binding;
}

function liveRecoveryOperationFencePolicyIsObserved() {
  const policy = kubectlAbsentOnlyJson([
    "get", "validatingadmissionpolicy", RECOVERY_OPERATION_FENCE_POLICY_NAME, "-o", "json"
  ], "recovery-operation-fence-policy");
  return policy !== null &&
    admissionPolicyIsObserved(policy) &&
    exactRecoveryOperationFencePolicy(policy, parentNamespace) &&
    isDeepStrictEqual(policy?.spec, generatedRecoveryOperationFencePolicy().spec);
}

function liveRecoveryOperationFenceAdmissionIsObserved({ active, expectedBinding = null }) {
  const binding = kubectlAbsentOnlyJson([
    "get", "validatingadmissionpolicybinding", RECOVERY_OPERATION_FENCE_POLICY_NAME, "-o", "json"
  ], "recovery-operation-fence-binding");
  return binding !== null &&
    liveRecoveryOperationFencePolicyIsObserved() &&
    transitionableRecoveryOperationFenceBinding(binding) &&
    exactRecoveryOperationFenceBinding(binding, parentNamespace, { active }) &&
    (
      expectedBinding === null ||
      (
        binding.metadata.uid === expectedBinding?.metadata?.uid &&
        binding.metadata.resourceVersion === expectedBinding?.metadata?.resourceVersion
      )
    );
}

function generatedCutoverJournalPolicy() {
  const policy = plan.resources.find(resource =>
    resource?.apiVersion === "admissionregistration.k8s.io/v1" &&
    resource?.kind === "ValidatingAdmissionPolicy" &&
    resource?.metadata?.name === CUTOVER_JOURNAL_POLICY_NAME
  );
  if (!policy) throw new Error("generated_manifest_cutover_journal_policy_missing");
  return policy;
}

function generatedCutoverJournalBinding() {
  const binding = plan.resources.find(resource =>
    resource?.apiVersion === "admissionregistration.k8s.io/v1" &&
    resource?.kind === "ValidatingAdmissionPolicyBinding" &&
    resource?.metadata?.name === CUTOVER_JOURNAL_POLICY_NAME
  );
  if (!binding) throw new Error("generated_manifest_cutover_journal_binding_missing");
  return binding;
}

function generatedParentFenceBinding() {
  const binding = plan.resources.find(resource =>
    resource?.apiVersion === "admissionregistration.k8s.io/v1" &&
    resource?.kind === "ValidatingAdmissionPolicyBinding" &&
    resource?.metadata?.name === PARENT_FENCE_POLICY_NAME
  );
  if (!binding) throw new Error("generated_manifest_parent_fence_binding_missing");
  return binding;
}

function generatedBotOrchestratorDeployment() {
  const deployment = plan.resources.find(resource =>
    resource?.apiVersion === "apps/v1" &&
    resource?.kind === "Deployment" &&
    resource?.metadata?.namespace === parentNamespace &&
    resource?.metadata?.name === "bot-orchestrator"
  );
  if (!deployment) throw new Error("generated_manifest_bot_orchestrator_missing");
  return deployment;
}

function cutoverTargetResources() {
  return {
    journalPolicy: generatedCutoverJournalPolicy(),
    journalBinding: generatedCutoverJournalBinding(),
    targetPolicy: generatedParentFencePolicy(),
    targetBinding: generatedParentFenceBinding(),
    targetDeployment: recoveryDeployment("bot-orchestrator", 0, "active")
  };
}

function cutoverManifestSha256() {
  return manifestSha256;
}

function cutoverTargetHashes() {
  const {
    journalPolicy,
    journalBinding,
    targetPolicy,
    targetBinding,
    targetDeployment
  } = cutoverTargetResources();
  return {
    journalPolicy: sha256Canonical(journalPolicy),
    journalBinding: sha256Canonical(journalBinding),
    parentPolicy: sha256Canonical(targetPolicy),
    parentBinding: sha256Canonical(targetBinding),
    parentDeployment: sha256Canonical(targetDeployment)
  };
}

function cutoverJournalVerification(namespaceUid, key = cutoverKey) {
  return {
    key,
    expectedKubeContext: kubectlContext,
    namespace: parentNamespace,
    namespaceUid,
    manifestSha256: cutoverManifestSha256(),
    targetHashes: cutoverTargetHashes()
  };
}

function durableCutoverJournalVerification(namespaceUid) {
  return {
    namespace: parentNamespace,
    namespaceUid,
    allowFutureIssuedAt: true
  };
}

function readCutoverJournalConfigMap() {
  return kubectlAbsentOnlyJson([
    "-n", parentNamespace, "get", "configmap", CUTOVER_JOURNAL_NAME, "-o", "json"
  ], "runner-cutover-journal");
}

function exactCutoverJournalConfigMap(value, namespaceUid, key = cutoverKey) {
  try {
    parseExactCutoverJournalConfigMap(value, cutoverJournalVerification(namespaceUid, key));
    return true;
  } catch (_error) {
    return false;
  }
}

function liveParentFenceAdmissionIsObserved() {
  const policy = kubectlAbsentOnlyJson([
    "get", "validatingadmissionpolicy", PARENT_FENCE_POLICY_NAME, "-o", "json"
  ], "parent-fence-policy");
  const binding = kubectlAbsentOnlyJson([
    "get", "validatingadmissionpolicybinding", PARENT_FENCE_POLICY_NAME, "-o", "json"
  ], "parent-fence-binding");
  return policy !== null && binding !== null &&
    admissionPolicyIsObserved(policy) &&
    exactParentFenceBinding(binding, parentNamespace) &&
    isDeepStrictEqual(policy?.spec, generatedParentFencePolicy().spec);
}

function liveCutoverJournalAdmissionIsObserved() {
  const policy = kubectlAbsentOnlyJson([
    "get", "validatingadmissionpolicy", CUTOVER_JOURNAL_POLICY_NAME, "-o", "json"
  ], "cutover-journal-policy");
  const binding = kubectlAbsentOnlyJson([
    "get", "validatingadmissionpolicybinding", CUTOVER_JOURNAL_POLICY_NAME, "-o", "json"
  ], "cutover-journal-binding");
  return policy !== null && binding !== null &&
    admissionPolicyIsObserved(policy) &&
    exactCutoverJournalBinding(binding, parentNamespace) &&
    isDeepStrictEqual(policy?.spec, generatedCutoverJournalPolicy().spec);
}

function liveParentFenceAdmissionIsDurable() {
  return durableParentFenceEvidence() !== null;
}

function resourceVersionEvidence(resource, label) {
  const uid = resource?.metadata?.uid;
  const resourceVersion = resource?.metadata?.resourceVersion;
  if (typeof uid !== "string" || !uid || typeof resourceVersion !== "string" || !resourceVersion) {
    throw new Error(`${label}_identity_unverifiable`);
  }
  return {
    uid,
    resourceVersion,
    specSha256: createHash("sha256")
      .update(canonicalJson(resource?.spec || null), "utf8")
      .digest("hex")
  };
}

function durableParentFenceEvidence() {
  const namespaceBefore = kubectlAbsentOnlyJson([
    "get", "namespace", parentNamespace, "-o", "json"
  ], "durable-parent-fence-namespace-before");
  if (!activeCutoverNamespace(namespaceBefore, parentNamespace)) return null;
  const namespaceUid = namespaceBefore.metadata.uid;
  const journalConfigMap = kubectlAbsentOnlyJson([
    "-n", parentNamespace, "get", "configmap", CUTOVER_JOURNAL_NAME, "-o", "json"
  ], "durable-cutover-journal");
  const journalPolicy = kubectlAbsentOnlyJson([
    "get", "validatingadmissionpolicy", CUTOVER_JOURNAL_POLICY_NAME, "-o", "json"
  ], "durable-cutover-journal-policy");
  const journalBinding = kubectlAbsentOnlyJson([
    "get", "validatingadmissionpolicybinding", CUTOVER_JOURNAL_POLICY_NAME, "-o", "json"
  ], "durable-cutover-journal-binding");
  const policy = kubectlAbsentOnlyJson([
    "get", "validatingadmissionpolicy", PARENT_FENCE_POLICY_NAME, "-o", "json"
  ], "durable-parent-fence-policy");
  const binding = kubectlAbsentOnlyJson([
    "get", "validatingadmissionpolicybinding", PARENT_FENCE_POLICY_NAME, "-o", "json"
  ], "durable-parent-fence-binding");
  const deployment = kubectlAbsentOnlyJson([
    "-n", parentNamespace, "get", "deployment", "bot-orchestrator", "-o", "json"
  ], "durable-parent-fence-deployment");
  const namespaceAfter = kubectlAbsentOnlyJson([
    "get", "namespace", parentNamespace, "-o", "json"
  ], "durable-parent-fence-namespace-after");
  let journal = null;
  try {
    journal = parseStructurallyExactCutoverJournalConfigMap(
      journalConfigMap,
      durableCutoverJournalVerification(namespaceUid)
    );
  } catch (_error) {
    return null;
  }
  const valid = activeCutoverNamespace(namespaceAfter, parentNamespace) &&
    namespaceAfter.metadata.uid === namespaceUid &&
    journal.namespace.uid === namespaceUid &&
    journalPolicy !== null && journalBinding !== null &&
    policy !== null && binding !== null && deployment !== null &&
    [journalPolicy, journalBinding, policy, binding, deployment]
      .every(liveObjectIsUnencumbered) &&
    (journal.mode === "clean-install" ||
      deployment.metadata.uid === journal.baselineDeployment.uid) &&
    admissionPolicyIsObserved(journalPolicy) &&
    exactCutoverJournalBinding(journalBinding, parentNamespace) &&
    isDeepStrictEqual(journalPolicy?.spec, generatedCutoverJournalPolicy().spec) &&
    admissionPolicyIsObserved(policy) &&
    exactParentFenceBinding(binding, parentNamespace) &&
    parentFencePolicyProtectsLiveOrTarget({
      livePolicy: policy,
      targetPolicy: generatedParentFencePolicy(),
      liveDeployment: deployment,
      targetDeployment: generatedBotOrchestratorDeployment()
    });
  if (!valid) return null;
  return {
    namespace: resourceVersionEvidence(namespaceAfter, "durable_parent_namespace"),
    journal: {
      ...resourceVersionEvidence(journalConfigMap, "durable_cutover_journal"),
      operationId: journal.operationId,
      namespaceUid: journal.namespace.uid,
      manifestSha256: journal.manifestSha256
    },
    journalPolicy: resourceVersionEvidence(journalPolicy, "durable_cutover_journal_policy"),
    journalBinding: resourceVersionEvidence(journalBinding, "durable_cutover_journal_binding"),
    deployment: resourceVersionEvidence(deployment, "durable_parent_deployment"),
    policy: resourceVersionEvidence(policy, "durable_parent_policy"),
    binding: resourceVersionEvidence(binding, "durable_parent_binding")
  };
}

function pristineLegacyCutoverLiveEvidence() {
  const liveNamespace = kubectlAbsentOnlyJson([
    "get", "namespace", parentNamespace, "-o", "json"
  ], "parent-namespace");
  const liveDeployment = kubectlAbsentOnlyJson([
    "-n", parentNamespace, "get", "deployment", "bot-orchestrator", "-o", "json"
  ], "bot-orchestrator-deployment");
  const runnerNamespace = kubectlAbsentOnlyJson([
    "get", "namespace", RUNNER_NAMESPACE, "-o", "json"
  ], "runner-namespace");
  const absentResources = {
    parentServiceAccount: ["-n", parentNamespace, "get", "serviceaccount", "bot-orchestrator", "-o", "json"],
    parentRole: ["-n", parentNamespace, "get", "role", "bot-orchestrator-runner-pods", "-o", "json"],
    parentRoleBinding: ["-n", parentNamespace, "get", "rolebinding", "bot-orchestrator-runner-pods", "-o", "json"],
    runnerAdmissionPolicy: ["get", "validatingadmissionpolicy", ADMISSION_POLICY_NAME, "-o", "json"],
    runnerAdmissionBinding: ["get", "validatingadmissionpolicybinding", ADMISSION_POLICY_NAME, "-o", "json"],
    runnerProtocolPolicy: ["get", "validatingadmissionpolicy", RUNNER_PROTOCOL_POLICY_NAME, "-o", "json"],
    runnerProtocolBinding: ["get", "validatingadmissionpolicybinding", RUNNER_PROTOCOL_POLICY_NAME, "-o", "json"],
    cutoverJournalPolicy: ["get", "validatingadmissionpolicy", CUTOVER_JOURNAL_POLICY_NAME, "-o", "json"],
    cutoverJournalBinding: ["get", "validatingadmissionpolicybinding", CUTOVER_JOURNAL_POLICY_NAME, "-o", "json"],
    parentFencePolicy: ["get", "validatingadmissionpolicy", PARENT_FENCE_POLICY_NAME, "-o", "json"],
    parentFenceBinding: ["get", "validatingadmissionpolicybinding", PARENT_FENCE_POLICY_NAME, "-o", "json"],
    recoveryOperationFencePolicy: ["get", "validatingadmissionpolicy", RECOVERY_OPERATION_FENCE_POLICY_NAME, "-o", "json"],
    recoveryOperationFenceBinding: ["get", "validatingadmissionpolicybinding", RECOVERY_OPERATION_FENCE_POLICY_NAME, "-o", "json"]
  };
  const isolatedResources = Object.fromEntries(Object.entries(absentResources).map(
    ([name, args]) => [name, kubectlAbsentOnlyJson(args, `isolated-control-plane-${name}`)]
  ));
  const authority = Object.fromEntries([
    ["parent", parentNamespace],
    ["runner", RUNNER_NAMESPACE]
  ].map(([label, namespace]) => [
    label,
    Object.fromEntries(["create", "delete", "patch"].map(verb => [
      verb,
      canServiceAccount(verb, namespace)
    ]))
  ]));
  return {
    liveNamespace,
    liveDeployment,
    runnerNamespace,
    isolatedResources,
    parentPodList: kubectlJson(["-n", parentNamespace, "get", "pods", "-o", "json"]),
    parentReplicaSetList: kubectlJson([
      "-n", parentNamespace, "get", "replicasets", "-o", "json"
    ]),
    authority
  };
}

function verifyPristineLegacyCutoverEvidence() {
  const attestation = readPrivateCutoverAttestation(
    process.env.PROCESS_LOCAL_CUTOVER_ATTESTATION_PATH
  );
  const key = readPrivateCutoverKey(process.env.PROCESS_LOCAL_CUTOVER_KEY_PATH);
  const evidence = pristineLegacyCutoverLiveEvidence();
  const result = verifyPristineLegacyCutoverGate({
    attestation,
    key,
    namespace: parentNamespace,
    expectedKubeContext: kubectlContext,
    ...evidence
  });
  cutoverKey = key;
  cutoverAttestation = attestation;
  cutoverBaselineEvidence = result;
  cutoverNamespaceUid = result.namespaceUid;
  return result;
}

function verifyCleanInstallCutoverEvidence(expectedNamespaceUid) {
  const evidence = pristineLegacyCutoverLiveEvidence();
  const result = verifyCleanInstallCutoverGate({
    namespace: parentNamespace,
    expectedNamespaceUid,
    ...evidence
  });
  cutoverBaselineEvidence = result;
  cutoverNamespaceUid = result.namespaceUid;
  return result;
}

function loadPrivateCutoverKey() {
  cutoverKey = readPrivateCutoverKey(process.env.PROCESS_LOCAL_CUTOVER_KEY_PATH);
  return cutoverKey;
}

function journalTransitionState(evidence, journalConfigMap, journal) {
  const {
    journalPolicy: targetJournalPolicy,
    journalBinding: targetJournalBinding,
    targetPolicy,
    targetBinding,
    targetDeployment: deploymentMutationResource
  } = cutoverTargetResources();
  const targetDeployment = serverNormalizedCutoverDeployment(
    deploymentMutationResource,
    evidence.liveDeployment
  );
  if (targetDeployment === null) {
    throw new Error("runner_cutover_parent_deployment_server_normalization_failed");
  }
  const policy = evidence.isolatedResources.parentFencePolicy;
  const binding = evidence.isolatedResources.parentFenceBinding;
  const journalPolicy = evidence.isolatedResources.cutoverJournalPolicy;
  const journalBinding = evidence.isolatedResources.cutoverJournalBinding;
  const deploymentIsTarget = liveDeploymentMatchesJournalTarget(
    evidence.liveDeployment,
    targetDeployment,
    journal
  );
  const parentFenceObserved = policy !== null && binding !== null &&
    admissionPolicyIsObserved(policy) &&
    exactParentFenceBinding(binding, parentNamespace) &&
    isDeepStrictEqual(policy.spec, targetPolicy.spec);
  const journalFenceObserved = journalPolicy !== null && journalBinding !== null &&
    admissionPolicyIsObserved(journalPolicy) &&
    exactCutoverJournalBinding(journalBinding, parentNamespace) &&
    isDeepStrictEqual(journalPolicy.spec, targetJournalPolicy.spec);
  return {
    journalConfigMap,
    journalPolicy,
    journalBinding,
    policy,
    binding,
    deployment: evidence.liveDeployment,
    parentFenceObserved,
    journalFenceObserved,
    parentQuiesced: deploymentIsTarget && parentIsQuiesced(
      evidence.liveDeployment,
      evidence.parentPodList,
      evidence.parentReplicaSetList,
      journal.baselineDeployment?.uid
    ),
    journal,
    targetJournalPolicy,
    targetJournalBinding,
    targetPolicy,
    targetBinding,
    targetDeployment,
    deploymentMutationResource
  };
}

function verifyJournalCutoverResumeEvidence(expectedOperationId = null) {
  const evidence = pristineLegacyCutoverLiveEvidence();
  const namespaceUid = evidence.liveNamespace?.metadata?.uid;
  if (!activeCutoverNamespace(evidence.liveNamespace, parentNamespace)) {
    throw new Error("runner_cutover_journal_namespace_invalid");
  }
  const journalConfigMap = readCutoverJournalConfigMap();
  if (journalConfigMap === null) throw new Error("runner_cutover_journal_missing");
  const journal = parseExactCutoverJournalConfigMap(
    journalConfigMap,
    cutoverJournalVerification(namespaceUid)
  );
  if (expectedOperationId !== null && journal.operationId !== expectedOperationId) {
    throw new Error("runner_cutover_journal_operation_replaced");
  }
  if (
    journal.mode === "clean-install" &&
    evidence.liveNamespace.metadata.annotations?.["yenhubs.org/runner-clean-install"] !==
      "fence-aware-bootstrap-v1"
  ) {
    throw new Error("runner_cutover_journal_clean_namespace_marker_missing");
  }
  const state = journalTransitionState(evidence, journalConfigMap, journal);
  const prefix = classifyCutoverJournalPrefix(state);
  const isolatedResources = {
    ...evidence.isolatedResources,
    cutoverJournalPolicy: null,
    cutoverJournalBinding: null,
    parentFencePolicy: null,
    parentFenceBinding: null
  };
  verifyJournalCutoverIsolationGate({
    ...evidence,
    isolatedResources,
    allowParentCandidates: ["P5", "P6"].includes(prefix)
  });
  cutoverJournal = journal;
  cutoverNamespaceUid = namespaceUid;
  return { journal, journalConfigMap, prefix, state };
}

function verifyPristineLegacyCutoverPreflight() {
  pristineLegacyCutoverRequired = false;
  cutoverKey = null;
  cutoverAttestation = null;
  cutoverBaselineEvidence = null;
  cutoverJournal = null;
  cutoverNamespaceUid = null;
  let observedFenceEvidence = null;
  const observedNamespace = kubectlAbsentOnlyJson([
    "get", "namespace", parentNamespace, "-o", "json"
  ], "parent-namespace-preflight");
  const observedDeployment = observedNamespace === null ? null : kubectlAbsentOnlyJson([
    "-n", parentNamespace, "get", "deployment", "bot-orchestrator", "-o", "json"
  ], "bot-orchestrator-preflight");
  const liveActivation =
    observedDeployment?.metadata?.annotations?.["yenhubs.org/runner-activation-phase"] || "legacy";
  if (observedNamespace !== null && liveActivation !== "legacy") {
    observedFenceEvidence = durableParentFenceEvidence();
    if (observedFenceEvidence !== null) {
      cutoverPreflightClassification = "fence-aware";
      cutoverFenceEvidence = observedFenceEvidence;
      return false;
    }
  }
  const observedJournal = observedNamespace === null ? null : readCutoverJournalConfigMap();
  if (observedJournal !== null) {
    if (plan.activationPhase !== "bootstrap") {
      throw new Error("partial_runner_cutover_journal_requires_bootstrap_target");
    }
    loadPrivateCutoverKey();
    const resume = verifyJournalCutoverResumeEvidence();
    cutoverPreflightClassification = `${resume.journal.mode}-journal-resume`;
    pristineLegacyCutoverRequired = resume.journal.mode === "pristine-cutover";
    cutoverFenceEvidence = null;
    return pristineLegacyCutoverRequired;
  }
  const classification = executeCutoverPreflight({
    targetActivation: plan.activationPhase,
    readParentNamespace: () => observedNamespace,
    readParentDeployment: () => observedDeployment,
    readDurableParentPolicyObserved: () => {
      observedFenceEvidence = durableParentFenceEvidence();
      return observedFenceEvidence !== null;
    },
    verifyPristineEvidence: verifyPristineLegacyCutoverEvidence,
    verifyCleanEvidence: uid => verifyCleanInstallCutoverEvidence(uid),
    verifyCleanCapability: loadPrivateCutoverKey
  });
  pristineLegacyCutoverRequired = classification === "pristine-cutover";
  cutoverPreflightClassification = classification;
  cutoverFenceEvidence = classification === "fence-aware" ? observedFenceEvidence : null;
  if (classification === "clean-install-resume") {
    cutoverNamespaceUid = verifyCleanInstallCutoverEvidence().namespaceUid;
  }
  return pristineLegacyCutoverRequired;
}

function verifyEmergencyRefencePreflight() {
  const emergencyNamespace = kubectlAbsentOnlyJson([
    "get", "namespace", parentNamespace, "-o", "json"
  ], "emergency-parent-namespace-preflight");
  if (emergencyNamespace !== null && readCutoverJournalConfigMap() !== null) {
    const emergencyDeployment = kubectlAbsentOnlyJson([
      "-n", parentNamespace, "get", "deployment", "bot-orchestrator", "-o", "json"
    ], "emergency-bot-orchestrator-journal-preflight");
    const emergencyActivation =
      emergencyDeployment?.metadata?.annotations?.["yenhubs.org/runner-activation-phase"] || "legacy";
    if (emergencyActivation === "legacy" || durableParentFenceEvidence() === null) {
      throw new Error("emergency_refence_blocked_by_partial_runner_cutover_journal");
    }
  }
  let observedFenceEvidence = null;
  const classification = executeCutoverPreflight({
    targetActivation: "active",
    readParentNamespace: () => emergencyNamespace,
    readParentDeployment: () => kubectlAbsentOnlyJson([
      "-n", parentNamespace, "get", "deployment", "bot-orchestrator", "-o", "json"
    ], "emergency-bot-orchestrator-preflight"),
    readDurableParentPolicyObserved: () => {
      observedFenceEvidence = durableParentFenceEvidence();
      return observedFenceEvidence !== null;
    },
    verifyPristineEvidence: () => {
      throw new Error("emergency_refence_cannot_perform_first_pristine_cutover");
    },
    verifyCleanEvidence: () => {
      throw new Error("emergency_refence_requires_fence_aware_live_control_plane");
    }
  });
  if (classification !== "fence-aware") {
    throw new Error("emergency_refence_requires_fence_aware_live_control_plane");
  }
  cutoverPreflightClassification = classification;
  cutoverFenceEvidence = observedFenceEvidence;
}

function verifyPristineLegacyCutoverUnderLease() {
  assertOperationLeaseHeld();
  if (!pristineLegacyCutoverRequired) return false;
  verifyPristineLegacyCutoverEvidence();
  assertOperationLeaseHeld();
  return true;
}

function verifyCutoverPreflightUnderLease() {
  assertOperationLeaseHeld();
  if (cutoverPreflightClassification?.endsWith("-journal-resume")) {
    verifyJournalCutoverResumeEvidence(cutoverJournal?.operationId || null);
    assertOperationLeaseHeld();
    return;
  }
  executeCutoverRevalidation({
    classification: cutoverPreflightClassification,
    expectedFenceEvidence: cutoverFenceEvidence,
    verifyPristineEvidence: verifyPristineLegacyCutoverEvidence,
    verifyCleanEvidence: () => verifyCleanInstallCutoverEvidence(cutoverNamespaceUid),
    readFenceEvidence: durableParentFenceEvidence
  });
  assertOperationLeaseHeld();
}

function liveRunnerControlPlaneErrors(generatedResources = plan.resources) {
  const liveResources = collectLiveRunnerControlPlane(kubectlJson, parentNamespace);
  return verifyLiveRunnerControlPlane(liveResources, generatedResources, parentNamespace);
}

function liveRunnerControlPlaneIsExact() {
  return liveRunnerControlPlaneErrors().length === 0;
}

function canServiceAccount(verb, namespace) {
  const username = `system:serviceaccount:${parentNamespace}:bot-orchestrator`;
  const result = runLeaseGuardedRead(() => spawnSync(
    "kubectl",
    contextArgs(["auth", "can-i", verb, "pods", "-n", namespace, `--as=${username}`]),
    { encoding: "utf8", timeout: kubectlReadTimeoutMs }
  ));
  const answer = `${result.stdout || ""}`.trim();
  if (result.status === 0 && answer === "yes") return true;
  if (result.status === 1 && answer === "no") return false;
  throw new Error(`subject_access_review_failed:${verb}:${namespace}:${result.status}`);
}

function exactRunnerAuthority(expectedInRunnerNamespace) {
  const podMutationAuthorityIsExact = ["create", "delete"].every(verb =>
    canServiceAccount(verb, parentNamespace) === false &&
    canServiceAccount(verb, RUNNER_NAMESPACE) === expectedInRunnerNamespace
  );
  return podMutationAuthorityIsExact && liveEffectiveRbacIsExact(expectedInRunnerNamespace);
}

function liveEffectiveRbacErrors(runnerAuthorityEnabled) {
  const reviews = new Map();
  for (const spec of effectiveRbacReviewSpecs(parentNamespace, runnerAuthorityEnabled)) {
    const result = runLeaseGuardedRead(() => spawnSync(
      "kubectl",
      contextArgs([
        "create",
        "--raw", "/apis/authorization.k8s.io/v1/selfsubjectrulesreviews",
        "-f", "-",
        `--as=${spec.username}`
      ]),
      {
        input: JSON.stringify(selfSubjectRulesReviewRequest(spec.namespace)),
        encoding: "utf8",
        timeout: kubectlReadTimeoutMs
      }
    ));
    if (result.status !== 0) {
      return [`${spec.id}:selfsubjectrulesreview_request_failed`];
    }
    try {
      reviews.set(spec.id, JSON.parse(result.stdout));
    } catch (_error) {
      return [`${spec.id}:selfsubjectrulesreview_response_invalid`];
    }
  }
  return verifyEffectiveRbacReviews(reviews, parentNamespace, runnerAuthorityEnabled);
}

function liveEffectiveRbacIsExact(runnerAuthorityEnabled) {
  return liveEffectiveRbacErrors(runnerAuthorityEnabled).length === 0;
}

function listPodsBySelector(namespace, selector) {
  const args = ["-n", namespace, "get", "pods"];
  if (selector) args.push("-l", selector);
  args.push("-o", "json");
  const result = runLeaseGuardedRead(() => spawnSync(
    "kubectl",
    contextArgs(args),
    { encoding: "utf8", timeout: kubectlReadTimeoutMs }
  ));
  if (result.status === 0) return JSON.parse(result.stdout);
  if (`${result.stderr || ""}`.includes("NotFound")) return { kind: "PodList", items: [] };
  throw new Error(`runner_pod_list_failed:${namespace}:${result.status}`);
}

function runnerRuntimeIsQuiesced() {
  const parentPods = uniqueRunnerPods([
    listPodsBySelector(parentNamespace, "app=bot-runner"),
    listPodsBySelector(parentNamespace, "component=bot-runner"),
    listPodsBySelector(parentNamespace, "yenhubs.org/managed-by=bot-orchestrator")
  ]);
  if (!Array.isArray(parentPods) || parentPods.length !== 0) return false;
  const inventory = completeRunnerNamespaceInventory(
    listPodsBySelector(RUNNER_NAMESPACE, "")
  );
  return inventory.runners.size === 0 && inventory.intents.size === 0;
}

function deletePodByExactUid(pod, { requireResourceVersion = false } = {}) {
  const namespace = pod?.metadata?.namespace;
  const name = pod?.metadata?.name;
  const uid = pod?.metadata?.uid;
  const resourceVersion = pod?.metadata?.resourceVersion;
  if (
    ![parentNamespace, RUNNER_NAMESPACE].includes(namespace) ||
    typeof name !== "string" || !name ||
    typeof uid !== "string" || !uid ||
    (requireResourceVersion && (typeof resourceVersion !== "string" || !resourceVersion))
  ) {
    throw new Error("runner_pod_delete_identity_invalid");
  }
  const path = `/api/v1/namespaces/${encodeURIComponent(namespace)}/pods/${encodeURIComponent(name)}`;
  const deleteOptions = {
    apiVersion: "v1",
    kind: "DeleteOptions",
    preconditions: {
      uid,
      ...(requireResourceVersion ? { resourceVersion } : {})
    },
    propagationPolicy: "Background"
  };
  const deleted = runLeaseGuardedMutation(
    assertOperationLeaseHeld,
    () => spawnSync(
      "kubectl",
      contextArgs(["--request-timeout=30s", "delete", "--raw", path, "-f", "-"]),
      {
        input: JSON.stringify(deleteOptions),
        encoding: "utf8",
        timeout: MUTATION_TIMEOUT_MS
      }
    )
  );
  if (deleted.status !== 0 && !`${deleted.stderr || ""}`.includes("NotFound")) {
    const error = new Error(`runner_pod_uid_delete_failed:${deleted.status}`);
    const diagnostic = `${deleted.stdout || ""}\n${deleted.stderr || ""}`;
    error.conflict = /\bConflict\b|object was modified|precondition.*failed/iu.test(diagnostic);
    throw error;
  }
}

async function deleteAllRunnerPodsByExactUid() {
  const parentPods = uniqueRunnerPods([
    listPodsBySelector(parentNamespace, "app=bot-runner"),
    listPodsBySelector(parentNamespace, "component=bot-runner"),
    listPodsBySelector(parentNamespace, "yenhubs.org/managed-by=bot-orchestrator")
  ]);
  if (!Array.isArray(parentPods)) throw new Error("runner_parent_pod_lists_invalid");
  const failures = await runBestEffortFenceSteps(parentPods.map(pod => ({
    name: `pod:${pod?.metadata?.namespace || "invalid"}/${pod?.metadata?.name || "invalid"}`,
    action: async () => deletePodByExactUid(pod)
  })));
  try {
    await reconcileRunnerNamespace({
      listPods: async () => listPodsBySelector(RUNNER_NAMESPACE, ""),
      getPod: async name => kubectlOptionalJson([
        "-n", RUNNER_NAMESPACE, "get", "pod", name, "-o", "json"
      ]),
      createPod: async document => {
        const created = runLeaseGuardedMutation(
          assertOperationLeaseHeld,
          () => spawnSync(
            "kubectl",
            contextArgs(["--request-timeout=30s", "create", "-f", "-", "-o", "json"]),
            {
              input: JSON.stringify(document),
              encoding: "utf8",
              timeout: MUTATION_TIMEOUT_MS
            }
          )
        );
        if (created.status !== 0) {
          const error = new Error(`runner_fence_create_failed:${created.status}`);
          error.status = created.status;
          throw error;
        }
        return JSON.parse(created.stdout);
      },
      deletePodByUid: async (pod, options) => deletePodByExactUid(pod, options),
      sleep
    });
  } catch (error) {
    failures.push(`runner-namespace:${error.message}`);
  }
  return failures;
}

function parentAndRunnerPodsAreAbsent() {
  return liveParentIsQuiesced() && runnerRuntimeIsQuiesced();
}

function podListForWatch(namespace) {
  return kubectlJson([
    "--request-timeout=30s", "get", "--raw", podListRawPath(namespace)
  ]);
}

function podListHasForbidden(
  namespace,
  podList,
  { includeRecoveryConsumers = false, recoveryConsumerReplicaSets = [] } = {}
) {
  return completeWatchListResourceVersion(podList, "PodList", "v1") === null ||
    podList.items.some(pod => {
      const normalizedPod = namespacedListItemWithTypeMeta(pod, "Pod", "v1", namespace);
      return !normalizedPod ||
      forbiddenPod(namespace, parentNamespace, normalizedPod, {
        includeRecoveryConsumers,
        recoveryConsumerReplicaSets
      });
    });
}

function startPodWatch(
  namespace,
  resourceVersion,
  { includeRecoveryConsumers = false, recoveryConsumerReplicaSets = [] } = {}
) {
  assertOperationLeaseHeld();
  const evidence = new PodWatchEvidence(namespace, parentNamespace, resourceVersion, {
    includeRecoveryConsumers,
    recoveryConsumerReplicaSets
  });
  const rawPath = podWatchRawPath(namespace, resourceVersion, watchServerTimeoutSeconds);
  return {
    ...startRawEvidenceWatch(rawPath, evidence),
    resource: "pods",
    namespace,
    watchOptions: { includeRecoveryConsumers, recoveryConsumerReplicaSets }
  };
}

function startReplicaSetWatch(resourceVersion, recoveryConsumerReplicaSets) {
  assertOperationLeaseHeld();
  const evidence = new ReplicaSetWatchEvidence(
    resourceVersion,
    recoveryConsumerReplicaSets,
    { initialEventsEnded: true, namespace: parentNamespace }
  );
  const rawPath = replicaSetWatchRawPath(
    parentNamespace,
    resourceVersion,
    watchServerTimeoutSeconds
  );
  return {
    ...startRawEvidenceWatch(rawPath, evidence),
    resource: "replicasets",
    namespace: parentNamespace,
    watchOptions: { recoveryConsumerReplicaSets }
  };
}

function startRawEvidenceWatch(rawPath, evidence) {
  const requestTimeoutSeconds = watchServerTimeoutSeconds + watchProcessGraceSeconds;
  return startEvidenceWatchProcess({
    spawnProcess: spawn,
    command: "kubectl",
    args: contextArgs([
      `--request-timeout=${requestTimeoutSeconds}s`, "get", "--raw", rawPath
    ]),
    evidence,
    serverTimeoutSeconds: watchServerTimeoutSeconds,
    processGraceSeconds: watchProcessGraceSeconds,
    maximumBufferBytes: maxWatchBufferBytes
  });
}

function startBookmarkedSuccessorWatch(predecessor) {
  const resourceVersion = predecessor?.evidence?.lastBookmarkResourceVersion;
  if (typeof resourceVersion !== "string" || !resourceVersion || resourceVersion === "0") {
    throw new Error("watch_successor_bookmark_invalid");
  }
  if (predecessor.resource === "pods") {
    const options = predecessor.watchOptions || {};
    return startPodWatch(
      predecessor.namespace,
      resourceVersion,
      options
    );
  }
  if (predecessor.resource === "replicasets") {
    return startReplicaSetWatch(
      resourceVersion,
      predecessor.watchOptions?.recoveryConsumerReplicaSets || []
    );
  }
  throw new Error("watch_successor_resource_invalid");
}

async function waitForInitialBookmarkedWatchBoundary(watches, deadline) {
  return await waitForBookmarkedWatchBoundary({
    successors: watches,
    startingBookmarkSequences: watches.map(watch => watch.bookmarkBaseline),
    deadline,
    assertHealthy: assertOperationLeaseProcessHealthy,
    sleep
  });
}

async function confirmWatchBoundary(
  predecessors,
  boundaryLists,
  deadline,
  { includeRecoveryConsumers = false, recoveryConsumerReplicaSets = [] } = {}
) {
  const podPredecessors = predecessors.filter(watch => watch.resource === "pods");
  if (
    !Array.isArray(boundaryLists) || boundaryLists.length !== podPredecessors.length ||
    boundaryLists.some((podList, index) =>
      podListHasForbidden(
        podPredecessors[index].namespace,
        podList,
        podPredecessors[index].watchOptions || {
          includeRecoveryConsumers,
          recoveryConsumerReplicaSets
        }
      )
    )
  ) return null;
  if (includeRecoveryConsumers) {
    const boundaryReplicaSets = recoveryReplicaSetEvidence();
    if (!boundaryReplicaSets.valid) return null;
    for (const replicaSet of boundaryReplicaSets.consumerReplicaSets) {
      if (!recoveryConsumerReplicaSets.some(value =>
        value?.metadata?.uid === replicaSet?.metadata?.uid ||
        value?.metadata?.name === replicaSet?.metadata?.name
      )) {
        recoveryConsumerReplicaSets.push(replicaSet);
      }
    }
  }
  return await replaceWithBookmarkedSuccessors({
    predecessors,
    startSuccessor: startBookmarkedSuccessorWatch,
    deadline,
    assertHealthy: assertOperationLeaseProcessHealthy,
    sleep
  });
}

function recoveryReplicaSetEvidence() {
  const replicaSetList = kubectlJson([
    "--request-timeout=30s",
    "get",
    "--raw",
    replicaSetListRawPath(parentNamespace)
  ]);
  const resourceVersion = completeWatchListResourceVersion(
    replicaSetList,
    "ReplicaSetList",
    "apps/v1"
  );
  const normalizedReplicaSets = Array.isArray(replicaSetList?.items)
    ? replicaSetList.items.map(replicaSet => namespacedListItemWithTypeMeta(
      replicaSet,
      "ReplicaSet",
      "apps/v1",
      parentNamespace
    ))
    : [];
  const consumerReplicaSets = normalizedReplicaSets.every(Boolean)
    ? recoveryConsumerReplicaSets({ ...replicaSetList, items: normalizedReplicaSets })
    : null;
  return {
    consumerReplicaSets,
    resourceVersion,
    valid: resourceVersion !== null &&
      Array.isArray(consumerReplicaSets) &&
      recoveryConsumerReplicaSetsAreStopped(consumerReplicaSets)
  };
}

async function acquireStablePodAbsenceMonitor(
  label,
  { includeRecoveryConsumers = false } = {}
) {
  const deadline = Date.now() + stableWatchAcquisitionTimeoutMs;
  while (Date.now() < deadline) {
    const replicaSetEvidence = includeRecoveryConsumers
      ? recoveryReplicaSetEvidence()
      : { consumerReplicaSets: [], resourceVersion: null, valid: true };
    if (!replicaSetEvidence.valid) {
      await sleep(1_000);
      continue;
    }
    const watchOptions = {
      includeRecoveryConsumers,
      recoveryConsumerReplicaSets: replicaSetEvidence.consumerReplicaSets
    };
    const initialLists = [podListForWatch(parentNamespace), podListForWatch(RUNNER_NAMESPACE)];
    if (
      initialLists.some((podList, index) =>
        podListHasForbidden(
          [parentNamespace, RUNNER_NAMESPACE][index],
          podList,
          watchOptions
        )
      )
    ) {
      await sleep(1_000);
      continue;
    }
    const successors = await withOwnedEvidenceWatches({
      start(watches) {
        for (const [index, podList] of initialLists.entries()) {
          watches.push(startPodWatch(
            [parentNamespace, RUNNER_NAMESPACE][index],
            podList?.metadata?.resourceVersion,
            watchOptions
          ));
        }
        if (includeRecoveryConsumers) {
          watches.push(startReplicaSetWatch(
            replicaSetEvidence.resourceVersion,
            replicaSetEvidence.consumerReplicaSets
          ));
        }
      },
      async attempt(watches) {
        if (!(await waitForInitialBookmarkedWatchBoundary(
          watches,
          Math.min(deadline, Date.now() + waitTimeoutMs)
        ))) return null;
        const stableUntil = Date.now() + stablePodAbsenceWindowMs;
        while (
          Date.now() < stableUntil &&
          Date.now() < deadline &&
          watches.every(watch => !watch.evidence.error && !watch.evidence.violation)
        ) {
          assertOperationLeaseProcessHealthy();
          await sleep(100);
        }
        if (
          Date.now() >= deadline ||
          watches.some(watch => watch.evidence.error || watch.evidence.violation)
        ) return null;
        const boundaryLists = [
          podListForWatch(parentNamespace),
          podListForWatch(RUNNER_NAMESPACE)
        ];
        if (boundaryLists.some((podList, index) =>
          podListHasForbidden(
            [parentNamespace, RUNNER_NAMESPACE][index],
            podList,
            watchOptions
          )
        )) return null;
        return await confirmWatchBoundary(
          watches,
          boundaryLists,
          Math.min(deadline, Date.now() + waitTimeoutMs),
          watchOptions
        );
      }
    });
    if (successors) return { watches: successors, watchOptions };
    await sleep(1_000);
  }
  throw new Error(`${label}_timeout`);
}

async function runWithStablePodAbsence(
  label,
  action,
  rollback,
  { includeRecoveryConsumers = false } = {}
) {
  const monitor = await acquireStablePodAbsenceMonitor(label, { includeRecoveryConsumers });
  let { watches } = monitor;
  const { watchOptions } = monitor;
  try {
    await action();
    const boundaryLists = [podListForWatch(parentNamespace), podListForWatch(RUNNER_NAMESPACE)];
    const forbiddenAtBoundary = boundaryLists.some((podList, index) =>
      podListHasForbidden(
        [parentNamespace, RUNNER_NAMESPACE][index],
        podList,
        watchOptions
      )
    );
    const successors = forbiddenAtBoundary
      ? null
      : await confirmWatchBoundary(
        watches,
        boundaryLists,
        Date.now() + waitTimeoutMs,
        watchOptions
      );
    if (!successors) {
      throw new Error(`${label}_event_or_watch_failure`);
    }
    watches = successors;
  } catch (error) {
    if (rollback) await rollback();
    throw error;
  } finally {
    await stopEvidenceWatches(watches);
  }
}

async function waitForStableFirstCutoverParentAbsence(label, finalPredicate) {
  const deadline = Date.now() + stableWatchAcquisitionTimeoutMs;
  while (Date.now() < deadline) {
    const initial = podListForWatch(parentNamespace);
    if (podListHasForbidden(parentNamespace, initial) || !finalPredicate()) {
      await sleep(1_000);
      continue;
    }
    let watch = startPodWatch(parentNamespace, initial.metadata?.resourceVersion);
    try {
      if (!(await waitForInitialBookmarkedWatchBoundary(
        [watch],
        Math.min(deadline, Date.now() + waitTimeoutMs)
      ))) continue;
      const stableUntil = Date.now() + stablePodAbsenceWindowMs;
      while (
        Date.now() < stableUntil &&
        Date.now() < deadline &&
        !watch.evidence.error &&
        !watch.evidence.violation
      ) {
        assertOperationLeaseProcessHealthy();
        await sleep(100);
      }
      if (Date.now() >= deadline || watch.evidence.error || watch.evidence.violation) continue;
      const boundary = podListForWatch(parentNamespace);
      const successors = (
        podListHasForbidden(parentNamespace, boundary) ||
        !finalPredicate()
      ) ? null : await confirmWatchBoundary(
        [watch],
        [boundary],
        Math.min(deadline, Date.now() + waitTimeoutMs)
      );
      if (!successors) continue;
      watch = successors[0];
      if (!finalPredicate()) continue;
      return;
    } finally {
      await stopEvidenceWatches([watch]);
    }
  }
  throw new Error(`${label}_timeout`);
}

function liveRecoveryConsumersAreQuiesced() {
  const deployments = kubectlJson([
    "-n", parentNamespace, "get", "deployments", "-o", "json"
  ]);
  const pods = kubectlJson([
    "-n", parentNamespace, "get", "pods", "-o", "json"
  ]);
  const replicaSets = kubectlJson([
    "-n", parentNamespace, "get", "replicasets", "-o", "json"
  ]);
  return recoveryConsumersAreQuiesced(deployments, pods, replicaSets);
}

function literalEnvMap(deployment) {
  const container = deployment?.spec?.template?.spec?.containers?.find(
    value => value?.name === "bot-orchestrator"
  );
  return new Map(
    (container?.env || [])
      .filter(entry => typeof entry?.name === "string" && typeof entry?.value === "string")
      .map(entry => [entry.name, entry.value])
  );
}

function admissionProbePod() {
  const deployment = plan.resources.find(
    resource => resource?.kind === "Deployment" && resource?.metadata?.name === "bot-orchestrator"
  );
  const env = literalEnvMap(deployment);
  const runnerEnvironmentNames = [
    "GHOST_FEATURED_FETCH_TIMEOUT_MS",
    "GHOST_FEATURED_MAX_BYTES",
    "GHOST_FEATURED_MAX_ENTRIES",
    "GHOST_FEATURED_MAX_REDIRECTS",
    "GHOST_FEATURED_MAX_REFS",
    "GHOST_NAVIGATION_MODE",
    "GHOST_NAVIGATION_RECOVERY_RESTART_MS",
    "GHOST_NAVIGATION_REQUIRE_NAVMESH",
    "GHOST_NAVMESH_MAX_ROUTE_POINTS",
    "GHOST_NAVMESH_MAX_SNAP_DISTANCE_M",
    "GHOST_NAVMESH_MAX_TRIANGLES",
    "GHOST_RAYCAST_MODE",
    "GHOST_SCENE_FETCH_TIMEOUT_MS",
    "GHOST_SCENE_MAX_BYTES",
    "GHOST_SCENE_MAX_EDGES",
    "GHOST_SCENE_MAX_JSON_BYTES",
    "GHOST_SCENE_MAX_NODES",
    "GHOST_SPAWN_RECOVERY_RESTART_MS"
  ];
  const runnerEnvironment = Object.fromEntries(
    runnerEnvironmentNames.map(name => {
      const value = env.get(name);
      if (typeof value !== "string") throw new Error(`admission_probe_env_missing:${name}`);
      return [name, value];
    })
  );
  const manager = new KubernetesRunnerManager({
    api: {},
    namespace: RUNNER_NAMESPACE,
    parentNamespace,
    ownerPodName: "admission-probe-parent",
    ownerPodUid: "admission-probe-parent-uid",
    runnerImage: env.get("BOT_RUNNER_IMAGE"),
    hubsBaseUrl: env.get("HUBS_BASE_URL"),
    controlUrl: env.get("RUNNER_CONTROL_URL"),
    credentialKey: "0".repeat(32),
    runnerEnvironment,
    tokenTtlSeconds: 3600,
    tokenFactory: () => `v1.e30.${"A".repeat(43)}`,
    now: () => Date.UTC(2030, 0, 1)
  });
  return manager.podDocument(manager.identity(
    "admission-probe",
    "55555555-5555-4555-8555-555555555555"
  ));
}

function admissionDenialProbe() {
  const username = `system:serviceaccount:${parentNamespace}:bot-orchestrator`;
  const result = runLeaseGuardedRead(() => spawnSync(
    "kubectl",
    contextArgs(["create", "--dry-run=server", "-f", "-", `--as=${username}`]),
    {
      input: JSON.stringify(admissionProbePod()),
      encoding: "utf8",
      timeout: kubectlReadTimeoutMs
    }
  ));
  const diagnostic = `${result.stdout || ""}\n${result.stderr || ""}`;
  return result.status !== 0 &&
    diagnostic.includes(ADMISSION_POLICY_NAME) &&
    diagnostic.includes("phase-bound parent or shape-limited recovery operator principal") &&
    !diagnostic.includes("violates PodSecurity");
}

function recoveryOperationParentWriterProbePod() {
  return {
    apiVersion: "v1",
    kind: "Pod",
    metadata: {
      generateName: "yenhubs-recovery-operation-writer-probe-",
      namespace: parentNamespace,
      labels: { app: "reticulum" }
    },
    spec: {
      automountServiceAccountToken: false,
      enableServiceLinks: false,
      restartPolicy: "Never",
      terminationGracePeriodSeconds: 0,
      securityContext: {
        runAsNonRoot: true,
        runAsUser: 10001,
        runAsGroup: 10001,
        seccompProfile: { type: "RuntimeDefault" }
      },
      containers: [{
        name: "reticulum",
        image: "registry.k8s.io/pause:3.10",
        imagePullPolicy: "IfNotPresent",
        securityContext: {
          runAsNonRoot: true,
          runAsUser: 10001,
          runAsGroup: 10001,
          allowPrivilegeEscalation: false,
          readOnlyRootFilesystem: true,
          capabilities: { drop: ["ALL"] },
          seccompProfile: { type: "RuntimeDefault" }
        },
        resources: {
          requests: { cpu: "1m", memory: "1Mi" },
          limits: { cpu: "1m", memory: "1Mi" }
        }
      }]
    }
  };
}

function recoveryOperationRunnerProbePod() {
  const identity = {
    roomKey: "55555555555555555555",
    processGeneration: randomUUID()
  };
  identity.name = `bot-runner-${identity.roomKey.substring(0, 16)}-${identity.processGeneration.substring(0, 8)}`;
  return guardPodDocumentForIdentity(identity, "fence", RUNNER_NAMESPACE);
}

function recoveryOperationFenceDryRun(pod) {
  return runLeaseGuardedRead(() => spawnSync(
    "kubectl",
    contextArgs(["create", "--dry-run=server", "-f", "-"]),
    {
      input: JSON.stringify(pod),
      encoding: "utf8",
      timeout: kubectlReadTimeoutMs
    }
  ));
}

function recoveryOperationFenceDiagnostic(result) {
  return `${result?.stdout || ""}\n${result?.stderr || ""}`;
}

function recoveryOperationFenceDenialProbe() {
  const probes = [
    [
      recoveryOperationParentWriterProbePod(),
      "recovery operation Pod fence denies database-writer Pod creation while checkpoint or restore is fenced"
    ],
    [
      recoveryOperationRunnerProbePod(),
      "recovery operation Pod fence denies runner Pod mutation while checkpoint or restore is fenced"
    ]
  ];
  return probes.every(([pod, message]) => {
    const result = recoveryOperationFenceDryRun(pod);
    const diagnostic = recoveryOperationFenceDiagnostic(result);
    return result.status !== 0 &&
      diagnostic.includes(RECOVERY_OPERATION_FENCE_POLICY_NAME) &&
      diagnostic.includes(message) &&
      !diagnostic.includes("violates PodSecurity");
  });
}

function recoveryOperationFenceInactiveProbe() {
  const parentResult = recoveryOperationFenceDryRun(recoveryOperationParentWriterProbePod());
  const runnerResult = recoveryOperationFenceDryRun(recoveryOperationRunnerProbePod());
  const diagnostic = [parentResult, runnerResult]
    .map(recoveryOperationFenceDiagnostic)
    .join("\n");
  return parentResult.status === 0 &&
    runnerResult.status === 0 &&
    !diagnostic.includes(RECOVERY_OPERATION_FENCE_POLICY_NAME) &&
    !diagnostic.includes("recovery operation Pod fence denies");
}

function liveRecoveryOperationFenceBinding() {
  return kubectlAbsentOnlyJson([
    "get", "validatingadmissionpolicybinding", RECOVERY_OPERATION_FENCE_POLICY_NAME, "-o", "json"
  ], "recovery-operation-fence-binding-transition");
}

function transitionableRecoveryOperationFenceBinding(binding) {
  const metadata = binding?.metadata;
  const annotations = metadata?.annotations || {};
  const allowedMetadata = new Set([
    "annotations",
    "creationTimestamp",
    "generation",
    "managedFields",
    "name",
    "resourceVersion",
    "uid"
  ]);
  return metadata &&
    Object.keys(metadata).every(key => allowedMetadata.has(key)) &&
    Object.keys(annotations).every(key => key === "kubectl.kubernetes.io/last-applied-configuration") &&
    (metadata.labels === undefined || Object.keys(metadata.labels).length === 0) &&
    metadata.deletionTimestamp === undefined &&
    metadata.finalizers === undefined &&
    typeof metadata.uid === "string" && metadata.uid.length > 0 &&
    typeof metadata.resourceVersion === "string" && metadata.resourceVersion.length > 0 &&
    (
      exactRecoveryOperationFenceBinding(binding, parentNamespace, { active: false }) ||
      exactRecoveryOperationFenceBinding(binding, parentNamespace, { active: true })
    );
}

function recoveryOperationFenceCanBecomeDormant() {
  return liveRecoveryConsumersAreQuiesced() &&
    liveParentIsQuiesced() &&
    runnerRuntimeIsQuiesced() &&
    exactRunnerAuthority(false);
}

function transitionRecoveryOperationFenceBinding(active, label, { allowCreate = false } = {}) {
  const before = liveRecoveryOperationFenceBinding();
  if (before === null) {
    if (!allowCreate) throw new Error(`${label}_binding_missing`);
    createCutoverResource(recoveryOperationFenceBinding(active), `${label}_binding`);
    const created = liveRecoveryOperationFenceBinding();
    if (
      !transitionableRecoveryOperationFenceBinding(created) ||
      !exactRecoveryOperationFenceBinding(created, parentNamespace, { active })
    ) {
      throw new Error(`${label}_binding_create_unconfirmed`);
    }
    return created;
  }
  if (!transitionableRecoveryOperationFenceBinding(before)) {
    throw new Error(`${label}_binding_source_not_exact`);
  }
  if (
    !active &&
    exactRecoveryOperationFenceBinding(before, parentNamespace, { active: true }) &&
    !recoveryOperationFenceCanBecomeDormant()
  ) {
    throw new Error(`${label}_binding_dormancy_requires_quiescence`);
  }
  if (!exactRecoveryOperationFenceBinding(before, parentNamespace, { active })) {
    const replacement = recoveryOperationFenceBinding(active);
    replacement.metadata = {
      ...replacement.metadata,
      uid: before.metadata.uid,
      resourceVersion: before.metadata.resourceVersion
    };
    const replaced = runLeaseGuardedMutation(
      assertOperationLeaseHeld,
      () => spawnSync(
        "kubectl",
        contextArgs(["--request-timeout=30s", "replace", "-f", "-"]),
        {
          input: JSON.stringify(replacement),
          encoding: "utf8",
          timeout: MUTATION_TIMEOUT_MS
        }
      )
    );
    if (replaced.status !== 0 && replaced.status !== null) {
      throw new Error(`${label}_binding_compare_and_swap_failed:${replaced.status}`);
    }
    const after = liveRecoveryOperationFenceBinding();
    if (
      !transitionableRecoveryOperationFenceBinding(after) ||
      after.metadata.uid !== before.metadata.uid ||
      !exactRecoveryOperationFenceBinding(after, parentNamespace, { active }) ||
      after.metadata.resourceVersion === before.metadata.resourceVersion
    ) {
      throw new Error(`${label}_binding_compare_and_swap_unconfirmed`);
    }
    return after;
  }
  return before;
}

async function setRecoveryOperationFenceBinding(active, label, options = {}) {
  const expectedBinding = transitionRecoveryOperationFenceBinding(active, label, options);
  await waitFor(
    `${label}_binding_and_${active ? "denial" : "inactive"}_probe_observed`,
    () => {
      if (!liveRecoveryOperationFenceAdmissionIsObserved({ active, expectedBinding })) {
        return false;
      }
      const probeAccepted = active
        ? recoveryOperationFenceDenialProbe()
        : recoveryOperationFenceInactiveProbe();
      return probeAccepted &&
        liveRecoveryOperationFenceAdmissionIsObserved({ active, expectedBinding });
    }
  );
  return expectedBinding;
}

async function ensureRecoveryOperationFenceBindingDormant(label) {
  await setRecoveryOperationFenceBinding(false, label, { allowCreate: true });
}

async function waitForAdmissionDenialProbe() {
  await waitFor("runner_admission_denial_probe", admissionDenialProbe);
}

function applyResource(resource) {
  const startsRunnerParent = resource?.apiVersion === "apps/v1" &&
    resource?.kind === "Deployment" &&
    resource?.metadata?.namespace === parentNamespace &&
    resource?.metadata?.name === "bot-orchestrator" &&
    Number(resource?.spec?.replicas || 0) > 0;
  if (startsRunnerParent && !exactRunnerAuthority(true)) {
    throw new Error("effective_rbac_not_exact_before_parent_start");
  }
  const applied = runLeaseGuardedMutation(
    assertOperationLeaseHeld,
    () => spawnSync(
      "kubectl",
      contextArgs(["--request-timeout=30s", "apply", "-f", "-"]),
      {
        input: JSON.stringify(resource),
        stdio: ["pipe", "inherit", "inherit"],
        timeout: MUTATION_TIMEOUT_MS
      }
    )
  );
  if (applied.status !== 0) throw new Error(`kubectl_resource_apply_failed:${applied.status}`);
  if (startsRunnerParent && !exactRunnerAuthority(true)) {
    throw new Error("effective_rbac_changed_during_parent_start");
  }
}

function applyNamedResources(identities) {
  for (const [kind, name, namespace] of identities) {
    const resource = plan.resources.find(value =>
      value?.kind === kind &&
      value?.metadata?.name === name &&
      (value?.metadata?.namespace || "") === (namespace || "")
    );
    if (!resource) throw new Error(`generated_manifest_resource_missing:${kind}:${name}`);
    applyResource(resource);
  }
}

async function prepareRecoveryAdmissionFence() {
  await prepareParentFenceAdmission();
  applyNamedResources([
    ["Namespace", RUNNER_NAMESPACE, ""],
    ["ServiceAccount", "bot-runner-guard", RUNNER_NAMESPACE],
    ["ResourceQuota", "bot-runner-guard-capacity", RUNNER_NAMESPACE]
  ]);
  applyResource(recoveryAdmissionPolicy());
  applyNamedResources([
    ["ValidatingAdmissionPolicyBinding", ADMISSION_POLICY_NAME, ""],
    ["ValidatingAdmissionPolicy", RUNNER_PROTOCOL_POLICY_NAME, ""],
    ["ValidatingAdmissionPolicyBinding", RUNNER_PROTOCOL_POLICY_NAME, ""],
    ["ValidatingAdmissionPolicy", RECOVERY_OPERATION_FENCE_POLICY_NAME, ""]
  ]);
  await waitFor("runner_recovery_admission_observed", liveRecoveryAdmissionIsObserved);
}

async function prepareParentFenceAdmission() {
  applyNamedResources([
    ["ValidatingAdmissionPolicy", PARENT_FENCE_POLICY_NAME, ""],
    ["ValidatingAdmissionPolicyBinding", PARENT_FENCE_POLICY_NAME, ""]
  ]);
  await waitFor("parent_fence_admission_observed", liveParentFenceAdmissionIsObserved);
}

function createCutoverResource(resource, label) {
  const result = runLeaseGuardedMutation(
    assertOperationLeaseHeld,
    () => spawnSync(
      "kubectl",
      contextArgs(["--request-timeout=30s", "create", "-f", "-"]),
      {
        input: JSON.stringify(resource),
        encoding: "utf8",
        timeout: MUTATION_TIMEOUT_MS
      }
    )
  );
  if (result.status !== 0) throw new Error(`${label}_create_failed:${result.status}`);
}

function writeCutoverParentDeployment(targetDeployment, baselineDeployment, mode) {
  const resource = structuredClone(targetDeployment);
  const verb = mode === "clean-install" ? "create" : "replace";
  if (mode === "pristine-cutover") {
    if (
      baselineDeployment?.name !== "bot-orchestrator" ||
      typeof baselineDeployment?.uid !== "string" || !baselineDeployment.uid ||
      typeof baselineDeployment?.resourceVersion !== "string" ||
      !baselineDeployment.resourceVersion
    ) {
      throw new Error("runner_cutover_parent_deployment_baseline_invalid");
    }
    resource.metadata = {
      ...resource.metadata,
      uid: baselineDeployment.uid,
      resourceVersion: baselineDeployment.resourceVersion
    };
  } else if (baselineDeployment !== null) {
    throw new Error("runner_clean_install_deployment_baseline_must_be_absent");
  }
  const result = runLeaseGuardedMutation(
    assertOperationLeaseHeld,
    () => spawnSync(
      "kubectl",
      contextArgs(["--request-timeout=30s", verb, "-f", "-"]),
      {
        input: JSON.stringify(resource),
        encoding: "utf8",
        timeout: MUTATION_TIMEOUT_MS
      }
    )
  );
  if (result.status !== 0) {
    throw new Error(`runner_cutover_parent_deployment_${verb}_failed:${result.status}`);
  }
}

function newCutoverJournalForCurrentOperation() {
  if (cutoverJournal !== null) return cutoverJournal;
  if (!Buffer.isBuffer(cutoverKey) || typeof cutoverNamespaceUid !== "string") {
    throw new Error("runner_cutover_journal_local_capability_missing");
  }
  const mode = cutoverPreflightClassification.startsWith("pristine-cutover")
    ? "pristine-cutover"
    : "clean-install";
  const baselineDeployment = mode === "pristine-cutover" ? {
    name: "bot-orchestrator",
    uid: cutoverBaselineEvidence?.deploymentUid,
    resourceVersion: cutoverBaselineEvidence?.deploymentResourceVersion
  } : null;
  cutoverJournal = createCutoverJournal({
    mode,
    operationId: randomUUID(),
    authorization: mode === "pristine-cutover" ? cutoverAttestation : null,
    expectedKubeContext: kubectlContext,
    namespace: parentNamespace,
    namespaceUid: cutoverNamespaceUid,
    baselineDeployment,
    manifestSha256: cutoverManifestSha256(),
    targetHashes: cutoverTargetHashes(),
    issuedAt: new Date().toISOString()
  }, cutoverKey);
  return cutoverJournal;
}

function readLiveCutoverTransitionState(journal) {
  const evidence = pristineLegacyCutoverLiveEvidence();
  const journalConfigMap = readCutoverJournalConfigMap();
  return {
    ...journalTransitionState(evidence, journalConfigMap, journal),
    evidence
  };
}

function validateLiveCutoverTransitionState(state) {
  const namespaceUid = state.evidence?.liveNamespace?.metadata?.uid;
  if (
    namespaceUid !== cutoverNamespaceUid ||
    !activeCutoverNamespace(state.evidence?.liveNamespace, parentNamespace)
  ) {
    throw new Error("runner_cutover_journal_namespace_replaced");
  }
  if (
    state.journal.mode === "clean-install" &&
    state.evidence.liveNamespace.metadata.annotations?.["yenhubs.org/runner-clean-install"] !==
      "fence-aware-bootstrap-v1"
  ) {
    throw new Error("runner_cutover_journal_clean_namespace_marker_missing");
  }
  if (
    state.journalConfigMap !== null &&
    !exactCutoverJournalConfigMap(state.journalConfigMap, cutoverNamespaceUid)
  ) {
    throw new Error("runner_cutover_journal_live_object_not_exact");
  }
  const prefix = classifyCutoverJournalPrefix(state);
  verifyJournalCutoverIsolationGate({
    ...state.evidence,
    isolatedResources: {
      ...state.evidence.isolatedResources,
      cutoverJournalPolicy: null,
      cutoverJournalBinding: null,
      parentFencePolicy: null,
      parentFenceBinding: null
    },
    allowParentCandidates: ["P5", "P6"].includes(prefix)
  });
}

function liveFirstCutoverParentIsQuiesced(journal) {
  const deployment = kubectlAbsentOnlyJson([
    "-n", parentNamespace, "get", "deployment", "bot-orchestrator", "-o", "json"
  ], "first-cutover-parent-deployment-quiescence");
  if (deployment === null) return false;
  const pods = kubectlJson(["-n", parentNamespace, "get", "pods", "-o", "json"]);
  const replicaSets = kubectlJson([
    "-n", parentNamespace, "get", "replicasets", "-o", "json"
  ]);
  return parentIsQuiesced(
    deployment,
    pods,
    replicaSets,
    journal.baselineDeployment?.uid
  );
}

async function performFirstCutoverFenceTransition() {
  const journal = newCutoverJournalForCurrentOperation();
  const journalConfigMap = cutoverJournalConfigMap(journal);
  const {
    journalPolicy: targetJournalPolicy,
    journalBinding: targetJournalBinding,
    targetPolicy,
    targetBinding,
    targetDeployment: deploymentMutationResource
  } = cutoverTargetResources();
  const initialDeployment = kubectlAbsentOnlyJson([
    "-n", parentNamespace, "get", "deployment", "bot-orchestrator", "-o", "json"
  ], "runner-cutover-parent-deployment-normalization");
  const targetDeployment = serverNormalizedCutoverDeployment(
    deploymentMutationResource,
    initialDeployment
  );
  if (targetDeployment === null) {
    throw new Error("runner_cutover_parent_deployment_server_normalization_failed");
  }
  await advanceCutoverJournalTransition({
    journal,
    journalConfigMap,
    targetJournalPolicy,
    targetJournalBinding,
    targetPolicy,
    targetBinding,
    targetDeployment,
    deploymentMutationResource,
    readState: async () => readLiveCutoverTransitionState(journal),
    validateState: async state => validateLiveCutoverTransitionState(state),
    isJournalExact: value =>
      exactCutoverJournalConfigMap(value, cutoverNamespaceUid) &&
      parseExactCutoverJournalConfigMap(
        value,
        cutoverJournalVerification(cutoverNamespaceUid)
      ).operationId === journal.operationId,
    createJournal: async resource =>
      createCutoverResource(resource, "runner_cutover_journal"),
    createJournalPolicy: async resource =>
      createCutoverResource(resource, "runner_cutover_journal_guard_policy"),
    waitJournalPolicyObserved: async () => waitFor(
      "runner_cutover_journal_guard_policy_observed",
      () => {
        const live = kubectlAbsentOnlyJson([
          "get", "validatingadmissionpolicy", CUTOVER_JOURNAL_POLICY_NAME, "-o", "json"
        ], "runner-cutover-journal-guard-policy-observation");
        return live !== null && admissionPolicyIsObserved(live) &&
          liveResourceMatchesTarget(live, targetJournalPolicy);
      }
    ),
    createJournalBinding: async resource =>
      createCutoverResource(resource, "runner_cutover_journal_guard_binding"),
    waitJournalFenceObserved: async () => waitFor(
      "runner_cutover_journal_guard_observed",
      liveCutoverJournalAdmissionIsObserved
    ),
    createPolicy: async resource =>
      createCutoverResource(resource, "runner_cutover_parent_policy"),
    waitPolicyObserved: async () => waitFor(
      "runner_cutover_parent_policy_observed",
      () => {
        const live = kubectlAbsentOnlyJson([
          "get", "validatingadmissionpolicy", PARENT_FENCE_POLICY_NAME, "-o", "json"
        ], "runner-cutover-parent-policy-observation");
        return live !== null && admissionPolicyIsObserved(live) &&
          liveResourceMatchesTarget(live, targetPolicy);
      }
    ),
    createBinding: async resource =>
      createCutoverResource(resource, "runner_cutover_parent_binding"),
    waitParentFenceObserved: async () => waitFor(
      "runner_cutover_parent_fence_observed",
      liveParentFenceAdmissionIsObserved
    ),
    writeDeployment: async (resource, baseline) =>
      writeCutoverParentDeployment(resource, baseline, journal.mode),
    waitParentQuiesced: async () => {
      await waitFor(
        "runner_cutover_parent_quiesced",
        () => liveFirstCutoverParentIsQuiesced(journal)
      );
      await waitForStableFirstCutoverParentAbsence(
        "runner_cutover_parent_stable_absence",
        () => liveFirstCutoverParentIsQuiesced(journal)
      );
    }
  });
  const durable = durableParentFenceEvidence();
  if (durable === null) throw new Error("runner_cutover_parent_fence_not_durable_after_transition");
  cutoverPreflightClassification = "fence-aware";
  cutoverFenceEvidence = durable;
  return journal.mode;
}

function runnerRole({ inert = false, recoveryPhase = plan.recoveryPhase } = {}) {
  const role = plan.resources.find(value =>
    value?.kind === "Role" &&
    value?.metadata?.name === "bot-orchestrator-runner-pods" &&
    value?.metadata?.namespace === RUNNER_NAMESPACE
  );
  if (!role) throw new Error("generated_manifest_runner_role_missing");
  return {
    ...role,
    metadata: {
      ...role.metadata,
      annotations: {
        ...role.metadata.annotations,
        [RECOVERY_PHASE_ANNOTATION]: recoveryPhase
      }
    },
    ...(inert ? { rules: [] } : {})
  };
}

function neutralizeRunnerAuthority(recoveryPhase = plan.recoveryPhase) {
  applyResource(runnerRole({ inert: true, recoveryPhase }));
}

function preGrantGeneratedResources() {
  const inertRole = runnerRole({ inert: true });
  return plan.resources.map(resource =>
    resource?.kind === "Role" &&
    resource?.metadata?.namespace === RUNNER_NAMESPACE &&
    resource?.metadata?.name === "bot-orchestrator-runner-pods"
      ? inertRole
      : resource
  );
}

async function prepareExactPreGrantControlPlane() {
  neutralizeRunnerAuthority();
  applyNamedResources([
    ["ValidatingAdmissionPolicy", ADMISSION_POLICY_NAME, ""],
    ["ValidatingAdmissionPolicyBinding", ADMISSION_POLICY_NAME, ""]
  ]);
  await waitFor("runner_admission_observed_before_rolebinding", liveAdmissionIsObserved);
  applyNamedResources([["RoleBinding", "bot-orchestrator-runner-pods", RUNNER_NAMESPACE]]);
  const expected = preGrantGeneratedResources();
  await waitFor(
    "runner_control_plane_exact_and_observed_before_authority",
    () => liveRunnerControlPlaneErrors(expected).length === 0
  );
}

function applyManifest() {
  applyResourcesSequentially(plan.resources, resource => {
    if (
      resource?.apiVersion === "admissionregistration.k8s.io/v1" &&
      resource?.kind === "ValidatingAdmissionPolicyBinding" &&
      resource?.metadata?.name === RECOVERY_OPERATION_FENCE_POLICY_NAME
    ) {
      transitionRecoveryOperationFenceBinding(
        plan.recoveryPhase === "restore-fence",
        "manifest_recovery_operation_fence",
        { allowCreate: true }
      );
      return;
    }
    applyResource(resource);
  });
}

function expectedDeployments() {
  return plan.resources.filter(resource =>
    resource?.apiVersion === "apps/v1" &&
    resource?.kind === "Deployment" &&
    resource?.metadata?.namespace === parentNamespace
  );
}

function serverNormalizedDeployment(expected, live) {
  const candidate = structuredClone(expected);
  candidate.metadata = {
    ...candidate.metadata,
    resourceVersion: live?.metadata?.resourceVersion
  };
  delete candidate.status;
  const result = runLeaseGuardedRead(() => spawnSync(
    "kubectl",
    contextArgs(["replace", "--dry-run=server", "-f", "-", "-o", "json"]),
    { input: JSON.stringify(candidate), encoding: "utf8", timeout: kubectlReadTimeoutMs }
  ));
  if (result.status !== 0) return null;
  try {
    return JSON.parse(result.stdout);
  } catch (_error) {
    return null;
  }
}

function serverNormalizedCutoverDeployment(expected, live) {
  if (live !== null) return serverNormalizedDeployment(expected, live);
  const candidate = structuredClone(expected);
  delete candidate.status;
  const result = runLeaseGuardedRead(() => spawnSync(
    "kubectl",
    contextArgs(["create", "--dry-run=server", "-f", "-", "-o", "json"]),
    { input: JSON.stringify(candidate), encoding: "utf8", timeout: kubectlReadTimeoutMs }
  ));
  if (result.status !== 0) return null;
  try {
    return JSON.parse(result.stdout);
  } catch (_error) {
    return null;
  }
}

function deploymentsMatchExpectedDesiredState(expected, { exactInventory = false } = {}) {
  assertOperationLeaseProcessHealthy();
  const deployments = kubectlJson(["-n", parentNamespace, "get", "deployment", "-o", "json"]);
  const liveByName = new Map(
    Array.isArray(deployments?.items)
      ? deployments.items.map(deployment => [deployment?.metadata?.name, deployment])
      : []
  );
  if (
    deployments?.kind !== "DeploymentList" ||
    (exactInventory && liveByName.size !== expected.length) ||
    expected.some(deployment => !liveByName.has(deployment.metadata.name))
  ) {
    return false;
  }
  return expected.every(deployment => {
    const live = liveByName.get(deployment.metadata.name);
    const normalized = serverNormalizedDeployment(deployment, live);
    return normalized !== null && exactDeploymentDesiredState(live, normalized);
  });
}

function deploymentsMatchGeneratedDesiredState() {
  return deploymentsMatchExpectedDesiredState(expectedDeployments(), { exactInventory: true });
}

function deploymentsAreReady() {
  const deployments = kubectlJson(["-n", parentNamespace, "get", "deployment", "-o", "json"]);
  const expectedNames = expectedDeployments().map(resource => resource.metadata.name).sort();
  const actualNames = Array.isArray(deployments?.items)
    ? deployments.items.map(deployment => deployment?.metadata?.name).sort()
    : [];
  return deployments?.kind === "DeploymentList" &&
      JSON.stringify(actualNames) === JSON.stringify(expectedNames) &&
      deployments.items.every(deployment => {
        const replicas = Number(deployment.spec?.replicas || 0);
        const generation = deployment?.metadata?.generation;
        return Number.isInteger(generation) && generation > 0 &&
          deployment?.status?.observedGeneration === generation &&
          Number(deployment.status?.updatedReplicas || 0) === replicas &&
          Number(deployment.status?.availableReplicas || 0) === replicas &&
          Number(deployment.status?.readyReplicas || 0) === replicas &&
          Number(deployment.status?.unavailableReplicas || 0) === 0;
      });
}

async function waitForDeployments() {
  await waitFor("deployments_ready", deploymentsAreReady);
}

function exactLiveRecoveryLock(expectedState) {
  const lock = kubectlOptionalJson([
    "-n", parentNamespace, "get", "configmap", RECOVERY_LOCK_NAME, "-o", "json"
  ]);
  const namespace = kubectlOptionalJson(["get", "namespace", parentNamespace, "-o", "json"]);
  const pvc = kubectlOptionalJson([
    "-n", parentNamespace, "get", "persistentvolumeclaim", "ret-pvc", "-o", "json"
  ]);
  return exactRecoveryOperationLock(
    lock,
    parentNamespace,
    plan.recoveryEpoch,
    expectedState,
    {
      namespaceUid: namespace?.metadata?.uid,
      pvcUid: pvc?.metadata?.uid,
      ...(recoveryLockIdentityGuard === null ? {} : {
        lockUid: recoveryLockIdentityGuard.uid,
        lockResourceVersion: recoveryLockIdentityGuard.resourceVersion
      })
    }
  );
}

function currentRecoveryLockSnapshot() {
  const lock = kubectlOptionalJson([
    "-n", parentNamespace, "get", "configmap", RECOVERY_LOCK_NAME, "-o", "json"
  ]);
  if (lock === null) return { state: null, lock: null };
  const namespace = kubectlOptionalJson(["get", "namespace", parentNamespace, "-o", "json"]);
  const pvc = kubectlOptionalJson([
    "-n", parentNamespace, "get", "persistentvolumeclaim", "ret-pvc", "-o", "json"
  ]);
  const options = { namespaceUid: namespace?.metadata?.uid, pvcUid: pvc?.metadata?.uid };
  for (const state of ["restore-fence-prepared", "restore-complete-awaiting-reactivation"]) {
    if (exactRecoveryOperationLock(lock, parentNamespace, plan.recoveryEpoch, state, options)) {
      return { state, lock };
    }
  }
  return { state: "invalid", lock };
}

function pinRecoveryLockSnapshot(snapshot) {
  if (
    !snapshot ||
    !["restore-fence-prepared", "restore-complete-awaiting-reactivation"].includes(snapshot.state) ||
    typeof snapshot?.lock?.metadata?.uid !== "string" || !snapshot.lock.metadata.uid ||
    typeof snapshot?.lock?.metadata?.resourceVersion !== "string" ||
    !snapshot.lock.metadata.resourceVersion
  ) {
    throw new Error("recovery_lock_snapshot_not_exact_for_pin");
  }
  recoveryLockIdentityGuard = {
    uid: snapshot.lock.metadata.uid,
    resourceVersion: snapshot.lock.metadata.resourceVersion
  };
  if (!exactLiveRecoveryLock(snapshot.state)) {
    throw new Error("recovery_lock_replaced_or_mutated_after_snapshot");
  }
  return recoveryLockIdentityGuard;
}

function recoveryLockExists() {
  return Boolean(kubectlOptionalJson([
    "-n", parentNamespace, "get", "configmap", RECOVERY_LOCK_NAME, "-o", "json"
  ]));
}

function recoveryDeployment(name, replicas, recoveryPhase) {
  const deployment = plan.resources.find(resource =>
    resource?.kind === "Deployment" &&
    resource?.metadata?.namespace === parentNamespace &&
    resource?.metadata?.name === name
  );
  if (!deployment) throw new Error(`generated_manifest_recovery_deployment_missing:${name}`);
  return {
    ...deployment,
    metadata: {
      ...deployment.metadata,
      annotations: {
        ...deployment.metadata.annotations,
        [RECOVERY_PHASE_ANNOTATION]: recoveryPhase
      }
    },
    spec: { ...deployment.spec, replicas }
  };
}

function recoveryFenceDeployments() {
  return RECOVERY_CONSUMERS.map(name => recoveryDeployment(name, 0, "restore-fence"));
}

function recoveryFenceDeploymentsAreExact() {
  return deploymentsMatchExpectedDesiredState(recoveryFenceDeployments());
}

function activeStagingFenceDeployments() {
  return RECOVERY_CONSUMERS.map(name => recoveryDeployment(name, 0, "active"));
}

function activeStagingFenceDeploymentsAreExact() {
  return deploymentsMatchExpectedDesiredState(activeStagingFenceDeployments());
}

async function establishRecoveryFenceMutations() {
  await prepareRecoveryAdmissionFence();
  return retryBestEffortFenceAttempt(
    async () => {
      const fenceFailures = await runBestEffortFenceSteps([
        {
          name: "runner-role-inert",
          action: async () => neutralizeRunnerAuthority("restore-fence")
        },
        ...recoveryFenceDeployments().map(deployment => ({
          name: `deployment:${deployment.metadata.name}`,
          action: async () => applyResource(deployment)
        }))
      ]);
      return fenceFailures;
    },
    { maxAttempts: 3, beforeRetry: async () => sleep(1_000) }
  );
}

async function establishActiveStagingFenceMutations() {
  await prepareRecoveryAdmissionFence();
  return retryBestEffortFenceAttempt(
    async () => {
      const fenceFailures = await runBestEffortFenceSteps([
        {
          name: "runner-role-inert",
          action: async () => neutralizeRunnerAuthority("active")
        },
        ...activeStagingFenceDeployments().map(deployment => ({
          name: `deployment:${deployment.metadata.name}`,
          action: async () => applyResource(deployment)
        }))
      ]);
      return fenceFailures;
    },
    { maxAttempts: 3, beforeRetry: async () => sleep(1_000) }
  );
}

async function refenceActiveReapplyForStaging() {
  const failures = await establishActiveStagingFenceMutations();
  let causalFenceReady = false;
  try {
    await waitFor("active_reapply_staging_parent_and_authority_quiesced", () =>
      liveParentIsQuiesced() &&
      liveRecoveryConsumersAreQuiesced() &&
      exactRunnerAuthority(false) &&
      liveRecoveryAdmissionIsObserved()
    );
    causalFenceReady = true;
  } catch (_error) {
    failures.push("causal-parent-authority-fence");
  }
  if (causalFenceReady) {
    failures.push(...await retryBestEffortFenceAttempt(
      async () => deleteAllRunnerPodsByExactUid(),
      { maxAttempts: 3, beforeRetry: async () => sleep(1_000) }
    ));
    try {
      await waitFor("active_reapply_staging_runner_runtime_quiesced", runnerRuntimeIsQuiesced);
    } catch (_error) {
      failures.push("runner-runtime-quiescence");
    }
  }
  try {
    await runWithStablePodAbsence(
      "stable_pod_absence_before_active_reapply_staging",
      async () => {
        if (recoveryLockExists()) {
          throw new Error("recovery_lock_appeared_during_active_reapply_refence");
        }
      },
      undefined,
      { includeRecoveryConsumers: true }
    );
  } catch (_error) {
    failures.push("stable-pod-absence");
  }
  for (const [name, predicate] of [
    ["deployment-fences-exact", activeStagingFenceDeploymentsAreExact],
    ["recovery-consumers-absent", liveRecoveryConsumersAreQuiesced],
    ["runner-authority-inert", () => exactRunnerAuthority(false)],
    ["runner-runtime-quiesced", runnerRuntimeIsQuiesced],
    ["recovery-lock-absent", () => !recoveryLockExists()]
  ]) {
    try {
      if (!predicate()) failures.push(name);
    } catch (_error) {
      failures.push(name);
    }
  }
  if (failures.length > 0) {
    throw new Error(`active_reapply_refence_incomplete:${[...new Set(failures)].join(",")}`);
  }
}

async function refenceRecoveryConsumers({ expectedRecoveryLockState = null, label }) {
  const failures = await establishRecoveryFenceMutations();
  let causalFenceReady = false;
  try {
    await waitFor(`${label}_parent_and_authority_quiesced`, () =>
      liveParentIsQuiesced() &&
      liveRecoveryConsumersAreQuiesced() &&
      exactRunnerAuthority(false) &&
      liveRecoveryAdmissionIsObserved()
    );
    causalFenceReady = true;
  } catch (_error) {
    failures.push("causal-parent-authority-fence");
  }
  if (causalFenceReady) {
    try {
      await ensureRecoveryOperationFenceBindingDormant(`${label}_operation_fence_reconciliation`);
    } catch (_error) {
      failures.push("recovery-operation-fence-dormancy");
      causalFenceReady = false;
    }
  }
  if (causalFenceReady) {
    failures.push(...await retryBestEffortFenceAttempt(
      async () => deleteAllRunnerPodsByExactUid(),
      { maxAttempts: 3, beforeRetry: async () => sleep(1_000) }
    ));
    try {
      await waitFor(`${label}_runner_runtime_quiesced`, runnerRuntimeIsQuiesced);
    } catch (_error) {
      failures.push("runner-runtime-quiescence");
    }
  }
  try {
    await runWithStablePodAbsence(
      `stable_pod_absence_before_${label}_operation_fence`,
      async () => {
        if (
          expectedRecoveryLockState &&
          !exactLiveRecoveryLock(expectedRecoveryLockState)
        ) {
          throw new Error(`${label}_recovery_lock_changed_during_refence`);
        }
        await setRecoveryOperationFenceBinding(true, `${label}_operation_fence_active`);
      },
      undefined,
      { includeRecoveryConsumers: true }
    );
  } catch (_error) {
    failures.push("stable-pod-absence");
  }
  for (const [name, predicate] of [
    ["deployment-fences-exact", recoveryFenceDeploymentsAreExact],
    ["recovery-consumers-absent", liveRecoveryConsumersAreQuiesced],
    ["runner-authority-inert", () => exactRunnerAuthority(false)],
    ["runner-runtime-quiesced", runnerRuntimeIsQuiesced],
    ["recovery-operation-fence-active", () =>
      liveRecoveryOperationFenceAdmissionIsObserved({ active: true }) &&
      recoveryOperationFenceDenialProbe()]
  ]) {
    try {
      if (!predicate()) failures.push(name);
    } catch (_error) {
      failures.push(name);
    }
  }
  if (failures.length > 0) {
    throw new Error(`recovery_fence_incomplete:${[...new Set(failures)].join(",")}`);
  }
}

async function refencePartialReactivation() {
  await refenceRecoveryConsumers({
    expectedRecoveryLockState: "restore-complete-awaiting-reactivation",
    label: "partial_reactivation"
  });
}

async function emergencyRefenceAfterFailedActivation() {
  if (recoveryLockExists()) {
    await refenceRecoveryConsumers({
      label: "failed_activation"
    });
  } else {
    await refenceActiveReapplyForStaging();
  }
  failClosedRefenceRequired = false;
}

async function applyRestoreFence() {
  if (!exactLiveRecoveryLock("restore-fence-prepared")) {
    throw new Error("restore_fence_requires_exact_prepared_recovery_lock");
  }
  const namespace = kubectlOptionalJson(["get", "namespace", parentNamespace, "-o", "json"]);
  if (namespace) {
    await refenceRecoveryConsumers({
      expectedRecoveryLockState: "restore-fence-prepared",
      label: "restore_fence_initial"
    });
  }
  if (!exactLiveRecoveryLock("restore-fence-prepared")) {
    throw new Error("restore_fence_recovery_lock_changed_before_full_apply");
  }
  applyManifest();
  await waitFor("parent_quiesced_in_restore_fence", liveParentIsQuiesced);
  await waitFor("recovery_consumers_quiesced", liveRecoveryConsumersAreQuiesced);
  await waitFor("runner_runtime_quiesced_in_restore_fence", runnerRuntimeIsQuiesced);
  await waitFor("restore_fence_deployments_exact", deploymentsMatchGeneratedDesiredState);
  await waitFor("runner_admission_observed_in_restore_fence", liveAdmissionIsObserved);
  await waitFor(
    "recovery_operation_fence_denial_probe_in_restore_fence",
    recoveryOperationFenceDenialProbe
  );
  await waitFor("runner_control_plane_exact_in_restore_fence", liveRunnerControlPlaneIsExact);
  if (!exactRunnerAuthority(false)) throw new Error("restore_fence_runner_rbac_not_inert");
  if (!exactLiveRecoveryLock("restore-fence-prepared")) {
    throw new Error("restore_fence_recovery_lock_changed_during_apply");
  }
  console.log("restore-fence gate passed; five consumers remain stopped and runner authority is inert");
}

async function applyBootstrap() {
  if (recoveryLockExists()) throw new Error("bootstrap_blocked_while_recovery_lock_exists");
  const namespace = kubectlOptionalJson(["get", "namespace", parentNamespace, "-o", "json"]);
  const firstCutoverClassifications = new Set([
    "pristine-cutover",
    "clean-install",
    "clean-install-resume",
    "pristine-cutover-journal-resume",
    "clean-install-journal-resume"
  ]);
  if (namespace && firstCutoverClassifications.has(cutoverPreflightClassification)) {
    const cutoverMode = await performFirstCutoverFenceTransition();
    if (!exactRunnerAuthority(false)) {
      throw new Error("first_cutover_runner_authority_not_inert");
    }
    if (cutoverMode === "pristine-cutover") {
      await prepareRecoveryAdmissionFence();
      const deletionFailures = await retryBestEffortFenceAttempt(
        async () => deleteAllRunnerPodsByExactUid(),
        { maxAttempts: 3, beforeRetry: async () => sleep(1_000) }
      );
      if (deletionFailures.length > 0) {
        throw new Error("pristine_cutover_runner_reconciliation_failed");
      }
      await waitFor("pristine_runner_runtime_quiesced", runnerRuntimeIsQuiesced);
    }
  } else if (namespace && cutoverPreflightClassification === "fence-aware") {
    if (recoveryLockExists()) throw new Error("bootstrap_lock_appeared_before_parent_quiesce");
    await prepareRecoveryAdmissionFence();
    const fenceFailures = await runBestEffortFenceSteps([
      {
        name: "runner-role-inert",
        action: async () => neutralizeRunnerAuthority()
      },
      {
        name: "deployment:bot-orchestrator",
        action: async () => applyResource(recoveryDeployment("bot-orchestrator", 0, "active"))
      }
    ]);
    if (fenceFailures.length > 0) {
      throw new Error("bootstrap_fence_mutations_failed");
    }
    await waitFor("parent_quiesced_before_bootstrap_reconciliation", () =>
      liveParentIsQuiesced() &&
      exactRunnerAuthority(false) &&
      liveRecoveryAdmissionIsObserved()
    );
    const deletionFailures = await retryBestEffortFenceAttempt(
      async () => deleteAllRunnerPodsByExactUid(),
      { maxAttempts: 3, beforeRetry: async () => sleep(1_000) }
    );
    if (deletionFailures.length > 0) throw new Error("bootstrap_runner_reconciliation_failed");
    await waitFor("runner_runtime_quiesced_before_bootstrap_authority", runnerRuntimeIsQuiesced);
    await runWithStablePodAbsence(
      "stable_pod_absence_before_bootstrap_authority_fence",
      async () => {
        if (recoveryLockExists()) throw new Error("bootstrap_lock_appeared_before_authority_fence");
        if (!exactRunnerAuthority(false)) throw new Error("bootstrap_runner_authority_changed");
      }
    );
  }
  if (recoveryLockExists()) throw new Error("bootstrap_lock_appeared_before_full_apply");
  applyManifest();
  await waitFor("parent_quiesced", liveParentIsQuiesced);
  await waitFor("runner_runtime_quiesced", runnerRuntimeIsQuiesced);
  await waitFor("runner_admission_observed", liveAdmissionIsObserved);
  await waitFor("runner_control_plane_exact", liveRunnerControlPlaneIsExact);
  if (!exactRunnerAuthority(false)) throw new Error("bootstrap_runner_rbac_not_inert");
  console.log("runner bootstrap gate passed; generate and apply the admission phase next");
}

async function applyAdmission() {
  if (recoveryLockExists()) throw new Error("admission_blocked_while_recovery_lock_exists");
  const live = liveState();
  if (live.activationPhase !== "bootstrap" || live.recoveryPhase !== "active") {
    throw new Error("runner_activation_transition_requires_active_recovery_bootstrap");
  }
  await waitFor("parent_quiesced_before_runner_authority", liveParentIsQuiesced);
  await waitFor("runner_runtime_quiesced_before_runner_authority", runnerRuntimeIsQuiesced);
  await waitFor("runner_admission_observed_before_authority", liveAdmissionIsObserved);
  if (!exactRunnerAuthority(false)) throw new Error("legacy_or_bootstrap_runner_authority_not_inert");
  await prepareExactPreGrantControlPlane();
  try {
    await runWithStablePodAbsence(
      "parent_serviceaccount_tokens_expired_before_runner_authority",
      async () => {
        if (recoveryLockExists()) throw new Error("admission_lock_appeared_before_runner_grant");
        failClosedRefenceRequired = true;
        applyResource(runnerRole());
      },
      async () => neutralizeRunnerAuthority()
    );
    if (!exactRunnerAuthority(true)) throw new Error("admission_runner_rbac_not_effective");
    await waitForAdmissionDenialProbe();
    if (recoveryLockExists()) throw new Error("admission_lock_appeared_before_full_apply");
    applyManifest();
  } catch (error) {
    neutralizeRunnerAuthority();
    failClosedRefenceRequired = false;
    throw error;
  }
  await waitFor("parent_quiesced_after_runner_authority", liveParentIsQuiesced);
  await waitFor("runner_runtime_quiesced_after_runner_authority", runnerRuntimeIsQuiesced);
  if (!exactRunnerAuthority(true)) throw new Error("admission_runner_rbac_not_effective");
  await waitForAdmissionDenialProbe();
  await waitFor("runner_control_plane_exact_after_admission", liveRunnerControlPlaneIsExact);
  console.log("runner admission denial probe passed; generate and apply the active phase next");
}

async function applyActiveTransition(mode) {
  const live = liveState();
  const lockExists = recoveryLockExists();
  const restoreReactivation = mode === "recovery-reactivation";
  let restoreAuthorityGranted = false;
  if (restoreReactivation) {
    if (!exactLiveRecoveryLock("restore-complete-awaiting-reactivation")) {
      throw new Error("runner_reactivation_requires_exact_completed_recovery_lock");
    }
    failClosedRefenceRequired = true;
    if (live.recoveryPhase === "active") {
      if (
        deploymentsAreReady() &&
        deploymentsMatchGeneratedDesiredState() &&
        liveRunnerControlPlaneIsExact() &&
        exactRunnerAuthority(true) &&
        admissionDenialProbe() &&
        liveRecoveryOperationFenceAdmissionIsObserved({ active: false }) &&
        recoveryOperationFenceInactiveProbe()
      ) {
        console.log(
          "reactivation already converged; recovery lock remains for the root live runner smoke and finalizer"
        );
        return;
      } else {
        await refencePartialReactivation();
      }
    }
    await waitFor("recovery_consumers_quiesced_before_reactivation", liveRecoveryConsumersAreQuiesced);
    await waitFor("parent_quiesced_before_reactivation", liveParentIsQuiesced);
    await waitFor("runner_runtime_quiesced_before_reactivation", runnerRuntimeIsQuiesced);
    await waitFor("runner_admission_observed_before_reactivation", () =>
      liveAdmissionCoreIsObserved() &&
      liveRecoveryOperationFenceAdmissionIsObserved({ active: true }) &&
      recoveryOperationFenceDenialProbe()
    );
    if (!exactRunnerAuthority(false)) throw new Error("restore_fence_runner_authority_not_inert");
    await setRecoveryOperationFenceBinding(false, "recovery_reactivation_operation_fence");
    if (!exactLiveRecoveryLock("restore-complete-awaiting-reactivation")) {
      throw new Error("recovery_lock_changed_during_operation_fence_deactivation");
    }
    await prepareExactPreGrantControlPlane();
    try {
      await runWithStablePodAbsence(
        "parent_serviceaccount_tokens_expired_before_reactivation",
        async () => {
          if (!exactLiveRecoveryLock("restore-complete-awaiting-reactivation")) {
            throw new Error("recovery_lock_changed_before_runner_authority");
          }
          applyResource(runnerRole());
        },
        async () => neutralizeRunnerAuthority()
      );
      if (!exactRunnerAuthority(true)) throw new Error("reactivation_runner_rbac_not_effective");
      restoreAuthorityGranted = true;
      await waitForAdmissionDenialProbe();
      if (!liveRecoveryConsumersAreQuiesced() || !exactLiveRecoveryLock("restore-complete-awaiting-reactivation")) {
        throw new Error("recovery_fence_or_lock_changed_before_reactivation_apply");
      }
    } catch (error) {
      neutralizeRunnerAuthority();
      throw error;
    }
  } else if (lockExists) {
    throw new Error("active_blocked_by_nonreactivatable_recovery_lock");
  } else if (mode === "active-reapply") {
    if (live.activationPhase !== "active" || live.recoveryPhase !== "active") {
      throw new Error("runner_active_reapply_requires_active_recovery_state");
    }
    failClosedRefenceRequired = true;
    let controlPlaneExact = false;
    let authorityExact = false;
    let denialExact = false;
    try {
      controlPlaneExact = liveRunnerControlPlaneIsExact();
      authorityExact = controlPlaneExact && exactRunnerAuthority(true);
      denialExact = authorityExact && admissionDenialProbe();
    } catch (_error) {
      controlPlaneExact = false;
      authorityExact = false;
      denialExact = false;
    }
    if (!controlPlaneExact || !authorityExact || !denialExact) {
      await refenceActiveReapplyForStaging();
      failClosedRefenceRequired = false;
      throw new Error(
        "active_reapply_control_plane_drift_refenced_generate_and_apply_bootstrap_then_admission_then_active_do_not_retry_active"
      );
    }
  } else if (live.activationPhase === "admission" && live.recoveryPhase === "active") {
    failClosedRefenceRequired = true;
    await waitFor("parent_quiesced_before_activation", liveParentIsQuiesced);
    await waitFor("runner_runtime_quiesced_before_activation", runnerRuntimeIsQuiesced);
    await waitFor("runner_admission_observed_before_activation", liveAdmissionIsObserved);
    await waitFor("runner_control_plane_exact_before_activation", liveRunnerControlPlaneIsExact);
    if (!exactRunnerAuthority(true)) throw new Error("admission_runner_rbac_not_effective");
    await waitForAdmissionDenialProbe();
  } else {
    throw new Error("runner_activation_transition_requires_active_recovery_admission");
  }
  try {
    if (restoreReactivation) {
      if (!exactLiveRecoveryLock("restore-complete-awaiting-reactivation")) {
        throw new Error("recovery_lock_changed_before_full_reactivation_apply");
      }
    } else if (recoveryLockExists()) {
      throw new Error("recovery_lock_appeared_before_normal_active_apply");
    }
    applyManifest();
    await waitForDeployments();
    await waitFor("deployments_exact_after_activation", deploymentsMatchGeneratedDesiredState);
    await waitFor("runner_control_plane_exact_after_activation", liveRunnerControlPlaneIsExact);
    await waitFor("runner_effective_rbac_exact_after_activation", () => exactRunnerAuthority(true));
    await waitForAdmissionDenialProbe();
    if (restoreReactivation) {
      await waitFor(
        "recovery_operation_fence_inactive_after_reactivation",
        recoveryOperationFenceInactiveProbe
      );
    }
    if (restoreReactivation && !exactLiveRecoveryLock("restore-complete-awaiting-reactivation")) {
      throw new Error("recovery_lock_changed_after_reactivation_apply");
    }
  } catch (error) {
    if (restoreAuthorityGranted) neutralizeRunnerAuthority();
    throw error;
  }
  console.log(
    restoreReactivation
      ? "all deployments ready; recovery lock remains for the root live runner smoke and finalizer"
      : mode === "active-reapply"
        ? "all deployments ready; runner active reapply gate passed"
        : "all deployments ready; runner activation gate passed"
  );
}

async function applyActive(mode) {
  try {
    await applyActiveTransition(mode);
  } catch (error) {
    if (failClosedRefenceRequired) {
      try {
        if (operationLeaseGuard?.heartbeatLost) {
          await recoverOperationLeaseForFailClosedCleanup();
        } else {
          assertOperationLeaseHeld();
        }
        await emergencyRefenceAfterFailedActivation();
      } catch (refenceError) {
        throw new Error(
          `activation_failed_and_refence_failed:${error.message}:${refenceError.message}`
        );
      }
    }
    throw error;
  }
}

async function applyUnderOperationLease(commandMode) {
  assertOperationLeaseHeld();
  verifyCutoverPreflightUnderLease();
  if (commandMode === "emergency-refence") {
    failClosedRefenceRequired = true;
    await emergencyRefenceAfterFailedActivation();
    console.log("emergency refence passed; five consumers are stopped and runner authority is inert");
    return;
  }
  const live = liveState();
  const lockSnapshot = currentRecoveryLockSnapshot();
  const lockState = lockSnapshot.state;
  if (lockState === "invalid") {
    failClosedRefenceRequired = true;
    await refenceRecoveryConsumers({ label: "invalid_recovery_lock" });
    failClosedRefenceRequired = false;
    throw new Error("invalid_recovery_lock_refenced_manual_repair_required");
  }
  if (lockState !== null) {
    failClosedRefenceRequired = true;
    pinRecoveryLockSnapshot(lockSnapshot);
  }
  const mode = decideApplyMode({
    targetActivation: plan.activationPhase,
    targetRecovery: plan.recoveryPhase,
    liveActivation: live.activationPhase,
    liveRecovery: live.recoveryPhase,
    lockState
  });
  if (mode === "restore-fence") {
    await applyRestoreFence();
    return;
  }
  if (mode === "bootstrap") {
    await applyBootstrap();
    return;
  }
  if (mode === "admission") {
    await applyAdmission();
    return;
  }
  await applyActive(mode);
}

async function main() {
  const commandMode = requestedCommandMode(process.argv.slice(2));
  verifyManifestBeforeClusterMutation();
  requirePinnedKubectlContext();
  if (commandMode === "emergency-refence") {
    verifyEmergencyRefencePreflight();
  } else {
    verifyPristineLegacyCutoverPreflight();
  }
  const foundationalNamespace = await ensureFoundationalNamespaceForLease();
  if (
    ["clean-install", "clean-install-resume", "clean-install-journal-resume",
      "pristine-cutover-journal-resume"].includes(cutoverPreflightClassification)
  ) {
    const uid = foundationalNamespace?.metadata?.uid;
    if (typeof uid !== "string" || !uid || (cutoverNamespaceUid && cutoverNamespaceUid !== uid)) {
      throw new Error("runner_clean_install_namespace_identity_changed");
    }
    cutoverNamespaceUid = uid;
  }
  operationLeaseGuard = acquireOperationLeaseGuard();
  let failure = null;
  try {
    await applyUnderOperationLease(commandMode);
  } catch (error) {
    failure = error;
    if (failClosedRefenceRequired) {
      try {
        if (operationLeaseGuard?.heartbeatLost) {
          await recoverOperationLeaseForFailClosedCleanup();
        } else {
          assertOperationLeaseHeld();
        }
        await emergencyRefenceAfterFailedActivation();
      } catch (refenceError) {
        failure = new Error(
          `operation_lease_lost_and_refence_failed:${error.message}:${refenceError.message}`
        );
      }
    }
  }
  try {
    await releaseOperationLeaseGuard();
  } catch (releaseError) {
    failure = failure
      ? new Error(`${failure.message}:operation_lease_release_failed:${releaseError.message}`)
      : releaseError;
  }
  if (failure) throw failure;
}

main().catch(error => {
  console.error(`Apply failed: ${error.message}`);
  process.exitCode = 1;
});
