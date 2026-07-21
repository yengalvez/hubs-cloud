const crypto = require("node:crypto");
const fs = require("node:fs");

const {
  requireCompletePodList
} = require("../services/bot-orchestrator/kubernetes-runner-manager");

const PROFILE_ID = "yenhubs-process-local-credential-rotation-v1";
const PROFILE_SHA256 = "8252ddb7a957950b022fdae482c6363fbc102a57a0c140022031408bc4f6ea1b";
const PROFILE_CLOUD_COMMIT = "5a82de5387d7296cd01470d5136b2c07c2d5c7ac";
const MAX_ATTESTATION_AGE_MS = 5 * 60 * 1000;
const MAX_ATTESTATION_BYTES = 64 * 1024;
const MIN_KEY_BYTES = 32;
const MAX_KEY_BYTES = 4 * 1024;
const HEX_SHA256 = /^[0-9a-f]{64}$/;
const CLEAN_INSTALL_ANNOTATION = "yenhubs.org/runner-clean-install";
const CLEAN_INSTALL_VALUE = "fence-aware-bootstrap-v1";
const LEGACY_ANNOTATIONS = Object.freeze([
  "yenhubs.org/runner-activation-phase",
  "yenhubs.org/bot-runner-recovery-phase",
  "yenhubs.org/bot-runner-recovery-epoch"
]);
const ISOLATED_RESOURCE_KEYS = Object.freeze([
  "parentServiceAccount",
  "parentRole",
  "parentRoleBinding",
  "runnerAdmissionPolicy",
  "runnerAdmissionBinding",
  "runnerProtocolPolicy",
  "runnerProtocolBinding",
  "cutoverJournalPolicy",
  "cutoverJournalBinding",
  "parentFencePolicy",
  "parentFenceBinding",
  "recoveryOperationFencePolicy",
  "recoveryOperationFenceBinding"
]);

function exactKeys(value, keys) {
  return value && typeof value === "object" && !Array.isArray(value) &&
    JSON.stringify(Object.keys(value).sort()) === JSON.stringify([...keys].sort());
}

function canonicalJson(value) {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  return `{${Object.keys(value).sort().map(key =>
    `${JSON.stringify(key)}:${canonicalJson(value[key])}`
  ).join(",")}}`;
}

function samePrivateFile(left, right) {
  return left.dev === right.dev && left.ino === right.ino &&
    left.mode === right.mode && left.uid === right.uid && left.nlink === right.nlink &&
    left.size === right.size && left.mtimeNs === right.mtimeNs && left.ctimeNs === right.ctimeNs;
}

function readPrivateBytes(filePath, { minimumBytes, maximumBytes, label }) {
  if (typeof filePath !== "string" || !filePath || filePath.includes("\0")) {
    throw new Error(`${label}_path_required`);
  }
  if (typeof fs.constants.O_NOFOLLOW !== "number") {
    throw new Error("process_local_cutover_private_file_contract_unsupported");
  }
  let descriptor;
  try {
    const before = fs.lstatSync(filePath, { bigint: true });
    const expectedUid = typeof process.getuid === "function" ? BigInt(process.getuid()) : before.uid;
    const permissions = Number(before.mode & 0o7777n);
    if (
      !before.isFile() || before.isSymbolicLink() || before.uid !== expectedUid ||
      before.nlink !== 1n || ![0o400, 0o600].includes(permissions) ||
      before.size < BigInt(minimumBytes) || before.size > BigInt(maximumBytes)
    ) {
      throw new Error(`${label}_must_be_private_regular_file`);
    }
    descriptor = fs.openSync(filePath, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
    const opened = fs.fstatSync(descriptor, { bigint: true });
    if (!samePrivateFile(before, opened)) throw new Error(`${label}_changed`);
    const body = Buffer.alloc(Number(opened.size));
    let offset = 0;
    while (offset < body.length) {
      const read = fs.readSync(descriptor, body, offset, body.length - offset, offset);
      if (read === 0) throw new Error(`${label}_changed`);
      offset += read;
    }
    const extra = Buffer.alloc(1);
    if (fs.readSync(descriptor, extra, 0, 1, body.length) !== 0) {
      throw new Error(`${label}_changed`);
    }
    const after = fs.fstatSync(descriptor, { bigint: true });
    const finalPath = fs.lstatSync(filePath, { bigint: true });
    if (!samePrivateFile(opened, after) || !samePrivateFile(after, finalPath)) {
      throw new Error(`${label}_changed`);
    }
    return body;
  } catch (error) {
    if (error?.message?.startsWith(`${label}_`)) throw error;
    throw new Error(`${label}_unreadable`);
  } finally {
    if (descriptor !== undefined) fs.closeSync(descriptor);
  }
}

function readPrivateCutoverAttestation(attestationPath) {
  const source = readPrivateBytes(attestationPath, {
    minimumBytes: 2,
    maximumBytes: MAX_ATTESTATION_BYTES,
    label: "process_local_cutover_attestation"
  });
  try {
    return JSON.parse(source.toString("utf8"));
  } catch (_error) {
    throw new Error("process_local_cutover_attestation_json_invalid");
  }
}

function readPrivateCutoverKey(keyPath) {
  return readPrivateBytes(keyPath, {
    minimumBytes: MIN_KEY_BYTES,
    maximumBytes: MAX_KEY_BYTES,
    label: "process_local_cutover_key"
  });
}

function verifyAttestationHmac(attestation, key) {
  if (!Buffer.isBuffer(key) || key.length < MIN_KEY_BYTES || key.length > MAX_KEY_BYTES) {
    throw new Error("process_local_cutover_key_invalid");
  }
  if (!HEX_SHA256.test(attestation?.hmacSha256 || "")) {
    throw new Error("process_local_cutover_attestation_hmac_invalid");
  }
  const unsigned = structuredClone(attestation);
  delete unsigned.hmacSha256;
  const expected = crypto.createHmac("sha256", key).update(canonicalJson(unsigned), "utf8").digest();
  const actual = Buffer.from(attestation.hmacSha256, "hex");
  if (actual.length !== expected.length || !crypto.timingSafeEqual(actual, expected)) {
    throw new Error("process_local_cutover_attestation_hmac_invalid");
  }
}

function verifyProcessLocalCutoverAttestation(
  attestation,
  { key, namespace, expectedKubeContext, liveNamespace, liveDeployment, now = () => Date.now() }
) {
  if (!exactKeys(attestation, [
    "schemaVersion",
    "profileId",
    "profileSha256",
    "cloudSourceCommit",
    "historicalResourceCount",
    "requiredDeploymentCount",
    "requiredImagePairCount",
    "runnerMode",
    "expectedKubeContext",
    "namespace",
    "namespaceUid",
    "capturedAt",
    "verifiedAud065ReportSha256",
    "botOrchestratorDeployment",
    "validator",
    "hmacSha256"
  ])) {
    throw new Error("process_local_cutover_attestation_shape_invalid");
  }
  verifyAttestationHmac(attestation, key);
  if (
    attestation.schemaVersion !== 1 ||
    attestation.profileId !== PROFILE_ID ||
    attestation.profileSha256 !== PROFILE_SHA256 ||
    attestation.cloudSourceCommit !== PROFILE_CLOUD_COMMIT ||
    attestation.historicalResourceCount !== 42 ||
    attestation.requiredDeploymentCount !== 12 ||
    attestation.requiredImagePairCount !== 13 ||
    attestation.runnerMode !== "process-local" ||
    attestation.expectedKubeContext !== expectedKubeContext ||
    attestation.namespace !== namespace ||
    !HEX_SHA256.test(attestation.verifiedAud065ReportSha256 || "") ||
    !exactKeys(attestation.validator, ["name", "status"]) ||
    attestation.validator.name !== "yenhubs-redacted-rollout-contract" ||
    attestation.validator.status !== "passed" ||
    !exactKeys(attestation.botOrchestratorDeployment, [
      "name", "uid", "resourceVersion", "processLocalExact"
    ]) ||
    attestation.botOrchestratorDeployment.name !== "bot-orchestrator" ||
    attestation.botOrchestratorDeployment.processLocalExact !== true
  ) {
    throw new Error("process_local_cutover_attestation_contract_invalid");
  }
  const capturedAt = Date.parse(attestation.capturedAt);
  const current = now();
  if (
    !Number.isFinite(capturedAt) ||
    capturedAt > current + 30_000 ||
    current - capturedAt > MAX_ATTESTATION_AGE_MS
  ) {
    throw new Error("process_local_cutover_attestation_stale");
  }
  if (
    !activeCutoverNamespace(liveNamespace, namespace) ||
    liveNamespace.metadata.uid !== attestation.namespaceUid ||
    liveDeployment?.apiVersion !== "apps/v1" ||
    liveDeployment?.kind !== "Deployment" ||
    liveDeployment?.metadata?.namespace !== namespace ||
    liveDeployment?.metadata?.name !== "bot-orchestrator" ||
    liveDeployment?.metadata?.uid !== attestation.botOrchestratorDeployment.uid ||
    liveDeployment?.metadata?.resourceVersion !==
      attestation.botOrchestratorDeployment.resourceVersion
  ) {
    throw new Error("process_local_cutover_live_binding_changed");
  }
  return true;
}

function parentPodCanExerciseRunnerAuthority(pod) {
  return pod?.spec?.serviceAccountName === "bot-orchestrator" ||
    pod?.metadata?.labels?.app === "bot-runner" ||
    pod?.metadata?.labels?.component === "bot-runner" ||
    pod?.metadata?.labels?.["yenhubs.org/managed-by"] === "bot-orchestrator";
}

function activeCutoverNamespace(namespace, expectedName) {
  return namespace?.apiVersion === "v1" &&
    namespace?.kind === "Namespace" &&
    namespace?.metadata?.name === expectedName &&
    typeof namespace?.metadata?.uid === "string" && namespace.metadata.uid.length > 0 &&
    typeof namespace?.metadata?.resourceVersion === "string" &&
    namespace.metadata.resourceVersion.length > 0 &&
    namespace.metadata.generateName === undefined &&
    namespace.metadata.deletionTimestamp === undefined &&
    namespace.metadata.deletionGracePeriodSeconds === undefined &&
    namespace.metadata.ownerReferences === undefined &&
    namespace.metadata.finalizers === undefined &&
    JSON.stringify(namespace?.spec) === JSON.stringify({ finalizers: ["kubernetes"] }) &&
    namespace?.status?.phase === "Active";
}

function classifyCutoverPreflight({
  targetActivation,
  parentNamespaceExists,
  liveActivation,
  durableParentPolicyObserved
}) {
  if (!parentNamespaceExists) {
    if (targetActivation !== "bootstrap") {
      throw new Error("runner_clean_install_requires_bootstrap_target");
    }
    return "clean-install";
  }
  if (liveActivation === "legacy") {
    if (targetActivation !== "bootstrap") {
      throw new Error("process_local_live_runtime_requires_pristine_bootstrap_target");
    }
    return "pristine-cutover";
  }
  if (durableParentPolicyObserved !== true) {
    throw new Error("pre_fence_control_plane_cannot_be_refenced_without_durable_parent_policy");
  }
  return "fence-aware";
}

function executeCutoverPreflight({
  targetActivation,
  readParentNamespace,
  readParentDeployment,
  readDurableParentPolicyObserved,
  verifyPristineEvidence,
  verifyCleanEvidence = () => {},
  verifyCleanCapability = () => {}
}) {
  if (
    typeof readParentNamespace !== "function" ||
    typeof readParentDeployment !== "function" ||
    typeof readDurableParentPolicyObserved !== "function" ||
    typeof verifyPristineEvidence !== "function" ||
    typeof verifyCleanEvidence !== "function" ||
    typeof verifyCleanCapability !== "function"
  ) {
    throw new Error("process_local_cutover_preflight_adapter_invalid");
  }
  const namespace = readParentNamespace();
  if (namespace === null) {
    const classification = classifyCutoverPreflight({
      targetActivation,
      parentNamespaceExists: false,
      liveActivation: "legacy",
      durableParentPolicyObserved: false
    });
    verifyCleanCapability();
    return classification;
  }
  const deployment = readParentDeployment();
  if (
    deployment === null &&
    namespace?.metadata?.annotations?.[CLEAN_INSTALL_ANNOTATION] === CLEAN_INSTALL_VALUE
  ) {
    if (targetActivation !== "bootstrap") {
      throw new Error("runner_clean_install_requires_bootstrap_target");
    }
    verifyCleanCapability();
    verifyCleanEvidence(namespace?.metadata?.uid);
    return "clean-install-resume";
  }
  const liveActivation =
    deployment?.metadata?.annotations?.["yenhubs.org/runner-activation-phase"] || "legacy";
  const classification = classifyCutoverPreflight({
    targetActivation,
    parentNamespaceExists: true,
    liveActivation,
    durableParentPolicyObserved: liveActivation === "legacy"
      ? false
      : readDurableParentPolicyObserved()
  });
  if (classification === "pristine-cutover") verifyPristineEvidence();
  return classification;
}

function executeCutoverRevalidation({
  classification,
  expectedFenceEvidence,
  verifyPristineEvidence,
  verifyCleanEvidence,
  readFenceEvidence
}) {
  if (classification === "pristine-cutover") {
    if (typeof verifyPristineEvidence !== "function") {
      throw new Error("process_local_cutover_revalidation_adapter_invalid");
    }
    verifyPristineEvidence();
    return true;
  }
  if (["clean-install", "clean-install-resume"].includes(classification)) {
    if (typeof verifyCleanEvidence !== "function") {
      throw new Error("process_local_cutover_revalidation_adapter_invalid");
    }
    verifyCleanEvidence();
    return true;
  }
  if (classification === "fence-aware") {
    if (typeof readFenceEvidence !== "function" || !expectedFenceEvidence) {
      throw new Error("process_local_cutover_revalidation_adapter_invalid");
    }
    const current = readFenceEvidence();
    if (!current || canonicalJson(current) !== canonicalJson(expectedFenceEvidence)) {
      throw new Error("durable_parent_fence_state_changed_after_preflight");
    }
    return true;
  }
  throw new Error("runner_cutover_preflight_classification_missing");
}

function exactZeroAuthority(authority) {
  return exactKeys(authority, ["parent", "runner"]) &&
    [authority.parent, authority.runner].every(value =>
      exactKeys(value, ["create", "delete", "patch"]) &&
      value.create === false && value.delete === false && value.patch === false
    );
}

function exactAbsentIsolatedResources(isolatedResources) {
  return exactKeys(isolatedResources, ISOLATED_RESOURCE_KEYS) &&
    Object.values(isolatedResources).every(resource => resource === null);
}

function verifyJournalCutoverIsolationGate({
  runnerNamespace,
  isolatedResources,
  parentPodList,
  authority,
  allowParentCandidates = false
}) {
  if (runnerNamespace !== null || !exactAbsentIsolatedResources(isolatedResources)) {
    throw new Error("runner_cutover_journal_control_plane_not_isolated");
  }
  const completeParentPods = requireCompletePodList(parentPodList);
  if (!allowParentCandidates && completeParentPods.items.some(parentPodCanExerciseRunnerAuthority)) {
    throw new Error("runner_cutover_journal_parent_candidate_present");
  }
  if (!exactZeroAuthority(authority)) {
    throw new Error("runner_cutover_journal_authority_present");
  }
  return { parentPodListResourceVersion: completeParentPods.resourceVersion };
}

function verifyCleanInstallCutoverGate({
  namespace,
  expectedNamespaceUid,
  liveNamespace,
  liveDeployment,
  runnerNamespace,
  isolatedResources,
  parentPodList,
  authority
}) {
  if (
    !activeCutoverNamespace(liveNamespace, namespace) ||
    liveNamespace.metadata.annotations?.[CLEAN_INSTALL_ANNOTATION] !== CLEAN_INSTALL_VALUE ||
    (expectedNamespaceUid !== undefined &&
      liveNamespace.metadata.uid !== expectedNamespaceUid) ||
    liveDeployment !== null
  ) {
    throw new Error("runner_clean_install_namespace_or_deployment_changed");
  }
  if (runnerNamespace !== null || !exactAbsentIsolatedResources(isolatedResources)) {
    throw new Error("runner_clean_install_control_plane_present");
  }
  const completeParentPods = requireCompletePodList(parentPodList);
  if (completeParentPods.items.some(parentPodCanExerciseRunnerAuthority)) {
    throw new Error("runner_clean_install_parent_candidate_present");
  }
  if (!exactZeroAuthority(authority)) {
    throw new Error("runner_clean_install_authority_present");
  }
  return {
    namespaceUid: liveNamespace.metadata.uid,
    parentPodListResourceVersion: completeParentPods.resourceVersion
  };
}

function verifyPristineLegacyCutoverGate({
  attestation,
  key,
  namespace,
  expectedKubeContext,
  liveNamespace,
  liveDeployment,
  runnerNamespace,
  isolatedResources,
  parentPodList,
  authority,
  now
}) {
  verifyProcessLocalCutoverAttestation(attestation, {
    key,
    namespace,
    expectedKubeContext,
    liveNamespace,
    liveDeployment,
    now
  });
  const annotations = liveDeployment?.metadata?.annotations || {};
  if (LEGACY_ANNOTATIONS.some(name => Object.hasOwn(annotations, name))) {
    throw new Error("process_local_cutover_live_runtime_not_legacy");
  }
  if (runnerNamespace !== null) {
    throw new Error("process_local_cutover_runner_namespace_present");
  }
  if (!exactAbsentIsolatedResources(isolatedResources)) {
    throw new Error("process_local_cutover_isolated_control_plane_present");
  }
  const completeParentPods = requireCompletePodList(parentPodList);
  if (completeParentPods.items.some(parentPodCanExerciseRunnerAuthority)) {
    throw new Error("process_local_cutover_parent_runner_candidate_present");
  }
  if (!exactZeroAuthority(authority)) {
    throw new Error("process_local_cutover_runner_authority_present");
  }
  return {
    namespaceUid: liveNamespace.metadata.uid,
    deploymentUid: liveDeployment.metadata.uid,
    deploymentResourceVersion: liveDeployment.metadata.resourceVersion,
    parentPodListResourceVersion: completeParentPods.resourceVersion
  };
}

module.exports = {
  MAX_ATTESTATION_AGE_MS,
  CLEAN_INSTALL_ANNOTATION,
  CLEAN_INSTALL_VALUE,
  ISOLATED_RESOURCE_KEYS,
  PROFILE_CLOUD_COMMIT,
  PROFILE_ID,
  PROFILE_SHA256,
  activeCutoverNamespace,
  canonicalJson,
  classifyCutoverPreflight,
  executeCutoverPreflight,
  executeCutoverRevalidation,
  parentPodCanExerciseRunnerAuthority,
  readPrivateCutoverAttestation,
  readPrivateCutoverKey,
  verifyAttestationHmac,
  verifyCleanInstallCutoverGate,
  verifyJournalCutoverIsolationGate,
  verifyPristineLegacyCutoverGate,
  verifyProcessLocalCutoverAttestation
};
