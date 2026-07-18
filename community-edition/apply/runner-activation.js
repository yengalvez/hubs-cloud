const fs = require("node:fs");
const { isDeepStrictEqual } = require("node:util");
const YAML = require("yaml");

const RUNNER_NAMESPACE = "hcce-bot-runners";
const ADMISSION_POLICY_NAME = "bot-runner-pods.yenhubs.org";
const ACTIVATION_PHASE_ANNOTATION = "yenhubs.org/runner-activation-phase";
const RECOVERY_PHASE_ANNOTATION = "yenhubs.org/bot-runner-recovery-phase";
const RECOVERY_EPOCH_ANNOTATION = "yenhubs.org/bot-runner-recovery-epoch";
const RECOVERY_LOCK_NAME = "yenhubs-recovery-operation-lock";
const RECOVERY_CONSUMERS = Object.freeze([
  "reticulum",
  "pgbouncer",
  "pgbouncer-t",
  "bot-orchestrator",
  "coturn"
]);

function manifestResources(manifestPath) {
  return YAML.parseAllDocuments(fs.readFileSync(manifestPath, "utf8"))
    .map(document => document.toJS())
    .filter(Boolean);
}

function findDeployment(resources, name) {
  return resources.find(
    resource => resource?.apiVersion === "apps/v1" &&
      resource?.kind === "Deployment" &&
      resource?.metadata?.name === name
  );
}

function readActivationPlan(manifestPath) {
  const resources = manifestResources(manifestPath);
  const deployment = findDeployment(resources, "bot-orchestrator");
  const roleBinding = resources.find(
    resource => resource?.kind === "RoleBinding" &&
      resource?.metadata?.namespace === RUNNER_NAMESPACE &&
      resource?.metadata?.name === "bot-orchestrator-runner-pods"
  );
  const role = resources.find(
    resource => resource?.kind === "Role" &&
      resource?.metadata?.namespace === RUNNER_NAMESPACE &&
      resource?.metadata?.name === "bot-orchestrator-runner-pods"
  );
  const activationPhase = deployment?.metadata?.annotations?.[ACTIVATION_PHASE_ANNOTATION];
  const recoveryPhase = deployment?.metadata?.annotations?.[RECOVERY_PHASE_ANNOTATION];
  const recoveryEpoch = deployment?.metadata?.annotations?.[RECOVERY_EPOCH_ANNOTATION];
  if (!["bootstrap", "admission", "active"].includes(activationPhase)) {
    throw new Error("generated_manifest_runner_activation_phase_invalid");
  }
  if (!["active", "restore-fence"].includes(recoveryPhase)) {
    throw new Error("generated_manifest_runner_recovery_phase_invalid");
  }
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(recoveryEpoch || "")) {
    throw new Error("generated_manifest_runner_recovery_epoch_invalid");
  }

  const authorityIsInert = activationPhase === "bootstrap" || recoveryPhase === "restore-fence";
  const expectedReplicas = recoveryPhase === "restore-fence"
    ? 0
    : activationPhase === "active" ? 1 : 0;
  const expectedRuleCount = authorityIsInert ? 0 : 1;
  if (
    deployment?.spec?.replicas !== expectedReplicas ||
    roleBinding?.metadata?.namespace !== RUNNER_NAMESPACE ||
    roleBinding?.metadata?.annotations?.[ACTIVATION_PHASE_ANNOTATION] !== activationPhase ||
    roleBinding?.metadata?.annotations?.[RECOVERY_PHASE_ANNOTATION] !== recoveryPhase ||
    roleBinding?.subjects?.length !== 1 ||
    roleBinding.subjects[0]?.name !== "bot-orchestrator" ||
    role?.metadata?.annotations?.[ACTIVATION_PHASE_ANNOTATION] !== activationPhase ||
    role?.metadata?.annotations?.[RECOVERY_PHASE_ANNOTATION] !== recoveryPhase ||
    !Array.isArray(role?.rules) ||
    role.rules.length !== expectedRuleCount
  ) {
    throw new Error("generated_manifest_runner_activation_contract_invalid");
  }

  const expectedRecoveryReplicas = recoveryPhase === "restore-fence" ? 0 : 1;
  for (const name of RECOVERY_CONSUMERS.filter(value => value !== "bot-orchestrator")) {
    const consumer = findDeployment(resources, name);
    if (
      consumer?.metadata?.annotations?.[RECOVERY_PHASE_ANNOTATION] !== recoveryPhase ||
      consumer?.spec?.replicas !== expectedRecoveryReplicas
    ) {
      throw new Error(`generated_manifest_recovery_consumer_invalid:${name}`);
    }
  }
  const pgsql = findDeployment(resources, "pgsql");
  if (
    pgsql?.metadata?.annotations?.[RECOVERY_PHASE_ANNOTATION] !== recoveryPhase ||
    pgsql?.spec?.replicas !== 1
  ) {
    throw new Error("generated_manifest_recovery_pgsql_invalid");
  }
  return { activationPhase, recoveryPhase, recoveryEpoch, resources };
}

function readActivationPhase(manifestPath) {
  return readActivationPlan(manifestPath).activationPhase;
}

function admissionPolicyIsObserved(policy) {
  const generation = policy?.metadata?.generation;
  const status = policy?.status;
  const typeChecking = status?.typeChecking;
  const warnings = typeChecking?.expressionWarnings;
  const warningsAreEmpty = warnings === undefined ||
    (Array.isArray(warnings) && warnings.length === 0);
  const conditions = status?.conditions;
  return Number.isInteger(generation) &&
    generation > 0 &&
    status?.observedGeneration === generation &&
    typeChecking !== undefined &&
    typeChecking !== null &&
    typeof typeChecking === "object" &&
    !Array.isArray(typeChecking) &&
    warningsAreEmpty &&
    (conditions === undefined ||
      (Array.isArray(conditions) && conditions.every(condition => condition?.status === "True")));
}

function podsUsingServiceAccount(pods, serviceAccountName) {
  if (pods?.kind !== "PodList" || !Array.isArray(pods.items)) return null;
  return pods.items.filter(pod => pod?.spec?.serviceAccountName === serviceAccountName);
}

function parentIsQuiesced(deployment, pods) {
  const serviceAccountPods = podsUsingServiceAccount(pods, "bot-orchestrator");
  return deployment?.spec?.replicas === 0 &&
    Number(deployment?.status?.replicas || 0) === 0 &&
    Number(deployment?.status?.readyReplicas || 0) === 0 &&
    Array.isArray(serviceAccountPods) &&
    serviceAccountPods.length === 0;
}

function uniqueRunnerPods(podLists) {
  const unique = new Map();
  for (const podList of podLists) {
    if (podList?.kind !== "PodList" || !Array.isArray(podList.items)) return null;
    for (const pod of podList.items) {
      const key = pod?.metadata?.uid ||
        `${pod?.metadata?.namespace || ""}/${pod?.metadata?.name || ""}`;
      unique.set(key, pod);
    }
  }
  return [...unique.values()];
}

function podIsRecoveryConsumer(pod, replicaSets = []) {
  if (!pod || typeof pod !== "object" || !Array.isArray(replicaSets)) return false;
  const labels = pod?.metadata?.labels;
  const labelValues = labels && typeof labels === "object" && !Array.isArray(labels)
    ? ["app", "component", "app.kubernetes.io/name", "k8s-app", "yenhubs.org/component"]
      .map(key => labels[key])
    : [];
  const names = [pod?.metadata?.name, pod?.metadata?.generateName].filter(
    value => typeof value === "string" && value
  );
  const containers = Array.isArray(pod?.spec?.containers) ? pod.spec.containers : [];
  const owners = Array.isArray(pod?.metadata?.ownerReferences)
    ? pod.metadata.ownerReferences
    : [];
  const replicaSetIdentities = new Set(
    replicaSets.flatMap(replicaSet => [
      replicaSet?.metadata?.name,
      replicaSet?.metadata?.uid
    ]).filter(value => typeof value === "string" && value)
  );
  const matchesConsumerName = value => typeof value === "string" &&
    RECOVERY_CONSUMERS.some(name => value === name || value.startsWith(`${name}-`));
  return pod?.spec?.serviceAccountName === "bot-orchestrator" ||
    labelValues.some(matchesConsumerName) ||
    names.some(matchesConsumerName) ||
    containers.some(container => matchesConsumerName(container?.name)) ||
    owners.some(owner =>
      (owner?.kind === "Deployment" && matchesConsumerName(owner?.name)) ||
      (owner?.kind === "ReplicaSet" && (
        matchesConsumerName(owner?.name) ||
        replicaSetIdentities.has(owner?.name) ||
        replicaSetIdentities.has(owner?.uid)
      ))
    );
}

function replicaSetIsRecoveryConsumer(replicaSet) {
  if (!replicaSet || typeof replicaSet !== "object") return false;
  const owners = Array.isArray(replicaSet?.metadata?.ownerReferences)
    ? replicaSet.metadata.ownerReferences
    : [];
  const labels = replicaSet?.metadata?.labels || {};
  return owners.some(owner =>
    owner?.kind === "Deployment" && RECOVERY_CONSUMERS.includes(owner?.name)
  ) || [labels.app, labels.component, labels["app.kubernetes.io/name"]]
    .some(value => RECOVERY_CONSUMERS.includes(value)) ||
    RECOVERY_CONSUMERS.some(name =>
      replicaSet?.metadata?.name === name ||
      replicaSet?.metadata?.name?.startsWith(`${name}-`)
    );
}

function recoveryConsumerReplicaSets(replicaSetList) {
  if (replicaSetList?.kind !== "ReplicaSetList" || !Array.isArray(replicaSetList.items)) {
    return null;
  }
  return replicaSetList.items.filter(replicaSetIsRecoveryConsumer);
}

function replicaSetIsStopped(replicaSet) {
  return replicaSet?.spec?.replicas === 0 &&
    Number(replicaSet?.status?.replicas || 0) === 0 &&
    Number(replicaSet?.status?.readyReplicas || 0) === 0 &&
    Number(replicaSet?.status?.availableReplicas || 0) === 0;
}

function recoveryConsumerReplicaSetsAreStopped(replicaSets) {
  return Array.isArray(replicaSets) && replicaSets.every(replicaSetIsStopped);
}

function recoveryConsumersAreQuiesced(deploymentList, podList, replicaSetList) {
  if (
    deploymentList?.kind !== "DeploymentList" || !Array.isArray(deploymentList.items) ||
    podList?.kind !== "PodList" || !Array.isArray(podList.items) ||
    replicaSetList?.kind !== "ReplicaSetList" || !Array.isArray(replicaSetList.items)
  ) {
    return false;
  }
  const deploymentsAreStopped = RECOVERY_CONSUMERS.every(name => {
    const deployment = deploymentList.items.find(item => item?.metadata?.name === name);
    return deployment?.spec?.replicas === 0 &&
      Number(deployment?.status?.replicas || 0) === 0 &&
      Number(deployment?.status?.readyReplicas || 0) === 0;
  }) && deploymentList.items.find(item => item?.metadata?.name === "pgsql")?.spec?.replicas === 1;
  const consumerReplicaSets = recoveryConsumerReplicaSets(replicaSetList);
  return deploymentsAreStopped &&
    recoveryConsumerReplicaSetsAreStopped(consumerReplicaSets) &&
    podList.items.every(pod => !podIsRecoveryConsumer(pod, consumerReplicaSets));
}

function createStableWindowPredicate(predicate, { now = () => Date.now(), windowMs = 61_000 } = {}) {
  if (typeof predicate !== "function" || typeof now !== "function" || windowMs < 0) {
    throw new Error("stable_window_configuration_invalid");
  }
  let stableSince = null;
  return () => {
    if (!predicate()) {
      stableSince = null;
      return false;
    }
    const current = now();
    if (stableSince === null) stableSince = current;
    return current - stableSince >= windowMs;
  };
}

function decideApplyMode({
  targetActivation,
  targetRecovery,
  liveActivation,
  liveRecovery,
  lockState
}) {
  if (targetRecovery === "restore-fence") {
    if (lockState !== "restore-fence-prepared") {
      throw new Error("restore_fence_requires_exact_prepared_recovery_lock");
    }
    return "restore-fence";
  }
  if (targetRecovery !== "active") throw new Error("target_recovery_phase_invalid");
  if (lockState !== null) {
    if (
      targetActivation === "active" &&
      lockState === "restore-complete-awaiting-reactivation" &&
      ["restore-fence", "active"].includes(liveRecovery)
    ) {
      return "recovery-reactivation";
    }
    throw new Error("recovery_lock_blocks_requested_apply_transition");
  }
  if (targetActivation === "bootstrap") return "bootstrap";
  if (
    targetActivation === "admission" &&
    liveActivation === "bootstrap" &&
    liveRecovery === "active"
  ) {
    return "admission";
  }
  if (
    targetActivation === "active" &&
    liveActivation === "admission" &&
    liveRecovery === "active"
  ) {
    return "active";
  }
  if (
    targetActivation === "active" &&
    liveActivation === "active" &&
    liveRecovery === "active"
  ) {
    return "active-reapply";
  }
  throw new Error("runner_activation_transition_invalid");
}

function exactAdmissionBinding(binding) {
  return binding?.apiVersion === "admissionregistration.k8s.io/v1" &&
    binding?.kind === "ValidatingAdmissionPolicyBinding" &&
    binding?.metadata?.name === ADMISSION_POLICY_NAME &&
    isDeepStrictEqual(binding?.spec, {
      policyName: ADMISSION_POLICY_NAME,
      validationActions: ["Deny"],
      matchResources: {
        matchPolicy: "Equivalent",
        namespaceSelector: {
          matchLabels: { "kubernetes.io/metadata.name": RUNNER_NAMESPACE }
        },
        objectSelector: {}
      }
    });
}

function exactRecoveryOperationLock(
  lock,
  namespace,
  recoveryEpoch,
  expectedState,
  { namespaceUid, pvcUid, lockUid, lockResourceVersion } = {}
) {
  const annotations = lock?.metadata?.annotations;
  const labels = lock?.metadata?.labels;
  const allowedMetadataKeys = new Set([
    "annotations",
    "creationTimestamp",
    "generation",
    "labels",
    "managedFields",
    "name",
    "namespace",
    "resourceVersion",
    "uid"
  ]);
  const metadataIsExact = lock?.metadata &&
    typeof lock.metadata === "object" &&
    !Array.isArray(lock.metadata) &&
    Object.keys(lock.metadata).every(key => allowedMetadataKeys.has(key));
  const expectedAnnotationKeys = [
    "yenhubs.org/operation-id",
    "yenhubs.org/recovery-token",
    "yenhubs.org/namespace-uid",
    "yenhubs.org/pvc-uid",
    "yenhubs.org/checkpoint-stamp",
    "yenhubs.org/dump-sha256",
    "yenhubs.org/storage-sha256",
    "yenhubs.org/pre-fence-epoch",
    "yenhubs.org/restore-fence-epoch",
    "yenhubs.org/deployment-inventory-sha256",
    "yenhubs.org/recovery-state"
  ];
  const exactKeys = value => value && typeof value === "object" && !Array.isArray(value) &&
    JSON.stringify(Object.keys(value).sort()) === JSON.stringify([...expectedAnnotationKeys].sort());
  const uuidV4 = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
  const hash = /^[0-9a-f]{64}$/;
  const sourceEpoch = annotations?.["yenhubs.org/pre-fence-epoch"];
  return lock?.apiVersion === "v1" &&
    lock?.kind === "ConfigMap" &&
    lock?.metadata?.name === RECOVERY_LOCK_NAME &&
    lock?.metadata?.namespace === namespace &&
    typeof lock?.metadata?.uid === "string" && lock.metadata.uid.length > 0 &&
    (lockUid === undefined || lock.metadata.uid === lockUid) &&
    typeof lock?.metadata?.resourceVersion === "string" && lock.metadata.resourceVersion.length > 0 &&
    (lockResourceVersion === undefined || lock.metadata.resourceVersion === lockResourceVersion) &&
    metadataIsExact &&
    labels && Object.keys(labels).length === 1 &&
    labels["yenhubs.org/recovery-owner"] === "checkpoint-restore" &&
    exactKeys(annotations) &&
    /^[0-9a-f]{32}$/.test(annotations["yenhubs.org/operation-id"] || "") &&
    /^[0-9a-f]{32}$/.test(annotations["yenhubs.org/recovery-token"] || "") &&
    typeof namespaceUid === "string" && namespaceUid.length > 0 &&
    annotations["yenhubs.org/namespace-uid"] === namespaceUid &&
    typeof pvcUid === "string" && pvcUid.length > 0 &&
    annotations["yenhubs.org/pvc-uid"] === pvcUid &&
    /^[0-9]{8}-[0-9]{6}$/.test(annotations["yenhubs.org/checkpoint-stamp"] || "") &&
    hash.test(annotations["yenhubs.org/dump-sha256"] || "") &&
    hash.test(annotations["yenhubs.org/storage-sha256"] || "") &&
    hash.test(annotations["yenhubs.org/deployment-inventory-sha256"] || "") &&
    (sourceEpoch === "legacy-absent" || uuidV4.test(sourceEpoch || "")) &&
    sourceEpoch !== recoveryEpoch &&
    annotations["yenhubs.org/restore-fence-epoch"] === recoveryEpoch &&
    annotations["yenhubs.org/recovery-state"] === expectedState &&
    lock?.immutable === true &&
    (lock?.data === undefined || Object.keys(lock.data).length === 0) &&
    (lock?.binaryData === undefined || Object.keys(lock.binaryData).length === 0);
}

function exactDeploymentDesiredState(live, normalizedExpected) {
  const expectedAnnotations = normalizedExpected?.metadata?.annotations || {};
  const expectedLabels = normalizedExpected?.metadata?.labels || {};
  const liveAnnotations = live?.metadata?.annotations || {};
  const liveLabels = live?.metadata?.labels || {};
  return live?.apiVersion === "apps/v1" &&
    live?.kind === "Deployment" &&
    live?.metadata?.name === normalizedExpected?.metadata?.name &&
    live?.metadata?.namespace === normalizedExpected?.metadata?.namespace &&
    Object.entries(expectedAnnotations).every(([key, value]) => liveAnnotations[key] === value) &&
    Object.entries(expectedLabels).every(([key, value]) => liveLabels[key] === value) &&
    isDeepStrictEqual(live?.spec, normalizedExpected?.spec);
}

function exactFoundationalNamespace(live, expected) {
  const expectedAnnotations = expected?.metadata?.annotations || {};
  const expectedLabels = expected?.metadata?.labels || {};
  const liveAnnotations = live?.metadata?.annotations || {};
  const liveLabels = live?.metadata?.labels || {};
  return live?.apiVersion === "v1" &&
    live?.kind === "Namespace" &&
    live?.metadata?.name === expected?.metadata?.name &&
    live?.metadata?.deletionTimestamp === undefined &&
    live?.metadata?.generateName === undefined &&
    live?.metadata?.ownerReferences === undefined &&
    live?.metadata?.finalizers === undefined &&
    isDeepStrictEqual(live?.spec, { finalizers: ["kubernetes"] }) &&
    live?.status?.phase === "Active" &&
    Object.entries(expectedAnnotations).every(([key, value]) => liveAnnotations[key] === value) &&
    Object.entries(expectedLabels).every(([key, value]) => liveLabels[key] === value);
}

async function runBestEffortFenceSteps(steps) {
  if (!Array.isArray(steps) || steps.some(step =>
    !step || typeof step.name !== "string" || !step.name || typeof step.action !== "function"
  )) {
    throw new Error("fence_steps_invalid");
  }
  const failures = [];
  for (const step of steps) {
    try {
      await step.action();
    } catch (_error) {
      failures.push(step.name);
    }
  }
  return failures;
}

async function retryBestEffortFenceAttempt(
  attempt,
  { maxAttempts = 3, beforeRetry = async () => {} } = {}
) {
  if (
    typeof attempt !== "function" ||
    typeof beforeRetry !== "function" ||
    !Number.isInteger(maxAttempts) ||
    maxAttempts < 1 ||
    maxAttempts > 10
  ) {
    throw new Error("fence_retry_configuration_invalid");
  }
  let failures = [];
  for (let index = 0; index < maxAttempts; index += 1) {
    failures = await attempt(index + 1);
    if (!Array.isArray(failures)) throw new Error("fence_retry_result_invalid");
    if (failures.length === 0) return [];
    if (index + 1 < maxAttempts) await beforeRetry(index + 1, failures);
  }
  return [...new Set(failures)];
}

function applyResourcesSequentially(resources, applyOne) {
  if (!Array.isArray(resources) || typeof applyOne !== "function") {
    throw new Error("sequential_apply_arguments_invalid");
  }
  for (const resource of resources) applyOne(resource);
}

module.exports = {
  ACTIVATION_PHASE_ANNOTATION,
  ADMISSION_POLICY_NAME,
  RECOVERY_CONSUMERS,
  RECOVERY_EPOCH_ANNOTATION,
  RECOVERY_LOCK_NAME,
  RECOVERY_PHASE_ANNOTATION,
  RUNNER_NAMESPACE,
  admissionPolicyIsObserved,
  applyResourcesSequentially,
  createStableWindowPredicate,
  decideApplyMode,
  exactAdmissionBinding,
  exactDeploymentDesiredState,
  exactFoundationalNamespace,
  exactRecoveryOperationLock,
  manifestResources,
  parentIsQuiesced,
  podIsRecoveryConsumer,
  podsUsingServiceAccount,
  readActivationPhase,
  readActivationPlan,
  replicaSetIsRecoveryConsumer,
  replicaSetIsStopped,
  recoveryConsumerReplicaSets,
  recoveryConsumerReplicaSetsAreStopped,
  recoveryConsumersAreQuiesced,
  retryBestEffortFenceAttempt,
  runBestEffortFenceSteps,
  uniqueRunnerPods
};
