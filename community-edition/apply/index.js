const { execFileSync, spawn, spawnSync } = require("node:child_process");
const { randomUUID } = require("node:crypto");
const path = require("node:path");
const utils = require("../utils");
const { KubernetesRunnerManager } = require("../services/bot-orchestrator/kubernetes-runner-manager");
const {
  collectLiveRunnerControlPlane,
  verifyLiveRunnerControlPlane
} = require("./live-runner-control-plane");
const {
  PodWatchEvidence,
  ReplicaSetWatchEvidence,
  forbiddenPod,
  podWatchRawPath,
  replicaSetWatchRawPath
} = require("./pod-quiescence-watch");
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
  RECOVERY_CONSUMERS,
  RECOVERY_EPOCH_ANNOTATION,
  RECOVERY_LOCK_NAME,
  RECOVERY_PHASE_ANNOTATION,
  RUNNER_NAMESPACE,
  admissionPolicyIsObserved,
  applyResourcesSequentially,
  decideApplyMode,
  exactAdmissionBinding,
  exactDeploymentDesiredState,
  exactFoundationalNamespace,
  exactRecoveryOperationLock,
  parentIsQuiesced,
  readActivationPlan,
  recoveryConsumerReplicaSets,
  recoveryConsumerReplicaSetsAreStopped,
  recoveryConsumersAreQuiesced,
  retryBestEffortFenceAttempt,
  runBestEffortFenceSteps,
  uniqueRunnerPods
} = require("./runner-activation");

const manifestPath = path.resolve(process.env.HCCE_MANIFEST_PATH || "hcce.yaml");
const config = utils.readConfig(process.env.HCCE_INPUT_VALUES_PATH);
const parentNamespace = config.Namespace;
const plan = readActivationPlan(manifestPath);
const waitTimeoutMs = 180_000;
const kubectlReadTimeoutMs = 30_000;
const kubectlContext = String(process.env.KUBECTL_CONTEXT || "");
const leaseHeartbeatPath = path.resolve(__dirname, "operation-lease.js");
const manifestVerifierPath = path.resolve(__dirname, "../generate_script/verify-generated-manifest.js");
let operationLeaseGuard = null;
let failClosedRefenceRequired = false;
let recoveryLockIdentityGuard = null;

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
    [manifestVerifierPath],
    { stdio: "inherit", timeout: MUTATION_TIMEOUT_MS }
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
      if (exactFoundationalNamespace(live, expected)) return;
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

function liveAdmissionIsObserved() {
  const policy = kubectlJson([
    "get", "validatingadmissionpolicy", ADMISSION_POLICY_NAME, "-o", "json"
  ]);
  const binding = kubectlJson([
    "get", "validatingadmissionpolicybinding", ADMISSION_POLICY_NAME, "-o", "json"
  ]);
  return admissionPolicyIsObserved(policy) && exactAdmissionBinding(binding);
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

function liveRunnerPods() {
  const podLists = [];
  podLists.push(listPodsBySelector(parentNamespace, "app=bot-runner"));
  podLists.push(listPodsBySelector(parentNamespace, "component=bot-runner"));
  podLists.push(listPodsBySelector(parentNamespace, "yenhubs.org/managed-by=bot-orchestrator"));
  podLists.push(listPodsBySelector(RUNNER_NAMESPACE, ""));
  const pods = uniqueRunnerPods(podLists);
  if (!Array.isArray(pods)) throw new Error("runner_pod_lists_invalid");
  return pods;
}

function deletePodByExactUid(pod) {
  const namespace = pod?.metadata?.namespace;
  const name = pod?.metadata?.name;
  const uid = pod?.metadata?.uid;
  if (
    ![parentNamespace, RUNNER_NAMESPACE].includes(namespace) ||
    typeof name !== "string" || !name ||
    typeof uid !== "string" || !uid
  ) {
    throw new Error("runner_pod_delete_identity_invalid");
  }
  const path = `/api/v1/namespaces/${encodeURIComponent(namespace)}/pods/${encodeURIComponent(name)}`;
  const deleteOptions = {
    apiVersion: "v1",
    kind: "DeleteOptions",
    preconditions: { uid },
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
    throw new Error(`runner_pod_uid_delete_failed:${deleted.status}`);
  }
}

async function deleteAllRunnerPodsByExactUid() {
  const pods = liveRunnerPods();
  return runBestEffortFenceSteps(pods.map(pod => ({
    name: `pod:${pod?.metadata?.namespace || "invalid"}/${pod?.metadata?.name || "invalid"}`,
    action: async () => deletePodByExactUid(pod)
  })));
}

function parentAndRunnerPodsAreAbsent() {
  return liveParentIsQuiesced() && liveRunnerPods().length === 0;
}

function podListForWatch(namespace) {
  return kubectlJson(["-n", namespace, "get", "pods", "-o", "json"]);
}

function podListHasForbidden(
  namespace,
  podList,
  { includeRecoveryConsumers = false, recoveryConsumerReplicaSets = [] } = {}
) {
  return podList?.kind !== "PodList" ||
    !Array.isArray(podList.items) ||
    podList.items.some(pod => forbiddenPod(namespace, parentNamespace, pod, {
      includeRecoveryConsumers,
      recoveryConsumerReplicaSets
    }));
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
  const rawPath = podWatchRawPath(namespace, resourceVersion);
  return startRawEvidenceWatch(rawPath, evidence);
}

function startReplicaSetWatch(resourceVersion, recoveryConsumerReplicaSets) {
  assertOperationLeaseHeld();
  const evidence = new ReplicaSetWatchEvidence(resourceVersion, recoveryConsumerReplicaSets);
  const rawPath = replicaSetWatchRawPath(parentNamespace, resourceVersion);
  return startRawEvidenceWatch(rawPath, evidence);
}

function startRawEvidenceWatch(rawPath, evidence) {
  const child = spawn("kubectl", contextArgs(["get", "--raw", rawPath]), {
    stdio: ["ignore", "pipe", "pipe"]
  });
  let buffer = "";
  let intentionalStop = false;
  child.stdout.setEncoding("utf8");
  child.stdout.on("data", chunk => {
    buffer += chunk;
    for (;;) {
      const newline = buffer.indexOf("\n");
      if (newline < 0) break;
      const line = buffer.slice(0, newline).trim();
      buffer = buffer.slice(newline + 1);
      if (!line) continue;
      try {
        evidence.ingest(JSON.parse(line));
      } catch (_error) {
        evidence.error = "watch_json_invalid";
      }
    }
  });
  child.stderr.on("data", () => {});
  child.once("error", () => { evidence.error = "watch_process_error"; });
  child.once("exit", () => {
    if (buffer.trim()) {
      try {
        evidence.ingest(JSON.parse(buffer));
      } catch (_error) {
        evidence.error = "watch_json_invalid";
      }
    }
    if (!intentionalStop && !evidence.error) evidence.error = "watch_ended_before_evidence_boundary";
  });
  return {
    evidence,
    stop() {
      intentionalStop = true;
      if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
    }
  };
}

async function waitForWatchInitialEventsEnd(watches, deadline) {
  while (Date.now() < deadline) {
    assertOperationLeaseProcessHealthy();
    if (watches.some(watch => watch.evidence.error || watch.evidence.violation)) return false;
    if (watches.every(watch => watch.evidence.coversInitialEvents())) return true;
    await sleep(100);
  }
  return false;
}

async function confirmWatchBoundary(
  predecessors,
  boundaryLists,
  deadline,
  { includeRecoveryConsumers = false, recoveryConsumerReplicaSets = [] } = {}
) {
  const successors = boundaryLists.map((podList, index) =>
    startPodWatch(
      [parentNamespace, RUNNER_NAMESPACE][index],
      podList?.metadata?.resourceVersion,
      { includeRecoveryConsumers, recoveryConsumerReplicaSets }
    )
  );
  if (includeRecoveryConsumers) {
    const boundaryReplicaSets = recoveryReplicaSetEvidence();
    if (!boundaryReplicaSets.valid) {
      successors.forEach(watch => watch.stop());
      return false;
    }
    for (const replicaSet of boundaryReplicaSets.consumerReplicaSets) {
      if (!recoveryConsumerReplicaSets.some(value =>
        value?.metadata?.uid === replicaSet?.metadata?.uid ||
        value?.metadata?.name === replicaSet?.metadata?.name
      )) {
        recoveryConsumerReplicaSets.push(replicaSet);
      }
    }
    successors.push(startReplicaSetWatch(
      boundaryReplicaSets.resourceVersion,
      recoveryConsumerReplicaSets
    ));
  }
  try {
    const successorReady = await waitForWatchInitialEventsEnd(successors, deadline);
    return successorReady &&
      predecessors.every(watch => !watch.evidence.error && !watch.evidence.violation) &&
      successors.every(watch => !watch.evidence.error && !watch.evidence.violation);
  } finally {
    successors.forEach(watch => watch.stop());
  }
}

function recoveryReplicaSetEvidence() {
  const replicaSetList = kubectlJson([
    "-n", parentNamespace, "get", "replicasets", "-o", "json"
  ]);
  const consumerReplicaSets = recoveryConsumerReplicaSets(replicaSetList);
  const resourceVersion = replicaSetList?.metadata?.resourceVersion;
  return {
    consumerReplicaSets,
    resourceVersion,
    valid: typeof resourceVersion === "string" && resourceVersion.length > 0 &&
      recoveryConsumerReplicaSetsAreStopped(consumerReplicaSets)
  };
}

async function acquireStablePodAbsenceMonitor(
  label,
  { includeRecoveryConsumers = false } = {}
) {
  const deadline = Date.now() + waitTimeoutMs;
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
    const watches = initialLists.map((podList, index) =>
      startPodWatch(
        [parentNamespace, RUNNER_NAMESPACE][index],
        podList?.metadata?.resourceVersion,
        watchOptions
      )
    );
    if (includeRecoveryConsumers) {
      watches.push(startReplicaSetWatch(
        replicaSetEvidence.resourceVersion,
        replicaSetEvidence.consumerReplicaSets
      ));
    }
    if (!(await waitForWatchInitialEventsEnd(watches, deadline))) {
      watches.forEach(watch => watch.stop());
      await sleep(1_000);
      continue;
    }
    const stableUntil = Date.now() + 61_000;
    while (
      Date.now() < stableUntil &&
      watches.every(watch => !watch.evidence.error && !watch.evidence.violation)
    ) {
      assertOperationLeaseProcessHealthy();
      await sleep(100);
    }
    if (watches.some(watch => watch.evidence.error || watch.evidence.violation)) {
      watches.forEach(watch => watch.stop());
      await sleep(1_000);
      continue;
    }
    const boundaryLists = [podListForWatch(parentNamespace), podListForWatch(RUNNER_NAMESPACE)];
    const forbiddenAtBoundary = boundaryLists.some((podList, index) =>
      podListHasForbidden(
        [parentNamespace, RUNNER_NAMESPACE][index],
        podList,
        watchOptions
      )
    );
    if (
      forbiddenAtBoundary ||
      !(await confirmWatchBoundary(watches, boundaryLists, deadline, {
        ...watchOptions
      })) ||
      watches.some(watch => watch.evidence.error || watch.evidence.violation)
    ) {
      watches.forEach(watch => watch.stop());
      await sleep(1_000);
      continue;
    }
    return { watches, watchOptions };
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
  const { watches, watchOptions } = monitor;
  try {
    await action();
    const boundaryLists = [podListForWatch(parentNamespace), podListForWatch(RUNNER_NAMESPACE)];
    if (
      boundaryLists.some((podList, index) =>
        podListHasForbidden(
          [parentNamespace, RUNNER_NAMESPACE][index],
          podList,
          watchOptions
        )
      ) ||
      !(await confirmWatchBoundary(
        watches,
        boundaryLists,
        Date.now() + 30_000,
        watchOptions
      )) ||
      watches.some(watch => watch.evidence.error || watch.evidence.violation)
    ) {
      throw new Error(`${label}_event_or_watch_failure`);
    }
  } catch (error) {
    if (rollback) await rollback();
    throw error;
  } finally {
    watches.forEach(watch => watch.stop());
  }
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
    diagnostic.includes("Pod-bound parent ServiceAccount principal") &&
    !diagnostic.includes("violates PodSecurity");
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
  applyResourcesSequentially(plan.resources, applyResource);
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
      const deletionFailures = await deleteAllRunnerPodsByExactUid();
      return [...fenceFailures, ...deletionFailures];
    },
    { maxAttempts: 3, beforeRetry: async () => sleep(1_000) }
  );
}

async function establishActiveStagingFenceMutations() {
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
      const deletionFailures = await deleteAllRunnerPodsByExactUid();
      return [...fenceFailures, ...deletionFailures];
    },
    { maxAttempts: 3, beforeRetry: async () => sleep(1_000) }
  );
}

async function refenceActiveReapplyForStaging() {
  const failures = await establishActiveStagingFenceMutations();
  try {
    await waitFor("active_reapply_staging_all_consumers_and_runners_quiesced", () =>
      liveParentIsQuiesced() &&
      liveRecoveryConsumersAreQuiesced() &&
      liveRunnerPods().length === 0
    );
  } catch (_error) {
    failures.push("quiescence-verification");
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
    ["runner-pods-absent", () => liveRunnerPods().length === 0],
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
  try {
    await waitFor(`${label}_all_consumers_and_runners_quiesced`, () =>
      liveParentIsQuiesced() &&
      liveRecoveryConsumersAreQuiesced() &&
      liveRunnerPods().length === 0
    );
  } catch (_error) {
    failures.push("quiescence-verification");
  }
  try {
    await runWithStablePodAbsence(
      `stable_pod_absence_before_${label}_authority_refence`,
      async () => {
        if (
          expectedRecoveryLockState &&
          !exactLiveRecoveryLock(expectedRecoveryLockState)
        ) {
          throw new Error(`${label}_recovery_lock_changed_during_refence`);
        }
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
    ["runner-pods-absent", () => liveRunnerPods().length === 0]
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
  await waitFor("runner_pods_absent_in_restore_fence", () => liveRunnerPods().length === 0);
  await waitFor("restore_fence_deployments_exact", deploymentsMatchGeneratedDesiredState);
  await waitFor("runner_admission_observed_in_restore_fence", liveAdmissionIsObserved);
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
  if (namespace && liveState().activationPhase !== "legacy") {
    if (recoveryLockExists()) throw new Error("bootstrap_lock_appeared_before_parent_quiesce");
    const fenceFailures = await runBestEffortFenceSteps([
      {
        name: "runner-namespace",
        action: async () => applyNamedResources([["Namespace", RUNNER_NAMESPACE, ""]])
      },
      {
        name: "runner-role-inert",
        action: async () => neutralizeRunnerAuthority()
      },
      {
        name: "deployment:bot-orchestrator",
        action: async () => applyResource(recoveryDeployment("bot-orchestrator", 0, "active"))
      }
    ]);
    const deletionFailures = await deleteAllRunnerPodsByExactUid();
    if (fenceFailures.length > 0 || deletionFailures.length > 0) {
      throw new Error("bootstrap_fence_mutations_failed");
    }
    await waitFor("parent_quiesced_before_bootstrap_authority", liveParentIsQuiesced);
    await waitFor("runner_pods_absent_before_bootstrap_authority", () => liveRunnerPods().length === 0);
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
  await waitFor("runner_pods_absent", () => liveRunnerPods().length === 0);
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
  await waitFor("runner_pods_absent_before_runner_authority", () => liveRunnerPods().length === 0);
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
  await waitFor("runner_pods_absent_after_runner_authority", () => liveRunnerPods().length === 0);
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
        admissionDenialProbe()
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
    await waitFor("runner_pods_absent_before_reactivation", () => liveRunnerPods().length === 0);
    await waitFor("runner_admission_observed_before_reactivation", liveAdmissionIsObserved);
    if (!exactRunnerAuthority(false)) throw new Error("restore_fence_runner_authority_not_inert");
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
    await waitFor("runner_pods_absent_before_activation", () => liveRunnerPods().length === 0);
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
  await ensureFoundationalNamespaceForLease();
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
