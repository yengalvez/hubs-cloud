const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

const {
  ADMISSION_POLICY_NAME,
  RECOVERY_LOCK_NAME,
  RUNNER_NAMESPACE,
  admissionPolicyIsObserved,
  applyResourcesSequentially,
  createStableWindowPredicate,
  decideApplyMode,
  exactAdmissionBinding,
  exactDeploymentDesiredState,
  exactFoundationalNamespace,
  exactRecoveryOperationLock,
  parentIsQuiesced,
  parentFencePolicyProtectsLiveOrTarget,
  policySpecForFenceAwareDeployment,
  podIsRecoveryConsumer,
  recoveryConsumersAreQuiesced,
  retryBestEffortFenceAttempt,
  runBestEffortFenceSteps
} = require("./runner-activation");
const {
  PodWatchEvidence,
  ReplicaSetWatchEvidence,
  forbiddenPod,
  podWatchRawPath,
  replicaSetWatchRawPath
} = require("./pod-quiescence-watch");

test("legacy default-ServiceAccount parent Pods remain physically visible until their Deployment UID chain is absent", () => {
  const deployment = {
    metadata: { name: "bot-orchestrator", namespace: "hcce", uid: "deployment-uid" },
    spec: { replicas: 0 },
    status: { replicas: 0, readyReplicas: 0 }
  };
  const replicaSets = {
    kind: "ReplicaSetList",
    metadata: { resourceVersion: "20" },
    items: [{
      metadata: {
        name: "bot-orchestrator-old",
        uid: "replicaset-uid",
        ownerReferences: [{
          apiVersion: "apps/v1",
          kind: "Deployment",
          name: "bot-orchestrator",
          uid: "deployment-uid",
          controller: true
        }]
      }
    }]
  };
  const legacyPod = {
    metadata: {
      name: "bot-orchestrator-old-abc",
      uid: "legacy-pod-uid",
      labels: { app: "bot-orchestrator" },
      ownerReferences: [{ kind: "ReplicaSet", uid: "replicaset-uid", controller: true }]
    },
    spec: {
      serviceAccountName: "default",
      automountServiceAccountToken: false,
      containers: [{ name: "bot-orchestrator" }]
    }
  };
  const pods = {
    kind: "PodList",
    metadata: { resourceVersion: "30" },
    items: [legacyPod]
  };
  assert.equal(parentIsQuiesced(deployment, pods, replicaSets, "deployment-uid"), false);
  assert.equal(forbiddenPod("hcce", "hcce", legacyPod), true);
  const terminating = structuredClone(pods);
  terminating.items[0].metadata.deletionTimestamp = "2026-07-19T12:00:00Z";
  assert.equal(parentIsQuiesced(deployment, terminating, replicaSets, "deployment-uid"), false);
  assert.equal(parentIsQuiesced(
    deployment,
    { ...pods, items: [] },
    replicaSets,
    "deployment-uid"
  ), true);
});
const { operationalDriftErrors } = require("./live-runner-control-plane");
const {
  guardPodDocumentForIdentity,
  runnerPodName
} = require("../services/bot-orchestrator/kubernetes-runner-manager");

const epoch = "44444444-4444-4444-8444-444444444444";
const sourceEpoch = "33333333-3333-4333-8333-333333333333";

function recoveryLock(state = "restore-complete-awaiting-reactivation") {
  return {
    apiVersion: "v1",
    kind: "ConfigMap",
    metadata: {
      name: RECOVERY_LOCK_NAME,
      namespace: "hcce",
      uid: "lock-uid",
      resourceVersion: "17",
      labels: { "yenhubs.org/recovery-owner": "checkpoint-restore" },
      annotations: {
        "yenhubs.org/operation-id": "a".repeat(32),
        "yenhubs.org/recovery-token": "b".repeat(32),
        "yenhubs.org/namespace-uid": "namespace-uid",
        "yenhubs.org/pvc-uid": "pvc-uid",
        "yenhubs.org/checkpoint-stamp": "20260718-081500",
        "yenhubs.org/dump-sha256": "c".repeat(64),
        "yenhubs.org/storage-sha256": "d".repeat(64),
        "yenhubs.org/pre-fence-epoch": sourceEpoch,
        "yenhubs.org/restore-fence-epoch": epoch,
        "yenhubs.org/deployment-inventory-sha256": "e".repeat(64),
        "yenhubs.org/runner-cutover-evidence-sha256": "f".repeat(64),
        "yenhubs.org/runner-runtime-generation": "durable-v2",
        "yenhubs.org/recovery-state": state
      }
    },
    immutable: true
  };
}

function exactBinding() {
  return {
    apiVersion: "admissionregistration.k8s.io/v1",
    kind: "ValidatingAdmissionPolicyBinding",
    metadata: { name: ADMISSION_POLICY_NAME },
    spec: {
      policyName: ADMISSION_POLICY_NAME,
      validationActions: ["Deny"],
      matchResources: {
        matchPolicy: "Equivalent",
        namespaceSelector: {
          matchLabels: { "kubernetes.io/metadata.name": RUNNER_NAMESPACE }
        },
        objectSelector: {}
      }
    }
  };
}

test("accepts observed VAP typeChecking={} and rejects warnings or stale generations", () => {
  const policy = {
    metadata: { generation: 7 },
    status: { observedGeneration: 7, typeChecking: {} }
  };
  assert.equal(admissionPolicyIsObserved(policy), true);
  assert.equal(admissionPolicyIsObserved({
    ...policy,
    status: {
      ...policy.status,
      typeChecking: { expressionWarnings: [{ fieldRef: "spec.validations[0]" }] }
    }
  }), false);
  assert.equal(admissionPolicyIsObserved({
    ...policy,
    status: { ...policy.status, observedGeneration: 6 }
  }), false);
});

test("requires the exact admission binding matchResources with no selector bypass", () => {
  const binding = exactBinding();
  assert.equal(exactAdmissionBinding(binding), true);
  const bypass = structuredClone(binding);
  bypass.spec.matchResources.namespaceSelector.matchExpressions = [{
    key: "never",
    operator: "Exists"
  }];
  assert.equal(exactAdmissionBinding(bypass), false);
  const excluded = structuredClone(binding);
  excluded.spec.matchResources.excludeResourceRules = [];
  assert.equal(exactAdmissionBinding(excluded), false);
});

function fenceAwareDeployment(parentImage, runnerImage) {
  return {
    apiVersion: "apps/v1",
    kind: "Deployment",
    metadata: {
      name: "bot-orchestrator",
      namespace: "hcce",
      annotations: { "yenhubs.org/runner-fence-protocol": "intent-fence-v1" }
    },
    spec: {
      template: {
        metadata: { annotations: { "yenhubs.org/runner-fence-protocol": "intent-fence-v1" } },
        spec: {
          containers: [{
            name: "bot-orchestrator",
            image: parentImage,
            env: [{ name: "BOT_RUNNER_IMAGE", value: runnerImage }]
          }]
        }
      }
    }
  };
}

function parentFencePolicy(parentImage, runnerImage) {
  return {
    spec: {
      failurePolicy: "Fail",
      validations: [{
        expression: `object.spec.template.spec.containers[0].image == '${parentImage}' && ` +
          `object.spec.template.spec.containers[0].env.exists(e, e.value == '${runnerImage}')`
      }]
    }
  };
}

test("durable parent policy accepts current or staged target images across update and rollback", () => {
  const oldDeployment = fenceAwareDeployment(
    "ghcr.io/example/parent@sha256:" + "a".repeat(64),
    "ghcr.io/example/runner@sha256:" + "b".repeat(64)
  );
  const newDeployment = fenceAwareDeployment(
    "ghcr.io/example/parent@sha256:" + "c".repeat(64),
    "ghcr.io/example/runner@sha256:" + "d".repeat(64)
  );
  const targetPolicy = parentFencePolicy(
    newDeployment.spec.template.spec.containers[0].image,
    newDeployment.spec.template.spec.containers[0].env[0].value
  );
  const oldPolicy = {
    spec: policySpecForFenceAwareDeployment(targetPolicy, newDeployment, oldDeployment)
  };
  assert.equal(parentFencePolicyProtectsLiveOrTarget({
    livePolicy: oldPolicy,
    targetPolicy,
    liveDeployment: oldDeployment,
    targetDeployment: newDeployment
  }), true, "live old policy permits staging a new target");
  assert.equal(parentFencePolicyProtectsLiveOrTarget({
    livePolicy: targetPolicy,
    targetPolicy,
    liveDeployment: oldDeployment,
    targetDeployment: newDeployment
  }), true, "a staged target policy survives retry before Deployment update");

  const rollbackPolicy = parentFencePolicy(
    oldDeployment.spec.template.spec.containers[0].image,
    oldDeployment.spec.template.spec.containers[0].env[0].value
  );
  assert.equal(parentFencePolicyProtectsLiveOrTarget({
    livePolicy: targetPolicy,
    targetPolicy: rollbackPolicy,
    liveDeployment: newDeployment,
    targetDeployment: oldDeployment
  }), true, "a fence-aware new release can roll back to a fence-aware old release");
  const oldShape = structuredClone(oldDeployment);
  delete oldShape.metadata.annotations["yenhubs.org/runner-fence-protocol"];
  assert.equal(parentFencePolicyProtectsLiveOrTarget({
    livePolicy: oldPolicy,
    targetPolicy,
    liveDeployment: oldShape,
    targetDeployment: newDeployment
  }), false, "pre-fence Deployment shape is never accepted as current protection");
});

test("recovery lock binds state, epoch, checkpoint inventory, journal evidence, generation, and storage identities", () => {
  const lock = recoveryLock();
  const options = { namespaceUid: "namespace-uid", pvcUid: "pvc-uid" };
  assert.equal(exactRecoveryOperationLock(
    lock,
    "hcce",
    epoch,
    "restore-complete-awaiting-reactivation",
    options
  ), true);

  for (const mutate of [
    value => { value.metadata.annotations["yenhubs.org/recovery-state"] = "restore-fence-prepared"; },
    value => { value.metadata.annotations["yenhubs.org/restore-fence-epoch"] = sourceEpoch; },
    value => { value.metadata.annotations["yenhubs.org/runner-cutover-evidence-sha256"] = "invalid"; },
    value => { value.metadata.annotations["yenhubs.org/runner-runtime-generation"] = "legacy-absent"; },
    value => { value.metadata.annotations["yenhubs.org/extra"] = "bypass"; },
    value => { value.metadata.labels["yenhubs.org/recovery-owner"] = "other"; },
    value => { value.immutable = false; },
    value => { value.metadata.deletionTimestamp = "2026-07-18T08:00:00Z"; },
    value => { value.metadata.deletionGracePeriodSeconds = 30; },
    value => { value.metadata.generateName = "recovery-lock-"; },
    value => { value.metadata.ownerReferences = []; },
    value => { value.metadata.finalizers = []; },
    value => { value.metadata.clusterName = "unexpected"; }
  ]) {
    const changed = structuredClone(lock);
    mutate(changed);
    assert.equal(exactRecoveryOperationLock(
      changed,
      "hcce",
      epoch,
      "restore-complete-awaiting-reactivation",
      options
    ), false);
  }
  assert.equal(exactRecoveryOperationLock(
    lock,
    "hcce",
    epoch,
    "restore-complete-awaiting-reactivation",
    { namespaceUid: "replaced", pvcUid: "pvc-uid" }
  ), false);
  assert.equal(exactRecoveryOperationLock(
    lock,
    "hcce",
    epoch,
    "restore-complete-awaiting-reactivation",
    { ...options, lockUid: "lock-uid", lockResourceVersion: "17" }
  ), true);
  const replacement = structuredClone(lock);
  replacement.metadata.uid = "replacement-lock-uid";
  assert.equal(exactRecoveryOperationLock(
    replacement,
    "hcce",
    epoch,
    "restore-complete-awaiting-reactivation",
    { ...options, lockUid: "lock-uid", lockResourceVersion: "17" }
  ), false, "a delete/recreate ABA lock cannot inherit the pinned transition authority");
  const mutatedAndRestored = structuredClone(lock);
  mutatedAndRestored.metadata.resourceVersion = "18";
  assert.equal(exactRecoveryOperationLock(
    mutatedAndRestored,
    "hcce",
    epoch,
    "restore-complete-awaiting-reactivation",
    { ...options, lockUid: "lock-uid", lockResourceVersion: "17" }
  ), false, "metadata mutation and restoration cannot cross the pinned RV boundary");

  const legacy = recoveryLock();
  legacy.metadata.annotations["yenhubs.org/pre-fence-epoch"] = "legacy-absent";
  legacy.metadata.annotations["yenhubs.org/runner-runtime-generation"] = "legacy-absent";
  assert.equal(exactRecoveryOperationLock(
    legacy,
    "hcce",
    epoch,
    "restore-complete-awaiting-reactivation",
    options
  ), false, "a legacy checkpoint cannot cross into the durable restore protocol");
});

test("recovery quiescence rejects old-ReplicaSet, standalone, and bot-SA consumer Pods", () => {
  const deployments = {
    kind: "DeploymentList",
    items: [
      ...["reticulum", "pgbouncer", "pgbouncer-t", "bot-orchestrator", "coturn"].map(name => ({
        metadata: { name, uid: `new-${name}-uid` },
        spec: { replicas: 0 },
        status: { replicas: 0, readyReplicas: 0 }
      })),
      { metadata: { name: "pgsql" }, spec: { replicas: 1 }, status: { replicas: 1 } }
    ]
  };
  const emptyPods = { kind: "PodList", items: [] };
  const oldReplicaSets = {
    kind: "ReplicaSetList",
    items: [{
      metadata: {
        name: "opaque-old-rs",
        uid: "old-rs-uid",
        ownerReferences: [{ kind: "Deployment", name: "reticulum", uid: "old-deployment-uid" }]
      },
      spec: { replicas: 0 },
      status: { replicas: 0, readyReplicas: 0, availableReplicas: 0 }
    }]
  };
  assert.equal(recoveryConsumersAreQuiesced(deployments, emptyPods, oldReplicaSets), true);
  const liveOldReplicaSet = structuredClone(oldReplicaSets);
  liveOldReplicaSet.items[0].spec.replicas = 1;
  assert.equal(recoveryConsumersAreQuiesced(deployments, emptyPods, liveOldReplicaSet), false);

  const orphanFromOldUid = {
    metadata: {
      name: "opaque-pod",
      ownerReferences: [{ kind: "ReplicaSet", name: "opaque-old-rs", uid: "old-rs-uid" }]
    },
    spec: { containers: [{ name: "application" }] }
  };
  assert.equal(podIsRecoveryConsumer(orphanFromOldUid, oldReplicaSets.items), true);
  assert.equal(recoveryConsumersAreQuiesced(
    deployments,
    { kind: "PodList", items: [orphanFromOldUid] },
    oldReplicaSets
  ), false);
  for (const pod of [
    {
      metadata: { name: "standalone", labels: { app: "coturn" } },
      spec: { containers: [{ name: "diagnostic" }] }
    },
    {
      metadata: { name: "opaque-standalone" },
      spec: { serviceAccountName: "bot-orchestrator", containers: [{ name: "application" }] }
    }
  ]) {
    assert.equal(recoveryConsumersAreQuiesced(
      deployments,
      { kind: "PodList", items: [pod] },
      oldReplicaSets
    ), false);
  }
  assert.equal(recoveryConsumersAreQuiesced(
    deployments,
    { kind: "PodList", items: [] },
    { kind: "ReplicaSetList" }
  ), false, "malformed inventories fail closed");
});

test("state machine blocks every recovery-lock bypass and permits only exact transitions", () => {
  const normalBase = {
    targetRecovery: "active",
    liveRecovery: "active",
    lockState: null
  };
  assert.equal(decideApplyMode({
    ...normalBase,
    targetActivation: "bootstrap",
    liveActivation: "legacy"
  }), "bootstrap");
  assert.equal(decideApplyMode({
    ...normalBase,
    targetActivation: "admission",
    liveActivation: "bootstrap"
  }), "admission");
  assert.equal(decideApplyMode({
    ...normalBase,
    targetActivation: "active",
    liveActivation: "admission"
  }), "active");
  assert.equal(decideApplyMode({
    ...normalBase,
    targetActivation: "active",
    liveActivation: "active"
  }), "active-reapply");
  assert.equal(decideApplyMode({
    targetActivation: "active",
    targetRecovery: "restore-fence",
    liveActivation: "active",
    liveRecovery: "active",
    lockState: "restore-fence-prepared"
  }), "restore-fence");
  assert.equal(decideApplyMode({
    targetActivation: "active",
    targetRecovery: "active",
    liveActivation: "active",
    liveRecovery: "restore-fence",
    lockState: "restore-complete-awaiting-reactivation"
  }), "recovery-reactivation");
  assert.equal(decideApplyMode({
    targetActivation: "active",
    targetRecovery: "active",
    liveActivation: "active",
    liveRecovery: "active",
    lockState: "restore-complete-awaiting-reactivation"
  }), "recovery-reactivation", "completed partial apply must be safely reentrant");

  for (const targetActivation of ["bootstrap", "admission"]) {
    assert.throws(() => decideApplyMode({
      targetActivation,
      targetRecovery: "active",
      liveActivation: "bootstrap",
      liveRecovery: "active",
      lockState: "restore-fence-prepared"
    }), /recovery_lock_blocks/);
  }
  assert.throws(() => decideApplyMode({
    targetActivation: "active",
    targetRecovery: "active",
    liveActivation: "admission",
    liveRecovery: "active",
    lockState: "restore-fence-prepared"
  }), /recovery_lock_blocks/);
  assert.throws(() => decideApplyMode({
    targetActivation: "active",
    targetRecovery: "restore-fence",
    liveActivation: "active",
    liveRecovery: "active",
    lockState: null
  }), /prepared_recovery_lock/);
});

test("stable window resets with injected time and never treats a gap as continuous", () => {
  let present = false;
  let now = 0;
  const stable = createStableWindowPredicate(() => !present, { now: () => now, windowMs: 61_000 });
  assert.equal(stable(), false);
  now = 60_999;
  assert.equal(stable(), false);
  present = true;
  assert.equal(stable(), false);
  present = false;
  now = 61_000;
  assert.equal(stable(), false);
  now = 122_000;
  assert.equal(stable(), true);
});

test("event-backed pod evidence catches transient Pods and fails closed on resourceVersion 410", () => {
  const rawPath = podWatchRawPath("hcce", "123");
  assert.match(rawPath, /watch=true/);
  assert.match(rawPath, /sendInitialEvents=true/);
  assert.match(rawPath, /resourceVersionMatch=NotOlderThan/);
  assert.match(rawPath, /resourceVersion=123/);
  const replicaSetPath = replicaSetWatchRawPath("hcce", "124");
  assert.match(replicaSetPath, /replicasets\?/);
  assert.match(replicaSetPath, /sendInitialEvents=true/);
  assert.match(replicaSetPath, /resourceVersion=124/);
  const parentWatch = new PodWatchEvidence("hcce", "hcce", "100");
  parentWatch.ingest({
    type: "ADDED",
    object: {
      metadata: { name: "transient", resourceVersion: "101", labels: {} },
      spec: { serviceAccountName: "bot-orchestrator" }
    }
  });
  parentWatch.ingest({
    type: "DELETED",
    object: {
      metadata: { name: "transient", resourceVersion: "102", labels: {} },
      spec: { serviceAccountName: "bot-orchestrator" }
    }
  });
  assert.equal(parentWatch.violation, true);
  parentWatch.ingest({
    type: "BOOKMARK",
    object: {
      metadata: {
        resourceVersion: "opaque-rv-a",
        annotations: { "k8s.io/initial-events-end": "true" }
      }
    }
  });
  assert.equal(parentWatch.coversInitialEvents(), true);

  const legacyWatch = new PodWatchEvidence("hcce", "hcce", "102");
  legacyWatch.ingest({
    type: "ADDED",
    object: {
      metadata: {
        name: "legacy-transient",
        resourceVersion: "103",
        labels: { component: "bot-runner" }
      },
      spec: { serviceAccountName: "default" }
    }
  });
  legacyWatch.ingest({
    type: "DELETED",
    object: {
      metadata: {
        name: "legacy-transient",
        resourceVersion: "104",
        labels: { component: "bot-runner" }
      },
      spec: { serviceAccountName: "default" }
    }
  });
  assert.equal(legacyWatch.violation, true);
  legacyWatch.ingest({
    type: "BOOKMARK",
    object: { metadata: { resourceVersion: "opaque-rv-b", annotations: {} } }
  });
  assert.equal(legacyWatch.coversInitialEvents(), false, "ordinary bookmarks are not an initial-list boundary");

  const runnerWatch = new PodWatchEvidence(RUNNER_NAMESPACE, "hcce", "200");
  runnerWatch.ingest({ type: "ERROR", object: { code: 410 } });
  assert.equal(runnerWatch.error, "watch_resource_version_expired");

  const fenceIdentity = {
    roomKey: "abcabcabcabcabcabcab",
    processGeneration: "11111111-1111-4111-8111-111111111111"
  };
  fenceIdentity.name = runnerPodName(
    fenceIdentity.roomKey,
    fenceIdentity.processGeneration
  );
  const fence = guardPodDocumentForIdentity(fenceIdentity, "fence", RUNNER_NAMESPACE);
  fence.metadata.uid = "fence-uid";
  fence.metadata.resourceVersion = "201";
  fence.status = { phase: "Pending" };
  const fenceAdded = new PodWatchEvidence(RUNNER_NAMESPACE, "hcce", "200");
  fenceAdded.ingest({ type: "ADDED", object: structuredClone(fence) });
  fenceAdded.ingest({ type: "MODIFIED", object: structuredClone(fence) });
  assert.equal(fenceAdded.violation, false, "an exact permanent fence is safe evidence");
  fenceAdded.ingest({
    type: "BOOKMARK",
    object: {
      metadata: {
        resourceVersion: "202",
        annotations: { "k8s.io/initial-events-end": "true" }
      }
    }
  });
  fenceAdded.ingest({ type: "DELETED", object: structuredClone(fence) });
  assert.equal(fenceAdded.violation, true, "deleting a permanent fence breaks causal absence");

  const terminatingFence = structuredClone(fence);
  terminatingFence.metadata.deletionTimestamp = "2026-07-19T12:00:00Z";
  terminatingFence.metadata.resourceVersion = "203";
  const fenceTerminating = new PodWatchEvidence(RUNNER_NAMESPACE, "hcce", "202");
  fenceTerminating.ingest({ type: "MODIFIED", object: terminatingFence });
  assert.equal(fenceTerminating.violation, true, "a terminating fence is never safe evidence");

  const recoveryWatch = new PodWatchEvidence("hcce", "hcce", "300", {
    includeRecoveryConsumers: true
  });
  recoveryWatch.ingest({
    type: "ADDED",
    object: {
      metadata: {
        name: "standalone-consumer",
        resourceVersion: "301",
        labels: { app: "reticulum" }
      },
      spec: { serviceAccountName: "default", containers: [{ name: "application" }] }
    }
  });
  assert.equal(recoveryWatch.violation, true);
  const oldReplicaSetWatch = new PodWatchEvidence("hcce", "hcce", "302", {
    includeRecoveryConsumers: true,
    recoveryConsumerReplicaSets: [{
      metadata: { name: "opaque-old-rs", uid: "old-rs-uid" },
      spec: { replicas: 0 },
      status: { replicas: 0 }
    }]
  });
  oldReplicaSetWatch.ingest({
    type: "ADDED",
    object: {
      metadata: {
        name: "opaque-pod",
        resourceVersion: "303",
        ownerReferences: [{ kind: "ReplicaSet", name: "opaque-old-rs", uid: "old-rs-uid" }]
      },
      spec: { serviceAccountName: "default", containers: [{ name: "application" }] }
    }
  });
  assert.equal(oldReplicaSetWatch.violation, true, "opaque old-RS Pods cannot cross the stable watch");
  const sharedReplicaSets = [];
  const replicaSetWatch = new ReplicaSetWatchEvidence("400", sharedReplicaSets);
  replicaSetWatch.ingest({
    type: "ADDED",
    object: {
      metadata: {
        name: "opaque-dynamic-rs",
        uid: "dynamic-rs-uid",
        resourceVersion: "401",
        ownerReferences: [{ kind: "Deployment", name: "reticulum", uid: "old-deployment" }]
      },
      spec: { replicas: 0 },
      status: { replicas: 0 }
    }
  });
  assert.equal(replicaSetWatch.violation, false, "initial stopped RS inventory is safe");
  assert.equal(sharedReplicaSets.length, 1);
  const bridgedPodWatch = new PodWatchEvidence("hcce", "hcce", "401", {
    includeRecoveryConsumers: true,
    recoveryConsumerReplicaSets: sharedReplicaSets
  });
  bridgedPodWatch.ingest({
    type: "ADDED",
    object: {
      metadata: {
        name: "opaque-dynamic-pod",
        resourceVersion: "402",
        ownerReferences: [{ kind: "ReplicaSet", name: "opaque-dynamic-rs", uid: "dynamic-rs-uid" }]
      },
      spec: { serviceAccountName: "default", containers: [{ name: "application" }] }
    }
  });
  assert.equal(bridgedPodWatch.violation, true, "RS watch identities bridge into the Pod watch");
  replicaSetWatch.ingest({
    type: "BOOKMARK",
    object: {
      metadata: {
        resourceVersion: "403",
        annotations: { "k8s.io/initial-events-end": "true" }
      }
    }
  });
  replicaSetWatch.ingest({
    type: "MODIFIED",
    object: {
      metadata: {
        name: "opaque-dynamic-rs",
        uid: "dynamic-rs-uid",
        resourceVersion: "404"
      },
      spec: { replicas: 0 },
      status: { replicas: 0 }
    }
  });
  assert.equal(replicaSetWatch.violation, true, "post-boundary RS churn resets the stable window");
  const runnerOnlyWatch = new PodWatchEvidence("hcce", "hcce", "300");
  runnerOnlyWatch.ingest({
    type: "ADDED",
    object: {
      metadata: {
        name: "standalone-consumer",
        resourceVersion: "301",
        labels: { app: "reticulum" }
      },
      spec: { serviceAccountName: "default", containers: [{ name: "application" }] }
    }
  });
  assert.equal(runnerOnlyWatch.violation, false, "normal activation watches only runner authority");
});

test("reentry requires the server-normalized Deployment spec, image, and recovery epoch", () => {
  const expected = {
    apiVersion: "apps/v1",
    kind: "Deployment",
    metadata: {
      name: "bot-orchestrator",
      namespace: "hcce",
      labels: { app: "bot-orchestrator" },
      annotations: {
        "yenhubs.org/bot-runner-recovery-epoch": epoch,
        "yenhubs.org/bot-runner-recovery-phase": "active"
      }
    },
    spec: {
      replicas: 1,
      selector: { matchLabels: { app: "bot-orchestrator" } },
      template: {
        metadata: {
          labels: { app: "bot-orchestrator" },
          annotations: { "yenhubs.org/bot-runner-recovery-epoch": epoch }
        },
        spec: {
          containers: [{ name: "bot-orchestrator", image: "ghcr.io/example/bot@sha256:abc" }]
        }
      }
    }
  };
  assert.equal(exactDeploymentDesiredState(structuredClone(expected), expected), true);
  for (const mutate of [
    value => { value.spec.template.spec.containers[0].image = "ghcr.io/example/bot@sha256:def"; },
    value => { value.spec.template.metadata.annotations["yenhubs.org/bot-runner-recovery-epoch"] = sourceEpoch; },
    value => { value.metadata.annotations["yenhubs.org/bot-runner-recovery-epoch"] = sourceEpoch; },
    value => { value.spec.template.spec.hostNetwork = true; }
  ]) {
    const drifted = structuredClone(expected);
    mutate(drifted);
    assert.equal(exactDeploymentDesiredState(drifted, expected), false);
  }
});

test("clean-install Lease bootstrap accepts only the exact active parent Namespace", () => {
  const expected = {
    apiVersion: "v1",
    kind: "Namespace",
    metadata: { name: "hcce", labels: { environment: "production" } }
  };
  const live = {
    ...structuredClone(expected),
    metadata: {
      ...structuredClone(expected.metadata),
      uid: "namespace-uid",
      resourceVersion: "1",
      labels: {
        ...expected.metadata.labels,
        "kubernetes.io/metadata.name": "hcce"
      }
    },
    spec: { finalizers: ["kubernetes"] },
    status: { phase: "Active" }
  };
  assert.equal(exactFoundationalNamespace(live, expected), true);
  const terminating = structuredClone(live);
  terminating.metadata.deletionTimestamp = "2026-07-18T08:00:00.000Z";
  assert.equal(exactFoundationalNamespace(terminating, expected), false);
  const drifted = structuredClone(live);
  drifted.metadata.labels.environment = "test";
  assert.equal(exactFoundationalNamespace(drifted, expected), false);
  const generated = structuredClone(live);
  generated.metadata.generateName = "hcce-";
  assert.equal(exactFoundationalNamespace(generated, expected), false);
  const finalized = structuredClone(live);
  finalized.spec.finalizers.push("evil.example/finalizer");
  assert.equal(exactFoundationalNamespace(finalized, expected), false);
});

test("fail-closed fencing attempts every mutation after an injected first failure", async () => {
  const attempted = [];
  const failures = await runBestEffortFenceSteps([
    {
      name: "runner-role-inert",
      action: async () => {
        attempted.push("runner-role-inert");
        throw new Error("injected");
      }
    },
    ...["reticulum", "pgbouncer", "pgbouncer-t", "bot-orchestrator", "coturn"].map(name => ({
      name,
      action: async () => { attempted.push(name); }
    }))
  ]);
  assert.deepEqual(attempted, [
    "runner-role-inert",
    "reticulum",
    "pgbouncer",
    "pgbouncer-t",
    "bot-orchestrator",
    "coturn"
  ]);
  assert.deepEqual(failures, ["runner-role-inert"]);
});

test("fail-closed fencing retries the complete idempotent fence after a transient failure", async () => {
  const attempts = [];
  const retries = [];
  const failures = await retryBestEffortFenceAttempt(
    async attempt => {
      attempts.push(attempt);
      return attempt === 1 ? ["runner-role-inert"] : [];
    },
    {
      maxAttempts: 3,
      beforeRetry: async attempt => { retries.push(attempt); }
    }
  );
  assert.deepEqual(attempts, [1, 2]);
  assert.deepEqual(retries, [1]);
  assert.deepEqual(failures, []);
});

test("manifest apply is resource-by-resource and stops after one bounded failing API operation", () => {
  const attempted = [];
  assert.throws(() => applyResourcesSequentially(
    [{ id: 1 }, { id: 2 }, { id: 3 }],
    resource => {
      attempted.push(resource.id);
      if (resource.id === 2) throw new Error("injected");
    }
  ), /injected/);
  assert.deepEqual(attempted, [1, 2]);

  const source = fs.readFileSync(path.resolve(__dirname, "index.js"), "utf8");
  assert.match(source, /--request-timeout=30s/);
  assert.match(source, /applyResourcesSequentially\(plan\.resources, applyResource\)/);
});

test("standard and emergency apply entrypoints verify the complete manifest first", () => {
  const packageJson = require("../package.json");
  assert.match(packageJson.scripts.apply, /^node generate_script\/verify-generated-manifest\.js &&/);
  assert.match(
    packageJson.scripts["refence-runner-control-plane"],
    /^node generate_script\/verify-generated-manifest\.js &&/
  );
});

test("normal active and recovery active failures share the same emergency refence path", () => {
  const source = fs.readFileSync(path.resolve(__dirname, "index.js"), "utf8");
  const wrapper = source.slice(
    source.indexOf("async function applyActive(mode)"),
    source.indexOf("async function applyUnderOperationLease")
  );
  assert.match(wrapper, /if \(failClosedRefenceRequired\)/);
  assert.match(wrapper, /emergencyRefenceAfterFailedActivation\(\)/);
  assert.doesNotMatch(wrapper, /mode === "recovery-reactivation" && failClosedRefenceRequired/);
  const mainWrapper = source.slice(source.indexOf("async function main()"));
  assert.match(mainWrapper, /if \(failClosedRefenceRequired\)/);
  assert.doesNotMatch(mainWrapper, /heartbeatLost && failClosedRefenceRequired/);
});

test("invalid recovery locks refence before rejection and the first exact UID/RV snapshot stays pinned", () => {
  const source = fs.readFileSync(path.resolve(__dirname, "index.js"), "utf8");
  const dispatch = source.slice(
    source.indexOf("async function applyUnderOperationLease"),
    source.indexOf("async function main()")
  );
  const invalidIndex = dispatch.indexOf('if (lockState === "invalid")');
  const decideIndex = dispatch.indexOf("const mode = decideApplyMode");
  assert.ok(invalidIndex >= 0 && invalidIndex < decideIndex);
  const invalidBranch = dispatch.slice(invalidIndex, decideIndex);
  assert.match(invalidBranch, /failClosedRefenceRequired = true/);
  assert.match(invalidBranch, /refenceRecoveryConsumers\(\{ label: "invalid_recovery_lock" \}\)/);
  assert.match(invalidBranch, /invalid_recovery_lock_refenced_manual_repair_required/);
  const pinIndex = dispatch.indexOf("pinRecoveryLockSnapshot(lockSnapshot)");
  assert.ok(pinIndex >= 0 && pinIndex < decideIndex, "the classified snapshot is pinned before deciding");

  const exactLock = source.slice(
    source.indexOf("function exactLiveRecoveryLock"),
    source.indexOf("function recoveryLockExists")
  );
  assert.match(exactLock, /lockUid: recoveryLockIdentityGuard\.uid/);
  assert.match(exactLock, /lockResourceVersion: recoveryLockIdentityGuard\.resourceVersion/);
  assert.match(exactLock, /recovery_lock_replaced_or_mutated_after_snapshot/);
});

test("restore reentry cannot accept additive RBAC or a missing admission denial", () => {
  const source = fs.readFileSync(path.resolve(__dirname, "index.js"), "utf8");
  const reentry = source.slice(
    source.indexOf("if (live.recoveryPhase === \"active\")"),
    source.indexOf("await waitFor(\"recovery_consumers_quiesced_before_reactivation\"")
  );
  assert.match(reentry, /exactRunnerAuthority\(true\)/);
  assert.match(reentry, /admissionDenialProbe\(\)/);
});

test("active reapply with control-plane drift refences and requires the staged bootstrap path", () => {
  const source = fs.readFileSync(path.resolve(__dirname, "index.js"), "utf8");
  const branch = source.slice(
    source.indexOf("} else if (mode === \"active-reapply\")"),
    source.indexOf("} else if (live.activationPhase === \"admission\"")
  );
  assert.match(branch, /failClosedRefenceRequired = true/);
  assert.match(branch, /liveRunnerControlPlaneIsExact/);
  assert.match(branch, /exactRunnerAuthority\(true\)/);
  assert.match(branch, /refenceActiveReapplyForStaging\(\)/);
  assert.match(
    branch,
    /active_reapply_control_plane_drift_refenced_generate_and_apply_bootstrap_then_admission_then_active_do_not_retry_active/
  );
  assert.doesNotMatch(branch, /waitFor\("runner_control_plane_exact_before_active_reapply"/);
  const wrapper = source.slice(
    source.indexOf("async function applyActive(mode)"),
    source.indexOf("async function applyUnderOperationLease")
  );
  assert.match(wrapper, /emergencyRefenceAfterFailedActivation\(\)/);

  assert.equal(decideApplyMode({
    targetActivation: "bootstrap",
    targetRecovery: "active",
    liveActivation: "active",
    liveRecovery: "active",
    lockState: null
  }), "bootstrap");
  assert.equal(decideApplyMode({
    targetActivation: "admission",
    targetRecovery: "active",
    liveActivation: "bootstrap",
    liveRecovery: "active",
    lockState: null
  }), "admission");
  assert.equal(decideApplyMode({
    targetActivation: "active",
    targetRecovery: "active",
    liveActivation: "admission",
    liveRecovery: "active",
    lockState: null
  }), "active");
});

test("live control-plane exactness rejects terminating, owner-bound, finalized, and immutable Secrets", () => {
  const expected = {
    apiVersion: "v1",
    kind: "Secret",
    metadata: { name: "bot-images-pull", namespace: RUNNER_NAMESPACE },
    type: "kubernetes.io/dockerconfigjson",
    data: { ".dockerconfigjson": "redacted-fixture" }
  };
  const live = {
    ...structuredClone(expected),
    metadata: {
      ...expected.metadata,
      uid: "secret-uid",
      resourceVersion: "7",
      annotations: { "kubectl.kubernetes.io/last-applied-configuration": "ignored-server-bookkeeping" }
    }
  };
  assert.deepEqual(operationalDriftErrors(live, expected), []);
  for (const mutate of [
    value => { value.metadata.deletionTimestamp = "2026-07-18T08:00:00Z"; },
    value => { value.metadata.ownerReferences = [{ uid: "owner" }]; },
    value => { value.metadata.finalizers = ["hold.example"]; },
    value => { value.immutable = true; }
  ]) {
    const drifted = structuredClone(live);
    mutate(drifted);
    assert.notDeepEqual(operationalDriftErrors(drifted, expected), []);
  }
});

test("live Namespace exactness rejects finalizer, status, generateName, and metadata drift", () => {
  const expected = {
    apiVersion: "v1",
    kind: "Namespace",
    metadata: {
      name: RUNNER_NAMESPACE,
      labels: { "pod-security.kubernetes.io/enforce": "restricted" }
    }
  };
  const live = {
    ...structuredClone(expected),
    metadata: {
      ...structuredClone(expected.metadata),
      uid: "namespace-uid",
      resourceVersion: "9",
      creationTimestamp: "2026-07-18T08:00:00Z",
      labels: {
        ...expected.metadata.labels,
        "kubernetes.io/metadata.name": RUNNER_NAMESPACE
      },
      annotations: {
        "kubectl.kubernetes.io/last-applied-configuration": "ignored-server-bookkeeping"
      }
    },
    spec: { finalizers: ["kubernetes"] },
    status: { phase: "Active" }
  };
  assert.deepEqual(operationalDriftErrors(live, expected), []);
  for (const mutate of [
    value => { value.spec.finalizers.push("evil.example/finalizer"); },
    value => { value.status.phase = "Terminating"; },
    value => { value.metadata.generateName = "runner-"; },
    value => { value.metadata.clusterName = "unexpected"; },
    value => { value.metadata.ownerReferences = []; },
    value => { value.metadata.finalizers = []; }
  ]) {
    const drifted = structuredClone(live);
    mutate(drifted);
    assert.notDeepEqual(operationalDriftErrors(drifted, expected), []);
  }
});
