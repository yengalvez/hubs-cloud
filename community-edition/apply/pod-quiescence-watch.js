const RUNNER_NAMESPACE = "hcce-bot-runners";
const {
  exactGuardRecordFromPod
} = require("../services/bot-orchestrator/kubernetes-runner-manager");
const {
  podIsRecoveryConsumer,
  podIsBotOrchestratorParent,
  replicaSetIsRecoveryConsumer,
  replicaSetIsStopped
} = require("./runner-activation");

function forbiddenPod(
  namespace,
  parentNamespace,
  pod,
  {
    includeRecoveryConsumers = false,
    recoveryConsumerReplicaSets = [],
    eventType = null
  } = {}
) {
  if (!pod || typeof pod !== "object") return true;
  if (namespace === RUNNER_NAMESPACE) {
    try {
      exactGuardRecordFromPod(pod, "fence", RUNNER_NAMESPACE);
      return eventType === "DELETED";
    } catch (_error) {
      return true;
    }
  }
  return namespace === parentNamespace && (
    podIsBotOrchestratorParent(pod) ||
    pod?.metadata?.labels?.app === "bot-runner" ||
    pod?.metadata?.labels?.component === "bot-runner" ||
    pod?.metadata?.labels?.["yenhubs.org/managed-by"] === "bot-orchestrator" ||
    (includeRecoveryConsumers && podIsRecoveryConsumer(pod, recoveryConsumerReplicaSets))
  );
}

function podWatchRawPath(namespace, resourceVersion, timeoutSeconds = 170) {
  if (
    typeof namespace !== "string" || !namespace ||
    typeof resourceVersion !== "string" || !resourceVersion ||
    !Number.isInteger(timeoutSeconds) || timeoutSeconds < 1 || timeoutSeconds > 600
  ) {
    throw new Error("pod_watch_path_input_invalid");
  }
  return `/api/v1/namespaces/${encodeURIComponent(namespace)}/pods?` +
    "watch=true&sendInitialEvents=true&allowWatchBookmarks=true&resourceVersionMatch=NotOlderThan&" +
    `resourceVersion=${encodeURIComponent(resourceVersion)}` +
    `&timeoutSeconds=${timeoutSeconds}`;
}

function replicaSetWatchRawPath(namespace, resourceVersion, timeoutSeconds = 170) {
  if (
    typeof namespace !== "string" || !namespace ||
    typeof resourceVersion !== "string" || !resourceVersion ||
    !Number.isInteger(timeoutSeconds) || timeoutSeconds < 1 || timeoutSeconds > 600
  ) {
    throw new Error("replicaset_watch_path_input_invalid");
  }
  return `/apis/apps/v1/namespaces/${encodeURIComponent(namespace)}/replicasets?` +
    "watch=true&sendInitialEvents=true&allowWatchBookmarks=true&resourceVersionMatch=NotOlderThan&" +
    `resourceVersion=${encodeURIComponent(resourceVersion)}` +
    `&timeoutSeconds=${timeoutSeconds}`;
}

function sameReplicaSetIdentity(left, right) {
  const leftUid = left?.metadata?.uid;
  const rightUid = right?.metadata?.uid;
  if (typeof leftUid === "string" && leftUid && typeof rightUid === "string" && rightUid) {
    return leftUid === rightUid;
  }
  return typeof left?.metadata?.name === "string" && left.metadata.name &&
    left.metadata.name === right?.metadata?.name;
}

class ReplicaSetWatchEvidence {
  constructor(initialResourceVersion, consumerReplicaSets) {
    if (!Array.isArray(consumerReplicaSets)) {
      throw new Error("replicaset_watch_consumer_inventory_invalid");
    }
    this.lastResourceVersion = initialResourceVersion;
    this.consumerReplicaSets = consumerReplicaSets;
    this.initialEventsEnded = false;
    this.violation = false;
    this.error = null;
  }

  ingest(event) {
    if (!event || typeof event !== "object") {
      this.error = "watch_event_invalid";
      return;
    }
    if (event.type === "ERROR") {
      this.error = event?.object?.code === 410 ? "watch_resource_version_expired" : "watch_error_event";
      return;
    }
    const resourceVersion = event?.object?.metadata?.resourceVersion;
    if (typeof resourceVersion === "string" && resourceVersion) {
      this.lastResourceVersion = resourceVersion;
    }
    if (
      event.type === "BOOKMARK" &&
      event?.object?.metadata?.annotations?.["k8s.io/initial-events-end"] === "true"
    ) {
      this.initialEventsEnded = true;
      return;
    }
    if (!["ADDED", "MODIFIED", "DELETED"].includes(event.type)) return;
    const replicaSet = event.object;
    const known = this.consumerReplicaSets.some(value => sameReplicaSetIdentity(value, replicaSet));
    const consumer = known || replicaSetIsRecoveryConsumer(replicaSet);
    if (!consumer) return;
    if (!known) this.consumerReplicaSets.push(replicaSet);
    if (!replicaSetIsStopped(replicaSet) || this.initialEventsEnded) this.violation = true;
  }

  coversInitialEvents() {
    return this.initialEventsEnded;
  }
}

class PodWatchEvidence {
  constructor(
    namespace,
    parentNamespace,
    initialResourceVersion,
    { includeRecoveryConsumers = false, recoveryConsumerReplicaSets = [] } = {}
  ) {
    this.namespace = namespace;
    this.parentNamespace = parentNamespace;
    this.lastResourceVersion = initialResourceVersion;
    this.includeRecoveryConsumers = includeRecoveryConsumers;
    this.recoveryConsumerReplicaSets = recoveryConsumerReplicaSets;
    this.initialEventsEnded = false;
    this.violation = false;
    this.error = null;
  }

  ingest(event) {
    if (!event || typeof event !== "object") {
      this.error = "watch_event_invalid";
      return;
    }
    if (event.type === "ERROR") {
      this.error = event?.object?.code === 410 ? "watch_resource_version_expired" : "watch_error_event";
      return;
    }
    const resourceVersion = event?.object?.metadata?.resourceVersion;
    if (typeof resourceVersion === "string" && resourceVersion) {
      this.lastResourceVersion = resourceVersion;
    }
    if (
      event.type === "BOOKMARK" &&
      event?.object?.metadata?.annotations?.["k8s.io/initial-events-end"] === "true"
    ) {
      this.initialEventsEnded = true;
    }
    if (
      ["ADDED", "MODIFIED", "DELETED"].includes(event.type) &&
      forbiddenPod(this.namespace, this.parentNamespace, event.object, {
        includeRecoveryConsumers: this.includeRecoveryConsumers,
        recoveryConsumerReplicaSets: this.recoveryConsumerReplicaSets,
        eventType: event.type
      })
    ) {
      this.violation = true;
    }
  }

  coversInitialEvents() {
    return this.initialEventsEnded;
  }
}

module.exports = {
  PodWatchEvidence,
  ReplicaSetWatchEvidence,
  forbiddenPod,
  podWatchRawPath,
  replicaSetWatchRawPath
};
