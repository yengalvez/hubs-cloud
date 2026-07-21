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

function completeWatchListResourceVersion(list, expectedKind, expectedApiVersion) {
  if (
    typeof expectedKind !== "string" || !expectedKind ||
    typeof expectedApiVersion !== "string" || !expectedApiVersion ||
    list?.apiVersion !== expectedApiVersion ||
    list?.kind !== expectedKind ||
    !Array.isArray(list.items) ||
    typeof list?.metadata?.resourceVersion !== "string" ||
    !list.metadata.resourceVersion ||
    list.metadata.resourceVersion === "0" ||
    (Object.hasOwn(list?.metadata || {}, "continue") && list.metadata.continue !== "") ||
    (Object.hasOwn(list?.metadata || {}, "remainingItemCount") &&
      list.metadata.remainingItemCount !== 0)
  ) return null;
  return list.metadata.resourceVersion;
}

function namespacedResourceMetadataIsValid(object, namespace) {
  return object !== null && typeof object === "object" &&
    typeof object?.metadata?.name === "string" && Boolean(object.metadata.name) &&
    typeof object?.metadata?.uid === "string" && Boolean(object.metadata.uid) &&
    object?.metadata?.namespace === namespace &&
    typeof object?.metadata?.resourceVersion === "string" &&
    Boolean(object.metadata.resourceVersion) &&
    object.metadata.resourceVersion !== "0";
}

// Kubernetes may omit TypeMeta from objects embedded in a LIST response. If it
// is present it must still identify the expected resource. Watch event objects,
// by contrast, must carry exact TypeMeta so an event cannot cross collections.
function namespacedListItemIsValid(object, expectedKind, expectedApiVersion, namespace) {
  return (object?.kind === undefined || object.kind === expectedKind) &&
    (object?.apiVersion === undefined || object.apiVersion === expectedApiVersion) &&
    namespacedResourceMetadataIsValid(object, namespace);
}

function namespacedListItemWithTypeMeta(object, expectedKind, expectedApiVersion, namespace) {
  if (!namespacedListItemIsValid(object, expectedKind, expectedApiVersion, namespace)) {
    return null;
  }
  // LIST endpoints may omit TypeMeta on embedded objects. Restore only the
  // type already proven by the exact collection endpoint and item validator so
  // the same strict resource-contract parsers can be reused safely.
  return { ...object, apiVersion: expectedApiVersion, kind: expectedKind };
}

function namespacedWatchObjectIsValid(object, expectedKind, expectedApiVersion, namespace) {
  return object?.kind === expectedKind &&
    object?.apiVersion === expectedApiVersion &&
    namespacedResourceMetadataIsValid(object, namespace);
}

function podListRawPath(namespace) {
  if (typeof namespace !== "string" || !namespace) {
    throw new Error("pod_list_path_input_invalid");
  }
  return `/api/v1/namespaces/${encodeURIComponent(namespace)}/pods`;
}

function replicaSetListRawPath(namespace) {
  if (typeof namespace !== "string" || !namespace) {
    throw new Error("replicaset_list_path_input_invalid");
  }
  return `/apis/apps/v1/namespaces/${encodeURIComponent(namespace)}/replicasets`;
}

function podWatchRawPath(namespace, resourceVersion, timeoutSeconds = 600) {
  if (
    typeof namespace !== "string" || !namespace ||
    typeof resourceVersion !== "string" || !resourceVersion || resourceVersion === "0" ||
    !Number.isInteger(timeoutSeconds) || timeoutSeconds < 1 || timeoutSeconds > 600
  ) {
    throw new Error("pod_watch_path_input_invalid");
  }
  return `/api/v1/namespaces/${encodeURIComponent(namespace)}/pods?` +
    "watch=true&allowWatchBookmarks=true&" +
    `resourceVersion=${encodeURIComponent(resourceVersion)}` +
    `&timeoutSeconds=${timeoutSeconds}`;
}

function replicaSetWatchRawPath(namespace, resourceVersion, timeoutSeconds = 600) {
  if (
    typeof namespace !== "string" || !namespace ||
    typeof resourceVersion !== "string" || !resourceVersion || resourceVersion === "0" ||
    !Number.isInteger(timeoutSeconds) || timeoutSeconds < 1 || timeoutSeconds > 600
  ) {
    throw new Error("replicaset_watch_path_input_invalid");
  }
  return `/apis/apps/v1/namespaces/${encodeURIComponent(namespace)}/replicasets?` +
    "watch=true&allowWatchBookmarks=true&" +
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
  constructor(
    initialResourceVersion,
    consumerReplicaSets,
    { initialEventsEnded = false, namespace } = {}
  ) {
    if (
      !Array.isArray(consumerReplicaSets) || typeof initialEventsEnded !== "boolean" ||
      typeof namespace !== "string" || !namespace
    ) {
      throw new Error("replicaset_watch_consumer_inventory_invalid");
    }
    this.namespace = namespace;
    this.lastResourceVersion = initialResourceVersion;
    this.lastBookmarkResourceVersion = null;
    this.bookmarkSequence = 0;
    this.consumerReplicaSets = consumerReplicaSets;
    this.initialEventsEnded = initialEventsEnded;
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
    if (!["ADDED", "MODIFIED", "DELETED", "BOOKMARK"].includes(event.type)) {
      this.error = "watch_event_type_invalid";
      return;
    }
    const resourceVersion = event?.object?.metadata?.resourceVersion;
    if (typeof resourceVersion !== "string" || !resourceVersion || resourceVersion === "0") {
      this.error = event.type === "BOOKMARK"
        ? "watch_bookmark_resource_version_invalid"
        : "watch_event_resource_version_invalid";
      return;
    }
    this.lastResourceVersion = resourceVersion;
    if (event.type === "BOOKMARK") {
      this.lastBookmarkResourceVersion = resourceVersion;
      this.bookmarkSequence += 1;
      if (event?.object?.metadata?.annotations?.["k8s.io/initial-events-end"] === "true") {
        this.initialEventsEnded = true;
      }
      return;
    }
    if (!namespacedWatchObjectIsValid(event.object, "ReplicaSet", "apps/v1", this.namespace)) {
      this.error = "watch_event_object_invalid";
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
    this.lastBookmarkResourceVersion = null;
    this.bookmarkSequence = 0;
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
    if (!["ADDED", "MODIFIED", "DELETED", "BOOKMARK"].includes(event.type)) {
      this.error = "watch_event_type_invalid";
      return;
    }
    const resourceVersion = event?.object?.metadata?.resourceVersion;
    if (typeof resourceVersion !== "string" || !resourceVersion || resourceVersion === "0") {
      this.error = event.type === "BOOKMARK"
        ? "watch_bookmark_resource_version_invalid"
        : "watch_event_resource_version_invalid";
      return;
    }
    this.lastResourceVersion = resourceVersion;
    if (event.type === "BOOKMARK") {
      this.lastBookmarkResourceVersion = resourceVersion;
      this.bookmarkSequence += 1;
      if (event?.object?.metadata?.annotations?.["k8s.io/initial-events-end"] === "true") {
        this.initialEventsEnded = true;
      }
      return;
    }
    if (!namespacedWatchObjectIsValid(event.object, "Pod", "v1", this.namespace)) {
      this.error = "watch_event_object_invalid";
      return;
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
  completeWatchListResourceVersion,
  forbiddenPod,
  namespacedListItemIsValid,
  namespacedListItemWithTypeMeta,
  namespacedWatchObjectIsValid,
  podListRawPath,
  podWatchRawPath,
  replicaSetListRawPath,
  replicaSetWatchRawPath
};
