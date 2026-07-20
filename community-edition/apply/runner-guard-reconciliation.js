const {
  FENCE_APP_LABEL,
  INTENT_APP_LABEL,
  RUNNER_APP_LABEL,
  RUNNER_NAMESPACE,
  exactGuardRecordFromPod,
  exactManagedRunnerRecordFromPod,
  guardPodDocumentForIdentity,
  requireCompletePodList
} = require("../services/bot-orchestrator/kubernetes-runner-manager");

function classifyRunnerNamespacePod(pod) {
  const app = pod?.metadata?.labels?.app;
  try {
    if (app === FENCE_APP_LABEL) {
      return { type: "fence", record: exactGuardRecordFromPod(pod, "fence"), pod };
    }
    if (app === INTENT_APP_LABEL) {
      return { type: "intent", record: exactGuardRecordFromPod(pod, "intent"), pod };
    }
    if (app === RUNNER_APP_LABEL) {
      return { type: "runner", record: exactManagedRunnerRecordFromPod(pod), pod };
    }
  } catch (error) {
    throw new Error(`runner_namespace_pod_contract_invalid:${error.message}`);
  }
  throw new Error("runner_namespace_unknown_pod");
}

function completeRunnerNamespaceInventory(response) {
  const list = requireCompletePodList(response);
  const inventory = {
    resourceVersion: list.resourceVersion,
    runners: new Map(),
    intents: new Map(),
    fences: new Map()
  };
  for (const pod of list.items) {
    const classified = classifyRunnerNamespacePod(pod);
    const key = classified.record.name;
    const collection = classified.type === "runner"
      ? inventory.runners
      : classified.type === "intent" ? inventory.intents : inventory.fences;
    if (collection.has(key)) throw new Error("runner_namespace_identity_ambiguous");
    collection.set(key, classified);
  }
  return inventory;
}

function sameGuardIdentity(left, right) {
  return !!left && !!right &&
    left.name === right.name &&
    left.roomKey === right.roomKey &&
    left.processGeneration === right.processGeneration;
}

function deletePreconditionConflict(error) {
  return error?.status === 409 || error?.conflict === true;
}

async function ensurePermanentFence(intent, api, { maxAttempts = 20, retryDelayMs = 250 } = {}) {
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    const target = await api.getPod(intent.name);
    if (target) {
      const classified = classifyRunnerNamespacePod(target);
      if (classified.type === "fence") {
        if (!sameGuardIdentity(classified.record, intent)) {
          throw new Error("runner_fence_identity_mismatch");
        }
        return classified;
      }
      if (classified.type !== "runner" || !sameGuardIdentity(classified.record, intent)) {
        throw new Error("runner_fence_target_identity_mismatch");
      }
      await api.deletePodByUid(classified.pod);
    } else {
      try {
        await api.createPod(guardPodDocumentForIdentity(intent, "fence", RUNNER_NAMESPACE));
      } catch (_error) {
        // A 409, timeout, or lost success response is resolved only by the
        // next exact GET. The fence POST is never inferred from transport.
      }
    }
    if (attempt + 1 < maxAttempts) await api.sleep(retryDelayMs);
  }
  throw new Error("runner_fence_unconfirmed");
}

async function reconcileRunnerNamespace(
  api,
  { maxAttempts = 40, retryDelayMs = 250 } = {}
) {
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    const inventory = completeRunnerNamespaceInventory(await api.listPods());
    if (inventory.runners.size === 0 && inventory.intents.size === 0) {
      return {
        runners: 0,
        intents: 0,
        fences: inventory.fences.size,
        resourceVersion: inventory.resourceVersion,
        attempts: attempt
      };
    }

    const protectedNames = new Set(inventory.fences.keys());
    for (const { record, pod } of inventory.intents.values()) {
      if (record.state === "unarmed") {
        try {
          await api.deletePodByUid(pod, { requireResourceVersion: true });
        } catch (error) {
          // A concurrent exact unarmed -> armed PATCH must win over cleanup.
          // Only the next complete LIST may classify the new durable state.
          if (!deletePreconditionConflict(error)) throw error;
        }
        continue;
      }
      const fence = await ensurePermanentFence(record, api, { retryDelayMs });
      protectedNames.add(fence.record.name);
      try {
        await api.deletePodByUid(pod, { requireResourceVersion: true });
      } catch (error) {
        if (!deletePreconditionConflict(error)) throw error;
      }
    }
    for (const { record, pod } of inventory.runners.values()) {
      // ensurePermanentFence may have replaced a runner from this stale LIST
      // with its permanent fence. Never feed that stale UID/name into another
      // deletion after the fence owns the name.
      if (!protectedNames.has(record.name)) await api.deletePodByUid(pod);
    }
    if (attempt < maxAttempts) await api.sleep(retryDelayMs);
  }
  throw new Error("runner_namespace_reconciliation_unconfirmed");
}

module.exports = {
  classifyRunnerNamespacePod,
  completeRunnerNamespaceInventory,
  deletePreconditionConflict,
  ensurePermanentFence,
  reconcileRunnerNamespace,
  requireCompletePodList,
  sameGuardIdentity
};
