const { spawnSync } = require("node:child_process");

const OPERATION_LEASE_NAME = "yenhubs-operation-serialization";
const OPERATION_LEASE_LABEL = "yenhubs.org/operation-serialization";
const OPERATION_LEASE_LABEL_VALUE = "deployment-recovery";
const LEASE_DURATION_SECONDS = 120;
const HEARTBEAT_INTERVAL_MS = 20_000;
const MUTATION_TIMEOUT_MS = 60_000;
const MUTATION_LEASE_MAX_AGE_MS = 40_000;
const HOLDER_PATTERN = /^(?:cloud-apply|root-recovery):[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;

function exactLeaseLabels(labels) {
  return labels &&
    typeof labels === "object" &&
    !Array.isArray(labels) &&
    Object.keys(labels).length === 1 &&
    labels[OPERATION_LEASE_LABEL] === OPERATION_LEASE_LABEL_VALUE;
}

function validTimestamp(value) {
  return typeof value === "string" &&
    /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$/u.test(value) &&
    Number.isFinite(Date.parse(value));
}

function leaseState(lease, namespace, holder, nowMs) {
  if (lease === null) return "absent";
  if (
    !lease ||
    lease.apiVersion !== "coordination.k8s.io/v1" ||
    lease.kind !== "Lease" ||
    lease?.metadata?.name !== OPERATION_LEASE_NAME ||
    lease?.metadata?.namespace !== namespace ||
    lease?.metadata?.deletionTimestamp !== undefined ||
    typeof lease?.metadata?.uid !== "string" ||
    !lease.metadata.uid ||
    typeof lease?.metadata?.resourceVersion !== "string" ||
    !lease.metadata.resourceVersion ||
    !exactLeaseLabels(lease?.metadata?.labels) ||
    (lease?.metadata?.annotations !== undefined &&
      (!lease.metadata.annotations || Object.keys(lease.metadata.annotations).length !== 0)) ||
    (lease?.metadata?.finalizers !== undefined &&
      (!Array.isArray(lease.metadata.finalizers) || lease.metadata.finalizers.length !== 0)) ||
    (lease?.metadata?.ownerReferences !== undefined &&
      (!Array.isArray(lease.metadata.ownerReferences) || lease.metadata.ownerReferences.length !== 0))
  ) {
    return "invalid";
  }
  const liveHolder = lease?.spec?.holderIdentity;
  const leaseTransitions = lease?.spec?.leaseTransitions;
  if (!Number.isInteger(leaseTransitions) || leaseTransitions < 0) return "invalid";
  if (liveHolder === undefined || liveHolder === null || liveHolder === "") {
    return Object.keys(lease?.spec || {}).sort().join(",") ===
      "leaseDurationSeconds,leaseTransitions" &&
      lease.spec.leaseDurationSeconds === LEASE_DURATION_SECONDS
      ? "available"
      : "invalid";
  }
  if (
    Object.keys(lease?.spec || {}).sort().join(",") !==
      "acquireTime,holderIdentity,leaseDurationSeconds,leaseTransitions,renewTime"
  ) {
    return "invalid";
  }
  if (!HOLDER_PATTERN.test(liveHolder)) return "invalid";
  if (lease?.spec?.leaseDurationSeconds !== LEASE_DURATION_SECONDS) return "invalid";
  if (!validTimestamp(lease.spec.acquireTime) || !validTimestamp(lease.spec.renewTime)) return "invalid";
  const referenceTime = lease.spec.renewTime;
  const expiresAt = Date.parse(referenceTime) + LEASE_DURATION_SECONDS * 1_000;
  if (nowMs >= expiresAt) return liveHolder === holder ? "expired-owned" : "stale";
  return liveHolder === holder ? "owned" : "busy";
}

function leaseDocument({ namespace, holder, now, current = null, release = false }) {
  if (!HOLDER_PATTERN.test(holder)) throw new Error("operation_lease_holder_invalid");
  const timestamp = new Date(now).toISOString().replace(/(\.\d{3})Z$/u, "$1000Z");
  const resourceVersion = current?.metadata?.resourceVersion;
  const previousTransitions = Number(current?.spec?.leaseTransitions || 0);
  const previousHolder = current?.spec?.holderIdentity;
  const acquiring = !current || previousHolder !== holder;
  const metadata = {
    name: OPERATION_LEASE_NAME,
    namespace,
    labels: { [OPERATION_LEASE_LABEL]: OPERATION_LEASE_LABEL_VALUE }
  };
  if (resourceVersion) metadata.resourceVersion = resourceVersion;
  const leaseTransitions = acquiring ? previousTransitions + (current ? 1 : 0) : previousTransitions;
  const spec = release
    ? { leaseDurationSeconds: LEASE_DURATION_SECONDS, leaseTransitions }
    : {
      holderIdentity: holder,
      leaseDurationSeconds: LEASE_DURATION_SECONDS,
      acquireTime: acquiring ? timestamp : current.spec.acquireTime,
      renewTime: timestamp,
      leaseTransitions
    };
  return {
    apiVersion: "coordination.k8s.io/v1",
    kind: "Lease",
    metadata,
    spec
  };
}

function runLeaseGuardedMutation(assertFresh, mutation) {
  if (typeof assertFresh !== "function" || typeof mutation !== "function") {
    throw new Error("lease_guarded_mutation_arguments_invalid");
  }
  assertFresh();
  let result;
  let mutationError;
  try {
    result = mutation();
  } catch (error) {
    mutationError = error;
  }
  assertFresh();
  if (mutationError) throw mutationError;
  return result;
}

class OperationLease {
  constructor(client, { namespace, holder, now = () => Date.now(), maxCasAttempts = 8 }) {
    if (!namespace || typeof namespace !== "string") throw new Error("operation_lease_namespace_invalid");
    if (!HOLDER_PATTERN.test(holder)) throw new Error("operation_lease_holder_invalid");
    this.client = client;
    this.namespace = namespace;
    this.holder = holder;
    this.now = now;
    this.maxCasAttempts = maxCasAttempts;
  }

  acquire() {
    for (let attempt = 0; attempt < this.maxCasAttempts; attempt += 1) {
      const current = this.client.get(this.namespace);
      const state = leaseState(current, this.namespace, this.holder, this.now());
      if (state === "busy") throw new Error("operation_serialization_lease_busy");
      if (state === "invalid") throw new Error("operation_serialization_lease_invalid");
      const document = leaseDocument({
        namespace: this.namespace,
        holder: this.holder,
        now: this.now(),
        current
      });
      const result = state === "absent"
        ? this.client.create(document)
        : this.client.replace(document);
      if (result?.conflict) continue;
      if (!result?.resource) throw new Error("operation_serialization_lease_write_failed");
      if (leaseState(result.resource, this.namespace, this.holder, this.now()) !== "owned") {
        throw new Error("operation_serialization_lease_not_owned_after_write");
      }
      return result.resource;
    }
    throw new Error("operation_serialization_lease_cas_exhausted");
  }

  renew() {
    const current = this.client.get(this.namespace);
    const state = leaseState(current, this.namespace, this.holder, this.now());
    if (state !== "owned") throw new Error(`operation_serialization_lease_lost:${state}`);
    const result = this.client.replace(leaseDocument({
      namespace: this.namespace,
      holder: this.holder,
      now: this.now(),
      current
    }));
    if (result?.conflict) throw new Error("operation_serialization_lease_lost:conflict");
    if (!result?.resource || leaseState(result.resource, this.namespace, this.holder, this.now()) !== "owned") {
      throw new Error("operation_serialization_lease_lost:write");
    }
    return result.resource;
  }

  assertHeld() {
    const state = leaseState(this.client.get(this.namespace), this.namespace, this.holder, this.now());
    if (state !== "owned") throw new Error(`operation_serialization_lease_lost:${state}`);
  }

  assertFreshForMutation(maxAgeMs = MUTATION_LEASE_MAX_AGE_MS) {
    if (!Number.isInteger(maxAgeMs) || maxAgeMs < 0) {
      throw new Error("operation_serialization_lease_freshness_invalid");
    }
    const current = this.client.get(this.namespace);
    const now = this.now();
    const state = leaseState(current, this.namespace, this.holder, now);
    if (state !== "owned") throw new Error(`operation_serialization_lease_lost:${state}`);
    const age = now - Date.parse(current.spec.renewTime);
    if (age < -5_000 || age > maxAgeMs) {
      throw new Error("operation_serialization_lease_not_fresh_for_mutation");
    }
  }

  release() {
    for (let attempt = 0; attempt < this.maxCasAttempts; attempt += 1) {
      const current = this.client.get(this.namespace);
      const state = leaseState(current, this.namespace, this.holder, this.now());
      if (state !== "owned") {
        throw new Error(`operation_lease_release_not_owned:${state}`);
      }
      const result = this.client.replace(leaseDocument({
        namespace: this.namespace,
        holder: this.holder,
        now: this.now(),
        current,
        release: true
      }));
      if (result?.conflict) continue;
      if (
        !result?.resource ||
        result.resource?.metadata?.uid !== current.metadata.uid ||
        leaseState(result.resource, this.namespace, this.holder, this.now()) !== "available"
      ) {
        throw new Error("operation_serialization_lease_release_failed");
      }
      return true;
    }
    throw new Error("operation_serialization_lease_release_cas_exhausted");
  }
}

class KubectlLeaseClient {
  constructor({ context }) {
    this.context = context;
  }

  command(args, input) {
    return spawnSync(
      "kubectl",
      ["--context", this.context, "--request-timeout=20s", ...args],
      { input, encoding: "utf8", timeout: 30_000 }
    );
  }

  get(namespace) {
    const result = this.command([
      "-n", namespace, "get", "lease", OPERATION_LEASE_NAME, "-o", "json"
    ]);
    if (result.status === 0) return JSON.parse(result.stdout);
    if (`${result.stderr || ""}`.includes("NotFound")) return null;
    throw new Error(`operation_serialization_lease_read_failed:${result.status}`);
  }

  write(verb, document) {
    const result = this.command([verb, "-f", "-", "-o", "json"], JSON.stringify(document));
    if (result.status === 0) return { resource: JSON.parse(result.stdout) };
    const diagnostic = `${result.stderr || ""}`;
    if (diagnostic.includes("Conflict") || diagnostic.includes("AlreadyExists")) {
      return { conflict: true };
    }
    throw new Error(`operation_serialization_lease_${verb}_failed:${result.status}`);
  }

  create(document) {
    return this.write("create", document);
  }

  replace(document) {
    return this.write("replace", document);
  }
}

function heartbeatArgs(argv) {
  if (argv.length !== 4) throw new Error("operation_lease_heartbeat_arguments_invalid");
  const [context, namespace, holder, parentPidText] = argv;
  const parentPid = Number(parentPidText);
  if (!context || !namespace || !HOLDER_PATTERN.test(holder) || !Number.isInteger(parentPid) || parentPid < 2) {
    throw new Error("operation_lease_heartbeat_arguments_invalid");
  }
  return { context, namespace, holder, parentPid };
}

function expectedParentIsAlive(expectedPid, {
  actualParentPid = process.ppid,
  signal = process.kill
} = {}) {
  if (!Number.isInteger(expectedPid) || expectedPid < 2 || actualParentPid !== expectedPid) return false;
  try {
    signal(expectedPid, 0);
    return true;
  } catch (_error) {
    return false;
  }
}

async function heartbeatMain(argv) {
  const { context, namespace, holder, parentPid } = heartbeatArgs(argv);
  const lease = new OperationLease(new KubectlLeaseClient({ context }), { namespace, holder });
  let stopping = false;
  process.once("SIGTERM", () => { stopping = true; });
  process.once("SIGINT", () => { stopping = true; });
  while (!stopping) {
    if (!expectedParentIsAlive(parentPid)) throw new Error("operation_lease_heartbeat_parent_lost");
    lease.renew();
    await new Promise(resolve => setTimeout(resolve, HEARTBEAT_INTERVAL_MS));
  }
}

if (require.main === module) {
  heartbeatMain(process.argv.slice(2)).catch(() => {
    process.exitCode = 2;
  });
}

module.exports = {
  HEARTBEAT_INTERVAL_MS,
  HOLDER_PATTERN,
  LEASE_DURATION_SECONDS,
  MUTATION_TIMEOUT_MS,
  MUTATION_LEASE_MAX_AGE_MS,
  OPERATION_LEASE_LABEL,
  OPERATION_LEASE_LABEL_VALUE,
  OPERATION_LEASE_NAME,
  KubectlLeaseClient,
  OperationLease,
  expectedParentIsAlive,
  exactLeaseLabels,
  leaseDocument,
  leaseState,
  runLeaseGuardedMutation
};
