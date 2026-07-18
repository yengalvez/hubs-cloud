const assert = require("node:assert/strict");
const test = require("node:test");

const {
  HEARTBEAT_INTERVAL_MS,
  LEASE_DURATION_SECONDS,
  MUTATION_LEASE_MAX_AGE_MS,
  MUTATION_TIMEOUT_MS,
  OperationLease,
  expectedParentIsAlive,
  leaseDocument,
  leaseState,
  runLeaseGuardedMutation
} = require("./operation-lease");

const namespace = "hcce";
const cloudHolder = "cloud-apply:11111111-1111-4111-8111-111111111111";
const recoveryHolder = "root-recovery:22222222-2222-4222-8222-222222222222";
const baseNow = Date.parse("2026-07-18T08:00:00.000Z");

class MemoryLeaseClient {
  constructor(resource = null) {
    this.resource = resource;
    this.version = Number(resource?.metadata?.resourceVersion || 0);
    this.writes = [];
  }

  get() {
    return this.resource ? structuredClone(this.resource) : null;
  }

  create(document) {
    this.writes.push({ verb: "create", document: structuredClone(document) });
    if (this.resource) return { conflict: true };
    this.version += 1;
    this.resource = structuredClone(document);
    this.resource.metadata.uid = "lease-uid";
    this.resource.metadata.resourceVersion = String(this.version);
    return { resource: this.get() };
  }

  replace(document) {
    this.writes.push({ verb: "replace", document: structuredClone(document) });
    if (!this.resource || document?.metadata?.resourceVersion !== this.resource.metadata.resourceVersion) {
      return { conflict: true };
    }
    this.version += 1;
    this.resource = structuredClone(document);
    this.resource.metadata.uid = "lease-uid";
    this.resource.metadata.resourceVersion = String(this.version);
    return { resource: this.get() };
  }
}

function existingLease(holder, renewTime = baseNow) {
  const resource = leaseDocument({
    namespace,
    holder,
    now: renewTime
  });
  resource.metadata.resourceVersion = "7";
  resource.metadata.uid = "lease-uid";
  return resource;
}

test("operation Lease is acquired and released only through resourceVersion CAS", () => {
  const client = new MemoryLeaseClient();
  const lease = new OperationLease(client, { namespace, holder: cloudHolder, now: () => baseNow });
  lease.acquire();
  assert.equal(client.writes[0].verb, "create");
  assert.equal(client.writes[0].document.metadata.resourceVersion, undefined);
  assert.equal(client.writes[0].document.spec.acquireTime, "2026-07-18T08:00:00.000000Z");
  assert.equal(client.writes[0].document.spec.renewTime, "2026-07-18T08:00:00.000000Z");
  assert.equal(leaseState(client.get(), namespace, cloudHolder, baseNow), "owned");

  assert.equal(lease.release(), true);
  const release = client.writes.at(-1);
  assert.equal(release.verb, "replace");
  assert.equal(release.document.metadata.resourceVersion, "1");
  assert.equal(release.document.spec.holderIdentity, undefined);
  assert.deepEqual(Object.keys(release.document.spec).sort(), [
    "leaseDurationSeconds",
    "leaseTransitions"
  ]);
  assert.equal(leaseState(client.get(), namespace, cloudHolder, baseNow), "available");
});

test("operation Lease rejects a live recovery holder without writing", () => {
  const client = new MemoryLeaseClient(existingLease(recoveryHolder));
  const lease = new OperationLease(client, { namespace, holder: cloudHolder, now: () => baseNow + 1_000 });
  assert.throws(() => lease.acquire(), /operation_serialization_lease_busy/);
  assert.equal(client.writes.length, 0);
});

test("operation Lease takes over stale ownership by exact resourceVersion CAS", () => {
  const staleTime = baseNow - LEASE_DURATION_SECONDS * 1_000 - 1;
  const client = new MemoryLeaseClient(existingLease(recoveryHolder, staleTime));
  const lease = new OperationLease(client, { namespace, holder: cloudHolder, now: () => baseNow });
  lease.acquire();
  assert.equal(client.writes.length, 1);
  assert.equal(client.writes[0].verb, "replace");
  assert.equal(client.writes[0].document.metadata.resourceVersion, "7");
  assert.equal(client.get().spec.holderIdentity, cloudHolder);
  assert.equal(client.get().spec.leaseTransitions, 1);
});

test("operation Lease detects holder loss and never clears another holder", () => {
  const client = new MemoryLeaseClient(existingLease(cloudHolder));
  const lease = new OperationLease(client, { namespace, holder: cloudHolder, now: () => baseNow + 1_000 });
  client.resource = existingLease(recoveryHolder, baseNow + 500);
  client.resource.metadata.resourceVersion = "8";
  assert.throws(() => lease.assertHeld(), /operation_serialization_lease_lost:busy/);
  assert.throws(() => lease.renew(), /operation_serialization_lease_lost:busy/);
  assert.throws(() => lease.release(), /operation_lease_release_not_owned:busy/);
  assert.equal(client.get().spec.holderIdentity, recoveryHolder);
  assert.equal(client.writes.length, 0);
});

test("operation Lease accepts release only when the same UID returns exact free state", () => {
  const client = new MemoryLeaseClient(existingLease(cloudHolder));
  client.replace = document => ({
    resource: {
      ...existingLease(cloudHolder),
      metadata: {
        ...existingLease(cloudHolder).metadata,
        resourceVersion: String(Number(document.metadata.resourceVersion) + 1)
      }
    }
  });
  const lease = new OperationLease(client, { namespace, holder: cloudHolder, now: () => baseNow + 1_000 });
  assert.throws(() => lease.release(), /operation_serialization_lease_release_failed/);
});

test("operation Lease fails closed while the Lease is terminating", () => {
  const terminating = existingLease(cloudHolder);
  terminating.metadata.deletionTimestamp = "2026-07-18T08:00:01.000Z";
  const client = new MemoryLeaseClient(terminating);
  const lease = new OperationLease(client, { namespace, holder: cloudHolder, now: () => baseNow + 1_000 });
  assert.equal(leaseState(client.get(), namespace, cloudHolder, baseNow + 1_000), "invalid");
  assert.throws(() => lease.acquire(), /operation_serialization_lease_invalid/);
  assert.equal(client.writes.length, 0);

  const explicitNull = existingLease(cloudHolder);
  explicitNull.metadata.deletionTimestamp = null;
  assert.equal(leaseState(explicitNull, namespace, cloudHolder, baseNow + 1_000), "invalid");
});

test("operation Lease rejects metadata and exact-spec drift", () => {
  const mutations = [
    value => { delete value.metadata.uid; },
    value => { value.metadata.annotations = { unexpected: "true" }; },
    value => { value.metadata.annotations = null; },
    value => { value.metadata.finalizers = ["unexpected"]; },
    value => { value.metadata.ownerReferences = null; },
    value => { value.spec.leaseTransitions = -1; },
    value => { value.spec.unexpected = true; },
    value => { delete value.spec.acquireTime; },
    value => { value.spec.renewTime = "2026-07-18T08:00:00.000Z"; }
  ];
  for (const mutate of mutations) {
    const drifted = existingLease(cloudHolder);
    mutate(drifted);
    assert.equal(leaseState(drifted, namespace, cloudHolder, baseNow + 1_000), "invalid");
  }
});

test("guarded mutations require enough remaining TTL and detect loss after execution", () => {
  let now = baseNow;
  const client = new MemoryLeaseClient(existingLease(cloudHolder));
  const lease = new OperationLease(client, { namespace, holder: cloudHolder, now: () => now });
  now += MUTATION_LEASE_MAX_AGE_MS + 1;
  assert.throws(
    () => lease.assertFreshForMutation(),
    /operation_serialization_lease_not_fresh_for_mutation/
  );

  now = baseNow + 1_000;
  let mutationRan = false;
  assert.throws(() => runLeaseGuardedMutation(
    () => lease.assertFreshForMutation(),
    () => {
      mutationRan = true;
      client.resource = existingLease(recoveryHolder, now);
      client.resource.metadata.resourceVersion = "9";
    }
  ), /operation_serialization_lease_lost:busy/);
  assert.equal(mutationRan, true);
  assert.ok(MUTATION_TIMEOUT_MS + MUTATION_LEASE_MAX_AGE_MS < LEASE_DURATION_SECONDS * 1_000);
});

test("heartbeat contract renews more often than every 30 seconds", () => {
  assert.ok(HEARTBEAT_INTERVAL_MS > 0);
  assert.ok(HEARTBEAT_INTERVAL_MS <= 30_000);
});

test("heartbeat refuses an orphaned or mismatched apply parent", () => {
  const signal = () => {};
  assert.equal(expectedParentIsAlive(1234, { actualParentPid: 1234, signal }), true);
  assert.equal(expectedParentIsAlive(1234, { actualParentPid: 1, signal }), false);
  assert.equal(expectedParentIsAlive(1234, {
    actualParentPid: 1234,
    signal: () => { throw new Error("gone"); }
  }), false);
});
