const assert = require("node:assert/strict");
const test = require("node:test");

const {
  canonicalJson,
  createCutoverJournal,
  cutoverJournalConfigMap
} = require("./cutover-journal");
const {
  verifyDurableCutoverJournalLiveEvidence
} = require("./verify-live-runner-control-plane");

const NAMESPACE = "hcce";
const NAMESPACE_UID = "namespace-uid";
const TARGET_HASHES = {
  journalPolicy: "1".repeat(64),
  journalBinding: "2".repeat(64),
  parentPolicy: "3".repeat(64),
  parentBinding: "4".repeat(64),
  parentDeployment: "5".repeat(64)
};

function liveNamespace() {
  return {
    apiVersion: "v1",
    kind: "Namespace",
    metadata: {
      name: NAMESPACE,
      uid: NAMESPACE_UID,
      resourceVersion: "10"
    },
    spec: { finalizers: ["kubernetes"] },
    status: { phase: "Active" }
  };
}

function liveJournal(mode = "clean-install") {
  const journal = createCutoverJournal({
    mode,
    operationId: "12345678-1234-4234-8234-123456789abc",
    authorization: mode === "clean-install" ? null : { approved: true },
    expectedKubeContext: "historic-context-alias",
    namespace: NAMESPACE,
    namespaceUid: NAMESPACE_UID,
    baselineDeployment: mode === "clean-install"
      ? null
      : { name: "bot-orchestrator", uid: "deployment-uid", resourceVersion: "1" },
    manifestSha256: "a".repeat(64),
    targetHashes: TARGET_HASHES,
    issuedAt: "2030-01-01T00:00:00.000Z"
  }, Buffer.alloc(32, 9));
  const configMap = cutoverJournalConfigMap(journal);
  configMap.metadata.uid = "journal-uid";
  configMap.metadata.resourceVersion = "11";
  configMap.metadata.creationTimestamp = "2026-07-19T12:00:00Z";
  return configMap;
}

function liveParentDeployment() {
  return {
    apiVersion: "apps/v1",
    kind: "Deployment",
    metadata: {
      name: "bot-orchestrator",
      namespace: NAMESPACE,
      uid: "deployment-uid",
      resourceVersion: "12"
    }
  };
}

function errors(
  liveNamespaceValue = liveNamespace(),
  journalConfigMap = liveJournal(),
  deployment = liveParentDeployment()
) {
  return verifyDurableCutoverJournalLiveEvidence({
    namespace: NAMESPACE,
    liveNamespace: liveNamespaceValue,
    journalConfigMap,
    liveParentDeployment: deployment
  });
}

test("live acceptance requires the protected journal bound to one Active Namespace UID", () => {
  assert.deepEqual(errors(), []);
  assert.notDeepEqual(errors(liveNamespace(), null), []);

  const terminatingJournal = liveJournal();
  terminatingJournal.metadata.deletionTimestamp = "2026-07-19T12:01:00Z";
  assert.notDeepEqual(errors(liveNamespace(), terminatingJournal), []);

  const unprotectedJournal = liveJournal();
  delete unprotectedJournal.metadata.finalizers;
  assert.notDeepEqual(errors(liveNamespace(), unprotectedJournal), []);

  const wrongUidJournal = liveJournal();
  const payload = JSON.parse(wrongUidJournal.data["journal.json"]);
  payload.namespace.uid = "recreated-namespace-uid";
  wrongUidJournal.data["journal.json"] = canonicalJson(payload);
  assert.notDeepEqual(errors(liveNamespace(), wrongUidJournal), []);

  const terminatingNamespace = liveNamespace();
  terminatingNamespace.metadata.deletionTimestamp = "2026-07-19T12:01:00Z";
  terminatingNamespace.status.phase = "Terminating";
  assert.notDeepEqual(errors(terminatingNamespace, liveJournal()), []);

  const terminatingDeployment = liveParentDeployment();
  terminatingDeployment.metadata.deletionTimestamp = "2026-07-19T12:01:00Z";
  assert.notDeepEqual(errors(liveNamespace(), liveJournal(), terminatingDeployment), []);

  const recreatedDeployment = liveParentDeployment();
  recreatedDeployment.metadata.uid = "recreated-deployment-uid";
  assert.notDeepEqual(
    errors(liveNamespace(), liveJournal("pristine-cutover"), recreatedDeployment),
    []
  );
});
