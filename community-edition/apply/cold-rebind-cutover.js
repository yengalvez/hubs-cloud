const crypto = require("node:crypto");

const COLD_REBIND_CUTOVER_PROFILE = "yenhubs-cold-rebind-runner-cutover-v1";
const HEX = /^[a-f0-9]{64}$/;
function canonical(value) {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${canonical(value[key])}`).join(",")}}`;
}
function exact(value, keys) {
  return value && typeof value === "object" && !Array.isArray(value) &&
    canonical(Object.keys(value).sort()) === canonical([...keys].sort());
}
function sha256(value) { return crypto.createHash("sha256").update(value).digest("hex"); }

// Include credentials privately in this hash; never serialize this snapshot to
// diagnostic output. Ignore status and Kubernetes bookkeeping only.
function coldRebindResourceDigest(list) {
  if (list?.kind !== "List" || !Array.isArray(list.items) || list.items.length !== 44) {
    throw new Error("cold_rebind_inventory_invalid");
  }
  const resources = list.items.map(resource => {
    const { metadata, status: _status, ...body } = resource;
    if (!metadata?.uid || metadata.deletionTimestamp || !metadata.name) {
      throw new Error("cold_rebind_resource_identity_invalid");
    }
    const annotations = { ...metadata.annotations };
    delete annotations["kubectl.kubernetes.io/last-applied-configuration"];
    delete annotations["deployment.kubernetes.io/revision"];
    return { ...body, metadata: { name: metadata.name, namespace: metadata.namespace || "",
      uid: metadata.uid, labels: metadata.labels || {}, annotations } };
  }).sort((a, b) => canonical([a.apiVersion, a.kind, a.metadata.namespace, a.metadata.name])
    .localeCompare(canonical([b.apiVersion, b.kind, b.metadata.namespace, b.metadata.name])));
  if (resources.filter(r => r.kind === "Deployment").length !== 12 ||
      new Set(resources.map(r => canonical([r.apiVersion, r.kind, r.metadata.namespace, r.metadata.name]))).size !== 44) {
    throw new Error("cold_rebind_inventory_invalid");
  }
  return sha256(canonical(resources));
}

function verifyColdRebindCutoverAttestation(attestation, options, verifyHmac, activeNamespace) {
  if (!exact(attestation, ["schemaVersion", "profileId", "expectedKubeContext", "namespace",
    "namespaceUid", "capturedAt", "checkpointManifestSha256", "targetManifestSha256",
    "baselineManifestSha256", "baselineResourceSha256", "botOrchestratorDeployment", "hmacSha256"])) {
    throw new Error("cold_rebind_attestation_shape_invalid");
  }
  verifyHmac(attestation, options.key);
  if (attestation.schemaVersion !== 1 || attestation.profileId !== COLD_REBIND_CUTOVER_PROFILE ||
      attestation.expectedKubeContext !== options.expectedKubeContext || attestation.namespace !== options.namespace ||
      ![attestation.checkpointManifestSha256, attestation.targetManifestSha256,
        attestation.baselineManifestSha256, attestation.baselineResourceSha256].every(v => typeof v === "string" && HEX.test(v)) ||
      attestation.targetManifestSha256 !== options.targetManifestSha256 ||
      attestation.baselineManifestSha256 !== options.baselineManifestSha256 ||
      attestation.baselineResourceSha256 !== options.baselineResourceSha256 ||
      !exact(attestation.botOrchestratorDeployment, ["name", "uid", "resourceVersion"]) ||
      attestation.botOrchestratorDeployment.name !== "bot-orchestrator") {
    throw new Error("cold_rebind_attestation_contract_invalid");
  }
  const captured = Date.parse(attestation.capturedAt);
  const now = (options.now || Date.now)();
  if (!Number.isFinite(captured) || captured > now + 30000 || now - captured > 5 * 60 * 1000) {
    throw new Error("cold_rebind_attestation_stale");
  }
  const live = options.liveDeployment;
  if (!activeNamespace(options.liveNamespace, options.namespace) ||
      options.liveNamespace.metadata.uid !== attestation.namespaceUid ||
      options.liveNamespace.metadata.annotations?.["yenhubs.org/target-profile"] !== "cold-rebind-legacy-active-v1" ||
      live?.apiVersion !== "apps/v1" || live?.kind !== "Deployment" ||
      live.metadata?.namespace !== options.namespace || live.metadata?.name !== "bot-orchestrator" ||
      live.metadata?.uid !== attestation.botOrchestratorDeployment.uid ||
      live.metadata?.resourceVersion !== attestation.botOrchestratorDeployment.resourceVersion) {
    throw new Error("cold_rebind_live_binding_changed");
  }
  return true;
}

module.exports = { COLD_REBIND_CUTOVER_PROFILE, coldRebindResourceDigest, verifyColdRebindCutoverAttestation };
