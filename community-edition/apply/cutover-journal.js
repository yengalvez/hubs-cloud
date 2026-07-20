const crypto = require("node:crypto");
const { isDeepStrictEqual } = require("node:util");

const CUTOVER_JOURNAL_NAME = "yenhubs-runner-cutover-v2";
const CUTOVER_JOURNAL_DATA_KEY = "journal.json";
const CUTOVER_JOURNAL_DOMAIN = "yenhubs-runner-cutover-v2\0";
const CUTOVER_JOURNAL_FINALIZER = "yenhubs.org/cutover-journal-protection";
const CUTOVER_JOURNAL_OPERATION = "first-fence-bootstrap";
const CUTOVER_JOURNAL_SCHEMA_VERSION = 2;
const HEX_SHA256 = /^[0-9a-f]{64}$/;
const UUID_V4 = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const MODES = new Set(["clean-install", "pristine-cutover"]);

function exactKeys(value, keys) {
  return value && typeof value === "object" && !Array.isArray(value) &&
    JSON.stringify(Object.keys(value).sort()) === JSON.stringify([...keys].sort());
}

function canonicalJson(value) {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  return `{${Object.keys(value).sort().map(key =>
    `${JSON.stringify(key)}:${canonicalJson(value[key])}`
  ).join(",")}}`;
}

function sha256Canonical(value) {
  return crypto.createHash("sha256").update(canonicalJson(value), "utf8").digest("hex");
}

function journalHmac(unsignedJournal, key) {
  if (!Buffer.isBuffer(key) || key.length < 32 || key.length > 4096) {
    throw new Error("runner_cutover_journal_key_invalid");
  }
  return crypto.createHmac("sha256", key)
    .update(CUTOVER_JOURNAL_DOMAIN, "utf8")
    .update(canonicalJson(unsignedJournal), "utf8")
    .digest("hex");
}

function exactBaseline(value, mode) {
  if (mode === "clean-install") return value === null;
  return exactKeys(value, ["name", "uid", "resourceVersion"]) &&
    value.name === "bot-orchestrator" &&
    typeof value.uid === "string" && value.uid.length > 0 &&
    typeof value.resourceVersion === "string" && value.resourceVersion.length > 0;
}

function exactTargetHashes(value) {
  return exactKeys(value, [
    "journalPolicy",
    "journalBinding",
    "parentPolicy",
    "parentBinding",
    "parentDeployment"
  ]) &&
    Object.values(value).every(hash => HEX_SHA256.test(hash));
}

function unsignedCutoverJournal({
  mode,
  operationId,
  authorizationSha256,
  expectedKubeContext,
  namespace,
  namespaceUid,
  baselineDeployment,
  manifestSha256,
  targetHashes,
  issuedAt
}) {
  const journal = {
    schemaVersion: CUTOVER_JOURNAL_SCHEMA_VERSION,
    mode,
    operation: CUTOVER_JOURNAL_OPERATION,
    operationId,
    authorizationSha256,
    expectedKubeContext,
    namespace: { name: namespace, uid: namespaceUid },
    baselineDeployment,
    manifestSha256,
    targetHashes,
    issuedAt
  };
  if (
    !MODES.has(mode) ||
    !UUID_V4.test(operationId || "") ||
    (mode === "pristine-cutover"
      ? !HEX_SHA256.test(authorizationSha256 || "")
      : authorizationSha256 !== null) ||
    typeof expectedKubeContext !== "string" || !expectedKubeContext ||
    typeof namespace !== "string" || !namespace ||
    typeof namespaceUid !== "string" || !namespaceUid ||
    !exactBaseline(baselineDeployment, mode) ||
    !HEX_SHA256.test(manifestSha256 || "") ||
    !exactTargetHashes(targetHashes) ||
    !Number.isFinite(Date.parse(issuedAt))
  ) {
    throw new Error("runner_cutover_journal_contract_invalid");
  }
  return journal;
}

function createCutoverJournal(input, key) {
  const { authorization, ...fields } = input;
  if (
    (fields.mode === "pristine-cutover" &&
      (!authorization || typeof authorization !== "object" || Array.isArray(authorization))) ||
    (fields.mode === "clean-install" && authorization !== null)
  ) {
    throw new Error("runner_cutover_journal_authorization_invalid");
  }
  const unsigned = unsignedCutoverJournal({
    ...fields,
    authorizationSha256: fields.mode === "pristine-cutover"
      ? sha256Canonical(authorization)
      : null
  });
  return { ...unsigned, hmacSha256: journalHmac(unsigned, key) };
}

function verifyCutoverJournalStructure(journal, {
  expectedKubeContext,
  namespace,
  namespaceUid,
  manifestSha256,
  targetHashes,
  allowFutureIssuedAt = false,
  now = () => Date.now()
}) {
  if (!exactKeys(journal, [
    "schemaVersion",
    "mode",
    "operation",
    "operationId",
    "authorizationSha256",
    "expectedKubeContext",
    "namespace",
    "baselineDeployment",
    "manifestSha256",
    "targetHashes",
    "issuedAt",
    "hmacSha256"
  ])) {
    throw new Error("runner_cutover_journal_shape_invalid");
  }
  if (!HEX_SHA256.test(journal.hmacSha256 || "")) {
    throw new Error("runner_cutover_journal_hmac_invalid");
  }
  const unsigned = structuredClone(journal);
  delete unsigned.hmacSha256;
  let rebuilt;
  try {
    rebuilt = unsignedCutoverJournal({
      mode: journal.mode,
      operationId: journal.operationId,
      authorizationSha256: journal.authorizationSha256,
      expectedKubeContext: journal.expectedKubeContext,
      namespace: journal.namespace?.name,
      namespaceUid: journal.namespace?.uid,
      baselineDeployment: journal.baselineDeployment,
      manifestSha256: journal.manifestSha256,
      targetHashes: journal.targetHashes,
      issuedAt: journal.issuedAt
    });
  } catch (_error) {
    throw new Error("runner_cutover_journal_contract_invalid");
  }
  const issuedAt = Date.parse(journal.issuedAt);
  if (!allowFutureIssuedAt && issuedAt > now() + 30_000) {
    throw new Error("runner_cutover_journal_issued_in_future");
  }
  if (
    !isDeepStrictEqual(rebuilt, unsigned) ||
    (expectedKubeContext !== undefined &&
      journal.expectedKubeContext !== expectedKubeContext) ||
    (namespace !== undefined && journal.namespace.name !== namespace) ||
    (namespaceUid !== undefined && journal.namespace.uid !== namespaceUid) ||
    (manifestSha256 !== undefined && journal.manifestSha256 !== manifestSha256) ||
    (targetHashes !== undefined && !isDeepStrictEqual(journal.targetHashes, targetHashes))
  ) {
    throw new Error("runner_cutover_journal_binding_invalid");
  }
  return journal;
}

function verifyCutoverJournal(journal, verification) {
  if (!exactKeys(journal, [
    "schemaVersion",
    "mode",
    "operation",
    "operationId",
    "authorizationSha256",
    "expectedKubeContext",
    "namespace",
    "baselineDeployment",
    "manifestSha256",
    "targetHashes",
    "issuedAt",
    "hmacSha256"
  ]) || !HEX_SHA256.test(journal?.hmacSha256 || "")) {
    throw new Error("runner_cutover_journal_shape_invalid");
  }
  const unsigned = structuredClone(journal);
  delete unsigned.hmacSha256;
  const expectedHmac = Buffer.from(journalHmac(unsigned, verification.key), "hex");
  const actualHmac = Buffer.from(journal.hmacSha256, "hex");
  if (actualHmac.length !== expectedHmac.length || !crypto.timingSafeEqual(actualHmac, expectedHmac)) {
    throw new Error("runner_cutover_journal_hmac_invalid");
  }
  return verifyCutoverJournalStructure(journal, verification);
}

function cutoverJournalConfigMap(journal) {
  return {
    apiVersion: "v1",
    kind: "ConfigMap",
    metadata: {
      name: CUTOVER_JOURNAL_NAME,
      namespace: journal.namespace.name,
      finalizers: [CUTOVER_JOURNAL_FINALIZER]
    },
    immutable: true,
    data: { [CUTOVER_JOURNAL_DATA_KEY]: canonicalJson(journal) }
  };
}

function parseCanonicalCutoverJournalConfigMap(configMap, verification) {
  if (
    configMap?.apiVersion !== "v1" ||
    configMap?.kind !== "ConfigMap" ||
    configMap?.metadata?.name !== CUTOVER_JOURNAL_NAME ||
    configMap?.metadata?.namespace !== verification.namespace ||
    typeof configMap?.metadata?.uid !== "string" || !configMap.metadata.uid ||
    typeof configMap?.metadata?.resourceVersion !== "string" ||
    !configMap.metadata.resourceVersion ||
    configMap?.metadata?.generateName !== undefined ||
    configMap?.metadata?.deletionTimestamp !== undefined ||
    configMap?.metadata?.deletionGracePeriodSeconds !== undefined ||
    configMap?.metadata?.labels !== undefined ||
    configMap?.metadata?.annotations !== undefined ||
    configMap?.metadata?.ownerReferences !== undefined ||
    !isDeepStrictEqual(
      configMap?.metadata?.finalizers,
      [CUTOVER_JOURNAL_FINALIZER]
    ) ||
    configMap?.immutable !== true ||
    configMap?.binaryData !== undefined ||
    !exactKeys(configMap?.data, [CUTOVER_JOURNAL_DATA_KEY]) ||
    typeof configMap.data[CUTOVER_JOURNAL_DATA_KEY] !== "string"
  ) {
    throw new Error("runner_cutover_journal_configmap_invalid");
  }
  let journal;
  try {
    journal = JSON.parse(configMap.data[CUTOVER_JOURNAL_DATA_KEY]);
  } catch (_error) {
    throw new Error("runner_cutover_journal_json_invalid");
  }
  if (configMap.data[CUTOVER_JOURNAL_DATA_KEY] !== canonicalJson(journal)) {
    throw new Error("runner_cutover_journal_json_not_canonical");
  }
  return journal;
}

function parseStructurallyExactCutoverJournalConfigMap(configMap, verification) {
  return verifyCutoverJournalStructure(
    parseCanonicalCutoverJournalConfigMap(configMap, verification),
    verification
  );
}

function parseExactCutoverJournalConfigMap(configMap, verification) {
  return verifyCutoverJournal(
    parseCanonicalCutoverJournalConfigMap(configMap, verification),
    verification
  );
}

function liveObjectIsUnencumbered(value) {
  return typeof value?.metadata?.uid === "string" && value.metadata.uid.length > 0 &&
    typeof value?.metadata?.resourceVersion === "string" &&
    value.metadata.resourceVersion.length > 0 &&
    value?.metadata?.generateName === undefined &&
    value?.metadata?.deletionTimestamp === undefined &&
    value?.metadata?.deletionGracePeriodSeconds === undefined &&
    value?.metadata?.ownerReferences === undefined &&
    value?.metadata?.finalizers === undefined;
}

function liveResourceMatchesTarget(live, target) {
  return live?.apiVersion === target?.apiVersion &&
    live?.kind === target?.kind &&
    live?.metadata?.name === target?.metadata?.name &&
    (live?.metadata?.namespace || "") === (target?.metadata?.namespace || "") &&
    liveObjectIsUnencumbered(live) &&
    isDeepStrictEqual(live?.metadata?.labels || {}, target?.metadata?.labels || {}) &&
    isDeepStrictEqual(live?.metadata?.annotations || {}, target?.metadata?.annotations || {}) &&
    isDeepStrictEqual(live?.spec, target?.spec) &&
    (target?.data === undefined || isDeepStrictEqual(live?.data, target.data)) &&
    (target?.immutable === undefined || live?.immutable === target.immutable);
}

function liveDeploymentMatchesBaseline(deployment, journal) {
  if (journal.mode === "clean-install") return deployment === null;
  return deployment?.apiVersion === "apps/v1" &&
    deployment?.kind === "Deployment" &&
    deployment?.metadata?.name === journal.baselineDeployment.name &&
    deployment?.metadata?.namespace === journal.namespace.name &&
    deployment?.metadata?.uid === journal.baselineDeployment.uid &&
    deployment?.metadata?.resourceVersion === journal.baselineDeployment.resourceVersion;
}

function liveDeploymentMatchesNormalizedTarget(live, normalizedTarget) {
  if (
    live?.apiVersion !== "apps/v1" ||
    live?.kind !== "Deployment" ||
    live?.metadata?.name !== normalizedTarget?.metadata?.name ||
    live?.metadata?.namespace !== normalizedTarget?.metadata?.namespace ||
    typeof live?.metadata?.uid !== "string" || !live.metadata.uid ||
    typeof live?.metadata?.resourceVersion !== "string" || !live.metadata.resourceVersion ||
    live?.metadata?.generateName !== undefined ||
    live?.metadata?.deletionTimestamp !== undefined ||
    live?.metadata?.deletionGracePeriodSeconds !== undefined ||
    live?.metadata?.ownerReferences !== undefined ||
    live?.metadata?.finalizers !== undefined ||
    !isDeepStrictEqual(live?.metadata?.labels || {}, normalizedTarget?.metadata?.labels || {}) ||
    !isDeepStrictEqual(live?.spec, normalizedTarget?.spec)
  ) {
    return false;
  }
  const annotations = { ...(live.metadata.annotations || {}) };
  const revision = annotations["deployment.kubernetes.io/revision"];
  delete annotations["deployment.kubernetes.io/revision"];
  return (revision === undefined || /^[1-9][0-9]*$/.test(revision)) &&
    isDeepStrictEqual(annotations, normalizedTarget?.metadata?.annotations || {});
}

function liveDeploymentMatchesJournalTarget(live, normalizedTarget, journal) {
  return liveDeploymentMatchesNormalizedTarget(live, normalizedTarget) &&
    (journal.mode === "clean-install" ||
      live.metadata.uid === journal.baselineDeployment.uid);
}

function classifyCutoverJournalPrefix({
  journal,
  journalPolicy,
  journalBinding,
  policy,
  binding,
  deployment,
  targetJournalPolicy,
  targetJournalBinding,
  targetPolicy,
  targetBinding,
  targetDeployment,
  parentFenceObserved = false,
  journalFenceObserved = false,
  parentQuiesced = false
}) {
  const baseline = liveDeploymentMatchesBaseline(deployment, journal);
  const target = liveDeploymentMatchesJournalTarget(deployment, targetDeployment, journal);
  if (journalPolicy === null) {
    if (
      journalBinding !== null || policy !== null || binding !== null || !baseline
    ) {
      throw new Error("runner_cutover_journal_non_prefix_state");
    }
    return "P0";
  }
  if (!liveResourceMatchesTarget(journalPolicy, targetJournalPolicy)) {
    throw new Error("runner_cutover_journal_guard_policy_not_exact");
  }
  if (journalBinding === null) {
    if (policy !== null || binding !== null || !baseline) {
      throw new Error("runner_cutover_journal_non_prefix_state");
    }
    return "P1";
  }
  if (!liveResourceMatchesTarget(journalBinding, targetJournalBinding)) {
    throw new Error("runner_cutover_journal_guard_binding_not_exact");
  }
  if (policy === null) {
    if (binding !== null || !baseline) throw new Error("runner_cutover_journal_non_prefix_state");
    return "P2";
  }
  if (!liveResourceMatchesTarget(policy, targetPolicy)) {
    throw new Error("runner_cutover_journal_policy_not_exact");
  }
  if (binding === null) {
    if (!baseline) throw new Error("runner_cutover_journal_non_prefix_state");
    return "P3";
  }
  if (!liveResourceMatchesTarget(binding, targetBinding)) {
    throw new Error("runner_cutover_journal_binding_not_exact");
  }
  if (baseline) return "P4";
  if (!target) throw new Error("runner_cutover_journal_deployment_not_exact");
  return journalFenceObserved && parentFenceObserved && parentQuiesced ? "P6" : "P5";
}

async function createOrResolveExact({ label, create, read, isExact }) {
  let mutationError = null;
  try {
    await create();
  } catch (error) {
    mutationError = error;
  }
  const observed = await read();
  if (isExact(observed)) return observed;
  if (mutationError) {
    throw new Error(`${label}_create_failed_and_live_state_not_exact:${mutationError.message}`);
  }
  throw new Error(`${label}_create_acknowledged_but_live_state_not_exact`);
}

async function replaceOrResolveExact({ label, replace, read, isExact }) {
  let mutationError = null;
  try {
    await replace();
  } catch (error) {
    mutationError = error;
  }
  const observed = await read();
  if (isExact(observed)) return observed;
  if (mutationError) {
    throw new Error(`${label}_replace_failed_and_live_state_not_exact:${mutationError.message}`);
  }
  throw new Error(`${label}_replace_acknowledged_but_live_state_not_exact`);
}

async function advanceCutoverJournalTransition({
  journal,
  journalConfigMap,
  targetJournalPolicy,
  targetJournalBinding,
  targetPolicy,
  targetBinding,
  targetDeployment,
  deploymentMutationResource = targetDeployment,
  readState,
  isJournalExact,
  createJournal,
  createJournalPolicy,
  waitJournalPolicyObserved,
  createJournalBinding,
  waitJournalFenceObserved,
  createPolicy,
  waitPolicyObserved,
  createBinding,
  waitParentFenceObserved,
  writeDeployment,
  waitParentQuiesced,
  validateState = async () => {},
  afterStep = async () => {}
}) {
  for (let attempt = 0; attempt < 8; attempt += 1) {
    let state = await readState();
    await validateState(state);
    if (state.journalConfigMap === null) {
      const beforeJournal = classifyCutoverJournalPrefix({
        journal,
        ...state,
        targetJournalPolicy,
        targetJournalBinding,
        targetPolicy,
        targetBinding,
        targetDeployment
      });
      if (beforeJournal !== "P0") {
        throw new Error("runner_cutover_journal_missing_after_staged_mutation");
      }
      await createOrResolveExact({
        label: "runner_cutover_journal",
        create: () => createJournal(journalConfigMap),
        read: async () => (await readState()).journalConfigMap,
        isExact: isJournalExact
      });
      await afterStep("journal");
      continue;
    }
    if (!isJournalExact(state.journalConfigMap)) {
      throw new Error("runner_cutover_journal_live_object_not_exact");
    }
    const prefix = classifyCutoverJournalPrefix({
      journal,
      ...state,
      targetJournalPolicy,
      targetJournalBinding,
      targetPolicy,
      targetBinding,
      targetDeployment
    });
    if (prefix === "P0") {
      await createOrResolveExact({
        label: "runner_cutover_journal_guard_policy",
        create: () => createJournalPolicy(targetJournalPolicy),
        read: async () => (await readState()).journalPolicy,
        isExact: value => liveResourceMatchesTarget(value, targetJournalPolicy)
      });
      await afterStep("journal-policy");
      continue;
    }
    if (prefix === "P1") {
      await waitJournalPolicyObserved();
      state = await readState();
      await validateState(state);
      if (
        !isJournalExact(state.journalConfigMap) ||
        classifyCutoverJournalPrefix({
          journal,
          ...state,
          targetJournalPolicy,
          targetJournalBinding,
          targetPolicy,
          targetBinding,
          targetDeployment
        }) !== "P1"
      ) {
        throw new Error("runner_cutover_journal_guard_policy_changed_before_binding");
      }
      await createOrResolveExact({
        label: "runner_cutover_journal_guard_binding",
        create: () => createJournalBinding(targetJournalBinding),
        read: async () => (await readState()).journalBinding,
        isExact: value => liveResourceMatchesTarget(value, targetJournalBinding)
      });
      await afterStep("journal-binding");
      continue;
    }
    if (prefix === "P2") {
      await waitJournalFenceObserved();
      state = await readState();
      await validateState(state);
      if (
        !isJournalExact(state.journalConfigMap) ||
        state.journalFenceObserved !== true ||
        classifyCutoverJournalPrefix({
          journal,
          ...state,
          targetJournalPolicy,
          targetJournalBinding,
          targetPolicy,
          targetBinding,
          targetDeployment
        }) !== "P2"
      ) {
        throw new Error("runner_cutover_journal_guard_changed_before_parent_policy");
      }
      await createOrResolveExact({
        label: "runner_cutover_parent_policy",
        create: () => createPolicy(targetPolicy),
        read: async () => (await readState()).policy,
        isExact: value => liveResourceMatchesTarget(value, targetPolicy)
      });
      await afterStep("parent-policy");
      continue;
    }
    if (prefix === "P3") {
      await waitJournalFenceObserved();
      await waitPolicyObserved();
      state = await readState();
      await validateState(state);
      if (
        !isJournalExact(state.journalConfigMap) ||
        state.journalFenceObserved !== true ||
        classifyCutoverJournalPrefix({
          journal,
          ...state,
          targetJournalPolicy,
          targetJournalBinding,
          targetPolicy,
          targetBinding,
          targetDeployment
        }) !== "P3"
      ) {
        throw new Error("runner_cutover_parent_policy_changed_before_binding");
      }
      await createOrResolveExact({
        label: "runner_cutover_parent_binding",
        create: () => createBinding(targetBinding),
        read: async () => (await readState()).binding,
        isExact: value => liveResourceMatchesTarget(value, targetBinding)
      });
      await afterStep("parent-binding");
      continue;
    }
    if (prefix === "P4") {
      await waitJournalFenceObserved();
      await waitParentFenceObserved();
      state = await readState();
      await validateState(state);
      if (
        !isJournalExact(state.journalConfigMap) ||
        state.journalFenceObserved !== true ||
        state.parentFenceObserved !== true ||
        classifyCutoverJournalPrefix({
          journal,
          ...state,
          targetJournalPolicy,
          targetJournalBinding,
          targetPolicy,
          targetBinding,
          targetDeployment
        }) !== "P4"
      ) {
        throw new Error("runner_cutover_parent_fence_changed_before_deployment");
      }
      await replaceOrResolveExact({
        label: "runner_cutover_parent_deployment",
        replace: () => writeDeployment(deploymentMutationResource, journal.baselineDeployment),
        read: async () => (await readState()).deployment,
        isExact: value => liveDeploymentMatchesJournalTarget(value, targetDeployment, journal)
      });
      await afterStep("deployment");
      continue;
    }
    if (prefix === "P5") {
      await waitJournalFenceObserved();
      await waitParentFenceObserved();
      await waitParentQuiesced();
      state = await readState();
      await validateState(state);
      if (
        !isJournalExact(state.journalConfigMap) ||
        classifyCutoverJournalPrefix({
          journal,
          ...state,
          targetJournalPolicy,
          targetJournalBinding,
          targetPolicy,
          targetBinding,
          targetDeployment
        }) !== "P6"
      ) {
        throw new Error("runner_cutover_parent_transition_not_observed");
      }
      return "P6";
    }
    if (prefix === "P6") return "P6";
  }
  throw new Error("runner_cutover_journal_transition_did_not_converge");
}

module.exports = {
  CUTOVER_JOURNAL_DATA_KEY,
  CUTOVER_JOURNAL_DOMAIN,
  CUTOVER_JOURNAL_FINALIZER,
  CUTOVER_JOURNAL_NAME,
  CUTOVER_JOURNAL_OPERATION,
  CUTOVER_JOURNAL_SCHEMA_VERSION,
  advanceCutoverJournalTransition,
  canonicalJson,
  classifyCutoverJournalPrefix,
  createCutoverJournal,
  createOrResolveExact,
  cutoverJournalConfigMap,
  journalHmac,
  liveDeploymentMatchesBaseline,
  liveDeploymentMatchesJournalTarget,
  liveDeploymentMatchesNormalizedTarget,
  liveObjectIsUnencumbered,
  liveResourceMatchesTarget,
  parseExactCutoverJournalConfigMap,
  parseStructurallyExactCutoverJournalConfigMap,
  replaceOrResolveExact,
  sha256Canonical,
  verifyCutoverJournal,
  verifyCutoverJournalStructure
};
