import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn, spawnSync } from "node:child_process";

import YAML from "yaml";

const POLICY_NAME = "recovery-operation-pod-fence.yenhubs.org";
const RUNNER_NAMESPACE = "hcce-bot-runners";
const PROBE_LABEL = "yenhubs.org/recovery-operation-fence-probe";
const TEST_USER = "yenhubs-recovery-fence-e2e";
const WRITER_DENIAL =
  "recovery operation Pod fence denies database-writer Pod creation while checkpoint or restore is fenced";
const RUNNER_DENIAL =
  "recovery operation Pod fence denies runner Pod mutation while checkpoint or restore is fenced";
const COMMAND_TIMEOUT_MS = 20_000;
const PROPAGATION_TIMEOUT_MS = 60_000;
const CONCURRENT_BURST_SIZE = 12;

const [manifestPath, parentNamespace, mode] = process.argv.slice(2);
if (!manifestPath || !parentNamespace) {
  throw new Error(
    "usage: node apply/recovery-operation-pod-fence.kind-e2e.mjs <generated-manifest> <parent-namespace> [--static-only]"
  );
}
if (mode !== undefined && mode !== "--static-only") {
  throw new Error(`unsupported mode: ${mode}`);
}
const staticOnly = mode === "--static-only";
if (!/^[a-z0-9]([-a-z0-9]*[a-z0-9])?$/.test(parentNamespace)) {
  throw new Error("parent namespace is not a valid DNS label");
}
if (parentNamespace === RUNNER_NAMESPACE) {
  throw new Error("parent and runner namespaces must be distinct");
}

process.umask(0o077);
const workDirectory = fs.mkdtempSync(path.join(os.tmpdir(), "yenhubs-recovery-fence-e2e-"));
fs.chmodSync(workDirectory, 0o700);

function cleanDiagnostic(value) {
  return String(value || "")
    .replaceAll(workDirectory, "<tmp>")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 1_500);
}

function runKubectl(args, { input, allowFailure = false, timeoutMs = COMMAND_TIMEOUT_MS } = {}) {
  const result = spawnSync("kubectl", args, {
    input,
    encoding: "utf8",
    timeout: timeoutMs,
    maxBuffer: 4 * 1024 * 1024
  });
  if (result.error) {
    throw new Error(`kubectl process failed: ${cleanDiagnostic(result.error.message)}`);
  }
  if (!allowFailure && result.status !== 0) {
    throw new Error(
      `kubectl ${args[0] || "command"} failed with status ${result.status}: ` +
      cleanDiagnostic(`${result.stdout || ""}\n${result.stderr || ""}`)
    );
  }
  return result;
}

function runKubectlAsync(args, { input, timeoutMs = COMMAND_TIMEOUT_MS } = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn("kubectl", args, { stdio: ["pipe", "pipe", "pipe"] });
    let stdout = "";
    let stderr = "";
    let settled = false;
    const finish = callback => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      callback();
    };
    const append = (current, chunk) => {
      const next = current + chunk.toString("utf8");
      if (Buffer.byteLength(next) > 4 * 1024 * 1024) {
        child.kill("SIGKILL");
        finish(() => reject(new Error("kubectl output exceeded the bounded buffer")));
      }
      return next;
    };
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      finish(() => reject(new Error("kubectl concurrent dry-run timed out")));
    }, timeoutMs);
    child.stdout.on("data", chunk => { stdout = append(stdout, chunk); });
    child.stderr.on("data", chunk => { stderr = append(stderr, chunk); });
    child.on("error", error => {
      finish(() => reject(new Error(`kubectl process failed: ${cleanDiagnostic(error.message)}`)));
    });
    child.on("close", status => {
      finish(() => resolve({ status, stdout, stderr }));
    });
    child.stdin.end(input);
  });
}

function kubectlJson(args, options) {
  const result = runKubectl(args, options);
  try {
    return JSON.parse(result.stdout);
  } catch (error) {
    throw new Error(`kubectl returned invalid JSON: ${cleanDiagnostic(error.message)}`);
  }
}

function documentInput(document) {
  return `${JSON.stringify(document)}\n`;
}

function activeNamespaceSelector() {
  return {
    matchExpressions: [{
      key: "kubernetes.io/metadata.name",
      operator: "In",
      values: [parentNamespace, RUNNER_NAMESPACE]
    }]
  };
}

function dormantNamespaceSelector() {
  return {
    matchExpressions: [{
      key: "kubernetes.io/metadata.name",
      operator: "DoesNotExist"
    }]
  };
}

function bindingSpec(active) {
  return {
    policyName: POLICY_NAME,
    validationActions: ["Deny"],
    matchResources: {
      matchPolicy: "Equivalent",
      namespaceSelector: active ? activeNamespaceSelector() : dormantNamespaceSelector(),
      objectSelector: {}
    }
  };
}

function assertExactBinding(binding, active) {
  assert.equal(binding?.apiVersion, "admissionregistration.k8s.io/v1");
  assert.equal(binding?.kind, "ValidatingAdmissionPolicyBinding");
  assert.equal(binding?.metadata?.name, POLICY_NAME);
  assert.ok(typeof binding?.metadata?.uid === "string" && binding.metadata.uid.length > 0);
  assert.ok(
    typeof binding?.metadata?.resourceVersion === "string" &&
    binding.metadata.resourceVersion.length > 0
  );
  assert.deepEqual(binding.spec, bindingSpec(active));
}

function minimalBindingUpdate(snapshot, active, { uid = snapshot?.metadata?.uid } = {}) {
  return {
    apiVersion: "admissionregistration.k8s.io/v1",
    kind: "ValidatingAdmissionPolicyBinding",
    metadata: {
      name: POLICY_NAME,
      uid,
      resourceVersion: snapshot?.metadata?.resourceVersion
    },
    spec: bindingSpec(active)
  };
}

function getBinding() {
  return kubectlJson([
    "get",
    "validatingadmissionpolicybinding",
    POLICY_NAME,
    "-o",
    "json"
  ]);
}

function replaceBinding(snapshot, active, options) {
  return kubectlJson(
    ["replace", "-f", "-", "-o", "json"],
    { input: documentInput(minimalBindingUpdate(snapshot, active, options)) }
  );
}

function assertFailedReplace(document, label, expectedFailurePattern) {
  const result = runKubectl(
    ["replace", "-f", "-", "-o", "json"],
    { input: documentInput(document), allowFailure: true }
  );
  assert.notEqual(result.status, 0, `${label} unexpectedly replaced the binding`);
  assert.match(
    diagnostic(result),
    expectedFailurePattern,
    `${label} did not fail with a Kubernetes identity/version precondition`
  );
}

function recursivelyRejectKeys(value, forbiddenKeys, location = "resource") {
  if (Array.isArray(value)) {
    value.forEach((entry, index) => recursivelyRejectKeys(entry, forbiddenKeys, `${location}[${index}]`));
    return;
  }
  if (!value || typeof value !== "object") return;
  for (const [key, entry] of Object.entries(value)) {
    assert.ok(!forbiddenKeys.has(key), `${location}.${key} is forbidden in the parameter-free fence`);
    recursivelyRejectKeys(entry, forbiddenKeys, `${location}.${key}`);
  }
}

function parseFencePair() {
  const source = fs.readFileSync(manifestPath, "utf8");
  const documents = YAML.parseAllDocuments(source);
  for (const document of documents) {
    if (document.errors.length > 0) {
      throw new Error("generated manifest YAML is invalid");
    }
  }
  const resources = documents
    .map(document => document.toJSON())
    .filter(resource => resource && typeof resource === "object");
  const namedResources = resources.filter(resource => resource?.metadata?.name === POLICY_NAME);
  assert.deepEqual(
    namedResources.map(resource => resource.kind).sort(),
    ["ValidatingAdmissionPolicy", "ValidatingAdmissionPolicyBinding"].sort(),
    "the generated manifest must contain only the exact policy and binding under the fence name"
  );

  const policy = namedResources.find(resource => resource.kind === "ValidatingAdmissionPolicy");
  const binding = namedResources.find(resource => resource.kind === "ValidatingAdmissionPolicyBinding");
  assert.equal(policy?.apiVersion, "admissionregistration.k8s.io/v1");
  assert.equal(policy?.spec?.failurePolicy, "Fail");
  assert.deepEqual(policy?.spec?.matchConstraints?.namespaceSelector, activeNamespaceSelector());
  assert.deepEqual(binding?.spec, bindingSpec(false));

  const policyMessages = (policy?.spec?.validations || []).map(validation => validation?.message);
  assert.deepEqual(policyMessages, [WRITER_DENIAL, RUNNER_DENIAL]);
  recursivelyRejectKeys(
    [policy, binding],
    new Set(["paramKind", "paramRef", "parameterNotFoundAction", "params"])
  );
  assert.ok(
    namedResources.every(resource => ![
      "Role",
      "RoleBinding",
      "ClusterRole",
      "ClusterRoleBinding",
      "Secret"
    ].includes(resource.kind)),
    "the fence pair must not introduce RBAC or Secret resources"
  );
  return { policy, binding };
}

function createResource(document) {
  runKubectl(["create", "-f", "-"], { input: documentInput(document) });
}

function namespaceDocument(name) {
  return {
    apiVersion: "v1",
    kind: "Namespace",
    metadata: { name }
  };
}

function probePod(namespace, namePrefix, labels, containerName = "probe") {
  return {
    apiVersion: "v1",
    kind: "Pod",
    metadata: {
      generateName: namePrefix,
      namespace,
      labels: { ...labels, [PROBE_LABEL]: "true" }
    },
    spec: {
      automountServiceAccountToken: false,
      enableServiceLinks: false,
      restartPolicy: "Never",
      terminationGracePeriodSeconds: 0,
      containers: [{
        name: containerName,
        image: "registry.k8s.io/pause:3.10",
        imagePullPolicy: "IfNotPresent"
      }]
    }
  };
}

const parentWriterProbe = probePod(
  parentNamespace,
  "yenhubs-recovery-writer-probe-",
  { app: "reticulum" },
  "reticulum"
);
const parentSafeProbe = probePod(
  parentNamespace,
  "yenhubs-recovery-safe-probe-",
  { app: "fence-e2e-safe" }
);
const runnerProbe = probePod(
  RUNNER_NAMESPACE,
  "yenhubs-recovery-runner-probe-",
  { app: "fence-e2e-runner" }
);

function podDryRun(pod, { asUser } = {}) {
  const args = [];
  if (asUser) args.push(`--as=${asUser}`);
  args.push("create", "--dry-run=server", "-f", "-", "-o", "name");
  return runKubectl(args, { input: documentInput(pod), allowFailure: true });
}

function podDryRunAsync(pod, { asUser } = {}) {
  const args = [];
  if (asUser) args.push(`--as=${asUser}`);
  args.push("create", "--dry-run=server", "-f", "-", "-o", "name");
  return runKubectlAsync(args, { input: documentInput(pod) });
}

function concurrentDryRunBurst(pod, { asUser, count = CONCURRENT_BURST_SIZE } = {}) {
  let release;
  let markAllStarted;
  let startedCount = 0;
  const gate = new Promise(resolve => { release = resolve; });
  const allStarted = new Promise(resolve => { markAllStarted = resolve; });
  const completion = Promise.all(Array.from({ length: count }, async () => {
    await gate;
    const request = podDryRunAsync(pod, { asUser });
    startedCount += 1;
    if (startedCount === count) markAllStarted();
    return request;
  }));
  return { release, allStarted, completion };
}

function diagnostic(result) {
  return `${result.stdout || ""}\n${result.stderr || ""}`;
}

function assertAllowed(result, label) {
  assert.equal(
    result.status,
    0,
    `${label} was unexpectedly rejected: ${cleanDiagnostic(diagnostic(result))}`
  );
  assert.ok(!diagnostic(result).includes(POLICY_NAME), `${label} mentioned the dormant policy`);
}

function exactDenial(result, message) {
  const output = diagnostic(result);
  return result.status !== 0 && output.includes(POLICY_NAME) && output.includes(message);
}

async function waitFor(label, probe, timeoutMs = PROPAGATION_TIMEOUT_MS) {
  const deadline = Date.now() + timeoutMs;
  let lastDetail = "condition was not observed";
  while (Date.now() < deadline) {
    const outcome = await probe();
    if (outcome?.done) return outcome.value;
    if (outcome?.fatal) throw new Error(`${label}: ${outcome.fatal}`);
    if (outcome?.detail) lastDetail = outcome.detail;
    await new Promise(resolve => setTimeout(resolve, 1_000));
  }
  throw new Error(`${label} timed out: ${cleanDiagnostic(lastDetail)}`);
}

async function waitForPolicyCompilation() {
  return waitFor("ValidatingAdmissionPolicy CEL compilation", () => {
    const policy = kubectlJson([
      "get",
      "validatingadmissionpolicy",
      POLICY_NAME,
      "-o",
      "json"
    ]);
    const generation = policy?.metadata?.generation;
    const observedGeneration = policy?.status?.observedGeneration;
    if (!Number.isInteger(generation) || generation < 1) {
      return { fatal: "metadata.generation is not a positive integer" };
    }
    if (observedGeneration !== generation) {
      return {
        done: false,
        detail: `observedGeneration=${observedGeneration ?? "missing"}, generation=${generation ?? "missing"}`
      };
    }
    const typeChecking = policy?.status?.typeChecking;
    if (typeChecking === undefined) {
      return { done: false, detail: "status.typeChecking is not present yet" };
    }
    if (!typeChecking || typeof typeChecking !== "object" || Array.isArray(typeChecking)) {
      return { fatal: "status.typeChecking has an invalid shape" };
    }
    const warnings = typeChecking.expressionWarnings;
    if (warnings !== undefined && (!Array.isArray(warnings) || warnings.length > 0)) {
      return {
        fatal: `CEL type checking reported ${Array.isArray(warnings) ? warnings.length : "invalid"} warning(s)`
      };
    }
    const conditions = policy?.status?.conditions;
    if (conditions !== undefined && !Array.isArray(conditions)) {
      return { fatal: "status.conditions has an invalid shape" };
    }
    const nonTrueConditions = (conditions || []).filter(condition => condition?.status !== "True");
    if (nonTrueConditions.length > 0) {
      const conditionTypes = nonTrueConditions.map(condition => condition?.type || "unknown").join(",");
      return { fatal: `policy condition(s) are not True: ${conditionTypes}` };
    }
    return { done: true, value: policy };
  });
}

async function waitForExactDenial(pod, message, { asUser, label }) {
  return waitFor(label, () => {
    const result = podDryRun(pod, { asUser });
    if (exactDenial(result, message)) return { done: true };
    if (result.status !== 0) {
      return { fatal: `unexpected rejection: ${cleanDiagnostic(diagnostic(result))}` };
    }
    return { done: false, detail: "the dry-run was still admitted" };
  });
}

async function waitForDormantAdmission() {
  return waitFor("dormant binding propagation", () => {
    const parentResult = podDryRun(parentWriterProbe, { asUser: TEST_USER });
    const runnerResult = podDryRun(runnerProbe);
    for (const [label, result] of [["parent", parentResult], ["runner", runnerResult]]) {
      if (result.status !== 0 && !diagnostic(result).includes(POLICY_NAME)) {
        return { fatal: `${label} probe failed unexpectedly: ${cleanDiagnostic(diagnostic(result))}` };
      }
    }
    if (parentResult.status === 0 && runnerResult.status === 0) return { done: true };
    return { done: false, detail: "the active fence was still propagating" };
  });
}

function createTestUserRbac() {
  createResource({
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "Role",
    metadata: { name: "yenhubs-fence-e2e-pod-creator", namespace: parentNamespace },
    rules: [{ apiGroups: [""], resources: ["pods"], verbs: ["create"] }]
  });
  createResource({
    apiVersion: "rbac.authorization.k8s.io/v1",
    kind: "RoleBinding",
    metadata: { name: "yenhubs-fence-e2e-pod-creator", namespace: parentNamespace },
    roleRef: {
      apiGroup: "rbac.authorization.k8s.io",
      kind: "Role",
      name: "yenhubs-fence-e2e-pod-creator"
    },
    subjects: [{
      apiGroup: "rbac.authorization.k8s.io",
      kind: "User",
      name: TEST_USER
    }]
  });
}

async function waitForTestUserRbac() {
  await waitFor("test user Pod CREATE authorization", () => {
    const result = runKubectl(
      ["--as", TEST_USER, "auth", "can-i", "create", "pods", "--namespace", parentNamespace],
      { allowFailure: true }
    );
    return result.status === 0 && result.stdout.trim() === "yes"
      ? { done: true }
      : { done: false, detail: diagnostic(result) };
  });
  const configMapAccess = runKubectl(
    ["--as", TEST_USER, "auth", "can-i", "get", "configmaps", "--namespace", parentNamespace],
    { allowFailure: true }
  );
  assert.notEqual(configMapAccess.status, 0, "test user unexpectedly has ConfigMap GET access");
  assert.equal(configMapAccess.stdout.trim(), "no");
}

function assertNoPersistedProbePods() {
  const pods = kubectlJson([
    "get",
    "pods",
    "--all-namespaces",
    "--selector",
    `${PROBE_LABEL}=true`,
    "-o",
    "json"
  ]);
  assert.deepEqual(pods?.items, [], "server dry-run probes must never persist Pods");
}

async function main() {
  const { policy, binding } = parseFencePair();
  if (staticOnly) {
    console.log("recovery operation Pod fence static E2E contract validation passed");
    return;
  }

  const version = kubectlJson(["version", "-o", "json"]);
  assert.equal(version?.clientVersion?.gitVersion, "v1.34.8");
  assert.equal(version?.serverVersion?.gitVersion, "v1.34.8");

  createResource(namespaceDocument(parentNamespace));
  createResource(namespaceDocument(RUNNER_NAMESPACE));
  createTestUserRbac();
  await waitForTestUserRbac();

  runKubectl(["create", "-f", "-"], {
    input: `${YAML.stringify(policy)}---\n${YAML.stringify(binding)}`
  });
  await waitForPolicyCompilation();

  const initialBinding = getBinding();
  assertExactBinding(initialBinding, false);
  assertAllowed(podDryRun(parentWriterProbe, { asUser: TEST_USER }), "dormant parent writer probe");
  assertAllowed(podDryRun(runnerProbe), "dormant runner probe");

  const activationRace = concurrentDryRunBurst(parentWriterProbe, { asUser: TEST_USER });
  activationRace.release();
  await activationRace.allStarted;
  const activeBinding = replaceBinding(initialBinding, true);
  assertExactBinding(activeBinding, true);
  assert.equal(activeBinding.metadata.uid, initialBinding.metadata.uid);
  assert.notEqual(activeBinding.metadata.resourceVersion, initialBinding.metadata.resourceVersion);

  const activationRaceResults = await activationRace.completion;
  for (const result of activationRaceResults) {
    assert.ok(
      result.status === 0 || exactDenial(result, WRITER_DENIAL),
      `concurrent activation dry-run failed unexpectedly: ${cleanDiagnostic(diagnostic(result))}`
    );
  }

  await waitForExactDenial(parentWriterProbe, WRITER_DENIAL, {
    asUser: TEST_USER,
    label: "parameter-free parent writer denial"
  });
  await waitForExactDenial(runnerProbe, RUNNER_DENIAL, {
    label: "runner namespace denial"
  });
  assertAllowed(
    podDryRun(parentSafeProbe, { asUser: TEST_USER }),
    "active non-writer parent probe"
  );

  const confirmedActiveBinding = getBinding();
  assertExactBinding(confirmedActiveBinding, true);
  assert.equal(confirmedActiveBinding.metadata.uid, activeBinding.metadata.uid);
  assert.equal(confirmedActiveBinding.metadata.resourceVersion, activeBinding.metadata.resourceVersion);
  const postActivationBurst = concurrentDryRunBurst(parentWriterProbe, { asUser: TEST_USER });
  postActivationBurst.release();
  await postActivationBurst.allStarted;
  const postActivationResults = await postActivationBurst.completion;
  assert.ok(
    postActivationResults.every(result => exactDenial(result, WRITER_DENIAL)),
    "a parent writer dry-run was admitted after active binding propagation was confirmed"
  );

  assertFailedReplace(
    minimalBindingUpdate(initialBinding, false),
    "stale resourceVersion CAS",
    /Conflict|object has been modified/i
  );
  const afterStaleAttempt = getBinding();
  assertExactBinding(afterStaleAttempt, true);
  assert.equal(afterStaleAttempt.metadata.uid, activeBinding.metadata.uid);
  assert.equal(afterStaleAttempt.metadata.resourceVersion, activeBinding.metadata.resourceVersion);

  runKubectl([
    "delete",
    "validatingadmissionpolicybinding",
    POLICY_NAME,
    "--wait=true",
    "--timeout=30s"
  ]);
  const recreatedActiveBinding = kubectlJson(
    ["create", "-f", "-", "-o", "json"],
    {
      input: documentInput({
        apiVersion: "admissionregistration.k8s.io/v1",
        kind: "ValidatingAdmissionPolicyBinding",
        metadata: { name: POLICY_NAME },
        spec: bindingSpec(true)
      })
    }
  );
  assertExactBinding(recreatedActiveBinding, true);
  assert.notEqual(
    recreatedActiveBinding.metadata.uid,
    activeBinding.metadata.uid,
    "delete/recreate must change binding UID"
  );

  assertFailedReplace(
    minimalBindingUpdate(recreatedActiveBinding, false, { uid: activeBinding.metadata.uid }),
    "old UID with current resourceVersion",
    /uid|precondition|immutable/i
  );
  const afterUidAttempt = getBinding();
  assertExactBinding(afterUidAttempt, true);
  assert.equal(afterUidAttempt.metadata.uid, recreatedActiveBinding.metadata.uid);
  assert.equal(afterUidAttempt.metadata.resourceVersion, recreatedActiveBinding.metadata.resourceVersion);

  assertFailedReplace(
    minimalBindingUpdate(activeBinding, false),
    "ABA snapshot",
    /Conflict|object has been modified|uid|precondition|immutable/i
  );
  const afterAbaAttempt = getBinding();
  assertExactBinding(afterAbaAttempt, true);
  assert.equal(afterAbaAttempt.metadata.uid, recreatedActiveBinding.metadata.uid);
  assert.equal(afterAbaAttempt.metadata.resourceVersion, recreatedActiveBinding.metadata.resourceVersion);

  await waitForExactDenial(runnerProbe, RUNNER_DENIAL, {
    label: "recreated active binding propagation"
  });

  const dormantBinding = replaceBinding(afterAbaAttempt, false);
  assertExactBinding(dormantBinding, false);
  assert.equal(dormantBinding.metadata.uid, recreatedActiveBinding.metadata.uid);
  assert.notEqual(
    dormantBinding.metadata.resourceVersion,
    recreatedActiveBinding.metadata.resourceVersion
  );
  await waitForDormantAdmission();

  const finalBinding = getBinding();
  assertExactBinding(finalBinding, false);
  assert.equal(finalBinding.metadata.uid, dormantBinding.metadata.uid);
  assert.equal(finalBinding.metadata.resourceVersion, dormantBinding.metadata.resourceVersion);
  assertNoPersistedProbePods();

  console.log(
    "recovery operation Pod fence Kind E2E passed: CEL observed, parameter-free denial, CAS/ABA, and dormant return"
  );
}

try {
  await main();
} finally {
  fs.rmSync(workDirectory, { recursive: true, force: true });
}
