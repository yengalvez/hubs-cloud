const assert = require("node:assert/strict");
const crypto = require("node:crypto");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { spawnSync } = require("node:child_process");
const test = require("node:test");
const YAML = require("yaml");
const { readActivationPlan, readActivationPlanText } = require("./runner-activation");
const {
  verifyActivePlanAndConfig,
  verifyLegacyAbsentPlanAndConfig,
  verifyManifestAgainstInputValues
} = require("./manifest-input-contract");

const communityEditionDir = path.resolve(__dirname, "..");
const generatorPath = path.resolve(communityEditionDir, "generate_script/index.js");
const generatedManifestVerifierPath = path.resolve(
  communityEditionDir,
  "generate_script/verify-generated-manifest.js"
);
const legacyProfile = "cold-rebind-legacy-absent-v1";
const ciInput = YAML.parse(fs.readFileSync(
  path.resolve(communityEditionDir, "input-values.ci.yaml"),
  "utf8"
));
const { privateKey: stableTestPermsKey } = crypto.generateKeyPairSync("rsa", {
  modulusLength: 2048,
  privateKeyEncoding: { type: "pkcs8", format: "pem" },
  publicKeyEncoding: { type: "spki", format: "pem" }
});

function generatedFixture(t, overrides = {}, environment = {}) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "hcce-live-contract-test-"));
  fs.chmodSync(directory, 0o700);
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const inputPath = path.join(directory, "input-values.yaml");
  const manifestPath = path.join(directory, "hcce.yaml");
  const input = {
    ...ciInput,
    PERMS_KEY: stableTestPermsKey,
    ...overrides
  };
  fs.writeFileSync(inputPath, YAML.stringify(input), { mode: 0o600 });
  const generated = spawnSync(process.execPath, [generatorPath], {
    cwd: communityEditionDir,
    env: {
      ...process.env,
      ...environment,
      HCCE_INPUT_VALUES_PATH: inputPath,
      HCCE_OUTPUT_PATH: manifestPath
    },
    encoding: "utf8"
  });
  assert.equal(generated.status, 0, generated.stderr);
  return { input, inputPath, manifestPath };
}

function legacyInputOverrides(overrides = {}) {
  return {
    OVERRIDE_BOT_RUNNER_IMAGE: "No",
    ...overrides
  };
}

test("standalone live verifier accepts only the exact reproducible active manifest", t => {
  const fixture = generatedFixture(t);
  const result = verifyManifestAgainstInputValues(fixture.inputPath, fixture.manifestPath);
  assert.equal(result.plan.activationPhase, "active");
  assert.equal(result.plan.recoveryPhase, "active");
  assert.equal(result.plan.recoveryEpoch, fixture.input.BOT_RUNNER_RECOVERY_EPOCH);
});

test("standalone live verifier accepts only the exact reproducible legacy-absent cold target", t => {
  const environment = { HCCE_TARGET_PROFILE: legacyProfile };
  const fixture = generatedFixture(t, legacyInputOverrides(), environment);
  const result = verifyManifestAgainstInputValues(
    fixture.inputPath,
    fixture.manifestPath,
    environment
  );
  assert.equal(result.targetProfile, legacyProfile);
  assert.equal(result.plan.activationPhase, "legacy-absent");
  assert.equal(result.plan.recoveryPhase, "legacy-absent");
  assert.equal(result.plan.recoveryEpoch, "legacy-absent");
});

test("standalone live verifier keeps durable and legacy target profiles disjoint", async t => {
  await t.test("durable manifest under legacy profile", child => {
    const fixture = generatedFixture(child);
    assert.throws(
      () => verifyManifestAgainstInputValues(
        fixture.inputPath,
        fixture.manifestPath,
        { HCCE_TARGET_PROFILE: legacyProfile }
      ),
      /canonical_manifest_generation_failed/
    );
  });
  await t.test("legacy manifest without target profile", child => {
    const environment = { HCCE_TARGET_PROFILE: legacyProfile };
    const fixture = generatedFixture(child, legacyInputOverrides(), environment);
    assert.throws(
      () => verifyManifestAgainstInputValues(fixture.inputPath, fixture.manifestPath, {}),
      /canonical_manifest_generation_failed/
    );
  });
  await t.test("legacy manifest with durable runner residue", child => {
    const environment = { HCCE_TARGET_PROFILE: legacyProfile };
    const fixture = generatedFixture(child, legacyInputOverrides(), environment);
    const resources = YAML.parseAllDocuments(fs.readFileSync(fixture.manifestPath, "utf8"))
      .map(document => document.toJS())
      .filter(Boolean);
    resources.push({
      apiVersion: "v1",
      kind: "Namespace",
      metadata: { name: "hcce-bot-runners" }
    });
    fs.writeFileSync(
      fixture.manifestPath,
      resources.map(resource => YAML.stringify(resource)).join("---\n"),
      { mode: 0o600 }
    );
    assert.throws(
      () => verifyManifestAgainstInputValues(
        fixture.inputPath,
        fixture.manifestPath,
        environment
      ),
      /manifest_does_not_match_input_values/
    );
  });
});

test("legacy live contract rejects invalid durable input phase combinations", async t => {
  const environment = { HCCE_TARGET_PROFILE: legacyProfile };
  await t.test("bootstrap activation input", child => {
    const fixture = generatedFixture(child, legacyInputOverrides({
      BOT_RUNNER_ACTIVATION_PHASE: "bootstrap"
    }), environment);
    assert.throws(
      () => verifyManifestAgainstInputValues(
        fixture.inputPath,
        fixture.manifestPath,
        environment
      ),
      /legacy_live_verifier_requires_config_activation_active/
    );
  });
  await t.test("restore-fence recovery input", child => {
    const fixture = generatedFixture(child, legacyInputOverrides({
      BOT_RUNNER_RECOVERY_PHASE: "restore-fence"
    }), environment);
    assert.throws(
      () => verifyManifestAgainstInputValues(
        fixture.inputPath,
        fixture.manifestPath,
        environment
      ),
      /legacy_live_verifier_requires_config_recovery_active/
    );
  });
});

test("legacy plan guard rejects mixed phases, durable epoch and missing exact profile", t => {
  const fixture = generatedFixture(
    t,
    legacyInputOverrides(),
    { HCCE_TARGET_PROFILE: legacyProfile }
  );
  const plan = readActivationPlan(fixture.manifestPath);
  assert.throws(
    () => verifyLegacyAbsentPlanAndConfig({ ...plan, activationPhase: "active" }, fixture.input),
    /legacy_live_verifier_requires_manifest_activation_absent/
  );
  assert.throws(
    () => verifyLegacyAbsentPlanAndConfig({ ...plan, recoveryPhase: "active" }, fixture.input),
    /legacy_live_verifier_requires_manifest_recovery_absent/
  );
  assert.throws(
    () => verifyLegacyAbsentPlanAndConfig({
      ...plan,
      recoveryEpoch: fixture.input.BOT_RUNNER_RECOVERY_EPOCH
    }, fixture.input),
    /legacy_live_verifier_requires_manifest_recovery_epoch_absent/
  );
  const resources = structuredClone(plan.resources);
  const namespace = resources.find(resource =>
    resource?.apiVersion === "v1" && resource?.kind === "Namespace"
  );
  delete namespace.metadata.annotations["yenhubs.org/target-profile"];
  assert.throws(
    () => verifyLegacyAbsentPlanAndConfig({ ...plan, resources }, fixture.input),
    /legacy_live_verifier_target_profile_mismatch/
  );
});

test("standalone live verifier rejects bootstrap/inert and restore-fence manifests", async t => {
  await t.test("bootstrap", child => {
    const fixture = generatedFixture(child, {
      BOT_RUNNER_ACTIVATION_PHASE: "bootstrap",
      BOT_RUNNER_RECOVERY_PHASE: "active"
    });
    assert.throws(
      () => verifyManifestAgainstInputValues(fixture.inputPath, fixture.manifestPath),
      /live_verifier_requires_config_activation_active/
    );
  });
  await t.test("restore-fence", child => {
    const fixture = generatedFixture(child, {
      BOT_RUNNER_ACTIVATION_PHASE: "active",
      BOT_RUNNER_RECOVERY_PHASE: "restore-fence"
    });
    assert.throws(
      () => verifyManifestAgainstInputValues(fixture.inputPath, fixture.manifestPath),
      /live_verifier_requires_config_recovery_active/
    );
  });
});

test("standalone live verifier rejects a tampered manifest and stale values", async t => {
  await t.test("tampered-manifest", child => {
    const fixture = generatedFixture(child);
    const resources = YAML.parseAllDocuments(fs.readFileSync(fixture.manifestPath, "utf8"))
      .map(document => document.toJS())
      .filter(Boolean);
    const namespace = resources.find(resource =>
      resource?.apiVersion === "v1" &&
      resource?.kind === "Namespace" &&
      resource?.metadata?.name !== "hcce-bot-runners"
    );
    namespace.metadata.annotations.domain = "tampered.invalid";
    fs.writeFileSync(
      fixture.manifestPath,
      resources.map(resource => YAML.stringify(resource)).join("---\n"),
      { mode: 0o600 }
    );
    assert.throws(
      () => verifyManifestAgainstInputValues(fixture.inputPath, fixture.manifestPath),
      /manifest_does_not_match_input_values/
    );
  });
  await t.test("stale-values", child => {
    const fixture = generatedFixture(child);
    fs.writeFileSync(
      fixture.inputPath,
      YAML.stringify({ ...fixture.input, HUB_DOMAIN: "stale-values.invalid" }),
      { mode: 0o600 }
    );
    assert.throws(
      () => verifyManifestAgainstInputValues(fixture.inputPath, fixture.manifestPath),
      /manifest_does_not_match_input_values/
    );
  });
});

test("active plan guard rejects a stale manifest epoch versus config", t => {
  const fixture = generatedFixture(t);
  const plan = readActivationPlan(fixture.manifestPath);
  assert.throws(
    () => verifyActivePlanAndConfig({ ...plan, activationPhase: "bootstrap" }, fixture.input),
    /live_verifier_requires_manifest_activation_active/
  );
  assert.throws(
    () => verifyActivePlanAndConfig({ ...plan, recoveryPhase: "restore-fence" }, fixture.input),
    /live_verifier_requires_manifest_recovery_active/
  );
  assert.throws(
    () => verifyActivePlanAndConfig(plan, {
      ...fixture.input,
      BOT_RUNNER_RECOVERY_EPOCH: "55555555-5555-4555-8555-555555555555"
    }),
    /live_verifier_recovery_epoch_mismatch/
  );
});

test("activation planning rejects scale and Pod-subresource bypasses of durable fences", t => {
  const fixture = generatedFixture(t);
  const resources = YAML.parseAllDocuments(fs.readFileSync(fixture.manifestPath, "utf8"))
    .map(document => document.toJS())
    .filter(Boolean);
  const policy = resources.find(resource =>
    resource?.kind === "ValidatingAdmissionPolicy" &&
    resource?.metadata?.name === "bot-orchestrator-fence-protocol.yenhubs.org"
  );
  assert.deepEqual(
    policy.spec.matchConstraints.resourceRules[0].resources,
    ["deployments", "deployments/scale"]
  );
  assert.equal(
    policy.spec.validations.some(validation =>
      validation.expression === "!has(request.subResource) || request.subResource != 'scale'"
    ),
    true
  );
  policy.spec.matchConstraints.resourceRules[0].resources = ["deployments"];
  assert.throws(
    () => readActivationPlanText(resources.map(resource => YAML.stringify(resource)).join("---\n")),
    /generated_manifest_fence_aware_parent_contract_invalid/
  );

  policy.spec.matchConstraints.resourceRules[0].resources = ["deployments", "deployments/scale"];
  const runnerPolicy = resources.find(resource =>
    resource?.kind === "ValidatingAdmissionPolicy" &&
    resource?.metadata?.name === "bot-runner-durable-protocol.yenhubs.org"
  );
  assert.deepEqual(
    runnerPolicy.spec.matchConstraints.resourceRules[0].resources,
    ["pods", "pods/ephemeralcontainers", "pods/eviction", "pods/resize"]
  );
  runnerPolicy.spec.matchConstraints.resourceRules[0].resources = ["pods"];
  assert.throws(
    () => readActivationPlanText(resources.map(resource => YAML.stringify(resource)).join("---\n")),
    /generated_manifest_fence_aware_parent_contract_invalid/
  );
});

test("activation planning requires the parameter-free recovery operation fence in the phase-bound state", t => {
  const fixture = generatedFixture(t);
  const resources = YAML.parseAllDocuments(fs.readFileSync(fixture.manifestPath, "utf8"))
    .map(document => document.toJS())
    .filter(Boolean);
  const policy = resources.find(resource =>
    resource?.kind === "ValidatingAdmissionPolicy" &&
    resource?.metadata?.name === "recovery-operation-pod-fence.yenhubs.org"
  );
  const binding = resources.find(resource =>
    resource?.kind === "ValidatingAdmissionPolicyBinding" &&
    resource?.metadata?.name === "recovery-operation-pod-fence.yenhubs.org"
  );
  assert.equal(policy.spec.paramKind, undefined);
  assert.equal(binding.spec.paramRef, undefined);
  assert.deepEqual(binding.spec.matchResources.namespaceSelector, {
    matchExpressions: [{
      key: "kubernetes.io/metadata.name",
      operator: "DoesNotExist"
    }]
  });

  const serialize = values => values.map(resource => YAML.stringify(resource)).join("---\n");
  assert.throws(
    () => readActivationPlanText(serialize(resources.filter(resource => resource !== policy))),
    /generated_manifest_fence_aware_parent_contract_invalid/
  );
  for (const mutate of [
    changed => { changed.policy.spec.paramKind = { apiVersion: "v1", kind: "ConfigMap" }; },
    changed => { changed.binding.spec.paramRef = { name: "yenhubs-recovery-operation-lock" }; },
    changed => {
      changed.policy.spec.variables.find(variable =>
        variable.name === "isParentWriterCreate"
      ).expression = "false";
    },
    changed => {
      changed.binding.spec.matchResources.namespaceSelector = {
        matchExpressions: [{
          key: "kubernetes.io/metadata.name",
          operator: "In",
          values: [fixture.input.Namespace, "hcce-bot-runners"]
        }]
      };
    }
  ]) {
    const changedResources = structuredClone(resources);
    const changed = {
      policy: changedResources.find(resource =>
        resource?.kind === "ValidatingAdmissionPolicy" &&
        resource?.metadata?.name === "recovery-operation-pod-fence.yenhubs.org"
      ),
      binding: changedResources.find(resource =>
        resource?.kind === "ValidatingAdmissionPolicyBinding" &&
        resource?.metadata?.name === "recovery-operation-pod-fence.yenhubs.org"
      )
    };
    mutate(changed);
    assert.throws(
      () => readActivationPlanText(serialize(changedResources)),
      /generated_manifest_fence_aware_parent_contract_invalid/
    );
  }
});

test("activation planning accepts only an exact stopped legacy-absent greenfield boundary", () => {
  const namespace = "hcce";
  const deployments = [
    "reticulum",
    "pgbouncer",
    "pgbouncer-t",
    "bot-orchestrator",
    "coturn"
  ].map(name => ({
    apiVersion: "apps/v1",
    kind: "Deployment",
    metadata: { name, namespace },
    spec: { replicas: 0 }
  }));
  deployments.push({
    apiVersion: "apps/v1",
    kind: "Deployment",
    metadata: { name: "pgsql", namespace },
    spec: { replicas: 1 }
  });
  const resources = [
    { apiVersion: "v1", kind: "Namespace", metadata: { name: namespace } },
    ...deployments
  ];
  const serialize = values => values.map(resource => YAML.stringify(resource)).join("---\n");
  const plan = readActivationPlanText(serialize(resources));
  assert.equal(plan.activationPhase, "legacy-absent");
  assert.equal(plan.recoveryPhase, "legacy-absent");
  assert.equal(plan.resources.length, resources.length);

  const unsafe = structuredClone(resources);
  unsafe.find(resource => resource?.metadata?.name === "reticulum").spec.replicas = 1;
  assert.throws(
    () => readActivationPlanText(serialize(unsafe)),
    /generated_manifest_legacy_absent_recovery_boundary_invalid/
  );
});

test("standalone entrypoint completes the values contract before any kubectl read", () => {
  const source = fs.readFileSync(
    path.resolve(__dirname, "verify-live-runner-control-plane.js"),
    "utf8"
  );
  const contractIndex = source.indexOf("verifyManifestAgainstInputValues(inputPath, manifestPath)");
  const kubectlIndex = source.indexOf('execFileSync("kubectl"');
  assert.ok(contractIndex >= 0 && contractIndex < kubectlIndex);
  assert.match(source, /const runnerAuthorityEnabled = true/);
});

test("apply verification and planning consume one immutable manifest byte snapshot", t => {
  const fixture = generatedFixture(t);
  const captured = fs.readFileSync(fixture.manifestPath);
  const plan = readActivationPlanText(captured.toString("utf8"));
  assert.equal(plan.activationPhase, "active");

  fs.writeFileSync(fixture.manifestPath, "apiVersion: v1\nkind: ConfigMap\n", { mode: 0o600 });
  const verifiedSnapshot = spawnSync(
    process.execPath,
    [generatedManifestVerifierPath, "--stdin"],
    {
      cwd: communityEditionDir,
      env: { ...process.env, HCCE_MANIFEST_PATH: fixture.manifestPath },
      input: captured,
      encoding: "utf8"
    }
  );
  assert.equal(verifiedSnapshot.status, 0, verifiedSnapshot.stderr);

  const verifiedChangedPath = spawnSync(process.execPath, [generatedManifestVerifierPath], {
    cwd: communityEditionDir,
    env: { ...process.env, HCCE_MANIFEST_PATH: fixture.manifestPath },
    encoding: "utf8"
  });
  assert.notEqual(verifiedChangedPath.status, 0);

  const applySource = fs.readFileSync(path.resolve(__dirname, "index.js"), "utf8");
  assert.match(applySource, /const manifestBytes = readFileSync\(manifestPath\)/);
  assert.match(applySource, /const plan = readActivationPlanText\(manifestText\)/);
  assert.match(applySource, /\[manifestVerifierPath, "--stdin"\]/);
  assert.equal(
    (applySource.match(/readFileSync\(manifestPath\)/g) || []).length,
    1,
    "the live manifest path is captured exactly once before any cluster action"
  );
});
