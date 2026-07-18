const assert = require("node:assert/strict");
const crypto = require("node:crypto");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { spawnSync } = require("node:child_process");
const test = require("node:test");
const YAML = require("yaml");
const { readActivationPlan } = require("./runner-activation");
const {
  verifyActivePlanAndConfig,
  verifyManifestAgainstInputValues
} = require("./manifest-input-contract");

const communityEditionDir = path.resolve(__dirname, "..");
const generatorPath = path.resolve(communityEditionDir, "generate_script/index.js");
const ciInput = YAML.parse(fs.readFileSync(
  path.resolve(communityEditionDir, "input-values.ci.yaml"),
  "utf8"
));
const { privateKey: stableTestPermsKey } = crypto.generateKeyPairSync("rsa", {
  modulusLength: 2048,
  privateKeyEncoding: { type: "pkcs8", format: "pem" },
  publicKeyEncoding: { type: "spki", format: "pem" }
});

function generatedFixture(t, overrides = {}) {
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
      HCCE_INPUT_VALUES_PATH: inputPath,
      HCCE_OUTPUT_PATH: manifestPath
    },
    encoding: "utf8"
  });
  assert.equal(generated.status, 0, generated.stderr);
  return { input, inputPath, manifestPath };
}

test("standalone live verifier accepts only the exact reproducible active manifest", t => {
  const fixture = generatedFixture(t);
  const result = verifyManifestAgainstInputValues(fixture.inputPath, fixture.manifestPath);
  assert.equal(result.plan.activationPhase, "active");
  assert.equal(result.plan.recoveryPhase, "active");
  assert.equal(result.plan.recoveryEpoch, fixture.input.BOT_RUNNER_RECOVERY_EPOCH);
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
