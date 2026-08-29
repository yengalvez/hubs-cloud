const crypto = require("node:crypto");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { spawnSync } = require("node:child_process");
const utils = require("../utils");
const {
  LEGACY_ACTIVE_COLD_REBIND_PROFILE,
  LEGACY_ABSENT_COLD_REBIND_PROFILE,
  targetProfileFromEnvironment
} = require("../generate_script/legacy-absent-cold-rebind-profile");
const { readActivationPlan } = require("./runner-activation");

const communityEditionDir = path.resolve(__dirname, "..");
const generatorPath = path.resolve(communityEditionDir, "generate_script/index.js");
const structuralVerifierPath = path.resolve(
  communityEditionDir,
  "generate_script/verify-generated-manifest.js"
);
const subprocessTimeoutMs = 120_000;
const subprocessMaxBuffer = 4 * 1024 * 1024;

function runNode(scriptPath, environment) {
  const childEnvironment = { ...process.env, ...environment };
  for (const [name, value] of Object.entries(childEnvironment)) {
    if (value === undefined) delete childEnvironment[name];
  }
  return spawnSync(process.execPath, [scriptPath], {
    cwd: communityEditionDir,
    env: childEnvironment,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
    timeout: subprocessTimeoutMs,
    maxBuffer: subprocessMaxBuffer
  });
}

function requireSuccessfulSubprocess(result, errorCode) {
  if (result?.status !== 0 || result?.error) throw new Error(errorCode);
}

function fileDigest(filePath) {
  const stat = fs.lstatSync(filePath);
  if (!stat.isFile() || stat.isSymbolicLink()) throw new Error("manifest_contract_file_invalid");
  return {
    size: stat.size,
    digest: crypto.createHash("sha256").update(fs.readFileSync(filePath)).digest()
  };
}

function exactFileContent(leftPath, rightPath) {
  const left = fileDigest(leftPath);
  const right = fileDigest(rightPath);
  return left.size === right.size && crypto.timingSafeEqual(left.digest, right.digest);
}

function verifyActivePlanAndConfig(plan, config) {
  if (String(config?.BOT_RUNNER_ACTIVATION_PHASE || "") !== "active") {
    throw new Error("live_verifier_requires_config_activation_active");
  }
  if (String(config?.BOT_RUNNER_RECOVERY_PHASE || "") !== "active") {
    throw new Error("live_verifier_requires_config_recovery_active");
  }
  if (plan?.activationPhase !== "active") {
    throw new Error("live_verifier_requires_manifest_activation_active");
  }
  if (plan?.recoveryPhase !== "active") {
    throw new Error("live_verifier_requires_manifest_recovery_active");
  }
  const configEpoch = String(config?.BOT_RUNNER_RECOVERY_EPOCH || "");
  if (
    !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(configEpoch) ||
    plan?.recoveryEpoch !== configEpoch
  ) {
    throw new Error("live_verifier_recovery_epoch_mismatch");
  }
}

function verifyLegacyAbsentPlanAndConfig(plan, config) {
  if (String(config?.BOT_RUNNER_ACTIVATION_PHASE || "") !== "active") {
    throw new Error("legacy_live_verifier_requires_config_activation_active");
  }
  if (String(config?.BOT_RUNNER_RECOVERY_PHASE || "") !== "active") {
    throw new Error("legacy_live_verifier_requires_config_recovery_active");
  }
  if (plan?.activationPhase !== "legacy-absent") {
    throw new Error("legacy_live_verifier_requires_manifest_activation_absent");
  }
  if (plan?.recoveryPhase !== "legacy-absent") {
    throw new Error("legacy_live_verifier_requires_manifest_recovery_absent");
  }
  if (plan?.recoveryEpoch !== "legacy-absent") {
    throw new Error("legacy_live_verifier_requires_manifest_recovery_epoch_absent");
  }
  const namespaces = (plan?.resources || []).filter(resource =>
    resource?.apiVersion === "v1" &&
    resource?.kind === "Namespace" &&
    resource?.metadata?.name !== "hcce-bot-runners"
  );
  if (
    namespaces.length !== 1 ||
    namespaces[0]?.metadata?.annotations?.["yenhubs.org/target-profile"] !==
      LEGACY_ABSENT_COLD_REBIND_PROFILE
  ) {
    throw new Error("legacy_live_verifier_target_profile_mismatch");
  }
}

function verifyLegacyActivePlanAndConfig(plan, config) {
  if (String(config?.BOT_RUNNER_ACTIVATION_PHASE || "") !== "active") {
    throw new Error("legacy_active_live_verifier_requires_config_activation_active");
  }
  if (String(config?.BOT_RUNNER_RECOVERY_PHASE || "") !== "active") {
    throw new Error("legacy_active_live_verifier_requires_config_recovery_active");
  }
  if (plan?.activationPhase !== "legacy-active") {
    throw new Error("legacy_active_live_verifier_requires_manifest_activation_active");
  }
  if (plan?.recoveryPhase !== "legacy-active" || plan?.recoveryEpoch !== "legacy-active") {
    throw new Error("legacy_active_live_verifier_requires_manifest_recovery_active");
  }
  const namespaces = (plan?.resources || []).filter(resource =>
    resource?.apiVersion === "v1" &&
    resource?.kind === "Namespace" &&
    resource?.metadata?.name !== "hcce-bot-runners"
  );
  if (
    namespaces.length !== 1 ||
    namespaces[0]?.metadata?.annotations?.["yenhubs.org/target-profile"] !==
      LEGACY_ACTIVE_COLD_REBIND_PROFILE
  ) {
    throw new Error("legacy_active_live_verifier_target_profile_mismatch");
  }
}

function verifyTargetPlanAndConfig(plan, config, targetProfile) {
  if (targetProfile === LEGACY_ABSENT_COLD_REBIND_PROFILE) {
    verifyLegacyAbsentPlanAndConfig(plan, config);
    return;
  }
  if (targetProfile === LEGACY_ACTIVE_COLD_REBIND_PROFILE) {
    verifyLegacyActivePlanAndConfig(plan, config);
    return;
  }
  if (targetProfile !== null) throw new Error("live_verifier_target_profile_invalid");
  verifyActivePlanAndConfig(plan, config);
}

function verifyManifestAgainstInputValues(
  inputPath,
  manifestPath,
  environment = process.env
) {
  const resolvedInputPath = path.resolve(inputPath);
  const resolvedManifestPath = path.resolve(manifestPath);
  const targetProfile = targetProfileFromEnvironment(environment);
  const targetEnvironment = {
    HCCE_TARGET_PROFILE: targetProfile || undefined
  };
  const temporaryDirectory = fs.mkdtempSync(path.join(os.tmpdir(), "hcce-live-contract-"));
  fs.chmodSync(temporaryDirectory, 0o700);
  const expectedManifestPath = path.join(temporaryDirectory, "hcce.yaml");
  try {
    const generated = runNode(generatorPath, {
      HCCE_INPUT_VALUES_PATH: resolvedInputPath,
      HCCE_OUTPUT_PATH: expectedManifestPath,
      ...targetEnvironment
    });
    requireSuccessfulSubprocess(generated, "canonical_manifest_generation_failed");
    if ((fs.statSync(expectedManifestPath).mode & 0o777) !== 0o600) {
      throw new Error("canonical_manifest_output_mode_invalid");
    }
    if (!exactFileContent(resolvedManifestPath, expectedManifestPath)) {
      throw new Error("manifest_does_not_match_input_values");
    }

    const structurallyVerified = runNode(structuralVerifierPath, {
      HCCE_MANIFEST_PATH: resolvedManifestPath,
      ...targetEnvironment
    });
    requireSuccessfulSubprocess(
      structurallyVerified,
      "canonical_manifest_structural_verification_failed"
    );

    const config = utils.readConfig(resolvedInputPath);
    const plan = readActivationPlan(resolvedManifestPath);
    verifyTargetPlanAndConfig(plan, config, targetProfile);
    return { config, plan, targetProfile };
  } finally {
    fs.rmSync(temporaryDirectory, { recursive: true, force: true });
  }
}

module.exports = {
  exactFileContent,
  verifyActivePlanAndConfig,
  verifyLegacyActivePlanAndConfig,
  verifyLegacyAbsentPlanAndConfig,
  verifyTargetPlanAndConfig,
  verifyManifestAgainstInputValues
};
