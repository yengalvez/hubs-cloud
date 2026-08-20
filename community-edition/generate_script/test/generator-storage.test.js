const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { spawnSync } = require("node:child_process");
const test = require("node:test");
const YAML = require("yaml");

const communityEditionDir = path.resolve(__dirname, "../..");
const generatorPath = path.join(communityEditionDir, "generate_script/index.js");
const verifierPath = path.join(communityEditionDir, "generate_script/verify-generated-manifest.js");
const ciInput = YAML.parse(fs.readFileSync(path.join(communityEditionDir, "input-values.ci.yaml"), "utf8"));
const legacyProfile = "cold-rebind-legacy-absent-v1";

function runNode(script, env) {
  return spawnSync(process.execPath, [script], {
    cwd: communityEditionDir,
    env: { ...process.env, ...env },
    encoding: "utf8"
  });
}

test("generator and verifier support dynamic and retained manual storage only", () => {
  for (const [storageClass, expectedResources] of [
    ["do-block-storage", 68],
    ["manual", 70]
  ]) {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "hcce-storage-contract-"));
    const inputPath = path.join(directory, "input-values.yaml");
    const outputPath = path.join(directory, "hcce.yaml");
    fs.writeFileSync(
      inputPath,
      YAML.stringify({
        ...ciInput,
        PERSISTENT_VOLUME_STORAGE_CLASS: storageClass,
        ALLOW_MANUAL_HOSTPATH_STORAGE: storageClass === "manual"
      }),
      { mode: 0o600 }
    );

    const generated = runNode(generatorPath, {
      HCCE_INPUT_VALUES_PATH: inputPath,
      HCCE_OUTPUT_PATH: outputPath
    });
    assert.equal(generated.status, 0, generated.stderr);
    const verified = runNode(verifierPath, { HCCE_MANIFEST_PATH: outputPath });
    assert.equal(verified.status, 0, verified.stderr);
    assert.match(verified.stdout, new RegExp(`\\(${expectedResources} resources\\)`));
  }

  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "hcce-storage-disabled-"));
  const inputPath = path.join(directory, "input-values.yaml");
  const outputPath = path.join(directory, "hcce.yaml");
  fs.writeFileSync(
    inputPath,
    YAML.stringify({ ...ciInput, GENERATE_PERSISTENT_VOLUMES: false }),
    { mode: 0o600 }
  );
  const rejected = runNode(generatorPath, {
    HCCE_INPUT_VALUES_PATH: inputPath,
    HCCE_OUTPUT_PATH: outputPath
  });
  assert.notEqual(rejected.status, 0);
  assert.match(rejected.stderr, /GENERATE_PERSISTENT_VOLUMES must be true/);

  for (const [name, mutate, expectedError] of [
    [
      "missing-class",
      input => { delete input.PERSISTENT_VOLUME_STORAGE_CLASS; },
      /PERSISTENT_VOLUME_STORAGE_CLASS must be configured explicitly/
    ],
    [
      "manual-without-opt-in",
      input => {
        input.PERSISTENT_VOLUME_STORAGE_CLASS = "manual";
        input.ALLOW_MANUAL_HOSTPATH_STORAGE = false;
      },
      /manual hostPath storage requires ALLOW_MANUAL_HOSTPATH_STORAGE/
    ]
  ]) {
    const rejectedDirectory = fs.mkdtempSync(path.join(os.tmpdir(), "hcce-storage-" + name + "-"));
    const rejectedInputPath = path.join(rejectedDirectory, "input-values.yaml");
    const rejectedOutputPath = path.join(rejectedDirectory, "hcce.yaml");
    const input = structuredClone(ciInput);
    mutate(input);
    fs.writeFileSync(rejectedInputPath, YAML.stringify(input), { mode: 0o600 });
    const result = runNode(generatorPath, {
      HCCE_INPUT_VALUES_PATH: rejectedInputPath,
      HCCE_OUTPUT_PATH: rejectedOutputPath
    });
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, expectedError);
  }
});

test("opt-in legacy cold-rebind profile is exact, fail-closed, and leaves the default durable profile unchanged", () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "hcce-legacy-cold-rebind-"));
  const inputPath = path.join(directory, "input-values.yaml");
  const outputPath = path.join(directory, "hcce.yaml");
  const legacyDockerConfig = JSON.parse(
    Buffer.from(ciInput.BOT_IMAGE_PULL_CONFIG_JSON_BASE64, "base64").toString("utf8")
  );
  legacyDockerConfig.auths["ghcr.io"] = {
    auth: Buffer.from("ci-user:ci-token", "utf8").toString("base64")
  };
  const legacyInput = {
    ...ciInput,
    OVERRIDE_BOT_RUNNER_IMAGE: "No",
    OVERRIDE_HUBS_IMAGE: `ghcr.io/yengalvez/hubs@sha256:${"9".repeat(64)}`,
    BOT_IMAGE_PULL_CONFIG_JSON_BASE64: Buffer.from(
      JSON.stringify(legacyDockerConfig), "utf8"
    ).toString("base64")
  };
  fs.writeFileSync(inputPath, YAML.stringify(legacyInput), { mode: 0o600 });
  const legacyEnv = {
    HCCE_INPUT_VALUES_PATH: inputPath,
    HCCE_OUTPUT_PATH: outputPath,
    HCCE_TARGET_PROFILE: legacyProfile
  };
  const generated = runNode(generatorPath, legacyEnv);
  assert.equal(generated.status, 0, generated.stderr);
  const verified = runNode(verifierPath, {
    HCCE_MANIFEST_PATH: outputPath,
    HCCE_TARGET_PROFILE: legacyProfile
  });
  assert.equal(verified.status, 0, verified.stderr);
  assert.match(verified.stdout, new RegExp(legacyProfile));

  const originalManifest = fs.readFileSync(outputPath, "utf8");
  const resources = YAML.parseAllDocuments(originalManifest)
    .map(document => document.toJS())
    .filter(Boolean);
  const namespaces = resources.filter(resource => resource.kind === "Namespace");
  assert.equal(namespaces.length, 1);
  assert.equal(namespaces[0].metadata.name, ciInput.Namespace);
  assert.equal(namespaces[0].metadata.annotations["yenhubs.org/target-profile"], legacyProfile);
  assert.deepEqual(
    resources.filter(resource => resource.kind === "PersistentVolumeClaim")
      .map(resource => resource.metadata.name).sort(),
    ["pgsql-pvc", "ret-pvc"]
  );
  const deployments = resources.filter(resource => resource.kind === "Deployment");
  assert.equal(deployments.length, 12);
  for (const name of ["reticulum", "pgbouncer", "pgbouncer-t", "bot-orchestrator", "coturn"]) {
    assert.equal(
      deployments.find(deployment => deployment.metadata.name === name).spec.replicas,
      0,
      `${name} must start fenced`
    );
  }
  const pgsql = deployments.find(deployment => deployment.metadata.name === "pgsql");
  assert.equal(pgsql.spec.replicas, 1);
  assert.deepEqual(pgsql.spec.template.spec.containers.map(container => container.name), ["postgresql"]);
  assert.equal(deployments.flatMap(deployment => deployment.spec.template.spec.containers).length, 13);
  assert.equal(resources.some(resource => resource.metadata?.namespace === "hcce-bot-runners"), false);
  assert.equal(resources.some(resource => resource.metadata?.name === "hcce-bot-runners"), false);
  assert.equal(resources.some(resource => resource.kind === "ValidatingAdmissionPolicy"), false);
  assert.equal(resources.some(resource => resource.kind === "ValidatingAdmissionPolicyBinding"), false);
  const pullSecret = resources.find(resource =>
    resource.kind === "Secret" && resource.metadata?.name === "bot-images-pull"
  );
  assert.equal(pullSecret.type, "kubernetes.io/dockerconfigjson");
  for (const deployment of deployments) {
    const usesGhcr = deployment.spec.template.spec.containers.some(container =>
      container.image.startsWith("ghcr.io/")
    );
    assert.deepEqual(
      deployment.spec.template.spec.imagePullSecrets,
      usesGhcr ? [{ name: "bot-images-pull" }] : undefined
    );
  }
  const processLocalSection =
    '[ret."Elixir.Ret.BotOrchestrator"]\n' +
    'endpoint = "http://bot-orchestrator.<POD_NS>:5001"\n' +
    'access_key = "<BOT_ACCESS_KEY>"';
  const retConfigText = resources.find(resource =>
    resource.kind === "ConfigMap" && resource.metadata.name === "ret-config"
  ).data["config.toml.template"];
  assert.equal((retConfigText.match(/\[ret\."Elixir\.Ret\.BotOrchestrator"\]/g) || []).length, 1);
  assert.equal(retConfigText.includes(processLocalSection), true);
  assert.equal(retConfigText.includes("<BOT_ORCHESTRATOR_ACCESS_KEY>"), false);

  function rejectMutation(name, mutate, expectedError) {
    const changed = YAML.parseAllDocuments(originalManifest)
      .map(document => document.toJS())
      .filter(Boolean);
    mutate(changed);
    fs.writeFileSync(
      outputPath,
      changed.map(resource => YAML.stringify(resource)).join("---\n")
    );
    const rejected = runNode(verifierPath, {
      HCCE_MANIFEST_PATH: outputPath,
      HCCE_TARGET_PROFILE: legacyProfile
    });
    assert.notEqual(rejected.status, 0, `${name} unexpectedly passed`);
    assert.match(rejected.stderr, expectedError);
  }

  rejectMutation("durable residual", changed => {
    changed.push({ apiVersion: "v1", kind: "Namespace", metadata: { name: "hcce-bot-runners" } });
  }, /exactly one target Namespace/);
  rejectMutation("writer nonzero", changed => {
    changed.find(resource => resource.kind === "Deployment" && resource.metadata.name === "reticulum")
      .spec.replicas = 1;
  }, /reticulum.*replicas=0/);
  rejectMutation("missing pull credential", changed => {
    changed.splice(changed.findIndex(resource =>
      resource.kind === "Secret" && resource.metadata?.name === "bot-images-pull"
    ), 1);
  }, /missing.*bot-images-pull|Secret\/bot-images-pull/);
  rejectMutation("missing GHCR pull binding", changed => {
    delete changed.find(resource =>
      resource.kind === "Deployment" && resource.metadata.name === "hubs"
    ).spec.template.spec.imagePullSecrets;
  }, /hubs must bind the legacy pull Secret/);
  rejectMutation("image drift", changed => {
    changed.find(resource => resource.kind === "Deployment" && resource.metadata.name === "hubs")
      .spec.template.spec.containers[0].image =
        `registry.invalid/yenhubs/hubs@sha256:${"f".repeat(64)}`;
  }, /image-map annotations/);
  rejectMutation("binding drift", changed => {
    const parent = changed.find(resource =>
      resource.kind === "Deployment" && resource.metadata.name === "bot-orchestrator"
    );
    parent.spec.template.spec.containers[0].env.find(entry => entry.name === "BOT_ACCESS_KEY")
      .valueFrom.secretKeyRef.key = "OPENAI_API_KEY";
  }, /process-local parent contract/);
  rejectMutation("annotation drift", changed => {
    const parent = changed.find(resource =>
      resource.kind === "Deployment" && resource.metadata.name === "bot-orchestrator"
    );
    parent.metadata.annotations["yenhubs.org/runner-activation-phase"] = "active";
  }, /residual runner annotation/);
  rejectMutation("missing process-local orchestrator section", changed => {
    const config = changed.find(resource =>
      resource.kind === "ConfigMap" && resource.metadata.name === "ret-config"
    );
    config.data["config.toml.template"] =
      config.data["config.toml.template"].replace(`${processLocalSection}\n\n`, "");
  }, /process-local bot orchestrator, runtime and dashboard bindings/);
  rejectMutation("duplicate process-local orchestrator section", changed => {
    const config = changed.find(resource =>
      resource.kind === "ConfigMap" && resource.metadata.name === "ret-config"
    );
    config.data["config.toml.template"] += `\n${processLocalSection}\n`;
  }, /process-local bot orchestrator, runtime and dashboard bindings/);
  rejectMutation("durable orchestrator access key", changed => {
    const config = changed.find(resource =>
      resource.kind === "ConfigMap" && resource.metadata.name === "ret-config"
    );
    config.data["config.toml.template"] = config.data["config.toml.template"].replace(
      'access_key = "<BOT_ACCESS_KEY>"',
      'access_key = "<BOT_ORCHESTRATOR_ACCESS_KEY>"'
    );
  }, /process-local bot orchestrator, runtime and dashboard bindings/);
  rejectMutation("hubs service-account token", changed => {
    const hubs = changed.find(resource =>
      resource.kind === "Deployment" && resource.metadata.name === "hubs"
    );
    hubs.spec.template.spec.automountServiceAccountToken = true;
  }, /hubs must disable service-account token automounting/);
  rejectMutation("widened NetworkPolicy", changed => {
    const policy = changed.find(resource =>
      resource.kind === "NetworkPolicy" && resource.metadata.name === "pgsql-ingress"
    );
    policy.spec.ingress[0].from.push({ ipBlock: { cidr: "0.0.0.0/0" } });
  }, /pgsql-ingress must exactly match its single audited ingress rule/);
  for (const [field, value] of [
    ["command", ["/bin/sh"]],
    ["args", ["-c", "exit 0"]],
    ["envFrom", [{ secretRef: { name: "configs" } }]]
  ]) {
    rejectMutation(`parent ${field}`, changed => {
      const parent = changed.find(resource =>
        resource.kind === "Deployment" && resource.metadata.name === "bot-orchestrator"
      );
      parent.spec.template.spec.containers[0][field] = value;
    }, /process-local parent contract/);
  }

  const defaultOutputPath = path.join(directory, "hcce-default.yaml");
  fs.writeFileSync(inputPath, YAML.stringify(ciInput), { mode: 0o600 });
  const defaultGenerated = runNode(generatorPath, {
    HCCE_INPUT_VALUES_PATH: inputPath,
    HCCE_OUTPUT_PATH: defaultOutputPath
  });
  assert.equal(defaultGenerated.status, 0, defaultGenerated.stderr);
  const defaultVerified = runNode(verifierPath, { HCCE_MANIFEST_PATH: defaultOutputPath });
  assert.equal(defaultVerified.status, 0, defaultVerified.stderr);
  const defaultResources = YAML.parseAllDocuments(fs.readFileSync(defaultOutputPath, "utf8"))
    .map(document => document.toJS())
    .filter(Boolean);
  assert.equal(defaultResources.length, 68);
  assert.equal(
    defaultResources.some(resource => resource.kind === "Namespace" && resource.metadata.name === "hcce-bot-runners"),
    true
  );
  assert.equal(
    defaultResources.find(resource =>
      resource.kind === "Deployment" && resource.metadata.name === "bot-orchestrator"
    ).spec.replicas,
    1
  );
});

test("generator requires four independent access-key trust domains", () => {
  for (const [name, mutate, error] of [
    [
      "missing-dashboard",
      input => { delete input.DASHBOARD_ACCESS_KEY; },
      /DASHBOARD_ACCESS_KEY must be configured independently/
    ],
    [
      "reused-runner",
      input => { input.BOT_RUNNER_ACCESS_KEY = input.BOT_ACCESS_KEY; },
      /access keys must all be distinct/
    ]
  ]) {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), `hcce-keys-${name}-`));
    const inputPath = path.join(directory, "input-values.yaml");
    const outputPath = path.join(directory, "hcce.yaml");
    const input = structuredClone(ciInput);
    mutate(input);
    fs.writeFileSync(inputPath, YAML.stringify(input), { mode: 0o600 });
    const rejected = runNode(generatorPath, {
      HCCE_INPUT_VALUES_PATH: inputPath,
      HCCE_OUTPUT_PATH: outputPath
    });
    assert.notEqual(rejected.status, 0);
    assert.match(rejected.stderr, error);
  }
});

test("generator binds activation and restore-fence phases to exact replicas and inert authority", () => {
  for (const [activationPhase, expectedBotReplicas, expectedRoleRules] of [
    ["bootstrap", 0, 0],
    ["admission", 0, 1],
    ["active", 1, 1]
  ]) {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), `hcce-phase-${activationPhase}-`));
    const inputPath = path.join(directory, "input-values.yaml");
    const outputPath = path.join(directory, "hcce.yaml");
    fs.writeFileSync(inputPath, YAML.stringify({
      ...ciInput,
      BOT_RUNNER_ACTIVATION_PHASE: activationPhase,
      BOT_RUNNER_RECOVERY_PHASE: "active"
    }), { mode: 0o600 });
    const generated = runNode(generatorPath, {
      HCCE_INPUT_VALUES_PATH: inputPath,
      HCCE_OUTPUT_PATH: outputPath
    });
    assert.equal(generated.status, 0, generated.stderr);
    const verified = runNode(verifierPath, { HCCE_MANIFEST_PATH: outputPath });
    assert.equal(verified.status, 0, verified.stderr);
    const resources = YAML.parseAllDocuments(fs.readFileSync(outputPath, "utf8"))
      .map(document => document.toJS())
      .filter(Boolean);
    const bot = resources.find(resource =>
      resource.kind === "Deployment" && resource.metadata?.name === "bot-orchestrator"
    );
    const role = resources.find(resource =>
      resource.kind === "Role" &&
      resource.metadata?.namespace === "hcce-bot-runners" &&
      resource.metadata?.name === "bot-orchestrator-runner-pods"
    );
    assert.equal(bot.spec.replicas, expectedBotReplicas);
    assert.equal(role.rules.length, expectedRoleRules);
    const recoveryFenceBinding = resources.find(resource =>
      resource.kind === "ValidatingAdmissionPolicyBinding" &&
      resource.metadata?.name === "recovery-operation-pod-fence.yenhubs.org"
    );
    assert.deepEqual(
      recoveryFenceBinding.spec.matchResources.namespaceSelector,
      {
        matchExpressions: [{
          key: "kubernetes.io/metadata.name",
          operator: "DoesNotExist"
        }]
      },
      `${activationPhase} must keep the recovery operation fence dormant`
    );
  }

  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "hcce-restore-fence-"));
  const inputPath = path.join(directory, "input-values.yaml");
  const outputPath = path.join(directory, "hcce.yaml");
  fs.writeFileSync(inputPath, YAML.stringify({
    ...ciInput,
    BOT_RUNNER_ACTIVATION_PHASE: "active",
    BOT_RUNNER_RECOVERY_PHASE: "restore-fence"
  }), { mode: 0o600 });
  const generated = runNode(generatorPath, {
    HCCE_INPUT_VALUES_PATH: inputPath,
    HCCE_OUTPUT_PATH: outputPath
  });
  assert.equal(generated.status, 0, generated.stderr);
  const verified = runNode(verifierPath, { HCCE_MANIFEST_PATH: outputPath });
  assert.equal(verified.status, 0, verified.stderr);
  const resources = YAML.parseAllDocuments(fs.readFileSync(outputPath, "utf8"))
    .map(document => document.toJS())
    .filter(Boolean);
  for (const name of ["reticulum", "pgbouncer", "pgbouncer-t", "bot-orchestrator", "coturn"]) {
    const deployment = resources.find(resource =>
      resource.kind === "Deployment" && resource.metadata?.name === name
    );
    assert.equal(deployment.spec.replicas, 0, `${name} must be fenced`);
    assert.equal(
      deployment.metadata.annotations["yenhubs.org/bot-runner-recovery-phase"],
      "restore-fence"
    );
  }
  assert.equal(resources.find(resource =>
    resource.kind === "Deployment" && resource.metadata?.name === "pgsql"
  ).spec.replicas, 1);
  assert.deepEqual(resources.find(resource =>
    resource.kind === "Role" &&
    resource.metadata?.namespace === "hcce-bot-runners" &&
    resource.metadata?.name === "bot-orchestrator-runner-pods"
  ).rules, []);
  const recoveryFenceBinding = resources.find(resource =>
    resource.kind === "ValidatingAdmissionPolicyBinding" &&
    resource.metadata?.name === "recovery-operation-pod-fence.yenhubs.org"
  );
  assert.deepEqual(recoveryFenceBinding.spec.matchResources.namespaceSelector, {
    matchExpressions: [{
      key: "kubernetes.io/metadata.name",
      operator: "In",
      values: [ciInput.Namespace, "hcce-bot-runners"]
    }]
  });
});

test("verifier binds every Reticulum access-key environment and TOML mapping to its own domain", () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "hcce-key-bindings-"));
  const inputPath = path.join(directory, "input-values.yaml");
  const outputPath = path.join(directory, "hcce.yaml");
  fs.writeFileSync(inputPath, YAML.stringify(ciInput), { mode: 0o600 });
  const generated = runNode(generatorPath, {
    HCCE_INPUT_VALUES_PATH: inputPath,
    HCCE_OUTPUT_PATH: outputPath
  });
  assert.equal(generated.status, 0, generated.stderr);
  const originalManifest = fs.readFileSync(outputPath, "utf8");

  function rejectMutation(mutate, expectedError) {
    const resources = YAML.parseAllDocuments(originalManifest).map(document => document.toJS()).filter(Boolean);
    mutate(resources);
    fs.writeFileSync(outputPath, resources.map(resource => YAML.stringify(resource)).join("---\n"));
    const rejected = runNode(verifierPath, { HCCE_MANIFEST_PATH: outputPath });
    assert.notEqual(rejected.status, 0);
    assert.match(rejected.stderr, expectedError);
  }

  for (const [envName, wrongSecretKey] of [
    ["turkeyCfg_BOT_ACCESS_KEY", "BOT_RUNNER_ACCESS_KEY"],
    ["turkeyCfg_BOT_RUNNER_ACCESS_KEY", "DASHBOARD_ACCESS_KEY"],
    ["turkeyCfg_BOT_ORCHESTRATOR_ACCESS_KEY", "BOT_ACCESS_KEY"],
    ["turkeyCfg_DASHBOARD_ACCESS_KEY", "BOT_RUNNER_ACCESS_KEY"]
  ]) {
    rejectMutation(resources => {
      const reticulum = resources.find(resource =>
        resource.kind === "Deployment" && resource.metadata?.name === "reticulum"
      );
      const container = reticulum.spec.template.spec.containers.find(value => value.name === "reticulum");
      const entry = container.env.find(value => value.name === envName);
      entry.valueFrom.secretKeyRef.key = wrongSecretKey;
    }, new RegExp(`${envName} must exclusively reference`));
  }

  for (const [expected, replacement] of [
    [
      'dashboard_access_key = "<DASHBOARD_ACCESS_KEY>"',
      'dashboard_access_key = "<BOT_RUNNER_ACCESS_KEY>"'
    ],
    [
      'header_value = "<DASHBOARD_ACCESS_KEY>"',
      'header_value = "<BOT_ORCHESTRATOR_ACCESS_KEY>"'
    ],
    ['bot_access_key = "<BOT_ACCESS_KEY>"', 'bot_access_key = "<BOT_RUNNER_ACCESS_KEY>"'],
    ['bot_runner_access_key = "<BOT_RUNNER_ACCESS_KEY>"', 'bot_runner_access_key = "<DASHBOARD_ACCESS_KEY>"'],
    [
      'bot_orchestrator_access_key = "<BOT_ORCHESTRATOR_ACCESS_KEY>"',
      'bot_orchestrator_access_key = "<BOT_RUNNER_ACCESS_KEY>"'
    ],
    [
      'access_key = "<BOT_ORCHESTRATOR_ACCESS_KEY>"',
      'access_key = "<BOT_ACCESS_KEY>"'
    ]
  ]) {
    rejectMutation(resources => {
      const config = resources.find(resource =>
        resource.kind === "ConfigMap" && resource.metadata?.name === "ret-config"
      );
      config.data["config.toml.template"] = config.data["config.toml.template"].replace(
        expected,
        replacement
      );
    }, /exact scoped access-key placeholder mappings/);
  }
});

test("verifier binds one active-room ceiling to Reticulum and bot-orchestrator", () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "hcce-room-limit-bindings-"));
  const inputPath = path.join(directory, "input-values.yaml");
  const outputPath = path.join(directory, "hcce.yaml");
  fs.writeFileSync(inputPath, YAML.stringify(ciInput), { mode: 0o600 });
  const generated = runNode(generatorPath, {
    HCCE_INPUT_VALUES_PATH: inputPath,
    HCCE_OUTPUT_PATH: outputPath
  });
  assert.equal(generated.status, 0, generated.stderr);
  const originalManifest = fs.readFileSync(outputPath, "utf8");

  function rejectMutation(mutate, expectedError) {
    const resources = YAML.parseAllDocuments(originalManifest).map(document => document.toJS()).filter(Boolean);
    mutate(resources);
    fs.writeFileSync(outputPath, resources.map(resource => YAML.stringify(resource)).join("---\n"));
    const rejected = runNode(verifierPath, { HCCE_MANIFEST_PATH: outputPath });
    assert.notEqual(rejected.status, 0);
    assert.match(rejected.stderr, expectedError);
  }

  rejectMutation(resources => {
    const config = resources.find(resource =>
      resource.kind === "ConfigMap" && resource.metadata?.name === "ret-config"
    );
    config.data["config.toml.template"] = config.data["config.toml.template"].replace(
      "max_active_bot_rooms = <MAX_ACTIVE_ROOMS>",
      "max_active_bot_rooms = 10"
    );
  }, /bind max_active_bot_rooms exactly to MAX_ACTIVE_ROOMS/);

  rejectMutation(resources => {
    const reticulum = resources.find(resource =>
      resource.kind === "Deployment" && resource.metadata?.name === "reticulum"
    );
    const container = reticulum.spec.template.spec.containers.find(value => value.name === "reticulum");
    container.env.find(value => value.name === "turkeyCfg_MAX_ACTIVE_ROOMS").value = "4";
  }, /must receive the same MAX_ACTIVE_ROOMS value/);
});
