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

function runNode(script, env) {
  return spawnSync(process.execPath, [script], {
    cwd: communityEditionDir,
    env: { ...process.env, ...env },
    encoding: "utf8"
  });
}

test("generator and verifier support dynamic and retained manual storage only", () => {
  for (const [storageClass, expectedResources] of [
    ["do-block-storage", 50],
    ["manual", 52]
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
