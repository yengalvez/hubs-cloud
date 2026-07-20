const crypto = require("crypto");
const path = require("path");
const YAML = require("yaml");
const pemJwk = require("pem-jwk");
const utils = require("../utils");
const { verifyDockerConfigCredentials } = require("./verify-manifest-contracts");

function generationOverrides() {
  const hasInput = Object.prototype.hasOwnProperty.call(process.env, "HCCE_INPUT_VALUES_PATH");
  const hasOutput = Object.prototype.hasOwnProperty.call(process.env, "HCCE_OUTPUT_PATH");
  if (hasInput !== hasOutput) {
    throw new Error("HCCE_INPUT_VALUES_PATH and HCCE_OUTPUT_PATH must be configured together");
  }
  if (!hasInput) return { inputPath: undefined, outputPath: undefined };

  const input = process.env.HCCE_INPUT_VALUES_PATH;
  const output = process.env.HCCE_OUTPUT_PATH;
  if (!input || !input.trim() || !output || !output.trim()) {
    throw new Error("HCCE generator path overrides must not be empty");
  }
  return { inputPath: path.resolve(input), outputPath: path.resolve(output) };
}

// Generate a private key and public key
function generateKeys() {
  const { publicKey, privateKey } = crypto.generateKeyPairSync("rsa", {
    modulusLength: 2048,
    publicKeyEncoding: {
      type: "spki",
      format: "pem",
    },
    privateKeyEncoding: {
      type: "pkcs8",
      format: "pem",
    },
  });

  return { publicKey, privateKey };
}

// Function to convert PEM to JWK
function convertPemToJwk(publicKey) {
  const jwk = pemJwk.pem2jwk(publicKey);
  return JSON.stringify(jwk);
}

function normalizePemPrivateKey(value) {
  if (!value || typeof value !== "string") return "";
  return value
    .replace(/\\r\\n/g, "\n")
    .replace(/\r\n/g, "\n")
    .replace(/\\n/g, "\n")
    .trim();
}

function generatePersistentVolumes(processedConfig, replacedContent) {
  const yamlDocuments = YAML.parseAllDocuments(replacedContent);
  let outputIdx = 2;
  const storageClass =
    typeof processedConfig.PERSISTENT_VOLUME_STORAGE_CLASS === "string"
      ? processedConfig.PERSISTENT_VOLUME_STORAGE_CLASS.trim()
      : "";
  if (!storageClass) {
    throw new Error("PERSISTENT_VOLUME_STORAGE_CLASS must be configured explicitly");
  }
  processedConfig.PERSISTENT_VOLUME_STORAGE_CLASS = storageClass;
  if (storageClass === "manual" && processedConfig.ALLOW_MANUAL_HOSTPATH_STORAGE !== true) {
    throw new Error(
      "manual hostPath storage requires ALLOW_MANUAL_HOSTPATH_STORAGE: true and is not for production"
    );
  }

  if ('manual' === storageClass) {
    // Add in the persistent volume configs to the hcce.yaml file
    const persistent_volumes_template = utils.readTemplate("/generate_script", "persistent_volumes.yam");
    const replacedPersistentVolumesContent = utils.replacePlaceholders(persistent_volumes_template, processedConfig);
    YAML.parseAllDocuments(replacedPersistentVolumesContent).forEach(doc => {
      yamlDocuments.splice(outputIdx++, 0, doc);
    });
  }

  const persistent_volume_claims_template = utils.readTemplate("/generate_script", "persistent_volume_claims.yam");
  const replacedPersistentVolumeClaimsContent = utils.replacePlaceholders(persistent_volume_claims_template, processedConfig);

  // Adds in the persistent volume claims to the hcce.yaml file
  YAML.parseAllDocuments(replacedPersistentVolumeClaimsContent).forEach(doc => {
    if ('default' !== storageClass) {
      doc = doc.toJS();
      doc.spec.storageClassName = storageClass;
      if ('manual' === storageClass) {
        // ReadWriteOncePod is only supported for CSI volumes
        doc.spec.accessModes = ["ReadWriteOnce"];
      }
      doc = new YAML.Document(doc);
    }
    yamlDocuments.splice(outputIdx++, 0, doc);
  });

  // update the volume specifications for pgsql and reticulum to point to the persistent volumes
  yamlDocuments.forEach((doc, index) => {
    const jsDoc = doc.toJS();
    if (jsDoc.kind === "Deployment" && jsDoc.metadata.name === "pgsql") {
      jsDoc.spec.template.spec.volumes[0] = {"name": "postgresql-data", "persistentVolumeClaim": {"claimName": "pgsql-pvc"}};
      yamlDocuments[index] = new YAML.Document(jsDoc);
    }
    if (jsDoc.kind === "Deployment" && jsDoc.metadata.name === "reticulum") {
      jsDoc.spec.template.spec.volumes[0] = {"name": "storage", "persistentVolumeClaim": {"claimName": "ret-pvc"}};
      yamlDocuments[index] = new YAML.Document(jsDoc);
    }
  });

  return `${yamlDocuments.map(doc => YAML.stringify(doc, {"lineWidth": 0, "directives": false})).join('---\n')}`;
}

function handleImageOverrides(processedConfig, replacedContent) {
  const yamlDocuments = YAML.parseAllDocuments(replacedContent);

  // Override the default images with custom ones if specified in the config
  yamlDocuments.forEach((doc, index) => {
    const jsDoc = doc.toJS();
    if (jsDoc.kind === "Deployment") {
      if (jsDoc.metadata.name === "reticulum") {
        let changed = false;
        if (processedConfig.OVERRIDE_RETICULUM_IMAGE) {
          jsDoc.spec.template.spec.containers[0].image = processedConfig.OVERRIDE_RETICULUM_IMAGE;
          changed = true;
        }
        if (processedConfig.OVERRIDE_POSTGREST_IMAGE) {
          jsDoc.spec.template.spec.containers[1].image = processedConfig.OVERRIDE_POSTGREST_IMAGE;
          changed = true;
        }
        if (changed) {
          yamlDocuments[index] = new YAML.Document(jsDoc);
        }
      }
      else if (jsDoc.metadata.name === "pgsql") {
        if (processedConfig.OVERRIDE_POSTGRES_IMAGE) {
          jsDoc.spec.template.spec.containers[0].image = processedConfig.OVERRIDE_POSTGRES_IMAGE;
          yamlDocuments[index] = new YAML.Document(jsDoc);
        }
      }
      else if (jsDoc.metadata.name === "pgbouncer" || jsDoc.metadata.name === "pgbouncer-t") {
        if (processedConfig.OVERRIDE_PGBOUNCER_IMAGE) {
          jsDoc.spec.template.spec.containers[0].image = processedConfig.OVERRIDE_PGBOUNCER_IMAGE;
          yamlDocuments[index] = new YAML.Document(jsDoc);
        }
      }
      else if (jsDoc.metadata.name === "hubs") {
        if (processedConfig.OVERRIDE_HUBS_IMAGE) {
          jsDoc.spec.template.spec.containers[0].image = processedConfig.OVERRIDE_HUBS_IMAGE;
          yamlDocuments[index] = new YAML.Document(jsDoc);
        }
      }
      else if (jsDoc.metadata.name === "spoke") {
        if (processedConfig.OVERRIDE_SPOKE_IMAGE) {
          jsDoc.spec.template.spec.containers[0].image = processedConfig.OVERRIDE_SPOKE_IMAGE;
          yamlDocuments[index] = new YAML.Document(jsDoc);
        }
      }
      else if (jsDoc.metadata.name === "nearspark") {
        if (processedConfig.OVERRIDE_NEARSPARK_IMAGE) {
          jsDoc.spec.template.spec.containers[0].image = processedConfig.OVERRIDE_NEARSPARK_IMAGE;
          yamlDocuments[index] = new YAML.Document(jsDoc);
        }
      }
      else if (jsDoc.metadata.name === "photomnemonic") {
        if (processedConfig.OVERRIDE_PHOTOMNEMONIC_IMAGE) {
          jsDoc.spec.template.spec.containers[0].image = processedConfig.OVERRIDE_PHOTOMNEMONIC_IMAGE;
          yamlDocuments[index] = new YAML.Document(jsDoc);
        }
      }
      else if (jsDoc.metadata.name === "dialog") {
        if (processedConfig.OVERRIDE_DIALOG_IMAGE) {
          jsDoc.spec.template.spec.containers[0].image = processedConfig.OVERRIDE_DIALOG_IMAGE;
          yamlDocuments[index] = new YAML.Document(jsDoc);
        }
      }
      else if (jsDoc.metadata.name === "coturn") {
        if (processedConfig.OVERRIDE_COTURN_IMAGE) {
          jsDoc.spec.template.spec.containers[0].image = processedConfig.OVERRIDE_COTURN_IMAGE;
          yamlDocuments[index] = new YAML.Document(jsDoc);
        }
      }
      else if (jsDoc.metadata.name === "haproxy") {
        if (processedConfig.OVERRIDE_HAPROXY_IMAGE) {
          jsDoc.spec.template.spec.containers[0].image = processedConfig.OVERRIDE_HAPROXY_IMAGE;
          yamlDocuments[index] = new YAML.Document(jsDoc);
        }
      }
      else if (jsDoc.metadata.name === "bot-orchestrator") {
        if (processedConfig.OVERRIDE_BOT_ORCHESTRATOR_IMAGE) {
          jsDoc.spec.template.spec.containers[0].image = processedConfig.OVERRIDE_BOT_ORCHESTRATOR_IMAGE;
          yamlDocuments[index] = new YAML.Document(jsDoc);
        }
      }
    }
  });

  return `${yamlDocuments.map(doc => YAML.stringify(doc, {"lineWidth": 0, "directives": false})).join('---\n')}`;
}

function handleRunnerActivation(processedConfig, replacedContent) {
  const yamlDocuments = YAML.parseAllDocuments(replacedContent);
  const resources = yamlDocuments.map(document => document.toJS());
  for (let index = 0; index < resources.length; index += 1) {
    const resource = resources[index];
    if (
      (processedConfig.BOT_RUNNER_ACTIVATION_PHASE === "bootstrap" ||
        processedConfig.BOT_RUNNER_RECOVERY_PHASE === "restore-fence") &&
      resource?.kind === "Role" &&
      resource?.metadata?.namespace === "hcce-bot-runners" &&
      resource?.metadata?.name === "bot-orchestrator-runner-pods"
    ) {
      resource.rules = [];
      yamlDocuments[index] = new YAML.Document(resource);
    }
  }

  const priority = resource => {
    const kind = resource?.kind;
    const name = resource?.metadata?.name;
    const namespace = resource?.metadata?.namespace || "";
    if (kind === "Namespace" && name !== "hcce-bot-runners") return 0;
    if (kind === "Namespace" && name === "hcce-bot-runners") return 1;
    if (kind === "Secret") return 2;
    if (kind === "ValidatingAdmissionPolicy" && name === "yenhubs-runner-cutover-journal-v2") return 2;
    if (kind === "ValidatingAdmissionPolicyBinding" && name === "yenhubs-runner-cutover-journal-v2") return 3;
    if (kind === "ValidatingAdmissionPolicy" && name === "bot-orchestrator-fence-protocol.yenhubs.org") return 4;
    if (kind === "ValidatingAdmissionPolicyBinding" && name === "bot-orchestrator-fence-protocol.yenhubs.org") return 5;
    if (kind === "ValidatingAdmissionPolicy" && name === "bot-runner-durable-protocol.yenhubs.org") return 8;
    if (kind === "ValidatingAdmissionPolicyBinding" && name === "bot-runner-durable-protocol.yenhubs.org") return 9;
    if (kind === "ValidatingAdmissionPolicy" && name === "bot-runner-pods.yenhubs.org") return 10;
    if (kind === "ValidatingAdmissionPolicyBinding" && name === "bot-runner-pods.yenhubs.org") return 11;
    if (["ServiceAccount", "ResourceQuota", "NetworkPolicy"].includes(kind) &&
        (namespace === "hcce-bot-runners" || name === "bot-orchestrator")) return 12;
    if (kind === "Role" && namespace !== "hcce-bot-runners" && name === "bot-orchestrator-runner-pods") return 20;
    if (kind === "RoleBinding" && namespace !== "hcce-bot-runners" && name === "bot-orchestrator-runner-pods") return 21;
    if (kind === "Role" && namespace === "hcce-bot-runners" && name === "bot-orchestrator-runner-pods") return 22;
    if (kind === "RoleBinding" && namespace === "hcce-bot-runners" && name === "bot-orchestrator-runner-pods") return 23;
    if (kind === "Deployment" && name === "bot-orchestrator") return 30;
    return 100;
  };
  return yamlDocuments
    .map((document, index) => ({ document, index, priority: priority(document.toJS()) }))
    .sort((left, right) => left.priority - right.priority || left.index - right.index)
    .map(({ document }) => YAML.stringify(document, { lineWidth: 0, directives: false }))
    .join("---\n");
}

// Main function to handle the script
function main() {
  try {
    const { inputPath, outputPath } = generationOverrides();
    // Values are already parsed YAML scalars. Do not reinterpret user-provided
    // strings as templates: a secret containing `$NAME` must remain literal.
    const processedConfig = utils.readConfig(inputPath);

    const runnerActivationPhase = String(
      processedConfig.BOT_RUNNER_ACTIVATION_PHASE || "bootstrap"
    );
    if (!["bootstrap", "admission", "active"].includes(runnerActivationPhase)) {
      throw new Error("BOT_RUNNER_ACTIVATION_PHASE must be exactly bootstrap, admission, or active");
    }
    processedConfig.BOT_RUNNER_ACTIVATION_PHASE = runnerActivationPhase;
    const runnerRecoveryPhase = String(processedConfig.BOT_RUNNER_RECOVERY_PHASE || "active");
    if (!["active", "restore-fence"].includes(runnerRecoveryPhase)) {
      throw new Error("BOT_RUNNER_RECOVERY_PHASE must be exactly active or restore-fence");
    }
    processedConfig.BOT_RUNNER_RECOVERY_PHASE = runnerRecoveryPhase;
    const recoveryIsActive = runnerRecoveryPhase === "active";
    processedConfig.RETICULUM_REPLICAS = recoveryIsActive ? 1 : 0;
    processedConfig.PGBOUNCER_REPLICAS = recoveryIsActive ? 1 : 0;
    processedConfig.PGBOUNCER_T_REPLICAS = recoveryIsActive ? 1 : 0;
    processedConfig.COTURN_REPLICAS = recoveryIsActive ? 1 : 0;
    processedConfig.BOT_ORCHESTRATOR_REPLICAS =
      recoveryIsActive && runnerActivationPhase === "active" ? 1 : 0;
    const runnerRecoveryEpoch = String(processedConfig.BOT_RUNNER_RECOVERY_EPOCH || "");
    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(runnerRecoveryEpoch)) {
      throw new Error("BOT_RUNNER_RECOVERY_EPOCH must be a canonical lowercase UUID v4");
    }
    processedConfig.BOT_RUNNER_RECOVERY_EPOCH = runnerRecoveryEpoch;

    // Backward compatibility for older local files that still use OPENAI.
    if (!processedConfig.OPENAI_API_KEY && processedConfig.OPENAI) {
      processedConfig.OPENAI_API_KEY = processedConfig.OPENAI;
    }

    if (!processedConfig.BOT_ACCESS_KEY) {
      processedConfig.BOT_ACCESS_KEY = crypto.randomBytes(32).toString("hex");
    } else if (String(processedConfig.BOT_ACCESS_KEY).length < 32) {
      throw new Error("BOT_ACCESS_KEY must contain at least 32 characters");
    }
    processedConfig.BOT_ACCESS_KEY = String(processedConfig.BOT_ACCESS_KEY);
    for (const keyName of [
      "BOT_RUNNER_ACCESS_KEY",
      "BOT_ORCHESTRATOR_ACCESS_KEY",
      "DASHBOARD_ACCESS_KEY"
    ]) {
      if (!processedConfig[keyName] || String(processedConfig[keyName]).length < 32) {
        throw new Error(`${keyName} must be configured independently with at least 32 characters`);
      }
      processedConfig[keyName] = String(processedConfig[keyName]);
    }
    const separatedAccessKeys = [
      "BOT_ACCESS_KEY",
      "BOT_RUNNER_ACCESS_KEY",
      "BOT_ORCHESTRATOR_ACCESS_KEY",
      "DASHBOARD_ACCESS_KEY"
    ];
    if (new Set(separatedAccessKeys.map(name => processedConfig[name])).size !== separatedAccessKeys.length) {
      throw new Error("Bot integration, runner, orchestrator and dashboard access keys must all be distinct");
    }
    for (const keyName of separatedAccessKeys) {
      processedConfig[`${keyName}_CHECKSUM`] = crypto
        .createHash("sha256")
        .update(processedConfig[keyName])
        .digest("hex");
    }
    processedConfig.DB_CREDENTIAL_CHECKSUM = crypto
      .createHash("sha256")
      .update(
        JSON.stringify({
          DB_USER: processedConfig.DB_USER,
          DB_PASS: processedConfig.DB_PASS,
          DB_NAME: processedConfig.DB_NAME,
          DB_HOST: processedConfig.DB_HOST,
          DB_HOST_T: processedConfig.DB_HOST_T,
          PGRST_DB_URI: processedConfig.PGRST_DB_URI,
          PSQL: processedConfig.PSQL
        })
      )
      .digest("hex");

    // Runner defaults (safe, reversible). These must exist because the template uses $VARS.
    if (!processedConfig.RUNNER_BACKEND) {
      processedConfig.RUNNER_BACKEND = "ghost";
    }
    if (String(processedConfig.RUNNER_BACKEND).toLowerCase().trim() !== "chromium") {
      processedConfig.RUNNER_BACKEND = "ghost";
    }
    if (!processedConfig.RUNNER_BACKEND_CANARY_HUBS) {
      processedConfig.RUNNER_BACKEND_CANARY_HUBS = "";
    }
    if (!processedConfig.GHOST_RUNNER_SCRIPT) {
      processedConfig.GHOST_RUNNER_SCRIPT = "/app/run-ghost-runner.js";
    }
    if (!processedConfig.GHOST_RAYCAST_MODE) {
      processedConfig.GHOST_RAYCAST_MODE = "spoke_colliders";
    }
    if (String(processedConfig.GHOST_NAVIGATION_MODE || "").toLowerCase().trim() !== "colliders") {
      processedConfig.GHOST_NAVIGATION_MODE = "navmesh_preferred";
    }
    processedConfig.GHOST_NAVIGATION_REQUIRE_NAVMESH =
      String(processedConfig.GHOST_NAVIGATION_REQUIRE_NAVMESH || "true").toLowerCase().trim() === "false"
        ? "false"
        : "true";
    if (!processedConfig.GHOST_NAVMESH_MAX_TRIANGLES) {
      processedConfig.GHOST_NAVMESH_MAX_TRIANGLES = "50000";
    }
    if (!processedConfig.GHOST_NAVMESH_MAX_ROUTE_POINTS) {
      processedConfig.GHOST_NAVMESH_MAX_ROUTE_POINTS = "64";
    }
    if (!processedConfig.GHOST_NAVMESH_MAX_SNAP_DISTANCE_M) {
      processedConfig.GHOST_NAVMESH_MAX_SNAP_DISTANCE_M = "3";
    }
    processedConfig.GHOST_FEATURED_FETCH_TIMEOUT_MS = "4000";
    processedConfig.GHOST_FEATURED_MAX_BYTES = "524288";
    processedConfig.GHOST_FEATURED_MAX_REDIRECTS = "2";
    processedConfig.GHOST_FEATURED_MAX_ENTRIES = "256";
    processedConfig.GHOST_FEATURED_MAX_REFS = "128";
    // Production generation is intentionally ghost-only. Chromium remains a
    // manual diagnostic tool and cannot authenticate safely from a browser.
    processedConfig.RUNNER_BACKEND = "ghost";
    processedConfig.RUNNER_BACKEND_CANARY_HUBS = "";
    processedConfig.OPENAI_MODEL = "gpt-5-nano";
    processedConfig.OPENAI_TOTAL_BUDGET_MS = "4000";
    const requestedMaxActiveRooms = Number(processedConfig.MAX_ACTIVE_ROOMS || 5);
    processedConfig.MAX_ACTIVE_ROOMS = String(
      Number.isFinite(requestedMaxActiveRooms) && requestedMaxActiveRooms > 0
        ? Math.min(Math.floor(requestedMaxActiveRooms), 10)
        : 5
    );
    processedConfig.MAX_BOTS_PER_ROOM = "10";
    processedConfig.BOT_RUNNER_IMAGE =
      processedConfig.OVERRIDE_BOT_RUNNER_IMAGE ||
      `${processedConfig.Container_Dockerhub_Username}/bot-runner:${processedConfig.Container_Tag}`;
    const botOrchestratorImage =
      processedConfig.OVERRIDE_BOT_ORCHESTRATOR_IMAGE ||
      `${processedConfig.Container_Dockerhub_Username}/bot-orchestrator:${processedConfig.Container_Tag}`;
    processedConfig.BOT_ORCHESTRATOR_IMAGE = botOrchestratorImage;
    const pullConfigBase64 = String(processedConfig.BOT_IMAGE_PULL_CONFIG_JSON_BASE64 || "").trim();
    try {
      verifyDockerConfigCredentials(pullConfigBase64, [botOrchestratorImage, processedConfig.BOT_RUNNER_IMAGE]);
    } catch (_error) {
      throw new Error(
        "BOT_IMAGE_PULL_CONFIG_JSON_BASE64 must contain canonical usable credentials for both bot image registries"
      );
    }
    processedConfig.BOT_IMAGE_PULL_CONFIG_JSON_BASE64 = pullConfigBase64;

    // Keep PERMS_KEY stable across runs. Rotate only when missing.
    let privateKey = normalizePemPrivateKey(processedConfig.PERMS_KEY);
    if (!privateKey) {
      ({ privateKey } = generateKeys());
    }

    const publicKey = crypto
      .createPublicKey(privateKey)
      .export({ type: "spki", format: "pem" });

    processedConfig.PGRST_JWT_SECRET = convertPemToJwk(publicKey);
    processedConfig.PERMS_KEY = privateKey.replace(/\n/g, "\\\\n");

    // generate the hcce.yaml file
    const template = utils.readTemplate("/generate_script", "hcce.yam");
    var replacedContent = utils.replacePlaceholders(template, processedConfig);

    if (processedConfig.GENERATE_PERSISTENT_VOLUMES !== true) {
      throw new Error(
        "GENERATE_PERSISTENT_VOLUMES must be true; untracked /tmp hostPath storage is not a supported deployment"
      );
    }
    replacedContent = generatePersistentVolumes(processedConfig, replacedContent);

    replacedContent = handleImageOverrides(processedConfig, replacedContent);
    replacedContent = handleRunnerActivation(processedConfig, replacedContent);

    utils.writeOutputFile(replacedContent, "", "hcce.yaml", outputPath);

  } catch (error) {
    console.error("Error in main function:", error);
    process.exitCode = 1;
  }
}

main();
