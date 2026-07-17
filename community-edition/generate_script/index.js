const crypto = require("crypto");
const path = require("path");
const YAML = require("yaml");
const pemJwk = require("pem-jwk");
const utils = require("../utils");

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
  processedConfig.PERSISTENT_VOLUME_STORAGE_CLASS ||= 'manual';

  if ('manual' === processedConfig.PERSISTENT_VOLUME_STORAGE_CLASS) {
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
    if ('default' !== processedConfig.PERSISTENT_VOLUME_STORAGE_CLASS) {
      doc = doc.toJS();
      doc.spec.storageClassName = processedConfig.PERSISTENT_VOLUME_STORAGE_CLASS;
      if ('manual' === processedConfig.PERSISTENT_VOLUME_STORAGE_CLASS) {
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

// Main function to handle the script
function main() {
  try {
    const { inputPath, outputPath } = generationOverrides();
    const config = utils.readConfig(inputPath);
    const processedConfig = YAML.parse(
      utils.replacePlaceholders(YAML.stringify(config), config),
      {"schema": "yaml-1.1"} // required to load yes/no as boolean values
    );

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
    processedConfig.BOT_ACCESS_KEY_CHECKSUM = crypto
      .createHash("sha256")
      .update(String(processedConfig.BOT_ACCESS_KEY))
      .digest("hex");
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

    if (processedConfig.GENERATE_PERSISTENT_VOLUMES) {
      replacedContent = generatePersistentVolumes(processedConfig, replacedContent);
    }

    replacedContent = handleImageOverrides(processedConfig, replacedContent);

    utils.writeOutputFile(replacedContent, "", "hcce.yaml", outputPath);

  } catch (error) {
    console.error("Error in main function:", error);
    process.exitCode = 1;
  }
}

main();
