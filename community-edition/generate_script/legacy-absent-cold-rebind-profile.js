const crypto = require("node:crypto");
const YAML = require("yaml");

const LEGACY_ABSENT_COLD_REBIND_PROFILE = "cold-rebind-legacy-absent-v1";

const DURABLE_ADMISSION_NAMES = new Set([
  "bot-runner-pods.yenhubs.org",
  "bot-runner-durable-protocol.yenhubs.org",
  "yenhubs-runner-cutover-journal-v2",
  "bot-orchestrator-fence-protocol.yenhubs.org",
  "recovery-operation-pod-fence.yenhubs.org"
]);

const DEPLOYMENT_CONTAINERS = Object.freeze({
  "bot-orchestrator": ["bot-orchestrator"],
  coturn: ["coturn"],
  dialog: ["dialog"],
  haproxy: ["haproxy"],
  hubs: ["hubs"],
  nearspark: ["nearspark"],
  pgbouncer: ["pgbouncer"],
  "pgbouncer-t": ["pgbouncer-t"],
  photomnemonic: ["photomnemonic"],
  pgsql: ["postgresql"],
  reticulum: ["postgrest", "reticulum"],
  spoke: ["spoke"]
});

const WRITER_DEPLOYMENTS = Object.freeze([
  "reticulum",
  "pgbouncer",
  "pgbouncer-t",
  "bot-orchestrator",
  "coturn"
]);

const FORBIDDEN_PARENT_ENV = new Set([
  "BOT_RUNNER_ACCESS_KEY",
  "BOT_ORCHESTRATOR_ACCESS_KEY",
  "DASHBOARD_ACCESS_KEY",
  "BOT_RUNNER_IMAGE",
  "BOT_RUNNER_RECOVERY_EPOCH",
  "POD_NAMESPACE",
  "ORCHESTRATOR_POD_NAME",
  "ORCHESTRATOR_POD_UID",
  "RUNNER_NAMESPACE",
  "RUNNER_POD_NAMESPACE",
  "RUNNER_CONTROL_URL"
]);

const FORBIDDEN_RETICULUM_ENV = new Set([
  "turkeyCfg_BOT_RUNNER_ACCESS_KEY",
  "turkeyCfg_BOT_ORCHESTRATOR_ACCESS_KEY",
  "turkeyCfg_BOT_RUNNER_RECOVERY_EPOCH",
  "turkeyCfg_DASHBOARD_ACCESS_KEY"
]);

const FORBIDDEN_RUNNER_ANNOTATIONS = new Set([
  "yenhubs.org/runner-clean-install",
  "yenhubs.org/runner-fence-protocol",
  "yenhubs.org/runner-activation-phase",
  "yenhubs.org/bot-runner-recovery-phase",
  "yenhubs.org/bot-runner-recovery-epoch",
  "yenhubs.org/bot-runner-access-key-checksum",
  "yenhubs.org/bot-orchestrator-access-key-checksum",
  "yenhubs.org/dashboard-access-key-checksum"
]);

function targetProfileFromEnvironment(environment = process.env) {
  if (!Object.prototype.hasOwnProperty.call(environment, "HCCE_TARGET_PROFILE")) return null;
  const requested = String(environment.HCCE_TARGET_PROFILE || "").trim();
  if (requested === LEGACY_ABSENT_COLD_REBIND_PROFILE) return requested;
  throw new Error(
    `HCCE_TARGET_PROFILE must be unset or exactly ${LEGACY_ABSENT_COLD_REBIND_PROFILE}`
  );
}

function apiGroup(apiVersion) {
  if (typeof apiVersion !== "string" || !apiVersion) return null;
  const parts = apiVersion.split("/");
  if (parts.length === 1) return "";
  return parts.length === 2 ? parts[0] : null;
}

function resourceIdentity(resource) {
  return [
    apiGroup(resource?.apiVersion),
    resource?.kind,
    resource?.metadata?.namespace || "",
    resource?.metadata?.name
  ];
}

function isLegacyRemovedIdentity(identity, primaryNamespace) {
  const [group, kind, namespace, name] = identity;
  if (kind === "Namespace" && name === "hcce-bot-runners") return true;
  if (namespace === "hcce-bot-runners") return true;
  if (
    namespace === primaryNamespace &&
    name === "bot-orchestrator" &&
    ["ServiceAccount"].includes(kind)
  ) return true;
  if (
    group === "rbac.authorization.k8s.io" &&
    namespace === primaryNamespace &&
    name === "bot-orchestrator-runner-pods" &&
    ["Role", "RoleBinding"].includes(kind)
  ) return true;
  if (
    group === "admissionregistration.k8s.io" &&
    ["ValidatingAdmissionPolicy", "ValidatingAdmissionPolicyBinding"].includes(kind) &&
    DURABLE_ADMISSION_NAMES.has(name)
  ) return true;
  return group === "networking.k8s.io" &&
    kind === "NetworkPolicy" && namespace === primaryNamespace &&
    name === "bot-orchestrator-ingress";
}

function findResource(resources, kind, namespace, name) {
  const matches = resources.filter(resource =>
    resource?.kind === kind &&
    (resource?.metadata?.namespace || "") === namespace &&
    resource?.metadata?.name === name
  );
  if (matches.length !== 1) {
    throw new Error(`legacy profile precondition requires exactly one ${kind}/${namespace}/${name}`);
  }
  return matches[0];
}

function exactContainerMap(resources, namespace) {
  const deployments = resources.filter(resource =>
    resource?.apiVersion === "apps/v1" &&
    resource?.kind === "Deployment" &&
    resource?.metadata?.namespace === namespace
  );
  if (deployments.length !== Object.keys(DEPLOYMENT_CONTAINERS).length) {
    throw new Error("legacy profile precondition requires exactly twelve Deployments");
  }
  for (const [name, expected] of Object.entries(DEPLOYMENT_CONTAINERS)) {
    const deployment = findResource(resources, "Deployment", namespace, name);
    const actual = (deployment.spec?.template?.spec?.containers || [])
      .map(container => container?.name)
      .sort();
    if (JSON.stringify(actual) !== JSON.stringify([...expected].sort())) {
      throw new Error(`legacy profile precondition rejected Deployment/${name} container drift`);
    }
  }
}

function expectedImageMap(processedConfig) {
  if (processedConfig.OVERRIDE_BOT_RUNNER_IMAGE !== "No") {
    throw new Error("legacy target requires OVERRIDE_BOT_RUNNER_IMAGE to be exactly No");
  }
  return {
    "bot-orchestrator/bot-orchestrator": processedConfig.OVERRIDE_BOT_ORCHESTRATOR_IMAGE,
    "coturn/coturn": processedConfig.OVERRIDE_COTURN_IMAGE,
    "dialog/dialog": processedConfig.OVERRIDE_DIALOG_IMAGE,
    "haproxy/haproxy": processedConfig.OVERRIDE_HAPROXY_IMAGE,
    "hubs/hubs": processedConfig.OVERRIDE_HUBS_IMAGE,
    "nearspark/nearspark": processedConfig.OVERRIDE_NEARSPARK_IMAGE,
    "pgbouncer/pgbouncer": processedConfig.OVERRIDE_PGBOUNCER_IMAGE,
    "pgbouncer-t/pgbouncer-t": processedConfig.OVERRIDE_PGBOUNCER_IMAGE,
    "photomnemonic/photomnemonic": processedConfig.OVERRIDE_PHOTOMNEMONIC_IMAGE,
    "pgsql/postgresql": processedConfig.OVERRIDE_POSTGRES_IMAGE,
    "reticulum/postgrest": processedConfig.OVERRIDE_POSTGREST_IMAGE,
    "reticulum/reticulum": processedConfig.OVERRIDE_RETICULUM_IMAGE,
    "spoke/spoke": processedConfig.OVERRIDE_SPOKE_IMAGE
  };
}

function deploymentImageMap(resources, namespace) {
  const pairs = {};
  for (const deployment of resources.filter(resource =>
    resource?.apiVersion === "apps/v1" &&
    resource?.kind === "Deployment" &&
    resource?.metadata?.namespace === namespace
  )) {
    for (const container of deployment.spec?.template?.spec?.containers || []) {
      pairs[`${deployment.metadata.name}/${container.name}`] = container.image;
    }
  }
  return Object.fromEntries(Object.entries(pairs).sort(([left], [right]) => left.localeCompare(right)));
}

function imageMapSha256(imageMap) {
  return crypto.createHash("sha256").update(JSON.stringify(imageMap)).digest("hex");
}

function removeRunnerAnnotations(metadata) {
  if (!metadata?.annotations) return;
  for (const name of FORBIDDEN_RUNNER_ANNOTATIONS) delete metadata.annotations[name];
  if (Object.keys(metadata.annotations).length === 0) delete metadata.annotations;
}

function replaceExactlyOnce(value, expected, replacement, contract) {
  const first = value.indexOf(expected);
  if (first < 0 || value.indexOf(expected, first + expected.length) >= 0) {
    throw new Error(`legacy profile precondition rejected ${contract} drift`);
  }
  return value.slice(0, first) + replacement + value.slice(first + expected.length);
}

function transformReticulumConfig(configMap) {
  let text = configMap?.data?.["config.toml.template"];
  if (typeof text !== "string") {
    throw new Error("legacy profile precondition requires ConfigMap/ret-config template");
  }
  text = replaceExactlyOnce(
    text,
    'dashboard_access_key = "<DASHBOARD_ACCESS_KEY>"',
    'dashboard_access_key = "<BOT_ACCESS_KEY>"',
    "dashboard access binding"
  );
  text = replaceExactlyOnce(
    text,
    'header_value = "<DASHBOARD_ACCESS_KEY>"',
    'header_value = "<BOT_ACCESS_KEY>"',
    "legacy header access binding"
  );
  for (const line of [
    'bot_runner_access_key = "<BOT_RUNNER_ACCESS_KEY>"\n',
    'bot_orchestrator_access_key = "<BOT_ORCHESTRATOR_ACCESS_KEY>"\n',
    'bot_runner_recovery_epoch = "<BOT_RUNNER_RECOVERY_EPOCH>"\n'
  ]) {
    text = replaceExactlyOnce(text, line, "", line.trim());
  }
  text = replaceExactlyOnce(
    text,
    '[ret."Elixir.Ret.BotOrchestrator"]\n' +
      'endpoint = "http://bot-orchestrator.<POD_NS>:5001"\n' +
      'access_key = "<BOT_ORCHESTRATOR_ACCESS_KEY>"\n\n',
    '[ret."Elixir.Ret.BotOrchestrator"]\n' +
      'endpoint = "http://bot-orchestrator.<POD_NS>:5001"\n' +
      'access_key = "<BOT_ACCESS_KEY>"\n\n',
    "Ret.BotOrchestrator section"
  );
  configMap.data["config.toml.template"] = text;
}

function applyLegacyAbsentColdRebindProfile(processedConfig, renderedManifest) {
  const documents = YAML.parseAllDocuments(renderedManifest);
  const parseErrors = documents.flatMap(document => document.errors);
  if (parseErrors.length > 0) throw parseErrors[0];
  let resources = documents.map(document => document.toJS()).filter(Boolean);
  const primaryNamespaces = resources.filter(resource =>
    resource?.apiVersion === "v1" && resource?.kind === "Namespace" &&
    resource?.metadata?.name !== "hcce-bot-runners"
  );
  if (primaryNamespaces.length !== 1) {
    throw new Error("legacy profile precondition requires exactly one target Namespace");
  }
  const namespace = primaryNamespaces[0].metadata.name;
  exactContainerMap(resources, namespace);

  const expectedImages = Object.fromEntries(
    Object.entries(expectedImageMap(processedConfig)).sort(([left], [right]) => left.localeCompare(right))
  );
  const currentImages = deploymentImageMap(resources, namespace);
  if (
    JSON.stringify(currentImages) !== JSON.stringify(expectedImages) ||
    Object.values(currentImages).some(image => !/^.+@sha256:[0-9a-f]{64}$/i.test(image || ""))
  ) {
    throw new Error("legacy profile requires all thirteen exact digest-pinned image overrides");
  }

  const currentIdentities = resources.map(resource => JSON.stringify(resourceIdentity(resource)));
  const requiredRemoved = [
    ["", "Namespace", "", "hcce-bot-runners"],
    ["", "Secret", "hcce-bot-runners", "bot-images-pull"],
    ["", "ServiceAccount", namespace, "bot-orchestrator"],
    ["rbac.authorization.k8s.io", "Role", namespace, "bot-orchestrator-runner-pods"],
    ["rbac.authorization.k8s.io", "RoleBinding", namespace, "bot-orchestrator-runner-pods"],
    ["networking.k8s.io", "NetworkPolicy", namespace, "bot-orchestrator-ingress"]
  ];
  for (const name of DURABLE_ADMISSION_NAMES) {
    requiredRemoved.push(
      ["admissionregistration.k8s.io", "ValidatingAdmissionPolicy", "", name],
      ["admissionregistration.k8s.io", "ValidatingAdmissionPolicyBinding", "", name]
    );
  }
  for (const identity of requiredRemoved) {
    if (currentIdentities.filter(value => value === JSON.stringify(identity)).length !== 1) {
      throw new Error(`legacy profile precondition requires durable identity ${identity.join("/")}`);
    }
  }
  resources = resources.filter(resource => !isLegacyRemovedIdentity(resourceIdentity(resource), namespace));

  const namespaceResource = findResource(resources, "Namespace", "", namespace);
  namespaceResource.metadata.annotations = {
    domain: processedConfig.HUB_DOMAIN,
    adm: processedConfig.ADM_EMAIL,
    "yenhubs.org/target-profile": LEGACY_ABSENT_COLD_REBIND_PROFILE,
    "yenhubs.org/target-image-map-sha256": imageMapSha256(expectedImages)
  };

  const configs = findResource(resources, "Secret", namespace, "configs");
  for (const name of ["BOT_RUNNER_ACCESS_KEY", "BOT_ORCHESTRATOR_ACCESS_KEY", "DASHBOARD_ACCESS_KEY"]) {
    if (!Object.prototype.hasOwnProperty.call(configs.stringData || {}, name)) {
      throw new Error(`legacy profile precondition requires Secret/configs ${name}`);
    }
    delete configs.stringData[name];
  }

  transformReticulumConfig(findResource(resources, "ConfigMap", namespace, "ret-config"));
  for (const deploymentName of Object.keys(DEPLOYMENT_CONTAINERS)) {
    const deployment = findResource(resources, "Deployment", namespace, deploymentName);
    removeRunnerAnnotations(deployment.metadata);
    removeRunnerAnnotations(deployment.spec?.template?.metadata);
    if (WRITER_DEPLOYMENTS.includes(deploymentName)) deployment.spec.replicas = 0;
    if (deploymentName === "pgsql") deployment.spec.replicas = 1;
    const usesGhcr = deployment.spec.template.spec.containers.some(container =>
      String(container?.image || "").toLowerCase().startsWith("ghcr.io/")
    );
    if (usesGhcr) {
      deployment.spec.template.spec.imagePullSecrets = [{ name: "bot-images-pull" }];
    } else {
      delete deployment.spec.template.spec.imagePullSecrets;
    }
  }

  const reticulum = findResource(resources, "Deployment", namespace, "reticulum");
  const reticulumContainer = reticulum.spec.template.spec.containers.find(
    container => container.name === "reticulum"
  );
  reticulumContainer.env = reticulumContainer.env.filter(entry =>
    !FORBIDDEN_RETICULUM_ENV.has(entry?.name)
  );

  const parent = findResource(resources, "Deployment", namespace, "bot-orchestrator");
  const podSpec = parent.spec.template.spec;
  if (podSpec.serviceAccountName !== "bot-orchestrator" || podSpec.automountServiceAccountToken !== true) {
    throw new Error("legacy profile precondition rejected bot parent service-account drift");
  }
  delete podSpec.serviceAccountName;
  podSpec.automountServiceAccountToken = false;
  const parentContainer = podSpec.containers[0];
  const botAccessEntry = {
    name: "BOT_ACCESS_KEY",
    valueFrom: { secretKeyRef: { name: "configs", key: "BOT_ACCESS_KEY" } }
  };
  parentContainer.env = [
    botAccessEntry,
    ...parentContainer.env.filter(entry => !FORBIDDEN_PARENT_ENV.has(entry?.name))
  ];

  const finalImages = deploymentImageMap(resources, namespace);
  if (JSON.stringify(finalImages) !== JSON.stringify(expectedImages)) {
    throw new Error("legacy profile transform changed the bound image inventory");
  }
  return resources
    .map(resource => YAML.stringify(resource, { lineWidth: 0, directives: false }))
    .join("---\n");
}

module.exports = {
  DEPLOYMENT_CONTAINERS,
  FORBIDDEN_PARENT_ENV,
  FORBIDDEN_RETICULUM_ENV,
  FORBIDDEN_RUNNER_ANNOTATIONS,
  LEGACY_ABSENT_COLD_REBIND_PROFILE,
  WRITER_DEPLOYMENTS,
  applyLegacyAbsentColdRebindProfile,
  deploymentImageMap,
  imageMapSha256,
  isLegacyRemovedIdentity,
  targetProfileFromEnvironment
};
