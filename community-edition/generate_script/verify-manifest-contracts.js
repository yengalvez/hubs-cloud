const crypto = require("node:crypto");
const fs = require("node:fs");
const path = require("node:path");
const YAML = require("yaml");

function hasExactOwnKeys(value, expectedKeys) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const actualKeys = Object.keys(value).sort();
  return JSON.stringify(actualKeys) === JSON.stringify([...expectedKeys].sort());
}

function exactStructuredValue(actual, expected) {
  if (Array.isArray(expected)) {
    return Array.isArray(actual) &&
      actual.length === expected.length &&
      expected.every((value, index) => exactStructuredValue(actual[index], value));
  }
  if (expected && typeof expected === "object") {
    return hasExactOwnKeys(actual, Object.keys(expected)) &&
      Object.entries(expected).every(([key, value]) => exactStructuredValue(actual[key], value));
  }
  return actual === expected;
}

function normalizedRegistryHost(value) {
  if (typeof value !== "string" || !value.trim()) return null;
  const raw = value.trim().toLowerCase();
  let host;
  try {
    host = raw.includes("://") ? new URL(raw).host : raw.split("/")[0];
  } catch (_error) {
    return null;
  }
  if (!host) return null;
  if (["index.docker.io", "registry-1.docker.io"].includes(host)) return "docker.io";
  return host;
}

function imageRegistryHost(image) {
  if (typeof image !== "string" || !image.trim() || image.includes("$")) return null;
  const reference = image.trim();
  const first = reference.split("/")[0].toLowerCase();
  if (reference.includes("/") && (first.includes(".") || first.includes(":") || first === "localhost")) {
    return normalizedRegistryHost(first);
  }
  return "docker.io";
}

function usableDockerRegistryCredential(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  if (
    typeof value.identitytoken === "string" &&
    value.identitytoken.trim() &&
    !/[\u0000-\u001f\u007f]/u.test(value.identitytoken)
  ) {
    return true;
  }
  if (typeof value.auth !== "string" || !value.auth) return false;
  try {
    const decoded = Buffer.from(value.auth, "base64");
    if (decoded.toString("base64") !== value.auth) return false;
    const credential = decoded.toString("utf8");
    const separator = credential.indexOf(":");
    return separator > 0 && separator < credential.length - 1 && !/[\u0000-\u001f\u007f]/u.test(credential);
  } catch (_error) {
    return false;
  }
}

function verifyDockerConfigCredentials(encoded, imageReferences = []) {
  if (typeof encoded !== "string" || !encoded) throw new Error("docker_config_base64_invalid");
  const decoded = Buffer.from(encoded, "base64");
  if (decoded.toString("base64") !== encoded) throw new Error("docker_config_base64_invalid");
  const parsed = JSON.parse(decoded.toString("utf8"));
  if (
    !parsed ||
    typeof parsed !== "object" ||
    Array.isArray(parsed) ||
    !parsed.auths ||
    typeof parsed.auths !== "object" ||
    Array.isArray(parsed.auths)
  ) {
    throw new Error("docker_config_auths_invalid");
  }
  const credentials = new Map();
  for (const [registry, credential] of Object.entries(parsed.auths)) {
    const host = normalizedRegistryHost(registry);
    if (host && usableDockerRegistryCredential(credential)) credentials.set(host, credential);
  }
  if (credentials.size === 0) throw new Error("docker_config_credentials_missing");
  const requiredRegistries = new Set(imageReferences.map(imageRegistryHost).filter(Boolean));
  for (const registry of requiredRegistries) {
    if (!credentials.has(registry)) throw new Error(`docker_config_registry_missing:${registry}`);
  }
  return parsed;
}

const EXPECTED_API_VERSION_BY_GROUP = Object.freeze({
  "": "v1",
  apps: "apps/v1",
  "admissionregistration.k8s.io": "admissionregistration.k8s.io/v1",
  "networking.k8s.io": "networking.k8s.io/v1",
  "rbac.authorization.k8s.io": "rbac.authorization.k8s.io/v1"
});

const BOT_ORCHESTRATOR_SECURITY_CONTEXT = Object.freeze({
  runAsNonRoot: true,
  runAsUser: 1000,
  runAsGroup: 1000,
  allowPrivilegeEscalation: false,
  readOnlyRootFilesystem: true,
  capabilities: Object.freeze({ drop: Object.freeze(["ALL"]) }),
  seccompProfile: Object.freeze({ type: "RuntimeDefault" })
});

const BOT_ORCHESTRATOR_RUNTIME_ENV = Object.freeze({
  RUNNER_AUTOSTART: "true",
  RUNNER_BACKEND: "ghost",
  RUNNER_BACKEND_CANARY_HUBS: "",
  RUNNER_SCRIPT: "/app/run-bot.js",
  RUNNER_POD_NAMESPACE: "hcce-bot-runners",
  RUNNER_CONTROL_URL: "http://bot-orchestrator.$Namespace.svc.cluster.local:5001",
  RUNNER_POD_RECONCILE_INTERVAL_MS: "5000",
  RUNNER_TOKEN_TTL_SECONDS: "3600",
  RUNNER_STARTUP_GRACE_MS: "180000",
  RET_INTERNAL_ENDPOINT: "http://ret:4001",
  RET_INTERNAL_PATH: "/api-internal/v1/hubs/configured_with_bots",
  RET_INTERNAL_ACCESS_HEADER: "x-ret-bot-orchestrator-access-key",
  GHOST_RUNNER_SCRIPT: "/app/run-ghost-runner.js",
  GHOST_RAYCAST_MODE: "spoke_colliders",
  GHOST_NAVIGATION_MODE: "navmesh_preferred",
  GHOST_NAVIGATION_REQUIRE_NAVMESH: "true"
});

const BOT_ORCHESTRATOR_ALLOWED_ENV_NAMES = Object.freeze([
  "BOT_ORCHESTRATOR_ACCESS_KEY",
  "OPENAI_API_KEY",
  "OPENAI_MODEL",
  "OPENAI_TOTAL_BUDGET_MS",
  "RUNNER_AUTOSTART",
  "RUNNER_BACKEND",
  "RUNNER_BACKEND_CANARY_HUBS",
  "RUNNER_SCRIPT",
  "BOT_RUNNER_IMAGE",
  "BOT_RUNNER_RECOVERY_EPOCH",
  "POD_NAMESPACE",
  "RUNNER_POD_NAMESPACE",
  "ORCHESTRATOR_POD_NAME",
  "ORCHESTRATOR_POD_UID",
  "RUNNER_CONTROL_URL",
  "RUNNER_POD_RECONCILE_INTERVAL_MS",
  "RUNNER_TOKEN_TTL_SECONDS",
  "RET_INTERNAL_ENDPOINT",
  "RET_INTERNAL_PATH",
  "RET_INTERNAL_ACCESS_HEADER",
  "GHOST_RUNNER_SCRIPT",
  "GHOST_RAYCAST_MODE",
  "GHOST_NAVIGATION_MODE",
  "GHOST_NAVIGATION_REQUIRE_NAVMESH",
  "GHOST_NAVIGATION_RECOVERY_RESTART_MS",
  "GHOST_SPAWN_RECOVERY_RESTART_MS",
  "GHOST_NAVMESH_MAX_TRIANGLES",
  "GHOST_NAVMESH_MAX_ROUTE_POINTS",
  "GHOST_NAVMESH_MAX_SNAP_DISTANCE_M",
  "GHOST_FEATURED_FETCH_TIMEOUT_MS",
  "GHOST_FEATURED_MAX_BYTES",
  "GHOST_FEATURED_MAX_REDIRECTS",
  "GHOST_FEATURED_MAX_ENTRIES",
  "GHOST_FEATURED_MAX_REFS",
  "GHOST_SCENE_FETCH_TIMEOUT_MS",
  "GHOST_SCENE_MAX_BYTES",
  "GHOST_SCENE_MAX_JSON_BYTES",
  "GHOST_SCENE_MAX_NODES",
  "GHOST_SCENE_MAX_EDGES",
  "HUBS_BASE_URL",
  "RET_SYNC_TIMEOUT_MS",
  "RET_SNAPSHOT_TTL_MS",
  "RUNNER_CONFIG_ACK_TIMEOUT_MS",
  "RUNNER_STARTUP_GRACE_MS",
  "RUNNER_STALE_RESTART_MS",
  "RUNNER_TERMINAL_RECOVERY_GRACE_MS",
  "RUNNER_WATCHDOG_INTERVAL_MS",
  "RUNNER_RESTART_BASE_MS",
  "RUNNER_RESTART_MAX_MS",
  "RUNNER_STABLE_RESET_MS",
  "RUNNER_TERMINATION_GRACE_MS",
  "RUNNER_KILL_GRACE_MS",
  "MAX_ACTIVE_ROOMS",
  "MAX_BOTS_PER_ROOM"
]);

const HAPROXY_CLUSTER_ROLE_RULES = Object.freeze([
  {
    apiGroups: [""],
    resources: [
      "configmaps",
      "nodes",
      "pods",
      "namespaces",
      "events",
      "serviceaccounts",
      "services",
      "endpoints"
    ],
    verbs: ["get", "list", "watch"]
  },
  {
    apiGroups: ["extensions", "networking.k8s.io"],
    resources: ["ingresses", "ingresses/status", "ingressclasses"],
    verbs: ["get", "list", "watch"]
  },
  {
    apiGroups: ["extensions", "networking.k8s.io"],
    resources: ["ingresses/status"],
    verbs: ["update"]
  },
  {
    apiGroups: [""],
    resources: ["secrets"],
    verbs: ["get", "list", "watch", "create", "patch", "update"]
  },
  {
    apiGroups: ["core.haproxy.org"],
    resources: ["*"],
    verbs: ["get", "list", "watch", "update"]
  },
  {
    apiGroups: ["apiextensions.k8s.io"],
    resources: ["customresourcedefinitions"],
    verbs: ["get", "list", "watch"]
  },
  {
    apiGroups: ["discovery.k8s.io"],
    resources: ["*"],
    verbs: ["get", "list", "watch"]
  },
  {
    apiGroups: ["gateway.networking.k8s.io"],
    resources: ["gateways", "gatewayclasses", "httproutes", "referencegrants", "tcproutes"],
    verbs: ["get", "list", "watch"]
  }
]);

const HAPROXY_CLUSTER_ROLE = Object.freeze({
  apiVersion: "rbac.authorization.k8s.io/v1",
  kind: "ClusterRole",
  metadata: Object.freeze({ name: "haproxy-cr" }),
  rules: HAPROXY_CLUSTER_ROLE_RULES
});

function apiGroup(apiVersion) {
  if (typeof apiVersion !== "string" || !apiVersion) return null;
  const parts = apiVersion.split("/");
  if (parts.length === 1 && parts[0]) return "";
  if (parts.length === 2 && parts[0] && parts[1]) return parts[0];
  return null;
}

function resourceIdentity(resource) {
  const group = apiGroup(resource && resource.apiVersion);
  const kind = resource && resource.kind;
  const metadata = resource && resource.metadata;
  const name = metadata && metadata.name;
  const rawNamespace = metadata && metadata.namespace;
  const namespace = rawNamespace === undefined || rawNamespace === null ? "" : rawNamespace;
  if (
    group === null ||
    typeof kind !== "string" ||
    !kind ||
    typeof name !== "string" ||
    !name ||
    typeof namespace !== "string"
  ) {
    return null;
  }
  return [group, kind, namespace, name];
}

function findExactResource(resources, group, kind, namespace, name) {
  return (Array.isArray(resources) ? resources : []).find(resource => {
    const identity = resourceIdentity(resource);
    return identity &&
      identity[0] === group &&
      identity[1] === kind &&
      identity[2] === namespace &&
      identity[3] === name;
  });
}

function canonicalizeUnorderedArrays(value) {
  if (Array.isArray(value)) {
    return value
      .map(canonicalizeUnorderedArrays)
      .sort((left, right) => JSON.stringify(left).localeCompare(JSON.stringify(right)));
  }
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map(key => [key, canonicalizeUnorderedArrays(value[key])])
    );
  }
  return value;
}

function verifyHaproxyClusterRole(clusterRole) {
  return exactStructuredValue(
    canonicalizeUnorderedArrays(clusterRole),
    canonicalizeUnorderedArrays(HAPROXY_CLUSTER_ROLE)
  )
    ? []
    : [
        "ClusterRole/haproxy-cr must exactly match the audited canonical rules with no extra capabilities"
      ];
}

function canonicalizeIngressSpec(spec) {
  if (!spec || typeof spec !== "object" || Array.isArray(spec)) return spec;
  const canonical = JSON.parse(JSON.stringify(spec));
  if (
    Array.isArray(canonical.ingress) &&
    canonical.ingress.length === 1 &&
    canonical.ingress[0] &&
    Array.isArray(canonical.ingress[0].from)
  ) {
    canonical.ingress[0].from.sort((left, right) =>
      JSON.stringify(left).localeCompare(JSON.stringify(right))
    );
  }
  return canonical;
}

function verifyExactIngressPolicy(policy, { name, targetApp, allowedApps = [], allowedPeers = null, port }) {
  const expectedSpec = {
    podSelector: { matchLabels: { app: targetApp } },
    policyTypes: ["Ingress"],
    ingress: [
      {
        from: allowedPeers || allowedApps.map(app => ({ podSelector: { matchLabels: { app } } })),
        ports: [{ protocol: "TCP", port }]
      }
    ]
  };

  return exactStructuredValue(
    canonicalizeIngressSpec(policy && policy.spec),
    canonicalizeIngressSpec(expectedSpec)
  )
    ? []
    : [
        `NetworkPolicy/${name} must exactly match its single audited ingress rule, peers and TCP port`
      ];
}

function expectedManifestInventory(namespace, { includeManualVolumes = false } = {}) {
  const namespaced = (group, kind, name) => [group, kind, namespace, name];
  const inventory = [
    ["", "Namespace", "", namespace],
    ["", "Namespace", "", "hcce-bot-runners"],
    namespaced("", "Secret", "configs"),
    namespaced("", "Secret", "bot-images-pull"),
    ["", "Secret", "hcce-bot-runners", "bot-images-pull"],
    namespaced("", "PersistentVolumeClaim", "pgsql-pvc"),
    namespaced("", "PersistentVolumeClaim", "ret-pvc"),
    namespaced("networking.k8s.io", "Ingress", "ret"),
    namespaced("networking.k8s.io", "Ingress", "dialog"),
    namespaced("networking.k8s.io", "Ingress", "nearspark"),
    namespaced("", "ConfigMap", "ret-config"),
    namespaced("apps", "Deployment", "reticulum"),
    namespaced("", "Service", "ret"),
    namespaced("apps", "Deployment", "bot-orchestrator"),
    namespaced("", "Service", "bot-orchestrator"),
    namespaced("", "ServiceAccount", "bot-orchestrator"),
    namespaced("rbac.authorization.k8s.io", "Role", "bot-orchestrator-runner-pods"),
    namespaced("rbac.authorization.k8s.io", "RoleBinding", "bot-orchestrator-runner-pods"),
    ["", "ServiceAccount", "hcce-bot-runners", "bot-runner"],
    ["", "ServiceAccount", "hcce-bot-runners", "bot-runner-guard"],
    ["", "ResourceQuota", "hcce-bot-runners", "bot-runner-capacity"],
    ["", "ResourceQuota", "hcce-bot-runners", "bot-runner-guard-capacity"],
    ["rbac.authorization.k8s.io", "Role", "hcce-bot-runners", "bot-orchestrator-runner-pods"],
    ["rbac.authorization.k8s.io", "RoleBinding", "hcce-bot-runners", "bot-orchestrator-runner-pods"],
    ["admissionregistration.k8s.io", "ValidatingAdmissionPolicy", "", "bot-runner-pods.yenhubs.org"],
    ["admissionregistration.k8s.io", "ValidatingAdmissionPolicyBinding", "", "bot-runner-pods.yenhubs.org"],
    ["admissionregistration.k8s.io", "ValidatingAdmissionPolicy", "", "bot-runner-durable-protocol.yenhubs.org"],
    ["admissionregistration.k8s.io", "ValidatingAdmissionPolicyBinding", "", "bot-runner-durable-protocol.yenhubs.org"],
    ["admissionregistration.k8s.io", "ValidatingAdmissionPolicy", "", "yenhubs-runner-cutover-journal-v2"],
    ["admissionregistration.k8s.io", "ValidatingAdmissionPolicyBinding", "", "yenhubs-runner-cutover-journal-v2"],
    ["admissionregistration.k8s.io", "ValidatingAdmissionPolicy", "", "bot-orchestrator-fence-protocol.yenhubs.org"],
    ["admissionregistration.k8s.io", "ValidatingAdmissionPolicyBinding", "", "bot-orchestrator-fence-protocol.yenhubs.org"],
    ["admissionregistration.k8s.io", "ValidatingAdmissionPolicy", "", "recovery-operation-pod-fence.yenhubs.org"],
    ["admissionregistration.k8s.io", "ValidatingAdmissionPolicyBinding", "", "recovery-operation-pod-fence.yenhubs.org"],
    namespaced("", "Service", "pgsql"),
    namespaced("apps", "Deployment", "pgsql"),
    namespaced("apps", "Deployment", "pgbouncer"),
    namespaced("", "Service", "pgbouncer"),
    namespaced("apps", "Deployment", "pgbouncer-t"),
    namespaced("", "Service", "pgbouncer-t"),
    namespaced("apps", "Deployment", "hubs"),
    namespaced("", "Service", "hubs"),
    namespaced("apps", "Deployment", "spoke"),
    namespaced("", "Service", "spoke"),
    namespaced("apps", "Deployment", "nearspark"),
    namespaced("", "Service", "nearspark"),
    namespaced("", "Service", "speelycaptor"),
    namespaced("apps", "Deployment", "photomnemonic"),
    namespaced("", "Service", "photomnemonic"),
    namespaced("apps", "Deployment", "dialog"),
    namespaced("", "Service", "dialog"),
    namespaced("apps", "Deployment", "coturn"),
    namespaced("", "Service", "coturn"),
    namespaced("", "ConfigMap", "haproxy-tcp-config"),
    namespaced("", "ConfigMap", "haproxy-config"),
    namespaced("apps", "Deployment", "haproxy"),
    namespaced("", "Service", "lb"),
    namespaced("", "ServiceAccount", "haproxy-sa"),
    ["rbac.authorization.k8s.io", "ClusterRole", "", "haproxy-cr"],
    ["rbac.authorization.k8s.io", "ClusterRoleBinding", "", "haproxy-rb"],
    namespaced("networking.k8s.io", "NetworkPolicy", "bot-orchestrator-ingress"),
    ["networking.k8s.io", "NetworkPolicy", "hcce-bot-runners", "bot-runner-default-deny"],
    ["networking.k8s.io", "NetworkPolicy", "hcce-bot-runners", "bot-runner-egress"],
    namespaced("networking.k8s.io", "NetworkPolicy", "pgsql-ingress"),
    namespaced("networking.k8s.io", "NetworkPolicy", "pgbouncer-ingress"),
    namespaced("networking.k8s.io", "NetworkPolicy", "pgbouncer-t-ingress"),
    namespaced("networking.k8s.io", "NetworkPolicy", "photomnemonic-ingress"),
    namespaced("networking.k8s.io", "NetworkPolicy", "photomnemonic-egress")
  ];
  if (includeManualVolumes) {
    inventory.splice(
      4,
      0,
      ["", "PersistentVolume", "", "pgsql-pv"],
      ["", "PersistentVolume", "", "ret-pv"]
    );
  }
  return inventory;
}

function verifyBotOrchestratorSecretEnv(container) {
  const errors = [];
  const env = Array.isArray(container && container.env) ? container.env : [];
  const contracts = [
    { name: "BOT_ORCHESTRATOR_ACCESS_KEY", key: "BOT_ORCHESTRATOR_ACCESS_KEY" },
    { name: "OPENAI_API_KEY", key: "OPENAI_API_KEY" }
  ];

  for (const contract of contracts) {
    const entries = env.filter(entry => entry && entry.name === contract.name);
    if (entries.length !== 1) {
      errors.push(
        `bot-orchestrator must define exactly one ${contract.name} environment entry ` +
        `(found ${entries.length})`
      );
      continue;
    }

    const entry = entries[0];
    const valueFrom = entry.valueFrom;
    const secretKeyRef = valueFrom && valueFrom.secretKeyRef;
    if (
      !hasExactOwnKeys(entry, ["name", "valueFrom"]) ||
      !hasExactOwnKeys(valueFrom, ["secretKeyRef"]) ||
      !hasExactOwnKeys(secretKeyRef, ["name", "key"]) ||
      secretKeyRef.name !== "configs" ||
      secretKeyRef.key !== contract.key
    ) {
      errors.push(
        `bot-orchestrator ${contract.name} must exclusively use ` +
        `valueFrom.secretKeyRef name=configs key=${contract.key} with no extra fields`
      );
    }
  }

  if (env.some(entry => entry?.name === "BOT_RUNNER_ACCESS_KEY")) {
    errors.push("bot-orchestrator must never receive BOT_RUNNER_ACCESS_KEY");
  }

  return errors;
}

function verifyBotOrchestratorSecurityContext(container) {
  return exactStructuredValue(
    container && container.securityContext,
    BOT_ORCHESTRATOR_SECURITY_CONTEXT
  )
    ? []
    : [
        "bot-orchestrator securityContext must exactly match the audited non-root, " +
        "read-only, no-escalation, drop-ALL and RuntimeDefault contract"
      ];
}

function verifyManifestResourceIdentities(resources) {
  const errors = [];
  const seen = new Map();

  for (const [index, resource] of (Array.isArray(resources) ? resources : []).entries()) {
    const identityParts = resourceIdentity(resource);
    if (!identityParts) {
      errors.push(`manifest resource ${index + 1} must have a complete apiVersion/kind/namespace/name identity`);
      continue;
    }

    const [group, kind, namespace, name] = identityParts;
    const identity = JSON.stringify(identityParts);
    if (seen.has(identity)) {
      errors.push(
        `manifest resource identity must be globally unique by API group: ${group || "core"}/${kind}/` +
        `${namespace}/${name}`
      );
    } else {
      seen.set(identity, index);
    }
  }

  for (const [kind, name] of [["Secret", "configs"], ["Deployment", "bot-orchestrator"]]) {
    const matches = (Array.isArray(resources) ? resources : []).filter(
      resource => resource && resource.kind === kind && resource.metadata && resource.metadata.name === name
    );
    if (matches.length !== 1) {
      errors.push(`manifest must contain exactly one ${kind}/${name} (found ${matches.length})`);
    }
  }

  return errors;
}

function verifyManifestResourceInventory(resources) {
  const errors = [];
  const namespaceResources = (Array.isArray(resources) ? resources : []).filter(resource => {
    const identity = resourceIdentity(resource);
    return identity && identity[0] === "" && identity[1] === "Namespace" && identity[2] === "";
  });
  const primaryNamespaces = namespaceResources.filter(
    resource => resource.metadata?.name !== "hcce-bot-runners"
  );
  if (namespaceResources.length !== 2 || primaryNamespaces.length !== 1) {
    return [
      `manifest must contain exactly one primary Namespace and hcce-bot-runners ` +
      `(found ${namespaceResources.length} total/${primaryNamespaces.length} primary)`
    ];
  }

  const namespace = primaryNamespaces[0].metadata.name;
  const includeManualVolumes = (Array.isArray(resources) ? resources : []).some(
    resource => resourceIdentity(resource)?.[1] === "PersistentVolume"
  );
  const expected = expectedManifestInventory(namespace, { includeManualVolumes });
  const expectedSet = new Set(expected.map(identity => JSON.stringify(identity)));
  const actual = (Array.isArray(resources) ? resources : []).map(resourceIdentity);
  const actualSet = new Set(actual.filter(Boolean).map(identity => JSON.stringify(identity)));

  for (const identity of expected) {
    if (!actualSet.has(JSON.stringify(identity))) {
      errors.push(`manifest inventory is missing ${identity.join("/")}`);
    }
  }
  for (const identity of actual.filter(Boolean)) {
    if (!expectedSet.has(JSON.stringify(identity))) {
      errors.push(`manifest inventory contains unexpected resource ${identity.join("/")}`);
    }
  }
  for (const resource of Array.isArray(resources) ? resources : []) {
    const identity = resourceIdentity(resource);
    if (!identity || !expectedSet.has(JSON.stringify(identity))) continue;
    const expectedApiVersion = EXPECTED_API_VERSION_BY_GROUP[identity[0]];
    if (resource.apiVersion !== expectedApiVersion) {
      errors.push(
        `manifest resource ${identity.join("/")} must use exact apiVersion ${expectedApiVersion} ` +
        `(found ${String(resource.apiVersion)})`
      );
    }
  }
  if (resources.length !== expected.length) {
    errors.push(`manifest must contain exactly ${expected.length} audited resources (found ${resources.length})`);
  }
  return errors;
}

const AUDITED_DEPLOYMENT_CONTAINERS = Object.freeze({
  reticulum: ["reticulum", "postgrest"],
  "bot-orchestrator": ["bot-orchestrator"],
  pgsql: ["postgresql"],
  pgbouncer: ["pgbouncer"],
  "pgbouncer-t": ["pgbouncer-t"],
  hubs: ["hubs"],
  spoke: ["spoke"],
  nearspark: ["nearspark"],
  photomnemonic: ["photomnemonic"],
  dialog: ["dialog"],
  coturn: ["coturn"],
  haproxy: ["haproxy"]
});

function verifyAuditedDeploymentContainers(resources, namespace) {
  const errors = [];
  for (const [deploymentName, expectedNames] of Object.entries(AUDITED_DEPLOYMENT_CONTAINERS)) {
    const deployment = findExactResource(resources, "apps", "Deployment", namespace, deploymentName);
    if (!deployment) {
      errors.push(`missing apps/Deployment/${namespace}/${deploymentName}`);
      continue;
    }
    const podSpec = deployment.spec && deployment.spec.template && deployment.spec.template.spec;
    const containers = podSpec && podSpec.containers;
    const actualNames = Array.isArray(containers)
      ? containers.map(container => container && container.name).sort()
      : [];
    if (JSON.stringify(actualNames) !== JSON.stringify([...expectedNames].sort())) {
      errors.push(
        `Deployment/${deploymentName} containers must be exactly ${expectedNames.join(", ")} ` +
        `(found ${actualNames.join(", ") || "none"})`
      );
    }
    for (const field of ["initContainers", "ephemeralContainers"]) {
      if (podSpec && Object.prototype.hasOwnProperty.call(podSpec, field)) {
        errors.push(`Deployment/${deploymentName} must not define ${field}`);
      }
    }
  }
  return errors;
}

function verifyBotOrchestratorContainers(deployment) {
  const containers = deployment && deployment.spec && deployment.spec.template &&
    deployment.spec.template.spec && deployment.spec.template.spec.containers;
  const matches = Array.isArray(containers)
    ? containers.filter(container => container && container.name === "bot-orchestrator")
    : [];
  return matches.length === 1
    ? []
    : [`Deployment/bot-orchestrator must contain exactly one bot-orchestrator container (found ${matches.length})`];
}

function verifyBotOrchestratorDeploymentContract(deployment) {
  const errors = [];
  const spec = deployment && deployment.spec;
  const phase = deployment?.metadata?.annotations?.["yenhubs.org/runner-activation-phase"];
  const recoveryPhase = deployment?.metadata?.annotations?.["yenhubs.org/bot-runner-recovery-phase"];
  const expectedReplicas = recoveryPhase === "restore-fence"
    ? 0
    : recoveryPhase === "active" && ["bootstrap", "admission"].includes(phase)
      ? 0
      : recoveryPhase === "active" && phase === "active" ? 1 : null;
  if (expectedReplicas === null || !spec || spec.replicas !== expectedReplicas) {
    errors.push(
      "Deployment/bot-orchestrator must be stopped by restore-fence and otherwise bind replicas to activation phase"
    );
  }

  const strategy = spec && spec.strategy;
  if (!hasExactOwnKeys(strategy, ["type"]) || strategy.type !== "Recreate") {
    errors.push(
      "Deployment/bot-orchestrator must set an exact strategy of type Recreate with no rolling-update fields"
    );
  }
  return errors;
}

function verifyBotRunnerRecoveryContract(resources, namespace) {
  const errors = [];
  const orchestrator = findExactResource(resources, "apps", "Deployment", namespace, "bot-orchestrator");
  const recoveryPhase = orchestrator?.metadata?.annotations?.["yenhubs.org/bot-runner-recovery-phase"];
  if (!["active", "restore-fence"].includes(recoveryPhase)) {
    return ["bot runner recovery phase must be exactly active or restore-fence"];
  }
  const expectedReplicas = recoveryPhase === "restore-fence" ? 0 : 1;
  for (const deploymentName of ["reticulum", "pgbouncer", "pgbouncer-t", "coturn"]) {
    const deployment = findExactResource(resources, "apps", "Deployment", namespace, deploymentName);
    if (
      deployment?.metadata?.annotations?.["yenhubs.org/bot-runner-recovery-phase"] !== recoveryPhase ||
      deployment?.spec?.replicas !== expectedReplicas
    ) {
      errors.push(
        `Deployment/${deploymentName} must bind recovery phase ${recoveryPhase} to replicas=${expectedReplicas}`
      );
    }
  }
  const pgsql = findExactResource(resources, "apps", "Deployment", namespace, "pgsql");
  if (
    pgsql?.metadata?.annotations?.["yenhubs.org/bot-runner-recovery-phase"] !== recoveryPhase ||
    pgsql?.spec?.replicas !== 1
  ) {
    errors.push("Deployment/pgsql must remain at replicas=1 and carry the exact recovery phase");
  }

  const epoch = orchestrator?.metadata?.annotations?.["yenhubs.org/bot-runner-recovery-epoch"];
  const uuidV4 = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
  const reticulum = findExactResource(resources, "apps", "Deployment", namespace, "reticulum");
  const orchestratorContainer = orchestrator?.spec?.template?.spec?.containers?.find(
    container => container?.name === "bot-orchestrator"
  );
  const reticulumContainer = reticulum?.spec?.template?.spec?.containers?.find(
    container => container?.name === "reticulum"
  );
  const literalEnv = (container, name) => {
    const entries = (container?.env || []).filter(entry => entry?.name === name);
    return entries.length === 1 && hasExactOwnKeys(entries[0], ["name", "value"])
      ? entries[0].value
      : null;
  };
  const epochBindings = [
    orchestrator?.spec?.template?.metadata?.annotations?.["yenhubs.org/bot-runner-recovery-epoch"],
    reticulum?.spec?.template?.metadata?.annotations?.["yenhubs.org/bot-runner-recovery-epoch"],
    literalEnv(orchestratorContainer, "BOT_RUNNER_RECOVERY_EPOCH"),
    literalEnv(reticulumContainer, "turkeyCfg_BOT_RUNNER_RECOVERY_EPOCH")
  ];
  if (!uuidV4.test(epoch || "") || epochBindings.some(value => value !== epoch)) {
    errors.push("Reticulum and bot-orchestrator must share one canonical recovery epoch in metadata and runtime");
  }
  return errors;
}

function verifyBotOrchestratorRuntimeEnv(container, namespace = "$Namespace") {
  const errors = [];
  const env = Array.isArray(container && container.env) ? container.env : [];
  const allowedNames = new Set(BOT_ORCHESTRATOR_ALLOWED_ENV_NAMES);
  const actualNames = env.map(entry => entry && entry.name);
  const uniqueActualNames = new Set(actualNames);
  const unexpectedNames = [...uniqueActualNames].filter(name => !allowedNames.has(name));
  const missingNames = BOT_ORCHESTRATOR_ALLOWED_ENV_NAMES.filter(name => !uniqueActualNames.has(name));
  if (
    env.length !== BOT_ORCHESTRATOR_ALLOWED_ENV_NAMES.length ||
    uniqueActualNames.size !== env.length ||
    unexpectedNames.length > 0 ||
    missingNames.length > 0
  ) {
    errors.push(
      "bot-orchestrator environment names must exactly match the audited allowlist " +
      `(unexpected=${unexpectedNames.join(",") || "none"}; missing=${missingNames.join(",") || "none"})`
    );
  }

  const secretNames = new Set(["BOT_ORCHESTRATOR_ACCESS_KEY", "OPENAI_API_KEY"]);
  const downwardApi = Object.freeze({
    POD_NAMESPACE: "metadata.namespace",
    ORCHESTRATOR_POD_NAME: "metadata.name",
    ORCHESTRATOR_POD_UID: "metadata.uid"
  });
  for (const entry of env) {
    if (!entry || secretNames.has(entry.name) || Object.hasOwn(downwardApi, entry.name)) continue;
    if (!hasExactOwnKeys(entry, ["name", "value"]) || typeof entry.value !== "string") {
      errors.push(`bot-orchestrator ${entry.name || "<unnamed>"} must be one literal string env entry`);
    }
  }
  for (const [name, fieldPath] of Object.entries(downwardApi)) {
    const matches = env.filter(entry => entry?.name === name);
    const expected = {
      name,
      valueFrom: { fieldRef: { apiVersion: "v1", fieldPath } }
    };
    if (matches.length !== 1 || !exactStructuredValue(matches[0], expected)) {
      errors.push(`bot-orchestrator ${name} must use the exact Downward API field ${fieldPath}`);
    }
  }
  const runnerImage = env.filter(entry => entry?.name === "BOT_RUNNER_IMAGE");
  if (
    runnerImage.length !== 1 ||
    !hasExactOwnKeys(runnerImage[0], ["name", "value"]) ||
    !(
      /^.+@sha256:[0-9a-f]{64}$/.test(runnerImage[0].value || "") ||
      runnerImage[0].value === "$BOT_RUNNER_IMAGE"
    )
  ) {
    errors.push("bot-orchestrator BOT_RUNNER_IMAGE must be one immutable sha256 digest reference");
  }
  const recoveryEpoch = env.filter(entry => entry?.name === "BOT_RUNNER_RECOVERY_EPOCH");
  if (
    recoveryEpoch.length !== 1 ||
    !hasExactOwnKeys(recoveryEpoch[0], ["name", "value"]) ||
    !(
      recoveryEpoch[0].value === "$BOT_RUNNER_RECOVERY_EPOCH" ||
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(
        recoveryEpoch[0].value || ""
      )
    )
  ) {
    errors.push("bot-orchestrator BOT_RUNNER_RECOVERY_EPOCH must be one canonical UUID v4 literal");
  }
  for (const [name, configuredValue] of Object.entries(BOT_ORCHESTRATOR_RUNTIME_ENV)) {
    const value = name === "RUNNER_CONTROL_URL"
      ? `http://bot-orchestrator.${namespace}.svc.cluster.local:5001`
      : configuredValue;
    const matches = Array.isArray(env) ? env.filter(entry => entry && entry.name === name) : [];
    if (
      matches.length !== 1 ||
      !hasExactOwnKeys(matches[0], ["name", "value"]) ||
      matches[0].value !== value
    ) {
      errors.push(
        `bot-orchestrator ${name} must have exactly one literal value equal to ${JSON.stringify(value)}`
      );
    }
  }
  return errors;
}

function verifyBotOrchestratorIsolationContract(deployment) {
  const errors = [];
  const podSpec = deployment?.spec?.template?.spec;
  const container = podSpec?.containers?.find(value => value?.name === "bot-orchestrator");
  const expectedMounts = [{ name: "bot-orchestrator-tmp", mountPath: "/tmp" }];
  const expectedVolumes = [{ name: "bot-orchestrator-tmp", emptyDir: { sizeLimit: "256Mi" } }];

  if (!exactStructuredValue(container?.volumeMounts, expectedMounts)) {
    errors.push("bot-orchestrator volumeMounts must be exactly writable emptyDir /tmp");
  }
  if (!exactStructuredValue(podSpec?.volumes, expectedVolumes)) {
    errors.push("Deployment/bot-orchestrator volumes must be exactly the bounded /tmp emptyDir");
  }
  for (const field of ["command", "args", "lifecycle", "envFrom", "volumeDevices"]) {
    if (container && Object.prototype.hasOwnProperty.call(container, field)) {
      errors.push(`bot-orchestrator must not define ${field}`);
    }
  }
  if (Array.isArray(podSpec?.initContainers) || Array.isArray(podSpec?.ephemeralContainers)) {
    errors.push("Deployment/bot-orchestrator must not define init or ephemeral containers");
  }
  if (
    podSpec?.serviceAccountName !== "bot-orchestrator" ||
    podSpec?.automountServiceAccountToken !== true ||
    !exactStructuredValue(podSpec?.imagePullSecrets, [{ name: "bot-images-pull" }])
  ) {
    errors.push("Deployment/bot-orchestrator must use its dedicated service account and image-pull Secret");
  }
  return errors;
}

function verifyBotImagePullSecret(resources, namespace) {
  const secret = findExactResource(resources, "", "Secret", namespace, "bot-images-pull");
  const runnerSecret = findExactResource(
    resources,
    "",
    "Secret",
    "hcce-bot-runners",
    "bot-images-pull"
  );
  if (
    !secret ||
    !hasExactOwnKeys(secret, ["apiVersion", "kind", "metadata", "type", "data"]) ||
    secret.apiVersion !== "v1" ||
    secret.kind !== "Secret" ||
    !exactStructuredValue(secret.metadata, { name: "bot-images-pull", namespace }) ||
    secret.type !== "kubernetes.io/dockerconfigjson" ||
    !hasExactOwnKeys(secret.data, [".dockerconfigjson"]) ||
    typeof secret.data[".dockerconfigjson"] !== "string"
  ) {
    return ["Secret/bot-images-pull must exactly define one Docker config JSON credential"];
  }
  if (
    !runnerSecret ||
    !hasExactOwnKeys(runnerSecret, ["apiVersion", "kind", "metadata", "type", "data"]) ||
    runnerSecret.apiVersion !== "v1" ||
    runnerSecret.kind !== "Secret" ||
    !exactStructuredValue(runnerSecret.metadata, {
      name: "bot-images-pull",
      namespace: "hcce-bot-runners"
    }) ||
    runnerSecret.type !== "kubernetes.io/dockerconfigjson" ||
    !hasExactOwnKeys(runnerSecret.data, [".dockerconfigjson"]) ||
    runnerSecret.data[".dockerconfigjson"] !== secret.data[".dockerconfigjson"]
  ) {
    return ["runner namespace Secret/bot-images-pull must be an exact copy of the parent pull credential"];
  }
  try {
    const encoded = secret.data[".dockerconfigjson"];
    const deployment = findExactResource(resources, "apps", "Deployment", namespace, "bot-orchestrator");
    const container = deployment?.spec?.template?.spec?.containers?.find(value => value?.name === "bot-orchestrator");
    const runnerImage = container?.env?.find(value => value?.name === "BOT_RUNNER_IMAGE")?.value;
    verifyDockerConfigCredentials(encoded, [container?.image, runnerImage]);
  } catch (_error) {
    return [
      "Secret/bot-images-pull Docker config must contain canonical credentials for both bot image registries"
    ];
  }
  return [];
}

function expectedBotRunnerControlPlaneResources(
  namespace,
  activationPhase = "active",
  recoveryPhase = "active"
) {
  if (!["bootstrap", "admission", "active"].includes(activationPhase)) {
    throw new Error("bot_runner_activation_phase_invalid");
  }
  if (!["active", "restore-fence"].includes(recoveryPhase)) {
    throw new Error("bot_runner_recovery_phase_invalid");
  }
  const runnerRoleRules = activationPhase === "bootstrap" || recoveryPhase === "restore-fence"
    ? []
    : [{ apiGroups: [""], resources: ["pods"], verbs: ["create", "delete", "get", "list", "patch"] }];
  return [
    {
      identity: ["", "Namespace", "", "hcce-bot-runners"],
      value: {
        apiVersion: "v1",
        kind: "Namespace",
        metadata: {
          name: "hcce-bot-runners",
          labels: {
            "pod-security.kubernetes.io/enforce": "restricted",
            "pod-security.kubernetes.io/enforce-version": "v1.34",
            "pod-security.kubernetes.io/audit": "restricted",
            "pod-security.kubernetes.io/audit-version": "v1.34",
            "pod-security.kubernetes.io/warn": "restricted",
            "pod-security.kubernetes.io/warn-version": "v1.34"
          }
        }
      }
    },
    {
      identity: ["", "ServiceAccount", namespace, "bot-orchestrator"],
      value: {
        apiVersion: "v1",
        kind: "ServiceAccount",
        metadata: { name: "bot-orchestrator", namespace },
        automountServiceAccountToken: true,
        imagePullSecrets: [{ name: "bot-images-pull" }]
      }
    },
    {
      identity: ["rbac.authorization.k8s.io", "Role", namespace, "bot-orchestrator-runner-pods"],
      value: {
        apiVersion: "rbac.authorization.k8s.io/v1",
        kind: "Role",
        metadata: {
          name: "bot-orchestrator-runner-pods",
          namespace,
          annotations: { "yenhubs.org/legacy-runner-authority": "neutralized" }
        },
        rules: []
      }
    },
    {
      identity: ["rbac.authorization.k8s.io", "RoleBinding", namespace, "bot-orchestrator-runner-pods"],
      value: {
        apiVersion: "rbac.authorization.k8s.io/v1",
        kind: "RoleBinding",
        metadata: {
          name: "bot-orchestrator-runner-pods",
          namespace,
          annotations: { "yenhubs.org/legacy-runner-authority": "neutralized" }
        },
        roleRef: {
          apiGroup: "rbac.authorization.k8s.io",
          kind: "Role",
          name: "bot-orchestrator-runner-pods"
        },
        subjects: [{ kind: "ServiceAccount", name: "bot-orchestrator", namespace }]
      }
    },
    {
      identity: ["", "ServiceAccount", "hcce-bot-runners", "bot-runner"],
      value: {
        apiVersion: "v1",
        kind: "ServiceAccount",
        metadata: { name: "bot-runner", namespace: "hcce-bot-runners" },
        automountServiceAccountToken: false,
        imagePullSecrets: [{ name: "bot-images-pull" }]
      }
    },
    {
      identity: ["", "ResourceQuota", "hcce-bot-runners", "bot-runner-capacity"],
      value: {
        apiVersion: "v1",
        kind: "ResourceQuota",
        metadata: { name: "bot-runner-capacity", namespace: "hcce-bot-runners" },
        spec: {
          scopes: ["NotBestEffort"],
          hard: {
            pods: "10",
            "requests.cpu": "250m",
            "requests.memory": "1280Mi",
            "limits.cpu": "5",
            "limits.memory": "5Gi"
          }
        }
      }
    },
    {
      identity: ["", "ServiceAccount", "hcce-bot-runners", "bot-runner-guard"],
      value: {
        apiVersion: "v1",
        kind: "ServiceAccount",
        metadata: { name: "bot-runner-guard", namespace: "hcce-bot-runners" },
        automountServiceAccountToken: false
      }
    },
    {
      identity: ["", "ResourceQuota", "hcce-bot-runners", "bot-runner-guard-capacity"],
      value: {
        apiVersion: "v1",
        kind: "ResourceQuota",
        metadata: { name: "bot-runner-guard-capacity", namespace: "hcce-bot-runners" },
        spec: {
          scopes: ["BestEffort"],
          hard: { pods: "100" }
        }
      }
    },
    {
      identity: ["rbac.authorization.k8s.io", "Role", "hcce-bot-runners", "bot-orchestrator-runner-pods"],
      value: {
        apiVersion: "rbac.authorization.k8s.io/v1",
        kind: "Role",
        metadata: {
          name: "bot-orchestrator-runner-pods",
          namespace: "hcce-bot-runners",
          annotations: {
            "yenhubs.org/runner-activation-phase": activationPhase,
            "yenhubs.org/bot-runner-recovery-phase": recoveryPhase
          }
        },
        rules: runnerRoleRules
      }
    },
    {
      identity: ["rbac.authorization.k8s.io", "RoleBinding", "hcce-bot-runners", "bot-orchestrator-runner-pods"],
      value: {
        apiVersion: "rbac.authorization.k8s.io/v1",
        kind: "RoleBinding",
        metadata: {
          name: "bot-orchestrator-runner-pods",
          namespace: "hcce-bot-runners",
          annotations: {
            "yenhubs.org/runner-activation-phase": activationPhase,
            "yenhubs.org/bot-runner-recovery-phase": recoveryPhase
          }
        },
        roleRef: {
          apiGroup: "rbac.authorization.k8s.io",
          kind: "Role",
          name: "bot-orchestrator-runner-pods"
        },
        subjects: [{ kind: "ServiceAccount", name: "bot-orchestrator", namespace }]
      }
    }
  ];
}

function verifyBotRunnerControlPlaneResources(resources, namespace) {
  const deployment = findExactResource(resources, "apps", "Deployment", namespace, "bot-orchestrator");
  const activationPhase = deployment?.metadata?.annotations?.["yenhubs.org/runner-activation-phase"];
  const recoveryPhase = deployment?.metadata?.annotations?.["yenhubs.org/bot-runner-recovery-phase"];
  let expected;
  try {
    expected = expectedBotRunnerControlPlaneResources(namespace, activationPhase, recoveryPhase);
  } catch (_error) {
    return [
      "bot runner phases must be activation=bootstrap/admission/active and recovery=active/restore-fence"
    ];
  }

  return expected.flatMap(({ identity, value }) => {
    const actual = findExactResource(resources, ...identity);
    return exactStructuredValue(
      canonicalizeUnorderedArrays(actual),
      canonicalizeUnorderedArrays(value)
    )
      ? []
      : [`${identity[1]}/${identity[3]} must exactly match the minimal runner-Pod RBAC contract`];
  });
}

function verifyBotRunnerDefaultDenyNetworkPolicy(policy) {
  const expected = {
    apiVersion: "networking.k8s.io/v1",
    kind: "NetworkPolicy",
    metadata: { name: "bot-runner-default-deny", namespace: "hcce-bot-runners" },
    spec: {
      podSelector: {},
      policyTypes: ["Ingress", "Egress"],
      ingress: [],
      egress: []
    }
  };
  return exactStructuredValue(
    canonicalizeUnorderedArrays(policy),
    canonicalizeUnorderedArrays(expected)
  )
    ? []
    : ["NetworkPolicy/bot-runner-default-deny must select every runner-namespace Pod and deny all traffic"];
}

function verifyBotRunnerNetworkPolicy(policy, parentNamespace = "$Namespace") {
  const expectedSpec = {
    podSelector: {
      matchLabels: {
        app: "bot-runner",
        "yenhubs.org/managed-by": "bot-orchestrator"
      }
    },
    policyTypes: ["Egress"],
    egress: [
      {
        to: [{
          namespaceSelector: {
            matchLabels: { "kubernetes.io/metadata.name": parentNamespace }
          },
          podSelector: { matchLabels: { app: "bot-orchestrator" } }
        }],
        ports: [{ protocol: "TCP", port: 5001 }]
      },
      {
        to: [
          {
            namespaceSelector: {
              matchLabels: { "kubernetes.io/metadata.name": "kube-system" }
            },
            podSelector: { matchLabels: { "k8s-app": "kube-dns" } }
          }
        ],
        ports: [
          { protocol: "UDP", port: 53 },
          { protocol: "TCP", port: 53 }
        ]
      },
      {
        to: [
          {
            ipBlock: {
              cidr: "0.0.0.0/0",
              except: [
                "0.0.0.0/8",
                "10.0.0.0/8",
                "100.64.0.0/10",
                "127.0.0.0/8",
                "169.254.0.0/16",
                "172.16.0.0/12",
                "192.0.0.0/24",
                "192.0.2.0/24",
                "192.168.0.0/16",
                "198.18.0.0/15",
                "198.51.100.0/24",
                "203.0.113.0/24",
                "224.0.0.0/4",
                "240.0.0.0/4"
              ]
            }
          }
        ],
        ports: [{ protocol: "TCP", port: 443 }]
      }
    ]
  };

  const expected = {
    apiVersion: "networking.k8s.io/v1",
    kind: "NetworkPolicy",
    metadata: { name: "bot-runner-egress", namespace: "hcce-bot-runners" },
    spec: expectedSpec
  };
  return exactStructuredValue(
    canonicalizeUnorderedArrays(policy),
    canonicalizeUnorderedArrays(expected)
  )
    ? []
    : ["NetworkPolicy/bot-runner-egress must exactly match the audited parent, DNS, and public-443 egress contract"];
}

const BOT_RUNNER_ADMISSION_TEMPLATE_SHA256 = "b6b46f6d9cfde523b231fddc6a1448722d0fe8836d269322ea4cc5710f568810";

function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.keys(value)
      .sort()
      .map(key => `${JSON.stringify(key)}:${canonicalJson(value[key])}`)
      .join(",")}}`;
  }
  return JSON.stringify(value);
}

function botRunnerAdmissionTemplateResources() {
  const templatePath = path.resolve(__dirname, "hcce.yam");
  const resources = YAML.parseAllDocuments(fs.readFileSync(templatePath, "utf8"))
    .map(document => document.toJS())
    .filter(Boolean);
  return [
    findExactResource(
      resources,
      "admissionregistration.k8s.io",
      "ValidatingAdmissionPolicy",
      "",
      "bot-runner-pods.yenhubs.org"
    ),
    findExactResource(
      resources,
      "admissionregistration.k8s.io",
      "ValidatingAdmissionPolicyBinding",
      "",
      "bot-runner-pods.yenhubs.org"
    ),
    findExactResource(
      resources,
      "admissionregistration.k8s.io",
      "ValidatingAdmissionPolicy",
      "",
      "bot-runner-durable-protocol.yenhubs.org"
    ),
    findExactResource(
      resources,
      "admissionregistration.k8s.io",
      "ValidatingAdmissionPolicyBinding",
      "",
      "bot-runner-durable-protocol.yenhubs.org"
    ),
    findExactResource(
      resources,
      "admissionregistration.k8s.io",
      "ValidatingAdmissionPolicy",
      "",
      "yenhubs-runner-cutover-journal-v2"
    ),
    findExactResource(
      resources,
      "admissionregistration.k8s.io",
      "ValidatingAdmissionPolicyBinding",
      "",
      "yenhubs-runner-cutover-journal-v2"
    ),
    findExactResource(
      resources,
      "admissionregistration.k8s.io",
      "ValidatingAdmissionPolicy",
      "",
      "bot-orchestrator-fence-protocol.yenhubs.org"
    ),
    findExactResource(
      resources,
      "admissionregistration.k8s.io",
      "ValidatingAdmissionPolicyBinding",
      "",
      "bot-orchestrator-fence-protocol.yenhubs.org"
    ),
    findExactResource(
      resources,
      "admissionregistration.k8s.io",
      "ValidatingAdmissionPolicy",
      "",
      "recovery-operation-pod-fence.yenhubs.org"
    ),
    findExactResource(
      resources,
      "admissionregistration.k8s.io",
      "ValidatingAdmissionPolicyBinding",
      "",
      "recovery-operation-pod-fence.yenhubs.org"
    )
  ];
}

function botRunnerAdmissionRenderValues(resources, namespace) {
  const deployment = findExactResource(resources, "apps", "Deployment", namespace, "bot-orchestrator");
  const container = deployment?.spec?.template?.spec?.containers?.find(value => value?.name === "bot-orchestrator");
  const env = new Map((container?.env || []).map(entry => [entry?.name, entry?.value]));
  const hubsBaseUrl = env.get("HUBS_BASE_URL");
  if (typeof hubsBaseUrl !== "string" || !/^https:\/\/[^/]+$/.test(hubsBaseUrl)) {
    throw new Error("bot_runner_admission_hubs_base_url_invalid");
  }
  const values = {
    Namespace: namespace,
    BOT_ORCHESTRATOR_IMAGE: container?.image,
    HUB_DOMAIN: hubsBaseUrl.slice("https://".length),
    BOT_RUNNER_IMAGE: env.get("BOT_RUNNER_IMAGE"),
    BOT_RUNNER_ACTIVATION_PHASE:
      deployment?.metadata?.annotations?.["yenhubs.org/runner-activation-phase"],
    BOT_RUNNER_RECOVERY_PHASE:
      deployment?.metadata?.annotations?.["yenhubs.org/bot-runner-recovery-phase"]
  };
  for (const name of [
    "GHOST_FEATURED_FETCH_TIMEOUT_MS",
    "GHOST_FEATURED_MAX_BYTES",
    "GHOST_FEATURED_MAX_ENTRIES",
    "GHOST_FEATURED_MAX_REDIRECTS",
    "GHOST_FEATURED_MAX_REFS",
    "GHOST_NAVMESH_MAX_ROUTE_POINTS",
    "GHOST_NAVMESH_MAX_SNAP_DISTANCE_M",
    "GHOST_NAVMESH_MAX_TRIANGLES"
  ]) {
    values[name] = env.get(name);
  }
  if (Object.values(values).some(value => typeof value !== "string")) {
    throw new Error("bot_runner_admission_render_value_missing");
  }
  return values;
}

function renderAdmissionTemplateValue(value, replacements) {
  if (Array.isArray(value)) {
    return value.map(item => renderAdmissionTemplateValue(item, replacements));
  }
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).map(([key, item]) => [key, renderAdmissionTemplateValue(item, replacements)])
    );
  }
  if (typeof value !== "string" || !value.includes("$")) return value;
  return value.replace(/\$([A-Za-z_][A-Za-z0-9_]*)/g, (_match, name) => {
    if (!Object.hasOwn(replacements, name)) throw new Error(`bot_runner_admission_placeholder_missing:${name}`);
    return replacements[name];
  });
}

function expectedBotRunnerAdmissionResources(resources, namespace) {
  const templateResources = botRunnerAdmissionTemplateResources();
  const actualHash = crypto
    .createHash("sha256")
    .update(canonicalJson(templateResources))
    .digest("hex");
  if (actualHash !== BOT_RUNNER_ADMISSION_TEMPLATE_SHA256) {
    throw new Error("bot_runner_admission_template_hash_mismatch");
  }
  const replacements = botRunnerAdmissionRenderValues(resources, namespace);
  return templateResources.map(resource => {
    const rendered = renderAdmissionTemplateValue(resource, replacements);
    if (
      rendered?.kind === "ValidatingAdmissionPolicyBinding" &&
      rendered?.metadata?.name === "recovery-operation-pod-fence.yenhubs.org"
    ) {
      rendered.spec.matchResources.namespaceSelector =
        replacements.BOT_RUNNER_RECOVERY_PHASE === "restore-fence"
          ? {
              matchExpressions: [{
                key: "kubernetes.io/metadata.name",
                operator: "In",
                values: [namespace, "hcce-bot-runners"]
              }]
            }
          : {
              matchExpressions: [{
                key: "kubernetes.io/metadata.name",
                operator: "DoesNotExist"
              }]
            };
    }
    return rendered;
  });
}

function verifyBotRunnerAdmissionResources(resources, namespace) {
  let expected;
  try {
    expected = expectedBotRunnerAdmissionResources(resources, namespace);
  } catch (_error) {
    return ["runner ValidatingAdmissionPolicy contract or its audited render inputs are invalid"];
  }
  return expected.flatMap(resource => {
    const identity = resourceIdentity(resource);
    const actual = findExactResource(resources, ...identity);
    return exactStructuredValue(actual, resource)
      ? []
      : [`${identity[1]}/${identity[3]} must exactly match the audited fail-closed admission contract`];
  });
}

function verifyReticulumBotRunnerAuthorityContract(deployment) {
  const errors = [];
  const spec = deployment && deployment.spec;
  const recoveryPhase = deployment?.metadata?.annotations?.["yenhubs.org/bot-runner-recovery-phase"];
  const expectedReplicas = recoveryPhase === "active"
    ? 1
    : recoveryPhase === "restore-fence" ? 0 : null;
  if (!spec || expectedReplicas === null || spec.replicas !== expectedReplicas) {
    errors.push(
      "Deployment/reticulum must use replicas=1 only in recovery active and replicas=0 in restore-fence"
    );
  }

  const strategy = spec && spec.strategy;
  if (!hasExactOwnKeys(strategy, ["type"]) || strategy.type !== "Recreate") {
    errors.push(
      "Deployment/reticulum must set an exact strategy of type Recreate with no rolling-update fields"
    );
  }

  return errors;
}

function verifyNoReticulumHorizontalPodAutoscaler(resources) {
  const autoscalers = (Array.isArray(resources) ? resources : []).filter(resource => {
    return apiGroup(resource && resource.apiVersion) === "autoscaling" &&
      resource?.kind === "HorizontalPodAutoscaler" &&
      resource?.spec?.scaleTargetRef?.name === "reticulum";
  });

  return autoscalers.map(resource => {
    const name = resource?.metadata?.name || "<unnamed>";
    return (
      `HorizontalPodAutoscaler/${name} must not target Reticulum while ` +
      "multi-replica readiness, endpoints, and ret-pvc RWO placement are unstaged"
    );
  });
}

function verifyNoYamlIndirections(documents, YAML) {
  let anchors = 0;
  let aliases = 0;
  let mergeKeys = 0;

  for (const document of Array.isArray(documents) ? documents : []) {
    YAML.visit(document, {
      Node(_key, node) {
        if (node && node.anchor) anchors += 1;
        if (YAML.isAlias(node)) aliases += 1;
      },
      Pair(_key, pair) {
        if (pair && pair.key && pair.key.value === "<<") mergeKeys += 1;
      }
    });
  }

  const errors = [];
  if (anchors > 0) errors.push(`manifest must not contain YAML anchors (found ${anchors})`);
  if (aliases > 0) errors.push(`manifest must not contain YAML aliases (found ${aliases})`);
  if (mergeKeys > 0) errors.push(`manifest must not contain YAML merge keys (found ${mergeKeys})`);
  return errors;
}

module.exports = {
  AUDITED_DEPLOYMENT_CONTAINERS,
  BOT_ORCHESTRATOR_ALLOWED_ENV_NAMES,
  BOT_ORCHESTRATOR_RUNTIME_ENV,
  BOT_ORCHESTRATOR_SECURITY_CONTEXT,
  BOT_RUNNER_ADMISSION_TEMPLATE_SHA256,
  EXPECTED_API_VERSION_BY_GROUP,
  HAPROXY_CLUSTER_ROLE,
  HAPROXY_CLUSTER_ROLE_RULES,
  apiGroup,
  expectedBotRunnerAdmissionResources,
  expectedBotRunnerControlPlaneResources,
  expectedManifestInventory,
  findExactResource,
  hasExactOwnKeys,
  resourceIdentity,
  verifyDockerConfigCredentials,
  verifyAuditedDeploymentContainers,
  verifyBotOrchestratorContainers,
  verifyBotOrchestratorDeploymentContract,
  verifyBotOrchestratorIsolationContract,
  verifyBotOrchestratorRuntimeEnv,
  verifyBotOrchestratorSecurityContext,
  verifyBotOrchestratorSecretEnv,
  verifyBotImagePullSecret,
  verifyBotRunnerAdmissionResources,
  verifyBotRunnerControlPlaneResources,
  verifyBotRunnerDefaultDenyNetworkPolicy,
  verifyBotRunnerNetworkPolicy,
  verifyBotRunnerRecoveryContract,
  verifyExactIngressPolicy,
  verifyHaproxyClusterRole,
  verifyManifestResourceIdentities,
  verifyManifestResourceInventory,
  verifyNoYamlIndirections,
  verifyNoReticulumHorizontalPodAutoscaler,
  verifyReticulumBotRunnerAuthorityContract
};
