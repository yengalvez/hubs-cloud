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

const EXPECTED_API_VERSION_BY_GROUP = Object.freeze({
  "": "v1",
  apps: "apps/v1",
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
  RET_INTERNAL_ENDPOINT: "http://ret:4001",
  RET_INTERNAL_PATH: "/api-internal/v1/hubs/configured_with_bots",
  RET_INTERNAL_ACCESS_HEADER: "x-ret-bot-runner-access-key",
  GHOST_RUNNER_SCRIPT: "/app/run-ghost-runner.js",
  GHOST_RAYCAST_MODE: "spoke_colliders",
  GHOST_NAVIGATION_MODE: "navmesh_preferred",
  GHOST_NAVIGATION_REQUIRE_NAVMESH: "true"
});

const BOT_ORCHESTRATOR_ALLOWED_ENV_NAMES = Object.freeze([
  "BOT_RUNNER_ACCESS_KEY",
  "BOT_ORCHESTRATOR_ACCESS_KEY",
  "OPENAI_API_KEY",
  "OPENAI_MODEL",
  "OPENAI_TOTAL_BUDGET_MS",
  "RUNNER_AUTOSTART",
  "RUNNER_BACKEND",
  "RUNNER_BACKEND_CANARY_HUBS",
  "RUNNER_SCRIPT",
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

function verifyExactIngressPolicy(policy, { name, targetApp, allowedApps, port }) {
  const expectedSpec = {
    podSelector: { matchLabels: { app: targetApp } },
    policyTypes: ["Ingress"],
    ingress: [
      {
        from: allowedApps.map(app => ({ podSelector: { matchLabels: { app } } })),
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
    namespaced("", "Secret", "configs"),
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
    { name: "BOT_RUNNER_ACCESS_KEY", key: "BOT_RUNNER_ACCESS_KEY" },
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
  if (namespaceResources.length !== 1) {
    return [`manifest must contain exactly one core Namespace (found ${namespaceResources.length})`];
  }

  const namespace = namespaceResources[0].metadata.name;
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
  if (!spec || spec.replicas !== 1) {
    errors.push("Deployment/bot-orchestrator must set replicas exactly to numeric 1");
  }

  const strategy = spec && spec.strategy;
  if (!hasExactOwnKeys(strategy, ["type"]) || strategy.type !== "Recreate") {
    errors.push(
      "Deployment/bot-orchestrator must set an exact strategy of type Recreate with no rolling-update fields"
    );
  }
  return errors;
}

function verifyBotOrchestratorRuntimeEnv(container) {
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

  const secretNames = new Set([
    "BOT_RUNNER_ACCESS_KEY",
    "BOT_ORCHESTRATOR_ACCESS_KEY",
    "OPENAI_API_KEY"
  ]);
  for (const entry of env) {
    if (!entry || secretNames.has(entry.name)) continue;
    if (!hasExactOwnKeys(entry, ["name", "value"]) || typeof entry.value !== "string") {
      errors.push(`bot-orchestrator ${entry.name || "<unnamed>"} must be one literal string env entry`);
    }
  }
  for (const [name, value] of Object.entries(BOT_ORCHESTRATOR_RUNTIME_ENV)) {
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
  return errors;
}

function verifyReticulumBotRunnerAuthorityContract(deployment) {
  const errors = [];
  const spec = deployment && deployment.spec;
  if (!spec || spec.replicas !== 1) {
    errors.push(
      "Deployment/reticulum must set replicas exactly to numeric 1 until multi-replica readiness, endpoints, and ret-pvc RWO placement are staged"
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
  EXPECTED_API_VERSION_BY_GROUP,
  HAPROXY_CLUSTER_ROLE,
  HAPROXY_CLUSTER_ROLE_RULES,
  apiGroup,
  expectedManifestInventory,
  findExactResource,
  hasExactOwnKeys,
  resourceIdentity,
  verifyAuditedDeploymentContainers,
  verifyBotOrchestratorContainers,
  verifyBotOrchestratorDeploymentContract,
  verifyBotOrchestratorIsolationContract,
  verifyBotOrchestratorRuntimeEnv,
  verifyBotOrchestratorSecurityContext,
  verifyBotOrchestratorSecretEnv,
  verifyExactIngressPolicy,
  verifyHaproxyClusterRole,
  verifyManifestResourceIdentities,
  verifyManifestResourceInventory,
  verifyNoYamlIndirections,
  verifyNoReticulumHorizontalPodAutoscaler,
  verifyReticulumBotRunnerAuthorityContract
};
