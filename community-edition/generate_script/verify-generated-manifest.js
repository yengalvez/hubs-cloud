const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const YAML = require("yaml");

const manifestPath = process.env.HCCE_MANIFEST_PATH
  ? path.resolve(process.env.HCCE_MANIFEST_PATH)
  : path.resolve(__dirname, "../hcce.yaml");
const errors = [];

function fail(message) {
  errors.push(message);
}

function findResource(resources, kind, name) {
  return resources.find(resource => resource && resource.kind === kind && resource.metadata?.name === name);
}

function hasRule(clusterRole, apiGroup, resource, requiredVerbs) {
  return (clusterRole?.rules || []).some(rule => {
    const groups = rule.apiGroups || [];
    const resources = rule.resources || [];
    const verbs = rule.verbs || [];
    return (
      groups.includes(apiGroup) &&
      (resources.includes(resource) || resources.includes("*")) &&
      requiredVerbs.every(verb => verbs.includes(verb) || verbs.includes("*"))
    );
  });
}

function isDigestPinnedImage(image) {
  return typeof image === "string" && /@sha256:[a-f0-9]{64}$/i.test(image);
}

function verifyIngressPolicy(resources, name, targetApp, allowedApps, port) {
  const policy = findResource(resources, "NetworkPolicy", name);
  if (!policy) {
    fail(`missing NetworkPolicy/${name}`);
    return;
  }
  if (policy.spec?.podSelector?.matchLabels?.app !== targetApp) {
    fail(`NetworkPolicy/${name} must select app=${targetApp}`);
  }
  const policyTypes = policy.spec?.policyTypes || [];
  if (!policyTypes.includes("Ingress") || policyTypes.includes("Egress")) {
    fail(`NetworkPolicy/${name} must isolate ingress only`);
  }

  const rules = policy.spec?.ingress || [];
  const peers = rules.flatMap(rule => rule.from || []);
  const actualApps = peers.map(peer => peer.podSelector?.matchLabels?.app).filter(Boolean).sort();
  const expectedApps = [...allowedApps].sort();
  if (
    peers.some(peer => !peer.podSelector || peer.namespaceSelector || peer.ipBlock) ||
    JSON.stringify(actualApps) !== JSON.stringify(expectedApps)
  ) {
    fail(`NetworkPolicy/${name} must allow only same-namespace apps: ${expectedApps.join(", ")}`);
  }

  const ports = rules.flatMap(rule => rule.ports || []);
  if (
    ports.length !== 1 ||
    String(ports[0].protocol || "TCP") !== "TCP" ||
    Number(ports[0].port) !== Number(port)
  ) {
    fail(`NetworkPolicy/${name} must allow only TCP/${port}`);
  }
}

function verifyPhotomnemonicEgressPolicy(resources) {
  const name = "photomnemonic-egress";
  const policy = findResource(resources, "NetworkPolicy", name);
  if (!policy) {
    fail(`missing NetworkPolicy/${name}`);
    return;
  }
  if (policy.spec?.podSelector?.matchLabels?.app !== "photomnemonic") {
    fail(`NetworkPolicy/${name} must select app=photomnemonic`);
  }
  if (JSON.stringify(policy.spec?.policyTypes || []) !== JSON.stringify(["Egress"])) {
    fail(`NetworkPolicy/${name} must isolate egress only`);
  }

  const rules = policy.spec?.egress || [];
  const dnsRule = rules.find(rule =>
    (rule.to || []).some(
      peer =>
        peer.namespaceSelector?.matchLabels?.["kubernetes.io/metadata.name"] === "kube-system" &&
        peer.podSelector?.matchLabels?.["k8s-app"] === "kube-dns"
    )
  );
  const dnsPorts = (dnsRule?.ports || [])
    .map(port => `${port.protocol || "TCP"}:${port.port}`)
    .sort();
  if (JSON.stringify(dnsPorts) !== JSON.stringify(["TCP:53", "UDP:53"])) {
    fail(`NetworkPolicy/${name} must allow only TCP/UDP 53 to kube-dns`);
  }

  const publicRule = rules.find(rule => (rule.to || []).some(peer => peer.ipBlock?.cidr === "0.0.0.0/0"));
  const ipBlock = (publicRule?.to || []).find(peer => peer.ipBlock)?.ipBlock;
  const expectedExcept = [
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
  ].sort();
  const actualExcept = [...(ipBlock?.except || [])].sort();
  const publicPorts = (publicRule?.ports || [])
    .map(port => `${port.protocol || "TCP"}:${port.port}`)
    .sort();
  if (
    !ipBlock ||
    JSON.stringify(actualExcept) !== JSON.stringify(expectedExcept) ||
    JSON.stringify(publicPorts) !== JSON.stringify(["TCP:443", "TCP:80"])
  ) {
    fail(`NetworkPolicy/${name} must allow only public TCP/80,443 and exclude audited reserved IPv4 ranges`);
  }
  if (rules.length !== 2) {
    fail(`NetworkPolicy/${name} must contain exactly DNS and public-web egress rules`);
  }
}

function verifyResourceBudget(resources, deploymentName, containerName, expected) {
  const deployment = findResource(resources, "Deployment", deploymentName);
  const container = deployment?.spec?.template?.spec?.containers?.find(value => value.name === containerName);
  if (!container) {
    fail(`missing Deployment/${deploymentName} container ${containerName}`);
    return;
  }
  const actual = container.resources || {};
  if (
    String(actual.requests?.cpu || "") !== expected.cpu ||
    String(actual.requests?.memory || "") !== expected.memory ||
    String(actual.limits?.memory || "") !== expected.memoryLimit
  ) {
    fail(
      `Deployment/${deploymentName} container ${containerName} must request ${expected.cpu}/${expected.memory} ` +
      `and limit memory to ${expected.memoryLimit}`
    );
  }
  if (actual.limits?.cpu) {
    fail(`Deployment/${deploymentName} container ${containerName} must not set a CPU limit`);
  }
}

if (!fs.existsSync(manifestPath)) {
  fail(`manifest not found: ${manifestPath}`);
} else {
  const raw = fs.readFileSync(manifestPath, "utf8");
  if (/\$[A-Za-z_][A-Za-z0-9_]*/.test(raw)) {
    fail("manifest contains unresolved template placeholders");
  }

  const documents = YAML.parseAllDocuments(raw);
  documents.forEach((document, index) => {
    document.errors.forEach(error => fail(`YAML document ${index + 1}: ${error.message}`));
  });
  const resources = documents.map(document => document.toJS()).filter(Boolean);

  const configsSecret = findResource(resources, "Secret", "configs");
  const botAccessKey = configsSecret?.stringData?.BOT_ACCESS_KEY;
  if (typeof botAccessKey !== "string" || botAccessKey.length < 32) {
    fail("Secret/configs BOT_ACCESS_KEY must contain at least 32 characters");
  }

  const retConfig = findResource(resources, "ConfigMap", "ret-config");
  const reticulum = findResource(resources, "Deployment", "reticulum");
  const reticulumContainer = reticulum?.spec?.template?.spec?.containers?.find(
    container => container.name === "reticulum"
  );
  const runtimeConfig = retConfig?.data?.["config.toml.template"] || "";
  const runtimePlaceholders = [...runtimeConfig.matchAll(/<([A-Z][A-Z0-9_]*)>/g)].map(match => match[1]);
  const runtimeVariables = new Set(
    (reticulumContainer?.env || [])
      .map(variable => variable.name)
      .filter(name => name.startsWith("turkeyCfg_"))
      .map(name => name.slice("turkeyCfg_".length))
  );
  const missingRuntimeVariables = [...new Set(runtimePlaceholders)].filter(
    name => !runtimeVariables.has(name)
  );
  if (missingRuntimeVariables.length > 0) {
    fail(
      `ret-config placeholders have no matching turkeyCfg_ environment variable: ${missingRuntimeVariables.join(", ")}`
    );
  }

  for (const deployment of resources.filter(resource => resource.kind === "Deployment")) {
    for (const container of deployment.spec?.template?.spec?.containers || []) {
      if (!isDigestPinnedImage(container.image)) {
        fail(
          `Deployment/${deployment.metadata?.name} container ${container.name || "<unnamed>"} ` +
          `must pin image by sha256 digest (got ${container.image || "<missing>"})`
        );
      }
    }
  }

  const tokenlessDeployments = [
    "bot-orchestrator",
    "reticulum",
    "pgsql",
    "pgbouncer",
    "pgbouncer-t",
    "hubs",
    "spoke",
    "nearspark",
    "photomnemonic",
    "dialog",
    "coturn"
  ];
  for (const name of tokenlessDeployments) {
    const deployment = findResource(resources, "Deployment", name);
    if (deployment?.spec?.template?.spec?.automountServiceAccountToken !== false) {
      fail(`Deployment/${name} must disable service-account token automounting`);
    }
  }

  const haproxyDeployment = findResource(resources, "Deployment", "haproxy");
  if (haproxyDeployment?.spec?.template?.spec?.serviceAccountName !== "haproxy-sa") {
    fail("Deployment/haproxy must keep its dedicated service account");
  }

  const coturnDeployment = findResource(resources, "Deployment", "coturn");
  const credentialLeakingCoturnImage =
    "docker.io/mozillareality/coturn@sha256:8380269c7bb2dc369f4126251199f0d603711debe8537b22cb7be470a50c51ce";
  if (coturnDeployment?.spec?.template?.spec?.containers?.[0]?.image === credentialLeakingCoturnImage) {
    fail("Deployment/coturn must not use the image that logs its database connection string");
  }

  const resourceBudgets = [
    ["reticulum", "reticulum", { cpu: "250m", memory: "2Gi", memoryLimit: "4Gi" }],
    ["reticulum", "postgrest", { cpu: "25m", memory: "32Mi", memoryLimit: "256Mi" }],
    ["bot-orchestrator", "bot-orchestrator", { cpu: "25m", memory: "128Mi", memoryLimit: "512Mi" }],
    ["pgsql", "postgresql", { cpu: "100m", memory: "256Mi", memoryLimit: "1Gi" }],
    ["pgbouncer", "pgbouncer", { cpu: "10m", memory: "16Mi", memoryLimit: "128Mi" }],
    ["pgbouncer-t", "pgbouncer-t", { cpu: "10m", memory: "16Mi", memoryLimit: "128Mi" }],
    ["hubs", "hubs", { cpu: "10m", memory: "16Mi", memoryLimit: "128Mi" }],
    ["spoke", "spoke", { cpu: "10m", memory: "16Mi", memoryLimit: "128Mi" }],
    ["nearspark", "nearspark", { cpu: "25m", memory: "32Mi", memoryLimit: "256Mi" }],
    ["photomnemonic", "photomnemonic", { cpu: "25m", memory: "384Mi", memoryLimit: "768Mi" }],
    ["dialog", "dialog", { cpu: "50m", memory: "96Mi", memoryLimit: "512Mi" }],
    ["coturn", "coturn", { cpu: "25m", memory: "32Mi", memoryLimit: "512Mi" }],
    ["haproxy", "haproxy", { cpu: "100m", memory: "128Mi", memoryLimit: "512Mi" }]
  ];
  const budgetedContainers = new Set(resourceBudgets.map(([deployment, container]) => `${deployment}/${container}`));
  const deploymentContainers = resources
    .filter(resource => resource.kind === "Deployment")
    .flatMap(deployment =>
      (deployment.spec?.template?.spec?.containers || []).map(container => `${deployment.metadata?.name}/${container.name}`)
    );
  const unbudgetedContainers = deploymentContainers.filter(value => !budgetedContainers.has(value));
  if (unbudgetedContainers.length) {
    fail(`all Deployment containers need an audited resource budget: ${unbudgetedContainers.join(", ")}`);
  }
  resourceBudgets.forEach(([deployment, container, expected]) =>
    verifyResourceBudget(resources, deployment, container, expected)
  );

  for (const name of ["reticulum", "pgsql", "dialog", "coturn"]) {
    const deployment = findResource(resources, "Deployment", name);
    if (deployment?.spec?.strategy?.type !== "Recreate") {
      fail(`Deployment/${name} must use Recreate for single-writer storage or exclusive host ports`);
    }
    if (deployment?.spec?.strategy?.rollingUpdate) {
      fail(`Deployment/${name} must not define rollingUpdate for exclusive runtime resources`);
    }
  }

  const botOrchestrator = findResource(resources, "Deployment", "bot-orchestrator");
  const dialog = findResource(resources, "Deployment", "dialog");
  const reticulumBotKeyChecksum =
    reticulum?.spec?.template?.metadata?.annotations?.["yenhubs.org/bot-access-key-checksum"];
  const botOrchestratorBotKeyChecksum =
    botOrchestrator?.spec?.template?.metadata?.annotations?.["yenhubs.org/bot-access-key-checksum"];
  const expectedBotKeyChecksum =
    typeof botAccessKey === "string"
      ? crypto.createHash("sha256").update(botAccessKey).digest("hex")
      : "";
  if (reticulumBotKeyChecksum !== expectedBotKeyChecksum) {
    fail("Deployment/reticulum bot access key checksum must match Secret/configs");
  }
  if (botOrchestratorBotKeyChecksum !== reticulumBotKeyChecksum) {
    fail("Reticulum and bot-orchestrator must share the bot access key checksum annotation");
  }

  const databaseConsumers = ["reticulum", "pgbouncer", "pgbouncer-t", "coturn"];
  const databaseChecksums = databaseConsumers.map(name => {
    const deployment = findResource(resources, "Deployment", name);
    return deployment?.spec?.template?.metadata?.annotations?.["yenhubs.org/db-credential-checksum"];
  });
  if (
    databaseChecksums.some(checksum => !/^[a-f0-9]{64}$/i.test(checksum || "")) ||
    new Set(databaseChecksums).size !== 1
  ) {
    fail("Reticulum, PgBouncer pools and Coturn must share the database credential checksum annotation");
  }

  if (!reticulumContainer) {
    fail("missing reticulum container");
  } else {
    const security = reticulumContainer.securityContext || {};
    const droppedCapabilities = security.capabilities?.drop || [];
    if (security.privileged === true) {
      fail("reticulum container must not be privileged");
    }
    if (security.allowPrivilegeEscalation !== false) {
      fail("reticulum container must disable privilege escalation");
    }
    if (!droppedCapabilities.includes("ALL")) {
      fail("reticulum container must drop all Linux capabilities");
    }
    if (security.seccompProfile?.type !== "RuntimeDefault") {
      fail("reticulum container must use the RuntimeDefault seccomp profile");
    }
    const storageMount = (reticulumContainer.volumeMounts || []).find(mount => mount.name === "storage");
    if (storageMount?.mountPropagation) {
      fail("reticulum storage mount must not propagate host mounts");
    }
  }

  const botContainer = botOrchestrator?.spec?.template?.spec?.containers?.find(
    container => container.name === "bot-orchestrator"
  );
  if (!botContainer) {
    fail("missing bot-orchestrator container");
  } else {
    const security = botContainer.securityContext || {};
    const droppedCapabilities = security.capabilities?.drop || [];
    if (security.runAsNonRoot !== true) {
      fail("bot-orchestrator container must run as non-root");
    }
    if (Number(security.runAsUser) !== 1000 || Number(security.runAsGroup) !== 1000) {
      fail("bot-orchestrator container must use the audited uid/gid 1000");
    }
    if (security.allowPrivilegeEscalation !== false) {
      fail("bot-orchestrator container must disable privilege escalation");
    }
    if (!droppedCapabilities.includes("ALL")) {
      fail("bot-orchestrator container must drop all Linux capabilities");
    }
    if (security.seccompProfile?.type !== "RuntimeDefault") {
      fail("bot-orchestrator container must use the RuntimeDefault seccomp profile");
    }
    const botEnv = Object.fromEntries(
      (botContainer.env || []).filter(entry => entry && entry.name).map(entry => [entry.name, entry.value])
    );
    if (botEnv.RUNNER_BACKEND !== "ghost" || botEnv.RUNNER_AUTOSTART !== "true") {
      fail("bot-orchestrator production runtime must autostart the authenticated ghost backend");
    }
    if (botEnv.RUNNER_BACKEND_CANARY_HUBS !== "") {
      fail("bot-orchestrator production runtime must not use Chromium canary routing");
    }
    if (
      botEnv.OPENAI_MODEL !== "gpt-5-nano" ||
      Number(botEnv.OPENAI_TOTAL_BUDGET_MS) !== 4000
    ) {
      fail("bot-orchestrator must use the audited GPT-5 Nano contract within Reticulum's timeout");
    }
    if (Number(botEnv.MAX_BOTS_PER_ROOM) !== 10) {
      fail("bot-orchestrator production runtime must enforce at most ten bots per room");
    }
    if (
      !Number.isInteger(Number(botEnv.MAX_ACTIVE_ROOMS)) ||
      Number(botEnv.MAX_ACTIVE_ROOMS) < 1 ||
      Number(botEnv.MAX_ACTIVE_ROOMS) > 10
    ) {
      fail("bot-orchestrator production runtime must enforce at most ten active ghost rooms");
    }
    if (botEnv.GHOST_NAVIGATION_MODE !== "navmesh_preferred") {
      fail("bot-orchestrator must prefer navmesh navigation");
    }
    if (botEnv.GHOST_NAVIGATION_REQUIRE_NAVMESH !== "true") {
      fail("bot-orchestrator must fail closed when the scene navmesh is unavailable");
    }
    const botReadinessPath = botContainer.readinessProbe?.httpGet?.path;
    const botLivenessPath = botContainer.livenessProbe?.httpGet?.path;
    if (botReadinessPath !== "/ready" || botLivenessPath !== "/health") {
      fail("bot-orchestrator must separate authoritative readiness from liveness");
    }
    if (
      Number(botEnv.GHOST_NAVMESH_MAX_TRIANGLES) !== 50000 ||
      Number(botEnv.GHOST_NAVMESH_MAX_ROUTE_POINTS) !== 64 ||
      Number(botEnv.GHOST_NAVMESH_MAX_SNAP_DISTANCE_M) !== 3 ||
      Number(botEnv.GHOST_NAVIGATION_RECOVERY_RESTART_MS) !== 30000 ||
      Number(botEnv.GHOST_FEATURED_FETCH_TIMEOUT_MS) !== 4000 ||
      Number(botEnv.GHOST_FEATURED_MAX_BYTES) !== 524288 ||
      Number(botEnv.GHOST_FEATURED_MAX_REDIRECTS) !== 2 ||
      Number(botEnv.GHOST_FEATURED_MAX_ENTRIES) !== 256 ||
      Number(botEnv.GHOST_FEATURED_MAX_REFS) !== 128
    ) {
      fail("bot-orchestrator navigation and Featured-fetch limits do not match the audited baseline");
    }
  }

  const dialogContainer = dialog?.spec?.template?.spec?.containers?.find(container => container.name === "dialog");
  if (!dialogContainer) {
    fail("missing dialog container");
  } else {
    const security = dialogContainer.securityContext || {};
    const droppedCapabilities = security.capabilities?.drop || [];
    if (security.runAsNonRoot !== true) {
      fail("dialog container must run as non-root");
    }
    if (Number(security.runAsUser) !== 1000 || Number(security.runAsGroup) !== 1000) {
      fail("dialog container must use the audited uid/gid 1000");
    }
    if (security.allowPrivilegeEscalation !== false) {
      fail("dialog container must disable privilege escalation");
    }
    if (!droppedCapabilities.includes("ALL")) {
      fail("dialog container must drop all Linux capabilities");
    }
    if (security.seccompProfile?.type !== "RuntimeDefault") {
      fail("dialog container must use the RuntimeDefault seccomp profile");
    }
    for (const probe of ["startupProbe", "readinessProbe", "livenessProbe"]) {
      if (Number(dialogContainer[probe]?.tcpSocket?.port) !== 4443) {
        fail(`dialog container must define ${probe} on TCP port 4443`);
      }
    }
  }

  const photomnemonic = findResource(resources, "Deployment", "photomnemonic");
  const photomnemonicContainer = photomnemonic?.spec?.template?.spec?.containers?.find(
    container => container.name === "photomnemonic"
  );
  if (!photomnemonicContainer) {
    fail("missing photomnemonic container");
  } else {
    const security = photomnemonicContainer.securityContext || {};
    const droppedCapabilities = security.capabilities?.drop || [];
    if (
      security.runAsNonRoot !== true ||
      Number(security.runAsUser) !== 1000 ||
      Number(security.runAsGroup) !== 1000 ||
      security.allowPrivilegeEscalation !== false ||
      !droppedCapabilities.includes("ALL") ||
      security.seccompProfile?.type !== "RuntimeDefault"
    ) {
      fail("photomnemonic container must use the audited non-root security context");
    }
    for (const [probeName, path] of [
      ["startupProbe", "/_readyz"],
      ["readinessProbe", "/_readyz"],
      ["livenessProbe", "/_healthz"]
    ]) {
      const probe = photomnemonicContainer[probeName];
      if (probe?.httpGet?.path !== path || Number(probe?.httpGet?.port) !== 5000) {
        fail(`photomnemonic ${probeName} must use ${path}:5000`);
      }
    }
  }

  for (const name of ["ret", "dialog", "nearspark"]) {
    const ingress = findResource(resources, "Ingress", name);
    if (!ingress) {
      fail(`missing Ingress/${name}`);
      continue;
    }
    const annotations = ingress.metadata?.annotations || {};
    if (annotations["cert-manager.io/cluster-issuer"] !== "letsencrypt-prod") {
      fail(`Ingress/${name} must use letsencrypt-prod`);
    }
    if (String(annotations["haproxy.org/ssl-redirect"]) !== "true") {
      fail(`Ingress/${name} must opt in to ssl redirect`);
    }
    if (ingress.spec?.ingressClassName !== "haproxy") {
      fail(`Ingress/${name} must set spec.ingressClassName=haproxy`);
    }
  }

  const haproxyConfig = findResource(resources, "ConfigMap", "haproxy-config");
  if (String(haproxyConfig?.data?.["ssl-redirect"]) !== "false") {
    fail("ConfigMap/haproxy-config must keep global ssl-redirect=false for ACME HTTP-01");
  }

  const haproxy = findResource(resources, "Deployment", "haproxy");
  const haproxyContainer = haproxy?.spec?.template?.spec?.containers?.find(container => container.name === "haproxy");
  if (!haproxyContainer) {
    fail("missing haproxy container");
  } else {
    if (String(haproxyContainer.image || "").includes("mozillareality/haproxy")) {
      fail("legacy mozillareality/haproxy image is incompatible with current Kubernetes");
    }
    if (haproxyContainer.securityContext) {
      fail("haproxy container must not restore the legacy securityContext");
    }
    for (const probe of ["startupProbe", "readinessProbe", "livenessProbe"]) {
      const value = haproxyContainer[probe];
      if (value?.httpGet?.path !== "/healthz" || Number(value?.httpGet?.port) !== 1042) {
        fail(`haproxy container must define ${probe} on /healthz:1042`);
      }
    }
  }

  const haproxyRole = findResource(resources, "ClusterRole", "haproxy-cr");
  if (!hasRule(haproxyRole, "apiextensions.k8s.io", "customresourcedefinitions", ["get", "list", "watch"])) {
    fail("ClusterRole/haproxy-cr is missing CRD read permissions");
  }
  if (!hasRule(haproxyRole, "gateway.networking.k8s.io", "gateways", ["get", "list", "watch"])) {
    fail("ClusterRole/haproxy-cr is missing Gateway API read permissions");
  }

  verifyIngressPolicy(resources, "bot-orchestrator-ingress", "bot-orchestrator", ["reticulum"], 5001);
  verifyIngressPolicy(resources, "pgsql-ingress", "pgsql", ["pgbouncer", "pgbouncer-t"], 5432);
  verifyIngressPolicy(resources, "pgbouncer-ingress", "pgbouncer", ["reticulum"], 5432);
  verifyIngressPolicy(resources, "pgbouncer-t-ingress", "pgbouncer-t", ["reticulum"], 5432);
  verifyIngressPolicy(resources, "photomnemonic-ingress", "photomnemonic", ["reticulum"], 5000);
  verifyPhotomnemonicEgressPolicy(resources);

  if (findResource(resources, "Secret", "cert-hcce")) {
    fail("unused self-signed Secret/cert-hcce must not be generated");
  }

  const loadBalancers = resources.filter(resource => resource.kind === "Service" && resource.spec?.type === "LoadBalancer");
  if (loadBalancers.length !== 1 || loadBalancers[0].metadata?.name !== "lb") {
    fail("manifest must create exactly one LoadBalancer Service named lb");
  }

  const persistentVolumeClaims = resources.filter(resource => resource.kind === "PersistentVolumeClaim");
  const expectedClaims = new Set(["pgsql-pvc", "ret-pvc"]);
  if (
    persistentVolumeClaims.length !== expectedClaims.size ||
    persistentVolumeClaims.some(claim => !expectedClaims.has(claim.metadata?.name))
  ) {
    fail("manifest must create exactly the pgsql-pvc and ret-pvc PersistentVolumeClaims");
  }
  for (const claim of persistentVolumeClaims) {
    if (claim.spec?.storageClassName !== "do-block-storage") {
      fail(`PersistentVolumeClaim/${claim.metadata?.name} must use do-block-storage`);
    }
    if (String(claim.spec?.resources?.requests?.storage) !== "10Gi") {
      fail(`PersistentVolumeClaim/${claim.metadata?.name} must request exactly 10Gi`);
    }
  }

  if (!errors.length) {
    console.log(`Manifest verification passed (${resources.length} resources).`);
  }
}

if (errors.length) {
  console.error("Manifest verification failed:");
  errors.forEach(error => console.error(`- ${error}`));
  process.exit(1);
}
