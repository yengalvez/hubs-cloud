const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const YAML = require("yaml");
const {
  findExactResource,
  hasExactOwnKeys,
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
} = require("./verify-manifest-contracts");

const manifestPath = process.env.HCCE_MANIFEST_PATH
  ? path.resolve(process.env.HCCE_MANIFEST_PATH)
  : path.resolve(__dirname, "../hcce.yaml");
const errors = [];

function fail(message) {
  errors.push(message);
}

function isDigestPinnedImage(image) {
  return typeof image === "string" && /@sha256:[a-f0-9]{64}$/i.test(image);
}

function verifyIngressPolicy(resources, namespace, name, targetApp, allowedApps, port, allowedPeers = null) {
  const policy = findExactResource(resources, "networking.k8s.io", "NetworkPolicy", namespace, name);
  if (!policy) {
    fail(`missing NetworkPolicy/${name}`);
    return;
  }
  verifyExactIngressPolicy(policy, { name, targetApp, allowedApps, allowedPeers, port }).forEach(fail);
}

function verifyPhotomnemonicEgressPolicy(resources, namespace) {
  const name = "photomnemonic-egress";
  const policy = findExactResource(resources, "networking.k8s.io", "NetworkPolicy", namespace, name);
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

function verifyResourceBudget(resources, namespace, deploymentName, containerName, expected) {
  const deployment = findExactResource(resources, "apps", "Deployment", namespace, deploymentName);
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
  const documents = YAML.parseAllDocuments(raw);
  documents.forEach((document, index) => {
    document.errors.forEach(error => fail(`YAML document ${index + 1}: ${error.message}`));
  });
  verifyNoYamlIndirections(documents, YAML).forEach(fail);
  const resources = documents.map(document => document.toJS()).filter(Boolean);
  verifyManifestResourceIdentities(resources).forEach(fail);
  verifyManifestResourceInventory(resources).forEach(fail);
  const primaryNamespace = resources.find(resource =>
    resource?.apiVersion === "v1" &&
    resource?.kind === "Namespace" &&
    resource?.metadata?.name !== "hcce-bot-runners"
  )?.metadata?.name;
  verifyBotRunnerRecoveryContract(resources, primaryNamespace).forEach(fail);
  verifyNoReticulumHorizontalPodAutoscaler(resources).forEach(fail);
  const namespaceResource = resources.find(resource => {
    return resource?.apiVersion === "v1" &&
      resource?.kind === "Namespace" &&
      resource?.metadata?.namespace === undefined;
  });
  const manifestNamespace = namespaceResource?.metadata?.name || "";
  verifyAuditedDeploymentContainers(resources, manifestNamespace).forEach(fail);

  const configsSecret = findExactResource(resources, "", "Secret", manifestNamespace, "configs");
  const accessKeyNames = [
    "BOT_ACCESS_KEY",
    "BOT_RUNNER_ACCESS_KEY",
    "BOT_ORCHESTRATOR_ACCESS_KEY",
    "DASHBOARD_ACCESS_KEY"
  ];
  const accessKeys = Object.fromEntries(
    accessKeyNames.map(name => [name, configsSecret?.stringData?.[name]])
  );
  for (const name of accessKeyNames) {
    if (typeof accessKeys[name] !== "string" || accessKeys[name].length < 32) {
      fail(`Secret/configs ${name} must contain at least 32 characters`);
    }
  }
  if (new Set(accessKeyNames.map(name => accessKeys[name])).size !== accessKeyNames.length) {
    fail("Secret/configs bot integration, runner, orchestrator and dashboard keys must be distinct");
  }

  const retConfig = findExactResource(resources, "", "ConfigMap", manifestNamespace, "ret-config");
  const reticulum = findExactResource(resources, "apps", "Deployment", manifestNamespace, "reticulum");
  verifyReticulumBotRunnerAuthorityContract(reticulum).forEach(fail);
  const reticulumContainer = reticulum?.spec?.template?.spec?.containers?.find(
    container => container.name === "reticulum"
  );
  const reticulumEnv = Array.isArray(reticulumContainer?.env) ? reticulumContainer.env : [];
  const reticulumAccessKeyContracts = accessKeyNames.map(name => ({
    envName: `turkeyCfg_${name}`,
    secretKey: name
  }));
  for (const contract of reticulumAccessKeyContracts) {
    const entries = reticulumEnv.filter(entry => entry?.name === contract.envName);
    const entry = entries[0];
    const valueFrom = entry?.valueFrom;
    const secretKeyRef = valueFrom?.secretKeyRef;
    if (
      entries.length !== 1 ||
      !hasExactOwnKeys(entry, ["name", "valueFrom"]) ||
      !hasExactOwnKeys(valueFrom, ["secretKeyRef"]) ||
      !hasExactOwnKeys(secretKeyRef, ["name", "key"]) ||
      secretKeyRef.name !== "configs" ||
      secretKeyRef.key !== contract.secretKey
    ) {
      fail(
        `Deployment/reticulum ${contract.envName} must exclusively reference ` +
        `Secret/configs key ${contract.secretKey}`
      );
    }
  }
  const reticulumAccessKeyReferences = reticulumEnv.filter(entry =>
    accessKeyNames.includes(entry?.valueFrom?.secretKeyRef?.key)
  );
  if (reticulumAccessKeyReferences.length !== reticulumAccessKeyContracts.length) {
    fail("Deployment/reticulum must contain exactly four scoped access-key Secret references");
  }
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
  const accessKeyPlaceholderSet = new Set(accessKeyNames);
  const observedAccessKeyMappings = [];
  const observedBotRoomLimitMappings = [];
  let currentRuntimeSection = "";
  for (const rawLine of runtimeConfig.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (/^\[[^\r\n]+\]$/.test(line)) {
      currentRuntimeSection = line;
      continue;
    }
    const assignment = line.match(/^([A-Za-z_][A-Za-z0-9_]*)\s*=\s*"<([A-Z][A-Z0-9_]*)>"\s*$/);
    if (assignment && accessKeyPlaceholderSet.has(assignment[2])) {
      observedAccessKeyMappings.push(`${currentRuntimeSection}\t${assignment[1]}\t${assignment[2]}`);
    }
    const botRoomLimitAssignment = line.match(
      /^(max_active_bot_rooms)\s*=\s*<([A-Z][A-Z0-9_]*)>\s*$/
    );
    if (botRoomLimitAssignment) {
      observedBotRoomLimitMappings.push(
        `${currentRuntimeSection}\t${botRoomLimitAssignment[1]}\t${botRoomLimitAssignment[2]}`
      );
    }
  }
  const expectedAccessKeyMappings = [
    '[ret."Elixir.RetWeb.Plugs.DashboardHeaderAuthorization"]\tdashboard_access_key\tDASHBOARD_ACCESS_KEY',
    '[ret."Elixir.RetWeb.Plugs.HeaderAuthorization"]\theader_value\tDASHBOARD_ACCESS_KEY',
    '[ret]\tbot_access_key\tBOT_ACCESS_KEY',
    '[ret]\tbot_runner_access_key\tBOT_RUNNER_ACCESS_KEY',
    '[ret]\tbot_orchestrator_access_key\tBOT_ORCHESTRATOR_ACCESS_KEY',
    '[ret."Elixir.Ret.BotOrchestrator"]\taccess_key\tBOT_ORCHESTRATOR_ACCESS_KEY'
  ];
  if (
    JSON.stringify(observedAccessKeyMappings.sort()) !== JSON.stringify(expectedAccessKeyMappings.sort())
  ) {
    fail("ret-config must preserve the exact scoped access-key placeholder mappings");
  }
  if (
    JSON.stringify(observedBotRoomLimitMappings) !==
    JSON.stringify(["[ret]\tmax_active_bot_rooms\tMAX_ACTIVE_ROOMS"])
  ) {
    fail("ret-config must bind max_active_bot_rooms exactly to MAX_ACTIVE_ROOMS");
  }

  const reticulumBotRoomLimitEntries = reticulumEnv.filter(
    entry => entry?.name === "turkeyCfg_MAX_ACTIVE_ROOMS"
  );
  const reticulumBotRoomLimitEntry = reticulumBotRoomLimitEntries[0];
  const capacityBotOrchestrator = findExactResource(
    resources,
    "apps",
    "Deployment",
    manifestNamespace,
    "bot-orchestrator"
  );
  const capacityBotContainer = capacityBotOrchestrator?.spec?.template?.spec?.containers?.find(
    container => container.name === "bot-orchestrator"
  );
  const botRoomLimitEntries = (capacityBotContainer?.env || []).filter(
    entry => entry?.name === "MAX_ACTIVE_ROOMS"
  );
  const botRoomLimitEntry = botRoomLimitEntries[0];
  if (
    reticulumBotRoomLimitEntries.length !== 1 ||
    botRoomLimitEntries.length !== 1 ||
    !hasExactOwnKeys(reticulumBotRoomLimitEntry, ["name", "value"]) ||
    !hasExactOwnKeys(botRoomLimitEntry, ["name", "value"]) ||
    String(reticulumBotRoomLimitEntry.value) !== String(botRoomLimitEntry.value)
  ) {
    fail("Reticulum and bot-orchestrator must receive the same MAX_ACTIVE_ROOMS value");
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
    const deployment = findExactResource(resources, "apps", "Deployment", manifestNamespace, name);
    if (deployment?.spec?.template?.spec?.automountServiceAccountToken !== false) {
      fail(`Deployment/${name} must disable service-account token automounting`);
    }
  }

  const haproxyDeployment = findExactResource(resources, "apps", "Deployment", manifestNamespace, "haproxy");
  if (haproxyDeployment?.spec?.template?.spec?.serviceAccountName !== "haproxy-sa") {
    fail("Deployment/haproxy must keep its dedicated service account");
  }

  const coturnDeployment = findExactResource(resources, "apps", "Deployment", manifestNamespace, "coturn");
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
    verifyResourceBudget(resources, manifestNamespace, deployment, container, expected)
  );

  for (const name of ["reticulum", "bot-orchestrator", "pgsql", "dialog", "coturn"]) {
    const deployment = findExactResource(resources, "apps", "Deployment", manifestNamespace, name);
    if (deployment?.spec?.strategy?.type !== "Recreate") {
      fail(`Deployment/${name} must use Recreate for single-writer storage or exclusive host ports`);
    }
    if (deployment?.spec?.strategy?.rollingUpdate) {
      fail(`Deployment/${name} must not define rollingUpdate for exclusive runtime resources`);
    }
  }

  const botOrchestrator = findExactResource(
    resources,
    "apps",
    "Deployment",
    manifestNamespace,
    "bot-orchestrator"
  );
  if (
    botOrchestrator?.spec?.template?.spec?.serviceAccountName !== "bot-orchestrator" ||
    botOrchestrator?.spec?.template?.spec?.automountServiceAccountToken !== true ||
    JSON.stringify(botOrchestrator?.spec?.template?.spec?.imagePullSecrets) !==
      JSON.stringify([{ name: "bot-images-pull" }])
  ) {
    fail("Deployment/bot-orchestrator must use its dedicated service account and image-pull Secret");
  }
  verifyBotOrchestratorContainers(botOrchestrator).forEach(fail);
  verifyBotOrchestratorDeploymentContract(botOrchestrator).forEach(fail);
  verifyBotOrchestratorIsolationContract(botOrchestrator).forEach(fail);
  const dialog = findExactResource(resources, "apps", "Deployment", manifestNamespace, "dialog");
  const reticulumBotKeyChecksum =
    reticulum?.spec?.template?.metadata?.annotations?.["yenhubs.org/bot-access-key-checksum"];
  const reticulumRunnerKeyChecksum =
    reticulum?.spec?.template?.metadata?.annotations?.["yenhubs.org/bot-runner-access-key-checksum"];
  const reticulumOrchestratorKeyChecksum =
    reticulum?.spec?.template?.metadata?.annotations?.["yenhubs.org/bot-orchestrator-access-key-checksum"];
  const reticulumDashboardKeyChecksum =
    reticulum?.spec?.template?.metadata?.annotations?.["yenhubs.org/dashboard-access-key-checksum"];
  const botOrchestratorRunnerKeyChecksum =
    botOrchestrator?.spec?.template?.metadata?.annotations?.["yenhubs.org/bot-runner-access-key-checksum"];
  const botOrchestratorAccessKeyChecksum =
    botOrchestrator?.spec?.template?.metadata?.annotations?.["yenhubs.org/bot-orchestrator-access-key-checksum"];
  const checksumFor = name =>
    typeof accessKeys[name] === "string"
      ? crypto.createHash("sha256").update(accessKeys[name]).digest("hex")
      : "";
  const expectedBotKeyChecksum = checksumFor("BOT_ACCESS_KEY");
  if (reticulumBotKeyChecksum !== expectedBotKeyChecksum) {
    fail("Deployment/reticulum bot access key checksum must match Secret/configs");
  }
  if (reticulumRunnerKeyChecksum !== checksumFor("BOT_RUNNER_ACCESS_KEY")) {
    fail("Deployment/reticulum runner key checksum must match Secret/configs");
  }
  if (botOrchestratorRunnerKeyChecksum) {
    fail("bot-orchestrator must not receive the master runner key checksum");
  }
  if (
    reticulumOrchestratorKeyChecksum !== checksumFor("BOT_ORCHESTRATOR_ACCESS_KEY") ||
    botOrchestratorAccessKeyChecksum !== reticulumOrchestratorKeyChecksum
  ) {
    fail("Reticulum and bot-orchestrator access key checksums must match Secret/configs");
  }
  if (reticulumDashboardKeyChecksum !== checksumFor("DASHBOARD_ACCESS_KEY")) {
    fail("Deployment/reticulum dashboard key checksum must match Secret/configs");
  }
  if (
    botOrchestrator?.spec?.template?.metadata?.annotations?.["yenhubs.org/bot-access-key-checksum"] ||
    botOrchestrator?.spec?.template?.metadata?.annotations?.["yenhubs.org/bot-runner-access-key-checksum"] ||
    botOrchestrator?.spec?.template?.metadata?.annotations?.["yenhubs.org/dashboard-access-key-checksum"]
  ) {
    fail("bot-orchestrator must not receive legacy integration or dashboard key checksums");
  }

  const databaseConsumers = ["reticulum", "pgbouncer", "pgbouncer-t", "coturn"];
  const databaseChecksums = databaseConsumers.map(name => {
    const deployment = findExactResource(resources, "apps", "Deployment", manifestNamespace, name);
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
    verifyBotOrchestratorSecretEnv(botContainer).forEach(fail);
    verifyBotOrchestratorSecurityContext(botContainer).forEach(fail);
    verifyBotOrchestratorRuntimeEnv(botContainer, manifestNamespace).forEach(fail);
    const botEnv = Object.fromEntries(
      (botContainer.env || []).filter(entry => entry && entry.name).map(entry => [entry.name, entry.value])
    );
    if (!isDigestPinnedImage(botEnv.BOT_RUNNER_IMAGE)) {
      fail("bot-orchestrator BOT_RUNNER_IMAGE must pin the dedicated runner image by digest");
    }
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
    if (botReadinessPath !== "/transport-ready" || botLivenessPath !== "/health") {
      fail("bot-orchestrator must expose transport readiness separately from authoritative /ready");
    }
    if (
      Number(botEnv.GHOST_NAVMESH_MAX_TRIANGLES) !== 50000 ||
      Number(botEnv.GHOST_NAVMESH_MAX_ROUTE_POINTS) !== 64 ||
      Number(botEnv.GHOST_NAVMESH_MAX_SNAP_DISTANCE_M) !== 3 ||
      Number(botEnv.GHOST_NAVIGATION_RECOVERY_RESTART_MS) !== 30000 ||
      Number(botEnv.GHOST_SPAWN_RECOVERY_RESTART_MS) !== 5000 ||
      Number(botEnv.GHOST_FEATURED_FETCH_TIMEOUT_MS) !== 4000 ||
      Number(botEnv.GHOST_FEATURED_MAX_BYTES) !== 524288 ||
      Number(botEnv.GHOST_FEATURED_MAX_REDIRECTS) !== 2 ||
      Number(botEnv.GHOST_FEATURED_MAX_ENTRIES) !== 256 ||
      Number(botEnv.GHOST_FEATURED_MAX_REFS) !== 128
    ) {
      fail("bot-orchestrator navigation and Featured-fetch limits do not match the audited baseline");
    }
    if (
      Number(botEnv.RET_SYNC_TIMEOUT_MS) !== 5000 ||
      Number(botEnv.RET_SNAPSHOT_TTL_MS) !== 120000 ||
      Number(botEnv.RUNNER_CONFIG_ACK_TIMEOUT_MS) !== 15000 ||
      Number(botEnv.RUNNER_STARTUP_GRACE_MS) !== 180000 ||
      Number(botEnv.RUNNER_STALE_RESTART_MS) !== 30000 ||
      Number(botEnv.RUNNER_TERMINAL_RECOVERY_GRACE_MS) !== 15000 ||
      Number(botEnv.RUNNER_WATCHDOG_INTERVAL_MS) !== 5000 ||
      Number(botEnv.RUNNER_RESTART_BASE_MS) !== 3000 ||
      Number(botEnv.RUNNER_RESTART_MAX_MS) !== 60000 ||
      Number(botEnv.RUNNER_STABLE_RESET_MS) !== 30000 ||
      Number(botEnv.RUNNER_TERMINATION_GRACE_MS) !== 10000 ||
      Number(botEnv.RUNNER_KILL_GRACE_MS) !== 5000 ||
      Number(botEnv.RUNNER_POD_RECONCILE_INTERVAL_MS) !== 5000 ||
      Number(botEnv.RUNNER_TOKEN_TTL_SECONDS) !== 3600
    ) {
      fail("bot-orchestrator desired-state and runner recovery bounds do not match the audited baseline");
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

  const photomnemonic = findExactResource(
    resources,
    "apps",
    "Deployment",
    manifestNamespace,
    "photomnemonic"
  );
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
    const ingress = findExactResource(resources, "networking.k8s.io", "Ingress", manifestNamespace, name);
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

  const haproxyConfig = findExactResource(resources, "", "ConfigMap", manifestNamespace, "haproxy-config");
  if (String(haproxyConfig?.data?.["ssl-redirect"]) !== "false") {
    fail("ConfigMap/haproxy-config must keep global ssl-redirect=false for ACME HTTP-01");
  }

  const haproxy = findExactResource(resources, "apps", "Deployment", manifestNamespace, "haproxy");
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

  const haproxyRole = findExactResource(
    resources,
    "rbac.authorization.k8s.io",
    "ClusterRole",
    "",
    "haproxy-cr"
  );
  verifyHaproxyClusterRole(haproxyRole).forEach(fail);

  verifyBotImagePullSecret(resources, manifestNamespace).forEach(fail);
  verifyBotRunnerControlPlaneResources(resources, manifestNamespace).forEach(fail);
  verifyBotRunnerAdmissionResources(resources, manifestNamespace).forEach(fail);
  const botRunnerDefaultDeny = findExactResource(
    resources,
    "networking.k8s.io",
    "NetworkPolicy",
    "hcce-bot-runners",
    "bot-runner-default-deny"
  );
  verifyBotRunnerDefaultDenyNetworkPolicy(botRunnerDefaultDeny).forEach(fail);
  const botRunnerNetworkPolicy = findExactResource(
    resources,
    "networking.k8s.io",
    "NetworkPolicy",
    "hcce-bot-runners",
    "bot-runner-egress"
  );
  verifyBotRunnerNetworkPolicy(botRunnerNetworkPolicy, manifestNamespace).forEach(fail);

  verifyIngressPolicy(
    resources,
    manifestNamespace,
    "bot-orchestrator-ingress",
    "bot-orchestrator",
    [],
    5001,
    [
      { podSelector: { matchLabels: { app: "reticulum" } } },
      {
        namespaceSelector: {
          matchLabels: {
            "kubernetes.io/metadata.name": "hcce-bot-runners"
          }
        },
        podSelector: {
          matchLabels: {
            app: "bot-runner",
            "yenhubs.org/managed-by": "bot-orchestrator"
          }
        }
      }
    ]
  );
  verifyIngressPolicy(resources, manifestNamespace, "pgsql-ingress", "pgsql", ["pgbouncer", "pgbouncer-t"], 5432);
  verifyIngressPolicy(resources, manifestNamespace, "pgbouncer-ingress", "pgbouncer", ["reticulum"], 5432);
  verifyIngressPolicy(resources, manifestNamespace, "pgbouncer-t-ingress", "pgbouncer-t", ["reticulum"], 5432);
  verifyIngressPolicy(resources, manifestNamespace, "photomnemonic-ingress", "photomnemonic", ["reticulum"], 5000);
  verifyPhotomnemonicEgressPolicy(resources, manifestNamespace);

  if (findExactResource(resources, "", "Secret", manifestNamespace, "cert-hcce")) {
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
  const claimByName = Object.fromEntries(
    persistentVolumeClaims.map(claim => [claim.metadata?.name, claim])
  );
  const claimSizes = persistentVolumeClaims.map(claim => String(claim.spec?.resources?.requests?.storage || ""));
  if (
    claimSizes.length !== 2 ||
    new Set(claimSizes).size !== 1 ||
    !/^[1-9][0-9]*(?:Mi|Gi|Ti)$/.test(claimSizes[0] || "")
  ) {
    fail("pgsql-pvc and ret-pvc must request the same positive Mi/Gi/Ti storage quantity");
  }
  const claimStorageClasses = persistentVolumeClaims.map(claim => claim.spec?.storageClassName);
  const sameExplicitStorageClass =
    claimStorageClasses.every(value => typeof value === "string" && value.length > 0) &&
    new Set(claimStorageClasses).size === 1;
  const bothUseClusterDefault = claimStorageClasses.every(value => value === undefined);
  if (!sameExplicitStorageClass && !bothUseClusterDefault) {
    fail("pgsql-pvc and ret-pvc must use one identical explicit storageClassName or both use the cluster default");
  }
  const storageClassName = bothUseClusterDefault ? "default" : claimStorageClasses[0];
  const manualStorage = storageClassName === "manual";
  const expectedAccessModes = manualStorage
    ? { "pgsql-pvc": ["ReadWriteOnce"], "ret-pvc": ["ReadWriteOnce"] }
    : { "pgsql-pvc": ["ReadWriteOncePod"], "ret-pvc": ["ReadWriteOnce"] };
  for (const [name, accessModes] of Object.entries(expectedAccessModes)) {
    if (JSON.stringify(claimByName[name]?.spec?.accessModes) !== JSON.stringify(accessModes)) {
      fail(`PersistentVolumeClaim/${name} must use ${accessModes.join(",")} for ${storageClassName} storage`);
    }
  }

  const persistentVolumes = resources.filter(resource => resource.kind === "PersistentVolume");
  if (manualStorage) {
    const volumeByName = Object.fromEntries(persistentVolumes.map(volume => [volume.metadata?.name, volume]));
    if (
      persistentVolumes.length !== 2 ||
      !volumeByName["pgsql-pv"] ||
      !volumeByName["ret-pv"]
    ) {
      fail("manual storage must create exactly pgsql-pv and ret-pv");
    }
    for (const [name, claimName, hostPath] of [
      ["pgsql-pv", "pgsql-pvc", "/mnt/pgsql_data"],
      ["ret-pv", "ret-pvc", "/mnt/ret_storage_data"]
    ]) {
      const volume = volumeByName[name];
      const spec = volume?.spec;
      if (
        !hasExactOwnKeys(spec, [
          "storageClassName",
          "capacity",
          "accessModes",
          "persistentVolumeReclaimPolicy",
          "claimRef",
          "hostPath"
        ]) ||
        spec.storageClassName !== "manual" ||
        String(spec.capacity?.storage) !== claimSizes[0] ||
        JSON.stringify(spec.accessModes) !== JSON.stringify(["ReadWriteOnce"]) ||
        spec.persistentVolumeReclaimPolicy !== "Retain" ||
        !hasExactOwnKeys(spec.claimRef, ["name", "namespace"]) ||
        spec.claimRef.name !== claimName ||
        spec.claimRef.namespace !== manifestNamespace ||
        !hasExactOwnKeys(spec.hostPath, ["path", "type"]) ||
        spec.hostPath.path !== hostPath ||
        spec.hostPath.type !== "DirectoryOrCreate"
      ) {
        fail(`PersistentVolume/${name} does not match the audited retained manual-storage contract`);
      }
    }
  } else if (persistentVolumes.length !== 0) {
    fail("dynamic or cluster-default storage must not create static PersistentVolumes");
  }

  const pgsql = findExactResource(resources, "apps", "Deployment", manifestNamespace, "pgsql");
  const pgsqlVolume = pgsql?.spec?.template?.spec?.volumes?.find(volume => volume.name === "postgresql-data");
  const reticulumVolume = reticulum?.spec?.template?.spec?.volumes?.find(volume => volume.name === "storage");
  if (
    !hasExactOwnKeys(pgsqlVolume, ["name", "persistentVolumeClaim"]) ||
    !hasExactOwnKeys(pgsqlVolume?.persistentVolumeClaim, ["claimName"]) ||
    pgsqlVolume.persistentVolumeClaim.claimName !== "pgsql-pvc"
  ) {
    fail("Deployment/pgsql must mount only PersistentVolumeClaim/pgsql-pvc for database storage");
  }
  if (
    !hasExactOwnKeys(reticulumVolume, ["name", "persistentVolumeClaim"]) ||
    !hasExactOwnKeys(reticulumVolume?.persistentVolumeClaim, ["claimName"]) ||
    reticulumVolume.persistentVolumeClaim.claimName !== "ret-pvc"
  ) {
    fail("Deployment/reticulum must mount only PersistentVolumeClaim/ret-pvc for media storage");
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
