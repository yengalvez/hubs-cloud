const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const YAML = require("yaml");

const {
  AUDITED_DEPLOYMENT_CONTAINERS,
  BOT_ORCHESTRATOR_ALLOWED_ENV_NAMES,
  BOT_ORCHESTRATOR_RUNTIME_ENV,
  HAPROXY_CLUSTER_ROLE,
  expectedManifestInventory,
  verifyAuditedDeploymentContainers,
  verifyBotOrchestratorContainers,
  verifyBotOrchestratorDeploymentContract,
  verifyBotOrchestratorIsolationContract,
  verifyBotOrchestratorRuntimeEnv,
  verifyBotOrchestratorSecurityContext,
  verifyBotOrchestratorSecretEnv,
  verifyBotImagePullSecret,
  verifyBotRunnerControlPlaneResources,
  verifyBotRunnerNetworkPolicy,
  verifyExactIngressPolicy,
  verifyHaproxyClusterRole,
  verifyManifestResourceIdentities,
  verifyManifestResourceInventory,
  verifyNoYamlIndirections,
  verifyNoReticulumHorizontalPodAutoscaler,
  verifyReticulumBotRunnerAuthorityContract
} = require("../verify-manifest-contracts");

function validContainer() {
  return {
    env: [
      {
        name: "BOT_ORCHESTRATOR_ACCESS_KEY",
        valueFrom: { secretKeyRef: { name: "configs", key: "BOT_ORCHESTRATOR_ACCESS_KEY" } }
      },
      {
        name: "OPENAI_API_KEY",
        valueFrom: { secretKeyRef: { name: "configs", key: "OPENAI_API_KEY" } }
      }
    ]
  };
}

function validBotContainer() {
  return {
    ...validContainer(),
    securityContext: {
      runAsNonRoot: true,
      runAsUser: 1000,
      runAsGroup: 1000,
      allowPrivilegeEscalation: false,
      readOnlyRootFilesystem: true,
      capabilities: { drop: ["ALL"] },
      seccompProfile: { type: "RuntimeDefault" }
    }
  };
}

function validRuntimeContainer() {
  const secrets = Object.fromEntries(validContainer().env.map(entry => [entry.name, entry]));
  const downwardApi = {
    POD_NAMESPACE: "metadata.namespace",
    ORCHESTRATOR_POD_NAME: "metadata.name",
    ORCHESTRATOR_POD_UID: "metadata.uid"
  };
  return {
    env: BOT_ORCHESTRATOR_ALLOWED_ENV_NAMES.map(name =>
      secrets[name]
        ? clone(secrets[name])
        : downwardApi[name]
          ? {
              name,
              valueFrom: { fieldRef: { apiVersion: "v1", fieldPath: downwardApi[name] } }
            }
          : {
              name,
              value:
                name === "BOT_RUNNER_IMAGE"
                  ? `registry.invalid/bot-runner@sha256:${"a".repeat(64)}`
                  : BOT_ORCHESTRATOR_RUNTIME_ENV[name] ?? "test-literal"
            }
    )
  };
}

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function validResources() {
  return [
    { apiVersion: "v1", kind: "Secret", metadata: { name: "configs" } },
    {
      apiVersion: "apps/v1",
      kind: "Deployment",
      metadata: { name: "bot-orchestrator" },
      spec: {
        replicas: 1,
        strategy: { type: "Recreate" },
        template: {
          spec: { containers: [{ name: "bot-orchestrator", env: validContainer().env }] }
        }
      }
    },
    { apiVersion: "v1", kind: "ConfigMap", metadata: { name: "other", namespace: "default" } }
  ];
}

function apiVersionForGroup(group) {
  if (group === "") return "v1";
  if (group === "apps") return "apps/v1";
  if (group === "networking.k8s.io") return "networking.k8s.io/v1";
  if (group === "rbac.authorization.k8s.io") return "rbac.authorization.k8s.io/v1";
  throw new Error(`unsupported test API group: ${group}`);
}

function validInventoryResources(namespace = "hcce") {
  return expectedManifestInventory(namespace).map(([group, kind, resourceNamespace, name]) => {
    const resource = {
      apiVersion: apiVersionForGroup(group),
      kind,
      metadata: { name }
    };
    if (resourceNamespace) resource.metadata.namespace = resourceNamespace;
    if (kind === "Deployment") {
      resource.spec = {
        template: {
          spec: {
            containers: AUDITED_DEPLOYMENT_CONTAINERS[name].map(containerName => ({ name: containerName }))
          }
        }
      };
    }
    return resource;
  });
}

test("accepts exactly one exclusive configs SecretKeyRef for each parent credential", () => {
  assert.deepEqual(verifyBotOrchestratorSecretEnv(validContainer()), []);
});

test("rejects missing and duplicate parent credential environment entries", () => {
  for (const name of ["BOT_ORCHESTRATOR_ACCESS_KEY", "OPENAI_API_KEY"]) {
    const missing = validContainer();
    missing.env = missing.env.filter(entry => entry.name !== name);
    assert.notDeepEqual(verifyBotOrchestratorSecretEnv(missing), [], `${name} missing`);

    const duplicate = validContainer();
    duplicate.env.push(clone(duplicate.env.find(entry => entry.name === name)));
    assert.notDeepEqual(verifyBotOrchestratorSecretEnv(duplicate), [], `${name} duplicate`);
  }
});

test("rejects literals, wrong Secret refs and extra fields for parent credentials", () => {
  for (const name of ["BOT_ORCHESTRATOR_ACCESS_KEY", "OPENAI_API_KEY"]) {
    const index = validContainer().env.findIndex(entry => entry.name === name);
    const cases = [
      entry => {
        delete entry.valueFrom;
        entry.value = "literal-must-not-be-accepted";
      },
      entry => {
        entry.value = "literal-alongside-reference";
      },
      entry => {
        entry.valueFrom.secretKeyRef.name = "other-secret";
      },
      entry => {
        entry.valueFrom.secretKeyRef.key = "WRONG_KEY";
      },
      entry => {
        entry.extra = true;
      },
      entry => {
        entry.valueFrom.extra = true;
      },
      entry => {
        entry.valueFrom.secretKeyRef.optional = false;
      }
    ];

    for (const mutate of cases) {
      const container = validContainer();
      mutate(container.env[index]);
      assert.notDeepEqual(verifyBotOrchestratorSecretEnv(container), [], `${name} mutation rejected`);
    }
  }

  const leakedMaster = validContainer();
  leakedMaster.env.push({
    name: "BOT_RUNNER_ACCESS_KEY",
    valueFrom: { secretKeyRef: { name: "configs", key: "BOT_RUNNER_ACCESS_KEY" } }
  });
  assert.match(verifyBotOrchestratorSecretEnv(leakedMaster).join("\n"), /must never receive/);
});

test("requires the exact fail-closed bot securityContext", () => {
  assert.deepEqual(verifyBotOrchestratorSecurityContext(validBotContainer()), []);

  const mutations = [
    container => { container.securityContext.privileged = true; },
    container => { container.securityContext.readOnlyRootFilesystem = false; },
    container => { container.securityContext.allowPrivilegeEscalation = true; },
    container => { container.securityContext.runAsNonRoot = false; },
    container => { container.securityContext.runAsUser = "1000"; },
    container => { container.securityContext.runAsGroup = 0; },
    container => { container.securityContext.capabilities.add = ["SYS_ADMIN"]; },
    container => { container.securityContext.capabilities.drop.push("NET_RAW"); },
    container => { container.securityContext.seccompProfile.type = "Unconfined"; },
    container => { container.securityContext.seccompProfile.extra = true; },
    container => { delete container.securityContext.readOnlyRootFilesystem; },
    container => { delete container.securityContext.capabilities; }
  ];

  for (const mutate of mutations) {
    const container = validBotContainer();
    mutate(container);
    assert.notDeepEqual(verifyBotOrchestratorSecurityContext(container), []);
  }
});

test("requires globally unique resource identities and exact critical resource cardinality", () => {
  const resources = validResources();
  assert.deepEqual(verifyManifestResourceIdentities(resources), []);

  const duplicateConfigMap = [...resources, clone(resources[2])];
  assert.match(verifyManifestResourceIdentities(duplicateConfigMap).join("\n"), /globally unique/);
  const alternateApiVersion = clone(resources[1]);
  alternateApiVersion.apiVersion = "apps/v1beta2";
  assert.match(
    verifyManifestResourceIdentities([...resources, alternateApiVersion]).join("\n"),
    /globally unique/
  );

  const secondSecret = [...resources, clone(resources[0])];
  const secretErrors = verifyManifestResourceIdentities(secondSecret).join("\n");
  assert.match(secretErrors, /globally unique/);
  assert.match(secretErrors, /exactly one Secret\/configs/);

  const secondDeployment = [...resources, clone(resources[1])];
  const deploymentErrors = verifyManifestResourceIdentities(secondDeployment).join("\n");
  assert.match(deploymentErrors, /globally unique/);
  assert.match(deploymentErrors, /exactly one Deployment\/bot-orchestrator/);
});

test("requires the exact 50-resource apiVersion/kind/namespace/name inventory", () => {
  const resources = validInventoryResources();
  assert.equal(resources.length, 50);
  assert.deepEqual(verifyManifestResourceInventory(resources), []);

  const extraJob = {
    apiVersion: "batch/v1",
    kind: "Job",
    metadata: { namespace: "hcce", name: "unexpected" }
  };
  const extraErrors = verifyManifestResourceInventory([...resources, extraJob]).join("\n");
  assert.match(extraErrors, /unexpected resource/);
  assert.match(extraErrors, /exactly 50/);

  const wrongNamespace = clone(resources);
  wrongNamespace.find(resource => resource.kind === "Secret").metadata.namespace = "other";
  const namespaceErrors = verifyManifestResourceInventory(wrongNamespace).join("\n");
  assert.match(namespaceErrors, /inventory is missing/);
  assert.match(namespaceErrors, /unexpected resource/);

  const manualResources = validInventoryResources();
  manualResources.push(
    { apiVersion: "v1", kind: "PersistentVolume", metadata: { name: "pgsql-pv" } },
    { apiVersion: "v1", kind: "PersistentVolume", metadata: { name: "ret-pv" } }
  );
  assert.equal(manualResources.length, 52);
  assert.deepEqual(verifyManifestResourceInventory(manualResources), []);

  const incompleteManual = manualResources.filter(resource => resource.metadata?.name !== "ret-pv");
  assert.match(verifyManifestResourceInventory(incompleteManual).join("\n"), /ret-pv/);

  for (const [prefix, replacement] of [
    ["apps/", "apps/v999"],
    ["networking.k8s.io/", "networking.k8s.io/v999"],
    ["rbac.authorization.k8s.io/", "rbac.authorization.k8s.io/v999"],
    ["v1", "v999"]
  ]) {
    const changed = clone(resources);
    const resource = changed.find(value =>
      prefix === "v1" ? value.apiVersion === "v1" : value.apiVersion.startsWith(prefix)
    );
    resource.apiVersion = replacement;
    assert.match(verifyManifestResourceInventory(changed).join("\n"), /exact apiVersion/);
  }
});

test("rejects alternate-api-version duplicates because version is not resource identity", () => {
  const resources = validInventoryResources();
  const deployment = clone(resources.find(resource => resource.kind === "Deployment"));
  deployment.apiVersion = "apps/v1beta2";
  assert.match(
    verifyManifestResourceIdentities([...resources, deployment]).join("\n"),
    /globally unique by API group/
  );
});

test("requires exact audited containers and forbids init and ephemeral containers", () => {
  const resources = validInventoryResources();
  assert.deepEqual(verifyAuditedDeploymentContainers(resources, "hcce"), []);

  for (const field of ["initContainers", "ephemeralContainers"]) {
    const changed = clone(resources);
    const deployment = changed.find(resource => resource.kind === "Deployment");
    deployment.spec.template.spec[field] = [{ name: "unexpected" }];
    assert.match(
      verifyAuditedDeploymentContainers(changed, "hcce").join("\n"),
      new RegExp(field)
    );
  }

  const extraContainer = clone(resources);
  extraContainer
    .find(resource => resource.metadata.name === "bot-orchestrator")
    .spec.template.spec.containers.push({ name: "sidecar" });
  assert.match(
    verifyAuditedDeploymentContainers(extraContainer, "hcce").join("\n"),
    /containers must be exactly/
  );
});

test("requires exactly one bot-orchestrator container", () => {
  const deployment = validResources()[1];
  assert.deepEqual(verifyBotOrchestratorContainers(deployment), []);
  deployment.spec.template.spec.containers.push(clone(deployment.spec.template.spec.containers[0]));
  assert.match(verifyBotOrchestratorContainers(deployment).join("\n"), /exactly one/);
});

test("requires exactly one Recreate bot-orchestrator replica", () => {
  const deployment = validResources()[1];
  assert.deepEqual(verifyBotOrchestratorDeploymentContract(deployment), []);

  const mutations = [
    value => delete value.spec.replicas,
    value => { value.spec.replicas = 0; },
    value => { value.spec.replicas = 2; },
    value => { value.spec.replicas = "1"; },
    value => delete value.spec.strategy,
    value => { value.spec.strategy = { type: "RollingUpdate" }; },
    value => { value.spec.strategy = { type: "Recreate", rollingUpdate: {} }; }
  ];

  for (const mutate of mutations) {
    const changed = clone(deployment);
    mutate(changed);
    assert.notDeepEqual(verifyBotOrchestratorDeploymentContract(changed), []);
  }
});

test("pins the production ghost runner path and navigation contract exactly once", () => {
  assert.deepEqual(verifyBotOrchestratorRuntimeEnv(validRuntimeContainer()), []);

  for (const name of Object.keys(BOT_ORCHESTRATOR_RUNTIME_ENV)) {
    const missing = validRuntimeContainer();
    missing.env = missing.env.filter(entry => entry.name !== name);
    assert.notDeepEqual(verifyBotOrchestratorRuntimeEnv(missing), [], `${name} missing`);

    const duplicate = validRuntimeContainer();
    duplicate.env.push(clone(duplicate.env.find(entry => entry.name === name)));
    assert.notDeepEqual(verifyBotOrchestratorRuntimeEnv(duplicate), [], `${name} duplicate`);

    const indirect = validRuntimeContainer();
    const entry = indirect.env.find(value => value.name === name);
    delete entry.value;
    entry.valueFrom = { configMapKeyRef: { name: "runtime", key: name } };
    assert.notDeepEqual(verifyBotOrchestratorRuntimeEnv(indirect), [], `${name} indirect`);
  }

  const wrongGhostPath = validRuntimeContainer();
  wrongGhostPath.env.find(entry => entry.name === "GHOST_RUNNER_SCRIPT").value = "/app/other.js";
  assert.match(verifyBotOrchestratorRuntimeEnv(wrongGhostPath).join("\n"), /GHOST_RUNNER_SCRIPT/);

  const wrongRaycast = validRuntimeContainer();
  wrongRaycast.env.find(entry => entry.name === "GHOST_RAYCAST_MODE").value = "disabled";
  assert.match(verifyBotOrchestratorRuntimeEnv(wrongRaycast).join("\n"), /GHOST_RAYCAST_MODE/);

  for (const injectedName of ["NODE_OPTIONS", "RET_INTERNAL_ENDPOINT_OVERRIDE", "OPENAI_ENDPOINT"]) {
    const injected = validRuntimeContainer();
    injected.env.push({ name: injectedName, value: "forbidden" });
    assert.match(verifyBotOrchestratorRuntimeEnv(injected).join("\n"), /audited allowlist/);
  }

  const indirectLiteral = validRuntimeContainer();
  const literal = indirectLiteral.env.find(entry => entry.name === "HUBS_BASE_URL");
  delete literal.value;
  literal.valueFrom = { configMapKeyRef: { name: "runtime", key: "HUBS_BASE_URL" } };
  assert.notDeepEqual(verifyBotOrchestratorRuntimeEnv(indirectLiteral), []);
});

test("pins the bot pod to one bounded tmp volume without executable injection fields", () => {
  const deployment = {
    spec: {
      template: {
        spec: {
          containers: [
            {
              name: "bot-orchestrator",
              volumeMounts: [{ name: "bot-orchestrator-tmp", mountPath: "/tmp" }]
            }
          ],
          serviceAccountName: "bot-orchestrator",
          automountServiceAccountToken: true,
          imagePullSecrets: [{ name: "bot-images-pull" }],
          volumes: [{ name: "bot-orchestrator-tmp", emptyDir: { sizeLimit: "256Mi" } }]
        }
      }
    }
  };
  assert.deepEqual(verifyBotOrchestratorIsolationContract(deployment), []);

  const mutations = [
    value => { value.spec.template.spec.containers[0].command = ["node"]; },
    value => { value.spec.template.spec.containers[0].args = ["/mounted/payload.js"]; },
    value => { value.spec.template.spec.containers[0].lifecycle = {}; },
    value => { value.spec.template.spec.containers[0].envFrom = []; },
    value => value.spec.template.spec.containers[0].volumeMounts.push({ name: "payload", mountPath: "/app/payload" }),
    value => value.spec.template.spec.volumes.push({ name: "payload", configMap: { name: "ret-config" } }),
    value => { value.spec.template.spec.initContainers = []; },
    value => { value.spec.template.spec.ephemeralContainers = []; }
  ];
  for (const mutate of mutations) {
    const changed = clone(deployment);
    mutate(changed);
    assert.notDeepEqual(verifyBotOrchestratorIsolationContract(changed), []);
  }
});

test("keeps the tracked production template on the exact ghost runtime contract", () => {
  const templatePath = path.resolve(__dirname, "../hcce.yam");
  const documents = YAML.parseAllDocuments(fs.readFileSync(templatePath, "utf8"));
  assert.deepEqual(documents.flatMap(document => document.errors), []);
  const deployment = documents
    .map(document => document.toJS())
    .filter(Boolean)
    .find(resource => resource.kind === "Deployment" && resource.metadata?.name === "bot-orchestrator");
  const container = deployment?.spec?.template?.spec?.containers?.find(
    value => value.name === "bot-orchestrator"
  );
  assert.deepEqual(verifyBotOrchestratorRuntimeEnv(container), []);
  assert.deepEqual(verifyBotOrchestratorIsolationContract(deployment), []);
});

test("keeps exact minimal ServiceAccounts, namespaced Pod RBAC, and runner NetworkPolicy", () => {
  const templatePath = path.resolve(__dirname, "../hcce.yam");
  const resources = YAML.parseAllDocuments(fs.readFileSync(templatePath, "utf8"))
    .map(document => document.toJS())
    .filter(Boolean);
  const pullSecret = resources.find(
    resource => resource.kind === "Secret" && resource.metadata?.name === "bot-images-pull"
  );
  assert.equal(pullSecret.data[".dockerconfigjson"], "$BOT_IMAGE_PULL_CONFIG_JSON_BASE64");
  const botContainer = resources
    .find(resource => resource.kind === "Deployment" && resource.metadata?.name === "bot-orchestrator")
    .spec.template.spec.containers.find(container => container.name === "bot-orchestrator");
  botContainer.image = `registry.invalid/yenhubs/bot-orchestrator@sha256:${"a".repeat(64)}`;
  botContainer.env.find(entry => entry.name === "BOT_RUNNER_IMAGE").value =
    `registry.invalid/yenhubs/bot-runner@sha256:${"b".repeat(64)}`;
  pullSecret.data[".dockerconfigjson"] = Buffer.from(
    JSON.stringify({
      auths: { "registry.invalid": { auth: Buffer.from("ci-user:ci-token").toString("base64") } }
    }),
    "utf8"
  ).toString("base64");

  assert.deepEqual(verifyBotImagePullSecret(resources, "$Namespace"), []);
  assert.deepEqual(verifyBotRunnerControlPlaneResources(resources, "$Namespace"), []);
  const policy = resources.find(
    resource => resource.kind === "NetworkPolicy" && resource.metadata?.name === "bot-runner-isolation"
  );
  assert.deepEqual(verifyBotRunnerNetworkPolicy(policy), []);

  const expandedRole = clone(resources);
  expandedRole
    .find(resource => resource.kind === "Role" && resource.metadata?.name === "bot-orchestrator-runner-pods")
    .rules[0].verbs.push("patch");
  assert.match(
    verifyBotRunnerControlPlaneResources(expandedRole, "$Namespace").join("\n"),
    /minimal runner-Pod RBAC/
  );

  const tokenizedRunner = clone(resources);
  tokenizedRunner
    .find(resource => resource.kind === "ServiceAccount" && resource.metadata?.name === "bot-runner")
    .automountServiceAccountToken = true;
  assert.notDeepEqual(verifyBotRunnerControlPlaneResources(tokenizedRunner, "$Namespace"), []);

  const malformedPullSecret = clone(resources);
  malformedPullSecret
    .find(resource => resource.kind === "Secret" && resource.metadata?.name === "bot-images-pull")
    .data[".dockerconfigjson"] = "not-base64";
  assert.notDeepEqual(verifyBotImagePullSecret(malformedPullSecret, "$Namespace"), []);

  const emptyPullCredential = clone(resources);
  emptyPullCredential
    .find(resource => resource.kind === "Secret" && resource.metadata?.name === "bot-images-pull")
    .data[".dockerconfigjson"] = Buffer.from(
      JSON.stringify({ auths: { "registry.invalid": {} } }),
      "utf8"
    ).toString("base64");
  assert.notDeepEqual(verifyBotImagePullSecret(emptyPullCredential, "$Namespace"), []);

  const wrongPullRegistry = clone(resources);
  wrongPullRegistry
    .find(resource => resource.kind === "Secret" && resource.metadata?.name === "bot-images-pull")
    .data[".dockerconfigjson"] = Buffer.from(
      JSON.stringify({
        auths: { "other.invalid": { auth: Buffer.from("ci-user:ci-token").toString("base64") } }
      }),
      "utf8"
    ).toString("base64");
  assert.notDeepEqual(verifyBotImagePullSecret(wrongPullRegistry, "$Namespace"), []);

  const broadenedPolicy = clone(policy);
  broadenedPolicy.spec.egress.push({ to: [{}] });
  assert.notDeepEqual(verifyBotRunnerNetworkPolicy(broadenedPolicy), []);
});

test("keeps Reticulum singleton until multi-replica storage and operational gates are staged", () => {
  const deployment = { spec: { replicas: 1, strategy: { type: "Recreate" } } };
  assert.deepEqual(verifyReticulumBotRunnerAuthorityContract(deployment), []);

  for (const replicas of [undefined, 0, 2, "1"]) {
    const changed = clone(deployment);
    if (replicas === undefined) delete changed.spec.replicas;
    else changed.spec.replicas = replicas;
    assert.notDeepEqual(verifyReticulumBotRunnerAuthorityContract(changed), []);
  }

  const strategyMutations = [
    value => delete value.spec.strategy,
    value => { value.spec.strategy = { type: "RollingUpdate" }; },
    value => {
      value.spec.strategy = {
        type: "RollingUpdate",
        rollingUpdate: { maxSurge: 1, maxUnavailable: 0 }
      };
    },
    value => { value.spec.strategy = { type: "Recreate", rollingUpdate: { maxSurge: 0 } }; }
  ];

  for (const mutate of strategyMutations) {
    const changed = clone(deployment);
    mutate(changed);
    assert.notDeepEqual(verifyReticulumBotRunnerAuthorityContract(changed), []);
  }
});

test("rejects every HorizontalPodAutoscaler targeting Reticulum", () => {
  assert.deepEqual(verifyNoReticulumHorizontalPodAutoscaler([]), []);

  const autoscaler = {
    apiVersion: "autoscaling/v2",
    kind: "HorizontalPodAutoscaler",
    metadata: { name: "reticulum-capacity", namespace: "hcce" },
    spec: {
      scaleTargetRef: { apiVersion: "apps/v1", kind: "Deployment", name: "reticulum" },
      minReplicas: 1,
      maxReplicas: 2
    }
  };

  assert.match(
    verifyNoReticulumHorizontalPodAutoscaler([autoscaler]).join("\n"),
    /must not target Reticulum/
  );

  const otherTarget = clone(autoscaler);
  otherTarget.spec.scaleTargetRef.name = "dialog";
  assert.deepEqual(verifyNoReticulumHorizontalPodAutoscaler([otherTarget]), []);

  for (const group of ["autoscaling/v1", "autoscaling/v2beta2"]) {
    const changed = clone(autoscaler);
    changed.apiVersion = group;
    assert.notDeepEqual(verifyNoReticulumHorizontalPodAutoscaler([changed]), []);
  }
});

test("rejects YAML anchors, aliases and merge keys including inherited env", () => {
  const anchoredEnv = YAML.parseAllDocuments(`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: bot-orchestrator
spec:
  template:
    spec:
      containers:
      - name: defaults
        env: &bot-env
        - name: BOT_ACCESS_KEY
          value: forbidden
      - name: bot-orchestrator
        env: *bot-env
`);
  const anchorErrors = verifyNoYamlIndirections(anchoredEnv, YAML).join("\n");
  assert.match(anchorErrors, /anchors/);
  assert.match(anchorErrors, /aliases/);

  const mergedEnv = YAML.parseAllDocuments(`
defaults: &defaults
  env:
  - name: BOT_ACCESS_KEY
    value: forbidden
container:
  <<: *defaults
  name: bot-orchestrator
`);
  const mergeErrors = verifyNoYamlIndirections(mergedEnv, YAML).join("\n");
  assert.match(mergeErrors, /anchors/);
  assert.match(mergeErrors, /aliases/);
  assert.match(mergeErrors, /merge keys/);
});

test("requires one exact closed ingress rule with exact peers and TCP port", () => {
  const contract = {
    name: "pgsql-ingress",
    targetApp: "pgsql",
    allowedApps: ["pgbouncer", "pgbouncer-t"],
    port: 5432
  };
  const valid = {
    spec: {
      podSelector: { matchLabels: { app: "pgsql" } },
      policyTypes: ["Ingress"],
      ingress: [
        {
          from: [
            { podSelector: { matchLabels: { app: "pgbouncer-t" } } },
            { podSelector: { matchLabels: { app: "pgbouncer" } } }
          ],
          ports: [{ protocol: "TCP", port: 5432 }]
        }
      ]
    }
  };

  assert.deepEqual(verifyExactIngressPolicy(valid, contract), []);

  const mutations = [
    policy => policy.spec.ingress.push({}),
    policy => delete policy.spec.ingress[0].from,
    policy => delete policy.spec.ingress[0].ports,
    policy => { policy.spec.ingress[0].from[0].namespaceSelector = {}; },
    policy => { policy.spec.ingress[0].from[0].podSelector.matchExpressions = []; },
    policy => policy.spec.ingress[0].from.push({ podSelector: { matchLabels: { app: "other" } } }),
    policy => policy.spec.ingress[0].ports.push({ protocol: "TCP", port: 9999 }),
    policy => { policy.spec.ingress[0].ports[0].endPort = 5432; },
    policy => { policy.spec.ingress[0].unexpected = true; },
    policy => policy.spec.policyTypes.push("Egress"),
    policy => { policy.spec.unexpected = true; }
  ];

  for (const mutate of mutations) {
    const changed = clone(valid);
    mutate(changed);
    assert.notDeepEqual(verifyExactIngressPolicy(changed, contract), []);
  }
});

test("requires the complete canonical HAProxy ClusterRole and rejects every added capability", () => {
  const valid = clone(HAPROXY_CLUSTER_ROLE);
  valid.rules.reverse();
  valid.rules.forEach(rule => {
    rule.apiGroups.reverse();
    rule.resources.reverse();
    rule.verbs.reverse();
  });
  assert.deepEqual(verifyHaproxyClusterRole(valid), []);

  const mutations = [
    role => role.rules.push({ apiGroups: ["*"], resources: ["*"], verbs: ["*"] }),
    role => role.rules[0].apiGroups.push("apps"),
    role => role.rules[0].resources.push("secrets"),
    role => role.rules[0].verbs.push("delete"),
    role => { role.rules[0].resourceNames = ["extra"]; },
    role => role.rules.pop(),
    role => { role.aggregationRule = {}; },
    role => { role.metadata.labels = { unexpected: "true" }; }
  ];

  for (const mutate of mutations) {
    const changed = clone(HAPROXY_CLUSTER_ROLE);
    mutate(changed);
    assert.notDeepEqual(verifyHaproxyClusterRole(changed), []);
  }
});
