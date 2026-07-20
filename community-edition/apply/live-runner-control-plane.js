const { isDeepStrictEqual } = require("node:util");
const {
  ADMISSION_POLICY_NAME,
  CUTOVER_JOURNAL_POLICY_NAME,
  PARENT_FENCE_POLICY_NAME,
  RUNNER_PROTOCOL_POLICY_NAME,
  RUNNER_NAMESPACE,
  admissionPolicyIsObserved,
  exactAdmissionBinding,
  exactCutoverJournalBinding,
  exactParentFenceBinding,
  exactRunnerProtocolBinding
} = require("./runner-activation");

const SERVER_OWNED_METADATA_FIELDS = new Set([
  "creationTimestamp",
  "generation",
  "managedFields",
  "resourceVersion",
  "uid"
]);

function identity(resource) {
  const apiGroup = resource?.apiVersion?.includes("/")
    ? resource.apiVersion.split("/")[0]
    : "";
  return [
    apiGroup,
    resource?.kind || "",
    resource?.metadata?.namespace || "",
    resource?.metadata?.name || ""
  ];
}

function identityKey(resource) {
  return JSON.stringify(identity(resource));
}

function serverProjection(actual, expected) {
  const metadata = {};
  for (const key of Object.keys(expected?.metadata || {})) {
    metadata[key] = actual?.metadata?.[key];
  }
  if (expected?.kind === "Namespace" && metadata.labels) {
    const labels = { ...metadata.labels };
    delete labels["kubernetes.io/metadata.name"];
    metadata.labels = labels;
  }
  if (metadata.annotations) {
    const annotations = { ...metadata.annotations };
    delete annotations["kubectl.kubernetes.io/last-applied-configuration"];
    metadata.annotations = annotations;
  }
  const projected = {
    apiVersion: actual?.apiVersion,
    kind: actual?.kind,
    metadata
  };
  for (const key of Object.keys(expected || {})) {
    if (!["apiVersion", "kind", "metadata"].includes(key)) projected[key] = actual?.[key];
  }
  return projected;
}

function metadataMap(value, { namespaceLabels = false, annotations = false } = {}) {
  const normalized = value && typeof value === "object" && !Array.isArray(value)
    ? { ...value }
    : {};
  if (namespaceLabels) delete normalized["kubernetes.io/metadata.name"];
  if (annotations) delete normalized["kubectl.kubernetes.io/last-applied-configuration"];
  return normalized;
}

function operationalDriftErrors(actual, expected) {
  const errors = [];
  const metadata = actual?.metadata;
  if (!metadata || metadata.deletionTimestamp !== undefined) errors.push("terminating");
  if (typeof metadata?.uid !== "string" || !metadata.uid) errors.push("metadata_uid_missing");
  if (typeof metadata?.resourceVersion !== "string" || !metadata.resourceVersion) {
    errors.push("metadata_resource_version_missing");
  }
  const expectedMetadata = expected?.metadata || {};
  for (const field of Object.keys(metadata || {})) {
    if (
      !Object.hasOwn(expectedMetadata, field) &&
      !SERVER_OWNED_METADATA_FIELDS.has(field) &&
      !["annotations", "labels"].includes(field)
    ) {
      errors.push(`metadata_${field}_unexpected`);
    }
  }
  const actualLabels = metadataMap(metadata?.labels, { namespaceLabels: actual?.kind === "Namespace" });
  const expectedLabels = metadataMap(expectedMetadata.labels);
  if (!isDeepStrictEqual(actualLabels, expectedLabels)) errors.push("metadata_labels_drift");
  const actualAnnotations = metadataMap(metadata?.annotations, { annotations: true });
  const expectedAnnotations = metadataMap(expectedMetadata.annotations);
  if (!isDeepStrictEqual(actualAnnotations, expectedAnnotations)) errors.push("metadata_annotations_drift");

  if (actual?.kind === "Namespace") {
    if (!isDeepStrictEqual(actual?.spec, { finalizers: ["kubernetes"] })) {
      errors.push("namespace_spec_finalizers_drift");
    }
    if (actual?.status?.phase !== "Active") errors.push("namespace_not_active");
  }

  if (actual?.kind === "Secret") {
    if (actual.immutable !== undefined && actual.immutable !== false && actual.immutable !== expected?.immutable) {
      errors.push("secret_immutable_unexpected");
    }
    if (
      expected?.binaryData === undefined &&
      actual?.binaryData !== undefined &&
      (!actual.binaryData || Object.keys(actual.binaryData).length !== 0)
    ) {
      errors.push("secret_binary_data_unexpected");
    }
  }
  if (actual?.kind === "ServiceAccount") {
    for (const field of ["secrets", "imagePullSecrets"]) {
      if (
        expected?.[field] === undefined &&
        actual?.[field] !== undefined &&
        (!Array.isArray(actual[field]) || actual[field].length !== 0)
      ) {
        errors.push(`serviceaccount_${field}_unexpected`);
      }
    }
  }
  return errors;
}

function expectedIdentities(namespace) {
  return [
    ["", "Namespace", "", RUNNER_NAMESPACE],
    ["", "Secret", RUNNER_NAMESPACE, "bot-images-pull"],
    ["", "ServiceAccount", namespace, "bot-orchestrator"],
    ["rbac.authorization.k8s.io", "Role", namespace, "bot-orchestrator-runner-pods"],
    ["rbac.authorization.k8s.io", "RoleBinding", namespace, "bot-orchestrator-runner-pods"],
    ["", "ServiceAccount", RUNNER_NAMESPACE, "bot-runner"],
    ["", "ServiceAccount", RUNNER_NAMESPACE, "bot-runner-guard"],
    ["", "ResourceQuota", RUNNER_NAMESPACE, "bot-runner-capacity"],
    ["", "ResourceQuota", RUNNER_NAMESPACE, "bot-runner-guard-capacity"],
    ["rbac.authorization.k8s.io", "Role", RUNNER_NAMESPACE, "bot-orchestrator-runner-pods"],
    ["rbac.authorization.k8s.io", "RoleBinding", RUNNER_NAMESPACE, "bot-orchestrator-runner-pods"],
    ["networking.k8s.io", "NetworkPolicy", RUNNER_NAMESPACE, "bot-runner-default-deny"],
    ["networking.k8s.io", "NetworkPolicy", RUNNER_NAMESPACE, "bot-runner-egress"],
    ["admissionregistration.k8s.io", "ValidatingAdmissionPolicy", "", ADMISSION_POLICY_NAME],
    ["admissionregistration.k8s.io", "ValidatingAdmissionPolicyBinding", "", ADMISSION_POLICY_NAME],
    ["admissionregistration.k8s.io", "ValidatingAdmissionPolicy", "", RUNNER_PROTOCOL_POLICY_NAME],
    ["admissionregistration.k8s.io", "ValidatingAdmissionPolicyBinding", "", RUNNER_PROTOCOL_POLICY_NAME],
    ["admissionregistration.k8s.io", "ValidatingAdmissionPolicy", "", CUTOVER_JOURNAL_POLICY_NAME],
    ["admissionregistration.k8s.io", "ValidatingAdmissionPolicyBinding", "", CUTOVER_JOURNAL_POLICY_NAME],
    ["admissionregistration.k8s.io", "ValidatingAdmissionPolicy", "", PARENT_FENCE_POLICY_NAME],
    ["admissionregistration.k8s.io", "ValidatingAdmissionPolicyBinding", "", PARENT_FENCE_POLICY_NAME]
  ];
}

function verifyLiveRunnerControlPlane(liveResources, generatedResources, namespace) {
  const errors = [];
  const liveByIdentity = new Map(liveResources.map(resource => [identityKey(resource), resource]));
  const generatedByIdentity = new Map(
    generatedResources.map(resource => [identityKey(resource), resource])
  );
  for (const expectedIdentity of expectedIdentities(namespace)) {
    const key = JSON.stringify(expectedIdentity);
    const expected = generatedByIdentity.get(key);
    const actual = liveByIdentity.get(key);
    if (
      !expected ||
      !actual ||
      operationalDriftErrors(actual, expected).length > 0 ||
      !isDeepStrictEqual(serverProjection(actual, expected), expected)
    ) {
      errors.push(`live_runner_control_plane_drift:${expectedIdentity.join("/")}`);
    }
  }

  const runnerRoleIdentities = liveResources.filter(resource =>
    resource?.metadata?.namespace === RUNNER_NAMESPACE && resource?.kind === "Role"
  );
  const runnerBindingIdentities = liveResources.filter(resource =>
    resource?.metadata?.namespace === RUNNER_NAMESPACE && resource?.kind === "RoleBinding"
  );
  const runnerPolicies = liveResources.filter(resource =>
    resource?.metadata?.namespace === RUNNER_NAMESPACE && resource?.kind === "NetworkPolicy"
  );
  if (runnerRoleIdentities.length !== 1) errors.push("runner_namespace_must_have_exactly_one_role");
  if (runnerBindingIdentities.length !== 1) errors.push("runner_namespace_must_have_exactly_one_rolebinding");
  if (runnerPolicies.length !== 2) errors.push("runner_namespace_must_have_exactly_two_networkpolicies");

  const policy = liveByIdentity.get(JSON.stringify([
    "admissionregistration.k8s.io", "ValidatingAdmissionPolicy", "", ADMISSION_POLICY_NAME
  ]));
  const binding = liveByIdentity.get(JSON.stringify([
    "admissionregistration.k8s.io", "ValidatingAdmissionPolicyBinding", "", ADMISSION_POLICY_NAME
  ]));
  const parentFencePolicy = liveByIdentity.get(JSON.stringify([
    "admissionregistration.k8s.io", "ValidatingAdmissionPolicy", "", PARENT_FENCE_POLICY_NAME
  ]));
  const parentFenceBinding = liveByIdentity.get(JSON.stringify([
    "admissionregistration.k8s.io", "ValidatingAdmissionPolicyBinding", "", PARENT_FENCE_POLICY_NAME
  ]));
  const runnerProtocolPolicy = liveByIdentity.get(JSON.stringify([
    "admissionregistration.k8s.io", "ValidatingAdmissionPolicy", "", RUNNER_PROTOCOL_POLICY_NAME
  ]));
  const runnerProtocolBinding = liveByIdentity.get(JSON.stringify([
    "admissionregistration.k8s.io", "ValidatingAdmissionPolicyBinding", "", RUNNER_PROTOCOL_POLICY_NAME
  ]));
  const cutoverJournalPolicy = liveByIdentity.get(JSON.stringify([
    "admissionregistration.k8s.io", "ValidatingAdmissionPolicy", "", CUTOVER_JOURNAL_POLICY_NAME
  ]));
  const cutoverJournalBinding = liveByIdentity.get(JSON.stringify([
    "admissionregistration.k8s.io", "ValidatingAdmissionPolicyBinding", "", CUTOVER_JOURNAL_POLICY_NAME
  ]));
  if (!admissionPolicyIsObserved(policy)) errors.push("runner_admission_policy_not_observed_or_typechecked");
  if (!exactAdmissionBinding(binding)) errors.push("runner_admission_binding_not_exact");
  if (!admissionPolicyIsObserved(runnerProtocolPolicy)) {
    errors.push("runner_protocol_admission_policy_not_observed_or_typechecked");
  }
  if (!exactRunnerProtocolBinding(runnerProtocolBinding)) {
    errors.push("runner_protocol_admission_binding_not_exact");
  }
  if (!admissionPolicyIsObserved(cutoverJournalPolicy)) {
    errors.push("cutover_journal_admission_policy_not_observed_or_typechecked");
  }
  if (!exactCutoverJournalBinding(cutoverJournalBinding, namespace)) {
    errors.push("cutover_journal_admission_binding_not_exact");
  }
  if (!admissionPolicyIsObserved(parentFencePolicy)) {
    errors.push("parent_fence_admission_policy_not_observed_or_typechecked");
  }
  if (!exactParentFenceBinding(parentFenceBinding, namespace)) {
    errors.push("parent_fence_admission_binding_not_exact");
  }
  return errors;
}

function collectLiveRunnerControlPlane(kubectlJson, namespace) {
  const runnerNamespace = kubectlJson(["get", "namespace", RUNNER_NAMESPACE, "-o", "json"]);
  const runnerSecret = kubectlJson([
    "-n", RUNNER_NAMESPACE, "get", "secret", "bot-images-pull", "-o", "json"
  ]);
  const parentServiceAccount = kubectlJson([
    "-n", namespace, "get", "serviceaccount", "bot-orchestrator", "-o", "json"
  ]);
  const legacyRole = kubectlJson([
    "-n", namespace, "get", "role", "bot-orchestrator-runner-pods", "-o", "json"
  ]);
  const legacyBinding = kubectlJson([
    "-n", namespace, "get", "rolebinding", "bot-orchestrator-runner-pods", "-o", "json"
  ]);
  const runnerServiceAccount = kubectlJson([
    "-n", RUNNER_NAMESPACE, "get", "serviceaccount", "bot-runner", "-o", "json"
  ]);
  const guardServiceAccount = kubectlJson([
    "-n", RUNNER_NAMESPACE, "get", "serviceaccount", "bot-runner-guard", "-o", "json"
  ]);
  const runnerQuota = kubectlJson([
    "-n", RUNNER_NAMESPACE, "get", "resourcequota", "bot-runner-capacity", "-o", "json"
  ]);
  const guardQuota = kubectlJson([
    "-n", RUNNER_NAMESPACE, "get", "resourcequota", "bot-runner-guard-capacity", "-o", "json"
  ]);
  const roles = kubectlJson(["-n", RUNNER_NAMESPACE, "get", "roles", "-o", "json"]);
  const roleBindings = kubectlJson([
    "-n", RUNNER_NAMESPACE, "get", "rolebindings", "-o", "json"
  ]);
  const networkPolicies = kubectlJson([
    "-n", RUNNER_NAMESPACE, "get", "networkpolicies", "-o", "json"
  ]);
  const policy = kubectlJson([
    "get", "validatingadmissionpolicy", ADMISSION_POLICY_NAME, "-o", "json"
  ]);
  const binding = kubectlJson([
    "get", "validatingadmissionpolicybinding", ADMISSION_POLICY_NAME, "-o", "json"
  ]);
  const parentFencePolicy = kubectlJson([
    "get", "validatingadmissionpolicy", PARENT_FENCE_POLICY_NAME, "-o", "json"
  ]);
  const parentFenceBinding = kubectlJson([
    "get", "validatingadmissionpolicybinding", PARENT_FENCE_POLICY_NAME, "-o", "json"
  ]);
  const runnerProtocolPolicy = kubectlJson([
    "get", "validatingadmissionpolicy", RUNNER_PROTOCOL_POLICY_NAME, "-o", "json"
  ]);
  const runnerProtocolBinding = kubectlJson([
    "get", "validatingadmissionpolicybinding", RUNNER_PROTOCOL_POLICY_NAME, "-o", "json"
  ]);
  const cutoverJournalPolicy = kubectlJson([
    "get", "validatingadmissionpolicy", CUTOVER_JOURNAL_POLICY_NAME, "-o", "json"
  ]);
  const cutoverJournalBinding = kubectlJson([
    "get", "validatingadmissionpolicybinding", CUTOVER_JOURNAL_POLICY_NAME, "-o", "json"
  ]);
  if (
    ![roles, roleBindings, networkPolicies].every(value => Array.isArray(value?.items))
  ) {
    throw new Error("runner_control_plane_list_invalid");
  }
  return [
    runnerNamespace,
    runnerSecret,
    parentServiceAccount,
    legacyRole,
    legacyBinding,
    runnerServiceAccount,
    guardServiceAccount,
    runnerQuota,
    guardQuota,
    ...roles.items,
    ...roleBindings.items,
    ...networkPolicies.items,
    policy,
    binding,
    runnerProtocolPolicy,
    runnerProtocolBinding,
    cutoverJournalPolicy,
    cutoverJournalBinding,
    parentFencePolicy,
    parentFenceBinding
  ];
}

module.exports = {
  collectLiveRunnerControlPlane,
  identity,
  operationalDriftErrors,
  serverProjection,
  verifyLiveRunnerControlPlane
};
