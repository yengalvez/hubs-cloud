const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const YAML = require("yaml");

const { verifyLiveRunnerControlPlane } = require("./live-runner-control-plane");

const NAMESPACE = "$Namespace";
const POLICY_NAME = "recovery-operation-pod-fence.yenhubs.org";

function generatedResources() {
  return YAML.parseAllDocuments(fs.readFileSync(
    path.resolve(__dirname, "../generate_script/hcce.yam"),
    "utf8"
  )).map(document => document.toJS()).filter(Boolean);
}

function liveResourcesFromGenerated(resources) {
  return structuredClone(resources).map((resource, index) => {
    resource.metadata = {
      ...resource.metadata,
      uid: `uid-${index}`,
      resourceVersion: String(index + 1)
    };
    if (resource.kind === "Namespace") {
      resource.metadata.labels = {
        ...resource.metadata.labels,
        "kubernetes.io/metadata.name": resource.metadata.name
      };
      resource.spec = { finalizers: ["kubernetes"] };
      resource.status = { phase: "Active" };
    }
    if (resource.kind === "ValidatingAdmissionPolicy") {
      resource.metadata.generation = 1;
      resource.status = { observedGeneration: 1, typeChecking: {} };
    }
    return resource;
  });
}

test("live runner control-plane verification requires the exact fifth parameter-free pair", () => {
  const generated = generatedResources();
  const live = liveResourcesFromGenerated(generated);
  assert.deepEqual(verifyLiveRunnerControlPlane(live, generated, NAMESPACE), []);

  const missing = live.filter(resource => !(
    resource.kind === "ValidatingAdmissionPolicyBinding" &&
    resource.metadata?.name === POLICY_NAME
  ));
  assert.match(
    verifyLiveRunnerControlPlane(missing, generated, NAMESPACE).join("\n"),
    /recovery-operation-pod-fence|recovery_operation_fence/
  );

  const activeBypass = structuredClone(live);
  activeBypass.find(resource =>
    resource.kind === "ValidatingAdmissionPolicyBinding" &&
    resource.metadata?.name === POLICY_NAME
  ).spec.matchResources.namespaceSelector = {
    matchExpressions: [{
      key: "kubernetes.io/metadata.name",
      operator: "In",
      values: [NAMESPACE, "hcce-bot-runners"]
    }]
  };
  assert.match(
    verifyLiveRunnerControlPlane(activeBypass, generated, NAMESPACE).join("\n"),
    /recovery-operation-pod-fence|recovery_operation_fence/
  );

  const warning = structuredClone(live);
  warning.find(resource =>
    resource.kind === "ValidatingAdmissionPolicy" &&
    resource.metadata?.name === POLICY_NAME
  ).status.typeChecking.expressionWarnings = [{ fieldRef: "spec.validations[0]" }];
  assert.match(
    verifyLiveRunnerControlPlane(warning, generated, NAMESPACE).join("\n"),
    /recovery_operation_fence/
  );

  const parameterized = structuredClone(live);
  parameterized.find(resource =>
    resource.kind === "ValidatingAdmissionPolicy" &&
    resource.metadata?.name === POLICY_NAME
  ).spec.paramKind = { apiVersion: "v1", kind: "ConfigMap" };
  assert.match(
    verifyLiveRunnerControlPlane(parameterized, generated, NAMESPACE).join("\n"),
    /recovery-operation-pod-fence|recovery_operation_fence/
  );
});
