const { execFileSync, spawnSync } = require("node:child_process");
const path = require("node:path");
const utils = require("../utils");
const {
  collectLiveRunnerControlPlane,
  verifyLiveRunnerControlPlane
} = require("./live-runner-control-plane");
const { readActivationPlan } = require("./runner-activation");
const {
  effectiveRbacReviewSpecs,
  selfSubjectRulesReviewRequest,
  verifyEffectiveRbacReviews
} = require("./effective-rbac");

const kubectlTimeoutMs = 30_000;

function requiredEnvironmentPath(name) {
  const value = process.env[name];
  if (typeof value !== "string" || !value || value !== value.trim()) {
    throw new Error("required_path_missing");
  }
  return path.resolve(value);
}

function main() {
  const inputPath = requiredEnvironmentPath("HCCE_INPUT_VALUES_PATH");
  const manifestPath = requiredEnvironmentPath("HCCE_MANIFEST_PATH");
  const context = process.env.KUBECTL_CONTEXT;
  if (typeof context !== "string" || !context || context !== context.trim()) {
    throw new Error("kubectl_context_invalid");
  }
  const currentContext = execFileSync("kubectl", ["config", "current-context"], {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
    timeout: kubectlTimeoutMs
  }).trim();
  if (currentContext !== context) throw new Error("kubectl_context_mismatch");

  const config = utils.readConfig(inputPath);
  const plan = readActivationPlan(manifestPath);
  const namespace = config.Namespace;
  const manifestNamespace = plan.resources.find(resource =>
    resource?.apiVersion === "v1" &&
    resource?.kind === "Namespace" &&
    resource?.metadata?.name !== "hcce-bot-runners"
  )?.metadata?.name;
  if (typeof namespace !== "string" || namespace !== manifestNamespace) {
    throw new Error("manifest_namespace_mismatch");
  }
  const kubectlJson = args => JSON.parse(execFileSync(
    "kubectl",
    ["--context", context, ...args],
    {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
      timeout: kubectlTimeoutMs
    }
  ));
  const liveResources = collectLiveRunnerControlPlane(kubectlJson, namespace);
  const errors = verifyLiveRunnerControlPlane(liveResources, plan.resources, namespace);
  const runnerRole = plan.resources.find(resource =>
    resource?.kind === "Role" &&
    resource?.metadata?.namespace === "hcce-bot-runners" &&
    resource?.metadata?.name === "bot-orchestrator-runner-pods"
  );
  const runnerAuthorityEnabled = Array.isArray(runnerRole?.rules) && runnerRole.rules.length === 1;
  const reviews = new Map();
  for (const spec of effectiveRbacReviewSpecs(namespace, runnerAuthorityEnabled)) {
    const result = spawnSync(
      "kubectl",
      [
        "--context", context,
        "create",
        "--raw", "/apis/authorization.k8s.io/v1/selfsubjectrulesreviews",
        "-f", "-",
        `--as=${spec.username}`
      ],
      {
        input: JSON.stringify(selfSubjectRulesReviewRequest(spec.namespace)),
        encoding: "utf8",
        stdio: ["pipe", "pipe", "pipe"],
        timeout: kubectlTimeoutMs
      }
    );
    if (result.status !== 0) throw new Error("effective_rbac_review_failed");
    reviews.set(spec.id, JSON.parse(result.stdout));
  }
  errors.push(...verifyEffectiveRbacReviews(reviews, namespace, runnerAuthorityEnabled));
  if (errors.length > 0) throw new Error("runner_live_control_plane_drift");
  process.stdout.write("runner_live_control_plane_verified\n");
}

try {
  main();
} catch (_error) {
  process.stderr.write("runner_live_control_plane_verification_failed\n");
  process.exitCode = 1;
}
