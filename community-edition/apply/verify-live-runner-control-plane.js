const { execFileSync, spawnSync } = require("node:child_process");
const path = require("node:path");
const {
  collectLiveRunnerControlPlane,
  verifyLiveRunnerControlPlane
} = require("./live-runner-control-plane");
const {
  CUTOVER_JOURNAL_NAME,
  liveObjectIsUnencumbered,
  parseStructurallyExactCutoverJournalConfigMap
} = require("./cutover-journal");
const { activeCutoverNamespace } = require("./process-local-cutover");
const { verifyManifestAgainstInputValues } = require("./manifest-input-contract");
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

function verifyDurableCutoverJournalLiveEvidence({
  namespace,
  liveNamespace,
  journalConfigMap,
  liveParentDeployment
}) {
  if (!activeCutoverNamespace(liveNamespace, namespace)) {
    return ["parent Namespace must be exact and Active for durable runner cutover"];
  }
  try {
    const journal = parseStructurallyExactCutoverJournalConfigMap(journalConfigMap, {
      namespace,
      namespaceUid: liveNamespace.metadata.uid,
      allowFutureIssuedAt: true
    });
    if (
      liveParentDeployment?.apiVersion !== "apps/v1" ||
      liveParentDeployment?.kind !== "Deployment" ||
      liveParentDeployment?.metadata?.namespace !== namespace ||
      liveParentDeployment?.metadata?.name !== "bot-orchestrator" ||
      !liveObjectIsUnencumbered(liveParentDeployment) ||
      (journal.mode === "pristine-cutover" &&
        liveParentDeployment.metadata.uid !== journal.baselineDeployment.uid)
    ) {
      return ["parent Deployment must preserve the durable first-cutover identity"];
    }
  } catch (_error) {
    return ["first-cutover journal must be canonical, protected and bound to the live Namespace UID"];
  }
  return [];
}

function main() {
  const inputPath = requiredEnvironmentPath("HCCE_INPUT_VALUES_PATH");
  const manifestPath = requiredEnvironmentPath("HCCE_MANIFEST_PATH");
  const { config, plan } = verifyManifestAgainstInputValues(inputPath, manifestPath);
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
  const liveNamespace = kubectlJson(["get", "namespace", namespace, "-o", "json"]);
  const journalConfigMap = kubectlJson([
    "-n", namespace, "get", "configmap", CUTOVER_JOURNAL_NAME, "-o", "json"
  ]);
  const liveParentDeployment = kubectlJson([
    "-n", namespace, "get", "deployment", "bot-orchestrator", "-o", "json"
  ]);
  errors.push(...verifyDurableCutoverJournalLiveEvidence({
    namespace,
    liveNamespace,
    journalConfigMap,
    liveParentDeployment
  }));
  const runnerAuthorityEnabled = true;
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

if (require.main === module) {
  try {
    main();
  } catch (_error) {
    process.stderr.write("runner_live_control_plane_verification_failed\n");
    process.exitCode = 1;
  }
}

module.exports = { main, verifyDurableCutoverJournalLiveEvidence };
