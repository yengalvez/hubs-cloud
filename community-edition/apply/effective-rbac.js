const RUNNER_NAMESPACE = "hcce-bot-runners";

const BASE_RESOURCE_TUPLES = Object.freeze([
  ["authorization.k8s.io", "selfsubjectaccessreviews", "create"],
  ["authorization.k8s.io", "selfsubjectrulesreviews", "create"],
  ["authentication.k8s.io", "selfsubjectreviews", "create"]
]);

const RUNNER_POD_TUPLES = Object.freeze(
  ["create", "delete", "get", "list", "patch"].map(verb => ["", "pods", verb])
);

const BASE_NON_RESOURCE_URLS = Object.freeze([
  "/.well-known/openid-configuration",
  "/.well-known/openid-configuration/",
  "/api",
  "/api/*",
  "/apis",
  "/apis/*",
  "/healthz",
  "/livez",
  "/openapi",
  "/openapi/*",
  "/openid/v1/jwks",
  "/openid/v1/jwks/",
  "/readyz",
  "/version",
  "/version/"
]);

function tupleKey(tuple) {
  return JSON.stringify(tuple);
}

function exactObjectKeys(value, expected) {
  return value &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    JSON.stringify(Object.keys(value).sort()) === JSON.stringify([...expected].sort());
}

function nonemptyStringArray(value) {
  return Array.isArray(value) && value.length > 0 &&
    value.every(item => typeof item === "string" && item.length > 0);
}

function apiGroupArray(value) {
  return Array.isArray(value) && value.length > 0 &&
    value.every(item => typeof item === "string");
}

function normalizeSelfSubjectRulesReview(review) {
  const errors = [];
  const resources = new Set();
  const nonResources = new Set();
  if (
    review?.apiVersion !== "authorization.k8s.io/v1" ||
    review?.kind !== "SelfSubjectRulesReview" ||
    review?.status?.incomplete !== false ||
    ![undefined, null, ""].includes(review?.status?.evaluationError) ||
    !Array.isArray(review?.status?.resourceRules) ||
    !Array.isArray(review?.status?.nonResourceRules)
  ) {
    errors.push("selfsubjectrulesreview_status_invalid");
    return { errors, resources, nonResources };
  }

  for (const rule of review.status.resourceRules) {
    if (
      !exactObjectKeys(rule, ["apiGroups", "resources", "verbs", "resourceNames"]) &&
      !exactObjectKeys(rule, ["apiGroups", "resources", "verbs"])
    ) {
      errors.push("resource_rule_shape_invalid");
      continue;
    }
    if (
      !apiGroupArray(rule.apiGroups) ||
      !nonemptyStringArray(rule.resources) ||
      !nonemptyStringArray(rule.verbs) ||
      (rule.resourceNames !== undefined &&
        (!Array.isArray(rule.resourceNames) || rule.resourceNames.length !== 0))
    ) {
      errors.push("resource_rule_values_invalid");
      continue;
    }
    for (const apiGroup of rule.apiGroups) {
      for (const resource of rule.resources) {
        for (const verb of rule.verbs) {
          if ([apiGroup, resource, verb].includes("*")) {
            errors.push("resource_rule_wildcard_forbidden");
          } else {
            resources.add(tupleKey([apiGroup, resource, verb]));
          }
        }
      }
    }
  }

  for (const rule of review.status.nonResourceRules) {
    if (
      !exactObjectKeys(rule, ["nonResourceURLs", "verbs"]) ||
      !nonemptyStringArray(rule.nonResourceURLs) ||
      !nonemptyStringArray(rule.verbs)
    ) {
      errors.push("nonresource_rule_invalid");
      continue;
    }
    for (const url of rule.nonResourceURLs) {
      for (const verb of rule.verbs) {
        if (verb === "*") errors.push("nonresource_verb_wildcard_forbidden");
        else nonResources.add(tupleKey([url, verb]));
      }
    }
  }
  return { errors, resources, nonResources };
}

function verifySelfSubjectRulesReview(review, { runnerPodAuthority = false } = {}) {
  const normalized = normalizeSelfSubjectRulesReview(review);
  const expectedResources = new Set(
    [...BASE_RESOURCE_TUPLES, ...(runnerPodAuthority ? RUNNER_POD_TUPLES : [])].map(tupleKey)
  );
  const expectedNonResources = new Set(
    BASE_NON_RESOURCE_URLS.map(url => tupleKey([url, "get"]))
  );
  if (
    normalized.resources.size !== expectedResources.size ||
    [...normalized.resources].some(value => !expectedResources.has(value)) ||
    [...expectedResources].some(value => !normalized.resources.has(value))
  ) {
    normalized.errors.push("resource_rules_not_exact");
  }
  if (
    normalized.nonResources.size !== expectedNonResources.size ||
    [...normalized.nonResources].some(value => !expectedNonResources.has(value)) ||
    [...expectedNonResources].some(value => !normalized.nonResources.has(value))
  ) {
    normalized.errors.push("nonresource_rules_not_exact");
  }
  return [...new Set(normalized.errors)];
}

function effectiveRbacReviewSpecs(parentNamespace, runnerAuthorityEnabled) {
  if (typeof parentNamespace !== "string" || !parentNamespace) {
    throw new Error("effective_rbac_parent_namespace_invalid");
  }
  const parentUsername = `system:serviceaccount:${parentNamespace}:bot-orchestrator`;
  const runnerUsername = `system:serviceaccount:${RUNNER_NAMESPACE}:bot-runner`;
  return [
    {
      id: "parent-in-parent",
      namespace: parentNamespace,
      username: parentUsername,
      runnerPodAuthority: false
    },
    {
      id: "parent-in-runner",
      namespace: RUNNER_NAMESPACE,
      username: parentUsername,
      runnerPodAuthority: runnerAuthorityEnabled === true
    },
    {
      id: "runner-in-parent",
      namespace: parentNamespace,
      username: runnerUsername,
      runnerPodAuthority: false
    },
    {
      id: "runner-in-runner",
      namespace: RUNNER_NAMESPACE,
      username: runnerUsername,
      runnerPodAuthority: false
    }
  ];
}

function selfSubjectRulesReviewRequest(namespace) {
  return {
    apiVersion: "authorization.k8s.io/v1",
    kind: "SelfSubjectRulesReview",
    spec: { namespace }
  };
}

function verifyEffectiveRbacReviews(reviews, parentNamespace, runnerAuthorityEnabled) {
  const errors = [];
  const specs = effectiveRbacReviewSpecs(parentNamespace, runnerAuthorityEnabled);
  for (const spec of specs) {
    const review = reviews instanceof Map ? reviews.get(spec.id) : reviews?.[spec.id];
    for (const error of verifySelfSubjectRulesReview(review, spec)) {
      errors.push(`${spec.id}:${error}`);
    }
  }
  return errors;
}

module.exports = {
  BASE_NON_RESOURCE_URLS,
  BASE_RESOURCE_TUPLES,
  RUNNER_NAMESPACE,
  RUNNER_POD_TUPLES,
  effectiveRbacReviewSpecs,
  normalizeSelfSubjectRulesReview,
  selfSubjectRulesReviewRequest,
  verifyEffectiveRbacReviews,
  verifySelfSubjectRulesReview
};
