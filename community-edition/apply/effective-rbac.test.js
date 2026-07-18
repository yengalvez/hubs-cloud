const assert = require("node:assert/strict");
const test = require("node:test");

const {
  BASE_NON_RESOURCE_URLS,
  BASE_RESOURCE_TUPLES,
  RUNNER_POD_TUPLES,
  effectiveRbacReviewSpecs,
  verifyEffectiveRbacReviews,
  verifySelfSubjectRulesReview
} = require("./effective-rbac");

function exactReview({ runnerPodAuthority = false } = {}) {
  const tuples = [...BASE_RESOURCE_TUPLES, ...(runnerPodAuthority ? RUNNER_POD_TUPLES : [])];
  const byGroupResource = new Map();
  for (const [apiGroup, resource, verb] of tuples) {
    const key = JSON.stringify([apiGroup, resource]);
    const value = byGroupResource.get(key) || { apiGroup, resource, verbs: [] };
    value.verbs.push(verb);
    byGroupResource.set(key, value);
  }
  return {
    apiVersion: "authorization.k8s.io/v1",
    kind: "SelfSubjectRulesReview",
    status: {
      resourceRules: [...byGroupResource.values()].map(value => ({
        apiGroups: [value.apiGroup],
        resources: [value.resource],
        verbs: value.verbs,
        resourceNames: []
      })),
      nonResourceRules: [{
        nonResourceURLs: [...BASE_NON_RESOURCE_URLS],
        verbs: ["get"]
      }],
      incomplete: false
    }
  };
}

test("effective RBAC accepts only the exact discovery/self-review baseline", () => {
  assert.deepEqual(verifySelfSubjectRulesReview(exactReview()), []);
  assert.deepEqual(verifySelfSubjectRulesReview(
    exactReview({ runnerPodAuthority: true }),
    { runnerPodAuthority: true }
  ), []);
});

test("effective RBAC rejects additive bindings, wildcards, resourceNames, and incomplete reviews", () => {
  const additive = exactReview();
  additive.status.resourceRules.push({
    apiGroups: [""],
    resources: ["secrets"],
    verbs: ["get"],
    resourceNames: []
  });
  assert.notDeepEqual(verifySelfSubjectRulesReview(additive), []);

  const wildcard = exactReview();
  wildcard.status.resourceRules[0].verbs.push("*");
  assert.notDeepEqual(verifySelfSubjectRulesReview(wildcard), []);

  const named = exactReview();
  named.status.resourceRules[0].resourceNames = ["one-object"];
  assert.notDeepEqual(verifySelfSubjectRulesReview(named), []);

  const incomplete = exactReview();
  incomplete.status.incomplete = true;
  assert.notDeepEqual(verifySelfSubjectRulesReview(incomplete), []);
});

test("four-identity matrix detects an additive runner or parent ClusterRoleBinding", () => {
  const parentNamespace = "hcce";
  const reviews = new Map(
    effectiveRbacReviewSpecs(parentNamespace, true).map(spec => [
      spec.id,
      exactReview({ runnerPodAuthority: spec.runnerPodAuthority })
    ])
  );
  assert.deepEqual(verifyEffectiveRbacReviews(reviews, parentNamespace, true), []);

  const runner = reviews.get("runner-in-runner");
  runner.status.resourceRules.push({
    apiGroups: ["authentication.k8s.io"],
    resources: ["serviceaccounts/token"],
    verbs: ["create"],
    resourceNames: []
  });
  assert.notDeepEqual(verifyEffectiveRbacReviews(reviews, parentNamespace, true), []);
});
