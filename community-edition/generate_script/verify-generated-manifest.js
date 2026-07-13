const fs = require("fs");
const path = require("path");
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
  }

  const haproxyRole = findResource(resources, "ClusterRole", "haproxy-cr");
  if (!hasRule(haproxyRole, "apiextensions.k8s.io", "customresourcedefinitions", ["get", "list", "watch"])) {
    fail("ClusterRole/haproxy-cr is missing CRD read permissions");
  }
  if (!hasRule(haproxyRole, "gateway.networking.k8s.io", "gateways", ["get", "list", "watch"])) {
    fail("ClusterRole/haproxy-cr is missing Gateway API read permissions");
  }

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
