const { spawnSync } = require("node:child_process");

const context = process.env.KUBECTL_CONTEXT;
if (typeof context !== "string" || !context || context !== context.trim()) {
  throw new Error("KUBECTL_CONTEXT_must_be_one_exact_nonempty_context");
}
const result = spawnSync(
  "kubectl",
  ["--context", context, "get", "svc", "--field-selector", "spec.type=LoadBalancer", "-A", "-o", "json"],
  { stdio: ["pipe", "pipe", "inherit"] }
);
if (result.status !== 0) throw new Error(`load_balancer_inventory_failed:${result.status}`);
const output = JSON.parse(result.stdout);
if (output.items.length === 0) {
  console.warn("can't determine external IP address: no load balancers in cluster");
  process.exit(1);
}
for (const item of output.items) {
  const name = item.metadata.name;
  const namespace = item.metadata.namespace;
  const status = item.status;
  const addr = status?.loadBalancer?.ingress?.[0]?.ip || status?.loadBalancer?.ingress?.[0]?.hostname;
  if (addr) {
    console.log(`load balancer “${name}” in namespace “${namespace}” external address: ${addr}`);
  } else {
    console.log(`load balancer “${name}” in namespace “${namespace}” not running yet:`, status);
  }
}
