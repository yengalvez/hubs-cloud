const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

test("load-balancer inventory is context-pinned and independent of values files", () => {
  const source = fs.readFileSync(path.resolve(__dirname, "index.js"), "utf8");
  assert.doesNotMatch(source, /readConfig|input-values\.yaml/);
  assert.match(source, /process\.env\.KUBECTL_CONTEXT/);
  assert.match(source, /\["--context", context, "get", "svc"/);
  assert.match(source, /load_balancer_inventory_failed/);
});
