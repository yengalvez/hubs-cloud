const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { spawnSync } = require("node:child_process");
const { test } = require("node:test");
const YAML = require("yaml");

const scriptPath = path.resolve(__dirname, "../set-bot-image-pull-config.js");

test("writes a usable private registry credential atomically without displaying it", () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "yenhubs-pull-config-"));
  const inputPath = path.join(directory, "input-values.local.yaml");
  const token = "ci-private-token-value";
  fs.writeFileSync(inputPath, "HUB_DOMAIN: hubs.invalid\nKEEP_ME: unchanged\n", { mode: 0o644 });

  try {
    const result = spawnSync(process.execPath, [scriptPath], {
      encoding: "utf8",
      env: {
        ...process.env,
        HCCE_INPUT_VALUES_PATH: inputPath,
        GHCR_USERNAME: "ci-user",
        GHCR_TOKEN: token,
        BOT_IMAGE_REGISTRY: "registry.invalid"
      }
    });
    assert.equal(result.status, 0, result.stderr);
    assert.equal(result.stdout.includes(token), false);
    assert.equal(result.stderr.includes(token), false);
    assert.equal(fs.statSync(inputPath).mode & 0o777, 0o600);

    const values = YAML.parse(fs.readFileSync(inputPath, "utf8"));
    assert.equal(values.KEEP_ME, "unchanged");
    const config = JSON.parse(Buffer.from(values.BOT_IMAGE_PULL_CONFIG_JSON_BASE64, "base64"));
    assert.equal(
      Buffer.from(config.auths["registry.invalid"].auth, "base64").toString("utf8"),
      `ci-user:${token}`
    );
  } finally {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});
