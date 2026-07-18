const crypto = require("node:crypto");
const fs = require("node:fs");
const path = require("node:path");
const YAML = require("yaml");

function requiredEnvironment(name) {
  const value = String(process.env[name] || "");
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function writePrivateAtomic(targetPath, content) {
  const directory = path.dirname(targetPath);
  const temporaryPath = path.join(
    directory,
    `.${path.basename(targetPath)}.${process.pid}.${crypto.randomBytes(8).toString("hex")}.tmp`
  );
  let descriptor;
  try {
    descriptor = fs.openSync(temporaryPath, "wx", 0o600);
    fs.writeFileSync(descriptor, content, "utf8");
    fs.fsyncSync(descriptor);
    fs.closeSync(descriptor);
    descriptor = undefined;
    fs.renameSync(temporaryPath, targetPath);
    fs.chmodSync(targetPath, 0o600);
  } catch (error) {
    if (descriptor !== undefined) fs.closeSync(descriptor);
    fs.rmSync(temporaryPath, { force: true });
    throw error;
  }
}

function main() {
  const inputPath = path.resolve(requiredEnvironment("HCCE_INPUT_VALUES_PATH"));
  const username = requiredEnvironment("GHCR_USERNAME");
  const token = requiredEnvironment("GHCR_TOKEN");
  const registry = String(process.env.BOT_IMAGE_REGISTRY || "ghcr.io").trim().toLowerCase();
  delete process.env.GHCR_TOKEN;

  if (!/^[a-z0-9][a-z0-9.-]*(?::[1-9][0-9]{0,4})?$/.test(registry)) {
    throw new Error("BOT_IMAGE_REGISTRY must be a registry host without a URL path");
  }
  if (!username.trim() || username.includes(":") || /[\u0000-\u001f\u007f]/u.test(username)) {
    throw new Error("GHCR_USERNAME is invalid");
  }
  if (!token.trim() || /[\u0000-\u001f\u007f]/u.test(token)) {
    throw new Error("GHCR_TOKEN is invalid");
  }

  const stat = fs.lstatSync(inputPath);
  if (!stat.isFile() || stat.isSymbolicLink()) {
    throw new Error("HCCE_INPUT_VALUES_PATH must be an existing regular file, not a symbolic link");
  }
  const document = YAML.parseDocument(fs.readFileSync(inputPath, "utf8"));
  if (document.errors.length > 0 || !YAML.isMap(document.contents)) {
    throw new Error("HCCE_INPUT_VALUES_PATH must contain one valid YAML mapping");
  }

  const dockerConfig = {
    auths: {
      [registry]: {
        auth: Buffer.from(`${username}:${token}`, "utf8").toString("base64")
      }
    }
  };
  document.set(
    "BOT_IMAGE_PULL_CONFIG_JSON_BASE64",
    Buffer.from(JSON.stringify(dockerConfig), "utf8").toString("base64")
  );
  writePrivateAtomic(inputPath, document.toString({ lineWidth: 0 }));
  console.log(`Updated private bot image-pull configuration in ${inputPath}; credential value was not displayed.`);
}

try {
  main();
} catch (error) {
  console.error(`Unable to update bot image-pull configuration: ${error.message}`);
  process.exitCode = 1;
}
