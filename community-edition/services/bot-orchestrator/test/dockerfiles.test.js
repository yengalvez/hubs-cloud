const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { test } = require("node:test");

const serviceRoot = path.resolve(__dirname, "..");

function dockerfile(name) {
  return fs.readFileSync(path.join(serviceRoot, name), "utf8");
}

function packageManifest(name) {
  return JSON.parse(fs.readFileSync(path.join(serviceRoot, name), "utf8"));
}

test("parent image contains only the control plane sources and no Chromium runtime", () => {
  const source = dockerfile("Dockerfile");

  assert.match(source, /COPY app\.js \/app\/app\.js/);
  assert.match(source, /COPY kubernetes-runner-manager\.js \/app\/kubernetes-runner-manager\.js/);
  assert.match(source, /COPY runner-generation-token\.js \/app\/runner-generation-token\.js/);
  assert.match(source, /FROM node:20-bookworm-slim AS parent-dependencies/);
  assert.match(source, /COPY package\.parent\.json \/app\/package\.json/);
  assert.match(source, /npm rm --no-save .*puppeteer-core/);
  assert.doesNotMatch(source, /chromium|run-bot\.js|run-ghost-runner\.js/i);
  assert.deepEqual(packageManifest("package.parent.json").dependencies, {
    express: "^4.21.2"
  });
});

test("runner image has a distinct user and only the ghost data-plane sources", () => {
  const source = dockerfile("Dockerfile.runner");

  assert.match(source, /USER 10001:10001/);
  assert.match(source, /COPY run-ghost-runner\.js \/app\/run-ghost-runner\.js/);
  assert.match(source, /COPY runner-control-client\.js \/app\/runner-control-client\.js/);
  assert.match(source, /FROM node:20-bookworm-slim AS runner-dependencies/);
  assert.match(source, /COPY package\.runner\.json \/app\/package\.json/);
  assert.match(source, /npm rm --no-save express puppeteer-core query-string/);
  assert.doesNotMatch(source, /chromium|COPY app\.js|COPY run-bot\.js/i);
  assert.deepEqual(Object.keys(packageManifest("package.runner.json").dependencies).sort(), [
    "docopt",
    "gl-matrix",
    "phoenix",
    "three",
    "three-pathfinding",
    "ws"
  ]);
});

test("Docker build context is deny-by-default with only audited image inputs", () => {
  const entries = dockerfile(".dockerignore").trim().split("\n");

  assert.deepEqual(entries, [
    "*",
    "!package.json",
    "!package-lock.json",
    "!package.parent.json",
    "!package.runner.json",
    "!Dockerfile",
    "!Dockerfile.runner",
    "!app.js",
    "!kubernetes-runner-manager.js",
    "!runner-generation-token.js",
    "!run-ghost-runner.js",
    "!runner-control-client.js"
  ]);
});
