const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const test = require("node:test");
const YAML = require("yaml");

const { renderYamlTemplate, writeAtomicPrivateFile } = require("../../utils");

test("YAML AST renderer preserves whole-scalar types and safely quotes embedded values", () => {
  const rendered = renderYamlTemplate(
    [
      "enabled: $ENABLED",
      "count: '$COUNT'",
      "secret: $SECRET",
      "url: https://$HOST/path",
      "literal: $DOLLARS"
    ].join("\n"),
    {
      ENABLED: false,
      COUNT: 7,
      SECRET: "quote: \\\" backslash: \\\\ newline:\ncontrol:\u0001",
      HOST: "example.test/\\\"\nmalicious: true #",
      DOLLARS: "$NOT_A_TEMPLATE"
    }
  );
  const value = YAML.parse(rendered);

  assert.equal(value.enabled, false);
  assert.equal(value.count, 7);
  assert.equal(value.secret, "quote: \\\" backslash: \\\\ newline:\ncontrol:\u0001");
  assert.equal(value.url, "https://example.test/\\\"\nmalicious: true #/path");
  assert.equal(value.literal, "$NOT_A_TEMPLATE");
  assert.equal(Object.prototype.hasOwnProperty.call(value, "malicious"), false);
});

test("YAML AST renderer replaces source placeholders once and rejects missing values", () => {
  const rendered = renderYamlTemplate("value: $FIRST\n", { FIRST: "$SECOND", SECOND: "expanded" });
  assert.equal(YAML.parse(rendered).value, "$SECOND");
  assert.throws(() => renderYamlTemplate("value: $MISSING\n", {}), /Missing YAML template value/);
});

test("atomic private output is mode 0600 for new and replaced files under a permissive umask", () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "yenhubs-private-output-"));
  const output = path.join(directory, "manifest.yaml");
  const previousUmask = process.umask(0);
  try {
    writeAtomicPrivateFile(output, "first");
    assert.equal(fs.statSync(output).mode & 0o777, 0o600);
    fs.chmodSync(output, 0o666);
    writeAtomicPrivateFile(output, "second");
    assert.equal(fs.readFileSync(output, "utf8"), "second");
    assert.equal(fs.statSync(output).mode & 0o777, 0o600);
    assert.deepEqual(fs.readdirSync(directory), ["manifest.yaml"]);
  } finally {
    process.umask(previousUmask);
    fs.rmSync(directory, { recursive: true });
  }
});

test("atomic private output refuses symbolic-link targets and removes temporary files", () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "yenhubs-private-output-"));
  const victim = path.join(directory, "victim");
  const output = path.join(directory, "manifest.yaml");
  try {
    fs.writeFileSync(victim, "unchanged");
    fs.symlinkSync(victim, output);
    assert.throws(() => writeAtomicPrivateFile(output, "forbidden"), /symbolic link/);
    assert.equal(fs.readFileSync(victim, "utf8"), "unchanged");
    assert.deepEqual(fs.readdirSync(directory).sort(), ["manifest.yaml", "victim"]);
  } finally {
    fs.rmSync(directory, { recursive: true });
  }
});
