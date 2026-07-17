const path = require("path");
const fs = require("fs");
const crypto = require("crypto");
const YAML = require("yaml");

const PLACEHOLDER = /\$([A-Za-z_][A-Za-z0-9_]*)/g;

function renderYamlTemplate(template, config) {
  const documents = YAML.parseAllDocuments(template);
  const renderedNodes = new WeakSet();
  for (const [index, document] of documents.entries()) {
    if (document.errors.length > 0) {
      throw new Error(`Invalid YAML template document ${index + 1}: ${document.errors[0].message}`);
    }

    YAML.visit(document, {
      Scalar(_key, node) {
        if (renderedNodes.has(node)) return YAML.visit.SKIP;
        if (typeof node.value !== "string" || !node.value.includes("$")) return undefined;

        const original = node.value;
        const exact = original.match(/^\$([A-Za-z_][A-Za-z0-9_]*)$/);
        if (exact) {
          const name = exact[1];
          if (!Object.prototype.hasOwnProperty.call(config, name)) {
            throw new Error(`Missing YAML template value: ${name}`);
          }
          const value = config[name];
          if (value !== null && typeof value === "object") {
            throw new Error(`YAML template value ${name} must be a scalar`);
          }
          const replacement = document.createNode(value);
          renderedNodes.add(replacement);
          return replacement;
        }

        PLACEHOLDER.lastIndex = 0;
        const rendered = original.replace(PLACEHOLDER, (_match, name) => {
          if (!Object.prototype.hasOwnProperty.call(config, name)) {
            throw new Error(`Missing YAML template value: ${name}`);
          }
          const value = config[name];
          if (value !== null && typeof value === "object") {
            throw new Error(`Embedded YAML template value ${name} must be a scalar`);
          }
          return String(value ?? "");
        });
        const replacement = document.createNode(rendered);
        renderedNodes.add(replacement);
        return replacement;
      }
    });
  }

  return documents
    .map(document => YAML.stringify(document, { lineWidth: 0, directives: false }))
    .join("---\n");
}

function fsyncDirectory(directory) {
  let fd;
  try {
    fd = fs.openSync(directory, fs.constants.O_RDONLY);
    fs.fsyncSync(fd);
  } finally {
    if (fd !== undefined) fs.closeSync(fd);
  }
}

function writeAtomicPrivateFile(outputPath, content) {
  const requestedDirectory = path.dirname(outputPath);
  const directory = fs.realpathSync(requestedDirectory);
  const target = path.join(directory, path.basename(outputPath));
  let temporaryPath;
  let fd;

  try {
    try {
      const existing = fs.lstatSync(target);
      if (existing.isSymbolicLink()) {
        throw new Error(`Refusing to replace symbolic link: ${target}`);
      }
      if (!existing.isFile()) {
        throw new Error(`Refusing to replace non-file output: ${target}`);
      }
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }

    for (let attempt = 0; attempt < 10; attempt += 1) {
      const suffix = crypto.randomBytes(12).toString("hex");
      temporaryPath = path.join(directory, `.${path.basename(target)}.${process.pid}.${suffix}.tmp`);
      try {
        fd = fs.openSync(
          temporaryPath,
          fs.constants.O_CREAT |
            fs.constants.O_EXCL |
            fs.constants.O_WRONLY |
            (fs.constants.O_NOFOLLOW || 0),
          0o600
        );
        break;
      } catch (error) {
        if (error.code !== "EEXIST" || attempt === 9) throw error;
      }
    }

    if (fd === undefined) throw new Error("Unable to create private temporary output file");
    fs.writeFileSync(fd, content, "utf8");
    fs.fchmodSync(fd, 0o600);
    fs.fsyncSync(fd);
    fs.closeSync(fd);
    fd = undefined;

    try {
      const existing = fs.lstatSync(target);
      if (existing.isSymbolicLink()) {
        throw new Error(`Refusing to replace symbolic link: ${target}`);
      }
      if (!existing.isFile()) {
        throw new Error(`Refusing to replace non-file output: ${target}`);
      }
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }

    fs.renameSync(temporaryPath, target);
    temporaryPath = undefined;
    fs.chmodSync(target, 0o600);
    fsyncDirectory(directory);
  } finally {
    if (fd !== undefined) fs.closeSync(fd);
    if (temporaryPath) {
      try {
        fs.unlinkSync(temporaryPath);
      } catch (error) {
        if (error.code !== "ENOENT") throw error;
      }
    }
  }
}

module.exports = {
  // Function to read and parse YAML config file
  readConfig: function readConfig(configPath = path.join(process.cwd(), "input-values.yaml")) {
    try {
      const fileContents = fs.readFileSync(configPath, "utf8");
      return YAML.parse(fileContents);
    } catch (error) {
      console.error("Error reading config file:", error);
      throw error;
    }
  },

  // Function to read template files
  readTemplate: function readTemplate(folder, file) {
    try {
      const templatePath = path.join(process.cwd(), folder, file);
      const fileContents = fs.readFileSync(templatePath, "utf8");
      return fileContents;
    } catch (error) {
      console.error("Error reading template file:", error);
      throw error;
    }
  },

  // Function to write the YAML output file
  writeOutputFile: function writeOutputFile(content, folder, filepath, explicitOutputPath = null) {
    try {
      const outputPath = explicitOutputPath || path.join(process.cwd(), folder, filepath);
      writeAtomicPrivateFile(outputPath, content);
      console.log(`${filepath} file generated successfully.`);
    } catch (error) {
      console.error("Error writing output file:", error);
      throw error;
    }
  },

  // Function to replace placeholders in template with config values
  replacePlaceholders: function replacePlaceholders(template, config) {
    return renderYamlTemplate(template, config);
  },

  renderYamlTemplate,
  writeAtomicPrivateFile
}
