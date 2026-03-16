#!/usr/bin/env bash
set -euo pipefail

# TODO: need a better one
PUB_IP_CURL="https://ipinfo.io/ip"

# NOTE: In our k8s manifests, the perms key comes from the `PERMS_KEY` secret and is stored
# with literal `\n` escapes (single-line string) so it can be embedded safely in TOML.
# Dialog needs a real PEM file, so we unescape it on startup.
mkdir -p /app/certs
if [[ -n "${perms_key:-}" ]]; then
  # `PERMS_KEY` is stored with escaped newlines in k8s secret stringData.
  # Depending on the generation path, it may be double-escaped (`\\n`).
  # Convert both `\\n` and `\n` sequences to real newlines.
  node - <<'NODE'
const fs = require("fs");
const key = process.env.perms_key || "";
let pem = key.replace(/\\\\n/g, "\n").replace(/\\n/g, "\n");
if (pem && !pem.endsWith("\n")) pem += "\n";
fs.writeFileSync("/app/certs/perms.pub.pem", pem, "utf8");
NODE
fi

MEDIASOUP_ANNOUNCED_IP="$(curl -fsSL "${PUB_IP_CURL}" || true)"
export MEDIASOUP_ANNOUNCED_IP
echo "MEDIASOUP_ANNOUNCED_IP: ${MEDIASOUP_ANNOUNCED_IP:-<empty>}"
export INTERACTIVE="nope"

exec env DEBUG='*INFO* *WARN* *ERROR*' node index.js
