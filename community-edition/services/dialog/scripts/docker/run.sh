#!/usr/bin/env bash
set -euo pipefail

# TODO: need a better one
PUB_IP_CURL="https://ipinfo.io/ip"

# NOTE: In our k8s manifests, the perms key comes from the `PERMS_KEY` secret and is stored
# with literal `\n` escapes (single-line string) so it can be embedded safely in TOML.
# Dialog needs a real PEM file, so we unescape it on startup.
mkdir -p /app/certs
if [[ -n "${perms_key:-}" ]]; then
  # printf %b interprets backslash escapes (e.g. \n) into real newlines.
  printf '%b' "${perms_key}" > /app/certs/perms.pub.pem
fi

MEDIASOUP_ANNOUNCED_IP="$(curl -fsSL "${PUB_IP_CURL}" || true)"
export MEDIASOUP_ANNOUNCED_IP
echo "MEDIASOUP_ANNOUNCED_IP: ${MEDIASOUP_ANNOUNCED_IP:-<empty>}"
export INTERACTIVE="nope"

exec env DEBUG='*INFO* *WARN* *ERROR*' node index.js
