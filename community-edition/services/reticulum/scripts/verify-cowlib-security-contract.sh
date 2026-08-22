#!/usr/bin/env bash

set -euo pipefail

SERVICE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MIX_FILE="$SERVICE_ROOT/mix.exs"
LOCK_FILE="$SERVICE_ROOT/mix.lock"
DEPENDENCY_ROOT="$SERVICE_ROOT/deps/cowlib"

BASE_VERSION="2.19.0"
BASE_COMMIT="d33876eddd3062f3f748e4879b0efe3990afdc73"
PATCH_COMMIT="89da27ee4c241f5d649ba7d9b7f2188918af6cea"
UPSTREAM_URL="https://github.com/ninenines/cowlib.git"
OSV_QUERY_URL="https://api.osv.dev/v1/query"
OSV_ADVISORY_URL="https://api.osv.dev/v1/vulns/EEF-CVE-2026-43971"

fail() {
  printf 'Cowlib security contract failed: %s\n' "$1" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "missing required command $1"
}

require_exact_line() {
  local file="$1" line="$2" label="$3"
  [[ "$(grep -Fxc -- "$line" "$file")" == 1 ]] || fail "$label is not exact"
}

osv_query() {
  local body="$1"
  curl --proto '=https' --tlsv1.2 -fsS \
    -H 'content-type: application/json' \
    --data "$body" \
    "$OSV_QUERY_URL"
}

normalized_osv_identities() {
  jq -er '
    if type != "object" then error("response is not an object") else . end
    | (.vulns // [])
    | if type != "array" then error("vulns is not an array") else . end
    | [ .[]
        | if (.id | type) != "string" then error("advisory id is not a string") else . end
        | [ .id ] + (.aliases // [])
        | .[]
        | if type != "string" then error("advisory alias is not a string") else . end
      ]
    | unique
    | join("\n")
  '
}

require_command curl
require_command git
require_command jq

[[ -f "$MIX_FILE" && ! -L "$MIX_FILE" ]] || fail 'mix.exs is not a regular file'
[[ -f "$LOCK_FILE" && ! -L "$LOCK_FILE" ]] || fail 'mix.lock is not a regular file'

expected_dependency=$(cat <<'DEPENDENCY'
      {:cowlib,
       git: "https://github.com/ninenines/cowlib.git",
       ref: "89da27ee4c241f5d649ba7d9b7f2188918af6cea",
       override: true},
DEPENDENCY
)
actual_dependency="$(sed -n '/^      {:cowlib,$/,/^       override: true},$/p' "$MIX_FILE")"
[[ "$actual_dependency" == "$expected_dependency" ]] || fail 'production dependency declaration is not exact'
[[ "$(grep -Fxc '      {:cowlib,' "$MIX_FILE")" == 1 ]] || fail 'production dependency is duplicated'
if grep -Fq 'ignore_advisories' "$MIX_FILE"; then
  fail 'production mix.exs must not hide advisories for a Git dependency'
fi

expected_lock_line="  \"cowlib\": {:git, \"$UPSTREAM_URL\", \"$PATCH_COMMIT\", [ref: \"$PATCH_COMMIT\"]},"
require_exact_line "$LOCK_FILE" "$expected_lock_line" 'production lock'
[[ "$(grep -c '^  "cowlib":' "$LOCK_FILE")" == 1 ]] || fail 'production lock has duplicate cowlib entries'

[[ -d "$DEPENDENCY_ROOT" ]] || fail 'fetched cowlib dependency is missing'
[[ "$(git -C "$DEPENDENCY_ROOT" rev-parse HEAD)" == "$PATCH_COMMIT" ]] || fail 'fetched cowlib commit differs'
git -C "$DEPENDENCY_ROOT" merge-base --is-ancestor "$BASE_COMMIT" "$PATCH_COMMIT" ||
  fail 'patch commit does not descend from the accepted Hex baseline'

expected_commits=$'3ec5c50ccb1b4670de7bbb1f160c145f79d97fed\n63abbff964fd1b6e6e9a3bbc7cbc86c5926088b5\n89da27ee4c241f5d649ba7d9b7f2188918af6cea'
actual_commits="$(git -C "$DEPENDENCY_ROOT" rev-list --reverse "$BASE_COMMIT..$PATCH_COMMIT")"
[[ "$actual_commits" == "$expected_commits" ]] || fail 'post-2.19.0 commit surface differs'

hex_response="$(osv_query "{\"package\":{\"purl\":\"pkg:hex/cowlib\"},\"version\":\"$BASE_VERSION\"}")" ||
  fail 'OSV Hex query failed'
hex_identities="$(printf '%s' "$hex_response" | normalized_osv_identities | LC_ALL=C sort)" ||
  fail 'OSV Hex response was invalid'
expected_hex_identities=$'CVE-2026-43966\nCVE-2026-43969\nCVE-2026-43971\nEEF-CVE-2026-43966\nEEF-CVE-2026-43969\nEEF-CVE-2026-43971\nGHSA-g2wm-735q-3f56\nGHSA-w4f7-4cxr-rv3c'
[[ "$hex_identities" == "$expected_hex_identities" ]] || fail 'Hex 2.19.0 advisory set changed'

patch_record="$(curl --proto '=https' --tlsv1.2 -fsS "$OSV_ADVISORY_URL")" ||
  fail 'OSV patch record query failed'
printf '%s' "$patch_record" | jq -e --arg repo "${UPSTREAM_URL%.git}" --arg fixed "$PATCH_COMMIT" '
  .id == "EEF-CVE-2026-43971"
  and any(.affected[]?.ranges[]?;
    .type == "GIT"
    and .repo == $repo
    and any(.events[]?; .fixed == $fixed))
' >/dev/null || fail 'OSV no longer binds CVE-2026-43971 to the pinned fix'

commit_response="$(osv_query "{\"commit\":\"$PATCH_COMMIT\"}")" ||
  fail 'OSV commit query failed'
commit_identities="$(printf '%s' "$commit_response" | normalized_osv_identities | LC_ALL=C sort)" ||
  fail 'OSV commit response was invalid'
allowed_commit_identities=$'CVE-2026-43966\nCVE-2026-43969\nEEF-CVE-2026-43966\nEEF-CVE-2026-43969\nGHSA-g2wm-735q-3f56\nGHSA-w4f7-4cxr-rv3c'
unexpected_commit_identities="$(comm -23 \
  <(printf '%s\n' "$commit_identities" | sed '/^$/d') \
  <(printf '%s\n' "$allowed_commit_identities"))"
[[ -z "$unexpected_commit_identities" ]] || fail 'pinned commit acquired a new advisory'

printf 'Cowlib security contract passed for Hex %s plus pinned upstream fix.\n' "$BASE_VERSION"
