#!/bin/sh

set -eu

script_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

mkdir -p "$tmp_dir/bin"

cat > "$tmp_dir/bin/ip" <<'EOF'
#!/bin/sh
echo '2: eth0    inet 10.0.0.2/24 scope global eth0'
EOF

cat > "$tmp_dir/bin/curl" <<'EOF'
#!/bin/sh
echo '203.0.113.10'
EOF

cat > "$tmp_dir/bin/turnserver" <<'EOF'
#!/bin/sh
printf '%s\n' "$@" > "$CAPTURE_ARGS"
cp "$TURN_CONFIG_PATH" "$CAPTURE_CONFIG"
EOF

chmod +x "$tmp_dir/bin/ip" "$tmp_dir/bin/curl" "$tmp_dir/bin/turnserver"

sentinel='postgres://user:must-not-leak@example.invalid/db'
output_file="$tmp_dir/output"
export CAPTURE_ARGS="$tmp_dir/args"
export CAPTURE_CONFIG="$tmp_dir/config-copy"
export TURN_CONFIG_PATH="$tmp_dir/turnserver.conf"

PATH="$tmp_dir/bin:$PATH" \
  PSQL="$sentinel" \
  REALM='test-realm' \
  COTURN_HEALTHCHECK_DISABLED=true \
  sh "$script_dir/entrypoint.sh" >"$output_file" 2>&1

if grep -Fq "$sentinel" "$output_file" || grep -Fq "$sentinel" "$CAPTURE_ARGS"; then
  echo "Coturn entrypoint leaked PSQL outside its configuration file" >&2
  exit 1
fi

grep -Fqx "psql-userdb=$sentinel" "$CAPTURE_CONFIG"
grep -Fqx "$TURN_CONFIG_PATH" "$CAPTURE_ARGS"

echo "Coturn entrypoint credential test passed."
