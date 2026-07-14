#!/bin/sh

set -eu

: "${PSQL:?PSQL is required}"
: "${REALM:?REALM is required}"

turn_config_path="${TURN_CONFIG_PATH:-/etc/turnserver.conf}"

healthcheck() {
  while true; do
    printf 'HTTP/1.1 200 OK\r\n\r\n 1' | nc -lp 1111 >/dev/null
  done
}

if [ "${COTURN_HEALTHCHECK_DISABLED:-false}" != "true" ]; then
  healthcheck &
fi

internal_ip="$(
  ip address |
    grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' |
    grep -Eo '([0-9]*\.){3}[0-9]*' |
    grep -v '127.0.0.1' |
    head -1
)"
external_ip="$(curl --fail --silent --show-error --max-time 10 https://ipinfo.io/ip)"

if [ -z "$internal_ip" ] || [ -z "$external_ip" ]; then
  echo "Coturn could not resolve its internal or external IP" >&2
  exit 1
fi

cat > "$turn_config_path" <<EOF
realm=$REALM
no-udp=true
no-tcp=true
no-dtls=false
no-tls=false
no-auth-pings=true
no-dynamic-ip-list=true
no-dynamic-realms=true
min-port=49152
max-port=51609
tls-listening-port=5349
use-auth-secret=true
cert=/certs/cert.pem
pkey=/certs/key.pem
listening-ip=$internal_ip
relay-ip=$internal_ip
external-ip=$external_ip
psql-userdb=$PSQL
EOF

echo "Starting Coturn for realm $REALM on the resolved node address"
exec turnserver -c "$turn_config_path" --log-file=stdout --lt-cred-mech
