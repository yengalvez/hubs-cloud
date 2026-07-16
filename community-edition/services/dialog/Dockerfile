# syntax=docker/dockerfile:1
ARG NODE_VERSION=22

FROM node:${NODE_VERSION} AS build
workdir /app
run apt-get update > /dev/null \
    && apt-get install -y --no-install-recommends python3-pip > /dev/null \
    && rm -rf /var/lib/apt/lists/*
run mkdir certs && openssl req -x509 -newkey rsa:2048 -sha256 -days 36500 -nodes -keyout certs/privkey.pem -out certs/fullchain.pem -subj '/CN=dialog'
copy package.json .
copy package-lock.json .
run npm ci
copy . .
FROM node:${NODE_VERSION}-slim
workdir /app
copy --chown=1000:1000 --from=build /app /app
# run apt-get update > /dev/null && apt-get install -y jq curl dnsutils netcat > /dev/null
run apt-get update > /dev/null \
    && apt-get install -y --no-install-recommends jq curl dnsutils > /dev/null \
    && rm -rf /var/lib/apt/lists/*
copy scripts/docker/run.sh /run.sh
USER 1000:1000
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD node -e "const s=require('net').connect(4443,'127.0.0.1',()=>{s.end();process.exit(0)});s.setTimeout(2000,()=>process.exit(1));s.on('error',()=>process.exit(1))"
cmd ["/bin/bash", "/run.sh"]
