FROM node:20-bookworm-slim AS parent-dependencies

WORKDIR /build
COPY package.json /build/package.json
COPY package-lock.json /build/package-lock.json
RUN npm ci --omit=dev \
  && npm rm --no-save docopt gl-matrix phoenix puppeteer-core query-string three three-pathfinding ws \
  && npm cache clean --force

FROM node:20-bookworm-slim

ENV NODE_ENV=production

WORKDIR /app
COPY package.parent.json /app/package.json
COPY --from=parent-dependencies --chown=1000:1000 /build/node_modules /app/node_modules
COPY app.js /app/app.js
COPY kubernetes-runner-manager.js /app/kubernetes-runner-manager.js
COPY runner-generation-token.js /app/runner-generation-token.js

RUN chown -R 1000:1000 /app
USER 1000:1000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:' + (process.env.PORT || 5001) + '/health').then(r => process.exit(r.ok ? 0 : 1)).catch(() => process.exit(1))"

CMD ["node", "/app/app.js"]
