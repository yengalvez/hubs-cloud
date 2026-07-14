FROM node:22-bookworm-slim

ENV AWS_LAMBDA_FUNCTION_NAME=turkey \
    NODE_ENV=production

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates fonts-liberation libexpat1 libnss3 \
    && rm -rf /var/lib/apt/lists/*

COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force

COPY --chown=1000:1000 app.js index.js url-policy.js utils.js ./

USER 1000:1000
EXPOSE 5000

CMD ["node", "app.js"]
