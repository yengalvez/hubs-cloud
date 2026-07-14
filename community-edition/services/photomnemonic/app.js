"use strict";

const express = require("express");

const lambda = require("./index");
const { closeBrowser, getBrowser, isBrowserReady } = require("./utils");

const DEFAULT_PORT = 5000;
const maxConcurrentScreenshots = Math.min(
  4,
  Math.max(1, Number.parseInt(process.env.MAX_CONCURRENT_SCREENSHOTS || "1", 10) || 1)
);

function invokeScreenshot(url) {
  return new Promise((resolve, reject) => {
    lambda.handler({ queryStringParameters: { url } }, null, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
}

function createApp() {
  const app = express();
  let inFlight = 0;

  app.disable("x-powered-by");

  app.get("/_healthz", (_request, response) => response.status(200).send("1"));
  app.get("/_readyz", (_request, response) =>
    response.status(isBrowserReady() ? 200 : 503).send(isBrowserReady() ? "1" : "0")
  );

  app.get("/screenshot", async (request, response) => {
    const url = request.query.url;
    if (typeof url !== "string") {
      response.status(400).send("missing url");
      return;
    }
    if (inFlight >= maxConcurrentScreenshots) {
      response.status(429).send("busy");
      return;
    }

    inFlight += 1;
    try {
      const result = await invokeScreenshot(url);
      const body = result.isBase64Encoded ? Buffer.from(result.body, "base64") : result.body;
      response.status(result.statusCode).set(result.headers || {}).send(body);
    } catch (error) {
      console.error("screenshot request failed", error?.name || "Error");
      response.status(502).send("screenshot failed");
    } finally {
      inFlight -= 1;
    }
  });

  return app;
}

async function main() {
  const app = createApp();
  const port = Number.parseInt(process.env.PORT || String(DEFAULT_PORT), 10);
  const server = app.listen(port, "0.0.0.0", () => console.log(`listening on :${port}`));
  server.headersTimeout = 10_000;
  server.requestTimeout = 30_000;
  server.keepAliveTimeout = 5_000;

  getBrowser().catch(error => console.error("browser warmup failed", error?.name || "Error"));

  const shutdown = async signal => {
    console.log(`received ${signal}; shutting down`);
    server.close();
    await closeBrowser();
    process.exit(0);
  };
  process.once("SIGTERM", () => shutdown("SIGTERM"));
  process.once("SIGINT", () => shutdown("SIGINT"));
}

if (require.main === module) {
  main().catch(error => {
    console.error("photomnemonic startup failed", error?.name || "Error");
    process.exit(1);
  });
}

module.exports = { createApp, invokeScreenshot };
