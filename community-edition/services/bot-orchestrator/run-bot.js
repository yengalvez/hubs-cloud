#!/usr/bin/env node
const doc = `
Usage:
    ./run-bot.js [options]
Options:
    -h --help            Show this screen
    -u --url=<url>       URL [default: https://meta-hubs.org]
    -r --room=<room>     Room id
    --runner             Enable room bot-runner mode for this process
`;

const docopt = require("docopt").docopt;
const options = docopt(doc);

const puppeteer = require("puppeteer-core");

const executablePath =
  process.env.PUPPETEER_EXECUTABLE_PATH || process.env.CHROMIUM_PATH || "/usr/bin/chromium";
const NAVIGATION_TIMEOUT_MS = Number(process.env.RUNNER_NAV_TIMEOUT_MS || 120000);
const STARTUP_TIMEOUT_MS = Number(process.env.RUNNER_STARTUP_TIMEOUT_MS || 120000);

function log(...objs) {
  console.log.call(null, [new Date().toISOString()].concat(objs).join(" "));
}

(async () => {
  const browser = await puppeteer.launch({
    executablePath,
    ignoreHTTPSErrors: true,
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--ignore-gpu-blacklist",
      "--ignore-certificate-errors",
      "--disable-dev-shm-usage"
    ]
  });

  const createPage = async () => {
    const page = await browser.newPage();
    await page.setBypassCSP(true);
    page.setDefaultNavigationTimeout(NAVIGATION_TIMEOUT_MS);
    page.setDefaultTimeout(NAVIGATION_TIMEOUT_MS);
    page.on("console", msg => log("PAGE:", msg.text()));
    page.on("error", err => log("ERROR:", err.toString().split("\n")[0]));
    page.on("pageerror", err => log("PAGE ERROR:", err.toString().split("\n")[0]));
    return page;
  };

  let page = await createPage();

  const baseUrl = options["--url"] || "https://meta-hubs.org";
  const roomOption = options["--room"];

  const params = {
    bot: true,
    allow_multi: true
  };

  if (options["--runner"]) {
    params.bot_runner = true;
  }

  const buildRunnerUrl = () => {
    const url = new URL(baseUrl);
    const isLegacyHubHtml = /\/hub\.html$/i.test(url.pathname);

    if (roomOption) {
      if (isLegacyHubHtml) {
        params.hub_id = roomOption;
      } else {
        const basePath = url.pathname.replace(/\/+$/, "");
        url.pathname = `${basePath}/${roomOption}`.replace(/\/{2,}/g, "/");
      }
    }

    url.search = new URLSearchParams(params).toString();
    return url.toString();
  };

  const url = buildRunnerUrl();
  log("Runner URL:", url);

  const navigateWithRetry = async (maxRetries = 8) => {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        log(`Launching room runner (attempt ${attempt}/${maxRetries})...`);
        await page.goto(url, { waitUntil: "domcontentloaded", timeout: NAVIGATION_TIMEOUT_MS });
        await page.mouse.click(100, 100);
        await page.waitForFunction(
          () => {
            const scene = document.querySelector("a-scene");
            return !!scene;
          },
          { timeout: STARTUP_TIMEOUT_MS }
        );
        await page.waitForFunction(
          () => {
            const scene = window.APP?.scene || document.querySelector("a-scene");
            const entered = !!(scene && scene.is && scene.is("entered"));
            const connection = window.NAF && window.NAF.connection;
            const connected = !!(connection && typeof connection.isConnected === "function" && connection.isConnected());
            return entered && connected;
          },
          { timeout: STARTUP_TIMEOUT_MS }
        );
        log("Runner startup complete (scene entered + network connected).");
        return;
      } catch (e) {
        log("Navigation error:", e.message);
        try {
          if (!page.isClosed()) {
            await page.close();
          }
        } catch (_closeErr) {}

        page = await createPage();
        await new Promise(resolve => setTimeout(resolve, 1500));
      }
    }

    log("Runner failed to start after retries, exiting.");
    process.exit(1);
  };

  await navigateWithRetry();

  const shutdown = async signal => {
    log(`Received ${signal}, shutting down runner.`);
    try {
      await browser.close();
    } catch (_err) {}
    process.exit(0);
  };

  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT", () => shutdown("SIGINT"));

  setInterval(async () => {
    try {
      const metrics = await page.evaluate(() => ({
        occupants: Object.keys(NAF.connection.adapter.occupants).length,
        bots: document.querySelectorAll("[bot-info]").length
      }));
      log("Runner metrics:", JSON.stringify(metrics));
    } catch (e) {
      log("Runner metrics read error:", e.message);
    }
  }, 60_000);
})();
