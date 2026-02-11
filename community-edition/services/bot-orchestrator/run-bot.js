#!/usr/bin/env node
const doc = `
Usage:
    ./run-bot.js [options]
Options:
    -h --help            Show this screen
    -u --url=<url>       URL [default: https://meta-hubs.org/hub.html]
    -r --room=<room>     Room id
    --runner             Enable room bot-runner mode for this process
`;

const docopt = require("docopt").docopt;
const options = docopt(doc);

const puppeteer = require("puppeteer-core");

const executablePath =
  process.env.PUPPETEER_EXECUTABLE_PATH || process.env.CHROMIUM_PATH || "/usr/bin/chromium";

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
    page.on("console", msg => log("PAGE:", msg.text()));
    page.on("error", err => log("ERROR:", err.toString().split("\n")[0]));
    page.on("pageerror", err => log("PAGE ERROR:", err.toString().split("\n")[0]));
    return page;
  };

  let page = await createPage();

  const baseUrl = options["--url"] || "https://meta-hubs.org/hub.html";
  const roomOption = options["--room"];

  const params = {
    bot: true,
    allow_multi: true
  };

  if (options["--runner"]) {
    params.bot_runner = true;
  }

  if (roomOption) {
    params.hub_id = roomOption;
  }

  const query = new URLSearchParams(params).toString();
  const separator = baseUrl.includes("?") ? "&" : "?";
  const url = `${baseUrl}${separator}${query}`;
  log("Runner URL:", url);

  const navigateWithRetry = async (maxRetries = 8) => {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        log(`Launching room runner (attempt ${attempt}/${maxRetries})...`);
        await page.goto(url, { waitUntil: "networkidle2" });
        await page.mouse.click(100, 100);
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
