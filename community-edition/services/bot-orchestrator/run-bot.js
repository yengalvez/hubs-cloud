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
const querystring = require("query-string");

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

  const page = await browser.newPage();
  await page.setBypassCSP(true);
  page.on("console", msg => log("PAGE:", msg.text()));
  page.on("error", err => log("ERROR:", err.toString().split("\n")[0]));
  page.on("pageerror", err => log("PAGE ERROR:", err.toString().split("\n")[0]));

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

  const url = `${baseUrl}?${querystring.stringify(params)}`;
  log("Runner URL:", url);

  const navigate = async () => {
    try {
      log("Launching room runner...");
      await page.goto(url, { waitUntil: "networkidle2" });
      await page.mouse.click(100, 100);
    } catch (e) {
      log("Navigation error:", e.message);
      setTimeout(navigate, 1500);
    }
  };

  await navigate();

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
