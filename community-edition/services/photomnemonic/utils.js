"use strict";

let browser = null;
let launchPromise = null;

async function launchBrowser() {
  const [{ default: chromium }, puppeteer] = await Promise.all([
    import("@sparticuz/chromium"),
    import("puppeteer-core")
  ]);
  const chromiumArgs = chromium.args.filter(argument => argument !== "--single-process");
  const args = await puppeteer.defaultArgs({
    args: [...chromiumArgs, "--hide-scrollbars", "--window-size=1280,720"],
    headless: "shell"
  });
  const launched = await puppeteer.launch({
    args,
    defaultViewport: { width: 1280, height: 720, deviceScaleFactor: 1 },
    executablePath: await chromium.executablePath(),
    headless: "shell",
    timeout: 20_000
  });
  launched.once("disconnected", () => {
    if (browser === launched) browser = null;
    launchPromise = null;
  });
  browser = launched;
  return launched;
}

async function getBrowser() {
  if (browser?.connected) return browser;
  if (!launchPromise) {
    launchPromise = launchBrowser().catch(error => {
      launchPromise = null;
      throw error;
    });
  }
  return launchPromise;
}

function isBrowserReady() {
  return Boolean(browser?.connected);
}

async function closeBrowser() {
  const current = browser;
  browser = null;
  launchPromise = null;
  if (current) await current.close().catch(() => undefined);
}

module.exports = { closeBrowser, getBrowser, isBrowserReady };
