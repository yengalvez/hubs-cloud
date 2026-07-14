"use strict";

const { getBrowser, closeBrowser } = require("./utils");
const { installRequestGuard, safeUrlForLog, validatePublicHttpUrl } = require("./url-policy");

const NAVIGATION_TIMEOUT_MS = 12_000;
const RENDER_SETTLE_MS = 300;

async function screenshot(rawUrl) {
  const url = await validatePublicHttpUrl(rawUrl);
  const browser = await getBrowser();
  const page = await browser.newPage();

  try {
    await installRequestGuard(page);
    page.setDefaultNavigationTimeout(NAVIGATION_TIMEOUT_MS);
    page.setDefaultTimeout(5_000);
    await page.setViewport({ width: 1280, height: 720, deviceScaleFactor: 1 });
    await page.goto(url.href, { waitUntil: "domcontentloaded", timeout: NAVIGATION_TIMEOUT_MS });
    await new Promise(resolve => setTimeout(resolve, RENDER_SETTLE_MS));
    return await page.screenshot({ encoding: "base64", type: "png" });
  } finally {
    await page.close().catch(() => undefined);
  }
}

module.exports.handler = async function handler(event, _context, callback) {
  const rawUrl = event?.queryStringParameters?.url || "https://www.mozilla.org";

  try {
    const data = await screenshot(rawUrl);
    console.log(`screenshot complete for ${safeUrlForLog(rawUrl)}`);
    callback(null, {
      statusCode: 200,
      body: data,
      isBase64Encoded: true,
      headers: {
        "Cache-Control": "no-store",
        "Content-Type": "image/png"
      }
    });
  } catch (error) {
    const statusCode = error?.code === "URL_NOT_ALLOWED" ? 403 : 502;
    console.error(`screenshot rejected for ${safeUrlForLog(rawUrl)}`, error?.name || "Error");
    callback(null, {
      statusCode,
      body: statusCode === 403 ? "forbidden" : "screenshot failed",
      isBase64Encoded: false,
      headers: { "Cache-Control": "no-store", "Content-Type": "text/plain" }
    });
  } finally {
    if (process.env.AWS_LAMBDA_FUNCTION_NAME !== "turkey") {
      await closeBrowser();
    }
  }
};

module.exports.screenshot = screenshot;
