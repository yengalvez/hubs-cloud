"use strict";

const dns = require("node:dns").promises;
const net = require("node:net");

const MAX_URL_LENGTH = 2048;
const DNS_CACHE_MS = 30_000;
const NETWORK_PROTOCOLS = new Set(["http:", "https:"]);
const LOCAL_PROTOCOLS = new Set(["about:", "blob:", "data:"]);
// Keep families separate: Node maps IPv4 checks into ::ffff/96 when a mixed
// BlockList is used, which would make an IPv6 mapped-address rule block all IPv4.
const blockedIPv4 = new net.BlockList();
const blockedIPv6 = new net.BlockList();

for (const [network, prefix] of [
  ["0.0.0.0", 8],
  ["10.0.0.0", 8],
  ["100.64.0.0", 10],
  ["127.0.0.0", 8],
  ["169.254.0.0", 16],
  ["172.16.0.0", 12],
  ["192.0.0.0", 24],
  ["192.0.2.0", 24],
  ["192.168.0.0", 16],
  ["198.18.0.0", 15],
  ["198.51.100.0", 24],
  ["203.0.113.0", 24],
  ["224.0.0.0", 4],
  ["240.0.0.0", 4]
]) {
  blockedIPv4.addSubnet(network, prefix, "ipv4");
}

for (const [network, prefix] of [
  ["::", 128],
  ["::1", 128],
  ["::ffff:0:0", 96],
  ["100::", 64],
  ["2001:db8::", 32],
  ["fc00::", 7],
  ["fe80::", 10],
  ["ff00::", 8]
]) {
  blockedIPv6.addSubnet(network, prefix, "ipv6");
}

function notAllowed(message) {
  const error = new Error(message);
  error.code = "URL_NOT_ALLOWED";
  return error;
}

function normalizeHostname(hostname) {
  return hostname.startsWith("[") && hostname.endsWith("]") ? hostname.slice(1, -1) : hostname;
}

function isBlockedAddress(address) {
  const family = net.isIP(address);
  if (!family) return true;
  return family === 6 ? blockedIPv6.check(address, "ipv6") : blockedIPv4.check(address, "ipv4");
}

async function resolvePublicAddresses(hostname, lookup = dns.lookup) {
  const normalized = normalizeHostname(hostname);
  const literalFamily = net.isIP(normalized);
  let addresses;

  if (literalFamily) {
    addresses = [{ address: normalized, family: literalFamily }];
  } else {
    const result = await lookup(normalized, { all: true, verbatim: true });
    addresses = Array.isArray(result) ? result : [result];
  }

  if (!addresses.length || addresses.some(entry => !entry?.address || isBlockedAddress(entry.address))) {
    throw notAllowed("host resolves to a blocked address");
  }
  return addresses;
}

async function validatePublicHttpUrl(rawUrl, options = {}) {
  if (typeof rawUrl !== "string" || rawUrl.length === 0 || rawUrl.length > MAX_URL_LENGTH) {
    throw notAllowed("invalid url length");
  }

  let url;
  try {
    url = new URL(rawUrl);
  } catch (_error) {
    throw notAllowed("invalid url");
  }

  if (
    !NETWORK_PROTOCOLS.has(url.protocol) ||
    !url.hostname ||
    url.username ||
    url.password ||
    (url.port && url.port !== "80" && url.port !== "443")
  ) {
    throw notAllowed("unsupported url");
  }
  await resolvePublicAddresses(url.hostname, options.lookup || dns.lookup);
  return url;
}

function safeUrlForLog(rawUrl) {
  try {
    const url = new URL(rawUrl);
    return NETWORK_PROTOCOLS.has(url.protocol) ? `${url.protocol}//${url.host}` : "unsupported-url";
  } catch (_error) {
    return "invalid-url";
  }
}

async function installRequestGuard(page, options = {}) {
  const lookup = options.lookup || dns.lookup;
  const cache = new Map();
  await page.setRequestInterception(true);

  const validateRequest = async rawUrl => {
    const url = new URL(rawUrl);
    if (LOCAL_PROTOCOLS.has(url.protocol)) return;
    if (!NETWORK_PROTOCOLS.has(url.protocol)) throw notAllowed("unsupported browser request");

    const cacheKey = url.hostname.toLowerCase();
    const cached = cache.get(cacheKey);
    if (cached && Date.now() - cached < DNS_CACHE_MS) return;
    await validatePublicHttpUrl(rawUrl, { lookup });
    cache.set(cacheKey, Date.now());
  };

  page.on("request", async request => {
    try {
      await validateRequest(request.url());
      if (!request.isInterceptResolutionHandled()) await request.continue();
    } catch (_error) {
      if (!request.isInterceptResolutionHandled()) await request.abort("blockedbyclient");
    }
  });
}

async function urlAllowed(rawUrl, options = {}) {
  try {
    await validatePublicHttpUrl(rawUrl, options);
    return true;
  } catch (_error) {
    return false;
  }
}

module.exports = {
  installRequestGuard,
  isBlockedAddress,
  resolvePublicAddresses,
  safeUrlForLog,
  urlAllowed,
  validatePublicHttpUrl
};
