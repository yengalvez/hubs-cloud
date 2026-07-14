"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");

const {
  installRequestGuard,
  isBlockedAddress,
  safeUrlForLog,
  urlAllowed,
  validatePublicHttpUrl
} = require("../url-policy");

const publicLookup = async () => [{ address: "93.184.216.34", family: 4 }];

test("accepts only credential-free public HTTP URLs", async () => {
  assert.equal(await urlAllowed("https://example.com/path?q=1", { lookup: publicLookup }), true);
  assert.equal(await urlAllowed("http://example.com:80", { lookup: publicLookup }), true);
  assert.equal(await urlAllowed("https://example.com:443", { lookup: publicLookup }), true);
  assert.equal(await urlAllowed("http://example.com:8080", { lookup: publicLookup }), false);
  assert.equal(await urlAllowed("file:///etc/passwd", { lookup: publicLookup }), false);
  assert.equal(await urlAllowed("https://user:secret@example.com", { lookup: publicLookup }), false);
  assert.equal(await urlAllowed("not a url", { lookup: publicLookup }), false);
});

test("rejects private, metadata, documentation and mapped addresses", async () => {
  for (const address of [
    "0.0.0.1",
    "10.0.0.1",
    "100.64.0.1",
    "127.0.0.1",
    "169.254.169.254",
    "172.16.0.1",
    "192.168.0.1",
    "198.18.0.1",
    "203.0.113.1",
    "::1",
    "fc00::1",
    "fe80::1",
    "::ffff:127.0.0.1"
  ]) {
    assert.equal(isBlockedAddress(address), true, address);
  }
  assert.equal(isBlockedAddress("93.184.216.34"), false);
  assert.equal(isBlockedAddress("2606:2800:220:1:248:1893:25c8:1946"), false);
});

test("fails closed when any DNS answer is private", async () => {
  const mixedLookup = async () => [
    { address: "93.184.216.34", family: 4 },
    { address: "127.0.0.1", family: 4 }
  ];
  await assert.rejects(() => validatePublicHttpUrl("https://example.com", { lookup: mixedLookup }), {
    code: "URL_NOT_ALLOWED"
  });
});

test("request interception checks redirected and subresource destinations", async () => {
  let requestHandler;
  const page = {
    setRequestInterception: async enabled => assert.equal(enabled, true),
    on: (event, handler) => {
      assert.equal(event, "request");
      requestHandler = handler;
    }
  };
  const lookup = async hostname => [
    { address: hostname === "internal.example" ? "10.0.0.10" : "93.184.216.34", family: 4 }
  ];
  await installRequestGuard(page, { lookup });

  const makeRequest = url => {
    const state = { continued: false, aborted: false };
    return {
      state,
      url: () => url,
      isInterceptResolutionHandled: () => state.continued || state.aborted,
      continue: async () => {
        state.continued = true;
      },
      abort: async () => {
        state.aborted = true;
      }
    };
  };

  const publicRequest = makeRequest("https://example.com/page");
  await requestHandler(publicRequest);
  assert.equal(publicRequest.state.continued, true);

  const redirectedRequest = makeRequest("http://internal.example/admin");
  await requestHandler(redirectedRequest);
  assert.equal(redirectedRequest.state.aborted, true);
});

test("logs only an origin and strips paths, queries and credentials", () => {
  assert.equal(safeUrlForLog("https://example.com/private?token=value"), "https://example.com");
  assert.equal(safeUrlForLog("https://user:secret@example.com/path"), "https://example.com");
  assert.equal(safeUrlForLog("invalid"), "invalid-url");
});
