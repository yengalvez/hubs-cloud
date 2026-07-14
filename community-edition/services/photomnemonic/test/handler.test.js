"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");

const { handler } = require("../index");

function invoke(url) {
  return new Promise((resolve, reject) => {
    handler({ queryStringParameters: { url } }, null, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
}

test("handler rejects loopback without launching Chromium", async () => {
  const response = await invoke("http://127.0.0.1/private?token=not-logged");
  assert.equal(response.statusCode, 403);
  assert.equal(response.body, "forbidden");
  assert.equal(response.isBase64Encoded, false);
});

test("handler rejects oversized URLs before navigation", async () => {
  const response = await invoke(`https://example.com/${"a".repeat(2048)}`);
  assert.equal(response.statusCode, 403);
  assert.equal(response.body, "forbidden");
});
