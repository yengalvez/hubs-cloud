const crypto = require("node:crypto");

const TOKEN_VERSION = "v1";
const TOKEN_AUDIENCE = "yenhubs-bot-runner";
const MAX_TOKEN_BYTES = 2048;
const MAX_CLOCK_SKEW_SECONDS = 30;
const MAX_TOKEN_TTL_SECONDS = 86_400;

function validUuid(value) {
  return typeof value === "string" &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
}

function validHubSid(value) {
  return typeof value === "string" && /^[A-Za-z0-9_-]{1,64}$/.test(value);
}

function validHolderId(value) {
  return typeof value === "string" && /^[A-Za-z0-9_.:-]{1,128}$/.test(value);
}

function strongKey(value) {
  return typeof value === "string" && Buffer.byteLength(value, "utf8") >= 32;
}

function base64UrlJson(value) {
  return Buffer.from(JSON.stringify(value), "utf8").toString("base64url");
}

function signEncodedPayload(encodedPayload, key) {
  return crypto.createHmac("sha256", key).update(`${TOKEN_VERSION}.${encodedPayload}`).digest("base64url");
}

function createRunnerGenerationToken(input) {
  const allowedInputKeys = ["expiresAtSeconds", "holderId", "hubSid", "key", "processGeneration"];
  if (
    !input ||
    typeof input !== "object" ||
    Array.isArray(input) ||
    Object.keys(input).sort().join("\0") !== allowedInputKeys.sort().join("\0")
  ) {
    throw new Error("runner_generation_token_scope_invalid");
  }
  const { key, hubSid, processGeneration, holderId, expiresAtSeconds } = input;
  if (!strongKey(key)) throw new Error("runner_generation_token_key_invalid");
  if (!validHubSid(hubSid)) throw new Error("runner_generation_token_hub_invalid");
  if (!validUuid(processGeneration)) throw new Error("runner_generation_token_generation_invalid");
  if (!validHolderId(holderId)) throw new Error("runner_generation_token_holder_invalid");
  if (!Number.isSafeInteger(expiresAtSeconds) || expiresAtSeconds <= 0) {
    throw new Error("runner_generation_token_expiry_invalid");
  }
  const payload = {
    v: 1,
    aud: TOKEN_AUDIENCE,
    hub_sid: hubSid,
    process_generation: processGeneration,
    holder_id: holderId,
    exp: expiresAtSeconds
  };
  const encodedPayload = base64UrlJson(payload);
  return `${TOKEN_VERSION}.${encodedPayload}.${signEncodedPayload(encodedPayload, key)}`;
}

function secureEqual(left, right) {
  if (typeof left !== "string" || typeof right !== "string") return false;
  const leftBuffer = Buffer.from(left);
  const rightBuffer = Buffer.from(right);
  return leftBuffer.length === rightBuffer.length && crypto.timingSafeEqual(leftBuffer, rightBuffer);
}

function exactPayloadKeys(payload) {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) return false;
  const required = ["aud", "exp", "holder_id", "hub_sid", "process_generation", "v"];
  const keys = Object.keys(payload).sort();
  return keys.length === required.length && required.every(key => keys.includes(key));
}

function verifyRunnerGenerationToken(
  token,
  key,
  {
    nowSeconds = Math.floor(Date.now() / 1000),
    hubSid = null,
    processGeneration = null,
    holderId = null,
    maxClockSkewSeconds = MAX_CLOCK_SKEW_SECONDS
  } = {}
) {
  if (
    !strongKey(key) ||
    typeof token !== "string" ||
    Buffer.byteLength(token, "utf8") > MAX_TOKEN_BYTES ||
    !Number.isSafeInteger(nowSeconds) ||
    !Number.isSafeInteger(maxClockSkewSeconds) ||
    maxClockSkewSeconds < 0 ||
    maxClockSkewSeconds > MAX_CLOCK_SKEW_SECONDS
  ) {
    return null;
  }
  const parts = token.split(".");
  if (parts.length !== 3 || parts[0] !== TOKEN_VERSION || !parts[1] || !parts[2]) return null;
  if (!secureEqual(parts[2], signEncodedPayload(parts[1], key))) return null;

  let payload;
  try {
    const decoded = Buffer.from(parts[1], "base64url").toString("utf8");
    if (base64UrlJson(JSON.parse(decoded)) !== parts[1]) return null;
    payload = JSON.parse(decoded);
  } catch (_error) {
    return null;
  }

  if (
    !exactPayloadKeys(payload) ||
    payload.v !== 1 ||
    payload.aud !== TOKEN_AUDIENCE ||
    !validHubSid(payload.hub_sid) ||
    !validUuid(payload.process_generation) ||
    !validHolderId(payload.holder_id) ||
    !Number.isSafeInteger(payload.exp) ||
    payload.exp <= nowSeconds - maxClockSkewSeconds ||
    payload.exp > nowSeconds + MAX_TOKEN_TTL_SECONDS + maxClockSkewSeconds
  ) {
    return null;
  }
  if (hubSid !== null && payload.hub_sid !== hubSid) return null;
  if (processGeneration !== null && payload.process_generation !== processGeneration) return null;
  if (holderId !== null && payload.holder_id !== holderId) return null;
  return payload;
}

module.exports = {
  MAX_TOKEN_BYTES,
  MAX_TOKEN_TTL_SECONDS,
  TOKEN_AUDIENCE,
  createRunnerGenerationToken,
  verifyRunnerGenerationToken
};
