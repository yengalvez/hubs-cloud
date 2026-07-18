const assert = require("node:assert/strict");
const { test } = require("node:test");

const {
  createRunnerGenerationToken,
  verifyRunnerGenerationToken
} = require("../runner-generation-token");

const key = "test-orchestrator-generation-key-at-least-32";
const claims = {
  hubSid: "Room_123",
  processGeneration: "11111111-1111-4111-8111-111111111111",
  holderId: "22222222-2222-4222-8222-222222222222",
  expiresAtSeconds: 2_000_000_300
};

test("runner generation credentials are exact room, generation, holder, and expiry scopes", () => {
  const token = createRunnerGenerationToken({ key, ...claims });
  const verified = verifyRunnerGenerationToken(token, key, {
    nowSeconds: 2_000_000_000,
    hubSid: claims.hubSid,
    processGeneration: claims.processGeneration,
    holderId: claims.holderId
  });

  assert.equal(verified.hub_sid, claims.hubSid);
  assert.equal(verified.process_generation, claims.processGeneration);
  assert.equal(verified.holder_id, claims.holderId);
  assert.equal(
    verifyRunnerGenerationToken(token, key, { nowSeconds: 2_000_000_000, hubSid: "other" }),
    null
  );
  assert.equal(verifyRunnerGenerationToken(token, key, { nowSeconds: 2_000_000_331 }), null);
  const farFuture = createRunnerGenerationToken({
    key,
    ...claims,
    expiresAtSeconds: 2_000_086_431
  });
  assert.equal(verifyRunnerGenerationToken(farFuture, key, { nowSeconds: 2_000_000_000 }), null);
});

test("runner generation credentials reject tampering and any authority pre-binding claim", () => {
  const token = createRunnerGenerationToken({ key, ...claims });
  assert.equal(verifyRunnerGenerationToken(`${token}x`, key, { nowSeconds: 2_000_000_000 }), null);
  assert.throws(
    () => createRunnerGenerationToken({ key, ...claims, fenceEpoch: 0 }),
    /scope_invalid/
  );
  assert.throws(
    () => createRunnerGenerationToken({ key, ...claims, leaseId: "33333333-3333-4333-8333-333333333333" }),
    /scope_invalid/
  );
  assert.throws(
    () => createRunnerGenerationToken({ key: "weak", ...claims }),
    /key_invalid/
  );
});
