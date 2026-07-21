const assert = require("node:assert/strict");
const { spawn } = require("node:child_process");
const { EventEmitter, once } = require("node:events");
const { PassThrough } = require("node:stream");
const test = require("node:test");

const { PodWatchEvidence } = require("./pod-quiescence-watch");
const {
  replaceWithBookmarkedSuccessors,
  startEvidenceWatchProcess,
  stopEvidenceWatches,
  waitForBookmarkedWatchBoundary,
  withOwnedEvidenceWatches
} = require("./watch-evidence-process");

class FakeChild extends EventEmitter {
  constructor({ ignoreSigterm = false } = {}) {
    super();
    this.stdout = new PassThrough();
    this.stderr = new PassThrough();
    this.exitCode = null;
    this.signalCode = null;
    this.signals = [];
    this.ignoreSigterm = ignoreSigterm;
  }

  kill(signal) {
    this.signals.push(signal);
    if (signal === "SIGTERM" && this.ignoreSigterm) return true;
    this.finish(null, signal);
    return true;
  }

  finish(code = 0, signal = null, trailingLines = []) {
    this.exitCode = code;
    this.signalCode = signal;
    this.emit("exit", code, signal);
    for (const line of trailingLines) this.stdout.write(`${JSON.stringify(line)}\n`);
    this.stdout.end();
    this.stderr.end();
    this.emit("close", code, signal);
  }

  exitWithoutClose(code = 0, signal = null) {
    this.exitCode = code;
    this.signalCode = signal;
    this.emit("exit", code, signal);
  }
}

function podEvent(type, resourceVersion, name = "runner") {
  return {
    type,
    object: {
      apiVersion: "v1",
      kind: "Pod",
      metadata: {
        name,
        namespace: "hcce",
        uid: `${name}-uid`,
        resourceVersion,
        labels: {}
      },
      spec: { serviceAccountName: "bot-orchestrator" }
    }
  };
}

function bookmark(resourceVersion) {
  return { type: "BOOKMARK", object: { metadata: { resourceVersion } } };
}

function processFixture({
  ignoreSigterm = false,
  maximumBufferBytes = 4 * 1024 * 1024,
  childPid = null,
  killProcessGroup = undefined
} = {}) {
  const child = new FakeChild({ ignoreSigterm });
  if (childPid !== null) child.pid = childPid;
  const timers = new Map();
  let nextTimerId = 1;
  const evidence = new PodWatchEvidence("hcce", "hcce", "200");
  const watch = startEvidenceWatchProcess({
    spawnProcess(command, args, options) {
      assert.equal(command, "kubectl");
      assert.deepEqual(args, ["get", "--raw", "/fixture"]);
      assert.deepEqual(options.stdio, ["ignore", "pipe", "pipe"]);
      return child;
    },
    command: "kubectl",
    args: ["get", "--raw", "/fixture"],
    evidence,
    serverTimeoutSeconds: 1,
    processGraceSeconds: 1,
    maximumBufferBytes,
    setTimer(callback, delay) {
      const id = nextTimerId++;
      timers.set(id, { callback, delay });
      return id;
    },
    clearTimer(id) { timers.delete(id); },
    ...(killProcessGroup ? { killProcessGroup } : {})
  });
  const expireTimerWithDelay = delay => {
    const entry = [...timers.entries()].find(([, timer]) => timer.delay === delay);
    assert.ok(entry, `expected a pending ${delay}ms timer`);
    timers.delete(entry[0]);
    entry[1].callback();
  };
  return {
    child,
    evidence,
    watch,
    expireWatchdog() {
      expireTimerWithDelay(2_000);
    },
    expireTerminationGrace() {
      expireTimerWithDelay(1_000);
    },
    expireForcedSettlement() {
      expireTimerWithDelay(1_000);
    },
    pendingTimerDelays: () => [...timers.values()].map(timer => timer.delay).sort()
  };
}

function logicalWatch(resourceVersion = "200") {
  const evidence = new PodWatchEvidence("hcce", "hcce", resourceVersion);
  let running = true;
  let stopped = false;
  return {
    bookmarkBaseline: 0,
    evidence,
    isRunning: () => running,
    stop() { stopped = true; running = false; },
    close() { running = false; },
    get stopped() { return stopped; }
  };
}

test("the transport drains trailing ADDED+DELETED bytes after exit before close", async () => {
  const fixture = processFixture();
  fixture.child.finish(0, null, [
    podEvent("ADDED", "201", "late-runner"),
    podEvent("DELETED", "202", "late-runner")
  ]);
  await fixture.watch.finished;
  assert.equal(fixture.evidence.violation, true);
  assert.equal(fixture.evidence.error, "watch_ended_before_evidence_boundary");
  assert.equal(fixture.watch.isRunning(), false);
});

test("a persistent watch closing cleanly or non-zero before stop fails closed", async () => {
  for (const code of [0, 7]) {
    const fixture = processFixture();
    fixture.child.finish(code);
    const result = await fixture.watch.finished;
    assert.equal(result.code, code);
    assert.equal(fixture.evidence.error, "watch_ended_before_evidence_boundary");
  }
});

test("a signalled watch and a hung watch both fail closed", async () => {
  const signalled = processFixture();
  signalled.child.finish(null, "SIGPIPE");
  assert.equal((await signalled.watch.finished).signal, "SIGPIPE");
  assert.equal(signalled.evidence.error, "watch_ended_before_evidence_boundary");

  const hung = processFixture();
  hung.expireWatchdog();
  await hung.watch.finished;
  assert.deepEqual(hung.child.signals, ["SIGKILL"]);
  assert.equal(hung.evidence.error, "watch_process_timeout");
});

test("invalid JSON, oversized buffering and 410 are sticky failures", async () => {
  const invalid = processFixture();
  invalid.child.stdout.write("not-json\n");
  await invalid.watch.finished;
  assert.equal(invalid.evidence.error, "watch_json_invalid");

  const oversized = processFixture({ maximumBufferBytes: 1024 });
  oversized.child.stdout.write("x".repeat(1025));
  await oversized.watch.finished;
  assert.equal(oversized.evidence.error, "watch_buffer_oversize");

  const expired = processFixture();
  expired.child.stdout.write(`${JSON.stringify({ type: "ERROR", object: { code: 410 } })}\n`);
  assert.equal(expired.evidence.error, "watch_resource_version_expired");
  expired.watch.stop();
  await expired.watch.finished;
});

test("child and pipe errors are bounded transport failures", async () => {
  for (const streamName of ["stdout", "stderr"]) {
    const fixture = processFixture();
    fixture.child[streamName].emit("error", new Error("pipe failed"));
    await fixture.watch.finished;
    assert.equal(fixture.evidence.error, `watch_${streamName}_error`);
  }
  const childError = processFixture();
  childError.child.emit("error", new Error("spawn failed"));
  childError.child.finish(1);
  await childError.watch.finished;
  assert.equal(childError.evidence.error, "watch_process_error");
});

test("an intentional stop is drained without manufacturing a watch failure", async () => {
  const fixture = processFixture();
  const result = await fixture.watch.stop();
  assert.equal(result.intentionalStop, true);
  assert.equal(fixture.evidence.error, null);
  assert.deepEqual(fixture.child.signals, ["SIGTERM"]);
  assert.deepEqual(fixture.pendingTimerDelays(), []);
});

test("stop escalates an ignored SIGTERM and collects the process within its own grace", async () => {
  const fixture = processFixture({ ignoreSigterm: true });
  const stopped = fixture.watch.stop();
  assert.deepEqual(fixture.child.signals, ["SIGTERM"]);
  assert.deepEqual(fixture.pendingTimerDelays(), [1_000, 2_000]);
  fixture.expireTerminationGrace();
  const result = await stopped;
  assert.equal(result.intentionalStop, true);
  assert.deepEqual(fixture.child.signals, ["SIGTERM", "SIGKILL"]);
  assert.deepEqual(fixture.pendingTimerDelays(), []);
});

test("a transport failure also escalates an ignored SIGTERM within the cleanup grace", async () => {
  const fixture = processFixture({ ignoreSigterm: true });
  fixture.child.stdout.write("not-json\n");
  assert.equal(fixture.evidence.error, "watch_json_invalid");
  assert.deepEqual(fixture.child.signals, ["SIGTERM"]);
  fixture.expireTerminationGrace();
  await fixture.watch.finished;
  assert.deepEqual(fixture.child.signals, ["SIGTERM", "SIGKILL"]);
  assert.deepEqual(fixture.pendingTimerDelays(), []);
});

test("a process-group termination failure cannot be accepted as successful cleanup", async () => {
  const fixture = processFixture({
    childPid: 42,
    killProcessGroup() {
      const error = new Error("operation not permitted");
      error.code = "EPERM";
      throw error;
    }
  });
  await assert.rejects(stopEvidenceWatches([fixture.watch]), /watch_cleanup_failed/);
  assert.equal(fixture.evidence.error, "watch_process_group_kill_error");
  assert.deepEqual(fixture.child.signals, ["SIGTERM"]);
});

test("exit without close fails closed within a bounded forced-settlement deadline", async () => {
  const fixture = processFixture();
  fixture.child.exitWithoutClose(0);
  const stopped = fixture.watch.stop();
  assert.deepEqual(fixture.pendingTimerDelays(), [1_000, 2_000]);
  fixture.expireTerminationGrace();
  assert.deepEqual(fixture.pendingTimerDelays(), [1_000, 2_000]);
  fixture.expireForcedSettlement();
  const result = await stopped;
  assert.equal(result.cleanupTimedOut, true);
  assert.equal(fixture.evidence.error, "watch_cleanup_timeout");
  assert.deepEqual(fixture.pendingTimerDelays(), []);
});

test("an exited leader cannot leave a descendant holding watch pipes indefinitely", async () => {
  let child;
  const evidence = new PodWatchEvidence("hcce", "hcce", "200");
  const watch = startEvidenceWatchProcess({
    spawnProcess(command, args, options) {
      child = spawn(command, args, options);
      return child;
    },
    command: "/bin/sh",
    args: ["-c", "(trap '' TERM; sleep 30) & exit 0"],
    evidence,
    serverTimeoutSeconds: 5,
    processGraceSeconds: 1
  });
  await once(child, "exit");
  const started = Date.now();
  const result = await watch.stop();
  assert.equal(result.cleanupTimedOut, false);
  assert.ok(Date.now() - started < 2_500, "the detached process group must be collected");
});

test("elapsed time without an in-band bookmark never proves a boundary", async () => {
  const successor = logicalWatch();
  let now = 0;
  const accepted = await waitForBookmarkedWatchBoundary({
    successors: [successor],
    startingBookmarkSequences: [0],
    deadline: 300,
    assertHealthy() {},
    sleep: async milliseconds => { now += milliseconds; },
    now: () => now
  });
  assert.equal(accepted, false);
  assert.equal(successor.evidence.error, "watch_bookmark_deadline_exceeded");
});

test("a bookmark proves progress and leaves the successor live", async () => {
  const successor = logicalWatch();
  let now = 0;
  let sleeps = 0;
  const accepted = await waitForBookmarkedWatchBoundary({
    successors: [successor],
    startingBookmarkSequences: [0],
    deadline: 1_000,
    assertHealthy() {},
    sleep: async milliseconds => {
      now += milliseconds;
      if (++sleeps === 1) successor.evidence.ingest(bookmark("300"));
    },
    now: () => now
  });
  assert.equal(accepted, true);
  assert.equal(successor.isRunning(), true);
  assert.equal(successor.evidence.lastBookmarkResourceVersion, "300");
});

test("a delayed transient delivered before the bookmark rejects the boundary", async () => {
  const successor = logicalWatch();
  let now = 0;
  const accepted = await waitForBookmarkedWatchBoundary({
    successors: [successor],
    startingBookmarkSequences: [0],
    deadline: 1_000,
    assertHealthy() {},
    sleep: async milliseconds => {
      now += milliseconds;
      successor.evidence.ingest(podEvent("ADDED", "201", "late-runner"));
      successor.evidence.ingest(podEvent("DELETED", "202", "late-runner"));
      successor.evidence.ingest(bookmark("300"));
    },
    now: () => now
  });
  assert.equal(accepted, false);
  assert.equal(successor.evidence.violation, true);
});

test("Lease health is revalidated immediately before accepting a bookmark", async () => {
  const successor = logicalWatch();
  let now = 0;
  let healthChecks = 0;
  await assert.rejects(waitForBookmarkedWatchBoundary({
    successors: [successor],
    startingBookmarkSequences: [0],
    deadline: 1_000,
    assertHealthy() {
      healthChecks += 1;
      if (healthChecks === 3) throw new Error("lease_lost");
    },
    sleep: async milliseconds => {
      now += milliseconds;
      successor.evidence.ingest(bookmark("300"));
    },
    now: () => now
  }), /lease_lost/);
  assert.equal(healthChecks, 3);
});

test("a proven successor replaces its predecessor and remains active", async () => {
  const predecessor = logicalWatch("200");
  predecessor.evidence.ingest(bookmark("250"));
  const successor = logicalWatch("250");
  let now = 0;
  const result = await replaceWithBookmarkedSuccessors({
    predecessors: [predecessor],
    startSuccessor(value) {
      assert.equal(value.evidence.lastBookmarkResourceVersion, "250");
      return successor;
    },
    deadline: 1_000,
    assertHealthy() {},
    sleep: async milliseconds => {
      now += milliseconds;
      successor.evidence.ingest(bookmark("300"));
    },
    now: () => now
  });
  assert.deepEqual(result, [successor]);
  assert.equal(predecessor.stopped, true);
  assert.equal(successor.stopped, false);
  assert.equal(successor.isRunning(), true);
});

test("successor failure during predecessor cleanup blocks adoption for 1/2/3 watches", async () => {
  for (const count of [1, 2, 3]) {
    const predecessors = Array.from({ length: count }, (_, index) =>
      logicalWatch(`p${index}`));
    const successors = Array.from({ length: count }, (_, index) =>
      logicalWatch(`s${index}`));
    predecessors.forEach((watch, index) => watch.evidence.ingest(bookmark(`b${index}`)));
    const originalStop = predecessors[0].stop.bind(predecessors[0]);
    predecessors[0].stop = async () => {
      originalStop();
      successors[count - 1].evidence.violation = true;
    };
    let now = 0;
    let populated = false;
    const result = await replaceWithBookmarkedSuccessors({
      predecessors,
      startSuccessor: (_predecessor, index) => successors[index],
      deadline: 1_000,
      assertHealthy() {},
      sleep: async milliseconds => {
        now += milliseconds;
        if (!populated) {
          successors.forEach((watch, index) => watch.evidence.ingest(bookmark(`c${index}`)));
          populated = true;
        }
      },
      now: () => now
    });
    assert.equal(result, null);
    assert.equal(successors.every(watch => watch.stopped), true);
  }
});

test("process-group cleanup failure rejects the handoff and collects its successor", async () => {
  const predecessor = logicalWatch("p");
  predecessor.evidence.ingest(bookmark("b"));
  predecessor.stop = async () => ({
    cleanupTimedOut: false,
    terminationError: "watch_process_group_kill_error"
  });
  const successor = logicalWatch("s");
  let now = 0;
  await assert.rejects(replaceWithBookmarkedSuccessors({
    predecessors: [predecessor],
    startSuccessor: () => successor,
    deadline: 1_000,
    assertHealthy() {},
    sleep: async milliseconds => {
      now += milliseconds;
      successor.evidence.ingest(bookmark("c"));
    },
    now: () => now
  }), /watch_cleanup_failed/);
  assert.equal(successor.stopped, true);
});

test("a bookmark arriving during successor startup is not lost from the baseline", async () => {
  const predecessor = logicalWatch("200");
  predecessor.evidence.ingest(bookmark("250"));
  const successor = logicalWatch("250");
  let now = 0;
  const result = await replaceWithBookmarkedSuccessors({
    predecessors: [predecessor],
    startSuccessor() {
      successor.evidence.ingest(bookmark("300"));
      return successor;
    },
    deadline: 1_000,
    assertHealthy() {},
    sleep: async milliseconds => { now += milliseconds; },
    now: () => now
  });
  assert.deepEqual(result, [successor]);
  assert.equal(predecessor.stopped, true);
});

test("a three-watch handoff waits for every successor bookmark before adoption", async () => {
  const predecessors = [logicalWatch("p1"), logicalWatch("p2"), logicalWatch("p3")];
  predecessors.forEach((watch, index) => watch.evidence.ingest(bookmark(`b${index + 1}`)));
  const successors = [logicalWatch("b1"), logicalWatch("b2"), logicalWatch("b3")];
  let now = 0;
  let round = 0;
  const result = await replaceWithBookmarkedSuccessors({
    predecessors,
    startSuccessor: (_predecessor, index) => successors[index],
    deadline: 1_000,
    assertHealthy() {},
    sleep: async milliseconds => {
      now += milliseconds;
      successors[round].evidence.ingest(bookmark(`c${round + 1}`));
      round += 1;
      if (round < successors.length) {
        assert.equal(predecessors.some(watch => watch.stopped), false);
      }
    },
    now: () => now
  });
  assert.deepEqual(result, successors);
  assert.equal(round, 3);
  assert.equal(predecessors.every(watch => watch.stopped), true);
  assert.equal(successors.every(watch => watch.isRunning()), true);
});

test("predecessor failure while another successor is pending prevents all adoption", async () => {
  const predecessors = [logicalWatch("p1"), logicalWatch("p2")];
  predecessors.forEach((watch, index) => watch.evidence.ingest(bookmark(`b${index + 1}`)));
  const successors = [logicalWatch("b1"), logicalWatch("b2")];
  let now = 0;
  const result = await replaceWithBookmarkedSuccessors({
    predecessors,
    startSuccessor: (_predecessor, index) => successors[index],
    deadline: 1_000,
    assertHealthy() {},
    sleep: async milliseconds => {
      now += milliseconds;
      successors[0].evidence.ingest(bookmark("c1"));
      predecessors[1].close();
    },
    now: () => now
  });
  assert.equal(result, null);
  assert.equal(predecessors.every(watch => !watch.stopped), true);
  assert.equal(successors.every(watch => watch.stopped), true);
});

test("opaque predecessor and successor bookmarks may be identical without comparison", async () => {
  const predecessor = logicalWatch("opaque-start");
  predecessor.evidence.ingest(bookmark("opaque-same"));
  const successor = logicalWatch("opaque-same");
  let now = 0;
  const result = await replaceWithBookmarkedSuccessors({
    predecessors: [predecessor],
    startSuccessor: () => successor,
    deadline: 1_000,
    assertHealthy() {},
    sleep: async milliseconds => {
      now += milliseconds;
      successor.evidence.ingest(bookmark("opaque-same"));
    },
    now: () => now
  });
  assert.deepEqual(result, [successor]);
});

test("a failed successor is stopped while the predecessor is retained", async () => {
  const predecessor = logicalWatch("200");
  predecessor.evidence.ingest(bookmark("250"));
  const successor = logicalWatch("250");
  let now = 0;
  const result = await replaceWithBookmarkedSuccessors({
    predecessors: [predecessor],
    startSuccessor: () => successor,
    deadline: 1_000,
    assertHealthy() {},
    sleep: async milliseconds => {
      now += milliseconds;
      successor.evidence.ingest(podEvent("ADDED", "251", "racing-runner"));
      successor.evidence.ingest(podEvent("DELETED", "252", "racing-runner"));
      successor.evidence.ingest(bookmark("300"));
    },
    now: () => now
  });
  assert.equal(result, null);
  assert.equal(predecessor.stopped, false);
  assert.equal(successor.stopped, true);
});

test("owned watch startup and acquisition exceptions collect every partial watch set", async () => {
  const partial = [logicalWatch("p1"), logicalWatch("p2")];
  await assert.rejects(withOwnedEvidenceWatches({
    start(watches) {
      watches.push(partial[0]);
      watches.push(partial[1]);
      throw new Error("third_watch_spawn_failed");
    },
    async attempt() {
      assert.fail("the acquisition callback must not run after partial startup failure");
    }
  }), /third_watch_spawn_failed/);
  assert.equal(partial.every(watch => watch.stopped), true);

  const running = [logicalWatch("p1"), logicalWatch("p2"), logicalWatch("p3")];
  await assert.rejects(withOwnedEvidenceWatches({
    start(watches) { watches.push(...running); },
    async attempt() { throw new Error("lease_or_boundary_failed"); }
  }), /lease_or_boundary_failed/);
  assert.equal(running.every(watch => watch.stopped), true);
});

test("cleanup attempts every watch and then propagates any collection failure", async () => {
  const collected = logicalWatch("collected");
  await assert.rejects(stopEvidenceWatches([
    { async stop() { throw new Error("kill_failed"); } },
    collected
  ]), /watch_cleanup_failed/);
  assert.equal(collected.stopped, true);
});

test("owned acquisition releases predecessors without collecting returned successors", async () => {
  const predecessors = [logicalWatch("p1"), logicalWatch("p2"), logicalWatch("p3")];
  const successors = [logicalWatch("s1"), logicalWatch("s2"), logicalWatch("s3")];
  const result = await withOwnedEvidenceWatches({
    start(watches) { watches.push(...predecessors); },
    async attempt() { return successors; }
  });
  assert.deepEqual(result, successors);
  assert.equal(predecessors.every(watch => watch.stopped), true);
  assert.equal(successors.every(watch => !watch.stopped), true);
});
