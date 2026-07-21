const DEFAULT_MAX_BUFFER_BYTES = 4 * 1024 * 1024;

function watchFailed(watch) {
  return Boolean(watch?.evidence?.error || watch?.evidence?.violation);
}

function watchHasCausalBookmarkAfter(watch, sequence) {
  return Number.isSafeInteger(watch?.evidence?.bookmarkSequence) &&
    watch.evidence.bookmarkSequence > sequence &&
    typeof watch.evidence.lastBookmarkResourceVersion === "string" &&
    Boolean(watch.evidence.lastBookmarkResourceVersion) &&
    watch.evidence.lastBookmarkResourceVersion !== "0";
}

function startEvidenceWatchProcess({
  spawnProcess,
  command,
  args,
  evidence,
  serverTimeoutSeconds,
  processGraceSeconds = 10,
  maximumBufferBytes = DEFAULT_MAX_BUFFER_BYTES,
  setTimer = setTimeout,
  clearTimer = clearTimeout,
  killProcessGroup = (pid, signal) => process.kill(-pid, signal)
}) {
  if (
    typeof spawnProcess !== "function" || typeof command !== "string" || !command ||
    !Array.isArray(args) || !evidence || typeof evidence.ingest !== "function" ||
    !Number.isInteger(serverTimeoutSeconds) || serverTimeoutSeconds < 1 ||
    !Number.isInteger(processGraceSeconds) || processGraceSeconds < 1 ||
    !Number.isInteger(maximumBufferBytes) || maximumBufferBytes < 1024 ||
    typeof setTimer !== "function" || typeof clearTimer !== "function" ||
    typeof killProcessGroup !== "function"
  ) {
    throw new Error("watch_process_input_invalid");
  }
  const bookmarkBaseline = evidence.bookmarkSequence;
  if (!Number.isSafeInteger(bookmarkBaseline) || bookmarkBaseline < 0) {
    throw new Error("watch_bookmark_baseline_invalid");
  }
  const requestTimeoutSeconds = serverTimeoutSeconds + processGraceSeconds;
  // The directly executed, trusted kubectl process and its ordinary children
  // form one dedicated POSIX process group. A command that deliberately leaves
  // that group is outside this transport boundary; failure to signal the group
  // is nevertheless propagated and blocks every handoff/action.
  const child = spawnProcess(command, args, {
    detached: true,
    stdio: ["ignore", "pipe", "pipe"]
  });
  let buffer = "";
  let intentionalStop = false;
  let exited = false;
  let closed = false;
  let watchdogExpired = false;
  let gracefulTerminationTimer = null;
  let forcedSettlementTimer = null;
  let finishedSettled = false;
  let terminationError = null;
  let resolveFinished;
  const finished = new Promise(resolve => { resolveFinished = resolve; });
  const clearProcessTimers = () => {
    clearTimer(watchdog);
    if (gracefulTerminationTimer !== null) {
      clearTimer(gracefulTerminationTimer);
      gracefulTerminationTimer = null;
    }
    if (forcedSettlementTimer !== null) {
      clearTimer(forcedSettlementTimer);
      forcedSettlementTimer = null;
    }
  };
  const drainTrailingBuffer = () => {
    if (!buffer.trim() || evidence.error) return;
    try {
      evidence.ingest(JSON.parse(buffer));
    } catch (_error) {
      evidence.error = "watch_json_invalid";
    }
    buffer = "";
  };
  const settleFinished = result => {
    if (finishedSettled) return;
    finishedSettled = true;
    clearProcessTimers();
    resolveFinished(result);
  };
  const terminate = signal => {
    if (closed || finishedSettled) return;
    let deliveredToGroup = false;
    if (Number.isSafeInteger(child.pid) && child.pid > 0) {
      try {
        killProcessGroup(child.pid, signal);
        deliveredToGroup = true;
      } catch (error) {
        terminationError = "watch_process_group_kill_error";
        if (!evidence.error) evidence.error = terminationError;
      }
    }
    if (
      !deliveredToGroup && !exited && child.exitCode === null && child.signalCode === null
    ) {
      try {
        child.kill(signal);
      } catch (_error) {
        terminationError = "watch_process_kill_error";
        if (!evidence.error) evidence.error = terminationError;
      }
    }
  };
  const forceTerminate = () => {
    terminate("SIGKILL");
    child.stdout.destroy();
    child.stderr.destroy();
    if (!closed && !finishedSettled && forcedSettlementTimer === null) {
      forcedSettlementTimer = setTimer(() => {
        forcedSettlementTimer = null;
        if (closed || finishedSettled) return;
        evidence.error = evidence.error || "watch_cleanup_timeout";
        settleFinished({
          code: child.exitCode,
          signal: child.signalCode,
          intentionalStop,
          watchdogExpired,
          terminationError,
          cleanupTimedOut: true
        });
      }, processGraceSeconds * 1_000);
    }
  };
  const terminateGracefully = () => {
    terminate("SIGTERM");
    if (!closed && !finishedSettled && gracefulTerminationTimer === null) {
      gracefulTerminationTimer = setTimer(() => {
        gracefulTerminationTimer = null;
        forceTerminate();
      }, processGraceSeconds * 1_000);
    }
  };
  const watchdog = setTimer(() => {
    watchdogExpired = true;
    if (!intentionalStop && !evidence.error) evidence.error = "watch_process_timeout";
    forceTerminate();
  }, requestTimeoutSeconds * 1_000);
  child.stdout.setEncoding("utf8");
  child.stdout.on("data", chunk => {
    if (evidence.error) return;
    buffer += chunk;
    if (Buffer.byteLength(buffer, "utf8") > maximumBufferBytes) {
      evidence.error = "watch_buffer_oversize";
      terminateGracefully();
      return;
    }
    const lines = buffer.split("\n");
    buffer = lines.pop();
    for (const line of lines) {
      if (!line.trim()) continue;
      try {
        evidence.ingest(JSON.parse(line));
      } catch (_error) {
        evidence.error = "watch_json_invalid";
        terminateGracefully();
        return;
      }
    }
  });
  child.stdout.once("error", () => {
    if (!intentionalStop && !evidence.error) evidence.error = "watch_stdout_error";
    terminateGracefully();
  });
  child.stderr.on("data", () => {});
  child.stderr.once("error", () => {
    if (!intentionalStop && !evidence.error) evidence.error = "watch_stderr_error";
    terminateGracefully();
  });
  child.once("error", () => {
    if (!intentionalStop && !evidence.error) evidence.error = "watch_process_error";
    terminateGracefully();
  });
  child.once("exit", () => { exited = true; });
  child.once("close", (code, signal) => {
    closed = true;
    drainTrailingBuffer();
    if (!intentionalStop && !evidence.error) {
      evidence.error = "watch_ended_before_evidence_boundary";
    }
    settleFinished({
      code,
      signal,
      intentionalStop,
      watchdogExpired,
      terminationError,
      cleanupTimedOut: false
    });
  });
  return {
    bookmarkBaseline,
    evidence,
    finished,
    isRunning() {
      return !exited && !closed && child.exitCode === null && child.signalCode === null;
    },
    stop() {
      intentionalStop = true;
      terminateGracefully();
      return finished;
    }
  };
}

async function stopEvidenceWatches(watches) {
  if (!Array.isArray(watches)) throw new Error("watch_cleanup_input_invalid");
  const results = await Promise.allSettled(watches.map(async watch => {
    if (!watch || typeof watch.stop !== "function") {
      throw new Error("watch_cleanup_handle_invalid");
    }
    const result = await watch.stop();
    if (result?.cleanupTimedOut || result?.terminationError) {
      throw new Error(result?.terminationError || "watch_cleanup_timeout");
    }
  }));
  if (results.some(result => result.status === "rejected")) {
    throw new Error("watch_cleanup_failed");
  }
  return results;
}

async function withOwnedEvidenceWatches({ start, attempt }) {
  if (typeof start !== "function" || typeof attempt !== "function") {
    throw new Error("watch_ownership_input_invalid");
  }
  const watches = [];
  try {
    await start(watches);
    return await attempt(watches);
  } finally {
    await stopEvidenceWatches(watches);
  }
}

async function waitForBookmarkedWatchBoundary({
  predecessors = [],
  successors,
  startingBookmarkSequences,
  deadline,
  assertHealthy,
  sleep,
  now = Date.now
}) {
  if (
    !Array.isArray(predecessors) ||
    !Array.isArray(successors) || !successors.length ||
    !Array.isArray(startingBookmarkSequences) ||
    startingBookmarkSequences.length !== successors.length ||
    startingBookmarkSequences.some(value => !Number.isSafeInteger(value) || value < 0) ||
    typeof deadline !== "number" || typeof assertHealthy !== "function" ||
    typeof sleep !== "function" || typeof now !== "function"
  ) {
    throw new Error("watch_boundary_input_invalid");
  }
  while (now() < deadline) {
    assertHealthy();
    if (
      predecessors.some(watch => !watch.isRunning() || watchFailed(watch)) ||
      successors.some(watch => !watch.isRunning() || watchFailed(watch))
    ) return false;
    if (successors.every((watch, index) =>
      watchHasCausalBookmarkAfter(watch, startingBookmarkSequences[index])
    )) {
      assertHealthy();
      return now() < deadline &&
        predecessors.every(watch => watch.isRunning() && !watchFailed(watch)) &&
        successors.every(watch => watch.isRunning() && !watchFailed(watch));
    }
    await sleep(100);
  }
  successors.forEach((watch, index) => {
    if (
      !watchFailed(watch) &&
      !watchHasCausalBookmarkAfter(watch, startingBookmarkSequences[index])
    ) {
      watch.evidence.error = "watch_bookmark_deadline_exceeded";
    }
  });
  return false;
}

async function replaceWithBookmarkedSuccessors({
  predecessors,
  startSuccessor,
  deadline,
  assertHealthy,
  sleep,
  now = Date.now
}) {
  if (!Array.isArray(predecessors) || !predecessors.length || typeof startSuccessor !== "function") {
    throw new Error("watch_successor_input_invalid");
  }
  const successors = [];
  let accepted = false;
  try {
    for (const [index, predecessor] of predecessors.entries()) {
      successors.push(startSuccessor(predecessor, index));
    }
    const startingBookmarkSequences = successors.map(watch => watch.bookmarkBaseline);
    const safe = await waitForBookmarkedWatchBoundary({
      predecessors,
      successors,
      startingBookmarkSequences,
      deadline,
      assertHealthy,
      sleep,
      now
    });
    if (!safe) return null;
    await stopEvidenceWatches(predecessors);
    assertHealthy();
    if (
      now() >= deadline ||
      successors.some(watch => !watch.isRunning() || watchFailed(watch))
    ) return null;
    accepted = true;
    return successors;
  } finally {
    if (!accepted) await stopEvidenceWatches(successors);
  }
}

module.exports = {
  replaceWithBookmarkedSuccessors,
  startEvidenceWatchProcess,
  stopEvidenceWatches,
  waitForBookmarkedWatchBoundary,
  withOwnedEvidenceWatches
};
