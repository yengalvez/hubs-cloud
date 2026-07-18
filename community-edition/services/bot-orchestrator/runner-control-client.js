const MAX_CONTROL_RESPONSE_BYTES = 32 * 1024;

function validControlConfiguration({ controlUrl, token, podUid, processGeneration }) {
  return controlUrl === "http://bot-orchestrator:5001" &&
    typeof token === "string" &&
    token.startsWith("v1.") &&
    Buffer.byteLength(token, "utf8") <= 2048 &&
    typeof podUid === "string" &&
    /^[A-Za-z0-9_.:-]{1,128}$/.test(podUid) &&
    typeof processGeneration === "string" &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
      processGeneration
    );
}

async function readBoundedJson(response) {
  const contentLength = Number(response?.headers?.get?.("content-length"));
  if (Number.isFinite(contentLength) && contentLength > MAX_CONTROL_RESPONSE_BYTES) {
    throw new Error("runner_control_response_too_large");
  }
  let text;
  if (response?.body && typeof response.body.getReader === "function") {
    const reader = response.body.getReader();
    const chunks = [];
    let totalBytes = 0;
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      const chunk = Buffer.from(value);
      totalBytes += chunk.length;
      if (totalBytes > MAX_CONTROL_RESPONSE_BYTES) {
        await reader.cancel().catch(() => {});
        throw new Error("runner_control_response_too_large");
      }
      chunks.push(chunk);
    }
    text = Buffer.concat(chunks, totalBytes).toString("utf8");
  } else {
    text = await response.text();
  }
  if (Buffer.byteLength(text, "utf8") > MAX_CONTROL_RESPONSE_BYTES) {
    throw new Error("runner_control_response_too_large");
  }
  try {
    return text ? JSON.parse(text) : null;
  } catch (_error) {
    throw new Error("runner_control_invalid_json");
  }
}

function createRunnerControlClient({
  controlUrl,
  token,
  podUid,
  processGeneration,
  fetchImpl = global.fetch,
  timeoutMs = 4_000
}) {
  if (!validControlConfiguration({ controlUrl, token, podUid, processGeneration })) {
    throw new Error("runner_control_configuration_invalid");
  }
  const requestTimeoutMs = Math.min(Math.max(Number(timeoutMs) || 4_000, 500), 10_000);
  const headers = {
    authorization: `Bearer ${token}`,
    "x-yenhubs-runner-pod-uid": podUid
  };

  async function request(path, options = {}, { readJson = false } = {}) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), requestTimeoutMs);
    try {
      const response = await fetchImpl(`${controlUrl}${path}`, {
        ...options,
        cache: "no-store",
        redirect: "error",
        headers: { ...(options.headers || {}), ...headers },
        signal: controller.signal
      });
      if (!response || response.ok !== true) {
        throw new Error(`runner_control_status_${Number(response?.status) || 0}`);
      }
      return readJson ? await readBoundedJson(response) : response;
    } finally {
      clearTimeout(timer);
    }
  }

  return {
    async fetchConfig() {
      const payload = await request("/internal/runner/v1/config", {}, { readJson: true });
      if (
        !payload ||
        payload.ok !== true ||
        payload.process_generation !== processGeneration ||
        (payload.message !== null &&
          (!payload.message || typeof payload.message !== "object" || Array.isArray(payload.message)))
      ) {
        throw new Error("runner_control_config_invalid");
      }
      return payload.message;
    },

    async publishStatus(message) {
      if (
        !message ||
        typeof message !== "object" ||
        Array.isArray(message) ||
        message.processGeneration !== processGeneration
      ) {
        throw new Error("runner_control_status_invalid");
      }
      const body = JSON.stringify({ message });
      if (Buffer.byteLength(body, "utf8") > MAX_CONTROL_RESPONSE_BYTES) {
        throw new Error("runner_control_status_too_large");
      }
      await request("/internal/runner/v1/status", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body
      });
      return true;
    }
  };
}

module.exports = {
  MAX_CONTROL_RESPONSE_BYTES,
  createRunnerControlClient,
  readBoundedJson,
  validControlConfiguration
};
