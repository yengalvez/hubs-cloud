const crypto = require("node:crypto");
const fs = require("node:fs");
const https = require("node:https");
const { EventEmitter } = require("node:events");
const { isDeepStrictEqual } = require("node:util");

const MANAGED_BY_LABEL = "yenhubs.org/managed-by";
const ROOM_KEY_LABEL = "yenhubs.org/room-key";
const GENERATION_LABEL = "yenhubs.org/generation";
const RUNNER_PROTOCOL_LABEL = "yenhubs.org/runner-protocol";
const RUNNER_PROTOCOL_VALUE = "durable-fence-v2";
const EXPIRES_AT_ANNOTATION = "yenhubs.org/expires-at";
const PARENT_NAMESPACE_ANNOTATION = "yenhubs.org/parent-namespace";
const PARENT_NAME_ANNOTATION = "yenhubs.org/parent-name";
const PARENT_UID_ANNOTATION = "yenhubs.org/parent-uid";
const INTENT_STATE_ANNOTATION = "yenhubs.org/intent-state";
const MANAGED_BY_VALUE = "bot-orchestrator";
const RUNNER_APP_LABEL = "bot-runner";
const INTENT_APP_LABEL = "bot-runner-intent";
const FENCE_APP_LABEL = "bot-runner-fence";
const RUNNER_NAMESPACE = "hcce-bot-runners";
const GUARD_SCHEDULER_NAME = "yenhubs-guard-scheduler";
const GUARD_SCHEDULING_GATE = "yenhubs.org/guard";
const GUARD_IMAGE = `registry.invalid/yenhubs/non-executable-guard@sha256:${"0".repeat(64)}`;
const MAX_GUARD_PODS = 100;
const MIN_GUARD_FENCE_RESERVE = 20;
const MAX_GUARD_START_COUNT = MAX_GUARD_PODS - MIN_GUARD_FENCE_RESERVE;
const GUARD_CAPACITY_WARNING_THRESHOLD = 60;
const MAX_API_RESPONSE_BYTES = 1024 * 1024;
const DEFAULT_CREATE_SERVER_TIMEOUT_SECONDS = 5;
const MAX_CREATE_SERVER_TIMEOUT_SECONDS = 30;

function encodePathSegment(value) {
  return encodeURIComponent(String(value));
}

function readRequiredFile(path, label) {
  const value = fs.readFileSync(path, "utf8").trim();
  if (!value) throw new Error(`${label}_missing`);
  return value;
}

class KubernetesApi {
  constructor({
    host = process.env.KUBERNETES_SERVICE_HOST,
    port = process.env.KUBERNETES_SERVICE_PORT_HTTPS || process.env.KUBERNETES_SERVICE_PORT || "443",
    tokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token",
    caPath = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
    requestTimeoutMs = 10_000,
    requestImpl = https.request,
    scheduleTimeout = setTimeout,
    cancelTimeout = clearTimeout
  } = {}) {
    if (typeof host !== "string" || !host) throw new Error("kubernetes_service_host_missing");
    this.host = host;
    this.port = Number(port);
    if (!Number.isInteger(this.port) || this.port < 1 || this.port > 65_535) {
      throw new Error("kubernetes_service_port_invalid");
    }
    this.tokenPath = tokenPath;
    this.ca = fs.readFileSync(caPath);
    this.requestTimeoutMs = Math.min(Math.max(Number(requestTimeoutMs) || 10_000, 1000), 30_000);
    this.requestImpl = requestImpl;
    this.scheduleTimeout = scheduleTimeout;
    this.cancelTimeout = cancelTimeout;
  }

  request(method, path, body = null, { contentType = "application/json" } = {}) {
    return new Promise((resolve, reject) => {
      let request;
      let deadline;
      let settled = false;
      const settle = (callback, value) => {
        if (settled) return;
        settled = true;
        if (deadline !== undefined) this.cancelTimeout(deadline);
        callback(value);
      };
      const fail = error => settle(reject, error);
      let token;
      try {
        // Projected ServiceAccount tokens rotate. Re-read for each bounded API
        // request so a healthy long-running parent does not retain an expired
        // bearer credential.
        token = readRequiredFile(this.tokenPath, "kubernetes_serviceaccount_token");
      } catch (error) {
        fail(error);
        return;
      }
      const serialized = body === null ? null : JSON.stringify(body);
      request = this.requestImpl(
        {
          protocol: "https:",
          host: this.host,
          port: this.port,
          method,
          path,
          ca: this.ca,
          servername: "kubernetes.default.svc",
          headers: {
            accept: "application/json",
            authorization: `Bearer ${token}`,
            ...(serialized === null
              ? {}
              : {
                  "content-type": contentType,
                  "content-length": Buffer.byteLength(serialized)
                })
          }
        },
        response => {
          const chunks = [];
          let totalBytes = 0;
          response.once("error", () => fail(new Error("kubernetes_response_error")));
          response.once("aborted", () => fail(new Error("kubernetes_response_aborted")));
          response.on("data", chunk => {
            if (settled) return;
            totalBytes += chunk.length;
            if (totalBytes > MAX_API_RESPONSE_BYTES) {
              const error = new Error("kubernetes_response_too_large");
              fail(error);
              request.destroy(error);
              return;
            }
            chunks.push(chunk);
          });
          response.on("end", () => {
            if (settled) return;
            const text = Buffer.concat(chunks, totalBytes).toString("utf8");
            let payload = null;
            if (text) {
              try {
                payload = JSON.parse(text);
              } catch (_error) {
                fail(new Error("kubernetes_invalid_json_response"));
                return;
              }
            }
            const status = Number(response.statusCode) || 0;
            if (status < 200 || status >= 300) {
              const error = new Error(`kubernetes_status_${status}`);
              error.status = status;
              error.payload = payload;
              fail(error);
              return;
            }
            settle(resolve, payload);
          });
        }
      );
      deadline = this.scheduleTimeout(() => {
        const error = new Error("kubernetes_request_timeout");
        fail(error);
        request.destroy(error);
      }, this.requestTimeoutMs);
      request.once("error", fail);
      if (serialized !== null) request.write(serialized);
      request.end();
    });
  }
}

function validUuid(value) {
  return typeof value === "string" &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
}

function strongCredentialKey(value) {
  return typeof value === "string" && Buffer.byteLength(value, "utf8") >= 32;
}

function validHttpsBaseUrl(value) {
  try {
    const url = new URL(value);
    return url.protocol === "https:" &&
      !url.username &&
      !url.password &&
      !url.search &&
      !url.hash &&
      url.pathname === "/";
  } catch (_error) {
    return false;
  }
}

function roomKey(hubSid, key) {
  return crypto.createHmac("sha256", key).update(hubSid).digest("hex").slice(0, 20);
}

function runnerPodName(roomDigest, generation) {
  if (!/^[0-9a-f]{20}$/.test(roomDigest) || !validUuid(generation)) {
    throw new Error("runner_pod_identity_invalid");
  }
  return `bot-runner-${roomDigest.slice(0, 16)}-${generation.replaceAll("-", "").slice(0, 8)}`;
}

function intentPodName(targetName) {
  if (typeof targetName !== "string" || !/^bot-runner-[0-9a-f]{16}-[0-9a-f]{8}$/.test(targetName)) {
    throw new Error("runner_intent_target_invalid");
  }
  return `bot-intent-${targetName}`;
}

function podIsReady(pod) {
  const statuses = pod?.status?.containerStatuses;
  return pod?.status?.phase === "Running" &&
    Array.isArray(statuses) &&
    statuses.length === 1 &&
    statuses[0]?.name === "bot-runner" &&
    statuses[0]?.ready === true;
}

function requirePodList(response) {
  if (
    !response ||
    typeof response !== "object" ||
    Array.isArray(response) ||
    response.kind !== "PodList" ||
    !Array.isArray(response.items)
  ) {
    throw new Error("runner_pod_list_invalid");
  }
  return response.items;
}

function requireCompletePodList(response) {
  const items = requirePodList(response);
  const metadata = response.metadata;
  if (
    !metadata ||
    typeof metadata !== "object" ||
    Array.isArray(metadata) ||
    typeof metadata.resourceVersion !== "string" ||
    metadata.resourceVersion.length < 1 ||
    metadata.resourceVersion.length > 256 ||
    /[\u0000-\u001f\u007f]/u.test(metadata.resourceVersion) ||
    (Object.hasOwn(metadata, "continue") && metadata.continue !== "") ||
    (Object.hasOwn(metadata, "remainingItemCount") && metadata.remainingItemCount !== 0)
  ) {
    throw new Error("runner_pod_list_incomplete");
  }
  return { items, resourceVersion: metadata.resourceVersion };
}

function podIsTerminal(pod) {
  return pod?.status?.phase === "Failed" || pod?.status?.phase === "Succeeded";
}

function exactJsonValue(actual, expected) {
  return isDeepStrictEqual(actual, expected);
}

function defaultFalse(value) {
  return value === undefined || value === false;
}

function emptyOrAbsentArray(value) {
  return value === undefined || (Array.isArray(value) && value.length === 0);
}

function emptyOrAbsentObject(value) {
  return value === undefined || (
    value && typeof value === "object" && !Array.isArray(value) && Object.keys(value).length === 0
  );
}

function exactDefaultNoExecuteTolerations(value) {
  if (value === undefined || (Array.isArray(value) && value.length === 0)) return true;
  if (!Array.isArray(value) || value.length !== 2) return false;
  const expectedKeys = new Set([
    "node.kubernetes.io/not-ready",
    "node.kubernetes.io/unreachable"
  ]);
  for (const toleration of value) {
    if (!toleration || typeof toleration !== "object" || Array.isArray(toleration)) return false;
    if (!exactJsonValue(Object.keys(toleration).sort(), ["effect", "key", "operator", "tolerationSeconds"])) {
      return false;
    }
    if (
      toleration.operator !== "Exists" ||
      toleration.effect !== "NoExecute" ||
      toleration.tolerationSeconds !== 300 ||
      !expectedKeys.delete(toleration.key)
    ) {
      return false;
    }
  }
  return expectedKeys.size === 0;
}

class RunnerPodHandle extends EventEmitter {
  constructor(manager, identity) {
    super();
    this.manager = manager;
    this.name = identity.name;
    this.hubSid = identity.hubSid;
    this.roomKey = identity.roomKey;
    this.processGeneration = identity.processGeneration;
    this.token = identity.token;
    this.expiresAtSeconds = identity.expiresAtSeconds;
    this.pid = 1;
    this.connected = true;
    this.podUid = null;
    this.podReady = false;
    this.pendingMessage = null;
    this.finished = false;
    this.createSettled = false;
    this.deleteRequested = false;
    this.deleteSignal = null;
    this.deleteGracePeriodSeconds = null;
    this.errorReported = false;
  }

  send(message, callback = () => {}) {
    if (this.finished || !this.connected) {
      queueMicrotask(() => callback(new Error("runner_control_channel_closed")));
      return false;
    }
    this.pendingMessage = JSON.parse(JSON.stringify(message));
    queueMicrotask(() => callback(null));
    return true;
  }

  kill(signal = "SIGTERM") {
    if (this.finished) return false;
    const normalizedSignal = signal === "SIGKILL" ? "SIGKILL" : "SIGTERM";
    if (this.deleteRequested && normalizedSignal !== "SIGKILL") return true;
    this.deleteRequested = true;
    this.deleteSignal = normalizedSignal === "SIGKILL" ? "SIGKILL" : (this.deleteSignal || "SIGTERM");
    this.podReady = false;
    this.manager.deleteHandle(this, normalizedSignal).catch(error => {
      this.manager.reportHandleError(this, error?.message || "runner_pod_delete_failed");
    });
    return true;
  }

  finish(exitCode = 0) {
    if (this.finished) return;
    this.finished = true;
    this.connected = false;
    this.podReady = false;
    this.emit("disconnect");
    this.emit("exit", exitCode, null);
    this.emit("close", exitCode, null);
  }
}

class KubernetesRunnerManager extends EventEmitter {
  constructor({
    api = new KubernetesApi(),
    namespace,
    parentNamespace,
    ownerPodName,
    ownerPodUid,
    runnerImage,
    hubsBaseUrl,
    controlUrl,
    credentialKey,
    runnerEnvironment = {},
    tokenFactory,
    tokenTtlSeconds = 3600,
    maxActiveRooms = 5,
    now = () => Date.now(),
    createServerTimeoutSeconds = DEFAULT_CREATE_SERVER_TIMEOUT_SECONDS,
    sleep = milliseconds => new Promise(resolve => setTimeout(resolve, milliseconds))
  }) {
    super();
    if (
      namespace !== RUNNER_NAMESPACE ||
      !/^[a-z0-9]([-a-z0-9]*[a-z0-9])?$/.test(parentNamespace || "") ||
      !/^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$/.test(ownerPodName || "") ||
      !/^[A-Za-z0-9_.:-]{1,128}$/.test(ownerPodUid || "") ||
      !/^.+@sha256:[0-9a-f]{64}$/.test(runnerImage || "") ||
      !strongCredentialKey(credentialKey) ||
      !validHttpsBaseUrl(hubsBaseUrl) ||
      controlUrl !== `http://bot-orchestrator.${parentNamespace}.svc.cluster.local:5001`
    ) {
      throw new Error("runner_pod_manager_configuration_missing");
    }
    if (typeof tokenFactory !== "function") throw new Error("runner_token_factory_missing");
    if (typeof sleep !== "function") throw new Error("runner_sleep_invalid");
    if (
      !Number.isInteger(createServerTimeoutSeconds) ||
      createServerTimeoutSeconds < 1 ||
      createServerTimeoutSeconds > MAX_CREATE_SERVER_TIMEOUT_SECONDS
    ) {
      throw new Error("runner_create_server_timeout_invalid");
    }
    const apiRequestTimeoutMs = Number(api?.requestTimeoutMs ?? 10_000);
    if (
      !Number.isInteger(apiRequestTimeoutMs) ||
      apiRequestTimeoutMs < 1_000 ||
      apiRequestTimeoutMs > 30_000
    ) {
      throw new Error("runner_api_request_timeout_invalid");
    }
    const environmentEntries = Object.entries(runnerEnvironment);
    const forbiddenEnvironmentName = /(KEY|TOKEN|SECRET|PASSWORD|CREDENTIAL)/;
    if (
      environmentEntries.length > 64 ||
      environmentEntries.some(([name, value]) =>
        !/^[A-Z][A-Z0-9_]{0,127}$/.test(name) ||
        forbiddenEnvironmentName.test(name) ||
        typeof value !== "string" ||
        Buffer.byteLength(value, "utf8") > 16_384
      )
    ) {
      throw new Error("runner_pod_environment_invalid");
    }
    this.api = api;
    this.namespace = namespace;
    this.parentNamespace = parentNamespace;
    this.ownerPodName = ownerPodName;
    this.ownerPodUid = ownerPodUid;
    this.runnerImage = runnerImage;
    this.hubsBaseUrl = hubsBaseUrl;
    this.controlUrl = controlUrl.replace(/\/+$/, "");
    this.credentialKey = credentialKey;
    this.runnerEnvironment = { ...runnerEnvironment };
    this.tokenFactory = tokenFactory;
    this.tokenTtlSeconds = Math.min(Math.max(Number(tokenTtlSeconds) || 3600, 300), 3_600);
    this.maxActiveRooms = Math.min(Math.max(Number(maxActiveRooms) || 5, 1), 10);
    this.now = now;
    this.createServerTimeoutSeconds = createServerTimeoutSeconds;
    this.sleep = sleep;
    this.handles = new Map();
    this.unknownPods = new Map();
    this.createIntents = new Map();
    this.fences = new Map();
    this.guardCapacityState = Object.freeze({
      observed: false,
      intents: 0,
      fences: 0,
      total: 0,
      warning: false,
      warning_threshold: GUARD_CAPACITY_WARNING_THRESHOLD,
      start_limit: MAX_GUARD_START_COUNT,
      reserve: MIN_GUARD_FENCE_RESERVE,
      quota: MAX_GUARD_PODS
    });
    this.operationTail = Promise.resolve();
  }

  guardCapacitySnapshot() {
    return { ...this.guardCapacityState };
  }

  updateGuardCapacitySnapshot(intents, fences) {
    if (!Number.isInteger(intents) || intents < 0 || !Number.isInteger(fences) || fences < 0) {
      throw new Error("runner_guard_capacity_snapshot_invalid");
    }
    const total = intents + fences;
    const warning = total >= GUARD_CAPACITY_WARNING_THRESHOLD;
    const previousWarning = this.guardCapacityState.warning;
    this.guardCapacityState = Object.freeze({
      observed: true,
      intents,
      fences,
      total,
      warning,
      warning_threshold: GUARD_CAPACITY_WARNING_THRESHOLD,
      start_limit: MAX_GUARD_START_COUNT,
      reserve: MIN_GUARD_FENCE_RESERVE,
      quota: MAX_GUARD_PODS
    });
    if (warning && !previousWarning) {
      this.emit("guard-capacity-warning", this.guardCapacitySnapshot());
    }
    return this.guardCapacitySnapshot();
  }

  podsPath(suffix = "") {
    return `/api/v1/namespaces/${encodePathSegment(this.namespace)}/pods${suffix}`;
  }

  createPath() {
    // This deadline only bounds an individual API call. A proposal already
    // accepted by the API server may still become visible after the client
    // loses its response, so neither this query nor the socket timeout is ever
    // used as terminal absence proof. Durable intent/fence Pods provide that.
    return this.podsPath(`?timeout=${this.createServerTimeoutSeconds}s`);
  }

  identity(hubSid, processGeneration) {
    if (typeof hubSid !== "string" || !/^[A-Za-z0-9_-]{1,64}$/.test(hubSid)) {
      throw new Error("runner_pod_hub_invalid");
    }
    const digest = roomKey(hubSid, this.credentialKey);
    const expiresAtSeconds = Math.floor(this.now() / 1000) + this.tokenTtlSeconds;
    return {
      hubSid,
      processGeneration,
      roomKey: digest,
      name: runnerPodName(digest, processGeneration),
      expiresAtSeconds,
      token: this.tokenFactory({
        hubSid,
        processGeneration,
        holderId: this.ownerPodUid,
        expiresAtSeconds
      })
    };
  }

  podDocument(identity) {
    const environment = Object.entries(this.runnerEnvironment).map(([name, value]) => ({
      name,
      value: String(value)
    }));
    environment.push(
      { name: "BOT_RUNNER_GENERATION_TOKEN", value: identity.token },
      { name: "RUNNER_PROCESS_GENERATION", value: identity.processGeneration },
      { name: "RUNNER_LEASE_HOLDER_ID", value: this.ownerPodUid },
      { name: "RUNNER_CONTROL_URL", value: this.controlUrl },
      {
        name: "RUNNER_POD_UID",
        valueFrom: { fieldRef: { apiVersion: "v1", fieldPath: "metadata.uid" } }
      }
    );

    return {
      apiVersion: "v1",
      kind: "Pod",
      metadata: {
        name: identity.name,
        namespace: this.namespace,
        labels: {
          app: RUNNER_APP_LABEL,
          [MANAGED_BY_LABEL]: MANAGED_BY_VALUE,
          [RUNNER_PROTOCOL_LABEL]: RUNNER_PROTOCOL_VALUE,
          [ROOM_KEY_LABEL]: identity.roomKey,
          [GENERATION_LABEL]: identity.processGeneration
        },
        annotations: {
          [EXPIRES_AT_ANNOTATION]: new Date(identity.expiresAtSeconds * 1000).toISOString(),
          [PARENT_NAMESPACE_ANNOTATION]: this.parentNamespace,
          [PARENT_NAME_ANNOTATION]: this.ownerPodName,
          [PARENT_UID_ANNOTATION]: this.ownerPodUid
        }
      },
      spec: {
        serviceAccountName: "bot-runner",
        automountServiceAccountToken: false,
        imagePullSecrets: [{ name: "bot-images-pull" }],
        enableServiceLinks: false,
        restartPolicy: "Never",
        terminationGracePeriodSeconds: 10,
        activeDeadlineSeconds: this.tokenTtlSeconds,
        hostNetwork: false,
        hostPID: false,
        hostIPC: false,
        shareProcessNamespace: false,
        securityContext: {
          runAsNonRoot: true,
          runAsUser: 10001,
          runAsGroup: 10001,
          fsGroup: 10001,
          seccompProfile: { type: "RuntimeDefault" },
          appArmorProfile: { type: "RuntimeDefault" }
        },
        containers: [
          {
            name: "bot-runner",
            image: this.runnerImage,
            imagePullPolicy: "Always",
            command: ["node", "/app/run-ghost-runner.js"],
            args: ["--url", this.hubsBaseUrl, "--room", identity.hubSid, "--runner"],
            env: environment,
            securityContext: {
              runAsNonRoot: true,
              runAsUser: 10001,
              runAsGroup: 10001,
              allowPrivilegeEscalation: false,
              readOnlyRootFilesystem: true,
              capabilities: { drop: ["ALL"] },
              seccompProfile: { type: "RuntimeDefault" },
              appArmorProfile: { type: "RuntimeDefault" }
            },
            resources: {
              requests: { cpu: "25m", memory: "128Mi" },
              limits: { cpu: "500m", memory: "512Mi" }
            },
            volumeMounts: [{ name: "runner-tmp", mountPath: "/tmp" }],
            readinessProbe: {
              exec: { command: ["test", "-f", "/tmp/runner-ready"] },
              initialDelaySeconds: 5,
              periodSeconds: 5,
              timeoutSeconds: 2,
              successThreshold: 1,
              failureThreshold: 2
            }
          }
        ],
        volumes: [{ name: "runner-tmp", emptyDir: { sizeLimit: "64Mi" } }]
      }
    };
  }

  guardPodDocument(identity, type) {
    if (type !== "intent" && type !== "fence") throw new Error("runner_guard_type_invalid");
    const targetName = identity.name;
    const labels = {
      app: type === "intent" ? INTENT_APP_LABEL : FENCE_APP_LABEL,
      [MANAGED_BY_LABEL]: MANAGED_BY_VALUE,
      [RUNNER_PROTOCOL_LABEL]: RUNNER_PROTOCOL_VALUE,
      [ROOM_KEY_LABEL]: identity.roomKey,
      [GENERATION_LABEL]: identity.processGeneration
    };
    return {
      apiVersion: "v1",
      kind: "Pod",
      metadata: {
        name: type === "intent" ? intentPodName(targetName) : targetName,
        namespace: this.namespace,
        labels,
        ...(type === "intent"
          ? { annotations: { [INTENT_STATE_ANNOTATION]: "unarmed" } }
          : {})
      },
      spec: {
        serviceAccountName: "bot-runner-guard",
        automountServiceAccountToken: false,
        imagePullSecrets: [],
        enableServiceLinks: false,
        restartPolicy: "Never",
        terminationGracePeriodSeconds: 0,
        hostNetwork: false,
        hostPID: false,
        hostIPC: false,
        shareProcessNamespace: false,
        dnsPolicy: "ClusterFirst",
        schedulerName: GUARD_SCHEDULER_NAME,
        schedulingGates: [{ name: GUARD_SCHEDULING_GATE }],
        securityContext: {
          runAsNonRoot: true,
          runAsUser: 10001,
          runAsGroup: 10001,
          fsGroup: 10001,
          seccompProfile: { type: "RuntimeDefault" },
          appArmorProfile: { type: "RuntimeDefault" }
        },
        containers: [
          {
            name: "bot-runner-guard",
            image: GUARD_IMAGE,
            imagePullPolicy: "Never",
            command: ["/bin/false"],
            args: [],
            env: [],
            securityContext: {
              runAsNonRoot: true,
              runAsUser: 10001,
              runAsGroup: 10001,
              allowPrivilegeEscalation: false,
              readOnlyRootFilesystem: true,
              capabilities: { drop: ["ALL"] },
              seccompProfile: { type: "RuntimeDefault" },
              appArmorProfile: { type: "RuntimeDefault" }
            },
            resources: {},
            volumeMounts: [],
            terminationMessagePath: "/dev/termination-log",
            terminationMessagePolicy: "File"
          }
        ],
        volumes: []
      }
    };
  }

  guardRecordFromPod(pod, expectedType = null) {
    const labels = pod?.metadata?.labels;
    const app = labels?.app;
    const type = app === INTENT_APP_LABEL
      ? "intent"
      : app === FENCE_APP_LABEL
        ? "fence"
        : null;
    if (!type || (expectedType && type !== expectedType)) {
      throw new Error("runner_guard_type_invalid");
    }
    const roomDigest = labels?.[ROOM_KEY_LABEL];
    const processGeneration = labels?.[GENERATION_LABEL];
    const targetName = runnerPodName(roomDigest, processGeneration);
    const record = {
      type,
      roomKey: roomDigest,
      processGeneration,
      name: targetName,
      intentName: intentPodName(targetName),
      uid: pod?.metadata?.uid,
      resourceVersion: pod?.metadata?.resourceVersion,
      state: type === "intent" ? pod?.metadata?.annotations?.[INTENT_STATE_ANNOTATION] : "fenced"
    };
    if (!this.guardPodMatchesRecord(pod, record, type)) {
      throw new Error("runner_guard_contract_invalid");
    }
    return record;
  }

  guardPodMatchesRecord(pod, record, type) {
    if (!record || (type !== "intent" && type !== "fence")) return false;
    const expected = this.guardPodDocument(record, type);
    const spec = pod?.spec;
    const container = Array.isArray(spec?.containers) && spec.containers.length === 1
      ? spec.containers[0]
      : null;
    const expectedContainer = expected.spec.containers[0];
    const expectedName = type === "intent" ? record.intentName : record.name;
    return pod?.apiVersion === "v1" &&
      pod?.kind === "Pod" &&
      pod?.metadata?.name === expectedName &&
      pod.metadata.namespace === this.namespace &&
      typeof pod.metadata.uid === "string" &&
      pod.metadata.uid.length > 0 &&
      typeof pod.metadata.resourceVersion === "string" &&
      pod.metadata.resourceVersion.length > 0 &&
      exactJsonValue(pod.metadata.labels, expected.metadata.labels) &&
      (type === "intent"
        ? (record.state === "unarmed" || record.state === "armed") &&
          exactJsonValue(pod.metadata.annotations, { [INTENT_STATE_ANNOTATION]: record.state })
        : emptyOrAbsentObject(pod.metadata.annotations)) &&
      !Object.hasOwn(pod.metadata, "ownerReferences") &&
      !Object.hasOwn(pod.metadata, "finalizers") &&
      !Object.hasOwn(pod.metadata, "deletionTimestamp") &&
      !Object.hasOwn(pod.metadata, "deletionGracePeriodSeconds") &&
      (type === "fence"
        ? (pod?.status?.phase === undefined || pod.status.phase === "Pending")
        : pod?.status?.phase !== "Succeeded" && pod?.status?.phase !== "Failed") &&
      spec?.serviceAccountName === "bot-runner-guard" &&
      (!Object.hasOwn(spec, "serviceAccount") || spec.serviceAccount === "bot-runner-guard") &&
      spec.automountServiceAccountToken === false &&
      emptyOrAbsentArray(spec.imagePullSecrets) &&
      spec.enableServiceLinks === false &&
      spec.restartPolicy === "Never" &&
      spec.terminationGracePeriodSeconds === 0 &&
      defaultFalse(spec.hostNetwork) &&
      defaultFalse(spec.hostPID) &&
      defaultFalse(spec.hostIPC) &&
      defaultFalse(spec.shareProcessNamespace) &&
      spec.dnsPolicy === "ClusterFirst" &&
      spec.schedulerName === GUARD_SCHEDULER_NAME &&
      exactJsonValue(spec.schedulingGates, expected.spec.schedulingGates) &&
      exactJsonValue(spec.securityContext, expected.spec.securityContext) &&
      !Object.hasOwn(spec, "activeDeadlineSeconds") &&
      !Object.hasOwn(spec, "affinity") &&
      !Object.hasOwn(spec, "dnsConfig") &&
      emptyOrAbsentArray(spec.hostAliases) &&
      !Object.hasOwn(spec, "hostname") &&
      !Object.hasOwn(spec, "hostnameOverride") &&
      !Object.hasOwn(spec, "hostUsers") &&
      !Object.hasOwn(spec, "nodeName") &&
      emptyOrAbsentObject(spec.nodeSelector) &&
      !Object.hasOwn(spec, "os") &&
      !Object.hasOwn(spec, "overhead") &&
      (!Object.hasOwn(spec, "priority") || spec.priority === 0) &&
      (!Object.hasOwn(spec, "preemptionPolicy") || spec.preemptionPolicy === "PreemptLowerPriority") &&
      (!Object.hasOwn(spec, "priorityClassName") || spec.priorityClassName === "") &&
      emptyOrAbsentArray(spec.readinessGates) &&
      emptyOrAbsentArray(spec.resourceClaims) &&
      !Object.hasOwn(spec, "resources") &&
      !Object.hasOwn(spec, "runtimeClassName") &&
      !Object.hasOwn(spec, "setHostnameAsFQDN") &&
      !Object.hasOwn(spec, "subdomain") &&
      emptyOrAbsentArray(spec.topologySpreadConstraints) &&
      exactDefaultNoExecuteTolerations(spec.tolerations) &&
      emptyOrAbsentArray(spec.initContainers) &&
      emptyOrAbsentArray(spec.ephemeralContainers) &&
      emptyOrAbsentArray(spec.volumes) &&
      container?.name === "bot-runner-guard" &&
      container.image === GUARD_IMAGE &&
      container.imagePullPolicy === "Never" &&
      exactJsonValue(container.command, expectedContainer.command) &&
      emptyOrAbsentArray(container.args) &&
      emptyOrAbsentArray(container.env) &&
      exactJsonValue(container.securityContext, expectedContainer.securityContext) &&
      emptyOrAbsentObject(container.resources) &&
      emptyOrAbsentArray(container.volumeMounts) &&
      container.terminationMessagePath === "/dev/termination-log" &&
      container.terminationMessagePolicy === "File" &&
      !Object.hasOwn(container, "envFrom") &&
      !Object.hasOwn(container, "readinessProbe") &&
      !Object.hasOwn(container, "livenessProbe") &&
      !Object.hasOwn(container, "startupProbe") &&
      !Object.hasOwn(container, "lifecycle") &&
      !Object.hasOwn(container, "resizePolicy") &&
      !Object.hasOwn(container, "restartPolicy") &&
      !Object.hasOwn(container, "restartPolicyRules") &&
      defaultFalse(container.stdin) &&
      defaultFalse(container.stdinOnce) &&
      defaultFalse(container.tty) &&
      emptyOrAbsentArray(container.volumeDevices) &&
      !Object.hasOwn(container, "workingDir") &&
      emptyOrAbsentArray(container.ports);
  }

  create(hubSid, processGeneration) {
    const identity = this.identity(hubSid, processGeneration);
    const trackedNames = new Set([
      ...this.handles.keys(),
      ...this.unknownPods.keys()
    ]);
    if (trackedNames.size >= this.maxActiveRooms) return null;
    if (this.handles.has(identity.name)) return null;
    const handle = new RunnerPodHandle(this, identity);
    this.handles.set(handle.name, handle);
    const intent = {
      hubSid: handle.hubSid,
      name: handle.name,
      intentName: intentPodName(handle.name),
      roomKey: handle.roomKey,
      processGeneration: handle.processGeneration,
      uid: null,
      state: "persisting",
      runnerPostIssued: false,
      runnerCreateConfirmed: false
    };
    this.createIntents.set(handle.name, intent);
    this.persistIntentAndCreate(handle, intent).catch(error => {
      this.reportFatal(handle, error?.message || "runner_intent_create_failed");
    });
    return handle;
  }

  async persistIntentAndCreate(handle, intent) {
    const before = await this.completeManagedInventory();
    const guardCountBefore = before.intents.size + before.fences.size;
    const unfencedIntentCount = Array.from(before.intents.keys())
      .filter(name => !before.fences.has(name)).length;
    if (
      guardCountBefore >= MAX_GUARD_START_COUNT ||
      before.runners.length + unfencedIntentCount >= this.maxActiveRooms
    ) {
      this.createIntents.delete(handle.name);
      handle.createSettled = true;
      this.failHandleConfirmedAbsent(handle, "runner_guard_capacity_reserved");
      return;
    }
    const document = this.guardPodDocument(intent, "intent");
    try {
      await this.api.request("POST", this.createPath(), document);
    } catch (_error) {
      // The exact GET below is the durable observation. A POST response (or
      // timeout) alone is intentionally never enough to arm runner creation.
    }
    let persisted;
    try {
      persisted = await this.api.request(
        "GET",
        this.podsPath(`/${encodePathSegment(intent.intentName)}`)
      );
    } catch (readError) {
      intent.state = "persist_ambiguous";
      this.reportHandleError(
        handle,
        readError?.status === 404
          ? "runner_intent_create_state_ambiguous"
          : "runner_intent_state_unverifiable"
      );
      return;
    }
    const observed = this.guardRecordFromPod(persisted, "intent");
    if (
      observed.name !== intent.name ||
      observed.roomKey !== intent.roomKey ||
      observed.processGeneration !== intent.processGeneration
    ) {
      throw new Error("runner_intent_identity_mismatch");
    }
    intent.uid = observed.uid;
    intent.resourceVersion = observed.resourceVersion;
    intent.state = observed.state;
    if (intent.state !== "unarmed") throw new Error("runner_intent_initial_state_invalid");

    await this.enqueueOperation(() => this.armIntentAndCreate(handle, intent));
  }

  async armIntentAndCreate(handle, intent) {
    if (handle.finished || handle.deleteRequested) return;
    const armedInventory = await this.completeManagedInventory();
    const armedGuardCount = armedInventory.intents.size + armedInventory.fences.size;
    const armedIntent = armedInventory.intents.get(intent.name);
    if (handle.finished || handle.deleteRequested) return;
    if (!armedIntent || armedIntent.uid !== intent.uid) {
      // No runner POST has been issued. A complete inventory proving that the
      // exact persisted intent disappeared (or was replaced) lets this create
      // fail closed without trying to delete a different UID.
      this.createIntents.delete(handle.name);
      handle.createSettled = true;
      this.failHandleConfirmedAbsent(handle, "runner_intent_reservation_unverifiable");
      return;
    }
    if (armedGuardCount > MAX_GUARD_START_COUNT) {
      await this.abandonUnarmedIntent(handle, intent, "runner_guard_capacity_reserved");
      return;
    }
    if (armedIntent.state !== "unarmed") {
      // An unexpected armed observation is potential runner authorization even
      // though this process has not posted one. Resolve it conservatively via
      // the same permanent fence protocol.
      Object.assign(intent, {
        resourceVersion: armedIntent.resourceVersion,
        state: "armed",
        runnerPostIssued: true
      });
      await this.ensureFence(intent);
      return;
    }
    const patch = [
      { op: "test", path: "/metadata/uid", value: intent.uid },
      { op: "test", path: "/metadata/resourceVersion", value: armedIntent.resourceVersion },
      {
        op: "test",
        path: "/metadata/annotations/yenhubs.org~1intent-state",
        value: "unarmed"
      },
      {
        op: "replace",
        path: "/metadata/annotations/yenhubs.org~1intent-state",
        value: "armed"
      }
    ];
    try {
      await this.api.request(
        "PATCH",
        this.podsPath(`/${encodePathSegment(intent.intentName)}`),
        patch,
        { contentType: "application/json-patch+json" }
      );
    } catch (_error) {
      // Only the exact GET below decides whether arming committed.
    }
    let armed;
    try {
      armed = this.guardRecordFromPod(
        await this.api.request("GET", this.podsPath(`/${encodePathSegment(intent.intentName)}`)),
        "intent"
      );
    } catch (error) {
      intent.state = "persist_ambiguous";
      this.reportHandleError(
        handle,
        error?.status === 404
          ? "runner_intent_arm_absent"
          : "runner_intent_arm_unverifiable"
      );
      return;
    }
    if (!this.guardIdentityMatches(armed, intent) || armed.uid !== intent.uid || armed.state !== "armed") {
      intent.state = "persist_ambiguous";
      this.reportHandleError(handle, "runner_intent_arm_unconfirmed");
      return;
    }
    intent.resourceVersion = armed.resourceVersion;
    intent.state = "armed";

    // The operation queue serializes this transition against stop proof, and
    // handle.kill can still mark deletion synchronously while PATCH/GET wait.
    // Revalidate after the final await; then setting runnerPostIssued and
    // initiating the one-and-only POST are one event-loop critical section.
    if (handle.finished || handle.deleteRequested) return;
    intent.runnerPostIssued = true;
    intent.state = "runner_inflight";
    this.api.request("POST", this.createPath(), this.podDocument(handle))
      .then(pod => this.acceptCreatedPod(handle, pod))
      .catch(error => this.handleCreateError(handle, error));
  }

  async abandonUnarmedIntent(handle, intent, reason) {
    handle.deleteRequested = true;
    handle.deleteSignal ||= "SIGKILL";
    handle.podReady = false;
    intent.state = "aborting_unarmed";
    const absent = await this.deleteIntent(intent);
    if (absent) {
      handle.createSettled = true;
      this.failHandleConfirmedAbsent(handle, reason);
    } else {
      // A successful DELETE response is not absence proof. Keep the local
      // intent so serialized reconciliation retries its UID-preconditioned
      // deletion until an exact GET/LIST observes it gone.
      this.reportHandleError(handle, reason);
    }
  }

  async deleteIntent(intent) {
    if (!intent?.uid) return false;
    try {
      await this.deletePodByUid(intent.intentName, intent.uid, 0, intent.resourceVersion);
    } catch (error) {
      if (error?.status === 409) {
        try {
          const current = this.guardRecordFromPod(
            await this.api.request("GET", this.podsPath(`/${encodePathSegment(intent.intentName)}`)),
            "intent"
          );
          if (!this.guardIdentityMatches(current, intent) || current.uid !== intent.uid) {
            throw new Error("runner_intent_uid_replaced");
          }
          intent.resourceVersion = current.resourceVersion;
          intent.state = current.state;
          return false;
        } catch (readError) {
          if (readError?.status !== 404) throw readError;
          this.createIntents.delete(intent.name);
          return true;
        }
      }
      // A lost DELETE response is ambiguous. The exact GET below, rather than
      // the transport outcome, decides whether the UID is still present.
    }
    try {
      const remaining = this.guardRecordFromPod(
        await this.api.request("GET", this.podsPath(`/${encodePathSegment(intent.intentName)}`)),
        "intent"
      );
      if (!this.guardIdentityMatches(remaining, intent) || remaining.uid !== intent.uid) {
        throw new Error("runner_intent_uid_replaced");
      }
      return false;
    } catch (error) {
      if (error?.status !== 404) throw error;
      this.createIntents.delete(intent.name);
      return true;
    }
  }

  acceptCreatedPod(handle, pod) {
    if (handle.finished) return;
    handle.createSettled = true;
    if (!this.podMatchesHandle(pod, handle)) {
      const intent = this.createIntents.get(handle.name);
      if (intent) intent.state = "ambiguous";
      const name = pod?.metadata?.name;
      const uid = pod?.metadata?.uid;
      if (name === handle.name && typeof uid === "string" && uid && !this.isFencePod(pod)) {
        handle.podUid = uid;
        handle.deleteRequested = true;
        handle.deleteSignal = "SIGKILL";
        handle.podReady = false;
        this.deleteHandle(handle, "SIGKILL").catch(() => {});
      }
      this.reportFatal(handle, "runner_pod_create_contract_mismatch");
      return;
    }
    const intent = this.createIntents.get(handle.name);
    if (intent) {
      intent.runnerCreateConfirmed = true;
      intent.state = "runner_confirmed";
      this.deleteIntent(intent).catch(error => this.reportFatal(
        handle,
        error?.message || "runner_intent_delete_failed"
      ));
    }
    handle.podUid = pod.metadata.uid;
    handle.podReady = podIsReady(pod);
    if (handle.deleteRequested) {
      this.deleteHandle(handle, handle.deleteSignal || "SIGTERM").catch(error => {
        this.reportHandleError(handle, error?.message || "runner_pod_delete_failed");
      });
      return;
    }
    handle.emit("spawn");
  }

  async handleCreateError(handle, error) {
    if (handle.finished) return;
    handle.createSettled = true;
    const intent = this.createIntents.get(handle.name);
    if (intent) intent.state = "ambiguous";
    try {
      const existing = await this.api.request("GET", this.podsPath(`/${encodePathSegment(handle.name)}`));
      if (this.isFencePod(existing, intent || handle)) {
        this.failHandleConfirmedAbsent(handle, "runner_pod_create_fenced");
        return;
      }
      this.acceptCreatedPod(handle, existing);
    } catch (readError) {
      if (readError?.status === 404) {
        // A single exact 404 does not resolve an issued runner CREATE. The
        // durable intent remains until stop/restart installs the same-name
        // fence, which is the only causal proof that a late proposal cannot
        // materialize a runner.
        this.reportHandleError(
          handle,
          error?.status === 409
            ? "runner_pod_create_conflict_unverifiable"
            : "runner_pod_create_state_ambiguous"
        );
        return;
      }
      this.reportHandleError(
        handle,
        error?.status === 409
          ? "runner_pod_create_conflict_unverifiable"
          : "runner_pod_create_state_unverifiable"
      );
    }
  }

  isFencePod(pod, identity = null) {
    try {
      const record = this.guardRecordFromPod(pod, "fence");
      return !identity || (
        record.name === identity.name &&
        record.roomKey === identity.roomKey &&
        record.processGeneration === identity.processGeneration
      );
    } catch (_error) {
      return false;
    }
  }

  podMatchesHandle(pod, handle) {
    const expectedLabels = {
      app: RUNNER_APP_LABEL,
      [MANAGED_BY_LABEL]: MANAGED_BY_VALUE,
      [RUNNER_PROTOCOL_LABEL]: RUNNER_PROTOCOL_VALUE,
      [ROOM_KEY_LABEL]: handle.roomKey,
      [GENERATION_LABEL]: handle.processGeneration
    };
    return pod?.metadata?.name === handle.name &&
      typeof pod?.metadata?.uid === "string" &&
      pod.metadata.uid.length > 0 &&
      exactJsonValue(pod.metadata.labels, expectedLabels) &&
      exactJsonValue(pod.metadata.annotations, {
        [EXPIRES_AT_ANNOTATION]: new Date(handle.expiresAtSeconds * 1000).toISOString(),
        [PARENT_NAMESPACE_ANNOTATION]: this.parentNamespace,
        [PARENT_NAME_ANNOTATION]: this.ownerPodName,
        [PARENT_UID_ANNOTATION]: this.ownerPodUid
      }) &&
      !Object.hasOwn(pod.metadata, "ownerReferences") &&
      this.podSpecMatchesHandle(pod, handle);
  }

  podSpecMatchesHandle(pod, handle) {
    const spec = pod?.spec;
    const container = Array.isArray(spec?.containers) && spec.containers.length === 1
      ? spec.containers[0]
      : null;
    const expected = this.podDocument(handle).spec;
    const expectedContainer = expected.containers[0];
    if (!container) return false;

    return spec.serviceAccountName === expected.serviceAccountName &&
      spec.automountServiceAccountToken === false &&
      exactJsonValue(spec.imagePullSecrets, expected.imagePullSecrets) &&
      spec.enableServiceLinks === false &&
      spec.restartPolicy === "Never" &&
      spec.terminationGracePeriodSeconds === expected.terminationGracePeriodSeconds &&
      spec.activeDeadlineSeconds === expected.activeDeadlineSeconds &&
      defaultFalse(spec.hostNetwork) &&
      defaultFalse(spec.hostPID) &&
      defaultFalse(spec.hostIPC) &&
      spec.shareProcessNamespace === false &&
      exactJsonValue(spec.securityContext, expected.securityContext) &&
      !Array.isArray(spec.initContainers) &&
      !Array.isArray(spec.ephemeralContainers) &&
      container.name === "bot-runner" &&
      container.image === expectedContainer.image &&
      container.imagePullPolicy === expectedContainer.imagePullPolicy &&
      exactJsonValue(container.command, expectedContainer.command) &&
      exactJsonValue(container.args, expectedContainer.args) &&
      exactJsonValue(container.env, expectedContainer.env) &&
      exactJsonValue(container.securityContext, expectedContainer.securityContext) &&
      exactJsonValue(container.resources, expectedContainer.resources) &&
      exactJsonValue(container.volumeMounts, expectedContainer.volumeMounts) &&
      exactJsonValue(container.readinessProbe, expectedContainer.readinessProbe) &&
      !Object.hasOwn(container, "envFrom") &&
      !Object.hasOwn(container, "lifecycle") &&
      !Object.hasOwn(container, "ports") &&
      !Object.hasOwn(container, "volumeDevices") &&
      !Object.hasOwn(container, "startupProbe") &&
      !Object.hasOwn(container, "livenessProbe") &&
      !Object.hasOwn(spec, "runtimeClassName") &&
      !Object.hasOwn(spec, "resourceClaims") &&
      !Object.hasOwn(spec, "hostAliases") &&
      !Object.hasOwn(spec, "dnsConfig") &&
      !Object.hasOwn(spec, "overhead") &&
      exactJsonValue(spec.volumes, expected.volumes);
  }

  reportHandleError(handle, reason) {
    if (!handle || handle.finished || handle.errorReported) return;
    handle.errorReported = true;
    handle.emit("error", new Error(reason));
  }

  reportFatal(handle, reason) {
    const error = new Error(reason);
    this.reportHandleError(handle, reason);
    this.emit("fatal", error);
  }

  finishHandle(handle, exitCode) {
    if (!handle || handle.finished) return;
    if (this.handles.get(handle.name) === handle) this.handles.delete(handle.name);
    handle.finish(exitCode);
  }

  failHandleConfirmedAbsent(handle, reason) {
    if (handle.finished) return;
    this.reportHandleError(handle, reason);
    this.finishHandle(handle, handle.deleteRequested ? this.deleteExitCode(handle) : 1);
  }

  deleteExitCode(handle) {
    return handle?.deleteSignal === "SIGKILL" ? 137 : 143;
  }

  async deleteHandle(handle, signal = "SIGTERM") {
    if (!handle || handle.finished) return false;
    const normalizedSignal = signal === "SIGKILL" ? "SIGKILL" : "SIGTERM";
    const gracePeriodSeconds = normalizedSignal === "SIGKILL" ? 0 : 10;
    handle.deleteRequested = true;
    handle.deleteSignal = normalizedSignal === "SIGKILL" ? "SIGKILL" : (handle.deleteSignal || "SIGTERM");
    handle.podReady = false;
    if (!handle.podUid) return true;
    if (
      Number.isInteger(handle.deleteGracePeriodSeconds) &&
      handle.deleteGracePeriodSeconds <= gracePeriodSeconds
    ) {
      return true;
    }
    handle.deleteGracePeriodSeconds = gracePeriodSeconds;
    try {
      await this.api.request("DELETE", this.podsPath(`/${encodePathSegment(handle.name)}`), {
        apiVersion: "v1",
        kind: "DeleteOptions",
        gracePeriodSeconds,
        preconditions: { uid: handle.podUid }
      });
    } catch (error) {
      if (error?.status === 404) {
        if (this.createIntents.has(handle.name)) return true;
        this.finishHandle(handle, this.deleteExitCode(handle));
        return true;
      }
      if (handle.deleteGracePeriodSeconds === gracePeriodSeconds) {
        handle.deleteGracePeriodSeconds = null;
      }
      if (error?.status === 409) throw new Error("runner_pod_uid_replaced");
      throw error;
    }
    // Kubernetes may return 200/202 while the container is still running or
    // Terminating. Reconciliation confirms GET/LIST absence (or a terminal
    // phase) before emitting the process-compatible exit event.
    return true;
  }

  async deletePodByUid(name, uid, gracePeriodSeconds = 0, resourceVersion = null) {
    if (typeof name !== "string" || !name || typeof uid !== "string" || !uid) return false;
    try {
      await this.api.request("DELETE", this.podsPath(`/${encodePathSegment(name)}`), {
        apiVersion: "v1",
        kind: "DeleteOptions",
        gracePeriodSeconds,
        preconditions: {
          uid,
          ...(typeof resourceVersion === "string" && resourceVersion
            ? { resourceVersion }
            : {})
        }
      });
      return true;
    } catch (error) {
      if (error?.status === 404) return false;
      throw error;
    }
  }

  listPath() {
    // The namespace is dedicated. A full LIST is required so a legacy,
    // relabelled, or malformed Pod cannot evade readiness by falling outside
    // an expected selector.
    return this.podsPath();
  }

  runnerRecordFromPod(pod) {
    const labels = pod?.metadata?.labels;
    const roomDigest = labels?.[ROOM_KEY_LABEL];
    const processGeneration = labels?.[GENERATION_LABEL];
    const name = runnerPodName(roomDigest, processGeneration);
    const { uid } = this.podIdentity(pod);
    if (
      pod?.apiVersion !== "v1" ||
      pod?.kind !== "Pod" ||
      pod?.metadata?.name !== name ||
      pod.metadata.namespace !== this.namespace ||
      !exactJsonValue(labels, {
        app: RUNNER_APP_LABEL,
        [MANAGED_BY_LABEL]: MANAGED_BY_VALUE,
        [RUNNER_PROTOCOL_LABEL]: RUNNER_PROTOCOL_VALUE,
        [ROOM_KEY_LABEL]: roomDigest,
        [GENERATION_LABEL]: processGeneration
      })
    ) {
      throw new Error("runner_pod_contract_invalid");
    }
    return { type: "runner", name, uid, roomKey: roomDigest, processGeneration, pod };
  }

  inspectManagedPods(pods) {
    if (!Array.isArray(pods)) throw new Error("runner_pod_list_invalid");
    const runners = [];
    const intents = new Map();
    const fences = new Map();
    for (const pod of pods) {
      const app = pod?.metadata?.labels?.app;
      if (app === RUNNER_APP_LABEL) {
        runners.push(this.runnerRecordFromPod(pod));
        continue;
      }
      if (app !== INTENT_APP_LABEL && app !== FENCE_APP_LABEL) {
        throw new Error("runner_managed_pod_type_invalid");
      }
      const record = this.guardRecordFromPod(pod);
      const collection = record.type === "intent" ? intents : fences;
      if (collection.has(record.name)) throw new Error("runner_guard_identity_ambiguous");
      collection.set(record.name, { ...record, pod });
    }
    if (intents.size + fences.size > MAX_GUARD_PODS) {
      throw new Error("runner_guard_capacity_exceeded");
    }
    this.fences = new Map(fences);
    this.updateGuardCapacitySnapshot(intents.size, fences.size);
    return { runners, intents, fences };
  }

  async completeManagedInventory(path = this.listPath()) {
    const response = await this.api.request("GET", path);
    const { items, resourceVersion } = requireCompletePodList(response);
    return { ...this.inspectManagedPods(items), resourceVersion };
  }

  guardIdentityMatches(left, right) {
    return !!left && !!right &&
      left.name === right.name &&
      left.roomKey === right.roomKey &&
      left.processGeneration === right.processGeneration;
  }

  async ensureFence(intent, { maxAttempts = 8, retryDelayMs = 250 } = {}) {
    if (!intent || !intent.name || !intent.roomKey || !intent.processGeneration) {
      throw new Error("runner_intent_identity_invalid");
    }
    for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
      let target = null;
      try {
        target = await this.api.request(
          "GET",
          this.podsPath(`/${encodePathSegment(intent.name)}`)
        );
      } catch (error) {
        if (error?.status !== 404) throw error;
      }

      if (target) {
        if (target?.metadata?.labels?.app === FENCE_APP_LABEL) {
          const fence = this.guardRecordFromPod(target, "fence");
          if (!this.guardIdentityMatches(fence, intent)) {
            throw new Error("runner_fence_identity_mismatch");
          }
          this.fences.set(fence.name, { ...fence, pod: target });
          if (intent.uid) await this.deleteIntent(intent);
          else this.createIntents.delete(intent.name);
          const handle = this.handles.get(intent.name);
          if (handle && !handle.finished) {
            this.failHandleConfirmedAbsent(handle, "runner_pod_create_fenced");
          }
          return fence;
        }
        if (target?.metadata?.labels?.app !== RUNNER_APP_LABEL) {
          throw new Error("runner_fence_target_occupied_by_unknown_pod");
        }
        const runner = this.runnerRecordFromPod(target);
        if (!this.guardIdentityMatches(runner, intent)) {
          throw new Error("runner_fence_target_identity_mismatch");
        }
        const handle = this.handles.get(intent.name);
        if (handle && !handle.finished) {
          handle.createSettled = true;
          handle.podUid = runner.uid;
          handle.podReady = false;
          handle.deleteRequested = true;
          handle.deleteSignal ||= "SIGKILL";
        }
        await this.deletePodByUid(runner.name, runner.uid, 0);
        if (attempt + 1 < maxAttempts) await this.sleep(retryDelayMs);
        continue;
      }

      try {
        await this.api.request("POST", this.createPath(), this.guardPodDocument(intent, "fence"));
      } catch (_error) {
        // 409, a lost success response, and a transport failure are handled in
        // exactly the same way: only the next exact GET can prove the fence.
      }
      if (attempt + 1 < maxAttempts) await this.sleep(retryDelayMs);
    }
    throw new Error("runner_fence_unconfirmed");
  }

  enqueueOperation(operation) {
    const result = this.operationTail.then(operation);
    this.operationTail = result.catch(() => {});
    return result;
  }

  podIdentity(pod) {
    const name = pod?.metadata?.name;
    const uid = pod?.metadata?.uid;
    if (typeof name !== "string" || !name || typeof uid !== "string" || !uid) {
      throw new Error("runner_pod_identity_unverifiable");
    }
    return { name, uid };
  }

  podMatchesRoom(pod, hubSid) {
    const labels = pod?.metadata?.labels;
    return labels &&
      typeof labels === "object" &&
      !Array.isArray(labels) &&
      labels.app === RUNNER_APP_LABEL &&
      labels[MANAGED_BY_LABEL] === MANAGED_BY_VALUE &&
      labels[RUNNER_PROTOCOL_LABEL] === RUNNER_PROTOCOL_VALUE &&
      labels[ROOM_KEY_LABEL] === roomKey(hubSid, this.credentialKey);
  }

  normalizeStopTargets(hubSid, expectedTargets) {
    if (!Array.isArray(expectedTargets)) throw new Error("runner_stop_targets_invalid");
    const targets = new Map();
    for (const target of expectedTargets) {
      const name = target?.name;
      const uid = target?.uid;
      if (
        typeof name !== "string" ||
        !/^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$/.test(name) ||
        !((typeof uid === "string" && uid.length > 0) || uid === null)
      ) {
        throw new Error("runner_stop_target_invalid");
      }
      const previous = targets.get(name);
      targets.set(name, previous || uid);
    }
    for (const handle of this.handles.values()) {
      if (handle.hubSid !== hubSid || handle.finished) continue;
      const previous = targets.get(handle.name);
      const uid = handle.podUid || null;
      // Different UIDs for one immutable Pod name are ABA evidence, not an
      // ambiguity that may block cleanup. The exact GET below observes and
      // deletes whichever incarnation currently owns the name.
      targets.set(handle.name, uid || previous || null);
    }
    for (const intent of this.createIntents.values()) {
      if (intent.hubSid !== hubSid) continue;
      const previous = targets.get(intent.name);
      targets.set(intent.name, previous || null);
    }
    return targets;
  }

  async performConfirmRoomStopped(hubSid, expectedTargets = []) {
    const targets = this.normalizeStopTargets(hubSid, expectedTargets);
    const expectedRoomKey = roomKey(hubSid, this.credentialKey);
    const inventory = await this.completeManagedInventory();
    const roomRunners = inventory.runners.filter(record => record.roomKey === expectedRoomKey);
    const roomIntents = Array.from(inventory.intents.values())
      .filter(record => record.roomKey === expectedRoomKey);
    const roomFences = new Map(Array.from(inventory.fences.entries())
      .filter(([, record]) => record.roomKey === expectedRoomKey));
    let acted = false;
    let pendingCreate = false;

    // A durable unarmed intent proves no runner POST was authorized. Delete it
    // by UID; a delayed CAS PATCH cannot recreate a deleted Pod. An armed
    // intent may have issued exactly one runner POST and must be fenced.
    for (const observedIntent of roomIntents) {
      const local = this.createIntents.get(observedIntent.name);
      const intent = local && this.guardIdentityMatches(local, observedIntent)
        ? Object.assign(local, {
            uid: observedIntent.uid,
            resourceVersion: observedIntent.resourceVersion,
            state: observedIntent.state
          })
        : { ...observedIntent, hubSid, runnerPostIssued: observedIntent.state === "armed" };
      if (intent.runnerCreateConfirmed) {
        await this.deleteIntent(intent);
        acted = true;
      } else if (observedIntent.state === "unarmed" && !intent.runnerPostIssued) {
        await this.deleteIntent(intent);
        const handle = this.handles.get(intent.name);
        if (handle && !handle.finished) {
          this.failHandleConfirmedAbsent(handle, "runner_intent_stopped_unarmed");
        }
        acted = true;
      } else {
        await this.ensureFence(intent);
        roomFences.set(intent.name, this.fences.get(intent.name));
        acted = true;
      }
    }

    // A local intent can be between API observations. If no runner POST was
    // issued, stop prevents it synchronously and no fence is required. Once
    // armed/issued, only the exact durable fence resolves it.
    for (const intent of Array.from(this.createIntents.values())) {
      if (intent.hubSid !== hubSid || roomIntents.some(record => record.name === intent.name)) continue;
      if (intent.runnerCreateConfirmed) {
        if (intent.uid) await this.deleteIntent(intent);
        else this.createIntents.delete(intent.name);
        continue;
      }
      if (!intent.runnerPostIssued) {
        const handle = this.handles.get(intent.name);
        if (handle && handle.deleteRequested && !handle.finished) {
          this.createIntents.delete(intent.name);
          this.failHandleConfirmedAbsent(handle, "runner_intent_stopped_before_arm");
        } else {
          pendingCreate = true;
        }
        continue;
      }
      await this.ensureFence(intent);
      roomFences.set(intent.name, this.fences.get(intent.name));
      acted = true;
    }

    for (const runner of roomRunners) {
      if (this.fences.has(runner.name)) continue;
      await this.deletePodByUid(runner.name, runner.uid, 0);
      const handle = this.handles.get(runner.name);
      if (handle && !handle.finished) {
        handle.createSettled = true;
        handle.podUid = runner.uid;
        handle.podReady = false;
      }
      acted = true;
    }

    // Exact target GETs prove the original runner UID absent. A fence may own
    // the same name; targetAbsent refers to executable runner state, not to a
    // free Kubernetes name.
    for (const [name, originalUid] of targets) {
      let pod;
      try {
        pod = await this.api.request("GET", this.podsPath(`/${encodePathSegment(name)}`));
      } catch (error) {
        if (error?.status === 404) continue;
        throw error;
      }
      if (pod?.metadata?.labels?.app === FENCE_APP_LABEL) {
        const fence = this.guardRecordFromPod(pod, "fence");
        if (fence.name !== name || (originalUid && fence.uid === originalUid)) {
          throw new Error("runner_stop_fence_identity_invalid");
        }
        roomFences.set(name, { ...fence, pod });
        continue;
      }
      if (pod?.metadata?.labels?.app !== RUNNER_APP_LABEL) {
        throw new Error("runner_stop_target_occupied_by_unknown_pod");
      }
      const runner = this.runnerRecordFromPod(pod);
      if (runner.roomKey !== expectedRoomKey) {
        throw new Error("runner_stop_target_room_mismatch");
      }
      await this.deletePodByUid(runner.name, runner.uid, 0);
      acted = true;
    }

    // Re-evaluate local state after every await. A create that armed while this
    // proof was fetching evidence must force another fenced round.
    if (Array.from(this.createIntents.values()).some(intent =>
      intent.hubSid === hubSid && intent.runnerPostIssued && !this.fences.has(intent.name)
    )) {
      pendingCreate = true;
    }

    if (acted || pendingCreate || roomRunners.length > 0) {
      return {
        terminal: false,
        targetAbsent: roomRunners.length === 0,
        managedRoomPods: roomRunners.length,
        pendingCreate,
        resourceVersion: inventory.resourceVersion,
        fenced: roomFences.size > 0
      };
    }

    // The complete inventory is terminal immediately: this operation is
    // serialized with the only arm-and-POST path, every issued intent requires
    // an exact durable fence, and an unarmed intent cannot authorize a POST.
    // Repeating an empty LIST or waiting for time to pass would add no causal
    // evidence to those invariants.
    return {
      terminal: true,
      targetAbsent: true,
      managedRoomPods: 0,
      pendingCreate: false,
      resourceVersion: inventory.resourceVersion,
      fenced: roomFences.size > 0
    };
  }

  confirmRoomStopped(hubSid, expectedTargets = []) {
    return this.enqueueOperation(() => this.performConfirmRoomStopped(hubSid, expectedTargets));
  }

  async observeHandlePod(handle, pod, nowSeconds) {
    const { name, uid } = this.podIdentity(pod);
    if (name !== handle.name) throw new Error("runner_pod_name_mismatch");
    if (handle.podUid && handle.podUid !== uid) throw new Error("runner_pod_uid_replaced");
    if (!this.podMatchesHandle(pod, handle)) {
      handle.createSettled = true;
      handle.podUid = uid;
      handle.deleteRequested = true;
      handle.deleteSignal = "SIGKILL";
      handle.podReady = false;
      await this.deleteHandle(handle, "SIGKILL");
      throw new Error("runner_pod_contract_mismatch");
    }

    handle.createSettled = true;
    handle.podUid = uid;
    const intent = this.createIntents.get(name);
    if (intent) {
      intent.runnerCreateConfirmed = true;
      intent.state = "runner_confirmed";
      await this.deleteIntent(intent);
    }
    if (podIsTerminal(pod)) {
      handle.podReady = false;
      await this.deletePodByUid(name, uid, 0);
      this.finishHandle(handle, handle.deleteRequested ? this.deleteExitCode(handle) : 1);
      return "terminal";
    }

    if (handle.expiresAtSeconds <= nowSeconds) {
      handle.deleteRequested = true;
      handle.deleteSignal = "SIGKILL";
      handle.podReady = false;
      await this.deleteHandle(handle, "SIGKILL");
      return "deleting";
    }

    if (pod?.metadata?.deletionTimestamp) {
      handle.deleteRequested = true;
      handle.deleteSignal ||= "SIGTERM";
      handle.podReady = false;
      return "deleting";
    }

    if (handle.deleteRequested) {
      handle.podReady = false;
      await this.deleteHandle(handle, handle.deleteSignal || "SIGTERM");
      return "deleting";
    }

    handle.podReady = podIsReady(pod);
    return "active";
  }

  async confirmUnseenHandle(handle, nowSeconds) {
    if (!handle.createSettled || handle.finished) return;
    try {
      const pod = await this.api.request("GET", this.podsPath(`/${encodePathSegment(handle.name)}`));
      await this.observeHandlePod(handle, pod, nowSeconds);
    } catch (error) {
      if (error?.status === 404) {
        const intent = this.createIntents.get(handle.name);
        if (intent && !intent.runnerCreateConfirmed) return;
        this.finishHandle(handle, handle.deleteRequested ? this.deleteExitCode(handle) : 1);
        return;
      }
      throw error;
    }
  }

  async performReconcile({ deleteUnknown = true } = {}) {
    const inventory = await this.completeManagedInventory();
    const seen = new Set();
    const observedUnknown = new Map();
    const nowSeconds = Math.floor(this.now() / 1000);

    // A residual intent with no matching local create belongs to a previous
    // parent. Unarmed is safe to delete by UID; armed is conservatively fenced
    // before its intent can be removed. Fences are permanent and never enter
    // any deletion path.
    for (const observedIntent of inventory.intents.values()) {
      const local = this.createIntents.get(observedIntent.name);
      if (local && this.guardIdentityMatches(local, observedIntent)) {
        local.uid = observedIntent.uid;
        local.resourceVersion = observedIntent.resourceVersion;
        const handle = this.handles.get(local.name);
        if (!handle || handle.finished) {
          if (observedIntent.state === "unarmed") {
            await this.deleteIntent(local);
          } else {
            local.state = "armed";
            local.runnerPostIssued = true;
            await this.ensureFence(local);
          }
        } else if (local.runnerCreateConfirmed) {
          await this.deleteIntent(local);
        } else if (handle?.deleteRequested) {
          if (observedIntent.state === "unarmed") {
            const absent = await this.deleteIntent(local);
            if (absent && !handle.finished) {
              handle.createSettled = true;
              this.failHandleConfirmedAbsent(handle, "runner_intent_stopped_unarmed");
            }
          } else {
            local.state = "armed";
            local.runnerPostIssued = true;
            await this.ensureFence(local);
          }
        }
        continue;
      }
      if (observedIntent.state === "unarmed") {
        await this.deletePodByUid(observedIntent.intentName, observedIntent.uid, 0);
      } else {
        await this.ensureFence({ ...observedIntent, runnerPostIssued: true });
      }
    }

    // A complete LIST also resolves local intent records that disappeared
    // after an ambiguous DELETE response. Issued/armed creates still require
    // an exact fence; non-issued creates can fail closed because this process
    // has no remaining path that can submit their runner POST.
    for (const local of Array.from(this.createIntents.values())) {
      if (inventory.intents.has(local.name)) continue;
      const handle = this.handles.get(local.name);
      if (local.runnerCreateConfirmed) {
        this.createIntents.delete(local.name);
        continue;
      }
      if (local.runnerPostIssued || local.state === "armed" || local.state === "runner_inflight") {
        await this.ensureFence(local);
        continue;
      }
      this.createIntents.delete(local.name);
      if (handle && !handle.finished) {
        handle.createSettled = true;
        this.failHandleConfirmedAbsent(handle, "runner_intent_absent_before_arm");
      }
    }

    for (const runner of inventory.runners) {
      const { name, uid, pod } = runner;
      if (this.fences.has(name)) continue;
      const handle = this.handles.get(name);
      if (!handle) {
        observedUnknown.set(name, uid);
        if (deleteUnknown) await this.deletePodByUid(name, uid, 0);
        continue;
      }
      seen.add(name);
      await this.observeHandlePod(handle, pod, nowSeconds);
    }

    for (const [name, handle] of this.handles.entries()) {
      if (!seen.has(name)) await this.confirmUnseenHandle(handle, nowSeconds);
    }
    this.unknownPods = observedUnknown;
    return {
      observed: inventory.runners.length,
      managed: this.handles.size,
      intents: inventory.intents.size,
      fences: inventory.fences.size
    };
  }

  reconcile(options = {}) {
    return this.enqueueOperation(() => this.performReconcile(options));
  }

  async performCleanupOrphans({ maxAttempts = 20, retryDelayMs = 250 } = {}) {
    if (this.handles.size !== 0) throw new Error("runner_orphan_cleanup_after_create");
    for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
      const inventory = await this.completeManagedInventory();
      this.unknownPods = new Map(inventory.runners.map(record => [record.name, record.uid]));
      let acted = false;
      let allIntentsProtected = true;
      for (const intent of inventory.intents.values()) {
        if (intent.state === "unarmed") {
          await this.deletePodByUid(intent.intentName, intent.uid, 0);
        } else {
          await this.ensureFence({ ...intent, runnerPostIssued: true });
        }
        acted = true;
        allIntentsProtected = allIntentsProtected && (
          intent.state === "unarmed" || this.fences.has(intent.name)
        );
      }
      for (const runner of inventory.runners) {
        if (this.fences.has(runner.name)) continue;
        await this.deletePodByUid(runner.name, runner.uid, 0);
        acted = true;
      }
      if (inventory.runners.length === 0 && allIntentsProtected && !acted) {
        return {
          observed: 0,
          managed: 0,
          intents: 0,
          fences: inventory.fences.size,
          attempts: attempt
        };
      }
      if (
        inventory.runners.length === 0 &&
        inventory.intents.size > 0 &&
        allIntentsProtected
      ) {
        // Intent deletion may be accepted but not yet visible. Require the
        // next complete LIST before opening readiness.
      }
      if (attempt < maxAttempts) await this.sleep(retryDelayMs);
    }
    throw new Error("runner_orphan_cleanup_unconfirmed");
  }

  cleanupOrphans(options = {}) {
    return this.enqueueOperation(() => this.performCleanupOrphans(options));
  }

  close() {
    for (const handle of this.handles.values()) {
      handle.connected = false;
    }
  }
}

function exactGuardRecordFromPod(pod, expectedType, namespace = RUNNER_NAMESPACE) {
  const verifier = Object.create(KubernetesRunnerManager.prototype);
  verifier.namespace = namespace;
  return verifier.guardRecordFromPod(pod, expectedType);
}

function exactManagedRunnerRecordFromPod(pod, namespace = RUNNER_NAMESPACE) {
  const verifier = Object.create(KubernetesRunnerManager.prototype);
  verifier.namespace = namespace;
  return verifier.runnerRecordFromPod(pod);
}

function guardPodDocumentForIdentity(identity, type, namespace = RUNNER_NAMESPACE) {
  const builder = Object.create(KubernetesRunnerManager.prototype);
  builder.namespace = namespace;
  return builder.guardPodDocument(identity, type);
}

module.exports = {
  EXPIRES_AT_ANNOTATION,
  FENCE_APP_LABEL,
  GENERATION_LABEL,
  GUARD_IMAGE,
  GUARD_CAPACITY_WARNING_THRESHOLD,
  GUARD_SCHEDULER_NAME,
  GUARD_SCHEDULING_GATE,
  INTENT_APP_LABEL,
  INTENT_STATE_ANNOTATION,
  KubernetesApi,
  KubernetesRunnerManager,
  MAX_GUARD_PODS,
  MAX_GUARD_START_COUNT,
  MIN_GUARD_FENCE_RESERVE,
  MANAGED_BY_LABEL,
  MANAGED_BY_VALUE,
  PARENT_NAME_ANNOTATION,
  PARENT_NAMESPACE_ANNOTATION,
  PARENT_UID_ANNOTATION,
  ROOM_KEY_LABEL,
  RUNNER_PROTOCOL_LABEL,
  RUNNER_PROTOCOL_VALUE,
  RUNNER_APP_LABEL,
  RUNNER_NAMESPACE,
  exactGuardRecordFromPod,
  exactManagedRunnerRecordFromPod,
  guardPodDocumentForIdentity,
  podIsReady,
  requireCompletePodList,
  roomKey,
  runnerPodName
};
