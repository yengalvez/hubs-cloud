const crypto = require("node:crypto");
const fs = require("node:fs");
const https = require("node:https");
const { EventEmitter } = require("node:events");
const { isDeepStrictEqual } = require("node:util");

const MANAGED_BY_LABEL = "yenhubs.org/managed-by";
const ROOM_KEY_LABEL = "yenhubs.org/room-key";
const GENERATION_LABEL = "yenhubs.org/generation";
const EXPIRES_AT_ANNOTATION = "yenhubs.org/expires-at";
const PARENT_NAMESPACE_ANNOTATION = "yenhubs.org/parent-namespace";
const PARENT_NAME_ANNOTATION = "yenhubs.org/parent-name";
const PARENT_UID_ANNOTATION = "yenhubs.org/parent-uid";
const MANAGED_BY_VALUE = "bot-orchestrator";
const RUNNER_APP_LABEL = "bot-runner";
const RUNNER_NAMESPACE = "hcce-bot-runners";
const MAX_API_RESPONSE_BYTES = 1024 * 1024;

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

  request(method, path, body = null) {
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
                  "content-type": "application/json",
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

function podIsTerminal(pod) {
  return pod?.status?.phase === "Failed" || pod?.status?.phase === "Succeeded";
}

function exactJsonValue(actual, expected) {
  return isDeepStrictEqual(actual, expected);
}

function defaultFalse(value) {
  return value === undefined || value === false;
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
    this.sleep = sleep;
    this.handles = new Map();
    this.unknownPods = new Map();
    this.operationTail = Promise.resolve();
  }

  podsPath(suffix = "") {
    return `/api/v1/namespaces/${encodePathSegment(this.namespace)}/pods${suffix}`;
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

  create(hubSid, processGeneration) {
    if (this.handles.size + this.unknownPods.size >= this.maxActiveRooms) return null;
    const identity = this.identity(hubSid, processGeneration);
    if (this.handles.has(identity.name)) return null;
    const handle = new RunnerPodHandle(this, identity);
    this.handles.set(handle.name, handle);
    const document = this.podDocument(identity);
    this.api.request("POST", this.podsPath(), document)
      .then(pod => this.acceptCreatedPod(handle, pod))
      .catch(error => this.handleCreateError(handle, error));
    return handle;
  }

  acceptCreatedPod(handle, pod) {
    if (handle.finished) return;
    handle.createSettled = true;
    if (!this.podMatchesHandle(pod, handle)) {
      const name = pod?.metadata?.name;
      const uid = pod?.metadata?.uid;
      if (name === handle.name && typeof uid === "string" && uid) {
        handle.podUid = uid;
        handle.deleteRequested = true;
        handle.deleteSignal = "SIGKILL";
        handle.podReady = false;
        this.deleteHandle(handle, "SIGKILL").catch(() => {});
      }
      this.reportFatal(handle, "runner_pod_create_contract_mismatch");
      return;
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
    try {
      const existing = await this.api.request("GET", this.podsPath(`/${encodePathSegment(handle.name)}`));
      this.acceptCreatedPod(handle, existing);
    } catch (readError) {
      if (readError?.status === 404) {
        handle.createSettled = true;
        this.failHandleConfirmedAbsent(
          handle,
          error?.status === 409
            ? "runner_pod_create_conflict_unverifiable"
            : "runner_pod_create_failed"
        );
        return;
      }
      this.reportFatal(
        handle,
        error?.status === 409
          ? "runner_pod_create_conflict_unverifiable"
          : "runner_pod_create_state_unverifiable"
      );
    }
  }

  podMatchesHandle(pod, handle) {
    const expectedLabels = {
      app: RUNNER_APP_LABEL,
      [MANAGED_BY_LABEL]: MANAGED_BY_VALUE,
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

  async deletePodByUid(name, uid, gracePeriodSeconds = 0) {
    if (typeof name !== "string" || !name || typeof uid !== "string" || !uid) return false;
    try {
      await this.api.request("DELETE", this.podsPath(`/${encodePathSegment(name)}`), {
        apiVersion: "v1",
        kind: "DeleteOptions",
        gracePeriodSeconds,
        preconditions: { uid }
      });
      return true;
    } catch (error) {
      if (error?.status === 404) return false;
      throw error;
    }
  }

  listPath() {
    const selector = encodeURIComponent(`app=${RUNNER_APP_LABEL},${MANAGED_BY_LABEL}=${MANAGED_BY_VALUE}`);
    return this.podsPath(`?labelSelector=${selector}`);
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
        this.finishHandle(handle, handle.deleteRequested ? this.deleteExitCode(handle) : 1);
        return;
      }
      throw error;
    }
  }

  async performReconcile({ deleteUnknown = true } = {}) {
    const response = await this.api.request("GET", this.listPath());
    const pods = requirePodList(response);
    const seen = new Set();
    const observedUnknown = new Map();
    const nowSeconds = Math.floor(this.now() / 1000);

    for (const pod of pods) {
      const { name, uid } = this.podIdentity(pod);
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
    return { observed: pods.length, managed: this.handles.size };
  }

  reconcile(options = {}) {
    return this.enqueueOperation(() => this.performReconcile(options));
  }

  async performCleanupOrphans({ maxAttempts = 20, retryDelayMs = 250 } = {}) {
    if (this.handles.size !== 0) throw new Error("runner_orphan_cleanup_after_create");
    for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
      const response = await this.api.request("GET", this.listPath());
      const pods = requirePodList(response);
      this.unknownPods = new Map(pods.map(pod => {
        const { name, uid } = this.podIdentity(pod);
        return [name, uid];
      }));
      if (pods.length === 0) return { observed: 0, managed: 0, attempts: attempt };
      for (const pod of pods) {
        const { name, uid } = this.podIdentity(pod);
        await this.deletePodByUid(name, uid, 0);
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

module.exports = {
  EXPIRES_AT_ANNOTATION,
  GENERATION_LABEL,
  KubernetesApi,
  KubernetesRunnerManager,
  MANAGED_BY_LABEL,
  MANAGED_BY_VALUE,
  PARENT_NAME_ANNOTATION,
  PARENT_NAMESPACE_ANNOTATION,
  PARENT_UID_ANNOTATION,
  ROOM_KEY_LABEL,
  RUNNER_APP_LABEL,
  RUNNER_NAMESPACE,
  podIsReady,
  roomKey,
  runnerPodName
};
