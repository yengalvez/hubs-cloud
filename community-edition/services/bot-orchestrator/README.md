# YenHubs bot orchestrator and isolated runner

Production runner mode uses two images built from this directory:

- `Dockerfile` is the parent control plane. It owns the OpenAI provider key,
  the bot-orchestrator credential and the Kubernetes Pod lifecycle.
- `Dockerfile.runner` is the ghost data plane. It contains no Chromium and runs
  as UID/GID 10001. It receives one generation-scoped runner credential and no
  parent, provider, legacy runner or Kubernetes credential.

Dispatch `.github/workflows/bot-images-build-push.yml` from an accepted commit
on `master` to build both images with the same `sha-<commit>` source tag and
SBOM/provenance attestations. Record their immutable GHCR
digests and set `OVERRIDE_BOT_ORCHESTRATOR_IMAGE` and
`OVERRIDE_BOT_RUNNER_IMAGE` before generating the Hubs CE manifest. Do not
deploy tags or manually edit the generated manifest.

Both runtime manifests contain only the dependencies used by that image. The
parent final stage contains Express and its transitive dependencies; the runner
final stage contains only the ghost networking/navigation dependencies. CI
builds both Dockerfiles independently before changes can merge.

## Private image-pull credential

The generated `bot-images-pull` Secret is required for both digest-pinned bot
images. It is referenced only by kubelet through `imagePullSecrets`; it is not
mounted into either container and is never passed to Node. Keep its source only
in the private input file selected by `HCCE_INPUT_VALUES_PATH` (for the YenHubs
root checkout, `deployment/input-values.local.yaml`, mode `0600`). Never put a
PAT in tracked YAML, a workflow input, a command argument or terminal output.

Create or rotate the value through hidden input, without displaying it. Run
this block in a Bash shell (not directly in zsh):

```bash
read -r -p "GHCR username: " GHCR_USERNAME
read -r -s -p "GHCR read-packages token: " GHCR_TOKEN; echo
export GHCR_USERNAME GHCR_TOKEN
HCCE_INPUT_VALUES_PATH=/absolute/path/to/input-values.local.yaml npm run set-bot-image-pull-config
unset GHCR_TOKEN GHCR_USERNAME
```

For a registry other than `ghcr.io`, also export `BOT_IMAGE_REGISTRY` with the
exact registry host. The updater atomically replaces only
`BOT_IMAGE_PULL_CONFIG_JSON_BASE64`, forces the input file to `0600` and never
prints the credential. The generator rejects empty credentials and credentials
that do not cover both selected bot-image registries. After rotation,
regenerate and apply the manifest through the approved deployment path; do not
edit the ignored generated manifest or create a second ad-hoc Secret.

## Pod lifecycle

The parent hashes the room SID with its signing key for Kubernetes names and
labels, allocates a UUID process generation, and first persists an inert
`unarmed` intent Pod. Only an exact UID/resourceVersion JSON-Patch CAS to
`armed`, followed by an exact GET of that armed intent, authorizes one and only
one runner Pod POST. The runner receives a token containing room, generation,
parent-Pod UID and expiry and uses `restartPolicy: Never`. Readiness requires
both Kubernetes Pod readiness and the existing authenticated
Reticulum/config/navmesh/spawn status. Stop and recovery delete executable
runners and disposable intents by exact name plus UID precondition.

At parent startup, a complete unfiltered LIST of the dedicated runner namespace
is reconciled before readiness. Existing runners are deleted rather than
adopted because their credential is tied to the previous parent UID. Unarmed
intents are deleted by UID. Every armed intent is treated as proof that one
runner POST may still materialize: the manager deletes any runner currently
occupying the target name, then installs and exactly reads a permanent inert
fence Pod with that same name before deleting the intent. Existing fences are
preserved. Unknown or malformed Pods, incomplete LISTs and unverifiable guard
shapes fail closed. Periodic reconciliation applies the same rules and exits the
parent on failure.

Executable runners are `NotBestEffort` and limited by a 10-Pod quota. Intents
and fences are fixed non-executable `BestEffort` sentinels, use a ServiceAccount
without a token, an impossible scheduler plus scheduling gate, and have a
separate 100-Pod quota. New authorization stops at 80 guards, reserving 20 slots
for fencing races and recovery; fences do not count toward `MAX_ACTIVE_ROOMS`.
Every complete unfiltered inventory caches only the non-sensitive counts
`intents`, `fences` and `total`. `/transport-ready`, `/health` and `/ready`
publish those counts with `warning`, `warning_threshold=60`, `start_limit=80`,
`reserve=20` and `quota=100`, without doing Kubernetes I/O in the endpoint. A
normal completed create/stop cycle leaves zero guards; an ambiguous CREATE can
permanently add one fence. Reaching 60 raises a capacity warning, while 80
rejects new authorization so the last 20 slots remain available for fencing
races. Neither condition is permission to remove a fence.

Fence cleanup is break-glass only after proving the previous control plane can
no longer submit a runner CREATE; ordinary startup, stop, update and rollback
paths never TTL, owner-reference or delete fences. Any future namespace-epoch
retirement must be a separate, explicitly quiesced and admission-fenced
campaign; it is not automatic garbage collection.

Every runner, intent and fence also carries the exact immutable label
`yenhubs.org/runner-protocol=durable-fence-v2`. A separately named durable
admission policy rejects old Pod shapes even if an older apply rewrites the
original runner policy, and still forbids fence deletion. Three separately
named durable policies then preserve the compatibility boundary: the durable
runner policy, the parent fence policy and the first-cutover journal policy.
The parent policy pins the fence-aware parent and runner images, requires the
parent protocol marker, and denies both direct deletion and Deployment
DeleteCollection with an empty admission `request.name`. The journal policy
protects its exact ConfigMap from update, direct delete and ConfigMap
DeleteCollection. These policies remain installed through bootstrap,
admission, active and any ordinary fence-aware rollback.

A container restart keeps the same parent Pod UID, so owner garbage collection
is not claimed. Startup proves zero executable runners and zero unresolved
intents, while permanent fences may remain, before reopening
`/transport-ready`. Capacity is also bounded by `MAX_ACTIVE_ROOMS` in desired
state and the manager. The production startup grace is three minutes because it
includes initial scheduling and a cold private-image pull; the shorter running
config-ACK deadline applies only after startup.

The Kubernetes readiness probe uses `/transport-ready`, which opens only after
orphan cleanup but before any runner is ready. This avoids a Service bootstrap
deadlock: runner control traffic can reach the parent while the stricter
`/ready` rollout gate continues to require authenticated bot readiness.

## Control and fencing interface

The runner polls `GET /internal/runner/v1/config` and posts status to
`POST /internal/runner/v1/status` with its Bearer generation token and Downward
API Pod UID. Neither credential is placed in the URL. The parent accepts only
the current room generation, holder, exact token and exact created Pod UID.

The generation-token v1 payload is exact and preauthorizes only the room,
process generation, parent holder and expiry. It cannot carry a lease ID or
fencing epoch. After the Phoenix join, Reticulum acquires the shared PostgreSQL
lease and assigns an exact UUID plus positive JavaScript-safe authority epoch.
Those mandatory values flow through the join and authenticated Presence;
Reticulum also attaches them to spawn ACKs and commands. The runner rejects
missing or stale fences, reports the accepted `runnerLeaseId` and
`runnerAuthorityEpoch` to the parent control plane, and the parent does not
consider that runner authenticated or ready without the exact UUID and epoch.
Reticulum revalidates the database fence while protected side effects execute,
so token rotation is not the authority boundary. `RetWeb.HubChannel.perform_join/4`
preserves both `Ret.BotRunnerGenerationToken.verify/2` room-scoped
preauthentication and the subsequent database lease registration.

The documented singleton/Recreate setting remains only because readiness,
Endpoint handling and the `ret-pvc` RWO storage topology have not yet been
staged for multiple cold Reticulum replicas. It is not the bot-authority fence.

## Durable desired-state delivery

Reticulum delivers approved bot configuration through protocol
`yenhubs-bot-runtime-v2`. Every `room-config` and `room-stop` request carries a
canonical UUIDv4 operation ID and a positive JavaScript-safe per-room
`runtime_revision`. The parent accepts retries idempotently, rejects reuse with
different payloads or revisions, and will not let an unversioned request or an
older snapshot cross a pending stop. Snapshot entries therefore also require a
per-room `runtime_revision`; omission is not authority to erase newer local v2
state.

Each durable config event also stores `runtime_chat_enabled`, the boolean
produced by Reticulum's `BotConfig.normalize/1` at enqueue time. The raw approved
JSON remains immutable in PostgreSQL, including future fields and numeric JSON
types. On the wire Reticulum copies that raw object but overwrites only
`bots.chat_enabled` with the stored boolean and sends the same value separately
as `runtime_chat_enabled`. This is intentional: ordinary JSON parsing cannot
distinguish numeric `1` from `1.0`, although Reticulum treats only the integer as
true. The duplicate exact boolean keeps both v2 and a legacy parent fail-closed,
makes retries stable across restarts, and lets a normalized snapshot of the same
revision agree with the applied parent state. A new type-sensitive behavioral
field must receive an equivalent durable projection or a new protocol version;
it must not be inferred from lossy JavaScript numbers.

Both the immutable raw approved configuration stored in PostgreSQL and the
transmitted projection must independently fit within 16 KiB. Raw legacy JSON
over that bound makes migration fail closed. If the raw JSON is valid and fits
but adding the persisted `chat_enabled` projection crosses the bound, the row
is quarantined rather than truncated or logged, records the exact reason
`runtime_payload_too_large_migration`, and emits a durable STOP. Payload
content is never written to logs.

A config operation is acknowledged only by the exact `200`/`state=applied`
response. A stop returns `202`/`state=pending` until a complete unfiltered
inventory of the dedicated namespace proves zero executable runners for the
room, the original UID is absent or its exact name is occupied by a valid
fence, and every armed/issued intent is fenced. Visible terminal runners,
unknown same-room runners and ABA replacements are deleted with the UID actually
observed. Only then may the parent return the exact
`200`/`state=stopped`, `target_absent=true`, `managed_room_pods=0` terminal
acknowledgement. The parent additionally requires its internal
`pendingCreate=false` proof before emitting that wire response. Here
`target_absent` means the
executable runner is absent; its Kubernetes name may deliberately remain
occupied by the permanent fence. A legacy stop remains best-effort and always
returns non-terminal `202`.

Runner and guard POSTs carry a bounded Kubernetes server-side timeout, and the
HTTP client has a total deadline, but neither time nor a lost response, 404 or
empty LIST is causal cancellation proof. An ambiguous runner POST remains tied
to its durable armed intent until an exact same-name fence is observed. Because
arming and stop proof share one serialized operation queue, a complete
inventory can be terminal immediately once all issued intents are fenced and no
runner is present; repeating an empty observation or waiting longer adds no
authority. Restart recovery derives the same conclusion solely from the
persisted intent/fence state.

Both runner-namespace admission policies explicitly match and deny
`pods/eviction`, `pods/ephemeralcontainers`, `pods/resize`, and CONNECT through
`pods/exec`, `pods/attach`, `pods/portforward` or `pods/proxy`. This prevents an
ordinary namespace editor, drain or debugging client from deleting a permanent
fence or entering/changing an executable runner through a subresource. Pod
`status`, `binding` and `log` are deliberately not matched so kubelet,
scheduling, readiness and bounded operational log access continue to work.
Quiesce runners through the guarded stop protocol before node maintenance;
forced eviction or executable debugging requires the documented break-glass
policy-removal sequence.

## First process-local cutover gate

An existing legacy `process-local` namespace may enter `bootstrap` only with a
fresh authenticated receipt produced by the YenHubs root integration tooling.
Set both `PROCESS_LOCAL_CUTOVER_ATTESTATION_PATH` and
`PROCESS_LOCAL_CUTOVER_KEY_PATH` to different owner-only, single-link regular
files with mode `0400` or `0600`. The HMAC key is never passed on argv or
printed. The receipt has an exact schema and binds the canonical AUD-065 profile
ID and digest, historical inventory counts, the verified redacted AUD-065
report SHA-256, Kubernetes context, namespace name/UID, and the exact
`bot-orchestrator` Deployment UID/resourceVersion and process-local result. Its
`hmacSha256` is HMAC-SHA256 over recursively key-sorted canonical JSON after
removing only `hmacSha256`; evidence older than five minutes is rejected.

The credential matrix is exact: pristine legacy cutover requires the fresh
attestation plus its HMAC key; a clean/new installation requires only that key,
loaded before creating its Namespace or journal; a P0-P5 resume requires the
same key even when the original receipt is stale or no longer present; P6 and
future fence-aware updates require neither file. Create a key without printing
it (for example, under `umask 077` with `openssl rand -out <private-path> 32`),
keep it as an owner-only `0400`/`0600` file through P6, and never put it in the
operational checkpoint. Its secure retention is separate from exporting the
non-secret journal evidence.

The apply command validates that receipt and the live absence of the runner
namespace, isolated RBAC/admission objects, runner candidates and all
create/delete/patch authority before any cluster mutation. It repeats the full
check after acquiring the operation Lease and immediately before installing
the durable policies. A new empty installation uses a tracked Namespace marker
and repeats an equivalent no-receipt absence/authority gate under the Lease.
Clean installation still reads and validates the private HMAC key before that
marked Namespace mutation. The marker permits a crash retry after Namespace
creation but cannot authorize an existing process-local Deployment.

Before the first fence mutation, apply creates one canonical immutable
ConfigMap named `yenhubs-runner-cutover-v2`. It carries the dedicated finalizer
`yenhubs.org/cutover-journal-protection` and a domain-separated HMAC over the
operation UUID, authorization-receipt hash (or `null` for clean install),
historical kube-context alias, Namespace name/UID, pristine Deployment UID and
resourceVersion (or an absent clean baseline), captured manifest SHA-256, five
target hashes (journal policy/binding, parent policy/binding and parent
Deployment), and issue time. The key remains only in the private file; it is
never stored in Kubernetes, argv or logs.

The only accepted transition is the exact prefix sequence: journal ConfigMap;
journal policy observed; journal binding observed; parent policy observed;
parent binding observed; Deployment CAS replace by baseline UID/resourceVersion
or clean create; then stable physical absence of every legacy parent Pod. A
lost response or `409` is resolved only by an exact GET of the intended object.
Any non-prefix state, terminating object, replaced Namespace UID or target
drift aborts before the next mutation. The finalizer prevents Namespace ABA
even though the Kubernetes namespace deleter uses DeleteCollection with an
empty `request.name`; both journal and parent policies explicitly deny their
respective bulk-delete form as well.
The same parent policy explicitly matches `deployments/scale` and denies every
scale-subresource update; replica changes must arrive only through the complete
verified Deployment transition, never through `kubectl scale` or an HPA.

Once P0 exists, receipt age may exceed five minutes, but the private key must
remain available to authenticate any P0-P5 resume. Do not delete or regenerate
the journal, manifest, image targets or recovery epoch before reaching P6,
because resume remains bound to the captured manifest and all five target
hashes. Preserve/export the journal with the operational checkpoint. After P6,
ordinary fence-aware releases no longer need that private key, do not depend on
the historical kube-context alias, and may change images and manifest hashes;
the protected journal remains durable evidence of the first cutover.

The Cloud repository defines and tests the consumer contract only. The root
producer must still be implemented: it must rerun
`verify-redacted-rollout.mjs` over the private AUD-065 operation artifacts,
verify the exact live process-local profile, then emit the fresh receipt/HMAC
bound to the current Namespace and Deployment. Until that producer has run,
legacy-to-bootstrap is intentionally blocked; this source change does not claim
that a production rollout is executable yet.

## Compatible rollout and rollback order

Deploy Reticulum first. The new Reticulum accepts both the legacy runner key
and generation-token v1, so this is the only compatible transition window.
Verify Reticulum readiness and a generation-token authentication probe before
deploying the digest-pinned parent and runner images. A new runner against old
Reticulum cannot authenticate and must remain fail-closed; abort rather than
trying to bypass that window.

After the durable intent/fence protocol is activated, an ordinary rollback may
target only a digest that is itself fence-aware and preserves all three
separately named durable admission policies (including the parent's
`deployments/scale` denial), the parent marker and the runner v2 marker.
A pre-AUD078 parent/runner rollback is rejected even when no current intent or
fence is visible: the API denies its old Deployment and Pod shapes, and the
permanent fences remain. Removing the durable policies or fences is a separate
break-glass operation. It requires independent proof that no current or older
control plane retains runner CREATE authority, followed by coordinated policy
and fence retirement; it is never part of normal rollback. Reticulum must not
be rolled back while generation-token runners remain.

A Reticulum schema downgrade is separately fail-closed. Before running the
runtime-outbox migration down or starting an older Reticulum, stored bot
configuration must be disabled, every approval quarantined, the durable outbox
must contain no pending operation, and the database must report zero active bot
runner lease. Restoring an older Reticulum image alone does not satisfy those
preconditions and is not a rollback procedure.

Namespace or guard retirement is a destructive break-glass campaign, never an
ordinary rollback. First create and validate a full DB-plus-media checkpoint,
export the exact journal, and prove zero remaining runner/parent authority and
zero executable or unresolved runner Pods. Record every inert fence that will
be retired. ConfigMap and Deployment DeleteCollection (including namespace
purge) are deliberately blocked and may leave the Namespace `Terminating` by
design. Only then remove each relevant binding followed by its policy, in this
order: `yenhubs-runner-cutover-journal-v2`,
`bot-orchestrator-fence-protocol.yenhubs.org`,
`bot-runner-durable-protocol.yenhubs.org`, and
`bot-runner-pods.yenhubs.org`. With the journal's exact live
UID/resourceVersion recorded, use a compare-and-swap metadata update to remove
only `yenhubs.org/cutover-journal-protection`, continue the coordinated purge,
and finally remove the campaign's remaining cluster-scoped artifacts. Skipping
the checkpoint, zero-authority/Pod proof, journal export,
binding-before-policy order or UID/resourceVersion precondition is unsupported.

Chromium remains a manual diagnostic through `run-bot.js`. It has no `--runner`
mode and explicitly removes parent, provider, legacy runner and generation
credentials before starting Puppeteer.

## Local verification

```bash
npm ci
npm test
```

From `community-edition/`, also run `npm run test:generator`, generate with the
non-secret CI fixture into a temporary path, and run
`generate_script/verify-generated-manifest.js` against that temporary file.
