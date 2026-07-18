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
labels, allocates a UUID process generation, signs a token containing room,
generation, parent-Pod UID and expiry, and creates one `restartPolicy: Never`
Pod. Create is idempotent: HTTP 409 is accepted only after an exact GET verifies
the expected UID-independent Pod contract. Readiness requires both Kubernetes
Pod readiness and the existing authenticated Reticulum/config/navmesh/spawn
status. Stop and recovery delete by exact name plus UID precondition.

At parent startup, every existing `app=bot-runner` Pod is treated as an orphan
and deleted rather than adopted: its credential is tied to the previous parent
UID. Periodic reconciliation deletes unknown, expired, terminal, owner-mismatch
or contract-mismatch Pods. Failure to list or reconcile the managed set exits
the parent process and makes its Service endpoint unready. A container restart
keeps the same parent Pod UID, so owner garbage collection is not claimed:
startup instead deletes every existing managed runner and proves the list empty
before reopening `/transport-ready`. Capacity is bounded by `MAX_ACTIVE_ROOMS`
in both desired state and the manager. The production startup grace is three
minutes because it includes initial scheduling and a cold private-image pull;
the shorter running config-ACK deadline applies only after startup.

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

## Compatible rollout and rollback order

Deploy Reticulum first. The new Reticulum accepts both the legacy runner key
and generation-token v1, so this is the only compatible transition window.
Verify Reticulum readiness and a generation-token authentication probe before
deploying the digest-pinned parent and runner images. A new runner against old
Reticulum cannot authenticate and must remain fail-closed; abort rather than
trying to bypass that window. Roll back in reverse compatibility order: restore
the old parent/runner first, verify legacy authentication, and only then restore
old Reticulum. Never roll back Reticulum while generation-token runners remain.

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
