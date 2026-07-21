![Hubs Cloud Community Edition](https://uploads-prod.reticulum.io/files/05884d13-e5e8-4f64-9aca-792aae6d7734.png)

# Hubs Cloud Community Edition

Community Edition is a free tool designed to help developers deploy the full Hubs stack on cloud computing software of their choosing. Community Edition simplifies and automates most of the complex deployment process using [Kubernetes](https://kubernetes.io/), a containerized software orchestration system.

Community Edition is designed for developers capable of working with the full Hubs stack and of navigating complex application infrastructure. With Community Edition, developers are responsible for designing, building, hosting, and maintaining their software throughout its lifetime. In order to create a production-ready version of Hubs similar to Hubs Cloud, Developers using Community Edition will need to implement additional features and customizations outside of those listed in this repo. See ["Considerations for Production Environment"](https://github.com/mozilla/hubs-cloud/tree/master/community-edition#considerations-for-production-environment) for more details.

## Why Kubernetes

[Kubernetes](https://kubernetes.io/) is an industry standard for allowing developers to build, deploy, and scale applications efficiently and reliably. Benefits to Kubernetes include:

- [Portability, Extensibility, and Open Source](https://kubernetes.io/docs/concepts/overview/)
- [Availability in many cloud environments](https://kubernetes.io/docs/setup/production-environment/turnkey-solutions/)
- Many options for single server deployments, such as [Minikube](https://minikube.sigs.k8s.io/docs/start/), [K3s](https://k3s.io/), [Microk8s](https://microk8s.io/), and [kind](https://kind.sigs.k8s.io/)

## Prerequisites


- Node.js installed on your system. You can download it from [here](https://nodejs.org/).
- Clone the repository:
   ```sh
   git clone https://github.com/hubs-foundation/hubs-cloud
   ```
- Navigate to the project directory:
   ```sh
   cd hubs-cloud/community-edition
   ```
- Install the module dependencies:
   ```sh
   npm ci
   ```
Before applying the configuration file to your Kubernetes cluster, you will need to choose and configure the following services...
- A hosting service with a Kubernetes cluster to receive your Community Edition deployment spec.
- Kubernetes controls on your device. Install kubectl to interact with your Kubernetes cluster from [here](https://kubernetes.io/docs/tasks/tools/#kubectl)
- A DNS service to reach Hubs on a domain
- Port to expose services to client
  - TCP: 80, 443, 4443, 5349
  - UDP: 35000 -> 60000
- An SMTP service for login emails and accounts


## Deploy to Kubernetes

To deploy to your K8s cluster on your chosen hosting solution, follow these steps:

> [!IMPORTANT]
> The YenHubs production profile does not permit direct edits to generated
> `hcce.yaml`, raw `kubectl apply -f`, `kubectl set image`, manual workload
> deletion, or ad-hoc scaling. Generate from the tracked source and private
> input, run the tracked verifier, review the approved redacted diff without
> exposing Secret bodies, and mutate the cluster only through `npm run apply`.
> Runner-control-plane changes use freshly generated complete manifests in the
> required `bootstrap -> admission -> active` order. The YenHubs root
> `deployment/README.md` is authoritative for the exact rollout procedure.

- In `input-values.yaml` edit `HUB_DOMAIN`, `ADM_EMAIL`, `SMTP_SERVER`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS` and optionally `SKETCHFAB_API_KEY` with the values for your site. Change `NODE_COOKIE`, `GUARDIAN_KEY`, and `PHX_KEY` to unique random values. Configure four different random values of at least 32 characters for `BOT_ACCESS_KEY` (legacy integrations only), `BOT_RUNNER_ACCESS_KEY`, `BOT_ORCHESTRATOR_ACCESS_KEY`, and `DASHBOARD_ACCESS_KEY`; the generator rejects reuse between these trust domains.
- Keep production values in the private `0600` input selected by `HCCE_INPUT_VALUES_PATH` (YenHubs uses `deployment/input-values.local.yaml`). Set both bot image overrides to accepted immutable digests, then run `npm run set-bot-image-pull-config` with `GHCR_USERNAME` and a hidden `GHCR_TOKEN` as documented in [`services/bot-orchestrator/README.md`](services/bot-orchestrator/README.md). The generated kubelet-only pull Secret is mandatory and the generator rejects empty or wrong-registry credentials.
- Keep `GENERATE_PERSISTENT_VOLUMES: true` and set `PERSISTENT_VOLUME_STORAGE_CLASS` explicitly. Use `default` for the cluster default or an explicit dynamic storage class supported by the cluster. Retained `manual` hostPath volumes are test-only and require the additional explicit `ALLOW_MANUAL_HOSTPATH_STORAGE: true`; omission never falls back to node-local storage.
- Generic CE installations can run `npm run gen-hcce` and then the repository's
  apply wrapper. YenHubs operators must additionally complete the verifier,
  redacted-diff, checkpoint and phased-rollout gates described above before
  running `npm run apply`. From the output read your load balancer's external IP
  address.
- Expose the services
  - On your DNS service, create four A-records to route your domains to the external IP address of your load balancer
    - <root_domain>
    - assets.<root_domain>
    - stream.<root_domain>
    - cors.<root_domain>

- Configure your HTTPS certs
  - Option #1: bring your own
    - package the certs into kubernetes secrets named `cert-<domain>` under the deploy namespace
  - Option #2: use Hubs' certbotbot
    - run `npm run gen-ssl` to get an SSL certificate provisioned for your domains
      - If it fails with an error like `namespaces "hcce" not found`, it's probably because the namespace hasn't finished generating from your initial application of the hcce.yaml file, so try running it again in a few seconds.
    - Generic CE installations may need to adjust their tracked generator for
      the selected certificate setup and regenerate. For YenHubs, never comment
      out a line in generated `hcce.yaml`; change tracked generator/input state,
      regenerate, verify, review the redacted diff and use `npm run apply`.


## Managing Kubernetes

While working with Community Edition and kubernetes you will likely need to perform debugging and maintenance on your cluster, we have found these commands useful to this process.


### Info Commands
- `kubectl config current-context` - Displays the current context.
- `kubectl get ingress -n hcce` - Shows information about which of your pods are connected to the internet and how.
- `kubectl get secrets -n hcce` - Used to get information on your SSL certificates.
- `kubectl get deployment -n hcce` - Used to list the services of your kubernetes deployment and their status.
- `kubectl describe deployment <servicename> -n hcce` - Used to get information on a service in your deployment.
- `kubectl get pods -n hcce` - Used to list your pods and their status.
- `kubectl describe pod <podname> -n hcce` - Used to get information about a pod.  This includes which containers it has.
- `kubectl logs <podname> -n hcce` - Used to get the logs from a pod.
- `kubectl logs <podname> <containername> -n hcce` - Used to get the logs from a container within the pod.
- `kubectl top pods -n hcce` - Used to get information on the CPU, Memory, etc. of all your pods.
                               This may require additional configuration on your kubernetes provider to be used.
- `kubectl top pod <podname> -n hcce` - Used to get information on the CPU, Memory, etc. of a specific pod.
                                        This may require additional configuration on your kubernetes provider to be used.
- `kubectl get svc lb -n hcce` - Used to get info on your load balancer, IP addresses, ports, etc..
- `kubectl get pv -n hcce` - Used to list your persistent volumes.
- `kubectl describe pv <pv-name> -n hcce` - Used to get info on a persistent volume.
- `kubectl get pvc -n hcce` - Used to list your persistent volume claims.
- `kubectl describe pvc <pvc-name> -n hcce` - Used to get info on a persistent volume claim.

### Mutating commands

Generic CE operators must design mutation procedures for their own topology.
For YenHubs, do not use raw apply, image replacement, workload deletion or
manual scaling as a repair shortcut. Those operations can bypass the generated
manifest verifier, the global operation Lease, the durable runner fences and the
fail-closed activation checks. Regenerate the complete manifest and use the
guarded `npm run apply` workflow. Use the tracked lifecycle and recovery
runbooks for shutdown, restart, restore or client deletion; stop and report the
exact failure if that workflow cannot converge.


### Graphical Clients

Kubernetes clusters can also be managed via GUI programs.  Here are some possibilities:

- [Podman Desktop](https://podman-desktop.io/) - Versions 1.8+.  Recent versions of Podman Desktop have both docker and kubernetes support.
- [Seabird](https://getseabird.github.io/)
- [K9s](https://k9scli.io/) - Terminal UI.
- [Headlamp](https://headlamp.dev/)
- [JET Pilot](https://www.jet-pilot.app/)


## Operations

### YenHubs bot authority and waypoint-reservation rollout

The complete YenHubs profile currently keeps Reticulum at exactly one replica
with the exact `Recreate` deployment strategy. Bot-runner authority now uses a
shared PostgreSQL lease and fencing epoch, so the singleton deployment setting
is no longer the authority primitive. Generated-manifest verification still
rejects any other replica count, rolling-update fields, or a
HorizontalPodAutoscaler targeting Reticulum because readiness, Endpoint checks
and the `ret-pvc` RWO placement constraint have not yet been staged for two
cold replicas. This topology deliberately accepts controlled Reticulum downtime.
Abort whenever inventory shows more than one Reticulum pod or Endpoint until a
separate rollout proves the multi-replica storage and operational gates and
updates the tracked generator and verifier. Do not remove the singleton boundary
merely because database fencing is present.

The bot trust boundary uses four distinct credentials. The historical
`BOT_ACCESS_KEY` remains scoped to integration routes such as hub bindings and
is never mounted into the bot orchestrator. The parent orchestrator receives a
dedicated inbound/signing key plus `OPENAI_API_KEY`; it never receives
`BOT_RUNNER_ACCESS_KEY`. Bot snapshots use the parent orchestrator credential.
Each ghost Pod receives only a short-lived HMAC credential scoped to its exact
room, process generation, orchestrator-Pod holder and expiry. Reticulum accepts
that credential only once for the matching room join; the credential is sent
in the Phoenix join payload and authenticated control headers, never in a URL
or client-visible state. A durable PostgreSQL generation ledger is consumed in
the same locked transaction before lease epoch allocation, rejects replay even
after release, revoke, expiry or restart, and does not consume a replacement
generation while another lease is active. The master runner key cannot acquire
Phoenix bot-runner authority. The generation credential contains no lease or
fencing claims. After join,
Reticulum assigns the mandatory shared-database lease UUID and authority epoch;
Presence, ACKs, commands and parent readiness must all agree on that exact
fence. `BOT_RUNNER_ACCESS_KEY` remains only in Reticulum for the legacy runner
transition and must not be mounted into either bot image.
Dashboard administration has a fourth independent key and never falls back to
any bot key. Generated manifests use an exact environment, mount and volume
allowlist for the secret-bearing parent process.

The source candidate uses a durable PostgreSQL outbox and a positive
JavaScript-safe per-room `runtime_revision` to order bot config and stop events.
Approval, quarantine, authority revocation and the corresponding immutable
outbox event commit in one transaction. Recoverable claims retry in strict room
order, and Reticulum accepts only the exact terminal acknowledgement; a timeout,
legacy 2xx or accepted Kubernetes DELETE is not proof that a stop completed.
Both the stored approved JSON and its normalized runtime projection are limited
to 16,384 encoded bytes. A legacy row whose raw form fits but whose runtime
projection does not is quarantined with
`runtime_payload_too_large_migration`, fenced and delivered as a durable stop
instead of being truncated or retried forever.

For each room generation, the parent first creates an inert `unarmed` intent
Pod. Only an exact UID/resourceVersion JSON-Patch transition to `armed`, followed
by an exact read, authorizes one runner POST. A lost or ambiguous POST is resolved
by installing a permanent, non-executable same-name fence before the intent is
removed. Ordinary successful create/stop cycles do not consume permanent
fences. The parent Role therefore has only the exact create/get/list/patch and
UID-preconditioned delete operations needed by this protocol. Runner, intent
and fence shapes are constrained by admission; malformed or incomplete
inventory fails closed. Room SIDs do not appear in Pod names or labels.

Each executable Pod still uses the dedicated digest-pinned `bot-runner` image,
UID/GID 10001, its own PID namespace and cgroup, explicit requests/limits,
read-only root, drop-ALL capabilities, RuntimeDefault seccomp and a bounded
`/tmp`. Its ServiceAccount has no token or RBAC. Guards use a separate no-token
ServiceAccount, an impossible scheduler and a scheduling gate. Executable and
guard Pods have separate quotas; new starts stop before the guard quota is
exhausted so recovery retains reserved fence capacity. Permanent fences have no
TTL and are never deleted by ordinary start, stop, update or rollback. Both
runner admission policies also deny Pod eviction, exec/attach/port-forward/proxy,
ephemeral-container injection and in-place resize subresources; status, binding
and logs remain available to the Kubernetes system and operators.

Checkpoint and restore byte streams use a separate parameter-free
`recovery-operation-pod-fence.yenhubs.org` admission pair. It never reads the
secret-bearing recovery lock and adds no ConfigMap RBAC. Its Binding is
permanently present but selects no Namespace (`kubernetes.io/metadata.name`
`DoesNotExist`) in bootstrap, admission and active manifests. A restore-fence
manifest selects exactly the parent and `hcce-bot-runners` Namespaces. While
active, the policy denies creation of the five database-writing workload Pods
(`reticulum`, both pgbouncers, `bot-orchestrator` and `coturn`) and denies main
and dangerous-subresource runner Pod mutations; status, binding and logs stay
outside the rule. Cleanup and permanent-fence reconciliation therefore finish
before activation.

Binding state changes are fail-closed compare-and-swap operations: read one
exact UID/resourceVersion, replace that same object, re-read the same UID and
wait for two server-side dry-run probes. Conflicts, deletion/recreation, drift,
type-check warnings or ambiguous propagation stop the operation. Reactivation
returns the Binding to its exact dormant selector and proves the writer probe is
accepted before runner authority or Deployments can resume. The initial
campaign checkpoint remains the pre-mutation legacy checkpoint; this pair is a
gate only for later durable-v2 checkpoint/restore windows.

`services-ci` compiles and exercises this exact generated pair in an ephemeral
Kind cluster pinned to Kubernetes 1.34.8. The job verifies the tool downloads,
requires observed CEL status without warnings, tests dormant/active propagation
and rejects stale resourceVersion, replaced UID and ABA transitions before the
cluster is deleted.

Runner ingress is denied. Egress is limited to kube-dns, the parent control
service, and public TCP 443 needed for the Hubs/scene endpoints. The
runner polls an authenticated parent endpoint for exact configuration and
posts generation-bound readiness status; three consecutive control failures
terminate it. A readiness file is exposed only after authenticated Reticulum
presence, exact config ACKs, required navmesh and authoritative bot spawn ACKs.
The parent reconciles on startup and every five seconds, fences ambiguous
creates, deletes executable or disposable orphaned Pods with UID preconditions,
enforces the existing room ceiling, and rotates a runner when its one-hour
credential/active deadline expires. The first transition from `process-local`
or a clean install is guarded by an authenticated, crash-resumable cutover
journal and separately protected admission policies before the parent
Deployment can change. The journal and permanent fences survive ordinary
updates. The parent policy also matches `deployments/scale` and rejects that
subresource outright, so `kubectl scale`, an HPA or another scale client cannot
bypass the complete guarded manifest or create a second authoritative parent.

Every stable-absence gate starts an exact watch from its validated `LIST`
resourceVersion and waits for an in-band `BOOKMARK`; elapsed time or a clean
connection close is never accepted as proof of progress. Initial and final
handoffs start the successor at the predecessor's last bookmarked, non-zero
resourceVersion, keep the predecessor live and adopt the successor only after
the successor produces its own bookmark. Resource versions remain opaque and
are only passed back to the same collection. A transient runner or recovery
consumer, missing bookmark, expired resource version, malformed event, early
close, stalled process or local timeout therefore fails closed. Kubernetes does
not guarantee bookmark delivery, so lack of one within the operation deadline
is intentionally an availability failure rather than a safety bypass.

For YenHubs, the journal ConfigMap has a permanent protection finalizer and its
admission policy denies direct deletion and ConfigMap `DeleteCollection`,
including the collection deletion used while purging a Namespace. A request to
delete that Namespace therefore remains `Terminating` by design. This is not a
stuck-resource repair signal and must never trigger manual finalizer removal or
an ordinary rollback. The separately reviewed break-glass order is documented
only in `services/bot-orchestrator/README.md`.

There is intentionally no mixed legacy/generation authority window: old
process-local runners cannot authenticate to this Reticulum, and new runners
cannot authenticate to older Reticulum. After the durable cutover is installed,
an ordinary rollback may target only a fence-aware parent/runner digest that
preserves the journal, durable admission policies, protocol markers and
permanent fences. Returning to a pre-AUD078/process-local parent is a separate
break-glass campaign requiring independent proof that no current or older
control plane retains runner CREATE authority; it is never an ordinary
rollback. A mixed version is an expected fail-closed incompatibility, never a
reason to bypass authentication.

This is still source-candidate behavior. It is not operationally complete until
Reticulum, parent and runner are built from the accepted same commit, pinned by
digest, generated through all three phases and accepted live. This change does
not claim that those images were built or that any cluster or production state
was mutated.

#### AUD078 preservation contract for future updates

YenHubs continues to use stable Hubs CE release `2.1.0` as its accepted upstream
baseline. AUD078 is a local compatibility surface, not a dependency or upstream
upgrade. A future stable release must be evaluated in its own branch and must
preserve or explicitly migrate all of these contracts:

- `bot_config_approvals.runtime_revision`, the immutable
  `bot_runtime_outbox`, exact JSON typing/size bounds and PostgreSQL 12/14
  migration/down guards;
- runtime protocol `yenhubs-bot-runtime-v2`, exact terminal config/stop ACKs,
  per-room snapshot ordering and provider-neutral privacy behavior;
- intent/fence Pod shapes, UID/resourceVersion CAS, dedicated quotas and RBAC,
  and all runner, parent-fence, recovery-operation-fence and cutover-journal
  admission policies;
- the authenticated first-cutover journal, generated-manifest verifier, global
  operation Lease and `bootstrap -> admission -> active` recovery path.

Run the Reticulum migration verifier on PostgreSQL 12 and 14 plus the complete
Reticulum, bot-orchestrator, apply and generator suites before accepting such an
update. Revalidate navmesh extraction/routing, isolated runner startup, terminal
stop/restart recovery, privacy, sitting and cold-browser behavior after any
Hubs, Spoke, Three.js, networked-aframe or Hubs CE change. Do not combine that
upstream update or a broad dependency modernization with an AUD078 feature or
production rollout.

Authenticated bot chat is private to the requesting browser and requires an
exact random capability for that Phoenix channel and account in the same room.
The capability is returned only in the private channel response, registered in
server-only `BotChatPresence` after `events:entered`, rotated on sign-in, and
invalidated on sign-out or channel termination. Neither it nor the account ID
is added to broadcast Presence metadata, so another browser using the same
account cannot reuse the proof. Permission to join without an entered channel
and its current capability receives HTTP 403.

`MAX_ACTIVE_ROOMS` is a hard admission and cost ceiling (five by default, ten
maximum), not only a readiness threshold. Reticulum serializes active-room
admission under a global PostgreSQL transaction lock and rejects N+1 before it
is persisted. Only a non-disabled global administrator may activate or modify
active bot configuration; ordinary room owners may preserve an approved
configuration while changing unrelated data or disable it. The generator and
manifest verifier bind the exact same 1-10 value into Reticulum and the
orchestrator, whose internal endpoint provides a second fail-closed check. If
inconsistent external state nevertheless exceeds the ceiling, readiness still
reports `capacity_exceeded` with HTTP 503 rather than declaring a partial fleet
ready. This candidate behavior is not deployed.

`DASHBOARD_ACCESS_KEY` is the single Reticulum administrative trust domain. It
authenticates the dedicated dashboard header and, for compatibility, the
legacy `x-ret-admin-access-key` wire header used by RetNotice,
SupportSubscription and Hub deletion. The two header names intentionally share
one value; no bot or runner service receives it, and the generator rejects any
binding to a bot credential.

Authoritative waypoint reservations use wire protocol 2. PostgreSQL assigns a
global, JSON-safe monotonic `state_version` to each state-change batch, and each
join includes a later `snapshot_state_version` barrier. Deploy and migrate
Reticulum first, verify it is ready, and only then deploy the matching Hubs
client. Protocol 1 and protocol 2 peers reject each other, so either mixed
deployment direction fails closed instead of accepting unversioned seat state.
The read-only `GET /health/capabilities` endpoint publishes the exact protocol
and state-version semantics so rollout tooling can negotiate the server/client
pair before enabling the Hubs client.

Do not edit generated `hcce.yaml`. Change the tracked generator or the private
input values, regenerate, run the tracked verifier, review the approved
redacted diff without emitting Secret bodies, and apply the generated manifest
through the guarded `npm run apply` procedure. The verifier rejects untracked
containers, environment variables, mounts, RBAC and storage resources.

#### Updating an active bot-runner control plane

An `active` apply that detects drift between the live and generated runner
Namespace, Secret, ServiceAccounts, quota, RBAC, NetworkPolicies or admission
policy deliberately stops with
`active_reapply_control_plane_drift_refenced_generate_and_apply_bootstrap_then_admission_then_active_do_not_retry_active`.
This is an expected safety boundary: the apply first makes runner authority
inert, scales the five recovery consumers to zero, deletes runner Pods with UID
preconditions and proves a continuous Pod-free window. Do not retry the same
`active` manifest and do not grant RBAC manually.

Keep the intended configuration and image digests unchanged, then use the
private input selected by `HCCE_INPUT_VALUES_PATH` to regenerate, verify and
apply these three phases in order:

1. Set `BOT_RUNNER_ACTIVATION_PHASE: bootstrap`, run
   `npm run gen-hcce`, verify, review the approved redacted diff, then run
   `npm run apply`.
2. Set `BOT_RUNNER_ACTIVATION_PHASE: admission`, regenerate, verify, review the
   redacted diff and apply again. This phase proves the admission denial before
   authority is considered usable.
3. Set `BOT_RUNNER_ACTIVATION_PHASE: active`, regenerate, verify, review the
   redacted diff and apply once more. Completion requires exact live resources,
   effective RBAC, admission denial and ready Deployments.

The stopped state intentionally retains the live `active` phase annotations so
the next generated `bootstrap` manifest is the only supported staged recovery
path. A repeated `active` apply will remain fail-closed.

An ordinary rollback follows the same guarded phases and may use only a
fence-aware parent/runner release that preserves the durable journal, admission
policies, protocol markers and permanent fences. A pre-AUD078 image is not a
normal rollback target. Removing those protections or retiring an old runner
namespace requires a separately reviewed break-glass or namespace-epoch
campaign after proving all older runner CREATE authority is gone.

If you just need to get the external IP address of your load balancer, run

`npm run get-ip`

### Backing up and restoring your instance

> [!IMPORTANT]
> A valid YenHubs checkpoint always contains both PostgreSQL metadata and the
> Reticulum media bytes from `ret-pvc`, plus exact commits/image digests,
> non-secret inventory, the generation-bound cutover-journal evidence and
> `SHA256SUMS`. From the YenHubs root, create it with
> `./deployment/create-checkpoint.sh` and validate the documented restore
> dry-run before any production mutation. Follow
> `deployment/client-instance-lifecycle.md` for freeze, restore or client
> deletion. A DB-only or storage-only archive cannot authorize a YenHubs
> rollback.

For generic CE installations, `npm run backup` creates a timestamped archive in
`data_backups`.

Generic CE installations can use
`npm run restore-backup data_backup_1234567890123`; without an argument it uses
the latest archive. The generated configuration must match that installation.
This generic path is not the YenHubs coordinated restore workflow.

If a generic CE installation uses an external database instead of the `pgsql`
Pod, these npm scripts back up and restore only Reticulum files. That partial
result is explicitly not a valid YenHubs checkpoint.

## Guides from the Hubs Team and Community

### 1. Beginner's Guide to CE

The [Beginner's Guide to CE](https://docs.hubsfoundation.org/beginners-guide-to-CE.html) takes you through the process of setting up Hubs, and all of it's required services, using the current Node.js version of Community Edition.  It is targeted at beginners, people without any programming experience, and the Windows operating system, but it should be useful to experienced developers, and those on other operating systems as well.  It uses DigitalOcean (for a kubernetes cluster), Porkbun (for the domain), and Scaleway (for the transactional email/smtp) as the additional services required by Hubs.

### 2. Deploying A "Hello-World" Instance Using Managed Kubernetes on GCP with AWS' DNS & SMTP

> [!IMPORTANT]
> This guide is based on the bash version of Community Edition, to follow along you will need to use the bash scripts from https://github.com/Hubs-Foundation/hubs-cloud/tree/bash-version

[The Hubs Team's case study](https://hubs.mozilla.com/labs/community-edition-case-study-quick-start-on-gcp-w-aws-services/) outlines the process of deploying your first, experimental instance on GCP's GKS. This tutorial walks you through the process of setting up DNS on AWS Route 53 and SMTP on AWS SES, deploying and trouble-shooting your instance on GCP, and configuring custom code & server settings.\
[Companion Video](https://youtu.be/8XNEWmf9tk4)

### 3. Community Edition Tips and Tricks by [@kfarr](https://github.com/kfarr)

> [!IMPORTANT]
> This guide is based on the bash version of Community Edition, to follow along you will need to use the bash scripts from https://github.com/Hubs-Foundation/hubs-cloud/tree/bash-version

[Documentation Awardee Kieran Farr's guide](https://hubs.mozilla.com/labs/tips-and-tricks-for-deploying-hubs-community-edition-to-google-cloud-platform/) shares the helpful tips and tricks he learned while following the Hubs Team's case study on GCP. This is an excellent repository of helpful commands and debugging techniques for new Kubernetes users.
[Companion Video](https://youtu.be/w4NlAhKaBrg)

### 4. Community Edition Helm Chart by [@Doginal](https://github.com/Doginal)

Documentation Awardee Alex Griggs maintains [an open-source Helm Chart for HCCE](https://github.com/hubs-community/mozilla-hubs-ce-chart). Helm is an abstraction above Kubernetes that improves maintainability, scalability, and ease-of-use of applications using K8s. Alex has also released three tutorials showing how to use his Helm chart to create production-ready CE deployments, including for large scale events:

1. [Deploying Mozilla Hubs CE on AWS with Ease: A Guide to the Personal Edition Helm Chart](https://hubs.mozilla.com/labs/deploying-mozilla-hubs-ce-on-aws-with-ease-a-guide-to-the-personal-edition-helm-chart/)
2. [Deploying Mozilla Hubs CE on AWS with Ease: A Guide to the Scale Edition Helm Chart](https://hubs.mozilla.com/labs/deploying-mozilla-hubs-ce-on-aws-with-ease-a-guide-to-the-scale-edition-helm-chart/)
3. [Deploying Mozilla Hubs CE on GCP with Ease: A Guide to the Personal Edition Helm Chart](https://hubs.mozilla.com/labs/deploying-mozilla-hubs-ce-on-gcp-with-ease-a-guide-to-the-personal-edition-helm-chart/)\

[AWS Companion Video](https://youtu.be/0VtKQYXTrn4)\
GCP Companion Video (Coming Soon!)

### 5. Azure Hubs Community Edition Installation by [@TophoStan](https://github.com/TophoStan)

> [!IMPORTANT]
> This guide is based on the bash version of Community Edition, to follow along you will need to use the bash scripts from https://github.com/Hubs-Foundation/hubs-cloud/tree/bash-version

Documentation Awardee Stan Tophoven has published steps for uploading a Community Edition instance to Microsoft Azure's managed Kubernetes Platform.

1. [Installing Community Edition on Microsoft Azure AKS](https://hubs.mozilla.com/labs/installing-mozilla-hubs-community-edition-on-your-own-microsoft-azure-kubernetes-service/)
2. [Stan Tophoven's Guide to Deploying Community Edition on Azure AKS](https://www.youtube.com/watch?v=j8dQEEEX4OA)

### 6. Community Edition Setup on OVH by [@utopiah](https://fabien.benetou.fr/Tools/HubsSelfHosting)

> [!IMPORTANT]
> This guide is based on the bash version of Community Edition, to follow along you will need to use the bash scripts from https://github.com/Hubs-Foundation/hubs-cloud/tree/bash-version

Documentation Awardee Fabien Benetou has produced a guide for hosting Community Edition on OVH, including some excellent information on setup time, cost considerations, custom client deployment, and how Hubs can live on beyond Mozilla! Fabien has also produced long form and short form tutorial videos, including one in French.

1. [Written Document](https://fabien.benetou.fr/Tools/HubsSelfHosting)
2. [Long Form Tutorial in English](https://video.benetou.fr/w/c5YUiW7xaKAx91GPbCvWxd)
3. [Long Form Tutorial in French](https://video.benetou.fr/w/o8MDuxro6vaiT7Bu3PdyVw)
4. [Short Form Tutorial in English](https://video.benetou.fr/w/1vJC37pEhkEqJv6wU1h1c8)
5. [Custom Client Deployment](https://video.benetou.fr/w/qUkZiRTXGnu2xXXudJyPxM)

### 7. Azure Hubs Community Edition Installation by [@vvdt](https://github.com/vvdt)

> [!IMPORTANT]
> This guide is based on the bash version of Community Edition, to follow along you will need to use the bash scripts from https://github.com/Hubs-Foundation/hubs-cloud/tree/bash-version

Community Mamber Vincent van den Tol has released [instructions for installing Community Edition on Microsoft Azure](https://github.com/imedu-vr/hubs-docs/blob/main/azure_hubs_ce_installation.md), including persistent volumes, custom client deployment, and many helpful tips and tricks.

### 8. Import Assets from Hubs Cloud to CE by [chris-metabi](https://github.com/chris-metabi)

Chris from MeTabi [has created a guide](https://github.com/hubs-community/import_assets) for copying data from an existing Hubs Cloud instance and porting it to a Community Edition instance.

### 9. A "Hello-World" Instance With VM On GCP

> [!IMPORTANT]
> This guide is based on the bash version of Community Edition, to follow along you will need to use the bash scripts from https://github.com/Hubs-Foundation/hubs-cloud/tree/bash-version

##### Step 1: Make a kubernetes environment

Replace `hcce-vm-1` and `us-central1-a` with your desired name and zone. Check [the official doc](https://cloud.google.com/sdk/gcloud/reference/compute/instances/create) for more options.

##### login gcp

gcloud auth login

##### create a vm

`gcloud compute instances create hcce-vm-1 --zone=us-central1-a`

##### ssh to the vm

`gcloud compute ssh --project=hubs-dev-333333 --zone=us-central1-a geng-test-2`

##### prepare the vm

`sudo apt update && sudo apt install npm && sudo npm install pem-jwk -g`

##### install k3s without traefik -- read https://docs.k3s.io/ for more info

- `curl https://get.k3s.io/ | INSTALL_K3S_EXEC="--disable=traefik" sh -`

##### Step 2: Install k3s without traefik

- `curl https://get.k3s.io/ | INSTALL_K3S_EXEC="--disable=traefik" sh -`

- read https://docs.k3s.io/ for more info

##### Step 3: Deploy to kubernetes

- Add your services to `render_hcce.sh`
- Run `bash render_hcce.sh && sudo k3s kubectl apply -f hcce.yaml`

#### Step 3: connect the ingress
- find the vm's external ip
- create a-records to the dns
- makesure the required ports are exposed to the client

### example -- a "hello-world" instance with managed kubernetes on gcp
##### Step 1: make a kubernetes environment
replace `hcce-gke-1` and `us-central1-a` with your desired name and zone, check [official doc](https://cloud.google.com/sdk/gcloud/reference/container/clusters/create) for more options

##### login gcp

gcloud auth login

##### create gke cluster

gcloud container clusters create hcce-gke-1 --zone=us-central1-a

###### get creds for kubectl

gcloud container clusters get-credentials --region us-central1-a hcce-gke-1

- Find the vm's external IP
- Expose IP to DNS
- Configure firewall

### 10. A "potentially-somewhat-production-ready" instance on AWS

- Coming soon!

## Considerations for Production Environment

- Infrastructure
  - Easy -- use managed kubernetes
  - Hard -- make it [production-ready](https://kubernetes.io/docs/setup/production-environment/)
- Security
  - Password and Keys
  - Add a WAF
- Scalability
  - Stateful services
    - PostgreSQL
      - use a managed pgsql ie. rds on aws or cloudsql on gcp
      - roll your own
    - Reticulum
      - use a network/shared storage for reticulum's /storage mount
  - Stateless Services (all except reticulum and pgsql)
    - Run multiple replicas
    - Use HPS
- Devops
  - The two yaml files in this repo are the entire infra on kubernetes. You may want to use git to track changes and an ops pipeline to auto deploy.
    - ex. Put the yaml file on a github repo and use github action to deploy to your hosting env.
  - Use dev env for staging/testing.
    - Use spot instances for nodes to save money.
    - Develop and integrate automated testing scripts into the ops pipeline
  - Configure devops for deploying custom versions of Spoke, Hubs, and Reticulum
```
