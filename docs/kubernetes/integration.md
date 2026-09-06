# Integration with Kubernetes

This doc explains how Multigres's per-node services — the Multipooler and its
co-located pgctld — present themselves to Kubernetes, and in particular what the
readiness and liveness probes do and deliberately do _not_ mean. It is
deployment-agnostic: it describes the semantics the binaries expose, not any one
manifest. The [kind demo](../../demo/k8s/) and the
[Multigres operator](https://github.com/multigres/multigres-operator) both build
on these semantics.

If you are looking for a concrete cluster setup, see
[Getting started on EKS](./eks.md).

## Readiness and Liveness

Every Multigres service embeds a small HTTP server (`servenv`) that exposes two
probe endpoints alongside its gRPC control plane:

- **`/live`** — the process is up. It always returns `200` once the HTTP server
  is serving. It makes no assertion about any dependency. This backs the
  Kubernetes **liveness** probe: if the process is running, it is live.
- **`/ready`** — the process's gRPC control plane is accepting RPCs. It runs the
  registered readiness checks and returns `503` if any fails. This backs the
  Kubernetes **readiness** probe.

> [!NOTE]
> The key design decision is what `/ready` covers. For both the Multipooler and
> pgctld, **readiness reflects only whether that service's own gRPC control plane
> is accepting connections — not whether Postgres is up.**

### What `/ready` checks

Both services register a single readiness check that dials their own gRPC server
and confirms a listener is actually accepting connections. The probe prefers the
gRPC Unix socket and falls back to a TCP dial on the configured bind address
when only a port is set. A successful dial distinguishes a live listener from a
stale socket file left behind by a crash. The implementations are intentionally
kept as small, self-contained probes:

- Multipooler: [`go/services/multipooler/ready.go`](../../go/services/multipooler/ready.go),
  registered in [`go/services/multipooler/init.go`](../../go/services/multipooler/init.go).
- pgctld: `go/cmd/pgctld/command/ready.go`, registered in
  [`go/cmd/pgctld/command/server.go`](../../go/cmd/pgctld/command/server.go).

Postgres health, replication mode, replication lag, and quarantine lifecycle are
all **excluded** from the probe on purpose.

### Why Postgres health is excluded

A Multipooler whose Postgres is down must stay reachable. It still needs to
serve control RPCs and keep publishing on its health stream so the orchestrator
and the operator can observe it and drive it back to health. If Postgres-down
flipped the pod to _not ready_, Kubernetes would pull it from the Service
endpoints and DNS — exactly the wrong move, because it would hide the node from
the very control plane responsible for recovering it. So readiness is scoped to
the control plane's own reachability, and detailed component health travels
out-of-band.

The same reasoning applies to pgctld: a pod whose Postgres is down must not be
pulled from Service endpoints, so operators and the control plane can still
reach it to observe and restart Postgres.

### Where component health actually goes

Detailed per-node health — Postgres up or down, recovery mode, replication lag,
and the quarantine lifecycle — is carried **out-of-band on the health stream**,
not through the Kubernetes probes. The health stream is consumed by
[Multiorch](../ha/recovery.md) (for consensus and failover decisions) and by the
operator (for pod-level remediation). Surfacing this state through probes would
conflate "is this node reachable?" with "is this node's database healthy?",
which are two different questions with two different consumers.

### Consequence: a dead-Postgres pod stays Running and Ready

Because the probes never look at Postgres, **a pod whose Postgres is dead stays
`Running` and `Ready`.** That is by design — but it means recovering it is a
_health-stream-consumer_ responsibility, not something the kubelet will do for
you:

- When Postgres is unrecoverable, the Multipooler latches itself into
  `LIFECYCLE_QUARANTINED` and publishes that on its own topology record (see
  [`quarantine.go`](../../go/services/multipooler/internal/manager/quarantine.go)).
  The **operator** watches for that marker and drives replacement — delete the
  pod, wipe its data directory, and let it re-bootstrap from a backup.
- On the [kind demo](../../demo/k8s/), there is **no operator** consuming that
  signal. Nothing reacts to the quarantine marker, so such a node stays dead
  until you manually intervene:

```bash
kubectl delete pod <multipooler-pod>
```

The liveness probe will not save you here either: the process is still up and
serving RPCs, so it is genuinely live — Postgres being dead is not a
process-liveness failure.

## Packaging as a StatefulSet

The [kind demo](../../demo/k8s/k8s-multipooler-statefulset.yaml) packages each
node as a **StatefulSet** pod that co-locates two containers — the Multipooler
and its pgctld — sharing the same data directory. A StatefulSet is a natural fit
because a Multigres node has **durable identity**: it owns a specific Postgres
data directory and registers itself in the topology, so a pod that restarts must
come back as the _same_ node rather than a fresh interchangeable replica.

Two StatefulSet properties are what the design actually relies on:

- **Stable pod identity.** Each replica gets a fixed ordinal name
  (`multipooler-zone1-0`, `-1`, …). The demo threads that name through
  `POD_NAME` / `POD_INDEX` into the container — for example the per-pod Postgres
  data directory is carved out with `subPathExpr: $(POD_NAME)` — so a pod keeps
  its own data across restarts instead of adopting another pod's.
- **Stable network identity.** A headless Service (`clusterIP: None`) gives each
  pod a predictable per-pod DNS name. That is what lets the control plane and
  peers address an individual node directly — for control RPCs, the health
  stream, and Postgres replication — rather than through a load-balanced VIP.

This is a demo packaging choice, not a fixed part of the design. The manifest
itself flags that a StatefulSet may not survive contact with multiple shards or
zones, in which case pod management moves to the
[Multigres operator](https://github.com/multigres/multigres-operator). The probe
semantics above hold regardless of how the pods are managed.

### Where the StatefulSet view and the Multiorch view diverge

The deeper reason a StatefulSet does not scale to the full design is that its
controller and Multiorch are **two independent controllers with two different
notions of node health and identity**, and neither defers to the other.

The StatefulSet controller sequences pod lifecycle purely by **ordinal and
readiness**: a `RollingUpdate` restarts pods in reverse-ordinal order, waiting
only for each to report Ready before moving to the next, and a scale-down always
deletes the highest ordinal first. But — as established above — Ready means only
"this pod's gRPC control plane is accepting". It says nothing about _consensus
role_. From the view of the StatefulSet, `multipooler-zone1-0` (say, the current
leader) and `-2` (a follower) are interchangeable Ready pods. The view of
Multiorch is the opposite: it cares which node is the leader, which followers
are caught up, and whether the surviving cohort still satisfies its durability
policy.

That mismatch bites concretely:

- **A rolling restart can evict the leader.** If ordinal `-0` happens to be the
  elected leader, `kubectl rollout restart` deletes and recreates it like any
  other pod, with no demote-and-hand-off first. Multiorch only learns of it
  after the fact — the health stream from `-0` goes silent — and is forced into
  an _unplanned_ failover, where a role-aware sequence would have handed
  leadership off cleanly before the pod went down.
- **A scale-down can pick the wrong victim.** `kubectl scale --replicas=2`
  removes `-2` regardless of whether dropping that node leaves the cohort below
  the quorum its durability policy requires. The StatefulSet has no way to ask
  "is this node safe to remove?" Multiorch, which re-checks durability
  achievability against the new cohort on every membership change, would have
  refused or chosen a different node.
- **Recreating a pod restores its broken data.** The same stable-identity
  property that makes StatefulSets attractive fights recovery: deleting a pod
  brings it back with the _same_ data directory. That is right for a transient
  crash, but wrong for the unrecoverable-Postgres case above — a plain
  `kubectl delete pod` heals the pod straight back into its broken state.
  Genuine replacement (wipe + re-bootstrap from backup) is a decision only a
  health-stream consumer can make, which is why the operator, not the
  StatefulSet controller, owns it.

In each case the StatefulSet is doing exactly what it was designed to do. It
simply lacks the consensus-role and durability context that lives in Multiorch.
An operator that manages pods directly can consume the health stream and
sequence these actions with that context, which is the direction the manifest's
own comment points to.

### Graceful shutdown can march the cohort below quorum

The sharpest version of this mismatch shows up during a _graceful_ shutdown —
what a rolling update, a node drain, or a scale-down actually does. Each sends
SIGTERM to the **Multipooler process**, which runs its `GracefulShutdown`
sequence (registered as a `senv.OnTermSync` hook in
[`manager.go`](../../go/services/multipooler/internal/manager/manager.go)): it
drains, advertises cohort ineligibility, stops Postgres, and then the process
exits, writing `LIFECYCLE_SHUTDOWN` to its topology record on the way out. Note
this takes the _whole node_ down — it is not a "stop Postgres only" operation
and not an RPC. A container whose process has exited is recreated (rolling
update / delete) or intentionally removed (scale-down), so the graceful path
itself self-heals. The wedge comes from the _pace_ of an ordered rollout, not
from any single node failing to restart:

- **Ready does not mean "rejoined the cohort."** A `RollingUpdate` waits only
  for each restarted pod to report Ready before it moves on to the next. But
  Ready means the gRPC control plane is accepting (see above). It says nothing
  about whether that pod's Postgres has finished catching up and rejoined the
  durability cohort. So the rollout can take the _next_ pod down while the
  previous one is Ready-but-not-yet-caught-up.

Put together, an ordered rollout can retire nodes faster than they actually
rejoin the cohort, walking the shard below its durability quorum. At that point
Multiorch does the correct thing — it refuses to elect a leader, because
promoting below quorum risks split-brain and data loss — and the shard wedges
with no writable primary. There is no automated escape from a genuine
below-quorum state: recovery needs enough members restored, by the operator or,
on the kind demo, by a human. A quorum-aware controller would gate each step on
cohort re-join rather than on the readiness probe. A StatefulSet has no way to
express that condition.

> [!NOTE]
> Do not confuse this with the "Ready-but-dead" case above. That one is _not_ a
> graceful shutdown: it is an out-of-band Postgres death (for example `SIGINT`
> to the postmaster) under a Multipooler that never received SIGTERM. The pooler
> and pgctld keep answering RPCs, the pod stays Ready, and — because
> `GracefulShutdown` never ran and nothing marks the node for replacement —
> nothing recreates it. There is no managed operation for "leave the cohort but
> keep the pod up and Ready"; a graceful shutdown always takes the whole node
> down.

### Probes on the demo manifest

Note that in that same StatefulSet the pgctld container defines **no** liveness
or readiness probe, even though the binary registers a `/ready` gRPC-accepting
check. This is a property of that particular manifest, not of the binary —
pgctld exposes the same `/live` and `/ready` semantics described above, and a
deployment (such as the operator's) may wire probes to them.

## See also

- [Recovery & orchestration](../ha/recovery.md) — how Multiorch consumes the
  health stream to detect failures and reconcile the cohort.
- [Getting started on EKS](./eks.md) — a concrete deployment path using the
  operator.
- [pgctld init](../pgctld-init.md) — data-directory initialization for the
  Postgres node.
