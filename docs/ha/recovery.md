# When We Change the Rules: Recovery & Orchestration

This doc covers _when_ and _why_ a rule change is triggered — the
responsibility of **multiorch**, the orchestrator. It assumes you've read
[the consensus overview](consensus-overview.md) and
[the state model](state-model.md). _How_ a rule change is carried out safely is
[rule-change.md](rule-change.md); this doc decides when to start one.

> **Status:** skeleton. The high-level responsibilities below are accurate; the
> detailed mechanics are TODO and will be filled in incrementally.

## Purpose and boundary

multiorch watches the health of every pooler in a shard and drives the cohort
back to a healthy state when something changes. It owns the _decision_ to change
the rules; it delegates the _mechanism_ to the rule-change protocol. It has
three most-important responsibilities:

### 1. Failover when the leader is dead — a coordinator-led rule change

When the consensus leader is unreachable or unhealthy, multiorch acts as the
coordinator and runs a [coordinator-led rule change](rule-change.md) (recruit →
promote) to appoint a new leader. This is the failure path: the old leader
cannot cooperate, so an external coordinator must eliminate any rogue quorum and
elect the most-advanced surviving pooler.

> TODO: how a dead leader is detected — health streams, heartbeat timeout, the
> `LEADERSHIP_SIGNAL_REQUESTING_DEMOTION` fast path. The entry point is
> `AppointLeaderAction` → `Coordinator.AppointLeader`.

### 2. Cohort membership changes — a leader-led rule change

As poolers are provisioned and deprovisioned, multiorch adjusts cohort
membership. Unlike failover, the leader is healthy here, so the change goes
**through the primary pooler's update-rules endpoint** — a leader-led rule
change. The leader writes the new rule itself (new cohort, same term, bumped
`leader_subterm`), with no external coordinator needed.

> TODO: name and link the exact update-rules RPC and its handler; describe the
> add-member vs remove-member flows and how the durability policy's
> achievability is re-checked against the new cohort.

### 3. Keeping poolers pointed at the right primary — SetPrimary reconciliation

Even with no rule change in flight, multiorch continually reconciles each
pooler's replication target, calling `SetPrimary` to keep every follower and
observer pointed at the current leader. This repairs poolers that drifted —
restarted, were partitioned, or were told about a primary while postgres was
down — without electing anyone or bumping the rule. `SetPrimary`'s rule comparison
makes a redundant call a safe no-op, so this can run freely.

> TODO: document the reconciliation trigger (`FixReplicationAction`), how the
> coordinator decides a pooler needs re-pointing (comparing published
> `ReplicationPrimary` against the current rule via `ReplicationPrimaryMatches`),
> and how this interacts with pg_rewind for diverged followers.

## The recovery loop

> TODO: document the engine and its loops (healthcheck, recovery, maintenance),
> the analyze → detect → dedup/prioritize → validate → act cycle, and the action
> registry (`AppointLeaderAction`, `DemoteStaleLeaderAction`,
> `FixReplicationAction`, `ReconcileCohortAction`, `ShardInitAction`).
> Files: `go/services/multiorch/recovery/`.

## Health input and leadership signaling

> TODO: how poolers report fitness to the coordinator — the health stream,
> `LeadershipStatus` / `LeadershipSignal` (ACTIVE, REQUESTING_DEMOTION), and
> `CohortEligibilityStatus`. This is the signaling layer the state model defers
> to recovery.

## Scenarios

> TODO: walk concrete cases end to end — clean leader failover, stale-primary
> demotion, a pooler joining the cohort, a pooler leaving — and how the
> consensus rules apply in each.

## See also

- [How we change the rules safely](rule-change.md) — the mechanism this doc
  triggers.
- [The HA state model](state-model.md) — health/leadership signals and rule state.
- [Consensus overview](consensus-overview.md) — the underlying algorithm.
