# Changing the Rules Safely

This is the consensus algorithm from the [overview](consensus-overview.md) at
implementation depth, with the PostgreSQL specifics folded in. It assumes you
have read the overview (the _why_) and [the state model](state-model.md) (the
_vocabulary_ — rules, cohorts, quorum, the revocation promise, positions).

The overview reduces a safe rule change to two steps:

1. **Block any rogue quorum** in the old rule's cohort by recruiting enough
   nodes to pledge they won't participate in the old rule.
2. **Write the rule change** under a unique higher rule number, to both the old
   and new cohorts.

This document shows how those two steps are carried out as a coordinator-driven
RPC protocol — **recruit → build proposal → promote/set-primary** — and what
each step does to a real PostgreSQL instance.

## Purpose and boundary

A rule change installs a new `ShardRule`: a new leader, cohort, or durability
policy, under a new rule number. This doc owns _how that happens safely_. It
does **not** own:

- _When_ a rule change is triggered — failure detection and the decision to act
  live in [recovery.md](recovery.md) (TODO). This doc starts from
  "a coordinator has decided to appoint a leader for this cohort."
- _What the state means_ — `RuleNumber`, `TermRevocation`, `PoolerPosition`,
  etc. are defined in [the state model](state-model.md).

The entry point is
[`Coordinator.AppointLeader`](../../go/services/multiorch/consensus/coordinator.go);
recovery calls it (via `AppointLeaderAction`), and bootstrap calls the sibling
`AppointInitialLeader`.

## The protocol at a glance

A coordinator (a multiorch instance) drives every cohort member through two
RPCs. The leader-to-be gets `Promote`; everyone else gets `SetPrimary`.

```text
coordinator                 cohort member (pooler)
-----------                 ----------------------
  │  Recruit(revocation)  →   stop replication, pledge term N to disk,
  │                           report committed position (rule + LSN)
  │  ← ConsensusStatus
  │
  │  (collect recruits until a safe proposal can be built;
  │   pick the most-advanced recruit as leader)
  │
  │  Promote(proposal)    →   [leader]    pg_promote, write the new rule
  │  SetPrimary(leader,   →   [followers] point primary_conninfo at the
  │             rule)                     new leader (pg_rewind if diverged)
  │  ← responses
```

`Recruit` is step 1 (block rogue quorum). `Promote`/`SetPrimary` is step 2
(write the change to both cohorts). The two RPCs map onto the proto messages in
[consensusdata.proto](../../proto/consensusdata.proto).

## Deriving the new term and the outgoing rule

Before any RPC, the coordinator builds the `TermRevocation` it will propose,
from the cohort's last-known statuses
([`NewTermRevocation`](../../go/common/consensus/revocation.go)):

- `revoked_below_term` = `max(observed terms across the cohort) + 1`. This is
  the overview's "unique higher rule number."
- `outgoing_rule` = the highest `RuleNumber` any cohort member reports — the
  rule the coordinator is transitioning _from_.

Uniqueness of the new term is not guaranteed by `max + 1` alone — two
coordinators could compute the same value. It is enforced during recruitment by
**majority overlap** (below): two coordinators cannot both win the same term
because their recruited sets must intersect, and a pooler accepts only one
coordinator per term.

## Step 1 — Recruit (block the rogue quorum)

The coordinator sends `Recruit(revocation)` to every cohort member concurrently
([`dispatchRecruit`](../../go/services/multiorch/consensus/rule_change.go)). On
the pooler side
([`Recruit`](../../go/services/multipooler/internal/manager/rpc_consensus.go)),
under the action lock:

1. **Validate** the revocation against committed state
   ([`ValidateRevocation`](../../go/common/consensus/revocation.go)) — reject if
   the pooler already committed at or beyond term _N_, or already pledged term
   _N_ to a different coordinator.
2. **Stop replication participation** — this is what makes the pledge real:
   - a **primary** demotes and restarts as a standby (`emergencyDemoteLocked`);
   - a **standby** pauses its WAL receiver and waits for replay to stabilize
     (`waitForReplayStabilize`; see the decision log
     [Wait for WAL Replay to Stabilize During Standby Revoke](decision-log/2026-02-12-wait-for-replay-stabilize-during-revoke.md)).
3. **Re-read** the now-stable position and re-validate (catches a WAL rule entry
   that raced in during the stop).
4. **Persist** the `TermRevocation` to local disk (`AcceptRevocation`) — the
   durable pledge — and **respond** with the pooler's `ConsensusStatus`
   (committed rule + LSN).

The response's position is what lets the coordinator pick the most-advanced
leader. Because the pledge is flushed to disk before the response, a recruited
pooler that crashes and restarts still honors it.

### Why recruitment is enough: majority + revocation

The coordinator only proceeds once the recruited set is large enough, checked by
[`CheckSufficientRecruitment`](../../go/common/consensus/policy_at_least_n.go)
over the **outgoing** cohort. Two thresholds combine:

```text
|recruited| ≥ max(
    ⌊|cohort|/2⌋ + 1,    // majority   — any two recruitments share a pooler
    |cohort| − N + 1,    // revocation — un-recruited set is too small to form an N-quorum
)
```

- **Majority** is what makes the new term unique. Any two concurrent
  recruitments of the same cohort must overlap on at least one pooler, and a
  pooler accepts only one coordinator per term, so at most one coordinator can
  complete a rule change at that term. This is the overview's "majority overlap
  forces a single winner."
- **Revocation** is the overview's rogue-cohort elimination: if `N` or more
  cohort members are _un_-recruited, they could still form their own `N`-quorum
  under the old rule. Requiring fewer than `N` un-recruited makes that
  impossible.

The most-advanced recruit is then a safe leader: because the recruited set
intersects every possible committing `N`-subset of the old cohort, it carries
every transaction that was ever durable under the old rule
([`discoverMostAdvancedTimeline`](../../go/common/consensus/proposals.go)). This
is the overview's "the most advanced transaction history becomes the starting
point — nothing durable is lost." "Most advanced" is well-defined because
positions are totally ordered even across diverged WALs; see
[Total ordering across divergence](state-model.md#total-ordering-across-divergence).

> The coordinator commits to the leader at the **first** moment a safe proposal
> can be built, streaming `Promote` from there rather than waiting for stragglers
> ([`collectRecruitsAndBuildProposal`](../../go/services/multiorch/consensus/rule_change.go)).
> This keeps failover fast. The tradeoff: _uncommitted_ WAL on a slower,
> more-advanced pooler is discarded (via pg_rewind) when it rejoins as a
> follower — safe, because uncommitted writes were never durable.

## Step 2 — Promote and SetPrimary (write to both cohorts)

Once a leader is chosen, the coordinator builds a `CoordinatorProposal` (new
`ShardRule` at the new term, carrying the same `TermRevocation`) and dispatches
it to the whole cohort
([`dispatchPromote`](../../go/services/multiorch/consensus/rule_change.go)):

- the designated leader receives **`Promote`**;
- every other member receives **`SetPrimary`**.

Members that didn't answer `Recruit`, or answered too late to factor into leader
selection, still receive `SetPrimary` so they learn the new term.

### Promote (the leader)

[`Promote`](../../go/services/multipooler/internal/manager/rpc_consensus.go)
requires a prior `Recruit` for the same term — the stored revocation term must
equal the proposal's term, and PostgreSQL must be a standby with no
`primary_conninfo` (proof that `Recruit` ran and no earlier `Promote` did). It
then writes the rule through
[`UpdateRule`](../../go/services/multipooler/internal/manager/consensus/rule_store.go)
with a **promotion hook** that calls `pg_promote()`. Only after the rule write
succeeds does the pooler advertise PRIMARY/SERVING to the gateway — the write is
the durability gate (see below). Opening writes earlier would risk acknowledging
transactions the new cohort can't yet guarantee.

### SetPrimary (the followers)

[`SetPrimary`](../../go/services/multipooler/internal/manager/rpc_consensus.go)
points a follower's replication at the new leader. Its gate is a pure rule
comparison: it applies the change only if the supplied rule is **strictly
higher** than the pooler's own (by `RuleNumber`), otherwise it's a successful
no-op — which makes it safe under retries and out-of-order delivery. It also
honors the revocation promise: if
[`IsRuleRevoked`](../../go/common/consensus/revocation.go) says the incoming
rule is below this pooler's pledged term, it's ignored.

When it does apply:

- a **standby** swaps `primary_conninfo` to the new leader and resumes streaming;
- a node still acting as **primary** is a _stale_ primary — the coordinator knows
  a newer rule with a different leader exists — so it is demoted and restarted as
  a standby, using **pg_rewind** to reconcile any divergence
  (`restartAsStandbyLocked`).

## Committing the change: the durability checkpoint

PostgreSQL has no transactional switch for "which quorum policy is in effect" —
`synchronous_standby_names` is a config setting. The rule store simulates one by
treating the **rule-change write itself as the durability checkpoint**, using a
two-phase GUC transition in
[`UpdateRule`](../../go/services/multipooler/internal/manager/consensus/rule_store.go)
/ [`BuildPolicyTransition`](../../go/common/consensus/durability.go):

1. Apply the **`Both`** policy _before_ the WAL write — a synchronous-standby
   configuration that satisfies the old and new durability requirements
   simultaneously (e.g. the smaller of the two cohorts on a membership change).
2. Write the rule change. This write **blocks until a synchronous-standby
   acknowledges** it. That ack is the checkpoint: once enough nodes confirm the
   rule-change LSN, the new policy has provably taken effect for every
   subsequent transaction.
3. Apply the **`Incoming`** (new) policy _after_ the write commits.

On a promotion the ack only arrives after the full `SetPrimary` round-trip
completes — `Recruit` cleared replication everywhere, so the followers only
reconnect (and start acking) once `SetPrimary` points them at the new leader,
optionally after a pg_rewind. The leader's `Promote` returning success therefore
means the cohort's durability quorum endorsed the new rule: **the change is
committed and irreversible.**

Combined with the revocation pledges from step 1, this is what makes split brain
impossible: revocation eliminates rogue cohorts, and committing only on a
quorum ack ensures no future coordinator can miss that the transition happened.

If the leader dies _between_ writing the rule and reaching that quorum ack, the
change is left in-flight — neither committed nor safe to discard. Completing such
an interrupted rule change is the special case covered in
[Propagating stuck rule transitions](propagation.md).

## Bootstrap and operator override: externally-certified revocation

`AppointInitialLeader`
([coordinator.go](../../go/services/multiorch/consensus/coordinator.go)) handles
a fresh shard, where there is **no outgoing cohort to recruit consent from**.
Instead of the outgoing-quorum check, it uses an
`ExternallyCertifiedRevocation`: the coordinator certifies a `frozen_lsn` (the
most-advanced position observed across the cohort) below which the outgoing
cohort cannot have committed anything. The proposal sets `skip_outgoing_quorum`,
and leader eligibility is floored at `frozen_lsn`
([`BuildExternallyCertifiedProposal`](../../go/common/consensus/proposals.go)).

A **majority** of the _incoming_ cohort is still required, for the same
single-winner reason as failover: it stops two concurrent bootstrap attempts
from both succeeding.

> TODO: this path is also the intended home for operator-driven overrides
> (forced failover when a quorum is permanently lost). Document the operator
> entry point once it lands.
>
> TODO: add a worked end-to-end example — a 3-node `AT_LEAST_2` cohort losing
> its leader, traced step by step (recruit responses, term/LSN selection,
> promote, the diverged-follower pg_rewind) — to make the protocol concrete.

## Implementation map

| Concern                             | Code                                                                                                                                                  |
| ----------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| Coordinator entry / failover wiring | [`coordinator.go`](../../go/services/multiorch/consensus/coordinator.go) (`AppointLeader`, `runFailover`, `AppointInitialLeader`)                     |
| Four-phase orchestration            | [`rule_change.go`](../../go/services/multiorch/consensus/rule_change.go) (`coordinatorLedRuleChange.Run`)                                             |
| Proposal building + quorum proofs   | [`proposals.go`](../../go/common/consensus/proposals.go) (`BuildSafeProposal`, `buildProposalCore`, `discoverMostAdvancedTimeline`)                   |
| Term derivation                     | [`revocation.go`](../../go/common/consensus/revocation.go) (`NewTermRevocation`)                                                                      |
| Recruitment thresholds              | [`policy_at_least_n.go`](../../go/common/consensus/policy_at_least_n.go) (`CheckSufficientRecruitment`)                                               |
| Pooler RPC handlers                 | [`rpc_consensus.go`](../../go/services/multipooler/internal/manager/rpc_consensus.go) (`Recruit`, `Promote`, `SetPrimary`)                            |
| Rule write + GUC transition         | [`rule_store.go`](../../go/services/multipooler/internal/manager/consensus/rule_store.go), [`durability.go`](../../go/common/consensus/durability.go) |
| RPC contracts                       | [`consensusdata.proto`](../../proto/consensusdata.proto)                                                                                              |

## See also

- [Consensus overview](consensus-overview.md) — the abstract two-step argument
  this protocol implements.
- [The HA state model](state-model.md) — the messages and tables referenced here.
- [When we change the rules](recovery.md) (TODO) — failure detection and the
  decision to start an appointment.
- Decision logs: [`synchronous_commit = on`](decision-log/2026-02-12-synchronous-commit-on.md),
  [replay stabilization during revoke](decision-log/2026-02-12-wait-for-replay-stabilize-during-revoke.md).
