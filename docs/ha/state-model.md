# The HA State Model

There's some vocabulary used in HA that's introduced here: rules, cohorts, quorum, durability policies, leader/follow/observer, terms, the revocation promise, and how PostgreSQL's own
state fits in.

The overview is abstract: transactions, rules, cohorts, rogue cohorts. This page
maps each of those ideas to a concrete proto message or table, and shows where
each piece of state lives and how durable it is.

> TODO: define the actors. "Coordinator" is used throughout (here and in the
> overview) but never defined: it is the agent that drives a rule change — a
> multiorch instance during failover, or the primary pooler itself for a
> leader-led change. Pair this with the pooler-roles note below (leader /
> follower / observer).

## State lives in three tiers

A poolers's complete view of its place in the cluster is `ConsensusStatus`
([clustermetadata.proto](../../proto/clustermetadata.proto)). It deliberately
combines three layers that differ in _where they are stored_ and _how much you
can trust them_:

| Tier                        | Message              | Backed by      | Survives restart? | Authoritative?                 |
| --------------------------- | -------------------- | -------------- | ----------------- | ------------------------------ |
| Revocation promise          | `TermRevocation`     | local disk     | yes               | yes — the node's pledge        |
| WAL position                | `PoolerPosition`     | PostgreSQL WAL | yes + backed up   | yes — WAL is durable & ordered |
| Most advanced known primary | `ReplicationPrimary` | process memory | no                | best-effort hint               |

## The shard rule

A **rule** is the redundancy policy. Concretely, this is represented as
`ShardRule`.

| Field               | Meaning                                                                                       |
| ------------------- | --------------------------------------------------------------------------------------------- |
| `rule_number`       | identity/ordering of this rule (see below)                                                    |
| `leader_id`         | the consensus leader; all transactions, including rule changes, are written to WAL through it |
| `cohort_members`    | the set of nodes eligible to acknowledge transactions                                         |
| `durability_policy` | what subset of the cohort must acknowledge for a write to be durable                          |
| `coordinator_id`    | which multiorch (or pooler, at bootstrap) applied this rule                                   |
| `creation_time`     | when the coordinator began applying it                                                        |

Leader, cohort, and policy are only meaningful _together and relative to the
rule that established them_. Shard rules are written to a table in postgres

> TODO: move the table schema up here and introduce the rules store?...

> TODO: comment on why leader is in the shard rule: the consensus algorithm doesn't
> require that the leader be persisted anywhere specific, but it's convenient in practice.

## Rule numbers and ordering

A `RuleNumber` is a pair `(coordinator_term, leader_subterm)`, compared
lexicographically by
[`CompareRuleNumbers`](../../go/common/consensus/compare.go):

- **`coordinator_term`** increases by one each time a coordinator runs a safe
  transition (the overview's "unique higher rule number"). The term is derived
  during recruitment as `max(observed terms across the cohort) + 1`
  ([`NewTermRevocation`](../../go/common/consensus/revocation.go)).
- **`leader_subterm`** distinguishes changes made _within the same coordinator
  term_ — e.g. a healthy leader reconfiguring its own cohort. It resets to `0`
  when `coordinator_term` increases, so subterms are **not** globally unique on
  their own; only the pair is.

A nil `RuleNumber` is treated as the zero value (smaller than everything), which
is how a freshly-initialized shard (term `0`) compares as "behind" any real
rule. Rendered in logs as `coordinator_term.leader_subterm` (e.g. `7.2`) by
`FormatRuleNumber`.

The overview's "unique rule number, or we could get stuck" requirement is
enforced here: because the term is `max + 1` and recruitment requires overlap
with a majority of the outgoing cohort, two coordinators cannot both commit the
same term.

For the full argument — why majority overlap forces a single winner — see
[Changing the Rules Safely](rule-change.md#why-recruitment-is-enough-majority--revocation).

## Cohort and quorum

> TODO: we should talk about the roles of poolers: leader & follower within a cohort, and observer (non-cohort member that can still replicate but won't be in sync standby names)

The **cohort** is `ShardRule.cohort_members`. The **quorum policy** is
`DurabilityPolicy`:

| Field            | Meaning                                                                                                                    |
| ---------------- | -------------------------------------------------------------------------------------------------------------------------- |
| `quorum_type`    | `QUORUM_TYPE_AT_LEAST_N` (N nodes from the cohort) or `QUORUM_TYPE_MULTI_CELL_AT_LEAST_N` (N distinct cells, ≥1 node each) |
| `required_count` | the N                                                                                                                      |
| `policy_name`    | human-readable label, e.g. `AT_LEAST_2`                                                                                    |

A **quorum** is any subset of the cohort that satisfies the policy. The
overview's example "AT_LEAST_2 of {N1, N2, N3, N4}" is a `DurabilityPolicy` with
`quorum_type = AT_LEAST_N`, `required_count = 2`, over a four-member cohort.

Whether a cohort can even satisfy a policy is checked by `CheckAchievable`
([policy_at_least_n.go](../../go/common/consensus/policy_at_least_n.go),
[policy_multi_cell.go](../../go/common/consensus/policy_multi_cell.go)) before a
rule is written — you cannot install `AT_LEAST_2` over a one-member cohort.

> TODO: multi-cell (availability-zone) quorum has its own subtleties worth a
> dedicated section. Document in [rule-change.md](rule-change.md) or a follow-up.

## Where the rule lives

The current rule is stored **in PostgreSQL itself**, in two tables created by
[`CreateRuleTables`](../../go/services/multipooler/internal/manager/consensus/rule_store.go):

- **`multigres.current_rule`** — exactly one row per shard, holding the rule in
  effect. It doubles as a lock target: writers take `SELECT ... FOR UPDATE
NOWAIT` on this row to serialize concurrent rule changes (failing fast rather
  than blocking).
- **`multigres.rule_history`** — append-only audit log, one row per rule change,
  keyed by `(coordinator_term, leader_subterm)`.

Storing the rule in the database is what gives the overview's "rule changes
carry the same durability guarantees as any other transaction": a rule change is
an ordinary WAL-replicated write, so it is recoverable by any node that can read
the database, and it is ordered relative to user transactions.

All access goes through the `RuleStorer` interface. Reads return a
`PoolerPosition` (rule + current LSN). Writes go through
[`UpdateRule`](../../go/services/multipooler/internal/manager/consensus/rule_store.go),
which assigns the next `leader_subterm` (`0` for a new term, `current + 1`
within the same term), performs a compare-and-swap against the row it read, and
writes both tables in a single CTE so the audit log can never diverge from
`current_rule`.

`UpdateRule` also orchestrates the `synchronous_standby_names` /
`synchronous_commit` GUC transition (the `Both` → `Incoming` policy handoff via
`BuildPolicyTransition`) and the promotion hook. That mechanism — and why the
blocking rule-write doubles as the durability checkpoint — is covered in
[Changing the Rules Safely](rule-change.md#committing-the-change-the-durability-checkpoint);
only the _state_ it produces is described here.

## The revocation promise

A `TermRevocation`
([revocation.go](../../go/common/consensus/revocation.go),
[clustermetadata.proto](../../proto/clustermetadata.proto)) is the overview's
"pledge not to participate in the old rule," made into durable state. It is
**persisted to local disk before the node responds**, so it survives a process
restart — the node cannot forget a promise it made.

| Field                      | Meaning                                                                 |
| -------------------------- | ----------------------------------------------------------------------- |
| `revoked_below_term`       | the node has revoked participation in **all** terms strictly below this |
| `accepted_coordinator_id`  | which coordinator the node accepted this term from                      |
| `coordinator_initiated_at` | which recruitment round (guards against a coordinator restart)          |
| `outgoing_rule`            | the rule the coordinator was transitioning _from_ at recruit time       |

Accepting a revocation at term _N_ revokes two things:

1. **Consensus participation** — the node refuses Recruit (and similar) requests
   for any term `< N`, and for term _N_ itself from any coordinator other than
   `accepted_coordinator_id`. This is what eliminates rogue cohorts: a quorum of
   the old cohort can no longer be assembled to commit under the old rule.
2. **Replication participation** — a revoked primary stops accepting writes; a
   revoked replica clears `primary_conninfo`. This resumes once the node's
   highest-known rule reaches or exceeds _N_ (the rule for that term has
   replicated through).

In-memory and on-disk state are kept in lockstep by `ConsensusState`
([consensus_state.go](../../go/services/multipooler/internal/manager/consensus/consensus_state.go)):
memory is updated **only after** the disk write succeeds, so the durable pledge
can never be weaker than what the node has told a coordinator.

Two predicates encode the safety rules:

- [`ValidateRevocation`](../../go/common/consensus/revocation.go) — may this node
  honor an incoming revocation? Checks WAL-position safety (the node hasn't
  already committed at or beyond term _N_), coordinator idempotency (same
  coordinator at the same term is re-accepted; a different one is refused), and
  recruitment idempotency (the `coordinator_initiated_at` must match).
- [`IsRuleRevoked`](../../go/common/consensus/revocation.go) — does a node's
  recorded promise forbid applying a given rule? Yes, unless durable WAL exists
  for a rule strictly newer than `outgoing_rule` — the **runaway-recruit
  override**: if the cohort has demonstrably moved past the rule the revocation
  was authored to leave, the promise is moot.

A related message, `ExternallyCertifiedRevocation`, revokes the outgoing cohort
via a coordinator's certification (a frozen LSN) rather than Recruit RPCs —
used for bootstrap and operator override. It is explained alongside the recruit
flow in
[Changing the Rules Safely](rule-change.md#bootstrap-and-operator-override-externally-certified-revocation).

## Positions and PostgreSQL state

> TODO: We could note that a purpose of PoolerPosition is identifying the
> most advanced timeline during leader selection for a rule change. We need
> to order poolers by their consensus term first and LSN second, which what
> position comparisons will do.

A `PoolerPosition` is a `ShardRule` plus an `lsn` string — the highest rule a
node has committed to its local WAL, and its current WAL head. Positions are
ordered by rule number first, LSN second
([`ComparePosition`](../../go/common/consensus/compare.go)); the most advanced
position across a cohort is the candidate the overview calls "the most advanced
transaction history."

> TODO: distinguish "transaction history" (the consensus concept — the single
> durable chain from the overview) from a Postgres **timeline** (the WAL lineage
> that forks on `pg_promote`, identified by a timeline ID). They are related but
> not the same: a Postgres timeline fork is the mechanism by which a node
> diverges, and `pg_rewind` reconciles a divergent timeline back onto the chosen
> history. Worth a short note so "history" and "timeline" aren't read as
> synonyms.

Two PostgreSQL facts shape how a position is read (see the `current_lsn`
expression in
[rule_store.go](../../go/services/multipooler/internal/manager/consensus/rule_store.go)):

- **Recovery mode** — `pg_is_in_recovery()` distinguishes a primary from a
  standby. A primary reads its WAL head with `pg_current_wal_lsn()`; a standby
  uses `pg_last_wal_receive_lsn()` (falling back to `pg_last_wal_replay_lsn()`).
  Recovery mode also decides whether a rule read may take `FOR UPDATE` (primary
  only; a standby is read-only).
- **Receive vs. replay LSN** — these track different things (bytes received vs.
  records applied) and are not directly comparable. The distinction matters when
  draining a standby during a transition; see the decision log
  [Wait for WAL Replay to Stabilize During Standby Revoke](decision-log/2026-02-12-wait-for-replay-stabilize-during-revoke.md).

## The best-effort tier: ReplicationPrimary

`ReplicationPrimary` is what a node has most recently been _told_ its primary
should be (by a `SetPrimary` or `Promote` RPC), plus the rule under which that
primary holds leadership. It is held in memory only — **not persisted** — so a
restart clears it and a coordinator re-informs the node.

It exists so a node in a degraded state (partition, mid-bootstrap, told to set a
primary while PostgreSQL was down) knows who to reconnect replication to. It is
also a coordinator optimization: `ReplicationPrimaryMatches`
([compare.go](../../go/common/consensus/compare.go)) lets a coordinator skip a
`SetPrimary` RPC that wouldn't change anything. Treat it as "what this node
currently believes," never as the canonical cluster primary — that authority
lives in `current_rule`.

> TODO: leadership signaling (`LeadershipStatus`, `LeadershipSignal` —
> ACTIVE / REQUESTING_DEMOTION) and cohort-eligibility signals are how a node
> advertises fitness to the coordinator. They belong with health monitoring;
> document in [recovery.md](recovery.md).

## See also

- [Consensus overview](consensus-overview.md) — the abstract algorithm these
  types implement.
- [Decision log](decision-log/) — point-in-time decisions, several of which turn
  on the state described here (`synchronous_commit = on`, replay stabilization).
