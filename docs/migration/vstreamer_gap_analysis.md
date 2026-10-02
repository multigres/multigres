# Vitess VStreamer/VReplication vs PostgreSQL: feature gap analysis

> Companion to the [Multigres Migrator design doc](./migrator_design.md) (this was originally Appendix A of that doc).
> Sourced from `github.com/vitessio/vitess` (`main`). Cited anchors are file + type/function + proto message; treat line
> numbers as approximate.

This document answers one question for the Multigres Migrator table-migration design: feature by feature, what does
PostgreSQL already give us that we can leverage, and what would we have to build into a `vstreamer`/`vplayer`-equivalent
component? Section references of the form "design §N" and named sections (e.g. _Multipooler changes_) point at the
[design doc](./migrator_design.md).

## The one architectural fact that drives every verdict

Vitess does **not** use MySQL's native replication apply. `vstreamer` reads the binlog as a **change-data-capture
source**, converts every change into a protobuf `VEvent`, and **re-applies it through Vitess's own apply loop**
(`vplayer`). Owning both ends — the decoder and the applier — is what makes exact-position stop, in-flight
re-keying/transforms, the journaled cutover barrier, and VDiff possible.

PostgreSQL therefore has **two** possible implementation strategies. These are not an either/or choice — they are a
**sequence**:

- **Strategy S (stock) — ship this first.** `CREATE SUBSCRIPTION` + the built-in apply worker (`pgoutput`). This is the
  Multigres Migrator MVP. The apply loop is **opaque and not controllable**, but everything runs from the
  target/coordinator over ordinary client connections, so **nothing is deployed on the source** — which is exactly what
  makes on-prem / standalone-Postgres onboarding trivial. This is the deliberate starting point.
- **Strategy O (owned apply loop) — introduce later, for speed.** A custom logical-decoding consumer over the
  replication protocol (`START_REPLICATION SLOT … LOGICAL`, or `pg_logical_slot_get_changes`), applying generated DML
  yourself — the PostgreSQL analogue of `vstreamer` + `vplayer`. Its headline payoff is a **faster, resumable initial
  copy** (parallel PK-range `COPY` instead of stock whole-table tablesync), plus the transforms/VDiff/throttling that
  ride on the same machinery.

**Why this order.** Strategy S gets on-prem migrations working with zero source-side footprint. Strategy O is a
performance-and-capability upgrade layered on top when the initial-copy time (or transforms/verification) justifies it.
Crucially, **Strategy O also stays client-side** for the source: both the logical-decoding consumer and the parallel
chunked copy drive the source over normal libpq/replication connections, so adding it does **not** require deploying an
agent on an on-prem source — the easy-onboarding property survives the upgrade. (Only true source-side _pushdown_ —
running filters/transforms at the source to cut network — would need source-local code, and that is not in this
program.)

Each feature's feasibility below is tagged by which strategy it needs.

Verdict legend used below — each capability is classified by **what Multigres Migrator has to do to get it**:

- **✅ Leverage** — already provided by stock PostgreSQL; Multigres Migrator uses the primitive as-is (Strategy S).
  Nothing to build.
- **◑ Build** — no stock equivalent; Multigres Migrator must build it into an owned logical-decoding component (Strategy
  O — the PostgreSQL analogue of `vstreamer` + `vplayer`).
- **✗ External** — no in-database primitive at all; belongs to a layer above PostgreSQL (the multigres gateway /
  sharding / control plane), not to any replication component.

## What we can leverage vs. what we must build

The whole point of the analysis: which of these does PostgreSQL already hand us, and which would we have to add to a
`vstreamer`/`vplayer`-equivalent component? The table below has the per-feature detail; this is the partition.

### Leverage — already in PostgreSQL, used as-is (Strategy S)

These are the primitives the MVP stands on. No new streaming/apply code — stock `CREATE SUBSCRIPTION` and the walsender
protocol cover them.

1. **Change capture / CDC** (#1) — logical decoding via `START_REPLICATION SLOT … LOGICAL pgoutput`.
2. **Event/wire model, basics** (#2) — `pgoutput`'s Begin/Commit/Relation/Insert/Update/Delete/Truncate/Type/Origin
   messages (before/after images via replica identity).
3. **Column projection + basic row filter** (#3, partial) — publication column lists and `WHERE` (PG15), within the
   replica-identity constraint on `UPDATE`/`DELETE`.
4. **Consistent snapshot pinned to a stream position** (#6) — `CREATE_REPLICATION_SLOT … (SNAPSHOT 'export')` +
   `SET TRANSACTION SNAPSHOT`; cleaner than MySQL's lock dance.
5. **Copy⇄stream alignment, fixed form** (#8) — stock tablesync aligns the per-table initial `COPY` with the stream
   automatically (whole-table, opaque).
6. **Journaled cutover barrier + reverse stream, with a quiesce** (#11, #12) — quiesce the source, wait
   `confirmed_flush_lsn ≥` captured LSN, then reverse `CREATE SUBSCRIPTION … copy_data=false`. This is why the symmetric
   switch works on stock PG.
7. **Decode-time schema tracking** (#17, decode half) — the catalog needed to decode WAL is pinned by the slot's
   `catalog_xmin` automatically.

### Build — add to an owned "PG streamer/player" component (Strategy O)

Everything here has **one shared prerequisite**: a custom logical-decoding consumer that applies changes itself and can
**stop at an exact LSN** (#9 — the PostgreSQL `vplayer`). Stock `CREATE SUBSCRIPTION` gives no apply-side control
(`DISABLE` exists; "apply through LSN X and halt" does not). Build that consumer once and the rest of this list becomes
reachable; skip it and the whole list stays closed.

1. **Owned apply loop with exact-position stop** (#9) — _the foundation._ A consumer over `START_REPLICATION … LOGICAL`
   that tracks position and halts at a target LSN.
2. **In-source transforms / re-keying** (#5) — computed columns, charset/tz/enum conversions; done in the consumer (or
   apply-side staging), since publications only filter+project.
3. **Arbitrary per-row SELECT filter** (#3, remainder) — beyond publication `WHERE`/replica-identity limits.
4. **PK-ordered, chunked, resumable copy** (#7) — `COPY (SELECT … WHERE pk-range)` at an exported snapshot plus our own
   `lastpk`/progress table; replaces whole-table one-txn tablesync.
5. **Chunked/interleaved copy⇄stream** (#8, beyond fixed form) — resumable interleave, not the opaque built-in.
6. **Custom batching, per-txn FK toggling, deferred secondary keys** (#10).
7. **VDiff — consistent, resumable, PK-merge verification** (#13) — dual exported-snapshot streams merged on PK (design
   §2).
8. **Load-aware throttling of copy & apply** (#14) — only possible if we own the copier/consumer.
9. **Quiesce-free, position-exact cutover barrier** (#11/#12, beyond the quiesce form) — a lossless handoff with no
   read-only window needs the owned apply loop.
10. **Custom stream events** (#2, remainder) — journal/lastpk/throttled/heartbeat markers Vitess carries in-band.

#### Phased build ordering

This is the **Strategy O program — undertaken only after the Strategy S MVP ships** and on-prem onboarding works. Its
primary driver is B1 (a faster, resumable initial copy); the other phases ride on the same owned apply loop once it
exists. The ten items are not delivered at once: they stack by dependency — each phase reuses the machinery of the one
before — and are sequenced so the highest operational value lands earliest. Every phase after B0 assumes the owned apply
loop exists, and each phase replaces a piece of the Strategy S path incrementally rather than in a big-bang cutover.

- **B0 — Foundation: owned apply loop with exact-position stop** (#9). A consumer over `START_REPLICATION … LOGICAL`
  that decodes `pgoutput` (or a custom plugin), applies DML itself, tracks position, and can halt at a target LSN.
  Delivers nothing user-visible on its own, but it is the prerequisite for every phase below — build it first or none of
  the rest are reachable. Ships alongside the stock (Strategy S) path, which keeps running until a phase replaces the
  relevant piece.
- **B1 — Resumable, chunked copy** (#7, #8). PK-ordered `COPY (SELECT … WHERE pk-range)` at an exported snapshot, with
  our own `lastpk`/progress table, interleaved with the stream to a position via B0. First phase with direct operator
  payoff: large-table migrations survive a restart instead of re-copying from zero, and the copy stops pinning WAL for
  the whole table. Depends on B0's position tracking for the copy⇄stream handoff.
- **B2 — VDiff verification** (#13). Consistent, resumable, PK-merge comparison of source and target using dual
  exported-snapshot readers synced to a common stop position. Reuses B1's chunked snapshot readers and B0's exact-stop
  to pin both sides to the same LSN. Sequenced early because verification is what makes a cutover trustworthy; without
  it operators fall back to the manual checksum.
- **B3 — Apply throughput & safety controls** (#14 throttling, #10 batching / per-txn FK toggling / deferred secondary
  keys). Load-aware throttling of copy and apply, plus apply-side tuning. Pure additions to the B0 consumer and B1
  copier — no new data path — so they slot in once those exist and the copy/apply volume is real enough to need
  governing.
- **B4 — Transforms & richer filtering** (#5 transforms / re-keying, #3 remainder — arbitrary per-row filter). Computed
  columns, charset/tz/enum conversions, and per-row predicates beyond publication `WHERE`/replica-identity limits,
  evaluated inside the B0 consumer. Deferred behind copy/verify/controls because it changes payload semantics and is
  only needed for re-keying and heterogeneous migrations, not the 1→1 whole-table case.
- **B5 — Quiesce-free cutover & in-band events** (#11/#12 beyond the quiesce form, #2 remainder). Lossless,
  position-exact handoff with no read-only window, plus journal/lastpk/throttled/heartbeat markers carried in the
  stream. Last because the MVP already has a _safe_ cutover (quiesce + lag-zero barrier); this phase only removes the
  read-only window, which is an optimization on top of everything else.

Phases B1–B5 are individually shippable and independently valuable once B0 exists; the order above is the recommended
default (dependency- and value-driven), not a hard chain — B3/B4 can swap based on demand. B0 is the one non-negotiable
prerequisite.

### External — not a replication feature at all (belongs above the DB)

No PostgreSQL primitive exists, and no `vstreamer`-equivalent would provide these; they live in the multigres
sharding/routing layer.

1. **Vindex/keyrange routing, `keyspace_id()`** (#4) — sharding-map awareness, not a source filter.
2. **Coordinated cross-shard fan-out/fan-in merge** (#15) — split is stream-able; merge needs cross-source ordering the
   DB can't give.
3. **Traffic routing / read-then-write cutover** (#16) — `SwitchTraffic`/routing-rules equivalent; the gateway shard-map
   (design §3).

## Summary table

The Vitess mechanism (file · symbol) column is omitted here for width; those source anchors are cited inline in the
sections above. This recap maps each capability to its PostgreSQL primitive and verdict.

| #   | Vitess capability                                                                               | PostgreSQL primitive                                                                                                  | Verdict                                                                                                                             |
| --- | ----------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| 1   | Change capture / CDC source                                                                     | Logical decoding: walsender `START_REPLICATION … LOGICAL pgoutput` / `pg_logical_slot_get_changes`                    | ✅ Leverage (different primitive)                                                                                                   |
| 2   | Event/wire model (before/after images, FIELD, GTID, HEARTBEAT, JOURNAL, LASTPK, COPY_COMPLETED) | `pgoutput` messages: Begin/Commit/Relation/Insert/Update/Delete/Truncate/Type/Origin (+ streaming PG14, 2PC PG15)     | ✅ Leverage for basics; ◑ for custom events (journal/lastpk/throttled)                                                              |
| 3   | In-source SELECT filter + projection                                                            | Publication row filter `WHERE` + column list (PG15); UPDATE/DELETE filter limited to replica-identity cols            | ✅ Leverage when filter cols ⊆ replica identity; **config** (RI) for other cols; ◑ Build for computed transforms; projection native |
| 4   | Keyrange / vindex sharding filter, `keyspace_id()`                                              | none                                                                                                                  | ✗ (no vindex/keyrange function; emulate via `WHERE hash(pk)…` only)                                                                 |
| 5   | Value transforms / re-keying (charset, tz, enum→text, computed)                                 | none (publications filter+project only)                                                                               | ◑ Build (or apply-side staging + triggers)                                                                                          |
| 6   | Consistent snapshot pinned to a stream position                                                 | `CREATE_REPLICATION_SLOT … (SNAPSHOT 'export')` → `SET TRANSACTION SNAPSHOT` (slot consistent_point)                  | ✅ Leverage (cleaner than MySQL's lock dance)                                                                                       |
| 7   | PK-ordered, chunked, resumable copy (lastpk)                                                    | `COPY (SELECT … WHERE pk-range)` at exported snapshot + own progress table                                            | ◑ Build (stock tablesync is whole-table, one txn, non-resumable)                                                                    |
| 8   | Copy⇄stream interleave to one position                                                          | Stock tablesync aligns per-table via temp slot+snapshot+syncpoint — opaque, whole-table                               | ✅ Leverage (fixed form); ◑ for chunked/interleaved/resumable                                                                       |
| 9   | Owned apply loop w/ **exact-position stop**                                                     | none — a subscription cannot be told "apply exactly to LSN X and stop"                                                | ◑ Build (foundational for 11/13/15)                                                                                                 |
| 10  | Custom batching, per-txn FK toggling, deferred secondary keys                                   | apply worker applies in commit order (parallel apply PG16); no per-txn FK toggle; index drop/rebuild only out-of-band | ◑ Build (FK/batching); ✅ index defer as separate manual step                                                                       |
| 11  | Journaled cutover barrier (catch-up to exact source pos, FROZEN)                                | quiesce + wait `confirmed_flush_lsn ≥ source LSN`; MVP journal in topo                                                | ✅ Leverage **with a quiesce**; ◑ for quiesce-free exact barrier                                                                    |
| 12  | Auto reverse stream (position-aligned)                                                          | reverse `CREATE SUBSCRIPTION … copy_data=false` after the barrier                                                     | ✅ Leverage **with the barrier**; ◑ for lossless quiesce-free handoff                                                               |
| 13  | VDiff (consistent, resumable, PK-merge)                                                         | nothing built-in; build via exported snapshots + PK merge (or `postgres_fdw`/`dblink`)                                | ◑ Build / external (design §2)                                                                                                      |
| 14  | Load-aware throttling of copy & apply                                                           | none (no cooperative copy/apply throttle)                                                                             | ◑ Build (only if you own copier/consumer)                                                                                           |
| 15  | Multi-shard fan-out/fan-in, split/merge                                                         | N subscriptions; merge risks conflicts, no cross-source ordering                                                      | ◑ split native-ish; ✗ coordinated merge/barrier                                                                                     |
| 16  | Traffic routing (SwitchTraffic reads/writes)                                                    | none in PG; needs multigres gateway shard-map                                                                         | ✗ External                                                                                                                          |
| 17  | Schema handling for decode + DDL policy                                                         | catalog pinned by `catalog_xmin`; **DDL not replicated**                                                              | ✅ decode-time schema native; ✗ DDL propagation                                                                                     |

## Notes on the non-obvious verdicts

**#3 filtering is native for the sharding case; the residual gap is a replica-identity constraint, not an owned-loop
one.** PostgreSQL publication `WHERE` filters (PG15) are fully sufficient **when every filter column is covered by the
replica identity** — which is the normal sharding case: an immutable shard key that is part of the primary key (PK
`(shard_key, …)`), filtered `WHERE shard_key = …`. There it is native and complete, and because the shard key is
immutable there is also no row-movement to handle.

The rule that bites: for `UPDATE`/`DELETE`, the filter may reference **only replica-identity columns**, because the WAL
carries only the replica-identity columns of the _old_ row image (unless `REPLICA IDENTITY FULL`) and the filter is
evaluated against that old image. Two real-world cases hit it:

- **Shard key not in the PK** (surrogate `id` PK plus a separate `tenant_id`/`org_id` shard column) — very common.
  `WHERE tenant_id = …` filters `INSERT`s but is rejected for `UPDATE`/`DELETE`. Fix without changing the PK:
  `REPLICA IDENTITY USING INDEX` on a unique, `NOT NULL`, non-partial index like `(tenant_id, id)` — or
  `REPLICA IDENTITY FULL` (works for any filter, but logs the whole old row and forces seq-scan apply on an unindexed
  target).
- **Attribute-based subset migration** (`WHERE status='active'`/`created_at > …`/`deleted_at IS NULL`) — a _different
  feature_ from sharding; those non-key columns fail for `UPDATE`/`DELETE` unless folded into the replica identity as
  above.

**This is a WAL/replica-identity limitation, not something Strategy O fixes.** No apply loop — ours or Vitess's — can
filter a `DELETE` (or the old-image side of an `UPDATE`) on a column that was never written to WAL; the only remedy is
to put that column in the replica identity. Vitess sidesteps it solely because MySQL row-based binlog logs full
before-images by default; the PostgreSQL equivalent of that is `REPLICA IDENTITY FULL`, not `vplayer`. The owned loop
only unlocks richer _new-image_ filtering and **computed** transforms (re-keying, expressions), which publications
genuinely cannot express. Column projection (publication column lists) is native.

Multigres Migrator turns this into a create-time check rather than a confusing `CREATE PUBLICATION` runtime error — see
the `ValidateSource` filter-column rule in _Multipooler changes_ of the [design doc](./migrator_design.md).

**#4 keyrange/vindex is the sharpest miss.** `analyzeInKeyRange` + `vindexes.Map` + `keyspace_id()` are Vitess-specific
routing built into the source stream. PostgreSQL has no vindex abstraction; the closest is a static
`WHERE hashtextextended(pk::text,0) % N = k` predicate, which is skew-blind and not a routing function. This is design
gap §5.

**#6 is a place PostgreSQL is arguably _better_.** MySQL needs a `LOCK TABLES`/`FLUSH TABLES` dance (`snapshot_conn.go`)
to pin a snapshot to a GTID; PostgreSQL's `CREATE_REPLICATION_SLOT … (SNAPSHOT 'export')` hands back a snapshot aligned
to the slot's `consistent_point` in one step, no table locks. The caveat (see the slot section of the
[design doc](./migrator_design.md)): the _SQL_ function `pg_create_logical_replication_slot` does **not** export a
usable snapshot; you must use the walsender/replication-protocol form.

**#9 is the crux.** Everything in the "◑ owned-loop" column ultimately depends on being able to apply changes yourself
and **stop at an exact LSN** — `vplayer`'s `hasAnotherCommit`/`posReached`. Stock `CREATE SUBSCRIPTION` gives no such
control: you can `DISABLE` a subscription but not "apply through LSN X and halt." Build the owned consumer once and gaps
5, 7, 10, 13, and 14 all become reachable; skip it and they stay out of reach.

**#11/#12 are native only because Multigres Migrator accepts a quiesce.** Vitess's `waitForCatchup` stops source writes
at the vtgate/denied-tables layer and waits for targets to reach the source's stopped position, then journals it — an
exact, application-transparent barrier. Multigres Migrator reproduces the _safety_ of this with a quiesce + lag-zero
barrier (source set read-only, wait `confirmed_flush_lsn ≥` captured LSN), which is native. A _quiesce-free_,
position-exact barrier (no read-only window) needs the owned apply loop.

## Translating Vitess's two dependency buckets to PostgreSQL

The inventory split Vitess features into "MySQL-binlog-specific" vs "owned-apply-loop." Mapped onto PostgreSQL:

- **Binlog-specific bits have clean PG replacements.** Binlog dump + RBR cell decoding → logical decoding + `pgoutput`.
  Consistent-snapshot-to-GTID (`snapshot_conn.go`) → exported-snapshot replication slot. So the _capture_ and
  _snapshot-alignment_ primitives are **not** a barrier — PostgreSQL has equivalents (in one case a nicer one).
- **Owned-apply-loop features are the real gap.** Exact-position stop, transforms/re-keying, chunked-resumable copy
  interleave, VDiff consistency, throttling, journaled/quiesce-free reverse, deferred secondary keys, per-txn FK
  toggling — none exist in stock PostgreSQL and all require Strategy O. This is the same "cross-cutting fork" the gap
  analysis flags: **build a custom logical-replication consumer** and this whole column unlocks; rely on
  `CREATE SUBSCRIPTION` and it stays closed.

## What PostgreSQL simply cannot do (needs a layer above the DB)

- **Traffic routing / read-then-write cutover** (#16) — there is no in-database equivalent of routing rules /
  `MigrateServedType`; this belongs to the multigres gateway shard-map (design §3), not to any PG feature.
- **Vindex/keyrange-aware routing and cross-shard merge coordination** (#4, #15) — no PG primitive; must live in the
  multigres sharding layer.

## Bottom line for Multigres Migrator

Mapped onto the _leverage / build / external_ partition above:

- **Leverage:** the MVP (Strategy S) gets ✅ items 1, 2 (basics), 6, 8 (fixed form), 11–12 (with quiesce), 17 (decode)
  essentially for free — which is why the single-stream + symmetric-switch prototype works on stock PostgreSQL.
- **Build:** reaching Vitess parity on the ◑ items (5, 7, 9, 10, 13, 14 — transforms, chunked/resumable copy,
  exact-stop, VDiff, throttling) is a **single, deliberate investment**, not a pile of independent features: the owned
  logical-decoding apply loop of #9. It is the PostgreSQL `vstreamer`+`vplayer` we would add, and every other ◑ item
  hangs off it.
- **External:** items 4, 15, 16 are **not** replication features at all; they belong to the multigres sharding/routing
  layer and are out of scope for a logical-replication component.
