# High Availability & Consensus

How Multigres keeps a shard available and its data safe across node failures.
Consensus can feel unintuitive and managing PostgreSQL additional complexity, so these
docs are layered. Start at the top, and descend only as far as you need.

## Reading order

1. **[Consensus overview](consensus-overview.md)** — the idea and the algorithm,
   abstractly. What it means for a transaction to survive failures, and how a
   rule change avoids losing data or splitting brain.
2. **[The HA state model](state-model.md)** — the shared vocabulary: rules,
   cohorts, quorum, terms, the on-disk revocation promise, and how PostgreSQL's
   own state fits. Read this before either implementation doc.
3. Then, in either order:
   - **[How we change the rules safely](rule-change.md)** — the recruit → build
     proposal → promote/set-primary protocol, with the PostgreSQL specifics.
     - **[Propagating stuck rule transitions](propagation.md)** — the special
       case where a rule change was interrupted before it committed. _(Planned —
       not yet implemented.)_
   - **[When we change the rules](recovery.md)** — how multiorch monitors health
     and decides to act. _(Skeleton — high-level responsibilities, details TODO.)_

## Want X? Read Y

| If you want to understand…                                    | Read                                      |
| ------------------------------------------------------------- | ----------------------------------------- |
| Why HA needs consensus at all; the preservation invariant     | [overview](consensus-overview.md)         |
| Rogue cohorts and why a safe rule change has two steps        | [overview](consensus-overview.md)         |
| What a "rule", "cohort", "quorum", or "term" _is_ in the code | [state model](state-model.md)             |
| Where the current rule is stored and how it's changed         | [state model](state-model.md)             |
| What the on-disk revocation promise guarantees                | [state model](state-model.md)             |
| How a safe rule change works (recruit → promote)              | [rule change](rule-change.md)             |
| How recruit / promote / set-primary act on PostgreSQL         | [rule change](rule-change.md)             |
| How failure is detected and recovery decided                  | recovery.md _(TODO)_                      |
| How an interrupted rule change is finished                    | [propagation](propagation.md) _(planned)_ |
| Why a specific design choice was made                         | [decision log](decision-log/)             |

## Decision log

Point-in-time records of specific HA decisions and their rationale:

- [Use `synchronous_commit = on`](decision-log/2026-02-12-synchronous-commit-on.md)
- [Wait for WAL replay to stabilize during standby revoke](decision-log/2026-02-12-wait-for-replay-stabilize-during-revoke.md)
- [Block user changes to `synchronous_commit`](decision-log/2026-05-29-block-synchronous-commit-changes.md)

## Status of these docs

This is living documentation, filled in incrementally. `TODO` markers throughout
flag content we intend to write but haven't yet — they are deliberate
placeholders, not omissions.
