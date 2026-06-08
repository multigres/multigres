# Block User Changes to `synchronous_commit`

**Date:** 2026-05-29
**Status:** Decided

## Context

PostgreSQL lets `synchronous_commit` be set globally (`ALTER SYSTEM`), per
database (`ALTER DATABASE ... SET`), per role (`ALTER ROLE ... SET`), and per
session/transaction (`SET`, `SET LOCAL`, `set_config(...)`).

Multigres manages replication durability centrally: the multipooler rule store
and `SyncStandbyManager` are the sole writer of `synchronous_commit` (via
`ALTER SYSTEM`), and we run at `synchronous_commit = on` so that an acknowledged
commit is durably flushed on the synchronous standby. See
[Use `synchronous_commit = on`](2026-02-12-synchronous-commit-on.md).

A user who lowers `synchronous_commit` on their session or transaction (e.g.
`SET synchronous_commit = off`) silently weakens that guarantee for their own
writes. The cluster keeps reporting healthy, but acknowledged commits may not be
where the HA contract assumes they are.

## Decision

For now, block every user attempt to **assign a value** to `synchronous_commit`
through the gateway, at all reachable levels. Reverting to the cluster-managed
value is still allowed: `RESET synchronous_commit`, `SET synchronous_commit TO
DEFAULT`, and `RESET ALL`.

`ALTER SYSTEM` is already blocked wholesale (Tier 2 unsafe statement). The
remaining paths — `SET` / `SET LOCAL` / `SET ... FROM CURRENT`, `ALTER DATABASE
... SET`, `ALTER ROLE ... SET`, and `set_config(...)` — are rejected at plan
time in the multigateway with SQLSTATE `0A000` (`feature_not_supported`).

## Rationale

The motivation is not that native PostgreSQL behavior necessarily breaks the
consensus API directly — it is that disabling synchronous_commit is a footgun
that users could easily misunderstand, especially around durability signals. The
safe default is to take the choice away rather than let it be made by accident.

## Future Work

This may be revisited behind some form of special permission or explicit opt-in,
so it is much harder to use accidentally. A plausible use case is batching a
high volume of transactions without waiting for replica round trips on each one,
then turning `synchronous_commit` back on for the final transaction in the
group.

## Implementation

See `docs/query_serving/unsafe_statement_rejection.md` ("Restricted GUC value
guard"). The guard lives in
`go/services/multigateway/planner/restricted_guc.go` — `synchronous_commit` is
an entry in the `restrictedGUCs` map — and is wired into the pre-dispatch
`planUnsupportedConstructs` check that covers both query protocols.
