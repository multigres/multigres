# Wait for WAL Replay to Stabilize During Standby Revoke

**Date:** 2026-02-12  
**Status:** Implemented  
**Participants:** Rafael C, David W.

## Context

During a term transition (BeginTerm with REVOKE action), standby nodes must stop receiving WAL from the old primary and report their WAL position so the orchestrator can decide which candidate has the most advanced state during discovery.

## Problem

PostgreSQL exposes two LSN values on a standby that sound like they should converge, but they track different things:

- **`pg_last_wal_receive_lsn()`**: the byte position of WAL received and written by the WAL receiver.
- **`pg_last_wal_replay_lsn()`**: the LSN of the last fully decoded and applied WAL record.

These are not directly comparable. The receive LSN is a raw byte position, while the replay LSN advances at WAL record boundaries. A standby can have received bytes that do not complete a WAL record, so `receive_lsn > replay_lsn` is normal and does not mean replay is “behind” in any actionable sense.

PostgreSQL also does not expose a signal that indicates “replay has finished processing all locally available WAL and is now idle.” There is no system function, catalog view, or hook for this. We can observe that `replay_lsn` has stopped advancing, but we cannot distinguish between these cases:

- replay is idle because it applied everything it could, or
- replay is stalled due to recovery conflicts, IO pressure, or resource contention.

This matters during revoke because we need to query the audit table and rely on it containing all rows up to the last commit that this standby durably received while it was an acknowledger. We use this table during discovery to derive:

- cohort membership
- primary term

## Decision

After disconnecting the WAL receiver during standby revoke, poll `pg_last_wal_replay_lsn()` until it stabilizes (same value for 3 consecutive polls at 10ms intervals), then report the final position. The full sequence is:

1. Reset `primary_conninfo` and reload config (breaks the connection to the old primary)
2. Wait for the WAL receiver process to fully disconnect (`pg_stat_wal_receiver` shows no rows)
3. Poll `pg_last_wal_replay_lsn()` until it stops advancing (3 consecutive identical readings, approximately 30ms of stability)
4. Query and report the final replication status

The overall timeout for the stabilization wait is 10 seconds.

This is a heuristic, not a guarantee. A stable reading can also occur if replay is stalled. We have not found a better approach in PostgreSQL. Alternatives considered:

- **Wait for receive LSN to equal replay LSN:** does not work because they track different units (byte position vs record boundary) and may never match exactly.

## Consensus Implications and What Could Go Wrong

In the worst case, replay is stalled but the standby has already received WAL that includes commits affecting the audit table. In that situation, the standby may report stale cohort or primary term information.

- A stale cohort could yield an election that assumes the wrong quorum and triggers failover without discovering a more advanced replica.
- A stale primary term could prevent multiorch from detecting conflicting timelines across multiple failovers.

Both failure modes are theoretical and unlikely in practice, especially for our 3 node deployment across Availability Zones.

## Rationale

- The 3 poll (30ms) stability window reduces false positives from brief pauses between replay progress while keeping revoke latency low.
- Waiting for the receiver to disconnect first ensures no new WAL arrives during the stabilization check.
- The 10 second overall timeout prevents unbounded waits if replay is not catching up.
- With `synchronous_commit=on`, commits acknowledged via this standby should be durably present in its WAL storage, so under normal conditions replay catches up quickly.

## Scope

**In scope:**

- Wait for replay stabilization during standby revoke in BeginTerm
- Report replay LSN after stabilization

**Not in scope:**

- PostgreSQL changes to expose an explicit replay idle signal
- Handling replay that is stuck for extended periods (the 10s timeout surfaces this as an error)
