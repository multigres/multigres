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

**Updated:** 2026-07-21

After disconnecting the WAL receiver during standby revoke, wait until replay has applied all received WAL, then report the final position. The full sequence is:

1. Reset `primary_conninfo` and `restore_command` and reload config (breaks the connection to the old primary)
2. Wait for the WAL receiver process to fully disconnect (`pg_stat_wal_receiver` shows no rows)
3. Poll until replay has applied all available WAL, decided by two positive signals only (see the update below).
4. Query and report the final replication status

The overall timeout for the wait is 10 seconds.

### Update: positive-signal completion (`waitForReplayComplete`)

The original implementation waited for `pg_last_wal_replay_lsn()` to stop
advancing (3 identical readings ≈30ms apart). Reported evidence showed this can
declare completion prematurely: anything that freezes the replay LSN below the
received position — a recovery conflict, an I/O stall, `recovery_min_apply_delay`
— looks identical to "replay finished" over the stability window, so `Recruit()`
could proceed before WAL was fully replayed and report a stale position.

The **stability heuristic was removed as a completion criterion.** Completion is
now decided by two positive signals, and every other state means "not done, keep
waiting" until the deadline:

1. **`pg_last_wal_replay_lsn() >= pg_last_wal_receive_lsn()`** — replay has applied
   every received byte. The receiver is already stopped so `receive_lsn` is
   frozen; the comparison is done in SQL for a single consistent snapshot and
   PostgreSQL's native `pg_lsn` ordering (LSN strings do not compare
   lexicographically).

2. **The startup (recovery) process is parked in an end-of-WAL wait** —
   `wait_event` is `RecoveryRetrieveRetryInterval` or `RecoveryWalStream` in
   `pg_stat_activity` for `backend_type = 'startup'`. This means recovery looked
   for more WAL and found none, so every _complete_ received record has been
   applied. This is necessary because streamed WAL normally ends in an _incomplete_
   record (a partial cross-page record): `pg_last_wal_receive_lsn()` counts those
   trailing bytes but replay stops at the last complete record, so `replay_lsn`
   legitimately settles below `receive_lsn`. Signal 1 alone would never fire in
   that (common) case. To avoid acting on a transient sample, the end-of-WAL wait
   must be observed on 2 consecutive polls (the startup process passes briefly
   through I/O / running states while cycling WAL sources).

Any other observed state — replay actively applying, a stall on I/O
(`DataFileRead` / `WalRead` / `WalSync`), a recovery conflict (`Lock` /
`BufferPin` / `RecoveryConflict*`), an apply delay, a `NULL`/uninstrumented wait,
or an unrecognized wait — is treated as "not done". A genuine stall therefore
rides out the 10s deadline and surfaces as a `DEADLINE_EXCEEDED` Recruit error
(the safe, loud failure) instead of a premature, unsafe "done".

Alternatives considered and rejected:

- **Stop when the replay LSN stops advancing (the original heuristic):** a stalled
  I/O or a recovery conflict freezes the LSN below `receive_lsn` while WAL is still
  pending, and is indistinguishable from completion. This is the bug this update fixes.
- **Require exact `replay_lsn == receive_lsn`:** rejected — they track different
  units (byte position vs record boundary), so with a trailing incomplete record
  they never match and Recruit would always time out.

Version note: the recovery wait-event names are stable across PostgreSQL 13+
(well below the multigres floor). Before 13 they were renamed and `RecoveryWalStream`
meant something different, and in a future major the `BufferPin` wait-event _class_
is renamed to `Buffer`; the completion logic keys on a small allow-list of
end-of-WAL events and biases every unrecognized state toward "keep waiting", so
name drift can only cost latency (up to the deadline), never a premature "done".

### What can move the startup process out of `RecoveryRetrieveRetryInterval`

Signal 2's soundness rests on the fact that, in our context, once the startup
process reaches the end-of-WAL retry wait it cannot resume applying WAL. That wait
is a `WaitLatch` (`xlogrecovery.c`, `WaitForWALToBecomeAvailable`) that returns on:

- **timeout** — every `wal_retrieve_retry_interval` (5s default);
- **latch set** (`WakeupRecovery()`) — a running walreceiver started/flushed WAL,
  or a `SIGHUP` (config reload) / `SIGUSR2` (promote) / `SIGTERM` (shutdown) /
  `pg_wal_replay_pause()`;
- **postmaster death**.

Crucially, waking is not the same as resuming replay: after any wake, recovery
just retries the sources (archive → pg_wal → stream) and re-enters the wait unless
a source now actually has the next record. Replay therefore resumes only via one
of two paths:

1. **Streaming (re)starts** — a walreceiver connects and delivers WAL. Requires
   `primary_conninfo` to be set.
2. **The archive grows** — `restore_command` succeeds on a retry because a new
   segment appeared. Requires a live primary still archiving to the repo.

Both callers stop the receiver before the wait, so path 1 is closed — and
`waitForReplayComplete` additionally verifies `primary_conninfo` is empty
(`checkNoWALSource`) as a defensive guard. Path 2 is closed in the failover and
rewind scenarios because the source primary is dead or demoted, leaving a static
archive; a set `restore_command` is therefore harmless there (recovery replays the
archive to exhaustion and then parks). This is why the recruit path clears
`restore_command` for a _consensus_ reason (a cohort member must not chase a
possibly-foreign archive) rather than for signal-2 timing, and why the rewind path
may legitimately run with `restore_command` set.

Residual limitation: if the archive were _growing_ (a live primary archiving to the
same repo) while `restore_command` is set, a new segment could be fetched between
two polls and replay would resume, making signal 2 premature. This does not arise
in the failover/rewind scenarios (dead/demoted primary), but it is the reason the
recruit path does not rely on it.

## Consensus Implications and What Could Go Wrong

With positive-signal completion (see the update above), neither a stalled
replay nor a trailing incomplete record can be mistaken for completion: the former
keeps us waiting until the deadline (then errors), and the latter is recognized
explicitly via the end-of-WAL wait event. The residual failure modes are now:

- **A false end-of-WAL reading.** If the startup process reported an end-of-WAL
  wait on 2 consecutive polls but WAL was in fact still pending, we could report a
  stale position. This requires the recovery process to misreport its wait state,
  which we treat as not credible.
- **A slow-but-progressing replay near the deadline** times out and fails Recruit.
  This is the safe direction (fail loud); the orchestrator retries.

If a stale position were ever reported, the standby could publish stale cohort or
primary term information:

- A stale cohort could yield an election that assumes the wrong quorum and triggers
  failover without discovering a more advanced replica.
- A stale primary term could prevent multiorch from detecting conflicting timelines
  across multiple failovers.

Both are theoretical and unlikely in practice, especially for our 3 node deployment
across Availability Zones.

## Rationale

- Completion is decided only by positive signals (`replay_lsn >= receive_lsn` or an end-of-WAL wait event); a frozen replay LSN is never itself treated as completion, so stalls cannot be mistaken for "done".
- Requiring the end-of-WAL wait on 2 consecutive polls avoids acting on a transient sample while keeping revoke latency low (polls are 100ms apart).
- Waiting for the receiver to disconnect first freezes `receive_lsn`, which is what makes signal 1 a stable target.
- The 10 second overall timeout prevents unbounded waits: a genuine stall surfaces as a `DEADLINE_EXCEEDED` error rather than blocking indefinitely or completing prematurely.
- With `synchronous_commit=on`, commits acknowledged via this standby should be durably present in its WAL storage, so under normal conditions replay completes quickly.

## Scope

**In scope:**

- Wait for replay to complete during standby revoke in BeginTerm
- Report replay LSN after completion

**Not in scope:**

- PostgreSQL changes to expose an explicit replay-idle signal
- Handling replay that is stuck for extended periods (the 10s timeout surfaces this as an error)
- Eliminating the recovery conflicts that can stall replay (e.g. stopping read serving before the wait); out of scope here — the completion check refuses to complete during such a stall, which is sufficient for correctness.
