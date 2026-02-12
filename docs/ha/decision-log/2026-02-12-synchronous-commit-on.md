# Use `synchronous_commit = on` to Enable Safe Replica Restart and Rejoin

**Date:** 2026-02-12
**Status:** Decided
**Participants:** Rafael C, Deepthi S, Sugu S, Manan G, David W

## Context

We need to choose the synchronous replication durability level for HA: `synchronous_commit = remote_write` vs `on`.

## Key Insight

`remote_write` can still satisfy our durability contract **if** we adopt a strict operational rule:

- **Any replica crash implies the replica is untrusted and must be discarded and rebuilt** before it can rejoin.

That keeps the contract coherent, but is operationally expensive and disruptive, during crash modes like OOM kills.

## Decision

Use `synchronous_commit = on` for HA synchronous standbys.

## Rationale

With `on`, we can safely restart and rejoin replicas after crashes without defaulting to discard and rebuild. We can trust that an acknowledged commit was durably flushed on the synchronous standby, so a crash is less likely to imply lost WAL relative to the acknowledged point.

In contrast, `remote_write` forces a harsher posture: either rebuild after crashes, or accept that a restarted replica may have lost acknowledged WAL.

## Undetectable data loss example

This is the failure mode we want to avoid.

1. Primary commits txn **T** and acks it to the client.
2. With `synchronous_commit = remote_write`, the synchronous standby acks once WAL is written to the OS, but not necessarily flushed to durable storage.
3. Standby crashes (for example OOM kill or node reboot) before those buffers are flushed. It restarts missing WAL that included txn **T**, even though **T** was acknowledged.
4. Separately, primary crashes and its local data is damaged or otherwise cannot be trusted for recovery.
5. We promote the standby. The system appears healthy, but txn **T** is silently missing.

Why this is hard to detect: the cluster can continue operating normally because there is no explicit "red flag" that says "an acknowledged commit was lost." If we allow a crashed standby to restart and rejoin under `remote_write`, we implicitly accept that possibility.

## Tradeoffs

**Pros of `on`:**

- Safer crash recovery semantics for replicas (restart and rejoin is viable)
- Simpler invariants for HA workflows: "ack implies durable on the sync standby"
- Fewer rebuilds, less operational disruption

**Cons of `on`:**

- Higher commit latency than `remote_write` (waits for stronger durability on the standby)
- More sensitivity to standby disk performance

## Operational Policy

- With `on`: replica crashes are not automatically "discard and rebuild" events.
- With `remote_write`: replica crashes effectively must be treated as "discard and rebuild" to preserve the durability contract.
