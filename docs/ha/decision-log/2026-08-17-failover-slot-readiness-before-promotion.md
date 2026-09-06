# Failover-slot readiness before promotion

**Date:** 2026-08-17
**Status:** Decided
**Participants:** Mats Kindahl

## Context

With native PostgreSQL 17 logical-slot failover, a consumer can resume on a promoted standby only if that
standby's synced failover slots are _failover-ready_ (`synced AND NOT temporary AND invalidation_reason IS
NULL`). At promotion the startup process stops the slot-sync worker, so a slot that is not ready by then
cannot become ready afterward — the consumer would be forced into a full re-seed.

The question: when a standby is promoted and one of its synced failover slots is not yet failover-ready, what
should the pooler do?

## Options considered

- **Advisory** — check once, log the not-ready slots, and promote anyway.
- **Hard gate** — refuse to promote a node whose failover slots aren't ready.
- **Advertise readiness to multiorch** — expose readiness in the pooler's health so the chooser prefers ready
  candidates.
- **Bounded wait** — wait a bounded time for readiness before `pg_promote`, then fall back.

## Decision

**Advisory.** Before `pg_promote` — while the node is still a standby — check once whether the node's synced
failover slots are failover-ready and log any that are not. Then promote regardless. Do **not** wait.

An earlier iteration used a bounded wait; it was superseded once durable slot creation was added to the design
(see below), which removes the transient the wait was meant to ride out.

## Rationale

- **Durable slot creation makes the wait redundant.** A failover slot's creation is not acknowledged to the
  client until the slot is `failover_ready` on the standbys the write-durability policy requires (see the
  durable-slot-creation section of the design doc). PostgreSQL creates a synced slot as `temporary` and only
  persists it once the standby has caught up — and persistence is sticky (a persisted slot never reverts to
  `temporary`). So the "temporary / catching-up" transient that a bounded wait was designed to ride out cannot
  occur at promotion on a node that passed the durability gate.
- **What remains is terminal.** Any slot that is still not failover-ready at promotion is in a state a wait
  cannot recover: `invalidation_reason` is set (permanent), or `synced = false` because the sync machinery is
  broken (not "not finished yet"). Waiting would only add latency. The check still tests `synced` (the readiness
  query already does), so a broken sync is surfaced in the log.
- **Failover is never blocked.** Logging and proceeding keeps failover the priority: the worst case is that a
  consumer re-seeds — correctness is preserved — never a stuck cluster. A hard gate could block failover
  indefinitely in exactly the moments HA matters most.

## Tradeoffs

**Pros:**

- No added promotion latency — a single catalog read, no polling loop or timeout.
- Never blocks failover; no change to the safety-critical guarantee.
- Pooler-local — no multiorch/cross-service change.
- Surfaces broken sync (`synced = false`) and invalidation in the promotion log.

**Cons:**

- The advisory check relies on durable slot creation to hold the "no transient at promotion" property. Durable
  creation ships later in the implementation order than the readiness handling, so in the interim a genuinely
  mid-sync slot on a planned switchover would re-seed rather than being waited out. This is a between-merges
  state, not a deployed one, and the feature is flag-gated.
- A consumer whose slot is in a terminal not-ready state still re-seeds; the check reports it but cannot recover
  it.

## Notes

- Gated behind `--enable-slot-based-replication` (dynamic, default off), so there is no effect until slot-based
  replication is enabled and failover slots exist.
- Implemented in the promotion path (`promoteLocked`) before `pg_promote` as `logUnreadyFailoverSlots`, reusing
  the `unreadyFailoverSlots` readiness query.
