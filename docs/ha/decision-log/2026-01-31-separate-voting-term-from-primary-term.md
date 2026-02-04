# Separate Voting Term from Primary Term for Primary Determination

**Date:** 2026-01-31
**Status:** In Progress
**Participants:** David W, Sugu S, Rafael C

## Context

We need a reliable way to determine the primary in a healthy multi-replica cluster. Today we infer "the primary" by selecting the node that reports itself as primary with the highest term.

## Problem

The current heuristic fails in an edge case where a node accepts a newer term while still unwinding its previous primary role. That can make the system treat it as primary for a term it has not actually established, or treat a stale primary claim as current.

This does not break failover today because in discovery we choose the most advanced candidate based on WAL position (LSN), so the new
appointment is anchored to the freshest replicated state. But it does break other operations, like DemoteStalePrimary: if two multipoolers
claim to be primary, we currently lack a reliable way to decide which one is the true, most advanced primary, because term acceptance and
primary authority are conflated.

## Decision

Track two distinct term numbers per node:

- **Voting term**: the highest term the node has agreed to participate in (term acceptance, voting, etc.)
- **Primary term**: the term for which the node actually became primary and is executing primary decisions

The "are you primary?" response must include both:

- whether it is currently a primary
- the specific primary term it is primary for

If a node has accepted term 6 but is still primary for term 5, it must report primary term 5, not 6.

## Rationale

- Removes the ambiguity between term acceptance and primary authority
- Prevents misclassifying nodes as primary for a term they have not established
- Aligns with the existing mental model used in propagation where "term participation" and "term authority" are not the same thing

## Scope

**In scope:**

- Add primary term storage and reporting so primary determination is correct under transitions

**Not in scope yet:**

- Full transaction term tracking and propagation correctness based on term numbered transactions. We will continue using timestamp-based ordering for now until synchronous replication carries term metadata (planned via two-phase sync)

## Implementation Notes

- Store primary term alongside primary status, not in the voting term file, so the separation is explicit and durable
- Set primary term during establishment (promotion), not during discovery or propagation. Establishment is when primary authority becomes canonical
- This also prevents two primaries from claiming the same primary term
- Delete the code in Postgres Monitor that changes the type to primary and use this as authoritative determination
- Publish the term number in the topo

## Related Reasoning and Implications

### Transaction Ordering

Transaction ordering and propagation will eventually need term numbers on transactions. During discovery, nodes should be able to report the term of their transaction state so the system can prefer the most advanced term state regardless of the election term.

### Propagation Atomicity

When propagating from higher term to lower term, the receiver must atomically update both the transaction log and its transaction term marker.

### Term Completion

Term completion is not guaranteed to be explicitly recorded. Revocation can be implicit by recruiting sync replicas. Only durable records count: any decision not recorded with durability never happened.

## Risks and Mitigations

**Risk:** Confusion about which term to trust in different phases.

**Mitigation:** Make "primary term is only set at establishment" a hard invariant and use it as the source of truth for primary determination.

## Next Steps

1. Land emergency handling changes (old primary receiving begin term during emergency failover)
2. Refactor BeginTerm to include an action field (start with "emergency"), and separate begin from revoke. Keep semantics that term acceptance is independent of action execution
3. Implement primary term recording and update the "are you primary?" RPC to return primary term
