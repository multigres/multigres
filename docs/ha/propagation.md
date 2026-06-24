# Propagating Stuck Rule Transitions

When a rule change is interrupted partway through, the cohort can be left holding
a rule-change transaction that is neither confirmed durable nor safe to discard.
This doc will describe how such a **stuck transition** is finalized by
_propagating_ the in-WAL entry to quorum instead of writing a new one.

> **Status: not yet implemented.** This is a placeholder.
