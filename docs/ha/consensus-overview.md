# Generalized Consensus Overview

## High Availability & Consensus

Building an available system means surviving outages. Servers fail, disks die,
networks partition. A system that stores data on a single node will eventually
lose that data or become unable to accept new transactions — often both at once.

Surviving outages means redundancy: copying the same data to multiple nodes so
that losing any one of them doesn't lose the data or stop the system. But
redundancy introduces a new problem. If data lives on multiple nodes, which copy
is authoritative? After a failure, some nodes may have transactions that were
written but never confirmed by enough other nodes to be safe — phantom
transactions that exist on one copy but could never have been recovered if that
node had failed instead. A system with redundancy needs a way to answer: which
transactions actually happened, and which ones only look like they did?

This is the consensus problem. Each node keeps a **WAL** — an append-only log of
transactions — and consensus is agreement on a single authoritative WAL across
the cohort despite failures. A transaction is **durable** once a quorum of the
cohort holds its position in their WAL, under the rule in force when it was
written. Because a WAL only grows by appending, a node that holds a position
holds everything before it — so "don't lose earlier durable transactions" isn't a
separate rule, it's just what a WAL is. That leaves one invariant,
**preservation**: a position that became durable is never lost.

Preservation makes recovery decidable. Durability is a position on a log, so the
recruited node with the furthest WAL holds every durable position — promoting it
loses nothing durable. Preservation runs one way: anything dropped during
recovery was, by this rule, never durable, but the converse doesn't hold — when we
cannot tell whether a position reached quorum we keep it, so a hung transaction
may be propagated and made durable going forward. A rule change is itself a
transaction in the WAL, so preservation holds even as the cohort or the
redundancy policy itself changes. Two nodes whose WALs diverged past their last
common position are the split-brain case the rest of this doc works to prevent.

## Terminology: Rules & Rogue Cohorts

**A rule describes the redundancy policy.** It names a cohort — the set of nodes
eligible to acknowledge transactions — and a quorum policy that defines what
subset of acknowledgements is sufficient for a transaction to be durable. For
example: "at least 2 of these 3 nodes must acknowledge." Every transaction is
committed under a rule. Changing which nodes participate, or how many
acknowledgements are required, means changing the rule. We store the rule in the
database itself and change it using transactions, so rule changes carry the same
durability guarantees as any other transaction and are always recoverable by any
node that can read the database.

A **rogue cohort** is any subset of nodes that can still achieve quorum under a
rule that was supposedly already closed. Rogue cohorts are the enemy of
preservation: if a rogue cohort exists after a rule change, it can commit new
transactions under the old rule that the new cohort has no knowledge of and no
obligation to preserve. The entire goal of a safe rule change is to ensure no
rogue cohort survives it.

To see why this matters, consider four nodes under R1 where any two can satisfy
quorum. A coordinator attempting a rule change recruits only N1 and N2. The two
unrecruited nodes are free to write their own conflicting rule change and
continue committing transactions — a rogue cohort:

**Example mistake: insufficient recruitment allows a rogue cohort & split brain**

```text
R1:  AT_LEAST_2 of {N1, N2, N3, N4}   ← the rule being changed
R2:  AT_LEAST_2 of {N1, N2}           ← recruited nodes accept this
R2': AT_LEAST_2 of {N3, N4}           ← rogue cohort writes this instead

N1 (recruited)   +----+----+----+-----------+----+
                 | t1 | t2 | t3 | R1→R2     | t4 |   ← durable under R2
                 +----+----+----+-----------+----+

N2 (recruited)   +----+----+----+-----------+----+
                 | t1 | t2 | t3 | R1→R2     | t4 |
                 +----+----+----+-----------+----+

                                  - - - - - - - - - - - - - - - - -
N3 (not          +----+----+----+-----------+-----+
   recruited)    | t1 | t2 | t3 | R1→R2'   | t4' |  ← durable under R2'
                 +----+----+----+-----------+-----+    t4 and t4' conflict
N4 (not          +----+----+----+-----------+-----+
   recruited)    | t1 | t2 | t3 | R1→R2'   | t4' |
                 +----+----+----+-----------+-----+
                                  - - - - - - - - - - - - - - - - -
                                  rogue cohort
```

Both rule changes satisfy R1's quorum. Both produce durable subsequent
transactions. The two histories are irreconcilable — this is split brain.
Recruiting 3 of the 4 nodes to close R1 would have left at most 1 unrecruited —
too few to form a rogue quorum.

## Safely Changing the Rules

A safe rule change requires two steps, in order:

**First, block the possibility of any rogue quorum within the old rule's
cohort.** Recruit enough nodes that pledge not to participate in the old rule so
a rogue quorum isn't possible.

**Then, write the rule change transaction to both cohorts with a unique higher
rule number.** The old cohort's acknowledgement ensures the transition appears in
any node's WAL that a future coordinator might recruit from. The new cohort's
acknowledgement makes it durable in the normal sense. Both are required before
the rule change is complete. The rule number uniqueness requirement is to avoid
getting stuck, see below.

These steps guarantee preservation. Split brain — two nodes simultaneously
committing durable transactions under conflicting rules — becomes impossible:
revocation eliminates rogue cohorts, and writing to both cohorts ensures no
future coordinator can miss that the transition happened.

**When the leader is healthy**, it closes the old rule itself. It writes the rule
change transaction, collects acknowledgement from the old cohort to eliminate
rogue cohorts, waits for the new cohort to confirm durability, and continues. No
external coordination needed.

**When the leader is unreachable**, an external coordinator must eliminate rogue
cohorts without the leader's cooperation. It collects enough pledges to block the
old rule from progressing with no possibility of rogue quorum under the old rule.
The most advanced transaction history across the recruited nodes becomes the
starting point for the new rule: any transaction that was durable under the old
rule must exist on at least one recruited node, so nothing is lost. This is why
we needed rule numbers to be unique — if they're not, we could get stuck at this
step and not know which history is the most advanced.

One way to ensure a unique rule number relative to an outgoing rule is to visit a
majority of cohort members within the outgoing rule's cohort. Any other
coordinator attempting a failover would need to visit a majority as well, forcing
at least one overlapping node that ensures at most one coordinator wins the right
to use that proposed rule number (which is relative to the outgoing rule number).

Once a rule change has reached quorum under both cohorts it is durable and
irreversible. Nodes that missed it replay WAL from a node that has it. Nodes with
phantom transactions from a failed attempt rewind to the last common checkpoint
and rejoin — safely, because any transaction lost in the rewind was never
durable. If it had been durable, it would exist on a recruited node, and would
already be part of the new rule's starting point.

Two practical details bridge the protocol to Postgres. First, Postgres has no
native concept of atomically switching which quorum policy is in effect —
`synchronous_standby_names` is a configuration setting, not a transactional
property. We simulate transactional GUC changes by treating the rule change
transaction itself as the durability checkpoint: once enough nodes have confirmed
that LSN, we know the new policy has taken effect for all subsequent
transactions. Second, diverged nodes are reconciled with pg_rewind back to the
last common checkpoint with an authoritative node — safe for the reason given
above.
