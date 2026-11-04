# Tablegroup operations

How a tablegroup goes from a `CREATE TABLEGROUP` statement to serving shards,
and from `DROP TABLEGROUP` back to nothing. The concepts (tablegroup, shard,
keyrange) are in [design.md](design.md). This document covers the control
plane: who records the intent, who provisions, and who decides an operation
is finished.

## Authority

- **The sharding catalog** (`multigres.*` on the catalog shard, the default
  tablegroup's `0-inf` shard) owns which tablegroups should exist, their shard
  layouts, their lifecycle, and the operations that change them. It is the
  single source of truth for desired state.
- **Cluster configuration** (the operator's YAML) owns templates, cells,
  replica counts, storage, images, and policy.
- **Topology** holds runtime pooler registrations.
- **Multiorch** bootstraps shards and elects primaries. Unchanged.

Desired state flows one way: catalog, then API, then provisioner, then pods.
The provisioner reports nothing back. Multigres detects completion from its
own topology.

## Components

**Catalog primary pooler.** The multipooler that is primary for the catalog
shard is the only writer of the catalog tables, through its admin connection.
It serves four RPCs on the pooler manager service:

- `CreateTableGroup(name, shards, if_not_exists)` records the intent in one
  transaction: a `multigres.tablegroup` row in state `creating`, one
  `multigres.shard` row per shard in state `preparing`, and a
  `multigres.operation` row of kind `create_tablegroup` in state `creating`.
  It returns the operation id and the tablegroup's catalog id.
- `DropTableGroup(name, if_exists)` moves an active tablegroup to `dropping`
  and records a `drop_tablegroup` operation in state `dropping`, in one
  transaction. For a tablegroup still `creating` it sets `cancel_requested`
  on the creation instead and returns that operation.
- `GetOperation(id)` returns the operation's state. Progress is read from
  topology at call time: `shards_serving` for a creation,
  `poolers_registered` for a drop.
- `ListTableGroups()` returns every non-default tablegroup that is `creating`
  or `active`, with its shard layout and catalog id.

`GetOperation` and `ListTableGroups` can be answered by any pooler of the
catalog shard, since replicas hold the replicated catalog. `CreateTableGroup`
and `DropTableGroup` need the primary.

**Multiadmin** serves `ListDesiredTableGroups(database)` for provisioners. It
finds the catalog shard's primary in topology and forwards to its
`ListTableGroups`. Multiadmin owns no state and no workflow.

**The provisioner** (the Kubernetes operator) reads desired tablegroups
through multiadmin and, for each, provisions every shard: its poolers, with
`--tablegroup-id` set to the catalog id it read, and a multiorch watching the
shard. For a tablegroup that leaves the list, or whose catalog id changed, it
removes what it provisioned, then deletes those poolers' records from
topology and each shard's initialization claim, so a later tablegroup of the
same name bootstraps afresh. It acts only on a successful read: a failed read
is never an empty list.

**Poolers and multiorch** are unchanged. A new pooler registers in topology
with its database, tablegroup, shard, and tablegroup id. Multiorch bootstraps
each shard and appoints a primary.

## Validation

`CreateTableGroup` validates before touching the catalog:

- The name is a lowercase DNS label, letters, digits and hyphens, at most 25
  characters. The name is used unchanged as a Kubernetes name and label, a
  pooler flag, a topology key, and a backup path, so it is held to the
  strictest of those rules. Lifting the rule later is additive.
- The shard list covers the whole keyspace with no gaps, overlaps, or
  duplicates (`keyrange.ValidateShardLayout`). It may be given in any order;
  the catalog stores it in canonical order. An empty list means one `0-inf`
  shard.

Idempotency on the name:

| Existing row | `CreateTableGroup`                                                       | `DropTableGroup`                             |
| ------------ | ------------------------------------------------------------------------ | -------------------------------------------- |
| none         | new tablegroup, shards, and operation                                    | `NOT_FOUND`, or `not_found` with `if_exists` |
| `creating`   | the existing operation if the layout matches, else `FAILED_PRECONDITION` | cancels the creation and returns it          |
| `active`     | `ALREADY_EXISTS`, or `already_exists` with `if_not_exists`               | new drop operation                           |
| `dropping`   | `FAILED_PRECONDITION`                                                    | the existing drop operation                  |

`DropTableGroup` also refuses the default tablegroup, and any tablegroup that
contains tables, with `FAILED_PRECONDITION`.

The default tablegroup is seeded `active` at catalog creation, so creating it
is `ALREADY_EXISTS` like any other active tablegroup.

## Completion

A tablegroup **creation** is complete when every shard in its layout has a
pooler registered in topology that:

- carries the tablegroup's catalog id (`Multipooler.tablegroup_id`),
- is the routing primary of its shard (`routing_state.role == PRIMARY`),
- has serving status `SERVING`, and
- has lifecycle `ACTIVE`, or no lifecycle at all for a record written by an
  older pooler.

A **drop** is complete when no pooler record carrying the tablegroup's
catalog id remains in topology. A pooler that stops gracefully leaves its
record behind marked shut down, and one that crashes leaves it as it was, so
the provisioner deleting the records is what completes a drop.

The tablegroup id is what keeps poolers of one incarnation of a name from
counting toward another, in both directions.

The check runs in a loop on the catalog primary. It is level-triggered on
purpose: every pass derives everything from the operation table and a
topology read, keeps nothing in memory between passes, and so resumes after a
failover or restart by running again. The first pass runs immediately on
start, then every three seconds. There is no topology watch and no wake-up
on a new operation; a pass is one query over the small operation table, and
a few seconds of latency is invisible next to provisioning.

When the rule holds, one transaction finishes the operation and advances
`catalog_state.routing_version`. A creation flips the operation to `done` and
the shards and tablegroup to `active`; a drop flips the operation to `done`
and deletes the shard and tablegroup rows. The first statement is a
compare-and-set on the operation's state, so a repeated or concurrent
completion finds nothing to do. A demoted primary cannot commit because its
postgres is read-only, which is the writer fence.

## Failure and cancellation

A creation fails when `cancel_requested` is set on its row. Failing deletes
the tablegroup and shard rows and marks the operation `failed` with the
reason. Deleting the rows is what frees the name for a retry and, because
`ListTableGroups` then omits the tablegroup, what tells the provisioner to
remove whatever it provisioned. The operation keeps its request, so it stays
inspectable.

A drop never fails: its teardown cannot be undone, so it stays `dropping`
until the provisioner finishes. Backups of the dropped shards are left in
place.

A transient error while checking either rule, such as topology being
unreachable, is recorded in the operation's `last_error` and the next pass
tries again. It does not fail the operation.

What this looks like from each side:

| Scenario                                                     | Behavior                                                                                                                     |
| ------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------- |
| Caller disconnects while waiting                             | The operation continues; `GetOperation` shows it                                                                             |
| `DropTableGroup` while the tablegroup is still being created | The creation is cancelled and fails on the next pass. If it completed first, a repeated drop drops the now-active tablegroup |
| Catalog primary fails over                                   | The new primary's first pass resumes every pending operation                                                                 |
| Provisioner restarts                                         | It reconciles from the list; nothing in Multigres changes                                                                    |
| Multiadmin or the catalog unreachable from the provisioner   | The provisioner must add nothing and remove nothing until a read succeeds                                                    |
| Teardown never deletes the pooler records                    | The drop stays `dropping`, with `poolers_registered` showing what is left                                                    |
| Duplicate `CreateTableGroup` or `DropTableGroup`             | The second caller gets the same operation                                                                                    |
| Drop then recreate with the same name                        | The new id excludes the old poolers                                                                                          |
