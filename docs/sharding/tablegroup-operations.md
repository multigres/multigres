# Tablegroup operations

How a tablegroup goes from a `CREATE TABLEGROUP` statement to serving shards.
The concepts (tablegroup, shard, keyrange) are in [design.md](design.md). This
document covers the control plane: who records the intent, who provisions,
and who decides the tablegroup is ready. The SQL surface is a separate change;
until it lands, the operations below are reachable only through the pooler
and multiadmin RPCs.

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
It serves three RPCs on the pooler manager service:

- `CreateTableGroup(name, shards, if_not_exists)` records the intent in one
  transaction: a `multigres.tablegroup` row in state `creating`, one
  `multigres.shard` row per shard in state `preparing`, and a
  `multigres.operation` row of kind `create_tablegroup` in state `creating`.
  It returns the operation id and the tablegroup's catalog id.
- `GetOperation(id)` returns the operation's state, with `shards_serving`
  read from topology at call time.
- `ListTableGroups()` returns every non-default tablegroup that is `creating`
  or `active`, with its shard layout and catalog id.

`GetOperation` and `ListTableGroups` can be answered by any pooler of the
catalog shard, since replicas hold the replicated catalog. `CreateTableGroup`
needs the primary.

**Multiadmin** serves `ListDesiredTableGroups(database)` for provisioners. It
finds the catalog shard's primary in topology and forwards to its
`ListTableGroups`. Multiadmin owns no state and no workflow.

**The provisioner** (the Kubernetes operator) reads desired tablegroups
through multiadmin, provisions pods for every shard, and passes each pooler
`--tablegroup-id` with the catalog id it read.

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

| Existing row | Result |
|---|---|
| none | new tablegroup, shards, and operation |
| `active`, `if_not_exists` set | no change, `already_exists` in the response |
| `active` | `ALREADY_EXISTS` |
| `creating`, same layout | the existing operation id |
| `creating`, different layout | `FAILED_PRECONDITION` |
| `dropping` | `FAILED_PRECONDITION` |

The default tablegroup is seeded `active` at catalog creation, so creating it
is `ALREADY_EXISTS` like any other active tablegroup.

## Completion

A tablegroup creation is complete when every shard in its layout has a pooler
registered in topology that:

- carries the tablegroup's catalog id (`Multipooler.tablegroup_id`),
- is the routing primary of its shard (`routing_state.role == PRIMARY`),
- has serving status `SERVING`, and
- has lifecycle `ACTIVE`, or no lifecycle at all for a record written by an
  older pooler.

The tablegroup id is what keeps poolers left over from a dropped tablegroup
of the same name from counting toward a new one.

The check runs in a loop on the catalog primary. It is level-triggered on
purpose: every pass derives everything from the operation table and a
topology read, keeps nothing in memory between passes, and so resumes after a
failover or restart by running again. The first pass runs immediately on
start. The timer is a few seconds while any operation is pending and thirty
seconds otherwise. There is no topology watch; provisioning takes minutes, so
a few seconds of completion latency is invisible.

When the rule holds, one transaction flips the operation to `done`, the
shards to `active`, the tablegroup to `active`, and advances
`catalog_state.routing_version`. The first statement is a compare-and-set on
the operation's state, so a repeated or concurrent completion finds nothing
to do. A demoted primary cannot commit because its postgres is read-only,
which is the writer fence.

## Failure and cancellation

An operation fails when `cancel_requested` is set on its row, or when it has
been pending longer than one hour. Failing deletes the tablegroup and shard
rows and marks the operation `failed` with the reason. Deleting the rows is
what frees the name for a retry and, because `ListTableGroups` then omits the
tablegroup, what tells the provisioner to remove whatever it provisioned. The
operation keeps its request, so it stays inspectable.

A transient error while checking readiness, such as topology being
unreachable, is recorded in the operation's `last_error` and the next pass
tries again. It does not fail the operation.

What this looks like from each side:

| Scenario | Behavior |
|---|---|
| Caller disconnects while waiting | The operation continues; `GetOperation` shows it |
| Catalog primary fails over | The new primary's first pass resumes every pending operation |
| Provisioner restarts | It reconciles from the list; nothing in Multigres changes |
| Multiadmin or the catalog unreachable from the provisioner | The provisioner must add nothing and remove nothing until a read succeeds |
| Provisioning never produces serving shards | The operation times out and fails; the provisioner removes what it made |
| Duplicate `CreateTableGroup` | The second caller gets the same operation |
| Drop then recreate with the same name | The new id excludes the old poolers |

## Not covered here

Executing the SQL statement in the gateway, the `multigres.operations` view
and `multigres.cancel_operation` function that set `cancel_requested`,
`RENAME`, `DROP`, `RESHARD`, placing tables in a tablegroup, and routing
queries to it. A completed creation produces serving shards with no tables.
`routing_version` advancing is the signal that routing consumers will use;
consuming it lands with table placement.
