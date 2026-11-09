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

## The statements

`CREATE TABLEGROUP` and `DROP TABLEGROUP` run in the gateway, which never
sends them to postgres. They are off unless the gateway runs with
`--enable-tablegroup-ddl`: only a cluster whose provisioner acts on the
catalog can complete them. Without the flag the gateway rejects them with
`feature_not_supported`. Minigres has no such flag: a database served by one
pooler cannot add shards.

Each statement:

1. Checks the caller's own session on the catalog shard and refuses, where
   and in the order PostgreSQL refuses `CREATE DATABASE`: a read-only session
   (`transaction_read_only`, always on for a session on the replica-reads
   port, whose queries run on a standby), a transaction block, and a
   statement after another in the same extended-protocol pipeline, unless
   that one committed on its own (a transaction statement, `DISCARD ALL`,
   `VACUUM`, `CREATE DATABASE` and the other statements PostgreSQL commits as
   soon as they complete, which start the next statement clean). Then the
   caller's current role must be a superuser.
2. Records the intent through the catalog primary (below) and sends a
   `NOTICE` naming the operation id. If the call fails without an answer (a
   timeout, an unreachable or failing-over primary, or the statement
   cancelled), the intent may have been recorded anyway, and the error says
   so: running the statement again with `IF NOT EXISTS` or `IF EXISTS` joins
   the operation.
3. Waits, polling the operation every second, until it is `DONE` or
   `FAILED`.

Cancelling the statement, a statement timeout, or closing the connection only
stops the wait. The operation is durable in the catalog and continues; the
notice says so and names where to follow it. `statement_timeout` applies as
it does to `CREATE DATABASE`, and the gateway's default is 30 seconds, so a
session that wants the statement to wait out provisioning sets
`statement_timeout = 0` first.

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
  on the creation instead and returns that operation. For a tablegroup
  already `dropping` it returns the drop operation, recording one first if
  the tablegroup has none, which is the case after a failed creation.
- `GetOperation(id)` returns the operation's state. Progress is read from
  topology at call time: `shards_serving` for a creation,
  `poolers_registered` for a drop.
- `ListTableGroups()` returns every non-default tablegroup, in any state, with
  its shard layout and catalog id: `creating` and `active` ones for the
  provisioner to provision, `dropping` ones for it to remove.

All four need the elected primary of the catalog shard: postgres out of
recovery and the pooler the active consensus leader, which is its routing
role `PRIMARY`. Out of recovery is not enough on its own: a standby promoted
before its rule is committed and a former primary not yet demoted are both out
of recovery, and either may lag the real primary, so `GetOperation` would
miss a write the caller just made and `ListTableGroups` would describe an
older desired state.

**Multiadmin** serves `ListDesiredTableGroups(database)` for provisioners. It
finds the catalog shard's serving primary in topology, taking the highest rule
if two poolers claim the role during a failover, and forwards to its
`ListTableGroups`. Multiadmin owns no state and no workflow.

**The provisioner** (the Kubernetes operator) reads tablegroups through
multiadmin. For a `creating` or `active` tablegroup it has nothing for yet, it
first writes a **provision claim** for the tablegroup's catalog id in the
global topology (`TableGroupProvisionClaim`, at
`databases/<database>/TableGroupClaims/<id>`,
`topoclient.ClaimTableGroupProvisioning`), reads the list again, and only if
the tablegroup is still `creating` or `active` provisions every shard: its
poolers, with `--tablegroup-id` set to the id, and a multiorch watching the
shard. If it is not, the provisioner releases the claim and provisions
nothing. The second read is what closes the gap between a list and a claim: a
tablegroup that was never claimed is deleted as soon as its drop is recorded,
so a provisioner that listed it `creating` just before could otherwise claim
and provision an id the catalog has already forgotten. It keys everything it
provisions by the id, so acting on one incarnation of a name never touches
another.

For a `dropping` tablegroup it removes what it provisioned, deletes those
poolers' records from topology, and **last** releases its claim
(`ReleaseTableGroupProvisioning`). The claim is the provisioner's word
that it still holds something for the id, and the catalog does not delete the
tablegroup while it stands (below).

A tablegroup missing from a list is never a reason to remove anything, and
neither is a failed read, which is never an empty list. A list from a node
whose catalog lags can therefore delay a change, but never removes what should
exist. The rule is that the one that owns the processes writes the record
first and deletes it last.

**Poolers and multiorch** are unchanged. A new pooler registers in topology
with its database, tablegroup, shard, and tablegroup id. Multiorch bootstraps
each shard and appoints a primary.

**Backups.** Every shard has its own pgBackRest repository under the
database's backup location, since all shards use the same stanza name and
cannot share one. The catalog shard's repository is the location's base path,
where it has always been. Any other shard's is
`tablegroups/<tablegroup>/<tablegroup id>/<shard>` below it:
`<path>/tablegroups/commerce/7/0-80` on a filesystem,
`/<prefix>/tablegroups/commerce/7/0-80/multigres` on S3. The fixed
`tablegroups` directory keeps any tablegroup name out of the catalog shard's
own repository layout. The catalog id keeps a tablegroup recreated under a
dropped one's name from reusing its repository, whose backups are kept. A
provisioner needs no per-shard backup configuration, only write access to
everything under the location.

Because the path includes the catalog id, the id of a running shard must
never change. A shard's repository is created once, at its first backup, so
a pooler pointed at another path finds no stanza there: archiving fails and
WAL builds up, and a new replica has nothing to restore. Each pooler therefore
records its repository's path below the location in
`<pooler-dir>/pgbackrest/repository_scope` on its first start, and refuses to
serve when a later start, with a different `--table-group`, `--tablegroup-id`
or `--shard`, would use another. Removing that file accepts the new
repository.

## Following operations from SQL

Two catalog objects let a superuser follow and steer operations with plain
SQL on the catalog shard; the gateway's `CREATE TABLEGROUP` and
`DROP TABLEGROUP` notices name them.

- `multigres.operations` is a view over `multigres.operation` with the
  request unpacked: `operation_id`, `kind`, `state`, `tablegroup_id`,
  `tablegroup_name`, `shards`, `cancel_requested`, `last_error`,
  `created_at`, `updated_at`. An operation stays readable after its
  tablegroup's rows are gone.
- `multigres.cancel_operation(uuid)` sets `cancel_requested` on a creation
  still in flight and returns whether this call set it: `false` for an
  operation that is not a pending creation or was already cancelled. The
  completion loop then fails the creation, exactly as `DROP TABLEGROUP` on a
  creating tablegroup does.

The view has no grant and the function's default `EXECUTE` for `PUBLIC` is
revoked, so only superusers can use them, like the statements.

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

| Existing row | `CreateTableGroup`                                                                               | `DropTableGroup`                             |
| ------------ | ------------------------------------------------------------------------------------------------ | -------------------------------------------- |
| none         | new tablegroup, shards, and operation                                                            | `NOT_FOUND`, or `not_found` with `if_exists` |
| `creating`   | the existing operation if the layout matches or with `if_not_exists`, else `FAILED_PRECONDITION` | cancels the creation and returns it          |
| `active`     | `ALREADY_EXISTS`, or `already_exists` with `if_not_exists`                                       | new drop operation                           |
| `dropping`   | `FAILED_PRECONDITION`                                                                            | the existing drop operation                  |

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
catalog id remains in topology **and** no provision claim for the id is held.
A pooler that stops gracefully leaves its record behind marked shut down, and
one that crashes leaves it as it was, so the provisioner deleting the records
is part of what completes a drop. The claim covers the case the records
cannot: a provisioner that has created resources whose poolers have not
registered yet. Without it, a drop issued in that window would complete at
once, the tablegroup would vanish from the list before the provisioner ever
saw it `dropping`, and the resources would be orphaned. A tablegroup that was
never claimed completes as soon as its drop is recorded.

The tablegroup id is what keeps poolers of one incarnation of a name from
counting toward another, in both directions.

The check runs in a loop on the catalog primary (`tablegroup_loop.go`),
started by the multipooler service on the catalog shard and relaunched on
every reopen. It is level-triggered on purpose: every pass derives everything
from the catalog and a topology read, keeps nothing in memory between passes,
and so resumes after a failover or restart by running again. The first pass
runs immediately on start, then every three seconds. There is no topology
watch and no wake-up on a new operation; a pass with nothing pending is two
small queries over tiny tables and reads no topology, and a few seconds of
latency is invisible next to provisioning. Only the elected primary runs a
pass; any other pooler skips it.

When the rule holds, one catalog write finishes the change. Like every
catalog write it runs through `writeCatalog` (`routing_snapshot.go`), which
locks `catalog_state` first, checks the snapshot the write would leave,
advances `catalog_state.routing_version`, commits, and publishes the new
version on the primary's health stream so gateways refetch their cached
snapshot at once. A creation flips the operation to `done` and the shards and
tablegroup to `active`. A drop deletes the shard and tablegroup rows and flips
the drop operation, if there is one, to `done`; once that has committed it
deletes each shard's initialization claim from topology, which is keyed by
name and would otherwise stop the next tablegroup of that name from
bootstrapping. The topology write comes after the commit because the commit is
the fence. The first statement is a
compare-and-set on the operation's or tablegroup's state, so a repeated or
concurrent completion finds nothing to do and advances nothing. Recording or
clearing a transient `last_error` goes through the same path and, touching
only operation rows, advances nothing. A demoted primary cannot commit because
its postgres is read-only, which is the writer fence.

A creation that has not completed within an hour is failed with that reason:
provisioning that has produced no serving shards in that long is stuck, and a
failed operation is inspectable and frees the name, where a pending one holds
it. Cancelling a creation sooner is `DROP TABLEGROUP` on it.

## Failure and cancellation

A creation fails when `cancel_requested` is set on its row. Failing marks the
operation `failed` with the reason and moves the tablegroup to `dropping`,
which tells the provisioner to remove whatever it provisioned. The rows are
deleted under the drop rule above, once no pooler record and no claim remain,
and that is what frees the name for a retry. The operation keeps its request,
so it stays inspectable.

A drop never fails: its teardown cannot be undone, so it stays `dropping`
until the provisioner finishes. Backups of the dropped shards are left in
place.

A transient error while checking either rule, such as topology being
unreachable, is recorded in the operation's `last_error`, once, and the next
pass tries again; the loop logs the outage when it starts and when it ends. It
does not fail the operation. A tablegroup dropping because its creation failed
has no drop operation to record on, and is covered by the log alone.

What this looks like from each side:

| Scenario                                                     | Behavior                                                                                                                                                                                                                                                                                        |
| ------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Caller disconnects while waiting                             | The operation continues; `GetOperation` shows it                                                                                                                                                                                                                                                |
| `DropTableGroup` while the tablegroup is still being created | The creation is cancelled and fails on the next pass, leaving the tablegroup `dropping`. The gateway then drops again and waits for the rows to go, so `DROP TABLEGROUP` succeeds only once the name is free; if the creation completed first, that second drop drops the now-active tablegroup |
| Catalog primary fails over                                   | The new primary's first pass resumes every pending operation                                                                                                                                                                                                                                    |
| Provisioner restarts                                         | It reconciles from the list; nothing in Multigres changes                                                                                                                                                                                                                                       |
| Multiadmin or the catalog unreachable from the provisioner   | The provisioner must add nothing and remove nothing until a read succeeds                                                                                                                                                                                                                       |
| Teardown never deletes the pooler records                    | The drop stays `dropping`, with `poolers_registered` showing what is left                                                                                                                                                                                                                       |
| The provisioner never releases its claim                     | The drop stays `dropping` until the claim is released or an operator removes it, like a leaked Kubernetes finalizer                                                                                                                                                                             |
| The list comes from a former primary that lags               | A change is delayed; nothing is removed, since absence is not a signal                                                                                                                                                                                                                          |
| Duplicate `CreateTableGroup` or `DropTableGroup`             | The second caller gets the same operation                                                                                                                                                                                                                                                       |
| Drop then recreate with the same name                        | The new id excludes the old poolers                                                                                                                                                                                                                                                             |
