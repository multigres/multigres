# Routing rules

A routing rule overrides where a database's catalog sends traffic while an
operation moves data: _traffic the catalog would send to X is served by Y_.
Rules live in the database's catalog, on the primary of its `default`
tablegroup, in `multigres.routing_rule`, and every rule belongs to the
operation (`multigres.operation`) that created it. When no operation runs,
there are no rules.

This page describes what is implemented today: the rule table, how a rule
change is validated, the admission fences a rule change requires, how rules
and fences reach gateways in the catalog snapshot, and the RPCs that set and
read them, and how primary poolers enforce fences. Gateways don't apply rules
yet, and `FenceScopes` does not yet refresh the poolers of the scopes it
fences; both come later.

No code creates an operation that moves data yet: the only operations the
catalog primary records are tablegroup creations, so the `import` and reshard
operations this page refers to exist only as rows a caller of these RPCs would
have to provide.

## Rules

A **shard rule** says that the routing identifiers of tablegroup `G` that fall
in shard `S`'s range are served by `S`, a shard of tablegroup `G'`. It covers
exactly its target shard's range. Rules apply at execute time, as a single hop:
a rule's target is never routed again, and the planner keeps planning against
the catalog.

The `default` tablegroup holds the catalog and is never a rule's source, with
one exception: an **import** may route `default` while no table is registered
in it (`multigres.tablegroup_table` has no `default` rows). Until the gateway
resolves objects, every statement goes to `default`, so this is how all of a
database's traffic (tables, functions, sequences, `SELECT 1`, LISTEN/NOTIFY)
reaches the import's source.

Table rules (a table planned as a member of another tablegroup) are modeled in
the catalog but not accepted yet.

Every change replaces the whole rule set of one operation, in one catalog
transaction that goes through `writeCatalog`, like every other catalog write.
`writeCatalog` locks `multigres.catalog_state` before anything else; the rule
write then checks the caller's expected routing version, validates the rule set
the change leaves behind, and writes it; `writeCatalog` then checks the
snapshot the write would commit, rules included, as gateways do, and advances
`routing_version` (and `plan_epoch`, when the change can alter how a query is
planned). A change that is already in effect writes nothing and returns the
current version, so a retry is safe.

Rules are part of the catalog snapshot that `GetRoutingSnapshot` serves, read
in the same transaction as every other catalog entity, so two snapshots at the
same `routing_version` carry the same rules. Gateways cache them with the rest
of the snapshot.

Operations record the tablegroup they act on by catalog id
(`request->>'tablegroup_id'`) and finish in state `done` or `failed`, as
[tablegroup operations](tablegroup-operations.md) do; a cancelled operation
ends `failed`. A finished operation may not change routing. While another
operation has not finished on a tablegroup, such as its creation, no rule may
route from or to it.

## Admission fences

A rule must only change while both of its sides are **fenced**: closed to
application work, so that no two placements of the same data accept writes at
once. An admission fence belongs to an operation, like a rule, and lives in
`multigres.admission_fence`. Its scope is a whole tablegroup or one shard of
it. A scope has at most one fence, in one of two states:

- `FENCING`: the fence is being raised (not written yet; see below).
- `FENCED`: the scope is closed to application work.

A fence row exists only while its scope is fenced: lifting a fence deletes it,
and so does finishing its operation. Every catalog write that moves an
operation to `done` or `failed` (completing or failing a tablegroup creation,
finishing a drop) deletes that operation's fences in the same transaction, and
advances `routing_version`. Catalog format 2 deleted the fences that finished
operations had left before. So every fence row in the snapshot holds its scope,
in `FENCING` as in `FENCED`, and every reader uses that one definition
(`shardingcatalog.TableGroup.Fenced`): the catalog's own checks, the pooler's
admission gate and the gateway's wait for a fence to lift. Each row records the
`routing_version` the fence was set at.

`FenceScopes` raises an operation's fences on a list of scopes and
`UnfenceScopes` lifts them. Each call is one catalog write through
`writeCatalog`, all or nothing: every fence it writes takes the same routing
version, so gateways see the fences appear together in one snapshot, and a
call it refuses writes none, so a failure never leaves part of a list fenced.
A reshard fences its target shards and the source shards they split in one
call; an import fences `default` and its source tablegroup in one call.

Scopes the operation has already fenced (or, for `UnfenceScopes`, holds no
fence on) are skipped, and a call with nothing left to write returns
the current version whatever version it passed, so a retry of the same list
succeeds. `FenceScopes` checks its expected version against the catalog's
routing version, as rule changes do. `UnfenceScopes` checks it only against
the fences it lifts: it fails if any of them was set after that version, so a
delayed unfence cannot lift a fence raised again since, but unrelated catalog
writes do not force a retry. Lifting is always allowed, even by an operation
that has finished (which by then holds no fences).

A list must be non-empty, and no two of its scopes may overlap (the same scope
twice, or a whole tablegroup and one of its shards); otherwise the call
answers `INVALID_ARGUMENT`. `FenceScopes` is refused when the operation
does not exist or has finished, a tablegroup is being dropped, or another
operation holds a fence on a scope that overlaps one in the list: a
whole-tablegroup fence overlaps every shard fence of the tablegroup. A
dropping tablegroup is deleted only once no fence is left on it.

`SetRoutingRules` and `DeleteRoutingRules` require `FENCED` fences of the same
operation on both sides of every rule the change inserts or removes (a delete
changes where traffic goes as much as an insert):

- the source tablegroup, by a fence on the whole tablegroup or on every active
  shard of it that overlaps the rule's target shard;
- the target, by a fence on the whole target tablegroup or on the target shard.

A missing fence answers `FAILED_PRECONDITION` naming the scope that isn't
fenced. A change that is already in effect needs no fences, so a retry after
the operation unfenced still succeeds.

Fences are part of the catalog snapshot, next to the rules.

### How poolers enforce fences

A primary pooler whose scope (its whole tablegroup, or its shard) is fenced
refuses new application work with `MTF02`, which the gateway retries once the
fence lifts (see [failover buffering](../query_serving/failover_buffering.md)).
The fence gate is a condition of its own: a fenced pooler stays `SERVING`, and
its consensus role, multiorch's view of it and its health stream don't change.
Work on an existing reserved connection is always admitted, and so are
requests that run no application work: a login's credential lookup and an
extended-protocol Describe. Replicas aren't
fenced in this version. A pooler matches its tablegroup by catalog id
(`--tablegroup-id`) when it has one, and otherwise by name; the `default`
tablegroup's poolers have no id, and its name is permanent.

A pooler reads the fences from its database's catalog primary
(`GetRoutingSnapshot`; the catalog primary reads its own catalog in process):

- on `RefreshFences(min_routing_version, drain_timeout)`, which the operation
  calls on every pooler of a scope it fences or unfences. The snapshot read
  must be at `min_routing_version` or later (`FAILED_PRECONDITION`
  otherwise), and a catalog primary that can't be reached fails the call
  (`UNAVAILABLE`) without applying anything. So does a cell whose topology
  can't be read while the pooler looks for the catalog primary: it may hold
  the current catalog primary, so the best primary of the other cells may be a
  deposed one. A refresh that changes nothing is
  a no-op; one that finds no fence on the scope reopens the gate. Every
  pooler serves it, unmanaged ones included.
- when it starts, and each time it becomes primary. Until that read is
  applied it refuses application work with `MTF02` (it fails closed), which
  the gateway retries with backoff. It reads only once it is registered in the
  topology, so an operation that lists the
  scope's poolers after recording a fence either lists the new pooler or the
  new pooler reads the fence. A failed read is retried in the background with
  backoff; nothing but application work on that primary waits for it. A
  catalog primary on which the catalog was never created has no fences, since
  every fence lives in the catalog.
- every 30 seconds while it is fenced, so an unfence it was not refreshed for
  is eventually lifted. Unfenced poolers don't poll.

Minigres has nothing to fence: it never gates and never reads fences.

A pooler applies a read only if the snapshot is not older, by consensus rule
and then `routing_version`, than the newest snapshot it has applied; an older
one fails with `UNAVAILABLE` and is read again. That newest position survives
promotions. During a failover of the catalog shard the topology may still name
the deposed catalog primary, which still answers, with an older snapshot; once
a pooler has applied its successor's snapshot, that answer can't reopen the
gate. A pooler that has just started has applied nothing, so its first read
can be such an answer. The operation's refresh at the fence's version is the
backstop: it must treat a listed pooler it can't refresh as blocking the fence.

When a refresh fences a primary, the pooler drains before the call returns,
the way every drain of the pooler runs (see
[ending a drain at its timeout](../query_serving/graceful_drain_design.md#ending-a-drain-at-its-timeout)),
with a phase of its own, independent of the not-serving drain of a failover:
first new reservations are refused while single queries continue and open
transactions finish; then single queries are refused too and the in-flight
ones finish. Each stage also waits for the requests admitted before it that
don't hold a connection yet. At `drain_timeout` (at most 10 minutes; when
unset, the pooler's drain grace period, or 30 seconds when that is zero),
the drain ends the work left: it seals the pools, so no connection is lent
and every request waiting for one, or asking for one later, is refused with
`MTF02`; it terminates the backends of the connections still lent, reserved
and regular (their transactions get `40001`), and confirms each one exited;
and it waits until nothing admitted is left, then unseals the pools. An
unmanaged pooler kills nothing and only waits.

The gate's states, as the refresh sees them:

- **Draining:** a drain is running. The gate refuses all new work, except
  single queries during the first stage. A refresh that finds it draining
  waits for that drain, bounded by its own deadline.
- **Closed:** the drain ended the fence's work. A closed gate is a drained
  one, so a refresh that finds it closed succeeds at once, and success always
  means drained: nothing admitted before the fence still runs or can start.
- **Draining, with no drain running:** the last drain failed (below).

**A drain that fails.** If a kill isn't confirmed, or work is still lent or
admitted at the settle bound, the drain fails and the refresh fails with
`UNAVAILABLE`, and every refresh waiting on that drain gets the same error.
The failure is reported, but the pooler does not roll back: the fence is
still in the catalog, and the pooler could not show that the scope is
drained. So the gate stays draining, refusing all new work, and the pools
stay sealed, because a request admitted before the fence may still be on its
way to the pool. The pooler's own fence reader keeps re-reading: every 30
seconds, and with backoff (up to 10 seconds) after a read of its own fails.

The pools are sealed for as long as some drain holds the seal, and the
failed drain is over, so the gate keeps its hold (`failedSeal`) until
another drain takes it over or the fence is lifted:

- **The next refresh, or the pooler's re-read, that finds the fence** starts
  a new drain. The new drain takes over the failed drain's hold rather than
  taking a new one, starts by refusing everything, and waits again. If it
  ends the work this time, it releases the hold, the pools unseal, and the
  gate closes. If it fails too, the hold goes back to the gate.
- **A read that finds no fence** opens the gate and releases the hold: the
  operation lifted the fence, so there is no work left to stop.
- **The pooler losing or regaining the primary role** releases the hold
  too: the gate is reset, or not consulted on a replica.

```text
t0  RefreshFences(drain_timeout=5s): the drain starts, the gate is draining
t5  timeout: the pools are sealed, what is lent is killed
    one backend does not exit in time: the drain fails
    the gate stays draining, the pools stay sealed, the gate keeps the hold
    RefreshFences returns UNAVAILABLE: the operation does not move data
t6  RefreshFences again: a new drain takes over the hold and waits
    the backend has exited, nothing is left: the drain succeeds
    the hold is released, the pools unseal, the gate is closed
    RefreshFences succeeds: the operation moves the data
```

**Cancellation.** A read that finds no fence, or the pooler losing or
regaining the primary role, cancels a running drain. It stops, past any kill
already under way, and releases its own hold; refreshes waiting on it get
`ABORTED` and read the fences again.

The drain metrics (`mg.pooler.drain.*`) carry `reason=fence`, or
`reason=failover` for the not-serving drain. A fence drain's outcome is
`graceful`, `force_close` (it ended the work left), `failed` or `cancelled`.

A fence closes only the poolers of its own scope. A rule `A → B` sends
traffic planned for `A` to `B`'s poolers, so while it is in effect a fence on
`A` alone stops none of that traffic. An operation that needs to stop traffic
that a rule routes must fence the rule's target too, as a rule change already
must (see `SetRoutingRules` above).

`FenceScopes` still writes `FENCED` directly and refreshes no pooler, so until
the caller has refreshed every pooler of every scope, the fence is not
enforced everywhere. A later change makes `FenceScopes` write the whole list as
`FENCING`, refresh and drain every pooler of every scope in parallel, and only
then write the list as `FENCED`.

When a statement is refused with `MTF02`, the gateway
(`go/services/multigateway/executor/fence.go`) waits until its cached catalog
snapshot no longer fences the tablegroup and shard that refused it, then plans
and runs the statement again. The first retry runs at once; a pooler that
refuses again hasn't caught up with the snapshot yet (it hasn't read its
fences since it started, or hasn't been refreshed since the unfence), so each
later retry backs off, from 10 ms doubling to 250 ms. The total wait of one
statement is bounded by `--buffer-window`, whether or not failover buffering
is enabled; when it runs out, the client gets the `MTF02`. A statement in a
transaction is retried only while the session holds no reserved connection: a
transaction's `BEGIN` is deferred to its first statement, so until a statement
of it reaches a pooler, nothing of it has run. A statement is not retried, and
the client gets the `MTF02` at once, when its session holds a reserved
connection in a transaction, when it is in an extended-protocol pipeline
before its Sync, when an earlier step of its plan already ran on a pooler, or
when output has already reached the client. Minigres has no catalog cache and
runs every statement once.

## API

The catalog primary serves the RPCs on its manager service; multiadmin
forwards them to it.

| Multiadmin RPC       | Multiadmin REST                                                                                        | Pooler RPC           |
| -------------------- | ------------------------------------------------------------------------------------------------------ | -------------------- |
| `GetRoutingRules`    | `GET /api/v1/databases/{database}/routing`                                                             | `GetRoutingSnapshot` |
| `SetRoutingRules`    | `PUT /api/v1/databases/{database}/routing/operations/{operation_id}/rules`                             | `SetRoutingRules`    |
| `DeleteRoutingRules` | `DELETE /api/v1/databases/{database}/routing/operations/{operation_id}/rules?expectedRoutingVersion=N` | `DeleteRoutingRules` |
| `FenceScopes`        | `POST /api/v1/databases/{database}/routing/operations/{operation_id}/fence`                            | `FenceScopes`        |
| `UnfenceScopes`      | `POST /api/v1/databases/{database}/routing/operations/{operation_id}/unfence`                          | `UnfenceScopes`      |

`GetRoutingRules` reads the catalog snapshot and returns its rules by name,
with the `routing_version` a rule change is based on. Every catalog write
advances that version, a new tablegroup as well as a rule change, so a change
based on an older read fails and the caller reads again.

The fence RPCs name their scopes in the request body; an empty `shard` fences
the whole tablegroup:

```json
{
  "scopes": [
    { "tablegroup": "commerce", "shard": "0-80" },
    { "tablegroup": "commerce", "shard": "0-40" },
    { "tablegroup": "commerce", "shard": "40-80" }
  ],
  "expectedRoutingVersion": 4
}
```

Errors: a pooler that is not the catalog primary answers `UNAVAILABLE` to a
rule or fence change (find the primary and retry); a stale expected version, a
missing fence, or a change the catalog does not allow answers
`FAILED_PRECONDITION`; a malformed request answers `INVALID_ARGUMENT`.
