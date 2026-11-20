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
  (`UNAVAILABLE`) without applying anything. A refresh that changes nothing is
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

When a refresh fences a primary, the pooler drains before the call returns,
with a phase of its own, independent of the not-serving drain of a failover:
first new reservations are refused while single queries continue and open
transactions finish; then single queries are refused too and the in-flight
ones finish. Each stage of either drain also waits for the requests admitted
before it that don't hold a connection yet, so a `BEGIN` admitted just before
the fence can't get its connection after the drain has finished. At
`drain_timeout` (the pooler's drain grace period when unset), the remaining
reserved connections are force-closed and their transactions get `40001`. The
drain metrics (`mg.pooler.drain.*`) carry `reason=fence`, or `reason=failover`
for the not-serving drain.

`FenceScopes` still writes `FENCED` directly and refreshes no pooler, so until
the caller has refreshed every pooler of every scope, the fence is not
enforced everywhere. A later change makes `FenceScopes` write the whole list as
`FENCING`, refresh and drain every pooler of every scope in parallel, and only
then write the list as `FENCED`.

The gateway side of enforcement is in place
(`go/services/multigateway/executor/fence.go`). A pooler that refuses work on
a fenced target is to answer `MTF02`; nothing returns it yet. When a statement
is refused with it, the gateway waits until its cached catalog snapshot has a
`routing_version` newer than the one the statement was planned against and no
longer fences that tablegroup and shard, then plans and runs the statement
again. The total wait of one statement is bounded by `--buffer-window`,
whether or not failover buffering is enabled; when it runs out, the client
gets the `MTF02`. A statement is not retried, and the client gets the `MTF02`
at once, when the session is in a transaction or a pipeline, when an earlier
step of its plan already ran on a pooler, or when output has already reached
the client. Minigres has no catalog cache and runs every statement once.

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
