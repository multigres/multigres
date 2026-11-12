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
read them. Gateways don't apply rules yet, and poolers don't enforce fences
yet; both come later.

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
it. A scope has at most one fence, in one of three states:

- `FENCING`: the fence is being raised (not written yet; see below).
- `FENCED`: the scope is closed to application work.
- `UNFENCED`: the fence was lifted.

Lifting a fence sets it to `UNFENCED` rather than deleting it, and every fence
write stamps the row with the `routing_version` it commits at, so a delayed
unfence cannot override a later fence. Fences are removed when their operation
completes (not implemented yet).

`FenceScopes` raises an operation's fences on a list of scopes and
`UnfenceScopes` lifts them. Each call is one catalog write through
`writeCatalog`, all or nothing: every fence it writes takes the same routing
version, so gateways see the fences appear together in one snapshot, and a
call it refuses writes none, so a failure never leaves part of a list fenced.
A reshard fences its target shards and the source shards they split in one
call; an import fences `default` and its source tablegroup in one call.

Calls follow the same expected version and retry rules as rule changes. Scopes
the operation has already fenced (or, for `UnfenceScopes`, holds no raised
fence on) are skipped, and a call with nothing left to write returns the
current version whatever version it passed, so a retry of the same list
succeeds. A list must be non-empty, and no two of its scopes may overlap (the
same scope twice, or a whole tablegroup and one of its shards); otherwise the
call answers `INVALID_ARGUMENT`. `FenceScopes` is refused when the operation
does not exist or has finished, a tablegroup is being dropped, or another
unfinished operation holds a raised fence on a scope that overlaps one in the
list: a whole-tablegroup fence overlaps every shard fence of the tablegroup. A
lifted fence, or one of a finished operation, holds nothing, and the next
operation to fence that scope takes the row over.

`SetRoutingRules` and `DeleteRoutingRules` require `FENCED` fences of the same
operation on both sides of every rule the change inserts or removes (a delete
changes where traffic goes as much as an insert):

- the source tablegroup, by a fence on the whole tablegroup or on every active
  shard of it that overlaps the rule's target shard;
- the target, by a fence on the whole target tablegroup or on the target shard.

A missing fence answers `FAILED_PRECONDITION` naming the scope that isn't
fenced. A change that is already in effect needs no fences, so a retry after
the operation unfenced still succeeds.

Fences are part of the catalog snapshot, next to the rules. **Nothing enforces
them yet**: poolers don't read fences, so `FenceScopes` writes `FENCED`
directly and that records intent only. Until poolers enforce them, the caller
must still quiesce the affected application traffic before changing rules. A
later change makes `FenceScopes` write the whole list as `FENCING`, refresh
and drain every pooler of every scope in parallel, and only then write the
list as `FENCED`.

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
