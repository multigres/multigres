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
it. A scope has at most one fence, in one of two states:

- `FENCING`: the fence is being raised (not written yet; see below).
- `FENCED`: the scope is closed to application work.

A fence row exists only while its scope is fenced: lifting a fence deletes it.
Each row records the `routing_version` the fence was set at. Fences an
operation never lifted are to be removed when it completes (not implemented
yet).

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
that has finished, so its fences can be cleaned up.

A list must be non-empty, and no two of its scopes may overlap (the same scope
twice, or a whole tablegroup and one of its shards); otherwise the call
answers `INVALID_ARGUMENT`. `FenceScopes` is refused when the operation
does not exist or has finished, a tablegroup is being dropped, or another
unfinished operation holds a fence on a scope that overlaps one in the list: a
whole-tablegroup fence overlaps every shard fence of the tablegroup. A fence
left by an operation that finished without lifting it holds nothing, and the
next operation to fence that scope takes its row over.

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
| `EnsureImportTarget` | `PUT /api/v1/databases/{database}/routing/operations/{operation_id}/import-target`                     | `EnsureImportTarget` |
| `SetRoutingRules`    | `PUT /api/v1/databases/{database}/routing/operations/{operation_id}/rules`                             | `SetRoutingRules`    |
| `DeleteRoutingRules` | `DELETE /api/v1/databases/{database}/routing/operations/{operation_id}/rules?expectedRoutingVersion=N` | `DeleteRoutingRules` |
| `FenceScopes`        | `POST /api/v1/databases/{database}/routing/operations/{operation_id}/fence`                            | `FenceScopes`        |
| `UnfenceScopes`      | `POST /api/v1/databases/{database}/routing/operations/{operation_id}/unfence`                          | `UnfenceScopes`      |

`GetRoutingRules` reads the catalog snapshot and returns its rules by name,
with the `routing_version` a rule change is based on. Every catalog write
advances that version, a new tablegroup as well as a rule change, so a change
based on an older read fails and the caller reads again.

`EnsureImportTarget` creates, if absent, the import operation of a tablegroup
that is already served from an external database. It creates no tablegroup and
no shard: the tablegroup must exist, be `active`, have exactly one shard,
`0-inf`, and that shard must be bound to a connection (a tablegroup created
with `CREATE TABLEGROUP name CONNECTION connection`, whose creation has
completed). A tablegroup or shard made here would be managed, and the unmanaged
pooler of an import would be told its shard is not. A tablegroup that does not
exist answers `NOT_FOUND`; one that is not such a source, or that another
unfinished operation acts on, answers `FAILED_PRECONDITION`, naming
`CREATE TABLEGROUP ... CONNECTION` as the fix where that is it. The default
tablegroup answers `INVALID_ARGUMENT`. It writes no rule and an operation row
is not in the catalog snapshot, so it advances no version, and a retry changes
nothing. The RPC is temporary, for the first version of imports, and will be
deprecated: the import's PREPARE step (registering its tables, the default
rule and fencing) will replace it.

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
`FAILED_PRECONDITION`; a malformed request answers `INVALID_ARGUMENT`; an
import of a tablegroup that does not exist answers `NOT_FOUND`.

## Example: serving a database from an external PostgreSQL

This routes the application traffic of database `app` to an external
PostgreSQL fronted by the unmanaged pooler of tablegroup `srctg`, shard
`0-inf`. `$MULTIADMIN` is multiadmin's HTTP address, `$OP` the import's
operation id (a UUID).

1. Copy the external PostgreSQL's roles into `app` verbatim.
   `pg_dumpall --roles-only` emits `CREATE ROLE` for roles that may already
   exist; edit those (and the bootstrap superuser) out, then load the rest:

   ```bash
   pg_dumpall --roles-only --dbname="$SOURCE_URL" > roles.sql
   ```

   ```bash
   psql "$APP_DEFAULT_PRIMARY_URL" --file=roles.sql
   ```

2. Create a connection to the external PostgreSQL, then the tablegroup it
   serves, through a gateway. The tablegroup's one shard, `0-inf`, is bound to
   the connection:

   ```sql
   CREATE CONNECTION srcdb
     OPTIONS (host 'db.example.com', dbname 'app', user 'repl', password 'secret', sslmode 'verify-full', sslrootcert 'system');
   CREATE TABLEGROUP srctg CONNECTION srcdb;
   ```

   The statement returns once the provisioner has started the shard's
   unmanaged pooler and it serves, which completes the creation and makes the
   tablegroup `active`. See [tablegroup-operations.md](tablegroup-operations.md).

3. Create the import operation. This refuses a tablegroup that is not such a
   source (see above) and creates nothing else:

   ```bash
   curl -X PUT "$MULTIADMIN/api/v1/databases/app/routing/operations/$OP/import-target" -H 'Content-Type: application/json' \
     -d '{"tablegroup": "srctg"}'
   ```

4. Read the current routing version:

   ```bash
   curl "$MULTIADMIN/api/v1/databases/app/routing"
   ```

5. Quiesce the application, then fence `default` and `srctg` in one call,
   passing the version from step 4. The call returns the version the fences
   were written at:

   ```bash
   curl -X POST "$MULTIADMIN/api/v1/databases/app/routing/operations/$OP/fence" -H 'Content-Type: application/json' \
     -d '{"scopes": [{"tablegroup": "default"}, {"tablegroup": "srctg"}], "expectedRoutingVersion": 4}'
   ```

6. Route `default` to `srctg`, passing the version step 5 returned:

   ```bash
   curl -X PUT "$MULTIADMIN/api/v1/databases/app/routing/operations/$OP/rules" -H 'Content-Type: application/json' \
     -d '{"rules": [{"fromTablegroup": "default", "toTablegroup": "srctg", "toShard": "0-inf"}], "expectedRoutingVersion": 5}'
   ```

7. Every gateway reads the new snapshot when the catalog primary's health
   stream reports its routing version, or at the latest within one
   `--sharding-catalog-poll-interval`, and then sends `app`'s application
   traffic to `srctg`. Verify through a gateway, for example by reading a row
   that only the external PostgreSQL has, and check `GET .../routing` lists
   the rule.

8. Deleting the rule undoes step 6. It is safe only while the application
   does not yet rely on `srctg`, for example right after the check in step 7:
   quiesce the application and delete the rule, passing the version the
   previous call returned. Once the application runs on `srctg`, deleting the
   rule sends its traffic to `default`, which does not have the external
   PostgreSQL's data.

   ```bash
   curl -X DELETE "$MULTIADMIN/api/v1/databases/app/routing/operations/$OP/rules?expectedRoutingVersion=6"
   ```
