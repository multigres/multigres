# Routing rules

A routing rule overrides where a database's catalog sends traffic while an
operation moves data: _traffic the catalog would send to X is served by Y_.
Rules live in the database's catalog, on the primary of its `default`
tablegroup, in `multigres.routing_rule`, and every rule belongs to the
operation (`multigres.operation`) that created it. When no operation runs,
there are no rules.

This page describes what is implemented today: the rule table, how a rule
change is validated, and the RPCs that set and read rules. Gateways don't apply
rules yet; that comes later.

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
transaction that locks `multigres.catalog_state`, checks the caller's expected
routing version, validates the rule set the change leaves behind, and
increments `routing_version`. A change that is already in effect succeeds and
returns the current version, so a retry is safe.

## API

The catalog primary serves the RPCs; multiadmin forwards them to it.

| RPC                  | Multiadmin REST                                                                                        | Service            |
| -------------------- | ------------------------------------------------------------------------------------------------------ | ------------------ |
| `GetRoutingSnapshot` | `GET /api/v1/databases/{database}/routing`                                                             | MultipoolerService |
| `SetRoutingRules`    | `PUT /api/v1/databases/{database}/routing/operations/{operation_id}/rules`                             | MultipoolerManager |
| `DeleteRoutingRules` | `DELETE /api/v1/databases/{database}/routing/operations/{operation_id}/rules?expectedRoutingVersion=N` | MultipoolerManager |

Errors: a pooler that is not the catalog primary answers `UNAVAILABLE` (find
the primary and retry); a stale expected version or a change the catalog does
not allow answers `FAILED_PRECONDITION`; a malformed request answers
`INVALID_ARGUMENT`.
