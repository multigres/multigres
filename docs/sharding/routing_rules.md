# Routing rules

A routing rule overrides where a database's catalog sends traffic while an
operation moves data: _traffic the catalog would send to X is served by Y_.
Rules live in the database's catalog, on the primary of its `default`
tablegroup, in `multigres.routing_rule`, and every rule belongs to the
operation (`multigres.operation`) that created it. When no operation runs,
there are no rules.

This page describes what is implemented today: the rule table, how a rule
change is validated, and how rules reach gateways in the catalog snapshot.
Nothing applies rules yet; the RPCs that set them, and the gateway routing that
applies them, come later.

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
