# Routing rules

A routing rule overrides where a database's catalog sends traffic while an
operation moves data: _traffic the catalog would send to X is served by Y_.
Rules live in the database's catalog, on the primary of its `default`
tablegroup, in `multigres.routing_rule`, and every rule belongs to the
operation (`multigres.operation`) that created it. When no operation runs,
there are no rules.

This page describes what is implemented today: the rule table and how a rule
change is validated. Nothing reads or applies rules yet; the RPCs that set and
read them, and the gateway routing that applies them, come later.

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
