# Sharding DDL syntax

User-facing SQL for declaring shard layouts. The concepts (tablegroup, shard,
keyrange-id, sharding function) are defined in [design.md](design.md). This
document grows with the implementation: it currently covers tablegroups.
Table placement and shard keys will be added when their syntax lands.

**Status:** the syntax below is parsed and deparsed by `go/common/parser`
(grammar in `postgres.y`, nodes in `ast/sharding.go`). `CREATE TABLEGROUP` and
`DROP TABLEGROUP` execute in the gateway when it runs with
`--enable-tablegroup-ddl`; how they record intent, provision and complete is
in [tablegroup-operations.md](tablegroup-operations.md). Without the flag,
and for `ALTER TABLEGROUP`, the gateway rejects the statement with
`feature_not_supported` rather than forwarding it to postgres, where it would
fail with a syntax error. The rules below that go beyond syntax (coverage,
contiguity, protected objects) are not enforced by the grammar; the catalog
primary enforces them.

Square brackets mark optional clauses; braces with `|` mark alternatives.
All existing PostgreSQL syntax remains available. The new keywords
(`TABLEGROUP`, `SHARDS`, `RESHARD`) are unreserved: they still work as
column, table, and label names.

## Tablegroups

A tablegroup owns a shard map shared by its member tables. Names are local to
the current database. Each database starts with a tablegroup named `default`.
Because `DEFAULT` is a reserved word, that tablegroup is always written
quoted: `"default"`.

```sql
CREATE TABLEGROUP [IF NOT EXISTS] group_name
    [SHARDS ('range' [, ...])];

ALTER TABLEGROUP group_name RENAME TO new_name;

ALTER TABLEGROUP group_name
    RESHARD ('source_range' [, ...])
    INTO ('target_range' [, ...]);

DROP TABLEGROUP [IF EXISTS] group_name [RESTRICT | CASCADE];
```

Without `SHARDS`, a tablegroup has a single shard, `0-inf`, and is unsharded.
A tablegroup is sharded exactly when it has more than one shard.

A tablegroup name is a lowercase DNS label: letters, digits and hyphens,
starting and ending with a letter or digit, at most 25 characters. The name is
reused as a Kubernetes resource name, a pooler flag and a backup path, so it is
held to the strictest of their rules.

`CREATE TABLEGROUP` and `DROP TABLEGROUP` require a superuser. Like
`CREATE DATABASE`, they cannot run inside a transaction block, a pipeline or a
function body, nor in a read-only session, which includes every session on the
gateway's replica-reads port. They return once the change is complete: for
`CREATE`, every shard has a serving primary; for `DROP`, every shard has been
torn down. Cancelling the statement only stops the wait; the change continues
and is visible in `multigres.operations`. If the statement fails before it
names its operation, for instance because the catalog primary is failing over,
the change may still have been recorded: running it again with `IF NOT EXISTS`
or `IF EXISTS` waits for it.

Ranges use `lower-upper` notation, with hexadecimal boundaries and an
exclusive upper bound. `0-inf` covers the entire keyspace. A shard map must
cover the keyspace without gaps or overlaps.

`RESHARD` replaces a contiguous range of active shards with new shards that
cover exactly the same range, for example
`RESHARD ('10-30', '30-40') INTO ('10-20', '20-40')`. Splits and merges are
special cases.

- Resharding a tablegroup's single shard (`RESHARD ('0-inf') INTO (...)`)
  makes it sharded. It is rejected unless every member table has a primary
  shard key.
- Merging a sharded tablegroup back into `0-inf` makes it unsharded again.
  Its tables keep their shard keys.
- The default tablegroup can never be resharded (see below).

`IF NOT EXISTS` and `IF EXISTS` behave as in PostgreSQL: the statement is a
no-op with a notice instead of an error.

`DROP TABLEGROUP` defaults to `RESTRICT`, requiring the tablegroup to be
empty and named by no routing rule. `CASCADE` drops its contained objects and recursively follows their
dependencies, using PostgreSQL dependency semantics; the gateway rejects it
with `feature_not_supported`. All applicable privilege and protected-object
checks must pass before execution. Dropping a tablegroup that is still being
created cancels the creation and, like any drop, returns once the name is
free again.

The default tablegroup is protected: it cannot be renamed, resharded, or
dropped. It houses the metadata tables and the database catalog (MUL-1783),
which the implementation locates by name and expects on a single shard.
