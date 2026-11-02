# Sharding DDL syntax

User-facing SQL for declaring shard layouts. The concepts (tablegroup, shard,
keyrange-id, sharding function) are defined in [design.md](design.md). This
document grows with the implementation: it currently covers tablegroups.
Table placement and shard keys will be added when their syntax lands.

**Status:** the syntax below is parsed and deparsed by `go/common/parser`
(grammar in `postgres.y`, nodes in `ast/sharding.go`). Nothing executes yet:
the gateway rejects every tablegroup statement with `feature_not_supported`
(`ast.IsTableGroupDDL`) rather than forwarding it to postgres, where it would
fail with a syntax error. The rules below that go beyond syntax (coverage,
contiguity, protected objects) are not enforced by the grammar; the
implementation will own those errors.

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
empty. `CASCADE` drops its contained objects and recursively follows their
dependencies, using PostgreSQL dependency semantics. All applicable privilege
and protected-object checks must pass before execution.

The default tablegroup is protected: it cannot be renamed, resharded, or
dropped. It houses the metadata tables and the database catalog (MUL-1783),
which the implementation locates by name and expects on a single shard.
