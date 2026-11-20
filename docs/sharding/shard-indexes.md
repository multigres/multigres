# Shard indexes

A table's rows are placed by its **primary shard key**: an ordered list of the
table's columns, passed to a **shard index**. A shard index is a named,
database-wide way to compute a **routing identifier** (a byte string) from
column values. The routing identifier picks the shard whose key range contains
it (see [the two functions](./design.md#the-two-functions)).

This page describes what is implemented today: the extension point in
`go/common/shardindex`, and the built-in `hash` index for integer columns.
Nothing calls it yet. Shard-index and shard-key DDL, planner integration,
`hash` for text, uuid and bytea, and the `range`, `multicol` and `lookup`
indexes come later.

## The model

- **Implementation**: a type name (`hash`) and a factory, compiled into the
  binaries and registered once with `shardindex.Register`. Built-in
  implementations and user-provided ones register the same way.
- **Shard index**: an instance of an implementation. It is a row of
  `multigres.shard_index` with a `name`, the implementation's `type` and JSON
  `params`, built with `shardindex.New(type, name, params)`.
  `shardindex.ParseParams` decodes the catalog's `params` text; every value
  must be a JSON string.
- **Parameters**: a string map that each factory validates itself. A parameter
  the implementation does not know is an **error**, not a warning: a misspelt
  parameter that is silently ignored would place rows by a mapping nobody asked
  for.

Every implementation provides the base interface, `ShardIndex`:

| Method       | Meaning                                                                                        |
| ------------ | ---------------------------------------------------------------------------------------------- |
| `Name()`     | The instance name                                                                              |
| `Type()`     | The implementation's type name                                                                 |
| `Cost()`     | 1 when the routing identifier is computed from the value alone, 2 or more when it needs lookup |
| `IsUnique()` | One value maps to at most one routing identifier                                               |

### Capabilities

What an index can do is declared by optional interfaces, detected by type
assertion. Each matches a kind of predicate the planner can route.

| Capability     | Methods                                                           | Used for                                         |
| -------------- | ----------------------------------------------------------------- | ------------------------------------------------ |
| `SingleColumn` | `Map(ctx, exec, values)`, `Verify(ctx, exec, values, routingIDs)` | Equality and `IN` lists on one column            |
| `Hashing`      | `Hash(value)`                                                     | Being a component of a future multi-column index |
| `TypeChecking` | `AcceptsType(oid)`                                                | Shard-key DDL validating a key's column types    |

A `Value` is a PostgreSQL type OID, a datum, and whether it is NULL.

`Map` and `Verify` take an `Executor`. Its method set is empty today; it is in the
signatures so that lookup indexes can later query shards without changing what
every implementation compiles against.

The package grows without breaking implementations: `ShardIndex` and the
existing capabilities never gain methods, and multi-column keys, ranges,
prefixes, lookup-entry maintenance and reverse mapping arrive as new optional
interfaces.

### Destinations

`Map` returns one `Destination` per value:

| Destination                  | Resolves to                               |
| ---------------------------- | ----------------------------------------- |
| `DestinationRoutingID(id)`   | The shard owning the routing identifier   |
| `DestinationRoutingIDs(ids)` | The shards owning each routing identifier |
| `DestinationKeyRange(kr)`    | Every shard overlapping the key range     |
| `DestinationAllShards()`     | Every shard                               |
| `DestinationNone()`          | No shard                                  |

A destination does not depend on a shard layout. The caller resolves it when it
routes, with `Destination.Resolve(tableGroup, add)`, against the tablegroup's
active shards in the catalog snapshot. Routing rules and fences are applied by
the caller, not by `Resolve`.

## The stability contract

**The mapping is permanent.** Rows are placed by the routing identifier, so
once an implementation has shipped, its mapping from value to routing
identifier must never change: not for a bug fix, not for a new PostgreSQL
version, not for a new platform. A different mapping needs a new type name.

This covers everything that feeds the routing identifier: the function, its
seed and constants, how each type is normalized, and how the result is encoded
as bytes. Every binary in a cluster must compute the same identifier.

`hash` is pinned by golden vectors generated from PostgreSQL 17.6
(`go/common/shardindex/testdata/hash_golden.json`). The unit tests fail if the
mapping moves, and the opt-in differential suite re-checks the vectors and
random values against a real server:

```text
RUN_PGEVAL_DIFFERENTIAL=1 /mt-dev integration pgeval TestScalarsAgainstPostgres
```

## `hash`

`hash` maps an integer column to a point spread uniformly over the keyspace.

| Property       | Value                                                |
| -------------- | ---------------------------------------------------- |
| Column types   | `smallint`, `integer`, `bigint`                      |
| Parameters     | None; any parameter is an error                      |
| `IsUnique()`   | true                                                 |
| `Cost()`       | 1                                                    |
| Capabilities   | `SingleColumn`, `Hashing`, `TypeChecking`            |
| NULL           | An error: primary shard key columns must be NOT NULL |
| Any other type | An error                                             |

### Definition

The routing identifier of a value is

```text
hashint8extended(value::int8, seed)
```

exactly as PostgreSQL computes it, encoded as 8 bytes, big-endian.

- **Function.** PostgreSQL's `hashint8extended`: the int8 is folded to 32 bits
  so that equal numbers hash equally at every width, then hashed with
  `hash_bytes_uint32_extended`. The port lives in `go/common/pghash`, which the
  expression engine's `hashint*` builtins share.
- **Widths.** `smallint` and `integer` values are sign-extended to `bigint`
  first. A value has the same routing identifier whatever integer type its
  column has, as it does in PostgreSQL's hash opclasses.
- **Seed.** `0x7A5B22367996DCFD` (`8816678312871386365` as an int8),
  PostgreSQL's `HASH_PARTITION_SEED`. It is the constant
  `shardindex.HashSeed`, and it is permanent.
- **Encoding.** The 64-bit result, read as unsigned, most significant byte
  first. Byte order is then numeric order, so the key range `0-80` owns the
  lower half of the hash values and `80-inf` the upper half.

### In SQL

The same routing identifier is computable on any shard, with no multigres code:

```sql
int8send(hashint8extended(col::int8, 8816678312871386365))
```

`int8send` writes the int8 big-endian, and `bytea` values compare bytewise,
which is how key ranges compare routing identifiers. To check which shard a row
belongs to from `psql`:

```sql
SELECT int8send(hashint8extended(42::int8, 8816678312871386365)) AS routing_id;
--      routing_id
-- --------------------
--  \x663218bcfb84b17f
```

`66…` is below `80`, so in a tablegroup split into `0-80` and `80-inf` the row
with key 42 belongs to `0-80`.

A key range is a predicate on the same expression: a start bound is inclusive,
an end bound exclusive, and an unbounded side is left out. The rows of
`orders` that belong to shard `40-80`:

```sql
SELECT *
FROM orders
WHERE int8send(hashint8extended(customer_id::int8, 8816678312871386365)) >= '\x40'
  AND int8send(hashint8extended(customer_id::int8, 8816678312871386365)) < '\x80';
```

The migrator will use this form to filter rows by key range during a reshard.

## Writing an implementation

1. Implement `shardindex.ShardIndex` and the capabilities the index supports.
2. Register a factory from `init()`. The factory validates its parameters;
   `shardindex.CheckParams` rejects unknown names.
3. Link the package into the gateway and pooler binaries with a blank import.

```go
package regionindex

import (
  "context"

  "github.com/multigres/multigres/go/common/shardindex"
)

func init() { shardindex.Register("region", newIndex) }

type index struct{ name string }

func newIndex(name string, params map[string]string) (shardindex.ShardIndex, error) {
  // No parameters: any name is unknown.
  if err := shardindex.CheckParams("region", name, params); err != nil {
    return nil, err
  }
  return &index{name: name}, nil
}

func (x *index) Name() string   { return x.name }
func (x *index) Type() string   { return "region" }
func (x *index) Cost() int      { return 1 }
func (x *index) IsUnique() bool { return true }

// Map makes the index a shardindex.SingleColumn, together with Verify.
// Verify and routingID, the mapping itself, are left out here.
func (x *index) Map(ctx context.Context, exec shardindex.Executor, values []shardindex.Value) ([]shardindex.Destination, error) {
  out := make([]shardindex.Destination, len(values))
  for i, v := range values {
    out[i] = shardindex.DestinationRoutingID(routingID(v))
  }
  return out, nil
}
```

The blank import goes in a plugin file next to each binary's `main`, the
pattern `go/cmd/multigateway/plugin_topo.go` uses for topology backends:

```go
// go/cmd/multigateway/plugin_shardindex_region.go
// go/cmd/multipooler/plugin_shardindex_region.go
package main

import _ "example.com/regionindex"
```

Rules for an implementation:

- Registering a type name twice panics at start-up. Type names are global:
  pick one that will not collide with a future built-in.
- All binaries of a cluster must link the implementation, and it must compute
  the same routing identifier in each. Do not depend on the platform, locale,
  time or anything outside the value and the parameters.
- The [stability contract](#the-stability-contract) applies. Check golden
  vectors into the implementation's tests.
- `Map` and `Hash` run on the query path. They must be safe for concurrent use
  and should not allocate beyond their result.
- Return errors built with `go/common/mterrors`, with a SQLSTATE: the client
  sees them.

How user-provided implementations are packaged and built into release binaries
is not designed yet.
