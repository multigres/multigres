# SQL interface for managing migrations

**Status.** Design proposal for team discussion. This note proposes a **new DDL interface** for managing migrations and source connections from an ordinary SQL client. A CALL-based alternative that was considered is kept in Appendix A. It covers the client-facing surface and where it is implemented, not the migrator internals (see the Multigres Migrator design doc).

## Overview

The Multigres Migrator coordinates logical-replication table migrations. Today it is driven only over RPC: multiadmin re-exposes the migrator RPCs (`CreateMigration`, `SetMigrationDirection`, `ListMigrations`, `GetMigration`, `DropMigration`), which forward to the owning migrator on the target shard's primary multipooler. `SetMigrationDirection` is the single RPC for every direction change — there is no separate start/activate/deactivate RPC: on a not-yet-started migration it runs the setup pipeline, otherwise it performs the cutover or the rollback, and setting the current direction is a no-op. There is no way for an ordinary Postgres client (psql, an application connection) to create, start, observe, cut over, or drop a migration — or to manage the source-server connection a migration reads from.

This note proposes a **new DDL interface** at the multigateway: first-class statements such as `CREATE MIGRATION`, `ALTER MIGRATION`, and `CREATE CONNECTION`. These verbs are syntax errors in stock Postgres, so the gateway recognizes and parses them and handles them in-gateway (never forwarding them to Postgres), translating each to the existing migrator RPCs. Because they are not valid Postgres, this surface cannot be delivered as an in-database extension — it lives in the gateway. A CALL-based, extension-shaped alternative (`CALL migrator.create_migration(...)` plus views) was considered and is documented in Appendix A.

Since the syntax will be sticky (once delivered, it will be hard to make users change their behavior), making a good decision here is essential.

For that reason we implement the **full grammar** up front — including forms the migration RPC does not back yet (per-table column lists and `WHERE` filters, `ONLY`/`*`, declaratively-partitioned tables, …). Options that are not yet wired are parsed but never silently dropped: most are rejected immediately by the gateway with a plain error (no SQLSTATE mapping), while migrating a declaratively-partitioned table is rejected by the backend with a typed `feature_not_supported` error (SQLSTATE `0A000`) — see "Backing in the migrator RPCs" for the exact split. This also anticipates a wider data-movement initiative that these statements are expected to serve, beyond the current migrator.

## Common ground

- **Same control path.** The surface translates to the existing RPCs: gateway → multiadmin → owning migrator (target shard primary) → coordinator. No new migration semantics are introduced here.
- **Result sets.** `SHOW CONNECTION[S]` and a recognized `SELECT` against `multigres.stat_migration` (see "Status and inspection") return a synthesized `RowDescription` + `DataRow`s built from the migrator RPCs; the mutating statements return a command tag (`CREATE MIGRATION`, `ALTER MIGRATION`, …). Errors surface as ordinary Postgres `ErrorResponse` (bad arguments, unknown migration, migrator unreachable, validation rejection).
- **Credentials.** A source DSN / password is passed straight to the RPC and is **never** written to gateway logs, or query/audit logs. Neither `SHOW CONNECTION[S]` nor a query against `multigres.stat_migration` ever includes the password: a migration's source is shown only as its `connection_name`, never a raw DSN.

## Connections

A connection is a stored, named source endpoint reused across migrations, so credentials are not repeated inline. Connections are persisted in Postgres (`multigres.migration_connection`) on the target shard's primary multipooler, physically replicated like a migration record — so a named connection survives a gateway restart and is visible to every gateway replica, not just the one that created it.

The grammar is specified in full below, taking inspiration from `CREATE SERVER` / `ALTER SERVER` / `DROP SERVER`.

```text
CREATE CONNECTION [ IF NOT EXISTS ] connection_name
    [ OPTIONS ( connection_option 'value' [, ... ] ) ]

DROP CONNECTION [ IF EXISTS ] connection_name [, ... ]

SHOW CONNECTIONS

SHOW CONNECTION connection_name
```

The terminals are: `connection_name`, an identifier; `connection_option`, an identifier naming a libpq connection keyword (`host`, `port`, `dbname`, `user`, `password`, `sslmode`, `sslrootcert`, `sslcert`, `sslkey`, and the rest); and `'value'`, a string literal. The grammar accepts any option name — the accepted set is validated at runtime, not by the grammar. `password` is stored but never shown by `SHOW`. Connections are immutable once created — there is no `ALTER CONNECTION`; changing one means `DROP` and `CREATE` a replacement. `DROP CONNECTION` is gated: the migration table's `connection_id` has no `ON DELETE` clause, so Postgres applies its default `RESTRICT` — dropping a connection still referenced by a migration fails with a foreign-key-violation error rather than silently orphaning it.

### Examples

```sql
CREATE CONNECTION onprem OPTIONS (host 'db.example.com', dbname 'app', user 'repl', sslmode 'verify-full');
-- Connections are immutable: changing one means dropping and recreating it.
DROP CONNECTION IF EXISTS onprem;
CREATE CONNECTION onprem OPTIONS (host 'db2.example.com', dbname 'app', user 'repl', sslmode 'verify-full');
SHOW CONNECTIONS;
```

## Migrations

The grammar is specified in full below. The table-selection clause takes inspiration from `CREATE PUBLICATION` and the `CONNECTION` reference from `CREATE FOREIGN TABLE`, but the grammar is defined independently here.

```text
CREATE MIGRATION [ IF NOT EXISTS ] migration_name
    CONNECTION connection_name
    { FOR ALL TABLES | FOR migration_object [, ... ] }
    [ WITH ( migration_option [ = value ] [, ... ] ) ]

ALTER MIGRATION [ IF EXISTS ] migration_name action

DROP MIGRATION [ IF EXISTS ] migration_name [, ... ]
    { FORCE | [ WHEN ( cond ) ] }
    [ WITH ( option [ = value ] ) ]
```

Migration status is read, not shown: see "Status and inspection" below — `SELECT ... FROM multigres.stat_migration [ WHERE migration_name = ... | WHERE migration_id = ... ]`, not a dedicated `SHOW` verb.

where `action` is:

```text
PHASE { IMPORT | EXPORT } [ WHEN ( cond ) ] [ WITH ( option [ = value ] ) ]
```

`CONNECTION connection_name` (re-pointing a migration's source) and `SET ( migration_option [ = value ] [, ... ] )` (changing `sequence_margin`/tables in place) are a candidate for future reintroduction (see the migration-foundation RFC) but are not currently implemented — `PHASE` is the only `ALTER MIGRATION` action today.

where `migration_object` is one of:

```text
TABLE [ ONLY ] table_name [ * ] [ ( column_name [, ... ] ) ]
    [ WHERE ( expression ) ]
    [, ... ]
TABLES IN SCHEMA { schema_name | CURRENT_SCHEMA } [, ... ]
```

and `migration_option` is one of:

```text
copy_data [ = boolean ]
skip_schema_copy [ = boolean ]
sequence_margin [ = integer ]
quiesce_roles [ = string ]
```

and `cond` — the `WHEN` readiness-gate condition shared by `ALTER MIGRATION`'s `PHASE` action and `DROP MIGRATION` — is:

```text
field OP constant | constant OP field
```

where `field` is one of `multigres.stat_migration`'s columns (see "Status and inspection") and `OP` is one of
`< <= = >= > <>`. Currently only `lag_bytes { < | = } size` is backed; every other field/operator combination parses
but is rejected at runtime with a typed `feature_not_supported` error (SQLSTATE `0A000`) — the same pattern "Backing in
the migrator RPCs" describes for other not-yet-wired forms. `size`/`constant` is an integer or a string: a bare
integer is exact bytes, a string is parsed the same way, optionally carrying a byte unit — binary `KiB`/`MiB`/`GiB`/
`TiB` or decimal `KB`/`MB`/`GB`/`TB`/`B`, case-insensitive, the space optional, fractional values allowed and rounded
(e.g. `'8 MiB'`, `'8MiB'`, `'1.5 GiB'`).

and `option` (the `WITH ( … )` bag for `PHASE`/`DROP MIGRATION`, a separate vocabulary from `migration_option`) is:

```text
wait_timeout [ = string ]
```

- `migration_name` and `connection_name` are Postgres identifiers.
- `table_name` is a table name optionally schema-qualified (`[ schema_name . ] table_name`)
- `ONLY` restricts it to just that table (no inheritance children) and a trailing `*` includes descendants (the default)
- `column_name` is a column of that table
- `expression` is a boolean row-filter over the table's columns
- `schema_name` is a schema name, with `CURRENT_SCHEMA` selecting the session's current schema
- `boolean` / `integer` / `string` Postgres literals (boolean, integer, or string).

Notes:

- `FOR ALL TABLES` migrates every table in the source database.
- `FOR TABLES IN SCHEMA` migrates every table in the named schema(s).
- `FOR TABLE` migrates the listed tables.
- A `FOR TABLE` entry may carry a projected column list and a row filter.
- A `FOR TABLES IN SCHEMA` entry may not carry a projected column list nor a row filter.
- An unqualified `table_name` resolves through the source `search_path`; qualify it (e.g. `FOR TABLE sales.orders, public.customers`) to select across schemas.
- There is no explicit target clause — the migration lands in the database/cluster the client is connected to through the gateway.
- The lifecycle operations are `ALTER MIGRATION` subcommands (mirroring `ALTER SUBSCRIPTION … ENABLE | DISABLE | REFRESH | SET`), not standalone verbs.
- `ALTER MIGRATION … PHASE { IMPORT | EXPORT }` sets the replication direction — the one lifecycle action for every direction change, maps to `SetMigrationDirection`. The first call on a `CREATED` migration (`PHASE IMPORT`) runs the full pipeline (validate → schema copy → publication → subscription → catch-up) and lands `IMPORTING`, not serving — there is no separate `START` action. A later call cuts over (`IMPORTING` → `EXPORTING`, serving from the target) or rolls back (`EXPORTING` → `IMPORTING`); if the current direction is already correct, it is a no-op. The keyword's two values name the **direction** being set, not a literal `migration_phase` value — `multigres.stat_migration`'s `migration_phase` column reports the derived steady state as `IMPORTING`/`EXPORTING`, not `IMPORT`/`EXPORT`.
- `WHEN ( cond )` gates the cutover (`PHASE EXPORT`) on replication readiness: it waits until the live replication lag is at or below the given threshold (e.g. `WHEN (lag_bytes < '1 MiB')`) before quiescing the source, bounded by `WITH (wait_timeout = ...)` (a duration like `'30s'` or a bare integer number of seconds; default 30s). If the lag doesn't converge in time, the call fails with a not-ready error and leaves the migration unchanged (still importing) rather than starting a cutover that could overrun the gateway's failover buffer window. `WHEN`/`WITH` are syntactically available on `PHASE IMPORT` too, but have no backed use there today.
- `quiesce_roles` names the source application role(s) (comma-separated) whose `CONNECT` is revoked during the cutover to `EXPORT`, so they cannot reconnect and write to the source once it becomes a subscriber (a stray write there would diverge). The cutover always freezes and terminates live client backends; naming roles additionally fences reconnects. Restored on rollback (`PHASE IMPORT`) / `DROP`. Each role must exist and must not be the connection's own role.
- `DROP MIGRATION` tears the migration down.
  - `FORCE` drops it from any phase without draining. It is equivalent to `WHEN (true)`: it overrides the readiness condition rather than bypassing the `WHEN`/`WITH` mechanism entirely.
  - `WHEN ( cond )` — the same `cond` grammar as `ALTER MIGRATION`'s `WHEN` — gates the drop on replication readiness; `=` is typically used to require an exact catch-up (`WHEN (lag_bytes = 0)`), the default behavior when `WHEN` is omitted (requires the migration already caught up right now, erroring otherwise, rather than waiting for it). `WITH (wait_timeout = ...)` bounds how long to wait for the condition instead of requiring it to hold immediately.
  - `FORCE` and `WHEN` are mutually exclusive. `FORCE` and `WITH` are not: `wait_timeout` is moot under `FORCE` (there's nothing to wait for), but other `WITH` options still apply.

### Examples

```sql
-- an explicit set of tables
CREATE MIGRATION orders_move CONNECTION onprem FOR TABLE orders, customers;

-- a column list plus a row filter on one table (parses; not yet wired, see "Backing in the migrator RPCs")
CREATE MIGRATION tenant42 CONNECTION onprem
    FOR TABLE orders (id, total, tenant_id) WHERE (tenant_id = 42), customers
    WITH (copy_data = true, sequence_margin = 1000);

-- every table in a schema
CREATE MIGRATION app_public CONNECTION onprem FOR TABLES IN SCHEMA public;

-- the whole source database
CREATE MIGRATION full_copy CONNECTION onprem FOR ALL TABLES;

ALTER MIGRATION orders_move PHASE IMPORT; -- first call: runs the full pipeline, lands IMPORTING
ALTER MIGRATION orders_move PHASE EXPORT WHEN (lag_bytes < '1 MiB') WITH (wait_timeout = '30s'); -- go-live cutover, gated on replication readiness
ALTER MIGRATION orders_move PHASE IMPORT; -- roll back (stop serving → flip)
SELECT * FROM multigres.stat_migration WHERE migration_name = 'orders_move';
DROP MIGRATION IF EXISTS orders_move WITH (wait_timeout = '60s');
```

### Partitioned and inherited tables

The selection maps directly onto stock Postgres logical replication — the `CREATE PUBLICATION` / `CREATE SUBSCRIPTION` primitives the migrator already drives — so nothing custom is needed in Postgres. All the relevant features are available at the Multigres PG17+ floor: `FOR ALL TABLES` (always), `FOR TABLES IN SCHEMA` and column-list / `WHERE` row filters (PG15+).

- **`ONLY` / `*`** are the inheritance markers from `CREATE PUBLICATION`. `TABLE ONLY t` includes `t` but not its descendants; `TABLE t` (or `t *`) includes descendants. This is meaningful for **legacy inheritance** (`INHERITS`), where the parent has its own rows. `TABLE t` / `TABLE t *` (the default, descendants included) works today for an ordinary or legacy-inherited table; `TABLE ONLY t` is rejected immediately by the gateway with a plain error — `ONLY` is parsed but not yet wired.
- **Declarative partitioning is not supported in any form today.** Migrating a declaratively-partitioned table is rejected unconditionally — regardless of `ONLY`, a column list, or a `WHERE` filter — by a typed `feature_not_supported` (SQLSTATE `0A000`) error raised when the source is validated (at `CREATE MIGRATION`, and again at `PHASE IMPORT` or a table-selection update): "table %q is a partitioned table; partitioned-table migration is not yet supported". This check runs against every table the migration resolves, regardless of which `FOR` form named it — `FOR ALL TABLES` or `FOR TABLES IN SCHEMA` fails the same way if the matched set includes a declaratively-partitioned table.

`FOR ALL TABLES`, `FOR TABLES IN SCHEMA`, and named/qualified tables are wired in the RPC (see "Backing in the migrator RPCs"); per-table column lists, `WHERE` filters, and `ONLY` are parsed but rejected immediately by the gateway, before any RPC call.

## Status and inspection

Migration status is read through `multigres.stat_migration`, a real Postgres view on the target (see the Multigres Migrator design doc for the exact `CREATE VIEW`), not a dedicated `SHOW` verb. It is a genuine view — queryable directly against the target Postgres, with any `SELECT` Postgres accepts (joins, aggregates, arbitrary `WHERE`) — but the gateway additionally recognizes one narrow shape and answers it itself, over the RPC path, instead of forwarding it:

```text
SELECT column_name [, ... ] | *
FROM multigres.stat_migration
[ WHERE migration_name = 'name' | WHERE migration_id = id ]
```

A column-list projection (or bare `*`) against this one view, with at most one equality filter on `migration_name` or `migration_id`, no joins, no aggregation. Omitting the `WHERE` lists every migration; filtering on `migration_name` or `migration_id` returns the single matching row — replacing what used to be separate `SHOW MIGRATIONS` / `SHOW MIGRATION <name>` verbs.

This recognized shape is answered directly via `ListMigrations` + `GetMigration`, bypassing the serving gate the same way `CREATE`/`ALTER`/`DROP MIGRATION` already do — the point is that migration status stays visible even while the target is `NOT_SERVING` (e.g. mid-`IMPORTING`, exactly when an operator most wants to check progress before cutover). A query outside the recognized shape has no RPC path, so the gateway falls back to forwarding it as an ordinary query against the real view — correct, but subject to the normal serving gate.

### `multigres.stat_migration` columns

| Column                | Type              | Description                                                                                                                      |
| --------------------- | ----------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| migration_id          | bigint            | Generated internal id (a UnixNano timestamp captured at creation); the stable key, independent of migration_name                  |
| migration_name        | text              | Optional human-friendly name, unique per target database; empty if none was given at create time                                 |
| connection_name       | name               | Name of the connection the migration reads its source from (see `SHOW CONNECTION`)                                               |
| migration_target      | ShardKey composite | The destination shard: `(database, table_group, shard)` — reuses `clustermetadata.ShardKey`, matching `Migration.target`         |
| migration_phase       | text              | Lifecycle phase (CREATED, VALIDATING, SCHEMA_COPY, CREATE_PUBLICATION, COPYING, IMPORTING, EXPORTING, SWITCHING_TO_EXPORT, SWITCHING_TO_IMPORT, COMPLETING, FAILED) |
| active_direction      | text              | Which side is currently the publisher (IMPORT / EXPORT), derived from migration_phase                                            |
| total_relations       | bigint            | Number of tables in the initial copy                                                                                              |
| ready_relations       | bigint            | Number of tables that have finished the initial copy                                                                              |
| lag_bytes             | bigint            | Replication lag, in bytes                                                                                                         |
| lag_seconds           | double precision  | Replication lag, in seconds                                                                                                       |
| last_error            | text              | Last error, if the migration is in the FAILED phase                                                                               |

`migration_phase`/`active_direction` are plain `text`, not native Postgres `ENUM`s — matching Postgres's own convention for state/kind discriminator columns (`pg_class.relkind`, `pg_stat_replication.state`) and avoiding the friction a native enum has for a vocabulary that is still being revised (can't remove a value, can't reorder without drop+recreate). `migration_target` is a genuine structured composite, not an evolving vocabulary, so it gets a dedicated type.

**Caveat on `total_relations`/`ready_relations`/`lag_bytes`/`lag_seconds` when the view is queried directly** (bypassing the gateway's RPC path): the real view can only read local Postgres catalogs on the target, so it only has full visibility into whichever side of the replication link currently lives on the target. During IMPORT the target holds the local subscription, so `total_relations`/`ready_relations` are accurate but `lag_bytes`/`lag_seconds` read 0 (the external source's publisher-side lag isn't locally visible). During EXPORT it's the reverse: `lag_bytes`/`lag_seconds` are accurate (the target is the local publisher) but `total_relations`/`ready_relations` read 0 (no local subscription for this migration). The gateway's RPC-backed path is authoritative in both directions, since the coordinator can reach across to the source directly — this is the practical reason the gate-bypass optimization exists, not just a serving-availability nicety.

### `SHOW CONNECTION[S]` columns

`SHOW CONNECTIONS` returns one row per connection; `SHOW CONNECTION <name>` returns the single matching row. The password is never included.

| Column  | Type | Description                         |
| ------- | ---- | ----------------------------------- |
| name    | text | Name of the connection              |
| host    | text | Source host from the connection     |
| port    | text | Source port                         |
| dbname  | text | Source database name                |
| user    | text | Role used to connect to the source  |
| sslmode | text | TLS mode negotiated with the source |

## Implementation notes

- Dedicated DDL that reads as first-class cluster administration, with command tags like `CREATE MIGRATION`, `ALTER MIGRATION`.
- Implemented in the gateway only, by **extending the goyacc grammar** with new keywords, productions, and AST nodes, so the statements parse into first-class AST that the gateway planner dispatches (rather than a separate prefix recognizer). The trade-off is a maintenance cost: the new productions must be carried across upstream Postgres grammar re-syncs, and new keywords should be added as unreserved to avoid breaking existing queries.
- Cannot be implemented as an extension, even if we wanted to — the verbs are not valid Postgres.
- Because it is a grammar extension we fully control, there is no need to split a statement into locally executed parts and parts forwarded to Postgres (contrast the CALL API in Appendix A).
- `SHOW CONNECTIONS` (the bare plural, no name) has no dedicated grammar production: it parses as an ordinary `SHOW <guc-name>` and is intercepted by a name match in the planner, not produced by the `ShowConnectionsStmt` production the singular, named form uses. Migration status has no `SHOW` grammar at all — a plain `SELECT` already parses, so the planner instead recognizes a supported shape against `multigres.stat_migration` directly at the `T_SelectStmt` dispatch (see "Status and inspection").

## Backing in the migrator RPCs

The migrator RPCs now back most of the grammar (implemented on `shard-migration`, `proto/migratorservice.proto`; multiadmin forwards the same request types unchanged, so the gateway and the multiadmin/CLI paths share them). Forms the RPC does not yet honor are rejected before anything mutates, either by the gateway itself or, for one case, by the backend with a typed error (see below).

Wired end-to-end:

- **Name.** `CreateMigrationRequest.name` (unique per target database) and `Migration.name` in the projection; `name` is accepted on Get/Start/Activate/Deactivate/Drop/Update. Resolution is id-first, then a unique name; a duplicate name at create is rejected. The generated id is a plain `int64` (a UnixNano timestamp captured at creation) and stays the stable internal key; the record lives in the sidecar migration table (`multigres.migration`).
- **Table selection.** `Migration.objects` is a `SelectionObject` oneof — `all` (bool, for `FOR ALL TABLES`), `table` (`TableSpec{qualified_names []string}`), or `schema` (`SchemaSpec{schemata []string}`), never more than one. So `FOR ALL TABLES`, `FOR TABLES IN SCHEMA`, and named/qualified tables all bind to real fields. `TableSpec` carries only `qualified_names` — there is no field for a per-table column list, `WHERE` filter, or `ONLY`/descendants, so those are rejected by the gateway itself (see below) rather than by the RPC.
- **Options.** `copy_data` (proto3 `optional bool` — unset means the default `true`; send `false` to skip the initial COPY), `skip_schema_copy`, and `quiesce_roles` (validated server-side: each named role must exist and must not be the connection's own role).
- **Cutover.** `ALTER MIGRATION … PHASE { IMPORT | EXPORT }` maps 1:1 to the single, serving-coupled `SetMigrationDirection` RPC. `PHASE EXPORT`'s `WHEN (lag_bytes < size)` gates on the replication-readiness threshold (`SetMigrationDirectionRequest.max_lag_bytes`) the coordinator enforces before quiescing the source, bounded by `WITH (wait_timeout = ...)`. `DROP MIGRATION`'s `WHEN (lag_bytes = 0)` maps onto the same request's existing `wait`/`wait_timeout_seconds` fields — no new RPC field was needed there, since "no `WHEN`" already meant "require caught up now" and `WHEN (lag_bytes = 0)` is just that same behavior spelled out explicitly. `PHASE` is the only `ALTER MIGRATION` action; re-pointing a migration's source connection or changing `sequence_margin`/tables in place (formerly `ALTER MIGRATION … SET (…)` / `… CONNECTION name`, mapped to `UpdateMigration`) is a candidate for future reintroduction (see the migration-foundation RFC) but not currently implemented.

Parsed but rejected before any mutation happens:

- Per-table **column lists**, **`WHERE`** row filters, and **`ONLY`**/descendants on `TABLE` entries are rejected immediately by the gateway with a plain error (no SQLSTATE mapping) — `TableSpec` has no wire representation for any of them, so there is nothing to forward to the RPC.
- Migrating a **declaratively-partitioned table**, in any `FOR` form, is rejected by the backend at source validation with a typed `feature_not_supported` error (SQLSTATE `0A000`, assertable with `mterrors.IsErrorCode(err, mterrors.PgSSFeatureNotSupported)`), surfaced to the client as an ordinary `ErrorResponse`.

**Status.** `ListMigrations` (returns ids) plus `GetMigration` (by name or id, returns one migration) back the recognized `SELECT ... FROM multigres.stat_migration` shape, projecting the 11 columns listed above. The sidecar row and status machinery carry more than this — `publication_name`, `subscription_name`, `created_at`, and `streaming_since` all exist but aren't projected into the view today; a per-table copy-state or journal view would need further projection additions.

## Open questions

1. **Target** — keep the target implicit (the database/cluster the client is connected to through the gateway), or add an explicit `INTO database/shard` clause?
2. Implement the full grammar now (see Overview); unimplemented options are parsed and rejected with a clear "not yet supported" error, rather than trimming the grammar to what the RPC backs today. This is currently the approach taken, but open to discussion.

## Closed questions

- Implement it by extending the goyacc grammar with new productions and AST nodes (not a standalone prefix recognizer), accepting the upstream-re-sync maintenance cost in exchange for first-class AST and planner integration.
- **Identity:** a distinct, unique-per-database `name` field; the generated `int64` id (a UnixNano timestamp captured at creation) stays the stable internal key.
- **Statement shape, resolved:** lifecycle operations are `ALTER MIGRATION migration_name PHASE { IMPORT | EXPORT } [ WHEN (…) ] [ WITH (…) ]` (mirroring `ALTER SUBSCRIPTION … ENABLE | DISABLE | REFRESH | SET`), not a standalone verb. `PHASE` is the one lifecycle action for every direction change — no separate `START`/`ACTIVATE`/`DEACTIVATE` — and maps 1:1 to the single `SetMigrationDirection` RPC (no serving-neutral `SWITCH`). This settles the alternative floated below (`SET DIRECTION TO IMPORT`): `PHASE` reads better alongside `WHEN`/`WITH`, and "setting the direction is a no-op when already there" replaces the "not idempotent" concern `SWITCH` raised. (A `CONNECTION conn` / `SET (…)` action re-pointing a migration's source or changing its configuration in place, mapped to an `UpdateMigration` RPC, existed at one point but was removed — a candidate for future reintroduction, see the migration-foundation RFC.)
- **Connection durability:** persisted in Postgres (`multigres.migration_connection`) on the target shard's primary multipooler, physically replicated alongside the migration row — not gateway-scoped, not topo-backed. A migration holds a live reference (`connection_id`) rather than a copy of the DSN, so a connection survives a gateway restart or replica swap. Connections are immutable once created, and a migration's `connection_id` is set once at create time and not currently changeable afterward.

## Appendix A — considered alternative: the CALL API (Postgres-native)

A `migrator` schema exposing procedures/functions for the mutating operations and views for the read side. Because it uses only statements Postgres already parses (`CALL`, `SELECT`), it changes no grammar and could in theory be shipped as a real `CREATE EXTENSION multigres_migrator` and run inside Postgres — or be intercepted by the gateway post-parse. It was set aside in favor of the DDL surface above; the comparison and details are kept here for the record.

### Comparison

| Dimension                     | DDL interface (this proposal)            | CALL API (considered)                           |
| ----------------------------- | ---------------------------------------- | ----------------------------------------------- |
| Statement style               | new DDL verbs                            | standard `CALL` / `SELECT`                      |
| Can be a Postgres extension   | No — gateway only                        | Yes                                             |
| Runs inside Postgres          | No — gateway only                        | Yes (as the extension)                          |
| Gateway grammar changes       | Yes — goyacc grammar productions         | None                                            |
| Upstream `gram.y` re-sync tax | Yes — new productions carried on re-sync | None                                            |
| Connection storage            | `CREATE CONNECTION` (Postgres table)     | `create_connection` proc, or native FDW objects |
| Reads                         | `SELECT` from a real view (see below)    | `SELECT` from views                             |
| Operator "feel"               | first-class DDL                          | function calls                                  |

**Reads converged with this alternative.** The row above originally read `SHOW MIGRATIONS` / `SHOW MIGRATION` on this side — a dedicated `SHOW` grammar, like the mutating verbs. It was later replaced with a real `multigres.stat_migration` view plus gateway recognition of a `SELECT` against it (see "Status and inspection"), which is exactly this column's own `SELECT from views` answer for the read side. The composability argument below is about the **mutating** verbs (`CREATE`/`ALTER`/`DROP MIGRATION`), which stayed closed DDL; reads don't carry that risk the same way, since a read has no state to corrupt by being composed into an arbitrary query — the gateway answers the shapes worth a serving-gate bypass and falls back to the real view (correct, just gate-bound) for everything else.

### Why the DDL surface was preferred

The deciding factor is **composability**. A function or view is an ordinary SQL object, so once it exists users can drop it into any SQL context — and each context forces the gateway to either run a mini query-engine locally or split the statement and round-trip sub-queries to the primary, then stitch the results back. The DDL verbs are **closed**: each can only appear as a whole top-level statement the gateway parses and fully owns, so interception is complete and unambiguous. Concretely, the function/view surface leaks into:

- **The view used as a data source composed with real relations** — the gateway must synthesize view rows and combine them with primary data:

  ```sql
  CREATE TABLE foo AS SELECT * FROM migrator.migrations;             -- CTAS / SELECT INTO
  INSERT INTO audit.snapshot SELECT * FROM migrator.migrations;      -- INSERT…SELECT
  SELECT m.name, t.owner FROM migrator.migrations m JOIN teams t USING (name);
  SELECT * FROM orders WHERE tenant = ANY (SELECT name FROM migrator.migrations);
  WITH f AS (SELECT name FROM migrator.migrations WHERE phase = 'FAILED')
    DELETE FROM jobs USING f WHERE jobs.mig = f.name;
  SELECT name FROM migrator.migrations UNION SELECT name FROM legacy;
  COPY (SELECT * FROM migrator.migrations) TO STDOUT CSV;
  ```

- **Query features over the synthesized view** — the gateway would have to reimplement executor semantics:

  ```sql
  SELECT phase, count(*) FROM migrator.migrations GROUP BY phase HAVING count(*) > 1;
  SELECT DISTINCT phase FROM migrator.migrations ORDER BY created_at LIMIT 5;
  DECLARE c CURSOR FOR SELECT * FROM migrator.migrations;
  PREPARE p AS SELECT * FROM migrator.migrations WHERE phase = $1;
  EXPLAIN ANALYZE SELECT * FROM migrator.migrations;                 -- there is no plan
  ```

- **Mutating functions evaluated inside a query** — side effects with planner-**undefined count, order, and timing**, and no rollback:

  ```sql
  SELECT migrator.create_migration(name, dsn) FROM my_table WHERE name LIKE 'magic%';
  SELECT * FROM t WHERE migrator.start_migration(t.name) IS NOT NULL;  -- side effect in a predicate
  BEGIN; SELECT migrator.drop_migration('x'); ROLLBACK;               -- ROLLBACK does not undo the RPC
  ```

- **References buried inside catalog objects — the worst case.** The gateway only intercepts top-level statements it parses; a reference nested in an object's body is invisible, and the object cannot even be created because the function/view is not real in Postgres. The call would have to run _inside_ the primary during ordinary DML, entirely out of the gateway's reach:

  ```sql
  CREATE VIEW exporting AS SELECT * FROM migrator.migrations WHERE phase = 'EXPORTING';
  CREATE MATERIALIZED VIEW mv AS SELECT * FROM migrator.migrations;
  CREATE POLICY p ON orders USING (tenant IN (SELECT name FROM migrator.migrations));
  CREATE FUNCTION f() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN PERFORM migrator.start_migration(NEW.name); RETURN NEW; END $$;  -- fires on INSERT into a real table
  CREATE TABLE t (x text GENERATED ALWAYS AS (migrator.something(x)) STORED);
  CREATE INDEX ON t (migrator.f(x));
  ```

- **Tooling and introspection** assume these are real catalog objects: `pg_dump` trying to dump `migrator.migrations`, `\d migrator.*`, psql autocomplete, and ORMs/migration tools reading `information_schema.tables` / `.routines` — which see nothing in the gateway-fake variant, or see them as real (and act accordingly) in the extension variant.

Supporting all of this would effectively require the gateway to become a distributed SQL engine that splits statements and forwards sub-queries to the primary. The DDL surface avoids the whole class of problems by never being embeddable.

#### The native read relation refines, but does not overturn, this

The migrator now persists migrations to a **real relation** on the target — `multigres.migration` (plus the normalized `multigres.migration_tables`) in the sidecar schema, physically replicated with the shard. That changes the picture for the **read** side only: reads do not have to be a gateway-synthesized view at all, since a native, composable view over that relation is viable. The read-side leak examples above apply to a view the gateway _fabricates_; they do not apply to a real one. Two caveats keep the base table from being a drop-in client surface as it stands, so the read surface would be a **view**, not the table itself:

- The base table gets **no `PUBLIC` grant** and stores the source DSN in clear text (superuser-only by design), so a client-facing read must be a **redacting** view.
- The live-derived columns (`caught_up`, `total_relations` / `ready_relations`, `publication_name` / `subscription_name`, lag) are computed at `GetMigration` time, not stored. A complete view would **join `multigres.migration` with the live replication catalogs** (`pg_stat_subscription`, the publication/subscription catalogs) to add them.

Such a view is a reasonable read surface on its own — and an alternative to the gateway's `SHOW` synthesis for the read side — regardless of which mutation surface is chosen. What it does **not** change is the decision, because that rests on the **mutating** verbs, and a real read relation says nothing about those. As plain top-level statements the CALL API's procedures would be fine: `CALL migrator.create_migration(...)` is interceptable and, being a procedure, runs outside a transaction. The problem is the composability the CALL API offers as its advantage. To put a mutation _inside a query_ — `SELECT migrator.create_migration(...) FROM my_table` — it has to be a _function_, and a function that creates or cuts over a migration is unsafe: the planner evaluates it an undefined number of times and in no fixed order, it runs inside the caller's transaction (so `CREATE` / `DROP SUBSCRIPTION` cannot run), and it can be buried in trigger, view, or generated-column bodies the gateway never sees. So the read side can be a native view, but the mutations stay DDL — closed top-level statements the gateway fully owns.

### Connections (CALL API)

```sql
CREATE PROCEDURE migrator.create_connection(conname name, condsn text);
CREATE PROCEDURE migrator.drop_connection(conname name);
```

| Argument | Description                                  |
| -------- | -------------------------------------------- |
| conname  | Name of the connection                       |
| condsn   | The DSN to use to connect to a remote server |

```sql
CALL migrator.create_connection('onprem', 'host=db.example.com port=5432 dbname=app sslmode=verify-full');
CALL migrator.drop_connection('onprem');
```

### Migrations (CALL API)

```sql
CREATE PROCEDURE migrator.create_migration(name text, tables text[], source text, dsn text);
CREATE PROCEDURE migrator.start_migration(name text);
CREATE PROCEDURE migrator.activate_migration(name text);
CREATE PROCEDURE migrator.deactivate_migration(name text);
CREATE PROCEDURE migrator.drop_migration(name text);
```

| Argument | Description                                        |
| -------- | -------------------------------------------------- |
| name     | Name of the migration                              |
| tables   | An array of table names to migrate from the source |
| source   | The optional connection to use as the source       |
| dsn      | The optional DSN for the source                    |

```sql
CALL migrator.create_migration('orders_move', ARRAY['orders', 'customers'], source => 'onprem');
CALL migrator.start_migration('orders_move');
CALL migrator.activate_migration('orders_move');
CALL migrator.deactivate_migration('orders_move');
CALL migrator.drop_migration('orders_move');
```

More advanced cases are possible with this interface since they are native functions:

```sql
SELECT migrator.create_migration(name, tables, dsn => dsn) FROM my_migrations
```

The drawback is that this must be implemented in the gateway, which would require splitting statements into locally executed code and queries forwarded to the server. A column-list/`WHERE`/all-tables/tables-in-schema selection would be expressed as procedure arguments rather than the `CREATE PUBLICATION`-style clause the DDL uses.

### Reads (CALL API)

**Note:** the sketch below predates `multigres.stat_migration`, the real view this design actually shipped with (see "Status and inspection") — kept here unchanged as the record of the considered alternative, not as a still-open proposal. The shipped view differs in schema/name (`multigres.stat_migration`, not `migrator.migrations`) and column set (no derived `tables`, `publication_name`/`subscription_name`, or timestamps — see that section for exactly what is projected and why), but the core idea below (a real, password-redacted view over `multigres.migration` joined with the live replication catalogs) is what was built.

On the current `shard-migration` branch the persisted migration state already lives in **real relations** — `multigres.migration` and the normalized `multigres.migration_tables` in the sidecar schema — but there is no `migrator.migrations` view yet, and the base table is superuser-only and holds the source DSN in clear text (see "The native read relation refines, but does not overturn, this" above). So a CALL-API read surface would layer views over that state rather than expose the table directly:

- `migrator.migrations` — a view over `multigres.migration` joined with the live replication catalogs to add the derived status columns (`caught_up`, `ready_relations` / `total_relations`, publication/subscription names), with the source DSN redacted.
- `migrator.connections` — a view over the stored connections, password redacted.

This illustrative view returns more than today's `SHOW MIGRATION` columns — it also derives `tables`, `publication_name`/`subscription_name`, and the timestamps, none of which the live `SHOW MIGRATION` currently projects (see Status and inspection) — all reproducible: the stored fields come straight from `multigres.migration` (joined with `multigres.migration_connection` for the source DSN, since a migration holds only a live `connection_id` reference, not its own DSN), `tables` aggregates `multigres.migration_tables`, the publication/subscription names follow the migrator's `mt_pub_<id>` / `mt_sub_<id>` convention, and `total_relations` / `ready_relations` / `caught_up` are derived from `pg_subscription_rel` exactly as the RPC's `SubscriptionStatus` computes them (a relation is ready when `srsubstate = 'r'`; caught up when every relation is ready). Concretely:

```sql
CREATE VIEW migrator.migrations AS
SELECT
    m.name,
    c.name                                                      AS connection_name,
    -- password redacted; a real definition would use a redaction function
    regexp_replace(c.dsn, 'password=[^ ]*', 'password=***')      AS source,
    m.target_database,
    m.target_shard,
    tl.tables,
    m.phase,
    m.active_direction,
    coalesce(s.total_relations, 0)                             AS total_relations,
    coalesce(s.ready_relations, 0)                             AS ready_relations,
    coalesce(s.total_relations > 0
             AND s.ready_relations = s.total_relations, false) AS caught_up,
    'mt_pub_' || m.migration_id                                AS publication_name,
    'mt_sub_' || m.migration_id                                AS subscription_name,
    m.streaming_since,
    m.created_at,
    nullif(m.last_error, '')                                   AS last_error
FROM multigres.migration m
JOIN multigres.migration_connection c ON c.connection_id = m.connection_id
LEFT JOIN LATERAL (
    SELECT array_agg(t.schema_name || '.' || t.table_name
                     ORDER BY t.schema_name, t.table_name) AS tables
    FROM multigres.migration_tables t
    WHERE t.migration_id = m.migration_id
) tl ON true
LEFT JOIN LATERAL (
    SELECT count(r.*)                                        AS total_relations,
           count(r.*) FILTER (WHERE r.srsubstate = 'r')     AS ready_relations
    FROM pg_subscription sub
    LEFT JOIN pg_subscription_rel r ON r.srsubid = sub.oid
    WHERE sub.subname = 'mt_sub_' || m.migration_id
) s ON true;
```

The view must be defined on the target primary, where both the sidecar table and the subscription catalogs (`pg_subscription`, `pg_subscription_rel`) live; stream position / lag, if ever exposed, would join `pg_stat_subscription` the same way. Once it exists it is queried with ordinary SQL:

```sql
SELECT * FROM migrator.migrations;
SELECT * FROM migrator.migrations WHERE name = 'orders_move';
SELECT * FROM migrator.connections;
```

### Notes (CALL API)

- The mutating operations would have to be procedures (invoked with `CALL`), not functions: `CREATE`/`DROP SUBSCRIPTION` cannot run inside a transaction block, and functions always run inside the caller's transaction — only a procedure invoked via `CALL` can do the required transaction control. So the `SELECT ... FROM my_migrations` composition above is workable only in the gateway-intercepted variant, not as a real in-database extension.
