# SQL interface for managing migrations

**Status.** Design proposal for team discussion. This note proposes a **new DDL interface** for managing migrations and source connections from an ordinary SQL client. A CALL-based alternative that was considered is kept in Appendix A. It covers the client-facing surface and where it is implemented, not the migrator internals (see the Multigres Migrator design doc).

## Overview

The Multigres Migrator coordinates logical-replication table migrations. Today it is driven only over RPC: multiadmin re-exposes the migrator RPCs (`CreateMigration`, `StartMigration`, `UpdateMigration`, `GetMigrations`, `ActivateMigration`, `DeactivateMigration`, `DropMigration`), which forward to the owning migrator on the target shard's primary multipooler. There is no way for an ordinary Postgres client (psql, an application connection) to create, start, observe, cut over, or drop a migration — or to manage the source-server connection a migration reads from.

This note proposes a **new DDL interface** at the multigateway: first-class statements such as `CREATE MIGRATION`, `ALTER MIGRATION`, and `CREATE CONNECTION`. These verbs are syntax errors in stock Postgres, so the gateway recognizes and parses them and handles them in-gateway (never forwarding them to Postgres), translating each to the existing migrator RPCs. Because they are not valid Postgres, this surface cannot be delivered as an in-database extension — it lives in the gateway. A CALL-based, extension-shaped alternative (`CALL migrator.create_migration(...)` plus views) was considered and is documented in Appendix A.

Since the syntax will be sticky (once delivered, it will be hard to make users change their behavior), making a good decision here is essential.

For that reason we implement the **full grammar** up front — including forms the migration RPC does not back yet (per-table column lists and `WHERE` filters, `FOR ALL TABLES` / `FOR TABLES IN SCHEMA`, `ONLY`/`*`, `publish_via_partition_root`, …). Options that are not yet wired are parsed and rejected at runtime with a typed `feature_not_supported` error (SQLSTATE `0A000`) rather than being left out of the grammar, so the surface stays stable as the backend catches up. This also anticipates a wider data-movement initiative that these statements are expected to serve, beyond the current migrator.

## Common ground

- **Same control path.** The surface translates to the existing RPCs: gateway → multiadmin → owning migrator (target shard primary) → coordinator. No new migration semantics are introduced here.
- **Result sets.** `SHOW` statements return a synthesized `RowDescription` + `DataRow`s built from `GetMigrations`; the mutating statements return a command tag (`CREATE MIGRATION`, `ALTER MIGRATION`, …). Errors surface as ordinary Postgres `ErrorResponse` (bad arguments, unknown migration, migrator unreachable, validation rejection).
- **Credentials.** A source DSN / password is passed straight to the RPC and is **never** written to gateway logs, or query/audit logs. Any connection shown to a client has its password redacted.

## Connections

A connection is a stored, named source endpoint reused across migrations, so credentials are not repeated inline. Connections are held at the gateway, which lets the gateway follow source failover and, if necessary, buffer incoming changes during a failover.

The grammar is specified in full below, taking inspiration from `CREATE SERVER` / `ALTER SERVER` / `DROP SERVER`.

```text
CREATE CONNECTION [ IF NOT EXISTS ] connection_name
    OPTIONS ( connection_option 'value' [, ... ] )

ALTER CONNECTION connection_name
    OPTIONS ( [ ADD | SET | DROP ] connection_option [ 'value' ] [, ... ] )

DROP CONNECTION [ IF EXISTS ] connection_name [, ... ]

SHOW CONNECTIONS

SHOW CONNECTION connection_name
```

The terminals are: `connection_name`, an identifier; `connection_option`, an identifier naming a libpq connection keyword (`host`, `port`, `dbname`, `user`, `password`, `sslmode`, `sslrootcert`, `sslcert`, `sslkey`, and the rest); and `'value'`, a string literal. The grammar accepts any option name — the accepted set is validated at runtime, not by the grammar. In `ALTER CONNECTION` an omitted action defaults to `ADD` (as in `ALTER SERVER`). `password` is stored but never shown by `SHOW`. `ALTER CONNECTION` and `DROP CONNECTION` are refused while the connection is in use by a **running** migration, raising an object-in-use error (SQLSTATE `55006`). A migration that has been created but not started does not count as in use, so the connection can still be edited or dropped then. Because a connection cannot change while a migration streams from it, there is no propagation to worry about and no `CASCADE`.

### Examples

```sql
CREATE CONNECTION onprem OPTIONS (host 'db.example.com', dbname 'app', user 'repl', sslmode 'verify-full');
ALTER CONNECTION onprem OPTIONS (SET host 'db2.example.com');
ALTER CONNECTION onprem OPTIONS (ADD sslrootcert '/etc/ssl/root.crt');
DROP CONNECTION IF EXISTS onprem;
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

DROP MIGRATION [ IF EXISTS ] migration_name [, ... ] [ FORCE | WAIT [ ( timeout_seconds ) ] ]

SHOW MIGRATIONS

SHOW MIGRATION migration_name
```

where `action` is one of:

```text
START
ACTIVATE
DEACTIVATE
CONNECTION connection_name
SET ( migration_option [ = value ] [, ... ] )
```

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
source_publication [ = string ]
publish_via_partition_root [ = boolean ]
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
- A `TABLE` entry may carry a projected column list and a row filter.
- A `TABLES IN SCHEMA` entry may not carry a projected column list nor a row filter.
- An unqualified `table_name` resolves through the source `search_path`; qualify it (e.g. `FOR TABLE sales.orders, public.customers`) to select across schemas.
- There is no explicit target clause — the migration lands in the database/cluster the client is connected to through the gateway.
- The lifecycle operations are `ALTER MIGRATION` subcommands (mirroring `ALTER SUBSCRIPTION … ENABLE | DISABLE | REFRESH | SET`), not standalone verbs.
- `ALTER MIGRATION … START` begins the migration (validate → schema copy → publication → subscription → catch-up); maps to `StartMigration`.
- `ALTER MIGRATION … ACTIVATE` is the go-live cutover — it drains to a consistent point, flips the active direction (IMPORT→EXPORT), and starts serving from the target.
- `ALTER MIGRATION … DEACTIVATE` stops serving and flips back (EXPORT→IMPORT), the rollback. Both are **serving-coupled** — they move where client traffic is served, not just the replication direction — and map to the `ActivateMigration` / `DeactivateMigration` RPCs.
- `ALTER MIGRATION … SET ( … )` updates configuration and `ALTER MIGRATION … CONNECTION connection_name` re-points the source; both map to `UpdateMigration`.
- These configuration/source changes (`SET`, `CONNECTION`) are only allowed while the migration is **not running** — the `CREATED` phase, before `START`. Applying them to a running migration (streaming in either direction) raises an object-in-use error (SQLSTATE `55006`). Editing config before start is the intended workflow; a live source re-point after a source failover is handled internally, not through this statement. (The lifecycle actions `START` / `ACTIVATE` / `DEACTIVATE` are the state transitions themselves and apply in their own phases.)
- `DROP MIGRATION` tears the migration down.
  - The `FORCE` drops it from any phase without draining.
  - The `WAIT` (optionally `WAIT ( timeout_seconds )`) drains to a caught-up state first, then tears down.
  - The two are mutually exclusive, and omitting both tears down immediately.

### Examples

```sql
-- an explicit set of tables
CREATE MIGRATION orders_move CONNECTION onprem FOR TABLE orders, customers;

-- a column list plus a row filter on one table
CREATE MIGRATION tenant42 CONNECTION onprem
    FOR TABLE orders (id, total, tenant_id) WHERE (tenant_id = 42), customers
    WITH (copy_data = true, sequence_margin = 1000);

-- every table in a schema
CREATE MIGRATION app_public CONNECTION onprem FOR TABLES IN SCHEMA public;

-- the whole source database
CREATE MIGRATION full_copy CONNECTION onprem FOR ALL TABLES;

-- configuration and source changes are only allowed before START (while CREATED)
ALTER MIGRATION orders_move SET (sequence_margin = 2000);
ALTER MIGRATION orders_move CONNECTION onprem_replica;

ALTER MIGRATION orders_move START;
ALTER MIGRATION orders_move ACTIVATE;   -- go-live cutover (drain → flip → serve from target)
ALTER MIGRATION orders_move DEACTIVATE; -- roll back (stop serving → flip)
SHOW MIGRATION orders_move;
DROP MIGRATION IF EXISTS orders_move;
```

### Partitioned and inherited tables

The selection maps directly onto stock Postgres logical replication — the `CREATE PUBLICATION` / `CREATE SUBSCRIPTION` primitives the migrator already drives — so nothing custom is needed in Postgres. All the relevant features are available at the Multigres PG17+ floor: `FOR ALL TABLES` (always), `FOR TABLES IN SCHEMA` and column-list / `WHERE` row filters (PG15+), partitioned tables in a publication and `publish_via_partition_root` (PG13+).

- **`ONLY` / `*`** are the inheritance markers from `CREATE PUBLICATION`. `TABLE ONLY t` includes `t` but not its descendants; `TABLE t` (or `t *`) includes descendants. This is meaningful for **legacy inheritance** (`INHERITS`), where the parent has its own rows.
- **Declarative partitioning** is different: the partitioned parent holds no rows of its own, so `ONLY` on it would migrate an empty shell. The control that matters there is `WITH (publish_via_partition_root = …)`:
  - `false` (default) publishes changes as the **leaf partitions**, so the target must have matching partitions (same partition layout).
  - `true` publishes changes as the **root** table, so the target may be a plain unpartitioned table or partitioned differently — this is what makes migrating a partitioned source into a differently-shaped target work.

These ride on real publication/subscription features. `FOR ALL TABLES` and `FOR TABLES IN SCHEMA` are wired in the RPC; `ONLY`/descendants and `publish_via_partition_root` are parsed but not yet honored — the RPC returns a typed `feature_not_supported` (SQLSTATE `0A000`) error until they are wired to the `CREATE PUBLICATION` the multipooler issues (see "Backing in the migrator RPCs").

## Status and inspection

`SHOW MIGRATIONS` returns one row per migration; `SHOW MIGRATION <name>` returns the single matching row. Both expose the same columns.

### `SHOW MIGRATION[S]` columns

| Column            | Type        | Description                                                            |
| ----------------- | ----------- | ---------------------------------------------------------------------- |
| name              | text        | Name (identifier) of the migration                                     |
| source            | text        | Source connection or DSN the migration reads from (password redacted)  |
| target_database   | text        | Target database receiving the data                                     |
| target_shard      | text        | Target shard receiving the data                                        |
| tables            | text[]      | Tables selected by the migration                                       |
| phase             | text        | Lifecycle phase (CREATED, VALIDATING, SCHEMA_COPY, COPYING, STREAMING) |
| active_direction  | text        | Which side is currently the publisher (IMPORT / EXPORT)                |
| total_relations   | integer     | Number of tables in the initial copy                                   |
| ready_relations   | integer     | Number of tables that have finished the initial copy                   |
| caught_up         | boolean     | Whether streaming has reached the source (lag zero)                    |
| publication_name  | text        | Publication backing the stream on the source                           |
| subscription_name | text        | Subscription backing the stream on the target                          |
| streaming_since   | timestamptz | When the migration entered the STREAMING phase                         |
| created_at        | timestamptz | When the migration was created                                         |
| last_error        | text        | Last error, if the migration is in the FAILED phase                    |

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

## Backing in the migrator RPCs

The migrator RPCs now back most of the grammar (implemented on `shard-migration`, `proto/migratorservice.proto`; multiadmin forwards the same request types unchanged, so the gateway and the multiadmin/CLI paths share them). Forms the RPC does not yet honor return a typed error the gateway surfaces verbatim.

Wired end-to-end:

- **Name.** `CreateMigrationRequest.name` (unique per target database) and `Migration.name` in the projection; `name` is accepted on Get/Start/Activate/Deactivate/Drop/Update. Resolution is id-first, then a unique name; a duplicate name at create is rejected. The generated `m<unixnano>` id stays the stable internal key, and the record lives in the sidecar migration table (`multigres.migration`).
- **Table selection.** The flat `tables` field (with the `*` / `schema.*` / `schema.table` markers) plus `all_tables`, `schemas`, and `table_specs` — a `TableSpec{qualified_name, columns, where, include_descendants}` with only `qualified_name` set is honored. So `FOR ALL TABLES`, `FOR TABLES IN SCHEMA`, and named/qualified tables all bind to real fields.
- **Options.** `copy_data` (proto3 `optional bool` — unset means the default `true`; send `false` to skip the initial COPY) and `skip_schema_copy`.
- **Cutover.** `ALTER MIGRATION … ACTIVATE` / `… DEACTIVATE` map 1:1 to the serving-coupled `ActivateMigration` / `DeactivateMigration`; `ALTER MIGRATION … SET (…)` and `… CONNECTION name` map to `UpdateMigration` (allowed only while `CREATED`).

Parsed but not yet supported — the RPC returns a typed `feature_not_supported` error (SQLSTATE `0A000`, with an actionable message; the gateway can assert it with `mterrors.IsErrorCode(err, mterrors.PgSSFeatureNotSupported)`) which surfaces to the client as an ordinary `ErrorResponse`:

- per-table **column lists**, **`WHERE`** row filters, and **`ONLY`/descendants** (`include_descendants`) on `TABLE` entries;
- `source_publication`;
- `publish_via_partition_root`.

**Status.** `GetMigrations` projects `name`, `phase`, `source` (redacted), `target_database`, `target_shard`, `tables`, `total_relations`, `ready_relations`, `caught_up`, `publication_name`, `subscription_name`, `active_direction`, `streaming_since`, `created_at`, `last_error`. So `SHOW MIGRATIONS` / `SHOW CONNECTIONS` are backed; per-table copy state and a journal view would need projection additions (copy progress is only `ready/total_relations` plus `caught_up`).

## Open questions

1. **Target** — keep the target implicit (the database/cluster the client is connected to through the gateway), or add an explicit `INTO database/shard` clause?
2. Implement the full grammar now (see Overview); unimplemented options are parsed and rejected with a clear "not yet supported" error, rather than trimming the grammar to what the RPC backs today. This is currently the approach taken, but open to discussion.
3. **Statement shape:** lifecycle operations are `ALTER MIGRATION migration_name { START | ACTIVATE | DEACTIVATE | SET (…) }` subcommands (mirroring `ALTER SUBSCRIPTION … ENABLE | DISABLE | REFRESH | SET`), not standalone verbs. `ACTIVATE` / `DEACTIVATE` map 1:1 to the serving-coupled `ActivateMigration` / `DeactivateMigration` RPCs (no serving-neutral `SWITCH`); `SET (…)` maps to `UpdateMigration`.
   1. Reason to not use `SWITCH`: it is not idempotent and using it several times can cause unexpected issues.
   2. Alternative to `ACTIVATE`/`DEACTIVATE` could be `ALTER MIGRATION foo SET DIRECTION TO IMPORT`.

## Closed questions

- Implement it by extending the goyacc grammar with new productions and AST nodes (not a standalone prefix recognizer), accepting the upstream-re-sync maintenance cost in exchange for first-class AST and planner integration.
- **Identity:** a distinct, unique-per-database `name` field; the generated `m<unixnano>` id stays the stable internal key.

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
| Connection storage            | `CREATE CONNECTION` (topo object)        | `create_connection` proc, or native FDW objects |
| Reads                         | `SHOW MIGRATIONS` / `SHOW MIGRATION`     | `SELECT` from views                             |
| Operator "feel"               | first-class DDL                          | function calls                                  |

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
  CREATE VIEW streaming AS SELECT * FROM migrator.migrations WHERE phase = 'STREAMING';
  CREATE MATERIALIZED VIEW mv AS SELECT * FROM migrator.migrations;
  CREATE POLICY p ON orders USING (tenant IN (SELECT name FROM migrator.migrations));
  CREATE FUNCTION f() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN PERFORM migrator.start_migration(NEW.name); RETURN NEW; END $$;  -- fires on INSERT into a real table
  CREATE TABLE t (x text GENERATED ALWAYS AS (migrator.something(x)) STORED);
  CREATE INDEX ON t (migrator.f(x));
  ```

- **Tooling and introspection** assume these are real catalog objects: `pg_dump` trying to dump `migrator.migrations`, `\d migrator.*`, psql autocomplete, and ORMs/migration tools reading `information_schema.tables` / `.routines` — which see nothing in the gateway-fake variant, or see them as real (and act accordingly) in the extension variant.

Supporting all of this would effectively require the gateway to become a distributed SQL engine that splits statements and forwards sub-queries to the primary. The DDL surface avoids the whole class of problems by never being embeddable.

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

Migrations and connections are exposed as the views `migrator.migrations` and `migrator.connections`, queried with ordinary SQL. They expose the same columns listed under Status and inspection above.

```sql
SELECT * FROM migrator.migrations;
SELECT * FROM migrator.migrations WHERE name = 'orders_move';
SELECT * FROM migrator.connections;
```

### Notes (CALL API)

- The mutating operations would have to be procedures (invoked with `CALL`), not functions: `CREATE`/`DROP SUBSCRIPTION` cannot run inside a transaction block, and functions always run inside the caller's transaction — only a procedure invoked via `CALL` can do the required transaction control. So the `SELECT ... FROM my_migrations` composition above is workable only in the gateway-intercepted variant, not as a real in-database extension.
