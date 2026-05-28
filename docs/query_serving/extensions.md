# PostgreSQL Extension Coverage

Multigres runs PostgreSQL extension compatibility coverage through the
`pgregresstest` end-to-end harness. Covered contrib extensions run their
PostgreSQL `pg_regress` suites through multigateway.

This page lists the extensions currently tracked by the harness. It is not a
complete `pg_available_extensions` inventory.

## Tested extensions

| Extension | Source | Notes |
|-----------|--------|-------|
| `btree_gin` | contrib | - |
| `btree_gist` | contrib | - |
| `citext` | contrib | - |
| `cube` | contrib | - |
| `earthdistance` | contrib | Depends on `cube`. |
| `fuzzystrmatch` | contrib | - |
| `hstore` | contrib | - |
| `ltree` | contrib | - |
| `pg_trgm` | contrib | - |
| `pgcrypto` | contrib | Requires PostgreSQL to be built with OpenSSL. |
| `unaccent` | contrib | - |
| `uuid-ossp` | contrib | Requires PostgreSQL UUID support. |

## Unsupported extensions

These extensions are not expected to be covered by the current harness.

| Extension | Source | Reason |
|-----------|--------|--------|
| `dblink` | contrib | Pooler blocks outbound connections. |
| `moddatetime` | contrib | `contrib/spi` does not ship an installable `pg_regress` suite. |
| `pg_stat_statements` | contrib | Marked `NO_INSTALLCHECK`; records query text rewritten by the gateway. |
| `plpgsql` | contrib | Built-in procedural language covered by the core regression suite, not contrib. |
| `postgres_fdw` | contrib | Pooler blocks `CREATE SERVER` and outbound connections. |

## External extensions

These extensions live outside the PostgreSQL source tree and need separate
build and test infrastructure before they can be run by this harness.

| Extension | Notes |
|-----------|-------|
| `http` | - |
| `hypopg` | - |
| `index_advisor` | Depends on `hypopg`. |
| `pg_cron` | - |
| `pg_graphql` | Rust extension. |
| `pg_jsonschema` | Rust extension. |
| `pg_net` | Uses a background worker. |
| `pgaudit` | - |
| `pgjwt` | Depends on `pgcrypto`. |
| `pgmq` | - |
| `pgsodium` | Requires `libsodium`. |
| `pgtap` | - |
| `plpgsql_check` | - |
| `postgis` | - |
| `postgis_topology` | Part of PostGIS. |
| `supabase_vault` | - |
| `vector` | pgvector. |
| `wrappers` | Rust extension. |
