# PostgreSQL Extension Coverage

Multigres runs PostgreSQL extension compatibility coverage through the
`pgregresstest` end-to-end harness. Covered extensions run their shipped test
suites (`pg_regress` or pgTAP) through multigateway.

This page lists the extensions currently tracked by the harness. It is not a
complete `pg_available_extensions` inventory.

## Tested extensions

| Extension       | Source   | Notes                                                                                          |
| --------------- | -------- | ---------------------------------------------------------------------------------------------- |
| `btree_gin`     | contrib  | -                                                                                              |
| `btree_gist`    | contrib  | -                                                                                              |
| `citext`        | contrib  | -                                                                                              |
| `cube`          | contrib  | -                                                                                              |
| `earthdistance` | contrib  | Depends on `cube`.                                                                             |
| `fuzzystrmatch` | contrib  | -                                                                                              |
| `hstore`        | contrib  | -                                                                                              |
| `ltree`         | contrib  | -                                                                                              |
| `pg_trgm`       | contrib  | -                                                                                              |
| `pgcrypto`      | contrib  | Requires PostgreSQL to be built with OpenSSL.                                                  |
| `unaccent`      | contrib  | -                                                                                              |
| `uuid-ossp`     | contrib  | Requires PostgreSQL UUID support.                                                              |
| `http`          | external | pgsql-http; requires `libcurl`. Runs against a harness-local httpbin-compatible server.        |
| `hypopg`        | external | Transaction-wrapped because hypothetical indexes are backend-local.                            |
| `index_advisor` | external | Pure-SQL; depends on `hypopg` (built as a dependency).                                         |
| `pg_jsonschema` | external | Rust extension built with cargo-pgrx; runs an in-repo SQL translation of its pgrx test corpus. |
| `pgaudit`       | external | Preloaded via `shared_preload_libraries`; multi-user `\connect`s use a harness `.pgpass`.      |
| `pgjwt`         | external | Pure-SQL; pgTAP suite; depends on `pgcrypto` and `pgtap`.                                      |
| `pgsodium`      | external | Requires `libsodium`; pgTAP suite in keyless mode (server-key/TCE tests self-skip).            |
| `pgtap`         | external | Runs its own pg_regress suite; also a test dependency of other covered suites.                 |
| `plpgsql_check` | external | Preloaded via `shared_preload_libraries` for passive checks and the profiler.                  |

## Unsupported extensions

These extensions are not expected to be covered by the current harness.

| Extension            | Source  | Reason                                                                          |
| -------------------- | ------- | ------------------------------------------------------------------------------- |
| `dblink`             | contrib | Pooler blocks outbound connections.                                             |
| `moddatetime`        | contrib | `contrib/spi` does not ship an installable `pg_regress` suite.                  |
| `pg_stat_statements` | contrib | Marked `NO_INSTALLCHECK`; records query text rewritten by the gateway.          |
| `plpgsql`            | contrib | Built-in procedural language covered by the core regression suite, not contrib. |
| `postgres_fdw`       | contrib | Pooler blocks `CREATE SERVER` and outbound connections.                         |

## External extensions

These extensions live outside the PostgreSQL source tree and need separate
build and test infrastructure before they can be run by this harness.

| Extension          | Notes                     |
| ------------------ | ------------------------- |
| `pg_cron`          | -                         |
| `pg_graphql`       | Rust extension.           |
| `pg_net`           | Uses a background worker. |
| `pgmq`             | -                         |
| `postgis`          | -                         |
| `postgis_topology` | Part of PostGIS.          |
| `supabase_vault`   | -                         |
| `vector`           | pgvector.                 |
| `wrappers`         | Rust extension.           |
