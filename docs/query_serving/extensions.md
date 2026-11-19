# PostgreSQL Extension Coverage

Multigres runs PostgreSQL extension compatibility coverage through the
`pgregresstest` end-to-end harness. Covered extensions run their shipped test
suites (`pg_regress` or pgTAP) through multigateway. Partial extensions also run
upstream suites, but known compatibility gaps are documented with patches.
Build-only extensions are built and smoke-loaded, but their upstream regression
suites are not used as compatibility signals.

This page lists the extensions currently tracked by the harness. It is not a
complete `pg_available_extensions` inventory. The source of truth is
`ExtensionCatalog` in `go/test/endtoend/pgregresstest/extensions.go`.

## Tested extensions

| Extension          | Source   | Notes                                                                                          |
| ------------------ | -------- | ---------------------------------------------------------------------------------------------- |
| `btree_gin`        | contrib  | -                                                                                              |
| `btree_gist`       | contrib  | -                                                                                              |
| `citext`           | contrib  | -                                                                                              |
| `cube`             | contrib  | -                                                                                              |
| `earthdistance`    | contrib  | Depends on `cube`.                                                                             |
| `file_fdw`         | contrib  | `explain_filter` helper (bare-parameter dynamic EXECUTE) patched.                              |
| `fuzzystrmatch`    | contrib  | -                                                                                              |
| `hstore`           | contrib  | -                                                                                              |
| `ltree`            | contrib  | -                                                                                              |
| `pg_prewarm`       | contrib  | -                                                                                              |
| `pg_trgm`          | contrib  | -                                                                                              |
| `pg_walinspect`    | contrib  | `NO_INSTALLCHECK`; explicit REGRESS list (cluster provides `wal_level=replica`).               |
| `pgcrypto`         | contrib  | Requires PostgreSQL to be built with OpenSSL.                                                  |
| `pgstattuple`      | contrib  | -                                                                                              |
| `unaccent`         | contrib  | -                                                                                              |
| `uuid-ossp`        | contrib  | Requires PostgreSQL UUID support.                                                              |
| `http`             | external | pgsql-http; local httpbin; upstream/autocommit suite; timeout-wording patch.                   |
| `index_advisor`    | external | Pure-SQL; depends on `hypopg` (built as a dependency).                                         |
| `pg_cron`          | external | Needs `shared_preload_libraries`.                                                              |
| `pg_graphql`       | external | Rust extension built with cargo-pgrx; loads `test/fixtures.sql` before its pg_regress suite.   |
| `pg_jsonschema`    | external | Rust extension built with cargo-pgrx; runs an in-repo SQL translation of its pgrx test corpus. |
| `pg_net`           | external | Needs `shared_preload_libraries`; in-repo SQL suite against the local httpbin.                 |
| `pg_partman`       | external | pgTAP suite; transaction-wrapped tests only; needs `pgtap`.                                    |
| `pgjwt`            | external | Pure-SQL; pgTAP suite; depends on `pgcrypto` and `pgtap`.                                      |
| `pgmq`             | external | Pure-SQL queue; partitioned-queue tests depend on `pg_partman`.                                |
| `pgsodium`         | external | Requires `libsodium`; pgTAP suite in keyless mode (server-key/TCE tests self-skip).            |
| `pgtap`            | external | Runs its own pg_regress suite; also a test dependency of other covered suites.                 |
| `plpgsql_check`    | external | Preloaded via `shared_preload_libraries` for passive checks and the profiler.                  |
| `postgis`          | external | Built with autotools; runs upstream `regress/run_test.pl`.                                     |
| `postgis_raster`   | external | Covered by the PostGIS regress runner when GDAL support is built.                              |
| `postgis_sfcgal`   | external | Covered by the PostGIS regress runner when SFCGAL support is built.                            |
| `postgis_topology` | external | Covered by the PostGIS regress runner.                                                         |
| `supabase_vault`   | external | Preloaded; needs `libsodium`; runs an in-repo SQL suite.                                       |
| `vector`           | external | pgvector.                                                                                      |

## Partial external extensions

These extensions are built and run through their upstream suites, but still have
known drop-in compatibility gaps documented by narrow patches.

| Extension | Notes                                                                                                                                                                                                                                                                                      |
| --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `hypopg`  | Hypothetical indexes are backend-local. In autocommit mode, multigateway may route `hypopg_create_index()` and later `EXPLAIN` / `hypopg()` statements to different pooled backends until automatic session pinning for hypopg functions is implemented. Narrow patches document that gap. |

## Build-only external extensions

These extensions are built, installed, preloaded when needed, and smoke-loaded
with `CREATE EXTENSION`, but their upstream regression suites are not run through
multigateway.

| Extension  | Notes                                                                                                                                                                                                                                 |
| ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `pgaudit`  | Requires `shared_preload_libraries`; build/load smoke only. Upstream's pg_regress suite asserts exact audit-log output for session-state replay, prepared statements, and database DDL, so it is not a valid multigateway signal yet. |
| `wrappers` | Rust extension built with the `helloworld_fdw` feature; the smoke test runs the wrapper DDL (`CREATE FOREIGN DATA WRAPPER` / `SERVER` / `FOREIGN TABLE`) plus a `SELECT` through multigateway.                                        |

## Unsupported extensions

These extensions are not expected to be covered by the current harness.

| Extension            | Source  | Reason                                                                                                                                                                        |
| -------------------- | ------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `dblink`             | contrib | Gateway's expression filter blocks the `dblink*` functions.                                                                                                                   |
| `moddatetime`        | contrib | `contrib/spi` does not ship an installable `pg_regress` suite.                                                                                                                |
| `pg_stat_statements` | contrib | Marked `NO_INSTALLCHECK`; records query text rewritten by the gateway.                                                                                                        |
| `plpgsql`            | contrib | Built-in procedural language covered by the core regression suite, not contrib.                                                                                               |
| `postgres_fdw`       | contrib | FDW DDL is allowed (covered by the queryserving e2e), but the upstream suite builds its loopback servers in a DO block with a raw `\|\|` dynamic EXECUTE the gateway rejects. |
