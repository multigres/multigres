# PostgreSQL Extension Coverage

Multigres runs PostgreSQL extension compatibility coverage through the
`pgregresstest` end-to-end harness. Covered extensions run their shipped test
suites (`pg_regress` or pgTAP) through multigateway — contrib extensions from
the PostgreSQL source tree, external extensions from their own pinned
repositories.

This page lists the extensions currently tracked by the harness. It is not a
complete `pg_available_extensions` inventory.

## Tested extensions

| Extension       | Source   | Notes                                                                                         |
| --------------- | -------- | --------------------------------------------------------------------------------------------- |
| `btree_gin`     | contrib  | -                                                                                             |
| `btree_gist`    | contrib  | -                                                                                             |
| `citext`        | contrib  | -                                                                                             |
| `cube`          | contrib  | -                                                                                             |
| `earthdistance` | contrib  | Depends on `cube`.                                                                            |
| `fuzzystrmatch` | contrib  | -                                                                                             |
| `hstore`        | contrib  | -                                                                                             |
| `ltree`         | contrib  | -                                                                                             |
| `pg_trgm`       | contrib  | -                                                                                             |
| `pgcrypto`      | contrib  | Requires PostgreSQL to be built with OpenSSL.                                                 |
| `unaccent`      | contrib  | -                                                                                             |
| `uuid-ossp`     | contrib  | Requires PostgreSQL UUID support.                                                             |
| `index_advisor` | external | Pure-SQL; depends on `hypopg` (built as a dependency).                                        |
| `pg_cron`       | external | Background worker; preloaded via `shared_preload_libraries`.                                  |
| `pg_graphql`    | external | Rust extension built with cargo-pgrx.                                                         |
| `pg_partman`    | external | pgTAP suite (transaction-wrapped subset); also pgmq's dependency.                             |
| `pgjwt`         | external | Pure-SQL; pgTAP suite; depends on `pgcrypto` and `pgtap`.                                     |
| `pgmq`          | external | Pure-SQL; partitioned-queue tests depend on `pg_partman`.                                     |
| `pgsodium`      | external | Requires `libsodium`; pgTAP suite in keyless mode (server-key/TCE tests self-skip).           |
| `pgtap`         | external | Runs its own pg_regress suite; also the test dependency of `pg_partman`, `pgjwt`, `pgsodium`. |
| `plpgsql_check` | external | Preloaded via `shared_preload_libraries` for passive checks and the shared-memory profiler.   |
| `vector`        | external | pgvector.                                                                                     |

## Unsupported extensions

These extensions are not expected to be covered by the current harness.

| Extension            | Source   | Reason                                                                                                                                                              |
| -------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `dblink`             | contrib  | Pooler blocks outbound connections.                                                                                                                                 |
| `moddatetime`        | contrib  | `contrib/spi` does not ship an installable `pg_regress` suite.                                                                                                      |
| `pg_stat_statements` | contrib  | Marked `NO_INSTALLCHECK`; records query text rewritten by the gateway.                                                                                              |
| `plpgsql`            | contrib  | Built-in procedural language covered by the core regression suite, not contrib.                                                                                     |
| `postgres_fdw`       | contrib  | Pooler blocks `CREATE SERVER` and outbound connections.                                                                                                             |
| `http`               | external | Test suite needs a live HTTP echo server (local httpbin, falling back to httpbin.org) — not deterministic.                                                          |
| `hypopg`             | external | Built as `index_advisor`'s dependency, but its own suite is autocommit and asserts on backend-local hypothetical indexes, which don't survive a transaction pooler. |
| `pg_jsonschema`      | external | Rust; ships no SQL suite (tests are pgrx `#[pg_test]` against a private server).                                                                                    |
| `pg_net`             | external | Background worker making live HTTP requests; pytest-based tests.                                                                                                    |
| `pgaudit`            | external | Suite repeatedly `\connect`s as freshly created password-auth users; pg_regress assumes trust auth on a temp instance.                                              |
| `supabase_vault`     | external | Autocommit pgTAP suite; pgTAP's session-temp plan state can't survive a transaction pooler.                                                                         |

## External extensions (not yet covered)

These extensions live outside the PostgreSQL source tree and need separate
build and test infrastructure before they can be run by this harness.

| Extension          | Notes                                                 |
| ------------------ | ----------------------------------------------------- |
| `postgis`          | -                                                     |
| `postgis_topology` | Part of PostGIS.                                      |
| `wrappers`         | Rust; needs toolchains the harness doesn't provision. |
| `supabase_vault`   | -                                                     |
| `vector`           | pgvector.                                             |
| `wrappers`         | Rust extension.                                       |
