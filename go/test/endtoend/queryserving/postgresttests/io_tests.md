# PostgREST `test/io` suite through the multigateway

`TestPostgRESTIO` runs the **proxy-relevant subset** of PostgREST's upstream
pytest [`test/io`](https://github.com/PostgREST/postgrest/tree/v14.16/test/io)
suite with PostgREST pointed at the multigateway. It is a sibling of the hspec
suite (`TestPostgREST`) and shares its source clone, PostgreSQL provisioning,
divergence classifier, and report.

Unlike the hspec suite, the io suite spawns **real `postgrest` binaries** and
drives them over HTTP, so it reaches paths the in-process suite does not —
notably role-level settings applied per request via `set_config($1, $2, true)`,
statement_timeout, hoisted transaction settings, and prepared statements.

## How it works

- **Image** (`testdata/Dockerfile.io`): PostgREST's official prebuilt release
  binary for the pinned tag (`v14.16`, checksum-pinned per arch) on a slim
  Python base with the pytest deps. No Haskell build — the io suite needs only
  the `postgrest` executable, not the test code.
- **Repointing**: the suite forwards only `PGDATABASE`/`PGHOST`/`PGUSER` into
  each spawned postgrest (see `test/io/conftest.py::baseenv`). The image installs
  a `postgrest` shim that additionally sources `PGPORT`/`PGPASSWORD`/`PGSSLMODE`
  (written by the entrypoint from the `docker run -e` env), so we repoint over
  TCP + password without patching upstream test files.
- **Auth**: the gateway authenticates each client role by SCRAM against
  `pg_authid`, so every login role PostgREST connects as needs a password. The io
  fixtures create several passwordless login roles (`timeout_authenticator`,
  `meta_authenticator`, `db_config_authenticator`, `other_authenticator`);
  `loadIOFixtures` gives them all one shared password so a single `PGPASSWORD`
  serves every per-test `PGUSER`.
- **Fixtures**: `test/io/fixtures/load.sql` is loaded directly on the primary
  over its unix socket (bypassing gateway DDL handling), as the bootstrap
  superuser, exactly like the hspec harness.
- **Classifier**: direct PostgreSQL is the asserted baseline — every selected
  test must pass on plain postgres, so a gateway failure is a divergence. Default
  runs are gateway-only; `POSTGREST_FULL_BASELINE=1` re-verifies the invariant.

## Running

```bash
# gateway-only (default); needs Docker + PostgreSQL 17 on PATH
RUN_POSTGREST=1 go test -v -run 'TestPostgRESTIO$' \
  ./go/test/endtoend/queryserving/postgresttests/...

# re-verify the invariant on a throwaway direct PG and classify
RUN_POSTGREST=1 POSTGREST_FULL_BASELINE=1 go test -v -run 'TestPostgRESTIO$' ...

# iterate on one test / opt into the timing-sensitive group
RUN_POSTGREST=1 POSTGREST_IO_SELECT='test/io/test_io.py::test_role_settings' go test ...
RUN_POSTGREST=1 POSTGREST_IO_SELECT='-k statement_timeout' go test ...
```

Env knobs: `POSTGREST_IO_REBUILD_IMAGE=1` (force image rebuild),
`POSTGREST_SRC_DIR` (use a local PostgREST checkout), plus the shared
`POSTGREST_CACHE_DIR` / `POSTGREST_RESULTS_DIR` / `POSTGREST_PG_BINDIR`.

## Included tests (run through the gateway)

These exercise the proxied DB-interaction path. ★ = the per-request
`set_config($1, $2, true)` role-settings hoisting path.

| Test                                             | Why it's in scope                                                                                                        |
| ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------ |
| `test_role_settings` ★                           | per-role `statement_timeout` hoisted via `set_config($1,$2,true)`                                                        |
| `test_statement_timeout` ★                       | `ALTER ROLE … SET statement_timeout`, hoisted per request; slow stmt cancels                                             |
| `test_work_mem_in_role_settings`                 | role-level `work_mem` hoisted per request (upstream #4955)                                                               |
| `test_isolation_level`                           | `default_transaction_isolation` hoisted per role and per function                                                        |
| `test_function_setting_statement_timeout_fails`  | function-local `SET statement_timeout` applied in the request tx                                                         |
| `test_function_setting_statement_timeout_passes` | function-local `SET statement_timeout` applied in the request tx                                                         |
| `test_function_setting_work_mem`                 | function-local `SET work_mem` hoisted into the request tx                                                                |
| `test_multiple_func_settings`                    | multiple hoisted tx settings applied together                                                                            |
| `test_first_hoisted_setting_is_applied`          | only the configured hoisted setting is applied                                                                           |
| `test_second_hoisted_setting_is_applied`         | only the configured hoisted setting is applied                                                                           |
| `test_succeed_w_role_having_superuser_settings`  | impersonated role with superuser-only settings must not error                                                            |
| `test_get_granted_superuser_setting`             | `GRANT SET ON PARAMETER` lets a granted superuser setting be hoisted                                                     |
| `test_db_prepared_statements_enable`             | prepared statements used when enabled (pooler must preserve them)                                                        |
| `test_db_schema_notify_reload`                   | `NOTIFY` config reload through the gateway makes PostgREST re-read db-schemas                                            |
| `test_max_rows_notify_reload`                    | `NOTIFY` config reload through the gateway makes PostgREST re-read db-max-rows                                           |
| `test_invalid_role_claim_key_notify_reload`      | `NOTIFY` config reload delivered through the gateway (PostgREST logs it)                                                 |
| `test_notify_do_nothing`                         | an unrecognized `NOTIFY` payload is delivered and harmlessly ignored                                                     |
| `test_notify_reloading_catalog_cache`            | `NOTIFY` schema reload after `DROP`+`CREATE`; recreated relation queryable — **currently an open divergence, see below** |

The `*_notify_*` tests exercise the LISTEN/NOTIFY path end to end: PostgREST
`LISTEN`s on the `pgrst` channel and a fixture RPC `pg_notify`s it, both through
the gateway/pooler. The canonical list lives in `io_selection.go`
(`ioSelectedTests`); this table mirrors it.

## Skipped (and why)

| What                                                                                                | Why skipped                                                                                                                                                                                                                                                                    |
| --------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `test_cli.py`                                                                                       | PostgREST CLI parsing / `--dump-config` — no DB path                                                                                                                                                                                                                           |
| `test_auth.py`                                                                                      | JWT/JWKS auth internals in PostgREST — not the proxied query path                                                                                                                                                                                                              |
| `test_sanity.py`                                                                                    | harness self-checks, not multigres behaviour                                                                                                                                                                                                                                   |
| `test_big_schema.py`                                                                                | needs the 392 KB `big_schema.sql` + long introspection; separate concern                                                                                                                                                                                                       |
| `test_replica.py`                                                                                   | needs a streaming replica wired into `PGRST_DB_URI`; separate concern                                                                                                                                                                                                          |
| `test_io.py`: admin / live / ready / metrics                                                        | PostgREST admin server + Prometheus metrics — internal                                                                                                                                                                                                                         |
| `test_io.py`: `log_level` / `log_query` / `db_error_logging`                                        | PostgREST structured-logging format — internal                                                                                                                                                                                                                                 |
| `test_io.py`: `app_settings_reload` / `max_rows_reload` / `db_schema_reload`                        | `SIGUSR1`/`SIGUSR2` signal-driven reload — PostgREST-internal, no DB path (the `NOTIFY`-driven variants **are** run — see the included list)                                                                                                                                   |
| `test_io.py`: `db_prepared_statements_disable`                                                      | asserts NO server-side prepared statements when the client disables them; multigres does not support turning off prepared statements through the pooler — won't-fix                                                                                                            |
| `test_io.py`: `connect_with_dburi` / `read_dburi` / `get_pgrst_version_*_connection_string`         | build a socket-style DB URI with no port/password — incompatible with the TCP+password repoint                                                                                                                                                                                 |
| `test_io.py`: preflight / CORS / `proxy_status_header`                                              | HTTP response-header behaviour internal to PostgREST                                                                                                                                                                                                                           |
| `test_io.py`: `pool_size` / `pool_acquisition_timeout` / `change_statement_timeout_held_connection` | assert sub-second wall-clock bounds — flaky across the extra proxy hop; opt in via `POSTGREST_IO_SELECT`                                                                                                                                                                       |
| `test_io.py`: `change_statement_timeout`                                                            | reconfigures a role's `statement_timeout` mid-run and depends on SIGUSR1 reload timing + cross-test role state not leaking; ordering-fragile on a shared cluster. `test_statement_timeout` already covers the per-request role-hoisting path. Opt in via `POSTGREST_IO_SELECT` |

The canonical skip notes live in `io_selection.go` (`ioSkipNotes`).

## Known open divergence (`test_notify_reloading_catalog_cache`)

This test is **included and currently fails** through the gateway — like the
hspec suite, the job stays red while a genuine divergence is open. The root cause
is understood (file a fix, not a skip):

- **Symptom:** after a `DROP`+`CREATE` of a table (via a fixture RPC) and a
  `NOTIFY 'reload schema'`, the next request to the recreated relation returns
  `400 22P02 invalid input syntax for type uuid` — PostgREST reuses a prepared
  statement whose `$1` was inferred as `uuid` when the column was `uuid`, even
  though it is now `bigint`. The error fires at Bind, before Execute.
- **Root cause:** the multipooler caches a backend prepared statement
  (`ppstmtN`) keyed by query text (`PoolerConsolidator`, entries never removed)
  and `ensurePrepared` reuses it without re-parsing when the query text matches,
  so the DDL never refreshes the frozen `uuid` parameter type. The client's
  `Close('S')` is not propagated to the backend statement, and the pooler's
  self-heal (close + re-parse + retry) fires only on SQLSTATE `0A000` "cached
  plan must not change result type", not on the `22P02` Bind-time mismatch.
