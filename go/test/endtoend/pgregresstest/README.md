# PostgreSQL Compatibility Tests

This test package validates multigres compatibility by running PostgreSQL's official regression and isolation test
suites against a multigres cluster.

## Overview

The test performs the following steps:

1. **Checkout PostgreSQL source**: Clones PostgreSQL 17.6 (REL_17_6) from GitHub
2. **Build with make**: Compiles PostgreSQL using traditional ./configure and make
3. **Build isolation tools** (if enabled): Builds `isolationtester` and `pg_isolation_regress`
4. **Configure PATH**: Prepends the built PostgreSQL bin directory to PATH
5. **Spin up multigres cluster**: Creates a 2-node cluster with multigateway using the built PostgreSQL
6. **Run regression tests** (if enabled): Executes PostgreSQL regression tests through multigateway
7. **Run isolation tests** (if enabled): Executes multi-connection concurrency tests through multigateway
8. **Report results**: Generates a unified compatibility report (failures are logged but don't fail the test)

**Important**: The test builds PostgreSQL from source and uses those binaries for the test cluster. This ensures the
PostgreSQL server and the regression test library (`regress.so`) are from the same version, avoiding symbol
compatibility issues.

## Requirements

### Build Dependencies

The following tools must be installed:

- **make** - Build tool
- **gcc** - C compiler
- **git** - For cloning PostgreSQL source
- Standard build tools (autoconf, automake, etc.)

### Install on Ubuntu/Debian

```bash
sudo apt-get update
sudo apt-get install build-essential git
```

### Install on macOS

```bash
# Xcode Command Line Tools (includes make, gcc, git)
xcode-select --install
```

## Running Tests

### Basic Usage

The test is **disabled by default**. Five env vars enable it; setting more
than one is fine (the union runs):

- `RUN_EXTENDED_QUERY_SERVING_TESTS=1` — runs **all** suites (regression,
  isolation, contrib, external). This is what CI uses (matches the "Run Extended
  Query Serving Tests" PR label, and also the broader "Run all Query Serving
  Tests" label).
- `RUN_PGREGRESS=1` — runs the regression suite only. Useful for local
  iteration when you don't need isolation.
- `RUN_PGISOLATION=1` — runs the isolation suite only.
- `RUN_PGCONTRIB=1` — runs the contrib extension suite only (see
  "Contrib Extension Tests" below).
- `RUN_PGEXTERNAL=1` — runs the external extension suite only (see
  "External Extension Tests" below).

```bash
# Run both suites (unified report) — same as CI
RUN_EXTENDED_QUERY_SERVING_TESTS=1 go test -v -timeout 60m ./go/test/endtoend/pgregresstest/...

# Local iteration: regression only
RUN_PGREGRESS=1 go test -v -timeout 60m ./go/test/endtoend/pgregresstest/...

# Local iteration: isolation only
RUN_PGISOLATION=1 go test -v -timeout 60m ./go/test/endtoend/pgregresstest/...

# Without any of these, the test is skipped
go test -v ./go/test/endtoend/pgregresstest/...
```

### Running Specific Tests

```bash
# Run specific regression tests only
PGREGRESS_TESTS="boolean char" RUN_PGREGRESS=1 go test -v -timeout 60m ./go/test/endtoend/pgregresstest/...

# Run specific isolation tests only
PGISOLATION_TESTS="deadlock-simple tuplelock-update" RUN_PGISOLATION=1 go test -v -timeout 60m ./go/test/endtoend/pgregresstest/...

# Run specific contrib modules only (directory names under contrib/)
PGCONTRIB_TESTS="citext hstore" RUN_PGCONTRIB=1 go test -v -timeout 60m ./go/test/endtoend/pgregresstest/...

# Run specific external extensions only (catalog names)
PGEXTERNAL_TESTS="vector" RUN_PGEXTERNAL=1 go test -v -timeout 60m ./go/test/endtoend/pgregresstest/...
```

## Contrib Extension Tests

The contrib suite runs the regression suites that core PostgreSQL extensions
ship in their own `contrib/<module>/{sql,expected}/` directories, executed
through multigateway. It validates that common PostgreSQL extensions work over
the pooled query path. The default module set lives in `DefaultContribModules`
(see `postgres_builder.go`).

How it differs from the core regression suite:

- **Build features**: two modules need optional `./configure` features, enabled
  only when the contrib suite runs: `uuid-ossp` (`--with-uuid`, override the
  implementation with `PG_UUID_LIB`, default `e2fs`) and `pgcrypto`
  (`--with-ssl=openssl`; PG16+ pgcrypto has no built-in crypto). CI installs
  `uuid-dev` and `libssl-dev` for these.
- **Per-module isolation**: every module shares the single `postgres` database
  (multigateway can't isolate per-DB), so the harness resets the `public`
  schema on the primary between modules to clear leftover objects/extensions.
- **Verification**: results go through the same patch pipeline as the core
  suite (`PGREGRESS_PATCH_MODE`), which whitespace-normalizes output — so
  error-cursor caret-position shifts caused by multigateway query rewriting are
  not treated as diffs. Genuine multigres-specific output differences are
  captured as per-module patches under
  `testdata/pg17/patches/contrib/<module>/`.

Some extensions are intentionally **excluded**: `dblink` and `postgres_fdw`
(open outbound connections, which the pooler blocks by design),
`pg_stat_statements` (`NO_INSTALLCHECK=1`; records query text the gateway
rewrites), and `moddatetime` (contrib/spi ships no pg_regress suite).

### Coverage map

`extensions.go` holds `ExtensionCatalog` — common PostgreSQL extensions, not
the full `pg_available_extensions` list, with each one's kind (contrib /
external) and coverage status (covered / pending / unsupported / external, each
with a reason). `DefaultContribModules` is **derived** from it (the `covered`
entries), so enrolling a new extension is a one-line catalog edit.

Every compatibility report includes a generated **Extension Coverage** table
(`ExtensionCoverageMarkdown`) that merges the catalog with the run's per-test
results: covered extensions expand to one row per sub-test with the live
pass/fail, while pending/unsupported/external extensions show a single row with
the reason. The table is the living coverage tracker — it updates automatically
as catalog entries move to `covered` and their suites run.

## External Extension Tests

The external suite runs the pg_regress suites of extensions that live **outside**
the PostgreSQL source tree (separate repositories), executed through
multigateway. The covered set lives in `externalSpecs` (`extensions.go`), each
pinned to a tag (or a commit, for upstreams that never tag): `vector`
(pgvector), `pg_cron` (Citus pg_cron), `pgmq` (tembo-io message queue),
`pg_graphql` (Supabase GraphQL), `index_advisor` (Supabase), `plpgsql_check`
(okbob), `pgjwt` (michelp), `pgsodium` (michelp), `pg_partman` (pgTAP suite),
and `pgtap` (its own pg_regress suite). `externalSpecs` also holds build-only
dependencies that are installed but not tested on their own — `hypopg`, which
index_advisor's tests `CREATE … CASCADE` (hypopg's own suite is autocommit and
asserts on backend-local hypothetical indexes, which don't survive a
transaction pooler), and `pg_partman` doubling as pgmq's build dependency (see
`DependsOn` below).

How it works, and how it differs from the contrib suite:

- **Build**: most external extensions are [PGXS](https://www.postgresql.org/docs/current/extend-pgxs.html)
  modules. `Builder.InstallExternalExtension` shallow-clones the pinned tag (or
  fetches the pinned commit) into the per-run build root, then runs
  `make && make install` with
  `PG_CONFIG` pointed at the from-source PostgreSQL so the extension `.so` links
  against the exact server ABI the cluster runs (the same guarantee the suite
  gives `regress.so`). pgvector, pg_cron, and pgmq need only a C compiler — no
  extra system libs; pgsodium needs libsodium, located via pkg-config
  (`PkgConfigDeps`; CI installs `libsodium-dev pkg-config`, macOS uses the
  Homebrew keg). Rust extensions (`BuildSystem: "pgrx"`, e.g. pg_graphql) are
  built with [cargo-pgrx](https://github.com/pgcentralfoundation/pgrx) instead:
  the harness installs the pinned `cargo-pgrx` (it must match the crate's `pgrx`
  dependency), then `cargo pgrx init --pgNN <pg_config>` adopts the from-source
  server and `cargo pgrx install --pg-config <pg_config>` builds and installs the
  extension into it. CI provisions the Rust toolchain; the from-source server
  guarantees the same ABI as the PGXS path.

  For suites whose dependencies need optional `./configure` features (pgjwt
  depends on contrib pgcrypto, which needs `--with-ssl=openssl`), the harness
  enables the flag automatically for external-only runs. On macOS, Homebrew
  kegs live outside the default search path; pass them with
  `PG_CONFIGURE_EXTRA_ARGS="--with-includes=/opt/homebrew/include --with-libraries=/opt/homebrew/lib"`
  (CI on Linux needs nothing).

- **Test execution**: unlike contrib we cannot use `make installcheck`. Under
  PGXS that target invokes `$(top_builddir)/src/test/regress/pg_regress`, and
  PGXS resolves `top_builddir` into the **install** tree, where `pg_regress` is
  not installed. The harness instead invokes the `pg_regress` it built directly,
  with the same flags the contrib suite relies on
  (`--use-existing --dbname=postgres`, because multigateway rejects DROP/CREATE
  DATABASE) plus the extension's `--inputdir`. The test list is derived from
  `<TestSubdir>/sql/*.sql`, mirroring the extension's
  `REGRESS = $(patsubst sql/%.sql,%,$(wildcard sql/*.sql))`.
- **Per-extension isolation** and **verification** work exactly like contrib:
  the shared database is reset to a clean baseline on the primary between
  extensions — every extension except plpgsql is dropped, every user schema
  except `public` and multipooler's internal `multigres` schema is dropped, and
  `public` is recreated (extensions whose control file pins a non-public schema,
  like pgmq or pg_graphql, would otherwise leak into later suites'
  catalog-introspection output, e.g. pgtap's `extensions_are`). Results go
  through the same patch pipeline (`PGREGRESS_PATCH_MODE`). Genuine
  multigres-specific output differences are captured under
  `testdata/pg17/patches/external/<ext>/`.

### Per-extension knobs

Extensions diverge from the pgvector baseline in a few ways, captured as fields
on `ExternalExtension` (`extensions.go`):

- **`Commit`** — pins a full commit SHA instead of `Tag`, for upstreams that
  never tag releases (pgjwt) or whose last tag predates a needed fix
  (pgsodium: v3.1.9's test fixtures predate PostgreSQL 17's automatic array
  types). Exactly one of `Tag`/`Commit` must be set.
- **`BuildSystem`** — the build toolchain: `""`/`"pgxs"` (make) or `"pgrx"`
  (cargo-pgrx, Rust). pg_graphql is `pgrx`; everything else is PGXS.
- **`PkgConfigDeps`** — pkg-config packages whose headers/libs the PGXS build
  needs (pgsodium: `libsodium`). Resolved to `-I`/`-L` flags and passed to make
  as `COPT`, the documented PostgreSQL hook that appends to both CFLAGS and
  LDFLAGS — right on Linux (dev packages in default paths, flags usually empty)
  and macOS (Homebrew kegs outside the default search path) alike.
- **`PgrxVersion`** — for `pgrx` extensions, the pinned `cargo-pgrx` CLI version.
  It must equal the crate's `pgrx` dependency (pg_graphql 1.6.1 → `0.16.1`) or the
  build is refused. Ignored for PGXS.
- **`BuildSubdir`** — where the build entry point (PGXS `Makefile` or pgrx crate)
  lives in the checkout. pgvector and pg_cron keep it at the repo root (`""`);
  pgmq keeps the extension under `pgmq-extension/`, so it builds there.
- **`FixturesFile`** — a SQL file (relative to `TestSubdir`) the harness loads
  through multigateway with `psql` before the suite, mirroring the extension's own
  runner. pg_graphql's `bin/installcheck` runs `psql -f test/fixtures.sql` first
  (it `CREATE`s the extension and sets the `graphql` schema comment), so the
  harness does too. Empty for extensions whose `.sql` files are self-contained.
- **`ContribDeps`** — contrib modules (by directory name) the harness installs
  with `InstallContribModules` before the suite, because the suite `CREATE`s them.
  pg_graphql's tests `create extension citext`, so it sets
  `ContribDeps: {"citext"}`; without it an external-only run fails those tests
  with "extension citext is not available". A full run has already installed all
  of contrib, so the targeted install is a no-op.
- **`TestSubdir`** — where the shipped `sql/` + `expected/` fixtures live in the
  checkout. pgvector keeps them under `test/`; pg_cron keeps them at the repo
  root (`.`); pgmq keeps them under `pgmq-extension/test`.
- **`ExpectedSubdir`** — where `expected/` lives when it is NOT next to `sql/`
  (pg_regress's `--expecteddir` is a separate knob defaulting to the CWD, and
  hypopg-style layouts keep `expected/` at the repo root). Empty means
  `TestSubdir`.
- **`RegressTests`** — an explicit pg_regress test list mirroring the
  extension's `REGRESS` Makefile variable, for Makefiles where it is not a
  plain wildcard. plpgsql_check ships per-major-version test files
  (`plpgsql_check_active-14` … `-19`) and selects only the pair matching
  `$(MAJORVERSION)`, so the wildcard derivation would run other majors' tests.
- **`ExcludeGlobs`** — removes files the wildcard would otherwise select, on
  both harness paths (pgTAP: relative to `TestSubdir`; pg_regress:
  `sql/<name>.sql`). pg_partman excludes a date-calibrated test; pgtap excludes
  the four files whose entire subject is passing SQL-level prepared-statement
  names into pgTAP assertions — multigateway owns SQL-level `PREPARE` by design
  (the backend never sees the statement name), pgTAP runs them as `EXECUTE`
  inside a plpgsql body the gateway can't see, and the resulting error aborts
  each file's single wrapping transaction. throwtap survives (its four such
  assertions are inside `throws_ok` exception traps) and carries a narrow patch
  instead.
- **`PreloadLibraries`** — shared libraries the extension needs in
  `shared_preload_libraries` (pg_cron's background worker; plpgsql_check's
  passive-mode hooks and shared-memory profiler, which must be active on every
  pooled backend). The harness merges the union across selected extensions into
  ONE generated conf snippet, because the GUC is a single list and
  per-extension `ServerConfigFile`s would clobber each other (snippets are
  last-write-wins per GUC).
- **`DependsOn`** — other `externalSpecs` the harness clones, builds, and installs
  first because the suite `CREATE`s them too. They are build-only (not tested on
  their own, so they need not ship a pg_regress suite). pgmq's `base.sql` creates
  partitioned queues via pg_partman's `create_parent`, so pgmq `DependsOn`
  `pg_partman` (which itself ships only a pgTAP suite). `ExternalBuildList` orders
  dependencies before their dependents.
- **`PreCreateExtensions`** — extensions the harness `CREATE`s through multigateway
  (each optionally into a specific schema) before the run, for fixtures that assume
  an extension already exists. pgvector's fixtures open with a bare
  `CREATE TABLE ... vector(3)`, so it lists `{Name: "vector"}`. pg_partman's pgTAP
  tests expect pgtap in `public` and pg_partman in `partman` (its control file pins
  no schema, so a bare `CREATE EXTENSION` would land it in `public` and break every
  `partman.*` reference), so it lists both with the schema set. Left empty for
  fixtures that manage the extension themselves (pg_cron's first statement is
  `CREATE EXTENSION pg_cron VERSION '1.0'`; pgmq each `DROP`s and re-`CREATE`s it).
- **`ServerConfigFile`** — a `postgresql.conf` snippet under
  `testdata/pg17/external/` the cluster must apply before postgres starts, for
  extensions needing server-level config the pooled query path can't set
  (pg_partman's `max_locks_per_transaction`; pg_cron's `cron.database_name`).
  Do **not** put `shared_preload_libraries` here — use `PreloadLibraries`.
  These snippets (and the generated preload snippet) are scoped to the
  **external phase's cluster only**: in an external-only run they are applied
  at initial initdb; in a combined `RUN_EXTENDED_QUERY_SERVING_TESTS` run the
  regression/isolation/contrib suites run first on a **stock** cluster, and the
  snippets are appended right before the reinitialization that precedes the
  external suite (which runs last). Preloads are not always inert —
  plpgsql_check's cursor-leak detection defaults to on and emits WARNINGs the
  core plpgsql test doesn't expect, and disabling it hits an upstream
  plpgsql_check hang bug — so the other suites must never see them.
- **`ScratchDatabases`** — databases the harness creates directly on the primary
  (bypassing the gateway) before the suite and drops afterward. This is a
  **test-only** accommodation, not a product capability: multigres is
  one-database-per-instance and the gateway blocks `CREATE`/`DROP DATABASE` by
  design (adding a database is a provisioning operation, see
  `docs/query_serving/unsafe_statement_rejection.md`). It exists because some
  suites reference other databases purely as _metadata_ — pg_cron passes a DB
  name to `cron.schedule_in_database` / `alter_job(database := ...)` and reads
  its ACL from the shared `pg_database` catalog, all over the same `postgres`
  connection, never opening a session against it. Creating the physical database
  is enough to make those catalog/privilege checks run for real; the suite's own
  `CREATE`/`DROP DATABASE` statements still hit the gateway block (the only lines
  left in pg_cron's patch). Don't use this to fake reachability of a feature
  multigres genuinely blocks — only for name/metadata references like the above.
- **`WrapTransactions`** — materializes a transformed copy of the suite with
  every test file wrapped in `BEGIN … COMMIT` (plus
  `\set ON_ERROR_ROLLBACK on`), applying the same insertions to the expected
  files so input and oracle stay in lockstep. Inside an explicit transaction
  multigateway reserves ONE pooled backend for its duration, which makes
  suites that assert on backend-local state across autocommit statements
  runnable through the transaction pooler: hypopg (hypothetical indexes live
  in backend memory), supabase_vault (pgTAP's session-temp plan state), http
  (curl option state set via `http_set_curlopt`), pgaudit (audit lines embed a
  per-backend statement counter). The wrap breaks around `\connect`, the
  file's own `BEGIN`/`COMMIT` blocks, and `VACUUM` (which can't run inside a
  transaction block); statements the gateway rejects before they reach a
  backend stay inside. See `wrap.go`.
- **`WrapSetupSQL`** — statements the wrap injects after each transaction
  REOPEN (post-`VACUUM` break or `\connect`), with their exact expected output
  spliced into the materialized expected files. hypopg injects
  `SELECT hypopg_reset();` so a backend acquired after a break starts clean
  regardless of pool history.
- **`TextRewrites`** — literal substitutions applied to the materialized .sql
  and expected copies (diff-neutral: echoed statement text changes identically
  on both sides). http redirects its hard-coded `https://postgis.net` TLS
  probes to the harness-local HTTPS server, keeping the suite hermetic while
  exercising the same libcurl TLS verification paths.
- **`NeedsHTTPBin`** — serves a local httpbin-compatible HTTP server on
  `127.0.0.1:9080` (the port pgsql-http's suite hard-codes) and a self-signed
  HTTPS server on `127.0.0.1:9443` for the suite's duration. Upstream's suite
  probes for a local httpbin and silently falls back to live httpbin.org —
  the in-process server makes the run deterministic with no live internet.
  See `httpbin.go`.
- **`PgPassUsers`** — role/password pairs the suite `\connect`s as. libpq
  applies `PGPASSWORD` to every connection regardless of user, so a suite that
  reconnects as freshly created test users (pgaudit's `regress_user1/2`) is
  run with a harness-written `.pgpass` file (`PGPASSFILE`) instead, which
  resolves the password per user name. The gateway authenticates any role
  whose SCRAM credentials resolve, so mid-test `CREATE USER` works.
- **`LocalTestDir`** — an in-repo `sql/` + `expected/` suite under
  `testdata/pg17/external/<dir>` used instead of fixtures from the checkout,
  for extensions that ship no SQL suite at all. pg_jsonschema's upstream tests
  are pgrx `#[pg_test]` functions inside a private embedded server; the
  harness carries a faithful SQL translation of that corpus (same inputs,
  same expected values, one block per upstream test name) and runs it through
  multigateway.
- **`CleanupSQL`** — statements run directly on the primary after the suite
  for cluster-level state the between-extension reset can't reach (roles:
  supabase_vault's fixtures `CREATE ROLE bob`).

Enrolling another external extension is a small catalog edit: add its
`externalSpecs` entry (repo, pinned tag, and the knobs above) and flip its
`ExtensionCatalog` row to `StatusCovered`. The catalog and report update
automatically. PGXS and Rust/pgrx extensions are both supported (CI provisions
the Rust toolchain); extensions that need other toolchains the harness doesn't
provision (e.g. `wrappers`) or that the pooler blocks by design (outbound
connections) stay `StatusExternal` / `StatusUnsupported`.

Regenerate the patches after an output change with:

```bash
make pgexternal-update-patches   # RUN_PGEXTERNAL=1 PGREGRESS_PATCH_MODE=generate
```

### First Run vs Cached Runs

**First run** (no cache):

- Clones PostgreSQL source (~200MB)
- Builds PostgreSQL from source
- Sets up cluster using built PostgreSQL
- Runs regression tests

**Subsequent runs** (cached source):

- Reuses cached source
- Rebuilds PostgreSQL (build directory is per-run)
- Sets up cluster using built PostgreSQL
- Runs regression tests

**Note**: Running all ~200+ PostgreSQL regression tests takes significantly longer than running a subset. For faster
iteration during development, consider running specific tests only (see "Running Specific Tests Only" section).

### Run Without Caching

To force a fresh clone and build every time (useful for testing cache behavior or CI):

```bash
# Option 1: Remove cache before running
rm -rf /tmp/multigres_pg_cache
RUN_EXTENDED_QUERY_SERVING_TESTS=1 go test -v -timeout 60m ./go/test/endtoend/pgregresstest/...

# Option 2: Use a different cache directory each time
MULTIGRES_PG_CACHE_DIR="/tmp/multigres_pg_test_$(date +%s)" RUN_EXTENDED_QUERY_SERVING_TESTS=1 go test -v -timeout 60m ./go/test/endtoend/pgregresstest/...

# Option 3: Use a custom temporary cache location
MULTIGRES_PG_CACHE_DIR="$(mktemp -d)" RUN_EXTENDED_QUERY_SERVING_TESTS=1 go test -v -timeout 60m ./go/test/endtoend/pgregresstest/...
# Cache will be in a unique temp directory
```

### Run from Root

```bash
# From repository root
RUN_EXTENDED_QUERY_SERVING_TESTS=1 go test -v -timeout 60m ./go/test/endtoend/pgregresstest/...
```

## Troubleshooting

### Build failures

**Symptom**: `configure failed` or `make failed`

**Solutions**:

1. Ensure all build dependencies are installed: `sudo apt-get install build-essential`
2. Check for errors in the configure output
3. Try clearing cache: `rm -rf /tmp/multigres_pg_cache`

### Git clone failures

**Symptom**: `failed to clone PostgreSQL`

**Solutions**:

1. Check network connectivity
2. Verify GitHub is accessible
3. Try manual clone: `git clone --depth=1 --branch REL_17_6 https://github.com/postgres/postgres /tmp/test-clone`

### Connection failures

**Symptom**: Test hangs or times out when running tests

**Solutions**:

1. Verify multigateway is running (check test output)
2. Check that PostgreSQL binaries are in PATH
3. Ensure no firewall blocking localhost connections

### Symbol compatibility errors

**Symptom**: `undefined symbol: pg_encoding_to_char_private` or similar when loading `regress.so`

**Cause**: The regression test library (`regress.so`) was built from a different PostgreSQL version than the running server.

**Solutions**:

1. Ensure the test builds PostgreSQL before setting up the cluster (this is the default behavior)
2. Clear the cache and rebuild: `rm -rf /tmp/multigres_pg_cache`
3. Verify PATH is correctly set to use the built PostgreSQL (check test output for "Using built PostgreSQL from")

### Disk space issues

**Symptom**: `No space left on device`

**Solutions**:

1. Clear build cache: `rm -rf /tmp/multigres_pg_cache/builds`
2. Free up space (source ~200MB, builds ~500MB)
3. Use custom cache location: `export MULTIGRES_PG_CACHE_DIR=~/multigres_cache`

## Live Results & Badges

The nightly CI run publishes the current pass count to GitHub Pages as
[shields.io endpoint](https://shields.io/badges/endpoint-badge) JSON, so the
README badges (and any blog post or doc that embeds them) render the live number
and update automatically after each run — no markdown edits.

Each suite gets a stable JSON URL under `https://multigres.github.io/multigres/pgregress/`:

<!-- markdownlint-disable MD013 -->

| Suite             | JSON endpoint                                                            | Badge markdown                                                                                                                     |
| ----------------- | ------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------- |
| Overall           | `https://multigres.github.io/multigres/pgregress/overall.json`           | `![Overall](https://img.shields.io/endpoint?url=https://multigres.github.io/multigres/pgregress/overall.json)`                     |
| Regression        | `https://multigres.github.io/multigres/pgregress/regression.json`        | `![Regression](https://img.shields.io/endpoint?url=https://multigres.github.io/multigres/pgregress/regression.json)`               |
| Isolation         | `https://multigres.github.io/multigres/pgregress/isolation.json`         | `![Isolation](https://img.shields.io/endpoint?url=https://multigres.github.io/multigres/pgregress/isolation.json)`                 |
| Contrib Extension | `https://multigres.github.io/multigres/pgregress/contrib-extension.json` | `![Contrib Extension](https://img.shields.io/endpoint?url=https://multigres.github.io/multigres/pgregress/contrib-extension.json)` |

<!-- markdownlint-enable MD013 -->

The Go test writes these files (`WriteBadgeEndpoints` in `postgres_builder.go`)
into a `badges/` directory next to `results.json`, and the `test-pgregress.yml`
workflow pushes that directory to the `gh-pages` branch under `pgregress/` on
scheduled/manual runs.

### One-time setup

GitHub Pages must be enabled once for the badges to resolve:

1. **Settings → Pages → Build and deployment → Source: _Deploy from a branch_**,
   branch `gh-pages`, folder `/ (root)`.
2. Trigger the workflow once (**Actions → PostgreSQL Compatibility Tests → Run
   workflow**) so the first JSON files are published. Until then the badges show
   shields.io's "endpoint not found" placeholder.

## Architecture

### Component Flow

<!-- markdownlint-disable MD013 -->

```text
┌─────────────────────────────────────────────────────────────┐
│ TestPostgreSQLRegression                                    │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ PostgresBuilder                                         │ │
│ │ ┌────────────┐  ┌──────────┐  ┌──────────────────────┐  │ │
│ │ │EnsureSource│─▶│  Build   │─▶│  Prepend to PATH     │  │ │
│ │ └────────────┘  └──────────┘  └──────────────────────┘  │ │
│ └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ Multigres Cluster (ShardSetup)                              │
│ Uses built PostgreSQL binaries from PATH                    │
│ ┌──────────-┐  ┌─────────-─┐  ┌─────────────┐               │
│ │multipooler│  │multipooler│  │multigateway │               │
│ │ (primary) │  │(standby)  │  │ (port 5432) │               │
│ └─────┬─────┘  └─────┬─────┘  └──────┬──────┘               │
│       │              │               │                      │
│       ▼              ▼               ▼                      │
│ ┌────────-─┐   ┌──-───────┐   ┌-─────────┐                  │
│ │PostgreSQL│   │PostgreSQL│   │  Proxy   │                  │
│ │ (17.6)   │   │ (17.6)   │   │          │                  │
│ └────────-─┘   └────────-─┘   └──-───────┘                  │
└─────────────────────────────────────────────────────────────┘
                            ▲
                            │
                ┌───────────┴──────────┐
                │ PostgreSQL Tests     │
                │ + regress.so (17.6)  │
                │ (PGHOST=localhost)   │
                │ (PGPORT=multigateway)│
                └──────────────────────┘
```

<!-- markdownlint-enable MD013 -->

**Version Consistency**: The test builds PostgreSQL 17.6 from source and uses those binaries for both the cluster (via
pgctld) and the regression test library (`regress.so`). This ensures symbol compatibility - the regression tests load
shared libraries that must match the running PostgreSQL version.

### File Structure

<!-- markdownlint-disable MD013 -->

```text
go/test/endtoend/pgregresstest/
├── README.md              # This file
├── main_test.go           # TestMain, shared setup management
├── postgres_builder.go    # PostgreSQL build and test execution
├── pgregress_test.go      # Regression + isolation test implementation
├── patch_verify.go        # Patch verification logic
└── patch_verify_test.go   # Tests for patch verification
```

<!-- markdownlint-enable MD013 -->

## Contributing

When adding new tests or expanding the test suite:

1. **Test selection**: Modify the `TESTS` variable in `RunRegressionTests()`
2. **Timeout**: Adjust timeout in `pgregress_test.go` if running more tests
3. **Documentation**: Update this README with new test scope

## References

- [PostgreSQL Source](https://github.com/postgres/postgres)
- [PostgreSQL Build Documentation](https://www.postgresql.org/docs/current/installation.html)
- [PostgreSQL Regression Tests](https://www.postgresql.org/docs/current/regress.html)
