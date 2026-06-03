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
multigateway. pgvector is the first: catalog name `vector`, pinned to a tag in
`externalSpecs` (`extensions.go`).

How it works, and how it differs from the contrib suite:

- **Build**: each external extension is a [PGXS](https://www.postgresql.org/docs/current/extend-pgxs.html)
  module. `Builder.InstallExternalExtension` shallow-clones the pinned tag into
  the per-run build root, then runs `make && make install` with
  `PG_CONFIG` pointed at the from-source PostgreSQL so the extension `.so` links
  against the exact server ABI the cluster runs (the same guarantee the suite
  gives `regress.so`). pgvector needs only a C compiler — no extra system libs.
- **Test execution**: unlike contrib we cannot use `make installcheck`. Under
  PGXS that target invokes `$(top_builddir)/src/test/regress/pg_regress`, and
  PGXS resolves `top_builddir` into the **install** tree, where `pg_regress` is
  not installed. The harness instead invokes the `pg_regress` it built directly,
  with the same flags the contrib suite relies on
  (`--use-existing --dbname=postgres`, because multigateway rejects DROP/CREATE
  DATABASE) plus the extension's `--inputdir`/`--load-extension`. The test list
  is derived from `test/sql/*.sql`, mirroring the extension's
  `REGRESS = $(patsubst test/sql/%.sql,%,$(wildcard test/sql/*.sql))`.
- **Per-extension isolation** and **verification** work exactly like contrib:
  the `public` schema is reset on the primary between extensions, and results go
  through the same patch pipeline (`PGREGRESS_PATCH_MODE`). Genuine
  multigres-specific output differences are captured under
  `testdata/pg17/patches/external/<ext>/`.

Enrolling another external extension is a two-line change: add its
`externalSpecs` entry (repo + pinned tag) and flip its `ExtensionCatalog` row to
`StatusCovered`. The catalog and report update automatically. Extensions that
need toolchains the harness doesn't provision (Rust: `pg_graphql`, `wrappers`)
or that the pooler blocks by design (outbound connections) stay `StatusExternal`
/ `StatusUnsupported`.

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
