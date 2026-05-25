# PostgreSQL Compatibility Tests

This test package validates multigres compatibility by running PostgreSQL's official regression and isolation test suites against one or more frontends.

## Overview

The test performs the following steps:

1. **Checkout PostgreSQL source**: Clones PostgreSQL 17.6 (REL_17_6) from GitHub
2. **Build with make**: Compiles PostgreSQL using traditional ./configure and make
3. **Build isolation tools** (if enabled): Builds `isolationtester` and `pg_isolation_regress`
4. **Configure PATH**: Prepends the built PostgreSQL bin directory to PATH
5. **Per target**: Spins up the target's cluster (multigateway, pgbouncer-session, or pgbouncer-tx) and runs the enabled suites against it
6. **Report results**: Generates a unified compatibility report (failures are logged but don't fail the test)

**Important**: The test builds PostgreSQL from source and uses those binaries for the test cluster. This ensures the PostgreSQL server and the regression test library (`regress.so`) are from the same version, avoiding symbol compatibility issues.

## Targets

`PGREGRESS_TARGETS` is a comma-separated list of frontends the suites run against. Each target spins up its own PostgreSQL cluster so failures cannot leak across targets.

| Target | Frontend | Backend | Purpose |
|--------|----------|---------|---------|
| `multigateway` (default) | multigres multigateway → multipooler | shardsetup PostgreSQL | Primary target. Patch-based verification is enabled for known multigres-vs-vanilla output differences. |
| `pgbouncer-session` | pgbouncer (pool_mode=session) | standalone PostgreSQL | Near-vanilla pooler baseline. Almost identical behaviour to a direct PostgreSQL connection. |
| `pgbouncer-tx` | pgbouncer (pool_mode=transaction) | standalone PostgreSQL | Strict pooler baseline. Session-scoped state (SET, LISTEN/NOTIFY, advisory locks, temp tables, etc.) is expected to break; the resulting failures are the signal. |

The pgbouncer targets are a **control baseline**: they answer "is this failure unique to multigres, or does any pooler in this mode show it?" The patch directory under `testdata/pg17/patches` is intentionally not consulted for pgbouncer targets — every test runs raw against the rendered config.

Regression detection and the Slack weekly summary read the **multigateway** target only. Pgbouncer columns exist in the report and the JSON results, but they are not part of the regression baseline.

**Isolation tests are multigateway-only.** The harness patches `isolationtester` to route lock-wait probes through `public.multigres_test_session_is_blocked`, a shim that maps virtual PIDs to real backend PIDs via `application_name`. Pgbouncer does not propagate the `multigres_vpid:N` stamp that the shim looks for (session-scoped state), so most specs would hang waiting for lock-release detection that can never fire. Pgbouncer targets therefore skip the isolation suite entirely; their columns render blank in the isolation table.

### Known pgbouncer-session limitations

The session-mode target ships with `server_lifetime = 0` so every client connection forces a fresh PostgreSQL backend. That restores the test invariants the upstream regression suite relies on for temp tables, prepared statements, `LOAD`'ed extensions, and the `ON login` event trigger — and brings pgbouncer-session to **221/222 (99.5%)** against the upstream fixtures.

The remaining fixed-cost failure is `event_trigger_login`:

- The test counts how many times `ON login` fires after creating its trigger. Pgbouncer's own internal connections to the backend (`server_check_query` keepalives, pool setup, admin probes) each open a fresh backend with `server_lifetime = 0` and therefore each fire the user-defined login trigger, inflating the count by one or more before the test's `\c` even runs.
- Avoiding this would require disabling pgbouncer's health-check machinery (`server_check_query = ''`), which trades a production-essential pooler reliability primitive for one test — not a trade we make.

Treat `event_trigger_login` as the expected pgbouncer-session ceiling. Any pooler that probes its own backends will hit the same wall.

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

### pgbouncer (only required for pgbouncer-* targets)

The default `multigateway` target does not need pgbouncer. Install it only when running a comparison target locally. The harness pins a specific version (`PinnedPgbouncerVersion` in `pgbouncer.go`) and hard-fails if the installed binary differs — keeping fixture output reproducible across machines.

```bash
# macOS
brew install pgbouncer
pgbouncer --version   # must match PinnedPgbouncerVersion

# Ubuntu / Debian (PGDG repo is the easiest way to get a current build)
sudo curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
  -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc
echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
  | sudo tee /etc/apt/sources.list.d/pgdg.list
sudo apt-get update
sudo apt-get install -y pgbouncer
pgbouncer --version
```

If the installed version differs from `PinnedPgbouncerVersion`, either bump the constant (and call it out in the PR) or pin your install to the matching version.

## Running Tests

### Basic Usage

The test is **disabled by default**. Three env vars enable it; setting more
than one is fine (the union runs):

- `RUN_EXTENDED_QUERY_SERVING_TESTS=1` — runs **both** regression and isolation.
  This is what CI uses (matches the "Run Extended Query Serving Tests" PR label).
- `RUN_PGREGRESS=1` — runs the regression suite only. Useful for local
  iteration when you don't need isolation.
- `RUN_PGISOLATION=1` — runs the isolation suite only.

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
```

### Running the pooler comparison locally

Pgbouncer comparison targets are opt-in via `PGREGRESS_TARGETS`. Install pgbouncer first (see the dependencies section above).

```bash
# All three targets (multigateway + both pgbouncer modes)
PGREGRESS_TARGETS=multigateway,pgbouncer-session,pgbouncer-tx \
RUN_PGREGRESS=1 RUN_PGISOLATION=1 \
go test -v -timeout 180m ./go/test/endtoend/pgregresstest/...

# Pgbouncer targets only (skip the multigres path for faster iteration)
PGREGRESS_TARGETS=pgbouncer-session,pgbouncer-tx \
RUN_PGREGRESS=1 \
go test -v -timeout 120m ./go/test/endtoend/pgregresstest/...
```

Each target gets a separate 20-minute suite timeout, so a slow target cannot starve later ones. Bump `-timeout` on the outer `go test` invocation accordingly.

### Triggering on GitHub

`test-pgregress.yml` exposes the comparison-target run through `workflow_dispatch` only. The nightly cron, weekly cron, and the "Run Extended Query Serving Tests" PR label always run multigateway-only.

To launch a comparison run from the GitHub UI:

1. Navigate to **Actions → PostgreSQL Compatibility Tests**.
2. Click **Run workflow**.
3. Pick the branch and choose a value for **targets**:
   - `multigateway` (default — same as cron)
   - `multigateway,pgbouncer-session,pgbouncer-tx`
   - `pgbouncer-session,pgbouncer-tx`
4. Click the green **Run workflow** button.

From the command line:

```bash
gh workflow run test-pgregress.yml \
  -f targets="multigateway,pgbouncer-session,pgbouncer-tx" \
  --ref <branch>
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

**Note**: Running all ~200+ PostgreSQL regression tests takes significantly longer than running a subset. For faster iteration during development, consider running specific tests only (see "Running Specific Tests Only" section).

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

**Version Consistency**: The test builds PostgreSQL 17.6 from source and uses those binaries for both the cluster (via pgctld) and the regression test library (`regress.so`). This ensures symbol compatibility - the regression tests load shared libraries that must match the running PostgreSQL version.

### File Structure

<!-- markdownlint-disable MD013 -->

```text
go/test/endtoend/pgregresstest/
├── README.md                  # This file
├── main_test.go               # TestMain, shared multigateway setup
├── postgres_builder.go        # PostgreSQL build, suite execution, report writers
├── pgregress_test.go          # Top-level test: target loop, suite drivers
├── target_cluster_test.go     # Per-target cluster lifecycle (multigateway, pgbouncer-*)
├── targets.go                 # Target enum + PGREGRESS_TARGETS parser
├── pgbouncer.go               # Pgbouncer process management + version pin
└── testdata/
    ├── pg17/patches/          # Multigateway-target output patches (no patches for pgb-*)
    └── pgbouncer/             # session.ini.tmpl + tx.ini.tmpl rendered per instance
```

<!-- markdownlint-enable MD013 -->

### Reading the comparison report

`compatibility-report.md` (also pasted into `GITHUB_STEP_SUMMARY` on CI) has the following shape when more than one target ran:

```text
## PostgreSQL Compatibility Report

**PostgreSQL Version:** `REL_17_6`
**Pgbouncer Version:** `1.24.1`

[Regression — multigateway badge] [Regression — pgbouncer-session badge] [Regression — pgbouncer-tx badge]

### Regression Tests

| # | Test     | multigateway | pgbouncer-session | pgbouncer-tx |
|---|----------|--------------|-------------------|--------------|
| 1 | boolean  | ✅           | ✅                 | ✅           |
| 2 | char     | ✅           | ✅                 | ❌           |
| 3 | numeric  | ❌           | ❌                 | -            |
```

- `✅` / `❌` / `⏭️` mean the test passed / failed / was skipped on that target.
- `-` means the test did not run on that target (e.g. pgbouncer-tx timed out before reaching it).
- A failure that appears in **every** target column is almost certainly a pooler-generic limitation — useful when triaging multigres-only failures.

## Contributing

When adding new tests or expanding the test suite:

1. **Test selection**: Modify the `TESTS` variable in `RunRegressionTests()`
2. **Timeout**: Adjust timeout in `pgregress_test.go` if running more tests
3. **Documentation**: Update this README with new test scope

## References

- [PostgreSQL Source](https://github.com/postgres/postgres)
- [PostgreSQL Build Documentation](https://www.postgresql.org/docs/current/installation.html)
- [PostgreSQL Regression Tests](https://www.postgresql.org/docs/current/regress.html)
