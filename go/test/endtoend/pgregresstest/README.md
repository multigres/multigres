# PostgreSQL Compatibility Tests

This test package validates multigres compatibility by running PostgreSQL's official regression and isolation test suites against a multigres cluster.

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

**Important**: The test builds PostgreSQL from source and uses those binaries for the test cluster. This ensures the PostgreSQL server and the regression test library (`regress.so`) are from the same version, avoiding symbol compatibility issues.

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

The test is **disabled by default**. Set `RUN_PGREGRESS=1` and/or `RUN_PGISOLATION=1` to enable:

```bash
# Run regression tests only
RUN_PGREGRESS=1 go test -v -timeout 60m ./go/test/endtoend/pgregresstest/...

# Run isolation tests only
RUN_PGISOLATION=1 go test -v -timeout 60m ./go/test/endtoend/pgregresstest/...

# Run both suites (unified report)
RUN_PGREGRESS=1 RUN_PGISOLATION=1 go test -v -timeout 60m ./go/test/endtoend/pgregresstest/...

# Without either variable, all tests are skipped
go test -v ./go/test/endtoend/pgregresstest/...
# Output: "skipping pg_regress/isolation tests (set RUN_PGREGRESS=1 and/or RUN_PGISOLATION=1 to run)"
```

### Running Specific Tests

```bash
# Run specific regression tests only
PGREGRESS_TESTS="boolean char" RUN_PGREGRESS=1 go test -v -timeout 60m ./go/test/endtoend/pgregresstest/...

# Run specific isolation tests only
PGISOLATION_TESTS="deadlock-simple tuplelock-update" RUN_PGISOLATION=1 go test -v -timeout 60m ./go/test/endtoend/pgregresstest/...
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
RUN_PGREGRESS=1 go test -v -timeout 60m ./go/test/endtoend/pgregresstest/...

# Option 2: Use a different cache directory each time
MULTIGRES_PG_CACHE_DIR="/tmp/multigres_pg_test_$(date +%s)" RUN_PGREGRESS=1 go test -v -timeout 60m ./go/test/endtoend/pgregresstest/...

# Option 3: Use a custom temporary cache location
MULTIGRES_PG_CACHE_DIR="$(mktemp -d)" RUN_PGREGRESS=1 go test -v -timeout 60m ./go/test/endtoend/pgregresstest/...
# Cache will be in a unique temp directory
```

### Run from Root

```bash
# From repository root
RUN_PGREGRESS=1 go test -v -timeout 60m ./go/test/endtoend/pgregresstest/...
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
├── README.md              # This file
├── main_test.go          # TestMain, shared setup management
├── postgres_builder.go   # PostgreSQL build and test execution
└── pgregress_test.go     # Regression + isolation test implementation
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
