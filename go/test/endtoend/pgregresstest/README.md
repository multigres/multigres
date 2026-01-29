# PostgreSQL Regression Tests

This test package validates multigres compatibility by running PostgreSQL's official regression test suite against a multigres cluster.

## Overview

The test performs the following steps:

1. **Checkout PostgreSQL source**: Clones PostgreSQL 17.6 (REL_17_6) from GitHub
2. **Build with make**: Compiles PostgreSQL using traditional ./configure and make
3. **Spin up multigres cluster**: Creates a 2-node cluster with multigateway
4. **Run regression tests**: Executes specific PostgreSQL tests through multigateway using make installcheck-tests
5. **Report results**: Logs test outcomes (failures are logged but don't fail the test)

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

```bash
# Navigate to test directory
cd go/test/endtoend/pgregresstest

# Run all regression tests (includes PostgreSQL clone, build, and test execution)
go test -v -timeout 60m  # Adjust the timeout based on the number of tests running
```

### First Run vs Cached Runs

**First run** (no cache):

- Clones PostgreSQL source (~200MB)
- Builds PostgreSQL
- Runs regression tests

**Subsequent runs** (cached source):

- Reuses cached source
- Rebuilds PostgreSQL
- Runs regression tests

**Note**: Running all ~200+ PostgreSQL regression tests takes significantly longer than running a subset. For faster iteration during development, consider running specific tests only (see "Running Specific Tests Only" section).

### Skip in Short Mode

```bash
# Short mode skips this test
go test -v -short
# Output: "skipping long-running PostgreSQL regression tests in short mode"
```

### Run Without Caching

To force a fresh clone and build every time (useful for testing cache behavior or CI):

```bash
# Option 1: Remove cache before running
rm -rf /tmp/multigres_pg_cache
go test -v -timeout 60m

# Option 2: Use a different cache directory each time
export MULTIGRES_PG_CACHE_DIR="/tmp/multigres_pg_test_$(date +%s)"
go test -v -timeout 60m

# Option 3: Use a custom temporary cache location
export MULTIGRES_PG_CACHE_DIR="$(mktemp -d)"
go test -v -timeout 60m
# Cache will be in a unique temp directory
```

### Run from Root

```bash
# From repository root
cd /path/to/multigres
go test -v -timeout 60m ./go/test/endtoend/pgregresstest/...
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
│ │ ┌────────────┐  ┌──────────┐  ┌──────────────────────┐ │ │
│ │ │EnsureSource│─▶│  Build   │─▶│RunRegressionTests    │ │ │
│ │ └────────────┘  └──────────┘  └──────────────────────┘ │ │
│ └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ Multigres Cluster (ShardSetup)                              │
│ ┌──────────┐  ┌──────────┐  ┌─────────────┐               │
│ │multipooler│  │multipooler│  │multigateway │               │
│ │ (primary) │  │(standby)  │  │ (port 5432) │               │
│ └─────┬─────┘  └─────┬─────┘  └──────┬──────┘               │
│       │              │               │                       │
│       ▼              ▼               ▼                       │
│ ┌─────────┐    ┌─────────┐    ┌─────────┐                  │
│ │PostgreSQL│   │PostgreSQL│   │  Proxy   │                 │
│ └─────────┘    └─────────┘    └─────────┘                  │
└─────────────────────────────────────────────────────────────┘
                            ▲
                            │
                ┌───────────┴──────────┐
                │ PostgreSQL Tests     │
                │ (PGHOST=localhost)   │
                │ (PGPORT=multigateway)│
                └──────────────────────┘
```

<!-- markdownlint-enable MD013 -->

### File Structure

<!-- markdownlint-disable MD013 -->

```text
go/test/endtoend/pgregresstest/
├── README.md              # This file
├── main_test.go          # TestMain, shared setup management
├── postgres_builder.go   # PostgreSQL build and test execution
└── pgregress_test.go     # Main test implementation
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
