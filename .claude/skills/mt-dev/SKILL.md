---
name: "Development Tools"
description: "Run unit tests, integration tests, and development tasks for multigres"
---

# Development Tools

Development commands for the multigres project.

## Command Structure

```text
/mt-dev [test-type] [args...]
```

## For Claude Code

When executing commands:

- Always run `make build` before integration tests
- Show the actual command being executed before running it
- Summarize test results (passed/failed counts, execution time)
- If tests fail, offer to show detailed output or logs
- For verbose output, provide a summary rather than dumping everything

---

## Unit Tests

Unit tests are fast, isolated tests for individual functions and packages. They don't require external services or build artifacts.
Note: plain `go test ./go/...` will also traverse `go/test/endtoend/...`; use `-short` for unit-focused runs.

### Command Syntax

**Run all unit tests:**

```bash
/mt-dev unit all
```

Executes: `go test -short ./go/...`

**Run specific package:**

```bash
/mt-dev unit <package-path>
```

Examples:

- `/mt-dev unit ./go/multipooler/...` - All multipooler package tests
- `/mt-dev unit ./go/multigateway/...` - All multigateway package tests
- `/mt-dev unit ./go/pgprotocol/...` - All pgprotocol package tests

**Run specific test:**

```bash
/mt-dev unit <package-path> <TestName>
```

Examples:

- `/mt-dev unit ./go/multipooler TestConnectionPool`
- `/mt-dev unit ./go/pgprotocol TestParseQuery`

**Run with pattern matching:**

```bash
/mt-dev unit <package-path> <TestPattern>
```

Examples:

- `/mt-dev unit ./go/multipooler TestConn.*` - All tests starting with TestConn
- `/mt-dev unit ./go/multigateway Test.*Route.*` - All tests with "Route" in name

### Common Flags

- `-v` - Verbose output (shows all test names as they run)
- `-race` - Enable race detector (slower, catches concurrency bugs)
- `-cover` - Show coverage percentage
- `-coverprofile=coverage.out` - Generate coverage report
- `-count=N` - Run tests N times (useful for flaky test detection); `-count=1` also forces re-run and bypasses test cache
- `-timeout=30s` - Set timeout (default: 10m)
- `-short` - Skip long-running tests
- `-parallel=N` - Run N tests in parallel (default: GOMAXPROCS)

### Examples

```bash
# Quick test run
/mt-dev unit all

# Verbose with race detection
/mt-dev unit ./go/multipooler/... -v -race

# Coverage report
/mt-dev unit ./go/pgprotocol/... -cover

# Test for flakiness
/mt-dev unit ./go/multigateway TestRouting -count=10

# Fast tests only
/mt-dev unit all -short

# Specific test with verbose output
/mt-dev unit ./go/multipooler TestConnectionPool -v
```

### Natural Language Support

- "run unit tests" → `/mt-dev unit all`
- "test the multipooler package" → `/mt-dev unit ./go/multipooler/...`
- "run TestConnectionPool" → `/mt-dev unit ./go/multipooler TestConnectionPool`
- "run all unit tests with coverage" → `/mt-dev unit all -cover`
- "check for race conditions in multigateway" → `/mt-dev unit ./go/multigateway/... -race`

---

## Integration Tests

Integration tests are end-to-end tests that start real components (MultiGateway, MultiPooler, PostgreSQL) and test their interactions. These tests are slower and require building the project first.

**IMPORTANT**: Integration tests always run `make build` first.

### Available Test Packages

- `all` - Run all integration tests
- `multipooler` - Connection pooling, pool lifecycle, connection management
- `multiorch` - Orchestration, failover, leader election, consensus protocol
- `queryserving` - Query routing, execution, transaction handling
- `localprovisioner` - Local cluster provisioning and setup
- `shardsetup` - Shard configuration and management
- `pgregresstest` - PostgreSQL regression tests (opt-in, comprehensive)

### Integration Test Command Syntax

**Run all integration tests:**

```bash
/mt-dev integration all
```

Executes: `make build && go test ./go/test/endtoend/...`

**Run specific package:**

```bash
/mt-dev integration <package-name>
```

Examples:

- `/mt-dev integration multipooler` → `make build && go test ./go/test/endtoend/multipooler/...`
- `/mt-dev integration multiorch` → `make build && go test ./go/test/endtoend/multiorch/...`
- `/mt-dev integration queryserving` → `make build && go test ./go/test/endtoend/queryserving/...`

**Run specific test:**

```bash
/mt-dev integration <package-name> <TestName>
```

Examples:

- `/mt-dev integration multiorch TestFixReplication` → `make build && go test -run TestFixReplication ./go/test/endtoend/multiorch/...`
- `/mt-dev integration multipooler TestConnCache` → `make build && go test -run TestConnCache ./go/test/endtoend/multipooler/...`

**Run specific test in all packages:**

```bash
/mt-dev integration all <TestName>
```

Example:

- `/mt-dev integration all TestBootstrap` → `make build && go test -run TestBootstrap ./go/test/endtoend/...`

**Run with pattern matching:**

```bash
/mt-dev integration <package-name> <TestPattern>
```

Examples:

- `/mt-dev integration queryserving Test.*Transaction.*` - All transaction tests
- `/mt-dev integration multipooler TestConn.*` - All connection tests

### Integration Test Flags

Same flags as unit tests, plus:

- `-timeout=30m` - Integration tests often need longer timeouts (default: 10m)
- `-p=1` - Run packages sequentially (useful if tests conflict on resources)
- `-count=N` - Run tests N times (useful to detect flakes); `-count=1` also forces re-run and bypasses test cache

### Integration Test Examples

```bash
# Run all integration tests
/mt-dev integration all

# Test multipooler with verbose output
/mt-dev integration multipooler -v

# Test specific failure scenario
/mt-dev integration multiorch TestFixReplication

# Check for race conditions in query serving
/mt-dev integration queryserving -race

# Test for flakiness (run 10 times)
/mt-dev integration multipooler TestConnCache -count=10

# Run with extended timeout
/mt-dev integration all -timeout=45m

# Sequential execution to avoid resource conflicts
/mt-dev integration all -p=1
```

### Integration Test Natural Language

- "run integration tests" → `/mt-dev integration all`
- "run multipooler tests" → `/mt-dev integration multipooler`
- "test multiorch TestFixReplication" → `/mt-dev integration multiorch TestFixReplication`
- "run all integration tests with race detector" → `/mt-dev integration all -race`
- "test query serving" → `/mt-dev integration queryserving`

---

## Interpreting Test Results

### Success Output

```text
PASS
ok      github.com/multigres/multigres/go/multipooler    2.456s
```

- All tests passed
- Shows package path and execution time

### Failure Output

```text
--- FAIL: TestConnectionPool (0.15s)
    pool_test.go:45: expected 10 connections, got 8
FAIL
FAIL    github.com/multigres/multigres/go/multipooler    2.456s
```

- Shows which test failed
- Shows file, line number, and failure message
- Claude should summarize: "TestConnectionPool failed in pool_test.go:45"

### Build Failure

```text
# github.com/multigres/multigres/go/multipooler
./connection.go:123:45: undefined: somethingMissing
FAIL    github.com/multigres/multigres/go/multipooler [build failed]
```

- Compilation error before tests could run
- Claude should highlight the build error and suggest checking the code

### Race Condition Detected

```text
==================
WARNING: DATA RACE
Read at 0x00c0001a2080 by goroutine 7:
  ...
==================
```

- Race detector found a potential concurrency bug
- Claude should flag this as critical and recommend investigation

### Timeout

```text
panic: test timed out after 10m0s
```

- Test exceeded timeout
- Claude should suggest increasing timeout or investigating hanging test

---

## Common Workflows

### Before Committing Code

```bash
# 1. Run unit tests (fast feedback)
/mt-dev unit all

# 2. If unit tests pass, run integration tests
/mt-dev integration all

# 3. Check for race conditions
/mt-dev integration all -race
```

### Debugging a Failing Test

```bash
# 1. Run the specific test with verbose output
/mt-dev integration multipooler TestConnCache -v

# 2. Check if it's flaky (intermittent failure)
/mt-dev integration multipooler TestConnCache -count=10

# 3. Run with race detector
/mt-dev integration multipooler TestConnCache -race -v
```

### Testing a Specific Package After Changes

```bash
# Unit tests first (fast)
/mt-dev unit ./go/multipooler/... -v

# Integration tests second
/mt-dev integration multipooler -v

```

---

## Troubleshooting

### "build failed" during integration tests

- Run `make build` manually to see detailed error
- Check for uncommitted generated files (protobuf)
- Verify all dependencies are installed

### Flaky tests (pass sometimes, fail others)

- Run with `-count=10` to reproduce
- Enable race detector: `-race`
- Check for timing-dependent code or shared state

---

## Debugging Integration Test Failures

When integration tests fail, logs are automatically preserved in a temp directory. Follow this systematic approach to debug failures:

### Step 1: Find the Log Location

After a test failure, look for this message in the test output:

```text
==== TEST LOGS PRESERVED ====
Logs available at: /tmp/shardsetup_test_XXXXXXXXXX
Set TEST_PRINT_LOGS=1 to print log contents
===========================
```

The temp directory contains all component logs from the failed test.

### Step 2: Understand the Directory Structure

Integration test log directories follow this structure:

```text
/tmp/shardsetup_test_XXXXXXXXXX/
├── multigateway.log              # Multigateway service logs
├── temp-multiorch/               # Temporary multiorch used during bootstrap
│   └── multiorch.log
├── pooler-1/                     # First multipooler instance (pooler-1, pooler-2, etc.)
│   ├── multipooler.log           # Multipooler service logs
│   ├── pgctld.log                # PostgreSQL control daemon logs
│   └── data/
│       ├── pg_data/
│       │   └── postgresql.log    # PostgreSQL server logs
│       ├── pg_sockets/           # Unix sockets for PostgreSQL connections
│       └── pgbackrest/
│           └── logs/             # Backup/restore logs
│               ├── multigres-backup.log
│               ├── multigres-restore.log
│               └── all-server.log
├── pooler-2/                     # Second multipooler instance
│   └── (same structure as pooler-1)
├── pooler-3/                     # Third multipooler instance
│   └── (same structure as pooler-1)
├── etcd_data/                    # etcd data directory
├── backup-repo/                  # pgBackRest backup repository
└── certs/                        # TLS certificates for pgBackRest
```

### Step 3: Identify the Failing Component

Read the test failure message to identify which component is involved:

**Example failure patterns:**

- **Connection/query errors** → Check multipooler logs and PostgreSQL logs
- **Replication issues** → Check all pooler's PostgreSQL logs and multipooler logs
- **Consensus/failover issues** → Check multipooler logs (consensus protocol) and temp-multiorch logs
- **Backup/restore errors** → Check pgbackrest logs in `pooler-*/data/pgbackrest/logs/`
- **Gateway routing issues** → Check multigateway.log
- **Bootstrap failures** → Check temp-multiorch/multiorch.log and all pooler logs

### Step 4: Examine Relevant Logs

**Use this decision tree:**

1. **What component is the test about?**
   - Multipooler → Start with `pooler-*/multipooler.log`
   - PostgreSQL → Start with `pooler-*/data/pg_data/postgresql.log`
   - Replication → Check PostgreSQL logs on all poolers
   - Multigateway → Check `multigateway.log`
   - Bootstrap/orchestration → Check `temp-multiorch/multiorch.log`

2. **Look for ERROR or FATAL level messages** in the relevant logs

3. **Check timestamps** - Look at logs around the time of the test failure

4. **Follow the chain** - If one component reports an error from another, check that component's logs next

### Step 5: Common Debugging Commands

```bash
# View all log files in the preserved directory
find /tmp/shardsetup_test_XXXXXXXXXX -name "*.log" -type f

# Search for errors across all logs
grep -r "ERROR\|FATAL" /tmp/shardsetup_test_XXXXXXXXXX/

# View logs for a specific pooler
cat /tmp/shardsetup_test_XXXXXXXXXX/pooler-1/multipooler.log
cat /tmp/shardsetup_test_XXXXXXXXXX/pooler-1/data/pg_data/postgresql.log

# View multigateway logs
cat /tmp/shardsetup_test_XXXXXXXXXX/multigateway.log

# View backup/restore logs
ls /tmp/shardsetup_test_XXXXXXXXXX/pooler-1/data/pgbackrest/logs/
```

### Step 6: Print Full Logs in CI

To see full log contents printed directly in test output (useful for CI):

```bash
TEST_PRINT_LOGS=1 /mt-dev integration shardsetup TestName
```

This will print all log files to stdout after the test fails, making them visible in CI logs without needing to download artifacts.

### Example Debugging Workflow

**Scenario:** `TestReplicationWorks` fails with "replication not configured"

1. **Find logs:** Test output shows `/tmp/shardsetup_test_1342939839`

2. **Identify components:** Replication involves multipooler and PostgreSQL on all nodes

3. **Check pooler logs:**

   ```bash
   # Check multipooler logs for all poolers
   grep -i "replication\|primary_conninfo" /tmp/shardsetup_test_1342939839/pooler-*/multipooler.log

   # Check PostgreSQL logs
   grep -i "replication\|standby" /tmp/shardsetup_test_1342939839/pooler-*/data/pg_data/postgresql.log
   ```

4. **Look for specific issues:**
   - Is `primary_conninfo` being set?
   - Are there connection errors from standby to primary?
   - Is WAL streaming active?
   - Check for permission or authentication errors

5. **Check multiorch logs** if bootstrap or failover is involved:
   ```bash
   cat /tmp/shardsetup_test_1342939839/temp-multiorch/multiorch.log
   ```

### Important Notes

- **Logs are only preserved on test failure** - successful tests clean up automatically
- **Logs are cleaned on test success** - if you need logs from passing tests, set a breakpoint or add `t.Fatal()` at the end
- **Each test run gets a unique temp directory** - look for the most recent timestamp
- **Service logs are JSON formatted** - use `jq` for pretty printing:
  ```bash
  cat pooler-1/multipooler.log | jq '.'
  ```

---

## Tips

- **Unit tests** are fast (~seconds) - run them frequently while developing
- **Integration tests** are slow (~minutes) - run them before committing
- Use `-short` flag on unit tests to skip slow tests during rapid iteration
- Use `-v` flag when debugging to see which test is running
- Use `-race` flag before committing to catch concurrency bugs
- Use `-count=10` to verify test stability (per CLAUDE.md guidelines)
- Use `-count=1` to bypass cached results after environment or build changes
- If build fails, check that you've committed all generated files
- Coverage reports help identify untested code: `-coverprofile=coverage.out`
- Most integration test failures indicate real bugs, not flaky tests
- **When integration tests fail, see "Debugging Integration Test Failures" section above for systematic debugging approach**

---

## Implementation Notes for Claude

### Argument Parsing Logic

1. **First argument** (required):
   - For unit tests: package path or "all"
   - For integration tests: package name or "all"

2. **Second argument** (optional):
   - If starts with `Test`: use as `-run <TestName>` argument
   - If starts with `-`: treat as flag
   - Otherwise: error, show help

3. **Additional arguments**:
   - Parse as flags and append to go test command
   - Preserve order and formatting

### Command Construction

**Unit tests:**

```bash
go test [flags] [-run TestName] <package-path>
```

**Integration tests:**

```bash
make build && go test [flags] [-run TestName] ./go/test/endtoend/<package>/...
```

### Output Handling

- Capture both stdout and stderr
- Summarize results: "X passed, Y failed in Z seconds"
- If failures, show failed test names and error messages
- For verbose output, provide condensed summary
- Offer to show full output if summary isn't sufficient
