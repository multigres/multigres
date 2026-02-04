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
- Check component logs when integration tests fail (future: use mt-local-cluster skill)

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
