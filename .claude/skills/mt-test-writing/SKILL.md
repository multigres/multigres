---
name: "Test Writing Guide"
description: "Comprehensive guide for writing thorough, reliable, and fast tests in multigres"
---

# Test Writing Guide

Guidance for writing high-quality tests in the multigres project. Multigres is mission-critical infrastructure - tests must be **thorough**, **reliable**, and **fast**.

## Quick Start

### Test Types

**Unit Tests** - Fast, isolated tests for individual functions and packages

- No external services required
- Run with `/mt-dev unit <package>`
- Examples: `go/services/multigateway/discovery_test.go`, `go/services/multiorch/recovery/engine_test.go`

**Integration Tests** - End-to-end tests with real components

- Require PostgreSQL binaries (`initdb`, `postgres`, `pg_ctl`, `pg_isready`)
- Run with `/mt-dev integration <package>`
- Located in `go/test/endtoend/`
- Skip with `-short` flag or when PostgreSQL binaries unavailable

### Running Tests

Use the `/mt-dev` skill for all test execution:

```bash
/mt-dev unit all                          # All unit tests
/mt-dev unit ./go/multipooler/...         # Specific package
/mt-dev unit ./go/multipooler TestConnPool # Specific test

/mt-dev integration all                    # All integration tests
/mt-dev integration multipooler            # Specific package
/mt-dev integration multiorch TestFailover # Specific test
```

Common flags: `-v` (verbose), `-race` (race detection), `-count=10` (check for flakiness), `-cover` (coverage)

---

## Core Principles

### 1. Thorough Testing

- **Aim for 100% code coverage** - it may not always be possible, but strive for it
- **Test error paths, not just happy paths** - failures matter more than successes
- **Critical coverage areas**:
  - Query path: protocol handling, connection pooling, routing
  - Management plane: multipooler manager operations, state transitions
  - Orchestration: failover, promotion/demotion, consensus

### 2. Reliable Testing

- **NO FLAKY TESTS** - flaky tests slow everything down
- Use `require.Eventually()` to wait for async operations, never assume timing
- Check error details, not just presence/absence
- Clean up all spawned processes reliably

### 3. Fast Testing

- Minimize unnecessary waits - use `require.Eventually()` with appropriate intervals
- Prefer deterministic patterns over time-based assumptions
- Use in-memory stores and `bufconn.Listener` for unit tests
- Keep integration tests focused and efficient
- **Pay attention to test execution times** - `go test -v` shows duration for each test
- **Check with the user before adding slow tests** (>5 seconds) - they compound over time

#### Test Performance Guidelines

**Unit tests should be fast:**

- Target: <100ms per test
- Acceptable: <1s per test
- If >1s: Consider if it's really a unit test or should be an integration test

**Integration tests will be slower but should still be optimized:**

- Target: <5s per test
- Acceptable: <10s per test
- If >10s: Check with user before adding - explain why it's slow and if it can be optimized

**Before adding a slow test:**

1. Run the test and note the duration from `go test -v` output
2. If it's >5 seconds, explain to the user:
   - What the test does
   - Why it takes that long
   - Whether there are optimization opportunities
3. Ask if the test value justifies the time cost

**Example:**

```
--- PASS: TestOrphanDetectionWithRealPostgreSQL (15.82s)
```

This test takes 15.8s because it:

- Starts pgctld and PostgreSQL
- Triggers orphan detection by deleting directories
- Waits up to 15s for watchdog to stop PostgreSQL (polls every 1s)

Optimization opportunities: Could reduce timeout or make watchdog check more frequently.

---

## Test Utilities

### Context Management (`go/test/utils/context.go`)

**Always use `t.Context()` as the parent context, never `context.Background()`**

```go
import "github.com/multigres/multigres/go/test/utils"

// Create context with timeout and automatic cleanup
ctx := utils.WithTimeout(t, 5*time.Second)

// Short deadline (2 seconds) for quick operations
ctx := utils.WithShortDeadline(t)

// Custom timeout
ctx := utils.WithTimeout(t, 30*time.Second)
```

**Exception**: Inside `t.Cleanup()` functions, the test context is already cancelled, so you may need `context.Background()` or a fresh context.

### Port Allocation (`go/test/utils/portallocator.go`)

**Never hardcode ports** - use the port allocator to prevent conflicts:

```go
import "github.com/multigres/multigres/go/test/utils"

port := utils.GetFreePort(t)  // Automatically cleaned up
```

The allocator prevents port reuse within the same process and registers cleanup automatically.

### PostgreSQL Binary Detection (`go/test/utils/binaries.go`)

Skip integration tests when PostgreSQL binaries unavailable:

```go
import "github.com/multigres/multigres/go/test/utils"

func TestIntegration(t *testing.T) {
    if testing.Short() {
        t.Skip("Skipping integration test (short mode)")
    }
    if utils.ShouldSkipRealPostgres() {
        t.Skip("Skipping integration test (no PostgreSQL binaries)")
    }

    // Test code...
}
```

### Configuration Builder (`go/test/utils/configbuilder.go`)

Fluent API for building test configurations:

```go
import "github.com/multigres/multigres/go/test/utils"

config := utils.NewTestConfig().
    WithLocalBackup("/tmp/backup").
    WithS3Backup("bucket", "region").
    WithEndpoint("http://localhost:9000").
    Build()
```

---

## Unit Test Patterns

### Basic Unit Test Structure

```go
package mypackage

import (
    "testing"

    "github.com/stretchr/testify/require"
    "github.com/multigres/multigres/go/test/utils"
)

func TestMyFunction(t *testing.T) {
    // Use t.Context() as parent
    ctx := utils.WithShortDeadline(t)

    // Setup
    input := "test data"

    // Execute
    result, err := MyFunction(ctx, input)

    // Assert
    require.NoError(t, err)
    require.Equal(t, "expected", result)
}
```

### Mock gRPC Services

Embed `pb.Unimplemented*Server` to create mocks:

```go
type MockMultipoolerManager struct {
    pb.UnimplementedMultiPoolerManagerServer

    // Track calls for assertions
    statusCalls int
    statusResp  *multipoolermanagerdatapb.StatusResponse
}

func (m *MockMultipoolerManager) Status(
    ctx context.Context,
    req *multipoolermanagerdatapb.StatusRequest,
) (*multipoolermanagerdatapb.StatusResponse, error) {
    m.statusCalls++
    if m.statusResp != nil {
        return m.statusResp, nil
    }
    return nil, errors.New("not configured")
}

func TestWithMockService(t *testing.T) {
    mock := &MockMultipoolerManager{
        statusResp: &multipoolermanagerdatapb.StatusResponse{
            // Configure response
        },
    }

    // Use mock in test...

    // Assert mock was called
    require.Equal(t, 1, mock.statusCalls)
}
```

### In-Memory Topology Store

Use `memorytopo` for fast unit tests:

```go
import (
    "github.com/multigres/multigres/go/clustermetadata/topoclient"
    "github.com/multigres/multigres/go/clustermetadata/topoclient/memorytopo"
)

func TestWithTopology(t *testing.T) {
    ctx := utils.WithShortDeadline(t)

    // Create in-memory topology store
    store, factory := memorytopo.NewServerAndFactory(ctx, "test-cell")
    defer store.Close()

    // Use store in test...
}
```

### Fast gRPC Testing with bufconn

Use `bufconn.Listener` for in-memory gRPC connections (no TCP):

```go
import (
    "google.golang.org/grpc"
    "google.golang.org/grpc/test/bufconn"
)

func TestGRPCService(t *testing.T) {
    ctx := utils.WithShortDeadline(t)

    // Create in-memory listener
    lis := bufconn.Listen(1024 * 1024)
    defer lis.Close()

    // Start gRPC server
    server := grpc.NewServer()
    pb.RegisterMyServiceServer(server, &MyServiceImpl{})
    go server.Serve(lis)
    defer server.Stop()

    // Create client with bufconn dialer
    conn, err := grpc.DialContext(ctx, "bufnet",
        grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
            return lis.Dial()
        }),
        grpc.WithInsecure(),
    )
    require.NoError(t, err)
    defer conn.Close()

    client := pb.NewMyServiceClient(conn)

    // Test with client...
}
```

### Waiting for Async Operations

**ALWAYS use `require.Eventually()`, NEVER `time.Sleep()`**

```go
const (
    testTimeout      = 5 * time.Second
    testPollInterval = 10 * time.Millisecond
)

func TestAsyncOperation(t *testing.T) {
    // Start async operation
    go doSomethingAsync()

    // Wait for it to complete
    require.Eventually(t, func() bool {
        return checkIfComplete()
    }, testTimeout, testPollInterval,
        "Operation should complete within %v", testTimeout)
}
```

**Pattern**: `require.Eventually(t, conditionFunc, timeout, pollInterval, message)`

Example from real tests:

```go
func waitForPoolerCount(t *testing.T, pd *CellPoolerDiscovery, expected int) {
    require.Eventually(t, func() bool {
        return pd.PoolerCount() == expected
    }, 5*time.Second, 10*time.Millisecond,
        "Expected %d poolers, but got %d", expected, pd.PoolerCount())
}
```

---

## Integration Test Patterns

### Basic Integration Test Structure

```go
package endtoend

import (
    "testing"

    "github.com/stretchr/testify/require"
    "github.com/multigres/multigres/go/test/utils"
    "github.com/multigres/multigres/go/test/endtoend/shardsetup"
)

func TestIntegrationScenario(t *testing.T) {
    // Skip if appropriate
    if testing.Short() {
        t.Skip("Skipping integration test (short mode)")
    }
    if utils.ShouldSkipRealPostgres() {
        t.Skip("Skipping integration test (no PostgreSQL binaries)")
    }

    // Setup isolated cluster
    setup, cleanup := shardsetup.NewIsolated(t,
        shardsetup.WithMultipoolerCount(3),
        shardsetup.WithoutInitialization(),
    )
    defer cleanup()

    // Verify initial state
    require.NotNil(t, setup.Multipoolers)

    // Start services and wait for readiness
    require.Eventually(t, func() bool {
        // Check if services are ready
        return checkServicesReady(t, setup)
    }, 30*time.Second, 1*time.Second,
        "Services should be ready")

    // Run sub-tests
    t.Run("verify state", func(t *testing.T) {
        // Test specific behavior
    })
}

func checkServicesReady(t *testing.T, setup *shardsetup.ShardSetup) bool {
    for _, inst := range setup.Multipoolers {
        client, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
        if err != nil {
            return false
        }

        ctx := utils.WithTimeout(t, 5*time.Second)
        _, err = client.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
        client.Close()

        if err != nil {
            return false
        }
    }
    return true
}
```

### Subprocess Management

Integration tests spawn subprocesses (postgres, etcd, services) that must be cleaned up reliably.

#### Process Monitoring Scripts

**`go/test/endtoend/run_in_test.sh`** - Monitors parent process and terminates child if parent dies:

```bash
# Usage in tests (example)
cmd := exec.CommandContext(ctx, "go/test/endtoend/run_in_test.sh",
    "postgres", "-D", dataDir)
cmd.Env = append(os.Environ(),
    "MULTIGRES_TEST_PARENT_PID="+strconv.Itoa(os.Getpid()),
    "MULTIGRES_TESTDATA_DIR="+testDataDir,
)
```

Features:

- Monitors parent PID and terminates child if parent dies
- Detects orphan processes via testdata directory deletion
- Graceful shutdown: SIGTERM first, waits 5 seconds, then SIGKILL
- Propagates signals (SIGTERM, SIGINT) to child

**`go/test/endtoend/run_command_if_parent_dies.sh`** - Runs cleanup commands when parent dies:

```bash
# Example: Stop PostgreSQL when test dies
run_command_if_parent_dies.sh pg_ctl stop -D /path/to/data -m fast
```

#### Environment Variables

- `MULTIGRES_TEST_PARENT_PID` - Process ID to monitor (defaults to parent)
- `MULTIGRES_TESTDATA_DIR` - If set and directory deleted, trigger cleanup

#### Cleanup Pattern

**Always register cleanup with `t.Cleanup()`:**

```go
func TestWithSubprocess(t *testing.T) {
    // Start subprocess
    cmd := exec.CommandContext(t.Context(), "some-binary", "args...")
    require.NoError(t, cmd.Start())

    // Register cleanup (runs in LIFO order)
    t.Cleanup(func() {
        terminateProcess(cmd.Process)
    })

    // Test code...
}

// Helper for graceful termination
func terminateProcess(proc *os.Process) {
    if proc == nil {
        return
    }

    // Try SIGTERM first
    proc.Signal(syscall.SIGTERM)

    // Wait up to 5 seconds
    done := make(chan error, 1)
    go func() {
        _, err := proc.Wait()
        done <- err
    }()

    select {
    case <-done:
        return  // Process exited gracefully
    case <-time.After(5 * time.Second):
        // Force kill
        proc.Kill()
    }
}
```

### Shared Setup Patterns

For tests that share expensive setup:

```go
package multipooler

import (
    "github.com/multigres/multigres/go/test/endtoend/shardsetup"
)

// One-time setup for entire test suite
var sharedSetup *shardsetup.ShardSetup

func getSharedTestSetup(t *testing.T) *shardsetup.ShardSetup {
    if sharedSetup == nil {
        setup, cleanup := shardsetup.NewIsolated(t, /* options */)
        t.Cleanup(cleanup)
        sharedSetup = setup
    }
    return sharedSetup
}

// Per-test cleanup and reset
func setupPoolerTest(t *testing.T, setup *shardsetup.ShardSetup, opts ...TestOption) {
    config := &testConfig{}
    for _, opt := range opts {
        opt(config)
    }

    // Register per-test cleanup in LIFO order
    if len(config.tablesToDrop) > 0 {
        t.Cleanup(func() {
            for _, table := range config.tablesToDrop {
                dropTable(t, setup, table)
            }
        })
    }

    if len(config.gucsToReset) > 0 {
        t.Cleanup(func() {
            for _, guc := range config.gucsToReset {
                resetGuc(t, setup, guc)
            }
        })
    }
}

// Test options for flexibility
type TestOption func(*testConfig)

func WithDropTables(tables ...string) TestOption {
    return func(c *testConfig) {
        c.tablesToDrop = tables
    }
}

func WithResetGuc(gucs ...string) TestOption {
    return func(c *testConfig) {
        c.gucsToReset = gucs
    }
}

// Usage in test
func TestMyFeature(t *testing.T) {
    setup := getSharedTestSetup(t)
    setupPoolerTest(t, setup,
        WithDropTables("test_table_1", "test_table_2"),
        WithResetGuc("synchronous_commit", "max_wal_senders"),
    )

    // Test code...
}
```

---

## Common Patterns

### Waiting and Polling

**Pattern**: Always use `require.Eventually()` with clear timeout and poll interval

```go
// Constants for consistency
const (
    integrationTimeout = 30 * time.Second
    integrationPoll    = 1 * time.Second

    unitTestTimeout = 5 * time.Second
    unitTestPoll    = 10 * time.Millisecond
)

// Complex condition with multiple checks
require.Eventually(t, func() bool {
    allReady := true

    for name, instance := range setup.Services {
        if !instance.IsReady() {
            t.Logf("Service %s not ready yet", name)
            allReady = false
        }
    }

    return allReady
}, integrationTimeout, integrationPoll,
    "All services should be ready within %v", integrationTimeout)
```

### Context Management

```go
// GOOD: Use t.Context() as parent
func TestWithContext(t *testing.T) {
    ctx := utils.WithTimeout(t, 5*time.Second)
    result, err := DoSomething(ctx)
    require.NoError(t, err)
}

// GOOD: Exception for cleanup where context is cancelled
func TestWithCleanup(t *testing.T) {
    ctx := utils.WithTimeout(t, 5*time.Second)

    // Start something
    service := StartService(ctx)

    t.Cleanup(func() {
        // t.Context() is cancelled here, need fresh context
        shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
        defer cancel()
        service.Shutdown(shutdownCtx)
    })
}

// BAD: Using context.Background() in test
func TestBad(t *testing.T) {
    ctx := context.Background()  // ❌ Wrong!
    // Should use: ctx := utils.WithShortDeadline(t)
}
```

### Error Checking

```go
// GOOD: Check specific error
err := DoSomething()
require.Error(t, err)
require.True(t, mterrors.IsError(err, "MT05001"),
    "Expected MT05001 error, got: %v", err)

// GOOD: Check error type with errors.Is
err := DoSomething()
require.Error(t, err)
require.ErrorIs(t, err, ErrNotFound,
    "Expected ErrNotFound, got: %v", err)

// GOOD: Check error code
err := DoSomething()
require.Error(t, err)
require.Equal(t, codes.NotFound, mterrors.Code(err),
    "Expected NotFound code, got: %v", mterrors.Code(err))

// BAD: Weak assertion
err := DoSomething()
require.Error(t, err)  // ❌ Too vague - what error?

// BAD: Only checking success
require.NoError(t, err)  // ❌ But what if unexpected error occurs?
```

### Process Cleanup

```go
// GOOD: Register cleanup immediately after starting process
func TestWithProcess(t *testing.T) {
    cmd := exec.CommandContext(t.Context(), "postgres", "-D", dataDir)
    require.NoError(t, cmd.Start())

    // Register cleanup right away
    t.Cleanup(func() {
        terminateProcess(cmd.Process)
    })

    // Test continues...
}

// Helper for graceful termination
func terminateProcess(proc *os.Process) {
    if proc == nil {
        return
    }

    // SIGTERM first (graceful)
    proc.Signal(syscall.SIGTERM)

    // Wait with timeout
    done := make(chan error, 1)
    go func() {
        _, err := proc.Wait()
        done <- err
    }()

    select {
    case <-done:
        return  // Exited gracefully
    case <-time.After(5 * time.Second):
        // Force kill if necessary
        proc.Kill()
        proc.Wait()
    }
}
```

---

## Advanced Patterns

### Instrumenting Dependencies with Wrappers

When you need to verify async operations like reconnections or retries, avoid `time.Sleep()` by using the **decorator pattern** to wrap dependencies and track behavior.

**Pattern:** Create a test wrapper that embeds the real dependency and intercepts specific methods:

```go
type countingStore struct {
    topoclient.Store
    watchCalls atomic.Int32
}

func (s *countingStore) ConnForCell(ctx context.Context, cell string) (topoclient.Conn, error) {
    conn, err := s.Store.ConnForCell(ctx, cell)
    if err != nil {
        return nil, err
    }
    return &countingConn{Conn: conn, store: s}, nil
}

type countingConn struct {
    topoclient.Conn
    store *countingStore
}

func (c *countingConn) Watch(ctx context.Context, path string) (*topoclient.WatchData, <-chan *topoclient.WatchData, error) {
    c.store.watchCalls.Add(1)  // Track the call
    return c.Conn.Watch(ctx, path)
}
```

**Use with `require.Eventually()` instead of `time.Sleep()`:**

```go
countingStore := newCountingStore(realStore)
service := NewService(ctx, countingStore, ...)

// Instead of: time.Sleep(100 * time.Millisecond)
// Do this:
require.Eventually(t, func() bool {
    return countingStore.WatchCallCount() == 2  // Verify reconnection
}, testTimeout, testPollInterval,
    "Should reconnect after watch closure")
```

**Benefits:** No production code changes, deterministic testing, verifies actual behavior

**When to use:** Verifying retry logic, reconnections, callback invocations, operation ordering

**See:** `go/services/multigateway/discovery_test.go` for complete working example

---

## Anti-Patterns to Avoid

### ❌ Using time.Sleep() Instead of require.Eventually()

```go
// BAD: Arbitrary wait for completion
func TestBad(t *testing.T) {
    startAsync()
    time.Sleep(2 * time.Second)  // ❌ How long is enough? Might be too short or too long
    require.True(t, isComplete())
}

// GOOD: Wait only as long as needed
func TestGood(t *testing.T) {
    startAsync()
    require.Eventually(t, func() bool {
        return isComplete()
    }, 5*time.Second, 10*time.Millisecond,
        "Operation should complete")
}
```

**When you encounter `time.Sleep()` in tests:**

1. **Flag it as a potential issue** - `time.Sleep()` is often a code smell indicating flaky test design
2. **Understand why it's there** - Read the surrounding code and comments
3. **Evaluate if it can be replaced**:
   - **If waiting for a checkable condition** → Replace with `require.Eventually()`
   - **If ensuring ordering of async events with no checkable state** → May be necessary but should be documented
   - **If you're unsure** → Ask the user: "I found a `time.Sleep()` at line X. It appears to be waiting for [reason]. Can this be replaced with `require.Eventually()` checking for [condition], or is this sleep necessary for event ordering?"

Example where `time.Sleep()` might be acceptable (but must be kept short and well-documented):

```go
// Trigger async event
factory.CloseWatches("test-cell", "poolers")

// Give the watch goroutine time to detect closure before proceeding
// NOTE: No checkable state available to use require.Eventually()
// TODO: Consider exposing internal state or adding synchronization
time.Sleep(100 * time.Millisecond)

// Continue with test that depends on closure being detected
addNewPooler()
```

**Never blindly remove `time.Sleep()` without understanding its purpose** - this can introduce race conditions and make tests flaky.

**Important**: `select` with `time.After()` for timeouts is **correct** and should NOT be replaced:

```go
// GOOD: Timing an operation that doesn't accept context
done := make(chan struct{})
go func() {
    service.Stop()  // No context parameter
    close(done)
}()

select {
case <-done:
    // Success
case <-time.After(5 * time.Second):
    t.Fatal("Operation took too long")
}
```

**When to use each pattern:**

- **`select` with `time.After()`** - For timing operations that don't accept context (like `Stop()`, `Close()`, or channel operations)
- **`utils.WithTimeout()`** - For passing timeout context to operations that accept context:
  ```go
  ctx := utils.WithTimeout(t, 5*time.Second)
  result, err := client.FetchData(ctx)  // Context passed to operation
  ```
- **`require.Eventually()`** - For polling until a condition becomes true:
  ```go
  require.Eventually(t, func() bool {
      return service.IsReady()
  }, 5*time.Second, 100*time.Millisecond)
  ```

The anti-pattern is using `time.Sleep()` to wait for async operations to complete.

### ❌ Using context.Background() Instead of t.Context()

```go
// BAD: Loses test cancellation
func TestBad(t *testing.T) {
    ctx := context.Background()  // ❌ Wrong!
    DoSomething(ctx)
}

// GOOD: Proper context from test
func TestGood(t *testing.T) {
    ctx := utils.WithShortDeadline(t)
    DoSomething(ctx)
}
```

### ❌ Weak Error Assertions

```go
// BAD: Could fail for wrong reason
func TestBad(t *testing.T) {
    err := DoSomething()
    require.Error(t, err)  // ❌ Any error passes!
}

// GOOD: Check specific error
func TestGood(t *testing.T) {
    err := DoSomething()
    require.Error(t, err)
    require.ErrorIs(t, err, ErrExpected)
}
```

### ❌ Forgetting Process Cleanup

```go
// BAD: Process may leak
func TestBad(t *testing.T) {
    cmd := exec.Command("postgres", "-D", dataDir)
    cmd.Start()  // ❌ No cleanup registered!

    // Test code...
}

// GOOD: Cleanup registered immediately
func TestGood(t *testing.T) {
    cmd := exec.Command("postgres", "-D", dataDir)
    cmd.Start()
    t.Cleanup(func() {
        terminateProcess(cmd.Process)
    })

    // Test code...
}
```

### ❌ Hardcoded Ports

```go
// BAD: Port may be in use
func TestBad(t *testing.T) {
    listener, _ := net.Listen("tcp", ":8080")  // ❌ Conflict!
}

// GOOD: Dynamic port allocation
func TestGood(t *testing.T) {
    port := utils.GetFreePort(t)
    listener, _ := net.Listen("tcp", fmt.Sprintf(":%d", port))
}
```

### ❌ Flaky Timing Assumptions

```go
// BAD: Race condition
func TestBad(t *testing.T) {
    go updateValue()
    time.Sleep(100 * time.Millisecond)  // ❌ Might not be enough!
    require.Equal(t, expected, getValue())
}

// GOOD: Wait for actual condition
func TestGood(t *testing.T) {
    go updateValue()
    require.Eventually(t, func() bool {
        return getValue() == expected
    }, 1*time.Second, 10*time.Millisecond,
        "Value should be updated")
}
```

---

## Code Templates

### Template: Basic Unit Test

```go
package mypackage

import (
    "testing"

    "github.com/stretchr/testify/require"
    "github.com/multigres/multigres/go/test/utils"
)

func TestFeatureName(t *testing.T) {
    // Setup
    ctx := utils.WithShortDeadline(t)
    input := setupTestData(t)

    // Execute
    result, err := FunctionUnderTest(ctx, input)

    // Assert
    require.NoError(t, err)
    require.Equal(t, expectedValue, result)
}

func TestFeatureName_ErrorCase(t *testing.T) {
    ctx := utils.WithShortDeadline(t)

    // Execute with invalid input
    _, err := FunctionUnderTest(ctx, invalidInput)

    // Assert specific error
    require.Error(t, err)
    require.ErrorIs(t, err, ErrExpected)
}
```

### Template: Unit Test with Mock gRPC Service

```go
package mypackage

import (
    "context"
    "testing"

    "github.com/stretchr/testify/require"
    "google.golang.org/grpc"
    "google.golang.org/grpc/test/bufconn"

    "github.com/multigres/multigres/go/pb/multipoolermanagerdatapb"
    "github.com/multigres/multigres/go/test/utils"
)

type mockService struct {
    multipoolermanagerdatapb.UnimplementedMultiPoolerManagerServer

    statusCalls int
    statusResp  *multipoolermanagerdatapb.StatusResponse
    statusErr   error
}

func (m *mockService) Status(
    ctx context.Context,
    req *multipoolermanagerdatapb.StatusRequest,
) (*multipoolermanagerdatapb.StatusResponse, error) {
    m.statusCalls++
    return m.statusResp, m.statusErr
}

func TestWithMock(t *testing.T) {
    ctx := utils.WithShortDeadline(t)

    // Setup mock
    mock := &mockService{
        statusResp: &multipoolermanagerdatapb.StatusResponse{
            // Configure response
        },
    }

    // Setup bufconn server
    lis := bufconn.Listen(1024 * 1024)
    defer lis.Close()

    server := grpc.NewServer()
    multipoolermanagerdatapb.RegisterMultiPoolerManagerServer(server, mock)
    go server.Serve(lis)
    defer server.Stop()

    // Create client
    conn, err := grpc.DialContext(ctx, "bufnet",
        grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
            return lis.Dial()
        }),
        grpc.WithInsecure(),
    )
    require.NoError(t, err)
    defer conn.Close()

    client := multipoolermanagerdatapb.NewMultiPoolerManagerClient(conn)

    // Execute test
    resp, err := client.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})

    // Assert
    require.NoError(t, err)
    require.NotNil(t, resp)
    require.Equal(t, 1, mock.statusCalls)
}
```

### Template: Integration Test

```go
package endtoend

import (
    "testing"

    "github.com/stretchr/testify/require"
    "github.com/multigres/multigres/go/test/utils"
    "github.com/multigres/multigres/go/test/endtoend/shardsetup"
)

func TestIntegrationFeature(t *testing.T) {
    // Skip appropriately
    if testing.Short() {
        t.Skip("Skipping integration test (short mode)")
    }
    if utils.ShouldSkipRealPostgres() {
        t.Skip("Skipping integration test (no PostgreSQL binaries)")
    }

    // Setup cluster
    setup, cleanup := shardsetup.NewIsolated(t,
        shardsetup.WithMultipoolerCount(3),
    )
    defer cleanup()

    // Wait for cluster ready
    require.Eventually(t, func() bool {
        return checkClusterReady(t, setup)
    }, 30*time.Second, 1*time.Second,
        "Cluster should be ready")

    // Execute test scenario
    t.Run("scenario 1", func(t *testing.T) {
        // Test code
    })

    t.Run("scenario 2", func(t *testing.T) {
        // Test code
    })
}

func checkClusterReady(t *testing.T, setup *shardsetup.ShardSetup) bool {
    // Check all services are ready
    for _, inst := range setup.Multipoolers {
        client, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
        if err != nil {
            return false
        }
        defer client.Close()

        ctx := utils.WithTimeout(t, 5*time.Second)
        _, err = client.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
        if err != nil {
            return false
        }
    }
    return true
}
```

### Template: require.Eventually() Pattern

```go
// Simple condition
require.Eventually(t, func() bool {
    return checkCondition()
}, 5*time.Second, 10*time.Millisecond,
    "Condition should be met")

// Complex condition with logging
require.Eventually(t, func() bool {
    ready := true

    for name, service := range services {
        if !service.IsReady() {
            t.Logf("Service %s not ready yet", name)
            ready = false
        }
    }

    return ready
}, 30*time.Second, 1*time.Second,
    "All services should be ready")

// With state checking
require.Eventually(t, func() bool {
    ctx := utils.WithTimeout(t, 2*time.Second)
    status, err := client.GetStatus(ctx)
    if err != nil {
        t.Logf("Failed to get status: %v", err)
        return false
    }

    return status.State == expectedState
}, 10*time.Second, 100*time.Millisecond,
    "Service should reach %v state", expectedState)
```

### Template: Subprocess with Cleanup

```go
import (
    "os"
    "os/exec"
    "syscall"
    "time"
)

func TestWithSubprocess(t *testing.T) {
    ctx := utils.WithTimeout(t, 60*time.Second)

    // Start subprocess
    cmd := exec.CommandContext(ctx, "postgres",
        "-D", dataDir,
        "-c", "port="+strconv.Itoa(port),
    )

    cmd.Env = append(os.Environ(),
        "MULTIGRES_TEST_PARENT_PID="+strconv.Itoa(os.Getpid()),
    )

    require.NoError(t, cmd.Start())

    // Register cleanup immediately
    t.Cleanup(func() {
        terminateProcess(cmd.Process)
    })

    // Wait for process ready
    require.Eventually(t, func() bool {
        return checkProcessReady()
    }, 10*time.Second, 100*time.Millisecond,
        "Process should be ready")

    // Test code...
}

func terminateProcess(proc *os.Process) {
    if proc == nil {
        return
    }

    // Try graceful shutdown
    proc.Signal(syscall.SIGTERM)

    // Wait with timeout
    done := make(chan error, 1)
    go func() {
        _, err := proc.Wait()
        done <- err
    }()

    select {
    case <-done:
        return
    case <-time.After(5 * time.Second):
        proc.Kill()
        proc.Wait()
    }
}
```

---

## Coverage Goals

High test coverage is required across these critical areas:

### Query Path

- Protocol handling (startup, authentication, query/response cycles)
- Connection pooling (acquire, release, reuse, cleanup)
- Query routing (parse, analyze, route to correct backend)
- Error handling and recovery

### Management Plane

- Multipooler manager operations (initialize, start, stop)
- State transitions (uninitialized → initialized → running)
- Configuration updates and reloads
- Health checks and status reporting

### Orchestration

- Failover scenarios (primary failure, replica promotion)
- Promotion and demotion operations
- Consensus algorithms and term management
- Recovery from split-brain or network partitions

### Error Paths

- Test failures, not just successes
- Invalid inputs and edge cases
- Resource exhaustion scenarios
- Network failures and timeouts
- Process crashes and restarts

---

## Related Skills and Commands

### Running Tests

- `/mt-dev unit <package>` - Run unit tests for a specific package
- `/mt-dev integration <package>` - Run integration tests for a specific package
- `/mt-dev unit all` - Run all unit tests
- `/mt-dev integration all` - Run all integration tests

### Local Cluster Management

- `/mt-local-cluster` - Manage local cluster for manual testing and debugging
- Use this to set up a local environment for testing changes

### Build Commands

```bash
make test           # Run all tests
make test-short     # Skip integration tests (fast)
make test-race      # Run with race detector
```

### Debugging Failed Tests

1. Run test with `-v` flag for verbose output
2. Use `/mt-local-cluster` to reproduce issue manually
3. Add logging with `t.Logf()` in test code
4. Check for race conditions with `-race` flag
5. Run multiple times with `-count=10` to detect flakiness

---

## Best Practices Summary

✅ **DO:**

- Use `require.Eventually()` to wait for async operations
- Derive contexts from `t.Context()` using `utils.WithTimeout()`
- Check specific error types with `require.ErrorIs()`
- Register cleanup with `t.Cleanup()` immediately after resource allocation
- Use `utils.GetFreePort()` for dynamic port allocation
- Use `bufconn.Listener` for fast gRPC unit tests
- Test error paths and edge cases thoroughly
- Aim for high code coverage

❌ **DON'T:**

- Use `time.Sleep()` for waiting (use `require.Eventually()`)
- Use `context.Background()` in test functions (use `t.Context()`)
- Make weak error assertions (check specific errors)
- Hardcode ports (use port allocator)
- Forget to register cleanup for spawned processes
- Make flaky timing assumptions
- Only test happy paths (test failures too)

---

## Questions?

For more information:

- See `go/test/utils/` for test utilities
- See `go/test/endtoend/` for integration test examples
- Use `/mt-dev` skill for running tests
- Use `/mt-local-cluster` skill for local cluster management
