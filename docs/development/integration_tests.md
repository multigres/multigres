# Integration Test Infrastructure

## Overview

Integration (end-to-end) tests start real Multigres binaries and a real
PostgreSQL, then exercise them over their public interfaces. They live under
`go/test/endtoend/` and are ordinary Go tests that skip themselves under
`go test -short`, which is how unit-test runs exclude them.

This guide describes how the test infrastructure is put together and how to
write a new test against it. For commands, prerequisites, log locations and
troubleshooting, see
[`go/test/endtoend/README.md`](../../go/test/endtoend/README.md). For the
PostgreSQL compatibility suites, see
[Testing strategy](../query_serving/testing_strategy.md).

## Where the code lives

```text
go/test/endtoend/
  clustersetup/    pieces shared by both topologies: Cluster interface,
                   RunTestMain, SharedSetupManager, process management
  shardsetup/      Multigres shard harness (ShardSetup)
  minigressetup/   Minigres harness (one pgctld, one minigres process)
  suiteutil/       helpers shared by the compatibility and benchmark suites
  pgbuilder/       builds a pinned PostgreSQL from source for the suites
  testconst/       shared constants
  run_in_test.sh, run_command_if_parent_dies.sh
                   orphan-protection wrappers (see below)
  <area>/          test packages: queryserving, multipooler, multiorch, ...
go/test/utils/     port allocation, leak checks, timeouts, config builders
go/test/s3mock/    in-process S3-compatible server for backup tests
go/cmd/portpoolserver/   port pool server binary
go/common/fakepgserver/  fake PostgreSQL server speaking the wire protocol
go/tools/fakepgdb/       fake PostgreSQL behind database/sql
go/tools/testpoll/       polling assertions (Never, WaitFor)
go/tools/prototest/      protobuf equality assertions
```

## How a test package is structured

Starting a cluster is expensive, so each test package starts its clusters once
and shares them between tests. Three pieces make this work.

### 1. `TestMain` and `RunTestMain`

`clustersetup.RunTestMain` (also exported as `shardsetup.RunTestMain`) puts
`bin/` on `PATH` so the service binaries, etcd and `run_in_test.sh` are found,
sets `MULTIGRES_TEST_PARENT_PID` for orphan detection, initializes telemetry,
and runs the tests. On an interrupt it reports failure so cleanup still runs.

### 2. `SharedSetupManager`

A `SharedSetupManager` holds one cluster. It builds the cluster with the
function you give it the first time a test calls `Get(t)`, and tears it down
from `TestMain`. A package that needs differently configured clusters declares
one manager per configuration; a cluster that no test asks for is never
started.

```go
var setupManager = shardsetup.NewSharedSetupManager(func(t *testing.T) *shardsetup.ShardSetup {
    return shardsetup.New(t,
        shardsetup.WithMultipoolerCount(2),
        shardsetup.WithMultigateway(),
    )
})

func TestMain(m *testing.M) {
    exitCode := shardsetup.RunTestMain(m)
    if exitCode != 0 {
        setupManager.DumpLogs()
    }
    setupManager.Cleanup()
    os.Exit(exitCode) //nolint:forbidigo // TestMain() is allowed to call os.Exit
}
```

`DumpLogs()` marks the run as failed, so `Cleanup()` keeps the temporary
directory with the service logs instead of deleting it.

### 3. `SetupTest` in every test

```go
func TestSomething(t *testing.T) {
    if testing.Short() {
        t.Skip("skipping integration test in short mode")
    }
    setup := setupManager.Get(t)
    setup.SetupTest(t)
    // ...
}
```

`SetupTest` checks that the shared cluster is in its clean state before the
test and registers a cleanup that restores it afterwards. Options change the
starting state:

| Option                    | Effect                                                          |
| ------------------------- | --------------------------------------------------------------- |
| `WithoutReplication()`    | Breaks replication first, for tests that set it up from scratch |
| `WithPausedReplication()` | Pauses WAL replay on the standbys                               |
| `WithResetGuc(names...)`  | Saves and restores additional GUCs around the test              |

## The Multigres harness: `shardsetup`

`shardsetup.New(t, opts...)` starts etcd, the requested number of
pgctld-plus-multipooler pairs, and optionally Multiorch, a multigateway and
Multiadmin. Defaults are two poolers, no Multiorch, database `postgres`, cell
`test-cell` and durability policy `AT_LEAST_2`. It fails the test if the
Multigres binaries or the PostgreSQL binaries are not on `PATH`.

Commonly used options (all in `shardsetup/setup.go`):

| Option                                                             | Purpose                                       |
| ------------------------------------------------------------------ | --------------------------------------------- |
| `WithMultipoolerCount(n)`, `WithMultiorchCount(n)`                 | Size of the shard and number of orchestrators |
| `WithMultigateway()`, `WithMultigatewayExtraArgs(...)`             | Start a gateway, optionally with extra flags  |
| `WithMultipoolerExtraArgs(...)`                                    | Extra flags for every pooler                  |
| `WithMultigatewayTLS()`, `WithMultigatewayRequireSSL()`            | TLS on the client listener                    |
| `WithMultigatewayReplicaPort()`                                    | Enable the replica-reads port                 |
| `WithMultiadmin()`                                                 | Start Multiadmin                              |
| `WithS3Backup(bucket, region, endpoint)`, `WithBackupEncryption()` | Backup configuration                          |
| `WithDurabilityPolicy(policy)`                                     | Bootstrap durability policy                   |
| `WithoutInitialization()`, `WithDeferredMultipoolerStart()`        | Start processes without the usual bootstrap   |
| `WithInitdbSQLFiles(...)`, `WithPgInitdbArgs(...)`                 | Customize `initdb`                            |

Pooler instances are named by index: `primary`, `standby`, `standby2`, and so
on. `ShardSetup` exposes them (`GetMultipoolerInstance`, `GetPrimary`,
`GetStandbys`, `NewPrimaryClient`) along with fault-injection helpers
(`KillPostgres`, `StopPostgres`, `ShutdownPostgres`, `FreezeMultipooler`) and
Multiorch controls (`DisableRecovery`, `EnableRecovery`,
`TriggerRecoveryOnce`, `RequireRecovery`).

### Destructive tests: `NewIsolated`

A test that cannot leave the cluster in a restorable state, such as one that
kills the primary for good, should not use a shared cluster:

```go
setup, cleanup := shardsetup.NewIsolated(t, shardsetup.WithMultipoolerCount(3))
defer cleanup()
```

The cleanup function dumps logs if the test failed and removes the cluster.

## Two topologies: Multigres and Minigres

The query-serving tests run against either a Multigres shard or a single
Minigres process. Both harnesses implement `clustersetup.Cluster`, which exposes
what those tests need: `ClientPort()`, `ComparisonTargets(t)`, `SetupTest`,
`WaitForQueryServing`, the PostgreSQL port and socket directory, and the log
file paths.

The topology is chosen by the environment variable `MULTIGRES_E2E_TOPOLOGY`:
`minigres` selects Minigres, and anything else selects Multigres. A package
supports both by branching inside its setup function:

```go
var setupManager = clustersetup.NewSharedSetupManager(func(t *testing.T) shardsetup.Cluster {
    if clustersetup.IsMinigres() {
        return minigressetup.New(t)
    }
    return shardsetup.New(t, shardsetup.WithMultipoolerCount(2), shardsetup.WithMultigateway())
})
```

A test that needs something only Multigres has (Multiorch, replicas, several
gateways) calls `clustersetup.RequireMultigresTopology(t, reason)`, which skips
it under Minigres with the reason.

`ComparisonTargets(t)` returns a direct PostgreSQL target and the proxy target,
so a test can loop over both and assert that the gateway behaves like
PostgreSQL.

## Port allocation and the port pool server

Every process in a test cluster needs free TCP ports. `utils.GetFreePort(t)`
hands them out and releases them when the test ends.

Within one test binary a process-local cache prevents duplicates. Across test
binaries running in parallel (the default for `go test ./...`) that is not
enough: two binaries can be given the same port by the OS. The port pool server
(`bin/portpoolserver`, protocol in `go/test/utils/poolserver`) is a small
process that keeps one registry for all of them.

- With `MULTIGRES_PORT_POOL_ADDR` set to the server's Unix socket, `GetFreePort`
  allocates through the server and returns the port to it at test cleanup.
- Without it, `GetFreePort` prints a warning once and falls back to the
  process-local allocator. It also falls back if it cannot connect.

`scripts/portpool.sh start|stop|status` manages a local server on
`/tmp/multigres-port-pool.sock`. `make test` and the integration CI workflow
start one for the duration of the run.

## Process lifetime and orphan protection

A killed or crashed test run must not leave PostgreSQL and service processes
behind. Two environment variables drive the cleanup: `MULTIGRES_TEST_PARENT_PID`
(set by `RunTestMain` to the test process) and `MULTIGRES_TESTDATA_DIR` (set by
the harness on each process it starts, pointing into the cluster's temporary
directory). A process is cleaned up when the parent PID disappears or the
directory is deleted.

- **Multigres services** watch for both conditions themselves through servenv
  (`go/common/servenv/servenv_unix.go`).
- **PostgreSQL**: when orphan detection is enabled, pgctld starts
  `run_command_if_parent_dies.sh` beside PostgreSQL, which runs `pg_ctl stop`
  when either condition occurs.
- **etcd** is started through `utils.CommandWithOrphanProtection`, which wraps
  it in `run_in_test.sh`. The wrapper sends SIGTERM, then SIGKILL after five
  seconds.

`utils.BaseTestEnv()` builds the environment for those processes. It strips
variables that would change how `initdb` sets up the cluster and forces the
`postgres` superuser, so tests behave the same inside images that preset those
variables.

## Test utilities

| Helper                                                          | Use                                                               |
| --------------------------------------------------------------- | ----------------------------------------------------------------- |
| `utils.GetFreePort(t)`                                          | Allocate a TCP port for the duration of the test                  |
| `utils.WithShortDeadline(t)`, `utils.WithTimeout(t, d)`         | Contexts bounded for a test                                       |
| `utils.ScaleTimeout(d)`                                         | Stretch a wait budget when running under coverage instrumentation |
| `utils.LeakCheckContext(t)`, `utils.EnsureNoLeaks(t)`           | Fail a passing test that leaks goroutines                         |
| `utils.RequirePgError(t, err, code)`                            | Assert a PostgreSQL error with a given SQLSTATE                   |
| `utils.ScrapeMetrics`, `ParseMetrics`, `AssertMetricValue`, ... | Read and assert on a service's `/metrics` output                  |
| `utils.NewTestConfig()`, `utils.WritePgBackrestConfig(...)`     | Build backup configuration for tests                              |
| `testpoll.WaitFor`, `testpoll.Never`                            | Polling assertions; `testpoll.Never` replaces testify's `Never`   |
| `prototest.AssertEqual`, `RequireEqual`, `RequireElementsMatch` | Compare protobuf messages                                         |

## Fakes for unit tests

Unit tests that need a PostgreSQL endpoint but not a real server use a fake:

- **`go/common/fakepgserver`** listens on a real socket and speaks the
  PostgreSQL wire protocol through `go/common/pgprotocol/server`. Register
  results with `AddQuery`, `AddQueryPattern`, `AddRejectedQuery` or an ordered
  list (`OrderMatters`, `AddExpectedExecuteFetch`), connect with
  `ClientConfig()`, and verify with `GetQueryCalledNum`, `QueryLog` or
  `VerifyAllExecutedOrFail`. The multipooler's connection-pool tests use it.
- **`go/tools/fakepgdb`** offers the same style of API behind `database/sql`.
  See [`go/tools/fakepgdb/README.md`](../../go/tools/fakepgdb/README.md).
- **`go/test/s3mock`** is an in-process S3-compatible server
  (`s3mock.NewServer(0)`, `CreateBucket`, `Endpoint()`), used for backup tests
  together with `shardsetup.WithS3Backup`. Set `MULTIGRES_TEST_LOG_S3MOCK=1` to
  log its requests.

## How to add an integration test

1. **Pick a package.** Add the test to the existing package for its area when
   the cluster configuration it needs already exists there. Add a new
   `SharedSetupManager` to that package when it needs a different
   configuration, and remember to call its `DumpLogs()` and `Cleanup()` in
   `TestMain`.
2. **Skip in short mode** at the top of the test.
3. **Get the shared cluster and call `SetupTest(t)`**, or use `NewIsolated` for
   a destructive test.
4. **Keep the cluster clean.** Anything the test changes outside what
   `SetupTest` restores must be undone in a `t.Cleanup`.
5. **Use bounded waits.** Prefer `require.Eventually` or `testpoll.WaitFor`
   with `utils.ScaleTimeout` over fixed sleeps.
6. **Run it** with the port pool server:

   ```text
   /mt-dev integration <package> <TestName>
   ```

## Pitfalls

- **Build first.** The tests run the binaries in `bin/`; a stale build tests old
  code. `make build` before running.
- **Do not stop PostgreSQL through pgctld directly.** The multipooler's monitor
  restarts it immediately. Use `ShardSetup.StopPostgres`, which disables
  restarts and returns a resume function. A lint rule enforces this.
- **Do not use testify's `Never`.** It can leak a goroutine into the next test.
  A lint rule requires `testpoll.Never`.
- **Shared clusters carry state between tests.** A test that passes alone but
  fails in the package usually depends on, or leaves behind, state on the shared
  cluster.
- **macOS shared memory limits** can make `initdb` fail when many packages run
  in parallel; see the README linked above.
