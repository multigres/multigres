# End-to-End Tests

Integration tests that start real multigres components (multigateway, multipooler, pgctld,
multiorch) and PostgreSQL, then exercise them end-to-end.

## Prerequisites

### 1. Build the binaries

```bash
make build
```

This must be run from the repository root before running integration tests. It compiles all service
binaries into `bin/`.

### 2. PostgreSQL binaries on PATH

The tests look for `initdb`, `postgres`, `pg_ctl`, and `pg_isready`
via `exec.LookPath`. If any are missing the tests skip the
real-PostgreSQL paths and run in a degraded mode.

On macOS with Homebrew, PostgreSQL is typically **not** on the default
PATH. Prepend it explicitly:

```bash
# PostgreSQL 18 via Homebrew (adjust version as needed)
export PATH="/opt/homebrew/Cellar/postgresql@18/18.2/bin:$PATH"

# Verify
which initdb   # should print the Homebrew path
which postgres
```

On Linux, installing `postgresql` via the system package manager (apt,
yum, etc.) usually puts the binaries on PATH automatically.

## Running the Tests

### Run all packages

```bash
go test -skip TestPostgreSQLRegression ./go/test/endtoend/...
```

If you run into issues with failing tests because of allocating shared
memory segments, you can add `-p=1` to the test. Go runs test packages
in parallel by default. Each package spins up its own PostgreSQL cluster,
which allocates a shared memory segment via `shmget`. macOS has a hard
system limit of 32 shared memory IDs (`kern.sysv.shmmni: 32`). Running
all packages concurrently exhausts this pool, causing PostgreSQL's `initdb`
to fail with the misleading error:

```text
FATAL: could not create shared memory segment: No space left on device
```

The `-p=1` flag forces packages to run sequentially, keeping at most
one cluster bootstrapping at a time. This is not needed on Linux,
which has a much higher default limit.

**Why `-skip TestPostgreSQLRegression`?** The `pgregresstest` package
clones PostgreSQL source and builds it from scratch (~10+
minutes). Skip it during normal development; see
[`pgregresstest/README.md`](pgregresstest/README.md) for how to run it
explicitly.

### Run a specific package

```bash
go test ./go/test/endtoend/multipooler/...
```

Single packages don't need `-p=1` (only one cluster at a time).

### Run a specific test

```bash
go test -run TestFixReplication ./go/test/endtoend/multiorch/...
```

### Verbose output

Add `-v` to see individual test names and log lines as they run:

```bash
go test -v -run TestConnPool ./go/test/endtoend/multipooler/...
```

### Print full logs inline (useful for CI or debugging)

```bash
TEST_PRINT_LOGS=1 go test -v -run TestFailover ./go/test/endtoend/multiorch/...
```

### Run the query-serving tests against Multigres or Minigres

The `queryserving` tests run against either topology. Without
`MULTIGRES_E2E_TOPOLOGY` (or with any value other than `minigres`) they run
against a Multigres shard, as they always have. With
`MULTIGRES_E2E_TOPOLOGY=minigres` they run against a single Minigres process
(one pgctld and one `minigres`); tests that need Multiorch, replicas or several
gateways skip themselves with the reason, and everything else runs unchanged.
The harness starts and stops every process itself, so there is nothing to start
by hand: build the binaries and run the tests.

```bash
make build
scripts/portpool.sh start   # optional: shares free ports between packages

# Multigres (default)
MULTIGRES_PORT_POOL_ADDR=/tmp/multigres-port-pool.sock \
  go test ./go/test/endtoend/queryserving/

# Minigres
MULTIGRES_PORT_POOL_ADDR=/tmp/multigres-port-pool.sock MULTIGRES_E2E_TOPOLOGY=minigres \
  go test ./go/test/endtoend/queryserving/
```

`MULTIGRES_PORT_POOL_ADDR` makes the tests take ports from the port pool server,
which avoids port collisions when several packages run at once; without it each
package picks free ports itself. The tests that skip under Minigres, and why, are
listed in [docs/general/minigres.md](../../../docs/general/minigres.md#testing).
CI runs `queryserving` and `queryserving/pgparity` on both topologies on every
pull request.

## Test Packages

| Package            | What it tests                                      |
| ------------------ | -------------------------------------------------- |
| `endtoend`         | etcd topology, service discovery                   |
| `localprovisioner` | local cluster provisioning                         |
| `pgctld`           | PostgreSQL control daemon lifecycle                |
| `clustersetup`     | harness pieces shared by both topologies           |
| `shardsetup`       | Multigres shard harness                            |
| `minigressetup`    | Minigres harness (one pgctld, one `minigres`)      |
| `queryserving`     | query routing, SSL, sessions, transactions         |
| `multipooler`      | connection pooling, backup, replication, consensus |
| `multiorch`        | failover, bootstrap, replication repair            |
| `pgregresstest`    | PostgreSQL official regression suite (opt-in)      |

## Debugging Failures

When a test fails, logs are preserved in a temp directory. The test output will show:

```text
==== TEST LOGS PRESERVED ====
Logs available at: /tmp/shardsetup_test_XXXXXXXXXX
Set TEST_PRINT_LOGS=1 to print log contents
===========================
```

### Log directory structure

```text
/tmp/shardsetup_test_XXXXXXXXXX/
├── multigateway.log
├── temp-multiorch/
│   └── multiorch.log
├── pooler-1/
│   ├── multipooler.log
│   ├── pgctld.log
│   └── data/
│       ├── pg_data/postgresql.log
│       └── pgbackrest/logs/
├── pooler-2/  (same structure)
├── pooler-3/  (same structure)
├── etcd_data/
└── backup-repo/
```

### Quick error search

```bash
# Find all errors across all logs
grep -r "ERROR\|FATAL" /tmp/shardsetup_test_XXXXXXXXXX/

# Tail a specific component
tail -50 /tmp/shardsetup_test_XXXXXXXXXX/pooler-1/multipooler.log

# Pretty-print JSON logs
cat /tmp/shardsetup_test_XXXXXXXXXX/pooler-1/multipooler.log | jq '.'
```

### What to look at first

| Failure type              | Start here                                                         |
| ------------------------- | ------------------------------------------------------------------ |
| Connection / query errors | `pooler-*/multipooler.log`, `pooler-*/data/pg_data/postgresql.log` |
| Replication issues        | PostgreSQL logs on all poolers                                     |
| Failover / consensus      | `pooler-*/multipooler.log`, `temp-multiorch/multiorch.log`         |
| Backup / restore errors   | `pooler-*/data/pgbackrest/logs/`                                   |
| Gateway routing issues    | `multigateway.log`                                                 |
| Bootstrap failures        | `temp-multiorch/multiorch.log`, all pooler logs                    |

## Troubleshooting

### Tests skip PostgreSQL entirely

**Symptom**: Tests pass instantly, output says "skipping" or reports 0 real operations.

**Cause**: `initdb`/`postgres`/`pg_ctl`/`pg_isready` not found on PATH.

**Fix**: Prepend the PostgreSQL bin directory as shown in Prerequisites above.

### `No space left on device` during bootstrap

**Symptom**: `FATAL: could not create shared memory segment: No space left on device` in test output
or logs.

**Cause**: macOS shared memory segment limit (`kern.sysv.shmmni: 32`) exhausted. This happens when
packages run in parallel, or when a previous run crashed and left stale segments behind.

**Fix 1**: Use `-p=1` when running multiple packages on macOS.

**Fix 2**: Clean up stale segments from a previous crashed run:

```bash
# List shared memory segments owned by your user
ipcs -m | grep $(whoami)

# Remove them all
for id in $(ipcs -m | grep $(whoami) | awk '{print $2}'); do ipcrm -m "$id"; done
```

### Build failure before tests run

**Symptom**: `[build failed]` in the output before any test runs.

**Fix**: Run `make build` manually to see the full compiler error.
