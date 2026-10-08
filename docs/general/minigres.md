# Minigres

Minigres runs a multigateway and a multipooler in **one process**, for a
database served by a single pooler with no replicas and no shards. It is built
as the `minigres` binary (`go/cmd/minigres`).

Minigres reuses the Multigres code. Both halves run the same query-serving code
as their Multigres binaries; what changes is who owns the process and who makes
the pooler writable. This document covers what exists today and is updated as
Minigres evolves.

## What runs

- **One process** with one servenv, one gRPC server, one HTTP server and one
  etcd connection, shared by both halves.
- **The multipooler as a static leader.** It is the only pooler of its shard, so
  it leads without consensus and without Multiorch.
- **The multigateway reaches the pooler the Multigres way:** it discovers the
  pooler through etcd, follows its gRPC health stream, and dials it over TCP,
  even though the pooler is in the same process. Making these in-process is
  planned work.
- **pgctld runs as its own process**, as in Multigres.

Many Minigres processes can share one etcd and one Multiadmin.

## Running it

There is no provisioner support yet, so Minigres is started by hand:

```bash
make build
export PATH=$PWD/bin:$PATH # restore_command calls pgctld

# 1. etcd, which may be shared
etcd --listen-client-urls http://localhost:35379 \
  --advertise-client-urls http://localhost:35379 ...

# 2. Cluster metadata, once per database
multigres createclustermetadata --global-topo-address localhost:35379 \
  --global-topo-root /minigres/global --cells zone1 \
  --durability-policy AT_LEAST_2 --backup-location /path/backups

# 3. pgctld. Do not run `pgctld init`: the pooler initializes the data directory
export PGDATA=/path/pooler/pg_data POSTGRES_PASSWORD_FILE=/path/pooler/postgres-password
pgctld server --pooler-dir /path/pooler --pg-port 45432 --grpc-port 35470 \
  --http-port 35400 --pg-user postgres --pg-database postgres

# 4. Minigres
minigres --pg-port 35432 --grpc-port 35170 --http-port 35100 \
  --topo-global-server-addresses localhost:35379 --topo-global-root /minigres/global \
  --cell zone1 --service-id db1 --pgctld-addr localhost:35470 --hostname localhost

psql "host=localhost port=35432 user=postgres dbname=postgres"
```

Things to know:

- **Ports:** the example mirrors the local Multigres cluster's defaults, shifted
  by 20000 (gateway `15xxx` to `35xxx`, PostgreSQL `25432` to `45432`), so a
  Minigres and a local Multigres cluster can run side by side.

- **Don't prepare the data directory with `pgctld init`.** The pooler bootstraps
  it itself, including the Multigres schema and the first backup. A directory
  from `pgctld init` has no Multigres schema.
- **The durability policy** must currently be `AT_LEAST_2`, because
  `createclustermetadata` has no single-pooler policy and rejects `none`. The
  static leader does not use the policy today (see below).
- **Status pages:** `/` is an index linking to `/multigateway` and
  `/multipooler`.
- **Configuration files are not supported yet.** Settings come from flags and
  `MT_*` environment variables. A config file (`--config-file`, or one found
  through `--config-name` and `--config-path`) makes `minigres` refuse to start:
  each half keeps its own settings registry, because both define keys such as
  `pg-port` with different meanings, so component settings in a file would
  otherwise be silently ignored. Separate configuration namespaces for the two
  halves are planned.

### Flags

| Flag                                                        | Behavior in Minigres                                                                                                    |
| ----------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `--pg-port`                                                 | The **client-facing** port (the gateway's). The pooler's Postgres port is not a flag; the pooler adopts it from pgctld  |
| `--cell`, `--service-id`, `--enable-slot-based-replication` | Given once and applied to both halves. Without a service ID, one is generated and shared by the process and both halves |
| `--table-group`, `--shard`                                  | Default to `default` and `0-inf`, the only pair the gateway routes to                                                   |
| servenv, gRPC server and topology flags                     | Defined once for the process                                                                                            |
| `--service-map grpc-consensus`                              | Has no effect: the consensus service is never registered (see below)                                                    |

A flag that both halves define is rejected at startup, so a new flag that
collides fails loudly instead of silently shadowing another.

## How it is wired

`servenv.ProcessResources` (`go/common/servenv/process_resources.go`) holds what
a process has only one of: the servenv and the gRPC server. Every binary's
`main` creates them and hands them to the halves it runs:
`multigateway.NewMultigateway` and `multipooler.NewMultipooler` always take them,
together with the path of the half's status page. The halves never create these
resources themselves, and they never register the process flags, call
`servenv.Init`, or open or close the topology store; `main` does all of that.
`main` opens the store after parsing flags and passes it to each half's `Init`.
`main` also passes the settings registry each half configures its own settings
on. `cmd/multigateway` and `cmd/multipooler` pass the registry their servenv
uses, so a configuration file sets the half's settings too. `cmd/minigres`
gives each half its own registry, because both define keys such as `pg-port`
with different meanings; that is why it refuses configuration files for now.

`cmd/multigateway` and `cmd/multipooler` do this for one half each, and
`cmd/minigres` does it once for both. `cmd/minigres` opens the store, calls
`servenv.Init` once with the service name `minigres` and the pooler's identity,
then runs the pooler's `Init`, the gateway's `Init`, and the serving loop.

## Where Minigres diverges from Multigres, and why

Everything not listed here runs the Multigres code path unchanged, including
the query path, the gateway's discovery, health stream and readiness, the
pooler's bootstrap, and shutdown.

### Process ownership

| Multigres                                                                                      | Minigres                                                                                      | Why                                                                                                                                                                    |
| ---------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Each binary creates its own servenv, gRPC server and etcd connection, and calls `servenv.Init` | `main` creates one of each for both halves, and `servenv.Init` runs once                      | `servenv.Init` may only run once per process (a second call is `log.Fatal`)                                                                                            |
| Service identity `multigateway` or `multipooler`                                               | One identity, service name `minigres`, with the pooler's cell, service ID, database and shard | One process should appear once in logs, metrics and traces                                                                                                             |
| Status page at `/`                                                                             | Index at `/`, with the halves' pages at `/multigateway` and `/multipooler`                    | The shared HTTP mux panics if `/` is registered twice                                                                                                                  |
| Each binary registers all its flags                                                            | Process flags defined once; shared flags given once; the pooler keeps its own flag set        | A flag set rejects duplicate names, both halves must agree on cell and service ID, and the pooler decides whether to adopt `pg-port` from pgctld by whether it was set |

### The static leader

`cmd/minigres` constructs the pooler as a static leader, which sets
`StaticLeader` in the pooler manager's configuration, and the consensus manager
handles it
(`go/services/multipooler/internal/manager/consensus/manager.go`): it answers
the role, how PostgreSQL starts, what to do about a standby, and whether
resigning is allowed. There is no Multiorch, so nothing else would ever make
the pooler writable.

| Multigres                                                                                 | Minigres                                                                                              | Why                                                                                                                                                           |
| ----------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| The routing role uses the consensus manager's status, which Multiorch's elections fill in | A fixed status naming this pooler leader, returned by the consensus manager's `CachedConsensusStatus` | Consensus never names a leader without Multiorch, so the pooler would never report PRIMARY. One answer for every reader keeps them from disagreeing           |
| The postgres monitor restarts a stopped Postgres as a standby; Multiorch promotes later   | It restarts Postgres as a primary                                                                     | pgctld defaults to standby because Multiorch decides who writes. After a crash nobody would promote it                                                        |
| Leader with a standby Postgres: the monitor signals resignation to Multiorch              | The monitor promotes Postgres itself (`promoteStandbyToPrimary`), without recruitment                 | The bootstrap restore leaves a standby (pgBackRest writes `standby.signal`), as does `pgctld restart --as-standby`. Recruitment only fences off other poolers |
| The consensus service is registered when the service map allows it                        | Never registered, even with `--service-map grpc-consensus`                                            | With a shared etcd, a Multiorch could find this pooler and recruit it, demoting a pooler that then promotes itself back                                       |
| `ResignLeadership` (Multiadmin `SwitchPrimary`) hands leadership to another pooler        | Returns `FAILED_PRECONDITION`                                                                         | There is no other pooler, and the monitor would promote it straight back, so the call would only cause an outage                                              |

Running Multiorch's own bootstrap with a cohort of one does not work as an
alternative: its shard-initialization analyzer never fires for a single pooler,
and consensus would have to keep running after every restart.

### Kept the same on purpose

- **The bootstrap.** The pooler's monitor still runs initdb, creates the
  Multigres schema, takes the first backup, deletes the data directory and
  restores from the backup. It guarantees a backup and a proven restore path
  before the first query, at the cost of one restore of an empty database.
- **The rule store is not written.** A static leader does not write its rule to
  `multigres.current_rule`, so the `Status` RPC reports term 0 and no leader
  while the routing role is PRIMARY. The write waits for a single-pooler
  durability policy: with `AT_LEAST_2`, the rule would make
  `synchronous_standby_names` wait for a standby that cannot exist.

## Testing

The end-to-end harness has a Minigres topology (`go/test/endtoend/minigressetup`)
that starts etcd, the topology records, one pgctld and one `minigres`, and waits
until the pooler has bootstrapped, promoted itself and serves a write. The
`queryserving` tests run against it with the topology switch:

```bash
MULTIGRES_E2E_TOPOLOGY=minigres go test ./go/test/endtoend/queryserving/
```

The same tests run against a Multigres shard without the switch. Tests that need
something Minigres doesn't have skip themselves under Minigres, with the reason
in the skip message (`clustersetup.RequireMultigresTopology`):

| Tests                                                                                                     | File                              | Why they need Multigres                                          |
| --------------------------------------------------------------------------------------------------------- | --------------------------------- | ---------------------------------------------------------------- |
| `TestReplicaReads_*`                                                                                      | `replica_reads_test.go`           | The replica-reads port routes to replicas, and Minigres has none |
| `TestMultigateway_LogicalReplicationSlotFailoverAdmission`                                                | `replication_slot_test.go`        | Slot-based replication to replicas                               |
| `TestBufferPlannedFailover`, `TestBufferMultipleFailovers`, `TestBufferTransactionsAndPreparedStatements` | `buffer_test.go`                  | Multiorch fails over to a standby while the gateway buffers      |
| `TestTransactionAbortedOnFailoverGraceExpiry`                                                             | `transaction_failover_test.go`    | Multiorch fails over to a standby                                |
| `TestUnloggedTablesAfterFailover`                                                                         | `unlogged_test.go`                | Multiorch fails over to a standby                                |
| `TestMultigateway_PostgresCrashRecovery`                                                                  | `postgres_crash_recovery_test.go` | Multiorch elects a new primary after PostgreSQL stops            |
| `TestGateway_EvictsStrandedPrimaryOnStaleHealthStream`                                                    | `stale_primary_eviction_test.go`  | Another pooler takes over a stranded primary                     |
| `TestMultigateway_QueryCancel_ForwardedOverGRPCTLS`                                                       | `query_cancel_test.go`            | Cancel forwarding between two gateways                           |

Everything else in `queryserving` runs unchanged on Minigres, including the
TLS, authentication, session, transaction, COPY and statement-timeout tests.

In CI, `queryserving` and `queryserving/pgparity` run on both topologies on
every pull request. The pgproto, sqllogictest and PostgREST suites run on both
topologies when their pull request label is set; pgproto and sqllogictest also
run daily.

## Known limitations

- Minigres is started by hand; there is no CLI or provisioner support.
- Backups need `ForcePrimary` on a primary, and the standby backup path relies on
  a consensus-recorded primary that a static leader does not have.
- Several Minigres instances serving the same database name in one etcd look
  like one shard to the topology.
- Shutdown uses the Multigres hooks, which run the halves' shutdowns
  concurrently, so shutdown under load does not yet drain the gateway before the
  pooler stops.
