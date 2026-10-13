# Minigres

Minigres runs a multigateway and a multipooler in **one process**, for a
database served by a single pooler with no replicas and no shards. It is built
as the `minigres` binary (`go/cmd/minigres`).

Minigres reuses the Multigres code. Both halves run the same query-serving code
as their Multigres binaries; what changes is who owns the process and who makes
the pooler writable. This document covers what exists today and is updated as
Minigres evolves.

## What runs

- **One process** with one servenv, one HTTP server and one etcd connection,
  shared by both halves, and two gRPC servers: an in-memory one for the
  queries between the halves, and a TCP one for the admin services.
- **The multipooler as a static leader.** It is the only pooler of its shard, so
  it leads without consensus and without Multiorch.
- **The multigateway reaches the pooler in memory.** Queries, authentication,
  LISTEN/NOTIFY and the health stream go over the in-memory gRPC server and
  never leave the process. The gateway learns of the pooler from the pooler
  itself, not from etcd, so it never sees other instances' poolers. It still
  follows the pooler's gRPC health stream (in memory); replacing that with an
  in-process subscription is planned work.
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
  static leader does not use the policy today (see
  [Bootstrap and the rule store](#bootstrap-and-the-rule-store)).
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
| `--service-map grpc-consensus`                              | Has no effect: the consensus service is never registered (see [The static leader](#the-static-leader))                  |

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

`ProcessResources` also names the server the pooler serves its query service
(`MultipoolerService`) on, `QueryGrpcServer`. The Multigres binaries set it to
their one gRPC server. `cmd/minigres` sets it to a server on a local endpoint
(`servenv.NewLocalGrpcServer`). A server listens wherever its endpoint
(`servenv.Endpoint`) does: the TCP port from the flags, or memory
(`servenv.NewInMemoryEndpoint`), which nothing outside the process can reach. A
server on a local endpoint has no port, no TLS and no auth, and uses the
external server's limits. The serving loop starts both servers
(`RunDefault(grpcServer, queryGrpcServer)`). The gateway finds its poolers, and reaches them, through a pooler source
(`poolergateway.PoolerSource`). The Multigres binaries use the remote source,
which watches etcd for poolers in every cell and dials each at the address in
its record with the `--multipooler-grpc-*` credentials. `cmd/minigres` passes a
local source, which follows the pooler in the same process through its record
(`Multipooler.WatchRecord`) and connects the way the query server's endpoint
says.

## The process

- **One servenv, initialized once.** `main` creates the servenv, both gRPC
  servers and the etcd connection for both halves, and calls `servenv.Init`
  once (a second call is fatal).
- **One identity.** The process reports the service name `minigres` with the
  pooler's cell, service ID, database and shard, so it appears once in logs,
  metrics and traces.
- **Status pages.** `/` is an index; the halves' pages are at `/multigateway`
  and `/multipooler`, because the shared HTTP server cannot register `/` twice.
- **Flags.** Process flags are defined once and shared flags are given once (see
  [Flags](#flags)). The pooler keeps its own flag set, so it can tell whether its
  own `pg-port` was set and otherwise adopt it from pgctld.

## How the gateway reaches the pooler

- **It learns of the pooler from the pooler.** The gateway follows the pooler's
  topology record through the pooler itself (`Multipooler.WatchRecord`), not
  through etcd. It knows exactly one pooler, its own, even when many Minigres
  instances share one etcd, so queries never reach another instance and
  readiness counts only its own pooler.
- **It connects in memory.** The pooler serves its query service
  (`MultipoolerService`) only on a gRPC server on an in-memory endpoint, and the
  gateway connects there, without TLS. Queries, authentication, LISTEN/NOTIFY
  and the health stream never leave the process.
- **The external gRPC port carries the admin services.** The pooler manager and
  gateway manager services stay on the TCP port, for Multiadmin and the test
  harness. Calling the query service on that port returns `Unimplemented`.
- **The gateway's `--multipooler-grpc-*` flags are not used for queries.** They
  still configure cancel-request forwarding between gateways, which does not
  apply to a single gateway and is planned to be turned off.

## The static leader

`cmd/minigres` constructs the pooler as a static leader: it is the only pooler
of its shard, and there is no Multiorch to make it writable. The consensus
manager (`go/services/multipooler/internal/manager/consensus/manager.go`)
answers every question about its role:

- **Role.** The pooler always names itself leader
  (`CachedConsensusStatus` returns a fixed status), so its routing role is
  PRIMARY whenever PostgreSQL is out of recovery, and every reader agrees.
- **Starting PostgreSQL.** The postgres monitor starts and restarts PostgreSQL as
  a primary, so it is writable again after a crash.
- **Promotion.** When PostgreSQL is a standby, the monitor promotes it itself
  (`promoteStandbyToPrimary`). The bootstrap restore leaves a standby (pgBackRest
  writes `standby.signal`), and so does `pgctld restart --as-standby`.
- **No consensus.** The consensus service is never registered, even with
  `--service-map grpc-consensus`, so a Multiorch sharing the etcd cannot recruit
  and demote the pooler.
- **No switchover.** `ResignLeadership` (Multiadmin `SwitchPrimary`) returns
  `FAILED_PRECONDITION`: there is no other pooler, and the monitor would promote
  this one straight back.

## Bootstrap and the rule store

- **The bootstrap runs in full.** The pooler's monitor runs initdb, creates the
  Multigres schema, takes the first backup, deletes the data directory and
  restores from the backup. This guarantees a backup and a proven restore path
  before the first query, at the cost of one restore of an empty database.
- **The rule store is not written.** The pooler does not write its rule to
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

**How the harness checks PostgreSQL.** Before each test the harness checks that
PostgreSQL is out of recovery and that a few settings
(`synchronous_standby_names`, `synchronous_commit`, `primary_conninfo`) are at
their baseline, and it can reset settings afterwards (`WithResetGuc`). The two
harnesses do this differently:

| Harness                    | Runs the checks and resets                                                     | Why                                                                                                                                                                                                    |
| -------------------------- | ------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Multigres (`shardsetup`)   | Through each pooler's query service on its gRPC port (`MultipoolerTestClient`) | The query service is reachable over the network in Multigres                                                                                                                                           |
| Minigres (`minigressetup`) | Directly on PostgreSQL, as the test user (`clustersetup.PostgresClient`)       | The query service is served only inside the `minigres` process, and the gateway rejects `ALTER SYSTEM`, so the checks go to PostgreSQL itself, like an operator would, not through the path under test |

The tests themselves connect through the gateway's client port on both
topologies, as a client would. The shared helpers (`QueryStringValue`,
`SaveGUCs`, `RestoreGUCs`, `ValidateGUCValue`) accept either client through
`clustersetup.QueryExecutor`.

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
