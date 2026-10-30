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
  calls between the halves that are not direct, and a TCP one for the admin
  services.
- **The multipooler as a static leader.** It is the only pooler of its shard, so
  it leads without consensus and without Multiorch.
- **The multigateway reaches the pooler in memory.** Queries are plain Go
  calls into the pooler; authentication, LISTEN/NOTIFY, logical replication
  and the health stream go over the in-memory gRPC server. None leave the
  process. The gateway learns of the pooler from the pooler
  itself, not from etcd, so it never sees other instances' poolers. An
  in-process health subscription, replacing the health stream, is planned work.
- **pgctld runs as its own process**, as in Multigres.

Many Minigres processes can share one etcd and one Multiadmin. Once a database
has bootstrapped, Minigres keeps serving and can restart while etcd is down (see
[Running without etcd](#running-without-etcd)).

## Running it

`multigres cluster init --topology minigres` generates a single-cell local
cluster config with one `minigres` service (paired with its own `pgctld`)
instead of the usual multigateway/multipooler/multiorch triad. From there,
the normal `multigres cluster start` / `stop` / `restart` commands work
unchanged — see the "Minigres topology" section of the `mt-local-cluster`
skill for the day-to-day commands.

```bash
make build
export PATH=$PWD/bin:$PATH

multigres cluster init --config-path ./minigres_local --topology minigres
multigres cluster start --config-path ./minigres_local --wait-for-bootstrap

psql "host=localhost port=15532 user=postgres dbname=postgres"

multigres cluster stop --config-path ./minigres_local
```

Under the hood this runs the same sequence that used to be manual — etcd,
`multigres createclustermetadata`, `pgctld server`, then `minigres` — through
the local provisioner (`go/provisioner/local`), so there's no separate
"minigres mode" anywhere in that code: a cell simply has a `minigres` service
configured instead of the usual three, and the generic provisioning/start/
stop logic doesn't need to know the difference.

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
- **Settings** come from flags, `MT_*` environment variables and a config
  file (see [Configuration file](#configuration-file)).

### Flags

| Flag                                                        | Behavior in Minigres                                                                                                    |
| ----------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `--pg-port`                                                 | The **client-facing** port (the gateway's). The pooler's Postgres port is not a flag; the pooler adopts it from pgctld  |
| `--cell`, `--service-id`, `--enable-slot-based-replication` | Given once and applied to both halves. Without a service ID, one is generated and shared by the process and both halves |
| `--table-group`, `--shard`                                  | Default to `default` and `0-inf`, the only pair the gateway routes to                                                   |
| servenv, gRPC server and topology flags                     | Defined once for the process                                                                                            |
| `--service-map grpc-consensus`                              | Has no effect: the consensus service is never registered (see [The static leader](#the-static-leader))                  |

A flag that both halves define is rejected at startup, so a new flag that
collides fails loudly instead of silently shadowing another. Likewise, an `MT_*`
environment variable that both halves read is rejected at startup unless it
belongs to a shared setting.

### Configuration file

One file configures the process and both halves (`--config-file`, or one found
through `--config-name` and `--config-path`). The process's settings and the
shared settings sit at the top level; each half's own settings are written
behind the half's name, with the same names as in a Multigres component's file:

```yaml
grpc-port: 15100
log-level: info
topo-global-server-addresses: localhost:2379
cell: zone1 # shared: applied to both halves
service-id: mg-1 # shared
multigateway.pg-port: 15432
multigateway.statement-timeout: 30s
multipooler.pgctld-addr: localhost:15200
multipooler.database: postgres
```

- **Nested sections** (`multigateway:` with `pg-port: 15432` indented under it)
  are read as the same settings.
- **Shared settings** (`cell`, `service-id`, `enable-slot-based-replication`)
  are written once at the top level and applied to both halves, as their flags
  are. Inside a section they are accepted only with the top-level value.
- **Priority** is the same as in Multigres: flag, then environment variable,
  then the file, then the default.
- **Settings nothing reads are rejected.** A typo, a half's setting without its
  prefix (the message suggests the prefixed name) or `multipooler.pg-port`
  makes `minigres` refuse to start.
- **Changes while running:** dynamic settings follow edits to the file. An edit
  with a setting nothing reads is rejected as a whole; the previous settings
  stay and an error is logged.

## How it is wired

`servenv.ProcessResources` (`go/common/servenv/process_resources.go`) holds what
a process has only one of: the servenv and its gRPC servers. Every binary's
`main` creates them and hands them to the halves it runs:
`multigateway.NewMultigateway` and `multipooler.NewMultipooler` always take them,
together with the path of the half's status page. The halves never create these
resources themselves, and they never register the process flags, call
`servenv.Init`, or open or close the topology store; `main` does all of that.
`main` opens the store after parsing flags and passes it to each half's `Init`.
`main` also passes the settings registry each half configures its own settings
on. `cmd/multigateway` and `cmd/multipooler` pass the registry their servenv
uses, so a configuration file sets the half's settings too. `cmd/minigres`
passes each half a view of its registry behind the half's name
(`Registry.WithPrefix`): both define keys such as `pg-port` with different
meanings, and the view stores the gateway's as `multigateway.pg-port` and the
pooler's as `multipooler.pg-port`. The halves' code is unchanged; the config
file's rules are in `cmd/minigres/config.go`.

`cmd/multigateway` and `cmd/multipooler` do this for one half each, and
`cmd/minigres` does it once for both. `cmd/minigres` opens the store, calls
`servenv.Init` once with the service name `minigres` and the pooler's identity,
then runs the pooler's `Init`, the gateway's `Init`, and the serving loop.

`ProcessResources` also names the server the pooler serves its query service
(`MultipoolerService`) on, `QueryGrpcServer`. The Multigres binaries set it to
their one gRPC server. `cmd/minigres` sets it to an in-memory server
(`servenv.NewInMemoryGrpcServer`), which nothing outside the process can reach:
no port, no TLS and no auth, and the external server's limits. The serving loop
starts both servers (`RunDefault(grpcServer, queryGrpcServer)`). `cmd/minigres`
also gives the gateway its pooler (`poolergateway.LocalPooler`): the pooler's ID
the in-memory server's dial options (`DialOptions`) and the pooler's record
(`Record`), and its query service (`QueryService`, from
`Multipooler.LocalQueryService`). The gateway then reads that pooler's record in memory instead of
watching etcd, keeps only that pooler (the pooler cache's filter), and connects
to it in memory; the Multigres binaries pass none and use every pooler
in the topology.

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

- **It uses only its own pooler.** The gateway reads the record of the pooler
  in its own process once, when it starts (`Multipooler.Record`), and never
  watches etcd for poolers. It needs nothing more: the fields it uses (ID and
  shard) never change, and the pooler's role and serving status come from the
  health stream. Even when many Minigres
  instances share one etcd, queries never reach another instance and readiness
  counts only its own pooler.
- **Queries are direct calls.** Every query-path call (statements, portals,
  Describe, COPY, transaction ends, reserved-connection release) calls the
  pooler's query service directly, in the gateway's goroutine and context: the
  same admission (`StartRequest`, reservation-reason validation) and executor
  the pooler's gRPC handler calls, with nothing marshalled. A statement timeout
  or cancellation reaches the client as the same error as over gRPC. At
  shutdown the pooler refuses new direct calls and waits for running ones
  before closing its pools, as the gRPC server's graceful stop does.
- **The rest connects in memory.** The pooler serves its gRPC query service
  (`MultipoolerService`) only on an in-memory gRPC server, and the gateway
  connects there, without TLS, for authentication, LISTEN/NOTIFY, logical
  replication (`StreamReplication`) and the health stream.
- **Observability differs on the query path.** The direct path has no
  pooler-side operation span and no transport metrics: traces go from the
  gateway's spans straight to the pooler's internal spans, and the gateway's
  stream-pool metrics (`mg.gateway.query_stream.*`) stay empty in Minigres.
- **The external gRPC port carries the admin services.** The pooler manager and
  gateway manager services stay on the TCP port, for Multiadmin and the test
  harness. Calling the query service on that port returns `Unimplemented`.

## What is turned off

The gateway is the only one in front of the only pooler, so the features that
need several poolers or gateways are off. They are off for any gateway given a
local pooler (`poolergateway.LocalPooler`), which `cmd/minigres` does: their
settings are never created, so neither a flag nor an `MT_*` environment variable
can turn them on, and their flags are rejected as unknown.

| Feature                                                                                                | Why it does not apply                                                                                                                                                                                                                             |
| ------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Replica reads (`--pg-replica-port`, `--low-replication-lag-ms`, `--high-replication-lag-tolerance-ms`) | There are no replicas                                                                                                                                                                                                                             |
| Failover buffering (`--buffer-*`)                                                                      | There is no failover: the static leader is never switched over, and nothing promotes another pooler                                                                                                                                               |
| Multipooler gRPC TLS (`--multipooler-grpc-*`)                                                          | The gateway reaches its pooler in memory, and the settings otherwise secure only cancel forwarding                                                                                                                                                |
| Cross-gateway cancel forwarding                                                                        | A client sends its cancel request to the address it connected to, which is this gateway. The gateway cancels its own connections and ignores a cancel request for any other, instead of forwarding it to another instance through the shared etcd |

Without cancel forwarding the gateway claims no connection-ID prefix: connection
IDs are used as they are, and the limit of 2,047 gateways per etcd and the
"PID prefix held" readiness check do not apply.

## Running without etcd

Minigres uses etcd so that Multiadmin can see it, not to serve queries. Once its
database has bootstrapped, it keeps serving when etcd goes down and starts while
etcd is down:

| Part                       | Without etcd                                                                                                                                                                                                  |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Opening the topology       | Does not wait: the connection is retried in the background                                                                                                                                                    |
| The gateway's pooler       | Read from the pooler's record in memory                                                                                                                                                                       |
| The gateway's registration | Retried in the background (`toporeg.Register`) instead of blocking startup, so Multiadmin sees the gateway once etcd is back                                                                                  |
| The pooler's registration  | Retried in the background, as in Multigres                                                                                                                                                                    |
| The database record        | The static leader keeps a copy of the database record it last read (`topo-database.pb` in the pooler directory) and starts from it when etcd is unreachable. Nothing else can change the record while it runs |

The first bootstrap still needs etcd: the first backup reads the durability
policy. Backups themselves take no etcd lease (see [Backups](#backups)), but
Multiadmin finds the pooler through etcd, so backups requested through it, and
the other admin RPCs that read the topology, wait for etcd.

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

## Backups

The static leader is the only pooler of its shard, so its backups are taken on
it, locally (`pg1` only):

- **No `ForcePrimary` needed.** A coordinated primary refuses a backup without
  `ForcePrimary`, to keep backup load off the primary; a static leader does not
  (`allowBackupOnPrimary`). `multigres cluster backup` works without
  `--primary`.
- **Multiadmin chooses it.** The pooler publishes
  `leadership_mode: LEADERSHIP_MODE_STATIC` in its topology record. For a shard
  whose pooler is a static leader, Multiadmin sends backups, listing, expiry,
  verification and job status to it, whatever "force primary" says
  (`findPoolerForBackup`); other shards still use a replica. A shard with a
  static leader and any other pooler is misconfigured, and Multiadmin refuses
  its backup operations rather than guess.
- **No etcd lease.** The backup, expire and first-backup leases exist so that
  only one pooler of a shard works on its repository at a time. A static leader
  is the only one, and the action lock already serializes its own operations,
  so it takes none of them (`withBackupLease`). This assumes each Minigres
  instance has its own topology root and backup location, as Multigres projects
  do, so two instances never share a shard's keys.
- **No pgBackRest TLS server.** It exists so a replica can reach the primary
  during a replica backup; a static leader backs up and restores locally.
- **Restore** is automatic, as in Multigres: with an empty data directory and a
  backup in the repository, the pooler restores the latest backup, starts
  PostgreSQL as a standby and promotes itself.

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

- `multigres cluster init --topology minigres` only generates a single
  all-minigres cell; mixing a minigres cell with full-Multigres cells in one
  cluster config is not supported yet.
- Several Minigres instances serving the same database name under one
  topology root look like one shard to the topology. Each instance needs its
  own topology root (`--topo-global-root`), as Multigres projects have.
- Shutdown uses the Multigres hooks, which run the halves' shutdowns
  concurrently, so shutdown under load does not yet drain the gateway before the
  pooler stops.
