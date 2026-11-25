# Local cluster and the multigres CLI

`multigres` (`go/cmd/multigres`) is the command-line tool. It does two things:

- It provisions a complete cluster on one machine, for development
  (`multigres cluster init|start|stop|restart`).
- It is a gRPC client of Multiadmin for administrative commands (backups,
  switchovers, topology queries, migrations).

See [Building Multigres](./building.md) for the prerequisites and the
quick-start commands.

## The local provisioner

`multigres cluster` delegates to a provisioner (`go/provisioner`). Only the
`local` provisioner (`go/provisioner/local`) exists. It runs every component as
a local process.

### Configuration

`multigres cluster init` writes `multigres.yaml` into the first `--config-path`
directory. The default is `./multigres_local`, or `$MTDATAROOT` when that
variable is set. `init` refuses to overwrite an existing file. The other
`cluster` commands and the admin commands read the same file.

The file has two top-level keys: `provisioner` (`local`) and
`provisioner-config`, which holds:

| Key                | Contents                                                              |
| ------------------ | --------------------------------------------------------------------- |
| `root-working-dir` | Directory that holds all cluster state. Same as the config directory. |
| `default-db-name`  | The database to provision (`postgres`).                               |
| `backup`           | Backup repository: a local path or S3, and whether it is encrypted.   |
| `etcd`             | etcd data directory and port.                                         |
| `topology`         | The global topology root and the name and root of each cell.          |
| `multiadmin`       | Binary path, ports and log level of the single Multiadmin.            |
| `cells`            | Per cell: the services to run, with their binary paths and ports.     |

Edit the file before `cluster start` to change ports or log levels. A cell runs
exactly the services whose `path` is set in its `cells` entry.

`init` flags:

| Flag                       | Meaning                                                                             |
| -------------------------- | ----------------------------------------------------------------------------------- |
| `--topology minigres`      | Generate one cell running a single `minigres` process instead of the default.       |
| `--backup-path`            | Local backup directory. Defaults to `<config dir>/data/backups`.                    |
| `--backup-url`, `--region` | Use an S3 repository (`s3://bucket/prefix`). Credentials come from the environment. |
| `--encrypt-backups`        | Encrypt backups client-side.                                                        |

See [Backups](./general/backups.md) and [Minigres](./general/minigres.md).

### Default topology

The default config describes one database, `postgres`, with one tablegroup
(`default`) and one shard (`0-inf`), spread over three cells. Each cell runs a
Multigateway, a Multipooler with its pgctld and Postgres, and a Multiorch. One
etcd and one Multiadmin are shared. The database's bootstrap durability policy
is `AT_LEAST_2`.

Default ports (`go/provisioner/local/ports`); per-cell services add 0, 1, and 2
for `zone1`, `zone2`, and `zone3`:

| Service      | HTTP  | gRPC  | Other                                |
| ------------ | ----- | ----- | ------------------------------------ |
| etcd         |       |       | 2379 (client)                        |
| Multiadmin   | 15000 | 15070 |                                      |
| Multigateway | 15100 | 15170 | 15432 (PostgreSQL)                   |
| Multipooler  | 15200 | 15270 |                                      |
| Multiorch    | 15300 | 15370 |                                      |
| pgctld       | 15400 | 15470 | 25432 (Postgres), 18432 (pgBackRest) |
| Minigres     | 15500 | 15570 | 15532 (PostgreSQL)                   |

Connect through a gateway with the default credentials:

```bash
PGPASSWORD=postgres psql -h localhost -p 15432 -U postgres
```

### What `cluster start` does

1. Checks that the configured Multigres binaries exist and that `etcd`,
   `pg_ctl`, `postgres`, `pg_isready`, and `pgbackrest` are on `$PATH`. The
   directory of the `multigres` binary is prepended to `$PATH` first.
2. Checks that PostgreSQL is version 17 and pgBackRest is at least 2.57.
3. Starts etcd.
4. Creates the cells in the global topology, then starts Multiadmin.
5. Creates the database in the global topology, then starts every per-cell
   service in parallel.
6. Unless `--wait-for-bootstrap=false` is passed, waits up to two minutes for
   every gateway to run `SELECT 1` successfully.

In the default topology, the shard itself is bootstrapped by Multiorch once
enough Multipoolers are up; see [Recovery](./ha/recovery.md).

### Files

Everything lives under the root working directory:

| Path               | Contents                                                                                |
| ------------------ | --------------------------------------------------------------------------------------- |
| `multigres.yaml`   | The configuration.                                                                      |
| `state/`           | One JSON file per running service (PID, ports, log file).                               |
| `logs/`            | One log file per service; database services are under `logs/dbs/<database>/<service>/`. |
| `data/etcd-data`   | etcd data.                                                                              |
| `data/pooler_<id>` | A Multipooler's directory; Postgres data is in its `pg_data` subdirectory.              |
| `data/backups`     | The default local backup repository.                                                    |
| `sockets/`         | Unix sockets for gRPC.                                                                  |
| `certs/pgbackrest` | Generated pgBackRest TLS certificates.                                                  |

`multigres cluster stop` stops the processes. `multigres cluster stop --clean`
also removes the `logs`, `state`, `data`, `sockets`, and `spool` directories.
`multigres cluster restart` is a stop followed by a start.

## Admin commands

The remaining commands call Multiadmin over gRPC:

| Commands                                                                                            | Purpose                              |
| --------------------------------------------------------------------------------------------------- | ------------------------------------ |
| `getcell`, `getcellnames`, `getdatabase`, `getdatabasenames`                                        | Read the global topology.            |
| `getgateways`, `getpoolers`, `getorchs`, `getpoolerstatus`                                          | Discover components and their state. |
| `cluster backup`, `cluster list-backups`, `cluster expire-backups`, `cluster verify-backups`        | Manage backups.                      |
| `cluster switch-primary`, `cluster apply-rule-change`                                               | Planned switchover and rule changes. |
| `create-migration`, `list-migrations`, `get-migration`, `set-migration-direction`, `drop-migration` | Table migrations.                    |

They find Multiadmin through `--admin-server host:port`. Without the flag, they
read `multigres.yaml` from `--config-path` and use `localhost` with the
configured Multiadmin gRPC port. The connection is not encrypted.

Three commands do not go through Multiadmin:

- `cluster check-backup-config` validates S3 credentials and access.
- `cluster refresh-credentials` refreshes the AWS credentials of a local
  cluster that uses S3 backups.
- `createclustermetadata` writes the cells and the `postgres` database directly
  into the global topology. It is meant to be invoked by the operator rather
  than by a local cluster.

`cluster status` is a placeholder: it does not inspect the cluster yet.

Run `multigres <command> --help` for the flags of each command.
