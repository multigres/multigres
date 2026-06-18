# Multigres docker-compose cluster

An all-in-one Multigres cluster in a single container, for local experimentation
and CI compatibility testing. It bootstraps a complete cluster with the
[`local` provisioner](../go/provisioner/local) — the same path used by
`multigres cluster start` during local development — and exposes a PostgreSQL
endpoint that test suites and applications can connect to.

## What's inside

The image (`Dockerfile.cluster`) bundles PostgreSQL, etcd, pgBackRest, and every
`multigres` binary. On startup the entrypoint runs `multigres cluster init`
followed by `multigres cluster start`, which launches:

- `etcd` (topology store)
- `multiadmin`
- per cell: `pgctld` + PostgreSQL, `multipooler`, `multiorch`, and
  `multigateway`

Clients connect through the **zone1 multigateway** on port `15432`.

### Sizing (`MULTIGRES_NUM_CELLS`)

A cell is the unit of scaling in the local provisioner: each bundles one
PostgreSQL, one multipooler, one multiorch, and one multigateway, and they scale
together rather than independently. Clients always talk to zone1's multigateway.

The default is **2 cells**, which is the minimum that works. The provisioner
bootstraps the shard with an `AtLeastN(2)` durability policy, so the cluster
needs **at least two poolers** to elect a leader — a single cell never becomes
ready to serve queries (the gateway reports "no leader observed yet"). The
supported range is therefore `2`–`3`; use `3` for a cluster that tolerates
losing a node:

```bash
MULTIGRES_NUM_CELLS=3 docker compose up --build
```

Values below 2 are clamped to 2, and values above 3 to 3 (`cluster init` only
generates three cells). Additional cells use offset ports — gateway PostgreSQL
`15433`/`15434`, etc. — so publish those in `docker-compose.yml` if you need to
reach them from the host. (Independently scaling, say, gateways without poolers
isn't supported by the local provisioner.)

### Gateway port (`MULTIGRES_GATEWAY_PG_PORT`)

The zone1 multigateway serves the PostgreSQL wire protocol on `15432` by
default. Set `MULTIGRES_GATEWAY_PG_PORT=5432` to make the container a drop-in
PostgreSQL on the standard port — useful when slotting Multigres into a setup
that hardcodes `5432`. Additional cells
take consecutive ports (`base+1`, `base+2`). Keep the published `ports:` and the
healthcheck in `docker-compose.yml` in sync with this value.

### Max connections (`MULTIGRES_PG_MAX_CONNECTIONS`)

The bundled PostgreSQL defaults to `max_connections = 60`. Set
`MULTIGRES_PG_MAX_CONNECTIONS` to raise it:

```bash
MULTIGRES_PG_MAX_CONNECTIONS=100 docker compose up --build
```

This does two things so PostgreSQL and the pooler stay consistent:

- sets PostgreSQL's `max_connections` to the value, and
- sizes the connection pooler's global capacity to that value **minus 10**
  (here, 90). The 10-connection reserve leaves room for superuser logins,
  replication, so the pooler never tries to
  open more backends than PostgreSQL allows.

`SHOW max_connections` through the gateway then reports the value you set, since
the gateway passes it through to PostgreSQL. The value must be greater than 10.

PostgreSQL's `max_connections` is applied only at first init (a fresh data dir),
so on a **persistent volume** changing `MULTIGRES_PG_MAX_CONNECTIONS` later has
no effect on PostgreSQL (the pooler would re-read it but PostgreSQL would keep
the original value, leaving them mismatched). To change it on a persisted
cluster, re-initialize from a clean state (`docker compose down -v`).

### Extra PostgreSQL config (`MULTIGRES_PG_EXTRA_CONF`)

For anything `MULTIGRES_PG_MAX_CONNECTIONS` doesn't cover, set
`MULTIGRES_PG_EXTRA_CONF` to raw `postgresql.conf` text. It is appended verbatim
onto every cell's config at init, last-write-wins:

```bash
MULTIGRES_PG_EXTRA_CONF=$'shared_buffers = 256MB\nwork_mem = 8MB' docker compose up --build
```

The value may span multiple lines (one setting per line). Like the knob above,
it is applied only at first init. Advanced callers can instead point
`POSTGRES_INITDB_EXTRA_CONF` at a mounted snippet directly — if both are set,
the values from these knobs are appended last and win.

## Usage

From the repository root:

```bash
docker compose up --build            # build the image and start the cluster
```

Wait for the healthcheck to pass (first boot bootstraps the cluster, so allow
~30–60s), then connect:

```bash
PGPASSWORD=postgres psql -h 127.0.0.1 -p 15432 -U postgres -d postgres
```

Stop and remove the container:

```bash
docker compose down
```

### Exposed ports

| Port    | Service                                       |
| ------- | --------------------------------------------- |
| `15432` | zone1 multigateway — PostgreSQL wire protocol |
| `15100` | multigateway HTTP (status/metrics)            |
| `15000` | multiadmin HTTP (status/metrics)              |

### Default credentials

| Field    | Value      |
| -------- | ---------- |
| user     | `postgres` |
| password | `postgres` |
| database | `postgres` |

The gateway/pooler is pinned to the `postgres` database and the `postgres`
superuser; use those for connections.

## CI

`.github/workflows/docker-compose-ci.yml` builds the image, brings the cluster
up, waits for the healthcheck, and runs a PostgreSQL compatibility smoke test
(version probe plus a DDL/DML round-trip) before tearing everything down. It
runs on pull requests that touch the compose setup or the Go sources.

## Persistence

The cluster is **ephemeral** by default — no volume is mounted, so every run
starts from a clean state. To persist cluster data across runs, mount a volume
at the working directory in `docker-compose.yml`:

```yaml
services:
  multigres:
    volumes:
      - multigres-data:/multigres/cluster
volumes:
  multigres-data:
```

The entrypoint reuses an existing `multigres.yaml` if one is present, so a
persisted volume keeps its configuration and data on restart.

## Notes

- `init: true` in the compose file runs an init process (tini) as PID 1 to reap
  the cluster's orphaned child processes and forward signals.
- The whole cluster runs inside one container; this is intended for testing, not
  production.
- The image runs as the unprivileged `postgres` user, since `initdb` and the
  PostgreSQL server refuse to run as root.
