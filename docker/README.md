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
that hardcodes `5432`, such as Supabase Storage's `tenant_db`. Additional cells
take consecutive ports (`base+1`, `base+2`). Keep the published `ports:` and the
healthcheck in `docker-compose.yml` in sync with this value.

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
