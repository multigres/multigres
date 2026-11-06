# Multigres Migrator migration demo (kind)

End-to-end demo of migrating a table from a **freestanding external PostgreSQL**
(a plain `postgres:17` Docker container) into a Multigres cluster using the
Multiadmin migration API, then **killing the target primary mid-migration** to
show the coordinator resumes on the newly elected primary, and finally
**cutting the application over** to the Multigres gateway with no data loss.

It runs on the kind cluster in [`demo/k8s`](k8s/) (no operator — raw
`StatefulSet` manifests) and uses the [`appclient`](appclient/) ledger workload.

> The ledger invariant: `appclient` keeps `SUM(balance)` constant. It transfers
> balance between rows and also opens and closes accounts (insert/delete), but
> every operation is balance-neutral, so the total never changes even though the
> row set and individual account ids do. Any lost or duplicated change during
> migration or failover makes the sum mismatch — a live no-data-loss signal on
> screen.

## Prerequisites

- Docker, [kind](https://kind.sigs.k8s.io/), `kubectl`, a Go toolchain, and a
  `psql` client on the host. For the one-command dashboard (below), also `tmux`
  and `watch`.
- Run everything from the repo root unless noted. Terminals below are labelled
  **T1..T4**; keep them open side by side.

> **One-command dashboard:** once the cluster and source are up (steps 0–2),
> [`demo/k8s/demo-dashboard.sh`](k8s/demo-dashboard.sh) opens a 4-pane tmux
> window — commands · migrations + state · source ledger · target (Multigres)
> ledger — that replaces the T2/T3/T4 terminals below and pre-fills a
> create/start/drop cheat sheet in the command pane. It runs the same commands
> the steps below spell out; follow the steps to understand each part, or drive
> the demo from the dashboard and use them as the reference.

## 0. Build the images and binaries (from this branch)

The images must be built from this branch so they contain the Multigres Migrator code.
`make images` builds straight from the working tree.

```bash
make images          # multigres/multigres:latest, multigres/pgctld-postgres:latest, multiadmin-web:latest
make build           # bin/multigres (CLI with the migration verbs)
go build -o bin/appclient ./demo/appclient
```

## 1. Start the cluster (T1)

```bash
mkdir -p demo/k8s/data
cd demo/k8s
./launch-infra.sh              # kind create + load images + etcd + cert-manager + pgbackrest certs + multiadmin
./launch-multigres-cluster.sh  # postgres secret + multipooler StatefulSet (3) + multiorch + multigateway
cd ../..
```

Both scripts start the port-forwards. Confirm the cluster is up and a primary was
elected:

```bash
kubectl --context kind-multidemo -n default get pods
# multipooler-zone1-0/1/2, multiorch, multigateway, multiadmin all Running

# gateway reachable (password: postgres):
PGPASSWORD=postgres psql -h localhost -p 15432 -U postgres -d postgres -c 'select 1'
```

Endpoints (from the port-forward scripts):

| service                                | address                  |
| -------------------------------------- | ------------------------ |
| Multigres gateway (PG)                 | `localhost:15432`        |
| Multiadmin gRPC (CLI `--admin-server`) | `localhost:18070`        |
| Multiadmin web UI                      | `http://localhost:18100` |

## 2. Start the freestanding source Postgres (T1)

Migration can be done from any Postgres server to Multigres. In this case we use a vanilla `postgres:17` container with logical replication enabled. Put it on the
**`kind` Docker network** so the in-cluster target Postgres can reach it, and
publish a host port so `appclient` (on the host) can reach it too.

```bash
docker run -d --name pg-source --network kind -p 5433:5432 \
  -e POSTGRES_PASSWORD=sourcepass \
  postgres:17 \
  -c wal_level=logical -c max_wal_senders=10 -c max_replication_slots=10
```

The source has **two addresses**, and it matters which you use where:

- **From the host** (`appclient`, `psql`): `host=localhost port=5433`.
- **From inside the cluster** (the migration's subscription CONNECTION, which the
  _target_ Postgres evaluates): the container's IP on the `kind` network, port
  `5432`. Pod DNS can't resolve the container name, so use the IP:

```bash
SRC_IP=$(docker inspect pg-source | jq -r '.[0].NetworkSettings.Networks.kind.IPAddress')
echo "source in-cluster address: $SRC_IP:5432"
```

## 3. Seed and start the writer (T1 → T2)

Seed the ledger on the source, then start continuous writes **against the
source** (host address). `--target-dsn` is the gateway, used later for the app
cutover (SIGUSR1).

```bash
# seed 10 accounts x 1000 = total 10000 (the invariant); few enough that the
# watchers show every row (see step 4)
bin/appclient setup \
  --source-dsn 'host=localhost port=5433 user=postgres password=sourcepass dbname=postgres sslmode=disable' \
  --accounts 10 --initial-balance 1000
```

**T2 — writer** (leave running):

```bash
bin/appclient write \
  --source-dsn 'host=localhost port=5433 user=postgres password=sourcepass dbname=postgres sslmode=disable' \
  --target-dsn 'host=localhost port=15432 user=postgres password=postgres dbname=postgres sslmode=disable' \
  --accounts 10 --interval 25ms --verify-interval 2s
# note the PID printed by the shell (or: pgrep -f 'appclient write') — needed in step 8
```

## 4. Start the reader dashboards (T3)

> Using `./demo-dashboard.sh`? These two watchers are already panes 3 (source)
> and 4 (target), and the command pane replaces T4 — skip ahead to step 5. Each
> watcher shows every account row plus a `TOTAL` rollup, so the invariant (and
> any replicated schema change) is visible at a glance.

Run two watchers so the audience sees the target catch up to the source. Split T3
or use two panes:

```bash
# source side
bin/appclient watch --source-dsn 'host=localhost port=5433 user=postgres password=sourcepass dbname=postgres sslmode=disable'

# Multigres side (starts empty; catches up once the migration streams)
bin/appclient watch --source-dsn 'host=localhost port=15432 user=postgres password=postgres dbname=postgres sslmode=disable'
```

The Multigres-side watcher will error/empty until step 5 creates the table there;
that's expected.

## 5. Drive the migration (T4)

Use the **in-cluster** source address (`$SRC_IP:5432`) — the target Postgres is
what connects to the source.

```bash
ADMIN=localhost:18070
SRC_IP=<from step 2>

# create (validates the source read-only) — capture the id
ID=$(bin/multigres create-migration --admin-server "$ADMIN" \
  --source-dsn "host=$SRC_IP port=5432 user=postgres password=sourcepass dbname=postgres sslmode=disable" \
  --target-database postgres \
  --tables public.accounts | jq -r .id)
echo "migration id: $ID"

# start: schema copy -> publication -> subscription (initial copy) -> streaming
bin/multigres set-migration-direction --admin-server "$ADMIN" --id "$ID" --direction=import

# poll until STREAMING
watch -n1 "bin/multigres get-migration --admin-server $ADMIN --id $ID | jq '{phase,total_relations,ready_relations,caught_up}'"
```

The Multigres-side watcher (T3) should now show `accounts`/`balance` climbing to
match the source, with `invariant: OK`.

## 6. Kill the target primary mid-migration (T4)

While the migration is streaming (T2 still writing to the source), delete the
primary pooler pod. `multiorch` re-elects a new primary and the migration
coordinator resumes there.

```bash
# identify the primary (routing_state PRIMARY); often multipooler-zone1-0
kubectl --context kind-multidemo -n default get pods -l app=multipooler \
  -o custom-columns=NAME:.metadata.name,STATUS:.status.phase

kubectl --context kind-multidemo -n default delete pod multipooler-zone1-0
```

## 7. Show the pickup, then cut the app over (T4 → T2)

```bash
# migration keeps going: phase returns to STREAMING on the new primary
bin/multigres get-migration --admin-server "$ADMIN" --id "$ID" | jq '{phase,caught_up,last_error}'
```

- The Multigres-side watcher (T3) stays `invariant: OK` and keeps up — the
  coordinator picked the migration up on the new primary with no data loss.
- Cut the application over to Multigres: send `SIGUSR1` to the **writer** (T2).
  It reconnects to `--target-dsn` (the gateway) and re-verifies the invariant on
  the target:

```bash
kill -USR1 $(pgrep -f 'appclient write')
# T2 prints: FAILED OVER to target: <n> accounts, total 10000, want 10000 => OK
```

The writer now drives Multigres directly and the invariant still holds — the
table migrated, the migration survived the primary kill, and the application cut
over transparently.

To finish the migration lifecycle (detach from the source) you can drop it:

```bash
bin/multigres drop-migration --admin-server "$ADMIN" --id "$ID" --wait
```

## Cleanup

```bash
kill %1 %2 2>/dev/null            # stop appclient processes in each terminal (Ctrl-C also works)
docker rm -f pg-source
cd demo/k8s && ./teardown.sh      # kind delete cluster + rm -rf data/*
```

## Notes and gotchas

- **Source addressing** is the most common mistake: host tools use
  `localhost:5433`; the migration's `--source-dsn` must use the container's
  `kind`-network IP on `5432`, because the _target_ Postgres dials it.
- **Replica identity**: the ledger table is `accounts(id bigint PRIMARY KEY, …)`,
  so it has a usable default replica identity — migration validation passes. A
  table with no PK/unique index would be rejected at `create-migration`.
- **Source privileges**: the container's `postgres` superuser satisfies the
  IMPORT requirements (REPLICATION, CREATE on the database, table ownership,
  `wal_level=logical`).
- **`--tables`** also accepts `public.*` (all owned tables in a schema) or `*`
  (all owned tables on the search_path); this demo migrates just
  `public.accounts`.
- **EKS**: `demo/k8s` is kind-specific (hostPath storage, kind-loaded images,
  pinned gateway clusterIP, cert SANs baked to the `default` namespace). Running
  this on EKS needs real PVCs/EFS-or-S3 backups, an image registry, and
  clusterIP/CIDR + cert/namespace changes — it is not a drop-in.
