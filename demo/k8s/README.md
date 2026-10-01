Here are the steps to run the demo:

## Prerequisites

- Docker
- kind (Kubernetes in Docker)
- kubectl
- Multigres Docker images built: multigres/multigres, multigres/pgctld-postgres, multigres/multiadmin-web

Step 1: Build Docker images

From the repo root:

```bash
make images
```

This builds the three required images (`multigres/multigres:latest`, `multigres/pgctld-postgres:latest`, `multigres/multiadmin-web:latest`) into your local Docker daemon. The scripts that follow assume they already exist there.

## Step 2: Create a data/ directory

The kind.yaml mounts ./data as a host path into all nodes, so create it first:

```bash
mkdir -p demo/k8s/data
```

## Step 3: Launch infrastructure

From demo/k8s/:

```bash
./launch-infra.sh
```

This will:

- Create a Kind cluster (multidemo) with 1 control-plane + 3 worker nodes
- Set kernel limits on nodes for high-connection workloads
- Load multigres images into the cluster
- Deploy etcd, observability stack (Prometheus, Tempo, Loki, Grafana), cert-manager
- Create pgBackRest TLS certificates
- Deploy multiadmin and multiadmin-web
- Start port-forwards for infra services

Step 4: Launch the multigres cluster

```bash
./launch-multigres-cluster.sh
```

This deploys multipooler, multiorch, and multigateway, waits for them to be ready, then starts port-forwards for the cluster.

The script also applies `k8s-postgres-password-secret.yaml`, which holds the plaintext PostgreSQL superuser password each multipooler/pgctld container reads via `POSTGRES_PASSWORD_FILE`. The committed value (`postgres`) is for the demo only — replace it before reusing this manifest anywhere real.

## Step 5 (optional): Load demo data with supafirehose

> **Note:** `supafirehose` is maintained in a separate repository and is not
> included in this tree. Skip this step unless you have already cloned and
> built it elsewhere.

```bash
# In a clone of the supafirehose repo:
docker build -t supafirehose:latest .

# Then, from demo/k8s/ in this repo:
./start-qs.sh
```

This loads the `supafirehose:latest` image into kind, initializes the DB with
100k users, and deploys supafirehose. Port-forward to access its UI:

```bash
kubectl port-forward svc/supafirehose 8080:8080
```

Access URLs (after port-forwards start)

| Service                       | URL                                                                    |
| ----------------------------- | ---------------------------------------------------------------------- |
| Multiadmin Web UI             | http://localhost:18100                                                 |
| Multiadmin REST API           | http://localhost:18000                                                 |
| Grafana                       | http://localhost:3000/dashboards                                       |
| Prometheus                    | http://localhost:9090                                                  |
| PostgreSQL (via multigateway) | PGPASSWORD=postgres psql -h localhost -p 15432 -U postgres -d postgres |
| Direct pooler (zone1-0)       | psql -h localhost -p 15433 -U postgres                                 |
| Direct pooler (zone1-1)       | psql -h localhost -p 15434 -U postgres                                 |
| Direct pooler (zone1-2)       | psql -h localhost -p 15435 -U postgres                                 |

## Step 6 (Optional): Backup and Restore Demo

Run the interactive backup/restore demo after the cluster is up and port-forwards are running:

```bash
./backup-restore.sh
```

The script walks through:

1. List existing backups (an initial full backup of the primary is created on cluster startup)
2. Perform an incremental backup
3. List backups again to confirm the new backup appears
4. Create an `animals` table and insert 100,000 rows
5. Perform a full backup with the new data
6. List all backups to show the final state

Press ENTER at each step to advance. Press Ctrl+C to exit.

Prerequisites for the backup demo:

- Cluster deployed and healthy (Steps 1–4 above)
- Port-forwards running for both infra and the multigres cluster
- `psql` client installed
- `bin/multigres` binary built (`make build` from repo root)

## Step 7 (Optional): Migration demo

This is a demo for how to migrate a table from a freestanding external PostgreSQL into the Multigres cluster with the Multigres Migrator migration API, then kill the target primary mid-migration to show the coordinator resumes on the newly elected primary, and finally cut a live application over to Multigres — all with no data loss. The full walkthrough (with explanation and the kill-primary/cutover steps) is in [`../migration-demo.md`](../migration-demo.md); the short version:

Prerequisites: cluster up and port-forwards running (Steps 1–4), plus `bin/multigres` (`make build`), `jq`, and — for the dashboard below — `tmux` and `watch` (`brew install tmux watch`).

1. Start and seed the external source (a `postgres:17` container on the kind network, `wal_level=logical`):

```bash
./launch-migration-source.sh
```

The script prints the source's in-cluster address and a ready-to-paste `create-migration` command.

2. Create and start the migration (multiadmin gRPC is port-forwarded on `localhost:18070`); use the in-cluster source address the script printed:

```bash
ID=$(bin/multigres create-migration --admin-server localhost:18070 \
  --source-dsn "host=<SRC_IP> port=5432 user=postgres password=sourcepass dbname=postgres sslmode=disable" \
  --target-database postgres --tables public.accounts | jq -r .id)
bin/multigres start-migration --admin-server localhost:18070 --id "$ID"
```

3. Run the application clients — each in its own terminal:

```bash
./run-appclient.sh write          # continuous writes to the source
./run-appclient.sh watch-gateway  # live summary of the Multigres side (fills in as it streams)
./run-appclient.sh watch-source   # live summary of the source (optional)
```

Or, instead of separate terminals, launch a single 4-pane tmux dashboard — a command pane, a live migrations+state table, the source balance, and the target (Multigres) balance:

```bash
./demo-dashboard.sh
```

The command pane opens with a ready-to-paste `create-migration`/`start`/`drop` cheat sheet (source IP filled in when the source container is up). Run `./run-appclient.sh write` in the command pane (or another terminal) to drive writes.

4. Once streaming, kill the primary pooler pod to show the migration is picked up on the new primary, then cut the app over to Multigres by sending `SIGUSR1` to the writer (`kill -USR1 $(pgrep -f 'appclient write')`). See [`../migration-demo.md`](../migration-demo.md) for details.

Remove the source container when done: `docker rm -f pg-source`.

Step 8: Teardown

```bash
./teardown.sh
```

This kills port-forwards, deletes the Kind cluster, and cleans up the data/ directory. If you ran the migration demo, also remove the source container: `docker rm -f pg-source`.
