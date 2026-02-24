Here are the steps to run the demo:

Prerequisites

- Docker
- kind (Kubernetes in Docker)
- kubectl
- Multigres Docker images built: multigres/multigres, multigres/pgctld-postgres, multigres/multiadmin-web

Step 1: Build Docker images

Build the multigres images before starting (from the repo root — the scripts assume they already exist in your local Docker daemon).

Step 2: Create a data/ directory

The kind.yaml mounts ./data as a host path into all nodes, so create it first:

mkdir -p demo/k8s/data

Step 3: Launch infrastructure

From demo/k8s/:

./launch-infra.sh

This will:

- Create a Kind cluster (multidemo) with 1 control-plane + 3 worker nodes
- Set kernel limits on nodes for high-connection workloads
- Load multigres images into the cluster
- Deploy etcd, observability stack (Prometheus, Tempo, Loki, Grafana), cert-manager
- Create pgBackRest TLS certificates
- Deploy multiadmin and multiadmin-web
- Start port-forwards for infra services

Step 4: Launch the multigres cluster

./launch-multigres-cluster.sh

This deploys multipooler, multiorch, and multigateway, waits for them to be ready, then starts port-forwards for the cluster.

Step 5 (optional): Load demo data with supafirehose

# First build the supafirehose image (in the supafirehose directory):

docker build -t supafirehose:latest .

# Then run from demo/k8s/:

./start-qs.sh

This loads the image into kind, initializes the DB with 100k users, and deploys supafirehose. Port-forward to access its UI:
kubectl port-forward svc/supafirehose 8080:8080

Access URLs (after port-forwards start)

┌───────────────────────────────┬────────────────────────────────────────────────────────────────────────┐
│ Service │ URL │
├───────────────────────────────┼────────────────────────────────────────────────────────────────────────┤
│ Multiadmin Web UI │ http://localhost:18100 │
├───────────────────────────────┼────────────────────────────────────────────────────────────────────────┤
│ Multiadmin REST API │ http://localhost:18000 │
├───────────────────────────────┼────────────────────────────────────────────────────────────────────────┤
│ Grafana │ http://localhost:3000/dashboards │
├───────────────────────────────┼────────────────────────────────────────────────────────────────────────┤
│ Prometheus │ http://localhost:9090 │
├───────────────────────────────┼────────────────────────────────────────────────────────────────────────┤
│ PostgreSQL (via multigateway) │ PGPASSWORD=postgres psql -h localhost -p 15432 -U postgres -d postgres │
├───────────────────────────────┼────────────────────────────────────────────────────────────────────────┤
│ Direct pooler (zone1-0) │ psql -h localhost -p 15433 -U postgres │
├───────────────────────────────┼────────────────────────────────────────────────────────────────────────┤
│ Direct pooler (zone1-1) │ psql -h localhost -p 15434 -U postgres │
├───────────────────────────────┼────────────────────────────────────────────────────────────────────────┤
│ Direct pooler (zone1-2) │ psql -h localhost -p 15435 -U postgres │
└───────────────────────────────┴────────────────────────────────────────────────────────────────────────┘

Step 6 (Optional): Backup and Restore Demo

Run the interactive backup/restore demo after the cluster is up and port-forwards are running:

./backup-restore.sh

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

Step 7: Teardown

./teardown.sh

This kills port-forwards, deletes the Kind cluster, and cleans up the data/ directory.
