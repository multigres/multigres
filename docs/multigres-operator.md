# Multigres Operator

## What is an operator?

Multigres is deployed using a Kubernetes operator. An operator is a Kubernetes controller that runs inside your cluster and manages the lifecycle of a complex application on your behalf. Instead of manually applying configuration changes, scaling resources, or handling failures, you declare the state you want and the operator continuously works to make it so.

The Multigres operator is what turns a single YAML declaration into a fully orchestrated, multi-zone Postgres cluster.

## What the operator manages

The operator manages pods — the individual containers running each component — directly, rather than delegating to Kubernetes StatefulSets. This means it can be primary-aware: it knows which pod holds the active Postgres primary at any given moment.

Primary-awareness ensures the operator always acts on standbys first and the primary last when restarting pods for a config change or scaling event, guaranteeing safe operations without interrupting writes.

A Multigres cluster is made up of several components that work together across availability zones. The operator provisions all of the following from a single `MultigresCluster` manifest:

- **GlobalTopoServer** — A managed etcd cluster that records topology state: which databases exist, which cells they live in, and where every component is registered. In a multi-zone deployment, the topology splits into a global server for cluster-wide state and per-cell servers for local discovery, so a partitioned cell keeps operating against its own local view.
- **MultiAdmin** — The management plane for the cluster, including a web UI.
- **MultiGateway** — Speaks the Postgres wire protocol. Applications connect to a gateway, which forwards queries to the right MultiPooler over gRPC. Adding more gateways scales connection capacity horizontally.
- **MultiOrch** — The orchestrator. One set of MultiOrch instances per shard, running across cells. Watches replication health, appoints leaders through a consensus protocol, runs failovers, and coordinates bootstrap. When you apply the manifest, MultiOrch runs bootstrap as a consensus-backed election — the same code path as every later failover — so there is no separate provisioning script that can race with itself.
- **Pools** — The Postgres pods themselves, managed by pgctld (which owns the local Postgres process) and MultiPooler (which owns the connection pool). One pooler per Postgres instance.

### Prerequisites

Before deploying a Multigres cluster on AWS EKS, ensure the following are in place:

- **AWS CLI v2** — installed and configured
- **kubectl** — configured for the target EKS cluster
- **Permissions** — to create namespaces, CRDs, pods, services, PVCs, secrets, and to install the Multigres operator

---

# Getting Started on EKS

This guide does not cover creating an [EKS cluster](https://docs.aws.amazon.com/eks/latest/userguide/getting-started.html), installing the [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html), or configuring [IAM](https://docs.aws.amazon.com/IAM/latest/UserGuide/introduction.html) from scratch.

## Deployment Shape

Multigres can run on a single Availability Zone or across multiple Availability Zones, as long as the cluster satisfies the minimum quorum requirements for the configured topology.

> **Note:** For EKS deployments, begin with the smallest topology that satisfies your availability requirements and validate operational workflows before expanding the deployment. At minimum, validate cluster creation, backup and restore, failover behavior, scaling, and cleanup in your own EKS environment.

## Setup Checklist

Complete these steps before applying the `MultigresCluster` manifest:

| Step | Command or check |
|------|-----------------|
| Install the Multigres operator | `kubectl apply -f https://github.com/multigres/multigres-operator/releases/download/v0.1.0/install.yaml` |
| Choose a namespace | `kubectl create namespace multigres-demo` |
| Confirm persistent volume provisioning | `kubectl get storageclass` |
| Confirm EKS zone labels | `kubectl get nodes -L topology.k8s.aws/zone-id,topology.kubernetes.io/zone` |
| Prepare an S3 backup bucket and prefix | Create or select a bucket, choose a unique `keyPrefix`, and grant the backup service account access to that prefix |
| Create the PostgreSQL password secret | `kubectl create secret generic multigres-admin-password --from-literal=password='<replace-me>'` |
| Apply the `MultigresCluster` manifest | `kubectl apply -f multigres-demo.yaml` |
| Verify the cluster with a SQL query | Run the `psql` check in [Step 4: Verify the Cluster](#4-verify-the-cluster) |

## Step 1: Install the Operator

Install the operator version that matches the Multigres release you want to run. For v0.1.0 Multigres Alpha:

```bash
kubectl apply -f https://github.com/multigres/multigres-operator/releases/download/v0.1.0/install.yaml
```

To use the latest available release:

```bash
kubectl apply -f https://github.com/multigres/multigres-operator/releases/latest/download/install.yaml
```

> **Note:** For reproducible deployments, prefer a versioned release URL over `latest`.

Wait for the operator to become ready:

```bash
kubectl get pods -n multigres-operator
```

### Image Selection

The `MultigresCluster` example below intentionally does not set `spec.images`. When image fields are omitted, the installed operator uses its default component images for that operator release.

Use the matching operator release for the Multigres release you want to test. Only set `spec.images` when you deliberately need to override the release defaults, for example while testing a custom build.

Optional image override shape:

```yaml
spec:
  images:
    multigateway: ghcr.io/multigres/multigres:<tag-or-digest>
    multiorch: ghcr.io/multigres/multigres:<tag-or-digest>
    multipooler: ghcr.io/multigres/multigres:<tag-or-digest>
    multiadmin: ghcr.io/multigres/multigres:<tag-or-digest>
    multiadminWeb: ghcr.io/multigres/multiadmin-web:<tag-or-digest>
    postgres: ghcr.io/multigres/pgctld:<tag-or-digest>
```

After the cluster is created, you can inspect the actual images selected by the operator:

```bash
kubectl get pods -o jsonpath='{range .items[*]}{.metadata.name}{" "}{range .spec.containers[*]}{.name}{"="}{.image}{" "}{end}{"\n"}{end}'
```

## Step 2: Choose a Namespace

Create a namespace for the Multigres cluster:

```bash
kubectl create namespace multigres-demo
```

Use the same namespace for the PostgreSQL password secret, backup service account, and `MultigresCluster` resource. Set it as your default context namespace:

```bash
kubectl config set-context --current --namespace=multigres-demo
```

## Step 3: Configure Storage

Multigres poolers require persistent volumes. On EKS, install and configure the AWS EBS CSI driver before creating a Multigres cluster. Verify that dynamic volume provisioning is available:

```bash
kubectl get storageclass
```

The reference manifest uses a `gp3` StorageClass named `multigres-gp3`. If your cluster already has a suitable default StorageClass, you can omit the explicit storage class from the manifest.

## Step 4: Confirm Zone Labels

Multigres cells map to Kubernetes topology labels. On EKS, verify that nodes expose AWS zone IDs:

```bash
kubectl get nodes -L topology.k8s.aws/zone-id,topology.kubernetes.io/zone
```

Use the `topology.k8s.aws/zone-id` values for the Multigres `zoneId` fields in the manifest. The supported zones for the shared demo cluster are:

| Zone ID | AWS AZ |
|---------|--------|
| use1-az1 | us-east-1a |
| use1-az2 | us-east-1b |
| use1-az6 | us-east-1d |

## Step 5: Prepare S3 Backups

Create or select an S3 bucket for backups. Use a unique `keyPrefix` for each cluster if you plan to recreate clusters or run multiple clusters in the same bucket.

The backup service account must be able to read, write, list, and delete objects under the configured backup prefix. On EKS, use IAM Roles for Service Accounts (IRSA) to grant the Kubernetes service account access to S3.

Create the service account:

```bash
kubectl create serviceaccount multigres-backup
```

If using IRSA, annotate the service account with the IAM role that has S3 access:

```bash
kubectl annotate serviceaccount multigres-backup \
  eks.amazonaws.com/role-arn=arn:aws:iam::<account-id>:role/<multigres-backup-role>
```

At minimum, the backup identity needs the following S3 permissions:

- `s3:ListBucket` and `s3:GetBucketLocation` on the bucket, scoped to the configured prefix
- `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` on objects under the prefix

Example IAM policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": "arn:aws:s3:::your-backup-bucket",
      "Condition": {
        "StringLike": {
          "s3:prefix": ["multigres-demo/", "multigres-demo/*"]
        }
      }
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::your-backup-bucket/multigres-demo/*"
    }
  ]
}
```

## Step 6: Create the PostgreSQL Password Secret

Create a Kubernetes secret containing the PostgreSQL superuser password:

```bash
kubectl create secret generic multigres-admin-password \
  --from-literal=password='change-this-password'
```

Replace `change-this-password` with the password you want Multigres to use for the `postgres` user during cluster initialization. The manifest references this secret via:

```yaml
postgresPasswordSecretRef:
  name: multigres-admin-password
  key: password
```

---

# Deploying the Operator on EKS

[![Deploying the Multigres Operator on EKS](https://img.youtube.com/vi/ds0bdNlaAoQ/maxresdefault.jpg)](https://www.youtube.com/watch?v=ds0bdNlaAoQ)

## 1. Configure Kubernetes Access

Update your kubeconfig to point at the EKS cluster:

```bash
aws eks update-kubeconfig \
  --name multigres-dev \
  --region us-east-1 \
  --profile eks-demo
```

Set your default namespace:

```bash
kubectl config set-context --current --namespace=eks-demo
```

Verify access:

```bash
kubectl get pods
kubectl get multigresclusters
kubectl get coretemplates,celltemplates,shardtemplates
```

You should see no running pods, no clusters, and the default templates present. The operator is installed and ready.

## 2. Create the Cluster Manifest

Create a file named `demo-multi-az.yaml` with the following contents. Replace placeholder values as indicated:

```yaml
apiVersion: multigres.com/v1alpha1
kind: MultigresCluster
metadata:
  name: <CLUSTER_NAME>
  namespace: <NAMESPACE>
spec:
  pvcDeletionPolicy:
    whenDeleted: Delete
    whenScaled: Delete

  durabilityPolicy: AT_LEAST_2

  templateDefaults:
    coreTemplate: <CORE_TEMPLATE>
    cellTemplate: <CELL_TEMPLATE>
    shardTemplate: <SHARD_TEMPLATE>

  postgresPasswordSecretRef:
    name: <PASSWORD_SECRET_NAME>
    key: password

  backup:
    type: filesystem
    filesystem:
      path: /backups
      storage:
        size: <BACKUP_STORAGE_SIZE>

  images:
    imagePullPolicy: Always

    # Same Multigres runtime image used by all Multigres components.
    multiadmin: <ECR_REGISTRY>/multigres:<IMAGE_TAG>
    multigateway: <ECR_REGISTRY>/multigres:<IMAGE_TAG>
    multiorch: <ECR_REGISTRY>/multigres:<IMAGE_TAG>
    multipooler: <ECR_REGISTRY>/multigres:<IMAGE_TAG>

    # pgctld/Postgres image.
    postgres: <ECR_REGISTRY>/pgctld:<IMAGE_TAG>

    # Web UI image.
    multiadminWeb: ghcr.io/multigres/multiadmin-web:<MULTIADMIN_WEB_TAG>

  cells:
    - name: <CELL_NAME_A>
      zoneId: <ZONE_ID_A>
    - name: <CELL_NAME_B>
      zoneId: <ZONE_ID_B>
    - name: <CELL_NAME_C>
      zoneId: <ZONE_ID_C>

  databases:
    - name: postgres
      default: true
      backup:
        type: s3
        s3:
          bucket: <S3_BUCKET>
          region: <AWS_REGION>
          keyPrefix: <S3_KEY_PREFIX>
          serviceAccountName: <BACKUP_SERVICE_ACCOUNT>
      tablegroups:
        - name: default
          default: true
          shards:
            - name: 0-inf
              spec:
                pools:
                  default:
                    replicasPerCell: <REPLICAS_PER_CELL>
                    storage: {}
                    multipooler:
                      resources: {}
                    postgres:
                      resources: {}
                multiorch:
                  resources: {}
```

The manifest defines three cells, each mapped to a real AWS availability zone. `AT_LEAST_2` durability means every committed write is acknowledged by at least one standby before the client receives confirmation. The operator infers resource limits, gateway configuration, and topo server sizing from the referenced templates.

## 3. Apply the Manifest

```bash
kubectl apply -f demo-multi-az.yaml
```

Watch the pods come up:

```bash
kubectl get pods -w
```

You should eventually see pods for:

- `global-topo`
- `multiadmin`
- `multigateway` (one per zone)
- `multiorch` (one per zone)
- pool pods (one per zone)

Within a minute the cluster should be fully running.

## 4. Verify the Cluster

Check the Multigres resources:

```bash
kubectl get multigresclusters
kubectl get shards
```

Check the pods:

```bash
kubectl get pods
```

A healthy cluster with minimum size should have one primary and two replicas. Run this to check directly:

```bash
PW=$(kubectl get secret <PASSWORD_SECRET_NAME> -o jsonpath='{.data.password}' | base64 -d)

for p in $(kubectl get pods -o name | grep '<CLUSTER_NAME>-postgres-default-0-inf'); do
  echo "== $p =="
  kubectl exec "$p" -c postgres -- env PGPASSWORD="$PW" \
    psql -h /var/lib/pooler/pg_sockets \
    -U postgres -d postgres -Atc \
    "select case when pg_is_in_recovery() then 'replica' else 'primary' end"
done
```

Expected result:

```
one primary
two replicas
```

### Verify Query Serving via MultiGateway

```bash
kubectl run psql-client \
  --rm -it --restart=Never \
  --image=postgres:17 \
  --env="PGPASSWORD=$(kubectl get secret <PASSWORD_SECRET_NAME> -o jsonpath='{.data.password}' | base64 -d)" \
  -- psql -h <CLUSTER_NAME>-multigateway \
  -U postgres -d postgres \
  -c "select 1, current_database(), current_user;"
```

Expected result:

```
 ?column? | current_database | current_user
----------+------------------+--------------
        1 | postgres         | postgres
```

## 5. Delete and Recreate a Cluster

Use this if you want to start over.

Delete the cluster:

```bash
kubectl delete multigrescluster <CLUSTER_NAME> --ignore-not-found
```

Wait until the old pods disappear:

```bash
kubectl get pods | grep <CLUSTER_NAME> || true
```

Delete all PVCs for the cluster, including Postgres and topo/etcd data:

```bash
kubectl get pvc -o name \
  | grep 'persistentvolumeclaim/data-<CLUSTER_NAME>-' \
  | xargs -r kubectl delete
```

Verify no PVCs remain:

```bash
kubectl get pvc | grep <CLUSTER_NAME> || true
```

Clear the S3 backup prefix before recreating a cluster with the same name and prefix:

```bash
aws s3 rm s3://<S3_BUCKET>/<S3_KEY_PREFIX> --recursive
```

Verify the S3 prefix is empty:

```bash
aws s3 ls s3://<S3_BUCKET>/<S3_KEY_PREFIX> --recursive
```

Apply the manifest again:

```bash
kubectl apply -f <MANIFEST_FILE>.yaml
```

## 6. How Applications Connect

Applications running inside the Kubernetes cluster connect to the Multigres gateway Service:

```
postgresql://postgres:<password>@demo-multi-az-multigateway:5432/postgres
```

If the application runs in another namespace, use the full Service name:

```
postgresql://postgres:<password>@demo-multi-az-multigateway.eks-demo.svc.cluster.local:5432/postgres
```

> **Note:** `*.svc.cluster.local` names are Kubernetes-internal DNS names and will not resolve from your laptop shell.

## 7. Scaling

To start:

```yaml
replicasPerCell: 1
```

This creates one pooler per cell — three poolers total across three AZs.

To scale, edit the `MultigresCluster` directly. Do not edit the generated `Shard`.

Example:

```bash
kubectl patch multigrescluster <CLUSTER_NAME> \
  --type=json \
  -p='[
    {
      "op": "replace",
      "path": "/spec/databases/0/tablegroups/0/shards/0/spec/pools/default/replicasPerCell",
      "value": 2
    }
  ]'
```

Set the value back to `1` for the basic demo shape.
