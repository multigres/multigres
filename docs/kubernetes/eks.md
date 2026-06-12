# Getting Started on EKS

This guide describes the EKS prerequisites and reference configuration for
running Multigres with the Kubernetes operator. It is intended as a tested path
for users who already have access to an EKS cluster.

It assumes an existing EKS cluster. Before creating a Multigres cluster, make
sure you have:

- `kubectl` configured for that cluster
- permissions to create namespaces, CRDs, pods, services, PVCs, and secrets
- permissions to install the
  [Multigres operator](https://github.com/multigres/multigres-operator)

This guide does not cover creating an
[EKS cluster](https://docs.aws.amazon.com/eks/latest/userguide/getting-started.html),
installing the
[AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html),
or configuring
[IAM](https://docs.aws.amazon.com/IAM/latest/UserGuide/introduction.html) from
scratch.

## Deployment Shape

Multigres can run on a single Availability Zone or across multiple Availability
Zones, as long as the cluster satisfies the minimum quorum requirements for the
configured topology. Multigres is currently in alpha; for EKS deployments, begin
with the smallest topology that satisfies your availability requirements and
validate operational workflows before expanding the deployment. At minimum,
validate cluster creation, backup and restore, failover behavior, scaling, and
cleanup in your own EKS environment.

The multi-AZ example below uses three cells to show an HA-oriented layout.

## Setup Checklist

Complete these steps before applying the `MultigresCluster` manifest:

| Step                                   | Command or check                                                                                                   |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| Install the Multigres operator         | `kubectl apply -f https://github.com/multigres/multigres-operator/releases/download/v0.1.0/install.yaml`           |
| Choose a namespace                     | `kubectl create namespace multigres-demo`                                                                          |
| Confirm persistent volume provisioning | `kubectl get storageclass`                                                                                         |
| Confirm EKS zone labels                | `kubectl get nodes -L topology.k8s.aws/zone-id,topology.kubernetes.io/zone`                                        |
| Prepare an S3 backup bucket and prefix | Create or select a bucket, choose a unique `keyPrefix`, and grant the backup service account access to that prefix |
| Create the PostgreSQL password secret  | `kubectl create secret generic multigres-admin-password --from-literal=password='<replace-me>'`                    |
| Apply the `MultigresCluster` manifest  | `kubectl apply -f multigres-demo.yaml`                                                                             |
| Verify the cluster with a SQL query    | Run the `psql` check in [Verify Query Serving](#8-verify-query-serving)                                            |

## 1. Install The Operator

Install the operator version that matches the Multigres release you want to
run. For example, for Multigres `v0.1.0`:

```bash
kubectl apply -f https://github.com/multigres/multigres-operator/releases/download/v0.1.0/install.yaml
```

If you are experimenting with the newest available operator release, you can
use:

```bash
kubectl apply -f https://github.com/multigres/multigres-operator/releases/latest/download/install.yaml
```

For reproducible tests, prefer a versioned release URL over `latest`.

Wait for the operator to become ready:

```bash
kubectl get pods -n multigres-operator
```

### Image Selection

The `MultigresCluster` example below intentionally does not set
`spec.images`. When image fields are omitted, the installed operator uses its
default component images for that operator release.

Use the matching operator release for the Multigres release you want to test.
Only set `spec.images` when you deliberately need to override the release
defaults, for example while testing a custom build.

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

After the cluster is created, you can inspect the actual images selected by the
operator:

```bash
kubectl get pods -o jsonpath='{range .items[*]}{.metadata.name}{" "}{range .spec.containers[*]}{.name}{"="}{.image}{" "}{end}{"\n"}{end}'
```

## 2. Choose A Namespace

Create a namespace for the Multigres cluster:

```bash
kubectl create namespace multigres-demo
```

Use the same namespace for the PostgreSQL password secret, backup service
account, and `MultigresCluster` resource.

You can either set the namespace on each command:

```bash
kubectl -n multigres-demo get pods
```

Or make it the default namespace for the current context:

```bash
kubectl config set-context --current --namespace=multigres-demo
```

The rest of this guide assumes your current context is set to the namespace
where you want to deploy Multigres.

## 3. Configure Storage

Multigres poolers require persistent volumes. On EKS, install and configure the
[AWS EBS CSI driver](https://docs.aws.amazon.com/eks/latest/userguide/ebs-csi.html)
before creating a Multigres cluster.

Verify that dynamic volume provisioning works:

```bash
kubectl get storageclass
```

The example below uses a `gp3` StorageClass named:

```text
multigres-gp3
```

If your cluster already has a suitable default StorageClass, you can omit the
explicit storage class from the sample.

## 4. Confirm Zone Labels

Multigres cells map to Kubernetes topology labels.

On EKS, verify that nodes expose AWS zone IDs:

```bash
kubectl get nodes -L topology.k8s.aws/zone-id,topology.kubernetes.io/zone
```

Example:

```text
use1-az1 -> us-east-1a
use1-az2 -> us-east-1b
use1-az6 -> us-east-1d
```

Use the `topology.k8s.aws/zone-id` values for the Multigres `zoneId` fields.

## 5. Prepare S3 Backups

Create or select an S3 bucket for backups.

Use a unique `keyPrefix` for each cluster if you plan to recreate clusters or
run multiple demos in the same bucket.

The backup service account used by Multigres must be able to read, write, list,
and delete objects under the configured backup prefix. On EKS, use
[IAM Roles for Service Accounts (IRSA)](https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html)
to grant the Kubernetes service account access to S3.

The sample manifest below uses a Kubernetes service account named
`multigres-backup` for S3 access.

Create the service account in the same namespace as the `MultigresCluster`:

```bash
kubectl create serviceaccount multigres-backup
```

If you use IRSA, annotate that service account with the IAM role that has S3
access:

```bash
kubectl annotate serviceaccount multigres-backup \
  eks.amazonaws.com/role-arn=arn:aws:iam::<account-id>:role/<multigres-backup-role>
```

At minimum, the backup identity needs access to:

- `s3:ListBucket` on the bucket, scoped to the configured prefix
- `s3:GetObject`
- `s3:PutObject`
- `s3:DeleteObject`

Example IAM policy for a single bucket and prefix:

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

## 6. Create Or Provide The PostgreSQL Password Secret

Create or provide a Kubernetes secret containing the PostgreSQL superuser
password. For a fresh test cluster, choose a password and create the secret
manually:

```bash
kubectl create secret generic multigres-admin-password \
  --from-literal=password='change-this-password'
```

Replace `change-this-password` with the password you want Multigres to use for
the `postgres` user during cluster initialization. The verification command
later in this guide reads the same password from this secret.

If your environment manages secrets through another system, create an
equivalent Kubernetes secret with the same name and key, or update
`postgresPasswordSecretRef` in the manifest to point to your secret.

The sample cluster references:

```yaml
postgresPasswordSecretRef:
  name: multigres-admin-password
  key: password
```

## 7. Create A Multi-AZ Cluster

The manifest below shows a multi-AZ configuration. It defines three cells and
places the shard's orchestration and read-write pool across those cells. It
starts with one pooler per cell.

```yaml
apiVersion: multigres.com/v1alpha1
kind: MultigresCluster
metadata:
  name: multigres-demo
spec:
  postgresPasswordSecretRef:
    name: multigres-admin-password
    key: password

  pvcDeletionPolicy:
    # Useful for repeatable alpha test runs.
    whenDeleted: Delete
    whenScaled: Delete

  backup:
    type: s3
    s3:
      bucket: your-backup-bucket
      region: us-east-1
      keyPrefix: multigres-demo/ # use a unique prefix per cluster
      serviceAccountName: multigres-backup # service account with S3 access

  cells:
    - name: zone-a
      zoneId: use1-az1 # topology.k8s.aws/zone-id value
    - name: zone-b
      zoneId: use1-az2
    - name: zone-c
      zoneId: use1-az6

  databases:
    - name: postgres
      default: true
      tablegroups:
        - name: default
          default: true
          shards:
            - name: 0-inf
              spec:
                durabilityPolicy: AT_LEAST_2 # start with the default alpha policy
                multiorch:
                  cells:
                    - zone-a
                    - zone-b
                    - zone-c
                  replicas: 1
                pools:
                  default:
                    type: readWrite
                    cells:
                      - zone-a
                      - zone-b
                      - zone-c
                    replicasPerCell: 1 # start modestly, then scale after validation
                    fsGroup: 999 # postgres user group in the container
                    storage:
                      size: 10Gi # example size for a test cluster
                      class: multigres-gp3 # omit if using a suitable default class
```

Save the manifest as `multigres-demo.yaml`, then apply it:

```bash
kubectl apply -f multigres-demo.yaml
```

Monitor the cluster:

```bash
kubectl get pods -w
```

In another terminal, check the Multigres resources:

```bash
kubectl get multigresclusters
kubectl get shards
```

## 8. Verify Query Serving

After the cluster is healthy, run a simple SQL query through the Multigres
gateway from inside the Kubernetes cluster.

```bash
POSTGRES_PASSWORD="$(
  kubectl get secret multigres-admin-password \
    -o jsonpath='{.data.password}' | base64 -d
)"

kubectl run psql-client \
  --rm \
  -i \
  --restart=Never \
  --image=postgres:17 \
  --env="PGPASSWORD=${POSTGRES_PASSWORD}" \
  -- psql \
    -h multigres-demo-multigateway \
    -U postgres \
    -d postgres \
    -c 'select 1 as result, current_database(), current_user;'
```

Expected result:

```text
 result | current_database | current_user
--------+------------------+--------------
      1 | postgres         | postgres
```

## Scaling

Scale through the `MultigresCluster` resource. Do not edit generated `Shard`
resources directly.

Example: scale from one pooler per cell to two:

```bash
kubectl patch multigrescluster multigres-demo \
  --type=json \
  -p='[
    {
      "op": "replace",
      "path": "/spec/databases/0/tablegroups/0/shards/0/spec/pools/default/replicasPerCell",
      "value": 2
    }
  ]'
```

## Cleanup

Delete the Multigres cluster:

```bash
kubectl delete multigrescluster multigres-demo
```

> [!WARNING]
> For alpha test deployments, remove state from the previous run before
> recreating a cluster with the same name and backup prefix. Remove only the
> state associated with the deleted cluster:

- persistent volume claims created for the deleted Multigres cluster
- objects under the S3 backup prefix configured for that cluster

Check for PVCs in the namespace where the cluster was deployed:

```bash
kubectl get pvc
```

Remove PVCs for the deleted cluster if any remain:

```bash
kubectl delete pvc -l app.kubernetes.io/instance=multigres-demo
```

Clear only the S3 backup prefix configured for this cluster:

```bash
aws s3 rm s3://your-backup-bucket/multigres-demo/ --recursive
```
