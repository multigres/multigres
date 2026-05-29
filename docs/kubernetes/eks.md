# Getting Started on EKS

This guide describes the EKS prerequisites and reference configuration for
running Multigres with the Kubernetes operator.

It assumes an existing EKS cluster. Before creating a Multigres cluster, make
sure you have:

- `kubectl` configured for that cluster
- permissions to create namespaces, CRDs, pods, services, PVCs, and secrets
- the [Multigres operator](https://github.com/multigres/multigres-operator)
  installed

## Deployment Shape

Multigres can run on a single Availability Zone or across multiple Availability
Zones, as long as the cluster satisfies the minimum quorum requirements for the
configured topology. Multigres is currently in alpha; for EKS deployments, begin
with the smallest topology that satisfies your availability requirements and
validate operational workflows before expanding the deployment. At minimum,
validate cluster creation, backup and restore, failover behavior, scaling, and
cleanup in your own EKS environment.

The multi-AZ example below uses three cells to show an HA-oriented layout.

## Cluster Requirements

### 1. EBS CSI Driver

Multigres poolers require persistent volumes. On EKS, install and configure the
AWS EBS CSI driver before creating a Multigres cluster.

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

### 2. Zone Labels

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

### 3. S3 Backup Bucket

Create or select an S3 bucket for backups.

The backup identity used by Multigres must be able to read, write, list, and
delete objects under the configured backup prefix.

At minimum, the backup identity needs access to:

- `s3:ListBucket` on the bucket, scoped to the configured prefix
- `s3:GetObject`
- `s3:PutObject`
- `s3:DeleteObject`

### 4. PostgreSQL Password Secret

Create a Kubernetes secret containing the PostgreSQL superuser password:

```bash
kubectl create secret generic multigres-admin-password \
  --from-literal=password='<replace-me>'
```

The sample cluster references:

```yaml
postgresPasswordSecretRef:
  name: multigres-admin-password
  key: password
```

## Example Multi-AZ Cluster

The manifest below shows a multi-AZ configuration. It defines three cells and
places the shard's orchestration and read-write pool across those cells.

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
    whenDeleted: Delete
    whenScaled: Delete

  backup:
    type: s3
    s3:
      bucket: your-backup-bucket
      region: us-east-1
      keyPrefix: multigres-demo/
      serviceAccountName: multigres-backup

  cells:
    - name: zone-a
      zoneId: use1-az1
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
                durabilityPolicy: AT_LEAST_2
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
                    replicasPerCell: 1
                    fsGroup: 999
                    storage:
                      size: 10Gi
                      class: multigres-gp3
```

Apply it:

```bash
kubectl apply -f multigres-demo.yaml
```

Monitor the cluster:

```bash
kubectl get pods -w
kubectl get multigresclusters
kubectl get shards
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
> Multigres is currently in alpha; for repeated EKS test deployments that reuse
> the same cluster name and backup prefix, remove state from the previous run
> before applying the manifest again. Remove only the state associated with the
> deleted cluster:

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
