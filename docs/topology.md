# Topology

The topology is the metadata store every Multigres component uses to find the
cluster's cells, databases, and running components. The client library is
`go/common/topoclient`. [Architecture](./architecture.md#toposervers) explains
why the store is split into a global and a per-cell part; this doc describes how
the data is laid out and accessed.

## Backends

A backend implements the `topoclient.Conn` interface and registers a `Factory`.
Two exist:

- `etcd` (`go/common/topoclient/etcdtopo`) — the only backend the binaries use.
  `TopoConfig.Open` always opens `topoclient.DefaultTopoImplementation`, which
  is `etcd`.
- `memory` (`go/common/topoclient/memorytopo`) — an in-process store for tests.

Binaries pull both in by importing `go/common/plugins/topo`.

## Connecting

Every service takes the same flags, registered by `TopoConfig.RegisterFlags`:

| Flag                             | Default | Meaning                                               |
| -------------------------------- | ------- | ----------------------------------------------------- |
| `--topo-global-server-addresses` | (none)  | Addresses of the global topology server. Required.    |
| `--topo-global-root`             | (none)  | Root path of the global topology data. Required.      |
| `--topo-read-concurrency`        | `32`    | Maximum concurrent reads per global or cell topology. |
| `--lock-timeout`                 | `45s`   | Maximum time to wait when acquiring a topology lock.  |
| `--remote-operation-timeout`     | `15s`   | Time to wait for a remote operation.                  |

The etcd backend adds `--topo-etcd-lease-ttl` (default `30` seconds) and the
client TLS flags `--topo-etcd-tls-cert`, `--topo-etcd-tls-key`, and
`--topo-etcd-tls-ca`.

A process is only told where the global topology is. To reach a cell,
`Store.ConnForCell` reads that cell's `Cell` record from the global topology and
connects to the `server_addresses` and `root` stored in it. Cell connections are
cached and replaced when the record changes. If a connection cannot be
established, the store keeps retrying in the background.

## Layout

Each connection has a root path; every path below is relative to it. In etcd,
the key is the root joined with the relative path, and the value is the
serialized protobuf message from `proto/clustermetadata.proto`.

The global and cell topologies can live on the same etcd cluster, because they
use different roots. The local provisioner does this: its default global root
is `/multigres/global` and its cells use `/multigres/zone1`, `/multigres/zone2`,
and `/multigres/zone3`.

### Global topology

| Path                                                       | Contents                                                                                |
| ---------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| `cells/<cell>/Cell`                                        | `Cell`: the server addresses and root of the cell's topology.                           |
| `databases/<database>/Database`                            | `Database`: backup location, the cells it runs in, and the bootstrap durability policy. |
| `databases/<database>/<tablegroup>/<shard>/ShardInitClaim` | `ShardInitClaim`: which Multiorch initialized the shard, and with which cohort.         |
| `databases/<database>/TableGroupClaims/<id>`               | `TableGroupProvisionClaim`: a provisioner holds resources for this tablegroup id.       |
| `gateway_prefixes/<prefix>`                                | The Multigateway that owns this PID prefix, used to route query cancels. Ephemeral.     |
| `backups/<database>/<tablegroup>/<shard>/lock`             | The backup lease of a shard.                                                            |

`ShardInitClaim` is written with a create-if-absent, so two Multiorchs can never
initialize the same shard with different cohorts. The backup lease has a 30
second TTL that its holder keeps alive; an on-demand backup may steal it from
the current holder.

### Cell topology

| Path                                          | Contents       | Lifetime                                  |
| --------------------------------------------- | -------------- | ----------------------------------------- |
| `poolers/multipooler-<cell>-<name>/Pooler`    | `Multipooler`  | Persistent. Removed when unregistered.    |
| `gateways/multigateway-<cell>-<name>/Gateway` | `Multigateway` | Ephemeral. Expires when the process dies. |
| `orchs/multiorch-<cell>-<name>/Orch`          | `Multiorch`    | Ephemeral. Expires when the process dies. |

The directory name is `topoclient.ComponentIDString` of the component's `ID`
(component type, cell, name).

Ephemeral records are bound to an etcd lease of `--topo-etcd-lease-ttl` seconds,
which the owning process keeps alive. Their owners write them again
periodically, so a record that was lost during an outage reappears.

Multipooler records are not ephemeral. A Multipooler that shuts down leaves its
record in place and marks it with a lifecycle status instead; see the
`PoolerLifecycleStatus` enum in `proto/clustermetadata.proto`.

## Who reads and writes what

- Cells and databases are created by whatever provisions the cluster: the local
  provisioner for `multigres cluster start`, or
  `multigres createclustermetadata`, which is meant to be invoked by the
  operator.
- Multipoolers, Multigateways, and Multiorchs register themselves in their own
  cell.
- Multigateways and Multiorchs watch the `poolers` directory of every cell
  (`go/common/topoclient/poolerwatch`) to discover Multipoolers.
- Multiadmin reads both levels to answer its API.

## Inspecting a cluster

Go through Multiadmin, using the `multigres` CLI:

```bash
multigres getcellnames
multigres getdatabase --name postgres
multigres getpoolers --cells zone1
```

For a local cluster you can also read etcd directly. Values are binary
protobuf, so list the keys only:

```bash
etcdctl get /multigres --prefix --keys-only
```
