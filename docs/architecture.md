# Architecture

This doc assumes that you're familiar with the [High Level Architecture](https://multigres.com/docs/architecture).

## Goals

### Compatibility

We intend to make Multigres as close to a drop-in replacement for PostgreSQL as possible.
However, Multigres is a sharded system with proxies in the front.
It also manages replicas on its own.
This will necessarily change the meaning or usefulness of some commands, hopefully for the better.

We will also need to extend the language to support features related to sharding and replication.

Other than these necessary differences, we will support all other constructs as drop-in replacements.

### Approachability

We intend to make Multigres approachable by making it easy to launch, and by providing intuitive command line parameters with reasonable defaults.
The price to pay for this will be a loss of flexibility. We believe that this is a worthy tradeoff.

### Opinionated

We will focus on functional completeness and ensure that all the features work together well.
To achieve this, we will make tooling decisions that are opinionated.
For example, we'll use etcd for the toposerver.
Other similar tools like zookeeper or consul will not be supported.

## Components

Although Multigres is a distributed system, we work hard at minimizing the number of moving parts.
This usually means that these components will take on secondary responsibilities apart from their primary ones.

### Multigateway

Multigateway's primary responsibility is to provide a PostgreSQL-compatible interface to the user.

- It will discover and keep track of the Multipoolers registered in every cell.
- It will discover and keep track of the current leader for each shard.
- It will analyze incoming queries, break them up into smaller parts to outsource them to various shards, and return the consolidated result to the user.
- It will smoothly redirect traffic to the new leader if there is a failover.

In its full form, Multigateway will emulate a large part of Postgres, especially for post-processing of results that span across multiple shards.

At this point, Multigateway does not have any secondary responsibilities.

### Multipooler

Multipooler's primary responsibility is connection pooling.

Additionally, Multipooler can take backups of the current instance, or restore a backup when a new instance is started.
It implements parts of the Multigres consensus protocol.
It will also provide support for materialization services.

### Pgctld

Pgctld is a lightweight component. Its sole purpose is to allow for Postgres to be run in a different container than the Multipooler.
The different containers allow for the Postgres resources to be provisioned independently from Multipooler.
Multipooler uses pgctld to start and stop Postgres as needed.

### Multiorch

Multiorch's primary responsibility is to manage failovers.

Multiorch also orchestrates the initial bootstrap of a cluster.

### Multiadmin

Multiadmin's primary responsibility is to expose administrative endpoints for cluster management. It serves both HTTP and gRPC APIs used by the `multiadmin` web UI and by operators.

The same `MultiadminService` is exposed three ways:

- gRPC, on the gRPC port. The `multigres` CLI uses this.
- Connect, on the HTTP port. The web UI (`web/multiadmin`, a Next.js app that runs as its own process) uses this.
- REST under `/api/v1`, on the HTTP port, transcoded from the same handlers. See the [Multiadmin REST API](./api/README.md).

Multiadmin reads cluster metadata from the toposervers and forwards operations such as backups, switchovers and migrations to the Multipoolers over gRPC. It also queries Multigateways for diagnostics.

Multiadmin does not have any secondary responsibilities.

### Minigres

Minigres runs a Multigateway and a Multipooler in one process, for a database served by a single pooler with no replicas and no shards. It needs no Multiorch. See [Minigres](./general/minigres.md).

### Multigres CLI

`multigres` is the command-line tool. It provisions and manages a local cluster (`multigres cluster ...`) and is a client of Multiadmin for administrative commands. See [Local cluster and the multigres CLI](./local-cluster.md).

### Operator

The Operator is a Kubernetes Operator. Its primary responsibility is to provision resources for a cluster and bring up all the required Multigres components.

The Operator does not have any secondary responsibilities.

The Operator has no secondary responsibilities in order to ease the development of future operators to allow for Multigres to be deployed in non-Kubernetes environments.

## Metadata

### Toposervers

Toposervers are etcd clusters that store runtime information for a Multigres cluster. There are two types of toposervers:

- **Global toposerver**: This cluster contains the list of cells and the information for the corresponding local toposervers. The global toposerver also contains the list of databases for each cluster. Under each database, it stores the bootstrap durability policy and the backup location.
- **Local toposervers**: There is one local toposerver cluster per cell. The local toposerver is primarily used for component discovery. For example, Multipoolers register themselves with the local toposerver, which allows Multigateways to discover them.

This architecture minimizes cross-cell dependency: It allows each cell to continue to serve traffic even if it is partitioned from the rest of the cluster.

The global and local toposervers are logically separate, but a deployment may place them on the same etcd cluster under different root paths. See [Topology](./topology.md) for the key layout.

### MultiSchema

Sharding related data is stored in Postgres itself. It can be viewed as a logical extension of the Postgres schema that is Multigres specific.

## Data Flow

1. A client connects to a Multigateway using the PostgreSQL wire protocol.
2. The Multigateway routes each query over gRPC to a Multipooler of the target shard: the shard leader, or a replica for connections that arrived on the gateway's [replica-reads port](./query_serving/replica_reads_design.md).
3. The Multipooler runs the query on its local Postgres through a pooled connection.
4. Pgctld starts and stops that Postgres on behalf of the Multipooler.
5. Multiorch watches the Multipoolers of each shard through their health streams and drives leader election and failover. It is not on the query path.
6. Multiadmin and the `multigres` CLI are also off the query path: they read the toposervers and call the other components over gRPC.

## Foundational Dependencies

Multigres components are built on top of the following foundational dependencies:

### gRPC and Protobuf

All communication between Multigres components is done using gRPC. The exceptions are the edges: clients and Postgres speak the PostgreSQL wire protocol, and Multiadmin additionally serves Connect and REST over HTTP. From gRPC, we rely on the following features:

- Stateless connections
- Certificates
- Multiplexing
- Context propagation
- Streaming
- Protobuf based encoding

### Open Telemetry

Open Telemetry is now the industry standard for metrics and tracing. Multigres components use OTel for observability.

### Viper

Viper is used for configuration management, through the `go/tools/viperutil` wrapper. Command line parsing uses Cobra and pflag; flags are bound to Viper keys so that they can also be set from a config file.

### servenv

Servenv is a Multigres framework that standardizes all server components of Multigres by consolidating and unifying components like grpc, etc.

### mterrors

mterrors provides a unified error handling framework.

## Code Structure

Some packages contain a `README.md` file that explains what the package is meant for. The following directories are noteworthy:

- `go/cmd` contains the main programs for all the binaries: `multigateway`, `multipooler`, `pgctld`, `multiorch`, `multiadmin`, `minigres`, the `multigres` CLI, and `portpoolserver` (a port allocator used only by integration tests). It can depend on anything.
- `go/services/<component>` (eg `go/services/multigateway`) contains component specific logic. It cannot depend on `go/cmd` or on another service.
- `go/common` contains common code (like `servenv`) that is shared among all components. It cannot depend on `go/cmd` or `go/services`.
- `go/tools` contains generic utilities that are not Multigres specific. It cannot depend on code outside `go/tools`.
- `go/provisioner` contains the provisioner used by `multigres cluster` to run a local cluster. Outside its own tree it may only depend on `go/tools`, `go/pb`, `go/common/constants`, and `go/common/topoclient`.
- `go/test/endtoend` contains the integration tests.
- `proto` contains the protobuf definitions; the generated Go code is in `go/pb`.
- `web/multiadmin` contains the Multiadmin web UI.
