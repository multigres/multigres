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

We will focus on functional completeness and ensure that all the features gel together well.
To achieve this, we will make tooling decisions that are opinionated.
For example, we'll use etcd for the toposerver.
Other similar tools like zookeeper or consul will not be supported.

## Components

Multigres is designed to run with a minimal set of loosely coupled components.
This usually means that these components will take on secondary responsibilities apart from their primary ones.

### MultiGateway

MultiGateway's primary responsibility is to provide a PostgreSQL-compatible interface to the user.

* It will discover and keep track of all MultiPoolers within the current cell.
* It will discover and keep track of the current primary for each shard.
* It will analyze incoming queries, break them up into smaller parts to outsource them to various shards, and return the consolidated result to the user.
* It will smoothly redirect traffic to the new primary if there is a failover.

In its full form, MultiGateway will emulate a large part of Postgres, especially for post-processing of results that span across multiple shards.

At this point, MultiGateway does not have any secondary responsibilities.

### MultiPooler

MultiPooler's primary responsibility is connection pooling.

Additionally, MultiPooler can take backups of the current instance, or restore a backup when a new instance is started.
It implements parts of the Multigres consensus protocol.
It will also provide support for materialization services.

### Pgctld

Pgctld is a lightweight component. Its sole purpose is to allow for Postgres to be run in a different container than the MultiPooler.
The different containers allow for the Postgres resources to be provisioned independently from MultiPooler.
MultiPooler uses pgctld to start and stop Postgres as needed.

### MultiOrch

MultiOrch's primary responsibility is to manage failovers.

MultiOrch also orchestrates the initial bootstrap of a cluster.

### Operator

The Operator is a Kubernetes Operator. Its primary responsibility is to provision resources for a cluster and bring up all the required Multigres components.

The Operator does not have any secondary responsibilities.

The Operator has no secondary responsibilities in order to ease the development of future operators to allow for Multigres to be deployed in non-Kubernetes environments.

## Metadata

### Toposervers

Toposervers are etcd clusters that store runtime information for a Multigres cluster. There are two types of toposervers:

- **Global toposerver**: This cluster contains the list of cells and the information for the corresponding local toposervers. The global toposerver also contains the list of databases for each cluster. Under each database, it stores the durability policy and the backup location.
- **Local toposervers**: There is one local toposerver cluster per cell. The local toposerver is primarily used for component discovery. For example, MultiPoolers register themselves with the local toposerver, which allows MultiGateways to discover them.

This architecture minimizes cross-cell dependency: It allows each cell to continue to serve traffic even if it is partitioned from the rest of the cluster.

### MultiSchema

Sharding  elated data is stored in Postgres itself. It can be viewed as a logical extension of the Postgres schema that is Multigres  pecific.

## Foundational Dependencies

Multigres components are built on top of the following foundational dependencies:

### gRPC and Protobuf

All interprocess communication is done using gRPC. From gRPC, we rely on the following features:

- Stateless connections
- Certificates
- Multiplexing
- Context propagation
- Streaming
- Protobuf based encoding

### Open Telemetry

Open Telemetry is now the industry standard for metrics and tracing. Multigres components use OTel for observability.

### Viper

Viper is used for command line parsing and configuration management.

### servenv

Servenv is a Multigres framework that standardizes all server components of Multigres by consolidating and unifying components like grpc, etc.

### mterrors

mterrors provides a unified error handling framework.

## Code Structure

Every go package contains a detailed `README.md` file that explains what the package is meant for. The following directories are noteworthy:

- `go/cmd` contains the main programs for all the binaries
- `go/<component>` (eg `multigateway`) contains component specific logic
- `go/common` contains common code (like `servenv`) that is shared among all components
