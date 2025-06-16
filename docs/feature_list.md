# Upcoming features
This is a losely ordered tentative list of features we intend to build or import from Vitess. Please refer to the project plan (when ready) for details on how they'll be implemented.

## Proxy layer and connection pooling
Multigres will have a two-level proxy layer. In the case of a single small server, the primary benefit of these two layers would be connection pooling.

### VTGate
VTGate is the top layer. In its simplest form, it will masquerade as a Postgres server. When it receives a query, it will forward the request to the next (VTTablet) layer. In the case of a single Postgres server, its primary usefulness is to shield the clients from restarts or failovers that may happen in the underlying layers due to software rollouts and failures.

In the case of master-replica configurations, VTGate can be configured to send read traffic to the replicas.

The VTGate layer could be conceptualized as the compute layer of Multigres. It can be horizontally scaled as needed.

### VTTablet
There will be one VTTablet per Postgres instance. VTTablet's primary function is to provide connection pooling. It will be aware of transaction state and connection specific changes of state, and will preserve the correct behavior to the clients connected at the VTGate level.

Each VTTablet, its associated Postgres instance, and its storage are treated as one unit. This trio can be conceptualized as one node in the storage layer. In this layer, data can be replicated and/or sharded across other (trio) nodes as needed. Traditional storage layers typically implement a file or object store API. In the case of Multigres, the storage layer API is that of a database.

## Sharding
As the data grows, you will soon encounter the need to split some tables into shards. When this need arises, Multigres will manage the workflows to split your monolithic Postgres instance into smaller parts. Typically, the smaller tables will remain in the original Postgres database, and the bigger tables will be migrated out into multiple (sharded) databases.

Multigres will provide a powerful relational sharding model that will help you keep tables along with their related rows in the same shard, providing optimum efficiency and performance as your data grows.

TODO: Doc on Vitess sharding model

### VTGate
In a sharded setup, VTGate's functionality will expand to present this distributed cluster as if it was a single Postgres server. For simpler queries, it will just act as a routing layer by sending it to where the data is. For more complex queries, it will act as a database engine while maximally leveraging the capabilities of the individual databases underneath.

VTGate will have the ability to push entire join queries into an underlying shard if it determines that all data for that join is within that shard. Similarly, VTGate will "scatter" entire joins across all shards if it determines that the related rows of a join are within their respective shards.

### VTTablet
The query serving functionality of VTTablet will have no awareness of sharding. It will just use the underlying Postgres instance to serve the requested queries. However, VTTablet will be the workhorse behind facilitating all the resharding efforts.

When the need to reshard arises, new (target) VTTablets will be created to receive filtered data from the original unsharded table. This data will be streamed by the source VTTablet. Once these tables are populated and up-to-date, a failover will be performed to move query traffic to the sharded VTTablets. For safety, the replication will be reversed. If any issue is found after the cut-over, it can be undone by switching traffic back to the source tables.

This failover and fail-back can be repeatedly performed without requiring any change in the application.

### 2PC
Due to the flexible sharding scheme of Multigres, you should be able minimize or completely eliminate the need for distributed transactions by a careful selection of an optimal sharding scheme. However, if the need arises, VTGate and VTTablet will work together and use the two-phase commit protocol supported by Postgres to complete transactions that span across multiple instances.

TODO: Doc on 2PC atomicity and isolation

## VTOrc: NVME performance, durability and High Availability

Multigres allows for the Postgres data files to be stored on the local NVME. For an OLTP system like Multigres, a local NVME has substantial advantages over a mounted drive:
* IOPS are free
* Lower latency, by one order of magnitude

These advantages translate into higher performance and reduced cost.

Multigres will implement a consensus protocol to ensure that transactions satisfy the required durability policy. This will be achieved using a two-phase sync to replicate the WAL to the required number of replicas in the quorum. This functionality eliminates the need to rely on a mounted (and replicated) file system like EBS to safeguard from data loss.

In case of a primary node failure, VTOrc (Orchestrator) will promote an existing replica to primary, ensuring that it contains all the committed transactions of the previous primary. Following this, Multigres will resume serving of traffic using the new primary. Essentially, this solves the problem of durability and high availability.

The above approach aligns with the conceptual view of the VTTablet+Postgres+Data trio as being part of the data layer. Hence, there is no need to rely on yet another (mounted) data layer underneath Postgres.

It is certainly possible to run Multigres on a mounted drive. It is just not necessary.

## Cluster management

## Cross-zone clusters

## Materialization

## Migrations

## Observability

## Schema deployment

## Messaging