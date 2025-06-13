# Upcoming features
This is a losely ordered tentative list of features we intend to build or import from Vitess. Please refer to the project plan (when ready) for details on how they'll be implemented.

## Proxy layer and connection pooling
Multigres will have a two-level proxy layer. In the case of a single small server, the primary benefit of these two layers would be connection pooling.

### VTGate
VTGate is the top layer. In its simplest form, it will masquerade as a Postgres server. When it receives a query, it will forward the request to the next (VTTablet) layer. In the case of a single Postgres server, the primary value it provides is to shield the clients from restarts or failovers that may happen in the underlying layers due to software rollouts and failures.

### VTTablet
There will be one VTTablet per Postgres instance. It will connect to Postgres through a Unix socket and will provide connection pooling. VTTablet will be aware of transaction state. It will also be aware of connection specific changes of state and will preserve the correct behavior to the clients connected at the VTGate level.

## Sharding
As the data grows, you will soon encounter the need to split some tables into shards. When this need arises, Multigres will manage the workflows to split your single Postgres instance into smaller parts. Typically, the smaller tables will remain in the original Postgres database, and the bigger tables will be migrated out into multiple (sharded) databases.

The choice of sharding schemes is a complex topic and will be covered in other documentation. Multigres will provide a powerful relational sharding model that will help you keep tables along with their related rows in the same shard, providing optimum efficiency and performance as your data grows.

### VTGate
The VTGate layer's functionality will be expanded to present this distributed cluster as if it was a single Postgres server. For simpler queries, it will just act as a routing layer by sending it to where the data is. For more complex queries, it will act as a database engine while maximally leveraging the capabilities of the individual databases underneath.

### VTTablet
In the case of sharded database, the query serving functionality of VTTablet will remain the same. However, VTTablet is the workhorse behind facilitating all the resharding efforts.

When the need to reshard arises, new (target) VTTablets are created to receive filtered data from the original unsharded table. This data is streamed by the source VTTablet. Once these tables are populated and up-to-date, a failover is performed to move query traffic to the sharded VTTablets. For safety, the replication is reversed. If any issue is found after the cut-over, it can be undone by switching traffic back to the source tables.

### 2PC
Due the flexible sharding scheme of Multigres, you should be able minimize or completely eliminate the need for distributed transactions. However, if the need arises, VTGate and VTTablet will work together and use the two-phase commit protocol supported by Postgres.

There are many trade-offs to discuss on this topic. They will be covered in a separate document.

## NVME performance, durability and High Availability
To be filled.

## Cluster management

## Cluster management

## Cross-zone clusters

## Materialization

## Migrations

## Observability

## Schema deployment

## Messaging