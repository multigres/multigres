# Multigres Upcoming features

Multigres is a project to build an adaptation of Vitess for Postgres.

This document is a losely ordered tentative list of features we intend to build or import from Vitess. Please refer to the project plan (when ready) for details on how they'll be implemented.

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

### VTGate (Sharding)

In a sharded setup, VTGate's functionality will expand to present this distributed cluster as if it was a single Postgres server. For simpler queries, it will just act as a routing layer by sending it to where the data is. For more complex queries, it will act as a database engine while maximally leveraging the capabilities of the individual databases underneath.

VTGate will have the ability to push entire join queries into an underlying shard if it determines that all data for that join is within that shard. Similarly, VTGate will "scatter" entire joins across all shards if it determines that the related rows of a join are within their respective shards.

### VTTablet (Sharding)

The query serving functionality of VTTablet will have no awareness of sharding. It will just use the underlying Postgres instance to serve the requested queries. However, VTTablet will be the workhorse behind facilitating all the resharding efforts.

When the need to reshard arises, new (target) VTTablets will be created to receive filtered data from the original unsharded table. This data will be streamed by the source VTTablet. Once these tables are populated and up-to-date, a failover will be performed to move traffic to the sharded VTTablets. For safety, the replication will be reversed. If any issue is found after the cut-over, it can be undone by switching traffic back to the source tables.

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

Multigres will come equipped with a variety of tools and automation to facilitate the management of your cluster. This tooling is capable of scaling to tens of thousands of node, and can be spread across multiple regions worldwide.

### Automated Backups

Multigres can automatically perform backups of your Postgres instances on a regular basis to a centralized location.

### New vttablets

Adding more replicas to a Postgres instance will be as trivial as launching a vttablet with the right command line arguments. The vttablet will seek the latest available backup, restore it, and point the Postgres instance to the current primary. Once the replication has caught up, it will open itself up to serve read traffic.

VTTablets can be of different types: They can be quorum participants, in which case they will follow the two-phase sync protocol during replication. If not, they will just be "observers", to just serve additional read traffic.

Bringing up a vttablet as a quorum participant will automatically make it join the quorum after it has fully caught up on replication.

VTTablets will have configurable user-defined tags that will allow vtgates to split traffic based on differing workloads. For example, you may tag a group of vttablets as "olap". Workloads that would like to perform analytics queries would specify "dbname@olap" as the database to connect to. This will inform the vtgates to redirect these reads to only vttablets that export the "olap" tag.

### Replication

Beyond managing the consensus protocol for vttablets in the quorum, vtorc will also monitor all vttablets and ensure that they are replicating from their respective primaries. If a connection is lost for any reason, vtorc will restore it.

### Cross-zone clusters

Multigres can be deployed across a large number of nodes distributed across the world. This is architecturally achieved by a star configuration in the topology. There will be a global topology server (etcd). It will contain information that changes infrequently, like sharding info, etc. This information will be propagated into cell-sepific topology servers (local etcds), one for each cell. VTGates and vttablets will be deployed in a cell, and they'll use the local topology to serve traffic for that cell. In case of a network partition, a cell will be capable of continuing to serve traffic for as long as the replication lag is tolerable.

Of course, there can exist only one Primary per shard. Any cell that does not host the primary database is meant to serve read-only traffic that is replicated from the primary.

### Cloud-Native

Multigres will be cloud-native. This means that it can run in a cloud framework like Kubernetes:

* All components are restartable without causing disruption to the overall system. This needs to be within a reasonable disruption budget, which is configurable.
* Components can be migrated to different nodes.
* Components can use mounted persistent storage, and will automatically reattach if moved.
* New components can be launched to increase capacity. They will initialize themselves correctly, and join the system to serve traffic. Conversely, components can be removed in order to scale down.
* Specific to databases: Primary failures and restarts will be handled automatically by a watchdog process (VTOrc) that can perform a failover to an up-to-date replica.
* Scale to zero: You can shut down all components. This will result in just the metadata and backups being preserved, with no active components to serve any traffic. Adding the serving components back to the system will bootstrap the cluster to an operational state.

Multigres will come with a Kubernetes Operator that will translate components described using Multigres terminology like cells, shards and replicas into corresponding Kubernetes components.

We intend to develop a way for Multigres to use local storage in order to leverage the proximity of Postgres and its data files.

TODO: Doc to elaborate on local NVME

## VReplication

The next few features described below will all be powered by a primitive called VReplication. This primitive is essentially a stream processor capable of materializing a source of tables into a target set of tables. The rules of materialization can be expressed as an SQL statement. The only limitation is that the expression is stream processable. In other words, we should be able to incrementally and independently apply every change event to the target. For example, a `count(*)` is stream processable, whereas a `max(col)` is not.

This materialization can happen without causing any downtime to the source database. Once the target tables are caught up to the sources, VReplication will verify correctness of the target tables by performing a diff.

If the materialization expression is non-lossy (reversible), you can atomically switch traffic from the source tables to the target. At this point, VReplication can reverse the replication to keep the source tables up-to-date. You can go back and forth indefinitely. This ability allows you to undo a migration if problems are encountered after a cutover.

VReplication streams operate orthogonally to each other. For example, resharding uses VReplication, but a simultaneous table materialization will correctly migrate to use the new shards after resharding.

VReplication works in conjunction with vtgate routing rules that allow you to smoothly and safely transition traffic from source to target.

## Migrations

### Migrate tables from anywhere to anywhere

MoveTables will be a workflow built using VReplication. This will allow you to migrate a group of tables from any source to any target. This can be used to split a database into smaller parts, or merge two databases into one. Let us assume that you want to migrate table `t` from database `a` to database `b`.

* Initially, the vtgates will have a rule to redirect traffic intended for `a.t` and `b.t` to `a.t`. MoveTables will setup these routing rules autmatically before starting VReplication.
* Once this rule is setup, you can start refactoring your application to write to `b.t` instead of `a.t`. Writing to any of these tables will get redirected to `a.t`.
* Once the data is verified to be correct, we can switch the vtgate routing rules to send traffic to `b.t` instead. MoveTables will have a subcommand to do this safely. If the application has not finished refactoring all the code, it's ok, because all traffic will flow to `b.t`.
* At this time, VReplication will reverse the replication to keep `a.t` up-to-date. If any problem is detected after the cutover, you can fall back to `a.t`. This back and forth can be repeated as often as necessary.
* After we are certain that all problems are resolved, and we have verified that the application refactor is complete, we can use the MoveTables clean up command to drop the reverse replication, routing rules, and source table `a.t`.

Of course, this workflow would require vtgates to have access to both `a` and `b`.

### Migrate across Postgres versions

VReplication will rely on Postgres logical replication. Due to this design, we can also use it to safely migrate from one version of Postgres to another without incurring downtime, and with the ability to revert a migration, just like the case of a table migration.

This will essentially be a MoveTables, but with the source and target running different versions of Postgres.

### Resharding

VReplication will be used for resharding. For this purpose, Multigres will use the filtering ability of VReplication to split sharded (or unsharded) tables into their target shards based on the target's sharding key ranges.

Just like the other cases, the ability to verify correctness and revert will be available.

### Change sharding key

It may happen that the original sharding key you chose for the table was suboptimal, or it may be possible that the application workload has changed substantially. In such cases, you can use VReplication to change the sharding key of a table to one that is more optimal to the current workload.

### Migrate from MySQL

If you are currently running on a MySQL database, or Vitess, Multigres will allow you to migrate from that source. In this situation, additional work may have to be done by you in the area of how you switch traffic. This is because Multigres will only support the Postgres protocol, and will not be able to orchestrate the traffic meant for the source MySQL.

### Exotic migrations

VReplication is versatile enough that it can be used to address migration use cases that have not been thought of yet. For exmaple, you could script it to merge tables from different databases into one. This can be a necessity if you decide to merge many multi-tenant databases into a single sharded one, or vice-versa.

## Materialization

## Schema deployment

## Messaging

## Change Data Capture

## Observability
