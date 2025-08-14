# Multigres MVP project plan



## Approach

The first goal of Multigres is to build an MVP. For this, there are three approaches:

1. Retrofit Postgres into a fork of Vitess
2. Build from the ground up using Vitess as a reference
3. Hybrid: Build from scratch, and copy what you can from Vitess

We'll be be using approach 3. Reasons:

* Approach 1 will likely get us to an MVP the fastest. However, we'll be continuously fighting against mysql-isms in the code. We think this will eventually result in a product that is subpar.
* Approach 2 could likely be the cleanest, but also the slowest. We'll be reinventing the wheel for many major features for which solutions already exist in Vitess.
* Approach 3 will take a bit longer than approach 1, but it will be clean. It will be faster than approach 2 because there are many substantially large parts of Vitess that are designed to be database agnostic. Those parts can be copied as is into Multigres.

For approach 3, we intend to leave behind:

* Anything that is MySQL specific
* Any legacy features that are not needed any more
* Anything that was not well implemented

Multigres will have the following project tracks, and they will be worked on in parallel to the extent possible. The tracks are listed in the expected order of MVP completion. These parts are independently useful and can be deployed into production as they become ready.

## Proxy

* MultiPooler: Connection pooling
* MultiGateway: Postgres and PostgREST protocols, discovery of MultiPoolers, route traffic to primary or replica MultiPoolers, load balance replica traffic

The MVP must be able to scale to support tens of thousands of connections per MultiGateway.

Each pool of MultiGateways should be able to support up to ten thousand databases.

## Cluster management

The purpose of cluster management is to minimize human intervention by automating mundane tasks.

* Initialize a new database
* Automated backups of databases and WALs
* Add (and remove) replicas, will use backups and WALs to make them catch up to the cluster, and publish to MultiGateway about readiness to serve traffic
* Deactivate a database (scale to zero) and bring up a previously deactivated database
* Kubernetes Operator

Planned cluster management operations should have no impact to user traffic.

## Performance, Durabilty and HA

These three features go hand in hand. Storing the data in a local NVME drive yields the best performance for an OLTP system like Postgres. However, this can lead to data loss in case of a node failure. This can be solved by implementing a consensus protocol. The side benefit of a consensus protocol is that it also helps us address the problem of High Availability, because there will always be an up-to-date replica if the primary node fails.

### Postgres

The existing Postgres primitives are insufficient to build a robust consensus protocol. Multigres will make changes to Postgres to implement a two-phase sync mechanism. A number of consensus protocols can be built once this functionality is in place. We will also work towards getting this change accepted upstream.

### Multigres

Multigres will build the coordination part of the consensus protocol using a brand new component called MultiOrch. This will not be ported from Vitess, because the VTOrc from Vitess has a large amount of legacy code that was inherited from the MySQL Orchestrator.

MultiOrch will operate as a coordinated cluster across failure zones to ensure that at least one of them can perform a successful failover if there is a network partition.

Functionally, MultiOrch will be the same as its Vitess counterpart:

* Elect a primary if none exist
* Perform smooth primary changes if requested
* Detect failures and failover as needed
* Rewire replicas and observers if they lose their connection to the primary, or if their connection to the primary needs to be updated
* Cooperate with other MultiOrchs to ensure that they don't step on each others' actions.
* Honor a variety of durability policies for each cluster.

## Materializer

The Postgres logical replication is functionally very close to MySQL's row-based binlog replication. Hence, most of Materializer will be copied from Vitess, and changes will be made to address any mismatch.

The MVP will support:

* Resharding
* MoveTables
* Migrations
* Materialization

## Sharded query serving

Sharded query serving has an extensive list of constructs to support. The MVP will support the following:

* SELECT, INSERT, UPDATE and DELETE that can be executed within a shard. This should cover all multi-tenant use cases.
* 2PC for transactions that span across shards.
* Stored procedures that can be executed within a shard.

The above functionality already exists in Vitess. The primary challenge will be to reconcile the Vitess parser and the associated data structures against the Postgres syntax.
