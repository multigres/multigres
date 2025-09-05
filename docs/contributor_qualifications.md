# Qualifications 

At some point, when the project’s foundational pieces are in place, we will be opening it up for external contributions. Multigres is a unique project and requires some level of expertise and specialization to make meaningful contributions. This post describes the kind of background needed for this.

Under the covers, Multigres will integrate a number of concepts that will give it unique power and flexibility. Some of these are adaptations of existing concepts, and some are unique to Multigres. Beyond the challenge of internalizing these concepts, they also have to be implemented in a way that they work at massive scale.

This requires contributors to either already possess these skills or have the ability to learn them quickly. Prior experience is almost a necessity. Listed below are various requirements for engineers to contribute to this project. Not all requirements are mandatory, but a healthy combination is expected.

## Database Sharding

### Relational Algebra

The sharding concepts of Multigres will be built on the foundations of relational algebra. In a traditional database, a query translates into file system operations like table scans and index lookups. In contrast, Multigres will build a query engine that can break up a query into relational operations, recombine them into smaller relational expressions and send them to the underlying databases as queries.

### Sharding scheme

Multigres will introduce a formal approach to describing sharding schemes. This approach will have the ability to ensure that related rows will be co-located within a single shard. Such relationships will be understood by the query engine to deduce how to push down combinations of operations into individual shards to maximize performance.

Additionally, Multigres will have the ability to accommodate user-defined sharding schemes, similar to Postgres extensions. The query engine must have the ability to use and optimize the execution of queries that depend on these indexes.

### Database concepts

Foundational knowledge of database concepts will be necessary. More specifically, the contributor must be familiar with the trade-offs related to isolation, locking, distributed transactions, and latencies of various resources.

### Postgres internals

In the project, we will be working with logic that mimics the postgres parser, optimizer and engine. Knowledge of these internals will be an added bonus.

## Consensus

Multigres will implement an advanced and flexible consensus system. The closest theoretical foundation for this system is [FlexPaxos](https://wcl.cs.rpi.edu/pilots/library/papers/collision/LIPIcs-OPODIS-2016-25.pdf).

This involves the implementation of a two-phase sync replication in Postgres along with reconciliation primitives in case of failovers. The rest of the consensus protocol will be handled by a group of distributed orchestrators that will monitor the state of the servers, and perform failovers as needed.

To effectively contribute to this subsystem, the engineer is expected to gain familiarity with existing protocols like Raft, Paxos, and FlexPaxos.

## Materialization

Materialization will be one of the core primitives of Multigres. It will be used for resharding, migrations, materialized views, schema deployments, change data capture, and other use cases.

This ability will use the foundation of stream processing. However, it has to work correctly in a sharded environment. For example, a resharding migration should also correctly migrate any materialized views that depend on the affected shards.

## Building for massive scale

Last but not least, familiarity with building and running distributed systems at massive scale is a prerequisite for contributors. Expectations for this are:

- Ability to write robust and performant code
- Anticipation of spikes and overloads
- Building observability to help troubleshoot during outages
- Usage of timeouts and retries
- Prevention of cascading failures
- Deployment strategies with minimal disruption
- Cross-cluster and cross-cloud interactions