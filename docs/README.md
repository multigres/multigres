# Multigres Developer Documentation

This documentation is **for developers working on Multigres**. If you're looking
to use Multigres for your applications, please refer to the
[official documentation](https://multigres.com/) instead.

## Structure

- **[Architecture](./architecture.md)**
- **[How to build](./building.md)**
- **[Working with us](./teamwork.md)**
- **[Github workflow](./workflow.md)**
- **[Contributing](./contributing.md)**
- **[Multiadmin REST API and OpenAPI specification](./api/README.md)**
- **[Local cluster and the `multigres` CLI](./local-cluster.md)** — the
  `multigres.yaml` schema, default topology and ports, what `cluster start`
  does, and the admin command groups.
- **[Topology](./topology.md)** — topology backends, the global and cell key
  layout, and which component writes what.
- **[Development guides](./development/README.md)** — the SQL parser and AST,
  code generation, integration-test infrastructure, error handling, and lint
  rules.

## Design Documents

Browse the design and reference docs by area:

- **[Query serving](./query_serving/)** — connection pooling, prepared
  statements, transactions, session settings, plan caching, query
  cancellation, failover buffering, replica reads, listen/notify, etc.
  Also covers
  [client connection startup](./query_serving/client_connection_startup.md),
  [row passthrough](./query_serving/row_passthrough.md),
  [query observability](./query_serving/query_observability.md), and the
  [reserved connection lifecycle](./query_serving/pooler_reserved_connection_lifecycle.md).
- **[Prepared-statement design](./query_serving/prepared_statements_design.md)**
  — backend preparation, query identity, and DDL recovery. See
  [prepared-statement testing](./query_serving/testing_strategy.md#prepared-statement-preparation-and-ddl)
  for regression coverage and validation commands.
- **[High availability](./ha/)** — consensus, failover, and the state model,
  plus the HA decision log.
- **[Sharding](./sharding/)** — the sharding design, tablegroup operations
  and DDL, and routing rules.
- **[Migration](./migration/)** — the migrator's
  [SQL interface](./migration/migrator_sql_interface.md).
- **[General](./general/)** — cross-cutting topics (e.g. serving state
  management, [logging conventions](./general/logging.md),
  [observability](./general/observability.md),
  [backups](./general/backups.md)).
- **[Testing against supabase-postgres](./supabase-postgres-testing.md)** —
  running the integration tests inside the supabase-postgres image.
- **[Minigres](./general/minigres.md)** — the multigateway and multipooler in
  one process for a single-pooler database: how to run it, and where it
  diverges from Multigres and why.
- **[pgctld init](./pgctld-init.md)** — data directory initialization: the init
  flags, execution order, superuser password resolution, init SQL, and init
  secrets (role passwords and database settings).

## Alpha Deployment Notes

- **[Integration with Kubernetes](./kubernetes/integration.md)** — how the
  Multipooler and pgctld present to Kubernetes, and what the readiness and
  liveness probes mean.
- **[Getting started on EKS](./kubernetes/eks.md)**

## Release Documentation

- **[Release artifacts and verification](../RELEASE.md)**

## PostgreSQL Compatibility

multigres runs the official PostgreSQL regression test suite to track compatibility.

- **Results:** See the [latest workflow run][pgregress-ci] for the detailed
  compatibility report in the Job Summary.
- **PostgreSQL compatibility artifacts:** Each run uploads
  `postgres-compatibility-results`, which includes the compatibility report and
  PostgreSQL regression diffs when results are produced.

[pgregress-ci]: https://github.com/multigres/multigres/actions/workflows/test-pgregress.yml
