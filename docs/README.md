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

## Design Documents

Browse the design and reference docs by area:

- **[Query serving](./query_serving/)** — connection pooling, prepared
  statements, transactions, session settings, plan caching, query
  cancellation, failover buffering, replica reads, listen/notify, etc.
- **[High availability](./ha/decision-log/)** — decision log for consensus,
  failover, and primary-term changes.
- **[General](./general/)** — cross-cutting topics (e.g. serving state
  management).

## Alpha Deployment Notes

- **[Getting started on EKS](./kubernetes/eks.md)**

## Release Documentation

- **[Release artifacts and verification](../RELEASE.md)**

## PostgreSQL Compatibility

multigres runs the official PostgreSQL regression test suite to track compatibility.

- **Results:** See the [latest workflow run](https://github.com/multigres/multigres/actions/workflows/test-pgregress.yml) for the detailed compatibility report in the Job Summary.
- **PostgreSQL compatibility artifacts:** Each run uploads
  `postgres-compatibility-results`, which includes the compatibility report and
  PostgreSQL regression diffs when results are produced.
