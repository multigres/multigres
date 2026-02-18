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

## PostgreSQL Compatibility

multigres runs the official PostgreSQL regression test suite to track compatibility.

- **Results:** See the [latest workflow run](https://github.com/multigres/multigres/actions/workflows/test-pgregress.yml) for the detailed compatibility report in the Job Summary.
- **Artifacts:** Each run uploads `compatibility-report.md` and `regression.diffs` as downloadable artifacts.
