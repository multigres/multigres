# Building Multigres

This document provides instructions for building and testing the Multigres
project in a local setup.

## Project Structure

Multigres is organized as a monorepo with all the core application code located
in the `go/` directory. The repository contains multiple components including:

- **multigateway** - Gateway service for routing and load balancing
- **multiorch** - Orchestration service for cluster management
- **multipooler** - Connection pooling service
- **pgctld** - PostgreSQL control daemon
- **multiadmin** - Administrative API server (gRPC, Connect, and REST)
- **multigres** - Main CLI tool
- **minigres** - Multigateway and multipooler in one process, for a database
  served by a single pooler (see [Minigres](./general/minigres.md))
- **portpoolserver** - Port allocator used by the integration tests

Each component has its own main entry point under `go/cmd/`. Component logic
lives under `go/services/`, and shared libraries are organized in the other
subdirectories of `go/` (see [Code Structure](./architecture.md#code-structure)).

## Prerequisites

Multigres requires the following on your `$PATH` before running `multigres cluster start`:

- Go (version 1.26 or later, as required by `go.mod`)
- **PostgreSQL 17.x** — `postgres --version` must report `(PostgreSQL) 17.x`. PG 14, 15, 16, 18+ are rejected.
- **pgBackRest ≥ 2.57** — `pgbackrest --version` must report `pgBackRest 2.57` or newer.
- **etcd** — required for topology.

`multigres cluster start` validates these before bootstrapping the cluster and fails fast with a clear error if anything is missing or wrong.

Most other build dependencies (like protoc) are automatically installed by the
make script.

`make tools` also downloads etcd into `bin/`, and links a system pgBackRest into
`bin/` (installing it through the system package manager or Homebrew if it is missing or too old). It
builds the `pgproto` test tool from source, which requires `pg_config` (the
libpq development headers) on your `$PATH`.

## Setup

To install build tools and dependencies:

```bash
make tools
```

## Building

```bash
make build
```

This builds the Go binaries and places them in the `bin/` directory. Add it to your PATH:

```bash
export PATH="$PWD/bin:$PATH"  # temporary, or add to ~/.bashrc / ~/.zshrc for permanent
```

## Running

```bash
multigres cluster init
```

This generates a default config file, `./multigres_local/multigres.yaml`.

```bash
multigres cluster start
```

This starts a multigres cluster. To stop:

```bash
multigres cluster stop
```

Add `--clean` to also delete the cluster's data. See
[Local cluster and the multigres CLI](./local-cluster.md) for what the cluster
contains and the other commands.

## Testing

To run all tests, including the integration tests that start PostgreSQL and
etcd:

```bash
make test
```

To run only unit tests (skipping integration tests):

```bash
make test-short
```

## Cleaning

```bash
make clean
```

This does not remove the generated protobuf files. As per Go standards, these
generated files are checked into the repository.

## Protocol Buffers

To generate protobuf files:

```bash
make proto
```

Generated `.pb.go` files are placed in the `go/pb/` directory. Unless you're
modifying the protobuf files, you do not need to `make proto`, because these
files are pre-generated and checked into the repository.

`make proto` also regenerates the TypeScript client of the Multiadmin web UI
(`web/multiadmin/lib/api/generated`, which requires `pnpm`) and the
[Multiadmin OpenAPI specification](./api/README.md).

`make build-all` runs every generator (protobuf, parser, metric catalog,
PostgreSQL catalog tables) and then builds the binaries. CI fails if the
checked-in generated files differ from its output.

## Super Linter

We run super linter in CI to ensure code quality and consistency. It checks for
style, formatting, and linting issues across various parts of the codebase. In order
to run it locally, you can use the following command:

```bash
 ./tools/run_super_linter.sh
```
