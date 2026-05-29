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
- **multigres** - Main CLI tool

Each component has its own main entry point under `go/cmd/` and shared libraries
are organized in the other subdirectories of `go/`.

## Prerequisites

Multigres requires the following on your `$PATH` before running `multigres cluster start`:

- Go (version 1.25 or later)
- **PostgreSQL 17.x** — `postgres --version` must report `(PostgreSQL) 17.x`. PG 14, 15, 16, 18+ are rejected.
- **pgBackRest ≥ 2.57** — `pgbackrest --version` must report `pgBackRest 2.57` or newer.
- **etcd** — required for topology.

`multigres cluster start` validates these before bootstrapping the cluster and fails fast with a clear error if anything is missing or wrong.

Most other build dependencies (like protoc) are automatically installed by the
make script.

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

This generates a default config file under `./multigres_local`.

```bash
multigres cluster start
```

This starts a multigres cluster. To stop:

```bash
multigres cluster stop
```

## Testing

To run all tests:

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

## Super Linter

We run super linter in CI to ensure code quality and consistency. It checks for
style, formatting, and linting issues across various parts of the codebase. In order
to run it locally, you can use the following command:

```bash
 ./tools/run_super_linter.sh
```
