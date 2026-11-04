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
- **pgbackrestd** - pgBackRest sidecar for shard pods (installed in the
  `multigres` image at `/multigres/bin/pgbackrestd`)
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
- **pgBackRest ≥ 2.59**: `pgbackrest --version` must report `pgBackRest 2.59` or newer.
  Every pgBackRest command runs in the pgbackrestd sidecar, which confines it with Landlock.
  It needs Landlock ABI 3 or later, which upstream Linux has had since 6.2. The kernel version alone does not
  settle it: a distribution kernel can carry a backport, and Landlock can be compiled in but left out of the
  boot-time LSM list. `cat /sys/kernel/security/lsm` must list `landlock`. The ABI number is not in any file;
  the sidecar asks the kernel for it at startup, and when it cannot confine it says why, for example
  `kernel ABI 2 is below the minimum 3` or `Landlock is disabled at boot (EOPNOTSUPP)`.
  `--sandbox` (env `MULTIGRES_PGBACKREST_SANDBOX`) chooses what happens without it:
  `required` (default) refuses to start, `auto` runs unconfined when the host reports Landlock unsupported,
  `off` never uses it. On macOS use `auto` or `off`, or run inside a Linux VM such as OrbStack.
- **etcd** — required for topology.

`multigres cluster start` validates these before bootstrapping the cluster and
fails fast with a clear error if anything is missing or wrong.

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

### A sidecar that refuses to start after a crash

Each pooler's pgbackrestd keeps its state in
`<cluster>/data/pgbackrestd-pooler_<id>/state`. While a pgBackRest command
runs, the sidecar holds a marker file `worker-<id>.pending` there and removes
it once the command and every process it started are gone. The pgBackRest
server on port 8432 is such a command and runs for as long as the sidecar
does, so a running sidecar always holds at least one marker. If the sidecar is
killed (`kill -9`, a power loss) at any time, a marker therefore stays, and
the next sidecar refuses to start, because a pgBackRest process from the old
one may still be writing to the repository or to the data directory:

```text
worker quiescence was not recorded; prior process domain is not proven ended
```

In a pod the sidecar is PID 1 of its container, a restart ends every process
of the old container, and the new sidecar clears the marker itself. That
holds only when pgbackrestd is the container's first process (no init wrapper,
no shared process namespace) and the pod's hostname is the same before and
after. The local provisioner starts each sidecar in a session of its own, so
a Ctrl-C during `multigres cluster start` does not reach it, but a local
sidecar that is killed has no such proof. Clear the marker by hand:

1. Make sure no pgBackRest process of that sidecar is left. Their command
   lines name the state directory:
   `pgrep -fl "pgbackrest.*pgbackrestd-pooler_<id>/state"`. Stop any that
   remain and wait for them to exit.
2. Remove the markers:
   `rm <cluster>/data/pgbackrestd-pooler_<id>/state/worker-*.pending`.
3. Run `multigres cluster start` again.

The same message with `cannot read` or `invalid` in it means a marker file is
damaged; the steps are the same.

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
