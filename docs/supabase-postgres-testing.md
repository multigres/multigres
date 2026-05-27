# Running Integration Tests Against Supabase Postgres

This guide covers how to run the Multigres integration test suite against the
Supabase-flavored Postgres image locally.

Tests run **inside a Docker container** with the Multigres repo mounted as a
volume. This avoids macOS/Linux binary compatibility issues and ensures tests
run in the same Linux environment as production.

## Prerequisites

- Docker Desktop running locally
- `~/repos/multigres/multigres` checked out (this repo)

## Quick start

```bash
cd ~/repos/multigres/multigres

# Build the test runner image from the published Supabase Postgres image
# Find the latest tag at https://hub.docker.com/r/supabase/postgres/tags
# (look for tags matching 17.*-multigres)
docker build \
  -f Dockerfile.integration-test \
  --build-arg SUPABASE_IMAGE=supabase/postgres:17.6.1.130-multigres \
  -t supabase-postgres-test:local \
  .

# Run all integration tests (~5 min after image is built)
docker run --rm \
  --name multigres-integration-test \
  -v $(pwd):/multigres \
  -v /tmp/go-cache:/home/postgres/.cache \
  -w /multigres \
  -e TEST_PRINT_LOGS=1 \
  -e AWS_ACCESS_KEY_ID=test-access-key \
  -e AWS_SECRET_ACCESS_KEY=test-secret-key \
  supabase-postgres-test:local
```

The `supabase/postgres` image is published on Docker Hub at
[docker.io/supabase/postgres](https://hub.docker.com/r/supabase/postgres/tags).
Tags matching `17.*-multigres` are built with the Multigres-specific extensions.
The image supports both `linux/amd64` and `linux/arm64`.

## Step-by-step

### Step 1: Build the test runner image

```bash
docker build \
  -f Dockerfile.integration-test \
  --build-arg SUPABASE_IMAGE=supabase/postgres:17.6.1.130-multigres \
  -t supabase-postgres-test:local \
  .
```

This extends the published Supabase Postgres image with Go 1.25.10, etcd, and
the test entrypoint script. Takes ~3 minutes on first run; cached thereafter.

To use a different tag, substitute the value of `SUPABASE_IMAGE`. Check
[Docker Hub](https://hub.docker.com/r/supabase/postgres/tags) for the latest
`17.*-multigres` tag.

### Step 2: Verify the Postgres binary origin

```bash
docker run --rm --entrypoint which supabase-postgres-test:local initdb
# → /nix/var/nix/profiles/default/bin/initdb

docker run --rm --entrypoint postgres supabase-postgres-test:local --version
# → postgres (PostgreSQL) 17.x
```

### Step 3: Run the full integration suite

```bash
docker run --rm \
  --name multigres-integration-test \
  -v $(pwd):/multigres \
  -v /tmp/go-cache:/home/postgres/.cache \
  -w /multigres \
  -e TEST_PRINT_LOGS=1 \
  -e AWS_ACCESS_KEY_ID=test-access-key \
  -e AWS_SECRET_ACCESS_KEY=test-secret-key \
  supabase-postgres-test:local
```

Result files are written to the repository root on the host (via the volume
mount) when the container exits:

- `integration-test-results.jsonl` — JSON test event stream (for CI reporting)
- `integration-test-results.xml` — JUnit XML (for Codecov and test reporters)

## Building the base image from source

If you need to test against an unreleased Supabase Postgres change that isn't
yet published on Docker Hub, build the base image locally from the
`supabase/postgres` source checkout instead.

**Additional prerequisite**: `~/repos/supabase/postgres` checked out. If your
checkout is elsewhere, override the path:

```bash
export SUPABASE_POSTGRES_DIR=/path/to/supabase/postgres
```

Then use the Makefile targets:

```bash
cd ~/repos/multigres/multigres

# Build both images and run all integration tests (~20–25 min on first run)
make test-integration-supabase
```

This runs the following sequence:

1. `make docker-supabase-postgres` — runs `docker build -f Dockerfile-17`
   from the supabase/postgres repo. Nix pulls pre-built packages from
   `nix-postgres-artifacts.s3.amazonaws.com`. Expect 10–20 minutes on a
   cold build; subsequent runs reuse Docker layer cache.
2. `make docker-supabase-postgres-test` — extends the locally built base image
   with Go 1.25.10, etcd, and the test entrypoint script. Takes ~3 minutes on
   first run; cached thereafter.
3. `docker run` — runs the full integration suite inside the container.

### Makefile targets reference

| Target                               | What it does                                                                           |
| ------------------------------------ | -------------------------------------------------------------------------------------- |
| `make docker-supabase-postgres`      | Build `supabase-postgres:local` from `~/repos/supabase/postgres` (rarely needed alone) |
| `make docker-supabase-postgres-test` | Build both images (`supabase-postgres:local` then `supabase-postgres-test:local`)      |
| `make test-integration-supabase`     | Build images if needed, then run all integration tests in the container                |

## How it works

The test runner image has a baked-in ENTRYPOINT:

```text
/usr/local/bin/run-integration-tests
```

This is `.github/scripts/run-integration-tests-container.sh` copied into the
image at build time. When `docker run supabase-postgres-test:local` runs with
no command, this script is executed inside the container:

1. `make build` — compiles multigres binaries from the volume-mounted source
2. Starts `portpoolserver` on a Unix socket
3. Runs `gotestsum` over `./go/test/endtoend/...`, skipping
   `TestPostgreSQLRegression`
4. Writes result files to the working directory (`/multigres`, which is the
   volume mount)

The host-side wrapper (`.github/scripts/run-integration-tests-supabase.sh`)
only handles the `docker run` invocation — volumes, environment variables, and
the image tag. All test logic lives in the container-side script.

The supabase image sets:

```dockerfile
ENV PATH="/nix/var/nix/profiles/default/bin:/usr/lib/postgresql/bin:${PATH}"
```

Multigres tests discover `initdb`, `postgres`, `pg_ctl`, and `pg_isready` via
`exec.LookPath` — so the supabase-built binaries are found automatically with
no changes to test code.

The `pgctld` binary used by tests is the **multigres-compiled** one from
`./bin/pgctld` (built via `make build`), not the one baked into the supabase
image. This is intentional: we test multigres's pgctld against supabase
Postgres, not supabase's own pgctld.

## Running a subset of tests

Override the ENTRYPOINT to get a shell, then drive the test run manually:

```bash
docker run --rm -it \
  --entrypoint sh \
  -v $(pwd):/multigres \
  -v /tmp/go-cache:/home/postgres/.cache \
  -w /multigres \
  -e TEST_PRINT_LOGS=1 \
  supabase-postgres-test:local
```

From that shell you can run any subset:

```bash
# Inside the container:
make build
./bin/portpoolserver --socket /tmp/multigres-port-pool.sock &
PORT_POOL_PID=$!

MULTIGRES_PORT_POOL_ADDR=/tmp/multigres-port-pool.sock \
  go test -v -run TestConnPool -timeout 10m \
  ./go/test/endtoend/multipooler/...

kill $PORT_POOL_PID
```

See `.github/scripts/run-integration-tests-container.sh` for the full setup
the ENTRYPOINT uses — it's the reference for the portpoolserver dance.

## Debugging failures

The interactive shell approach above also works for debugging. Additional
things to check:

```bash
# Confirm binary paths
which initdb postgres pg_ctl

# Check the Supabase-built version
initdb --version

# Inspect environment inherited by test processes
env | grep POSTGRES
```

## Scripts reference

| Script                                                  | Role                                                           |
| ------------------------------------------------------- | -------------------------------------------------------------- |
| `.github/scripts/build-supabase-postgres.sh`            | Builds the supabase base image from a local checkout           |
| `.github/scripts/build-supabase-postgres-test-image.sh` | Builds the test runner image from the base image               |
| `.github/scripts/run-integration-tests-supabase.sh`     | Host-side wrapper: invokes `docker run` with right env/volumes |
| `.github/scripts/run-integration-tests-container.sh`    | Container-side ENTRYPOINT: build + run tests                   |
