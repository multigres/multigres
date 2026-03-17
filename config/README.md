# Configuration

Multigres components support configuration through multiple sources with the following precedence (highest to lowest):

1. **Command line flags**
2. **Environment variables**
3. **Configuration files**
4. **Default values**

## Configuration Files

Each component looks for its configuration file in the following locations:

- Current directory (`./<component>.yaml`)
- `./config/<component>.yaml`
- `/etc/multigres/<component>.yaml`

You can also specify a custom config file path using the `--config` / `-c` flag.

## Environment Variables

Environment variables are prefixed with the component name in uppercase:

- `MULTIGATEWAY_*` for multigateway
- `MULTIPOOLER_*` for multipooler
- `PGCTLD_*` for pgctld
- `MULTIORCH_*` for multiorch

For example: `MULTIGATEWAY_PORT=5432` or `PGCTLD_LOG_LEVEL=debug`

## Components

### multigateway

PostgreSQL proxy that accepts client connections.

**Configuration file**: `multigateway.yaml`
**Environment prefix**: `MULTIGATEWAY_`

### multipooler

Connection pooling service that communicates with pgctld.

**Configuration file**: `multipooler.yaml`
**Environment prefix**: `MULTIPOOLER_`

### pgctld

PostgreSQL interface daemon that connects directly to PostgreSQL.

**Configuration file**: `pgctld.yaml`  
**Environment prefix**: `PGCTLD_`

### multiorch

Cluster orchestration service for consensus and failover.

**Configuration file**: `multiorch.yaml`
**Environment prefix**: `MULTIORCH_`

## PostgreSQL Environment Variables

The following environment variables are recognized by `pgctld` (and to
some extent, Multipooler) and follow the Docker `postgres` image
convention.

| Variable               | CLI flag               | Default    | Description                           |
| ---------------------- | ---------------------- | ---------- | ------------------------------------- |
| `POSTGRES_USER`        | `--pg-user` / `-U`     | `postgres` | PostgreSQL user name                  |
| `POSTGRES_PASSWORD`    | _(none)_               | _(empty)_  | PostgreSQL password                   |
| `POSTGRES_DB`          | `--pg-database` / `-D` | `postgres` | PostgreSQL database name              |
| `POSTGRES_INITDB_ARGS` | `--pg-initdb-args`     | _(empty)_  | Extra arguments forwarded to `initdb` |

`POSTGRES_USER`
: PostgreSQL user for connecting to PostgreSQL server and perform administrative actions. This user requires `SUPERUSER` privileges and is expected to use SCRAM-256 password authentication over a local socket connection, but this is not enforced.

`POSTGRES_PASSWORD`
: Password for the user provided in `POSTGRES_USER`.

`POSTGRES_DB`
: PostgreSQL database name to use when connecting to the database.

`POSTGRES_INITDB_ARGS`
: Extra arguments forwarded to `initdb` during cluster initialisation (e.g. `--locale-provider=icu --icu-locale=en_US.UTF-8`). Note that these are split into separate arguments on space using `strings.Fields` so you should not use spaces in values to options.

## Example Usage

```bash
# Using command line flags
./bin/multigateway --http-port 5432 --log-level debug

# Using environment variables
MULTIGATEWAY_HTTP_PORT=5432 MULTIGATEWAY_LOG_LEVEL=debug ./bin/multigateway

# Using config file
./bin/multigateway --config ./my-config.yaml

# Using default config file location
./bin/multigateway  # reads ./multigateway.yaml if it exists
```
