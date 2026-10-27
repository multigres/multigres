# Configuration

Multigres components support configuration through multiple sources with the following precedence (highest to lowest):

1. **Command line flags**
2. **Environment variables**
3. **Configuration files**
4. **Default values**

## Configuration Files

A component reads one YAML file whose keys are its flag names, for example `pg-port: 15432`.

- `--config-file` gives the file's full path.
- Otherwise the component looks for a file named `--config-name` (default `mtconfig`, any supported extension) in the `--config-path` directories (default `$MTDATAROOT`, or `./multigres_local`).
- `--config-file-not-found-handling` controls what happens when no file is found (default: warn and continue).

Dynamic settings follow changes to the file while the component runs.

The YAML files in this directory are examples.

## Environment Variables

Settings that read an environment variable use the `MT_` prefix, shared by all components: for example `MT_PG_PORT` or `MT_CELL`. Each flag's help text and code name its variable; not every setting has one.

## Components

### multigateway

PostgreSQL proxy that accepts client connections. Example: `multigateway.yaml`.

### multipooler

Connection pooling service that communicates with pgctld. Example: `multipooler.yaml`.

### pgctld

PostgreSQL interface daemon that connects directly to PostgreSQL. Example: `pgctld.yaml`.

### multiorch

Cluster orchestration service for consensus and failover. Example: `multiorch.yaml`.

### minigres

A multigateway and a multipooler in one process. Its file holds the process's settings at the top level and each half's settings behind the half's name (`multigateway.pg-port`, `multipooler.pgctld-addr`). Example: `minigres.yaml`; see `docs/general/minigres.md`.

## PostgreSQL Environment Variables

The following environment variables are recognized by `pgctld` (and to
some extent, Multipooler) and follow the Docker `postgres` image
convention.

| Variable                 | CLI flag               | Default    | Description                                                |
| ------------------------ | ---------------------- | ---------- | ---------------------------------------------------------- |
| `POSTGRES_USER`          | `--pg-user` / `-U`     | `postgres` | PostgreSQL user name                                       |
| `POSTGRES_PASSWORD`      | _(none)_               | _(empty)_  | PostgreSQL password (inline)                               |
| `POSTGRES_PASSWORD_FILE` | _(none)_               | _(empty)_  | Path to a file containing the password (preferred for k8s) |
| `POSTGRES_DB`            | `--pg-database` / `-D` | `postgres` | PostgreSQL database name                                   |
| `POSTGRES_INITDB_ARGS`   | `--pg-initdb-args`     | _(empty)_  | Extra arguments forwarded to `initdb`                      |

`POSTGRES_USER`
: PostgreSQL user that Multigres uses to connect to the PostgreSQL server and perform administrative actions. This user **must have `SUPERUSER` privileges**.
The default `initdb` configuration sets up SCRAM-SHA-256 authentication over a local socket connection, which is what we recommend. Multigres itself does not require any specific authentication method, so any `pg_hba.conf` policy (including `trust`) will work, but anything weaker than SCRAM is not secure.

`POSTGRES_PASSWORD`
: Password for the user provided in `POSTGRES_USER`. Use `POSTGRES_PASSWORD_FILE` instead when running in environments that mount secrets as files (the k8s demo, for example).

`POSTGRES_PASSWORD_FILE`
: Path to a file containing the password for `POSTGRES_USER`. When set, `pgctld` reads the password from this file instead of `POSTGRES_PASSWORD`.

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
