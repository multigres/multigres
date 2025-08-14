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

## Example Usage

```bash
# Using command line flags
./bin/multigateway --port 5432 --log-level debug

# Using environment variables
MULTIGATEWAY_PORT=5432 MULTIGATEWAY_LOG_LEVEL=debug ./bin/multigateway

# Using config file
./bin/multigateway --config ./my-config.yaml

# Using default config file location
./bin/multigateway  # reads ./multigateway.yaml if it exists
```