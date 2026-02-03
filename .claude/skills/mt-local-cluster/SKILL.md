---
name: "Local Cluster Manager"
description: "Manage local multigres cluster components (multipooler, pgctld, multiorch, multigateway) - start/stop services, view logs, connect with psql"
---

# Local Cluster Manager

Manage local multigres cluster - both cluster-wide operations and individual components.

## When to Use This Skill

Invoke this skill when the user asks to:

- Start/stop/restart the entire cluster or individual components
- View logs for any component
- Connect to multipooler or multigateway with psql
- Check status of cluster components
- Check multipooler topology status (PRIMARY/REPLICA roles)
- Check if PostgreSQL instances are in recovery mode

## Performance Optimization

Parse `./multigres_local/multigres.yaml` once when this skill is first invoked and cache the cluster configuration in memory for the duration of the conversation. Use the cached data for all subsequent commands. Only re-parse if the user explicitly asks to "reload config" or if a command fails due to stale config.

## Cluster-Wide Operations

**Start entire cluster**:

```bash
./bin/multigres cluster start
```

**Stop entire cluster**:

```bash
./bin/multigres cluster stop
```

**Stop entire cluster and delete all cluster data**:

```bash
./bin/multigres cluster stop --clean
```

**Check cluster status**:

```bash
./bin/multigres cluster status
```

**Initialize new cluster**:

```bash
./bin/multigres cluster init
```

**Get all multipoolers from topology**:

```bash
./bin/multigres getpoolers
```

Returns JSON with all multipoolers, their cells, service IDs, ports, and pooler directories.

**Get detailed status for a specific multipooler**:

```bash
./bin/multigres getpoolerstatus --cell <cell-name> --service-id <service-id>
```

Returns detailed status including:

- `pooler_type`: 1 = PRIMARY, 2 = REPLICA
- `postgres_role`: "primary" or "standby"
- `postgres_running`: Whether PostgreSQL is running
- `wal_position`: Current WAL position
- `consensus_term`: Current consensus term
- `primary_status`: (for PRIMARY) connected followers and sync replication config
- `replication_status`: (for REPLICA) replication lag and primary connection info

Example:

```bash
./bin/multigres getpoolerstatus --cell zone1 --service-id thhcdhbp
```

**Check PostgreSQL recovery mode directly**:

```bash
psql -h <pooler-dir>/pg_sockets -p <pg-port> -U postgres -d postgres -c "SELECT pg_is_in_recovery();"
```

Returns `t` (true) if in recovery/standby mode, `f` (false) if primary.

## Individual Component Operations

### Configuration

1. **Parse the config**: Read `./multigres_local/multigres.yaml` to discover available components and their IDs

2. **Component ID mapping**:
   - multipooler IDs: extracted from `.provisioner-config.cells.<zone>.multipooler.service-id`
   - pgctld uses the same IDs as multipooler
   - multiorch has separate IDs for each zone
   - multigateway has separate IDs for each zone

3. **If no ID provided**: Use AskUserQuestion to let the user select which instance to operate on
   - Show available IDs with their zone names
   - Example: "xf42rpl6 (zone1)", "hm9hmxzm (zone2)", "n6t8hvgl (zone3)"

### Commands

**Stop pgctld**:

```bash
./bin/pgctld stop --pooler-dir <pooler-dir-from-config>
```

**Start pgctld**:

```bash
./bin/pgctld start --pooler-dir <pooler-dir-from-config>
```

**Restart pgctld (as standby)**:

```bash
./bin/pgctld restart --pooler-dir <pooler-dir-from-config> --as-standby
```

**Check pgctld status**:

```bash
./bin/pgctld status --pooler-dir <pooler-dir-from-config>
```

**View logs**:

- multipooler: `./multigres_local/logs/dbs/postgres/multipooler/[id].log`
- pgctld: `./multigres_local/logs/dbs/postgres/pgctld/[id].log`
- multiorch: `./multigres_local/logs/dbs/postgres/multiorch/[id].log`
- multigateway: `./multigres_local/logs/dbs/postgres/multigateway/[id].log`
- PostgreSQL: `./multigres_local/data/pooler_[id]/pg_data/postgresql.log`

**Tail logs**:

```bash
tail -f <log-path>
```

**Connect to multipooler** (via Unix socket):

```bash
psql -h <pooler-dir>/pg_sockets -p <pg-port> -U postgres -d postgres
```

Where:

- pooler-dir is from `.provisioner-config.cells.<zone>.multipooler.pooler-dir`
- pg-port is from `.provisioner-config.cells.<zone>.pgctld.pg-port`
- PostgreSQL socket is at `<pooler-dir>/pg_sockets/.s.PGSQL.<pg-port>`

Example:

```bash
psql -h /Users/rafael/sandboxes/multigres/multigres_local/data/pooler_xf42rpl6/pg_sockets -p 25432 -U postgres -d postgres
```

**Connect to multigateway** (via TCP):

```bash
psql -h localhost -p <pg-port> -U postgres -d postgres
```

Where:

- pg-port is from `.provisioner-config.cells.<zone>.multigateway.pg-port`

Example:

```bash
psql -h localhost -p 15432 -U postgres -d postgres
```

### Config Paths

Extract from YAML config at `.provisioner-config.cells.<zone>.pgctld.pooler-dir`

## Examples

**Cluster-wide:**

User: "start the cluster"

- Execute: `./bin/multigres cluster start`

User: "stop cluster"

- Execute: `./bin/multigres cluster stop`

User: "cluster status"

- Execute: `./bin/multigres cluster status`

User: "show me all multipoolers" or "get poolers"

- Execute: `./bin/multigres getpoolers`

User: "check if multipoolers are in recovery" or "check multipooler status"

- Parse config to get all zones and service IDs
- Execute: `./bin/multigres getpoolerstatus --cell <zone> --service-id <id>` for each
- Display pooler_type (PRIMARY/REPLICA) and postgres_role (primary/standby)

User: "check zone1 multipooler status"

- Look up service ID for zone1
- Execute: `./bin/multigres getpoolerstatus --cell zone1 --service-id <id>`

**Individual components:**

User: "stop pgctld"

- Read config to find available pgctld instances
- Ask user which one to stop (zone1, zone2, or zone3)
- Execute stop command with selected pooler-dir

User: "restart pgctld xf42rpl6 as standby"

- Look up pooler-dir for xf42rpl6 in config
- Execute: `./bin/pgctld restart --pooler-dir /path/to/pooler_xf42rpl6 --as-standby`

User: "logs multipooler hm9hmxzm"

- Show: `./multigres_local/logs/dbs/postgres/multipooler/hm9hmxzm.log`

User: "tail pgctld"

- Ask which instance
- Tail the corresponding log file

User: "connect to multipooler zone1" or "psql multipooler xf42rpl6"

- Look up pooler-dir and pg-port from config
- Show: `psql -h <pooler-dir>/pg_sockets -p <pg-port> -U postgres -d postgres`

User: "connect to multigateway" or "psql multigateway"

- Ask which zone
- Show: `psql -h localhost -p <pg-port> -U postgres -d postgres`
