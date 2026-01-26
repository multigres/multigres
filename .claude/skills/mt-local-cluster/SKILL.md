---
name: "Local Cluster Manager"
description: "Manage local multigres cluster components (multipooler, pgctld, multiorch, multigateway, s3mock) - start/stop services, view logs, connect with psql, test S3 backups locally"
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
- Start a cluster with S3 backup support using s3mock
- Start/stop/check status of s3mock server
- View s3mock logs

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

## S3Mock Integration

Manage s3mock server for local S3 backup testing. S3mock provides a lightweight S3-compatible server for testing backups locally without requiring AWS credentials or external services.

### S3Mock Binary

The skill automatically builds `bin/s3mock` if it doesn't exist:

```bash
go build -o bin/s3mock go/tools/s3mock/cmd/s3mock/main.go
```

This happens transparently when starting s3mock - no user action needed.

### Starting Cluster with S3Mock

**User says:** "start cluster with s3mock" / "init cluster with s3" / "create s3 cluster"

**Workflow:**

1. **Build s3mock if needed**
   - Check if `bin/s3mock` exists
   - If not, run: `go build -o bin/s3mock go/tools/s3mock/cmd/s3mock/main.go`

2. **Create necessary directories**
   - `mkdir -p multigres_local/logs`
   - `mkdir -p multigres_local/state`

3. **Start s3mock in background**
   ```bash
   bin/s3mock multigres-test > multigres_local/logs/s3mock.log 2>&1 &
   ```
   - Capture PID: `echo $!`
   - S3mock always runs on port 9000: `https://127.0.0.1:9000`
   - Wait briefly for startup (1-2 seconds) to ensure port is ready

5. **Save state to file**
   - Write `multigres_local/state/s3mock.json`:
   ```json
   {
     "pid": 12345,
     "endpoint": "https://127.0.0.1:9000",
     "bucket": "multigres-test",
     "started_at": "2026-02-05T10:30:00Z",
     "log_file": "multigres_local/logs/s3mock.log",
     "aws_access_key_id": "test",
     "aws_secret_access_key": "test"
   }
   ```

6. **Export credentials**
   ```bash
   export AWS_ACCESS_KEY_ID=test
   export AWS_SECRET_ACCESS_KEY=test
   ```
   - These are dummy credentials (s3mock ignores them)
   - Required for multigres credential verification

7. **Initialize cluster with S3 configuration**
   ```bash
   bin/multigres cluster init \
     --backup-url=s3://multigres-test/backups/ \
     --region=us-east-1
   ```
   - S3mock always runs on port 9000
   - Region can be any value (s3mock doesn't enforce it)
   - S3mock endpoint is automatically detected by the cluster

8. **Start the cluster**
   ```bash
   bin/multigres cluster start
   ```

9. **Show success message**
   - Display: "Cluster started with S3 backup support"
   - Show endpoint: `https://127.0.0.1:9000` (fixed port)
   - Show bucket: `multigres-test`
   - Show log location: `multigres_local/logs/s3mock.log`

### Independent S3Mock Operations

**Start s3mock:** "start s3mock"
- Builds s3mock if needed
- Starts s3mock with bucket 'multigres-test'
- Saves state to `multigres_local/state/s3mock.json`
- Shows endpoint and credentials

**Stop s3mock:** "stop s3mock"
- Reads PID from `multigres_local/state/s3mock.json`
- Sends SIGTERM: `kill <pid>`
- Waits for graceful shutdown (5 second timeout)
- Removes state file
- Logs are preserved for debugging

**Check status:** "s3mock status" / "check s3mock"
- If state file doesn't exist: "s3mock not running"
- If state file exists but PID dead: "s3mock not running (stale state cleaned up)" + remove state file
- If running, show:
  - Running: ✓ (PID: 12345)
  - Endpoint: <https://127.0.0.1:9000>
  - Bucket: multigres-test
  - Log file: multigres_local/logs/s3mock.log
  - Started: 2026-02-05T10:30:00Z

**View logs:** "s3mock logs" / "tail s3mock"
- If s3mock not running: show full log with `cat multigres_local/logs/s3mock.log`
- If s3mock running: tail log with `tail -f multigres_local/logs/s3mock.log`

**Restart s3mock:** "restart s3mock"
- Stop s3mock if running
- Start s3mock
- Update state file with new PID and endpoint

### Auto-Stop Behavior

**When running `cluster stop`:**
1. Check if `multigres_local/state/s3mock.json` exists
2. If exists, automatically stop s3mock:
   - Read PID from state file
   - Send SIGTERM to process
   - Remove state file
3. Continue with normal cluster stop

**When running `cluster stop --clean`:**
1. Stop s3mock as above
2. Also remove logs: `rm -f multigres_local/logs/s3mock.log`
3. Continue with normal cluster cleanup

### State Management and Credentials

**State file location:** `multigres_local/state/s3mock.json`

**Automatic credential export:**
- Before running any `multigres cluster` command, check if `multigres_local/state/s3mock.json` exists
- If exists, automatically export credentials from state file:
  ```bash
  export AWS_ACCESS_KEY_ID=$(jq -r .aws_access_key_id multigres_local/state/s3mock.json)
  export AWS_SECRET_ACCESS_KEY=$(jq -r .aws_secret_access_key multigres_local/state/s3mock.json)
  ```
- This happens transparently - user doesn't need to think about credentials

**Stale state handling:**
- If state file exists but PID is not running: `ps -p <pid> > /dev/null 2>&1`
- Clean up stale state file automatically
- Report to user: "s3mock not running (stale state cleaned up)"

### Error Handling

**S3mock binary missing during build:**
- Source path doesn't exist: "Error: s3mock source not found at go/tools/s3mock/cmd/s3mock/main.go"
- Build fails: Show build error and suggest checking Go installation

**S3mock fails to start:**
- Parse error from `multigres_local/logs/s3mock.log`
- Show clear message: "Failed to start s3mock: [error details]"
- Clean up any partial state

**S3mock crashes during operation:**
- Backup/restore operations will fail with connection errors from pgbackrest
- User can check: `multigres_local/logs/s3mock.log`
- Skill status check will detect (PID dead but state exists)
- User can restart: "restart s3mock"

**Cluster already initialized:**
- If `multigres_local/multigres.yaml` exists when user says "start cluster with s3mock"
- Warn: "Cluster already exists. To reinitialize with s3mock, run 'cluster stop --clean' first"
- Option: "Or run 'start s3mock' separately to start s3mock for existing cluster"

**Port conflicts:**
- S3mock always uses port 9000
- If port 9000 is already in use, s3mock will fail to start
- Check for conflicts: `lsof -i :9000` or `netstat -an | grep 9000`
- Error will appear in s3mock.log: "address already in use"
- User must stop the conflicting service or choose to use that existing s3mock instance

### Examples

**Example 1: Start fresh cluster with S3 backup**

User: "start cluster with s3mock"

Skill:
- Builds s3mock binary
- Starts s3mock on <https://127.0.0.1:9000>
- Exports AWS_ACCESS_KEY_ID=test and AWS_SECRET_ACCESS_KEY=test
- Initializes cluster with S3 backup
- Starts cluster
- Shows: "Cluster started with S3 backup support. Endpoint: <https://127.0.0.1:9000>"

**Example 2: Stop cluster with s3mock**

User: "cluster stop"

Skill:
- Stops all cluster components
- Detects s3mock is running (via state file)
- Automatically stops s3mock
- Shows: "Cluster stopped (including s3mock)"

**Example 3: Check s3mock status**

User: "s3mock status"

Skill shows:
```text
s3mock status:
  Running: ✓ (PID: 12345)
  Endpoint: https://127.0.0.1:9000
  Bucket: multigres-test
  Log file: multigres_local/logs/s3mock.log
  Started: 2026-02-05 10:30:00
```

**Example 4: Independent s3mock for existing cluster**

User: "start s3mock"

Skill:
- Starts s3mock independently
- Shows endpoint and bucket
- Notes: "s3mock started. To use with cluster, reinitialize with: cluster stop --clean && start cluster with s3mock"

**Example 5: View s3mock logs**

User: "s3mock logs"

Skill:
- If running: `tail -f multigres_local/logs/s3mock.log`
- If stopped: `cat multigres_local/logs/s3mock.log`

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

## Command Examples

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

**S3Mock operations:**

User: "start cluster with s3mock" or "init cluster with s3"

- Build s3mock if needed
- Start s3mock in background on port 9000
- Export AWS credentials (test/test)
- Execute: `./bin/multigres cluster init --backup-url=s3://multigres-test/backups/ --region=us-east-1`
- Execute: `./bin/multigres cluster start`
- Display endpoint and bucket info

User: "start s3mock"

- Build s3mock if needed
- Execute: `bin/s3mock multigres-test > multigres_local/logs/s3mock.log 2>&1 &`
- Save state with endpoint <https://127.0.0.1:9000>
- Display: "s3mock started at <https://127.0.0.1:9000>"

User: "stop s3mock"

- Read PID from state file
- Execute: `kill <pid>`
- Remove state file

User: "s3mock status"

- Check if state file exists and PID is running
- Display status (running/not running), endpoint, bucket, log location

User: "s3mock logs" or "tail s3mock"

- If running: `tail -f multigres_local/logs/s3mock.log`
- If stopped: `cat multigres_local/logs/s3mock.log`
