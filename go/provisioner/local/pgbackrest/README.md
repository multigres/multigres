# Generation of pgbackrest.conf

## pgBackRest is opinionated

pgBackRest has specific ways of doing things and handles many backup-related tasks, including setting up standby clusters and ensuring backup integrity across timeline changes.

Here are the main parts of pgBackRest's design that affect Multigres:

- It has a config file (usually `pgbackrest.conf`) that specifies the backup
  storage location (known as the pgBackRest repo), a list of all Postgres
  replicas that may perform backups, and other options.
- Because the role of each replica can change over time, the config does _not_
  specify whether each replica is a primary or standby. This information is
  determined at runtime by pgBackRest by querying each Postgres cluster
  directly.
- pgBackRest has an optional server mode. It acts as a TLS-secured rsyncd-like
  server that is used by the pgBackRest command-line tool to copy required files
  from the primary. Multigres needs this.
- Backing up a standby requires:
  1. pgBackRest running on the standby Postgres host (or the pgBackRest server
     running there). It backs up PGDATA files through the file system.
  2. Postgres client connection to the primary Postgres cluster
  3. TLS connection to a pgBackRest server running on the primary Postgres
     host. (They also allow SSH connections, which are not recommended in many
     production environments.)
- When backing up a standby, it does not allow you to specify which standby to backup. This is addressed in our design below.

## How Multigres creates pgBackRest configs

The local provisioner creates one `pgBackRest.conf` file for each cell. These configurations are almost identical, with two important differences:

1. **The Postgres replica tied to the cell is always listed as pg1** (first replica)
2. **Each cell's TLS server listens on a different port**

The following are examples of a 3-cell Multigres deployment (zone1, zone2, zone3) to illustrate this.

### zone1 pooler's pgbackrest.conf

```ini
[global]
repo1-path=/backup/repo
repo1-retention-full=7
; ... other global settings ...

; TLS server configuration
tls-server-cert-file=/backup/repo/tls/server.crt
tls-server-key-file=/backup/repo/tls/server.key
tls-server-ca-file=/backup/repo/tls/ca.crt
tls-server-address=0.0.0.0
tls-server-port=8432            ; <---- this node's TLS server port
tls-server-auth=pgbackrest=*

[multigres]
; pg1 is the local Postgres (zone1's replica)
pg1-path=/zone1/pg_data
pg1-socket-path=/zone1/pg_sockets
pg1-port=5432
pg1-user=postgres
pg1-database=postgres

; When backing up pg1, if pg2 is the primary, pgBackRest uses TLS to retrieve
; a small set of files from pg2. pg1's pgBackRest also connects to pg2's
; Postgres to call pg_start_backup().
pg2-host=localhost
pg2-host-type=tls
pg2-host-port=8433              ; pg2's TLS server port
pg2-host-ca-file=/backup/repo/tls/ca.crt
pg2-host-cert-file=/backup/repo/tls/server.crt
pg2-host-key-file=/backup/repo/tls/server.key
pg2-port=5433
pg2-user=postgres
pg2-database=postgres

; If pg3 is the current primary, these are needed for the same reasons that we
; need pg2-*.
pg3-host=localhost
pg3-host-type=tls
pg3-host-port=8434              ; pg3's TLS server port
pg3-host-ca-file=/backup/repo/tls/ca.crt
pg3-host-cert-file=/backup/repo/tls/server.crt
pg3-host-key-file=/backup/repo/tls/server.key
pg3-port=5434
pg3-user=postgres
pg3-database=postgres
```

There are some things to note:

1. All of these Postgres clusters are just replicas. At backup or restore time,
   pgBackRest will determine which of them is the primary.
2. The Postgres instance associated with this cell is listed as pg1 and uses
   local socket access. The order of the replicas does not matter, except for
   one thing: pgBackRest uses the first declared standby when you allow it to
   backup from standby. So, Multigres must control the ordering of the replicas
   in pgbackrest.conf.
3. Remote replicas (pg2, pg3) use TLS connections to retrieve files from other nodes when needed.
4. There is one _stanza_ called `multigres`, under which all the replicas are declared.

### zone2 pooler's pgbackrest.conf

```ini
[global]
; ... same global settings ...
tls-server-port=8433            ; <---- this node's TLS server port
tls-server-auth=pgbackrest=*

[multigres]
; pg1 is the local Postgres (zone2's replica)
pg1-path=/zone2/pg_data         ; <---- zone2's Postgres is now pg1
pg1-socket-path=/zone2/pg_sockets
pg1-port=5433
pg1-user=postgres
pg1-database=postgres

; The local pgBackRest connects to pg2 via TLS + Postgres protocol (see zone1
; docs for details)
pg2-host=localhost
pg2-host-type=tls
pg2-host-port=8432              ; pg2's TLS server port
; ... TLS cert/key settings ...
pg2-port=5432
pg2-user=postgres
pg2-database=postgres

; The local pgBackRest connects to pg3 via TLS + Postgres protocol (see zone1
; docs for details)
pg3-host=localhost
pg3-host-type=tls
pg3-host-port=8434              ; pg3's TLS server port
; ... TLS cert/key settings ...
pg3-port=5434
pg3-user=postgres
pg3-database=postgres
```

The key difference is that `pg1-*` now refers to zone2's Postgres cluster. This allows us to force pgBackRest to backup `pg1`, not some other replica.

### zone3 pooler's pgbackrest.conf

```ini
[global]
; ... same global settings ...
tls-server-port=8434            ; <---- this node's TLS server port
tls-server-auth=pgbackrest=*

[multigres]
; pg1 is the local Postgres (zone3's replica)
pg1-path=/zone3/pg_data         ; <---- zone3's Postgres is now pg1
pg1-socket-path=/zone3/pg_sockets
pg1-port=5434
pg1-user=postgres
pg1-database=postgres

; The local pgBackRest connects to pg2 via TLS + Postgres protocol (see zone1
; docs for details)
pg2-host=localhost
pg2-host-type=tls
pg2-host-port=8432              ; pg2's TLS server port
; ...

; The local pgBackRest connects to pg3 via TLS + Postgres protocol (see zone1
; docs for details)
pg3-host=localhost
pg3-host-type=tls
pg3-host-port=8433              ; pg3's TLS server port
; ...
```

## Backup of a specific standby

This difference in replica ordering across the MultiPooler's `pgbackrest.conf` files is what allows Multigres to choose which standby to backup.

Suppose that pgBackrest has been asked to backup from a standby. Recall that given a list of Postgres clusters in its config file, pgBackRest selects the first standby to backup. Because each MultiPooler has its own Postgres replica listed first in `pgbackrest.conf`, MultiAdmin simply has to connect to a specific standby MultiPooler to backup the corresponding Postgres standby.

## TLS Server Mode

For each cell, the local provisioner launches an instance of pgBackRest in TLS server mode. This allows nodes to securely retrieve Postgres files from one another. The TLS server is used when backing up from a standby, because pgBackRest requires a small subset of files (like `pg_control`) to be retrieved from the primary.

### How TLS is configured

1. During `GeneratePgBackRestConfigs()`, the provisioner generates TLS certificates:
   - A shared CA certificate and key (`<backup_repo>/tls/ca.crt`, `ca.key`)
   - A shared server certificate and key (`<backup_repo>/tls/server.crt`,
     `server.key`)

2. Each `pgbackrest.conf` includes:
   - TLS server configuration (`tls-server-cert-file`, `tls-server-key-file`,
     `tls-server-ca-file`, `tls-server-address`, `tls-server-port`)
   - TLS client configuration for connecting to other nodes
     (`pgN-host-type=tls`, `pgN-host-port`, `pgN-host-ca-file`,
     `pgN-host-cert-file`, `pgN-host-key-file`)
   - Server authorization (`tls-server-auth=pgbackrest=*`) allowing clients with certificate CN "pgbackrest" to execute any command

3. During database provisioning, a `pgbackrest-server` service is started for each cell via `StartPgBackRestServer()`.

4. When backing up from a standby, the pgBackRest client connects to the primary's TLS server to fetch required files.

Consult the [pgBackRest server reference](https://pgbackrest.org/configuration.html#section-server) for full details on configuring the TLS server.

### Port allocation

pgBackRest TLS servers use port 8432 + cell_index (cells sorted alphabetically, 0-indexed):

- zone1 (index 0): 8432
- zone2 (index 1): 8433
- zone3 (index 2): 8434

The base port is defined in `ports.DefaultPgBackRestTLSPort`.

### Certificate generation

The `tls.go` file provides two functions for certificate generation:

- `GenerateCA()` - Creates a self-signed ECDSA P256 CA certificate (valid for
  10 years)
- `GenerateServerCert()` - Creates a server certificate signed by the CA with
  ServerAuth and ClientAuth extended key usage (valid for 1 year)

The server certificate uses CN "pgbackrest" and includes SANs for `localhost`, `127.0.0.1`, and `::1`.

## Summary of pgbackrest.conf generation rules

### Every pooler has its own `pgbackrest.conf`

- Its own Postgres instance is listed first (as `pg1-*`), because that's the
  implicit target of restores and the first choice for backup source.
- The other poolers have entries (`pg2-*`, `pg3-*`, etc.) are needed because of
  the potential for inter-pooler TCP connections.

### Backing up a primary database

When backing up a primary (e.g. during bootstrap), pgBackRest does not need to
connect to other poolers. However, it will _attempt_ to connect to other
poolers to verify replication status, etc. So, if during bootstrap, if the
standbys are potentially not available, the backup may fail or timeout. A fix
for this (implemented in some test code) is to initially create a
`pgbackrest.conf` that does not reference the standbys. Then, after bootstrap
has completed, the primary would then have a full `pgbackrest.conf` file.

### Backing up a standby database

A pgBackRest process on the standby initiates two connections to the primary pooler node:

1. pgBackRest TLS server: used for retrieving `pg_control` and other files that
   must come from the primary
2. Postgres server port: for issuing `pg_start_backup()`

This is why each `pgbackrest.conf` needs to have the `pgN-*` entries for the other poolers.
