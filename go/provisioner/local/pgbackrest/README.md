# Generation of pgbackrest.conf

## pgBackRest is opinionated

pgBackRest has specific ways of doing things and handles many backup-related tasks, including setting up standby clusters and ensuring backup integrity across timeline changes.

Here are the main parts of pgBackRest's design that affect Multigres:
- It has a config file (usually `pgbackrest.conf`) that specifies the backup repo, a list of all Postgres replicas that may perform backups, and other options.
- Because the role of each replica can change over time, the config does *not* specify whether each replica is a primary or standby. This information is determined at runtime by pgBackRest by querying each Postgres cluster directly.
- pgBackRest has an optional server mode. It acts as a TLS-secured rsyncd-like server that is used by the pgBackRest command-line tool to copy required files from the primary. Multigres needs this.
- Backing up a standby requires:
  1. pgBackRest running on the standby Postgres host (or the pgBackRest server running there). It backs up PGDATA files through the file system.
  2. Postgres client connection to the primary Postgres cluster
  3. TLS connection to a pgBackRest server running on the primary Postgres host. (They also allow SSH connections, which are not recommended in many production environments.)
- When backing up a standby, it does not allow you to specify which standby to backup. This is addressed in our design below.

## How Multigres creates pgBackRest configs

The local provisioner creates one `pgBackRest.conf` file for each multipooler. These configurations are almost identical, with one important difference: **the Postgres replica tied to the MultiPooler is always listed as the first replica.**

The following are examples of a 3-node Multigres deployment to illustrate this.

### MultiPooler 1's pgbackrest.conf

```
[global]
(defines global parameters, such as compression and where backups go)

[multigres]
; this is the configuration for the Postgres cluster associated with this
; MultiPooler
pg1-host=replica1 ; <---- MultiPooler 1's Postgres host
pg1-user=postgres
pg1-database=postgres

pg2-host=replica2
pg2-user=postgres
pg2-database=postgres

pg3-host=replica3
pg3-user=postgres
pg3-database=postgres
```

There are some things to note:
1. All of these Postgres clusters are just replicas. At backup or restore time, pgBackRest will determine which of them is the primary.
2. The Postgres instance associated with this MultiPooler is listed as pg1. The order of the replicas does not matter, except for one thing: pgBackRest uses the first declared standby when you allow it to backup from standby. So, Multigres must control the ordering of the replicas in pgbackrest.conf.
3. There is one *stanza* called `multigres`, under which all the replicas are declared.

## MultiPooler 2's pgbackrest.conf
```
[global]
(defines global parameters, such as compression and where backups go)

[multigres]
; this is the configuration for the Postgres cluster associated with this
; MultiPooler
pg1-host=replica2 ; <--- the only difference: MultiPooler 2's Postgres host
pg1-user=postgres
pg1-database=postgres

pg2-host=replica1
pg2-user=postgres
pg2-database=postgres

pg3-host=replica3
pg3-user=postgres
pg3-database=postgres
```

The only difference here is that `pg1-*` now refers to MultiPooler 2's Postgres cluster. This allows us to force pgBackRest to backup `pg1-host`, not some other replica.

## MultiPooler 3's pgbackrest.conf
```
[global]
(defines global parameters, such as compression and where backups go)

[multigres]
; this is the configuration for the Postgres cluster associated with this
; MultiPooler
pg1-host=replica3 ; <--- the only difference: MultiPooler 2's Postgres host
pg1-user=postgres
pg1-database=postgres

pg2-host=replica1
pg2-user=postgres
pg2-database=postgres

pg3-host=replica2
pg3-user=postgres
pg3-database=postgres
```

## Backup of a specific standby

This difference in replica ordering across the MultiPooler's `pgbackrest.conf` files is what allows Multigres to choose which standby to backup.

Suppose that pgBackrest has been asked to backup from a standby. Recall that given a list of Postgres clusters in its config file, pgBackRest selects the first standby to backup. Because each MultiPooler has its own Postgres replica listed first in `pgbackrest.conf`, MultiAdmin simply has to connect to a specific standby MultiPooler to backup the corresponding Postgres standby.
