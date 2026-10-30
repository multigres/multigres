# Routing rules

A routing rule overrides where a database's catalog sends traffic while an
operation moves data: _traffic the catalog would send to X is served by Y_.
Rules live in the database's catalog, on the primary of its `default`
tablegroup, in `multigres.routing_rule`, and every rule belongs to the
operation (`multigres.operation`) that created it. When no operation runs,
there are no rules.

This page describes what is implemented today: shard rules, the RPCs that set
and read them, and how gateways apply them. It is enough to serve a database's
application traffic from an external PostgreSQL behind an unmanaged pooler
during an import. Fences, the fenced switch and revert, and the fold that ends
an operation come later; until then **rules may change only while the
affected application traffic is quiesced**.

## Rules

A **shard rule** says that the routing identifiers of tablegroup `G` that fall
in shard `S`'s range are served by `S`, a shard of tablegroup `G'`. It covers
exactly its target shard's range. Rules apply at execute time, as a single hop:
a rule's target is never routed again, and the planner keeps planning against
the catalog.

The `default` tablegroup holds the catalog and is never a rule's source, with
one exception: an **import** may route `default` while no table is registered
in it (`multigres.tablegroup_table` has no `default` rows). Until the gateway
resolves objects, every statement goes to `default`, so this is how all of a
database's traffic (tables, functions, sequences, `SELECT 1`, LISTEN/NOTIFY)
reaches the import's source.

Table rules (a table planned as a member of another tablegroup) are modeled in
the catalog but not accepted yet.

Every change replaces the whole rule set of one operation, in one catalog
transaction that locks `multigres.catalog_state`, checks the caller's expected
routing version, validates the rule set the change leaves behind, and
increments `routing_version`. A change that is already in effect succeeds and
returns the current version, so a retry is safe.

## Traffic classes

The gateway classifies a request by where it originates, never by its SQL.

- **Application traffic** is everything the planner builds from client SQL,
  including Describe, COPY, cursors, transactions, temporary tables,
  replication connections and the LISTEN stream. It follows the database's
  routing rules.
- **Control traffic** always goes to the database's `default` primary and is
  never rewritten: credential lookups, routing snapshot reads, and the
  multiadmin and migrator RPCs. The gateway marks it with `Target.control`.
- **Cancel requests** are not routed; they go to the pooler holding the
  backend.

### Logins

Credential lookups always read the `default` tablegroup. Poolers, managed and
unmanaged alike, log in to their PostgreSQL with the SCRAM keys the client's
login produced, so **every PostgreSQL of the database must hold the same role
verifiers**. Before an import routes traffic to an external PostgreSQL, its
roles are copied into the database **verbatim** (the same `rolpassword` SCRAM
verifiers, and the role memberships). Running `ALTER ROLE ... PASSWORD` instead
produces a new salt and breaks logins through the external side. A role
changed on the external PostgreSQL during an import must be copied again.

### Sessions across a rule change

A session that holds a reserved backend (an open transaction, a temporary
table, a cursor, an advisory lock) keeps using it until it is released, even if
the rules change in between; its later statements follow the rules again. This
is why rules must change only while the application is quiesced until fences
exist.

### LISTEN/NOTIFY

NOTIFY is an ordinary statement and follows the rules. A session's LISTEN
stream goes where its database's application traffic goes; when a rule change
moves that traffic, the stream is closed and reopened on the new side with the
session's channels. Notifications sent in between are not delivered.

### Replication connections

Replication connections follow the rules like other application traffic.
Replication slots do not move with them: after a rule change, a consumer
recreates its slot on the side that now serves the database.

## Gateways

Each gateway keeps the routing of every database it serves:

- The first statement of a database waits for a snapshot read from the
  database's catalog primary (`GetRoutingSnapshot`), shared by every statement
  that arrives meanwhile. During a planned failover of the catalog primary
  that read waits in the failover buffer, as the statement itself would. If
  the read fails (no catalog primary and no failover in progress, or the
  failover outlasts the buffer), the database's application traffic fails
  with **57P03**, which clients retry; it never goes to the catalog's
  placement on a guess.
- Afterwards the gateway reads the snapshot again every
  `--routing-poll-interval` (default 10s), without waiting out failovers. It
  installs only a snapshot with a higher `routing_version`, and keeps the last
  one when a read fails.
- A database no statement used for 10 minutes is forgotten; its next statement
  reads the snapshot again.

Two consequences of failing closed:

- A gateway that starts, or first sees a database, while that database's
  catalog primary is down and no failover is in progress serves none of the
  database's application traffic, replica-port reads included, until a
  catalog primary answers.
- Replica-port reads follow the rules like everything else. When a rule sends
  a database to a tablegroup without replicas (an import's unmanaged poolers),
  they fail rather than read the `default` tablegroup's replicas, which hold
  only a partial copy during an import.

A catalog primary that predates routing rules answers `GetRoutingSnapshot`
with UNIMPLEMENTED; the gateway then routes the database as it is placed and
logs a warning once per database. **Upgrade every pooler of a database's
`default` tablegroup before creating any rule**, or a gateway reaching an old
catalog primary sends traffic to `default` despite the rule.

## API

The catalog primary serves the RPCs; multiadmin forwards them to it.

| RPC                  | Multiadmin REST                                                                                        | Service            |
| -------------------- | ------------------------------------------------------------------------------------------------------ | ------------------ |
| `GetRoutingSnapshot` | `GET /api/v1/databases/{database}/routing`                                                             | MultipoolerService |
| `EnsureImportTarget` | `PUT /api/v1/databases/{database}/routing/operations/{operation_id}/import-target`                     | MultipoolerManager |
| `SetRoutingRules`    | `PUT /api/v1/databases/{database}/routing/operations/{operation_id}/rules`                             | MultipoolerManager |
| `DeleteRoutingRules` | `DELETE /api/v1/databases/{database}/routing/operations/{operation_id}/rules?expectedRoutingVersion=N` | MultipoolerManager |

`EnsureImportTarget` creates, if absent, the import operation, the tablegroup
it imports into and that tablegroup's `0-inf` shard. It changes no routing.
The RPC is temporary, for the first version of imports, and will be
deprecated. Tablegroup creation (the equivalent of `CREATE TABLEGROUP`) plus
the import's PREPARE step (registering its tables, the default rule and
fencing) will replace it.

Errors: a pooler that is not the catalog primary answers `UNAVAILABLE` (find
the primary and retry); a stale expected version or a change the catalog does
not allow answers `FAILED_PRECONDITION`; a malformed request answers
`INVALID_ARGUMENT`.

## Example: serving a database from an external PostgreSQL

This routes the application traffic of database `app` to an external
PostgreSQL fronted by unmanaged poolers of tablegroup `srctg`, shard `0-inf`,
and back. `$MULTIADMIN` is multiadmin's HTTP address.

1. Copy the external PostgreSQL's roles into `app` verbatim.
   `pg_dumpall --roles-only` emits `CREATE ROLE` for roles that may already
   exist; edit those (and the bootstrap superuser) out, then load the rest:

   ```bash
   pg_dumpall --roles-only --dbname="$SOURCE_URL" > roles.sql
   ```

   ```bash
   psql "$APP_DEFAULT_PRIMARY_URL" --file=roles.sql
   ```

2. Create the import operation and the target tablegroup:

   ```bash
   curl -X PUT "$MULTIADMIN/api/v1/databases/app/routing/operations/$OP/import-target" -H 'Content-Type: application/json' \
     -d '{"tablegroup": "srctg"}'
   ```

3. Read the current routing version:

   ```bash
   curl "$MULTIADMIN/api/v1/databases/app/routing"
   ```

4. Quiesce the application, then route `default` to `srctg`, passing the version
   from step 3:

   ```bash
   curl -X PUT "$MULTIADMIN/api/v1/databases/app/routing/operations/$OP/rules" -H 'Content-Type: application/json' \
     -d '{"rules": [{"fromTablegroup": "default", "toTablegroup": "srctg", "toShard": "0-inf"}], "expectedRoutingVersion": "0"}'
   ```

5. Within one poll interval, every gateway sends `app`'s application traffic
   to `srctg`. Verify through a gateway, for example by reading a row that only
   the external PostgreSQL has, and check `GET .../routing` lists the rule.

6. To route back, quiesce the application and delete the rule, passing the
   version the previous call returned:

   ```bash
   curl -X DELETE "$MULTIADMIN/api/v1/databases/app/routing/operations/$OP/rules?expectedRoutingVersion=1"
   ```
