# Multigres cluster views

The gateway exposes what it knows about the cluster as two read-only views
in the `multigres` schema, so an operator can inspect the deployment from a
SQL session without shell access or the admin API:

| View                      | Rows                                                           |
| ------------------------- | -------------------------------------------------------------- |
| `multigres.multigateways` | Every multigateway registered in the topology                  |
| `multigres.multipoolers`  | Every multipooler this gateway tracks, with the health it sees |

```console
psql> SELECT name, cell, routing_role, leadership, serving_status, replication_lag
      FROM multigres.multipoolers ORDER BY name;
   name    | cell  | routing_role | leadership | serving_status | replication_lag
-----------+-------+--------------+------------+----------------+-----------------
 pooler-1  | zone1 | primary      | leader     | serving        |
 pooler-2  | zone1 | replica      | follower   | serving        | 00:00:00.0012
(2 rows)

psql> SELECT name, cell, pid_prefix, is_self FROM multigres.multigateways;
    name     | cell  | pid_prefix | is_self
-------------+-------+------------+---------
 gateway-a1  | zone1 |        533 | t
 gateway-a2  | zone2 |       1640 | f
(2 rows)
```

Both views work in the simple and extended protocols, in any query shape
PostgreSQL accepts for a relation: `WHERE`, `JOIN` (including with each
other and with real tables), aggregates, `ORDER BY`, `EXPLAIN`, subqueries,
prepared statements with parameters. They are readable by every client the
gateway admits (see below).

## The `multigres.` qualifier is required

Only a reference qualified with the `multigres` schema is recognized. The
gateway does not know a session's effective `search_path` (it may come from
`SET`, connection options, `ALTER ROLE ... SET`, `set_config()` with a runtime
value, or a function body) and cannot see the catalog to tell whether an
unqualified `multipoolers` shadows a real table. An unqualified name is sent
to the backend as written and fails there. This is the same rule as
[`multigres.version()`](./multigres_version.md).

## Columns

### `multigres.multipoolers`

Rows come from the gateway's pooler cache and the health streams it holds
open to each pooler, so the view shows the cluster **as this gateway sees
it**. Two gateways can legitimately disagree during a failover.

| Column                 | Type        | Source                                                                                                           |
| ---------------------- | ----------- | ---------------------------------------------------------------------------------------------------------------- |
| `name`                 | text        | Pooler ID                                                                                                        |
| `cell`                 | text        | Pooler ID                                                                                                        |
| `database`             | text        | Shard key                                                                                                        |
| `table_group`          | text        | Shard key                                                                                                        |
| `shard`                | text        | Shard key                                                                                                        |
| `routing_role`         | text        | `primary`, `replica` or `unknown`: the pooler's self-reported writability, from the health stream                |
| `leadership`           | text        | `leader`, `stale-leader` or `follower`: the gateway's elected routing primary for the shard (see load balancing) |
| `serving_status`       | text        | `serving`, `disabled` or `draining`, from the health stream                                                      |
| `lifecycle`            | text        | `starting`, `active`, `stopping`, `shutdown`, `quarantined` or `unknown`, from the topology record               |
| `lifecycle_reason`     | text        | Operator-readable context for the lifecycle state                                                                |
| `lifecycle_updated`    | timestamptz | When the lifecycle state was written                                                                             |
| `replication_lag`      | interval    | Lag reported on the health stream; NULL on the primary or when unmeasured                                        |
| `connected`            | boolean     | The gateway has a live, non-timed-out health stream to the pooler                                                |
| `health_last_response` | timestamptz | Last health update received; NULL if none yet                                                                    |
| `discovery_state`      | text        | `live`, or `missing-from-topo` while the pooler is in the cache's grace window                                   |
| `last_change`          | timestamptz | Last topology change or cache state transition for the pooler                                                    |

When the gateway has no health stream to a pooler yet, `routing_role` and
`serving_status` fall back to the topology record and `leadership` is
`follower`.

### `multigres.multigateways`

Rows are read from the topology on every query: the gateway does not watch
its peers, and the view is for operators rather than the query path. When
the topology is unreachable the query fails with `connection_failure`.

| Column       | Type    | Source                                                                                  |
| ------------ | ------- | --------------------------------------------------------------------------------------- |
| `name`       | text    | Gateway ID                                                                              |
| `cell`       | text    | Gateway ID                                                                              |
| `pid_prefix` | integer | Prefix encoded into client PIDs for cancel routing; NULL for a gateway that claims none |
| `is_self`    | boolean | True on the row for the gateway serving the session                                     |

The gateway's registered hostname and ports are deliberately not exposed: in
a Kubernetes deployment they name the pod, not anything a client can connect
to. `pid_prefix` is the column that ties a row to a session — it is the upper
bits of the PID every connection through that gateway receives — and `is_self`
marks the gateway serving the current one.

## How it works: execution-time folding

PostgreSQL has no such relations, so — as with `multigres.version()` — the
gateway rewrites the query before the backend sees it. Each reference in a
`FROM` position becomes a subquery over the current snapshot, carried as one
JSON value:

```sql
SELECT name FROM multigres.multigateways WHERE is_self
-- becomes, at execution time:
SELECT name FROM (SELECT * FROM json_to_recordset(<snapshot>)
                  AS _ (name TEXT, cell TEXT, pid_prefix INT, is_self BOOLEAN))
                 AS multigateways
WHERE is_self
```

Unlike the version fold, which runs once at parse time because the value is a
constant, this fold runs **at every execution**: the planner only wraps the
plan (`engine.ClusterViewRoute`), and the primitive clones the statement,
folds the snapshot in, and routes the result. The plan and its plan-cache
entry therefore carry no data, and a prepared statement sees fresh rows each
time it is executed. The column definition list types every column, so an
empty snapshot (`[]`) yields zero correctly typed rows.

The snapshot travels differently per protocol, and the statement text never
depends on it:

- **Simple protocol**: the snapshot is a string literal in the query. Nothing
  is prepared, so the text can vary freely.
- **Extended protocol**: each view's snapshot is appended to the portal as
  one extra text bind parameter, numbered after the client's, and the portal
  is forwarded with the folded text as its prepared statement — the way any
  rewritten query is forwarded. The folded text is identical for every
  snapshot, so the backend prepares it once per client statement rather than
  once per execution; it binds the client's parameters and honours the
  result formats the client asked for (drivers request binary results for
  integer, boolean and timestamp columns).
- **Describe**, and **Parse inside an explicit transaction** (which the
  gateway forwards to the backend immediately), use the fold over an empty
  literal: valid SQL with the exact column names and types, no extra bind
  parameter, and again a stable text.

A user alias goes on the outer subquery, so `FROM multigres.multipoolers AS
p(n)` renames only the first column and the rest keep their names, as against
a real relation. The views appear only in queries: there are no catalog
entries, so `\d multigres.multipoolers` in psql finds nothing.

## What is rejected

The gateway rejects, with `feature_not_supported`:

- `INSERT`, `UPDATE`, `DELETE`, `MERGE` and `TRUNCATE` targeting a view
  ("is a read-only view"). DDL, `LOCK` and `GRANT` naming a view are not
  folded and reach the backend, where the superuser-owned `multigres`
  sidecar schema rejects them.
- `CREATE VIEW` and `CREATE MATERIALIZED VIEW` over a view: the definition
  would persist one snapshot and silently stop reflecting the cluster.
  `CREATE TABLE ... AS SELECT` is allowed: it materializes a snapshot, which
  is what it says.
- Holdable cursors (`DECLARE ... WITH HOLD`), temporary tables created from a
  view (`CREATE TEMP TABLE ... AS`, `SELECT ... INTO TEMP`) and `COPY (SELECT
...)`: these take plan paths that forward the statement text unfolded.
  Plain cursors, `UPDATE ... FROM`, `DELETE ... USING` and `MERGE ... USING`
  a view all work.

The same rejections apply to a Parse sent inside an explicit transaction,
so the client sees the gateway's message rather than a backend error that
aborts the transaction.

When the topology cannot be read for `multigres.multigateways`, the query
fails with `cannot_connect_now` (57P03) rather than a connection-class
error, so connection pools do not discard the client connection.

## Who can read the views

Every client the gateway admits can select from both views; nothing is
granted or checked on the backend, and no superuser or role check is made.
This is deliberate: the views are the operator's first stop when debugging
routing from a SQL session. It does mean any database role can see gateway
and pooler names, cells, PID prefixes and lifecycle reasons.
Gating the views on a role is a one-line check in the primitive if a
deployment needs it.

## Code organization

| File                                 | Role                                                       |
| ------------------------------------ | ---------------------------------------------------------- |
| `common/constants/gateway.go`        | View names                                                 |
| `engine/cluster_view.go`             | Column specs, fold, Describe stub, `ClusterViewRoute`      |
| `planner/cluster_view.go`            | Wraps the Route, rejects writes                            |
| `executor/executor.go`               | Describe and in-transaction Parse use the empty fold       |
| `poolergateway/cluster_view_rows.go` | `multipoolers` rows from the pooler cache and health state |
| `multigateway/cluster_views.go`      | `multigateways` rows from the topology; wires both sources |
