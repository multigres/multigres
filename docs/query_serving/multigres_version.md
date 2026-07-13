# Multigres version

The gateway reports its own version two ways, mirroring how PostgreSQL reports
its own — a short GUC and a full-string function. This lets an operator check
which multigateway build is serving them without shell access to a binary, and
makes the version easy to include in a bug report.

| Surface                      | Analogous to     | Returns               |
| ---------------------------- | ---------------- | --------------------- |
| `SHOW multigres_version`     | `server_version` | Short release version |
| `SELECT multigres.version()` | `version()`      | Full build string     |

```console
psql> SHOW multigres_version;
 multigres_version
-------------------
 0.1.0-SNAPSHOT
(1 row)

psql> SELECT multigres.version();
                       version
------------------------------------------------------
 Multigres 0.1.0-SNAPSHOT (cb4f578bfd76, 2026-07-06) built with go1.26.0
(1 row)
```

Both work in the simple and extended (prepared-statement) query protocols.
`SHOW multigres_version` is answered entirely by the gateway;
`SELECT multigres.version()` is folded to a literal and then runs on PostgreSQL
(see below), so it composes into any query.

## How the function works: constant folding

`multigres.version()` has no backend implementation (PostgreSQL has no
`multigres` schema), so the gateway **folds it into a text literal of the version
before the query reaches the backend** — `multigres.version()` becomes
`'Multigres 0.1.0-SNAPSHOT (...)'`. The query then runs on PostgreSQL unchanged,
which means the function works in **any expression position**, not just as a
standalone target:

```sql
SELECT multigres.version();
SELECT id FROM t WHERE created_by = multigres.version();
SELECT length(multigres.version());
```

The fold runs in the handler (`handler/gateway_functions.go`), applied once the
query is parsed so that the planner, the plan cache, and the extended protocol's
Describe/Execute — which reuse the stored statement text — all see the same
already-substituted query. `HandleQuery` (simple protocol) folds the statements
it already parsed and refreshes the query text from the rewritten AST;
`HandleParse` (extended protocol), which only has a string, folds that. A cheap
substring check skips the work for queries that don't mention the `multigres`
schema. When the sole target is a bare `multigres.version()` with no alias, the
fold also sets the column label to `version`, matching what PostgreSQL would
label a `version()` call (a bare literal is otherwise labelled `?column?`).

## Why a namespaced function

The function is `multigres.version()`, **not** `version()`. A bare `version()`
already exists in PostgreSQL, and tools and ORMs call it expecting the
_PostgreSQL_ version string — so the gateway leaves it alone and routes it to the
backend. `multigres.version()` lives in its own `multigres` schema and does not
shadow it. A `SELECT version()` still reports PostgreSQL; `SELECT
multigres.version()` reports multigres.

## `multigres_version` is not a gateway-managed variable

Despite the `SHOW <name>` surface, `multigres_version` is **not** a
[gateway-managed variable](./gateway_managed_variables.md). It is a read-only
pseudo-variable: not in the `gatewayManagedVariables` registry, and with no
`SET` / `RESET` / `set_config` / transaction behavior. A `SET multigres_version
= ...` falls through to PostgreSQL, which rejects it as an unrecognized
configuration parameter — matching how PostgreSQL treats its own read-only
`server_version`.

## Where the version comes from

The **short** version (`servenv.Version()`) is the `versionName` constant in
`common/servenv/version.go` — a property of the source tree, bumped when cutting
a release and carrying a `-SNAPSHOT` suffix in between. It is deliberately **not**
derived from git tags at build time: a tag is created after (and often on a
different branch from) the commit it names, so a build of a given commit can't
reliably infer its own release version from `git describe`, and the result would
depend on which branch it was built from. Keeping it in source makes the version
a stable property of the commit.

The **full** string (`servenv.AppVersion()`) prefixes that release version with
the binary's VCS identity — revision, a `modified` marker for a dirty tree,
commit date, and Go toolchain — read from `runtime/debug.BuildInfo`. This is the
same build snapshot already used by the `/version` HTTP endpoint and the
OpenTelemetry `service.version` attribute. Go does not embed VCS settings when
building inside a linked git worktree, so worktree dev builds omit the VCS fields
(`Multigres 0.1.0-SNAPSHOT (unknown revision) built with <go>`); normal checkouts
and CI stamp them.

## Code organization

| File                             | Role                                                             |
| -------------------------------- | ---------------------------------------------------------------- |
| `common/servenv/version.go`      | `versionName` — the committed release version constant           |
| `common/servenv/buildinfo.go`    | `Version()` (short) and `AppVersion()` (full) version strings    |
| `common/constants/gateway.go`    | `MultigresVersionVariable`, `MultigresSchema`                    |
| `handler/gateway_functions.go`   | Folds `multigres.version()` into a literal                       |
| `planner/variable_show_stmt.go`  | Intercepts `SHOW multigres_version`                              |
| `engine/gateway_show_version.go` | `GatewayShowVersion` primitive; SHOW matcher and Describe helper |
| `executor/executor.go`           | Serves the extended-protocol `Describe` for SHOW locally         |

## Extended protocol notes

`SHOW multigres_version` is served by the gateway, so it needs two things over
the extended protocol that a backend-routed query gets for free:

- **Describe** is normally forwarded straight to the backend, which has no such
  GUC. The executor intercepts it and returns a synthetic `StatementDescription`
  (no bind parameters, one text column).
- **Execute** must not re-send `RowDescription`. The wire layer emits it whenever
  a result carries `Fields`, so the primitive attaches column metadata only when
  a `Describe('P')` was folded into the `Execute` (`includeDescribe`). Attaching
  it otherwise would send an illegal second `RowDescription` mid-`Execute`.

`SELECT multigres.version()` needs none of this: it is folded to a literal at
parse time, so the backend describes and executes an ordinary `SELECT`.
