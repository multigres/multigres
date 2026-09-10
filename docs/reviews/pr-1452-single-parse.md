# PR #1452: Single Backend Parse Review

## Outcome and Scope

Inside a transaction, a client Parse now creates or refreshes the named backend
statement that ordinary Describe and Execute will reuse. The former unnamed
validation Parse is replaced by useful named preparation. Transaction-time
validation and locks remain at receipt time.

This review covers the local optimization relative to commit `da4c4c92`
(`feat: reparse only in transaction`) on
[PR #1452](https://github.com/multigres/multigres/pull/1452). The implementation
and tests were validated on September 9, 2026; this review was written on
September 10. The changes are uncommitted at the time of writing.

The PR already introduced `PreparedStatement.force_reparse`, the gateway's
pending-Parse signal, broader reactive recovery for stale parameter types,
and the DDL matrix. Those are the starting point for this optimization, not
new features introduced by it.

## Why the Previous Path Parsed Twice

The pooler consolidates statements by exact SQL text and parameter type hints,
then caches each named PostgreSQL statement on the backend that prepared it.
After DDL, an unrelated client can inherit a cached statement with outdated
result columns or inferred parameter types. The PR's forced preparation
prevents a fresh client Parse from reusing that stale statement inside a
transaction, where a Bind/Execute error would abort the transaction.

Before this optimization, two operations served different purposes:

| Stage                           | Previous behavior                                                  | New behavior                                                           |
| ------------------------------- | ------------------------------------------------------------------ | ---------------------------------------------------------------------- |
| Receive Parse in a transaction  | Parse the unnamed backend statement for validation and locks       | Force preparation of the named `ppstmt` and cache it                   |
| First ordinary Describe/Execute | Consume pending signal and prepare the named statement again       | Reuse the statement already prepared on that backend                   |
| Subsequent ordinary use         | Reuse named statement                                              | Reuse named statement                                                  |
| Receive Parse in autocommit     | Register at gateway; backend preparation is lazy                   | Same lazy preparation                                                  |
| Execute a semantic SQL rewrite  | Prepare the rewritten query, using the pending signal if available | Prepare the rewritten query and consume the signal specifically for it |

The duplicate Parse already existed on cache misses. The PR made the later
named Parse forced even on a cache hit. An unnamed statement cannot serve as
persistent storage for several prepared queries: another unnamed Parse or a
simple query can replace it. Preparing a named statement at receipt time makes
the first preparation reusable.

“Single Parse” refers to the backend preparation of ordinary SQL on the same
reserved connection. It does not eliminate the gateway's SQL-to-AST parsing,
its routing-plan lookup, or PostgreSQL's execution planning at Bind time.

## Resulting Flow

```mermaid
sequenceDiagram
    participant C as Client
    participant G as Multigateway
    participant P as Multipooler
    participant B as Reserved PostgreSQL backend
    C->>G: Parse / SQL PREPARE while in transaction
    G->>P: StreamExecute: prepare_only + force_reparse
    P->>B: Replay BEGIN if needed
    opt Statement already cached on this backend
        P->>B: Close named ppstmt
    end
    P->>B: Parse named ppstmt
    B-->>P: Parse succeeds
    Note over P: Cache named statement on this connection
    P-->>G: Reservation state / success
    Note over G: Register client statement; record possible rewrite refresh
    G-->>C: ParseComplete / PREPARE completion
    C->>G: Describe / Bind + Execute
    G->>P: Original statement metadata and portal, as applicable
    Note over P: ensurePrepared finds cached statement
    P->>B: Describe / Bind + Execute
    B-->>P: Response
    P-->>G: Response
    G-->>C: Response
```

If preparation fails, registration does not occur and the existing error and
reservation handling applies. A failed PostgreSQL transaction cannot be fixed
by the reactive retry helper; it returns the error to the client.

## Production Changes, File by File

Paths below are relative to the repository root.

| File                                                                             | Change and reason                                                                                                                                                                                                                                                                                                                    |
| -------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| [proto/query.proto](../../proto/query.proto)                                     | Renames `ExecuteSqlPreparedStatement.force_unnamed_parse` to `prepare_only`, retaining boolean field number 4. The request now means named preparation without executing a SQL wrapper. `PreparedStatement.force_reparse` separately determines whether a cache hit must be refreshed.                                               |
| [go/pb/query/query.pb.go](../../go/pb/query/query.pb.go)                         | Regenerated with `make proto`; updates the Go field, getter, comments, and protobuf descriptor for `PrepareOnly`. It is generated output, not a second hand-written implementation.                                                                                                                                                  |
| [gateway handler](../../go/services/multigateway/handler/handler.go)             | Renames the executor interface method and call to `PrepareInTransaction`. Keeps preparation before consolidator registration and preserves transaction-error handling. The pending signal now exists only for a possible execution-time rewrite.                                                                                     |
| [gateway executor](../../go/services/multigateway/executor/executor.go)          | `PrepareInTransaction` sends `PrepareOnly: true` and `ForceReparse: true` through the existing reservation-capable `StreamExecute` path. A fresh client Parse reaches PostgreSQL even if the named statement is already cached.                                                                                                      |
| [pooler executor](../../go/services/multipooler/internal/executor/executor.go)   | Renames the prepare-only dispatch and helpers. Replaces `conn.Parse(ctx, "", ...)` with `ensurePrepared`, which resolves the pooler name, refreshes when requested, and records a successful named preparation. Both new and existing reservation paths retain BEGIN-before-preparation ordering and their existing failure cleanup. |
| [Route](../../go/services/multigateway/engine/route.go)                          | Preserves the original stored query if route SQL differs only by AST normalization. For a semantic rewrite, builds separate metadata and consumes the pending signal there, setting `ForceReparse` only on the new metadata. Bind values and type hints remain attached to the rewritten query.                                      |
| [scatter connection](../../go/services/multigateway/scatterconn/scatter_conn.go) | Removes `preparedStatementForSend`. Describe and portal execution send their statement metadata directly, avoiding a redundant force at ordinary materialization and preventing Describe from consuming a rewrite's signal.                                                                                                          |
| [connection state](../../go/services/multigateway/handler/connection_state.go)   | Documents `reparsePending` as a signal keyed by canonical gateway name for semantic rewrites. Commit and rollback clear unused entries so they do not accumulate across transactions or trigger a later transaction's refresh. This does not deallocate backend statements.                                                          |

### Why Preserve the Received SQL?

The gateway routing cache uses normalized SQL; the pooler uses exact SQL text.
For example, these can share a gateway routing plan while selecting different
pooler statement names:

```sql
SELECT  *  FROM  reptest  WHERE  id = $1
SELECT * FROM reptest WHERE id = $1
```

Previously, Route could reconstruct and prepare the normalized version at
Execute, even if receipt-time preparation and Describe used the first string.
The new comparison also accepts `psi.AstStmt().SqlString()` as equivalent for
routing purposes, and forwards the stored original SQL. This avoids a second
backend key and the construction/parsing of replacement metadata for that case.

This is equality against the parser's own normalized representation, not a
general-purpose SQL equivalence check.

### Why Keep Any Pending-Reparse State?

Some routes change behavior, such as replacing a gateway-managed `set_config`
call with a constant. Executing the original would discard that rewrite.
The rewritten SQL has its own pooler cache entry, which may also be stale.
Preparing the original cannot refresh that entry.

The signal therefore survives an ordinary Describe and is consumed when Route
first materializes the semantic rewrite. It is applied to newly allocated
metadata because the consolidator's original `PreparedStatementInfo` is shared
across client connections. Later use of that route does not force another
refresh. The focused DDL test exercises Describe between Parse and the rewritten
execution, which is the important ordering case.

## Test Changes, File by File

| File                                                                                                               | Change and what it establishes                                                                                                                                                                                                                                                                                 |
| ------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [engine/route_test.go](../../go/services/multigateway/engine/route_test.go)                                        | Adds `TestRoutePortalPreservesPreparedQuery`: normalization keeps the original portal; a semantic rewrite changes SQL, preserves binds, forces once, and does not mutate shared statement metadata.                                                                                                            |
| [engine/copy_statement_test.go](../../go/services/multigateway/engine/copy_statement_test.go)                      | Extends the shared `mockIExecute` to record the portal it receives, enabling the Route assertions. No COPY behavior changes.                                                                                                                                                                                   |
| [gateway executor tests](../../go/services/multigateway/executor/executor_test.go)                                 | Renames the preparation test and checks both `PrepareOnly` and `ForceReparse` on the outgoing carrier.                                                                                                                                                                                                         |
| [handler/connection_state_test.go](../../go/services/multigateway/handler/connection_state_test.go)                | Adds `TestReparsePendingEndsWithTransaction` for both commit and rollback.                                                                                                                                                                                                                                     |
| [handler/handler_test.go](../../go/services/multigateway/handler/handler_test.go)                                  | Renames the mock executor method to match the interface. Existing tests still check transaction-only eager preparation and no registration after failure.                                                                                                                                                      |
| [handler/transaction_helpers_test.go](../../go/services/multigateway/handler/transaction_helpers_test.go)          | Renames the tracking mock's interface method; transaction-helper behavior is unchanged.                                                                                                                                                                                                                        |
| [planner/prepared_stmt_test.go](../../go/services/multigateway/planner/prepared_stmt_test.go)                      | Renames the mock handler executor's interface method; planner behavior is unchanged.                                                                                                                                                                                                                           |
| [pooler executor tests](../../go/services/multipooler/internal/executor/executor_test.go)                          | Updates prepare-only names and flags in reservation/error tests. Adds `TestPrepareOnlyReusesNamedStatement`: after successful preparation, the fake server rejects further Parses; ordinary cache reuse still succeeds, but a fresh forced preparation reaches the server and fails, evicting the stale entry. |
| [prepared_stmt_test.go](../../go/test/endtoend/queryserving/prepared_stmt_test.go)                                 | Adds `TestTransactionParseMaterializesOnce`: PostgreSQL exposes the named statement immediately after Parse; its name and preparation time survive another Parse, Describe, and repeated execution. No normalized duplicate appears.                                                                           |
| [describe_stale_across_clients_test.go](../../go/test/endtoend/queryserving/describe_stale_across_clients_test.go) | Extends the UUID-to-bigint regression into formatting-only and semantic-rewrite cases, adding Describe before execution. Much of this diff is indentation from the new subtests.                                                                                                                               |

The existing DDL matrix was run without changing its assertions.

## Validation

These are recorded results from the implementation turn, not new test runs
performed while writing this document.

| Check                                                                 | Recorded result                                                                       |
| --------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| `make proto` and `make build`                                         | Passed                                                                                |
| Affected unit packages                                                | All 12 packages passed                                                                |
| Selected query-serving end-to-end run                                 | All 11 top-level tests passed; 65 tests including subtests; no skips; 138.739 seconds |
| `TestPreparedDDLMatrix`                                               | Passed in 33.25 seconds; 108 comparison cells                                         |
| Extended UUID-to-bigint regression, run after adding rewrite coverage | Both subtests passed; package completed in 28.962 seconds                             |
| Transaction-end signal cleanup unit test                              | Passed; package completed in 1.214 seconds                                            |
| PostgREST I/O                                                         | 18 passed, 0 failed, 0 pending; package completed in about 47.6 seconds               |
| Formatting and whitespace                                             | `goimports`, `gofumpt`, and `git diff --check` completed successfully                 |

The matrix covers nine DDL variants × three operations × two reuse modes ×
two transaction modes. It reported **21 allowed differences out of 108 cells**,
all within the autocommit reuse-without-reparse allowance. The invariant is
that every reprepare case and every in-transaction case matches direct
PostgreSQL. Passing does not mean all 108 cells are identical.

The PostgREST run used the harness's curated I/O selection, including
`test_notify_reloading_catalog_cache`. It did not run every upstream I/O test,
the hspec suite, or a fresh direct-PostgreSQL baseline. No end-to-end latency
benchmark or mixed-version deployment test was run.

Reproduction commands, from the repository root using the `mt-dev` workflow:

```bash
go test -short -count=1 \
  ./go/services/multigateway/... \
  ./go/services/multipooler/internal/executor

make build
scripts/portpool.sh start
export MULTIGRES_PORT_POOL_ADDR=/tmp/multigres-port-pool.sock

go test -json -count=1 -timeout=20m \
  -run 'Test(PreparedDDLMatrix|TransactionParseMaterializesOnce|SQLPrepareEagerParseInTransaction|PreparedStatementTransactionSemantics|SimpleProtocolPreparedStatements|WrappedPreparedStatementExecution|ReprepareParamTypeAfterDDLInTransaction|ReservedExecuteStaleAcrossClients|DescribeStaleAcrossClients|CachedPlanReprepareAfterDDL|PreparedExecuteSetConfig_TrackedAndIsolated)$' \
  ./go/test/endtoend/queryserving

RUN_POSTGREST=1 go test -json -count=1 -timeout=55m \
  -run '^TestPostgRESTIO$' ./go/test/endtoend/queryserving/postgresttests
```

Local logs from the recorded runs are in `/tmp/quasar-prepare-unit.log`,
`/tmp/quasar-prepare-e2e.jsonl`, `/tmp/quasar-prepare-rewrite.jsonl`,
`/tmp/quasar-prepare-state.log`, and `/tmp/quasar-prepare-postgrest.jsonl`.
These temporary files are not committed or durable CI artifacts.

## Review Boundaries and Tradeoffs

- **Preserve receipt-time behavior.** This is still eager backend preparation
  in a transaction. The change makes that work reusable; simply delaying it
  until Execute would lose prepare-time locks and error timing.
- **One query on one backend.** Semantic rewrites and another pooled backend
  can require separate preparation. The implementation currently routes this
  preparation to the default table group/shard; it does not establish a
  multi-shard single-Parse guarantee.
- **Measured scope.** The ordinary flow loses a duplicate backend Parse and a
  formatting-only metadata rebuild. Required Describe/Execute RPCs and a Close
  when refreshing an existing named statement remain. No latency percentage
  is claimed.
- **Wire compatibility needs explicit review.** Field 4 remains a boolean,
  but its name and behavior change from unnamed validation to named
  preparation. Binary decodability alone does not prove rolling-upgrade
  compatibility. An old pooler still creates only the unnamed statement,
  while the new gateway expects the original named statement to be refreshed
  already. Matching gateway/pooler versions were tested; mixed-version
  operation requires separate validation or a compatibility design.
- **Existing recovery remains.** The PR's reactive handling of `0A000`,
  `22P02`, and `42883` is unchanged by this optimization. It cannot retry a
  statement inside an already-aborted transaction.

## Documentation Updates

The accompanying documentation changes describe the implemented behavior:

- [Prepared statements](../query_serving/prepared_statements_design.md):
  receipt-time named preparation, actual connection selection, exact query
  identity, rewrite refresh, DDL recovery, and SQL EXECUTE name resolution.
  Removes the obsolete `ensurePreparedWithName` description and clarifies that
  client names are still unavailable to dynamic SQL inside backend functions.
- [Plan cache](../query_serving/plan_cache_design.md): separates normalized
  gateway routing keys from exact-text backend statement keys and describes
  the extended-protocol forwarding path.
- [Transactions](../query_serving/transaction_design.md): explains Parse as a
  trigger for deferred BEGIN, prepare-time errors/locks, and signal cleanup.
- [Connection pooling](../query_serving/connection_pooling.md): corrects the
  EXECUTE preparation helper and distinguishes prepare-only ordering from
  ordinary reservation validation.
- [Testing strategy](../query_serving/testing_strategy.md): documents matrix
  allowances, single-preparation evidence, rewrite coverage, and runnable
  PostgREST I/O commands.
- [Documentation index](../README.md): links the design and this review.
