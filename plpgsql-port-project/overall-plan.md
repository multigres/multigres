# Overall Plan — PL/pgSQL body analysis for Tier 1 session-state leak prevention

## Context

DO blocks and `CREATE FUNCTION ... LANGUAGE plpgsql` embed procedural code as
opaque text. The SQL parser never sees inside, so
`DO $$ BEGIN SET work_mem = '10GB'; END $$` bypasses every gateway-side
session-state tracker and leaks configuration when the pool reuses the
connection. This is the Tier 1 vector in
`docs/query_serving/unsafe_statement_rejection.md`.

Two past attempts frame the problem:

- Blocking all DO / CREATE FUNCTION outright broke real workloads (migrations,
  ORMs, NOTICE/ERROR-forwarding proxy tests) without actually closing the leak,
  since `SELECT set_config(...)` achieved the same effect outside PL/pgSQL.
- PR #849 shipped Tier 2 rejection only (LOAD, ALTER SYSTEM, CREATE DATABASE,
  …) and left Tier 1 pass-through.

PR #880 has **merged**. It added the expression-level walker plus a combined
pre-dispatch entry point, `analyzeStatement(stmt)`
(`go/services/multigateway/planner/unsafe_funccall.go`), which every planning
path runs and which chains three guards:

- `rejectUnsupportedStatement` — Tier 2 statement rejection.
- `checkRestrictedGUCChange` (`restricted_guc.go`) — rejects value assignments
  to cluster-managed GUCs (`restrictedGUCs`, currently `synchronous_commit`)
  across SET / SET LOCAL / ALTER ROLE … SET / ALTER DATABASE … SET. Reverts
  (RESET / SET … TO DEFAULT) stay allowed. The reusable `restrictedGUCError(name)`
  helper is also consulted from the set_config path.
- `analyzeFunctionCalls` — walks `FuncCall` nodes, rejects dangerous
  built-ins (`dblink*`, `pg_read_file*`, `lo_import/export`,
  `pg_execute_server_program`, `query_to_xml*`, …), and tracks top-level
  `SELECT set_config(...)` so the pooler's `SessionSettings` stays in sync.

Together these close the `SELECT set_config()` and direct-SET vectors, but they
all explicitly punt on constructs buried inside PL/pgSQL bodies.

This project closes the remaining Tier 1 hole by porting PostgreSQL's PL/pgSQL
parser and handing every embedded SQL fragment back to the _same_
`analyzeStatement` policy — so the blocklist, the restricted-GUC guard
(`synchronous_commit`, etc.), and the set_config rules apply identically inside
and outside PL/pgSQL, with no re-implementation.

## Approach

Port `pl_gram.y` and `pl_scanner.c` to Go as a **separate grammar** alongside
the existing `postgres.y`, reusing the SQL lexer, SQL parser, and AST packages.
At plan time, the gateway detects `DoStmt` and `CreateFunctionStmt` with
`LANGUAGE plpgsql`/`sql`, parses the body, walks the resulting PL/pgSQL AST,
and applies the Tier 1 blocklist to every statement and embedded SQL fragment.
Rejections use the same `*mterrors.PgDiagnostic` shape as Tier 2 and are
**enforced by default**; the only way to disable them is the explicit
`unsafe-pooler-mode` override (below) — there is no log-only middle ground.

The dynamic `EXECUTE` case is rejected outright when the argument is not a
string literal. Non-plpgsql trusted languages (`plperl`, `plpython3u`) remain
out of scope — they require superuser in PostgreSQL and inherit that
protection.

### Override flag — `unsafe-pooler-mode`

The gateway ships a config flag (working name `unsafe-pooler-mode`, **off by
default**) that lets an operator turn these gateway-side safety rejections off
wholesale. When enabled, `planTier1Stmt` becomes a no-op and DO blocks,
CREATE FUNCTION bodies, and the other flagged constructs pass straight through
to the backend unmodified.

This is a deliberate escape hatch — a footgun — for deployments that knowingly
accept the risk: single-tenant or non-pooled setups where session-state leakage
across connections is irrelevant, trusted internal tooling, or a migration
window where the checks are temporarily in the way. It is a full bypass, not a
log-only mode: the checks either run and reject, or are disabled entirely.
Default-off preserves the safe behavior for everyone who does not opt in. When
enabled, the gateway should surface that it is running in an unsafe mode (a
startup log line at minimum) so the relaxed posture is never silent.

**Open question (settle in Phase 2): scope.** The flag clearly covers the
Tier 1 PL/pgSQL rejections this project adds. Whether it should _also_ relax
the pre-existing Tier 2 statement blocklist and the restricted-GUC guard
(`analyzeStatement` / `checkRestrictedGUCChange`) — or leave those always-on,
since several of them are gated by PG superuser anyway — is a design decision
to make when the planner wiring lands. The recommendation: scope it to the
gateway-imposed pooling-safety checks (Tier 1 + restricted-GUC + the
literal-`SET`-in-PL/pgSQL rule) and keep genuinely-unsupported Tier 2
statements rejected regardless.

## Reference code to reuse

- **Grammar pipeline** — `go/common/parser/postgres.y` (15.6k lines, the
  porting precedent), driven by `go/common/parser/generate.go` via
  `go/common/parser/goyacc/`. The same toolchain works for a second grammar
  file.
- **SQL lexer** — `go/common/parser/lexer.go` (1.8k lines). The PL/pgSQL
  scanner wraps this, same pattern as PG's `pl_scanner.c` wrapping
  `parser/scanner.h`. `go/common/parser/keywords.go` for keyword tables.
- **SQL parser entry** — `ParseSQL(input string) ([]ast.Stmt, error)` in
  `go/common/parser/postgres.go:1182`. PL/pgSQL's embedded-SQL fragments
  (`PLpgSQL_expr`) hand text back to this entry point, matching PG's
  `raw_parser()` boundary.
- **AST infrastructure** — the SQL AST is `go/common/parser/ast/`
  (hand-written nodes, `go/tools/asthelpergen` generating clone/rewrite). The
  PL/pgSQL nodes are a **separate hierarchy** in their own subpackage
  `go/common/parser/ast/plpgsqlast/` (package `plpgsqlast`): own `Node`
  interface, own `NodeTag`, own `BaseNode`. They are intentionally NOT
  `ast.Node`, which keeps the ~49-type PL/pgSQL vocabulary and its generated
  helpers out of the 17k-line central `ast` clone/rewrite files and the
  842-entry `NodeTag` enum. The two hierarchies meet only at the embedded-SQL
  boundary: a PL/pgSQL expression node holds an `ast.Stmt` for its parsed SQL
  fragment, handed to the SQL-side `analyzeStatement`. Dependencies flow one
  way (`plpgsqlast` → `ast`), so there is no import cycle. `plpgsqlast` gets
  its own `asthelpergen` run for clone/rewrite (see Phase 1).
- **Existing planner boundary** — `analyzeStatement()` in
  `go/services/multigateway/planner/unsafe_funccall.go`, called from `Plan()`
  (planner.go:104) and `PlanPortal()` (planner.go:370). New `planTier1Stmt()`
  sits alongside and is called from the same two sites.
- **Expression walker + combined guard (PR #880, merged)** —
  `go/services/multigateway/planner/unsafe_funccall.go`
  (`analyzeStatement`, `analyzeFunctionCalls`, `funcBlocklist`)
  and `restricted_guc.go` (`checkRestrictedGUCChange`, `restrictedGUCs`,
  `restrictedGUCError`). The PL/pgSQL walker runs `analyzeStatement` on
  every `PLpgSQL_expr.Parsed` (the embedded SQL AST) rather than
  re-implementing FuncCall rejection, the restricted-GUC guard, or Tier 2
  checks. To restrict a new GUC (e.g. another durability/replication knob),
  add it to `restrictedGUCs` and both surfaces pick it up.
- **Test pattern** — `go/services/multigateway/planner/unsafe_stmt_test.go`
  and `unsafe_funccall_test.go` (table-driven, `mterrors.PgDiagnostic`
  assertions, SQLSTATE `0A000`).
- **Upstream source** — `~/postgres/src/pl/plpgsql/src/pl_gram.y` (4,172 lines,
  ~29 statement productions), `pl_scanner.c` (730 lines), parsing-side hooks
  in `pl_comp.c` (`plpgsql_parser_setup`, `plpgsql_pre/post_column_ref`,
  `plpgsql_param_ref`), AST in `plpgsql.h` (49 typedef structs).
- **Regression corpus** — `~/postgres/src/pl/plpgsql/src/sql/` (13 files,
  3,438 lines) with expected outputs in `expected/` (4,530 lines).

## Phases

### Phase 1 — Parser and scanner port

Port `pl_gram.y` and `pl_scanner.c` behind a new public entry
`ParsePLpgSQL(body string) (*plpgsqlast.PLpgSQL_function, error)`. Build
pipeline, keyword table, scanner wrapper, grammar, the `plpgsqlast` node
hierarchy and its clone/rewrite generator wiring, compile-side hooks for
variable resolution, PG regression corpus harness.

Chunked execution in `phase-1-chunks.md`.

### Phase 2 — Planner wire-in

- **New file:** `go/services/multigateway/planner/unsafe_plpgsql.go`
  - `planTier1Stmt(stmt ast.Stmt) error` — switches on `NodeTag()`:
    - `T_DoStmt` — extract body + language from `Args` (DefElem list). If
      language is `plpgsql` (or omitted, which defaults to plpgsql) or `sql`,
      parse and walk. Anything else passes through.
    - `T_CreateFunctionStmt` — read `Options` for `language`. If
      `plpgsql`/`sql`, parse body and walk. `plperl`, `plpython3u`, and other
      untrusted languages pass through (superuser gate upstream).
  - Walker applies the Tier 1 policy to each PL/pgSQL statement:
    - **Embedded SQL fragments (reuse, do not reimplement):** every
      `PLpgSQL_expr.Parsed` and every `stmt_execsql`'s parsed SQL is handed to
      `analyzeStatement` from PR #880. That single call already
      enforces the funccall blocklist (`dblink*`, `pg_read_file*`, …), the
      restricted-GUC guard (`synchronous_commit` and anything else in
      `restrictedGUCs`), the set_config policy, and Tier 2 statement rejection —
      so a `SET synchronous_commit = off` or a `dblink(...)` inside a DO block
      is rejected by exactly the same code path as at top level.
    - **PL/pgSQL-specific statement blocklist (the gap reuse can't cover):** a
      bare `SET`/`stmt_execsql` → `VariableSetStmt` that pins _any_ value is
      rejected in PL/pgSQL context, because the pooler can't observe it to keep
      `SessionSettings` in sync — even GUCs that are perfectly trackable at top
      level (e.g. `SET work_mem = '10GB'`). Same for `DISCARD`, `LISTEN`,
      `UNLISTEN`, `PREPARE TRANSACTION`, `RELEASE SAVEPOINT`. Reuse
      `restrictedGUCError` for the managed-GUC message where it applies.
    - **Dynamic EXECUTE:** `stmt_dynexecute` is rejected unless its expression
      is a bare string literal `A_Const` (which is then re-parsed and run back
      through `analyzeStatement`). Migration tooling that concatenates
      runs via the admin path.
  - Returns `*mterrors.PgDiagnostic` with SQLSTATE `0A000`
    (`feature_not_supported`); `nil` when safe.
- **Call sites:** add `planTier1Stmt(stmt)` immediately after
  `analyzeStatement()` in `Plan()` (planner.go:104) and `PlanPortal()`
  (planner.go:370).
- **`unsafe-pooler-mode` gate:** when the flag is set, `planTier1Stmt` returns
  `nil` immediately (no parse, no walk) so flagged bodies pass through. Plumb
  the flag from `RegisterFlags` in `go/services/multigateway/init.go` (a
  `fs.Bool`, bound via `viperutil.BindFlags`) into the `Planner` so the call
  sites can consult it. Per the Approach open question, the recommended scope
  also short-circuits the pre-existing `analyzeStatement` pooling-safety
  rejections — decide the exact boundary here.
- **Parse-error policy:** if `ParsePLpgSQL` fails on a body, reject with a
  distinct SQLSTATE-tagged diagnostic ("body failed to parse"). Pass-through-
  on-parse-error would re-open the hole this ticket closes.

### Phase 3 — Tests and docs

- **Walker unit tests** —
  `go/services/multigateway/planner/unsafe_plpgsql_test.go`, table-driven,
  covering each blocklist entry, literal vs. dynamic EXECUTE, embedded
  expression-level rejections (delegating to walker from #880), and negative
  cases (safe bodies pass).
- **E2E** — new `go/test/endtoend/queryserving/unsafe_plpgsql_test.go`
  following `unsafe_funccall_test.go`. Verify:
  - `DO $$ BEGIN SET work_mem = '10GB'; END $$` rejected, connection healthy
    after.
  - `CREATE FUNCTION ... LANGUAGE plpgsql` with literal SET inside rejected.
  - Safe DO block (no Tier 1 constructs) allowed end-to-end.
  - `EXECUTE 'SET ' || var` rejected.
  - Pool rotation preserves session state after safe DO bodies run.
  - With `unsafe-pooler-mode` enabled, a body that is rejected by default (e.g.
    `DO $$ BEGIN SET work_mem = '10GB'; END $$`) passes through to the backend.
- **Docs** — update
  `docs/query_serving/unsafe_statement_rejection.md`: move PL/pgSQL body
  analysis from Future Work into the Tier 1 section, document the
  dynamic-EXECUTE policy, document the "parse failure = reject" choice,
  document the `unsafe-pooler-mode` override (what it disables, its risk, and
  the default-off posture), and list trusted-language (`plperl`/`plpython3u`)
  as an explicit known gap mitigated by PG's superuser requirement.

### Phase 4 — Cleanup (mostly obviated)

PR #880 has merged, and the Phase 2 design now _calls_ its
`analyzeStatement` directly rather than copying the blocklist /
restricted-GUC / set_config policy — so the original "extract into a shared
`tier1policy/` package and re-export from both files" dedup is largely
unnecessary. What may remain is small: if `unsafe_plpgsql.go`'s
language/body-extraction helpers or the PL/pgSQL-specific statement blocklist
grow enough to warrant their own file or a shared helper, split them out in a
scoped follow-up. Decide once Phase 2 lands and the real duplication (if any)
is visible.

## Critical files

### Modified

- `Makefile`, `go/common/parser/generate.go` — add plpgsql grammar target.
- `go/services/multigateway/planner/planner.go` — call `planTier1Stmt` in
  `Plan()` and `PlanPortal()`, gated by `unsafe-pooler-mode`.
- `go/services/multigateway/init.go` — register the `unsafe-pooler-mode`
  `fs.Bool` flag (and bind it via `viperutil.BindFlags`), threaded into the
  `Planner`.
- `docs/query_serving/unsafe_statement_rejection.md` — promote body analysis
  out of Future Work; document `unsafe-pooler-mode`.

### Added

- `go/common/parser/plpgsql/` — parser package: `plpgsql.y`, generated
  `plpgsql.go`, `generate.go`, `lexer.go`, `keywords.go`, `api.go`.
- `go/common/parser/ast/plpgsqlast/` — PL/pgSQL AST subpackage: `nodes.go`
  (own `Node`/`NodeTag`/`BaseNode`), per-family node files, `generate.go`, and
  the generated `ast_clone.go`/`ast_rewrite.go` from its own asthelpergen run.
- `go/common/parser/testdata/plpgsql_cases.json`
- `go/common/parser/plpgsql_parse_test.go`
- `go/common/parser/plpgsql_regression_test.go` (PG corpus harness)
- `go/services/multigateway/planner/unsafe_plpgsql.go`
- `go/services/multigateway/planner/unsafe_plpgsql_test.go`
- `go/test/endtoend/queryserving/unsafe_plpgsql_test.go`

### Not modified (explicitly)

- `go/common/parser/postgres.y` — the SQL grammar stays as-is.
- `go/services/multigateway/planner/unsafe_stmt.go` — Tier 2 logic stays
  as-is; Tier 1 is an independent call path.

## Verification

Parser level:

- `/mt-dev unit ./go/common/parser/...` — plpgsql unit tests and PG regression
  corpus pass.
- `make parser` regenerates `plpgsql.go` cleanly on a fresh checkout.

Planner level:

- `/mt-dev unit ./go/services/multigateway/planner/...` — `planTier1Stmt`
  tests pass; pre-existing `rejectUnsupportedStatement` tests unaffected.

End-to-end:

- `/mt-dev integration multigateway` — full integration suite including the
  new `unsafe_plpgsql_test.go` E2E.
- Manual smoke test via `/mt-local-cluster`:
  - `psql` → `DO $$ BEGIN SET work_mem = '10GB'; END $$` → expect SQLSTATE
    `0A000`, session unchanged, subsequent queries succeed.
  - `CREATE FUNCTION foo() RETURNS void LANGUAGE plpgsql AS $$ BEGIN SET
work_mem = '10GB'; END $$` → rejected.
  - Safe DO block (pure RAISE NOTICE / assignment) → executes, no
    session-state drift after connection returns to pool.

## Out of scope

- Trusted-language ports (`plperl`, `plpython3u`): protected by PG superuser
  requirement.
- Runtime body analysis for dynamic `EXECUTE` with non-literal args: not
  solved here. Rejection is the stopgap; a connection-reset backstop
  (DISCARD ALL on pool return) is the long-term cover and belongs in a
  separate ticket.

## Effort signal (reference, not commitment)

Parser + scanner + AST ~3–6 weeks · PG regression corpus hardening ~+2–4
weeks · planner integration + tests ~+1–2 weeks. The `postgres.y` precedent
(15.6k lines, 10× larger, already working) is the main de-risker.
