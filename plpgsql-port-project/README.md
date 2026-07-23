# PL/pgSQL Port Project

Tracking the port of PostgreSQL's PL/pgSQL parser to Go for gateway-side
session-state leak prevention. Closes the Tier 1 vector from
`docs/query_serving/unsafe_statement_rejection.md`.

## Files in this directory

- `overall-plan.md` — full scope plan, approach, critical files, verification.
- `phase-1-chunks.md` — Phase 1 broken into small, ordered chunks. This is the
  working document for execution.
- `chunk-NN-*.md` — per-chunk detail files, written just-in-time before we
  start each chunk.

The whole port ships as a **single PR**. Chunks are internal working
increments that keep the branch green and the work ordered — they are **not**
separate PRs.

Subsequent phases get their own `phase-N-chunks.md` once Phase 1 is close
to done.

## Progress

Legend: `[ ]` not started · `[~]` in progress · `[x]` done · `[!]` blocked

### Phase 1 — Parser and scanner port

See `phase-1-chunks.md` for the full chunk list. Summary:

- [x] 1.1 Grammar + build scaffolding (empty plpgsql.y compiles) — committed
      on the branch
- [x] 1.2 PL/pgSQL keyword table + lexer wrapper — committed on the branch
- [x] 1.3 Core AST nodes (Stmt iface, stmt_block, expr; function reshape) in
      the `plpgsqlast` subpackage — committed on the branch
- [x] 1.3a Clone/rewrite generator machinery for the `plpgsqlast` subpackage
      (own asthelpergen run, folded into `make parser`) — committed on the
      branch
- [x] 1.4 Minimal block parsing (`BEGIN … END;`) — committed on the branch
- [x] 1.5 DECLARE section + types **and the `read_sql_construct` boundary**
      (folded in) — committed. `PLpgSQL_expr.Parsed` left nil (text only).
- [x] 1.6 Assignment (`x := expr;`) — uses `T_WORD`/`T_CWORD`, not PG's
      `T_DATUM` (see resolution note) — committed on the branch
- [x] 1.7 Control flow: IF, LOOP, WHILE, **and EXIT/CONTINUE** (pulled in from
      1.8 — cheap, reuses the existing scanner, ships with the loops) — on the branch
- [x] 1.8 FOR family + CASE (EXIT/CONTINUE moved to 1.7) — code complete and
      green; committed. Cursor FOR loops parse as query FOR (no
      resolution); dynamic FOR (`IN EXECUTE`) and comma-target lists deferred.
- [x] 1.9 SQL-embedding: EXECSQL, PERFORM, CALL, RETURN/NEXT/QUERY — committed.
      Corrects 1.6's invented `assign_target`: assignment now dispatched from the
      word-initiated `stmt_execsql` action (PG's shape). INTO extraction / RETURN
      QUERY EXECUTE deferred
- [x] 1.10a Dynamic EXECUTE: DYNEXECUTE, DYNFORS (`FOR … IN EXECUTE`), RETURN
      QUERY EXECUTE — committed. INTO target kept as text; INTO/USING source order
      preserved
- [x] 1.10b Cursor statements: OPEN, FETCH, MOVE, CLOSE (cursor_variable as
      `T_WORD`; OPEN disambiguated syntactically; FETCH direction parsed to PG's
      enum+count and deparsed canonically) — committed
- [x] 1.10c Cursor declaration `decl_cursor` + **ALIAS** — committed. The
      `decl_statement` variable/alias/cursor split is conflict-free in goyacc (PG's
      `%expect 0` held); `readDatatype` gained `,`/`)` terminators for cursor args
- [x] 1.11 RAISE + ASSERT — committed. RAISE hand-scanned like PG's `stmt_raise`
      (optional level → message/condname/SQLSTATE/USING; `read_raise_options` +
      `check_raise_parameters` ported); condition-name recognition dropped
      (resolution), SQLSTATE 5-char/charset check kept. `ElogLevel` carries PG's exact
      elog.h values; `IsSqlState` is a deparse aid. ASSERT is the two-expression scan.
      Also folded in a lexer faithfulness fix: `scanNext` is now the single
      `plpgsql_yylex` analogue (full T_WORD/T_CWORD classification), `Lex` delegates
      to it, and hand-scan actions check `T_WORD` not bare `IDENT`
- [~] 1.12 Exception blocks — code complete and green; not yet committed. Added
  `exception_sect` to `pl_block`; defined `PLpgSQL_exception` + `PLpgSQL_condition`
  and fleshed out the `PLpgSQL_exception_block` placeholder (its `ExcList`). WHEN
  OR-lists via append helpers; `SQLSTATE 'xxxxx'` read mid-action like PG (dance
  robust to lookahead-or-default-reduction), validated like RAISE. Condition-name
  resolution + implicit sqlstate/sqlerrm namespace vars dropped
- [ ] 1.13 GET DIAGNOSTICS, COMMIT, ROLLBACK
- [!] 1.14 Compile-side parser-setup hooks (variable resolution) — DEFERRED /
  optional; not needed for Tier 1 (we use `T_WORD`/`T_CWORD`, never `T_DATUM`)
- [ ] 1.15 PG regression corpus harness (all `pl/plpgsql/src/sql/*.sql` parse)

### Phase 2 — Planner wire-in

- [ ] 2.1 `planTier1Stmt()` skeleton + statement-level blocklist
- [ ] 2.2 Embedded SQL fragment walk via `analyzeStatement` (PR #880, merged)
- [ ] 2.3 Dynamic EXECUTE policy (reject non-literal)
- [ ] 2.4 Call-site wiring in `Plan()` and `PlanPortal()`
- [ ] 2.5 `unsafe-pooler-mode` flag (default off) to bypass the checks

### Phase 3 — Tests and docs

- [ ] 3.1 E2E suite (`unsafe_plpgsql_test.go`)
- [ ] 3.2 Update `docs/query_serving/unsafe_statement_rejection.md`

### Phase 4 — Cleanup (mostly obviated; PR #880 merged)

- [ ] 4.1 Split out plpgsql-specific helpers only if real duplication appears
      (Phase 2 calls `analyzeStatement` directly, so the shared-`tier1policy/`
      extraction is largely unnecessary)

## Operating rhythm

- The whole port is **one PR**. Chunks are working increments within that
  branch, not separate PRs — each is self-contained and leaves the branch
  green so the single PR grows coherently.
- Before starting a chunk, write its `chunk-NN-*.md` detail file and get
  user sign-off.
- Mark progress in this README as chunks land on the branch.
- Keep `overall-plan.md` stable unless we learn something that changes the
  scope; small learnings are captured in individual chunk files instead.
