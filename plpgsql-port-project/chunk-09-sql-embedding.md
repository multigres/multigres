# Chunk 1.9 — SQL-embedding: EXECSQL, PERFORM, CALL, RETURN

Adds the statements that embed SQL: the `stmt_execsql` catch-all (any SQL
statement not handled by a PL/pgSQL production), plus `PERFORM`, `CALL`/`DO`, and
the `RETURN` family. This is where the no-`T_DATUM` decision bites hardest —
`stmt_execsql` and assignment both begin with a bare word, which PG only tells
apart by variable resolution.

Ported from `pl_gram.y` (`stmt_execsql`, `make_execsql_stmt`, `stmt_perform`,
`stmt_call`, `stmt_return` + `make_return_stmt`/`_next`/`_query`) and `plpgsql.h`.

## The crux — assignment vs. execsql (the one hard problem)

**PG's real grammar:** `stmt_assign: T_DATUM` (target only — the `:=` and RHS are
read inside the action via `read_sql_construct` in `RAW_PARSE_PLPGSQL_ASSIGN{1,2,3}`
mode), and `stmt_execsql: K_IMPORT | K_INSERT | K_MERGE | T_WORD | T_CWORD`.
Assign-vs-execsql is decided **entirely by the lexer**: a resolved variable is
`T_DATUM` → assign; an unresolved word is `T_WORD` → execsql. PG's `T_WORD`
execsql action even peeks the next token and, if it is `=`/`:=`/`[`/`.`, errors
`word_is_not_variable` — because a real variable would have come through as
`T_DATUM`, so assigning to a non-variable word is a mistake.

**Our problem:** we never emit `T_DATUM` (no resolution), so every target is a
word. Chunk 1.6 invented `assign_target: T_WORD | T_CWORD` + an explicit
`stmt_assign: assign_target assign_operator …`. That deviates from PG and sets up
a collision with `stmt_execsql: T_WORD`.

**The fix — adopt PG's shape and decide in the word action.** Drop the invented
`assign_target` and the `stmt_assign` production entirely; use PG's `stmt_execsql`
alternatives, and dispatch inside the `T_WORD`/`T_CWORD` action — the exact spot
where PG peeks:

```text
stmt_execsql: K_IMPORT | K_INSERT | K_MERGE
           |  T_WORD   { $$ = makeWordStmt($1) }
           |  T_CWORD  { $$ = makeWordStmt($1) }
```

`makeWordStmt` (beginScan dance, then peek the next token):

- next is `:=` or `=` → **assignment**: consume the operator, read the RHS to `;`,
  return `PLpgSQL_stmt_assign{Target: $1, Expr: rhs}` (same node/behavior as 1.6).
  This is PG's peek, building an assign instead of erroring — the one honest
  consequence of no resolution.
- otherwise → **execsql**: capture the whole statement from the word's byte
  offset to `;`, return `PLpgSQL_stmt_execsql`.

No separate assignment production, no invented tokens, and **no grammar conflict**
(the decision is Go code in the action, not the LALR tables). `assign_operator`
stays — it is still used by `decl_defkey` in DECLARE, so it isn't removed.

Consequence: a **subscripted target** (`x[i] := …`) needs multi-token lookahead
to find the `:=` past `[...]`; it is deferred (1.6 didn't handle it either — no
regression). Simple (`x :=`) and compound (`a.b :=`, already one `T_CWORD`)
targets work via the single-token peek.

## execsql — whole-statement capture

`make_execsql_stmt` scans from the first word to the terminating `;` and stores
the verbatim text. Our version:

- Capture from the first token's byte offset (the symbol's `location`) up to `;`
  at paren depth 0, via the existing scanner. The first word is already consumed
  by the grammar, so the action captures from its recorded location.
- **`INTO`-clause extraction is skipped.** PG splits a `SELECT … INTO target …`
  into a query plus resolved target variables; that is resolution, which we don't
  do. We keep the whole statement (including any `INTO`) as raw text. The Tier-1
  walker gets the full statement; splitting is a later/optional refinement.
- **`;` detection uses paren depth only.** PG additionally tracks `BEGIN/END` and
  `CASE/END` nesting so a semicolon inside an embedded `CREATE FUNCTION … BEGIN …
END` body doesn't terminate early. That is a rare edge case (nested routine
  definitions); deferred, noted here. Dollar-quoted bodies are already handled by
  the lexer.
- Drop PG's `mod_stmt` flag (derivable; not needed for parse).

Node `PLpgSQL_stmt_execsql` — `Sqlstmt *PLpgSQL_expr` (the statement text,
`RAW_PARSE_DEFAULT`).

## PERFORM, CALL/DO

- **PERFORM** — scan the expression after `K_PERFORM` up to `;`. PG rewrites the
  leading `PERFORM` to `SELECT` in the stored text (PERFORM isn't valid SQL); we
  keep the expression text and re-emit the `PERFORM` prefix on deparse, so it
  round-trips. The SELECT-substitution is an execution concern (Phase 2). Node
  `PLpgSQL_stmt_perform` — `Expr *PLpgSQL_expr`.
- **CALL / DO** — capture the whole statement (push the `K_CALL`/`K_DO` back,
  scan to `;`) so the query text includes the `CALL`/`DO` keyword. Node
  `PLpgSQL_stmt_call` — `Expr *PLpgSQL_expr`, `IsCall bool` (true for CALL, false
  for DO), matching PG's shared struct.

## RETURN family

`stmt_return` peeks the token after `K_RETURN` (mirroring PG's action):

- `NEXT` → `RETURN NEXT expr` → `PLpgSQL_stmt_return_next` — `Expr`.
- `QUERY` → `RETURN QUERY <query>` → `PLpgSQL_stmt_return_query` — `Query
*PLpgSQL_expr`. **`RETURN QUERY EXECUTE <expr>` is deferred to 1.10** (dynamic
  EXECUTE); the `DynQuery`/`Params` fields are left off until then.
- otherwise → `RETURN [expr]` → `PLpgSQL_stmt_return` — `Expr *PLpgSQL_expr` (nil
  for a bare `RETURN`).

Drops PG's `retvarno` (resolution).

## AST nodes (all `plpgsqlast`, all `Stmt`)

`PLpgSQL_stmt_execsql`, `PLpgSQL_stmt_perform`, `PLpgSQL_stmt_call`,
`PLpgSQL_stmt_return`, `PLpgSQL_stmt_return_next`, `PLpgSQL_stmt_return_query` —
new NodeTags + `String()`/`SqlString()`/constructors; `make parser` regenerates
clone/rewrite.

Deparse (no trailing `;` — the block owns it):

```text
<sqlstmt text>                    -- execsql: verbatim
PERFORM <expr>
CALL <...>  /  DO <...>           -- verbatim (keyword already in text)
RETURN / RETURN <expr>
RETURN NEXT <expr>
RETURN QUERY <query>
```

## Scanner additions

- `readExecSqlStmt(startPos int)` — scan to `;`, capture `input[startPos:;]` as a
  `RAW_PARSE_DEFAULT` expr (the whole-statement variant of `readSQLExpr`).
- A `pushBackKeyword`-style helper to re-inject `K_CALL`/`K_DO`/`K_PERFORM` for
  the whole-statement scans, or capture-from-location as with execsql — whichever
  is cleaner in implementation.

## Scope — out (deferred)

- `RETURN QUERY EXECUTE` and all dynamic `EXECUTE` — 1.10.
- `INTO [STRICT] target` extraction from execsql — needs resolution; kept as raw
  text.
- `BEGIN/END` / `CASE/END` nesting in execsql `;` detection — rare edge case.
- Subscripted assignment targets (`x[i] := …`) — need multi-token lookahead.
- `PLpgSQL_expr.Parsed` still nil.

## Conflict / risk

- The assign-vs-execsql dispatch is now Go code in the `makeWordStmt` action, so
  it can't produce a grammar conflict — but the **`y.output` conflict count must
  still be 0** after dropping `stmt_assign`/`assign_target` and adding the execsql
  alternatives. Test `x := 1;` / `a.b := 2;` (assign) vs `SELECT x;` /
  `UPDATE t SET c = 1;` (execsql) all parse correctly, and the existing
  `assign_cases.json` still passes unchanged.
- execsql text capture from the first word's location — off-by-one gives a
  truncated statement; test exact captured text.
- The `makeWordStmt` peek reuses the `beginScan` lookahead dance — test both the
  lookahead-present and default-reduction paths.
- RETURN peek: both lookahead-present and default-reduction paths.

## Acceptance

New `testdata/execsql_cases.json` and `testdata/return_cases.json` (added to
`caseFiles`), golden deparse via `PLPGSQL_REWRITE=1`:

- execsql: `SELECT 1`, `INSERT INTO t VALUES (1)`, `UPDATE t SET c = 1 WHERE id =
2`, `DELETE FROM t`, a `SELECT` with a parenthesised subquery containing `;`-free
  content, a compound-name call `schema.func()`.
- assign still works and is not mis-parsed as execsql: `x := 1`, `a.b := 2`.
- `PERFORM f(1)`; `CALL p(1, 2)`; `DO $$ BEGIN END $$`.
- `RETURN`; `RETURN x + 1`; `RETURN NEXT r`; `RETURN QUERY SELECT * FROM t`.
- Nesting: these inside IF / loops / CASE.
- Round-trip stable; `make parser` no new conflicts; `go build` +
  `golangci-lint` clean; package `ast` byte-identical to upstream.
