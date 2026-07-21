# Chunk 1.10b — Cursor statements: OPEN, FETCH, MOVE, CLOSE

The executable cursor statements. Cohesive (all key on a cursor name), and
testable standalone — a cursor is just a `T_WORD` name to us, so `OPEN c; FETCH c
INTO x; CLOSE c;` parse without any declaration.

Ported from `pl_gram.y` (`stmt_open`, `stmt_fetch`, `stmt_move`, `stmt_close`,
`cursor_variable`, `opt_fetch_direction`, `read_fetch_direction`) and `plpgsql.h`.

## Proposed split of 1.10b

Cursor work splits cleanly into **statements** (this doc) and the **declaration**
(`decl_cursor`, next). The declaration is a separate concern — it changes the
DECLARE-section grammar and carries a real variable-vs-cursor disambiguation risk
(`decl_statement` has three forms sharing the `decl_varname` prefix). Keeping it
apart keeps this chunk to statement productions only.

- **1.10b (this doc)** — OPEN / FETCH / MOVE / CLOSE.
- **1.10c (next)** — `decl_cursor` (`name [[NO] SCROLL] CURSOR [(args)] FOR query`).

## Divergences forced by no-`T_DATUM`

1. **`cursor_variable` is a `T_WORD`** (the name as text). PG requires a resolved
   `refcursor` `T_DATUM`; we capture the name. Node fields hold `Curvar string`,
   not a datum number.
2. **OPEN is disambiguated syntactically, not by resolution.** PG branches on the
   cursor's `cursor_explicit_expr` (was it declared bound?) — which needs
   resolution. We peek the token after the cursor name instead:
   - `(` → bound-cursor args (`argquery`);
   - `SCROLL` / `NO SCROLL` / `FOR` → the `[scroll] FOR query | FOR EXECUTE expr
[USING]` form;
   - `;` → bare `OPEN c` (bound cursor, no args).
     This covers the same surface syntax without knowing whether the cursor is bound.
3. **FETCH/MOVE `INTO` target and the count keep as text** (no resolution).

## Statements

### CLOSE — `CLOSE cursor`

`stmt_close: K_CLOSE cursor_variable ';'`. Node `PLpgSQL_stmt_close{Curvar}`.

### OPEN — `OPEN cursor [ [NO] SCROLL FOR query | FOR EXECUTE expr [USING …] | (args) ]`

`stmt_open: K_OPEN cursor_variable { makeOpen($2) }`. `makeOpen` does the
syntactic peek above and fills one of: `Argquery` (bound args, scanned as a whole
statement / arg list), `Query` (static `FOR` query), or `DynQuery` + `Params`
(`FOR EXECUTE`). Node `PLpgSQL_stmt_open{Curvar, CursorOptions, Argquery, Query,
DynQuery, Params}`; `CursorOptions` carries the SCROLL flag.

### FETCH / MOVE — `FETCH|MOVE [direction] [FROM|IN] cursor [INTO target]`

`stmt_fetch: K_FETCH opt_fetch_direction cursor_variable K_INTO { readIntoTarget }`
and `stmt_move: K_MOVE opt_fetch_direction cursor_variable ';'`, sharing one node
via `IsMove`. `opt_fetch_direction` runs `readFetchDirection` — a faithful port of
PG's `read_fetch_direction`, which builds a partially-filled fetch struct from the
direction clause:

- `NEXT` (default) / `PRIOR` / `FIRST` / `LAST` / `ALL` / `FORWARD` / `BACKWARD`;
- `ABSOLUTE <expr>` / `RELATIVE <expr>` (count expression, scanned to `FROM`/`IN`);
- a bare count expression; empty (cursor name follows directly).

Node `PLpgSQL_stmt_fetch{Curvar, Direction, HowMany, Expr, IsMove, Target,
ReturnsMultipleRows}`. New `FetchDirection` enum in `plpgsqlast` mirroring PG's
(`FETCH_FORWARD/BACKWARD/ABSOLUTE/RELATIVE`).

**Deparse of the direction canonicalizes.** Several surface forms map to the same
`(direction, how_many)` — e.g. `NEXT` = default `FORWARD 1`. The deparse emits one
canonical keyword per parsed state, so equivalent inputs round-trip stably (same
pattern as IF's empty-ELSE collapse). Round-trip stability, not byte-fidelity of
the direction word, is the invariant.

## AST nodes (all `plpgsqlast`, all `Stmt`)

`PLpgSQL_stmt_open`, `PLpgSQL_stmt_fetch`, `PLpgSQL_stmt_close` + the
`FetchDirection` enum + SCROLL option constants. New NodeTags,
`String()`/`SqlString()`/constructors; `make parser` regenerates clone/rewrite.

## Scanner additions

- `readFetchDirection() *PLpgSQL_stmt_fetch` — the direction dispatch (reuses
  `readSQLConstruct` for `ABSOLUTE`/`RELATIVE`/count expressions).
- `makeOpen(curvar string) *PLpgSQL_stmt_open` — the OPEN syntactic dispatch.
- `cursor_variable: T_WORD` in the grammar.

## Conflict / risk

- `cursor_variable` after `K_FETCH opt_fetch_direction` — `opt_fetch_direction` is
  an empty production whose action scans the direction and leaves the cursor name;
  verify the `beginScan`/lookahead handling and that `y.output` stays at 0
  conflicts.
- FETCH's `K_INTO` is a grammar token (`… cursor_variable K_INTO { readIntoTarget }`),
  MOVE ends at `';'` — mirror PG exactly.
- The OPEN peek must push back correctly for each branch.

## Scope — out (deferred)

- `decl_cursor` (cursor declaration) — 1.10c.
- All resolution (curvar/target/argrow as text).
- `PLpgSQL_expr.Parsed` still nil.

## Acceptance

New `testdata/cursor_cases.json`, golden deparse via `PLPGSQL_REWRITE=1`:

- `OPEN c;`, `OPEN c(1, 2);`, `OPEN c FOR SELECT * FROM t;`, `OPEN c SCROLL FOR
SELECT 1;`, `OPEN c FOR EXECUTE q USING x;`.
- `CLOSE c;`.
- `FETCH c INTO x;`, `FETCH NEXT FROM c INTO x;`, `FETCH PRIOR c INTO x;`,
  `FETCH ABSOLUTE 3 FROM c INTO x;`, `FETCH FORWARD 2 FROM c INTO x;`.
- `MOVE c;`, `MOVE FORWARD 2 IN c;`, `MOVE LAST c;`.
- Nesting inside loops/blocks.
- Round-trip stable; `make parser` 0 new conflicts; `go build` + `golangci-lint`
  clean; package `ast` byte-identical to upstream.
