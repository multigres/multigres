# Chunk 1.8 — FOR family + CASE

The most complex grammar chunk. FOR is highly ambiguous in PG and its
disambiguation leans on `T_DATUM` resolution we deliberately don't have, so this
chunk is where the no-resolution stance forces the clearest divergences. CASE is
self-contained and simpler.

Ported from `postgres/src/pl/plpgsql/src/pl_gram.y` (`stmt_for`, `for_control`,
`for_variable`, `stmt_foreach_a`, `foreach_slice`, `stmt_case`,
`opt_expr_until_when`, `case_when_list`, `case_when`, `opt_case_else`) and
`plpgsql.h` (`PLpgSQL_stmt_fori`, `_fors`, `_foreach_a`, `_stmt_case`,
`_case_when`).

## Structure

Landed as a single commit (CASE + FOR family together). Within it, CASE comes
first (no new scanner), then the FOR family (two-terminator scan + REVERSE peek).

## Part A — CASE

Grammar (mirrors PG):

```text
stmt_case: K_CASE opt_expr_until_when case_when_list opt_case_else K_END K_CASE ';'
opt_expr_until_when: /*empty, action*/   // peek K_WHEN; if absent scan expr until K_WHEN, push K_WHEN back
case_when_list: case_when_list case_when | case_when
case_when: K_WHEN expr_until_then proc_sect
opt_case_else: /*empty*/ | K_ELSE proc_sect
```

- **Simple vs searched CASE** is disambiguated exactly as PG: `opt_expr_until_when`
  peeks one token; if it is `K_WHEN` there is no test expression (searched CASE),
  otherwise scan a test expression up to `K_WHEN` (simple CASE) and push `K_WHEN`
  back. This is the same lexer-peek + pushback the other empty scanners use.
- `case_when_list` appends via a helper (`appendCaseWhen`) — list-append in an
  action must not be the bare `$$ = append($1,$2)` idiom (goyacc fast-append
  hazard; see [[goyacc-append-inplace-pitfall]]).
- **Empty `ELSE` collapses to no-ELSE**, same simplification as IF's `ElseBody`
  in 1.7: `opt_case_else` yields nil for both absent and empty, and the deparse
  emits `ELSE` only when non-empty. Round-trip is stable; PG's execution-time
  distinction (empty ELSE = no-op vs no ELSE = CASE_NOT_FOUND error) doesn't
  matter for a parse-only / Tier-1 port. (So we drop PG's `have_else` flag and
  the `list_make1(NULL)` trick.)

AST:

- `PLpgSQL_stmt_case` — `TestExpr *PLpgSQL_expr` (nil for searched CASE),
  `WhenList []*PLpgSQL_case_when`, `ElseStmts []Stmt`. Drops PG's `t_varno`
  (resolution).
- `PLpgSQL_case_when` — `Expr *PLpgSQL_expr`, `Stmts []Stmt`. Helper node (Node,
  not Stmt), like `PLpgSQL_if_elsif`.

Deparse: `CASE [test]\nWHEN e THEN\n<stmts>\n…[ELSE\n<stmts>]\nEND CASE` — bodies
via the shared `deparseBody`; no trailing `;` (the block owns it).

## Part B — FOR family

### The scanner addition

`read_sql_expression2` in PG scans to the first of TWO terminators and reports
which it hit. `scanFragment` already returns the matched terminator, so add a thin
wrapper:

```go
func (l *lexer) readSQLConstructUntil(terminators ...int) (*plpgsqlast.PLpgSQL_expr, int)
// returns the expr plus the terminator token that ended it (consumed)
```

### Grammar

```text
stmt_for:   opt_loop_label K_FOR for_control loop_body   // graft label+body onto the for_control node
for_control: for_variable K_IN { …manual scan, builds fori or fors… }
for_variable: T_WORD | T_CWORD                            // single target name (see divergence)
stmt_foreach_a: opt_loop_label K_FOREACH for_variable foreach_slice K_IN K_ARRAY expr_until_loop loop_body
foreach_slice: /*empty*/ { 0 } | K_SLICE ICONST
```

`for_control` mirrors PG's one-big-action structure. After `for_variable K_IN`
(beginScan dance), it:

1. peeks for `K_REVERSE` (consume + set reverse, else push back);
2. scans the first construct up to **`DOT_DOT` or `K_LOOP`** via
   `readSQLConstructUntil(DOT_DOT, K_LOOP)`;
3. if it stopped on `DOT_DOT` → **integer FOR** (`PLpgSQL_stmt_fori`): read the
   upper bound up to `K_LOOP`/`K_BY`, then an optional `BY` step up to `K_LOOP`;
4. else (stopped on `K_LOOP`) → **query FOR** (`PLpgSQL_stmt_fors`) capturing the
   construct as the query.

`stmt_for` then type-switches on the returned node to set `Label` and `Body`
(PG does the same `cmd_type` check) and validates the end label via `checkLabels`.

### Divergences forced by no-`T_DATUM` (the important part)

1. **Cursor FOR loops are not distinguishable from query FOR.** PG tells
   `FOR r IN curvar LOOP` from `FOR r IN <query> LOOP` by checking whether the
   token after `IN` resolves to a `refcursor` `T_DATUM`. We have no namespace, so
   a bound-cursor FOR loop parses as a **query FOR** whose "query" text is the
   cursor name. Acceptable for a parse-only / Tier-1 port (we don't execute, and
   `Parsed` is nil); proper cursor handling is a 1.10/1.14 concern. No
   `PLpgSQL_stmt_forc` node in this chunk.
2. **Dynamic FOR (`FOR … IN EXECUTE …`) deferred to 1.10** (the phase plan puts
   DYNFORS there). Interim: such a loop parses as a query FOR with query text
   `EXECUTE …`. When 1.10 adds the `K_EXECUTE` peek to `for_control`, it becomes a
   proper `PLpgSQL_stmt_dynfors`. Noted as a known interim inaccuracy.
3. **`for_variable` is a single `T_WORD`/`T_CWORD` name.** PG's comma-separated
   scalar target list (`FOR a, b IN query`) is built by `read_into_scalar_list`,
   which is resolution. Multi-target query FOR is **deferred** (note it; it
   resurfaces for the 1.15 corpus). Integer FOR / single-target query FOR /
   FOREACH all need only one name.

### AST

- `PLpgSQL_stmt_fori` — `Label string`, `Var string`, `Lower, Upper, Step
*PLpgSQL_expr` (Step nil = default BY 1), `Reverse bool`, `Body []Stmt`.
- `PLpgSQL_stmt_fors` — `Label string`, `Var string`, `Query *PLpgSQL_expr`,
  `Body []Stmt`. (PG's `forq` supertype isn't modeled; fors stands alone.)
- `PLpgSQL_stmt_foreach_a` — `Label string`, `Var string`, `Slice int`,
  `Expr *PLpgSQL_expr`, `Body []Stmt`. Drops `varno` (resolution) → keep `Var`
  text.

Deparse (round-trippable):

```text
[<<lbl>> ]FOR i IN [REVERSE] <lower> .. <upper> [BY <step>] LOOP
<body>
END LOOP[ lbl]

[<<lbl>> ]FOR r IN <query> LOOP … END LOOP[ lbl]

[<<lbl>> ]FOREACH x [SLICE n] IN ARRAY <expr> LOOP … END LOOP[ lbl]
```

## Conflict / lookahead risks (verify in y.output)

- `opt_loop_label K_FOR` joins `opt_loop_label K_LOOP`/`K_WHILE` from 1.7 — same
  `<<label>>` prefix, disambiguated by the keyword after `>>`. Expect still 0
  new conflicts; check the `y.output` summary line.
- `for_control`'s manual scan + `K_REVERSE`/`DOT_DOT` peeking is the
  error-prone part. Test both lookahead-present and default-reduction paths and
  the `..`-vs-query and `BY`-present/absent branches with exact captured text.
- CASE's `opt_expr_until_when` peek mirrors `decl_datatype`; test simple and
  searched forms.

## Scope — out (deferred)

- `FOR … IN EXECUTE` (dynamic FOR / DYNFORS) and cursor FOR `PLpgSQL_stmt_forc`
  — 1.10.
- Comma-separated FOR target lists — later (corpus-driven).
- `PLpgSQL_expr.Parsed` still nil (text-only), as before.

## Acceptance

JSON cases in new `testdata/for_cases.json` and `testdata/case_cases.json`
(added to `caseFiles`), golden deparse via `PLPGSQL_REWRITE=1`:

- Integer FOR: `FOR i IN 1..10 LOOP …`, `REVERSE`, `BY 2`, labeled, expression
  bounds (`FOR i IN a+1 .. b*2 LOOP`).
- Query FOR: `FOR r IN SELECT * FROM t LOOP …`, labeled.
- FOREACH: `FOREACH x IN ARRAY arr LOOP …`, `SLICE 1`, labeled.
- CASE searched: `CASE WHEN x>0 THEN … WHEN … ELSE … END CASE`.
- CASE simple: `CASE x WHEN 1 THEN … WHEN 2 THEN … ELSE … END CASE`.
- Nesting: FOR/CASE inside loops and blocks; EXIT inside a FOR.
- Errors: loop end-label mismatch.
- Round-trip stable for every positive case.
- `make parser` deterministic, no new `y.output` conflicts; `go build` +
  `golangci-lint` clean; package `ast` byte-identical to upstream.
  </content>
