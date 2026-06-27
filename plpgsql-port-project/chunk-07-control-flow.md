# Chunk 1.7 — Control flow: IF, LOOP, WHILE, EXIT/CONTINUE

Adds the simple control-flow statement families. Each just adds a few grammar
productions plus one AST node, reusing the `read_sql_construct` boundary from 1.5
for every embedded condition. No new scanner machinery.

EXIT/CONTINUE is folded in here (not deferred to 1.8): it is cheap, uses the
existing single-terminator scanner (`expr_until_semi`), and is the loop-control
companion of the LOOP/WHILE this chunk introduces. FOR family + CASE stay in 1.8,
where the real complexity lives (two-terminator scanning, `case_when_list`).

Ported from `postgres/src/pl/plpgsql/src/pl_gram.y` (`stmt_if`, `stmt_elsifs`,
`stmt_else`, `stmt_loop`, `stmt_while`, `loop_body`, `opt_loop_label`,
`stmt_exit`, `exit_type`, `opt_exitcond`, `expr_until_then`, `expr_until_loop`)
and `plpgsql.h` (`PLpgSQL_stmt_if`, `PLpgSQL_if_elsif`, `PLpgSQL_stmt_loop`,
`PLpgSQL_stmt_while`, `PLpgSQL_stmt_exit`).

## Part A — AST nodes (`plpgsqlast`)

Four new statement-ish nodes + one helper, in `statements.go`, with NodeTags in
`nodes.go`. Parse-level subsets — drop PG's `stmtid`/`cmd_type` (carried by
BaseNode):

- `PLpgSQL_stmt_if` — `Cond *PLpgSQL_expr`, `ThenBody []Stmt`,
  `ElsifList []*PLpgSQL_if_elsif`, `ElseBody []Stmt`. Implements `Stmt`.
- `PLpgSQL_if_elsif` — `Cond *PLpgSQL_expr`, `Stmts []Stmt`. A helper node (PG's
  `PLpgSQL_if_elsif` is **not** a `PLpgSQL_stmt`), so it implements `Node` only,
  **not** `Stmt`. Needed as a `Node` so asthelpergen walks it.
- `PLpgSQL_stmt_loop` — `Label string`, `Body []Stmt`. Implements `Stmt`.
- `PLpgSQL_stmt_while` — `Label string`, `Cond *PLpgSQL_expr`, `Body []Stmt`.
  Implements `Stmt`.
- `PLpgSQL_stmt_exit` — `IsExit bool` (EXIT vs CONTINUE), `Label string`,
  `Cond *PLpgSQL_expr` (the optional `WHEN` condition). Implements `Stmt`.

New NodeTags: `T_PLpgSQL_stmt_if`, `T_PLpgSQL_if_elsif`, `T_PLpgSQL_stmt_loop`,
`T_PLpgSQL_stmt_while`, `T_PLpgSQL_stmt_exit` (+ `String()` cases). `make parser` regenerates the
`plpgsqlast` clone/rewrite helpers; the new `[]*PLpgSQL_if_elsif` /`[]Stmt`
fields generate cleanly (the generator already emits `CloneSliceOfStmt` etc.).

### Deparse (`SqlString`)

The block body emits each statement as `s.SqlString()` + `";\n"`, so these nodes
must **not** emit their own trailing `;`. Bodies render their inner statements
the same way the block does (`stmt;\n` per statement). Shapes:

```text
IF <cond> THEN
<then stmts>
ELSIF <cond> THEN
<elsif stmts>
ELSE
<else stmts>
END IF

[<<lbl>> ]LOOP
<body stmts>
END LOOP[ lbl]

[<<lbl>> ]WHILE <cond> LOOP
<body stmts>
END LOOP[ lbl]

EXIT[ lbl][ WHEN <cond>]
CONTINUE[ lbl][ WHEN <cond>]
```

ELSIF/ELSE clauses render only when present. A helper to render a `[]Stmt` body
(the `stmt;\n` loop) is shared by all three and reused from the block deparse if
convenient.

## Part B — the scanner add-on

PG's `expr_until_then` / `expr_until_loop` call `read_sql_expression(K_THEN)` /
`read_sql_expression(K_LOOP)` — the same scan as the `;`-terminated expression,
just a different terminator (which is **consumed**, like `;` is). Generalize the
1.5 helper in `read_construct.go`:

```go
func (l *lexer) readSQLExprUntil(terminators ...int) *plpgsqlast.PLpgSQL_expr
// existing readSQLExpr() becomes readSQLExprUntil(';')
```

No change to `scanFragment` (already takes variadic terminators) or `beginScan`.

## Part C — grammar

```text
proc_stmt:  … | stmt_if | stmt_loop | stmt_while | stmt_exit   // add four alts

stmt_if:    K_IF expr_until_then proc_sect stmt_elsifs stmt_else K_END K_IF ';'
stmt_elsifs: /*empty*/ | stmt_elsifs K_ELSIF expr_until_then proc_sect
stmt_else:   /*empty*/ | K_ELSE proc_sect

stmt_loop:  opt_loop_label K_LOOP loop_body
stmt_while: opt_loop_label K_WHILE expr_until_loop loop_body
loop_body:  proc_sect K_END K_LOOP opt_label ';'

stmt_exit:  exit_type opt_label opt_exitcond
exit_type:  K_EXIT { true } | K_CONTINUE { false }
opt_exitcond: ';' { nil } | K_WHEN expr_until_semi

opt_loop_label: /*empty*/ | LESS_LESS any_identifier GREATER_GREATER

expr_until_semi: /*empty*/ { beginScan…; readSQLExprUntil(';') }   // = readSQLExpr
expr_until_then: /*empty*/ { beginScan…; readSQLExprUntil(K_THEN) }
expr_until_loop: /*empty*/ { beginScan…; readSQLExprUntil(K_LOOP) }
```

`stmt_exit` keys on `K_EXIT`/`K_CONTINUE` (unreserved keywords), which begin no
other `proc_stmt` alternative, so it shifts unambiguously. `K_WHEN` is reserved,
so it is never mistaken for the `opt_label` identifier.

**Dropped semantic checks (no namespace).** PG's `stmt_exit` action validates the
label exists in an enclosing block/loop, rejects block labels in `CONTINUE`, and
rejects an unlabeled EXIT/CONTINUE outside any loop — all via the namespace we
deliberately don't build (decision #1). We capture `IsExit`/`Label`/`Cond` and
skip these checks, consistent with the no-resolution stance; the Tier-1 walker
doesn't need them.

- `expr_until_then` / `expr_until_loop` are empty productions that scan in their
  action, using the exact `beginScan(plpgsqlrcvr.char)` / clear-lookahead dance
  already proven in `decl_datatype` and `stmt_assign`.
- `loop_body` returns a small `loopBody{stmts []plpgsqlast.Stmt; endLabel string}`
  Go struct (new union field), mirroring PG's `loop_body` struct; the
  `stmt_loop`/`stmt_while` actions call `checkLabels(label, endLabel)`.
- `opt_loop_label` is byte-identical to `opt_block_label` but kept as a separate
  production to mirror PG (decision: stay close to PG's grammar).

New `%type`s: `<stmt> stmt_if stmt_loop stmt_while stmt_exit`,
`<stmts> stmt_else` and the then/elsif bodies, `<elsifs> stmt_elsifs` (new union
field `elsifs` of type `[]*PLpgSQL_if_elsif`), `<expr> expr_until_semi
expr_until_then expr_until_loop opt_exitcond`, `<bval> exit_type`,
`<loopbody> loop_body` (new union field `loopbody`), `<str> opt_loop_label`.

## Conflict risk (must verify in `y.output`)

`<<label>>` can begin a labeled **block** (`proc_stmt: pl_block ';'` →
`opt_block_label`) or a labeled **loop** (`stmt_loop`/`stmt_while` →
`opt_loop_label`). After `LESS_LESS any_identifier GREATER_GREATER` the parser
reduces to one of the two by one-token lookahead — `K_BEGIN`/`K_DECLARE` ⇒ block,
`K_LOOP`/`K_WHILE` ⇒ loop. Disjoint, so LALR(1)-clean in PG; goyacc should agree.
**Verify the `y.output` summary line still reports the same conflict count as
before this chunk** (the 8 pre-existing never-reduced PLAssignStmt rules are
inherited from the SQL grammar fragment and unrelated). Any _new_ shift/reduce or
reduce/reduce there means the labeled-block-vs-loop split needs attention.

## Scope — out (deferred)

- `FOR` family, `CASE`, `FOREACH` — 1.8 (two-terminator scanning, when-lists).
- EXIT/CONTINUE label-existence and outside-loop validation — needs the
  namespace we don't build (decision #1); we parse the statement, not its
  semantics.
- `PLpgSQL_expr.Parsed` still left nil (text-only capture), as in 1.5/1.6.

## Acceptance

JSON cases in a new `testdata/control_flow_cases.json` (added to `caseFiles`),
golden deparse filled via `PLPGSQL_REWRITE=1`:

- `IF`: simple `IF cond THEN …`, with `ELSE`, with one and multiple `ELSIF`,
  nested IF, IF inside a loop; condition captured as the RHS `Query`.
- `LOOP`: bare `LOOP … END LOOP`, labeled `<<l>> LOOP … END LOOP l`, empty body.
- `WHILE`: `WHILE cond LOOP … END LOOP`, labeled, condition captured.
- `EXIT`/`CONTINUE`: bare, with label, with `WHEN cond`, with label + `WHEN`;
  inside a loop body; assert `IsExit`, `Label`, captured `Cond.Query`.
- Error: end label mismatch on a loop (`END LOOP wrong`) → `checkLabels` error;
  `IF … END IF` label rules (PG allows no label on `END IF`).
- Round-trip stable for every positive case.
- `make parser` deterministic, no _new_ conflicts in `y.output`; `go build` +
  `golangci-lint` clean; package `ast` byte-identical to upstream.

## Risks / things to verify

- **Labeled-block-vs-loop conflict** (above) — the one real grammar risk.
- **Lookahead in `expr_until_*`** — same machinery as `decl_datatype`; test both
  the lookahead-present and default-reduction paths (a condition that starts with
  a word vs. with `(`).
- **Deparse trailing `;`** — nodes must not self-append `;`; the block/body loop
  owns it. Round-trip catches mistakes.
