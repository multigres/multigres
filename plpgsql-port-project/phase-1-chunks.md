# Phase 1 — Parser and scanner port, chunked

The whole port ships as a single PR; the chunks below are ordered working
increments within that branch, not separate PRs. Each is sized so the branch
stays green after it and later chunks build incrementally on earlier ones.
When starting a chunk, open its `chunk-NN-*.md` detail file — that's where we
sign off on scope before writing code.

## Ordering principle

1. Plumbing before grammar: build pipeline, lexer, keyword table, and the
   minimum AST vocabulary come first so subsequent chunks only touch grammar
   rules and the one corresponding AST node type.
2. Grammar grows by statement family: each chunk adds one family (block, IF,
   LOOP, etc.), a handful of grammar rules, and the matching AST node, with
   JSON fixtures covering both positive parses and deparse round-trip where
   feasible.
3. Hardening comes last: PG's regression corpus runs against the finished
   parser as an acceptance gate, not incrementally.

## Chunk list

### 1.1 Grammar + build scaffolding

Empty `plpgsql.y` that builds via goyacc into `plpgsql.go`. New
`generate.go` drives `go generate`. `Makefile` `parser` target regenerates
both files. Public stub `ParsePLpgSQL(body string)
(*plpgsqlast.PLpgSQL_function, error)` returns a trivial AST (just a container)
so later chunks have a landing point. The `PLpgSQL_function` root lives in the
new `plpgsqlast` subpackage (its own `Node`/`NodeTag`/`BaseNode`), kept out of
package `ast`. No real parsing yet.

**Acceptance:** `make parser` succeeds on a fresh checkout; `go build ./...`
green; one smoke-test asserts `ParsePLpgSQL("")` returns a non-nil function
with zero statements.

### 1.2 PL/pgSQL keyword table + lexer wrapper

`plpgsql_keywords.go` with PG's reserved + unreserved PL/pgSQL keywords
(`BEGIN`, `END`, `DECLARE`, `IF`, `LOOP`, `FOR`, `RAISE`, `EXECUTE`,
`PERFORM`, `CALL`, `RETURN`, `EXCEPTION`, etc.). `plpgsql_lexer.go` wraps the
existing `Lexer` from `lexer.go`, reclassifying identifiers that match
PL/pgSQL keywords. Mirrors PG's `plpgsql_IdentifierLookup` state so
identifiers in a DECLARE clause resolve differently than in a statement
body. No grammar integration yet beyond the tokens the 1.1 scaffolding
needs.

**Acceptance:** unit tests lex `BEGIN RAISE NOTICE 'hi'; END` and assert the
expected token stream; identifier-vs-keyword edge cases covered (e.g. `end`
as column name inside an SQL fragment must not become the PL/pgSQL `END`
token).

### 1.3 Core AST nodes

Hand-write only the genuinely **shared** foundation nodes, all in the
`plpgsqlast` subpackage (per-family nodes land with their grammar in later
chunks, not as speculative stubs here):

- `Stmt` marker interface (Go analogue of PG's `PLpgSQL_stmt` supertype)
- `PLpgSQL_function` reshaped to `Action *PLpgSQL_stmt_block` (PG's
  `function->action`)
- `PLpgSQL_stmt_block` (container: `Label`, `Body []Stmt`, `Exceptions`)
- `PLpgSQL_expr` (SQL fragment: `Query` text + parsed `ast.Stmt` — the one place
  `plpgsqlast` references the SQL AST; `Parsed` is `ast.Stmt`, with bare
  expressions wrapped as `SELECT <expr>` by chunk 1.6)
- `PLpgSQL_exception_block` empty placeholder (real body in 1.12)

Parse-level subset only — PG's execution-engine fields (SPI plans, `ExprState`,
`stmtid`, datum-number bookkeeping) are dropped. All implement
`plpgsqlast.Node` (statements also implement `Stmt`), NOT `ast.Node`. No grammar
productions created yet beyond what 1.1 already has.

**Deferred out of 1.3, to be DEFINED by the chunk that introduces the grammar
that produces them:** `PLpgSQL_stmt_null` (1.4), the datum family
`PLpgSQL_var`/`_row`/`_rec`/`_recfield` and `PLpgSQL_type` (1.5),
`PLpgSQL_condition` and the full `PLpgSQL_exception`/`_exception_block` body
(1.12).

**Acceptance:** `go build ./...` green; no new parser behavior.

### 1.3a Clone/rewrite generator machinery for `plpgsqlast`

Wire `go/tools/asthelpergen` (or an equivalent generator) to the `plpgsqlast`
subpackage so the PL/pgSQL nodes get generated `Clone`/`Rewrite`/walk helpers
against the `plpgsqlast.Node` interface — the analogue of what `ast` already
has, but self-contained in the subpackage. Add a `//go:generate` directive and
fold it into `make parser`. Sort out the BaseNode/clone-helper plumbing the
generator expects (`CloneBaseNode` analogue, etc.) and the embedded-SQL
boundary (a `PLpgSQL_expr`'s `ast.Stmt` should be handed to the SQL-side
`ast` helpers, not deep-walked by the `plpgsqlast` generator — likely a
clone-exclude / boundary case).

**Acceptance:** the generator runs under `make parser`, output is deterministic
across reruns, `go build ./...` green, and a round-trip/clone smoke test over a
small `plpgsqlast` tree passes. Defer until 1.3 lands a few real node types so
there is something worth generating.

### 1.4 Minimal block parsing — `BEGIN … END;`

First real grammar work. Add productions for the outermost block: optional
label, optional DECLARE section (parsed empty for now), `BEGIN` … `END`,
optional label after `END`, trailing `;`. Emits a `PLpgSQL_stmt_block`.
`ParsePLpgSQL` now parses bodies like `BEGIN END;` and
`BEGIN NULL; END;` (NULL statement is the simplest leaf — **define** a trivial
`PLpgSQL_stmt_null` node here, matching PG; 1.3 deliberately left it out).

**Acceptance:** table-driven parse tests for empty block, NULL-only block,
labeled block, multiple blocks; testdata JSON fixtures.

### 1.5 DECLARE section + type names

Add grammar for `DECLARE var [CONSTANT] type [NOT NULL] [:= expr];` inside
the outer block. Types are scanned until the initializer-or-terminator, the
token slice handed to the main SQL parser for TypeName parsing. **Define** the
datum family here — `PLpgSQL_var` (and `PLpgSQL_row`/`_rec`/`_recfield` and
`PLpgSQL_type` as needed); 1.3 deferred all of them. `PLpgSQL_var` gets
populated for the first time.

**Acceptance:** parse `DECLARE i int; BEGIN END;` and variants with default
values, CONSTANT, NOT NULL, %TYPE, %ROWTYPE. Type-resolution stubs are OK;
we only need the parse.

### 1.6 Assignment + SQL-fragment boundary

Implement the `read_sql_construct` equivalent: scan tokens until a
terminator set (`;`, `LOOP`, `WHEN`, `USING`, `THEN`, `ELSE`, etc.),
stringify, set `PLpgSQL_expr.Query` + `ParseMode`, parse, and store the
result in `PLpgSQL_expr.Parsed`. Then use it for the simplest consumer:
assignment (`variable := expr;`). This is the boundary that the Tier 1 walker
will ultimately traverse — critical to get right.

**DECISION to make here — how to parse a bare expression fragment** (`ParseMode
== RAW_PARSE_PLPGSQL_EXPR`, e.g. an IF condition or assignment RHS), since our
`ParseSQL` only parses full statement lists (≈ PG's `RAW_PARSE_DEFAULT`):

- **(a) Pragmatic:** wrap the fragment as `SELECT <expr>` and parse that, so
  `Parsed` is always an `ast.Stmt`. Cheap; no shared-parser change; the walker
  unwraps the single target.
- **(b) Faithful to PG:** teach the SQL parser an expression entry mode (PG's
  `RAW_PARSE_PLPGSQL_EXPR` / `RAW_PARSE_PLPGSQL_ASSIGN1..3` via `raw_parser`).
  Closer to PG, but a change to the shared `go/common/parser`.

`PLpgSQL_expr.ParseMode` already exists to carry the distinction; pick (a) or
(b) when writing the chunk-06 detail doc.

**Acceptance:** parse `BEGIN x := 1; END;`, `BEGIN x := (SELECT foo());
END;`, confirm `PLpgSQL_expr.Parsed` is a valid SQL AST.

### 1.7 Control flow — IF, LOOP, WHILE

Grammar for `IF … THEN … [ELSIF … THEN …] [ELSE …] END IF`, bare `LOOP … END
LOOP`, `WHILE expr LOOP … END LOOP`. Corresponding AST nodes
(`PLpgSQL_stmt_if`, `_loop`, `_while`). Each embedded expression goes
through the 1.6 boundary.

### 1.8 FOR family + CASE + EXIT

`FOR var IN a..b [BY step] [REVERSE] LOOP` (integer),
`FOR var [, …] IN query LOOP` (query), `FOR var IN cursor LOOP` (cursor),
`FOREACH var [SLICE n] IN ARRAY expr LOOP`. `CASE [expr] WHEN … THEN …
[ELSE …] END CASE`. `EXIT [label] [WHEN expr]`, `CONTINUE` counterparts.

### 1.9 SQL-embedding — EXECSQL, PERFORM, CALL, RETURN

`stmt_execsql` is "any SQL statement that isn't handled by a PL/pgSQL
production" — parsed via the 1.6 boundary. `PERFORM expr;`,
`CALL proc(args);`, `RETURN [expr]`, `RETURN NEXT expr`, `RETURN QUERY
query`, `RETURN QUERY EXECUTE expr`.

### 1.10 Dynamic + cursor — DYNEXECUTE, DYNFORS, OPEN, FETCH, CLOSE

`EXECUTE expr [INTO target] [USING args]`, `FOR var IN EXECUTE expr LOOP`,
cursor declaration + `OPEN`, `FETCH [direction] FROM cursor INTO target`,
`MOVE`, `CLOSE cursor`. These are the statements the Tier 1 walker needs
to reason about for the dynamic-EXECUTE policy, so the AST must expose the
expression node cleanly.

### 1.11 RAISE + ASSERT

`RAISE [level] [format [, args]] [USING option = expr, …];` including the
no-args re-raise form. `ASSERT expr [, message];`.

### 1.12 Exception blocks

`EXCEPTION WHEN condition [OR condition …] THEN … [WHEN …] END;` inside a
block. **Define** `PLpgSQL_condition` and `PLpgSQL_exception` here, and flesh
out the `PLpgSQL_exception_block` placeholder that 1.3 stubbed (add its
`Exc_list` of WHEN clauses) — this is where they get their real population.

### 1.13 GET DIAGNOSTICS, COMMIT, ROLLBACK

Close out the remaining statement productions. `GET [CURRENT] DIAGNOSTICS
var = item [, …];`, bare `COMMIT [AND [NO] CHAIN];`, `ROLLBACK …`.

### 1.14 Compile-side parser-setup hooks

Port just the parsing-side hooks from `pl_comp.c`:
`plpgsql_parser_setup`, `plpgsql_pre_column_ref`, `plpgsql_post_column_ref`,
`plpgsql_param_ref`. Wired into the main SQL parser's hookable points so
embedded SQL fragments correctly classify PL/pgSQL variable references. For
static analysis we only need enough resolution to distinguish "variable"
from "literal/column" — the walker uses that to reason about
`set_config(literal, …)` vs `set_config(var, …)`.

### 1.15 PG regression corpus harness

Golden-file suite: every `.sql` file in `~/postgres/src/pl/plpgsql/src/sql/`
must parse without error through `ParsePLpgSQL`. Expected-output comparison
is out of scope (we don't execute). Vendor the inputs into
`go/common/parser/testdata/plpgsql_pg_corpus/` or read them from a
configurable path. Failures are the acceptance gate for Phase 1.

## Phase 1 exit criteria

- All 15 chunks merged.
- `make parser` green.
- `/mt-dev unit ./go/common/parser/...` green including the PG corpus test.
- `ParsePLpgSQL` accepts the full PG regression corpus.
- No planner-side changes yet; the parser is importable but unused outside
  its own tests. Phase 2 picks up wiring.
