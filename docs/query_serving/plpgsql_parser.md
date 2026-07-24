# PL/pgSQL Parser

## Overview

Multigres includes a parser for **PL/pgSQL function bodies** — the procedural
language used by `DO` blocks and `CREATE FUNCTION` / `CREATE PROCEDURE … LANGUAGE
plpgsql`. It is a Go port of PostgreSQL's own PL/pgSQL parser
(`src/pl/plpgsql/src/pl_gram.y` and `pl_scanner.c`), built with `goyacc`, and it
lives beside the SQL parser under `go/common/parser/`.

The parser turns a body such as

```plpgsql
DECLARE x int := 0;
BEGIN
  PERFORM set_config('work_mem', '1GB', false);
  x := foo() + 1;
END
```

into an AST of `PLpgSQL_stmt_*` nodes (`go/common/parser/ast/plpgsqlast`), where
each embedded SQL statement or expression (the `PERFORM` query, the assignment
RHS, …) is captured as a `PLpgSQL_expr` the caller can reach and analyze.

Its purpose is **static analysis, not execution**: it exists so the gateway can
see inside otherwise-opaque procedural bodies. It does not compile, resolve, or
run anything.

## Why parse PL/pgSQL

A connection pooler multiplexes many client sessions onto a shared set of
backends. Statements that change backend session state (`SET`, `set_config`, …)
must be observed by the gateway's session tracker so the state travels with the
logical session and does not leak to the next client on that backend.

A procedural body is opaque to the SQL parser: `DO $$ BEGIN SET work_mem = '1GB';
END $$` changes backend state without the SQL parser ever seeing a `SET`. This
is the **Tier 1** concern described in
[unsafe_statement_rejection.md](./unsafe_statement_rejection.md). Closing that
vector means looking _inside_ the body — extracting every embedded SQL statement
and expression so the same analysis applied to top-level SQL can run over the
body's fragments. The PL/pgSQL parser is the tool that makes those fragments
reachable.

The parser is the foundational layer; wiring its output into the planner's
Tier 1 walk is a separate step tracked by the unsafe-statement work.

## Design principles

Three decisions shape the whole implementation.

### 1. Parse-only — no execution, no compilation

PostgreSQL's PL/pgSQL parser is the front half of a compiler: it builds an AST
and threads execution-engine state through it (SPI plans, datum-number
bookkeeping, statement ids, resolved type OIDs). We keep **only the parse-level
structure**. Every ported node drops PG's execution fields; what remains is the
syntax tree and the verbatim text of embedded SQL fragments.

### 2. No variable resolution (no `T_DATUM`)

This is the load-bearing decision. PostgreSQL's scanner resolves an identifier to
a **declared variable** by consulting a namespace built from the `DECLARE`
section, emitting a special `T_DATUM` token for it. The grammar leans on
`T_DATUM` to (a) tell an assignment (`x := …`) apart from an ordinary SQL
statement and (b) bind loop and cursor variables.

For static analysis we need **neither**: we capture every embedded SQL and
expression fragment regardless of whether an identifier is a declared variable.
So we do **not** build a namespace and never emit `T_DATUM`. Everywhere PG's
grammar uses `T_DATUM`, we use `T_WORD` (a plain identifier) or `T_CWORD` (a
compound `a.b.c`), capturing the name as source text.

This works because the scanner collapses a compound name into a single token
just as PG's does, preserving the one-token lookahead property the LALR(1)
grammar relies on (e.g. `target := …` vs. a SQL statement). The consequence is
that our parser **accepts a superset** of what PG accepts: checks that require
resolution (is this a known variable? a known exception condition?) are dropped.
Those consequences are enumerated in [Divergences from PostgreSQL](#divergences-from-postgresql).

### 3. Deparse round-trip

Every AST node implements `SqlString()`, which renders the node back to PL/pgSQL
source. PostgreSQL has no deparser; this is ours. It is used to validate the
parser: a body must satisfy `parse → deparse → parse` producing a stable result.
The deparse canonicalizes some equivalent surface forms (e.g. `GET CURRENT
DIAGNOSTICS` and the no-area form both render as `GET DIAGNOSTICS`), so the
round-trip is a fixpoint rather than byte-identity with the input.

## Architecture

```text
                ParsePLpgSQL(body string)
                          │
                          ▼
   ┌────────────────────────────────────────────────┐
   │  lexer.go  — PL/pgSQL scanner                    │
   │    core SQL lexer (go/common/parser)             │
   │      → internalLex   (raw tokens)                │
   │      → scanNext/Lex  (keyword + compound name    │
   │                       classification: the        │
   │                       plpgsql_yylex analogue)    │
   └────────────────────────────────────────────────┘
                          │ tokens
                          ▼
   ┌────────────────────────────────────────────────┐
   │  plpgsql.y (→ plpgsql.go) — goyacc grammar       │
   │    productions dispatch on a leading keyword,    │
   │    then hand-scan the rest via …                 │
   └────────────────────────────────────────────────┘
                          │ calls
                          ▼
   ┌────────────────────────────────────────────────┐
   │  read_construct.go — hand-scan helpers           │
   │    capture embedded SQL/expressions by byte      │
   │    offset up to terminators                      │
   └────────────────────────────────────────────────┘
                          │ builds
                          ▼
        *plpgsqlast.PLpgSQL_function  (AST)
```

### The scanner (`lexer.go`, `keywords.go`)

The scanner wraps the existing SQL lexer (`go/common/parser`) and mirrors PG's
two-function split:

- **`internalLex`** returns a raw token from the SQL lexer, translating its token
  codes to the PL/pgSQL grammar's codes but doing no PL/pgSQL identifier work —
  the analogue of PG's `internal_yylex`.
- **`scanNext`** is the single fully-classifying token source — the analogue of
  PG's `plpgsql_yylex`. It applies keyword lookup (against the reserved /
  unreserved PL/pgSQL keyword tables in `keywords.go`, taken verbatim from PG's
  `pl_reserved_kwlist.h` / `pl_unreserved_kwlist.h`), compound-name assembly
  (`a.b.c` → one `T_CWORD`), and the `T_WORD` fallback. It never emits `T_DATUM`.
- **`Lex`** is the thin goyacc adapter over `scanNext` (it publishes the semantic
  value to the parser). Both the grammar and the hand-scan helpers pull tokens
  from `scanNext`, exactly as PG routes both through `plpgsql_yylex`; there is no
  separate partially-classified path.

### The grammar (`plpgsql.y`)

The grammar is a `goyacc` port of `pl_gram.y`. Its productions are kept in the
**same order and with the same names** as PostgreSQL's, so each rule maps to the
same-named PG rule and the two files can be read side by side. Rules PG has that
we do not port are the `comp_options` preamble (`#variable_conflict` etc.),
`decl_varname` (its namespace registration), and the separate `stmt_assign`
(assignment is dispatched from the word-initiated statement, matching PG's
resolution-free path). It compiles with **zero shift/reduce and reduce/reduce
conflicts**, like PG's `%expect 0`.

Because much of PL/pgSQL is not context-free in a way `goyacc` can express
directly (embedded SQL fragments are scanned as raw text up to a terminator),
many productions are single-token rules whose action hand-scans the rest of the
statement — precisely how `pl_gram.y` does it.

### The hand-scan helpers (`read_construct.go`)

Each helper ports a specific PostgreSQL scanning function and captures embedded
SQL/expression text **by byte offset** (first token's start … terminator),
ending the capture at the last real token so trailing comments and whitespace are
excluded — matching PG's `read_sql_construct`. Representative helpers:

| Our helper                                                    | Ports (PG)                                                              |
| ------------------------------------------------------------- | ----------------------------------------------------------------------- |
| `readSQLConstruct` / `scanFragment`                           | `read_sql_construct`                                                    |
| `readDatatype`                                                | `read_datatype`                                                         |
| `scanStmtText` / `makeWordStmt`                               | `make_execsql_stmt`                                                     |
| `readForControl`                                              | the `for_control` action                                                |
| `makeReturnStmt`                                              | `make_return_*_stmt`                                                    |
| `makeDynExecute`                                              | the `stmt_dynexecute` action                                            |
| `readFetchDirection`                                          | `read_fetch_direction`                                                  |
| `makeRaiseStmt` / `readRaiseOptions` / `checkRaiseParameters` | the `stmt_raise` action, `read_raise_options`, `check_raise_parameters` |

`make_execsql_stmt`'s subtlety is faithfully reproduced: a `;` ends the statement
only at paren depth 0 **and** outside any `BEGIN/CASE … END` block inside a
`CREATE [OR REPLACE] {FUNCTION|PROCEDURE}` definition, so a `BEGIN ATOMIC … END`
routine body's inner semicolons do not cut the statement short.

## The AST (`plpgsqlast`)

The AST is a self-contained subpackage, `go/common/parser/ast/plpgsqlast`, with
its own `Node` / `NodeTag` hierarchy (separate from the SQL AST's), and its own
generated `Clone`/`Rewrite` walk helpers (via `asthelpergen`, run by `make
parser`).

Every `PLpgSQL_*` type ports the **same-named struct** in PostgreSQL's
`src/pl/plpgsql/src/plpgsql.h`; each carries a `// Ported from …plpgsql.h:START-END`
comment giving the exact source lines, so a type name doubles as its lookup key.

### The embedded-SQL boundary: `PLpgSQL_expr`

`PLpgSQL_expr` is where the two AST hierarchies meet and the node the Tier 1
analysis exists to reach. It holds:

- **`Query`** — the verbatim SQL text of the fragment (the assignment RHS, the
  `PERFORM`/`RETURN` query, an `IF`/`WHILE` condition, `EXECUTE`'s dynamic string,
  …).
- **`ParseMode`** — how `Query` should be parsed (PG's `RawParseMode`).
- **`Parsed`** — the SQL AST that `Query` parses to. It is currently left `nil`:
  turning the fragment text into a SQL `ast.Stmt` (and walking it) is a distinct
  planner-side step. The boundary is deliberately thin so the fragment text is
  always available even before that step exists.

### Statement coverage

The full PL/pgSQL statement grammar is covered: blocks and `DECLARE`
(variables, `CONSTANT`, `COLLATE`, `NOT NULL`, defaults, cursors, `ALIAS`);
assignment; control flow (`IF`/`ELSIF`/`ELSE`, `LOOP`, `WHILE`, `EXIT`/`CONTINUE`,
integer/query/dynamic `FOR`, `FOREACH`, `CASE`); embedded SQL, `PERFORM`, `CALL`,
`DO`, the `RETURN` family; dynamic `EXECUTE`; cursors (`OPEN`/`FETCH`/`MOVE`/`CLOSE`
and cursor declarations); `RAISE`/`ASSERT`; exception blocks; `GET DIAGNOSTICS`;
and `COMMIT`/`ROLLBACK`.

## Divergences from PostgreSQL

Because we do not resolve identifiers, run a namespace, or execute anything, our
parser **accepts a superset** of what PostgreSQL accepts. These are intentional:

- Unknown exception / `RAISE` condition names are accepted (PG validates them
  against a known-condition table).
- Undeclared or constant comma-separated `FOR` targets are accepted (PG requires
  declared, assignable variables).
- A bound-cursor `FOR` loop parses as a query `FOR` loop (distinguishing it needs
  a resolved `refcursor` variable).
- `EXIT`/`CONTINUE` label existence and loop-nesting are not validated; duplicate
  `DECLARE`s are accepted; the implicit `sqlstate`/`sqlerrm` exception variables
  are not created.
- Compound and array-element assignment targets are captured as text rather than
  resolved.

Crucially, the parser stays faithful to every check that is **purely syntactic**
and needs no resolution, so it rejects the same malformed input PG does:
`SQLSTATE` code length/charset validation, the `RAISE` `%`-placeholder/parameter
count, `GET DIAGNOSTICS` item validity per `CURRENT`/`STACKED` area, an integer
`FOR` loop with more than one target, `FETCH` returning multiple rows,
`NOT NULL` without a default, end-label matching, and mismatched parentheses.

Also not ported: the `comp_options` preamble (`#variable_conflict`,
`#print_strict_params`, `#option dump`).

## Testing

Two complementary layers, both driven by the same data-driven harness
(`cases_test.go`, modelled on the SQL parser's `parse_test`):

- **Curated cases** — `testdata/*_cases.json` files, one per statement family.
  Each case is `{comment, body, deparse?, error?}`: a body must parse and its
  deparse must round-trip to a stable result (or, for a negative case, fail with
  the given error substring). Regenerate the golden `deparse` values by running
  the case test with `PLPGSQL_REWRITE=1`.

- **PostgreSQL regression corpus** — `testdata/pg_corpus_cases.json` holds every
  PL/pgSQL body extracted from PostgreSQL's own PL/pgSQL regression SQL. This is
  the acceptance gate: every body must parse and round-trip, except the bodies
  PostgreSQL itself rejects (its negative tests), which carry an expected error
  and must fail the same way. Following the SQL parser's `testdata/postgres`
  convention, the extracted cases are committed but the raw `.sql` files are not;
  `TestGeneratePGCorpusCases` regenerates the JSON from a local PostgreSQL
  checkout (`PLPGSQL_CORPUS_SRC=<pg>/src/pl/plpgsql/src/sql`), pulling bodies out
  of `CREATE FUNCTION`/`PROCEDURE … LANGUAGE plpgsql` and `DO` blocks via the SQL
  parser and recording PG's parse error for its negative tests.

The AST's generated `Clone`/`Rewrite` helpers have their own round-trip smoke
tests in the `plpgsqlast` package.

## Package layout

```text
go/common/parser/plpgsql/
  api.go            ParsePLpgSQL — the public entry point
  plpgsql.y         goyacc grammar (port of pl_gram.y)
  plpgsql.go        generated parser (do not edit; `make parser`)
  lexer.go          PL/pgSQL scanner (port of pl_scanner.c)
  keywords.go       reserved / unreserved keyword tables
  read_construct.go hand-scan helpers (read_sql_construct, make_execsql_stmt, …)
  labels.go         end-label validation (check_labels)
  cases_test.go     data-driven case harness
  corpus_test.go    PG regression corpus test + regenerator
  testdata/         *_cases.json, pg_corpus_cases.json, THIRD_PARTY_NOTICES.md

go/common/parser/ast/plpgsqlast/
  nodes.go              Node / NodeTag / BaseNode infrastructure
  plpgsql_function.go   PLpgSQL_function (AST root)
  statements.go         PLpgSQL_stmt_* nodes + enums
  datums.go             PLpgSQL_var / PLpgSQL_type / PLpgSQL_alias
  expr.go               PLpgSQL_expr (embedded-SQL boundary), RawParseMode
  ast_clone.go          generated deep-clone helpers
  ast_rewrite.go        generated tree-walk/rewrite helpers
```

Generated files (`plpgsql.go`, `ast_clone.go`, `ast_rewrite.go`) are regenerated
by `make parser`; edit `plpgsql.y` and the hand-written AST types, not the
generated output.

## API

```go
import "github.com/multigres/multigres/go/common/parser/plpgsql"

// body is the function body text with the dollar-quote / string delimiters
// already stripped by the caller.
fn, err := plpgsql.ParsePLpgSQL(body)
if err != nil {
    // a parse error; the body is not valid PL/pgSQL (or hits an
    // unsupported construct)
}
_ = fn.Action // *plpgsqlast.PLpgSQL_stmt_block — the top-level BEGIN … END
```

A body must be a block (`BEGIN … END`, optionally with a `DECLARE` section and a
block label), matching PostgreSQL; empty input is a parse error. The caller is
responsible for extracting the body from its surrounding statement — e.g. the
`AS` clause of `CREATE FUNCTION … LANGUAGE plpgsql`, or a `DO` block — before
calling `ParsePLpgSQL`.

## Non-goals

- **Execution / compilation.** No SPI plans, no datum resolution, no runtime.
- **Variable resolution.** No namespace; identifiers are text, never `T_DATUM`.
- **Semantic validation that needs resolution** — unknown condition names,
  undeclared loop targets, etc. are accepted, not rejected.
- **Sub-parsing embedded SQL.** `PLpgSQL_expr.Parsed` is left `nil`; turning
  fragment text into a SQL AST (and walking it for the Tier 1 analysis) is a
  separate planner-side concern.
