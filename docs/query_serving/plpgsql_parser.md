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
see inside otherwise-opaque procedural bodies. It resolves declared variables
(enough to tell an assignment from a SQL statement and to validate targets) but
does not resolve types against a catalog, compile, or run anything.

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

### 2. Statement-level variable resolution, but no catalog

PostgreSQL's scanner resolves an identifier to a **declared variable** by
consulting a namespace built from the `DECLARE` section, emitting a special
`T_DATUM` token for it. The grammar leans on `T_DATUM` to (a) tell an assignment
(`x := …`) apart from an ordinary SQL statement, (b) bind loop and cursor
variables, and (c) validate targets (a `CONSTANT` is not assignable, a loop
target must be a known variable, …).

We port this — a compile-time namespace stack (`namespace.go`, from PG's
`plpgsql_ns_*`) and a flat datum list — so the parser reaches exact accept/reject
parity on the checks that need only a namespace. What we deliberately **do not**
do is resolve types against a **system catalog**: we parse a body in isolation,
with no database. So a declared type is captured as text (`DECLARE x foo`
accepts any `foo`), `%TYPE`/`%ROWTYPE`/collation names are not resolved, and a
variable declared with a _named composite type_ can't be told from a scalar. The
composite cases we can recognize **syntactically** — the `RECORD` pseudo-type and
`%ROWTYPE` — are typed as records, so `rec.field` resolves; a named composite type
stays a scalar.

Resolution is **mode-gated** (PG's `IdentifierLookup`): a scalar resolves only in
`NORMAL` mode, which is exactly the statement-level tokens the grammar reads
directly. Embedded SQL/expression fragments are scanned in `EXPR` mode and the
`DECLARE` section in `DECLARE` mode, so identifiers there are left as text — which
keeps fragment capture byte-identical to the top-level SQL the fragment contains.
See [Variable resolution](#variable-resolution-namespacego).

The residual effect is that our parser still **accepts a superset** of what PG
accepts: checks that need the catalog (unknown type, unknown exception condition)
are dropped. Those consequences are enumerated in
[Divergences from PostgreSQL](#divergences-from-postgresql).

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
  (`a.b.c` → one token), variable **resolution** against the namespace, and the
  `T_WORD` / `T_CWORD` fallback. A name that resolves to a declared variable
  becomes `T_DATUM` (carrying the datum); otherwise it is `T_WORD` / `T_CWORD` /
  a keyword. Resolution is gated by the lookup mode (see below).
- **`Lex`** is the thin goyacc adapter over `scanNext` (it publishes the semantic
  value to the parser). Both the grammar and the hand-scan helpers pull tokens
  from `scanNext`, exactly as PG routes both through `plpgsql_yylex`; there is no
  separate partially-classified path.

### Variable resolution (`namespace.go`)

The namespace is a stack of block/loop-labelled levels (`plpgsql_ns_*`); each
declared variable is added to a flat datum list and registered in the namespace
by name. Resolution is a two-hop indirection: a name looks up to a namespace item
carrying a `dno`, which indexes the datum list. The datum kinds are `PLpgSQL_var`
(scalar), `PLpgSQL_rec` (record — `RECORD`/`%ROWTYPE`), `PLpgSQL_row` (a transient
scalar list behind a comma-separated targetlist), `PLpgSQL_recfield` (`rec.field`,
built lazily), and `PLpgSQL_alias`.

Two subtleties make this safe and small:

- **Lookup mode** (`IdentifierLookup`: `NORMAL` / `DECLARE` / `EXPR`). A scalar is
  resolved to `T_DATUM` only in `NORMAL`. The `DECLARE` section runs in `DECLARE`
  mode (a name being declared is not resolved against an outer variable); every
  embedded fragment read runs in `EXPR` mode (identifiers inside SQL are left for
  the SQL parser). So resolution touches only the handful of statement-level
  tokens the grammar reads directly; fragment capture is unaffected.
- **`AT_STMT_START`** (from `pl_scanner.c`). At the start of a statement a word is
  resolved to a variable only when it is immediately followed by `:=`, `=`, or
  `[`. This is what lets a variable named like a statement-introducing keyword
  (`comment`, `forward`) be used as an assignment target while the keyword still
  introduces its statement elsewhere. The only new statement-start `T_DATUM`
  position is therefore the assignment target; the other positions (`FOR`/`FOREACH`
  target, cursor, `INTO`, `GET DIAGNOSTICS` target, labels) follow a keyword and
  each has its own `T_DATUM` grammar arm.
- **`tok_is_keyword`** (`tokKeyword`). The dual of `AT_STMT_START`: where a
  hand-scan expects an unreserved keyword (a `FETCH` direction, a `RAISE`
  option/level, a `GET DIAGNOSTICS` item, `OPEN … SCROLL`, `RETURN NEXT`/`QUERY`,
  integer-`FOR` `REVERSE`), a same-named variable resolves to `T_DATUM` first, so
  the token is rechecked by name and the keyword wins — matching PG.

### The grammar (`plpgsql.y`)

The grammar is a `goyacc` port of `pl_gram.y`. Its productions are kept in the
**same order and with the same names** as PostgreSQL's, so each rule maps to the
same-named PG rule and the two files can be read side by side — including
`stmt_assign` (dispatched from a resolved `T_DATUM` target) and the namespace
lifecycle: `plpgsql_ns_push`/`pop` at block and loop boundaries, and
`decl_varname` registering each declared variable (its duplicate-declaration check
falls out of the namespace lookup, as in PG). The `comp_options` preamble
(`#variable_conflict`, `#print_strict_params`, `#option dump`) **is** ported — it
parses and round-trips — but its directives are semantically inert here: they
configure name-resolution policy and execution, which we do not do. It compiles
with **zero shift/reduce and reduce/reduce conflicts**, like PG's `%expect 0`.

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
and `COMMIT`/`ROLLBACK`. The leading `comp_options` preamble
(`#variable_conflict`, `#print_strict_params`, `#option dump`) is also parsed.

## Divergences from PostgreSQL

Because we resolve variables but not types, run no catalog, and don't sub-parse
embedded SQL, our parser **accepts a superset** of what PostgreSQL accepts. Every
divergence traces to one of three structural roots:

1. **Parse-only, no execution.** No SPI plans, resolved type OIDs, or runtime.
   `PLpgSQL_expr.Parsed` is `nil` — a fragment is verbatim text, not a parsed tree.
2. **No system catalog.** Declared types, `%TYPE`/`%ROWTYPE`, table rowtypes,
   collations, and error-condition names are not resolved.
3. **Embedded SQL fragments are not sub-parsed.** PG runs each through the raw SQL
   grammar (`check_sql_expr`); we capture the text and stop.

### What we match exactly

The resolution work brings the checks that need only a namespace (root #2 aside)
to exact parity with PG. We reject the same malformed input PG does:

- **Purely syntactic:** `SQLSTATE` code length/charset, the `RAISE`
  `%`-placeholder/parameter count, `GET DIAGNOSTICS` item validity per
  `CURRENT`/`STACKED` area, an integer `FOR` loop with more than one target,
  `FETCH` returning multiple rows, `NOT NULL` without a default, end-label
  matching, mismatched parentheses, `#print_strict_params` `on`/`off`.
- **Needs the namespace:** duplicate declaration in a block; assignment /
  `GET DIAGNOSTICS` / loop target is `CONSTANT` (including a field of a `CONSTANT`
  record and a `CONSTANT` comma-list `FOR` member); a `GET DIAGNOSTICS` target
  that is a record/row (`is not a scalar variable`); `EXIT`/`CONTINUE` outside a
  loop, a nonexistent label, or a block label under `CONTINUE`; a cursor `FOR`
  over an unbound/non-refcursor variable; an undeclared name in a comma-list `FOR`
  target, and an undeclared single target of a query/dynamic `FOR` or `FOREACH`.
- **Record typing (catalog-free cases):** a `RECORD`/`%ROWTYPE` declaration builds
  a record, so `rec.field` resolves to a field reference and its assignability is
  checked.
- **Aliases and implicit variables:** an `ALIAS FOR` reuses its target's datum, so
  an alias of a `CONSTANT` is not assignable and `alias.field` resolves; the
  implicit `sqlstate`/`sqlerrm` handler variables are created as `CONSTANT`s scoped
  to the exception block; the private loop variable of an integer/cursor `FOR` is
  created, so `i := i + 1` (or `rec.field := …`) inside the body resolves.
- **Keyword shadowing** (`tok_is_keyword`): a variable named like an unreserved
  keyword resolves to a `T_DATUM`, but where the grammar requires that keyword
  (`FETCH` direction, `RAISE` level/option/`SQLSTATE`, `GET DIAGNOSTICS` item,
  `OPEN … SCROLL`, `RETURN NEXT`/`QUERY`, integer-`FOR` `REVERSE`) the keyword
  still wins — the token is rechecked by name, as in PG.
- The former **subset** divergence is fixed: a variable named like an unreserved
  keyword (`forward`) is now a valid assignment target (PG resolves it; so do we).

### What still diverges (superset — we accept; PG rejects)

| Body                                        | PG                    | Us                                | Root |
| ------------------------------------------- | --------------------- | --------------------------------- | ---- |
| `DECLARE x nonexistent_type;`               | error                 | accepted (type is text)           | #2   |
| `DECLARE x tbl.c%TYPE;` / `COLLATE "en_US"` | resolved              | captured as text                  | #2   |
| `DECLARE r my_composite_type; … r.f := 1`   | resolves `r.f`        | `r` is a scalar, `r.f` stays text | #2   |
| `EXCEPTION WHEN no_such_cond THEN …`        | error                 | accepted                          | #2   |
| `PERFORM not valid sql;` / `x := 1 +;`      | error (SQL sub-parse) | accepted as text                  | #3   |
| `param := 1;` (`param` an unseen argument)  | assignment            | execsql text (round-trips)        | #1   |

A shadowed-outer-variable warning is also not emitted (PG's check is off by
default and needs a cross-block comparison). The single residual **record** gap is
the named-composite-type row above: a `RECORD`/`%ROWTYPE` gives a syntactic hint, a
named type does not, so only that case falls back to text.

### Deparse canonicalization

The `parse → deparse → parse` round-trip is a fixpoint, not byte-identity:
`GET CURRENT DIAGNOSTICS` → `GET DIAGNOSTICS`, `=` assignment → `:=`, trailing
comments dropped, a simple `CASE` not rewritten to `var IN (…)`, and the
`comp_options` preamble parsed but inert.

### The parity ceiling

What remains is set by the roots: a small independent port (the
unrecognized-exception-condition table — a static SQLSTATE list, no catalog), and
the permanently catalog-blocked cases (unknown-type rejection,
`%TYPE`/collation resolution, record typing for **named composite types**, and
validating embedded SQL fragments).

## Testing

Two complementary layers, both driven by the same data-driven harness
(`cases_test.go`, modelled on the SQL parser's `parse_test`):

- **Curated cases** — `testdata/*_cases.json` files, one per statement family
  (plus `resolution_cases.json` for the namespace/resolution behaviors). Each case
  is `{comment, body, deparse?, error?}`: a body must parse and its deparse must
  round-trip to a stable result (or, for a negative case, fail with the given
  error substring). Regenerate the golden `deparse` values by running the case
  test with `PLPGSQL_REWRITE=1`. The namespace helpers also have direct unit tests
  in `namespace_test.go`.

- **PostgreSQL regression corpus** — two committed JSON files hold every PL/pgSQL
  body extracted from PostgreSQL's regression SQL. This is the acceptance gate:
  every body must parse and round-trip, except the bodies PostgreSQL itself rejects
  (its negative tests), which carry an expected error and must fail the same way.
  - `testdata/pg_corpus_cases.json` — from the plpgsql module's own tests
    (`src/pl/plpgsql/src/sql`), regenerated by `TestGeneratePGCorpusCases`
    (`PLPGSQL_CORPUS_SRC=<pg>/src/pl/plpgsql/src/sql`).
  - `testdata/pg_regress_corpus_cases.json` — from the main, larger regression file
    (`src/test/regress/sql/plpgsql.sql`), regenerated by
    `TestGeneratePGRegressCorpusCases`
    (`PLPGSQL_REGRESS_CORPUS_SRC=<pg>/src/test/regress/sql/plpgsql.sql`). This file
    covers families the module tests do not (`FOREACH`, `GET DIAGNOSTICS`,
    `RETURN QUERY`, `MOVE`, `CLOSE`, …). Nearly every body parses and round-trips;
    the handful that carry an expected `error` are PG's own negative tests — the
    `RAISE` parameter-count cases and the resolution rejections (unbound cursor
    `FOR`, `EXIT`/`CONTINUE` misuse, assignment to a `CONSTANT`), which our ported
    checks now reject with PG's message.

  Following the SQL parser's `testdata/postgres` convention, the extracted cases
  are committed but the raw `.sql` files are not; both generators pull bodies out
  of `CREATE FUNCTION`/`PROCEDURE … LANGUAGE plpgsql` and `DO` blocks via the SQL
  parser and record PG's parse error for the negative tests. A body that our old
  no-resolution parser accepted but PG rejects (e.g. the `forward` assignment, now
  fixed, or the resolution negatives) has its expectation curated by hand.

The AST's generated `Clone`/`Rewrite` helpers have their own round-trip smoke
tests in the `plpgsqlast` package.

## Package layout

```text
go/common/parser/plpgsql/
  api.go            ParsePLpgSQL — the public entry point
  plpgsql.y         goyacc grammar (port of pl_gram.y)
  plpgsql.go        generated parser (do not edit; `make parser`)
  lexer.go          PL/pgSQL scanner (port of pl_scanner.c), incl. resolution
  keywords.go       reserved / unreserved keyword tables
  namespace.go      namespace stack + datum list + resolution checks (pl_funcs.c)
  read_construct.go hand-scan helpers (read_sql_construct, make_execsql_stmt, …)
  labels.go         end-label validation (check_labels)
  cases_test.go     data-driven case harness
  namespace_test.go namespace helper unit tests
  corpus_test.go    PG regression corpus tests + regenerators
  testdata/         *_cases.json (incl. resolution_cases.json), pg_corpus_cases.json,
                    pg_regress_corpus_cases.json, THIRD_PARTY_NOTICES.md

go/common/parser/ast/plpgsqlast/
  nodes.go              Node / NodeTag / BaseNode infrastructure
  plpgsql_function.go   PLpgSQL_function (AST root)
  statements.go         PLpgSQL_stmt_* nodes + enums
  datums.go             PLpgSQL_var / _rec / _row / _recfield / _type / _alias
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

- **Execution / compilation.** No SPI plans, no resolved type OIDs, no runtime.
- **Type / catalog resolution.** No database; declared types, `%TYPE`/`%ROWTYPE`,
  collations, and named composite types are captured as text, not resolved.
  (Variable resolution against the DECLARE-section namespace **is** done — see
  [Design principle 2](#2-statement-level-variable-resolution-but-no-catalog).)
- **Semantic validation that needs the catalog** — unknown condition names,
  unknown types, etc. are accepted, not rejected.
- **Sub-parsing embedded SQL.** `PLpgSQL_expr.Parsed` is left `nil`; turning
  fragment text into a SQL AST (and walking it for the Tier 1 analysis) is a
  separate planner-side concern.
