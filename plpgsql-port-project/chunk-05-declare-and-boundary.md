# Chunk 1.5 — DECLARE + the SQL-fragment boundary (folded)

The biggest, trickiest chunk: it folds in the `read_sql_construct` /
`read_datatype` **scan-from-grammar-actions** machinery (the heart of the port)
because DECLARE is its first consumer. After this, the assignment chunk (`x :=
expr;`) is a thin add-on that reuses the same boundary.

Ported from `postgres/src/pl/plpgsql/src/pl_gram.y` (`decl_sect`,
`decl_statement`, `read_sql_construct`, `read_datatype`) and `plpgsql.h`
(`PLpgSQL_var`, `PLpgSQL_type`).

## Part A — the SQL-fragment boundary (the hard part)

PG scans embedded SQL/expression/type text by **looping `yylex()` from inside a
grammar action** until a terminator, then slicing the raw source. We replicate
that on our lexer.

### The scanner

A method on the concrete `*lexer` (reached from actions by type-asserting
`plpgsqllex`, the existing `plpgsqlResultSetter` idiom):

```text
readSQLConstruct(firstTok auxToken, terminators...) -> (rawText string, endTok int)
  parenLevel := 0; start := -1; end := -1
  tok := firstTok (or scan one if none)
  loop:
    if tok in terminators && parenLevel == 0: break
    if start < 0: start = tok.pos
    track '(' '[' depth up / ')' ']' down (error if < 0)
    if tok == EOF or stray ';': error "missing <terminator>"
    end = tok.pos + tok.len        // end+1 of last accepted token
    tok = internalLex()
  rawText = input[start:end]       // excludes the terminator + trailing ws
```

- The lexer must keep the original **`input string`** (re-add the field dropped
  in 1.2) to slice raw text by byte offset. Each token already carries its byte
  `pos`; we also need its length (track `tok.pos + len(tok.text)` — add a `len`
  to `auxToken`, fed from the SQL token's text).
- Built on the existing `internalLex` + pushback stack from chunk 1.2.

### Lookahead handling (the crux)

When the action runs, the parser may or may not have already read a lookahead
token (`plpgsqlrcvr.char`), depending on whether the reduction was a default
reduction. PG handles both: `read_datatype(int tok)` takes the lookahead and
does `if (tok == YYEMPTY) tok = yylex();`. We do the same:

- The action reads `plpgsqlrcvr.char` / `plpgsqlrcvr.lval`; if a lookahead is
  pending (`char >= 0`), it is the fragment's **first token** — reconstruct an
  `auxToken` from it (`tok`, `str`, `ival`, `pos`) and pass it to
  `readSQLConstruct`; then clear it (`plpgsqlrcvr.char = -1; plpgsqltoken = -1`
  — the `yyclearin` equivalent) so the parser re-reads after the action.
- If no lookahead (`char < 0`), `readSQLConstruct` scans its first token fresh.
- These few lines of `plpgsqlrcvr` poking are wrapped in a small helper
  (in a `.go` file, same package, so it can name the generated
  `plpgsqlParserImpl`) and called from the action, keeping the `.y` clean.

### Producing the fragment node

- **Default value expression** → `PLpgSQL_expr{Query: rawText, ParseMode:
RAW_PARSE_PLPGSQL_EXPR}`. **Decision to make:** populate `Parsed` now
  (parse the fragment) or leave it `nil` for a later step?
  - Proposal: leave `Parsed = nil` in this chunk and capture only `Query`. The
    fragment-parsing half (turning `Query` into an `ast.Stmt` — the SELECT-wrap
    vs. expression-parse-mode decision) is a focused follow-up so this chunk
    stays about the scanner + DECLARE. The walker work is Phase 2 anyway.
- **Datatype** → for static analysis we do **not** resolve types or handle
  `%TYPE`/`%ROWTYPE` specially; we just capture the type text. So
  `read_datatype` collapses to "scan until a decl terminator (`;`,
  `COLON_EQUALS`, `K_DEFAULT`, `K_NOT`, `K_COLLATE`), capture text" →
  `PLpgSQL_type{TypeName: rawText}`. (PG's `plpgsql_parse_wordtype` resolution
  is chunk 1.14.)

## Part B — datum nodes (`plpgsqlast`)

- `Datum` marker interface (`Node` + `isDatum()`) — Go analogue of PG's
  `PLpgSQL_datum` supertype. Only `PLpgSQL_var` implements it for now;
  `_row`/`_rec`/`_recfield` arrive with variable resolution (1.14).
- `PLpgSQL_var` — parse-level subset of PG's struct: `Refname string`,
  `IsConst bool`, `NotNull bool`, `DataType *PLpgSQL_type`, `DefaultVal
*PLpgSQL_expr`. (Drops `dno`, cursor/promise/resolution fields.)
- `PLpgSQL_type` — `TypeName string` (the captured text). (Drops `typoid`,
  `ttype`, tupdesc, etc. — all resolution.)
- New NodeTags + `String()`/`SqlString()`; regenerate `plpgsqlast` clone/rewrite
  via `make parser`. The `DefaultVal` expr is reachable by the walker through
  `PLpgSQL_var`.

## Part C — block reshape + DECLARE grammar

- Reshape `pl_block` to PG's shape: `decl_sect K_BEGIN proc_sect K_END
opt_label` (replacing chunk 1.4's inlined `opt_block_label`). `decl_sect`
  carries the label **and** the declared variables onto the block.
  - `decl_sect: opt_block_label | opt_block_label K_DECLARE decl_stmts`.
  - `decl_stmts: decl_stmts decl_stmt | decl_stmt`.
  - `decl_statement: decl_varname decl_const decl_datatype decl_notnull
decl_defval` (PG also has `decl_collate`, ALIAS, and CURSOR forms — defer
    ALIAS/CURSOR; keep `decl_collate` optional/no-op for now).
  - `decl_const: /*empty*/ | K_CONSTANT`; `decl_notnull: /*empty*/ | K_NOT
K_NULL`; `decl_defval: ';' | (':='|'='|K_DEFAULT) <read expr ';'>`.
  - `decl_varname: any_identifier`.
- Add `block.Decls []plpgsqlast.Datum`.
- Reject NOT NULL without a default (PG does), as a parse error.

## Scope

### Out (deferred)

- Parsing fragment text into `ast.Stmt` (`PLpgSQL_expr.Parsed`) — focused
  follow-up (SELECT-wrap vs expr parse-mode).
- Assignment statement (`stmt_assign`) — the thin next chunk, reusing the
  boundary.
- ALIAS and CURSOR declarations; `%TYPE`/`%ROWTYPE` resolution; the
  `row`/`rec`/`recfield` datums — 1.14.
- `IDENTIFIER_LOOKUP_DECLARE`/`EXPR` state — only matters for variable
  resolution (1.14); we capture text regardless.

## Acceptance

- Parse `DECLARE i int; BEGIN END;` → block with one `PLpgSQL_var`
  (`Refname "i"`, `DataType.TypeName "int"`).
- Variants: `CONSTANT`, `NOT NULL` (+ default), `:=`/`=`/`DEFAULT` initializers,
  `varchar(10)`, `numeric(10,2)`, `foo.bar%TYPE`, `int[]` — assert `Refname`,
  flags, captured `TypeName`, and `DefaultVal.Query`.
- `DECLARE x int NOT NULL; BEGIN END;` → parse error (NOT NULL without default).
- Multiple decls; decls + body together.
- `make parser` deterministic, no goyacc conflicts; `go build`/`golangci-lint`
  clean; package `ast` untouched.

## Risks / things to verify

- **Lookahead correctness.** The single most error-prone part. Verify with
  tests that exercise both the lookahead-present and default-reduction paths
  (datatype after `decl_const`, default value after `:=`). A wrong
  `char`/`token` reset will drop or duplicate a token.
- **Raw-text offsets.** Off-by-one on `start`/`end` yields a truncated or
  comment-polluted `Query`/`TypeName`; test exact captured strings.
- **goyacc conflicts** from the new decl productions; resolve/document.
- **Chunk size.** This is large. If it gets unwieldy we can land Part A+B
  (boundary + nodes, exercised by a tiny test) before Part C (DECLARE grammar)
  in two commits on the branch — still one PR.
