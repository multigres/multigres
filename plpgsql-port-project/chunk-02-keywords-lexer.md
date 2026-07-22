# Chunk 1.2 — PL/pgSQL keyword table + lexer wrapper

Second chunk of Phase 1. Goal: turn a PL/pgSQL body into the correct PL/pgSQL
**token stream**, by wrapping the existing SQL lexer and reclassifying words
against PL/pgSQL's own keyword tables. Ported from
`postgres/src/pl/plpgsql/src/pl_scanner.c` + `pl_reserved_kwlist.h` +
`pl_unreserved_kwlist.h`. No real grammar productions yet (still chunk 1.4+);
this chunk is verified by asserting the token stream directly.

## Key difference from PostgreSQL (read this first)

In PG, the core scanner is handed the PL/pgSQL **reserved** keyword list at
`scanner_init`, so it recognizes those (and core SQL keywords) and returns
plain `IDENT` for everything else. `pl_scanner.c` then only has to (a) look up
variables and (b) check the **unreserved** list.

Our `go/common/parser/lexer.go` is different: it already classifies **all** SQL
keywords itself and returns a distinct token (e.g. `begin` → the SQL `BEGIN`
token, not `IDENT`). It knows nothing about PL/pgSQL keywords.

Consequence: our wrapper reclassifies by the token's lowercased **text**, not
by trusting the SQL lexer's token code. For every word-like token we re-derive
the PL/pgSQL token from the PL/pgSQL tables, overriding whatever the SQL lexer
called it. Non-word tokens (numbers, strings, operators, punctuation) pass
through unchanged (with the small operator fix-ups below).

## Scope

### In

- New `go/common/parser/plpgsql/keywords.go` — two ordered tables mirroring the
  PG headers exactly:
  - `reservedKeywords`: 24 entries (`all, begin, by, case, declare, else, end,
execute, for, foreach, from, if, in, into, loop, not, null, or, strict,
then, to, using, when, while`).
  - `unreservedKeywords`: the ~83 tokens from `pl_unreserved_kwlist.h` (note
    `elseif` and `elsif` are two spellings of the same `K_ELSIF` token).
  - Each maps a lowercase name → the PL/pgSQL token code. Lookups are
    case-insensitive; keep them as `map[string]int` built once.
- Token codes come from `plpgsql.y` `%token` declarations (generated into
  `plpgsql.go`), so the tables and the eventual grammar share one definition.
  This chunk adds the `%token` block (all `K_*` keyword tokens, plus
  `T_WORD T_CWORD T_DATUM IDENT SCONST ICONST FCONST PARAM LESS_LESS
GREATER_GREATER` and friends). Grammar **productions stay as-is** (the single
  empty `pl_function` rule) — only the token vocabulary grows.
- Rewrite `go/common/parser/plpgsql/lexer.go` from the EOF stub into a real
  wrapper around `parser.NewLexer(body)`:
  - `internalLex()` — calls the SQL lexer's `NextToken()`, applies a pushback
    stack (`MAX_PUSHBACKS = 4`, matching PG), and fixes up the operators the
    PL/pgSQL grammar names specially: `Op "<<"` → `LESS_LESS`, `Op ">>"` →
    `GREATER_GREATER`, `Op "#"` → `'#'`. (`:=`/`::`/`..` already arrive as named
    tokens `COLON_EQUALS`/`TYPECAST`/`DOT_DOT` from our lexer — no fix-up
    needed, unlike PG.)
  - `Lex()` (the entry the goyacc parser calls) — the `plpgsql_yylex` logic:
    1. Pull a token. If it is **word-like** (an `IDENT`, or any token whose
       text is an identifier word the SQL lexer classified as a keyword) and
       **not** a quoted identifier, reclassify it:
       - reserved table hit → the reserved `K_*` token;
       - else do compound-word lookahead `A.B` / `A.B.C` (peek `.` + word),
         producing `T_CWORD` for a multi-part name;
       - else single word: unreserved table hit → that `K_*` token, otherwise
         `T_WORD`.
    2. Quoted identifiers and everything else pass straight through.
  - Carry the token's byte offset (`Token.Position`) into the symbol value so
    later chunks get locations for free.

### Out (explicitly deferred)

- **Variable resolution / `T_DATUM`.** PG's `plpgsql_parse_word/dblword/tripword`
  resolve identifiers against the compile-time namespace and return `T_DATUM`.
  We have no namespace yet, so words become `T_WORD`/`T_CWORD`, never `T_DATUM`.
  Variable resolution is chunk **1.14** (parser-setup hooks). The `T_DATUM`
  token is declared now so the grammar/tables are stable, but never emitted.
- **`IdentifierLookup` state (NORMAL/DECLARE/NONE)** and the **`AT_STMT_START`**
  "prefer unreserved keyword over variable name" heuristic. Both exist only to
  steer variable lookup; with no variable lookup they change nothing. Add the
  `IdentifierLookup` field as a stub (default NORMAL) for later, but the
  state machine and the heuristic land with 1.14.
- **Grammar productions.** Still just the empty rule; consuming these tokens is
  chunks 1.4+.
- **SQL-fragment scanning** (`read_sql_construct`) — chunk 1.6. See the note on
  the `end`-as-column case below.

## Implementation notes

### "end as a column name" — what the acceptance really tests

At the PL/pgSQL lexer level, `end` is reserved and **always** lexes as `K_END`;
there is no context in which the PL/pgSQL scanner returns it as an identifier.
The case where `end` is a column name (`SELECT end FROM t`) only works because
that SQL lives inside a fragment that `read_sql_construct` slurps as raw text
and hands to the SQL parser — the PL/pgSQL lexer never tokenizes it word by
word. So this chunk's job is the opposite of what it sounds: confirm the
PL/pgSQL lexer **does** reserve `end`/`begin`/etc. unconditionally; the
"identifier" behavior is the fragment boundary's responsibility (1.6), and the
test here documents that boundary rather than implementing it.

### Word-like detection

A token is reclassifiable iff it is an unquoted identifier word. Our SQL lexer
gives us `Token.Type == IDENT` for unquoted non-keyword words and a distinct
keyword token for SQL keywords; both carry the original text in `Token.Text`.
Treat a token as word-like when `Type == IDENT` or `LookupKeyword(Token.Text)`
matched (i.e. the SQL lexer saw a keyword). Quoted/delimited identifiers
(case-preserved) are never keyword-reclassified — mirror PG's `!word.quoted`.

### Pushback stack

Mirror PG: a fixed `[4]` stack of (token, auxdata). `internalLex` pops from it
before calling the SQL lexer. The compound-word lookahead pushes back the
tokens it consumed but didn't use (the `.`-less or `A.B`-only cases). Our SQL
lexer also supports byte-level `PutBack`, but a token-level stack in the wrapper
is simpler and matches the source 1:1.

## Files touched

**New:**

- `go/common/parser/plpgsql/keywords.go`
- `go/common/parser/plpgsql/keywords_test.go` (table integrity: no name in both
  lists; every unreserved name resolves; counts match the PG headers)
- `go/common/parser/plpgsql/lexer_test.go` (token-stream assertions)

**Modified:**

- `go/common/parser/plpgsql/plpgsql.y` — add the `%token` vocabulary; regenerate
  `plpgsql.go`.
- `go/common/parser/plpgsql/lexer.go` — stub → real wrapper.

## Acceptance

- `make parser` regenerates `plpgsql.go` with the new token constants; output
  deterministic; package `ast` untouched.
- `go build ./...` green; `golangci-lint` clean.
- Token-stream tests pass:
  - `BEGIN RAISE NOTICE 'hi'; END` →
    `K_BEGIN K_RAISE K_NOTICE SCONST ';' K_END`.
  - Reserved vs. unreserved: `IF`, `LOOP`, `WHILE` → reserved tokens; `RAISE`,
    `PERFORM`, `EXCEPTION` → unreserved tokens.
  - `x := 1` → `T_WORD COLON_EQUALS ICONST`.
  - Compound word `a.b.c` → `T_CWORD` (single token spanning all three parts);
    `a.b` → `T_CWORD`; bare `a` → `T_WORD`.
  - Quoted `"End"` → `T_WORD` (not `K_END`), case preserved.
  - Block label operators `<<` / `>>` → `LESS_LESS` / `GREATER_GREATER`.
- A table test asserts no keyword appears in both the reserved and unreserved
  maps, guarding the PG invariant.

## Risks / things to verify during execution

- **goyacc unused tokens.** We declare ~110 `%token`s that no production uses
  yet. Confirm goyacc emits the constants without erroring on unused terminals
  (expected fine — yacc warns at most). If it balks, we add a throwaway
  `unused_tokens` rule guarded out, or declare incrementally per later chunk.
- **SQL-keyword reclassification.** Because our lexer pre-classifies SQL
  keywords, double-check words that are BOTH a SQL keyword and a PL/pgSQL
  keyword (`begin end case when from into not null or all in to using then
else`) reclassify to the PL/pgSQL token, and that a SQL keyword that is NOT a
  PL/pgSQL keyword (e.g. `select`) becomes `T_WORD` here (it will be handled as
  the start of an SQL statement by `stmt_execsql` later, via the 1.6 boundary).
- **Float/numeric tokens.** Verify our lexer's `FCONST`/`ICONST`/typecast
  interplay matches what the grammar will expect; adjust the `%token` list if a
  name differs.
- **Location fidelity.** Compound-word tokens must report the offset of the
  first part (PG adjusts `leng` to span `A.B.C`); replicate so deparse/error
  positions stay correct.
