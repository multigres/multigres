# Chunk 1.1 — Grammar + build scaffolding

First chunk of Phase 1. Goal: land the build pipeline for the new PL/pgSQL
grammar so every subsequent chunk is "just add a production". Nothing
user-visible changes; the parser accepts empty input and returns an empty
function node.

## Scope

### In

- New file `go/common/parser/plpgsql.y` — a grammar that compiles under
  goyacc but accepts only the empty body. Declares the token set and
  start rule; body productions are stubs to be filled in by later chunks.
- New file `go/common/parser/plpgsql_generate.go` — `//go:generate` block
  that runs `goyacc -f -o plpgsql.go plpgsql.y` followed by `goimports` and
  `gofumpt`, matching `generate.go`.
- New file `go/common/parser/plpgsql.go` — the generated parser (committed,
  like `postgres.go` already is).
- New file `go/common/parser/plpgsql_api.go` — the public surface:
  `ParsePLpgSQL(body string) (*plpgsqlast.PLpgSQL_function, error)`. Initially
  just instantiates the lexer, runs the parser, returns the root node or the
  collected errors. If the body is empty or whitespace-only, returns an
  empty `PLpgSQL_function`.
- Minimal AST additions in the new `plpgsqlast` subpackage: the node system
  (`Node`/`NodeTag`/`BaseNode`) plus `PLpgSQL_function` with an `Actions` field
  for the (not-yet-parsed) top block. Implements `plpgsqlast.Node`. No
  `asthelpergen` wiring yet — clone/rewrite generation is its own work item
  (chunk 1.3a); package `ast` is not touched.
- `Makefile` `parser` target (or whatever currently drives `make parser`) —
  extend so it triggers `go generate` for both grammar files. Verify the
  target is idempotent.
- One smoke test in a new `go/common/parser/plpgsql_api_test.go`:
  `ParsePLpgSQL("")` returns non-nil, no error, zero Actions.

### Out (explicitly deferred to later chunks)

- Real statement productions — BLOCK, DECLARE, IF, LOOP, etc. All grammar
  rules in 1.1 are stubs.
- The PL/pgSQL keyword table and identifier reclassification — Chunk 1.2.
- The `read_sql_construct` equivalent — Chunk 1.6.
- Any integration with `DoStmt` / `CreateFunctionStmt` — Phase 2.
- Dollar-quote body extraction — callers pass already-extracted body text.
  Trivial to add later; not needed for the scaffolding.

## Implementation notes

### Grammar file shape

`plpgsql.y` should mirror the overall shape of PG's `pl_gram.y` so future
chunks can translate productions 1:1. At this chunk's state, the grammar
is:

```text
%union { /* enough for future chunks to extend without churn */ }

%token <...> /* the tokens that the shared SQL lexer already emits,
               plus placeholder tokens for PL/pgSQL keywords — full
               keyword wiring is Chunk 1.2 */

%type <function> pl_function
%start pl_function

%%

pl_function:
    /* empty */         { setResult(yylex, plpgsqlast.NewPLpgSQL_function()) }
    ;
```

The `setResult` helper pattern follows whatever `postgres.y` already uses
to attach the parse result to the lexer context. Copy that pattern rather
than inventing a new one.

### goyacc invocation

Match `generate.go` exactly. The relevant directive in
`plpgsql_generate.go`:

```go
//go:generate go run ./goyacc -f -o plpgsql.go plpgsql.y
//go:generate go tool goimports -w plpgsql.go
//go:generate go tool gofumpt -w plpgsql.go
```

If the existing `generate.go` uses a slightly different flag set, copy
that. The goal is zero friction in `make parser`.

### Lexer instantiation for the new entry point

For now, `ParsePLpgSQL` reuses `NewLexer(input)` from `lexer.go` directly.
Chunk 1.2 replaces that call with a PL/pgSQL-aware wrapper. Keeping the
same interface now means 1.2 is a drop-in swap.

### AST regeneration

`PLpgSQL_function` is hand-written in the `plpgsqlast` subpackage and needs no
generation in this chunk:

```text
make parser    # regenerates plpgsql.go only
```

Package `ast`'s generated files are untouched (the node is not an `ast.Node`).
The `plpgsqlast` subpackage gets its own clone/rewrite generation later, in
chunk 1.3a.

### Makefile

Find the current `parser` target (driven by `go generate ./go/common/parser/...`
per the exploration report). Confirm it already picks up
`plpgsql_generate.go` automatically via `go generate`. If not, add the
file explicitly. No new top-level target.

## Files touched

**New:**

- `go/common/parser/plpgsql/plpgsql.y`
- `go/common/parser/plpgsql/generate.go`
- `go/common/parser/plpgsql/plpgsql.go` (generated)
- `go/common/parser/plpgsql/api.go`
- `go/common/parser/plpgsql/api_test.go`
- `go/common/parser/ast/plpgsqlast/nodes.go`
- `go/common/parser/ast/plpgsqlast/plpgsql_function.go`

**Regenerated:**

- none in package `ast` — it is untouched.

**Modified (only if needed):**

- `Makefile` — only if the existing `parser` target doesn't pick up the new
  generate directive automatically.

## Acceptance

- `make parser` succeeds on a clean checkout, regenerates `plpgsql.go` and
  AST helpers deterministically.
- `go build ./...` green.
- `/mt-dev unit ./go/common/parser/...` green; the new smoke test
  (`ParsePLpgSQL("")` returns non-nil function with zero Actions) passes.
- No lint regressions (`golangci-lint` clean).
- Diff sized so a human reviewer can read it end-to-end in one sitting:
  hand-written lines (grammar + API + AST + test) should be small; the
  bulk is generator output.

## Decisions made during execution

- **Commit the generated `plpgsql.go`** — matches `postgres.go` precedent.
  `y.output` is gitignored (added `go/common/parser/plpgsql/y.output` to
  `.gitignore`).
- **Reuse `%union` shape, not contents** — PL/pgSQL semantic values are a
  different vocabulary (no need for SQL-only types like `jexpr`, `indexElem`,
  etc.). The chunk lands with a single `function *plpgsqlast.PLpgSQL_function`
  union member; later chunks extend it as grammar rules land.
- **Sub-package `go/common/parser/plpgsql/`** — chosen over a flat layout so
  goyacc's generated package-level symbols (token constants from `%token`
  declarations) don't collide with the SQL parser's. Many PL/pgSQL reserved
  keywords (`LOOP`, `RAISE`, `EXECUTE`, `CASE`, `WHEN`, …) would have
  clashed. The `-p plpgsql` prefix covers everything else (`plpgsqlParser`,
  `plpgsqlSymType`, `plpgsqllex`).
- **PL/pgSQL AST as a separate hierarchy in `go/common/parser/ast/plpgsqlast/`**
  — the nodes are NOT `ast.Node`; `plpgsqlast` has its own `Node`, `NodeTag`,
  and `BaseNode`. Keeping the ~49-type vocabulary out of package `ast` avoids
  bloating its 17k-line generated clone/rewrite files and 842-entry `NodeTag`
  enum, and matches PostgreSQL (PLpgSQL_stmt/\_expr are a distinct hierarchy).
  An `ast.Node`-in-a-subpackage variant was rejected: package `ast`'s central
  `CloneNode`/`rewriteNode` switches would have to import the subpackage to
  dispatch to it → import cycle, with the nodes silently falling into the
  `default:` case (clone returns nil, rewrite won't descend). The subpackage
  gets its own asthelpergen run instead (tracked as chunk 1.3a).
- **File name `plpgsql.y`** — user was indifferent.

## What landed

- `go/common/parser/ast/plpgsqlast/nodes.go` — the `plpgsqlast` node system:
  own `Node` interface, `NodeTag` type + enum, `BaseNode`.
- `go/common/parser/ast/plpgsqlast/plpgsql_function.go` — `PLpgSQL_function`
  AST root (embeds `plpgsqlast.BaseNode`).
- Package `ast` is left untouched — no new NodeTag, no clone/rewrite deltas.
- `go/common/parser/plpgsql/plpgsql.y` — empty grammar, single
  `pl_function` start rule, building a `*plpgsqlast.PLpgSQL_function`.
- `go/common/parser/plpgsql/generate.go` — `//go:generate` directives.
- `go/common/parser/plpgsql/plpgsql.go` — generated, committed.
- `go/common/parser/plpgsql/lexer.go` — minimal `lexer` struct; `Lex`
  returns EOF immediately (chunk 1.2 replaces with real tokenization).
- `go/common/parser/plpgsql/api.go` — public `ParsePLpgSQL(body string)`
  entry point.
- `go/common/parser/plpgsql/api_test.go` — smoke test for empty body.
- `.gitignore` — added `go/common/parser/plpgsql/y.output`.

## Verification run

- `make parser` — succeeds, output is deterministic across reruns (MD5s
  of `plpgsql.go` and `postgres.go` match on repeat invocation); package
  `ast`'s generated files are unchanged from upstream.
- `go build ./...` — green.
- `go test ./go/common/parser/...` — all parser + new plpgsql package
  tests pass.
