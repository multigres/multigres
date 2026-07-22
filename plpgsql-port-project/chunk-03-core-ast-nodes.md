# Chunk 1.3 — Core AST nodes

Third chunk of Phase 1. Goal: hand-write the small set of **foundational**
PL/pgSQL AST nodes that everything else builds on, in the `plpgsqlast`
subpackage. No grammar changes and no generator wiring — this chunk is pure
type definitions plus their `plpgsqlast.Node` plumbing. Ported (parse-level
subset only) from `postgres/src/pl/plpgsql/src/plpgsql.h`.

## Guiding decision: parse-level subset only

PG's `plpgsql.h` structs are fat with execution-engine state — `SPIPlanPtr`,
`ExprState`, `CachedPlan`, `Bitmapset` of datum numbers, `stmtid`, expanded-record
caching, etc. We are a **static analyzer**, not an executor: we parse a body,
walk it, and apply the Tier-1 policy. So every node keeps only the fields needed
to represent and walk the parse tree. Concretely that drops ~80% of each PG
struct.

## Proposed scope (tighter than the original phase-1 sketch)

The phase-1 outline lumped the datum family and exception scaffolding into 1.3
as stubs. I propose **not** doing that. The ordering principle elsewhere in the
plan is "each chunk adds a statement family plus its matching node," so
speculative stubs here would just be rewritten when their grammar lands (DECLARE
in 1.5, EXCEPTION in 1.12). Chunk 1.3 should be only the genuinely **shared**
foundation that multiple later chunks reference:

### In

1. **`Stmt` marker interface** — `type Stmt interface { Node; isStmt() }` with
   an unexported marker method, so only `plpgsqlast` types can be statements and
   a block's body can be `[]Stmt` rather than `[]Node`. Mirrors PG's
   `PLpgSQL_stmt` supertype.

2. **`PLpgSQL_expr`** — the embedded-SQL carrier and the single most important
   node for this project. Parse-level fields only:
   - `Query string` — the verbatim SQL fragment text (PG's `expr->query`).
   - `ParseMode RawParseMode` — mirrors PG's `expr->parseMode`. A new enum
     (values/order matching PG's `RawParseMode` in `parser.h`:
     `RAW_PARSE_DEFAULT`, `RAW_PARSE_TYPE_NAME`, `RAW_PARSE_PLPGSQL_EXPR`,
     `RAW_PARSE_PLPGSQL_ASSIGN1..3`). Records what kind of fragment this is so
     1.6 knows how to parse it; defaults to `RAW_PARSE_DEFAULT`.
   - `Parsed ast.Stmt` — the parsed SQL AST. **No PG parse-time equivalent**:
     PG stores only the text and parses lazily at execution via SPI (its `plan`
     field), discarding the compile-time validation parse. We parse eagerly
     because the gateway analyzes statically and never executes. This is the
     **one place** `plpgsqlast` imports `go/common/parser/ast`; the dependency
     is one-way (`plpgsqlast` → `ast`), so no cycle. The Tier-1 walker hands
     this field to `analyzeStatement`.
   - `SqlString()` returns `Query` (the verbatim text — a meaningful deparse).
   - Deferred to chunk 1.6: how a bare expression fragment
     (`ParseMode == RAW_PARSE_PLPGSQL_EXPR`) becomes an `ast.Stmt` — SELECT-wrap
     (pragmatic) vs. an expression parse mode in the shared SQL parser
     (faithful to PG). `Parsed` is typed `ast.Stmt` either way.

3. **`PLpgSQL_stmt_block`** — the `BEGIN … END` container:
   - `Label string` (optional block label).
   - `Body []Stmt`.
   - `Exceptions *PLpgSQL_exception_block` — typed as a forward-declared
     placeholder for now (the real `PLpgSQL_exception_block` node lands in 1.12);
     defined here as an empty struct so the field type exists, or left as a
     `nil`-only `any`/placeholder. Proposal: define a minimal empty
     `PLpgSQL_exception_block` struct now and flesh it out in 1.12.
   - Drops PG's `n_initvars`/`initvarnos`/`stmtid` (execution bookkeeping).

4. **Reshape `PLpgSQL_function`** — today (scaffolding) it has `Actions []Node`.
   PG models the function body as a single top-level block (`function->action`
   is a `PLpgSQL_stmt_block`). Change the field to
   `Action *PLpgSQL_stmt_block` to match. For the current empty-body parse the
   field is simply `nil`; chunk 1.4 sets it. Updates `api_test.go`
   (`fn.Action` nil instead of `fn.Actions` empty).

### Out (deferred to the chunk that introduces the matching grammar)

- `PLpgSQL_stmt_null` — chunk 1.4 (first real block parsing).
- Datum family `PLpgSQL_var` / `_row` / `_rec` / `_recfield` and `PLpgSQL_type`
  — chunk 1.5 (DECLARE), where their real fields are exercised.
- `PLpgSQL_condition` and the full `PLpgSQL_exception` / `_exception_block`
  bodies — chunk 1.12 (exception blocks).
- Any per-statement nodes (`stmt_if`, `stmt_loop`, …) — their own chunks.
- `asthelpergen` clone/rewrite wiring — chunk 1.3a.

## Node tags

Add `T_PLpgSQL_stmt_block` and `T_PLpgSQL_expr` to the `plpgsqlast` NodeTag enum
and its `String()` switch. (`T_PLpgSQL_function` already exists.)
`PLpgSQL_exception_block` gets a tag too if we define the empty struct now.

## Files touched

**New:**

- `go/common/parser/ast/plpgsqlast/statements.go` — `Stmt` interface,
  `PLpgSQL_stmt_block` (and the placeholder `PLpgSQL_exception_block`).
- `go/common/parser/ast/plpgsqlast/expr.go` — `PLpgSQL_expr`.
- Test(s): `plpgsqlast` constructor/round-trip smoke tests if useful.

**Modified:**

- `go/common/parser/ast/plpgsqlast/nodes.go` — new NodeTags + `String()` cases.
- `go/common/parser/ast/plpgsqlast/plpgsql_function.go` — reshape to
  `Action *PLpgSQL_stmt_block`.
- `go/common/parser/plpgsql/...` — only if the function reshape touches the
  grammar action or `api_test.go`.

## Acceptance

- `go build ./...` green; package `ast` untouched.
- `golangci-lint` clean.
- The nodes implement `plpgsqlast.Node` (and statements implement `Stmt`); a
  small test constructs a block containing a `PLpgSQL_expr` and asserts tags +
  `SqlString()`.
- No new parser behavior: `ParsePLpgSQL("")` still returns a function with a
  nil `Action`.

## Risks / things to verify

- **`PLpgSQL_expr.Parsed` type.** `ast.Stmt` vs `ast.Node` (see open question
  above). Picking `ast.Stmt` commits chunk 1.6 to wrapping bare expressions in
  `SELECT`; that matches PG's approach and the walker's `analyzeStatement`
  signature, but confirm before we hard-code it.
- **`Exceptions` placeholder.** Defining an empty `PLpgSQL_exception_block` now
  vs. typing the field as something we backfill in 1.12. The empty-struct route
  keeps the field type stable; the alternative avoids a half-defined node.
- **Function reshape ripple.** `Action *PLpgSQL_stmt_block` changes the one
  existing test and the eventual grammar action; trivial now, messier later, so
  do it in this chunk.
