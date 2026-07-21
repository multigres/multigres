# Chunk 1.10a — Dynamic EXECUTE (EXECUTE / FOR-IN-EXECUTE / RETURN QUERY EXECUTE)

The dynamic-SQL family — the direct target of the Tier-1 dynamic-EXECUTE policy
(reject non-literal `EXECUTE`). Self-contained and resolution-free: it captures
the query and USING expressions cleanly, which is exactly what the walker needs.

Ported from `pl_gram.y` (`stmt_dynexecute`, the `K_EXECUTE` branch of
`for_control`, `make_return_query_stmt`) and `plpgsql.h`.

## Proposed split of 1.10

Phase-1 chunk 1.10 as planned bundles dynamic EXECUTE **and** cursors
(OPEN/FETCH/MOVE/CLOSE + cursor declaration). They're separable, and cursors are
much more entangled with resolution (`cursor_variable: T_DATUM`,
`cursor_explicit_expr`). Proposal:

- **1.10a (this doc)** — dynamic EXECUTE: `stmt_dynexecute`, dynamic FOR
  (`FOR … IN EXECUTE`), and `RETURN QUERY EXECUTE` (the 1.9 deferral). High Tier-1
  value, no cursor/resolution mess.
- **1.10b (next)** — cursors: `decl_cursor`, `stmt_open`, `stmt_fetch`,
  `stmt_move`, `stmt_close`, `cursor_variable` (as `T_WORD`).

## Statements

### `EXECUTE expr [INTO [STRICT] target] [USING arg, …]`

`stmt_dynexecute: K_EXECUTE { makeDynExecute() }`. `makeDynExecute` mirrors PG's
action: scan the query expr up to `INTO`/`USING`/`;`, then loop — `INTO` and
`USING` may appear in either order — reading an INTO target and/or a USING
expression list until `;`.

Node `PLpgSQL_stmt_dynexecute` — `Query *PLpgSQL_expr`, `Into bool`,
`Strict bool`, `Target string` (the INTO target text; PG resolves it to variables,
we capture names), `Params []*PLpgSQL_expr` (USING expressions), and
`UsingFirst bool` — records whether `USING` preceded `INTO` in the source, so the
deparse re-emits them in the **original order** (only `stmt_dynexecute` has both
clauses; dynfors / return-query-execute have USING only).

### `FOR var IN EXECUTE expr [USING arg, …] LOOP … END LOOP`

This is the `K_EXECUTE` branch of `for_control` that 1.8 deferred. After
`for_variable K_IN`, `readForControl` peeks for `K_EXECUTE`; if present it builds
a dynamic FOR: scan the query up to `LOOP`/`USING`, then a USING list up to
`LOOP`.

Node `PLpgSQL_stmt_dynfors` — `Label string`, `Var string`, `Query
*PLpgSQL_expr`, `Params []*PLpgSQL_expr`, `Body []Stmt`. `stmt_for`'s type-switch
gains a `*PLpgSQL_stmt_dynfors` case (sets Label/Body like fori/fors).

### `RETURN QUERY EXECUTE expr [USING arg, …]`

Finishes 1.9's deferral. In `makeReturnStmt`'s `K_QUERY` branch, peek for
`K_EXECUTE`: if present, read the dynamic query (up to `USING`/`;`) plus a USING
list into new fields; otherwise the static `Query` path as today.

Extend `PLpgSQL_stmt_return_query` with `DynQuery *PLpgSQL_expr` and `Params
[]*PLpgSQL_expr` (mirroring PG). `Query` xor `DynQuery` is set; deparse emits
`RETURN QUERY <query>` or `RETURN QUERY EXECUTE <dynquery> [USING …]`.

## Scanner additions

- `makeDynExecute()` — the INTO/USING either-order loop.
- `readIntoTarget(terminators…) (strict bool, target string, endtoken int)` —
  optional `STRICT`, then capture target text up to a terminator. Shared shape
  with what 1.10b's FETCH will need.
- `readUsingList(terminators…) ([]*PLpgSQL_expr, int)` — comma-separated USING
  expressions, returning the terminator that ended the list. Reused by
  dynexecute, dynfors, return-query-execute, and 1.10b's OPEN.
- The `Params` appends live in Go (not grammar actions), so the goyacc
  fast-append hazard does not apply — plain `append` is fine here.

## Deparse

```text
EXECUTE <query>[ INTO[ STRICT] <target>][ USING <p1>, <p2>]
[<<lbl>> ]FOR <var> IN EXECUTE <query>[ USING …] LOOP … END LOOP[ lbl]
RETURN QUERY EXECUTE <dynquery>[ USING …]
```

INTO/USING are re-emitted in their **source order** via `UsingFirst`, so
`EXECUTE q USING a INTO x` round-trips byte-faithfully rather than being
canonicalised.

## Scope — out (deferred)

- All cursor statements and `decl_cursor` — 1.10b.
- INTO target resolution (we keep target text).
- `PLpgSQL_expr.Parsed` still nil.

## Conflict / risk

- `stmt_dynexecute`/`stmt_return`/`for_control` all start from a single keyword
  and do their work in the action, so no new grammar ambiguity — verify
  `y.output` still 0 conflicts.
- The either-order INTO/USING loop is the fiddly part — test `EXECUTE q INTO x`,
  `EXECUTE q USING a, b`, `EXECUTE q INTO x USING a`, `EXECUTE q USING a INTO x`,
  `INTO STRICT`.
- `for_control` now has three shapes (integer / query / dynamic) selected by the
  `..`-scan and the `EXECUTE` peek — make sure the peek is consumed/pushed-back
  correctly and the non-EXECUTE paths are unchanged (1.8 cases still pass).

## Acceptance

New `testdata/dynexecute_cases.json` (+ additions to `for_cases.json` and
`return_cases.json`), golden deparse via `PLPGSQL_REWRITE=1`:

- `EXECUTE 'SELECT 1'`; with `INTO x`; `INTO STRICT r`; `USING a, b`; both orders.
- `FOR r IN EXECUTE q LOOP …`; with `USING`.
- `RETURN QUERY EXECUTE q`; with `USING`.
- Existing 1.8 FOR cases and 1.9 RETURN cases still pass unchanged.
- Round-trip stable; `make parser` 0 new conflicts; `go build` +
  `golangci-lint` clean; package `ast` byte-identical to upstream.
