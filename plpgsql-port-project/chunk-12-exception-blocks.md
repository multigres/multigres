# Chunk 1.12 — Exception blocks

The `EXCEPTION WHEN … THEN …` handler section of a block. This is where the
`PLpgSQL_exception_block` placeholder from 1.3 finally gets its body, and where
`PLpgSQL_exception` and `PLpgSQL_condition` are defined.

Ported from `pl_gram.y` (`pl_block`, `exception_sect`, `proc_exceptions`,
`proc_exception`, `proc_conditions`, `proc_condition`) and `plpgsql.h`
(`PLpgSQL_exception_block`, `PLpgSQL_exception`, `PLpgSQL_condition`).

## Grammar

`pl_block` grows the `exception_sect` slot PG has always had (1.4 omitted it):

```text
pl_block: decl_sect K_BEGIN proc_sect exception_sect K_END opt_label
```

New productions:

```text
exception_sect: /*empty*/ { nil } | K_EXCEPTION proc_exceptions
proc_exceptions: proc_exceptions proc_exception | proc_exception
proc_exception:  K_WHEN proc_conditions K_THEN proc_sect
proc_conditions: proc_conditions K_OR proc_condition | proc_condition
proc_condition:  any_identifier   { named condition, or SQLSTATE 'xxxxx' }
```

PG uses a mid-rule action in `exception_sect` to add the implicit `sqlstate` /
`sqlerrm` variables to the namespace — **dropped** (no namespace). So our
`exception_sect` collapses to a single action.

The OR-list appends (`proc_exceptions`, `proc_conditions`) go through
`appendException` / `appendCondition` helpers — the goyacc fast-append hazard,
same as `appendElsif`.

## The `SQLSTATE 'xxxxx'` wrinkle in `proc_condition`

`proc_condition` is just `any_identifier`, but PG special-cases the identifier
`sqlstate`: it then reads a following `SCONST` via `yylex()` inside the action
and validates the 5-char/charset code (same check as RAISE, `validSQLState`).
A normal identifier is a **condition name**, captured as text — PG's
`plpgsql_parse_err_condition` (which resolves the name to a SQLSTATE) is
**dropped** (resolution).

The mid-action token read uses the standard `beginScan` dance
(`decl_cursor_query`-style): it is robust to whether goyacc read the `SCONST` as
a lookahead (push it back) or reduced `proc_condition` by default (scan fresh) —
PG relies on the default reduction; the dance covers both. `readSQLStateCondition`
does the scan + validation.

## AST

- `PLpgSQL_condition` — `Condname string` only. PG's `sqlerrstate` (int) is
  resolution → dropped; PG's `next` linked list → we use a slice on the
  exception. No form flag: a SQLSTATE code (five `[0-9A-Z]`) and a lowercased
  condition name occupy disjoint string-spaces, so the deparse recovers the
  `SQLSTATE 'xxxxx'` vs bare-name form from `Condname` via the shared
  `IsSQLStateCode` predicate (also the parse-time validator).
- `PLpgSQL_exception` — `Conditions []*PLpgSQL_condition` (the OR-list),
  `Action []Stmt`. Node, not Stmt (a helper like `PLpgSQL_case_when`).
- `PLpgSQL_exception_block` — gains `ExcList []*PLpgSQL_exception` (fleshing out
  the 1.3 placeholder). PG's `sqlstate_varno` / `sqlerrm_varno` (namespace) →
  dropped.

`PLpgSQL_stmt_block.SqlString` now renders `Exceptions` between the body and END.

## Deparse

```text
BEGIN
  <body>
EXCEPTION
WHEN <cond>[ OR <cond>…] THEN
  <action>
[WHEN … THEN …]
END
```

`<cond>` is a bare condition name, or `SQLSTATE '<code>'` when `IsSqlState`.

## Scope — out

- Namespace/implicit vars (`sqlstate`, `sqlerrm`), condition-name resolution.
- `PLpgSQL_expr.Parsed` still nil.

## Acceptance

New `testdata/exception_cases.json`, golden deparse via `PLPGSQL_REWRITE=1`:

- single handler `BEGIN … EXCEPTION WHEN others THEN … END`.
- OR-list `WHEN no_data_found OR too_many_rows THEN`.
- multiple WHEN clauses.
- `WHEN SQLSTATE '22012' THEN`; invalid-length / invalid-charset → error.
- handler with a body of statements; nested block with its own EXCEPTION.
- existing `block_cases.json` (no EXCEPTION) still pass unchanged.
- Round-trip stable; `make parser` 0 new conflicts; `go build` +
  `golangci-lint` clean; package `ast` byte-identical to upstream.
