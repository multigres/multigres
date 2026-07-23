# Chunk 1.13 — GET DIAGNOSTICS, COMMIT, ROLLBACK

The last statement productions, closing out Phase 1's grammar. `GET
[CURRENT|STACKED] DIAGNOSTICS target := item [, …]`, and bare `COMMIT` /
`ROLLBACK [AND [NO] CHAIN]`.

Ported from `pl_gram.y` (`stmt_getdiag`, `getdiag_area_opt`, `getdiag_list`,
`getdiag_list_item`, `getdiag_target`, `getdiag_item`, `stmt_commit`,
`stmt_rollback`, `opt_transaction_chain`) and `plpgsql.h`.

## Grammar

`proc_stmt` gains `stmt_getdiag`, `stmt_commit`, `stmt_rollback`.

```text
stmt_getdiag: K_GET getdiag_area_opt K_DIAGNOSTICS getdiag_list ';'
getdiag_area_opt: /*empty*/ {false} | K_CURRENT {false} | K_STACKED {true}
getdiag_list: getdiag_list ',' getdiag_list_item | getdiag_list_item
getdiag_list_item: getdiag_target assign_operator getdiag_item
getdiag_target: T_WORD | T_CWORD          // PG: T_DATUM (resolved var)
getdiag_item: /*empty*/ { readGetDiagItem() }   // reads the kind keyword
stmt_commit:   K_COMMIT   opt_transaction_chain ';'
stmt_rollback: K_ROLLBACK opt_transaction_chain ';'
opt_transaction_chain: K_AND K_CHAIN {true} | K_AND K_NO K_CHAIN {false} | /*empty*/ {false}
```

Two PG-mirroring wrinkles:

- **`getdiag_target`** is PG's `T_DATUM` (a resolved scalar variable); PG's `T_WORD`
  arm only exists to emit a nicer "not a variable" error. With no resolution we
  take the target as text via `T_WORD`/`T_CWORD`, same substitution as
  `stmt_assign` / FOR / INTO.
- **`getdiag_item`** is an empty production that reads the kind keyword itself via
  `yylex` (PG does the same, to run `tok_is_keyword` against a possible variable
  named `row_count`). We read it with the `beginScan` dance in `readGetDiagItem`,
  mapping the keyword token to the kind and erroring "unrecognized GET
  DIAGNOSTICS item" otherwise.

The `getdiag_list` append goes through an `appendDiagItem` helper (goyacc
fast-append hazard).

## STACKED / CURRENT validation (kept — syntactic)

PG rejects certain items by area, and so do we (no resolution needed):

- `ROW_COUNT`, `PG_ROUTINE_OID` — not allowed in `GET STACKED DIAGNOSTICS`.
- the error/context/name items (`PG_EXCEPTION_*`, `RETURNED_SQLSTATE`,
  `*_NAME`, `MESSAGE_TEXT`) — not allowed in `GET CURRENT DIAGNOSTICS`.
- `PG_CONTEXT` — allowed in both.

Same error strings as PG ("diagnostics item %s is not allowed in GET
STACKED/CURRENT DIAGNOSTICS"), with `%s` the kind's keyword name.

## AST

- `PLpgSQL_getdiag_kind` — typed int enum, values/order matching PG; `KindName()`
  gives the keyword spelling (PG's `plpgsql_getdiag_kindname`), used for both the
  deparse and the validation messages.
- `PLpgSQL_diag_item` — `Kind`, `Target string` (PG stores a resolved `dno`; we
  keep the target name as text). Node, not Stmt.
- `PLpgSQL_stmt_getdiag` — `IsStacked bool`, `DiagItems []*PLpgSQL_diag_item`.
- `PLpgSQL_stmt_commit` / `PLpgSQL_stmt_rollback` — `Chain bool`.

## Deparse

```text
GET [STACKED ]DIAGNOSTICS <target> := <KIND>[, <target> := <KIND>…]
COMMIT[ AND CHAIN]
ROLLBACK[ AND CHAIN]
```

`CURRENT` and the no-area form both mean not-stacked, so the deparse emits the
plain `GET DIAGNOSTICS`; `AND NO CHAIN` and the no-chain form both mean
`chain=false`, so the deparse omits the clause. Both collapse to their default,
faithfully (PG treats them identically) and round-trip stably.

## Scope — out

- Target resolution (kept as text); array-element targets (PG errors on `[`).
- `PLpgSQL_expr.Parsed` still nil.

## Acceptance

New `testdata/getdiag_cases.json` and `testdata/transaction_cases.json`, golden
deparse via `PLPGSQL_REWRITE=1`:

- `GET DIAGNOSTICS n := ROW_COUNT`; multi-item list; compound target.
- `GET STACKED DIAGNOSTICS x := RETURNED_SQLSTATE`; `GET CURRENT DIAGNOSTICS`.
- STACKED-with-ROW_COUNT and CURRENT-with-MESSAGE_TEXT → error.
- unrecognized item → error.
- `COMMIT`; `COMMIT AND CHAIN`; `COMMIT AND NO CHAIN`; same for `ROLLBACK`.
- Round-trip stable; `make parser` 0 new conflicts; `go build` +
  `golangci-lint` clean; package `ast` byte-identical to upstream.
