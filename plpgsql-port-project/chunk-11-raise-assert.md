# Chunk 1.11 — RAISE + ASSERT

The two diagnostic statements. RAISE has the most elaborate hand-scan in
`pl_gram.y` (optional level, then one of message / condition-name / SQLSTATE /
USING, with a param list and an option list); ASSERT is a two-expression scan.
Both are resolution-free — no `T_DATUM`, no namespace — so they port cleanly.

Ported from `pl_gram.y` (`stmt_raise`, `stmt_assert`, `read_raise_options`,
`check_raise_parameters`) and `plpgsql.h` (`PLpgSQL_stmt_raise`,
`PLpgSQL_raise_option`, `PLpgSQL_stmt_assert`, `PLpgSQL_raise_option_type`).

## Grammar

Two single-token productions, dispatched into hand-scanners like `stmt_return`
/ `stmt_dynexecute`:

```text
stmt_raise:  K_RAISE  { beginScan…; $$ = lx.makeRaiseStmt() }
stmt_assert: K_ASSERT { beginScan…; $$ = lx.makeAssertStmt() }
```

`proc_stmt` gains `| stmt_raise` and `| stmt_assert`. K_RAISE / K_ASSERT are
distinct tokens (already declared, already in `unreserved_keyword`), so at a
statement start they unambiguously enter these productions — PG declares
`%expect 0` here; verify `y.output` stays at our 0-conflict baseline.

## RAISE scan (`makeRaiseStmt`)

Mirrors PG's `stmt_raise` action step for step:

1. `RAISE ;` → bare re-raise (level defaults to EXCEPTION, everything else empty).
2. Optional elog level keyword: EXCEPTION / WARNING / NOTICE / INFO / LOG / DEBUG.
   (PG's `tok_is_keyword` T_DATUM recheck is irrelevant — we never emit T_DATUM,
   so it reduces to a plain token compare.)
3. Then exactly one of:
   - `SCONST` — old-style message format; then a `,`-separated param expression
     list (each scanned up to `,` / `;` / USING), or straight to `;` / USING.
   - a **condition name** (`T_WORD` or an unreserved keyword) — captured as text.
     PG's `plpgsql_recognize_err_condition` validation is **dropped** (it checks
     the name against a known-condition table; that is resolution, not parsing).
   - `SQLSTATE 'xxxxx'` — the 5-char length + charset check is **kept** (it is
     purely lexical, needs no resolution).
   - `USING` — jump straight to the option list.
4. `USING option = expr, …` via `readRaiseOptions` (ERRCODE / MESSAGE / DETAIL /
   HINT / COLUMN / CONSTRAINT / DATATYPE / TABLE / SCHEMA; `=` or `:=`).
5. `checkRaiseParameters` — counts `%` placeholders in the message vs the number
   of params (kept; purely syntactic). Errors "too many/few parameters
   specified for RAISE".

## Lexer fix — one scanner, like PG (`scanNext` = `plpgsql_yylex`)

RAISE reads a condition name as a bare word, which surfaced a latent divergence.
PG has exactly **one** scanner, `plpgsql_yylex`: the grammar and every `read_*`
helper (`read_sql_construct`, `read_fetch_direction`, `stmt_raise`) call it via
`yylex()`, and it always fully classifies a word as `T_WORD` / `T_CWORD` /
keyword (bare `IDENT` never escapes `internal_yylex`).

Our port had drifted: `scanNext` did only partial keyword reclassification and
returned bare `IDENT` for a plain word, so a hand-scan action reading an
identifier saw `IDENT` where PG sees `T_WORD`. This chunk makes `scanNext` the
faithful `plpgsql_yylex` analogue — full keyword + compound (`T_CWORD`) + `T_WORD`
classification (still no `T_DATUM`, the sanctioned no-resolution divergence) —
and `Lex` now delegates to it. The two actions that inspected `IDENT`
(this chunk's RAISE condname, and `read_fetch_direction`'s cursor name, where PG
checks `T_DATUM`) now check `T_WORD`, matching PG. Compound-merging inside
fragment scans is safe: text is still captured by byte offset, exactly as PG's
`plpgsql_append_source_text` does.

## ASSERT scan (`makeAssertStmt`)

`ASSERT cond [, message]` — `cond` scanned up to `,` / `;`
(`read_sql_expression2`); if it stopped on `,`, `message` scanned up to `;`
(`read_sql_expression`).

## AST

New nodes in `plpgsqlast` (tags appended to `nodes.go`):

- `PLpgSQL_stmt_raise` — `ElogLevel RaiseLevel`, `Condname string`,
  `IsSqlState bool`, `Message string`, `Params []*PLpgSQL_expr`,
  `Options []*PLpgSQL_raise_option`.
  - `ElogLevel` is a typed int carrying PG's exact `elog.h` values (DEBUG1=14,
    LOG=15, INFO=17, NOTICE=18, WARNING=19, ERROR/EXCEPTION=21), so the field
    maps 1:1 to PG's `elog_level`.
  - `IsSqlState` is a deparse aid (PG stores both a condition name and a
    SQLSTATE code in the same `condname` char\* and never deparses; we need to
    know which form to re-emit). Same spirit as `dynexecute`'s `UsingFirst`.
- `PLpgSQL_raise_option` — `OptType RaiseOptionType`, `Expr *PLpgSQL_expr`
  (Node, not Stmt — a helper like `PLpgSQL_case_when`).
- `PLpgSQL_stmt_assert` — `Cond *PLpgSQL_expr`, `Message *PLpgSQL_expr`.

`Message` on RAISE is the **dequoted** literal value (as PG stores it); the
deparse re-quotes via `ast.QuoteStringLiteral`.

## Deparse

PG never deparses these; the canonical forms below are chosen to re-parse to the
same AST (round-trip stable), not to reproduce the input byte-for-byte. A
non-bare RAISE always renders its level keyword (PG collapses "no level" and
"EXCEPTION" to the same `elog_level`, so re-emitting EXCEPTION is faithful).

```text
RAISE                                              (bare: all fields empty/default)
RAISE <LEVEL>[ <condname>|  SQLSTATE '<code>'| '<message>'[, <p>…]][ USING <opt> = <e>, …]
ASSERT <cond>[, <message>]
```

`<LEVEL>` ∈ {DEBUG, LOG, INFO, NOTICE, WARNING, EXCEPTION}. Options render with
`=` (not `:=`).

## Scope — out

- `plpgsql_recognize_err_condition` condition-name validation (resolution).
- `PLpgSQL_expr.Parsed` still nil.

## Acceptance

New `testdata/raise_cases.json` and `testdata/assert_cases.json`, golden deparse
via `PLPGSQL_REWRITE=1`:

- bare `RAISE`; `RAISE NOTICE 'hi'`; message with params `RAISE 'x=%, y=%', a, b`.
- condition name `RAISE division_by_zero`; `RAISE EXCEPTION unique_violation`.
- `RAISE SQLSTATE '22012'`; invalid-length / invalid-charset SQLSTATE → error.
- USING options in every form; USING-only (`RAISE USING MESSAGE = 'x'`); both
  message-then-USING and condname-then-USING.
- param-count mismatch → "too many/few parameters specified for RAISE".
- `ASSERT x > 0`; `ASSERT x > 0, 'must be positive'`.
- Round-trip stable; `make parser` 0 new conflicts; `go build` +
  `golangci-lint` clean; package `ast` byte-identical to upstream.
