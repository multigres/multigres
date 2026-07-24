# Chunk 1.15 — PG regression corpus harness

The Phase 1 acceptance gate. Every PL/pgSQL body in PostgreSQL's own plpgsql
regression SQL must parse through `ParsePLpgSQL` (or be an explicitly
allowlisted failure). This is what proves the grammar port is complete enough.

## What it does

Following the SQL parser's `testdata/postgres/*.json` convention, the extracted
cases are committed as JSON and the raw PostgreSQL `.sql` files are **not**
vendored (`.out` files are irrelevant — we assert bodies _parse_, not _execute_).

- **`testdata/pg_corpus_cases.json`** — the committed corpus, in the same
  `parseCase` shape as the hand-written case files (`{comment, body}`, plus an
  `error` substring for the bodies PG rejects). A `THIRD_PARTY_NOTICES.md` gives
  PostgreSQL-License attribution.
- **`TestGeneratePGCorpusCases`** (`corpus_test.go`) regenerates that JSON from a
  local PG checkout (skipped unless `PLPGSQL_CORPUS_SRC` points at PG's plpgsql
  SQL dir). It strips psql `\` lines, splits each file into statements by
  tokenising with the SQL lexer (dollar-/single-quoted bodies are single tokens,
  so their inner `;` don't leak), parses each with the SQL parser, and pulls the
  body from `CREATE FUNCTION` / `PROCEDURE … LANGUAGE plpgsql` and `DO` blocks via
  their `as` / `language` DefElems. Bodies are de-duplicated; a body PG rejects
  gets its parse error recorded as the `error` substring.
- **`TestPGCorpus`** runs `ParsePLpgSQL` on every case: bodies with no `error`
  must parse (and their deparse must re-parse to the same deparse — full
  round-trip), bodies with an `error` must fail with that substring. (Round-trip
  was originally not asserted because some bodies embed a line comment inside an
  expression, e.g. `WHEN 1 -- c THEN`, which our raw-text capture kept; audit fix
  T2.1 ends the capture at the last real token, so the whole corpus now
  round-trips and the assertion was enabled.)

## Result: 280 bodies, 7 expected errors

**273 parse; 7 are PG's own negative tests** carrying the matching parse error,
which our parser reproduces:

- 3 × end-loop label mismatch (our `checkLabels` error matches PG),
- 3 × `NOT NULL` variable without a default (our error matches PG verbatim),
- 1 × array-element FOR target `x[1]` (unsupported in PG too).

There are no genuine gaps left: every body either parses or is a PG negative
test we also reject. (280 unique bodies, de-duplicated from 287 occurrences.)

## Gaps closed (the corpus surfaced these; we fixed them)

The harness initially flagged two bodies PG accepts but we rejected. Both are now
handled:

- **Comma-separated FOR target list** (`for a, b, c in select …`): `for_variable`
  now peeks past the first name for a comma list (PG's `for_variable`
  behaviour), captured as text via `readForVariable`; an integer `..` loop with a
  list errors "integer FOR loop must have only one target variable", matching PG.
  Because we have no resolution, we accept any names (PG requires declared
  scalars) — so two of PG's negative tests that reject a _constant_/field comma
  target now parse here, the usual no-resolution trade-off (accept-more).
- **Embedded `BEGIN ATOMIC` body**: `scanStmtText` now mirrors PG's
  `make_execsql_stmt` — inside a `CREATE [OR REPLACE] {FUNCTION|PROCEDURE}` it
  tracks BEGIN/CASE … END depth and ends the statement only at a `;` outside any
  such block, so a `BEGIN ATOMIC … END` routine body's inner `;` no longer cut
  the execsql short.

## Scope — out

- Executing the corpus / comparing `.out` output.

## Acceptance

- `TestPGCorpus` green: 280 bodies, 273 parse, 7 expected-error negative tests.
- `make parser` 0 conflicts; `go build` + `golangci-lint` clean; package `ast`
  byte-identical; no changes outside the plpgsql package and its testdata.
