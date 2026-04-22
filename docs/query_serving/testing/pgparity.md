# pgparity Correctness Suite

## Overview

`pgparity` runs query-parity tests to verify that multigres proxies produce
exactly the same query results as direct PostgreSQL. A curated corpus of
`.slt` files is executed against two targets — direct `postgres` and the
multigres proxy `multigateway` — and any record that passes on postgres but
fails on multigateway is flagged as a compatibility bug.

The suite lives in `go/test/endtoend/pgparity/` and runs as a standard Go
integration test — part of the regular `test-integration.yml` workflow on every
push and PR to `main`, no opt-in flag needed. It is a **correctness** test, not
a benchmark; pgbouncer is intentionally excluded because it is a pass-through
proxy and has nothing new to verify.

## Architecture

```text
                ┌────────────────┐
                │  .slt corpus   │
                └───────┬────────┘
                        │
            ┌───────────┴───────────┐
            ▼                       ▼
      ┌──────────┐          ┌────────────┐
      │ postgres │          │multigateway│
      │ (direct) │          │  (proxy)   │
      └────┬─────┘          └─────┬──────┘
           │                      │
           └────────┬─────────────┘
                    ▼
              ┌──────────┐
              │ postgres │
              │ (same    │
              │ backend) │
              └──────────┘
```

Both targets point at the same PostgreSQL backend (the primary pgctld node),
so any divergence is proxy-introduced.

## File format

Test files use a subset of SQLite's
[sqllogictest](https://www.sqlite.org/sqllogictest/) format — SQL statements
followed by expected results, executed and verified by our own runner using
the `pgx` driver.

Supported directives:

| Directive                                    | Meaning                                   |
| -------------------------------------------- | ----------------------------------------- |
| `statement ok`                               | SQL must execute without error            |
| `statement error [regex]`                    | SQL must return an error (optional regex) |
| `query <types> [nosort\|rowsort\|valuesort]` | Query result must match listed rows       |
| `skipif postgres` / `onlyif postgres`        | Gate the next record on engine name       |
| `halt`                                       | Stop processing the file                  |
| `#` comments, blank lines                    | Ignored                                   |

Type characters in the `query` line: `I` (integer), `R` (real / 3-decimal
float), `T` (text). `NULL` renders as the literal string `NULL`; empty text
renders as `(empty)`.

Not currently supported:

- Large-result MD5 hashing (`N values hashing to ...`). The corpus uses inline
  expected output only.
- Conditional directives for non-postgres engines skip the record.

## Test corpus

The initial corpus under `go/test/endtoend/pgparity/testdata/`:

| File               | Focus                                              |
| ------------------ | -------------------------------------------------- |
| `select.slt`       | Literals, arithmetic, NULL handling, string funcs  |
| `tables.slt`       | DDL, DML, constraint violations                    |
| `joins.slt`        | Inner / left join, subquery filters                |
| `aggregates.slt`   | GROUP BY, HAVING, ORDER BY, LIMIT, count(DISTINCT) |
| `types.slt`        | Casts, booleans, NULL arithmetic, divide-by-zero   |
| `transactions.slt` | BEGIN / COMMIT / ROLLBACK semantics                |

The `.slt` extension is standard sqllogictest; `.test` is also accepted by the
runner but avoided here because the repo `.gitignore` strips `*.test` (Go's
`go test -c` build artifacts).

## Running locally

```bash
go test -v -run TestPgParity ./go/test/endtoend/pgparity/... -timeout 30m
```

### Using the mt-dev skill

```bash
/mt-dev integration pgparity TestPgParity -v
```

### Environment variables (optional)

| Variable          | Default    | Description                                      |
| ----------------- | ---------- | ------------------------------------------------ |
| `PGPARITY_CORPUS` | `testdata` | Directory to scan for `.slt` / `.test` files     |
| `PGPARITY_FILES`  | (all)      | Comma-separated filenames to restrict the run to |

Example — run just two files:

```bash
PGPARITY_FILES=select,joins \
  go test -v -run TestPgParity ./go/test/endtoend/pgparity/... -timeout 10m
```

## How failures are reported

Each `.slt` file runs as a `t.Run` subtest with postgres and multigateway as
child subtests. The orchestrator compares outcomes inline:

- **multigateway fails a record postgres passed** — `t.Errorf` fails the test.
  This is the parity contract.
- **postgres fails a record** — logged as a likely test-file bug (expected
  output is wrong); does not fail the test.
- **both fail** — logged under the multigateway subtest, does not fail the
  test, because it's not a proxy-specific regression.

Standard `go test -v` output is the full report — no separate JSON or markdown
artifacts are emitted.

## CI integration

`pgparity` runs as part of the normal integration test suite in
`.github/workflows/test-integration.yml`, which executes
`go test ./go/test/endtoend/...` on every push and PR to `main`. There is no
separate cron, workflow, or label gate — a parity regression fails the standard
integration check like any other bug.

## File structure

```text
go/test/endtoend/pgparity/
├── main_test.go              # TestMain + shared 2-node + multigateway cluster
├── pgparity_test.go          # TestPgParity orchestrator, divergence check
├── parser.go                 # .slt file parser
├── runner.go                 # pgx-based execution + result comparison
├── schema.go                 # Between-file schema reset helper
├── parser_test.go            # Parser unit tests
├── runner_test.go            # formatValue / rowsort / comparison unit tests
├── corpus_parse_test.go      # Parses every testdata file to catch typos
└── testdata/                 # Curated .slt corpus
    ├── aggregates.slt
    ├── joins.slt
    ├── select.slt
    ├── tables.slt
    ├── transactions.slt
    └── types.slt
```

## Adding a new test file

1. Drop a new `.slt` file under `go/test/endtoend/pgparity/testdata/`.
2. Run `go test -run TestCorpusParses ./go/test/endtoend/pgparity/...` to
   catch parser errors (no cluster needed for this check).
3. Run the full suite locally to confirm both targets pass:
   `PGPARITY_FILES=<your_file> go test -run TestPgParity ./go/test/endtoend/pgparity/...`
4. If postgres fails a record, the record is wrong — fix the expected output.
   If only multigateway fails, you've found a bug worth filing.
