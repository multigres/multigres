# pgparity Correctness Suite

## Overview

`pgparity` runs query-parity tests to verify that multigres proxies produce
exactly the same query results as direct PostgreSQL. A curated corpus of
`.slt` files is executed against two targets — direct `postgres` and the
multigres proxy `multigateway` — and any record that passes on postgres but
fails on multigateway is flagged as a compatibility bug.

The suite lives in `go/test/endtoend/queryserving/pgparity/` and runs as a standard Go
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

Test files are plain text — a sequence of **records**, each one a directive
header followed by SQL and (for queries) the expected result. Records are
separated by blank lines. Lines starting with `#` are comments. Any directive
not listed below is a parse error, so a misspelled directive fails loudly
rather than silently disabling tests.

### Directives

| Directive                 | Meaning                                             |
| ------------------------- | --------------------------------------------------- |
| `statement ok`            | SQL must execute without error                      |
| `statement error`         | SQL must return any error                           |
| `statement error <regex>` | SQL must return an error whose message matches      |
| `query <types>`           | Result rows must match the expected block, in order |
| `query <types> nosort`    | Explicit form of the default (no reordering)        |
| `query <types> rowsort`   | Sort whole rows before comparing                    |

### Type string

The `<types>` on a `query` line is a compact string, one character per result
column:

| Char | PostgreSQL types               | Rendering                                |
| ---- | ------------------------------ | ---------------------------------------- |
| `I`  | integer, bigint, bool, numeric | decimal integer (bools become `0` / `1`) |
| `R`  | real, double, numeric, integer | fixed 3-decimal float, e.g. `3.140`      |
| `T`  | text, varchar, anything else   | string representation                    |

`NULL` always renders as the literal `NULL`; an empty string renders as
`(empty)` so you can tell it apart from a missing value in the expected block.

### Expected rows block

For `query` records, the lines after `----` are the expected output — one
_row_ per line, tab-separated within a row. Rows are flattened to individual
values so the runner compares cell-by-cell. **Use real tabs between columns,
not spaces** — the parser splits only on `\t`.

<!-- markdownlint-disable MD010 -->

```text
query IT rowsort
SELECT a, b FROM t
----
1	one
2	two
```

<!-- markdownlint-enable MD010 -->

A `query` record with a blank line immediately after the SQL (no `----` at
all) asserts that the query merely runs without error, regardless of output —
useful for things like `EXPLAIN`.

### Example

<!-- markdownlint-disable MD010 -->

```text
# Basic parity test: DDL, DML, queries, expected errors.

statement ok
CREATE TABLE t (a INT PRIMARY KEY, b TEXT)

statement ok
INSERT INTO t VALUES (1, 'one'), (2, 'two')

query I
SELECT count(*) FROM t
----
2

query IT rowsort
SELECT a, b FROM t
----
1	one
2	two

statement error duplicate key
INSERT INTO t VALUES (1, 'dup')
```

<!-- markdownlint-enable MD010 -->

## Test corpus

The initial corpus under `go/test/endtoend/queryserving/pgparity/testdata/`:

| File               | Focus                                              |
| ------------------ | -------------------------------------------------- |
| `select.slt`       | Literals, arithmetic, NULL handling, string funcs  |
| `tables.slt`       | DDL, DML, constraint violations                    |
| `joins.slt`        | Inner / left join, subquery filters                |
| `aggregates.slt`   | GROUP BY, HAVING, ORDER BY, LIMIT, count(DISTINCT) |
| `types.slt`        | Casts, booleans, NULL arithmetic, divide-by-zero   |
| `transactions.slt` | BEGIN / COMMIT / ROLLBACK semantics                |

The `.slt` extension is required — the repo `.gitignore` strips `*.test`
(Go's `go test -c` build artifacts), so test files need to use `.slt` to be
tracked by git. The runner ignores any other extension in the corpus
directory.

## Running locally

```bash
go test -v -run TestPgParity ./go/test/endtoend/queryserving/pgparity/... -timeout 30m
```

### Using the mt-dev skill

```bash
/mt-dev integration pgparity TestPgParity -v
```

### Environment variables (optional)

| Variable          | Default    | Description                                      |
| ----------------- | ---------- | ------------------------------------------------ |
| `PGPARITY_CORPUS` | `testdata` | Directory to scan for `.slt` files               |
| `PGPARITY_FILES`  | (all)      | Comma-separated filenames to restrict the run to |

Example — run just two files:

```bash
PGPARITY_FILES=select,joins \
  go test -v -run TestPgParity ./go/test/endtoend/queryserving/pgparity/... -timeout 10m
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
go/test/endtoend/queryserving/pgparity/
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

1. **Create the file.** Drop `<topic>.slt` under
   `go/test/endtoend/queryserving/pgparity/testdata/`. Group related SQL in one file — the
   parser reuses connection state within a file, so DDL and the queries that
   exercise it belong together. Between files, the schema is reset (DROP
   SCHEMA public CASCADE), so you can assume a clean slate at the top.

2. **Write records.** Each record is a directive header, the SQL, and (for
   `query`) a `----` separator plus expected rows. Separate records with a
   blank line. See the example above for layout.
   - Use **`query I`** / **`query R`** / **`query T`** deliberately — the
     type char drives how values render before comparison. `SELECT avg(x)`
     returns a `NUMERIC`; treat it as `R` to get a clean `3.500`-style
     expected value instead of a raw struct dump.
   - Use **`rowsort`** whenever you don't care about order (most joins,
     aggregates without `ORDER BY`). PostgreSQL is free to return rows in
     any order otherwise, and the test will flake.
   - Use **`statement error`** (bare) unless you want to pin a specific
     error message — pinning on exact PG error text is fragile across PG
     versions.

3. **Parse-check without a cluster:**

   ```bash
   go test -run TestCorpusParses ./go/test/endtoend/queryserving/pgparity/...
   ```

   This catches typos and unsupported directives without needing to spin up
   the cluster. Unknown directives now error at parse time.

4. **Run just your file against the cluster:**

   ```bash
   PGPARITY_FILES=<your_file> \
     go test -v -run TestPgParity ./go/test/endtoend/queryserving/pgparity/... -timeout 10m
   ```

5. **Interpret the result:**
   - **Both targets pass** — ship it.
   - **postgres fails a record** — the expected output is wrong. Either fix
     the expected rows or rewrite the query. The runner logs the actual
     value it got, which is usually enough to patch.
   - **multigateway fails a record postgres passed** — you've found a real
     parity bug. File it and either omit that record, or commit the file as
     a failing test that will start passing once the proxy is fixed (if you
     want the failure tracked in CI).
