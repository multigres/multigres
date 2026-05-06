# sqllogictest differential suite

Runs the pinned upstream [sqllogictest][slt] corpus against two targets,
under both the simple and extended PostgreSQL wire protocols, in the same
Go test run:

- **Targets:** standalone PostgreSQL 17.6 (built by
  `go/test/endtoend/pgbuilder`, independent of pgctld/multigateway) and
  Multigres via the multigateway backed by a 2-multipooler shardsetup
  cluster.
- **Protocols:** `-e postgres` (simple) and `-e postgres-extended`
  (extended).

Four invocations per corpus file (2 targets × 2 protocols). Per-file,
per-protocol pass/fail is recorded for both targets and written to
`results.json` + a markdown summary. **Divergences and failures do not
fail the Go test** — this suite is a tracker, not a gate. CI compares the
new results against a cached baseline and flags regressions there, the
same way `pgregresstest` does.

## Why the differential framing

The corpus' own expected hashes are the baseline correctness signal.
Running the same corpus through both direct PG and multigateway lets us
distinguish three failure modes:

- **Baseline failure** (both fail) — the corpus expectation doesn't
  match direct PG (e.g. SQLite-specific integer semantics, or a dialect
  mismatch). Not a proxy bug; recorded and categorized so it doesn't
  pollute the signal channel.
- **Proxy divergence** (PG passes, multigateway fails) — a bug in the
  multigres query path. High-signal.
- **Unexpected MG pass** (PG fails, multigateway passes) — usually an
  earlier statement aborted the session on PG but got through on MG.
  Low signal but recorded.

## Corpus: the upstream SQLite sqllogictest

The harness shallow-clones [gregrahn/sqllogictest][slt-mirror] (an
actively-maintained git mirror of the SQLite fossil repo at
<https://www.sqlite.org/sqllogictest/>) at a pinned SHA into
`/tmp/multigres_slt_cache/source/sqllogictest`. This is the canonical
5.7M-query corpus. First run: ~30 s clone; subsequent runs hit the
cache.

### Licensing

The upstream [`COPYRIGHT.md`][slt-copyright] is explicit:

> Copyright © 2008 D. Richard Hipp
>
> This program is free software. As far as the author is concerned, you
> can redistribute it and/or modify the code as you see fit. No
> attribution is required. Use whichever of the following license terms
> best applies to your situation.
>
> 1. GNU General Public License
> 2. BSD License
> 3. MIT License
> 4. CC0 License

Per the upstream's own quad-licensing offer, we consume the corpus under
the **MIT License** — compatible with the Apache-2.0 license this repo
uses. No attribution is required by the upstream, but for clarity the
corpus provenance (repo URL + pinned SHA) is recorded in
`results.json:corpus_commit` and in the rendered markdown report.

Bump `CorpusCommit` in `corpus.go` when you want to pull newer upstream
changes. The baseline comparison will naturally reset to match.

## Why sqllogictest-rs

| Option                       | Pros                                                                                                 | Cons                                                            |
| ---------------------------- | ---------------------------------------------------------------------------------------------------- | --------------------------------------------------------------- |
| **sqllogictest-rs** (chosen) | Actively maintained (used by RisingWave, DuckDB); first-class Postgres executor; SHA-pinned releases | Requires one extra binary in `make tools`                       |
| Original C runner            | The canonical reference                                                                              | Ancient; no prebuilt releases; patchy Postgres-engine support   |
| Java port                    | Used by DoltgreSQL                                                                                   | JVM dependency in CI; upstream less active than sqllogictest-rs |

sqllogictest-rs takes one `.test` file at a time via the CLI, exits zero
on success and non-zero on the first failing record. That's enough
granularity for file-level tracking; record-level tracking is a possible
follow-up.

## How to run it locally

```bash
# One-time setup: downloads sqllogictest-bin with pinned SHA into bin/
make tools

# Required before integration tests.
make build

# Run (skipped without RUN_EXTENDED_QUERY_SERVING_TESTS=1; matches the
# "Run Extended Query Serving Tests" PR label).
scripts/portpool.sh start
RUN_EXTENDED_QUERY_SERVING_TESTS=1 \
  MULTIGRES_PORT_POOL_ADDR=/tmp/multigres-port-pool.sock \
  go test -v -timeout 3h \
    -run TestPostgreSQLSqlLogicTest \
    ./go/test/endtoend/queryserving/sqllogictest/...
```

The full corpus is a multi-hour run. During iteration, scope via
`SLT_CORPUS_GLOB` — e.g. the handful of hand-written top-level files
runs in seconds:

```bash
SLT_CORPUS_GLOB='test/select?.test' RUN_EXTENDED_QUERY_SERVING_TESTS=1 \
  MULTIGRES_PORT_POOL_ADDR=/tmp/multigres-port-pool.sock \
  go test -v -run TestPostgreSQLSqlLogicTest \
    ./go/test/endtoend/queryserving/sqllogictest/...
```

To point at a local checkout (internal fork, working copy, etc.),
override `SLT_CORPUS_DIR`:

```bash
SLT_CORPUS_DIR=/path/to/my/slt-checkout RUN_EXTENDED_QUERY_SERVING_TESTS=1 \
  go test -v -run TestPostgreSQLSqlLogicTest \
    ./go/test/endtoend/queryserving/sqllogictest/...
```

## Environment variables

| Var                                | Default                    | Purpose                                               |
| ---------------------------------- | -------------------------- | ----------------------------------------------------- |
| `RUN_EXTENDED_QUERY_SERVING_TESTS` | unset (test skips)         | Set to `1` to enable. Also enables pgregresstest.     |
| `SLT_CORPUS_DIR`                   | cloned upstream            | Override to point at a local corpus checkout.         |
| `SLT_CORPUS_GLOB`                  | `test/**/*.test`           | Scope which corpus files run. Supports `**`.          |
| `SLT_CACHE_DIR`                    | `/tmp/multigres_slt_cache` | Override cache root for the upstream clone.           |
| `SLT_PER_FILE_TIMEOUT`             | `5m`                       | Per-file (per-target, per-protocol) wall-time budget. |
| `MULTIGRES_PG_CACHE_DIR`           | `/tmp/multigres_pg_cache`  | Shared with pgregresstest — PG source + build cache.  |

## Outputs

Written under `builder.OutputDir` (i.e.
`/tmp/multigres_pg_cache/results/<timestamp>/`):

- `results.json` — **array of suite objects**, one per protocol
  (`SQLLogicTest-simple`, `SQLLogicTest-extended`). Per-file `status`
  and `note` fields, plus per-target pass booleans, counters for
  `passed_both` / `passed_pg_only` (proxy divergence) /
  `passed_mg_only` / `failed_both`, and the pinned `corpus_commit`.
  Shape is intentionally compatible with `pgregresstest` results so
  `.github/scripts/detect-regressions.sh` can compare runs without
  changes — `suite_name + "/" + file_name` is the dedup key, so
  simple-protocol and extended-protocol tracking stay fully
  independent.
- `compatibility-report.md` — one section per protocol with
  postgres/multigateway shields.io badges, counters, and a list of
  proxy divergences. Appended to `GITHUB_STEP_SUMMARY` when set.

## CI integration

`.github/workflows/test-sqllogictest.yml` runs the suite on:

- **PRs labeled `Run Extended Query Serving Tests`** — opt-in per-PR. The
  same label also triggers `pgregresstest`.
- **Nightly cron** (`30 3 * * *` UTC) — baseline tracking.
- **Weekly summary cron** (`30 9 * * 1` UTC) — Slack digest.
- **`workflow_dispatch`** — manual kick.

The workflow caches `results.json` as the new baseline on each run, and
the next run compares against it. Regressions (pass → fail) surface in a
Slack notification from the nightly job.

## Known gaps and follow-ups

- **File-level granularity.** One failing record fails the whole file
  in our counters. `sqllogictest-rs` can emit JUnit XML per record
  (`--junit`); a richer report is a future enhancement.
- **SQLite-specific expectations.** The corpus was generated by
  comparing SQLite / MySQL / PostgreSQL reference engines, but some
  tests lean on SQLite-specific integer semantics (e.g. integer `avg`)
  and will show up as `failed_both` against PG. That's categorized as a
  baseline failure and doesn't contribute to the proxy-divergence
  counter.

[slt]: https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki
[slt-mirror]: https://github.com/gregrahn/sqllogictest
[slt-copyright]: https://github.com/gregrahn/sqllogictest/blob/master/COPYRIGHT.md
