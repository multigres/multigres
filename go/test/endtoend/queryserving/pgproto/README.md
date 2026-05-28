# pgproto wire-protocol conformance suite

Runs [Tatsuo Ishii's `pgproto`][pgproto] — a data-file-driven frontend/backend
protocol tester, written for exercising PostgreSQL proxies like pgpool-II —
against two targets in the same Go test run, and records where the
multigateway's response trace diverges from a real PostgreSQL's:

- **Targets:** standalone PostgreSQL (built by `go/test/endtoend/pgbuilder`,
  independent of pgctld/multigateway) and Multigres via the multigateway
  backed by a 2-multipooler shardsetup cluster.
- **Corpus:** the in-tree `.pgproto` data files under `testdata/`. Each file is
  a hand-written sequence of raw V3 protocol messages.

Two invocations per corpus file (one per target). **PostgreSQL is the oracle:**
a file "passes" on the multigateway when its normalized response trace matches
PostgreSQL's exactly. Per-file match/mismatch is recorded for both targets and
written to `results.json` + a markdown summary. **Divergences do not fail the
Go test** — this suite is a tracker, not a gate. CI flags any file that
produces a PostgreSQL baseline but a differing multigateway trace, the same way
`sqllogictest` and `pgregresstest` do.

## Why pgproto, and why differential

Our Go-native protocol tests
(`go/test/endtoend/queryserving/extended_query_protocol_test.go`, built on
`go/common/pgprotocol/client`) cover the extended-query path piecemeal, but
each scenario is hand-coded in Go. `pgproto` instead reads a **data file** of
raw protocol messages and sends them verbatim — in any order, including
deliberately malformed or out-of-spec sequences that are awkward to express
through a client library (Bind before Parse, Execute against a closed portal,
COPY interrupted by CopyFail, error-then-Sync recovery). That declarative,
expand-by-adding-a-file shape is exactly what makes it valuable for a proxy:
the multigateway sits in the middle of the wire
protocol, so feeding it adversarial message orderings and diffing its responses
against PostgreSQL catches relay bugs the normal-path tests never reach.

Running each file through both direct PG and the multigateway distinguishes
three outcomes:

- **Match** (multigateway trace == postgres trace) — the proxy relayed the
  exchange faithfully.
- **Proxy divergence** (postgres produced a baseline, multigateway differs) —
  a bug in the multigres query/protocol path. High-signal; this is what CI
  alerts on.
- **No baseline** (postgres itself could not produce a trace, e.g. a malformed
  data file or a connection failure) — a harness/corpus problem, recorded and
  categorized so it doesn't pollute the divergence channel.

## The pgproto data-file format

Each non-comment line is one frontend message. The first token is the message
kind in single quotes; remaining tokens are tab-separated. Blank lines and
lines starting with `#` are ignored.

| Directive                                                               | Message / meaning                                             |
| ----------------------------------------------------------------------- | ------------------------------------------------------------- |
| `'Q'  "sql"`                                                            | Simple Query                                                  |
| `'P'  "stmt" "sql" <noids>`                                             | Parse (prepared statement)                                    |
| `'B'  "portal" "stmt" <ncodes> <nparams> <nresultcodes>`                | Bind                                                          |
| `'D'  'S'\|'P' "name"`                                                  | Describe statement / portal                                   |
| `'E'  "portal" <maxrows>`                                               | Execute                                                       |
| `'C'  'S'\|'P' "name"`                                                  | Close statement / portal                                      |
| `'S'`                                                                   | Sync                                                          |
| `'H'`                                                                   | Flush                                                         |
| `'d'  "data"`                                                           | CopyData (one value; text only)                               |
| `'c'`                                                                   | CopyDone                                                      |
| `'f'  "msg"`                                                            | CopyFail                                                      |
| `'F'  <oid> <ncodes> <code...> <nparams> <plen> "val" ... <resultcode>` | Fast-path FunctionCall                                        |
| `'X'`                                                                   | Terminate                                                     |
| `'Y'`                                                                   | Read backend messages until ReadyForQuery (use after `Q`/`S`) |
| `'y'`                                                                   | Read backend messages for up to 1s, then stop (use after `H`) |

A query spanning multiple lines uses a trailing `\` for continuation; a `"`
inside a string is escaped as `\"`.

`pgproto` prints the full FE→BE trace to **stderr** in a format similar to the
PostgreSQL JDBC driver (see "Output and normalization").

### Tool limitations to know when authoring data files

- **V3 protocol only.** No SSL, no GSSAPI, no protocol negotiation.
- **No CancelRequest.** Query cancellation is a special out-of-band packet with
  no data-file directive, so it cannot be driven from a corpus file. Cancel
  behaviour is covered by the Go-native tests instead.
- **Bound parameter values cannot be sent.** Only zero-parameter Binds work, so
  embed literals in the query text instead of using `$1` placeholders. This is
  not documented upstream — we found it by reading `src/extended_query.c` and
  confirming empirically: in `process_bind`, the parameter-_value_ read is the
  one field missing a `SKIP_TABS` before it, so the tab left by the preceding
  length field makes `buffer_read_string` reject the value. The upstream README
  only ever shows zero-parameter Binds (`'B' "" "S1" 0 0 0`), so the path is
  never exercised there.
- **CopyData is text-only and cannot contain tabs or newlines** — an unescaped
  tab/newline terminates the data-file string, and `\t`/`\n` escapes emit the
  literal letter, not the control byte. Use single-column COPY targets and one
  value per `'d'`.

## Output and normalization

`pgproto`'s trace is highly deterministic, which is what makes the differential
diff clean:

- `DataRow`, `RowDescription`, and `ParameterStatus` print only the message
  name — the payload is discarded — so row values, column types, and GUC values
  never enter the trace.
- `BackendKeyData` (backend PID + secret key) is consumed by libpq during
  connection startup, **before** the trace begins, so it never appears. There
  is therefore no per-connection run-varying data (PID, secret, timestamp) to
  strip.

`ErrorResponse` / `NoticeResponse` are the one case needing real normalization.
`pgproto` prints every diagnostic field PostgreSQL sends, and several of them are
**not something a proxy can reproduce**:

- `F` (source file), `L` (source line), `R` (routine) are PostgreSQL's internal
  **C-source** location. The multigateway has no access to these, so it can never
  emit them.
- For errors the multigateway raises itself (e.g. a parse failure from its own
  parser), the message wording (`M`) and character position (`P`) legitimately
  differ from PostgreSQL's — the gateway parser reports a different position and
  phrasing than the backend scanner.

The only part of an error a proxy must preserve is the **SQLSTATE** (`C`). So
`reduceErrorLine` (in `runner.go`) collapses every `Error`/`NoticeResponse` line
to its message kind and SQLSTATE — e.g. `<= BE ErrorResponse(C 42601)` — and the
differential comparison is on that alone. Diffing the dropped fields would only
ever surface noise, never a real proxy bug, whereas a SQLSTATE mismatch (e.g.
`57014` vs `08P01` on a failed COPY) is a genuine divergence and is still caught.

Aside from this, normalization (`normalizeTrace` in `runner.go`) just trims
trailing whitespace and drops blank lines. If a future divergence turns out to
be benign target-specific noise, add the rule there and document it here.

## Tool install

`pgproto` publishes no prebuilt binaries — it is C built against `libpq` — so
`make tools` fetches the GitHub source archive at a **pinned commit**
(SHA256-verified in `tools/tool_checksums.sh`), runs its committed autoconf
`configure` + `make`, and symlinks the binary into `bin/pgproto`. Bump
`PGPROTO_VER` in the `Makefile` (and `PgprotoCommit` in `corpus.go`, plus the
checksum) to move the pin.

Building needs a C compiler and the libpq development headers, located via
`pg_config`:

- Debian/Ubuntu: `sudo apt-get install -y libpq-dev`
- RHEL/Fedora: `sudo dnf install -y libpq-devel`
- macOS: `brew install libpq && export PATH="$(brew --prefix libpq)/bin:$PATH"`

Any libpq version works — libpq is used only to open the connection and
complete startup/auth; the wire-protocol trace is produced by pgproto's own
socket code.

## How to run it locally

```bash
# One-time setup: builds pgproto from pinned source into bin/.
make tools

# Required before integration tests.
make build

# Run (skipped without RUN_EXTENDED_QUERY_SERVING_TESTS=1; matches the
# "Run Extended Query Serving Tests" PR label).
scripts/portpool.sh start
RUN_EXTENDED_QUERY_SERVING_TESTS=1 \
  MULTIGRES_PORT_POOL_ADDR=/tmp/multigres-port-pool.sock \
  go test -v -timeout 30m \
    -run TestPgProtoConformance \
    ./go/test/endtoend/queryserving/pgproto/...
```

During iteration, scope to a single file with `PGPROTO_CORPUS_GLOB`:

```bash
PGPROTO_CORPUS_GLOB='extended_query.pgproto' RUN_EXTENDED_QUERY_SERVING_TESTS=1 \
  MULTIGRES_PORT_POOL_ADDR=/tmp/multigres-port-pool.sock \
  go test -v -run TestPgProtoConformance \
    ./go/test/endtoend/queryserving/pgproto/...
```

Per the project convention, prefer the `/mt-dev` skill for test execution:
`/mt-dev integration pgproto`.

## Environment variables

| Var                                | Default                   | Purpose                                             |
| ---------------------------------- | ------------------------- | --------------------------------------------------- |
| `RUN_EXTENDED_QUERY_SERVING_TESTS` | unset (test skips)        | Set to `1` to enable. Also enables sqllogictest.    |
| `PGPROTO_CORPUS_DIR`               | `testdata/`               | Override to point at an external set of data files. |
| `PGPROTO_CORPUS_GLOB`              | `**/*.pgproto`            | Scope which corpus files run. Supports `**`.        |
| `PGPROTO_PER_FILE_TIMEOUT`         | `2m`                      | Per-file (per-target) wall-time budget.             |
| `MULTIGRES_PG_CACHE_DIR`           | `/tmp/multigres_pg_cache` | Shared with sqllogictest — PG source + build cache. |

## Outputs

Written under `builder.OutputDir` (i.e.
`/tmp/multigres_pg_cache/results/<timestamp>/`):

- `results.json` — a one-element **array** holding the `PgProto` suite object,
  with per-file `status`/`note`, per-target `passed` booleans, counters
  (`passed_both` = matched, `passed_pg_only` = proxy divergence,
  `passed_mg_only`, `failed_both` = no baseline), and the pinned `corpus_commit`
  (the pgproto tool revision). The array shape and field names match
  `sqllogictest`/`pgregresstest` so `.github/scripts/detect-regressions.sh` and
  the CI `jq` divergence filter (`postgres.passed == true and
multigateway.passed == false`) work unchanged. Divergent files carry a
  unified diff of the two traces in the multigateway `output`.
- `compatibility-report.md` — shields.io badges, counters, and a per-file diff
  for each proxy divergence. Appended to `GITHUB_STEP_SUMMARY` when set.

## Expanding the corpus

Add a `testdata/<name>.pgproto` file. Make it **self-contained**: it should
create whatever tables it needs (the harness drops and recreates the `public`
schema before each file, so nothing carries over between files). Use the
directive table above; end read-expecting blocks with `'Y'` and finish with
`'X'`.

Validate a new file against a local PostgreSQL before relying on it — `pgproto`
exits non-zero and prints a parse error if the data file is malformed:

```bash
PGPASSWORD=<pw> bin/pgproto -h 127.0.0.1 -p <port> -u <user> -d <db> \
  -f go/test/endtoend/queryserving/pgproto/testdata/<name>.pgproto
```

## CI integration

`.github/workflows/test-pgproto.yml` runs the suite on:

- **PRs labeled `Run Extended Query Serving Tests`** — opt-in per-PR (the same
  label also triggers `sqllogictest` and `pgregresstest`).
- **Weekly cron** — baseline tracking + Slack digest.
- **`workflow_dispatch`** — manual kick.

Files that produce a PostgreSQL baseline but a differing multigateway trace are
surfaced in a Slack notification from the scheduled job.

[pgproto]: https://github.com/tatsuo-ishii/pgproto
