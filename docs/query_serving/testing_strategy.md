# Testing Strategy & PostgreSQL Compatibility

Multigres is a proxy in front of PostgreSQL, so every compatibility claim has to
be earned test-by-test. This document is the complete, honest record of how we
test that compatibility: what we run, every non-standard thing the harness does
to run it, and every way our output differs from stock PostgreSQL and why.

Nothing here is aspirational. If the harness pre-creates an object, rewrites a
test file, masks a column, or accepts a divergence, it is documented below.

## Scoreboard

| Suite      |   Score | What the number covers                                                       |
| ---------- | ------: | ---------------------------------------------------------------------------- |
| Regression | 222/222 | Every file in PostgreSQL 17's core `pg_regress` schedule.                    |
| Isolation  | 117/117 | Every `pg_isolation_regress` concurrency test.                               |
| Contrib    | 103/103 | Bundled contrib-extension suites (`hstore`, `citext`, `pgcrypto`, …).        |
| External   | 539/539 | Out-of-tree extensions run through their own suites (`postgis`, `pgtap`, …). |

These are live counts published by the nightly run (see
[Coverage tracking](#coverage-tracking-and-reproducibility)). A green number
means **zero un-accounted-for failures**: every test either produces
byte-identical output to upstream, or a difference that matches a committed,
human-reviewed patch describing a divergence we accept. It does **not** mean
multigres is byte-for-byte PostgreSQL - the accepted divergences are real and
are enumerated in full in [Known divergences](#known-divergences-the-complete-list).

## Why a proxy diverges at all

Client traffic flows `client → multigateway → multipooler → PostgreSQL`. Three
properties of that path make multigres behave differently from a bare
`postgres`, and every divergence in this doc traces back to one of them:

- **Transaction pooling.** A logical client session is not pinned to one
  physical backend; between statements the pooler can hand it a different one.
  Any value stored in _backend-local_ memory - a temp namespace, a PRNG seed,
  pending statistics, a hypothetical index, a `pg_prepared_statements` row, the
  physical backend PID - can be invisible to, or different for, the next
  statement unless multigres explicitly manages it.
- **Query rewriting.** The gateway normalizes every query (literals become
  `$1, $2, …` for the plan cache) and sends the reconstructed SQL to the
  backend, never the client's original bytes. That shifts byte-level artifacts
  such as error-cursor caret positions even when the result is identical.
- **A safety boundary.** To keep a shared, pooled, multi-tenant fleet safe, the
  gateway _rejects_ some statements outright - outbound connections, database
  provisioning, filesystem access, and session state hidden inside procedural
  bodies it cannot faithfully replay onto the next client. A rejection is
  correct behavior, but it changes a test's output.

We handle the resulting divergences three ways: some we **fix** so multigres
behaves like Postgres; some we **accept and document** because they are cosmetic
or semantically equivalent; and some are the safety boundary **working as
designed**. This doc draws those lines explicitly. To measure these divergences,
we run a comprehensive suite of PostgreSQL's own tests.

## The compatibility suites

We run PostgreSQL's own official suites, unmodified _input SQL_, through a real
multigres cluster. The harness lives in `go/test/endtoend/pgregresstest/` (see
its [README](../../go/test/endtoend/pgregresstest/README.md)).

| Suite          | What it runs                                                                                               | Flag                |
| -------------- | ---------------------------------------------------------------------------------------------------------- | ------------------- |
| **Regression** | PostgreSQL's `pg_regress` core suite - the same 222 files stock PG runs.                                   | `RUN_PGREGRESS=1`   |
| **Isolation**  | The `pg_isolation_regress` multi-connection concurrency tests (deadlocks, tuple locks, SSI).               | `RUN_PGISOLATION=1` |
| **Contrib**    | The regression suites bundled with core contrib extensions.                                                | `RUN_PGCONTRIB=1`   |
| **External**   | Out-of-tree extensions run through their own upstream suites (`postgis`, `pgtap`, `pgvector`, `pg_cron`…). | `RUN_PGEXTERNAL=1`  |

`RUN_EXTENDED_QUERY_SERVING_TESTS=1` runs all four - this is what CI runs. The
cluster is fully reinitialized between suites, and **external always runs last**,
because its preloaded libraries are not inert (e.g. `plpgsql_check` emits
cursor-leak warnings that would pollute earlier suites).

**The one inviolable rule: upstream input SQL is never modified to change a
result.** We run exactly the queries Postgres runs. The only sanctioned source
mutations are the PostGIS identifier renames and runner rewrites in
[Harness accommodations](#harness-accommodations), which change no test's
subject. Everything else lives in the _expected-output_ comparison.

### The cluster the suites run against

The suites do not run against a stock `postgres`; they run against a purpose-built
cluster, and several build/config choices are load-bearing:

- **PostgreSQL source.** Pinned to `REL_17_6`, shallow-cloned from
  `github.com/postgres/postgres` and built from source so the server and
  `regress.so` share an ABI. The built `bin/` is prepended to `PATH` so pgctld
  runs the exact PG the regress library was compiled against.
- **`./configure` flags.** Defaults are `--enable-cassert=no
--enable-tap-tests=no --without-icu`. Suites add flags on demand:
  `--with-lz4` (regression compression test), `--with-uuid` and
  `--with-ssl=openssl` (contrib `uuid-ossp` and `pgcrypto`). **`--with-libxml`
  is deliberately omitted** - this is why `xml`/`xmlmap` compare against
  PostgreSQL's own no-libxml `_1.out` baselines rather than the canonical files.
- **`initdb`.** Run with `--no-locale --encoding=UTF8`: a C locale for
  deterministic collation/currency output, but UTF-8 kept so unicode / collate /
  json-encoding tests still see a multibyte codec.
- **`regression_overrides.conf`** (appended at initdb) forces `lc_messages`,
  `lc_monetary`, `lc_numeric`, `lc_time = 'C'` (overriding pgctld's template),
  sets `max_prepared_transactions = 10` (for `prepared_xacts`), and
  `max_worker_processes = 8` / `max_parallel_workers = 8` (for `select_parallel`).
- **Planner GUCs per session.** `PGOPTIONS` pins `work_mem=4MB`,
  `random_page_cost=4.0`, `effective_cache_size=4GB`,
  `max_parallel_workers_per_gather=2` so the planner picks upstream plan shapes
  despite pgctld's tuned defaults - without changing those defaults.
- **Topology.** Two multipoolers (a primary + a standby); multipooler runs with
  `--connpool-global-capacity=50`, under the generated `max_connections=60`.
- **`--keep-transaction-on-gateway-rejection`.** The compatibility cluster
  starts the gateway with this flag so a single rejected statement inside a long
  `pg_regress` transaction does **not** cascade the rest of the script into
  "current transaction is aborted". Normal clients still get abort-on-error;
  this is scoped to the suites.

## What "pass" means - the patch pipeline

For each test the harness compares the actual output against the upstream
`expected/<name>.out`. It first tries PostgreSQL's own stock numbered
alternatives (`<name>_0.out … _9.out`) - a match there is a plain compatible
pass. If output still differs, it applies a committed **accepted-divergence
patch** (`testdata/pg17/patches/<name>.patch`) to the expected file and
re-compares. Every result is therefore one of three buckets:

- **`pass`, no patch** - byte-identical (after normalization) to upstream.
  Genuinely compatible.
- **`pass`, with patch** - output differs, but matches a reviewed, committed
  patch describing a divergence we accept. Counted as a pass.
- **`fail`** - output differs in a way no patch explains. A genuine residual.

There are **130 committed patch files** today (69 core, 4 contrib, 51 external,
6 isolation); a meaningful share of every suite is `pass`-with-patch. Those
files _are_ the reviewable list of divergences - [Known
divergences](#known-divergences-the-complete-list) is their inventory.

The harness **replaces** `pg_regress`'s own strict-diff verdict: it recomputes
pass/fail per test from the normalized comparison plus the patch, and overrides
`pg_regress`'s TAP aggregates. `PGREGRESS_PATCH_MODE` selects verify (default) or
generate; generate mode also deletes stale patches that no longer apply.

**CI caveat:** the Go test wrapper logs residual failures but only fails the Go
test when a suite produced _zero_ tests. A green Go check is not the signal -
read the pass count in the compatibility report / badges, not the verdict.

### Guardrails that keep the patch set honest

A patch is a liability, not a free pass. One guardrail is mechanically enforced;
the rest are review discipline (spelled out in the patches directory
[README](../../go/test/endtoend/pgregresstest/testdata/pg17/README.md)), and a
patch is reviewed like any other diff.

- **Cosmetic or semantically-equivalent only** _(review)_ - reworded errors, a
  dropped/shifted error caret, `postgres`-vs-`regression` catalog naming, an
  added unlogged-table warning, or the single "rejected by the pooler" line from
  a by-design block. A patch **must never absorb a wrong result row, a flipped
  success/error, or a changed column type** - those are bugs.
- **Every safety/security divergence patch carries a `# Known divergence (by
design)` preamble** _(review)_ naming the blocked capability and any
  deterministic fallout. Comment lines are stripped before the diff applies, so
  the preamble never affects matching. All 130 patches currently carry a
  preamble.
- **Pooling-caused differences must be stable across ≥3 consecutive runs**
  _(review)_ before acceptance - a flaky difference is a bug, not a divergence.
- **A patch applies to the current upstream file or the test fails**
  _(mechanically enforced)_. When a future PG point release changes a test, its
  stale patch stops applying and the suite goes red, forcing re-review.

### The patch set moves in both directions

Full green is not a one-way ratchet of piling on patches. Two forces act on it:

- **Real fixes _remove_ patches.** Backend-local state (temp namespaces, pending
  stats) once leaked across pooled backends and was papered over with large
  patches. Fixing it - destroying a reserved backend after temporary-object
  access so its state can't leak, and pinning namespace/seed-instantiating calls
  to a reserved connection - let `temp`, `stats`, and `sysviews` pass for real.
  The `stats` patch shrank ~100 lines because the divergence stopped existing.
- **New safety policy _adds_ patches.** Closing the "unsafe statement inside a
  procedural body" gap (the gateway now parses `DO` / `CREATE FUNCTION` bodies -
  see [Unsafe Statement Rejection](unsafe_statement_rejection.md)) is a security
  win that, by construction, makes some tests emit a rejection where stock PG
  runs the statement. Each is recorded as a `by design` patch whose only delta is
  the rejection line.

## Output normalization

Before comparing, the verifier normalizes **both** the expected and actual
output identically - so a normalizer can never manufacture a false match, it can
only remove non-determinism symmetrically. The chain (applied in this order) is:

1. **NOTIFY source PID** → `PostgreSQL backend PID`. `LISTEN`/`NOTIFY` delivery
   is preserved, but the notification carries the _physical_ backend PID, not the
   gateway virtual PID (see [VPIDs](#virtual-pids-vpids)), so the raw number
   varies per run and per pooled backend. Matches both the isolationtester trace
   form and psql's `received from server process with PID N`.
2. **Whitespace** - every run of spaces/tabs collapses to one space and each
   line is trimmed (newlines and trailing-newline presence preserved). This lets
   the harness drop `diff -b` (which drifts between BSD and GNU), and it is what
   makes a pure **error-cursor caret shift** - the `^` under a `LINE n:` context
   sitting one column off - collapse to identical text and pass with _no patch_.
3. **Per-run paths** - `/private/tmp`→`/tmp` (macOS symlink) and timestamped
   build dirs `builds/<YYYYMMDD-HHMMSS.n>`→`builds/[RUN]`, which otherwise leak
   into client output (e.g. a rejected `lo_export`'s error path).
4. **Name-keyed masking** - applied only to specific tests:
   - `pg_prepared_statements` result blocks are replaced with a single marker
     (`<pooler internal prepared statements>` for `prepare`/`guc`,
     `<pg_prepared_statements result>` for `stats`/`sysviews`) because the view
     reflects whichever pooled backend served the query.
   - The pooler's internal prepared-statement names `ppstmt<N>` → `ppstmt<ID>`
     (`prepare`, `guc`, `psql`) - the integer suffix is scheduling-dependent.
   - Isolation `stats`: `test_stat_func` call counters → `<calls>` and the
     `seq_scan`/`seq_tup_read` columns → placeholders, because those depend on
     which pooled backend ran `pg_stat_force_next_flush()`. Stable counters
     (writes, tuple counts, booleans) stay verified.
   - Isolation `detach-partition-concurrently-4`: the echoed SQL is rewritten
     back to the upstream text, undoing the backend-pinning injection described
     under [Harness accommodations](#harness-accommodations).

Diffs are generated with stable `--label a/--label b` headers so patch files
embed no absolute paths or timestamps. All masking is in-code Go, dispatched by
test name - not driven by patch preambles.

## Harness accommodations

These are the "gnarly" things the harness does beyond "send the SQL through the
gateway and diff." Every one is **test-harness only** and is listed here in full.
The guiding line: an accommodation may only let a test's _real subject_ run - it
never makes a blocked operation appear to succeed. The test's own attempt at the
blocked thing still gets rejected, and that rejection is what the patch records.

### One database for everything

`pg_regress` normally drops and recreates a `regression` database. The gateway
blocks `DROP`/`CREATE DATABASE` by design, so the harness runs the entire suite
on the single pre-existing `postgres` database via `EXTRA_REGRESS_OPTS=--use-existing
--dbname=postgres` (and `CONTRIB_TESTDB=postgres` / `ISOLATION_TESTDB=postgres`
for those suites). Upstream fixtures that hard-code the `regression` database
name therefore diverge only in that name - recorded as output-only patches, never
by rewriting input SQL.

### Pre-seeded helper functions (created directly on the primary)

Many regression tests define their own helper functions whose bodies use dynamic
`EXECUTE` - exactly the shape the gateway's PL/pgSQL body analysis rejects at
`CREATE`. Left alone, that one rejection cascades into thousands of "function
does not exist" errors that bury the real question the test asks. The harness
opens a **direct** connection to the primary (bypassing the gateway) and creates
these benign helpers from `testdata/pg17/regress_preseed.sql` before the suite
runs; the DDL replicates to standbys, so every pooled backend sees them. The
test's _own_ `CREATE` still hits the gateway and is still rejected - so the single
honest rejection line remains the only recorded divergence.

| Helper (from `regress_preseed.sql`)                                            | Serves test        |
| ------------------------------------------------------------------------------ | ------------------ |
| `check_ddl_rewrite(regclass, text)`                                            | `alter_table`      |
| `find_hash(json)`, `hash_join_batches(text)`                                   | `join_hash`        |
| `explain_analyze_without_memory(text)`, `explain_analyze_inc_sort_nodes(text)` | `incremental_sort` |
| `eval(text)`                                                                   | `interval`         |
| `explain_memoize(text, bool)`                                                  | `memoize`          |
| `explain_merge(text)`                                                          | `merge`            |
| `check_estimated_rows(text)`                                                   | `stats_ext`        |
| `depth_b_tf()` (trigger fn)                                                    | `triggers`         |
| `explain_filter(text)`, `explain_filter_to_json(text)`                         | `explain`          |
| `explain_parallel_append(text)`                                                | `partition_prune`  |

(`find_hash` is the lone helper with no dynamic `EXECUTE`; it is seeded only so
`hash_join_batches`' body resolves at seed time.) External extensions have their
own primary-side preseeds - currently `hypopg`'s `do_explain`, and the PostGIS
helpers below.

### Public-schema reset and scratch databases

Because every contrib module and external extension shares the single `postgres`
database, the harness resets state directly on the primary between them:
`resetContribState` drops every non-`plpgsql` extension and every user schema
except `public`/`information_schema`/`multigres`, then recreates `public` with
stock grants. Extensions that install into their own schema (`pg_partman`→
`partman`, `pgsodium`→`pgsodium`) are torn down explicitly.

`pg_cron` is the one extension whose tests need real databases to exist for
catalog/ACL checks; the harness creates its `ScratchDatabases` directly on the
primary as pure catalog metadata. This is a test accommodation, explicitly **not**
a product capability - the gateway still blocks `CREATE DATABASE` for clients.

`CREATE EXTENSION` for preloaded extensions is deliberately routed _through the
gateway_ (not the primary), both to exercise the pooled path and to avoid a
create-on-primary / read-from-standby race.

### PostGIS: identifier renames and runner rewrites

PostGIS cannot use `make check`; the harness drives `run_test.pl` directly and
applies two source rewrites (pure identifier substitution / runner plumbing that
change no test's subject):

- **Helper renames** so a single shared preseed can hold helpers that upstream
  defines with clashing signatures across files:
  `check_changes`→`check_changes_ap` (`topogeo_addpolygon`),
  `runTest`→`runtest_alr` / `runtest_apme` (two `topogeo_*` robustness specs),
  `make_test_raster`→`make_test_raster_tickets` (raster `tickets`).
- **Runner rewrites** in `run_test.pl`: `template1`→`postgres`; the
  `ALTER DATABASE … SET test.executor_slow_factor` call is neutered (ALTER
  DATABASE is gateway-blocked); and a per-test hook re-seeds the PostGIS helpers
  onto the primary before each test, because PostGIS test files `CREATE OR
REPLACE` their own helpers (gateway-rejected) and `DROP` them at the end, so a
  one-time preseed would be gone after the first test.

### An HTTP server for `http` and `pg_net`

The `http` (pgsql-http) and `pg_net` extensions make real libcurl calls, so the
harness runs a local httpbin-compatible server on **127.0.0.1:9080** for the
duration of those suites. The port is fixed by pgsql-http's own
`SET http.server_host = 'http://localhost:9080'`, which falls back to the live
`httpbin.org` only when nothing answers locally - a fallback the harness must
never hit, so it **fails loudly if :9080 is already taken**. It serves
`/anything`, `/get`, `/status/N`, `/response-headers`, `/delay/N`, `/redirect-to`,
and `/image/png` (exactly 8090 NUL-free ASCII bytes, because the expected file
pins `length_binary=8090`). Live TLS probes to `https://postgis.net` in that
suite are left unchanged.

### `--unsafe-pooler-mode` for a handful of scaffolding files

A gateway started with `--unsafe-pooler-mode` (default off) skips the
unsafe-statement rejections. The harness uses it in exactly one place: a second
gateway that runs a small set of `pg_partman` pgTAP files
(`UnsafePoolerGlobs`) whose scaffolding opens with a `DO … EXECUTE 'DROP TABLE
'||to_char(…)` block the enforcing gateway rejects. These files exercise
`pg_partman` for real; routing them to the unsafe gateway lets them run instead
of being dropped. It is never used for the core, isolation, or contrib suites,
and never to inflate a compatibility signal.

### Virtual PIDs (VPIDs)

Under pooling a client is not pinned to one physical backend, but the wire
protocol still needs one stable process identity per client connection (for
`BackendKeyData` / query cancellation, and for anything reasoning about "who is
blocking whom"). The **multigateway assigns each client connection a virtual PID
(VPID)** that is stable for the life of that connection, independent of which
physical backend serves any given statement. This is a product feature, not just
a test device.

- **Encoding.** A VPID is a 32-bit value: an 11-bit gateway prefix (≤2047) in the
  high bits and a ~20-bit local connection id in the low bits. The prefix cap
  keeps bit 31 clear, so a VPID is always positive when read as PostgreSQL's
  signed `int32` (no negative PIDs in `pg_stat_activity` or clients).
- **Wire identity.** The gateway sends the VPID (plus a gateway-held secret) in
  `BackendKeyData`; the client's `pg_backend_pid()`-equivalent identity and
  cancel key are the VPID, never the physical backend PID.
- **Cancellation.** On a `CancelRequest` the gateway decodes the prefix to route:
  a different gateway's prefix is forwarded; a local one is matched to the
  connection by VPID after verifying the secret.
- **The mapping table.** `multigres.backend_vpid` is an **unlogged** sidecar
  table `(backend_pid, vpid, updated_at)`. Multipooler upserts a row at
  hand-off points (fresh backend checkout / new reservation) through the **admin
  pool** - independent of the borrowed backend's role, transaction, or GUC state,
  so the mapping survives `RESET ALL`/`DISCARD ALL` and is not rolled back with
  client work - and deletes it on release/recycle (closing the backend if a
  clean idle state can't be confirmed). It is the one sidecar table customer
  roles may `SELECT` (writes are admin-only), and it is preserved across
  leader-promotion sidecar sweeps. Writing is gated by
  `--backend-vpid-tracking-enabled` (default off; replicas skip writes since the
  table is unlogged) and is currently enabled **only for the isolation suite**.

### Isolationtester: session mapping and the lock-wait shim

`pg_isolation_regress` runs multiple named sessions and asks the server "is
session A blocked by session B?" - which through a pooler means mapping VPIDs
back to real backends. The harness patches `isolationtester.c` (idempotently, via
`git checkout` first):

- **Clears `PGAPPNAME`** and skips the per-session `set_config('application_name',
…)` - upstream sets a unique `application_name` per session, and that
  high-cardinality backend state would exhaust the pool before it resets.
- **Retargets the wait probe** from the C builtin
  `pg_isolation_test_session_is_blocked($1, '{…}')` to a plpgsql shim
  `public.multigres_test_session_is_blocked($1::int4, '{…}'::int4[])` (explicit
  casts are required because isolationtester prepares with null param types).
- **Routes the control connection** (`conns[0]`, which runs global setup/teardown
  and the lock-wait watchdog - trusted scaffolding) directly at the primary via
  `ISOLATION_CONTROL_CONNINFO`, bypassing gateway body-analysis; the sessions
  under test still go through the gateway.

The shim `public.multigres_test_session_is_blocked` is installed directly on the
primary in the `postgres` database. It joins `multigres.backend_vpid` to
`pg_stat_activity` to translate the VPIDs it receives into real backend PIDs,
then runs `pg_blocking_pids` / `pg_safe_snapshot_blocking_pids` over **every**
real backend mapped to the VPID (aggregating, not picking one). It has a
direct-PID fallback that fires only when no mapping is found - guarded, because a
VPID can coincidentally equal an unrelated real backend PID. Every invocation
logs to `public.isolation_debug_log`, dumped after the run. The spec
`detach-partition-concurrently-4` is additionally rewritten to `pg_advisory_lock`
its VPID and pin the backend before recording the PID a later step cancels
(otherwise pooling could cancel a different backend); that rewrite's echoed SQL
is masked back to upstream text in the verifier.

## Known divergences: the complete list

These are the honest edges, grouped by _why_ they diverge because the "why"
determines whether they will ever change. Each entry names the affected tests /
patches. Extension-specific instances of the same cause are grouped under
[Extension divergences](#extension-divergences).

### 1. Blocked by design (will not change without a new product policy)

A shared, pooled, multi-tenant fleet cannot safely expose these. The gateway
rejects them (`unsafe_stmt.go`, `unsafe_funccall.go`, `restricted_guc.go`,
`execute_unwrap.go`); the resulting rejection line is the recorded divergence. A
`--unsafe-pooler-mode` flag (default off) disables these rejections for trusted
single-tenant deployments that accept the risk.

<!-- markdownlint-disable MD013 -->

| Blocked capability                                                                                                                                                            | Why                                                                                                                                                                                                                                                                 | Core patches affected                                                                                                                                                                                                               |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`CREATE`/`DROP DATABASE`**                                                                                                                                                  | Provisioning is a cluster operation, not a client query.                                                                                                                                                                                                            | `database`, `create_role`, `drop_if_exists`                                                                                                                                                                                         |
| **Outbound connections** (FDW, `CREATE SERVER`, `CREATE SUBSCRIPTION`, `dblink`, `postgres_fdw`)                                                                              | Open connections out of the pool / replication.                                                                                                                                                                                                                     | `alter_generic`, `create_am`, `fast_default`, `foreign_data`, `object_address`, `stats_ext`, `subscription`                                                                                                                         |
| **`CREATE LANGUAGE`**                                                                                                                                                         | Installs a procedural language handler into the shared fleet.                                                                                                                                                                                                       | `alter_generic`                                                                                                                                                                                                                     |
| **`LOAD` / shared libraries**                                                                                                                                                 | Loading a `.so` into a pooled backend leaks state onto every later client.                                                                                                                                                                                          | `create_function_c`, `guc`                                                                                                                                                                                                          |
| **Server filesystem access** (`pg_read_file`, `pg_stat_file`, `pg_ls_dir`, `lo_import`/`lo_export`, libpq fast-path FunctionCall)                                             | Reaches the server's disk / raw protocol.                                                                                                                                                                                                                           | `misc_functions`, `privileges`, `largeobject`                                                                                                                                                                                       |
| **Session state in opaque bodies** - `SET`/`RESET`/`DISCARD`/`LISTEN`, non-local `set_config`, or dynamic `EXECUTE` the pooler can't vet, inside a `DO`/function/trigger body | Can't be faithfully mirrored onto the next pooled client. Self-reverting forms (`SET LOCAL`, `SET TRANSACTION`, `set_config(…, is_local:=true)`) and injection-safe dynamic `EXECUTE` are allowed. See [Unsafe Statement Rejection](unsafe_statement_rejection.md). | `brin_bloom`, `brin_multi`, `select_parallel`, `gin`, `guc`, `oidjoins`, `explain`, `alter_table`, `incremental_sort`, `join_hash`, `memoize`, `merge`, `partition_prune`, `interval`, `triggers`, `plpgsql`, `create_function_sql` |
| **Restricted GUCs** (`synchronous_commit`)                                                                                                                                    | Durability is a cluster property, not a client's to change.                                                                                                                                                                                                         | `test_setup`                                                                                                                                                                                                                        |
| **Schema-qualified temp namespace** (`pg_temp.x`, `pg_temp` in `search_path`)                                                                                                 | The temp namespace belongs to a pooled backend, not the session. `CREATE TEMP` is supported.                                                                                                                                                                        | `temp`, `window`                                                                                                                                                                                                                    |
| **Persistent replication slots**                                                                                                                                              | Slot position isn't yet migratable across failover.                                                                                                                                                                                                                 | contrib `pg_walinspect` (below)                                                                                                                                                                                                     |

<!-- markdownlint-enable MD013 -->

Most session-body rejections whose helper is _benign_ are pre-seeded (see
[Harness accommodations](#pre-seeded-helper-functions-created-directly-on-the-primary))
so the test's substance still runs and only the rejection line remains.

### 2. Cosmetic / semantically equivalent (documented, low value to change)

The parity bar we hold is **same SQLSTATE, same error code, same message text and
result rows.** These differences fall below that bar.

- **Error-cursor caret drift.** The `LINE n: … ^` caret is frequently dropped or
  shifted because the backend computes positions against the gateway's normalized
  query text. Truly preserving it would mean sending un-normalized bytes down the
  latency-sensitive route interface for a purely visual payoff; declined. Affects
  ~25 suites incl. `aggregates`, `insert_conflict`, `join`, `json`, `jsonb`,
  `select_implicit`, `sqljson_queryfuncs`, `union`, `with`, `functional_deps`,
  `xml`, and most PostGIS geometry tests. Won't-fix.
- **Reworded parser / gateway errors.** Where multigres' parser or the gateway
  raises the error (rather than the backend), wording differs while SQLSTATE and
  HINT are preserved: `numerology`, `event_trigger`, `sqljson`,
  `sqljson_jsontable`, `foreign_key`, `rowsecurity`, `strings`.
- **`CREATE UNLOGGED` warning.** Multigres adds a `WARNING`/`HINT` that an
  unlogged table is not replicated and is lost on failover; the table is still
  created. See [Unlogged tables](unlogged_tables.md). Appears in `brin`,
  `create_index`, `create_table`, `gist`, `identity`, `spgist`, `alter_table`,
  `gin`, `sequence`, `publication`, and the two `vector` patches (12 total).
- **`postgres` vs `regression` database naming.** `information_schema` and catalog
  listings report the actual `postgres` database: `domain`, `updatable_views`,
  `sequence`, `psql`.

### 3. Pooling / backend-local state

Values that legitimately reflect _which physical backend_ served an observation.
Where the value is genuinely backend-local, the verifier masks that specific
block rather than pretend a single global value exists (see [Output
normalization](#output-normalization)); where it is a stable property, it is
patched.

- **`pg_prepared_statements` / pooler `ppstmt` names** - masked. `prepare`, `guc`,
  `psql`, `stats`, `sysviews`, and `plancache` (plan counters + generic/custom
  plan transition are backend-local through the consolidator).
- **`pg_stat` counters** - masked or patched where flush-visibility lags per
  backend. `stats`, isolation `stats`.
- **`LISTEN`/`NOTIFY` source PID** - normalized to a placeholder; delivery is
  preserved but the PID is the physical backend's, not the VPID. isolation
  `async-notify`.
- **PG17 login event triggers** fire once per pooled-backend creation, not per
  client `\c`, so the login counter stays 0. `event_trigger_login`.
- **Plan-cache consolidation** by text+types can reuse a cached plan across an
  in-between `SET`. isolation `drop-index-concurrently-1`.
- **`application_name` cleared** in isolationtester (pool-fragmentation fix), so
  `pg_stat_activity` lock queries return no rows even though blocking is verified
  via the VPID shim. isolation `insert-conflict-specconflict`,
  `partition-drop-index-locking`.

### 4. Harness / environment

Consequences of the single-`postgres`-database, no-libxml, single-tenant harness
rather than of multigres behavior.

- **Single-database fallout** - `GRANT`/`REVOKE` on the absent default
  `regression` database, and schema/ownership/cleanup knock-on: `dependency`,
  `publication`, isolation `intra-grant-inplace-db`.
- **No libxml** - `xmlmap` compares against the `_1.out` no-libxml baseline.
- **Per-run paths** - masked (see normalization).

### Extension divergences

Every contrib and external divergence, grouped by extension. Root causes are the
same classes as above; the table names the specific cause.

| Extension                   | Patches | Cause                                                                                                                                                                                                                                                                                          |
| --------------------------- | ------: | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **citext** (contrib)        |       1 | `setter()` body has `SET search_path` → `CREATE` rejected → cascade in `create_index_acl`.                                                                                                                                                                                                     |
| **pg_walinspect** (contrib) |       2 | Persistent (non-temporary) replication slot rejected; failed drop is fallout.                                                                                                                                                                                                                  |
| **pgstattuple** (contrib)   |       1 | FDW/`SERVER` blocked → dummy foreign-table checks become "does not exist".                                                                                                                                                                                                                     |
| **hypopg**                  |       2 | Hypothetical indexes live in backend-local memory; a later statement on another pooled backend can't see them. `do_explain` dynamic `EXECUTE` also rejected (preseeded). Tracked, `partial`.                                                                                                   |
| **pg_graphql**              |       1 | Backend-local schema cache goes stale on a reused backend for rolled-back in-transaction DDL - same class as hypopg, not a bug.                                                                                                                                                                |
| **pg_cron**                 |       1 | `CREATE`/`DROP DATABASE` blocked through the pooler.                                                                                                                                                                                                                                           |
| **http**                    |       1 | `SET http.server_host` in a `DO` exception handler rejected (dead code; local httpbin used) + statement-timeout cancel wording.                                                                                                                                                                |
| **plpgsql_check**           |       4 | `LOAD` rejected (lib is preloaded instead) + PL/pgSQL bodies PG only rejects at runtime + dynamic `EXECUTE` rejected.                                                                                                                                                                          |
| **pgtap**                   |       4 | FDW in a helper rejected (`aretap`, `hastap`, `ownership`); SQL `PREPARE` is gateway-managed so `EXECUTE` inside a body → "prepared statement does not exist" (`throwtap`).                                                                                                                    |
| **postgis** (core)          |      20 | Mostly `qnodes()`/helper dynamic `EXECUTE` rejected + per-test reseed, plus geometry error-cursor offsets and a few index scan-type cost diffs. One `SET`/`RESET`-in-body (`union`), one statement-timeout (`interrupt_relate`), a couple of shared-preseed "schema already exists" artifacts. |
| **postgis_raster**          |       6 | Dynamic `EXECUTE` map-algebra helpers rejected + reseed; one non-literal-bind-param wording diff.                                                                                                                                                                                              |
| **postgis_topology**        |      10 | Dynamic `EXECUTE` helpers rejected + reseed (some renamed), plus cursor offsets; two `SET`-in-`runTest`-body rejections.                                                                                                                                                                       |
| **vector** (pgvector)       |       2 | `CREATE UNLOGGED TABLE` warning/hint.                                                                                                                                                                                                                                                          |

## Extension coverage

Tracked in a living catalog (`ExtensionCatalog` in `extensions.go`) with an
explicit status per extension; the Extension Coverage table in every report is
generated from it, so it can't drift from reality. Statuses:

- **covered** - full upstream suite runs through the gateway.
- **partial** - runs with documented drop-in gaps (narrow patches).
- **build-only** - built, preloaded, and smoke-loaded with `CREATE EXTENSION`,
  but its suite is not a valid pass/fail signal yet.
- **unsupported** - cannot be covered by this harness; carries a reason so the
  table shows _why_, not a blank.

| Extension                          | Kind     | Status          | Note                                                                                                                                                                                                  |
| ---------------------------------- | -------- | --------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| btree_gin                          | contrib  | covered         |                                                                                                                                                                                                       |
| btree_gist                         | contrib  | covered         |                                                                                                                                                                                                       |
| citext                             | contrib  | covered         |                                                                                                                                                                                                       |
| cube                               | contrib  | covered         |                                                                                                                                                                                                       |
| earthdistance                      | contrib  | covered         | depends on cube                                                                                                                                                                                       |
| fuzzystrmatch                      | contrib  | covered         |                                                                                                                                                                                                       |
| hstore                             | contrib  | covered         |                                                                                                                                                                                                       |
| ltree                              | contrib  | covered         |                                                                                                                                                                                                       |
| pg_prewarm                         | contrib  | covered         |                                                                                                                                                                                                       |
| pg_trgm                            | contrib  | covered         |                                                                                                                                                                                                       |
| pg_walinspect                      | contrib  | covered         | `NO_INSTALLCHECK`; needs `wal_level=replica` (satisfied by the standby); explicit test list.                                                                                                          |
| pgcrypto                           | contrib  | covered         | needs `--with-ssl=openssl`                                                                                                                                                                            |
| pgstattuple                        | contrib  | covered         | FDW section diverges (blocked by design).                                                                                                                                                             |
| unaccent                           | contrib  | covered         |                                                                                                                                                                                                       |
| uuid-ossp                          | contrib  | covered         | needs `--with-uuid`                                                                                                                                                                                   |
| http                               | external | covered         | pgsql-http; local httpbin on :9080; only patch is timeout wording.                                                                                                                                    |
| index_advisor                      | external | covered         | `BEGIN/ROLLBACK`-wrapped, so hypopg's backend-local indexes stay pinned. Depends on hypopg.                                                                                                           |
| pg_cron                            | external | covered         | needs `shared_preload_libraries`; scratch DBs created on the primary for catalog checks.                                                                                                              |
| pg_graphql                         | external | covered         | Rust/pgrx; loads `fixtures.sql` through the gateway before its suite.                                                                                                                                 |
| pg_jsonschema                      | external | covered         | Rust/pgrx; ships no SQL suite, so an in-repo SQL translation runs through the gateway.                                                                                                                |
| pg_net                             | external | covered         | needs libcurl + preload; in-repo SQL suite against local httpbin.                                                                                                                                     |
| pg_partman                         | external | covered         | pgTAP via psql; runs transaction-wrapped tests only (see exclusions).                                                                                                                                 |
| pgjwt                              | external | covered         | pure-SQL; pgTAP; pinned to a commit (never tagged). Depends on pgcrypto + pgtap.                                                                                                                      |
| pgmq                               | external | covered         | pure-SQL PGXS; partitioned-queue tests depend on pg_partman.                                                                                                                                          |
| pgsodium                           | external | covered         | libsodium; pgTAP keyless mode - server-key/TCE tests self-skip.                                                                                                                                       |
| pgtap                              | external | covered         | own pg_regress suite, every test `BEGIN…ROLLBACK`-wrapped. Test dep of others.                                                                                                                        |
| plpgsql_check                      | external | covered         | linter/profiler; needs preload; gateway-blocked `LOAD`s are patched.                                                                                                                                  |
| postgis (+ raster/sfcgal/topology) | external | covered         | PostGIS 3.6.3 via `run_test.pl`; see harness rewrites and divergences.                                                                                                                                |
| supabase_vault                     | external | covered         | libsodium; generated test getkey script; in-repo SQL suite.                                                                                                                                           |
| vector                             | external | covered         | pgvector; PGXS.                                                                                                                                                                                       |
| hypopg                             | external | **partial**     | hypothetical indexes are backend-local under autocommit pooling; narrow patches document the gap. Passes cleanly when tests wrap in `BEGIN…ROLLBACK` (which is why `index_advisor` is fully covered). |
| pgaudit                            | external | **build-only**  | its suite asserts an exact `SET ROLE`/`PREPARE`/`EXECUTE` audit stream that isn't a valid signal until session-state replay around `SET ROLE` and `pgaudit.*` GUCs is finished.                       |
| wrappers                           | external | **build-only**  | Supabase Wrappers; smoke-loaded. Full FDW usability needs a guarded policy for the `CREATE FOREIGN DATA WRAPPER`/`CREATE SERVER` the gateway blocks.                                                  |
| dblink                             | contrib  | **unsupported** | pooler blocks outbound connections.                                                                                                                                                                   |
| postgres_fdw                       | contrib  | **unsupported** | pooler blocks `CREATE SERVER` / outbound connections.                                                                                                                                                 |
| pg_stat_statements                 | contrib  | **unsupported** | `NO_INSTALLCHECK`; records the query text the gateway rewrites.                                                                                                                                       |
| moddatetime                        | contrib  | **unsupported** | `contrib/spi` ships no pg_regress suite.                                                                                                                                                              |
| plpgsql                            | contrib  | **unsupported** | built-in PL; exercised by the core regression suite, not contrib.                                                                                                                                     |

### Suite exclusions (named, not silently dropped)

- **pg_partman** - `test_bgw/`, `test_tablespace/`, `test_nonsuperuser/`, and
  `test_procedure/` subfolders run pgTAP in **autocommit** (their procedures
  `COMMIT`, so they can't be `BEGIN…ROLLBACK`-wrapped); `plan()`'s session-temp
  table then leaks onto an unpinned pooled backend → "You tried to plan twice!".
  A hard limit of pgTAP through a transaction pooler, not a harness bug. One more
  file (`test-time-monthly-source-generated`) is excluded because it asserts a
  row count calibrated to a specific run date - it fails identically with and
  without the gateway.
- **pgtap** - `performs_ok`, `performs_within`, `resultset`, `valueset`, `privs`
  are excluded: their whole subject is passing a SQL-level prepared-statement name
  or a runtime-built `SET search_path` into an `EXECUTE` inside a function body,
  which the gateway can't see; a patch would have to absorb the entire file and
  hide real regressions. The FDW-tail files (`aretap`, `hastap`, `ownership`,
  `throwtap`) _are_ kept, with bounded tail patches.

## Beyond the PostgreSQL suites

The `pg_regress` suites prove we match Postgres's output on a single session. A
separate family of tests in `go/test/endtoend/queryserving/` proves the _proxy
layer_ specifically - including the multi-tenant isolation the single-user
regression suite can't reach:

- **`pgparity`** - a differential suite: the same `.slt` corpus runs against
  direct `postgres` and against `multigateway` pointed at the _same_ backend, so
  any divergence is provably proxy-introduced. Runs on every PR.
  See [pgparity](testing/pgparity.md).
- **`sqllogictest`** - large-scale query-result verification via the sqllogictest
  corpus.
- **`postgrest` / `pgbouncer` differential suites** - real-world client
  ecosystems (PostgREST's hspec, PgBouncer's suite) run through the gateway,
  differentially against direct PG.
- **Targeted proxy tests** - session-state leakage across pooled sessions,
  reserved-connection lifecycle, temp-table timeout, prepared-statement handling,
  transaction failover, query cancellation, `LISTEN`/`NOTIFY`, replica reads.
  This is where multi-tenant isolation is actually proven.

## Coverage tracking and reproducibility

- **The catalog is the source of truth.** `ExtensionCatalog` holds every
  extension with its kind, status, and reason; `DefaultContribModules` is derived
  from the `covered` entries, and every report's coverage table is generated by
  merging the catalog with the live pass/fail. Enrolling an extension is a
  one-line catalog edit; the table updates itself.
- **Reports.** Each run writes `compatibility-report.md` (per-suite badges + a
  per-test table with the patch link), the Extension Coverage table,
  `results.json` (consumed by CI to diff runs for regressions), and shields.io
  badge endpoints (`regression.json`, `isolation.json`, `contrib-extension.json`,
  `overall.json`).
- **Live badges.** The nightly run publishes those endpoints to GitHub Pages, so
  any README or blog badge shows the current pass count automatically.
- **Reproducibility.** Stock PG output is platform-sensitive (glibc collation,
  timezone formatting, error-cursor positions), so patches are tied to the
  CI-matching Linux environment (`ubuntu-24.04`) and regenerated inside a
  container that mirrors the runner (`make pgregress-update-patches-docker`),
  never directly on macOS.

## The bottom line

A full green across regression, isolation, contrib, and external means every
PostgreSQL test we run either matches upstream exactly or matches a difference we
have deliberately reviewed and written down - and this document, together with
the 130 patch files, is that written record in full. It is not a claim that
multigres is indistinguishable from PostgreSQL. The blocked-by-design operations,
the cosmetic and pooling divergences, and the tracked limitations above are the
honest boundary of the product, and the patch pipeline exists precisely so that
boundary is explicit, reviewed, and impossible to move by accident.
