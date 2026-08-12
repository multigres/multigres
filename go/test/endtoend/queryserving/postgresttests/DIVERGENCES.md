# PostgREST suite — known gateway divergences

`TestPostgREST` runs PostgREST's upstream spec suite with PostgREST pointed at
the multigateway (see `postgrest_test.go`). The direct-PostgreSQL baseline is an
**asserted invariant**: every non-skipped spec passes on plain postgres, so the
default run is **gateway-only** and any gateway failure is a **gateway
divergence** — a real behavioural gap on the proxied path. The test **fails**
while any divergence remains; this file is the checklist for closing them.

Regenerate the list (gateway-only):

```bash
RUN_POSTGREST=1 go test -v -run 'TestPostgREST$' ./go/test/endtoend/queryserving/postgresttests/...
# DIVERGE: lines are gateway divergences.
```

Re-verify the baseline invariant after bumping the PostgREST tag or editing
fixtures — this also runs a throwaway direct PostgreSQL and classifies each
gateway failure as a divergence or an **environment failure** (fails on both
paths, so the invariant is broken — fix the harness/fixtures, not the gateway):

```bash
RUN_POSTGREST=1 POSTGREST_FULL_BASELINE=1 \
  go test -v -run 'TestPostgREST$' ./go/test/endtoend/queryserving/postgresttests/...
# ENV: lines are environment failures; BASELINE FAIL lines break the invariant.
```

**Status (2026-08-11, full suite = 1303 examples):** 49 gateway divergences,
0 environment failures — the direct-PostgreSQL baseline is fully green. ~42 of
the divergences are a single root cause (set_config position, below) — fixing
that one closes the large majority.

## Normalizations already applied (so these are NOT divergences)

Handled in `loadFixtures` / the runner so environment differences don't masquerade
as gateway bugs:

- **Timezone** → `ALTER DATABASE … SET timezone = 'UTC'` (our built PG inherits the
  host tz; upstream bakes UTC via `TZ=utc initdb`). Fixed the 11 data-representation
  failures.
- **Planner GUCs** → `ALTER DATABASE … SET work_mem/random_page_cost/effective_cache_size/
max_parallel_workers_per_gather/max_parallel_workers` back to PostgreSQL defaults
  (pgctld tunes them, which changes EXPLAIN costs and EXPLAIN `(SETTINGS)` output).
  Fixed 5 PlanSpec cost/settings divergences.
- **RangeSpec ANALYZE hook** → the suite ANALYZEs `test.items` / `test.child_entities`
  before the RangeSpec group "to get accurate results from EXPLAIN"
  (`SpecHelper.analyzeTable`), by shelling out to `psql -U postgres` inside the spec
  container. That is the suite's only subprocess call, and it has no in-container psql
  and no `postgres` superuser reachable through the proxy, so it failed on both paths.
  `loadFixtures` now runs the ANALYZE itself as the loader's superuser, and the image
  ships a no-op `psql` shim so the redundant hook exits 0. Fixed the 1 environment failure.

## Open gateway divergences

### 1. `set_config()` rejected outside a top-level SELECT target — ~42 divergences ⬅ **fix this first**

**Affected:** all mutation-path specs — `UpsertSpec` (29), `PlanSpec` upsert-plan
tests (6), `PostGISSpec`/`MultipleSchemaSpec`/`RpcPreRequestGucsSpec` PUT tests (3),
and the `Rollback{Allowed,Disallowed,Forced}Spec` POST tests.

**Symptom:** HTTP 400/500; backend error `SQLSTATE 0A000: set_config is only
supported as a top-level SELECT target list entry — use a SET statement, or
set_config(..., true) for a transaction-scoped change`.

**Root cause:** PostgREST's mutation SQL calls `set_config('pgrst.inserted', …, true)`
inside the INSERT's `WHERE` clause (its row-count trick), wrapped in a CTE:

```sql
WITH pgrst_source AS (
  INSERT INTO t (...) SELECT ... WHERE set_config('pgrst.inserted', …, true) <> '0'
  ON CONFLICT (...) DO UPDATE SET ... WHERE set_config('pgrst.inserted', …, true) <> '-1'
  RETURNING ...
) SELECT ...
```

The gateway's unsafe-funccall analyzer only allows `set_config` as a top-level
`SelectStmt` target — `go/services/multigateway/planner/unsafe_funccall.go:501-504`,
and `collectTopLevelSetConfigs` deliberately does not recurse into CTEs / subqueries
/ WHERE. So it rejects the call, even though it is `is_local=true` (transaction-scoped
and harmless — the analyzer already leaves top-level `is_local` set_config untracked
at `unsafe_funccall.go:548`). PostgreSQL accepts `set_config` anywhere, so the write
works on a direct connection and fails only through the gateway.

**Minimal repro (deterministic):**

```sql
SELECT 1 WHERE set_config('x','1',true) <> '0'   -- gateway: 0A000; postgres: ok
SELECT set_config('x','1',true)                  -- gateway: ok (top-level, allowed)
```

**Fix direction:** allow `set_config(name, value, true)` (is_local) in non-top-level
positions — it is transaction-scoped, so the pooler needn't track or intercept it
(the same reasoning already applied to top-level is_local calls).

### 2. `ServerTimingSpec` — 1 divergence

**Symptom:** Server-Timing response header differs. Likely timing-value/format
dependent; needs its own look. Status: open, unexamined.

## Intentionally skipped specs

Skipped via `specSkips()` in `spec_runner.go` (extend with `POSTGREST_SKIP`):

- **`Feature.Query.PgSafeUpdateSpec`** — needs the `pg-safeupdate` extension
  (github.com/eradman/pg-safeupdate), which we don't build. Not a gateway bug;
  install the extension to cover these 4 tests.
