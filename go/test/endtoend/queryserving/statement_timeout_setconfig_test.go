// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package queryserving

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestStatementTimeoutSetConfig exhaustively exercises set_config('statement_timeout', v, is_local)
// across the session/local and in-transaction/out-of-transaction axes. It runs the
// identical logic against both native PostgreSQL and the multigateway (via
// GetComparisonTargets), so every assertion is grounded in PostgreSQL's real
// behavior — a divergence surfaces as the multigateway subtest failing while the
// postgres subtest passes.
//
// The two views of "did it take effect" are checked independently:
//   - SHOW statement_timeout — the effective value the session reports.
//   - enforcement — whether a slow query is actually cancelled (57014).
//
// Note on baselines: native PG's default statement_timeout is 0 (disabled) while
// the multigateway's default is the --statement-timeout flag (30s). To keep
// assertions identical across both targets, every subtest establishes its own
// explicit baseline with `SET statement_timeout = 0` rather than relying on the
// server default.
func TestStatementTimeoutSetConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping set_config statement_timeout test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping set_config statement_timeout test")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	for _, target := range setup.GetComparisonTargets(t) {
		t.Run(target.Name, func(t *testing.T) {
			connStr := shardsetup.GetTestUserDSN("localhost", target.Port, "sslmode=disable", "connect_timeout=5")
			db, err := sql.Open("postgres", connStr)
			require.NoError(t, err)
			defer db.Close()

			// Force a single backing connection so every statement in a subtest
			// lands on the same session (session GUC state must persist).
			db.SetMaxOpenConns(1)

			ctx := utils.WithTimeout(t, 150*time.Second)
			require.NoError(t, db.PingContext(ctx))

			// show returns the current effective statement_timeout as reported by SHOW.
			show := func(t *testing.T, q querier) string {
				t.Helper()
				var v string
				require.NoError(t, q.QueryRowContext(ctx, "SHOW statement_timeout").Scan(&v))
				return v
			}

			// baseline resets to a disabled (0) session-level timeout, identical on
			// both targets regardless of the server default.
			baseline := func(t *testing.T) {
				t.Helper()
				_, err := db.ExecContext(ctx, "SET statement_timeout = 0")
				require.NoError(t, err)
				require.Equal(t, "0", show(t, db))
			}

			// -----------------------------------------------------------------
			// Effective-value (SHOW) matrix
			// -----------------------------------------------------------------

			// set_config(..., false) outside a transaction is a session-level SET:
			// it takes effect immediately and persists across subsequent statements.
			t.Run("session set_config(false) persists across queries", func(t *testing.T) {
				baseline(t)

				var ret string
				require.NoError(t, db.QueryRowContext(ctx,
					"SELECT set_config('statement_timeout', '1000', false)").Scan(&ret))
				assert.Equal(t, "1s", ret, "set_config returns the applied value")

				assert.Equal(t, "1s", show(t, db), "session value takes effect")

				// An unrelated query in between must not clear it.
				var one int
				require.NoError(t, db.QueryRowContext(ctx, "SELECT 1").Scan(&one))
				assert.Equal(t, "1s", show(t, db), "session value persists across queries")
			})

			// set_config(..., true) outside an explicit transaction is scoped to the
			// implicit single-statement transaction — it must NOT persist to the next
			// statement. (SET LOCAL outside a transaction is a no-op in PostgreSQL.)
			t.Run("local set_config(true) outside txn does not persist", func(t *testing.T) {
				baseline(t)

				// Establish a known session value so we can prove the local one did
				// not overwrite it.
				_, err := db.ExecContext(ctx, "SELECT set_config('statement_timeout', '1000', false)")
				require.NoError(t, err)
				require.Equal(t, "1s", show(t, db))

				var ret string
				require.NoError(t, db.QueryRowContext(ctx,
					"SELECT set_config('statement_timeout', '2000', true)").Scan(&ret))
				assert.Equal(t, "2s", ret, "set_config returns the value it applied for the statement")

				assert.Equal(t, "1s", show(t, db),
					"local set_config outside a transaction must not persist; session value stays")
			})

			// set_config(..., true) inside a transaction is a transaction-local
			// override: SHOW reflects it until the transaction ends, then reverts to
			// the session value.
			t.Run("local set_config(true) inside txn overrides then reverts on commit", func(t *testing.T) {
				baseline(t)
				_, err := db.ExecContext(ctx, "SELECT set_config('statement_timeout', '1000', false)")
				require.NoError(t, err)
				require.Equal(t, "1s", show(t, db))

				tx, err := db.BeginTx(ctx, nil)
				require.NoError(t, err)

				var ret string
				require.NoError(t, tx.QueryRowContext(ctx,
					"SELECT set_config('statement_timeout', '2000', true)").Scan(&ret))
				assert.Equal(t, "2s", ret)

				assert.Equal(t, "2s", show(t, tx),
					"transaction-local override must be the effective value inside the txn")

				require.NoError(t, tx.Commit())

				assert.Equal(t, "1s", show(t, db),
					"transaction-local override must revert to the session value after commit")
			})

			// Same as above but rolled back — the local override still reverts.
			t.Run("local set_config(true) inside txn reverts on rollback", func(t *testing.T) {
				baseline(t)
				_, err := db.ExecContext(ctx, "SELECT set_config('statement_timeout', '1000', false)")
				require.NoError(t, err)

				tx, err := db.BeginTx(ctx, nil)
				require.NoError(t, err)

				var ret string
				require.NoError(t, tx.QueryRowContext(ctx,
					"SELECT set_config('statement_timeout', '2000', true)").Scan(&ret))
				assert.Equal(t, "2s", ret)
				assert.Equal(t, "2s", show(t, tx), "local override effective inside txn")

				require.NoError(t, tx.Rollback())
				assert.Equal(t, "1s", show(t, db), "local override reverts after rollback")
			})

			// set_config(..., false) inside a transaction is a session-level SET; it
			// persists past COMMIT.
			t.Run("session set_config(false) inside txn persists after commit", func(t *testing.T) {
				baseline(t)
				_, err := db.ExecContext(ctx, "SELECT set_config('statement_timeout', '1000', false)")
				require.NoError(t, err)

				tx, err := db.BeginTx(ctx, nil)
				require.NoError(t, err)

				var ret string
				require.NoError(t, tx.QueryRowContext(ctx,
					"SELECT set_config('statement_timeout', '3000', false)").Scan(&ret))
				assert.Equal(t, "3s", ret)
				assert.Equal(t, "3s", show(t, tx), "session set effective inside txn")

				require.NoError(t, tx.Commit())
				assert.Equal(t, "3s", show(t, db), "session set persists after commit")
			})

			// -----------------------------------------------------------------
			// Enforcement (ground truth: does a slow query actually get cancelled?)
			// -----------------------------------------------------------------

			// A transaction-local statement_timeout set via set_config(..., true) must
			// actually cancel a slow query in the same transaction.
			t.Run("local set_config(true) inside txn enforces timeout", func(t *testing.T) {
				baseline(t)

				tx, err := db.BeginTx(ctx, nil)
				require.NoError(t, err)
				defer func() { _ = tx.Rollback() }()

				var ret string
				require.NoError(t, tx.QueryRowContext(ctx,
					"SELECT set_config('statement_timeout', '300ms', true)").Scan(&ret))
				assert.Equal(t, "300ms", ret)
				require.Equal(t, "300ms", show(t, tx), "local override effective inside txn")

				start := time.Now()
				_, err = tx.ExecContext(ctx, "SELECT pg_sleep(3)")
				elapsed := time.Since(start)
				assertQueryCanceled(t, err)
				assert.Less(t, elapsed, 2*time.Second, "timeout should fire well before the 3s sleep completes")

				require.NoError(t, tx.Rollback())

				// After the transaction ends the local override is gone (baseline 0),
				// so a short sleep completes.
				_, err = db.ExecContext(ctx, "SELECT pg_sleep(1)")
				require.NoError(t, err, "local override must not leak past the transaction")
			})

			// Sanity: session-level set_config(..., false) enforces the timeout too.
			t.Run("session set_config(false) enforces timeout", func(t *testing.T) {
				baseline(t)

				_, err := db.ExecContext(ctx,
					"SELECT set_config('statement_timeout', '300ms', false)")
				require.NoError(t, err)

				start := time.Now()
				_, err = db.ExecContext(ctx, "SELECT pg_sleep(3)")
				elapsed := time.Since(start)
				assertQueryCanceled(t, err)
				assert.Less(t, elapsed, 2*time.Second)

				baseline(t) // disable so it doesn't affect later subtests
			})

			// Sanity: local set_config(..., true) OUTSIDE a transaction must NOT
			// enforce on the next statement (it was scoped to its own implicit txn).
			t.Run("local set_config(true) outside txn does not enforce next query", func(t *testing.T) {
				baseline(t)

				_, err := db.ExecContext(ctx,
					"SELECT set_config('statement_timeout', '300ms', true)")
				require.NoError(t, err)

				// Baseline is 0 (disabled) on both targets, so this must complete.
				_, err = db.ExecContext(ctx, "SELECT pg_sleep(1)")
				require.NoError(t, err,
					"local set_config outside a transaction must not enforce on a later statement")
			})
		})
	}
}

// TestStatementTimeoutSetConfigScopeBatch covers a transaction-local
// statement_timeout applied alongside a batch of other transaction-local
// set_config(...) calls (a role change and several custom GUCs) run as one
// parameterized SELECT. It pins the three orderings that matter:
//
//   - statement_timeout set inside the batch;
//   - statement_timeout set first via set_config(..., true), then a later batch that
//     does not mention it;
//   - statement_timeout set first via SET LOCAL, then a later batch that does not
//     mention it.
//
// In every case the effective transaction timeout (via SHOW) must be the value set,
// and a later batch that omits statement_timeout must not disturb it. Dual-target:
// postgres grounds the expected behavior, so any multigateway divergence fails.
func TestStatementTimeoutSetConfigScopeBatch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping set_config scope-batch test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping set_config scope-batch test")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	// A batch of transaction-local set_config(...) calls — a role change plus several
	// custom GUCs — applied as one parameterized SELECT, with statement_timeout ($10)
	// included at the end. One value ('true') is a literal to exercise a literal mixed
	// in with bound args.
	const localBatch = `SELECT
		set_config('role', $1, true),
		set_config('app.claim_role', $2, true),
		set_config('app.token', $3, true),
		set_config('app.claim_sub', $4, true),
		set_config('app.claims', $5, true),
		set_config('app.headers', $6, true),
		set_config('app.method', $7, true),
		set_config('app.path', $8, true),
		set_config('app.operation', $9, true),
		set_config('app.allow_delete', 'true', true),
		set_config('statement_timeout', $10, true)`

	// The same batch without statement_timeout — binds $1..$9 only.
	const localBatchNoTimeout = `SELECT
		set_config('role', $1, true),
		set_config('app.claim_role', $2, true),
		set_config('app.token', $3, true),
		set_config('app.claim_sub', $4, true),
		set_config('app.claims', $5, true),
		set_config('app.headers', $6, true),
		set_config('app.method', $7, true),
		set_config('app.path', $8, true),
		set_config('app.operation', $9, true),
		set_config('app.allow_delete', 'true', true)`

	// $1..$9 for both batches. localBatch additionally binds statement_timeout as $10.
	batchValues := []any{
		"postgres", // $1 role — must exist in the test cluster
		"authenticated",
		"token-abc",
		"subject-123",
		`{"key":"value"}`,
		`{"x-forwarded-for":"127.0.0.1"}`,
		"GET",
		"/some/path",
		"op",
	}

	for _, target := range setup.GetComparisonTargets(t) {
		t.Run(target.Name, func(t *testing.T) {
			connStr := shardsetup.GetTestUserDSN("localhost", target.Port, "sslmode=disable", "connect_timeout=5")
			ctx := utils.WithTimeout(t, 150*time.Second)

			// Each subtest uses its own single-backend connection so transaction-local
			// GUC state from one scenario can't bleed into the next.
			newConn := func(t *testing.T) *sql.DB {
				t.Helper()
				db, err := sql.Open("postgres", connStr)
				require.NoError(t, err)
				db.SetMaxOpenConns(1)
				t.Cleanup(func() { _ = db.Close() })
				require.NoError(t, db.PingContext(ctx))
				return db
			}

			// These scenarios assert the effective statement_timeout VALUE via SHOW,
			// not enforcement. A generous value (30s) keeps the batch itself from ever
			// running under a tight deadline — that would exercise per-query latency,
			// not set_config semantics. Enforcement is covered by
			// TestStatementTimeoutSetConfig.

			// statement_timeout included in the batch is the transaction's effective value.
			t.Run("statement_timeout set inside the batch", func(t *testing.T) {
				db := newConn(t)
				_, err := db.ExecContext(ctx, "SET statement_timeout = 0")
				require.NoError(t, err)

				tx, err := db.BeginTx(ctx, nil)
				require.NoError(t, err)
				defer func() { _ = tx.Rollback() }()

				withTimeout := append(append([]any{}, batchValues...), "30s")
				_, err = tx.ExecContext(ctx, localBatch, withTimeout...)
				require.NoError(t, err, "batch should succeed")

				var shown string
				require.NoError(t, tx.QueryRowContext(ctx, "SHOW statement_timeout").Scan(&shown))
				assert.Equal(t, "30s", shown,
					"statement_timeout from the batch must be the effective value")
			})

			// statement_timeout set on its own, then a later batch that doesn't mention
			// it — the timeout must survive the batch.
			t.Run("statement_timeout then a later batch without it", func(t *testing.T) {
				db := newConn(t)
				_, err := db.ExecContext(ctx, "SET statement_timeout = 0")
				require.NoError(t, err)

				tx, err := db.BeginTx(ctx, nil)
				require.NoError(t, err)
				defer func() { _ = tx.Rollback() }()

				var applied string
				require.NoError(t, tx.QueryRowContext(ctx,
					"SELECT set_config('statement_timeout', $1, true)", "30s").Scan(&applied))
				require.Equal(t, "30s", applied)

				var afterSet string
				require.NoError(t, tx.QueryRowContext(ctx, "SHOW statement_timeout").Scan(&afterSet))
				require.Equal(t, "30s", afterSet, "statement_timeout effective right after being set")

				_, err = tx.ExecContext(ctx, localBatchNoTimeout, batchValues...)
				require.NoError(t, err, "batch should succeed")

				var afterBatch string
				require.NoError(t, tx.QueryRowContext(ctx, "SHOW statement_timeout").Scan(&afterBatch))
				assert.Equal(t, "30s", afterBatch,
					"a later batch that omits statement_timeout must not reset it")
			})

			// Same as above but statement_timeout is set via SET LOCAL rather than
			// set_config(..., true).
			t.Run("SET LOCAL statement_timeout then a later batch without it", func(t *testing.T) {
				db := newConn(t)
				_, err := db.ExecContext(ctx, "SET statement_timeout = 0")
				require.NoError(t, err)

				tx, err := db.BeginTx(ctx, nil)
				require.NoError(t, err)
				defer func() { _ = tx.Rollback() }()

				_, err = tx.ExecContext(ctx, "SET LOCAL statement_timeout = '30s'")
				require.NoError(t, err)

				var afterSet string
				require.NoError(t, tx.QueryRowContext(ctx, "SHOW statement_timeout").Scan(&afterSet))
				require.Equal(t, "30s", afterSet)

				_, err = tx.ExecContext(ctx, localBatchNoTimeout, batchValues...)
				require.NoError(t, err, "batch should succeed")

				var afterBatch string
				require.NoError(t, tx.QueryRowContext(ctx, "SHOW statement_timeout").Scan(&afterBatch))
				assert.Equal(t, "30s", afterBatch,
					"a later batch that omits statement_timeout must not reset the SET LOCAL value")
			})
		})
	}
}

// TestStatementTimeoutSetConfigBackendLeak pins the invariant deterministically:
// after a session set_config followed by a reset, the value the session reports and
// the value on the backend must agree. `SHOW statement_timeout` reports the
// gateway-managed value; `SELECT setting FROM pg_settings WHERE name =
// 'statement_timeout'` reads the pooled backend's real GUC (a catalog view read,
// never rewritten — unlike current_setting, which the gateway now spoofs to the
// gateway value). If a set_config('statement_timeout', ...) ever runs on the pooled
// backend, resetting it at the gateway can't reach it, leaving the backend value
// stale — a leak across pooled clients.
//
// On native PostgreSQL, SHOW and pg_settings always agree, so any divergence on
// multigateway is a leak.
func TestStatementTimeoutSetConfigBackendLeak(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping set_config backend-leak test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping set_config backend-leak test")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	for _, target := range setup.GetComparisonTargets(t) {
		t.Run(target.Name, func(t *testing.T) {
			connStr := shardsetup.GetTestUserDSN("localhost", target.Port, "sslmode=disable", "connect_timeout=5")
			db, err := sql.Open("postgres", connStr)
			require.NoError(t, err)
			defer db.Close()
			db.SetMaxOpenConns(1)

			ctx := utils.WithTimeout(t, 150*time.Second)
			require.NoError(t, db.PingContext(ctx))

			backend := func(t *testing.T) string {
				t.Helper()
				var v string
				require.NoError(t, db.QueryRowContext(ctx,
					"SELECT setting FROM pg_settings WHERE name = 'statement_timeout'").Scan(&v))
				return v
			}
			gateway := func(t *testing.T) string {
				t.Helper()
				var v string
				require.NoError(t, db.QueryRowContext(ctx, "SHOW statement_timeout").Scan(&v))
				return v
			}

			_, err = db.ExecContext(ctx, "SET statement_timeout = 0")
			require.NoError(t, err)

			// A session-scoped set_config takes effect for the session.
			_, err = db.ExecContext(ctx, "SELECT set_config('statement_timeout', '1000', false)")
			require.NoError(t, err)
			require.Equal(t, "1s", gateway(t), "gateway effective value after set_config")

			// Reset the timeout the way a client would. The backend GUC must follow:
			// on multigateway, set_config('statement_timeout', ...) is answered by the
			// gateway and never written to the backend, so it never goes stale here. A
			// stale non-zero backend value would mean the set_config leaked onto the
			// pooled connection.
			_, err = db.ExecContext(ctx, "SET statement_timeout = 0")
			require.NoError(t, err)
			assert.Equal(t, "0", gateway(t), "gateway reset to 0")
			assert.Equal(t, "0", backend(t),
				"backend statement_timeout must be 0 after reset — a stale non-zero value is the leak")
		})
	}
}

// TestStatementTimeoutSetConfigMixed covers a SELECT that sets BOTH a
// gateway-managed variable (statement_timeout) and an ordinary one (work_mem) in
// one set_config batch — the "mixed" case. The gateway-managed call must be
// applied to gateway state and returned to the client without persisting on the
// backend (no leak), while the ordinary call still runs on the backend as usual.
//
// It runs the batch on BOTH the simple query protocol (literal args) and the
// extended protocol (bound args) — the gateway-managed call is rewritten out of
// the backend query in both, so no GUC is persisted on the backend. All
// assertions are grounded in native PostgreSQL via GetComparisonTargets.
func TestStatementTimeoutSetConfigMixed(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping mixed set_config test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping mixed set_config test")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	// checkMixed runs one mixed set_config batch on a fresh connection and asserts
	// the gateway-managed value is returned/effective, the ordinary one is applied
	// on the backend, and no backend statement_timeout leaks after a reset. query
	// carries $1 (statement_timeout) and $2 (work_mem) when args are provided
	// (extended protocol); otherwise the values are inlined (simple protocol).
	checkMixed := func(t *testing.T, connStr, query string, args ...any) {
		t.Helper()
		db, err := sql.Open("postgres", connStr)
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)

		ctx := utils.WithTimeout(t, 150*time.Second)
		require.NoError(t, db.PingContext(ctx))

		_, err = db.ExecContext(ctx, "SET statement_timeout = 0")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "RESET work_mem")
		require.NoError(t, err)

		// '1000' (ms) must come back canonicalized as '1s', exactly like PostgreSQL.
		var st, wm string
		require.NoError(t, db.QueryRowContext(ctx, query, args...).Scan(&st, &wm))
		assert.Equal(t, "1s", st, "gateway-managed set_config must return the canonical value")
		assert.Equal(t, "64MB", wm, "ordinary set_config must return its value")

		// Both must be the effective session values.
		var shownST, shownWM string
		require.NoError(t, db.QueryRowContext(ctx, "SHOW statement_timeout").Scan(&shownST))
		assert.Equal(t, "1s", shownST)
		require.NoError(t, db.QueryRowContext(ctx, "SHOW work_mem").Scan(&shownWM))
		assert.Equal(t, "64MB", shownWM, "ordinary variable must still be applied on the backend")

		// Reset statement_timeout; the backend GUC must not carry a stale value.
		// (On multigateway the mixed batch never writes the backend's
		// statement_timeout since the gateway-managed call is rewritten out, so this
		// gateway-handled reset leaves nothing stale on the backend.)
		_, err = db.ExecContext(ctx, "SET statement_timeout = 0")
		require.NoError(t, err)
		var beST string
		require.NoError(t, db.QueryRowContext(ctx,
			"SELECT setting FROM pg_settings WHERE name = 'statement_timeout'").Scan(&beST))
		assert.Equal(t, "0", beST,
			"backend statement_timeout must be 0 after reset — a stale value is the leak")
	}

	for _, target := range setup.GetComparisonTargets(t) {
		t.Run(target.Name, func(t *testing.T) {
			connStr := shardsetup.GetTestUserDSN("localhost", target.Port, "sslmode=disable", "connect_timeout=5")

			// Simple protocol: no bind params, literal args in the SQL text.
			t.Run("simple protocol", func(t *testing.T) {
				checkMixed(t, connStr,
					"SELECT set_config('statement_timeout', '1000', false), set_config('work_mem', '64MB', false)")
			})

			// Extended protocol: bound value args. The gateway-managed value ($1) is
			// resolved and canonicalized at execute time and rewritten out of the
			// backend query, so the extended path does not leak either.
			t.Run("extended protocol", func(t *testing.T) {
				checkMixed(t, connStr,
					"SELECT set_config('statement_timeout', $1, false), set_config('work_mem', $2, false)",
					"1000", "64MB")
			})
		})
	}
}

// TestStatementTimeoutSetConfigSharedBoundValue covers the shared-param edge: a
// single bind ($1) used BOTH as a gateway-managed set_config value AND elsewhere in
// the same query. Canonicalizing $1 in place would corrupt its other use, so the
// gateway routes the set_config value through a fresh synthetic bind slot (sourced
// from $1 but canonicalized independently) and leaves $1 untouched. Critically, the
// set_config is still rewritten out of the backend query — the earlier behavior of
// leaving this pattern for the backend leaked the real GUC. Dual-target: PostgreSQL
// grounds both the returned values and the no-leak assertion.
func TestStatementTimeoutSetConfigSharedBoundValue(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping shared bound value set_config test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping shared bound value set_config test")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	for _, target := range setup.GetComparisonTargets(t) {
		t.Run(target.Name, func(t *testing.T) {
			connStr := shardsetup.GetTestUserDSN("localhost", target.Port, "sslmode=disable", "connect_timeout=5")
			db, err := sql.Open("postgres", connStr)
			require.NoError(t, err)
			defer db.Close()
			db.SetMaxOpenConns(1)

			ctx := utils.WithTimeout(t, 150*time.Second)
			require.NoError(t, db.PingContext(ctx))

			_, err = db.ExecContext(ctx, "SET statement_timeout = 0")
			require.NoError(t, err)

			// $1 is the statement_timeout value AND a second projected column.
			// Extended protocol (bound arg), so $1 is a real client param used twice.
			var st, raw string
			require.NoError(t, db.QueryRowContext(ctx,
				"SELECT set_config('statement_timeout', $1, false), $1", "1000").Scan(&st, &raw))
			assert.Equal(t, "1s", st, "the gateway-managed value returns canonicalized")
			assert.Equal(t, "1000", raw, "the shared param's other use is untouched (raw value)")

			// Effective session value is the gateway-managed one.
			var shownST string
			require.NoError(t, db.QueryRowContext(ctx, "SHOW statement_timeout").Scan(&shownST))
			assert.Equal(t, "1s", shownST)

			// No backend leak: after a reset, the backend GUC must be 0.
			_, err = db.ExecContext(ctx, "SET statement_timeout = 0")
			require.NoError(t, err)
			var beST string
			require.NoError(t, db.QueryRowContext(ctx,
				"SELECT setting FROM pg_settings WHERE name = 'statement_timeout'").Scan(&beST))
			assert.Equal(t, "0", beST,
				"backend statement_timeout must be 0 — a stale value is the leak this pattern used to cause")
		})
	}
}

// TestSetConfigGatewayManagedVariables covers set_config for EACH gateway-managed
// variable and a batch that sets more than one at once. Every gateway-managed
// variable must be answered by the rewrite path — its set_config is rewritten out
// of the backend query, returns the canonical value, is the effective session
// value, and leaves no backend GUC behind. idle_session_timeout was added on main
// (#1206); this is the regression that keeps it (and any future GMV) working with
// the set_config rewrite. If a GMV is missing from GatewayManagedCanonicalValue,
// its set_config fails with the internal "please report this as a bug" error.
func TestSetConfigGatewayManagedVariables(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping gateway-managed set_config test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping gateway-managed set_config test")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	// name → a value and its canonical (PostgreSQL) form. Both current GMVs are
	// GUC_UNIT_MS timeouts.
	gmvs := []struct{ name, value, canonical string }{
		{"statement_timeout", "2000", "2s"},
		{"idle_session_timeout", "5000", "5s"},
	}

	for _, target := range setup.GetComparisonTargets(t) {
		t.Run(target.Name, func(t *testing.T) {
			connStr := shardsetup.GetTestUserDSN("localhost", target.Port, "sslmode=disable", "connect_timeout=5")
			ctx := utils.WithTimeout(t, 150*time.Second)

			// Each gateway-managed variable, on its own. The variable name is a
			// literal (how the rewrite path recognizes a GMV at plan time), with the
			// value bound ($1) — the extended protocol.
			for _, v := range gmvs {
				t.Run(v.name, func(t *testing.T) {
					db, err := sql.Open("postgres", connStr)
					require.NoError(t, err)
					defer db.Close()
					db.SetMaxOpenConns(1)
					require.NoError(t, db.PingContext(ctx))

					_, err = db.ExecContext(ctx, "SET "+v.name+" = 0")
					require.NoError(t, err)

					var ret string
					require.NoError(t, db.QueryRowContext(ctx,
						"SELECT set_config('"+v.name+"', $1, false)", v.value).Scan(&ret))
					assert.Equal(t, v.canonical, ret, "set_config must return the canonical value")

					var shown string
					require.NoError(t, db.QueryRowContext(ctx, "SHOW "+v.name).Scan(&shown))
					assert.Equal(t, v.canonical, shown, "must be the effective session value")

					// Reset at the gateway; the backend GUC must not be left stale.
					_, err = db.ExecContext(ctx, "SET "+v.name+" = 0")
					require.NoError(t, err)
					var backend string
					require.NoError(t, db.QueryRowContext(ctx,
						"SELECT setting FROM pg_settings WHERE name = '"+v.name+"'").Scan(&backend))
					assert.Equal(t, "0", backend, "no backend GUC leak after reset")
				})
			}

			// A single batch setting BOTH gateway-managed variables — exercises
			// multiple rewrite columns in one GatewayManagedValueRoute.
			t.Run("batch of multiple gateway-managed variables", func(t *testing.T) {
				db, err := sql.Open("postgres", connStr)
				require.NoError(t, err)
				defer db.Close()
				db.SetMaxOpenConns(1)
				require.NoError(t, db.PingContext(ctx))

				_, err = db.ExecContext(ctx, "SET statement_timeout = 0")
				require.NoError(t, err)
				_, err = db.ExecContext(ctx, "SET idle_session_timeout = 0")
				require.NoError(t, err)

				var st, idle string
				require.NoError(t, db.QueryRowContext(ctx,
					"SELECT set_config('statement_timeout', '2000', false), set_config('idle_session_timeout', '5000', false)").
					Scan(&st, &idle))
				assert.Equal(t, "2s", st)
				assert.Equal(t, "5s", idle)

				var shownST, shownIdle string
				require.NoError(t, db.QueryRowContext(ctx, "SHOW statement_timeout").Scan(&shownST))
				require.NoError(t, db.QueryRowContext(ctx, "SHOW idle_session_timeout").Scan(&shownIdle))
				assert.Equal(t, "2s", shownST)
				assert.Equal(t, "5s", shownIdle)
			})
		})
	}
}

// TestStatementTimeoutSetConfigNullValue pins the behavior of a NULL value
// argument. In PostgreSQL, set_config(name, NULL, _) resets the parameter to its
// default and returns the resulting value. Multigres does NOT support a NULL
// set_config value for ANY variable (gateway-managed or not): the planner rejects
// it up front with "set_config value argument must be a literal constant or a
// bound parameter", so it never reaches the gateway-managed synthesize/rewrite
// paths. This is a pre-existing limitation, not specific to this change; the test
// documents the divergence so a future change to NULL handling is intentional.
func TestStatementTimeoutSetConfigNullValue(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping NULL set_config test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping NULL set_config test")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	// bare and mixed shapes; both must behave the same way per target.
	queries := map[string]string{
		"bare":  "SELECT set_config('statement_timeout', NULL, false)",
		"mixed": "SELECT set_config('statement_timeout', NULL, false), set_config('work_mem', '64MB', false)",
	}

	for _, target := range setup.GetComparisonTargets(t) {
		t.Run(target.Name, func(t *testing.T) {
			connStr := shardsetup.GetTestUserDSN("localhost", target.Port, "sslmode=disable", "connect_timeout=5")
			db, err := sql.Open("postgres", connStr)
			require.NoError(t, err)
			defer db.Close()
			db.SetMaxOpenConns(1)

			ctx := utils.WithTimeout(t, 150*time.Second)
			require.NoError(t, db.PingContext(ctx))

			for shape, q := range queries {
				t.Run(shape, func(t *testing.T) {
					// ExecContext (not QueryContext) so rows are consumed/closed and
					// don't hold the single pooled connection open for the next subtest.
					_, err := db.ExecContext(ctx, q)
					if target.Name == "postgres" {
						// Native PG: NULL resets statement_timeout to its default.
						require.NoError(t, err, "PostgreSQL treats a NULL set_config value as reset-to-default")
					} else {
						// multigateway: NULL set_config values are not supported (rejected
						// by the planner before any gateway-managed handling).
						require.Error(t, err, "multigateway rejects a NULL set_config value")
						assert.Contains(t, err.Error(), "must be a literal constant or a bound parameter",
							"NULL is rejected up front, so it never reaches the gateway-managed paths")
					}
				})
			}
		})
	}
}

// TestCurrentSettingGatewayManaged pins the fix for the current_setting divergence:
// a gateway-managed variable is never applied to the pooled backend, so
// current_setting('<gmv>') evaluated on the backend would see the backend default
// and disagree with SHOW. The gateway rewrites the call to return the gateway-owned
// value, so SHOW and current_setting agree — exactly as they do on native
// PostgreSQL. Grounded on both targets via GetComparisonTargets.
func TestCurrentSettingGatewayManaged(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping current_setting gateway-managed test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping current_setting gateway-managed test")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	gmvs := []struct{ name, value, canonical string }{
		{"statement_timeout", "2000", "2s"},
		{"idle_session_timeout", "5000", "5s"},
	}

	for _, target := range setup.GetComparisonTargets(t) {
		t.Run(target.Name, func(t *testing.T) {
			connStr := shardsetup.GetTestUserDSN("localhost", target.Port, "sslmode=disable", "connect_timeout=5")
			ctx := utils.WithTimeout(t, 150*time.Second)

			for _, v := range gmvs {
				t.Run(v.name, func(t *testing.T) {
					db, err := sql.Open("postgres", connStr)
					require.NoError(t, err)
					defer db.Close()
					db.SetMaxOpenConns(1)
					require.NoError(t, db.PingContext(ctx))

					_, err = db.ExecContext(ctx, "SET "+v.name+" = "+v.value)
					require.NoError(t, err)

					var shown string
					require.NoError(t, db.QueryRowContext(ctx, "SHOW "+v.name).Scan(&shown))
					require.Equal(t, v.canonical, shown, "SHOW reports the value we set")

					// Simple protocol: literal name, no binds. Both the two-arg
					// (missing_ok) and one-arg forms must equal SHOW.
					var cs2, cs1 string
					require.NoError(t, db.QueryRowContext(ctx,
						"SELECT current_setting('"+v.name+"', true)").Scan(&cs2))
					assert.Equal(t, shown, cs2, "current_setting(name, true) must agree with SHOW")
					require.NoError(t, db.QueryRowContext(ctx,
						"SELECT current_setting('"+v.name+"')").Scan(&cs1))
					assert.Equal(t, shown, cs1, "current_setting(name) must agree with SHOW")

					// Extended protocol: an unrelated bound param alongside the
					// literal-named current_setting exercises the portal decode +
					// synthetic read-slot path. The passthrough is text so the whole
					// result stays text-format (GatewayManagedValueRoute re-runs the
					// portal as a simple query, which only emits text).
					var csExt, passthrough string
					require.NoError(t, db.QueryRowContext(ctx,
						"SELECT current_setting('"+v.name+"'), $1::text", "passthrough").Scan(&csExt, &passthrough))
					assert.Equal(t, shown, csExt, "current_setting must agree with SHOW over the extended protocol")
					assert.Equal(t, "passthrough", passthrough, "the client's bound param is untouched by the rewrite")

					// A transaction-local override must be reflected too.
					tx, err := db.BeginTx(ctx, nil)
					require.NoError(t, err)
					_, err = tx.ExecContext(ctx, "SET LOCAL "+v.name+" = 0")
					require.NoError(t, err)
					var local string
					require.NoError(t, tx.QueryRowContext(ctx,
						"SELECT current_setting('"+v.name+"', true)").Scan(&local))
					assert.Equal(t, "0", local, "current_setting reflects the SET LOCAL override")
					require.NoError(t, tx.Rollback())
				})
			}
		})
	}
}

// TestCurrentSettingMaterialized covers the statements that materialize a
// gateway-managed current_setting once (CREATE TABLE AS, SELECT INTO): the stored
// row must hold the gateway value (matching native PostgreSQL), and the bare call
// must keep its "current_setting" column name so the new table's column is named
// like PostgreSQL's. Grounded on both targets via GetComparisonTargets. A
// materialized view is deliberately NOT covered here — it is not rewritten (so its
// data reflects the backend, diverging from native PG on purpose to keep the stored
// query refreshable); the not-rewritten decision is asserted in the planner test.
func TestCurrentSettingMaterialized(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping current_setting materialize test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping current_setting materialize test")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	for _, target := range setup.GetComparisonTargets(t) {
		t.Run(target.Name, func(t *testing.T) {
			connStr := shardsetup.GetTestUserDSN("localhost", target.Port, "sslmode=disable", "connect_timeout=5")
			db, err := sql.Open("postgres", connStr)
			require.NoError(t, err)
			defer db.Close()
			db.SetMaxOpenConns(1)

			ctx := utils.WithTimeout(t, 150*time.Second)
			require.NoError(t, db.PingContext(ctx))

			_, err = db.ExecContext(ctx, "SET statement_timeout = 2000")
			require.NoError(t, err)
			var shown string
			require.NoError(t, db.QueryRowContext(ctx, "SHOW statement_timeout").Scan(&shown))
			require.Equal(t, "2s", shown)

			// Each shape stores the resolved value under a "current_setting" column
			// (bare call → PostgreSQL names the column "current_setting"; the rewrite
			// preserves it). Querying that column by name also proves the name survived.
			materializers := map[string]string{
				"CREATE TABLE AS": "CREATE TABLE cs_ctas AS SELECT current_setting('statement_timeout')",
				"SELECT INTO":     "SELECT current_setting('statement_timeout') INTO cs_into",
			}
			tables := map[string]string{"CREATE TABLE AS": "cs_ctas", "SELECT INTO": "cs_into"}

			for name, create := range materializers {
				t.Run(name, func(t *testing.T) {
					tbl := tables[name]
					_, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+tbl)
					require.NoError(t, err)
					_, err = db.ExecContext(ctx, create)
					require.NoError(t, err)

					var v string
					require.NoError(t, db.QueryRowContext(ctx,
						"SELECT current_setting FROM "+tbl).Scan(&v))
					assert.Equal(t, shown, v, "the materialized value must be the gateway value")
				})
			}
		})
	}
}

// querier is the subset of database/sql shared by *sql.DB and *sql.Tx that the
// SHOW helper needs.
type querier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// assertQueryCanceled asserts err is a PostgreSQL query_canceled (57014) error,
// as raised by statement_timeout enforcement.
func assertQueryCanceled(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err, "query should be cancelled by statement_timeout")
	var pqErr *pq.Error
	require.True(t, errors.As(err, &pqErr), "expected pq.Error, got %T: %v", err, err)
	assert.Equal(t, "57014", string(pqErr.Code), "should be query_canceled (57014)")
}
