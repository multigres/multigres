// Copyright 2025 Supabase, Inc.
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
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestMultigateway_SessionSettings tests that SET/SHOW/RESET commands work correctly
// through the complete client → multigateway → multipooler → PostgreSQL flow.
func TestMultigateway_SessionSettings(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping session settings test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping session settings tests")
	}

	// Use shared test cluster with multigateway
	setup := getSharedSetup(t)
	setup.SetupTest(t)

	// Connect to multigateway
	connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err, "failed to open database connection")
	defer db.Close()

	ctx := utils.WithTimeout(t, 150*time.Second)

	err = db.PingContext(ctx)
	require.NoError(t, err, "failed to ping database - multigateway may not be ready")

	// Test 1: Basic SET and SHOW
	t.Run("basic SET and SHOW", func(t *testing.T) {
		// Execute SET command
		_, err := db.ExecContext(ctx, "SET search_path = 'myschema'")
		require.NoError(t, err, "failed to execute SET")

		// Execute SHOW multiple times to verify consistency (bucket routing)
		for i := range 100 {
			var result string
			err = db.QueryRowContext(ctx, "SHOW search_path").Scan(&result)
			require.NoError(t, err, "failed to execute SHOW on iteration %d", i)

			require.Equal(t, "myschema", result, "iteration %d: expected consistent value", i)
			time.Sleep(200 * time.Millisecond)
		}
	})

	// Test 2: Multiple session variables
	t.Run("multiple session variables", func(t *testing.T) {
		// Set multiple variables
		_, err := db.ExecContext(ctx, "SET work_mem = '256MB'")
		require.NoError(t, err, "failed to SET work_mem")

		_, err = db.ExecContext(ctx, "SET statement_timeout = '30s'")
		require.NoError(t, err, "failed to SET statement_timeout")

		_, err = db.ExecContext(ctx, "SET search_path = 'custom'")
		require.NoError(t, err, "failed to SET search_path")

		// Verify all variables (order independent)
		var workMem string
		err = db.QueryRowContext(ctx, "SHOW work_mem").Scan(&workMem)
		require.NoError(t, err, "failed to SHOW work_mem")
		assert.Equal(t, "256MB", workMem, "work_mem should be 256MB")

		var timeout string
		err = db.QueryRowContext(ctx, "SHOW statement_timeout").Scan(&timeout)
		require.NoError(t, err, "failed to SHOW statement_timeout")
		assert.Equal(t, "30s", timeout, "statement_timeout should be 30s (gateway-managed, PG display format)")

		var searchPath string
		err = db.QueryRowContext(ctx, "SHOW search_path").Scan(&searchPath)
		require.NoError(t, err, "failed to SHOW search_path")
		assert.Equal(t, "custom", searchPath, "search_path should be custom")
	})

	// Test 3: Connection pooling with settings
	t.Run("connection pooling with settings", func(t *testing.T) {
		// Use search_path here: this scenario covers session-state
		// persistence across queries on the same logical connection, and
		// search_path does that just as well as any other GUC.
		_, err := db.ExecContext(ctx, "SET search_path = 'test_path_1'")
		require.NoError(t, err, "failed to SET search_path")

		// Execute 100 queries - all should see the same setting
		for i := range 100 {
			var searchPath string
			err = db.QueryRowContext(ctx, "SHOW search_path").Scan(&searchPath)
			require.NoError(t, err, "failed to SHOW search_path on iteration %d", i)
			require.Equal(t, "test_path_1", searchPath, "iteration %d: expected consistent search_path", i)

			// Interleave with other queries
			var one int
			err = db.QueryRowContext(ctx, "SELECT 1").Scan(&one)
			require.NoError(t, err, "failed to execute SELECT on iteration %d", i)
			require.Equal(t, 1, one)
			time.Sleep(200 * time.Millisecond)
		}
	})

	// Test 4: RESET single variable
	t.Run("RESET single variable", func(t *testing.T) {
		// Set custom value
		_, err := db.ExecContext(ctx, "SET search_path = 'custom'")
		require.NoError(t, err, "failed to SET search_path")

		var customPath string
		err = db.QueryRowContext(ctx, "SHOW search_path").Scan(&customPath)
		require.NoError(t, err, "failed to SHOW search_path")
		assert.Equal(t, "custom", customPath, "should show custom value")

		// Reset
		_, err = db.ExecContext(ctx, "RESET search_path")
		require.NoError(t, err, "failed to RESET search_path")

		// Should return to default (which contains "$user", public by default)
		var resetPath string
		err = db.QueryRowContext(ctx, "SHOW search_path").Scan(&resetPath)
		require.NoError(t, err, "failed to SHOW search_path after RESET")
		// After RESET, it should NOT be "custom" anymore
		assert.NotEqual(t, "custom", resetPath, "should not show custom value after RESET")
		// Default typically contains "$user", public
		assert.Contains(t, resetPath, "public", "default should contain public schema")
	})

	// Test 5: RESET ALL clears all variables
	t.Run("RESET ALL clears all variables", func(t *testing.T) {
		// Set multiple variables.
		_, err := db.ExecContext(ctx, "SET work_mem = '512MB'")
		require.NoError(t, err, "failed to SET work_mem")

		_, err = db.ExecContext(ctx, "SET search_path = 'myschema'")
		require.NoError(t, err, "failed to SET search_path")

		_, err = db.ExecContext(ctx, "SET client_min_messages = 'warning'")
		require.NoError(t, err, "failed to SET client_min_messages")

		// Verify custom values are set
		var clientMin string
		err = db.QueryRowContext(ctx, "SHOW client_min_messages").Scan(&clientMin)
		require.NoError(t, err, "failed to SHOW client_min_messages")
		assert.Equal(t, "warning", clientMin, "should show custom client_min_messages before RESET ALL")

		// Execute RESET ALL
		_, err = db.ExecContext(ctx, "RESET ALL")
		require.NoError(t, err, "failed to execute RESET ALL")

		// Verify variables are reset (values changed from custom values)
		var searchPath string
		err = db.QueryRowContext(ctx, "SHOW search_path").Scan(&searchPath)
		require.NoError(t, err, "failed to SHOW search_path after RESET ALL")
		assert.NotEqual(t, "myschema", searchPath, "search_path should not be custom value after RESET ALL")

		// client_min_messages should revert to its default (notice).
		err = db.QueryRowContext(ctx, "SHOW client_min_messages").Scan(&clientMin)
		require.NoError(t, err, "failed to SHOW client_min_messages after RESET ALL")
		assert.NotEqual(t, "warning", clientMin, "client_min_messages should not be custom value after RESET ALL")
	})

	// Test 6: Different value types
	t.Run("different value types", func(t *testing.T) {
		testCases := []struct {
			name     string
			setCmd   string
			showCmd  string
			expected string
		}{
			{
				name:     "unquoted identifier",
				setCmd:   "SET search_path = public",
				showCmd:  "SHOW search_path",
				expected: "public",
			},
			{
				name:     "quoted string",
				setCmd:   "SET search_path = 'myschema'",
				showCmd:  "SHOW search_path",
				expected: "myschema",
			},
			{
				name:     "number with unit",
				setCmd:   "SET work_mem = '256MB'",
				showCmd:  "SHOW work_mem",
				expected: "256MB",
			},
			{
				name:     "comma separated list",
				setCmd:   "SET search_path TO public, pg_catalog",
				showCmd:  "SHOW search_path",
				expected: "public, pg_catalog",
			},
			{
				name:     "boolean value",
				setCmd:   "SET enable_seqscan = off",
				showCmd:  "SHOW enable_seqscan",
				expected: "off",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				execAndVerify(t, db, ctx, tc.setCmd, tc.showCmd, tc.expected)
			})
		}
	})

	// Test 7: Cross-connection isolation
	t.Run("cross-connection isolation", func(t *testing.T) {
		// This test verifies that separate database/sql connection pools maintain
		// independent session state. Each sql.DB object should track its own session
		// settings independently - setting a variable in one connection pool should
		// not affect queries from a different connection pool.

		// Open two connections
		db1, err := sql.Open("postgres", connStr)
		require.NoError(t, err, "failed to open db1")
		defer db1.Close()

		db2, err := sql.Open("postgres", connStr)
		require.NoError(t, err, "failed to open db2")
		defer db2.Close()

		// Ensure connections are established and clean
		_, err = db1.ExecContext(ctx, "RESET ALL")
		require.NoError(t, err, "failed to RESET ALL in db1")
		_, err = db2.ExecContext(ctx, "RESET ALL")
		require.NoError(t, err, "failed to RESET ALL in db2")

		// Set value in connection 1. The cross-connection isolation
		// invariant we want to verify is identical for any GUC;
		// search_path is as good as any.
		_, err = db1.ExecContext(ctx, "SET search_path = 'conn1_path'")
		require.NoError(t, err, "failed to SET in db1")

		// Verify connection 1 sees it multiple times (ensures consistent bucket routing)
		for i := range 100 {
			var path1 string
			err = db1.QueryRowContext(ctx, "SHOW search_path").Scan(&path1)
			require.NoError(t, err, "failed to SHOW in db1 iteration %d", i)
			require.Equal(t, "conn1_path", path1, "db1 should see conn1_path on iteration %d", i)
		}

		// Set different value in connection 2
		_, err = db2.ExecContext(ctx, "SET search_path = 'conn2_path'")
		require.NoError(t, err, "failed to SET in db2")

		// Verify connection 2 sees its own value
		for i := range 100 {
			var path2 string
			err = db2.QueryRowContext(ctx, "SHOW search_path").Scan(&path2)
			require.NoError(t, err, "failed to SHOW in db2 iteration %d", i)
			require.Equal(t, "conn2_path", path2, "db2 should see conn2_path on iteration %d", i)
		}

		// Verify both connections STILL have their independent values after cross-execution
		var path1Final string
		err = db1.QueryRowContext(ctx, "SHOW search_path").Scan(&path1Final)
		require.NoError(t, err, "failed to SHOW in db1 after all operations")
		require.Equal(t, "conn1_path", path1Final, "db1 should still see conn1_path after all operations")

		var path2Final string
		err = db2.QueryRowContext(ctx, "SHOW search_path").Scan(&path2Final)
		require.NoError(t, err, "failed to SHOW in db2 after all operations")
		require.Equal(t, "conn2_path", path2Final, "db2 should still see conn2_path after all operations")
	})

	// Test 8: Extended query protocol with pgx
	t.Run("extended query protocol", func(t *testing.T) {
		// Create pgx connection
		pgxConnStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable")
		conn, err := pgx.Connect(ctx, pgxConnStr)
		require.NoError(t, err, "failed to create pgx connection")
		defer conn.Close(ctx)

		// Execute SET via extended protocol
		_, err = conn.Exec(ctx, "SET work_mem = '128MB'")
		require.NoError(t, err, "failed to execute SET via extended protocol")

		// Verify setting persists consistently across 100 queries
		for i := range 100 {
			var result string
			err = conn.QueryRow(ctx, "SHOW work_mem").Scan(&result)
			require.NoError(t, err, "failed to execute SHOW on iteration %d", i)
			require.Equal(t, "128MB", result, "iteration %d: extended protocol should show correct value", i)

			// Interleave with prepared statement execution
			_, err = conn.Exec(ctx, "SELECT 1")
			require.NoError(t, err, "failed to execute SELECT on iteration %d", i)
		}
	})

	// Test 9: SET and SHOW in same query
	t.Run("SET and SHOW in same query", func(t *testing.T) {
		// Execute multi-statement query
		rows, err := db.QueryContext(ctx, "SET search_path = 'test'; SHOW search_path;")
		require.NoError(t, err, "failed to execute multi-statement query")

		// Should get result from SHOW
		require.True(t, rows.Next(), "expected result from SHOW")
		var result string
		err = rows.Scan(&result)
		require.NoError(t, err, "failed to scan result")
		assert.Equal(t, "test", result, "SHOW should return value set in same query")

		// Close rows before the loop to ensure the connection is released
		// and can be reused for subsequent queries. Otherwise, database/sql
		// will use a different connection for the loop queries, and that
		// connection won't have the session state.
		rows.Close() //nolint:sqlclosecheck // Intentionally not using defer - must close before loop

		// Verify setting persists consistently across 100 queries
		for i := range 100 {
			err = db.QueryRowContext(ctx, "SHOW search_path").Scan(&result)
			require.NoError(t, err, "failed to execute follow-up SHOW on iteration %d", i)
			require.Equal(t, "test", result, "iteration %d: setting should persist after multi-statement query", i)
		}
	})

	// Test 10: Error handling — invalid SET params
	//
	// SET is validated against PostgreSQL at SET time (the gateway runs
	// set_config(..., is_local := true) before tracking it), so an unrecognized
	// variable errors immediately and is never tracked — matching PostgreSQL,
	// and leaving the session uncorrupted (no RESET-to-recover dance).
	t.Run("error handling", func(t *testing.T) {
		// Set valid value first
		_, err := db.ExecContext(ctx, "SET work_mem = '64MB'")
		require.NoError(t, err, "failed to SET valid value")

		// An unrecognized variable now errors at SET time and is not tracked.
		_, err = db.ExecContext(ctx, "SET invalid_variable_12345 = 'value'")
		require.Error(t, err, "invalid SET should error at SET time (validated against PG)")
		assert.Contains(t, err.Error(), "unrecognized configuration parameter",
			"should be PostgreSQL's unrecognized-parameter error")

		// The rejected SET corrupts nothing: the connection stays healthy and the
		// earlier valid setting is intact.
		var result string
		for i := range 100 {
			err = db.QueryRowContext(ctx, "SHOW work_mem").Scan(&result)
			require.NoError(t, err, "iteration %d: connection should remain usable", i)
			require.Equal(t, "64MB", result, "iteration %d: work_mem should still be 64MB", i)

			var one int
			err = db.QueryRowContext(ctx, "SELECT 1").Scan(&one)
			require.NoError(t, err, "iteration %d: connection should be usable", i)
			require.Equal(t, 1, one)
		}
	})

	// Test 11: SET and SET LOCAL behavior inside transactions.
	//
	// Exercises the full client → gateway → multipooler → PostgreSQL flow for
	// SET commands issued inside an explicit transaction:
	//   - a regular SET inside a txn is discarded on ROLLBACK, kept on COMMIT
	//   - SET LOCAL on a gateway-managed variable (statement_timeout) applies
	//     within the txn and reverts at COMMIT — it must not survive the boundary
	//   - SET LOCAL on a non-managed variable (search_path) is passed through to
	//     PostgreSQL and likewise does not leak past COMMIT
	//
	// Session state is per gateway connection, so all statements run on a single
	// pinned *sql.Conn. The follow-up SHOW loops still route through the pool, so
	// they also verify the gateway re-applies the post-txn state to fresh backends.
	t.Run("SET LOCAL behavior", func(t *testing.T) {
		conn, err := db.Conn(ctx)
		require.NoError(t, err, "failed to pin connection")
		defer conn.Close()

		// Start from a clean, known session state.
		_, err = conn.ExecContext(ctx, "RESET ALL")
		require.NoError(t, err, "failed to RESET ALL")

		t.Run("regular SET inside txn is discarded on ROLLBACK", func(t *testing.T) {
			_, err := conn.ExecContext(ctx, "SET search_path = 'before_txn'")
			require.NoError(t, err, "failed to SET pre-txn search_path")

			tx, err := conn.BeginTx(ctx, nil)
			require.NoError(t, err, "failed to BEGIN")

			_, err = tx.ExecContext(ctx, "SET search_path = 'inside_txn'")
			require.NoError(t, err, "failed to SET inside txn")

			var inside string
			err = tx.QueryRowContext(ctx, "SHOW search_path").Scan(&inside)
			require.NoError(t, err, "failed to SHOW inside txn")
			assert.Equal(t, "inside_txn", inside, "SET inside txn should be visible within the txn")

			require.NoError(t, tx.Rollback(), "failed to ROLLBACK")

			// ROLLBACK discards the in-txn SET; search_path reverts to the
			// pre-txn value.
			for i := range 10 {
				var after string
				err = conn.QueryRowContext(ctx, "SHOW search_path").Scan(&after)
				require.NoError(t, err, "iteration %d: failed to SHOW after rollback", i)
				require.Equal(t, "before_txn", after, "iteration %d: ROLLBACK should discard in-txn SET", i)
			}
		})

		t.Run("regular SET inside txn persists on COMMIT", func(t *testing.T) {
			_, err := conn.ExecContext(ctx, "SET search_path = 'before_commit'")
			require.NoError(t, err, "failed to SET pre-commit search_path")

			tx, err := conn.BeginTx(ctx, nil)
			require.NoError(t, err, "failed to BEGIN")

			_, err = tx.ExecContext(ctx, "SET search_path = 'after_commit'")
			require.NoError(t, err, "failed to SET inside txn")
			require.NoError(t, tx.Commit(), "failed to COMMIT")

			// COMMIT makes the in-txn SET the persistent session value.
			for i := range 10 {
				var after string
				err = conn.QueryRowContext(ctx, "SHOW search_path").Scan(&after)
				require.NoError(t, err, "iteration %d: failed to SHOW after commit", i)
				require.Equal(t, "after_commit", after, "iteration %d: COMMIT should persist in-txn SET", i)
			}
		})

		t.Run("SET LOCAL statement_timeout reverts at COMMIT", func(t *testing.T) {
			// Establish a session-level value the LOCAL override masks.
			_, err := conn.ExecContext(ctx, "SET statement_timeout = '30s'")
			require.NoError(t, err, "failed to SET session statement_timeout")

			tx, err := conn.BeginTx(ctx, nil)
			require.NoError(t, err, "failed to BEGIN")

			_, err = tx.ExecContext(ctx, "SET LOCAL statement_timeout = '5s'")
			require.NoError(t, err, "failed to SET LOCAL statement_timeout")

			var inside string
			err = tx.QueryRowContext(ctx, "SHOW statement_timeout").Scan(&inside)
			require.NoError(t, err, "failed to SHOW inside txn")
			assert.Equal(t, "5s", inside, "SET LOCAL should apply within the txn")

			require.NoError(t, tx.Commit(), "failed to COMMIT")

			// SET LOCAL does not survive the transaction; the session value is restored.
			for i := range 10 {
				var after string
				err = conn.QueryRowContext(ctx, "SHOW statement_timeout").Scan(&after)
				require.NoError(t, err, "iteration %d: failed to SHOW after commit", i)
				require.Equal(t, "30s", after, "iteration %d: SET LOCAL must not persist past COMMIT", i)
			}
		})

		t.Run("SET LOCAL on non-managed variable does not leak past COMMIT", func(t *testing.T) {
			// search_path is not gateway-managed: SET LOCAL is passed through to
			// PostgreSQL, which scopes it to the transaction.
			_, err := conn.ExecContext(ctx, "SET search_path = 'session_path'")
			require.NoError(t, err, "failed to SET session search_path")

			tx, err := conn.BeginTx(ctx, nil)
			require.NoError(t, err, "failed to BEGIN")

			_, err = tx.ExecContext(ctx, "SET LOCAL search_path = 'local_path'")
			require.NoError(t, err, "failed to SET LOCAL search_path")

			var inside string
			err = tx.QueryRowContext(ctx, "SHOW search_path").Scan(&inside)
			require.NoError(t, err, "failed to SHOW inside txn")
			assert.Equal(t, "local_path", inside, "SET LOCAL should apply within the txn")

			require.NoError(t, tx.Commit(), "failed to COMMIT")

			// The LOCAL value is gone after COMMIT; the session value remains.
			for i := range 10 {
				var after string
				err = conn.QueryRowContext(ctx, "SHOW search_path").Scan(&after)
				require.NoError(t, err, "iteration %d: failed to SHOW after commit", i)
				require.Equal(t, "session_path", after, "iteration %d: SET LOCAL must not persist past COMMIT", i)
			}
		})

		t.Run("RESET ROLE reverts SET LOCAL ROLE mid-transaction", func(t *testing.T) {
			// A throwaway, unprivileged role to SET LOCAL into. NOLOGIN is fine —
			// we only ever SET ROLE into it as the superuser test connection, we
			// never authenticate as it.
			_, err := conn.ExecContext(ctx, "DROP ROLE IF EXISTS test_reset_role_mid_txn")
			require.NoError(t, err, "failed to drop pre-existing test role")
			_, err = conn.ExecContext(ctx, "CREATE ROLE test_reset_role_mid_txn NOLOGIN")
			require.NoError(t, err, "failed to create test role")
			defer func() {
				_, err := conn.ExecContext(ctx, "DROP ROLE IF EXISTS test_reset_role_mid_txn")
				assert.NoError(t, err, "failed to clean up test role")
			}()

			var original string
			err = conn.QueryRowContext(ctx, "SELECT current_user").Scan(&original)
			require.NoError(t, err, "failed to read original current_user")
			require.NotEqual(t, "test_reset_role_mid_txn", original, "test role must differ from the connection's own login role")

			tx, err := conn.BeginTx(ctx, nil)
			require.NoError(t, err, "failed to BEGIN")

			_, err = tx.ExecContext(ctx, "SET LOCAL ROLE test_reset_role_mid_txn")
			require.NoError(t, err, "failed to SET LOCAL ROLE")

			var duringRole string
			err = tx.QueryRowContext(ctx, "SELECT current_user").Scan(&duringRole)
			require.NoError(t, err, "failed to SELECT current_user after SET LOCAL ROLE")
			require.Equal(t, "test_reset_role_mid_txn", duringRole, "SET LOCAL ROLE should apply within the txn")

			_, err = tx.ExecContext(ctx, "RESET ROLE")
			require.NoError(t, err, "failed to RESET ROLE")

			// This is the regression this test guards: before the fix, RESET ROLE
			// was gateway-tracking-only and never reached the backend that
			// SET LOCAL ROLE had already (correctly) changed for real, so
			// current_user stayed test_reset_role_mid_txn for the rest of the
			// transaction instead of reverting immediately.
			var afterReset string
			err = tx.QueryRowContext(ctx, "SELECT current_user").Scan(&afterReset)
			require.NoError(t, err, "failed to SELECT current_user after RESET ROLE")
			assert.Equal(t, original, afterReset, "RESET ROLE must revert mid-transaction, not just at COMMIT/ROLLBACK")

			require.NoError(t, tx.Rollback(), "failed to ROLLBACK")
		})
	})
}

// TestMultigateway_SetResetGUCRestoration verifies that RESET actually restores
// the GUC value at the PostgreSQL level, not just in the gateway's session tracking.
//
// This catches a specific bug where:
// 1. SET runs on connection A (pool tracks the setting)
// 2. RESET removes from session tracking
// 3. Connection A is returned to the "clean" pool — but PG-side GUC is still dirty
// 4. Subsequent queries may get connection A with the stale GUC value
func TestMultigateway_SetResetGUCRestoration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping GUC restoration test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping GUC restoration tests")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	connStr := fmt.Sprintf("host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable connect_timeout=5",
		setup.MultigatewayPgPort, shardsetup.TestPostgresPassword)

	ctx := utils.WithTimeout(t, 120*time.Second)

	t.Run("SET then RESET restores GUC on all pool connections", func(t *testing.T) {
		db, err := sql.Open("postgres", connStr)
		require.NoError(t, err)
		defer db.Close()

		err = db.PingContext(ctx)
		require.NoError(t, err)

		// Get the default value of extra_float_digits
		var defaultVal string
		err = db.QueryRowContext(ctx, "SHOW extra_float_digits").Scan(&defaultVal)
		require.NoError(t, err, "failed to get default extra_float_digits")
		t.Logf("Default extra_float_digits = %s", defaultVal)

		// Pick a value that differs from the default
		defaultInt, err := strconv.Atoi(defaultVal)
		require.NoError(t, err, "failed to parse default extra_float_digits")
		nonDefaultVal := defaultInt - 1
		nonDefaultStr := strconv.Itoa(nonDefaultVal)

		// SET to a non-default value
		_, err = db.ExecContext(ctx, fmt.Sprintf("SET extra_float_digits = %d", nonDefaultVal))
		require.NoError(t, err, "failed to SET extra_float_digits")

		// Verify it took effect
		var customVal string
		err = db.QueryRowContext(ctx, "SHOW extra_float_digits").Scan(&customVal)
		require.NoError(t, err)
		assert.Equal(t, nonDefaultStr, customVal, "SET should have changed the value")

		// RESET the GUC
		_, err = db.ExecContext(ctx, "RESET extra_float_digits")
		require.NoError(t, err, "failed to RESET extra_float_digits")

		// Now verify across many iterations that the value is truly restored.
		// The bug: connection A still has extra_float_digits=0 at the PG level
		// even though the gateway thinks it's clean. If the pool hands us
		// connection A, SHOW will return 0 instead of the default.
		for i := range 100 {
			var result string
			err = db.QueryRowContext(ctx, "SHOW extra_float_digits").Scan(&result)
			require.NoError(t, err, "iteration %d: failed to SHOW", i)
			require.Equal(t, defaultVal, result,
				"iteration %d: extra_float_digits should be restored to default %q but got %q (stale pool connection)",
				i, defaultVal, result)
		}
	})

	t.Run("SET then RESET ALL restores all GUCs on all pool connections", func(t *testing.T) {
		db, err := sql.Open("postgres", connStr)
		require.NoError(t, err)
		defer db.Close()

		err = db.PingContext(ctx)
		require.NoError(t, err)

		// Get defaults
		var defaultFloatDigits, defaultByteaOutput string
		err = db.QueryRowContext(ctx, "SHOW extra_float_digits").Scan(&defaultFloatDigits)
		require.NoError(t, err)
		err = db.QueryRowContext(ctx, "SHOW bytea_output").Scan(&defaultByteaOutput)
		require.NoError(t, err)

		// Pick a non-default value for extra_float_digits
		defaultInt, err := strconv.Atoi(defaultFloatDigits)
		require.NoError(t, err)
		nonDefaultFloatDigits := defaultInt - 1

		// SET multiple GUCs to non-default values
		_, err = db.ExecContext(ctx, fmt.Sprintf("SET extra_float_digits = %d", nonDefaultFloatDigits))
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "SET bytea_output = 'escape'")
		require.NoError(t, err)

		// RESET ALL
		_, err = db.ExecContext(ctx, "RESET ALL")
		require.NoError(t, err, "failed to RESET ALL")

		// Verify both are restored across many iterations
		for i := range 100 {
			var floatDigits, byteaOut string
			err = db.QueryRowContext(ctx, "SHOW extra_float_digits").Scan(&floatDigits)
			require.NoError(t, err, "iteration %d: failed to SHOW extra_float_digits", i)
			require.Equal(t, defaultFloatDigits, floatDigits,
				"iteration %d: extra_float_digits not restored after RESET ALL", i)

			err = db.QueryRowContext(ctx, "SHOW bytea_output").Scan(&byteaOut)
			require.NoError(t, err, "iteration %d: failed to SHOW bytea_output", i)
			require.Equal(t, defaultByteaOutput, byteaOut,
				"iteration %d: bytea_output not restored after RESET ALL", i)
		}
	})
}

// TestMultigateway_SessionSettingsSQLInjection is the end-to-end regression test
// for https://github.com/multigres/multigres/issues/587.
//
// Session variable values flow client → multigateway → multipooler → PostgreSQL.
// When a pooled connection is checked out, the multipooler applies the tracked
// settings by building a SQL string from Settings.ApplyQuery() (see
// go/services/multipooler/connstate/connection_state.go). That string embeds the
// value inside a pg_catalog.set_config('name', 'value', false) literal.
//
// If the value's single quotes were not escaped by doubling them, a crafted value
// could close the literal and the set_config() call, then append arbitrary
// statements that run on the shared, pooled backend connection — affecting later
// requests routed to the same connection.
//
// The fix (PR #693) escapes single quotes in both the GUC name and value. This
// test proves the whole stack neutralizes the injection: a malicious value is
// stored verbatim as an opaque string and the injected statement never executes.
func TestMultigateway_SessionSettingsSQLInjection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping SQL injection test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping SQL injection tests")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err, "failed to open database connection")
	defer db.Close()

	// Pin to a single backing connection so the SET and the follow-up SHOW are
	// guaranteed to hit the same gateway session (and therefore the same tracked
	// settings) rather than racing across database/sql's idle pool.
	db.SetMaxOpenConns(1)

	ctx := utils.WithTimeout(t, 120*time.Second)

	err = db.PingContext(ctx)
	require.NoError(t, err, "failed to ping database - multigateway may not be ready")

	// A custom (dotted) GUC acts as a placeholder that stores an arbitrary string
	// verbatim, so we can read the exact bytes back with SHOW. The injected
	// payload, if it were not escaped, would break out of the set_config() literal
	// and call set_config() again to define the sentinel GUC below.
	const payloadVar = "mtinject.payload"
	const sentinelVar = "mtinject.sentinel"

	// Crafted to escape the apply query if quotes are not doubled. With broken
	// escaping the multipooler would emit:
	//
	//   SELECT pg_catalog.set_config('mtinject.payload', 'pwned', false);
	//   SELECT set_config('mtinject.sentinel', 'compromised', false); --', false)
	//
	// i.e. the sentinel GUC would get set as a side effect. With correct escaping
	// the whole thing is one opaque string literal.
	payload := `pwned', false); SELECT set_config('` + sentinelVar + `', 'compromised', false); --`

	// Escape the value for the SET command we send to the gateway (client-side
	// quoting), so the gateway receives `payload` as the intended literal value.
	setSQL := fmt.Sprintf("SET %s = '%s'", payloadVar, strings.ReplaceAll(payload, "'", "''"))

	t.Run("malicious value is stored verbatim and injection does not execute", func(t *testing.T) {
		// SET is tracked locally by the gateway and is not validated yet.
		_, err := db.ExecContext(ctx, setSQL)
		require.NoError(t, err, "SET with malicious value should succeed locally")

		// The first query after SET forces the pool to apply the tracked setting
		// to the backend via ApplyQuery(). If escaping were broken this would
		// either error (malformed SQL / wrong set_config arity) or silently run
		// the injected statement. With the fix it succeeds and round-trips.
		var got string
		err = db.QueryRowContext(ctx, "SHOW "+payloadVar).Scan(&got)
		require.NoError(t, err, "applying a setting with single quotes must not break the apply query")
		require.Equal(t, payload, got, "malicious value must be stored verbatim, not interpreted as SQL")

		// The injected statement, if it had executed, would have defined the
		// sentinel placeholder GUC on the backend. It must remain undefined, which
		// PostgreSQL surfaces as an "unrecognized configuration parameter" error.
		var sentinel string
		err = db.QueryRowContext(ctx, "SHOW "+sentinelVar).Scan(&sentinel)
		require.Error(t, err, "injected set_config() must not have run; sentinel GUC should be undefined")
		require.Contains(t, strings.ToLower(err.Error()), "unrecognized configuration parameter",
			"unexpected error for sentinel SHOW: %v", err)
	})

	t.Run("pooled connection remains healthy after malicious setting", func(t *testing.T) {
		// Re-apply the malicious value, then hammer the pool: every checkout must
		// re-apply the setting cleanly and the backend must stay usable. This
		// guards against a partial breakout corrupting the shared connection.
		_, err := db.ExecContext(ctx, setSQL)
		require.NoError(t, err, "re-SET with malicious value should succeed")

		for i := range 50 {
			var got string
			err = db.QueryRowContext(ctx, "SHOW "+payloadVar).Scan(&got)
			require.NoError(t, err, "iteration %d: SHOW after malicious SET should succeed", i)
			require.Equal(t, payload, got, "iteration %d: value must remain verbatim", i)

			var one int
			err = db.QueryRowContext(ctx, "SELECT 1").Scan(&one)
			require.NoError(t, err, "iteration %d: connection should stay usable", i)
			require.Equal(t, 1, one)
		}
	})

	t.Run("connection survives a single quote in the variable name", func(t *testing.T) {
		// The GUC name is embedded in the set_config validation literal, so a
		// single quote in it is escaped (doubled), exactly like the value
		// (covered deterministically by TestSettingsApplyQuerySingleQuoteInName).
		// Such a name is not a valid GUC, so validation now fails at SET time —
		// but it must fail as a clean PostgreSQL error and leave the pooled
		// connection usable, never as a broken-out multi-statement query.
		_, err := db.ExecContext(ctx, `SET "mtinject.it's" = 'value'`)
		require.Error(t, err, "SET with an invalid (quoted) name should error, not inject")

		// The quote must not have escaped the literal: the connection stays
		// usable and the next statement runs normally.
		var one int
		err = db.QueryRowContext(ctx, "SELECT 1").Scan(&one)
		require.NoError(t, err, "connection should be usable after the rejected SET")
		require.Equal(t, 1, one)
	})
}

// TestMultigateway_StringLiteralReconstruction verifies that escape (E”),
// Unicode (U&”), and ordinary string literals retain PostgreSQL semantics when
// the gateway normalizes and reconstructs a single-statement query.
//
// The cases run over the SIMPLE query protocol ('Q'), the path where the
// gateway normalizes a single-statement SELECT and re-emits every literal via
// SqlString()/QuoteStringLiteral. Under the default
// standard_conforming_strings=on this round-trips; under =off a re-emitted bare
// '...' literal is re-interpreted as an escape string, so E” identity and
// backslash contents must still match PostgreSQL (the lexical desync coupled to
// ParameterStatus). The extended protocol forwards verbatim and is not the
// concern here.
func TestMultigateway_StringLiteralReconstruction(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping string literal test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping string literal test")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)
	ctx := utils.WithTimeout(t, 30*time.Second)

	cases := []struct {
		name   string
		sql    string
		scsOff bool
	}{
		{name: "escape_hex", sql: `SELECT E'\x41' AS v`},
		{name: "escape_octal", sql: `SELECT E'\101' AS v`},
		{name: "escape_backslash", sql: `SELECT E'a\\b' AS v`},
		{name: "escape_quote", sql: `SELECT E'a\'b' AS v`},
		{name: "unicode_basic", sql: `SELECT U&'\0041' AS v`},
		{name: "unicode_named_delim", sql: `SELECT U&'!0041' UESCAPE '!' AS v`},
		{name: "unicode_surrogate_pair", sql: `SELECT U&'\D83D\DE00' AS v`},
		{name: "bytea_octal", sql: `SELECT E'\\047'::bytea AS v`},
		{name: "bytea_nul_escape", sql: `SELECT E'De\\000dBeEf'::bytea AS v`},
		{name: "unistr_basic", sql: `SELECT unistr('\0041\0042') AS v`},
		{name: "ordinary_doubled_quote", sql: `SELECT 'a''b' AS v`},

		// standard_conforming_strings = off: a plain '...' literal treats
		// backslashes as escapes, so the gateway's re-emitted literal must
		// preserve PostgreSQL's meaning rather than double-decode.
		{name: "scs_off_backslash_escape", sql: `SELECT '\134' AS v`, scsOff: true},
		{name: "scs_off_octal_quote", sql: `SELECT '\047' AS v`, scsOff: true},
		{name: "scs_off_mixed", sql: `SELECT 'a\\b\'cd' AS v`, scsOff: true},
		{name: "scs_off_escape_string", sql: `SELECT E'a\\b' AS v`, scsOff: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			type outcome struct {
				value  string
				isNil  bool
				errMsg string
			}
			out := map[string]outcome{}
			for _, target := range setup.GetComparisonTargets(t) {
				connStr := shardsetup.GetTestUserDSN("localhost", target.Port, "sslmode=disable")
				conn, err := pgconn.Connect(ctx, connStr)
				require.NoError(t, err)

				if tc.scsOff {
					_, setErr := conn.Exec(ctx, "SET standard_conforming_strings = off").ReadAll()
					require.NoError(t, setErr)
				}

				results, execErr := conn.Exec(ctx, tc.sql).ReadAll()
				var o outcome
				if execErr != nil {
					o.errMsg = execErr.Error()
				} else if len(results) == 1 && len(results[0].Rows) == 1 && len(results[0].Rows[0]) == 1 {
					if results[0].Rows[0][0] == nil {
						o.isNil = true
					} else {
						o.value = string(results[0].Rows[0][0])
					}
				}
				t.Logf("%s: sql=%s value=%q nil=%v err=%q", target.Name, tc.sql, o.value, o.isNil, o.errMsg)
				out[target.Name] = o
				conn.Close(context.Background())
			}
			assert.Equal(t, out["postgres"], out["multigateway"],
				"multigateway literal handling must match PostgreSQL for %s", tc.sql)
		})
	}
}

// Helper Functions

// execAndVerify executes SET and verifies SHOW returns expected value
func execAndVerify(t *testing.T, db *sql.DB, ctx context.Context,
	setCmd string, showCmd string, expected string,
) {
	t.Helper()
	_, err := db.ExecContext(ctx, setCmd)
	require.NoError(t, err, "SET failed: %s", setCmd)

	var result string
	err = db.QueryRowContext(ctx, showCmd).Scan(&result)
	require.NoError(t, err, "SHOW failed: %s", showCmd)
	assert.Equal(t, expected, result, "unexpected value for %s", showCmd)
}
