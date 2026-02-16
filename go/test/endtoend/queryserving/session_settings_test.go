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
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestMultiGateway_SessionSettings tests that SET/SHOW/RESET commands work correctly
// through the complete client → multigateway → multipooler → PostgreSQL flow.
func TestMultiGateway_SessionSettings(t *testing.T) {
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
	connStr := fmt.Sprintf("host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable connect_timeout=5",
		setup.MultigatewayPgPort, shardsetup.TestPostgresPassword)
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
		assert.Equal(t, "30s", timeout, "statement_timeout should be 30s")

		var searchPath string
		err = db.QueryRowContext(ctx, "SHOW search_path").Scan(&searchPath)
		require.NoError(t, err, "failed to SHOW search_path")
		assert.Equal(t, "custom", searchPath, "search_path should be custom")
	})

	// Test 3: Connection pooling with settings
	t.Run("connection pooling with settings", func(t *testing.T) {
		// Set a distinctive value
		_, err := db.ExecContext(ctx, "SET application_name = 'test_app_1'")
		require.NoError(t, err, "failed to SET application_name")

		// Execute 100 queries - all should see the same setting
		for i := range 100 {
			var appName string
			err = db.QueryRowContext(ctx, "SHOW application_name").Scan(&appName)
			require.NoError(t, err, "failed to SHOW application_name on iteration %d", i)
			require.Equal(t, "test_app_1", appName, "iteration %d: expected consistent app name", i)

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
		// Set multiple variables
		_, err := db.ExecContext(ctx, "SET work_mem = '512MB'")
		require.NoError(t, err, "failed to SET work_mem")

		_, err = db.ExecContext(ctx, "SET search_path = 'myschema'")
		require.NoError(t, err, "failed to SET search_path")

		_, err = db.ExecContext(ctx, "SET application_name = 'test_app'")
		require.NoError(t, err, "failed to SET application_name")

		// Verify custom values are set
		var appName string
		err = db.QueryRowContext(ctx, "SHOW application_name").Scan(&appName)
		require.NoError(t, err, "failed to SHOW application_name")
		assert.Equal(t, "test_app", appName, "should show custom application_name before RESET ALL")

		// Execute RESET ALL
		_, err = db.ExecContext(ctx, "RESET ALL")
		require.NoError(t, err, "failed to execute RESET ALL")

		// Verify variables are reset (values changed from custom values)
		var searchPath string
		err = db.QueryRowContext(ctx, "SHOW search_path").Scan(&searchPath)
		require.NoError(t, err, "failed to SHOW search_path after RESET ALL")
		assert.NotEqual(t, "myschema", searchPath, "search_path should not be custom value after RESET ALL")

		// Application name should be reset to empty or default
		err = db.QueryRowContext(ctx, "SHOW application_name").Scan(&appName)
		require.NoError(t, err, "failed to SHOW application_name after RESET ALL")
		assert.NotEqual(t, "test_app", appName, "application_name should not be custom value after RESET ALL")
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
				setCmd:   "SET search_path = 'public, pg_catalog'",
				showCmd:  "SHOW search_path",
				expected: "\"public, pg_catalog\"", // PostgreSQL adds quotes around comma-separated values
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

		// Set value in connection 1
		_, err = db1.ExecContext(ctx, "SET application_name = 'conn1_app'")
		require.NoError(t, err, "failed to SET in db1")

		// Verify connection 1 sees it multiple times (ensures consistent bucket routing)
		for i := range 100 {
			var app1 string
			err = db1.QueryRowContext(ctx, "SHOW application_name").Scan(&app1)
			require.NoError(t, err, "failed to SHOW in db1 iteration %d", i)
			require.Equal(t, "conn1_app", app1, "db1 should see conn1_app on iteration %d", i)
		}

		// Set different value in connection 2
		_, err = db2.ExecContext(ctx, "SET application_name = 'conn2_app'")
		require.NoError(t, err, "failed to SET in db2")

		// Verify connection 2 sees its own value
		for i := range 100 {
			var app2 string
			err = db2.QueryRowContext(ctx, "SHOW application_name").Scan(&app2)
			require.NoError(t, err, "failed to SHOW in db2 iteration %d", i)
			require.Equal(t, "conn2_app", app2, "db2 should see conn2_app on iteration %d", i)
		}

		// Verify both connections STILL have their independent values after cross-execution
		var app1Final string
		err = db1.QueryRowContext(ctx, "SHOW application_name").Scan(&app1Final)
		require.NoError(t, err, "failed to SHOW in db1 after all operations")
		require.Equal(t, "conn1_app", app1Final, "db1 should still see conn1_app after all operations")

		var app2Final string
		err = db2.QueryRowContext(ctx, "SHOW application_name").Scan(&app2Final)
		require.NoError(t, err, "failed to SHOW in db2 after all operations")
		require.Equal(t, "conn2_app", app2Final, "db2 should still see conn2_app after all operations")
	})

	// Test 8: Extended query protocol with pgx
	t.Run("extended query protocol", func(t *testing.T) {
		// Create pgx connection
		pgxConnStr := fmt.Sprintf("host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable",
			setup.MultigatewayPgPort, shardsetup.TestPostgresPassword)
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

	// Test 10: Error handling
	t.Run("error handling", func(t *testing.T) {
		// Set valid value first
		_, err := db.ExecContext(ctx, "SET work_mem = '64MB'")
		require.NoError(t, err, "failed to SET valid value")

		// Try invalid SET (should fail)
		_, err = db.ExecContext(ctx, "SET invalid_variable_12345 = 'value'")
		require.Error(t, err, "invalid SET should return error")

		// Verify connection still usable and setting persists across 100 queries
		for i := range 100 {
			var result string
			err = db.QueryRowContext(ctx, "SHOW work_mem").Scan(&result)
			require.NoError(t, err, "iteration %d: previous setting should still work after error", i)
			require.Equal(t, "64MB", result, "iteration %d: work_mem should still be 64MB", i)

			// Verify can execute other queries
			var one int
			err = db.QueryRowContext(ctx, "SELECT 1").Scan(&one)
			require.NoError(t, err, "iteration %d: connection should still be usable after error", i)
			require.Equal(t, 1, one)
		}
	})

	// Test 11: SET LOCAL behavior (Phase 2 placeholder)
	t.Run("SET LOCAL behavior", func(t *testing.T) {
		t.Skip("SET LOCAL requires transaction support (Phase 2)")
		// TODO Phase 2: Implement transaction-scoped settings tracking
	})
}

// TestMultiGateway_SetResetGUCRestoration verifies that RESET actually restores
// the GUC value at the PostgreSQL level, not just in the gateway's session tracking.
//
// This catches a specific bug where:
// 1. SET runs on connection A (pool tracks the setting)
// 2. RESET removes from session tracking
// 3. Connection A is returned to the "clean" pool — but PG-side GUC is still dirty
// 4. Subsequent queries may get connection A with the stale GUC value
func TestMultiGateway_SetResetGUCRestoration(t *testing.T) {
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

		// SET to a non-default value
		_, err = db.ExecContext(ctx, "SET extra_float_digits = 0")
		require.NoError(t, err, "failed to SET extra_float_digits")

		// Verify it took effect
		var customVal string
		err = db.QueryRowContext(ctx, "SHOW extra_float_digits").Scan(&customVal)
		require.NoError(t, err)
		assert.Equal(t, "0", customVal, "SET should have changed the value")

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

		// SET multiple GUCs to non-default values
		_, err = db.ExecContext(ctx, "SET extra_float_digits = 0")
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

	t.Run("SET affects query output then RESET restores it", func(t *testing.T) {
		// This test uses a GUC that visibly changes query output,
		// not just SHOW — proving the PG-side state is correct.
		db, err := sql.Open("postgres", connStr)
		require.NoError(t, err)
		defer db.Close()

		err = db.PingContext(ctx)
		require.NoError(t, err)

		// Get default float output
		var defaultOutput string
		err = db.QueryRowContext(ctx, "SELECT 1.0::float8::text").Scan(&defaultOutput)
		require.NoError(t, err)
		t.Logf("Default float8 output: %s", defaultOutput)

		// SET extra_float_digits to 0 (changes float formatting)
		_, err = db.ExecContext(ctx, "SET extra_float_digits = 0")
		require.NoError(t, err)

		var customOutput string
		err = db.QueryRowContext(ctx, "SELECT 1.0::float8::text").Scan(&customOutput)
		require.NoError(t, err)
		t.Logf("Custom float8 output (extra_float_digits=0): %s", customOutput)

		// RESET
		_, err = db.ExecContext(ctx, "RESET extra_float_digits")
		require.NoError(t, err)

		// Verify output is back to default across many iterations
		for i := range 100 {
			var result string
			err = db.QueryRowContext(ctx, "SELECT 1.0::float8::text").Scan(&result)
			require.NoError(t, err, "iteration %d", i)
			require.Equal(t, defaultOutput, result,
				"iteration %d: float8 output should match default after RESET (got %q, want %q)",
				i, result, defaultOutput)
		}
	})
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
