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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/utils"
)

// TestMultiGateway_CopyCommands tests that COPY commands work correctly
// through the complete client → multigateway → multipooler → PostgreSQL flow.
func TestMultiGateway_CopyCommands(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping COPY tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping COPY tests")
	}

	// Use shared test cluster with multigateway
	setup := getSharedSetup(t)
	setup.SetupTest(t)

	// Connect to multigateway using pgx for COPY protocol support
	connStr := fmt.Sprintf("host=localhost port=%d user=postgres password=postgres dbname=postgres sslmode=disable connect_timeout=5",
		setup.MultigatewayPgPort)

	ctx := utils.WithTimeout(t, 150*time.Second)

	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err, "failed to connect to multigateway")
	defer conn.Close(ctx)

	// Ping to verify connection
	err = conn.Ping(ctx)
	require.NoError(t, err, "failed to ping database - multigateway may not be ready")

	// Test 1: Basic COPY FROM STDIN - TEXT format
	t.Run("basic COPY FROM STDIN - TEXT format", func(t *testing.T) {
		tableName := "copy_basic_text"
		createCopyTestTable(t, conn, ctx, tableName, "id INT, name TEXT, value INT")

		// Execute COPY FROM STDIN
		data := [][]any{
			{1, "Alice", 100},
			{2, "Bob", 200},
			{3, "Charlie", 300},
		}

		rowsAffected := executeCopyAndVerify(t, conn, ctx, tableName, []string{"id", "name", "value"}, data)
		assert.Equal(t, int64(3), rowsAffected, "expected 3 rows inserted")

		// Verify data was inserted correctly
		verifyTableData(t, conn, ctx, tableName, data)
	})

	// Test 2: COPY FROM STDIN - CSV format variations
	t.Run("COPY FROM STDIN - CSV format variations", func(t *testing.T) {
		testCases := []struct {
			name         string
			columns      string
			copyOptions  string
			data         string
			expectedRows int64
		}{
			{
				name:         "basic CSV",
				columns:      "id INT, name TEXT, value INT",
				copyOptions:  "CSV",
				data:         "1,Alice,100\n2,Bob,200\n",
				expectedRows: 2,
			},
			{
				name:         "CSV with HEADER",
				columns:      "id INT, name TEXT, value INT",
				copyOptions:  "CSV HEADER",
				data:         "id,name,value\n1,Alice,100\n2,Bob,200\n",
				expectedRows: 2,
			},
			{
				name:         "CSV with custom DELIMITER",
				columns:      "id INT, name TEXT, value INT",
				copyOptions:  "CSV DELIMITER '|'",
				data:         "1|Alice|100\n2|Bob|200\n",
				expectedRows: 2,
			},
			{
				name:         "modern syntax with parentheses",
				columns:      "id INT, name TEXT, value INT",
				copyOptions:  "(FORMAT csv, DELIMITER ',')",
				data:         "1,Alice,100\n2,Bob,200\n",
				expectedRows: 2,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				tableName := "copy_csv_" + strings.ReplaceAll(strings.ToLower(tc.name), " ", "_")
				createCopyTestTable(t, conn, ctx, tableName, tc.columns)

				// Execute COPY FROM STDIN with CSV options using CopyFrom
				copyCmd := fmt.Sprintf("COPY %s FROM STDIN %s", tableName, tc.copyOptions)
				_, err := conn.PgConn().CopyFrom(ctx, strings.NewReader(tc.data), copyCmd)
				require.NoError(t, err, "failed to execute COPY with CSV data")

				// Verify row count
				var count int64
				err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM "+tableName).Scan(&count)
				require.NoError(t, err, "failed to count rows")
				assert.Equal(t, tc.expectedRows, count, "row count mismatch")
			})
		}
	})

	// Test 3: COPY FROM STDIN with column list
	t.Run("COPY FROM STDIN with column list", func(t *testing.T) {
		tableName := "copy_partial"
		createCopyTestTable(t, conn, ctx, tableName, "id INT, name TEXT, value INT, extra TEXT")

		// Copy only specific columns (id, name)
		data := [][]any{
			{1, "Alice"},
			{2, "Bob"},
		}

		rowsAffected := executeCopyWithColumns(t, conn, ctx, tableName, []string{"id", "name"}, data)
		assert.Equal(t, int64(2), rowsAffected, "expected 2 rows inserted")

		// Verify NULL columns
		var id int
		var name string
		var value, extra *int
		err := conn.QueryRow(ctx, fmt.Sprintf("SELECT id, name, value, extra FROM %s WHERE id = 1", tableName)).Scan(&id, &name, &value, &extra)
		require.NoError(t, err)
		assert.Equal(t, 1, id)
		assert.Equal(t, "Alice", name)
		assert.Nil(t, value, "value should be NULL")
		assert.Nil(t, extra, "extra should be NULL")
	})

	// Test 4: COPY FROM STDIN with options
	t.Run("COPY FROM STDIN with options", func(t *testing.T) {
		t.Run("custom NULL representation", func(t *testing.T) {
			tableName := "copy_opts_null"
			createCopyTestTable(t, conn, ctx, tableName, "id INT, name TEXT, value INT")

			copyCmd := fmt.Sprintf("COPY %s FROM STDIN NULL 'NULL'", tableName)
			data := "1\tAlice\tNULL\n2\tNULL\t200\n"

			_, copyErr := conn.PgConn().CopyFrom(ctx, strings.NewReader(data), copyCmd)
			require.NoError(t, copyErr, "COPY with custom NULL failed")

			// Verify NULL values
			var id int
			var name *string
			var value *int
			err := conn.QueryRow(ctx, fmt.Sprintf("SELECT id, name, value FROM %s WHERE id = 1", tableName)).Scan(&id, &name, &value)
			require.NoError(t, err)
			assert.Equal(t, 1, id)
			assert.NotNil(t, name)
			assert.Equal(t, "Alice", *name)
			assert.Nil(t, value, "value should be NULL")
		})

		t.Run("custom DELIMITER", func(t *testing.T) {
			tableName := "copy_opts_delim"
			createCopyTestTable(t, conn, ctx, tableName, "id INT, name TEXT, value INT")

			copyCmd := fmt.Sprintf("COPY %s FROM STDIN DELIMITER '|'", tableName)
			data := "1|Alice|100\n2|Bob|200\n"

			_, copyErr := conn.PgConn().CopyFrom(ctx, strings.NewReader(data), copyCmd)
			require.NoError(t, copyErr, "COPY with custom DELIMITER failed")

			// Verify row count
			var count int64
			err := conn.QueryRow(ctx, "SELECT COUNT(*) FROM "+tableName).Scan(&count)
			require.NoError(t, err)
			assert.Equal(t, int64(2), count)
		})

		t.Run("ENCODING UTF8", func(t *testing.T) {
			tableName := "copy_opts_encoding"
			createCopyTestTable(t, conn, ctx, tableName, "id INT, name TEXT, value INT")

			copyCmd := fmt.Sprintf("COPY %s FROM STDIN ENCODING 'UTF8'", tableName)
			data := "1\tJosé\t100\n2\tMüller\t200\n"

			_, copyErr := conn.PgConn().CopyFrom(ctx, strings.NewReader(data), copyCmd)
			require.NoError(t, copyErr, "COPY with ENCODING failed")

			// Verify UTF8 names
			var name string
			err := conn.QueryRow(ctx, fmt.Sprintf("SELECT name FROM %s WHERE id = 1", tableName)).Scan(&name)
			require.NoError(t, err)
			assert.Equal(t, "José", name)
		})
	})

	// Test 5: Multiple sequential COPY operations
	t.Run("multiple sequential COPY operations", func(t *testing.T) {
		table1 := "copy_seq_1"
		table2 := "copy_seq_2"
		table3 := "copy_seq_3"

		createCopyTestTable(t, conn, ctx, table1, "id INT, name TEXT")
		createCopyTestTable(t, conn, ctx, table2, "id INT, value INT")
		createCopyTestTable(t, conn, ctx, table3, "id INT, data TEXT")

		// Execute 3 COPY operations in sequence
		data1 := [][]any{{1, "Alice"}, {2, "Bob"}}
		data2 := [][]any{{1, 100}, {2, 200}}
		data3 := [][]any{{1, "test"}, {2, "data"}}

		executeCopyAndVerify(t, conn, ctx, table1, []string{"id", "name"}, data1)
		executeCopyAndVerify(t, conn, ctx, table2, []string{"id", "value"}, data2)
		executeCopyAndVerify(t, conn, ctx, table3, []string{"id", "data"}, data3)

		// Verify all tables have data
		verifyTableData(t, conn, ctx, table1, data1)
		verifyTableData(t, conn, ctx, table2, data2)
		verifyTableData(t, conn, ctx, table3, data3)
	})

	// Test 6: Large data COPY (streaming stress test)
	t.Run("large data COPY (streaming stress test)", func(t *testing.T) {
		tableName := "copy_large"
		createCopyTestTable(t, conn, ctx, tableName, "id INT, data TEXT")

		// Generate ~2MB of data to stress test streaming
		var dataBuilder strings.Builder
		rowCount := 10000
		for i := 1; i <= rowCount; i++ {
			// Each row ~200 bytes
			dataBuilder.WriteString(fmt.Sprintf("%d\t%s\n", i, strings.Repeat("x", 190)))
		}

		copyCmd := fmt.Sprintf("COPY %s FROM STDIN", tableName)
		_, err := conn.PgConn().CopyFrom(ctx, strings.NewReader(dataBuilder.String()), copyCmd)
		require.NoError(t, err, "failed to execute large COPY")

		// Verify row count
		var count int64
		err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM "+tableName).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, int64(rowCount), count, "row count mismatch for large COPY")
	})

	// Test 7: Unsupported COPY operations
	t.Run("unsupported COPY operations", func(t *testing.T) {
		// Create a test table for unsupported operations
		tableName := "copy_unsupported"
		createCopyTestTable(t, conn, ctx, tableName, "id INT, name TEXT")

		t.Run("COPY TO STDOUT fails", func(t *testing.T) {
			_, err := conn.Exec(ctx, fmt.Sprintf("COPY %s TO STDOUT", tableName))
			require.Error(t, err, "COPY TO STDOUT should fail")
			assert.Contains(t, err.Error(), "not yet supported", "should mention not supported")
		})

		t.Run("COPY FROM file pass-through", func(t *testing.T) {
			tableName := "copy_from_file_test"
			createCopyTestTable(t, conn, ctx, tableName, "id INT, name TEXT")

			// This will fail because /tmp/nonexistent.csv doesn't exist on PG server
			// But the error should be from PostgreSQL, not multigateway
			_, err := conn.Exec(ctx, fmt.Sprintf("COPY %s FROM '/tmp/nonexistent.csv'", tableName))
			require.Error(t, err, "COPY FROM nonexistent file should fail")

			// Error should be from PostgreSQL (file not found), not multigateway validation
			assert.NotContains(t, err.Error(), "not supported",
				"should not be rejected by multigateway planner")
			// Log the actual error for debugging
			t.Logf("Got PostgreSQL error (expected): %v", err)
		})

		t.Run("COPY TO file pass-through", func(t *testing.T) {
			tableName := "copy_to_file_test"
			createCopyTestTable(t, conn, ctx, tableName, "id INT, name TEXT")

			// Insert test data
			data := [][]any{{1, "Alice"}, {2, "Bob"}}
			executeCopyAndVerify(t, conn, ctx, tableName, []string{"id", "name"}, data)

			// Try COPY TO (may fail due to permissions, but should pass through multigateway)
			_, err := conn.Exec(ctx, fmt.Sprintf("COPY %s TO '/tmp/multigres_test_output.csv'", tableName))

			// The key verification is that multigateway doesn't reject it with "not yet supported"
			if err != nil {
				assert.NotContains(t, err.Error(), "not yet supported",
					"should not be rejected by multigateway planner")
				t.Logf("COPY TO failed (expected if PostgreSQL lacks write permissions): %v", err)
			} else {
				t.Logf("COPY TO succeeded - file written to PostgreSQL server")
			}
		})

		t.Run("COPY with PROGRAM fails", func(t *testing.T) {
			_, err := conn.Exec(ctx, fmt.Sprintf("COPY %s FROM PROGRAM 'cat /tmp/data.csv'", tableName))
			require.Error(t, err, "COPY with PROGRAM should fail")
			assert.Contains(t, err.Error(), "security reasons", "should mention security in rejection message")
		})
	})

	// Test 8: Connection resilience after COPY error
	t.Run("connection resilience after COPY error", func(t *testing.T) {
		// Execute invalid COPY (nonexistent table)
		_, err := conn.Exec(ctx, "COPY nonexistent_table FROM STDIN")
		require.Error(t, err, "COPY to nonexistent table should fail")

		// Verify connection still usable with regular queries
		var result int
		err = conn.QueryRow(ctx, "SELECT 1").Scan(&result)
		require.NoError(t, err, "connection should still be usable after COPY error")
		assert.Equal(t, 1, result)

		// Execute CREATE TABLE to verify DDL works
		tableName := "copy_resilience_test"
		_, err = conn.Exec(ctx, "DROP TABLE IF EXISTS "+tableName)
		require.NoError(t, err, "DROP TABLE should work after COPY error")

		_, err = conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, name TEXT)", tableName))
		require.NoError(t, err, "CREATE TABLE should work after COPY error")

		// Execute INSERT to verify DML works
		_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, 'test')", tableName))
		require.NoError(t, err, "INSERT should work after COPY error")

		// Verify data
		var id int
		var name string
		err = conn.QueryRow(ctx, "SELECT id, name FROM "+tableName).Scan(&id, &name)
		require.NoError(t, err, "SELECT should work after COPY error")
		assert.Equal(t, 1, id)
		assert.Equal(t, "test", name)

		// Cleanup
		_, _ = conn.Exec(ctx, "DROP TABLE "+tableName)
	})

	// Test 8: Empty COPY (no data) - verifies fix for broken pipe bug
	t.Run("empty COPY FROM STDIN", func(t *testing.T) {
		tableName := "copy_empty"
		createCopyTestTable(t, conn, ctx, tableName, "id INT, name TEXT")

		// Execute COPY with no data (immediately terminate)
		rowsAffected, err := conn.CopyFrom(
			ctx,
			pgx.Identifier{tableName},
			[]string{"id", "name"},
			pgx.CopyFromRows([][]any{}), // Empty data
		)
		require.NoError(t, err, "empty COPY should succeed without broken pipe error")
		assert.Equal(t, int64(0), rowsAffected, "should insert 0 rows")

		// Verify table is empty
		var count int64
		err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM "+tableName).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, int64(0), count, "table should be empty")

		// CRITICAL: Verify connection still works - execute another COPY
		// This proves the connection wasn't corrupted by the empty COPY
		data := [][]any{{1, "Alice"}, {2, "Bob"}}
		rowsAffected = executeCopyAndVerify(t, conn, ctx, tableName, []string{"id", "name"}, data)
		assert.Equal(t, int64(2), rowsAffected, "subsequent COPY should work")

		// Verify the data
		err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM "+tableName).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, int64(2), count, "should have 2 rows after subsequent COPY")

		// Execute multiple empty COPYs in sequence to stress test connection reuse
		for i := range 3 {
			_, err = conn.CopyFrom(
				ctx,
				pgx.Identifier{tableName},
				[]string{"id", "name"},
				pgx.CopyFromRows([][]any{}),
			)
			require.NoError(t, err, "multiple empty COPYs should succeed (iteration %d)", i)
		}

		// Final verification: connection still works
		err = conn.QueryRow(ctx, "SELECT 1").Scan(&count)
		require.NoError(t, err, "connection should still be usable after multiple empty COPYs")
		assert.Equal(t, int64(1), count)
	})

	// Test 10: COPY initiation error recovery
	t.Run("COPY initiation error recovery", func(t *testing.T) {
		tableName := "copy_error_recovery"

		// Attempt COPY on nonexistent table (should fail)
		_, err := conn.CopyFrom(
			ctx,
			pgx.Identifier{tableName},
			[]string{"id", "name"},
			pgx.CopyFromRows([][]any{{1, "Alice"}}),
		)
		require.Error(t, err, "COPY to nonexistent table should fail")
		// Error could be "does not exist" or "describe failed" depending on how pgx handles it
		assert.True(t, strings.Contains(err.Error(), "does not exist") || strings.Contains(err.Error(), "describe failed"),
			"should get table-related error, got: %s", err.Error())

		// Verify connection is still usable
		var count int64
		err = conn.QueryRow(ctx, "SELECT 1").Scan(&count)
		require.NoError(t, err, "connection should still be usable after failed COPY")
		assert.Equal(t, int64(1), count)

		// Create the table
		createCopyTestTable(t, conn, ctx, tableName, "id INT, name TEXT")

		// Retry COPY - should succeed now
		data := [][]any{
			{1, "Alice"},
			{2, "Bob"},
		}
		rowsAffected := executeCopyAndVerify(t, conn, ctx, tableName, []string{"id", "name"}, data)
		assert.Equal(t, int64(2), rowsAffected, "COPY should succeed after creating table")

		// Verify data was inserted correctly
		verifyTableData(t, conn, ctx, tableName, data)
	})

	// Test: COPY with invalid data validation error recovery (Bug 3)
	t.Run("COPY with invalid data error recovery", func(t *testing.T) {
		tableName := "copy_invalid_data"
		// Create table with CHECK constraint that will fail server-side
		createCopyTestTable(t, conn, ctx, tableName, "id INT CHECK (id > 0), name TEXT")

		// Attempt COPY with data that violates CHECK constraint
		// This passes client-side type validation but fails server-side during DONE phase
		invalidData := [][]any{
			{-1, "invalid"}, // Violates CHECK constraint (id > 0)
			{0, "invalid"},  // Violates CHECK constraint (id > 0)
		}
		_, err := conn.CopyFrom(
			ctx,
			pgx.Identifier{tableName},
			[]string{"id", "name"},
			pgx.CopyFromRows(invalidData),
		)
		require.Error(t, err, "COPY with data violating CHECK constraint should fail")
		assert.Contains(t, err.Error(), "check constraint", "should get CHECK constraint violation error")

		// Verify connection is still usable (Bug 3: this used to hang)
		var count int64
		err = conn.QueryRow(ctx, "SELECT 1").Scan(&count)
		require.NoError(t, err, "connection should still be usable after COPY data validation error")
		assert.Equal(t, int64(1), count)

		// Retry COPY with valid data - should succeed now (Bug 3: this used to hang)
		data := [][]any{
			{1, "Alice"},
			{2, "Bob"},
		}
		rowsAffected := executeCopyAndVerify(t, conn, ctx, tableName, []string{"id", "name"}, data)
		assert.Equal(t, int64(2), rowsAffected, "COPY should succeed after fixing data")

		// Verify data was inserted correctly
		verifyTableData(t, conn, ctx, tableName, data)
	})
}

// Helper Functions

// createCopyTestTable creates a test table and registers cleanup
func createCopyTestTable(t *testing.T, conn *pgx.Conn, ctx context.Context, tableName string, columns string) {
	t.Helper()

	// Drop if exists
	_, err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+tableName)
	require.NoError(t, err, "failed to drop table")

	// Create table
	_, err = conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (%s)", tableName, columns))
	require.NoError(t, err, "failed to create table: %s", tableName)

	// Register cleanup
	t.Cleanup(func() {
		_, _ = conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+tableName)
	})
}

// executeCopyAndVerify executes COPY FROM STDIN and verifies the result
func executeCopyAndVerify(t *testing.T, conn *pgx.Conn, ctx context.Context, tableName string, columns []string, data [][]any) int64 {
	t.Helper()

	// Use CopyFrom with pgx
	rowsAffected, err := conn.CopyFrom(
		ctx,
		pgx.Identifier{tableName},
		columns,
		pgx.CopyFromRows(data),
	)
	require.NoError(t, err, "COPY failed for table %s", tableName)

	return rowsAffected
}

// executeCopyWithColumns executes COPY FROM STDIN with specific columns
func executeCopyWithColumns(t *testing.T, conn *pgx.Conn, ctx context.Context, tableName string, columns []string, data [][]any) int64 {
	t.Helper()

	rowsAffected, err := conn.CopyFrom(
		ctx,
		pgx.Identifier{tableName},
		columns,
		pgx.CopyFromRows(data),
	)
	require.NoError(t, err, "COPY with columns failed for table %s", tableName)

	return rowsAffected
}

// verifyTableData verifies specific rows in the table
func verifyTableData(t *testing.T, conn *pgx.Conn, ctx context.Context, tableName string, expectedRows [][]any) {
	t.Helper()

	rows, err := conn.Query(ctx, fmt.Sprintf("SELECT * FROM %s ORDER BY id", tableName))
	require.NoError(t, err, "failed to query table")
	defer rows.Close()

	rowNum := 0
	for rows.Next() {
		require.Less(t, rowNum, len(expectedRows), "more rows than expected")

		// Get column count
		fieldDescs := rows.FieldDescriptions()
		vals := make([]any, len(fieldDescs))
		valPtrs := make([]any, len(fieldDescs))
		for i := range vals {
			valPtrs[i] = &vals[i]
		}

		err = rows.Scan(valPtrs...)
		require.NoError(t, err, "failed to scan row %d", rowNum)

		// Compare with expected (handle type conversions)
		for i, expected := range expectedRows[rowNum] {
			actual := vals[i]
			switch v := actual.(type) {
			case int32:
				assert.Equal(t, expected, int(v), "row %d, column %d mismatch", rowNum, i)
			case int64:
				assert.Equal(t, expected, int(v), "row %d, column %d mismatch", rowNum, i)
			default:
				assert.Equal(t, expected, actual, "row %d, column %d mismatch", rowNum, i)
			}
		}

		rowNum++
	}

	assert.Equal(t, len(expectedRows), rowNum, "row count mismatch")
}
