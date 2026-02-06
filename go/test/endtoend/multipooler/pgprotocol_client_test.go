// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package multipooler

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/test/utils"
)

// TestPgProtocolClientConnection tests basic connection establishment.
func TestPgProtocolClientConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)

	t.Run("connect_and_close", func(t *testing.T) {
		ctx := utils.WithTimeout(t, 10*time.Second)

		conn, err := client.Connect(ctx, &client.Config{
			Host:        "localhost",
			Port:        setup.PrimaryPgctld.PgPort,
			User:        "postgres",
			Password:    testPostgresPassword,
			Database:    "postgres",
			DialTimeout: 5 * time.Second,
		})
		require.NoError(t, err)
		require.NotNil(t, conn)

		// Verify connection state
		assert.False(t, conn.IsClosed())
		assert.NotZero(t, conn.ProcessID())
		assert.NotZero(t, conn.SecretKey())

		// Check server parameters
		params := conn.ServerParams()
		assert.NotEmpty(t, params)
		t.Logf("Server version: %s", params["server_version"])

		// Close connection
		err = conn.Close()
		require.NoError(t, err)
		assert.True(t, conn.IsClosed())
	})

	t.Run("connection_state", func(t *testing.T) {
		ctx := utils.WithTimeout(t, 10*time.Second)

		conn, err := client.Connect(ctx, &client.Config{
			Host:        "localhost",
			Port:        setup.PrimaryPgctld.PgPort,
			User:        "postgres",
			Password:    testPostgresPassword,
			Database:    "postgres",
			DialTimeout: 5 * time.Second,
		})
		require.NoError(t, err)
		defer conn.Close()

		// Test connection state get/set
		assert.Nil(t, conn.GetConnectionState())

		testState := map[string]string{"key": "value"}
		conn.SetConnectionState(testState)
		assert.Equal(t, testState, conn.GetConnectionState())
	})
}

// TestPgProtocolClientSimpleQuery tests the simple query protocol.
func TestPgProtocolClientSimpleQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)

	ctx := utils.WithTimeout(t, 30*time.Second)

	conn, err := client.Connect(ctx, &client.Config{
		Host:        "localhost",
		Port:        setup.PrimaryPgctld.PgPort,
		User:        "postgres",
		Password:    testPostgresPassword,
		Database:    "postgres",
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	defer conn.Close()

	t.Run("select_constant", func(t *testing.T) {
		results, err := conn.Query(ctx, "SELECT 1 AS num, 'hello' AS greeting")
		require.NoError(t, err)
		require.Len(t, results, 1)

		result := results[0]
		require.Len(t, result.Fields, 2)
		assert.Equal(t, "num", result.Fields[0].Name)
		assert.Equal(t, "greeting", result.Fields[1].Name)

		require.Len(t, result.Rows, 1)
		assert.Equal(t, "1", string(result.Rows[0].Values[0]))
		assert.Equal(t, "hello", string(result.Rows[0].Values[1]))

		assert.Contains(t, result.CommandTag, "SELECT")
		assert.Equal(t, uint64(0), result.RowsAffected) // SELECT doesn't populate RowsAffected
	})

	t.Run("select_multiple_rows", func(t *testing.T) {
		results, err := conn.Query(ctx, "SELECT generate_series(1, 5) AS n")
		require.NoError(t, err)
		require.Len(t, results, 1)

		result := results[0]
		require.Len(t, result.Rows, 5)

		for i, row := range result.Rows {
			assert.Equal(t, strconv.Itoa(i+1), string(row.Values[0]))
		}
		assert.Equal(t, uint64(0), result.RowsAffected) // SELECT doesn't populate RowsAffected
	})

	t.Run("multiple_statements", func(t *testing.T) {
		results, err := conn.Query(ctx, "SELECT 1; SELECT 2; SELECT 3")
		require.NoError(t, err)
		require.Len(t, results, 3)

		for i, result := range results {
			require.Len(t, result.Rows, 1)
			assert.Equal(t, strconv.Itoa(i+1), string(result.Rows[0].Values[0]))
		}
	})

	t.Run("empty_result", func(t *testing.T) {
		// Create and query empty table
		_, err := conn.Query(ctx, "CREATE TEMP TABLE empty_test (id int)")
		require.NoError(t, err)

		results, err := conn.Query(ctx, "SELECT * FROM empty_test")
		require.NoError(t, err)
		require.Len(t, results, 1)

		result := results[0]
		assert.Len(t, result.Rows, 0)
		assert.Equal(t, uint64(0), result.RowsAffected)
	})

	t.Run("dml_operations", func(t *testing.T) {
		// Create table
		results, err := conn.Query(ctx, "CREATE TEMP TABLE dml_test (id serial PRIMARY KEY, name text)")
		require.NoError(t, err)
		require.Len(t, results, 1)
		assert.Contains(t, results[0].CommandTag, "CREATE TABLE")

		// Insert
		results, err = conn.Query(ctx, "INSERT INTO dml_test (name) VALUES ('alice'), ('bob'), ('charlie')")
		require.NoError(t, err)
		require.Len(t, results, 1)
		assert.Equal(t, uint64(3), results[0].RowsAffected)
		assert.Contains(t, results[0].CommandTag, "INSERT")

		// Update
		results, err = conn.Query(ctx, "UPDATE dml_test SET name = upper(name) WHERE id <= 2")
		require.NoError(t, err)
		require.Len(t, results, 1)
		assert.Equal(t, uint64(2), results[0].RowsAffected)
		assert.Contains(t, results[0].CommandTag, "UPDATE")

		// Delete
		results, err = conn.Query(ctx, "DELETE FROM dml_test WHERE id = 1")
		require.NoError(t, err)
		require.Len(t, results, 1)
		assert.Equal(t, uint64(1), results[0].RowsAffected)
		assert.Contains(t, results[0].CommandTag, "DELETE")

		// Verify
		results, err = conn.Query(ctx, "SELECT * FROM dml_test ORDER BY id")
		require.NoError(t, err)
		require.Len(t, results, 1)
		assert.Len(t, results[0].Rows, 2)
	})
}

// TestPgProtocolClientExtendedQuery tests the extended query protocol.
func TestPgProtocolClientExtendedQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)

	ctx := utils.WithTimeout(t, 30*time.Second)

	conn, err := client.Connect(ctx, &client.Config{
		Host:        "localhost",
		Port:        setup.PrimaryPgctld.PgPort,
		User:        "postgres",
		Password:    testPostgresPassword,
		Database:    "postgres",
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	defer conn.Close()

	t.Run("parse_and_bind_execute", func(t *testing.T) {
		// Parse - creates a prepared statement
		err := conn.Parse(ctx, "test_stmt", "SELECT $1::int + $2::int AS sum", nil)
		require.NoError(t, err)

		// BindAndExecute - binds parameters and executes atomically
		var results []*sqltypes.Result
		completed, err := conn.BindAndExecute(ctx, "test_stmt", [][]byte{[]byte("10"), []byte("20")}, nil, nil, 0,
			func(ctx context.Context, result *sqltypes.Result) error {
				results = append(results, result)
				return nil
			})
		require.NoError(t, err)
		assert.True(t, completed, "expected execution to complete")
		require.Len(t, results, 1)

		result := results[0]
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "30", string(result.Rows[0].Values[0]))

		// Close statement
		err = conn.CloseStatement(ctx, "test_stmt")
		require.NoError(t, err)
	})

	t.Run("describe_prepared", func(t *testing.T) {
		// Parse - creates a prepared statement
		err := conn.Parse(ctx, "desc_stmt", "SELECT $1::text AS name, $2::int AS age", nil)
		require.NoError(t, err)

		// DescribePrepared - describes the prepared statement
		desc, err := conn.DescribePrepared(ctx, "desc_stmt")
		require.NoError(t, err)

		// Check parameters
		require.Len(t, desc.Parameters, 2)

		// Check fields
		require.Len(t, desc.Fields, 2)
		assert.Equal(t, "name", desc.Fields[0].Name)
		assert.Equal(t, "age", desc.Fields[1].Name)

		// Cleanup
		err = conn.CloseStatement(ctx, "desc_stmt")
		require.NoError(t, err)
	})

	t.Run("bind_and_describe", func(t *testing.T) {
		// Parse - creates a prepared statement
		err := conn.Parse(ctx, "bind_desc_stmt", "SELECT $1::text AS greeting", nil)
		require.NoError(t, err)

		// BindAndDescribe - binds parameters and describes the resulting portal
		desc, err := conn.BindAndDescribe(ctx, "bind_desc_stmt", [][]byte{[]byte("hello")}, nil, nil)
		require.NoError(t, err)

		// Portal description only has fields (no parameters)
		require.Len(t, desc.Fields, 1)
		assert.Equal(t, "greeting", desc.Fields[0].Name)

		// Cleanup
		err = conn.CloseStatement(ctx, "bind_desc_stmt")
		require.NoError(t, err)
	})

	t.Run("prepare_and_execute_combined", func(t *testing.T) {
		var results []*sqltypes.Result
		// Use unnamed statement ("") for one-shot execution
		err := conn.PrepareAndExecute(ctx, "", "SELECT $1::text || ' ' || $2::text AS greeting",
			[][]byte{[]byte("Hello"), []byte("World")},
			func(ctx context.Context, result *sqltypes.Result) error {
				results = append(results, result)
				return nil
			})
		require.NoError(t, err)
		require.Len(t, results, 1)

		result := results[0]
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "Hello World", string(result.Rows[0].Values[0]))
	})

	t.Run("null_parameters", func(t *testing.T) {
		var results []*sqltypes.Result
		// Use unnamed statement ("") for one-shot execution
		err := conn.PrepareAndExecute(ctx, "", "SELECT $1::text AS val",
			[][]byte{nil}, // NULL parameter
			func(ctx context.Context, result *sqltypes.Result) error {
				results = append(results, result)
				return nil
			})
		require.NoError(t, err)
		require.Len(t, results, 1)

		result := results[0]
		require.Len(t, result.Rows, 1)
		assert.Nil(t, result.Rows[0].Values[0]) // NULL value
	})
}

// TestPgProtocolClientDataTypes tests handling of various PostgreSQL data types.
func TestPgProtocolClientDataTypes(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)

	ctx := utils.WithTimeout(t, 30*time.Second)

	conn, err := client.Connect(ctx, &client.Config{
		Host:        "localhost",
		Port:        setup.PrimaryPgctld.PgPort,
		User:        "postgres",
		Password:    testPostgresPassword,
		Database:    "postgres",
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	defer conn.Close()

	t.Run("numeric_types", func(t *testing.T) {
		results, err := conn.Query(ctx, `
			SELECT
				42::smallint AS small,
				123456::int AS medium,
				9876543210::bigint AS big,
				3.14::real AS float4,
				2.718281828::double precision AS float8,
				123.456::numeric(10,3) AS num
		`)
		require.NoError(t, err)
		require.Len(t, results, 1)

		row := results[0].Rows[0]
		assert.Equal(t, "42", string(row.Values[0]))
		assert.Equal(t, "123456", string(row.Values[1]))
		assert.Equal(t, "9876543210", string(row.Values[2]))
	})

	t.Run("string_types", func(t *testing.T) {
		results, err := conn.Query(ctx, `
			SELECT
				'hello'::text AS txt,
				'world'::varchar(100) AS vchar,
				'abc'::char(5) AS ch
		`)
		require.NoError(t, err)
		require.Len(t, results, 1)

		row := results[0].Rows[0]
		assert.Equal(t, "hello", string(row.Values[0]))
		assert.Equal(t, "world", string(row.Values[1]))
		assert.Equal(t, "abc  ", string(row.Values[2])) // char is padded
	})

	t.Run("boolean_type", func(t *testing.T) {
		results, err := conn.Query(ctx, "SELECT true AS t, false AS f")
		require.NoError(t, err)
		require.Len(t, results, 1)

		row := results[0].Rows[0]
		assert.Equal(t, "t", string(row.Values[0]))
		assert.Equal(t, "f", string(row.Values[1]))
	})

	t.Run("date_time_types", func(t *testing.T) {
		results, err := conn.Query(ctx, `
			SELECT
				'2024-01-15'::date AS d,
				'14:30:00'::time AS t,
				'2024-01-15 14:30:00'::timestamp AS ts
		`)
		require.NoError(t, err)
		require.Len(t, results, 1)

		row := results[0].Rows[0]
		assert.Equal(t, "2024-01-15", string(row.Values[0]))
		assert.Equal(t, "14:30:00", string(row.Values[1]))
		assert.Contains(t, string(row.Values[2]), "2024-01-15")
	})

	t.Run("null_values", func(t *testing.T) {
		results, err := conn.Query(ctx, "SELECT NULL::text AS nullval, 'notnull'::text AS val")
		require.NoError(t, err)
		require.Len(t, results, 1)

		row := results[0].Rows[0]
		assert.Nil(t, row.Values[0]) // NULL
		assert.Equal(t, "notnull", string(row.Values[1]))
	})

	t.Run("array_types", func(t *testing.T) {
		results, err := conn.Query(ctx, "SELECT ARRAY[1, 2, 3]::int[] AS arr")
		require.NoError(t, err)
		require.Len(t, results, 1)

		row := results[0].Rows[0]
		assert.Equal(t, "{1,2,3}", string(row.Values[0]))
	})

	t.Run("json_types", func(t *testing.T) {
		results, err := conn.Query(ctx, `SELECT '{"key": "value"}'::json AS j, '{"num": 42}'::jsonb AS jb`)
		require.NoError(t, err)
		require.Len(t, results, 1)

		row := results[0].Rows[0]
		assert.Contains(t, string(row.Values[0]), "key")
		assert.Contains(t, string(row.Values[1]), "num")
	})

	t.Run("uuid_type", func(t *testing.T) {
		results, err := conn.Query(ctx, "SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid AS id")
		require.NoError(t, err)
		require.Len(t, results, 1)

		row := results[0].Rows[0]
		assert.Equal(t, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", string(row.Values[0]))
	})
}

// TestPgProtocolClientErrors tests error handling.
func TestPgProtocolClientErrors(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)

	ctx := utils.WithTimeout(t, 30*time.Second)

	conn, err := client.Connect(ctx, &client.Config{
		Host:        "localhost",
		Port:        setup.PrimaryPgctld.PgPort,
		User:        "postgres",
		Password:    testPostgresPassword,
		Database:    "postgres",
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	defer conn.Close()

	t.Run("syntax_error", func(t *testing.T) {
		_, err := conn.Query(ctx, "SELEC 1") // typo
		require.Error(t, err)

		var diag *sqltypes.PgDiagnostic
		require.True(t, errors.As(err, &diag), "expected *sqltypes.PgDiagnostic, got %T", err)
		assert.Equal(t, "ERROR", diag.Severity)
		assert.NotEmpty(t, diag.Code)
		assert.NotEmpty(t, diag.Message)
		t.Logf("Error: %s", diag.Error())
	})

	t.Run("undefined_table", func(t *testing.T) {
		_, err := conn.Query(ctx, "SELECT * FROM nonexistent_table_xyz")
		require.Error(t, err)

		var diag *sqltypes.PgDiagnostic
		require.True(t, errors.As(err, &diag), "expected *sqltypes.PgDiagnostic, got %T", err)
		assert.Equal(t, "42P01", diag.Code) // undefined_table
	})

	t.Run("division_by_zero", func(t *testing.T) {
		_, err := conn.Query(ctx, "SELECT 1/0")
		require.Error(t, err)

		var diag *sqltypes.PgDiagnostic
		require.True(t, errors.As(err, &diag), "expected *sqltypes.PgDiagnostic, got %T", err)
		assert.Equal(t, "22012", diag.Code) // division_by_zero
	})

	t.Run("connection_recovers_after_error", func(t *testing.T) {
		// First query fails
		_, err := conn.Query(ctx, "INVALID SQL")
		require.Error(t, err)

		// Connection should still be usable
		results, err := conn.Query(ctx, "SELECT 1")
		require.NoError(t, err)
		require.Len(t, results, 1)
		assert.Equal(t, "1", string(results[0].Rows[0].Values[0]))
	})
}

// TestPgProtocolClientTransactions tests transaction handling.
func TestPgProtocolClientTransactions(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)

	ctx := utils.WithTimeout(t, 30*time.Second)

	conn, err := client.Connect(ctx, &client.Config{
		Host:        "localhost",
		Port:        setup.PrimaryPgctld.PgPort,
		User:        "postgres",
		Password:    testPostgresPassword,
		Database:    "postgres",
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	defer conn.Close()

	t.Run("begin_commit", func(t *testing.T) {
		// Create table
		_, err := conn.Query(ctx, "CREATE TEMP TABLE txn_test (id int)")
		require.NoError(t, err)

		// Begin transaction
		_, err = conn.Query(ctx, "BEGIN")
		require.NoError(t, err)

		// Insert data
		_, err = conn.Query(ctx, "INSERT INTO txn_test VALUES (1), (2), (3)")
		require.NoError(t, err)

		// Commit
		_, err = conn.Query(ctx, "COMMIT")
		require.NoError(t, err)

		// Verify data persisted
		results, err := conn.Query(ctx, "SELECT COUNT(*) FROM txn_test")
		require.NoError(t, err)
		assert.Equal(t, "3", string(results[0].Rows[0].Values[0]))
	})

	t.Run("begin_rollback", func(t *testing.T) {
		// Create table
		_, err := conn.Query(ctx, "CREATE TEMP TABLE rollback_test (id int)")
		require.NoError(t, err)

		// Insert initial data
		_, err = conn.Query(ctx, "INSERT INTO rollback_test VALUES (1)")
		require.NoError(t, err)

		// Begin transaction
		_, err = conn.Query(ctx, "BEGIN")
		require.NoError(t, err)

		// Insert more data
		_, err = conn.Query(ctx, "INSERT INTO rollback_test VALUES (2), (3)")
		require.NoError(t, err)

		// Rollback
		_, err = conn.Query(ctx, "ROLLBACK")
		require.NoError(t, err)

		// Verify only initial data exists
		results, err := conn.Query(ctx, "SELECT COUNT(*) FROM rollback_test")
		require.NoError(t, err)
		assert.Equal(t, "1", string(results[0].Rows[0].Values[0]))
	})
}

// TestPgProtocolClientStreaming tests streaming query results.
func TestPgProtocolClientStreaming(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)

	ctx := utils.WithTimeout(t, 30*time.Second)

	conn, err := client.Connect(ctx, &client.Config{
		Host:        "localhost",
		Port:        setup.PrimaryPgctld.PgPort,
		User:        "postgres",
		Password:    testPostgresPassword,
		Database:    "postgres",
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	defer conn.Close()

	t.Run("streaming_callback", func(t *testing.T) {
		var callbackCount int
		var totalRows int
		var resultSetCount int

		err := conn.QueryStreaming(ctx, "SELECT generate_series(1, 100); SELECT generate_series(1, 50)",
			func(ctx context.Context, result *sqltypes.Result) error {
				callbackCount++
				totalRows += len(result.Rows)
				// CommandTag being set signals end of a result set
				if result.CommandTag != "" {
					resultSetCount++
				}
				return nil
			})
		require.NoError(t, err)

		// With optimized batched streaming:
		// - Small result sets get Fields + Rows + CommandTag in a single callback
		// - First query: 1 callback (100 rows + CommandTag)
		// - Second query: 1 callback (50 rows + CommandTag)
		// - Total: 2 callbacks
		assert.Equal(t, 2, callbackCount)
		assert.Equal(t, 2, resultSetCount) // Two result sets (signaled by CommandTag)
		assert.Equal(t, 150, totalRows)    // 100 + 50 rows total
	})

	t.Run("large_result_multiple_batches", func(t *testing.T) {
		// Generate enough data to exceed DefaultStreamingBatchSize (2MB) and trigger multiple batches.
		// Each row is ~1KB (1000 chars), so 5000 rows = ~5MB = at least 2-3 batches.
		var callbackCount int
		var rowBatchCount int // callbacks that contain rows (not RowDescription or CommandComplete)
		var totalRows int
		var resultSetCount int

		err := conn.QueryStreaming(ctx, "SELECT repeat('x', 1000) AS data FROM generate_series(1, 5000)",
			func(ctx context.Context, result *sqltypes.Result) error {
				callbackCount++
				if len(result.Rows) > 0 {
					rowBatchCount++
					totalRows += len(result.Rows)
					t.Logf("Batch %d: %d rows", rowBatchCount, len(result.Rows))
				}
				if result.CommandTag != "" {
					resultSetCount++
				}
				return nil
			})
		require.NoError(t, err)

		// Verify we got multiple row batches (proving the 2MB threshold works)
		assert.Equal(t, 5000, totalRows)
		assert.Equal(t, 1, resultSetCount)
		// With 5MB of data and 2MB batch size, we expect at least 2 row batches
		assert.GreaterOrEqual(t, rowBatchCount, 2, "expected multiple row batches for large result set")
		t.Logf("Total callbacks: %d, row batches: %d, rows: %d", callbackCount, rowBatchCount, totalRows)
	})
}
