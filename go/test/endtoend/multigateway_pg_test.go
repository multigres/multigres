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

package endtoend

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

	"github.com/multigres/multigres/go/test/utils"
)

// TestMultiGateway_PostgreSQLConnection tests that we can connect to multigateway via PostgreSQL protocol
// and execute queries. This is a true end-to-end test that uses the full cluster setup.
func TestMultiGateway_PostgreSQLConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping PostgreSQLConnection test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping cluster lifecycle tests")
	}

	// Setup full test cluster with all services (includes waiting for bootstrap)
	cluster := setupTestCluster(t)
	t.Cleanup(cluster.Cleanup)

	// Find a multigateway that has access to the PRIMARY pooler.
	// Each multigateway only discovers poolers in its own zone, and the PRIMARY
	// may be in any zone after bootstrap. Try all zones to find one that works.
	pgPorts := []int{
		cluster.PortConfig.Zones[0].MultigatewayPGPort,
		cluster.PortConfig.Zones[1].MultigatewayPGPort,
	}
	findCtx, findCancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer findCancel()
	readyPort, err := findReadyMultigateway(t, findCtx, pgPorts)
	require.NoError(t, err, "should find a ready multigateway")

	// Connect to the multigateway that has the PRIMARY pooler
	connStr := fmt.Sprintf("host=localhost port=%d user=postgres password=postgres dbname=postgres sslmode=disable connect_timeout=5",
		readyPort)
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err, "failed to open database connection")
	defer db.Close()

	// Set connection timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Ping to verify connection
	err = db.PingContext(ctx)
	require.NoError(t, err, "failed to ping database - multigateway may not be ready")

	t.Run("basic SELECT query", func(t *testing.T) {
		// Execute a simple query that should work on any PostgreSQL database
		rows, err := db.QueryContext(ctx, "SELECT 1 as num, 'hello' as greeting")
		require.NoError(t, err, "failed to execute query")
		defer rows.Close()

		// Verify we got columns
		columns, err := rows.Columns()
		require.NoError(t, err, "failed to get columns")
		assert.Equal(t, []string{"num", "greeting"}, columns, "unexpected columns")

		// Verify we got the expected row
		require.True(t, rows.Next(), "expected at least one row")

		var num int
		var greeting string
		err = rows.Scan(&num, &greeting)
		require.NoError(t, err, "failed to scan row")
		assert.Equal(t, 1, num, "unexpected num value")
		assert.Equal(t, "hello", greeting, "unexpected greeting value")

		// Verify no more rows
		assert.False(t, rows.Next(), "expected only one row")
		assert.NoError(t, rows.Err(), "rows iteration error")
	})

	t.Run("test table create, insert, select, drop", func(t *testing.T) {
		tableName := fmt.Sprintf("mg_test_%d", time.Now().UnixNano())

		// Create table
		_, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, name TEXT)", tableName))
		require.NoError(t, err, "failed to create table")

		// Insert data
		_, err = db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'test1'), (2, 'test2')", tableName))
		require.NoError(t, err, "failed to insert data")

		// Select data
		rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT id, name FROM %s ORDER BY id", tableName))
		require.NoError(t, err, "failed to select data")
		defer rows.Close()

		// Verify rows
		var results []struct {
			id   int
			name string
		}
		for rows.Next() {
			var r struct {
				id   int
				name string
			}
			err = rows.Scan(&r.id, &r.name)
			require.NoError(t, err, "failed to scan row")
			results = append(results, r)
		}

		assert.Len(t, results, 2, "expected 2 rows")
		assert.Equal(t, 1, results[0].id)
		assert.Equal(t, "test1", results[0].name)
		assert.Equal(t, 2, results[1].id)
		assert.Equal(t, "test2", results[1].name)

		// Drop table
		_, err = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE %s", tableName))
		require.NoError(t, err, "failed to drop table")
	})

	t.Run("test UPDATE and DELETE", func(t *testing.T) {
		tableName := fmt.Sprintf("mg_test_%d", time.Now().UnixNano())

		// Create and populate table
		_, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, value INT)", tableName))
		require.NoError(t, err, "failed to create table")
		_, err = db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, value) VALUES (1, 10), (2, 20), (3, 30)", tableName))
		require.NoError(t, err, "failed to insert data")

		// Update
		result, err := db.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET value = 100 WHERE id = 1", tableName))
		require.NoError(t, err, "failed to update")
		rowsAffected, err := result.RowsAffected()
		require.NoError(t, err)
		assert.Equal(t, int64(1), rowsAffected, "expected 1 row affected by UPDATE")

		// Delete
		result, err = db.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE id = 2", tableName))
		require.NoError(t, err, "failed to delete")
		rowsAffected, err = result.RowsAffected()
		require.NoError(t, err)
		assert.Equal(t, int64(1), rowsAffected, "expected 1 row affected by DELETE")

		// Verify final state
		rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT id, value FROM %s ORDER BY id", tableName))
		require.NoError(t, err)
		defer rows.Close()

		var results []struct {
			id    int
			value int
		}
		for rows.Next() {
			var r struct {
				id    int
				value int
			}
			err = rows.Scan(&r.id, &r.value)
			require.NoError(t, err)
			results = append(results, r)
		}

		assert.Len(t, results, 2, "expected 2 rows after UPDATE and DELETE")
		assert.Equal(t, 1, results[0].id)
		assert.Equal(t, 100, results[0].value)
		assert.Equal(t, 3, results[1].id)
		assert.Equal(t, 30, results[1].value)

		// Cleanup
		_, err = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE %s", tableName))
		require.NoError(t, err)
	})
}

// TestMultiGateway_ExtendedQueryProtocol tests the Extended Query Protocol (prepared statements, parameterized queries)
// using pgx which uses extended protocol by default.
func TestMultiGateway_ExtendedQueryProtocol(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping ExtendedQueryProtocol test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping cluster lifecycle tests")
	}

	// Setup full test cluster with all services
	cluster := setupTestCluster(t)
	t.Cleanup(cluster.Cleanup)

	// Connect using pgx (which uses Extended Query Protocol by default)
	connStr := fmt.Sprintf("host=localhost port=%d user=postgres password=postgres dbname=postgres sslmode=disable",
		cluster.PortConfig.Zones[0].MultigatewayPGPort)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err, "failed to connect with pgx")
	defer conn.Close(ctx)

	t.Run("parameterized query with args", func(t *testing.T) {
		// pgx uses extended protocol for parameterized queries
		var num int
		var greeting string
		err := conn.QueryRow(ctx, "SELECT $1::int as num, $2::text as greeting", 42, "hello pgx").Scan(&num, &greeting)
		require.NoError(t, err, "failed to execute parameterized query")
		assert.Equal(t, 42, num)
		assert.Equal(t, "hello pgx", greeting)
	})

	t.Run("prepared statement execution", func(t *testing.T) {
		tableName := fmt.Sprintf("pgx_test_%d", time.Now().UnixNano())

		// Create table
		_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, name TEXT, value NUMERIC)", tableName))
		require.NoError(t, err, "failed to create table")
		defer func() {
			_, _ = conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
		}()

		// Prepare a statement (explicit prepare)
		stmtName := "insert_stmt"
		_, err = conn.Prepare(ctx, stmtName, fmt.Sprintf("INSERT INTO %s (id, name, value) VALUES ($1, $2, $3)", tableName))
		require.NoError(t, err, "failed to prepare statement")

		// Execute prepared statement multiple times with different values
		testCases := []struct {
			id    int
			name  string
			value float64
		}{
			{1, "first", 100.50},
			{2, "second", 200.75},
			{3, "third", 300.25},
		}

		for _, tc := range testCases {
			_, err = conn.Exec(ctx, stmtName, tc.id, tc.name, tc.value)
			require.NoError(t, err, "failed to execute prepared statement for id=%d", tc.id)
		}

		// Verify all rows were inserted
		rows, err := conn.Query(ctx, fmt.Sprintf("SELECT id, name, value FROM %s ORDER BY id", tableName))
		require.NoError(t, err, "failed to select data")
		defer rows.Close()

		var results []struct {
			id    int
			name  string
			value float64
		}
		for rows.Next() {
			var r struct {
				id    int
				name  string
				value float64
			}
			err = rows.Scan(&r.id, &r.name, &r.value)
			require.NoError(t, err, "failed to scan row")
			results = append(results, r)
		}

		assert.Len(t, results, 3, "expected 3 rows")
		for i, tc := range testCases {
			assert.Equal(t, tc.id, results[i].id)
			assert.Equal(t, tc.name, results[i].name)
			assert.InDelta(t, tc.value, results[i].value, 0.01)
		}
	})

	t.Run("multiple data types via extended protocol", func(t *testing.T) {
		// Test various PostgreSQL data types through extended protocol
		var (
			boolVal   bool
			intVal    int32
			bigintVal int64
			floatVal  float64
			textVal   string
			bytesVal  []byte
		)

		err := conn.QueryRow(ctx,
			"SELECT $1::boolean, $2::integer, $3::bigint, $4::double precision, $5::text, $6::bytea",
			true, int32(12345), int64(9876543210), 3.14159, "test string", []byte{0xDE, 0xAD, 0xBE, 0xEF},
		).Scan(&boolVal, &intVal, &bigintVal, &floatVal, &textVal, &bytesVal)

		require.NoError(t, err, "failed to query multiple data types")
		assert.True(t, boolVal)
		assert.Equal(t, int32(12345), intVal)
		assert.Equal(t, int64(9876543210), bigintVal)
		assert.InDelta(t, 3.14159, floatVal, 0.0001)
		assert.Equal(t, "test string", textVal)
		assert.Equal(t, []byte{0xDE, 0xAD, 0xBE, 0xEF}, bytesVal)
	})

	t.Run("batch queries via extended protocol", func(t *testing.T) {
		t.Skip("Batch queries require pipeline support which is not yet implemented")

		tableName := fmt.Sprintf("batch_test_%d", time.Now().UnixNano())

		// Create table
		_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, data TEXT)", tableName))
		require.NoError(t, err)
		defer func() {
			_, _ = conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
		}()

		// Use pgx batch for multiple operations
		batch := &pgx.Batch{}
		for i := 1; i <= 5; i++ {
			batch.Queue(fmt.Sprintf("INSERT INTO %s (id, data) VALUES ($1, $2)", tableName), i, fmt.Sprintf("data_%d", i))
		}
		batch.Queue(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName))

		results := conn.SendBatch(ctx, batch)
		defer results.Close()

		// Check insert results
		for i := range 5 {
			_, err := results.Exec()
			require.NoError(t, err, "batch insert %d failed", i+1)
		}

		// Check count result
		var count int
		err = results.QueryRow().Scan(&count)
		require.NoError(t, err, "failed to get count from batch")
		assert.Equal(t, 5, count, "expected 5 rows after batch insert")
	})

	t.Run("NULL handling via extended protocol", func(t *testing.T) {
		t.Skip("Null Handling fails currently")
		var nullableInt *int
		var nullableText *string

		// Query NULL values
		err := conn.QueryRow(ctx, "SELECT $1::integer, $2::text", nil, nil).Scan(&nullableInt, &nullableText)
		require.NoError(t, err, "failed to query NULL values")
		assert.Nil(t, nullableInt, "expected NULL integer")
		assert.Nil(t, nullableText, "expected NULL text")

		// Query non-NULL values
		err = conn.QueryRow(ctx, "SELECT $1::integer, $2::text", 42, "not null").Scan(&nullableInt, &nullableText)
		require.NoError(t, err, "failed to query non-NULL values")
		require.NotNil(t, nullableInt)
		require.NotNil(t, nullableText)
		assert.Equal(t, 42, *nullableInt)
		assert.Equal(t, "not null", *nullableText)
	})

	t.Run("cursor via DECLARE and FETCH", func(t *testing.T) {
		t.Skip("Cursors require transaction management which is not yet implemented")

		tableName := fmt.Sprintf("cursor_test_%d", time.Now().UnixNano())

		// Create and populate table
		_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, value TEXT)", tableName))
		require.NoError(t, err)
		defer func() {
			_, _ = conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
		}()

		// Insert test data
		for i := 1; i <= 10; i++ {
			_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s (id, value) VALUES ($1, $2)", tableName), i, fmt.Sprintf("value_%d", i))
			require.NoError(t, err)
		}

		// Start transaction for cursor
		tx, err := conn.Begin(ctx)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback(ctx) }()

		// Declare cursor
		cursorName := "test_cursor"
		_, err = tx.Exec(ctx, fmt.Sprintf("DECLARE %s CURSOR FOR SELECT id, value FROM %s ORDER BY id", cursorName, tableName))
		require.NoError(t, err, "failed to declare cursor")

		// Fetch first 3 rows
		rows, err := tx.Query(ctx, fmt.Sprintf("FETCH 3 FROM %s", cursorName))
		require.NoError(t, err, "failed to fetch from cursor")

		var fetchedIds []int
		for rows.Next() {
			var id int
			var value string
			err = rows.Scan(&id, &value)
			require.NoError(t, err)
			fetchedIds = append(fetchedIds, id)
		}
		rows.Close()

		assert.Equal(t, []int{1, 2, 3}, fetchedIds, "expected first 3 rows from cursor")

		// Fetch next 3 rows
		rows, err = tx.Query(ctx, fmt.Sprintf("FETCH 3 FROM %s", cursorName))
		require.NoError(t, err)

		fetchedIds = nil
		for rows.Next() {
			var id int
			var value string
			err = rows.Scan(&id, &value)
			require.NoError(t, err)
			fetchedIds = append(fetchedIds, id)
		}
		rows.Close()

		assert.Equal(t, []int{4, 5, 6}, fetchedIds, "expected next 3 rows from cursor")

		// Close cursor
		_, err = tx.Exec(ctx, fmt.Sprintf("CLOSE %s", cursorName))
		require.NoError(t, err, "failed to close cursor")

		err = tx.Commit(ctx)
		require.NoError(t, err)
	})

	t.Run("transaction with prepared statements", func(t *testing.T) {
		t.Skip("Transactions require transaction management which is not yet implemented")

		tableName := fmt.Sprintf("txn_ps_test_%d", time.Now().UnixNano())

		// Create table
		_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, balance NUMERIC)", tableName))
		require.NoError(t, err)
		defer func() {
			_, _ = conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
		}()

		// Insert initial data
		_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s (id, balance) VALUES (1, 1000), (2, 500)", tableName))
		require.NoError(t, err)

		// Start transaction
		tx, err := conn.Begin(ctx)
		require.NoError(t, err)

		// Prepare statement within transaction
		updateStmt := "update_balance"
		_, err = tx.Prepare(ctx, updateStmt, fmt.Sprintf("UPDATE %s SET balance = balance + $1 WHERE id = $2", tableName))
		require.NoError(t, err)

		// Transfer 200 from account 1 to account 2
		_, err = tx.Exec(ctx, updateStmt, -200, 1)
		require.NoError(t, err)
		_, err = tx.Exec(ctx, updateStmt, 200, 2)
		require.NoError(t, err)

		// Commit transaction
		err = tx.Commit(ctx)
		require.NoError(t, err)

		// Verify balances
		var balance1, balance2 float64
		err = conn.QueryRow(ctx, fmt.Sprintf("SELECT balance FROM %s WHERE id = 1", tableName)).Scan(&balance1)
		require.NoError(t, err)
		err = conn.QueryRow(ctx, fmt.Sprintf("SELECT balance FROM %s WHERE id = 2", tableName)).Scan(&balance2)
		require.NoError(t, err)

		assert.InDelta(t, 800.0, balance1, 0.01, "account 1 should have 800")
		assert.InDelta(t, 700.0, balance2, 0.01, "account 2 should have 700")
	})

	t.Run("error handling in extended protocol", func(t *testing.T) {
		t.Skip("Error handling requires transaction state management which is not yet implemented")

		// Test that errors are properly propagated through extended protocol

		// Division by zero
		var result int
		err := conn.QueryRow(ctx, "SELECT $1::int / $2::int", 10, 0).Scan(&result)
		require.Error(t, err, "expected division by zero error")
		assert.Contains(t, err.Error(), "division by zero")

		// Invalid type cast
		err = conn.QueryRow(ctx, "SELECT $1::integer", "not a number").Scan(&result)
		require.Error(t, err, "expected invalid input syntax error")

		// Connection should still be usable after errors
		err = conn.QueryRow(ctx, "SELECT 1").Scan(&result)
		require.NoError(t, err, "connection should still work after error")
		assert.Equal(t, 1, result)
	})
}
