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

package queryserving

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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

// TestMultigateway_PostgreSQLConnection tests that we can connect to multigateway via PostgreSQL protocol
// and execute queries. This is a true end-to-end test that uses the full cluster setup.
// Each subtest runs against both direct PostgreSQL and multigateway to ensure
// the proxy behavior matches native PostgreSQL exactly.
func TestMultigateway_PostgreSQLConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping PostgreSQLConnection test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping cluster lifecycle tests")
	}

	// Use shared test cluster with multigateway
	setup := getSharedSetup(t)

	// Setup test cleanup - this will ensure clean state after test completes
	setup.SetupTest(t)

	// Set connection timeout
	ctx := utils.WithTimeout(t, 10*time.Second)

	for _, target := range setup.GetComparisonTargets(t) {
		t.Run(target.Name, func(t *testing.T) {
			// Connect to the target
			connStr := shardsetup.GetTestUserDSN("localhost", target.Port, "sslmode=disable", "connect_timeout=5")
			db, err := sql.Open("postgres", connStr)
			require.NoError(t, err, "failed to open database connection")
			defer db.Close()

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
				_, err = db.ExecContext(ctx, "DROP TABLE "+tableName)
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
				_, err = db.ExecContext(ctx, "DROP TABLE "+tableName)
				require.NoError(t, err)
			})
		})
	}
}

// TestMultigateway_ExtendedQueryProtocol tests the Extended Query Protocol (prepared statements, parameterized queries)
// using pgx which uses extended protocol by default.
// Each subtest runs against both direct PostgreSQL and multigateway to ensure
// the proxy behavior matches native PostgreSQL exactly.
func TestMultigateway_ExtendedQueryProtocol(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping ExtendedQueryProtocol test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping cluster lifecycle tests")
	}

	// Use shared test cluster with multigateway
	setup := getSharedSetup(t)

	// Setup test cleanup - this will ensure clean state after test completes
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 30*time.Second)

	for _, target := range setup.GetComparisonTargets(t) {
		t.Run(target.Name, func(t *testing.T) {
			// Connect using pgx (which uses Extended Query Protocol by default)
			connStr := shardsetup.GetTestUserDSN("localhost", target.Port, "sslmode=disable")

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
					_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+tableName)
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
					_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+tableName)
				}()

				// Use pgx batch for multiple operations
				batch := &pgx.Batch{}
				for i := 1; i <= 5; i++ {
					batch.Queue(fmt.Sprintf("INSERT INTO %s (id, data) VALUES ($1, $2)", tableName), i, fmt.Sprintf("data_%d", i))
				}
				batch.Queue("SELECT COUNT(*) FROM " + tableName)

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

			t.Run("NULL vs empty string distinction", func(t *testing.T) {
				tableName := fmt.Sprintf("null_empty_test_%d", time.Now().UnixNano())

				// Create a table with a nullable text column
				_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, value TEXT)", tableName))
				require.NoError(t, err, "failed to create table")
				defer func() {
					_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+tableName)
				}()

				// Insert NULL, empty string, and regular string
				_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s (id, value) VALUES ($1, $2)", tableName), 1, nil)
				require.NoError(t, err, "failed to insert NULL value")
				_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s (id, value) VALUES ($1, $2)", tableName), 2, "")
				require.NoError(t, err, "failed to insert empty string")
				_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s (id, value) VALUES ($1, $2)", tableName), 3, "hello")
				require.NoError(t, err, "failed to insert regular string")

				// Query and verify the distinction is preserved
				rows, err := conn.Query(ctx, fmt.Sprintf("SELECT id, value FROM %s ORDER BY id", tableName))
				require.NoError(t, err, "failed to query table")
				defer rows.Close()

				// Row 1: NULL
				require.True(t, rows.Next(), "expected row 1")
				var id int
				var value *string
				err = rows.Scan(&id, &value)
				require.NoError(t, err, "failed to scan row 1")
				assert.Equal(t, 1, id)
				assert.Nil(t, value, "row 1 should be NULL, not empty string")

				// Row 2: empty string
				require.True(t, rows.Next(), "expected row 2")
				err = rows.Scan(&id, &value)
				require.NoError(t, err, "failed to scan row 2")
				assert.Equal(t, 2, id)
				require.NotNil(t, value, "row 2 should be empty string, not NULL")
				assert.Equal(t, "", *value, "row 2 should be empty string")

				// Row 3: regular string
				require.True(t, rows.Next(), "expected row 3")
				err = rows.Scan(&id, &value)
				require.NoError(t, err, "failed to scan row 3")
				assert.Equal(t, 3, id)
				require.NotNil(t, value, "row 3 should be 'hello'")
				assert.Equal(t, "hello", *value)

				require.False(t, rows.Next(), "expected no more rows")
				require.NoError(t, rows.Err())
			})

			t.Run("cursor via DECLARE and FETCH", func(t *testing.T) {
				tableName := fmt.Sprintf("cursor_test_%d", time.Now().UnixNano())

				// Create and populate table
				_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, value TEXT)", tableName))
				require.NoError(t, err)
				defer func() {
					_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+tableName)
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
				rows, err := tx.Query(ctx, "FETCH 3 FROM "+cursorName)
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
				rows, err = tx.Query(ctx, "FETCH 3 FROM "+cursorName)
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
				_, err = tx.Exec(ctx, "CLOSE "+cursorName)
				require.NoError(t, err, "failed to close cursor")

				err = tx.Commit(ctx)
				require.NoError(t, err)
			})

			// MUL-389: DECLARE … WITH HOLD promotes the cursor to session-level
			// state that must survive COMMIT. Without backend pinning, the
			// reserved connection is released to the pool when the transaction
			// commits and the next FETCH lands on a different backend, raising
			// `cursor "x" does not exist`. This test exercises the full
			// gateway-managed pin / release lifecycle.
			t.Run("cursor WITH HOLD survives COMMIT", func(t *testing.T) {
				tableName := fmt.Sprintf("hold_cursor_test_%d", time.Now().UnixNano())

				_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (i int)", tableName))
				require.NoError(t, err)
				defer func() {
					_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+tableName)
				}()
				_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s SELECT generate_series(1,5)", tableName))
				require.NoError(t, err)

				cursorName := "hold_cur"

				tx, err := conn.Begin(ctx)
				require.NoError(t, err)
				_, err = tx.Exec(ctx, fmt.Sprintf("DECLARE %s SCROLL CURSOR WITH HOLD FOR SELECT i FROM %s ORDER BY i", cursorName, tableName))
				require.NoError(t, err, "failed to declare WITH HOLD cursor")

				rows, err := tx.Query(ctx, "FETCH 1 FROM "+cursorName)
				require.NoError(t, err, "failed to fetch from WITH HOLD cursor inside txn")
				var got []int
				for rows.Next() {
					var v int
					require.NoError(t, rows.Scan(&v))
					got = append(got, v)
				}
				rows.Close()
				require.NoError(t, rows.Err())
				assert.Equal(t, []int{1}, got, "expected first row inside transaction")

				require.NoError(t, tx.Commit(ctx), "COMMIT should succeed and leave WITH HOLD cursor open")

				// Acceptance #1: FETCH after COMMIT must still see the cursor.
				rows, err = conn.Query(ctx, "FETCH FROM "+cursorName)
				require.NoError(t, err, "failed to fetch from WITH HOLD cursor AFTER COMMIT — cursor was lost")
				got = nil
				for rows.Next() {
					var v int
					require.NoError(t, rows.Scan(&v))
					got = append(got, v)
				}
				rows.Close()
				require.NoError(t, rows.Err())
				assert.Equal(t, []int{2}, got, "WITH HOLD cursor must continue from row 2 after COMMIT")

				// Acceptance #3: CLOSE on the HOLD cursor releases the backend
				// pin. A follow-up FETCH on the (now-closed) cursor errors.
				// Use Exec — pgx.Query is lazy and would defer the error
				// past require.Error.
				_, err = conn.Exec(ctx, "CLOSE "+cursorName)
				require.NoError(t, err, "failed to close WITH HOLD cursor")

				_, err = conn.Exec(ctx, "FETCH FROM "+cursorName)
				require.Error(t, err, "cursor should not exist after CLOSE")
			})

			// MUL-389 follow-up: WITH HOLD declared via the extended query
			// protocol (Parse/Bind/Execute) must also pin the backend.
			// Without gateway dispatch for T_DeclareCursorStmt on the
			// extended-protocol Plan path, the DECLARE would land on a pooled
			// connection and the cursor would be lost on COMMIT.
			t.Run("cursor WITH HOLD via extended protocol", func(t *testing.T) {
				tableName := fmt.Sprintf("hold_ext_test_%d", time.Now().UnixNano())

				_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (i int)", tableName))
				require.NoError(t, err)
				defer func() {
					_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+tableName)
				}()
				_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s SELECT generate_series(1,5)", tableName))
				require.NoError(t, err)

				cursorName := "hold_cur_ext"
				declStmt := "decl_hold_ext"
				closeStmt := "close_hold_ext"

				// Forcing Parse/Bind/Execute by prepared name routes the
				// statement through the extended-protocol Plan path — the exact
				// code path the reviewer flagged.
				_, err = conn.Prepare(ctx, declStmt, fmt.Sprintf("DECLARE %s SCROLL CURSOR WITH HOLD FOR SELECT i FROM %s ORDER BY i", cursorName, tableName))
				require.NoError(t, err)
				_, err = conn.Prepare(ctx, closeStmt, "CLOSE "+cursorName)
				require.NoError(t, err)

				tx, err := conn.Begin(ctx)
				require.NoError(t, err)
				_, err = tx.Exec(ctx, declStmt)
				require.NoError(t, err, "extended-protocol DECLARE WITH HOLD failed")

				rows, err := tx.Query(ctx, "FETCH 1 FROM "+cursorName)
				require.NoError(t, err)
				var got []int
				for rows.Next() {
					var v int
					require.NoError(t, rows.Scan(&v))
					got = append(got, v)
				}
				rows.Close()
				require.NoError(t, rows.Err())
				assert.Equal(t, []int{1}, got)

				require.NoError(t, tx.Commit(ctx), "COMMIT after extended-protocol DECLARE WITH HOLD")

				rows, err = conn.Query(ctx, "FETCH FROM "+cursorName)
				require.NoError(t, err, "cursor lost after COMMIT — extended-protocol DECLARE bypassed HoldCursorRoute")
				got = nil
				for rows.Next() {
					var v int
					require.NoError(t, rows.Scan(&v))
					got = append(got, v)
				}
				rows.Close()
				require.NoError(t, rows.Err())
				assert.Equal(t, []int{2}, got)

				_, err = conn.Exec(ctx, closeStmt)
				require.NoError(t, err, "extended-protocol CLOSE failed")

				_, err = conn.Exec(ctx, "FETCH FROM "+cursorName)
				require.Error(t, err, "cursor should not exist after CLOSE")
			})

			// MUL-389 follow-up: explicit ROLLBACK must close WITH HOLD
			// cursors (PG semantics: ROLLBACK drops every open cursor).
			t.Run("WITH HOLD closed by explicit ROLLBACK", func(t *testing.T) {
				tableName := fmt.Sprintf("hold_rb_test_%d", time.Now().UnixNano())

				_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (i int)", tableName))
				require.NoError(t, err)
				defer func() {
					_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+tableName)
				}()
				_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s SELECT generate_series(1,3)", tableName))
				require.NoError(t, err)

				cursorName := "hold_cur_rb"
				tx, err := conn.Begin(ctx)
				require.NoError(t, err)
				_, err = tx.Exec(ctx, fmt.Sprintf("DECLARE %s CURSOR WITH HOLD FOR SELECT i FROM %s ORDER BY i", cursorName, tableName))
				require.NoError(t, err)
				require.NoError(t, tx.Rollback(ctx))

				_, err = conn.Exec(ctx, "FETCH FROM "+cursorName)
				require.Error(t, err, "WITH HOLD cursor must be closed by explicit ROLLBACK")
				var n int
				require.NoError(t, conn.QueryRow(ctx, "SELECT 1").Scan(&n))
				assert.Equal(t, 1, n)
			})

			// MUL-389 follow-up: CLOSE ALL must drop every HOLD pin.
			t.Run("CLOSE ALL releases all HOLD cursors", func(t *testing.T) {
				tableName := fmt.Sprintf("hold_ca_test_%d", time.Now().UnixNano())

				_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (i int)", tableName))
				require.NoError(t, err)
				defer func() {
					_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+tableName)
				}()
				_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s SELECT generate_series(1,5)", tableName))
				require.NoError(t, err)

				tx, err := conn.Begin(ctx)
				require.NoError(t, err)
				for _, name := range []string{"hc1", "hc2", "hc3"} {
					_, err = tx.Exec(ctx, fmt.Sprintf("DECLARE %s CURSOR WITH HOLD FOR SELECT i FROM %s ORDER BY i", name, tableName))
					require.NoError(t, err, "DECLARE %s WITH HOLD failed", name)
				}
				require.NoError(t, tx.Commit(ctx))

				// All three should still be open post-COMMIT.
				for _, name := range []string{"hc1", "hc2", "hc3"} {
					_, err = conn.Exec(ctx, "FETCH FROM "+name)
					require.NoError(t, err, "%s should survive COMMIT", name)
				}

				_, err = conn.Exec(ctx, "CLOSE ALL")
				require.NoError(t, err)

				for _, name := range []string{"hc1", "hc2", "hc3"} {
					_, err = conn.Exec(ctx, "FETCH FROM "+name)
					require.Error(t, err, "%s should be closed after CLOSE ALL", name)
				}
				var n int
				require.NoError(t, conn.QueryRow(ctx, "SELECT 1").Scan(&n))
				assert.Equal(t, 1, n)
			})

			// MUL-389 follow-up: DISCARD ALL closes every cursor. The
			// gateway handles DISCARD ALL locally and releases the reserved
			// connection (pinned by the HOLD cursor) back to the pool, so
			// the multipooler's portal pin set drains alongside PG's wipe.
			t.Run("DISCARD ALL releases HOLD cursor pins", func(t *testing.T) {
				tableName := fmt.Sprintf("hold_da_test_%d", time.Now().UnixNano())

				_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (i int)", tableName))
				require.NoError(t, err)
				defer func() {
					_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+tableName)
				}()
				_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s SELECT generate_series(1,3)", tableName))
				require.NoError(t, err)

				tx, err := conn.Begin(ctx)
				require.NoError(t, err)
				_, err = tx.Exec(ctx, fmt.Sprintf("DECLARE hold_cur_da CURSOR WITH HOLD FOR SELECT i FROM %s ORDER BY i", tableName))
				require.NoError(t, err)
				require.NoError(t, tx.Commit(ctx))

				_, err = conn.Exec(ctx, "DISCARD ALL")
				require.NoError(t, err, "DISCARD ALL should succeed even with a HOLD cursor open")

				_, err = conn.Exec(ctx, "FETCH FROM hold_cur_da")
				require.Error(t, err, "HOLD cursor must be closed by DISCARD ALL")
				// pgx's stmtcache holds prepared-statement names that PG
				// dropped on DISCARD ALL — a regular conn.Exec for
				// `SELECT 1` reuses the cache and tries to Bind a
				// no-longer-prepared name. Use Ping (simple protocol,
				// no cache) to verify the session is still alive without
				// tripping the cache.
				require.NoError(t, conn.Ping(ctx),
					"session must be usable after DISCARD ALL on a session with HOLD cursors")
			})

			// MUL-389 follow-up: ROLLBACK TO SAVEPOINT must close any HOLD
			// cursor declared inside the rolled-back sub-transaction, and
			// leave cursors declared before the savepoint open.
			t.Run("ROLLBACK TO SAVEPOINT closes sub-txn HOLD cursors", func(t *testing.T) {
				tableName := fmt.Sprintf("hold_sp_test_%d", time.Now().UnixNano())

				_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (i int)", tableName))
				require.NoError(t, err)
				defer func() {
					_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+tableName)
				}()
				_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s SELECT generate_series(1,3)", tableName))
				require.NoError(t, err)

				tx, err := conn.Begin(ctx)
				require.NoError(t, err)
				_, err = tx.Exec(ctx, fmt.Sprintf("DECLARE hold_before CURSOR WITH HOLD FOR SELECT i FROM %s ORDER BY i", tableName))
				require.NoError(t, err)

				_, err = tx.Exec(ctx, "SAVEPOINT sp1")
				require.NoError(t, err)
				_, err = tx.Exec(ctx, fmt.Sprintf("DECLARE hold_after CURSOR WITH HOLD FOR SELECT i FROM %s ORDER BY i", tableName))
				require.NoError(t, err)

				_, err = tx.Exec(ctx, "ROLLBACK TO SAVEPOINT sp1")
				require.NoError(t, err)

				require.NoError(t, tx.Commit(ctx))

				// `hold_before` survives — declared before the savepoint.
				rows, err := conn.Query(ctx, "FETCH 1 FROM hold_before")
				require.NoError(t, err)
				var got []int
				for rows.Next() {
					var v int
					require.NoError(t, rows.Scan(&v))
					got = append(got, v)
				}
				rows.Close()
				require.NoError(t, rows.Err())
				assert.Equal(t, []int{1}, got)

				// `hold_after` was declared inside the rolled-back
				// subtransaction → must be gone.
				_, err = conn.Exec(ctx, "FETCH FROM hold_after")
				require.Error(t, err, "HOLD cursor declared inside rolled-back subtxn must be closed")

				_, err = conn.Exec(ctx, "CLOSE hold_before")
				require.NoError(t, err)
			})

			// MUL-389 follow-up: PostgreSQL's `CLOSE` is *not*
			// transactional — a cursor explicitly CLOSE'd inside a
			// sub-transaction stays closed after ROLLBACK TO. Verify
			// the gateway agrees: a follow-up FETCH must error, and
			// the gateway's HOLD-cursor tracking must drop the entry
			// (no stale ReasonPortal pin lingering past COMMIT).
			t.Run("CLOSE inside savepoint survives ROLLBACK TO", func(t *testing.T) {
				tableName := fmt.Sprintf("hold_cs_test_%d", time.Now().UnixNano())

				_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (i int)", tableName))
				require.NoError(t, err)
				defer func() {
					_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+tableName)
				}()
				_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s SELECT generate_series(1,3)", tableName))
				require.NoError(t, err)

				tx, err := conn.Begin(ctx)
				require.NoError(t, err)
				_, err = tx.Exec(ctx, fmt.Sprintf("DECLARE hold_cs CURSOR WITH HOLD FOR SELECT i FROM %s ORDER BY i", tableName))
				require.NoError(t, err)

				_, err = tx.Exec(ctx, "SAVEPOINT sp_cs")
				require.NoError(t, err)
				_, err = tx.Exec(ctx, "CLOSE hold_cs")
				require.NoError(t, err)

				// ROLLBACK TO does NOT undo the CLOSE — PG keeps the
				// cursor closed.
				_, err = tx.Exec(ctx, "ROLLBACK TO SAVEPOINT sp_cs")
				require.NoError(t, err)

				require.NoError(t, tx.Commit(ctx))

				_, err = conn.Exec(ctx, "FETCH FROM hold_cs")
				require.Error(t, err, "CLOSE must survive ROLLBACK TO — cursor stays closed")

				require.NoError(t, conn.Ping(ctx),
					"session must be usable after the CLOSE/ROLLBACK TO sequence")
			})

			// MUL-389 follow-up: HOLD cursor + temp table in the same
			// transaction — both ReasonPortal and ReasonTempTable must
			// survive COMMIT, with HOLD cursor still readable and temp
			// table still queryable.
			t.Run("WITH HOLD + temp table survive COMMIT together", func(t *testing.T) {
				tableName := fmt.Sprintf("hold_tt_test_%d", time.Now().UnixNano())
				tempName := fmt.Sprintf("tt_%d", time.Now().UnixNano())

				_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (i int)", tableName))
				require.NoError(t, err)
				defer func() {
					_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+tableName)
				}()
				_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s SELECT generate_series(1,3)", tableName))
				require.NoError(t, err)

				tx, err := conn.Begin(ctx)
				require.NoError(t, err)
				_, err = tx.Exec(ctx, fmt.Sprintf("CREATE TEMP TABLE %s (j int)", tempName))
				require.NoError(t, err)
				_, err = tx.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (42)", tempName))
				require.NoError(t, err)
				_, err = tx.Exec(ctx, fmt.Sprintf("DECLARE hold_tt CURSOR WITH HOLD FOR SELECT i FROM %s ORDER BY i", tableName))
				require.NoError(t, err)
				require.NoError(t, tx.Commit(ctx))

				// HOLD cursor survives.
				rows, err := conn.Query(ctx, "FETCH 1 FROM hold_tt")
				require.NoError(t, err)
				var got []int
				for rows.Next() {
					var v int
					require.NoError(t, rows.Scan(&v))
					got = append(got, v)
				}
				rows.Close()
				require.NoError(t, rows.Err())
				assert.Equal(t, []int{1}, got)

				// Temp table survives on the same backend.
				var v int
				require.NoError(t, conn.QueryRow(ctx, "SELECT j FROM "+tempName).Scan(&v))
				assert.Equal(t, 42, v)

				_, err = conn.Exec(ctx, "CLOSE hold_tt")
				require.NoError(t, err)
			})

			// MUL-389 follow-up: DECLARE WITH HOLD outside an explicit
			// transaction must not leak a reserved connection. PG itself
			// allows the statement under extended-protocol autocommit
			// (the implicit txn wraps the single Parse/Bind/Execute/Sync
			// cycle); whether the cursor survives the autocommit boundary
			// is up to PG. Either way, the gateway must clean up — no
			// reserved backend lingering, and the session must continue
			// to serve queries normally.
			t.Run("WITH HOLD outside txn does not leak reservation", func(t *testing.T) {
				cursorName := fmt.Sprintf("c_outside_%d", time.Now().UnixNano())
				if _, err := conn.Exec(ctx, fmt.Sprintf("DECLARE %s CURSOR WITH HOLD FOR SELECT 1", cursorName)); err == nil {
					// PG accepted the DECLARE — best-effort cleanup so the
					// next iteration sees a clean session. The cursor may
					// or may not exist at this point depending on PG's
					// implicit-commit semantics.
					_, _ = conn.Exec(ctx, "CLOSE "+cursorName)
				}

				// The real assertion: session is still alive.
				require.NoError(t, conn.Ping(ctx),
					"session must be usable after DECLARE WITH HOLD outside a transaction")
			})

			// MUL-389 follow-up: a HOLD cursor declared *before* an explicit
			// BEGIN block must survive a ROLLBACK of that block. PG only
			// closes cursors created inside the transaction; cursors
			// established earlier (under autocommit) persist. Multigres
			// must mirror this by forwarding only the in-txn cursor names
			// as release_portal_names on the ConcludeTransaction RPC.
			t.Run("pre-BEGIN WITH HOLD survives explicit ROLLBACK", func(t *testing.T) {
				tableName := fmt.Sprintf("hold_preb_test_%d", time.Now().UnixNano())

				_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (i int)", tableName))
				require.NoError(t, err)
				defer func() {
					_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+tableName)
				}()
				_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s SELECT generate_series(1,5)", tableName))
				require.NoError(t, err)

				preName := fmt.Sprintf("hold_pre_%d", time.Now().UnixNano())

				// DECLARE under autocommit (no explicit BEGIN).
				_, err = conn.Exec(ctx, fmt.Sprintf("DECLARE %s CURSOR WITH HOLD FOR SELECT i FROM %s ORDER BY i", preName, tableName))
				if err != nil {
					t.Skipf("backend rejected DECLARE WITH HOLD under autocommit: %v", err)
				}

				// Now open an explicit transaction and roll it back.
				tx, err := conn.Begin(ctx)
				require.NoError(t, err)
				_, err = tx.Exec(ctx, "SELECT 1")
				require.NoError(t, err)
				require.NoError(t, tx.Rollback(ctx))

				// Pre-BEGIN cursor must still be readable on the same
				// reserved backend. Drop the pgx stmtcache hop via Ping
				// before the FETCH to avoid pre-existing prepared-stmt
				// invalidation noise.
				require.NoError(t, conn.Ping(ctx))

				rows, err := conn.Query(ctx, "FETCH 1 FROM "+preName)
				if err != nil {
					t.Fatalf("pre-BEGIN HOLD cursor lost across explicit ROLLBACK: %v", err)
				}
				var got []int
				for rows.Next() {
					var v int
					require.NoError(t, rows.Scan(&v))
					got = append(got, v)
				}
				rows.Close()
				require.NoError(t, rows.Err())
				assert.Equal(t, []int{1}, got, "pre-BEGIN cursor should continue from row 1")

				_, err = conn.Exec(ctx, "CLOSE "+preName)
				require.NoError(t, err)
			})

			// MUL-389 follow-up: HOLD cursors split across the BEGIN
			// boundary — one declared pre-BEGIN, one declared inside the
			// txn. Explicit ROLLBACK closes only the inside-txn cursor.
			t.Run("mixed pre/inside-txn HOLD on explicit ROLLBACK", func(t *testing.T) {
				tableName := fmt.Sprintf("hold_mix_test_%d", time.Now().UnixNano())

				_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (i int)", tableName))
				require.NoError(t, err)
				defer func() {
					_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+tableName)
				}()
				_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s SELECT generate_series(1,3)", tableName))
				require.NoError(t, err)

				preName := fmt.Sprintf("hold_mix_pre_%d", time.Now().UnixNano())
				insideName := fmt.Sprintf("hold_mix_in_%d", time.Now().UnixNano())

				_, err = conn.Exec(ctx, fmt.Sprintf("DECLARE %s CURSOR WITH HOLD FOR SELECT i FROM %s ORDER BY i", preName, tableName))
				if err != nil {
					t.Skipf("backend rejected DECLARE WITH HOLD under autocommit: %v", err)
				}

				tx, err := conn.Begin(ctx)
				require.NoError(t, err)
				_, err = tx.Exec(ctx, fmt.Sprintf("DECLARE %s CURSOR WITH HOLD FOR SELECT i FROM %s ORDER BY i", insideName, tableName))
				require.NoError(t, err)
				require.NoError(t, tx.Rollback(ctx))

				require.NoError(t, conn.Ping(ctx))

				// Pre-BEGIN cursor survives.
				rows, err := conn.Query(ctx, "FETCH 1 FROM "+preName)
				require.NoError(t, err, "pre-BEGIN cursor must survive ROLLBACK of the outer txn")
				var got []int
				for rows.Next() {
					var v int
					require.NoError(t, rows.Scan(&v))
					got = append(got, v)
				}
				rows.Close()
				require.NoError(t, rows.Err())
				assert.Equal(t, []int{1}, got)

				// Inside-txn cursor is gone.
				_, err = conn.Exec(ctx, "FETCH FROM "+insideName)
				require.Error(t, err, "cursor declared inside the txn must be closed by ROLLBACK")

				_, err = conn.Exec(ctx, "CLOSE "+preName)
				require.NoError(t, err)
			})

			// MUL-389 follow-up: a HOLD cursor declared BEFORE an explicit
			// BEGIN must survive when that block fails and COMMIT triggers
			// PG's implicit ROLLBACK. Mirrors the explicit-ROLLBACK
			// pre-BEGIN test but exercises the implicit-rollback path in
			// executeCommit, which had a state-ordering bug where
			// RollbackTransaction wiped the savepoint stack before
			// RestoreOpenHoldCursorsToBeginSnapshot could read it.
			t.Run("pre-BEGIN WITH HOLD survives failed-txn COMMIT", func(t *testing.T) {
				tableName := fmt.Sprintf("hold_prefail_test_%d", time.Now().UnixNano())

				_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (i int)", tableName))
				require.NoError(t, err)
				defer func() {
					_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+tableName)
				}()
				_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s SELECT generate_series(1,5)", tableName))
				require.NoError(t, err)

				preName := fmt.Sprintf("hold_prefail_%d", time.Now().UnixNano())

				_, err = conn.Exec(ctx, fmt.Sprintf("DECLARE %s CURSOR WITH HOLD FOR SELECT i FROM %s ORDER BY i", preName, tableName))
				if err != nil {
					t.Skipf("backend rejected DECLARE WITH HOLD under autocommit: %v", err)
				}

				tx, err := conn.Begin(ctx)
				require.NoError(t, err)
				_, err = tx.Exec(ctx, "SELECT 1/0")
				require.Error(t, err, "division by zero must abort the transaction")
				// COMMIT on a failed txn is rewritten to ROLLBACK by PG.
				// pgx Commit() may surface a non-nil error for the
				// non-COMMIT tag; that's expected and not a failure.
				_ = tx.Commit(ctx)

				require.NoError(t, conn.Ping(ctx))

				rows, err := conn.Query(ctx, "FETCH 1 FROM "+preName)
				if err != nil {
					t.Fatalf("pre-BEGIN HOLD cursor lost across failed-txn COMMIT: %v", err)
				}
				var got []int
				for rows.Next() {
					var v int
					require.NoError(t, rows.Scan(&v))
					got = append(got, v)
				}
				rows.Close()
				require.NoError(t, rows.Err())
				assert.Equal(t, []int{1}, got, "pre-BEGIN cursor should continue from row 1")

				_, err = conn.Exec(ctx, "CLOSE "+preName)
				require.NoError(t, err)
			})

			// MUL-389 follow-up: COMMIT issued on a failed transaction is
			// rewritten by PostgreSQL into ROLLBACK and closes every cursor
			// (including WITH HOLD). Multigateway must mirror this: drop
			// the gateway's HOLD-cursor bookkeeping and release the
			// multipooler-side portal pin so the reserved backend is
			// returned to the pool.
			t.Run("WITH HOLD released on COMMIT of failed transaction", func(t *testing.T) {
				tableName := fmt.Sprintf("hold_fail_test_%d", time.Now().UnixNano())

				_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (i int)", tableName))
				require.NoError(t, err)
				defer func() {
					_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+tableName)
				}()
				_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s SELECT generate_series(1,3)", tableName))
				require.NoError(t, err)

				cursorName := "hold_cur_fail"

				tx, err := conn.Begin(ctx)
				require.NoError(t, err)

				_, err = tx.Exec(ctx, fmt.Sprintf("DECLARE %s CURSOR WITH HOLD FOR SELECT i FROM %s ORDER BY i", cursorName, tableName))
				require.NoError(t, err)

				// Poison the transaction with a guaranteed runtime error.
				_, err = tx.Exec(ctx, "SELECT 1/0")
				require.Error(t, err, "division by zero must abort the transaction")

				// COMMIT on a failed txn is rewritten to ROLLBACK by PG.
				// pgx's Commit() reports `pgx: ...` for any non-COMMIT
				// command tag, so we accept either nil (when the driver
				// already swallowed the rewrite into rollback semantics) or
				// a non-nil error that does NOT indicate a leaked
				// transaction. The assertion that matters: the next query
				// must run on a healthy pooled connection.
				_ = tx.Commit(ctx)

				// After implicit-ROLLBACK the HOLD cursor must be gone
				// (PG closes it) AND a follow-up query must succeed without
				// the "current transaction is aborted" wrapper. Use Ping
				// rather than a cached `SELECT 1` — pgx's stmtcache can
				// hold names PG dropped at ROLLBACK and would surface a
				// spurious 26000 error on the next prepared exec, masking
				// the actual liveness signal we care about here.
				_, err = conn.Exec(ctx, "FETCH FROM "+cursorName)
				require.Error(t, err, "WITH HOLD cursor must be closed by implicit ROLLBACK")
				require.NoError(t, conn.Ping(ctx),
					"session should be back to idle on a fresh pooled connection")
			})

			// MUL-389 follow-up: explicit-ROLLBACK variant of the failed-txn
			// path. Mirrors the test above but ends with `tx.Rollback(ctx)`
			// instead of the failed `tx.Commit(ctx)`, exercising
			// executeRollback (not the implicit-rollback branch of
			// executeCommit). Both code paths must release the reserved
			// backend so the session can immediately serve queries on a
			// fresh pooled connection.
			t.Run("WITH HOLD released on explicit ROLLBACK after failure", func(t *testing.T) {
				tableName := fmt.Sprintf("hold_failrb_test_%d", time.Now().UnixNano())

				_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (i int)", tableName))
				require.NoError(t, err)
				defer func() {
					_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+tableName)
				}()
				_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s SELECT generate_series(1,3)", tableName))
				require.NoError(t, err)

				cursorName := "hold_cur_failrb"

				tx, err := conn.Begin(ctx)
				require.NoError(t, err)

				_, err = tx.Exec(ctx, fmt.Sprintf("DECLARE %s CURSOR WITH HOLD FOR SELECT i FROM %s ORDER BY i", cursorName, tableName))
				require.NoError(t, err)

				_, err = tx.Exec(ctx, "SELECT 1/0")
				require.Error(t, err, "division by zero must abort the transaction")

				require.NoError(t, tx.Rollback(ctx), "explicit ROLLBACK after a failed statement must succeed")

				_, err = conn.Exec(ctx, "FETCH FROM "+cursorName)
				require.Error(t, err, "WITH HOLD cursor must be closed by explicit ROLLBACK")
				require.NoError(t, conn.Ping(ctx),
					"session should be back to idle on a fresh pooled connection")
			})

			t.Run("transaction with prepared statements", func(t *testing.T) {
				t.Skip("Transactions require transaction management which is not yet implemented")

				tableName := fmt.Sprintf("txn_ps_test_%d", time.Now().UnixNano())

				// Create table
				_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, balance NUMERIC)", tableName))
				require.NoError(t, err)
				defer func() {
					_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+tableName)
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
		})
	}
}

// TestMultigateway_DatabaseSQLTransactions tests explicit transactions using Go's
// standard database/sql package (db.BeginTx). This reproduces the bug
// where Go's database/sql driver sends BEGIN/COMMIT via the extended query protocol
// (Parse/Bind/Execute messages), unlike the simple query protocol used by our
// custom client.Conn in transaction_test.go.
// Each subtest runs against both direct PostgreSQL and multigateway to ensure
// the proxy behavior matches native PostgreSQL exactly.
func TestMultigateway_DatabaseSQLTransactions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping DatabaseSQLTransactions test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping cluster lifecycle tests")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 30*time.Second)

	for _, target := range setup.GetComparisonTargets(t) {
		t.Run(target.Name, func(t *testing.T) {
			connStr := shardsetup.GetTestUserDSN("localhost", target.Port, "sslmode=disable", "connect_timeout=5")
			db, err := sql.Open("postgres", connStr)
			require.NoError(t, err, "failed to open database connection")
			defer db.Close()

			// Force a single connection so all operations use the same backend connection.
			db.SetMaxOpenConns(1)

			err = db.PingContext(ctx)
			require.NoError(t, err, "failed to ping database")

			t.Run("BeginTx commit", func(t *testing.T) {
				tableName := fmt.Sprintf("dbtx_commit_%d", time.Now().UnixNano())

				_, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, name TEXT)", tableName))
				require.NoError(t, err, "failed to create table")
				defer func() {
					_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS "+tableName)
				}()

				// database/sql sends BEGIN via extended query protocol (Parse/Bind/Execute).
				tx, err := db.BeginTx(ctx, nil)
				require.NoError(t, err, "BeginTx failed")

				_, err = tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, name) VALUES ($1, $2)", tableName), 1, "Alice")
				require.NoError(t, err, "INSERT in transaction failed")

				_, err = tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, name) VALUES ($1, $2)", tableName), 2, "Bob")
				require.NoError(t, err, "second INSERT in transaction failed")

				err = tx.Commit()
				require.NoError(t, err, "Commit failed")

				// Verify data was committed
				var count int
				err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+tableName).Scan(&count)
				require.NoError(t, err, "failed to verify committed data")
				assert.Equal(t, 2, count, "expected 2 rows after commit")
			})

			t.Run("BeginTx rollback", func(t *testing.T) {
				tableName := fmt.Sprintf("dbtx_rollback_%d", time.Now().UnixNano())

				_, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, name TEXT)", tableName))
				require.NoError(t, err, "failed to create table")
				defer func() {
					_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS "+tableName)
				}()

				// Insert initial data outside transaction
				_, err = db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, name) VALUES ($1, $2)", tableName), 1, "Alice")
				require.NoError(t, err, "failed to insert initial data")

				// Start transaction, insert, then rollback
				tx, err := db.BeginTx(ctx, nil)
				require.NoError(t, err, "BeginTx failed")

				_, err = tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, name) VALUES ($1, $2)", tableName), 2, "Bob")
				require.NoError(t, err, "INSERT in transaction failed")

				err = tx.Rollback()
				require.NoError(t, err, "Rollback failed")

				// Verify only initial data exists (Bob was rolled back)
				var count int
				err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+tableName).Scan(&count)
				require.NoError(t, err, "failed to verify data after rollback")
				assert.Equal(t, 1, count, "expected only 1 row after rollback (Alice)")
			})
		})
	}
}

// TestMultigateway_PartialRowsBeforeError verifies that rows produced before a
// terminal execution error remain visible ahead of ErrorResponse. PostgreSQL
// can stream DataRows and then hit a row-level error; those rows must not be
// discarded, and no CommandComplete may be sent for the failed statement.
//
// The query streams (no sort/aggregate to block output) and divides by zero on
// the third row, so PostgreSQL emits two DataRows then ErrorResponse. The
// multigateway must deliver the same partial rows.
//
// The simple and extended query protocols are separate client read loops that
// each drop partial rows independently, and extended is the one most drivers use
// (pgx's default mode, JDBC), so both get a subtest.
func TestMultigateway_PartialRowsBeforeError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping partial-rows test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping partial-rows test")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)
	ctx := utils.WithTimeout(t, 30*time.Second)

	// g runs 1..5; 10/(g-3) errors at g=3 after emitting rows for g=1,2.
	const query = "SELECT 10/(g-3) AS v FROM generate_series(1,5) AS g"

	// countRowsBeforeError drives the query over one protocol and returns the
	// number of DataRows observed before the terminal error, plus that error.
	protocols := []struct {
		name  string
		count func(conn *pgconn.PgConn) (int, error)
	}{
		{
			// Low-level simple-query protocol so we observe DataRows as they
			// stream, independent of the terminal error.
			name: "simple",
			count: func(conn *pgconn.PgConn) (int, error) {
				mrr := conn.Exec(ctx, query)
				rows := 0
				for mrr.NextResult() {
					rr := mrr.ResultReader()
					for rr.NextRow() {
						rows++
					}
					_, _ = rr.Close()
				}
				return rows, mrr.Close()
			},
		},
		{
			// ExecParams uses the extended protocol (Parse/Bind/Describe/
			// Execute/Sync) even with no bind params.
			name: "extended",
			count: func(conn *pgconn.PgConn) (int, error) {
				rr := conn.ExecParams(ctx, query, nil, nil, nil, nil)
				rows := 0
				for rr.NextRow() {
					rows++
				}
				_, execErr := rr.Close()
				return rows, execErr
			},
		},
	}

	type result struct {
		rowsBeforeError int
		code            string
	}

	for _, proto := range protocols {
		t.Run(proto.name, func(t *testing.T) {
			results := map[string]result{}
			for _, target := range setup.GetComparisonTargets(t) {
				t.Run(target.Name, func(t *testing.T) {
					connStr := shardsetup.GetTestUserDSN("localhost", target.Port, "sslmode=disable")
					conn, err := pgconn.Connect(ctx, connStr)
					require.NoError(t, err)
					defer conn.Close(context.Background())

					rows, execErr := proto.count(conn)
					require.Error(t, execErr, "query must terminate with a division-by-zero error")

					var pgErr *pgconn.PgError
					require.True(t, errors.As(execErr, &pgErr), "expected pgconn.PgError, got %T", execErr)
					assert.Equal(t, "22012", pgErr.Code, "expected division_by_zero SQLSTATE")

					t.Logf("%s: rowsBeforeError=%d code=%s msg=%q", target.Name, rows, pgErr.Code, pgErr.Message)
					results[target.Name] = result{rowsBeforeError: rows, code: pgErr.Code}
				})
			}

			pg, gw := results["postgres"], results["multigateway"]
			require.Equal(t, 2, pg.rowsBeforeError, "sanity: PostgreSQL must stream 2 rows before the error")
			assert.Equal(t, pg.rowsBeforeError, gw.rowsBeforeError,
				"multigateway must deliver the partial rows PostgreSQL emits before ErrorResponse")
			assert.Equal(t, pg.code, gw.code, "the terminal error SQLSTATE must match PostgreSQL")
		})
	}
}
