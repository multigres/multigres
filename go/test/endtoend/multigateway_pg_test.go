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

	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/utils"
)

// TestMultiGateway_PostgreSQLConnection tests that we can connect to multigateway via PostgreSQL protocol
// and execute queries. This is a true end-to-end test that uses the full cluster setup.
func TestMultiGateway_PostgreSQLConnection(t *testing.T) {
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping cluster lifecycle tests")
	}

	// Setup full test cluster with all services
	cluster := setupTestCluster(t)
	defer cluster.Cleanup()

	// Connect to multigateway's PostgreSQL port using PostgreSQL driver
	connStr := fmt.Sprintf("host=localhost port=%d user=postgres dbname=postgres sslmode=disable connect_timeout=5",
		cluster.PortConfig.MultigatewayPGPort)
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
