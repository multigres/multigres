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
)

// TestMultiGateway_PostgreSQLConnection tests that we can connect to multigateway via PostgreSQL protocol
// and execute queries. This is a true end-to-end test that uses the full cluster setup.
func TestMultiGateway_PostgreSQLConnection(t *testing.T) {
	ensureBinaryBuilt(t)

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

	// Execute a simple query
	rows, err := db.QueryContext(ctx, "SELECT * FROM test")
	require.NoError(t, err, "failed to execute query")
	defer rows.Close()

	// Verify we got columns
	columns, err := rows.Columns()
	require.NoError(t, err, "failed to get columns")
	assert.Equal(t, []string{"message"}, columns, "unexpected columns")

	// Verify we got the expected row
	require.True(t, rows.Next(), "expected at least one row")

	var message string
	err = rows.Scan(&message)
	require.NoError(t, err, "failed to scan row")
	// Phase 2: ScatterConn returns information about the execution
	assert.Contains(t, message, "ScatterConn:", "unexpected message - should contain ScatterConn info")
	assert.Contains(t, message, "tablegroup=default", "unexpected message - should show default tablegroup")
	assert.Contains(t, message, "database=postgres", "unexpected message - should show database")
	assert.Contains(t, message, "SELECT * FROM test", "unexpected message - should show original query")

	// Verify no more rows
	assert.False(t, rows.Next(), "expected only one row")
	assert.NoError(t, rows.Err(), "rows iteration error")
}
