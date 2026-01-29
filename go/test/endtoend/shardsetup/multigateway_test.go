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

package shardsetup

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/utils"
)

// TestMultigatewaySetup verifies that multigateway can be started and can execute queries.
// This test validates the shardsetup multigateway infrastructure before migrating full e2e tests.
func TestMultigatewaySetup(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multigateway setup test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping")
	}

	// Use shared setup with multigateway
	setup := getSharedSetup(t)

	// Verify multigateway instance exists
	require.NotNil(t, setup.Multigateway, "multigateway instance should be created")
	require.True(t, setup.Multigateway.IsRunning(), "multigateway should be running")
	require.Greater(t, setup.MultigatewayPgPort, 0, "multigateway PG port should be allocated")

	// Connect to multigateway
	connStr := fmt.Sprintf("host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable connect_timeout=5",
		setup.MultigatewayPgPort, TestPostgresPassword)
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err, "failed to open database connection")
	defer db.Close()

	ctx := utils.WithTimeout(t, 10*time.Second)

	// Test 1: Basic connectivity
	err = db.PingContext(ctx)
	require.NoError(t, err, "failed to ping multigateway")

	// Test 2: Simple query execution
	var result int
	err = db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
	require.NoError(t, err, "failed to execute SELECT 1")
	assert.Equal(t, 1, result, "unexpected query result")

	// Test 3: Query routed through multigateway to primary
	var num int
	var greeting string
	err = db.QueryRowContext(ctx, "SELECT 42 as num, 'hello' as greeting").Scan(&num, &greeting)
	require.NoError(t, err, "failed to execute parameterized query")
	assert.Equal(t, 42, num)
	assert.Equal(t, "hello", greeting)

	// Test 4: Verify multigateway can route DDL/DML
	tableName := fmt.Sprintf("mg_test_%d", time.Now().UnixNano())
	_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, value TEXT)", tableName))
	require.NoError(t, err, "failed to create table via multigateway")

	_, err = db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, value) VALUES (1, 'test')", tableName))
	require.NoError(t, err, "failed to insert via multigateway")

	var value string
	err = db.QueryRowContext(ctx, fmt.Sprintf("SELECT value FROM %s WHERE id = 1", tableName)).Scan(&value)
	require.NoError(t, err, "failed to select via multigateway")
	assert.Equal(t, "test", value)

	_, err = db.ExecContext(ctx, "DROP TABLE "+tableName)
	require.NoError(t, err, "failed to drop table via multigateway")

	t.Log("Multigateway setup test passed - infrastructure is ready")
}
