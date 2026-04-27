// Copyright 2026 Supabase, Inc.
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
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// getReplicaSetup returns the shared setup that has the replica port enabled.
// Skips the test in short mode since integration tests require PostgreSQL.
func getReplicaSetup(t *testing.T) *shardsetup.ShardSetup {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	return replicaSetupManager.Get(t)
}

// TestReplicaReads_SelectWorks verifies that SELECT queries succeed on the replica port.
func TestReplicaReads_SelectWorks(t *testing.T) {
	setup := getReplicaSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 15*time.Second)
	connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayReplicaPgPort, "sslmode=disable")
	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx)

	var result int
	err = conn.QueryRow(ctx, "SELECT 1").Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, 1, result)
}

// TestReplicaReads_WritesRejectedByPostgres verifies that write operations fail
// with a read-only error when sent to the replica port. The error comes from
// PostgreSQL on the standby, not from multigateway.
func TestReplicaReads_WritesRejectedByPostgres(t *testing.T) {
	setup := getReplicaSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 15*time.Second)

	primaryDSN := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable")
	replicaDSN := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayReplicaPgPort, "sslmode=disable")

	// Create a table on the primary so it replicates to the standby.
	primaryConn, err := pgx.Connect(ctx, primaryDSN)
	require.NoError(t, err)
	_, err = primaryConn.Exec(ctx, "CREATE TABLE IF NOT EXISTS replica_writes_test (id int)")
	require.NoError(t, err)
	primaryConn.Close(ctx)

	// Cleanup on primary after test completes.
	t.Cleanup(func() {
		cleanupCtx := utils.WithTimeout(t, 5*time.Second)
		conn, err := pgx.Connect(cleanupCtx, primaryDSN)
		if err != nil {
			return
		}
		defer conn.Close(cleanupCtx)
		_, _ = conn.Exec(cleanupCtx, "DROP TABLE IF EXISTS replica_writes_test")
	})

	// Connect to replica port and wait for the table to replicate.
	replicaConn, err := pgx.Connect(ctx, replicaDSN)
	require.NoError(t, err)
	defer replicaConn.Close(ctx)

	require.Eventually(t, func() bool {
		var exists bool
		err := replicaConn.QueryRow(ctx,
			"SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'replica_writes_test')").Scan(&exists)
		return err == nil && exists
	}, 10*time.Second, 100*time.Millisecond, "table replica_writes_test did not replicate to standby")

	writes := []struct {
		name string
		sql  string
	}{
		{"CREATE TABLE", "CREATE TABLE should_fail_on_replica (id int)"},
		{"INSERT", "INSERT INTO replica_writes_test VALUES (1)"},
		{"UPDATE", "UPDATE replica_writes_test SET id = 2"},
		{"DELETE", "DELETE FROM replica_writes_test"},
	}

	for _, tc := range writes {
		t.Run(tc.name, func(t *testing.T) {
			_, err := replicaConn.Exec(ctx, tc.sql)
			require.Error(t, err, "%s should fail on replica", tc.name)
			assert.Contains(t, err.Error(), "read-only",
				"%s error should indicate read-only: %v", tc.name, err)
		})
	}
}

// TestReplicaReads_ReadOnlyTransactionWorks verifies that read-only transactions
// work on the replica port, providing consistent snapshot reads.
func TestReplicaReads_ReadOnlyTransactionWorks(t *testing.T) {
	setup := getReplicaSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 15*time.Second)
	connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayReplicaPgPort, "sslmode=disable")
	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx)

	tx, err := conn.Begin(ctx)
	require.NoError(t, err)

	var result int
	err = tx.QueryRow(ctx, "SELECT 1").Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, 1, result)

	err = tx.Commit(ctx)
	require.NoError(t, err)
}

// TestReplicaReads_SessionSettingsWork verifies that SET and SHOW commands work
// on the replica port (session-local operations).
func TestReplicaReads_SessionSettingsWork(t *testing.T) {
	setup := getReplicaSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 15*time.Second)
	connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayReplicaPgPort, "sslmode=disable")
	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx)

	// SET should work
	_, err = conn.Exec(ctx, "SET statement_timeout = '5s'")
	require.NoError(t, err)

	// SHOW should work
	var timeout string
	err = conn.QueryRow(ctx, "SHOW statement_timeout").Scan(&timeout)
	require.NoError(t, err)
	assert.Equal(t, "5s", timeout)
}

// TestReplicaReads_PrimaryPortUnaffected verifies that the primary port still works
// normally for both reads and writes when the replica port is also active.
func TestReplicaReads_PrimaryPortUnaffected(t *testing.T) {
	setup := getReplicaSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 15*time.Second)

	// Connect to the primary port
	connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable")
	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx)

	// SELECT works
	var result int
	err = conn.QueryRow(ctx, "SELECT 1").Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, 1, result)

	// DML works on primary port
	_, err = conn.Exec(ctx, "CREATE TEMP TABLE primary_port_test (id int)")
	require.NoError(t, err)

	_, err = conn.Exec(ctx, "INSERT INTO primary_port_test VALUES (1)")
	require.NoError(t, err)
}
