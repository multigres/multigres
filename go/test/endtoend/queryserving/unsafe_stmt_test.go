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
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestMultiGateway_UnsafeStatementRejection verifies that the multigateway
// rejects Tier 2 SQL statements (server-level operations unsafe for a hosted
// connection pooler). These statements are blocked at plan time with SQLSTATE
// 0A000 (feature_not_supported).
//
// This test is multigateway-only: PostgreSQL itself allows these statements
// (with appropriate privileges), but the pooler must reject them.
func TestMultiGateway_UnsafeStatementRejection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping unsafe statement tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping unsafe statement tests")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 60*time.Second)

	connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable")
	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx)

	// Tier 2: server-level operations that should not be available through a
	// shared connection pooler in a hosted environment.
	t.Run("tier2 infrastructure safety", func(t *testing.T) {
		tests := []struct {
			name    string
			sql     string
			wantMsg string
		}{
			{
				name:    "LOAD",
				sql:     "LOAD 'auto_explain'",
				wantMsg: "LOAD is not supported",
			},
			{
				name:    "ALTER SYSTEM",
				sql:     "ALTER SYSTEM SET log_min_messages = 'warning'",
				wantMsg: "ALTER SYSTEM is not supported",
			},
			{
				name:    "CREATE DATABASE",
				sql:     "CREATE DATABASE _test_unsafe_db",
				wantMsg: "CREATE DATABASE is not supported",
			},
			{
				name:    "DROP DATABASE",
				sql:     "DROP DATABASE IF EXISTS _test_unsafe_db",
				wantMsg: "DROP DATABASE is not supported",
			},
			{
				name:    "CREATE LANGUAGE",
				sql:     "CREATE LANGUAGE _test_unsafe_lang HANDLER plpgsql_call_handler",
				wantMsg: "CREATE LANGUAGE is not supported",
			},
			{
				name:    "CREATE SUBSCRIPTION",
				sql:     "CREATE SUBSCRIPTION _test_unsafe_sub CONNECTION 'host=localhost' PUBLICATION mypub",
				wantMsg: "CREATE SUBSCRIPTION is not supported",
			},
			{
				name:    "CREATE FOREIGN DATA WRAPPER",
				sql:     "CREATE FOREIGN DATA WRAPPER _test_unsafe_fdw",
				wantMsg: "CREATE FOREIGN DATA WRAPPER is not supported",
			},
			{
				name:    "CREATE FOREIGN SERVER",
				sql:     "CREATE SERVER _test_unsafe_srv FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host 'localhost')",
				wantMsg: "CREATE SERVER is not supported",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := conn.Exec(ctx, tt.sql)
				require.Error(t, err, "%s should be rejected", tt.name)

				var pgErr *pgconn.PgError
				require.True(t, errors.As(err, &pgErr), "expected pgconn.PgError, got %T", err)
				assert.Equal(t, "0A000", pgErr.Code, "SQLSTATE should be feature_not_supported")
				assert.Contains(t, pgErr.Message, tt.wantMsg)
				t.Logf("rejected %s: %s", tt.name, pgErr.Message)
			})
		}
	})

	// Verify the connection is still healthy after all rejections.
	// This confirms that rejected statements don't corrupt connection state.
	t.Run("connection healthy after rejections", func(t *testing.T) {
		var result int
		err := conn.QueryRow(ctx, "SELECT 1").Scan(&result)
		require.NoError(t, err, "connection should still be usable after rejected statements")
		assert.Equal(t, 1, result)
	})

	// Verify that allowed statements still work through the pooler.
	t.Run("allowed statements pass through", func(t *testing.T) {
		var result int
		err := conn.QueryRow(ctx, "SELECT abs(-42)").Scan(&result)
		require.NoError(t, err, "function calls in expressions should be allowed")
		assert.Equal(t, 42, result)
	})
}

// TestMultiGateway_UnsafeStatementRejection_ExtendedProtocol verifies that
// unsafe statements are also rejected through the extended query protocol
// (Parse/Bind/Execute), not just the simple query protocol.
func TestMultiGateway_UnsafeStatementRejection_ExtendedProtocol(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping unsafe statement extended protocol tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping unsafe statement tests")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 60*time.Second)

	connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable")
	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx)

	// pgx uses the extended query protocol by default for parameterized
	// queries. Using pgx.QueryExecModeDescribeExec forces Parse/Describe
	// before Execute, exercising the PlanPortal path.
	tests := []struct {
		name    string
		sql     string
		wantMsg string
	}{
		{
			name:    "LOAD via extended protocol",
			sql:     "LOAD 'auto_explain'",
			wantMsg: "LOAD is not supported",
		},
		{
			name:    "ALTER SYSTEM via extended protocol",
			sql:     "ALTER SYSTEM SET log_min_messages = 'warning'",
			wantMsg: "ALTER SYSTEM is not supported",
		},
		{
			name:    "CREATE DATABASE via extended protocol",
			sql:     "CREATE DATABASE _test_unsafe_db_ext",
			wantMsg: "CREATE DATABASE is not supported",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Force extended protocol via Exec with QueryExecModeDescribeExec.
			_, err := conn.Exec(ctx, tt.sql, pgx.QueryExecModeDescribeExec)
			require.Error(t, err, "%s should be rejected via extended protocol", tt.name)

			var pgErr *pgconn.PgError
			require.True(t, errors.As(err, &pgErr), "expected pgconn.PgError, got %T", err)
			assert.Equal(t, "0A000", pgErr.Code, "SQLSTATE should be feature_not_supported")
			assert.Contains(t, pgErr.Message, tt.wantMsg)
		})
	}

	// Verify connection health after extended protocol rejections.
	var result int
	err = conn.QueryRow(ctx, "SELECT 1").Scan(&result)
	require.NoError(t, err, "connection should still be usable")
	assert.Equal(t, 1, result)
}

// TestMultiGateway_UnsafeStatementRejection_InTransaction verifies that
// unsafe statements are rejected even inside a transaction, and that the
// transaction can be rolled back cleanly.
func TestMultiGateway_UnsafeStatementRejection_InTransaction(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping unsafe statement transaction tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping unsafe statement tests")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 60*time.Second)

	connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable")
	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx)

	// Start a transaction.
	_, err = conn.Exec(ctx, "BEGIN")
	require.NoError(t, err)

	// Attempt an unsafe statement inside the transaction.
	_, err = conn.Exec(ctx, "LOAD 'auto_explain'")
	require.Error(t, err, "LOAD should be rejected inside transaction")

	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr))
	assert.Equal(t, "0A000", pgErr.Code)

	// The transaction should be in a failed state. ROLLBACK should succeed.
	_, err = conn.Exec(ctx, "ROLLBACK")
	require.NoError(t, err, "ROLLBACK after rejected statement should succeed")

	// Connection should be healthy after rollback.
	var result int
	err = conn.QueryRow(ctx, "SELECT 1").Scan(&result)
	require.NoError(t, err, "connection should be healthy after ROLLBACK")
	assert.Equal(t, 1, result)
}
