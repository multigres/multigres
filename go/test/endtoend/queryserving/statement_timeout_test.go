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
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestMultiGateway_StatementTimeout tests that statement_timeout is managed
// entirely at the gateway layer: SET/SHOW/RESET work without a PostgreSQL
// round-trip, the effective timeout is enforced via context deadlines, and
// the priority chain (directive > session > flag) is respected.
func TestMultiGateway_StatementTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping statement timeout test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping statement timeout tests")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 150*time.Second)

	t.Run("SET and SHOW", func(t *testing.T) {
		conn := connectPgx(t, ctx, setup)
		defer conn.Close(ctx)

		// SET with millisecond integer (PostgreSQL's default unit)
		_, err := conn.Exec(ctx, "SET statement_timeout = 5000")
		require.NoError(t, err)

		var result string
		err = conn.QueryRow(ctx, "SHOW statement_timeout").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "5s", result, "should display using PG GUC_UNIT_MS format")

		// SET with Go duration string
		_, err = conn.Exec(ctx, "SET statement_timeout = '2s'")
		require.NoError(t, err)

		err = conn.QueryRow(ctx, "SHOW statement_timeout").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "2s", result, "2s should display as 2s")

		// SET to 0 disables timeout
		_, err = conn.Exec(ctx, "SET statement_timeout = 0")
		require.NoError(t, err)

		err = conn.QueryRow(ctx, "SHOW statement_timeout").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "0", result, "0 should disable timeout")
	})

	t.Run("RESET reverts to flag default", func(t *testing.T) {
		conn := connectPgx(t, ctx, setup)
		defer conn.Close(ctx)

		// Override to a non-default value
		_, err := conn.Exec(ctx, "SET statement_timeout = 1000")
		require.NoError(t, err)

		var result string
		err = conn.QueryRow(ctx, "SHOW statement_timeout").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "1s", result)

		// RESET should revert to the --statement-timeout flag default (30s)
		_, err = conn.Exec(ctx, "RESET statement_timeout")
		require.NoError(t, err)

		err = conn.QueryRow(ctx, "SHOW statement_timeout").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "30s", result, "RESET should revert to flag default (30s)")
	})

	t.Run("SET TO DEFAULT reverts to flag default", func(t *testing.T) {
		conn := connectPgx(t, ctx, setup)
		defer conn.Close(ctx)

		_, err := conn.Exec(ctx, "SET statement_timeout = 1000")
		require.NoError(t, err)

		var result string
		err = conn.QueryRow(ctx, "SHOW statement_timeout").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "1s", result)

		// SET TO DEFAULT should behave like RESET
		_, err = conn.Exec(ctx, "SET statement_timeout TO DEFAULT")
		require.NoError(t, err)

		err = conn.QueryRow(ctx, "SHOW statement_timeout").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "30s", result, "SET TO DEFAULT should revert to flag default (30s)")
	})

	t.Run("RESET ALL also resets statement_timeout", func(t *testing.T) {
		conn := connectPgx(t, ctx, setup)
		defer conn.Close(ctx)

		_, err := conn.Exec(ctx, "SET statement_timeout = 1000")
		require.NoError(t, err)

		_, err = conn.Exec(ctx, "RESET ALL")
		require.NoError(t, err)

		var result string
		err = conn.QueryRow(ctx, "SHOW statement_timeout").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "30s", result, "RESET ALL should revert statement_timeout to flag default")
	})

	t.Run("timeout enforced on slow query", func(t *testing.T) {
		conn := connectClient(t, ctx, setup)
		defer conn.Close()

		// Set a short timeout
		_, err := conn.Query(ctx, "SET statement_timeout = '500ms'")
		require.NoError(t, err)

		// pg_sleep(5) should be cancelled after 500ms
		start := time.Now()
		_, err = conn.Query(ctx, "SELECT pg_sleep(5)")
		elapsed := time.Since(start)

		require.Error(t, err, "query should be cancelled by statement timeout")

		var diag *mterrors.PgDiagnostic
		require.True(t, errors.As(err, &diag), "expected PgDiagnostic error, got %T: %v", err, err)
		assert.Equal(t, "57014", diag.Code, "should be query_canceled (57014)")

		// Should have been cancelled well before 5 seconds
		assert.Less(t, elapsed, 3*time.Second, "timeout should fire before the 5s sleep completes")

		// Reset so subsequent queries aren't affected
		_, err = conn.Query(ctx, "SET statement_timeout = '0'")
		require.NoError(t, err)
	})

	t.Run("timeout=0 disables enforcement", func(t *testing.T) {
		conn := connectClient(t, ctx, setup)
		defer conn.Close()

		// Explicitly disable timeout
		_, err := conn.Query(ctx, "SET statement_timeout = '0'")
		require.NoError(t, err)

		// A short sleep should complete without being cancelled
		_, err = conn.Query(ctx, "SELECT pg_sleep(0.5)")
		require.NoError(t, err, "query should complete when timeout is disabled")
	})

	t.Run("connection usable after timeout", func(t *testing.T) {
		conn := connectClient(t, ctx, setup)
		defer conn.Close()

		_, err := conn.Query(ctx, "SET statement_timeout = '200ms'")
		require.NoError(t, err)

		// Trigger a timeout
		_, err = conn.Query(ctx, "SELECT pg_sleep(5)")
		require.Error(t, err, "should timeout")

		// Disable timeout and verify the connection is still usable
		_, err = conn.Query(ctx, "SET statement_timeout = '0'")
		require.NoError(t, err)

		results, err := conn.Query(ctx, "SELECT 1")
		require.NoError(t, err)
		require.Len(t, results, 1)
		assert.Equal(t, "1", string(results[0].Rows[0].Values[0]))
	})

	t.Run("session override persists across queries", func(t *testing.T) {
		conn := connectClient(t, ctx, setup)
		defer conn.Close()

		_, err := conn.Query(ctx, "SET statement_timeout = '300ms'")
		require.NoError(t, err)

		// Both should be cancelled
		_, err = conn.Query(ctx, "SELECT pg_sleep(5)")
		require.Error(t, err, "first query should timeout")

		_, err = conn.Query(ctx, "SELECT pg_sleep(5)")
		require.Error(t, err, "second query should also timeout (session persists)")

		// Reset for cleanup
		_, err = conn.Query(ctx, "SET statement_timeout = '0'")
		require.NoError(t, err)
	})

	t.Run("cross-connection isolation", func(t *testing.T) {
		conn1 := connectClient(t, ctx, setup)
		defer conn1.Close()
		conn2 := connectClient(t, ctx, setup)
		defer conn2.Close()

		// Set different timeouts on each connection
		_, err := conn1.Query(ctx, "SET statement_timeout = '200ms'")
		require.NoError(t, err)

		_, err = conn2.Query(ctx, "SET statement_timeout = '0'")
		require.NoError(t, err)

		// conn1 should timeout
		_, err = conn1.Query(ctx, "SELECT pg_sleep(5)")
		require.Error(t, err, "conn1 should timeout with 200ms limit")

		// conn2 should complete (timeout disabled)
		_, err = conn2.Query(ctx, "SELECT pg_sleep(0.5)")
		require.NoError(t, err, "conn2 should complete with timeout disabled")

		// Cleanup
		_, err = conn1.Query(ctx, "SET statement_timeout = '0'")
		require.NoError(t, err)
	})

	t.Run("invalid SET value returns error", func(t *testing.T) {
		conn := connectPgx(t, ctx, setup)
		defer conn.Close(ctx)

		_, err := conn.Exec(ctx, "SET statement_timeout = 'not-a-number'")
		require.Error(t, err, "invalid value should return error")

		var pgErr *pgconn.PgError
		require.True(t, errors.As(err, &pgErr), "expected PgError, got %T: %v", err, err)
		assert.Equal(t, "22023", pgErr.Code, "should be invalid_parameter_value")
	})

	t.Run("negative SET value returns error", func(t *testing.T) {
		conn := connectPgx(t, ctx, setup)
		defer conn.Close(ctx)

		_, err := conn.Exec(ctx, "SET statement_timeout = '-1'")
		require.Error(t, err, "negative value should return error")

		var pgErr *pgconn.PgError
		require.True(t, errors.As(err, &pgErr), "expected PgError, got %T: %v", err, err)
		assert.Equal(t, "22023", pgErr.Code, "should be invalid_parameter_value")
	})

	t.Run("extended query protocol timeout", func(t *testing.T) {
		conn := connectPgx(t, ctx, setup)
		defer conn.Close(ctx)

		// SET via extended protocol
		_, err := conn.Exec(ctx, "SET statement_timeout = '500ms'")
		require.NoError(t, err)

		// Verify SHOW via extended protocol
		var result string
		err = conn.QueryRow(ctx, "SHOW statement_timeout").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "500ms", result)

		// Timeout should be enforced via extended protocol too
		start := time.Now()
		_, err = conn.Exec(ctx, "SELECT pg_sleep(5)")
		elapsed := time.Since(start)
		require.Error(t, err, "extended protocol query should be cancelled")
		assert.Less(t, elapsed, 3*time.Second)

		// Reset
		_, err = conn.Exec(ctx, "SET statement_timeout = '0'")
		require.NoError(t, err)
	})

	t.Run("timeout in transaction aborts transaction", func(t *testing.T) {
		conn := connectClient(t, ctx, setup)
		defer conn.Close()

		_, err := conn.Query(ctx, "DROP TABLE IF EXISTS st_txn_test")
		require.NoError(t, err)
		_, err = conn.Query(ctx, "CREATE TABLE st_txn_test (id INT PRIMARY KEY, name TEXT)")
		require.NoError(t, err)

		t.Cleanup(func() {
			cleanupConn := connectClient(t, context.Background(), setup)
			defer cleanupConn.Close()
			_, _ = cleanupConn.Query(context.Background(), "DROP TABLE IF EXISTS st_txn_test")
		})

		_, err = conn.Query(ctx, "BEGIN")
		require.NoError(t, err)
		_, err = conn.Query(ctx, "INSERT INTO st_txn_test VALUES (1, 'Alice')")
		require.NoError(t, err)

		// Set short timeout and trigger it inside the transaction
		_, err = conn.Query(ctx, "SET statement_timeout = '200ms'")
		require.NoError(t, err)
		_, err = conn.Query(ctx, "SELECT pg_sleep(5)")
		require.Error(t, err, "should timeout inside transaction")

		// Transaction should be aborted — further queries should fail
		_, err = conn.Query(ctx, "INSERT INTO st_txn_test VALUES (2, 'Bob')")
		require.Error(t, err, "should fail in aborted transaction")

		// ROLLBACK should recover
		_, err = conn.Query(ctx, "ROLLBACK")
		require.NoError(t, err)

		// Reset timeout
		_, err = conn.Query(ctx, "SET statement_timeout = '0'")
		require.NoError(t, err)

		// Alice should not have been committed
		results, err := conn.Query(ctx, "SELECT COUNT(*) FROM st_txn_test")
		require.NoError(t, err)
		assert.Equal(t, "0", string(results[0].Rows[0].Values[0]), "nothing should be committed after rollback")
	})
}

// TestMultiGateway_StatementTimeoutStartupParam tests that statement_timeout
// can be set via the connection startup parameters and is intercepted by
// the gateway (not forwarded to PostgreSQL).
func TestMultiGateway_StatementTimeoutStartupParam(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping statement timeout startup param test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping statement timeout startup param tests")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 150*time.Second)

	t.Run("startup param sets default", func(t *testing.T) {
		connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable")
		connCfg, err := pgx.ParseConfig(connStr)
		require.NoError(t, err)
		connCfg.RuntimeParams["statement_timeout"] = "2000"

		conn, err := pgx.ConnectConfig(ctx, connCfg)
		require.NoError(t, err)
		defer conn.Close(ctx)

		var result string
		err = conn.QueryRow(ctx, "SHOW statement_timeout").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "2s", result, "startup param should set the default to 2s")
	})

	t.Run("SET overrides startup param", func(t *testing.T) {
		connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable")
		connCfg, err := pgx.ParseConfig(connStr)
		require.NoError(t, err)
		connCfg.RuntimeParams["statement_timeout"] = "2000"

		conn, err := pgx.ConnectConfig(ctx, connCfg)
		require.NoError(t, err)
		defer conn.Close(ctx)

		_, err = conn.Exec(ctx, "SET statement_timeout = 5000")
		require.NoError(t, err)

		var result string
		err = conn.QueryRow(ctx, "SHOW statement_timeout").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "5s", result, "SET should override startup param")
	})

	t.Run("RESET reverts to startup param default", func(t *testing.T) {
		connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable")
		connCfg, err := pgx.ParseConfig(connStr)
		require.NoError(t, err)
		connCfg.RuntimeParams["statement_timeout"] = "7000"

		conn, err := pgx.ConnectConfig(ctx, connCfg)
		require.NoError(t, err)
		defer conn.Close(ctx)

		_, err = conn.Exec(ctx, "SET statement_timeout = 1000")
		require.NoError(t, err)

		_, err = conn.Exec(ctx, "RESET statement_timeout")
		require.NoError(t, err)

		var result string
		err = conn.QueryRow(ctx, "SHOW statement_timeout").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "7s", result, "RESET should revert to startup param default, not flag default")
	})

	t.Run("startup param enforces timeout", func(t *testing.T) {
		connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable")
		connCfg, err := pgx.ParseConfig(connStr)
		require.NoError(t, err)
		connCfg.RuntimeParams["statement_timeout"] = "500"

		conn, err := pgx.ConnectConfig(ctx, connCfg)
		require.NoError(t, err)
		defer conn.Close(ctx)

		start := time.Now()
		_, err = conn.Exec(ctx, "SELECT pg_sleep(5)")
		elapsed := time.Since(start)

		require.Error(t, err, "query should be cancelled by startup param timeout")

		var pgErr *pgconn.PgError
		require.True(t, errors.As(err, &pgErr), "expected PgError, got %T: %v", err, err)
		assert.Equal(t, "57014", pgErr.Code, "should be query_canceled")
		assert.Less(t, elapsed, 3*time.Second)
	})
}

// connectPgx creates a pgx connection to the multigateway (extended query protocol).
func connectPgx(t *testing.T, ctx context.Context, setup *shardsetup.ShardSetup) *pgx.Conn {
	t.Helper()
	connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable")
	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err, "failed to connect with pgx")
	return conn
}

// connectClient creates a client.Conn to the multigateway (simple query protocol).
func connectClient(t *testing.T, ctx context.Context, setup *shardsetup.ShardSetup) *client.Conn {
	t.Helper()
	conn, err := client.Connect(ctx, ctx, &client.Config{
		Host:        "localhost",
		Port:        setup.MultigatewayPgPort,
		User:        shardsetup.DefaultTestUser,
		Password:    shardsetup.TestPostgresPassword,
		Database:    "postgres",
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err, "failed to connect with client.Conn")
	return conn
}
