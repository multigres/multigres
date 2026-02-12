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
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestMultiGateway_StartupParamForwarding tests that startup parameters sent in
// the PostgreSQL StartupMessage are forwarded through multigateway to the backend.
func TestMultiGateway_StartupParamForwarding(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping startup param forwarding test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping startup param forwarding tests")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 150*time.Second)

	t.Run("DateStyle via pgx RuntimeParams", func(t *testing.T) {
		connCfg, err := pgx.ParseConfig(fmt.Sprintf(
			"host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable",
			setup.MultigatewayPgPort, shardsetup.TestPostgresPassword))
		require.NoError(t, err)
		connCfg.RuntimeParams["DateStyle"] = "German"

		conn, err := pgx.ConnectConfig(ctx, connCfg)
		require.NoError(t, err)
		defer conn.Close(ctx)

		var result string
		err = conn.QueryRow(ctx, "SHOW DateStyle").Scan(&result)
		require.NoError(t, err)
		assert.Contains(t, result, "German", "DateStyle should contain 'German'")
	})

	t.Run("multiple startup parameters", func(t *testing.T) {
		connCfg, err := pgx.ParseConfig(fmt.Sprintf(
			"host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable",
			setup.MultigatewayPgPort, shardsetup.TestPostgresPassword))
		require.NoError(t, err)
		connCfg.RuntimeParams["DateStyle"] = "SQL"
		connCfg.RuntimeParams["TimeZone"] = "US/Pacific"
		connCfg.RuntimeParams["work_mem"] = "16MB"

		conn, err := pgx.ConnectConfig(ctx, connCfg)
		require.NoError(t, err)
		defer conn.Close(ctx)

		var dateStyle string
		err = conn.QueryRow(ctx, "SHOW DateStyle").Scan(&dateStyle)
		require.NoError(t, err)
		assert.Contains(t, dateStyle, "SQL", "DateStyle should contain 'SQL'")

		var tz string
		err = conn.QueryRow(ctx, "SHOW TimeZone").Scan(&tz)
		require.NoError(t, err)
		assert.Equal(t, "US/Pacific", tz, "TimeZone should be US/Pacific")

		var workMem string
		err = conn.QueryRow(ctx, "SHOW work_mem").Scan(&workMem)
		require.NoError(t, err)
		assert.Equal(t, "16MB", workMem, "work_mem should be 16MB")
	})

	t.Run("SET overrides startup parameter", func(t *testing.T) {
		connCfg, err := pgx.ParseConfig(fmt.Sprintf(
			"host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable",
			setup.MultigatewayPgPort, shardsetup.TestPostgresPassword))
		require.NoError(t, err)
		connCfg.RuntimeParams["idle_in_transaction_session_timeout"] = "7s"

		conn, err := pgx.ConnectConfig(ctx, connCfg)
		require.NoError(t, err)
		defer conn.Close(ctx)

		// Verify startup value
		var result string
		err = conn.QueryRow(ctx, "SHOW idle_in_transaction_session_timeout").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "7s", result)

		// Override with SET
		_, err = conn.Exec(ctx, "SET idle_in_transaction_session_timeout = '15s'")
		require.NoError(t, err)

		// Verify SET takes precedence across multiple queries
		for i := range 10 {
			err = conn.QueryRow(ctx, "SHOW idle_in_transaction_session_timeout").Scan(&result)
			require.NoError(t, err, "iteration %d", i)
			require.Equal(t, "15s", result, "iteration %d: SET should override startup param", i)
		}
	})

	t.Run("connection pooling isolation", func(t *testing.T) {
		// Connection 1 with lock_timeout=11s
		connCfg1, err := pgx.ParseConfig(fmt.Sprintf(
			"host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable",
			setup.MultigatewayPgPort, shardsetup.TestPostgresPassword))
		require.NoError(t, err)
		connCfg1.RuntimeParams["lock_timeout"] = "11s"

		conn1, err := pgx.ConnectConfig(ctx, connCfg1)
		require.NoError(t, err)
		defer conn1.Close(ctx)

		// Connection 2 with lock_timeout=22s
		connCfg2, err := pgx.ParseConfig(fmt.Sprintf(
			"host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable",
			setup.MultigatewayPgPort, shardsetup.TestPostgresPassword))
		require.NoError(t, err)
		connCfg2.RuntimeParams["lock_timeout"] = "22s"

		conn2, err := pgx.ConnectConfig(ctx, connCfg2)
		require.NoError(t, err)
		defer conn2.Close(ctx)

		// Verify each connection sees its own value across 10 queries
		for i := range 10 {
			var v1 string
			err = conn1.QueryRow(ctx, "SHOW lock_timeout").Scan(&v1)
			require.NoError(t, err, "conn1 iteration %d", i)
			require.Equal(t, "11s", v1, "conn1 iteration %d: should see 11s", i)

			var v2 string
			err = conn2.QueryRow(ctx, "SHOW lock_timeout").Scan(&v2)
			require.NoError(t, err, "conn2 iteration %d", i)
			require.Equal(t, "22s", v2, "conn2 iteration %d: should see 22s", i)
		}
	})

	t.Run("server_version unchanged", func(t *testing.T) {
		// Get baseline server_version without startup params
		baselineCfg, err := pgx.ParseConfig(fmt.Sprintf(
			"host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable",
			setup.MultigatewayPgPort, shardsetup.TestPostgresPassword))
		require.NoError(t, err)

		baselineConn, err := pgx.ConnectConfig(ctx, baselineCfg)
		require.NoError(t, err)
		var baselineVersion string
		err = baselineConn.QueryRow(ctx, "SHOW server_version").Scan(&baselineVersion)
		require.NoError(t, err)
		baselineConn.Close(ctx)

		// Connect with startup params and verify server_version is the same
		connCfg, err := pgx.ParseConfig(fmt.Sprintf(
			"host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable",
			setup.MultigatewayPgPort, shardsetup.TestPostgresPassword))
		require.NoError(t, err)
		connCfg.RuntimeParams["TimeZone"] = "US/Eastern"

		conn, err := pgx.ConnectConfig(ctx, connCfg)
		require.NoError(t, err)
		defer conn.Close(ctx)

		var version string
		err = conn.QueryRow(ctx, "SHOW server_version").Scan(&version)
		require.NoError(t, err)
		assert.Equal(t, baselineVersion, version, "server_version should remain unchanged with startup params")
	})

	t.Run("invalid startup parameter allows connection but fails queries", func(t *testing.T) {
		connCfg, err := pgx.ParseConfig(fmt.Sprintf(
			"host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable",
			setup.MultigatewayPgPort, shardsetup.TestPostgresPassword))
		require.NoError(t, err)
		connCfg.RuntimeParams["completely_invalid_guc_12345"] = "some_value"

		// Connection should succeed — no validation at startup.
		conn, err := pgx.ConnectConfig(ctx, connCfg)
		require.NoError(t, err, "connection with invalid startup parameter should succeed")
		defer conn.Close(ctx)

		// Queries should fail because the invalid GUC is sent as a SET SESSION command.
		var result string
		err = conn.QueryRow(ctx, "SELECT 1").Scan(&result)
		require.Error(t, err, "query should fail due to invalid startup parameter")
	})
}

// TestMultiGateway_PGOPTIONSMultipleFlags tests that PGOPTIONS startup parameter
// is correctly parsed and forwarded through the multigateway stack.
func TestMultiGateway_PGOPTIONSMultipleFlags(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping PGOPTIONS test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping PGOPTIONS tests")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 150*time.Second)

	t.Run("single -c flag via lib/pq", func(t *testing.T) {
		connStr := fmt.Sprintf(
			"host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable connect_timeout=5 options='-c work_mem=64MB'",
			setup.MultigatewayPgPort, shardsetup.TestPostgresPassword)

		db, err := sql.Open("postgres", connStr)
		require.NoError(t, err)
		defer db.Close()

		var result string
		err = db.QueryRowContext(ctx, "SHOW work_mem").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "64MB", result, "work_mem should be 64MB")
	})

	t.Run("multiple -c flags", func(t *testing.T) {
		connStr := fmt.Sprintf(
			"host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable connect_timeout=5 options='-c work_mem=32MB -c lock_timeout=5s'",
			setup.MultigatewayPgPort, shardsetup.TestPostgresPassword)

		db, err := sql.Open("postgres", connStr)
		require.NoError(t, err)
		defer db.Close()

		var workMem string
		err = db.QueryRowContext(ctx, "SHOW work_mem").Scan(&workMem)
		require.NoError(t, err)
		assert.Equal(t, "32MB", workMem, "work_mem should be 32MB")

		var lockTimeout string
		err = db.QueryRowContext(ctx, "SHOW lock_timeout").Scan(&lockTimeout)
		require.NoError(t, err)
		assert.Equal(t, "5s", lockTimeout, "lock_timeout should be 5s")
	})

	t.Run("--key=value with hyphen-to-underscore", func(t *testing.T) {
		connStr := fmt.Sprintf(
			"host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable connect_timeout=5 options='--statement-timeout=10s'",
			setup.MultigatewayPgPort, shardsetup.TestPostgresPassword)

		db, err := sql.Open("postgres", connStr)
		require.NoError(t, err)
		defer db.Close()

		var result string
		err = db.QueryRowContext(ctx, "SHOW statement_timeout").Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "10s", result, "statement_timeout should be 10s")
	})

	t.Run("mixed -c and -- formats", func(t *testing.T) {
		connStr := fmt.Sprintf(
			"host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable connect_timeout=5 options='-c work_mem=48MB --lock-timeout=3s'",
			setup.MultigatewayPgPort, shardsetup.TestPostgresPassword)

		db, err := sql.Open("postgres", connStr)
		require.NoError(t, err)
		defer db.Close()

		var workMem string
		err = db.QueryRowContext(ctx, "SHOW work_mem").Scan(&workMem)
		require.NoError(t, err)
		assert.Equal(t, "48MB", workMem, "work_mem should be 48MB")

		var lockTimeout string
		err = db.QueryRowContext(ctx, "SHOW lock_timeout").Scan(&lockTimeout)
		require.NoError(t, err)
		assert.Equal(t, "3s", lockTimeout, "lock_timeout should be 3s")
	})
}
