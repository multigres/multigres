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
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestMultiGateway_StartupParamForwarding tests that client startup parameters
// (DateStyle, TimeZone, work_mem via PGOPTIONS, etc.) are correctly forwarded
// to the backend PostgreSQL and that ParameterStatus values are returned.
func TestMultiGateway_StartupParamForwarding(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping startup param forwarding test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 30*time.Second)

	// Test 1: DateStyle startup parameter via pgx RuntimeParams.
	// Verifies that the parameter is applied on the backend and SHOW returns
	// the correct value.
	t.Run("DateStyle startup parameter", func(t *testing.T) {
		cfg, err := pgx.ParseConfig(fmt.Sprintf(
			"host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable",
			setup.MultigatewayPgPort, shardsetup.TestPostgresPassword,
		))
		require.NoError(t, err)
		cfg.RuntimeParams["DateStyle"] = "German"

		conn, err := pgx.ConnectConfig(ctx, cfg)
		require.NoError(t, err)
		defer conn.Close(ctx)

		// Verify SHOW returns the startup param value
		var dateStyle string
		err = conn.QueryRow(ctx, "SHOW DateStyle").Scan(&dateStyle)
		require.NoError(t, err)
		assert.Contains(t, dateStyle, "German", "SHOW DateStyle should contain German")

		// Verify ParameterStatus received during connection setup contains the value.
		// Note: On first connection, the backend sends ParameterStatus with the new value.
		ps := conn.PgConn().ParameterStatus("DateStyle")
		assert.Contains(t, ps, "German", "ParameterStatus should reflect German DateStyle")
	})

	// Test 2: PGOPTIONS with work_mem via lib/pq options parameter
	t.Run("PGOPTIONS work_mem via options", func(t *testing.T) {
		connStr := fmt.Sprintf(
			"host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable options='-c work_mem=64MB'",
			setup.MultigatewayPgPort, shardsetup.TestPostgresPassword,
		)
		db, err := sql.Open("postgres", connStr)
		require.NoError(t, err)
		defer db.Close()

		var workMem string
		err = db.QueryRowContext(ctx, "SHOW work_mem").Scan(&workMem)
		require.NoError(t, err)
		assert.Equal(t, "64MB", workMem, "work_mem should be 64MB from PGOPTIONS")
	})

	// Test 3: Multiple startup parameters
	t.Run("multiple startup parameters", func(t *testing.T) {
		cfg, err := pgx.ParseConfig(fmt.Sprintf(
			"host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable",
			setup.MultigatewayPgPort, shardsetup.TestPostgresPassword,
		))
		require.NoError(t, err)
		cfg.RuntimeParams["DateStyle"] = "SQL"
		cfg.RuntimeParams["TimeZone"] = "US/Pacific"
		cfg.RuntimeParams["work_mem"] = "16MB"

		conn, err := pgx.ConnectConfig(ctx, cfg)
		require.NoError(t, err)
		defer conn.Close(ctx)

		var dateStyle string
		err = conn.QueryRow(ctx, "SHOW DateStyle").Scan(&dateStyle)
		require.NoError(t, err)
		assert.Contains(t, dateStyle, "SQL", "DateStyle should contain SQL")

		var tz string
		err = conn.QueryRow(ctx, "SHOW TimeZone").Scan(&tz)
		require.NoError(t, err)
		assert.Equal(t, "US/Pacific", tz, "TimeZone should be US/Pacific")

		var workMem string
		err = conn.QueryRow(ctx, "SHOW work_mem").Scan(&workMem)
		require.NoError(t, err)
		assert.Equal(t, "16MB", workMem, "work_mem should be 16MB")
	})

	// Test 4: SET command overrides startup parameter.
	// Verifies that an explicit SET takes precedence over the value sent at startup.
	t.Run("SET overrides startup parameter", func(t *testing.T) {
		cfg, err := pgx.ParseConfig(fmt.Sprintf(
			"host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable",
			setup.MultigatewayPgPort, shardsetup.TestPostgresPassword,
		))
		require.NoError(t, err)
		cfg.RuntimeParams["idle_in_transaction_session_timeout"] = "7s"

		conn, err := pgx.ConnectConfig(ctx, cfg)
		require.NoError(t, err)
		defer conn.Close(ctx)

		// Override startup param with SET
		_, err = conn.Exec(ctx, "SET idle_in_transaction_session_timeout = '15s'")
		require.NoError(t, err)

		// Verify SET took precedence over startup param across multiple queries
		for i := range 10 {
			var val string
			err = conn.QueryRow(ctx, "SHOW idle_in_transaction_session_timeout").Scan(&val)
			require.NoError(t, err, "iteration %d", i)
			require.Equal(t, "15s", val, "iteration %d: SET should override startup parameter", i)
		}
	})

	// Test 5: Connection pooling with different startup parameters.
	// Two concurrent connections with different lock_timeout values should
	// each see their own value consistently.
	t.Run("connection pooling with different startup params", func(t *testing.T) {
		cfgA, err := pgx.ParseConfig(fmt.Sprintf(
			"host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable",
			setup.MultigatewayPgPort, shardsetup.TestPostgresPassword,
		))
		require.NoError(t, err)
		cfgA.RuntimeParams["lock_timeout"] = "11s"

		connA, err := pgx.ConnectConfig(ctx, cfgA)
		require.NoError(t, err)
		defer connA.Close(ctx)

		cfgB, err := pgx.ParseConfig(fmt.Sprintf(
			"host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable",
			setup.MultigatewayPgPort, shardsetup.TestPostgresPassword,
		))
		require.NoError(t, err)
		cfgB.RuntimeParams["lock_timeout"] = "22s"

		connB, err := pgx.ConnectConfig(ctx, cfgB)
		require.NoError(t, err)
		defer connB.Close(ctx)

		// Verify each connection maintains its own startup params
		var valA, valB string
		err = connA.QueryRow(ctx, "SHOW lock_timeout").Scan(&valA)
		require.NoError(t, err)
		assert.Equal(t, "11s", valA, "connection A should have lock_timeout=11s")

		err = connB.QueryRow(ctx, "SHOW lock_timeout").Scan(&valB)
		require.NoError(t, err)
		assert.Equal(t, "22s", valB, "connection B should have lock_timeout=22s")

		// Cross-verify isolation across multiple queries
		for i := range 10 {
			err = connA.QueryRow(ctx, "SHOW lock_timeout").Scan(&valA)
			require.NoError(t, err, "iteration %d", i)
			require.Equal(t, "11s", valA, "conn A iteration %d: should still be 11s", i)

			err = connB.QueryRow(ctx, "SHOW lock_timeout").Scan(&valB)
			require.NoError(t, err, "iteration %d", i)
			require.Equal(t, "22s", valB, "conn B iteration %d: should still be 22s", i)
		}
	})

	// Test 6: server_version remains multigres while other params come from backend
	t.Run("server_version remains multigres", func(t *testing.T) {
		cfg, err := pgx.ParseConfig(fmt.Sprintf(
			"host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable",
			setup.MultigatewayPgPort, shardsetup.TestPostgresPassword,
		))
		require.NoError(t, err)
		cfg.RuntimeParams["TimeZone"] = "US/Eastern"

		conn, err := pgx.ConnectConfig(ctx, cfg)
		require.NoError(t, err)
		defer conn.Close(ctx)

		// server_version should always be multigres
		sv := conn.PgConn().ParameterStatus("server_version")
		assert.Equal(t, "17.0 (multigres)", sv, "server_version should be multigres")

		// The startup param should still be applied
		var tz string
		err = conn.QueryRow(ctx, "SHOW TimeZone").Scan(&tz)
		require.NoError(t, err)
		assert.Equal(t, "US/Eastern", tz, "TimeZone should be US/Eastern from startup param")
	})

	// Test 7: Invalid startup parameter fails connection immediately
	t.Run("invalid startup param fails connection", func(t *testing.T) {
		cfg, err := pgx.ParseConfig(fmt.Sprintf(
			"host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable",
			setup.MultigatewayPgPort, shardsetup.TestPostgresPassword,
		))
		require.NoError(t, err)
		cfg.RuntimeParams["completely_invalid_guc_12345"] = "some_value"

		_, err = pgx.ConnectConfig(ctx, cfg)
		require.Error(t, err, "connection with invalid startup param should fail")
	})
}

// TestMultiGateway_PGOPTIONSMultipleFlags tests PGOPTIONS parsing with various
// flag formats: -c, --, mixed formats, and backslash-escaped spaces.
func TestMultiGateway_PGOPTIONSMultipleFlags(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping PGOPTIONS multiple flags test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 30*time.Second)

	// Test 1: Single -c flag
	t.Run("single -c flag", func(t *testing.T) {
		connStr := fmt.Sprintf(
			"host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable options='-c geqo=off'",
			setup.MultigatewayPgPort, shardsetup.TestPostgresPassword,
		)
		db, err := sql.Open("postgres", connStr)
		require.NoError(t, err)
		defer db.Close()

		var geqo string
		err = db.QueryRowContext(ctx, "SHOW geqo").Scan(&geqo)
		require.NoError(t, err)
		assert.Equal(t, "off", geqo, "geqo should be off from PGOPTIONS")
	})

	// Test 2: Multiple -c flags
	t.Run("multiple -c flags", func(t *testing.T) {
		connStr := fmt.Sprintf(
			"host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable options='-c geqo=off -c work_mem=64MB'",
			setup.MultigatewayPgPort, shardsetup.TestPostgresPassword,
		)
		db, err := sql.Open("postgres", connStr)
		require.NoError(t, err)
		defer db.Close()

		var geqo string
		err = db.QueryRowContext(ctx, "SHOW geqo").Scan(&geqo)
		require.NoError(t, err)
		assert.Equal(t, "off", geqo, "geqo should be off")

		var workMem string
		err = db.QueryRowContext(ctx, "SHOW work_mem").Scan(&workMem)
		require.NoError(t, err)
		assert.Equal(t, "64MB", workMem, "work_mem should be 64MB")
	})

	// Test 3: Double-dash format with hyphen-to-underscore conversion
	t.Run("double-dash format", func(t *testing.T) {
		connStr := fmt.Sprintf(
			"host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable options='--statement-timeout=5min'",
			setup.MultigatewayPgPort, shardsetup.TestPostgresPassword,
		)
		db, err := sql.Open("postgres", connStr)
		require.NoError(t, err)
		defer db.Close()

		var timeout string
		err = db.QueryRowContext(ctx, "SHOW statement_timeout").Scan(&timeout)
		require.NoError(t, err)
		assert.Equal(t, "5min", timeout, "statement_timeout should be 5min from --statement-timeout")
	})

	// Test 4: Mixed formats (-c and --)
	t.Run("mixed formats", func(t *testing.T) {
		connStr := fmt.Sprintf(
			"host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable options='-c geqo=off --statement-timeout=5min'",
			setup.MultigatewayPgPort, shardsetup.TestPostgresPassword,
		)
		db, err := sql.Open("postgres", connStr)
		require.NoError(t, err)
		defer db.Close()

		var geqo string
		err = db.QueryRowContext(ctx, "SHOW geqo").Scan(&geqo)
		require.NoError(t, err)
		assert.Equal(t, "off", geqo, "geqo should be off")

		var timeout string
		err = db.QueryRowContext(ctx, "SHOW statement_timeout").Scan(&timeout)
		require.NoError(t, err)
		assert.Equal(t, "5min", timeout, "statement_timeout should be 5min")
	})

	// Test 5: Backslash-escaped space in value
	t.Run("backslash-escaped space", func(t *testing.T) {
		// Note: The backslash-space in the connection string gets interpreted by lib/pq
		// and then sent to multigateway. We need to double-escape the backslash for
		// the Go string literal, then lib/pq sends it as a single backslash-space.
		connStr := fmt.Sprintf(
			"host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable options='-c search_path=public,\\\\ private'",
			setup.MultigatewayPgPort, shardsetup.TestPostgresPassword,
		)
		db, err := sql.Open("postgres", connStr)
		require.NoError(t, err)
		defer db.Close()

		var searchPath string
		err = db.QueryRowContext(ctx, "SHOW search_path").Scan(&searchPath)
		require.NoError(t, err)
		assert.Contains(t, searchPath, "public", "search_path should contain public")
		assert.Contains(t, searchPath, "private", "search_path should contain private")
	})
}
