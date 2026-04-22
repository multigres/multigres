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
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestMultiGateway_SetConfigRoutedAsSET verifies the expression-level rewrite:
// a bare `SELECT set_config('work_mem', ..., false)` must have the same
// end-to-end effect as `SET work_mem = ...`, including persisting across
// pooled connection rotations. Without the rewrite, set_config would set the
// value on one backend connection only, and a later query that lands on a
// different pooled connection would observe the stale value — the original
// session-state leak this layer was built to close.
func TestMultiGateway_SetConfigRoutedAsSET(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping set_config routing test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping set_config routing tests")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	defer db.Close()

	ctx := utils.WithTimeout(t, 150*time.Second)
	require.NoError(t, db.PingContext(ctx))

	// The core assertion: set_config(..., false) via SELECT must persist in
	// exactly the same way SET does. 100 SHOW iterations is enough to rotate
	// across every pooled connection the gateway holds.
	t.Run("SELECT set_config persists across pooled connection rotations", func(t *testing.T) {
		_, err := db.ExecContext(ctx, "RESET ALL")
		require.NoError(t, err)

		var setConfigResult string
		err = db.QueryRowContext(ctx,
			"SELECT set_config('work_mem', '256MB', false)").Scan(&setConfigResult)
		require.NoError(t, err, "SELECT set_config should succeed")
		assert.Equal(t, "256MB", setConfigResult,
			"set_config returns the new value (same as a direct SET would report via SHOW)")

		for i := range 100 {
			var got string
			err = db.QueryRowContext(ctx, "SHOW work_mem").Scan(&got)
			require.NoError(t, err, "iteration %d", i)
			require.Equal(t, "256MB", got,
				"iteration %d: work_mem must persist across pool rotations", i)
		}
	})

	// pg_catalog.set_config should resolve to the same built-in and route
	// through the same rewrite path. Confirms the qualified-name handling.
	t.Run("pg_catalog.set_config also rewrites", func(t *testing.T) {
		_, err := db.ExecContext(ctx, "RESET ALL")
		require.NoError(t, err)

		_, err = db.ExecContext(ctx,
			"SELECT pg_catalog.set_config('search_path', 'myschema', false)")
		require.NoError(t, err)

		for i := range 100 {
			var got string
			err = db.QueryRowContext(ctx, "SHOW search_path").Scan(&got)
			require.NoError(t, err)
			require.Equal(t, "myschema", got, "iteration %d", i)
		}
	})

	// Extended protocol path: pgx parses+executes via Parse/Bind/Execute,
	// which runs through PlanPortal rather than Plan. Both paths must apply
	// the same rewrite or the tracker diverges depending on which driver
	// feature the client used.
	t.Run("extended protocol also rewrites", func(t *testing.T) {
		pgxConnStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable")
		conn, err := pgx.Connect(ctx, pgxConnStr)
		require.NoError(t, err)
		defer conn.Close(ctx)

		_, err = conn.Exec(ctx, "RESET ALL")
		require.NoError(t, err)

		var val string
		err = conn.QueryRow(ctx,
			"SELECT set_config('work_mem', '128MB', false)",
			pgx.QueryExecModeDescribeExec).Scan(&val)
		require.NoError(t, err)
		assert.Equal(t, "128MB", val)

		for i := range 100 {
			var got string
			err = conn.QueryRow(ctx, "SHOW work_mem").Scan(&got)
			require.NoError(t, err)
			require.Equal(t, "128MB", got, "iteration %d", i)
		}
	})
}

// TestMultiGateway_UnsafeFuncCallRejection verifies that the expression-level
// blocklist (dblink, pg_read_file, lo_import, …) is enforced end-to-end.
// Each function is rejected at plan time with SQLSTATE 0A000 so the call
// never reaches a backend.
func TestMultiGateway_UnsafeFuncCallRejection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping unsafe funccall rejection test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping unsafe funccall tests")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 60*time.Second)

	connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable")
	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx)

	tests := []struct {
		name    string
		sql     string
		wantMsg string
	}{
		{"dblink bare", "SELECT dblink('host=example.com', 'SELECT 1')", "dblink is not supported"},
		{"dblink schema-qualified", "SELECT pg_catalog.dblink('host=example.com', 'SELECT 1')", "dblink is not supported"},
		{"pg_read_file", "SELECT pg_read_file('/etc/passwd')", "pg_read_file is not supported"},
		{"pg_ls_dir", "SELECT pg_ls_dir('/')", "pg_ls_dir is not supported"},
		{"lo_import", "SELECT lo_import('/tmp/x')", "lo_import is not supported"},
		{"query_to_xml", "SELECT query_to_xml('SELECT 1', true, false, '')", "query_to_xml is not supported"},
		{
			"embedded set_config(..., false)",
			"SELECT 1 WHERE set_config('work_mem','256MB',false) IS NOT NULL",
			"set_config(..., false) is only supported",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := conn.Exec(ctx, tt.sql)
			require.Error(t, err, "%s should be rejected", tt.name)

			var pgErr *pgconn.PgError
			require.True(t, errors.As(err, &pgErr), "expected pgconn.PgError, got %T", err)
			assert.Equal(t, "0A000", pgErr.Code)
			assert.Contains(t, pgErr.Message, tt.wantMsg)
		})
	}

	// Connection must still be usable after a series of rejected statements —
	// rejection at plan time should not corrupt connection state.
	var result int
	err = conn.QueryRow(ctx, "SELECT 1").Scan(&result)
	require.NoError(t, err, "connection should still be usable after rejections")
	assert.Equal(t, 1, result)
}
