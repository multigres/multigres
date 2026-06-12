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

	// set_config alongside a real query (`SELECT set_config(...), * FROM t`).
	// The planner uses Sequence[silent ApplySessionState, Route]: update the
	// tracker first, then let PG execute the query normally so it returns
	// both the set_config column and the table rows. Verify both effects:
	// the tracker update persists across pooled connections, and the table
	// rows come through intact.
	t.Run("SELECT set_config alongside table read", func(t *testing.T) {
		_, err := db.ExecContext(ctx, "RESET ALL")
		require.NoError(t, err)

		// Scratch table so we have something to SELECT * FROM.
		_, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS set_config_mix_t")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "CREATE TABLE set_config_mix_t (id int, name text)")
		require.NoError(t, err)
		defer func() {
			_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS set_config_mix_t")
		}()
		_, err = db.ExecContext(ctx,
			"INSERT INTO set_config_mix_t VALUES (1, 'one'), (2, 'two')")
		require.NoError(t, err)

		// Run the mixed query. It returns one row per table row, each with
		// the set_config column (always equal to the new value) and the
		// table columns.
		rows, err := db.QueryContext(ctx,
			"SELECT set_config('work_mem', '384MB', false), id, name FROM set_config_mix_t ORDER BY id")
		require.NoError(t, err, "mixed set_config + table read should succeed")

		type row struct {
			setConfigVal string
			id           int
			name         string
		}
		var got []row
		for rows.Next() {
			var r row
			require.NoError(t, rows.Scan(&r.setConfigVal, &r.id, &r.name))
			got = append(got, r)
		}
		require.NoError(t, rows.Err())
		rows.Close() //nolint:sqlclosecheck // close-before-loop, not defer

		require.Len(t, got, 2)
		assert.Equal(t, row{setConfigVal: "384MB", id: 1, name: "one"}, got[0])
		assert.Equal(t, row{setConfigVal: "384MB", id: 2, name: "two"}, got[1])

		// Tracker update must persist across pool rotations.
		for i := range 100 {
			var got string
			err = db.QueryRowContext(ctx, "SHOW work_mem").Scan(&got)
			require.NoError(t, err, "iteration %d", i)
			require.Equal(t, "384MB", got,
				"iteration %d: tracker update from mixed query should persist", i)
		}
	})

	// Multiple set_config calls in the same target list — each must update
	// the tracker. Tests the Sequence primitive's ordering (two silent
	// ApplySessionState primitives before the Route step).
	t.Run("multiple set_configs in one query", func(t *testing.T) {
		_, err := db.ExecContext(ctx, "RESET ALL")
		require.NoError(t, err)

		_, err = db.ExecContext(ctx,
			"SELECT set_config('work_mem', '192MB', false), set_config('search_path', 'mixschema', false)")
		require.NoError(t, err)

		for i := range 100 {
			var wm, sp string
			err = db.QueryRowContext(ctx, "SHOW work_mem").Scan(&wm)
			require.NoError(t, err, "iteration %d", i)
			require.Equal(t, "192MB", wm, "iteration %d", i)

			err = db.QueryRowContext(ctx, "SHOW search_path").Scan(&sp)
			require.NoError(t, err, "iteration %d", i)
			require.Equal(t, "mixschema", sp, "iteration %d", i)
		}
	})

	// Regression for the simple-protocol shape `SELECT set_config(...), <literal>`.
	// The normalizer skips inside set_config but still parameterizes the
	// sibling literal, and the planner emits a Sequence whose trailing Route
	// reconstructs the SQL from bindVars. If Sequence.StreamExecute drops
	// bindVars on its way to children, Route ships the normalized `$N`
	// placeholder to PG and the backend errors with "there is no parameter
	// $1". Verify both the row data the client sees and the tracker update
	// for the set_config side effect.
	t.Run("SELECT set_config alongside sibling literal", func(t *testing.T) {
		_, err := db.ExecContext(ctx, "RESET ALL")
		require.NoError(t, err)

		var setConfigVal string
		var num int
		err = db.QueryRowContext(ctx,
			"SELECT set_config('work_mem', '256MB', false), 42 AS num").
			Scan(&setConfigVal, &num)
		require.NoError(t, err, "sibling literal alongside set_config must not break SQL reconstruction")
		assert.Equal(t, "256MB", setConfigVal)
		assert.Equal(t, 42, num)

		// Tracker must still reflect the set_config so the value persists
		// across pool rotations.
		for i := range 50 {
			var got string
			err = db.QueryRowContext(ctx, "SHOW work_mem").Scan(&got)
			require.NoError(t, err, "iteration %d", i)
			require.Equal(t, "256MB", got, "iteration %d", i)
		}
	})

	// set_config(..., true) — transaction-scoped — is allowed but NOT tracked
	// by the pooler (it's scoped to the backend transaction, so the pool
	// shouldn't carry it forward). PG handles it directly.
	t.Run("set_config is_local=true passes through", func(t *testing.T) {
		_, err := db.ExecContext(ctx, "RESET ALL")
		require.NoError(t, err)

		// Run inside a transaction so is_local=true has something to scope to.
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback() //nolint:errcheck

		var val string
		err = tx.QueryRowContext(ctx,
			"SELECT set_config('work_mem', '999MB', true)").Scan(&val)
		require.NoError(t, err, "is_local=true should pass through to PG")
		assert.Equal(t, "999MB", val)
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
			"embedded set_config in WHERE",
			"SELECT 1 WHERE set_config('work_mem','256MB',false) IS NOT NULL",
			"set_config is only supported as a top-level SELECT target list entry",
		},
		{
			"set_config nested inside another function",
			"SELECT length(set_config('work_mem','256MB',false))",
			"set_config is only supported as a top-level SELECT target list entry",
		},
		{
			"set_config in CTE",
			"WITH c AS (SELECT set_config('work_mem','256MB',false)) SELECT * FROM c",
			"set_config is only supported as a top-level SELECT target list entry",
		},
		{
			"non-literal set_config value",
			"SELECT set_config('work_mem', name, false) FROM (SELECT 'x' AS name) s",
			"set_config value argument must be a literal constant",
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
