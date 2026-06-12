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
	"testing"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestDynamicSetConfig covers the resolve-and-apply path for a SELECT whose
// target list is entirely set_config(...) with an argument that can't be
// resolved at plan time (a column reference) — the shape pg_dump uses on PG17+.
// The gateway runs the argument projection once to learn the concrete
// (name, value, is_local) tuples, tracks the session-scoped ones so they
// survive pool rotation, and applies them with literals.
func TestDynamicSetConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping dynamic set_config test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping dynamic set_config test")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err, "failed to open database connection")
	defer db.Close()

	ctx := utils.WithTimeout(t, 150*time.Second)
	require.NoError(t, db.PingContext(ctx), "failed to ping multigateway")

	// Dynamic GUC name resolved from pg_settings, then tracked so it survives
	// pool rotation — the core of the pg_dump fix.
	t.Run("dynamic name tracked and survives pool rotation", func(t *testing.T) {
		_, err := db.ExecContext(ctx,
			`SELECT set_config(name, 'dynmatic_schema', false) FROM pg_settings WHERE name = 'search_path'`)
		require.NoError(t, err, "dynamic-name set_config should succeed")

		// Verify across many queries (interleaved) — the tracked setting must
		// be replayed onto whatever backend the pool hands out.
		for i := range 30 {
			var got string
			err = db.QueryRowContext(ctx, "SHOW search_path").Scan(&got)
			require.NoError(t, err, "SHOW search_path iteration %d", i)
			require.Equal(t, "dynmatic_schema", got, "iteration %d", i)

			var one int
			require.NoError(t, db.QueryRowContext(ctx, "SELECT 1").Scan(&one))
		}
	})

	// The actual PG17+ pg_dump probe statement. set_config returns the new
	// value; the restriction must then be in effect for the session.
	t.Run("pg_dump restrict_nonsystem_relation_kind probe", func(t *testing.T) {
		var got string
		err := db.QueryRowContext(ctx,
			`SELECT set_config(name, 'view, foreign-table', false) FROM pg_settings WHERE name = 'restrict_nonsystem_relation_kind'`).Scan(&got)
		require.NoError(t, err, "restrict_nonsystem_relation_kind probe should succeed")
		require.Equal(t, "view, foreign-table", got, "set_config returns the applied value")

		var shown string
		err = db.QueryRowContext(ctx, "SHOW restrict_nonsystem_relation_kind").Scan(&shown)
		require.NoError(t, err, "SHOW restrict_nonsystem_relation_kind")
		assert.Equal(t, "view, foreign-table", shown, "restriction must be in effect for the session")
	})

	// WHERE matching no row: nothing is set, an empty result comes back, and no
	// error — the version-guarded form's whole point (GUC absent → no-op).
	t.Run("zero rows is a harmless no-op", func(t *testing.T) {
		rows, err := db.QueryContext(ctx,
			`SELECT set_config(name, 'whatever', false) FROM pg_settings WHERE name = 'no_such_guc_xyz'`)
		require.NoError(t, err, "zero-row resolve should not error")
		defer rows.Close()
		assert.False(t, rows.Next(), "expected zero rows")
		require.NoError(t, rows.Err())
	})

	// Multiple set_config columns, both with a dynamic argument, in one SELECT.
	t.Run("multi-column", func(t *testing.T) {
		var workMem, searchPath string
		err := db.QueryRowContext(ctx,
			`SELECT set_config(name, '256MB', false), set_config('search_path', name, false) FROM pg_settings WHERE name = 'work_mem'`).
			Scan(&workMem, &searchPath)
		require.NoError(t, err, "multi-column dynamic set_config should succeed")
		assert.Equal(t, "256MB", workMem)
		assert.Equal(t, "work_mem", searchPath)

		var shownWorkMem string
		require.NoError(t, db.QueryRowContext(ctx, "SHOW work_mem").Scan(&shownWorkMem))
		assert.Equal(t, "256MB", shownWorkMem, "first column's setting must be tracked")

		var shownSearchPath string
		require.NoError(t, db.QueryRowContext(ctx, "SHOW search_path").Scan(&shownSearchPath))
		assert.Equal(t, "work_mem", shownSearchPath, "second column's setting must be tracked")
	})

	// A dynamic is_local resolving to true inside a transaction: applied
	// transaction-locally (visible until COMMIT) but NOT tracked, so it does
	// not survive the transaction.
	t.Run("dynamic is_local=true is transaction-scoped, untracked", func(t *testing.T) {
		// Establish a known session value first.
		_, err := db.ExecContext(ctx, "SET work_mem = '64MB'")
		require.NoError(t, err)

		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()

		var applied string
		err = tx.QueryRowContext(ctx,
			`SELECT set_config('work_mem', '128MB', islocal) FROM (SELECT true AS islocal) s`).Scan(&applied)
		require.NoError(t, err, "dynamic is_local set_config should succeed")
		require.Equal(t, "128MB", applied)

		var inTxn string
		require.NoError(t, tx.QueryRowContext(ctx, "SHOW work_mem").Scan(&inTxn))
		assert.Equal(t, "128MB", inTxn, "SET LOCAL visible within the transaction")

		require.NoError(t, tx.Commit())

		// After commit the local change is gone and the session value remains.
		var afterTxn string
		require.NoError(t, db.QueryRowContext(ctx, "SHOW work_mem").Scan(&afterTxn))
		assert.Equal(t, "64MB", afterTxn, "transaction-local change must not persist")
	})
}
