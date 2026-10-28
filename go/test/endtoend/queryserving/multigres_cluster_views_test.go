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
	"testing"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestMultigateway_ClusterViews covers multigres.multigateways and
// multigres.multipoolers: gateway-served views folded into the query at
// execution time, so they compose with WHERE, JOIN and the extended protocol.
func TestMultigateway_ClusterViews(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster views test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping cluster views test")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	connStr := shardsetup.GetTestUserDSN("localhost", setup.ClientPort(), "sslmode=disable", "connect_timeout=5")
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	defer db.Close()

	ctx := utils.WithTimeout(t, 60*time.Second)
	require.NoError(t, db.PingContext(ctx))

	t.Run("multipoolers lists the poolers with exactly one routing primary", func(t *testing.T) {
		var total, primaries, leaders int
		require.NoError(t, db.QueryRowContext(ctx, `
			SELECT count(*),
			       count(*) FILTER (WHERE routing_role = 'primary'),
			       count(*) FILTER (WHERE leadership = 'leader')
			FROM multigres.multipoolers`).Scan(&total, &primaries, &leaders))
		assert.GreaterOrEqual(t, total, 1)
		assert.Equal(t, 1, primaries)
		assert.Equal(t, 1, leaders)
	})

	t.Run("every pooler row is well-formed", func(t *testing.T) {
		rows, err := db.QueryContext(ctx, `
			SELECT name, cell, database, serving_status, lifecycle, connected,
			       lifecycle_updated IS NOT NULL, last_change, discovery_state
			FROM multigres.multipoolers ORDER BY name`)
		require.NoError(t, err)
		defer rows.Close()
		n := 0
		for rows.Next() {
			var name, cell, database, serving, lifecycle, discovery string
			var connected, hasLifecycleTime bool
			var lastChange time.Time
			require.NoError(t, rows.Scan(&name, &cell, &database, &serving, &lifecycle, &connected, &hasLifecycleTime, &lastChange, &discovery))
			assert.NotEmpty(t, name)
			assert.NotEmpty(t, cell)
			assert.NotEmpty(t, database)
			assert.Equal(t, "serving", serving, name)
			assert.Contains(t, []string{"starting", "active"}, lifecycle, name)
			assert.True(t, connected, name)
			assert.True(t, hasLifecycleTime, name)
			assert.False(t, lastChange.IsZero(), name)
			assert.Equal(t, "live", discovery, name)
			n++
		}
		require.NoError(t, rows.Err())
		assert.GreaterOrEqual(t, n, 1)
	})

	t.Run("multigateways flags the serving gateway", func(t *testing.T) {
		var name, cell string
		var prefix sql.NullInt64
		var selves int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FILTER (WHERE is_self) FROM multigres.multigateways`).Scan(&selves))
		assert.Equal(t, 1, selves)
		require.NoError(t, db.QueryRowContext(ctx,
			`SELECT name, cell, pid_prefix FROM multigres.multigateways WHERE is_self`).
			Scan(&name, &cell, &prefix))
		assert.NotEmpty(t, name)
		assert.NotEmpty(t, cell)
	})

	t.Run("the views compose: join, aggregate, alias", func(t *testing.T) {
		var n int
		require.NoError(t, db.QueryRowContext(ctx, `
			SELECT count(*) FROM multigres.multipoolers AS p
			JOIN multigres.multigateways AS g ON g.cell = p.cell
			WHERE g.is_self`).Scan(&n))
		assert.GreaterOrEqual(t, n, 1, "the gateway and its poolers share a cell")
	})

	t.Run("extended protocol: prepared with a parameter, executed twice", func(t *testing.T) {
		stmt, err := db.PrepareContext(ctx, `SELECT name FROM multigres.multipoolers WHERE routing_role = $1 ORDER BY name`)
		require.NoError(t, err)
		defer stmt.Close()
		for range 2 {
			var name string
			require.NoError(t, stmt.QueryRowContext(ctx, "primary").Scan(&name))
			assert.NotEmpty(t, name)
		}
		var none sql.NullString
		err = stmt.QueryRowContext(ctx, "no-such-role").Scan(&none)
		assert.ErrorIs(t, err, sql.ErrNoRows, "the parameter is applied by PostgreSQL over the folded rows")
	})

	t.Run("extended protocol inside an explicit transaction", func(t *testing.T) {
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		var n int
		require.NoError(t, tx.QueryRowContext(ctx, `SELECT count(*) FROM multigres.multigateways WHERE is_self = $1`, true).Scan(&n))
		assert.Equal(t, 1, n)
	})

	t.Run("a partial column alias keeps the remaining column names", func(t *testing.T) {
		var n, cell string
		require.NoError(t, db.QueryRowContext(ctx, `SELECT p.n, p.cell FROM multigres.multipoolers AS p(n) ORDER BY p.n LIMIT 1`).Scan(&n, &cell))
		assert.NotEmpty(t, n)
		assert.NotEmpty(t, cell)
	})

	t.Run("a snapshot can be materialized but not defined as a view", func(t *testing.T) {
		_, err := db.ExecContext(ctx, `CREATE TABLE cluster_views_snap AS SELECT name, cell FROM multigres.multipoolers`)
		require.NoError(t, err)
		defer func() { _, _ = db.ExecContext(ctx, `DROP TABLE cluster_views_snap`) }()
		var n int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM cluster_views_snap`).Scan(&n))
		assert.GreaterOrEqual(t, n, 1)

		_, err = db.ExecContext(ctx, `CREATE VIEW cluster_views_v AS SELECT * FROM multigres.multipoolers`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "view cannot be defined")
	})

	t.Run("writes are rejected and the unqualified name is not a view", func(t *testing.T) {
		_, err := db.ExecContext(ctx, `DELETE FROM multigres.multipoolers`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "read-only view")

		// Inside a transaction the Parse goes to the backend immediately; the
		// gateway still answers with its own rejection.
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		_, err = tx.ExecContext(ctx, `DELETE FROM multigres.multipoolers WHERE name = $1`, "x")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "read-only view")
		_ = tx.Rollback()

		_, err = db.ExecContext(ctx, `SELECT * FROM multipoolers`)
		require.Error(t, err, "without the multigres. qualifier the name goes to the backend as written")
		assert.Contains(t, err.Error(), "does not exist")
	})
}
