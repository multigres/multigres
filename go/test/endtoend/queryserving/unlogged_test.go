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
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestUnloggedTableCreateWarning verifies that the multigateway emits a WARNING
// NoticeResponse (pointing at the failover doc) when a client creates an unlogged
// table, that the CREATE still succeeds, and that a permanent table is silent.
//
// It runs over both the simple and extended query protocols, since the warning is
// attached on both the simple and extended-protocol (IsPortal) Plan paths.
func TestUnloggedTableCreateWarning(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping unlogged-warning test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping unlogged-warning test")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)
	ctx := utils.WithTimeout(t, 30*time.Second)

	// default_query_exec_mode selects pgx's wire protocol: simple_protocol hits
	// Plan with IsPortal=false, exec (unnamed extended) hits Plan with IsPortal=true.
	for _, mode := range []string{"simple_protocol", "exec"} {
		t.Run(mode, func(t *testing.T) {
			connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort,
				"sslmode=disable", "default_query_exec_mode="+mode)
			conn, collector := connectWithNotices(ctx, t, connStr)
			defer conn.Close(ctx)

			unlogged := fmt.Sprintf("ul_warn_%s_%d", mode, time.Now().UnixNano())
			defer func() { _, _ = conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+unlogged) }()

			collector.reset()
			_, err := conn.Exec(ctx, fmt.Sprintf("CREATE UNLOGGED TABLE %s (id int)", unlogged))
			require.NoError(t, err, "CREATE UNLOGGED TABLE must still succeed")

			warn := findWarning(collector.collect(), "01000")
			require.NotNil(t, warn, "expected a WARNING/01000 notice for CREATE UNLOGGED TABLE")
			assert.Equal(t, "WARNING", warn.Severity)
			assert.Contains(t, warn.Message, "table")
			assert.Contains(t, warn.Message, "failover")
			assert.Contains(t, warn.Hint, "docs/query_serving/unlogged_tables.md")
			t.Logf("Received WARNING: Code=%s Message=%q Hint=%q", warn.Code, warn.Message, warn.Hint)

			// An unlogged sequence gets a sequence-specific warning.
			seqName := fmt.Sprintf("ul_seq_%s_%d", mode, time.Now().UnixNano())
			defer func() { _, _ = conn.Exec(context.Background(), "DROP SEQUENCE IF EXISTS "+seqName) }()
			collector.reset()
			_, err = conn.Exec(ctx, "CREATE UNLOGGED SEQUENCE "+seqName)
			require.NoError(t, err, "CREATE UNLOGGED SEQUENCE must still succeed")
			seqWarn := findWarning(collector.collect(), "01000")
			require.NotNil(t, seqWarn, "expected a WARNING/01000 notice for CREATE UNLOGGED SEQUENCE")
			assert.Contains(t, seqWarn.Message, "sequence")
			assert.Contains(t, seqWarn.Message, "failover")

			// A permanent table must not trigger the warning.
			perm := fmt.Sprintf("perm_%s_%d", mode, time.Now().UnixNano())
			defer func() { _, _ = conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+perm) }()
			collector.reset()
			_, err = conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id int)", perm))
			require.NoError(t, err)
			assert.Nil(t, findWarning(collector.collect(), "01000"),
				"permanent table must not emit the unlogged warning")
		})
	}
}

// TestUnloggedTablesAfterFailover exercises the post-promotion unlogged-table
// sweep end to end. Unlogged table data is never replicated to standbys, so on
// promotion PostgreSQL resets these tables to empty. Rather than silently
// presenting an empty table, the new primary best-effort drops every unlogged
// table so clients hit a clear "relation does not exist" error and rebuild.
//
// The test covers both observable outcomes after a single failover:
//   - u_dropped (no dependents) is dropped — querying it returns 42P01.
//   - u_kept (a view depends on it) cannot be dropped without CASCADE, so it
//     survives but is empty; the dependent view is left intact.
//
// Failover is triggered by killing the primary's postgres and letting multiorch
// elect a new primary — this exercises the promotion path (where the sweep runs)
// without relying on the demoted primary rejoining.
func TestUnloggedTablesAfterFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping unlogged-failover test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping unlogged-failover test")
	}

	setup, cleanup := newFailoverTxnTestCluster(t)
	defer cleanup()

	ctx := context.Background()
	connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable")

	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx)

	// u_dropped: a plain unlogged table with no dependents — the sweep can drop it.
	_, err = conn.Exec(ctx, "CREATE UNLOGGED TABLE u_dropped (id INT PRIMARY KEY, val TEXT)")
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "INSERT INTO u_dropped VALUES (1, 'a'), (2, 'b')")
	require.NoError(t, err)

	// u_kept: an unlogged table a view depends on. DROP TABLE (no CASCADE) fails,
	// so the sweep leaves it in place.
	_, err = conn.Exec(ctx, "CREATE UNLOGGED TABLE u_kept (id INT PRIMARY KEY, val TEXT)")
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "INSERT INTO u_kept VALUES (1, 'a'), (2, 'b')")
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "CREATE VIEW v_kept AS SELECT id, val FROM u_kept")
	require.NoError(t, err)

	// Sanity: both tables hold their data before the failover.
	assertRowCount(t, ctx, conn, "u_dropped", 2)
	assertRowCount(t, ctx, conn, "u_kept", 2)

	// Disable automatic postgres restarts so the killed primary is not brought back
	// underneath the new one; multiorch alone drives the failover.
	disablePostgresRestarts(t, setup)

	oldPrimary := setup.PrimaryName
	t.Logf("unlogged tables populated; killing primary %s to force failover", oldPrimary)
	setup.KillPostgres(t, oldPrimary)

	newPrimary := shardsetup.WaitForNewPrimary(t, setup, oldPrimary, 90*time.Second)
	t.Logf("new primary elected: %s", newPrimary)

	// Wait for the gateway to resume serving against the new primary before asserting.
	require.Eventually(t, func() bool {
		c, err := pgx.Connect(ctx, connStr)
		if err != nil {
			return false
		}
		defer c.Close(ctx)
		var one int
		return c.QueryRow(ctx, "SELECT 1").Scan(&one) == nil
	}, 30*time.Second, 500*time.Millisecond, "gateway did not resume serving after failover")

	// Reuse the original client connection: it must survive the failover and
	// transparently re-route to the new primary.
	// u_dropped is dropped asynchronously on promotion; poll until the drop propagates.
	var dropErr error
	require.Eventually(t, func() bool {
		var n int
		dropErr = conn.QueryRow(ctx, "SELECT count(*) FROM u_dropped").Scan(&n)
		return dropErr != nil
	}, 5*time.Second, 100*time.Millisecond, "u_dropped should be dropped after failover")
	var pgErr *pgconn.PgError
	require.True(t, errors.As(dropErr, &pgErr), "expected pgconn.PgError, got %T: %v", dropErr, dropErr)
	assert.Equal(t, "42P01", pgErr.Code, "dropped unlogged table should surface undefined_table")

	// u_kept could not be dropped (the view depends on it), so it survives — but
	// empty, because promotion reset its contents.
	assertRowCount(t, ctx, conn, "u_kept", 0)

	// The dependent view is intact (the sweep never uses CASCADE).
	assertRowCount(t, ctx, conn, "v_kept", 0)
}

// findWarning returns the first notice with the given SQLSTATE code, or nil.
func findWarning(notices []*pgconn.Notice, code string) *pgconn.Notice {
	for _, n := range notices {
		if n.Code == code {
			return n
		}
	}
	return nil
}

// disablePostgresRestarts turns off the postgres monitor's automatic restarts on
// every node, so a killed primary stays down and multiorch orchestrates recovery.
func disablePostgresRestarts(t *testing.T, setup *shardsetup.ShardSetup) {
	t.Helper()
	for name, inst := range setup.Multipoolers {
		mc, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
		require.NoError(t, err, "connect to multipooler %s", name)
		_, err = mc.Manager.SetPostgresRestartsEnabled(utils.WithTimeout(t, 5*time.Second),
			&multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: false})
		mc.Close()
		require.NoError(t, err, "disable postgres restarts on %s", name)
	}
}

// assertRowCount asserts that SELECT count(*) over the given relation equals want.
func assertRowCount(t *testing.T, ctx context.Context, conn *pgx.Conn, relation string, want int) {
	t.Helper()
	var got int
	err := conn.QueryRow(ctx, "SELECT count(*) FROM "+relation).Scan(&got)
	require.NoError(t, err, "counting %s", relation)
	assert.Equal(t, want, got, "row count of %s", relation)
}
