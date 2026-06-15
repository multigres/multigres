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
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// newSearchPath is a distinctive per-database default that a freshly-connected
// backend reports verbatim from SHOW search_path, but that PostgreSQL's boot
// default ("$user", public) never produces — so the assertion is meaningful.
const newSearchPath = "public, information_schema"

// resetDatabaseSearchPath clears the per-database search_path default so the
// shared cluster is left clean for other tests. Runs on its own connection with
// a background context so it still fires when the test's context is done.
func resetDatabaseSearchPath(db *sql.DB, dbName string) error {
	cc, err := db.Conn(context.Background())
	if err != nil {
		return err
	}
	defer cc.Close()
	_, err = cc.ExecContext(context.Background(),
		fmt.Sprintf("ALTER DATABASE %s RESET search_path", dbName))
	return err
}

// TestMultiGateway_ConnectionDefaultsRefreshedAfterAlterDatabase verifies that
// changing a per-database session GUC default via the multigateway is observed
// by already-open pooled backends. PostgreSQL applies pg_db_role_setting only at
// session start, so a pooled backend opened before the ALTER would keep the old
// default until it reconnects. The multigateway flags ALTER DATABASE ... SET
// (ExecuteOptions.InvalidatesConnectionDefaults) and the multipooler refreshes
// its pooled connections, so the next statement observes the new default.
//
// This is the non-PostGIS analogue of the PostGIS `totopogeom` failure, where
// CREATE EXTENSION postgis_topology runs ALTER DATABASE ... SET search_path
// internally and a stale pooled backend then resolved unqualified topology
// functions against the pre-extension search_path.
func TestMultiGateway_ConnectionDefaultsRefreshedAfterAlterDatabase(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctx := utils.WithTimeout(t, 120*time.Second)

	c, err := db.Conn(ctx)
	require.NoError(t, err)
	defer c.Close()

	var dbName string
	require.NoError(t, c.QueryRowContext(ctx, "SELECT current_database()").Scan(&dbName))
	require.NoError(t, resetDatabaseSearchPath(db, dbName))
	t.Cleanup(func() { require.NoError(t, resetDatabaseSearchPath(db, dbName)) })

	// Warm a pooled backend so at least one connection exists BEFORE the ALTER;
	// it caches PostgreSQL's session-start search_path and is therefore stale.
	_, err = c.ExecContext(ctx, "SELECT 1")
	require.NoError(t, err)

	var before string
	require.NoError(t, c.QueryRowContext(ctx, "SHOW search_path").Scan(&before))
	require.NotEqual(t, newSearchPath, before, "baseline search_path must differ for the assertion to be meaningful")

	// Autocommit ALTER DATABASE ... SET — durable immediately, so the bump fires
	// as soon as the statement completes.
	_, err = c.ExecContext(ctx, fmt.Sprintf("ALTER DATABASE %s SET search_path TO %s", dbName, newSearchPath))
	require.NoError(t, err)

	// A pooled backend opened before the ALTER must now report the new default.
	// Without the refresh it would keep the stale, pre-ALTER value.
	var after string
	require.NoError(t, c.QueryRowContext(ctx, "SHOW search_path").Scan(&after))
	require.Equal(t, newSearchPath, after,
		"pooled backend must reconnect and observe the new per-database search_path default")
}

// TestMultiGateway_ConnectionDefaultsRefreshedAfterAlterDatabaseInTransaction is
// the transaction-correct counterpart: ALTER DATABASE ... SET inside an explicit
// transaction is durable only at COMMIT, so the multipooler defers the refresh
// until then. After COMMIT, a pooled backend must observe the new default; a
// ROLLBACK (covered by unit tests) must not refresh, because the change is
// discarded. This guards the deferred (COMMIT-time) bump path.
func TestMultiGateway_ConnectionDefaultsRefreshedAfterAlterDatabaseInTransaction(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctx := utils.WithTimeout(t, 120*time.Second)

	c, err := db.Conn(ctx)
	require.NoError(t, err)
	defer c.Close()

	var dbName string
	require.NoError(t, c.QueryRowContext(ctx, "SELECT current_database()").Scan(&dbName))
	require.NoError(t, resetDatabaseSearchPath(db, dbName))
	t.Cleanup(func() { require.NoError(t, resetDatabaseSearchPath(db, dbName)) })

	_, err = c.ExecContext(ctx, "SELECT 1")
	require.NoError(t, err)

	_, err = c.ExecContext(ctx, "BEGIN")
	require.NoError(t, err)
	_, err = c.ExecContext(ctx, fmt.Sprintf("ALTER DATABASE %s SET search_path TO %s", dbName, newSearchPath))
	require.NoError(t, err)
	_, err = c.ExecContext(ctx, "COMMIT")
	require.NoError(t, err)

	var after string
	require.NoError(t, c.QueryRowContext(ctx, "SHOW search_path").Scan(&after))
	require.Equal(t, newSearchPath, after,
		"after COMMIT, pooled backends must observe the new per-database search_path default")
}

// TestMultiGateway_ConnectionDefaultsRefreshedWithHeldReservedConn verifies that
// a checked-out reserved backend (held for temp tables) is reconnected inline
// after a durable defaults change, without requiring the connection to be
// released back to the pool first.
func TestMultiGateway_ConnectionDefaultsRefreshedWithHeldReservedConn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctx := utils.WithTimeout(t, 120*time.Second)

	c, err := db.Conn(ctx)
	require.NoError(t, err)
	defer c.Close()

	var dbName string
	require.NoError(t, c.QueryRowContext(ctx, "SELECT current_database()").Scan(&dbName))
	require.NoError(t, resetDatabaseSearchPath(db, dbName))
	t.Cleanup(func() { require.NoError(t, resetDatabaseSearchPath(db, dbName)) })

	// Reserve a backend for the session (temp table reason).
	_, err = c.ExecContext(ctx, "CREATE TEMP TABLE connection_defaults_hold (x int)")
	require.NoError(t, err)

	_, err = c.ExecContext(ctx, "SELECT 1")
	require.NoError(t, err)

	var before string
	require.NoError(t, c.QueryRowContext(ctx, "SHOW search_path").Scan(&before))
	require.NotEqual(t, newSearchPath, before)

	_, err = c.ExecContext(ctx, fmt.Sprintf("ALTER DATABASE %s SET search_path TO %s", dbName, newSearchPath))
	require.NoError(t, err)

	var after string
	require.NoError(t, c.QueryRowContext(ctx, "SHOW search_path").Scan(&after))
	require.Equal(t, newSearchPath, after,
		"held reserved backend must reconnect inline and observe the new per-database search_path default")
}
