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
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// setReadOnly flips the database's topo read-only flags the way multiadmin
// SetDatabaseReadOnly does, and waits until the gateway has picked it up: an
// autocommit INSERT is rejected with 25006 while read-only, accepted otherwise.
func setReadOnly(t *testing.T, ctx context.Context, setup *shardsetup.ShardSetup, readOnly, force bool) {
	t.Helper()
	require.NoError(t, setup.TopoServer.UpdateDatabaseFields(ctx, constants.DefaultPostgresDatabase, func(db *clustermetadatapb.Database) error {
		db.ReadOnly, db.ReadOnlyForce = readOnly, readOnly && force
		return nil
	}))
	probe := connectPgx(t, ctx, setup)
	defer probe.Close(ctx)
	require.Eventually(t, func() bool {
		_, err := probe.Exec(ctx, "INSERT INTO read_only_probe VALUES (1)")
		return readOnly == (sqlState(err) == "25006")
	}, 10*time.Second, 50*time.Millisecond, "gateway did not observe read_only=%v", readOnly)
}

func sqlState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

func TestMultigateway_ReadOnlyMode(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping read-only mode test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping read-only mode test")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)
	ctx := utils.WithTimeout(t, 120*time.Second)

	admin := connectPgx(t, ctx, setup)
	defer admin.Close(ctx)
	_, err := admin.Exec(ctx, "CREATE TABLE read_only_probe (id int)")
	require.NoError(t, err)
	_, err = admin.Exec(ctx, "CREATE FUNCTION read_only_probe_insert() RETURNS void LANGUAGE sql AS 'INSERT INTO read_only_probe VALUES (2)'")
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		setReadOnly(t, cleanupCtx, setup, false, false)
		_, _ = admin.Exec(cleanupCtx, "DROP FUNCTION IF EXISTS read_only_probe_insert()")
		_, _ = admin.Exec(cleanupCtx, "DROP TABLE IF EXISTS read_only_probe")
	})

	setReadOnly(t, ctx, setup, true, false)

	t.Run("reads work, writes are rejected by postgres", func(t *testing.T) {
		conn := connectPgx(t, ctx, setup)
		defer conn.Close(ctx)

		var n int
		require.NoError(t, conn.QueryRow(ctx, "SELECT count(*) FROM read_only_probe").Scan(&n))

		for _, sql := range []string{
			"INSERT INTO read_only_probe VALUES (1)",
			"SELECT read_only_probe_insert()", // a write hidden in a function
			"WITH w AS (INSERT INTO read_only_probe VALUES (3) RETURNING id) SELECT * FROM w",
			"CREATE TABLE read_only_probe_2 (id int)",
		} {
			_, err := conn.Exec(ctx, sql)
			assert.Equal(t, "25006", sqlState(err), "%s: %v", sql, err)
		}
		// The session is still usable afterwards.
		require.NoError(t, conn.QueryRow(ctx, "SELECT 1").Scan(&n))
	})

	t.Run("session-level overrides are refused by the gateway", func(t *testing.T) {
		for _, sql := range []string{
			"SET default_transaction_read_only = off",
			"SET transaction_read_only = off",
			"SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE",
			"RESET default_transaction_read_only",
			"BEGIN READ WRITE",
		} {
			for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeSimpleProtocol, pgx.QueryExecModeExec} {
				conn := connectPgx(t, ctx, setup)
				_, err := conn.Exec(ctx, sql, mode)
				assert.Equal(t, "25006", sqlState(err), "%s (%v): %v", sql, mode, err)
				// Refused before reaching postgres: the session stays read-only.
				_, err = conn.Exec(ctx, "INSERT INTO read_only_probe VALUES (4)")
				assert.Equal(t, "25006", sqlState(err), "after %s (%v): %v", sql, mode, err)
				conn.Close(ctx)
			}
		}

		conn := connectPgx(t, ctx, setup)
		defer conn.Close(ctx)
		_, err := conn.Exec(ctx, "BEGIN")
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "SET TRANSACTION READ WRITE")
		assert.Equal(t, "25006", sqlState(err))
		_, err = conn.Exec(ctx, "ROLLBACK")
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "SET transaction_read_only = on")
		require.NoError(t, err, "keeping the session read-only is allowed")
	})

	t.Run("force terminates open transactions", func(t *testing.T) {
		// Start the transaction while still read-write, so the backend's
		// transaction is genuinely read-write: exactly the session that plain
		// read-only mode cannot reach.
		setReadOnly(t, ctx, setup, false, false)
		inTx := connectPgx(t, ctx, setup)
		defer inTx.Close(ctx)
		_, err := inTx.Exec(ctx, "BEGIN")
		require.NoError(t, err)
		_, err = inTx.Exec(ctx, "INSERT INTO read_only_probe VALUES (5)")
		require.NoError(t, err)

		idle := connectPgx(t, ctx, setup)
		defer idle.Close(ctx)

		setReadOnly(t, ctx, setup, true, false)
		_, err = inTx.Exec(ctx, "INSERT INTO read_only_probe VALUES (6)")
		require.NoError(t, err, "without force an open read-write transaction keeps writing")

		setReadOnly(t, ctx, setup, true, true)
		_, err = inTx.Exec(ctx, "INSERT INTO read_only_probe VALUES (7)")
		require.Error(t, err)
		assert.True(t, inTx.IsClosed() || sqlState(err) == "57P01", "expected admin_shutdown, got %v", err)

		var n int
		require.NoError(t, idle.QueryRow(ctx, "SELECT 1").Scan(&n), "idle sessions outside a transaction survive force")

		setReadOnly(t, ctx, setup, false, false)
		require.NoError(t, admin.QueryRow(ctx, "SELECT count(*) FROM read_only_probe WHERE id IN (5, 6, 7)").Scan(&n))
		assert.Equal(t, 0, n, "the terminated transaction rolled back")
	})

	t.Run("lifting the mode restores writes", func(t *testing.T) {
		setReadOnly(t, ctx, setup, false, false)
		conn := connectPgx(t, ctx, setup)
		defer conn.Close(ctx)
		_, err := conn.Exec(ctx, "INSERT INTO read_only_probe VALUES (8)")
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "BEGIN READ WRITE")
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "ROLLBACK")
		require.NoError(t, err)
	})
}
