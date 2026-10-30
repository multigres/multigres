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
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestMultigateway_ForeignDataWrappers covers the foreign-data-wrapper DDL the
// gateway used to reject (CREATE FOREIGN DATA WRAPPER, CREATE SERVER) end to
// end through the pooler: a bare wrapper, postgres_fdw with the gateway itself
// as the remote (so the gateway is exercised as a postgres_fdw remote too:
// connect-time SETs, explicit remote transactions, DECLARE/FETCH cursors,
// catalog queries for IMPORT FOREIGN SCHEMA), and file_fdw reading a
// server-local CSV.
func TestMultigateway_ForeignDataWrappers(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping FDW tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping FDW tests")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 120*time.Second)

	connStr := shardsetup.GetTestUserDSN("localhost", setup.ClientPort(), "sslmode=disable")
	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx)

	exec := func(t *testing.T, sql string, args ...any) {
		t.Helper()
		_, err := conn.Exec(ctx, sql, args...)
		require.NoError(t, err, "exec %s", sql)
	}

	// Both contrib modules ship with every packaged PostgreSQL but not with a
	// bare source build, so skip rather than fail without them.
	var contrib int
	require.NoError(t, conn.QueryRow(ctx, "SELECT count(*) FROM pg_available_extensions WHERE name IN ('postgres_fdw', 'file_fdw')").Scan(&contrib))
	if contrib != 2 {
		t.Skip("postgres_fdw / file_fdw contrib modules not installed")
	}

	t.Run("bare wrapper", func(t *testing.T) {
		exec(t, "DROP FOREIGN DATA WRAPPER IF EXISTS fdw_e2e_dummy CASCADE")
		defer exec(t, "DROP FOREIGN DATA WRAPPER IF EXISTS fdw_e2e_dummy CASCADE")
		exec(t, "CREATE FOREIGN DATA WRAPPER fdw_e2e_dummy")

		var owner string
		err := conn.QueryRow(ctx, "SELECT fdwowner::regrole::text FROM pg_foreign_data_wrapper WHERE fdwname = 'fdw_e2e_dummy'").Scan(&owner)
		require.NoError(t, err)
		assert.Equal(t, shardsetup.DefaultTestUser, owner)

		exec(t, "CREATE SERVER fdw_e2e_dummy_srv FOREIGN DATA WRAPPER fdw_e2e_dummy OPTIONS (note 'no handler, catalog only')")
		exec(t, "ALTER SERVER fdw_e2e_dummy_srv OPTIONS (SET note 'altered')")
		exec(t, "DROP SERVER fdw_e2e_dummy_srv")
	})

	t.Run("postgres_fdw via the gateway", func(t *testing.T) {
		// Cleanup is registered first and is idempotent, so an aborted earlier
		// run on the shared cluster cannot wedge this one.
		cleanup := func() {
			exec(t, "DROP SCHEMA IF EXISTS fdw_e2e_imp CASCADE")
			exec(t, "DROP SERVER IF EXISTS fdw_e2e_gw CASCADE")
			exec(t, "DROP TABLE IF EXISTS fdw_e2e_local")
			exec(t, "DROP EXTENSION IF EXISTS postgres_fdw")
		}
		cleanup()
		defer cleanup()
		exec(t, "CREATE EXTENSION postgres_fdw")
		exec(t, "CREATE TABLE fdw_e2e_local (id int PRIMARY KEY, v text)")
		exec(t, "INSERT INTO fdw_e2e_local VALUES (1, 'a'), (2, 'b')")

		// The remote is the gateway: postgres_fdw's libpq session comes back in
		// through the front door as a second pooled client.
		exec(t, fmt.Sprintf(
			"CREATE SERVER fdw_e2e_gw FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host 'localhost', port '%d', dbname 'postgres', sslmode 'disable')",
			setup.ClientPort()))
		exec(t, fmt.Sprintf(
			"CREATE USER MAPPING FOR CURRENT_USER SERVER fdw_e2e_gw OPTIONS (user '%s', password '%s')",
			shardsetup.DefaultTestUser, shardsetup.TestPostgresPassword))
		exec(t, "CREATE FOREIGN TABLE fdw_e2e_remote (id int, v text) SERVER fdw_e2e_gw OPTIONS (table_name 'fdw_e2e_local')")

		var n int
		require.NoError(t, conn.QueryRow(ctx, "SELECT count(*) FROM fdw_e2e_remote").Scan(&n))
		assert.Equal(t, 2, n, "scan through the foreign table")

		// Extended protocol with a bound parameter, pushed down as a remote WHERE.
		var v string
		require.NoError(t, conn.QueryRow(ctx, "SELECT v FROM fdw_e2e_remote WHERE id = $1", 2).Scan(&v))
		assert.Equal(t, "b", v)

		// DML inside one explicit transaction: postgres_fdw opens a remote
		// transaction and commits it with ours.
		tx, err := conn.Begin(ctx)
		require.NoError(t, err)
		_, err = tx.Exec(ctx, "INSERT INTO fdw_e2e_remote VALUES (3, 'c')")
		require.NoError(t, err)
		_, err = tx.Exec(ctx, "UPDATE fdw_e2e_remote SET v = 'b2' WHERE id = 2")
		require.NoError(t, err)
		_, err = tx.Exec(ctx, "DELETE FROM fdw_e2e_remote WHERE id = 1")
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		rows, err := conn.Query(ctx, "SELECT id, v FROM fdw_e2e_local ORDER BY id")
		require.NoError(t, err)
		got, err := pgx.CollectRows(rows, pgx.RowToStructByPos[struct {
			ID int
			V  string
		}])
		require.NoError(t, err)
		require.Len(t, got, 2)
		assert.Equal(t, 2, got[0].ID)
		assert.Equal(t, "b2", got[0].V)
		assert.Equal(t, 3, got[1].ID)
		assert.Equal(t, "c", got[1].V)

		// A rolled-back local transaction rolls the remote one back too.
		tx, err = conn.Begin(ctx)
		require.NoError(t, err)
		_, err = tx.Exec(ctx, "DELETE FROM fdw_e2e_remote")
		require.NoError(t, err)
		require.NoError(t, tx.Rollback(ctx))
		require.NoError(t, conn.QueryRow(ctx, "SELECT count(*) FROM fdw_e2e_local").Scan(&n))
		assert.Equal(t, 2, n, "rollback reached the remote")

		// IMPORT FOREIGN SCHEMA runs catalog queries against the remote (the
		// gateway) and creates the foreign table under the remote's name.
		exec(t, "CREATE SCHEMA fdw_e2e_imp")
		exec(t, "IMPORT FOREIGN SCHEMA public LIMIT TO (fdw_e2e_local) FROM SERVER fdw_e2e_gw INTO fdw_e2e_imp")
		require.NoError(t, conn.QueryRow(ctx, "SELECT count(*) FROM fdw_e2e_imp.fdw_e2e_local").Scan(&n))
		assert.Equal(t, 2, n, "imported foreign table reads the remote rows")
	})

	t.Run("file_fdw", func(t *testing.T) {
		// The backend runs on this machine, so a temp file is server-local.
		path := filepath.Join(t.TempDir(), "rows.csv")
		require.NoError(t, os.WriteFile(path, []byte("1,one\n2,two\n3,three\n"), 0o644))

		cleanup := func() {
			exec(t, "DROP SERVER IF EXISTS fdw_e2e_files CASCADE")
			exec(t, "DROP EXTENSION IF EXISTS file_fdw")
		}
		cleanup()
		defer cleanup()
		exec(t, "CREATE EXTENSION file_fdw")
		exec(t, "CREATE SERVER fdw_e2e_files FOREIGN DATA WRAPPER file_fdw")
		exec(t, fmt.Sprintf("CREATE FOREIGN TABLE fdw_e2e_csv (a int, b text) SERVER fdw_e2e_files OPTIONS (filename '%s', format 'csv')", path))

		var sum int
		require.NoError(t, conn.QueryRow(ctx, "SELECT sum(a) FROM fdw_e2e_csv").Scan(&sum))
		assert.Equal(t, 6, sum)
		var b string
		require.NoError(t, conn.QueryRow(ctx, "SELECT b FROM fdw_e2e_csv WHERE a = $1", 3).Scan(&b))
		assert.Equal(t, "three", b)

		// file_fdw's program option runs a shell command on the server; the
		// gateway refuses it like COPY ... PROGRAM.
		_, err := conn.Exec(ctx, "CREATE FOREIGN TABLE fdw_e2e_prog (l text) SERVER fdw_e2e_files OPTIONS (program 'id')")
		var pgErr *pgconn.PgError
		require.ErrorAs(t, err, &pgErr)
		assert.Equal(t, "0A000", pgErr.Code)
		_, err = conn.Exec(ctx, "ALTER FOREIGN TABLE fdw_e2e_csv OPTIONS (ADD program 'id')")
		require.ErrorAs(t, err, &pgErr)
		assert.Equal(t, "0A000", pgErr.Code)
	})
}
