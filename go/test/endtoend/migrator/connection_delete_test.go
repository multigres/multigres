// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package migrator

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/pgprotocol/client"
	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

func connIDRef(id int64) *migratorpb.ConnectionRef {
	return &migratorpb.ConnectionRef{Ref: &migratorpb.ConnectionRef_Id{Id: id}}
}

// TestDropConnectionAgainstPostgres runs DropConnection's single statement on a real
// catalog shard: a connection nothing references is deleted, one a migration references is
// refused with FAILED_PRECONDITION and stays, and a missing one is NOT_FOUND. The last case
// is a migration inserted by another session while the delete runs: the delete waits for that
// transaction's key-share lock on the connection row, the statement's own reference check
// does not see the insert, and the foreign key refuses the delete once the insert commits.
// That is reported as the same FAILED_PRECONDITION, and the connection stays.
func TestDropConnectionAgainstPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Multigres Migrator connection e2e in short mode")
	}
	if !utils.HasPostgreSQLBinaries() {
		t.Skip("PostgreSQL client/server binaries not on PATH")
	}
	ctx := t.Context()

	setup, cleanup := shardsetup.NewIsolated(t, shardsetup.WithMultipoolerCount(2))
	defer cleanup()
	primary := setup.GetPrimary(t)
	shardsetup.WaitForManagerReady(t, primary.Multipooler)
	targetDB := waitForPrimaryDB(t, ctx, setup)

	mt, mtClose := migrationClient(t, primary)
	defer mtClose()
	create := func(name string) int64 {
		t.Helper()
		resp, err := mt.CreateConnection(ctx, &migratorpb.CreateConnectionRequest{
			Connection: &migratorpb.Connection{Name: name, Dsn: "host=127.0.0.1 port=1 dbname=app"},
		})
		require.NoError(t, err)
		return resp.GetConnection().GetId()
	}
	exists := func(id int64) bool {
		t.Helper()
		_, err := mt.GetConnection(ctx, &migratorpb.GetConnectionRequest{Ref: connIDRef(id)})
		if err != nil {
			require.Equal(t, codes.NotFound, status.Code(err), "unexpected error: %v", err)
		}
		return err == nil
	}

	db := targetConn(t, ctx, primary, targetDB)
	defer db.Close()
	insertMigration := func(c *client.Conn, id int64, connID int64) {
		t.Helper()
		_, err := c.Query(ctx, "INSERT INTO multigres.migration (migration_id, migration_phase, migration_target, connection_id) "+
			"VALUES ("+strconv.FormatInt(id, 10)+", 'EXPORTING', ROW('"+targetDB+"', 'default', '0-inf')::multigres.shard_ref, "+
			strconv.FormatInt(connID, 10)+")")
		require.NoError(t, err)
	}
	deleteMigration := func(id int64) {
		t.Helper()
		_, err := db.Query(ctx, "DELETE FROM multigres.migration WHERE migration_id = "+strconv.FormatInt(id, 10))
		require.NoError(t, err)
	}

	t.Run("unreferenced is deleted", func(t *testing.T) {
		id := create("drop_unreferenced")
		_, err := mt.DropConnection(ctx, &migratorpb.DropConnectionRequest{Ref: connIDRef(id)})
		require.NoError(t, err)
		require.False(t, exists(id))
	})

	t.Run("missing is not found", func(t *testing.T) {
		_, err := mt.DropConnection(ctx, &migratorpb.DropConnectionRequest{Ref: connIDRef(987654321)})
		require.Equal(t, codes.NotFound, status.Code(err), "got %v", err)
	})

	t.Run("referenced is refused and stays", func(t *testing.T) {
		id := create("drop_referenced")
		insertMigration(db, 9001, id)
		_, err := mt.DropConnection(ctx, &migratorpb.DropConnectionRequest{Ref: connIDRef(id)})
		require.Equal(t, codes.FailedPrecondition, status.Code(err), "got %v", err)
		require.True(t, exists(id))

		deleteMigration(9001)
		_, err = mt.DropConnection(ctx, &migratorpb.DropConnectionRequest{Ref: connIDRef(id)})
		require.NoError(t, err, "once nothing references it the connection can go")
		require.False(t, exists(id))
	})

	t.Run("a reference committed while the delete waits is refused", func(t *testing.T) {
		id := create("drop_raced")
		writer := targetConn(t, ctx, primary, targetDB)
		defer writer.Close()
		_, err := writer.Query(ctx, "BEGIN")
		require.NoError(t, err)
		insertMigration(writer, 9002, id) // holds a key-share lock on the connection row

		dropped := make(chan error, 1)
		go func() {
			_, err := mt.DropConnection(ctx, &migratorpb.DropConnectionRequest{Ref: connIDRef(id)})
			dropped <- err
		}()

		// The delete is blocked on the writer's lock: wait until postgres says so, not for a
		// fixed time, so the commit below always lands while the delete is waiting.
		require.Eventually(t, func() bool {
			res, err := db.Query(ctx, "SELECT count(*) FROM pg_stat_activity WHERE pid <> pg_backend_pid() "+
				"AND wait_event_type = 'Lock' AND query LIKE '%DELETE FROM multigres.connections%'")
			return err == nil && len(res) > 0 && len(res[0].Rows) > 0 && string(res[0].Rows[0].Values[0]) == "1"
		}, 30*time.Second, 50*time.Millisecond, "the delete must be waiting on the writer's lock")
		select {
		case err := <-dropped:
			t.Fatalf("the delete returned while the writer's transaction was open: %v", err)
		default:
		}

		_, err = writer.Query(ctx, "COMMIT")
		require.NoError(t, err)
		select {
		case err := <-dropped:
			require.Equal(t, codes.FailedPrecondition, status.Code(err), "got %v", err)
		case <-time.After(30 * time.Second):
			t.Fatal("the delete did not finish after the writer committed")
		}
		require.True(t, exists(id), "the refused delete leaves the connection")

		deleteMigration(9002)
		_, err = mt.DropConnection(ctx, &migratorpb.DropConnectionRequest{Ref: connIDRef(id)})
		require.NoError(t, err)
	})
}
