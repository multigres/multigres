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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/client"
	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestDDLReplication exercises the experimental DDL-replication path (ddlrepl.go):
// table-modification DDL on the source rides the same subscription and is replayed
// on the target by the apply trigger. It deliberately mixes schema-qualified and
// UNQUALIFIED statements — the unqualified ones are the regression guard: the
// apply worker runs with an empty search_path, so before the fix an unqualified
// name failed to resolve, the apply worker retried forever, and the whole
// subscription (data included) stalled. The test asserts the target schema
// converges for both forms and that streaming keeps flowing afterwards.
func TestDDLReplication(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Multigres Migrator e2e in short mode")
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

	srcPort := startStandaloneSource(t)
	seedSource(t, ctx, srcPort)

	mt, mtClose := migrationClient(t, primary)
	defer mtClose()

	createResp, err := mt.CreateMigration(ctx, &migratorpb.CreateMigrationRequest{
		SourceDsn:      sourceDSN(srcPort),
		TargetDatabase: targetDB,
		Tables:         []string{"public.orders"},
	})
	require.NoError(t, err)
	id := createResp.GetMigration().GetId()

	startResp, err := mt.StartMigration(ctx, &migratorpb.StartMigrationRequest{Id: id})
	require.NoError(t, err, "start; last_error=%s", startResp.GetMigration().GetLastError())

	require.Eventually(t, func() bool {
		resp, err := mt.GetMigrations(ctx, &migratorpb.GetMigrationsRequest{Id: id})
		if err != nil || len(resp.GetMigrations()) == 0 {
			return false
		}
		return resp.GetMigrations()[0].GetCaughtUp()
	}, 60*time.Second, 500*time.Millisecond, "migration must catch up")

	tc := targetConn(t, ctx, primary, targetDB)
	defer tc.Close()
	require.Eventually(t, func() bool {
		n, ok := countRows(t, ctx, tc, "public.orders")
		return ok && n == 3
	}, 30*time.Second, 500*time.Millisecond, "initial rows must land")
	require.Equal(t, "id,v", columnsOf(t, ctx, tc, "public", "orders"), "baseline target columns")

	sc := dialSource(t, ctx, srcPort)
	defer sc.Close()

	// 1) Schema-qualified ADD COLUMN replicates.
	_, err = sc.Query(ctx, "ALTER TABLE public.orders ADD COLUMN note text")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return columnsOf(t, ctx, tc, "public", "orders") == "id,v,note"
	}, 30*time.Second, 500*time.Millisecond, "qualified ADD COLUMN must replicate")

	// 2) UNQUALIFIED ADD COLUMN replicates — regression guard for the empty
	// search_path in the apply worker (this is what previously stalled the stream).
	_, err = sc.Query(ctx, "ALTER TABLE orders ADD COLUMN memo text")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return columnsOf(t, ctx, tc, "public", "orders") == "id,v,note,memo"
	}, 30*time.Second, 500*time.Millisecond, "unqualified ADD COLUMN must replicate (search_path fix)")

	// 3) UNQUALIFIED DROP COLUMN replicates — the exact statement that froze the demo.
	_, err = sc.Query(ctx, "ALTER TABLE orders DROP COLUMN note")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return columnsOf(t, ctx, tc, "public", "orders") == "id,v,memo"
	}, 30*time.Second, 500*time.Millisecond, "unqualified DROP COLUMN must replicate")

	// 4) Streaming still flows after the DDL — the subscription was never stalled.
	_, err = sc.Query(ctx, "INSERT INTO public.orders (v, memo) VALUES ('d', 'x')")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		n, ok := countRows(t, ctx, tc, "public.orders")
		return ok && n == 4
	}, 30*time.Second, 500*time.Millisecond, "streaming must continue after DDL")

	// 5) DDL on a table that is NOT part of the migration must NOT reach the
	// target: it has no ddl_capture_tables membership, so capture_ddl skips it and
	// nothing rides the stream. Create and ALTER an unrelated table on the source,
	// then push a migrated-table write AFTER it and wait for that write to land —
	// so the unrelated DDL has had at least as long to (wrongly) replicate.
	for _, stmt := range []string{
		"CREATE TABLE public.untracked (id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, x text)",
		"ALTER TABLE public.untracked ADD COLUMN y text",
	} {
		_, err = sc.Query(ctx, stmt)
		require.NoError(t, err, stmt)
	}
	_, err = sc.Query(ctx, "INSERT INTO public.orders (v, memo) VALUES ('e', 'x2')")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		n, ok := countRows(t, ctx, tc, "public.orders")
		return ok && n == 5
	}, 30*time.Second, 500*time.Millisecond, "later migrated write must land (barrier)")
	require.Equal(t, "", columnsOf(t, ctx, tc, "public", "untracked"),
		"DDL on a non-migrated table must not reach the target")
}

// columnsOf returns the comma-joined column names of a table (ordinal order) via a
// pgprotocol connection, so an added/dropped column is observable as it replicates.
func columnsOf(t *testing.T, ctx context.Context, conn *client.Conn, schema, table string) string {
	t.Helper()
	res, err := conn.Query(ctx,
		"SELECT string_agg(column_name, ',' ORDER BY ordinal_position) "+
			"FROM information_schema.columns WHERE table_schema = '"+schema+"' AND table_name = '"+table+"'")
	if err != nil || len(res) == 0 || len(res[0].Rows) == 0 {
		return ""
	}
	return string(res[0].Rows[0].Values[0])
}
