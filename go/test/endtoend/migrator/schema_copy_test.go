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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestSchemaCopyDropsExistingTargetTable verifies that the schema-copy step drops
// a table that already exists on the target before applying the source schema, so
// a migration does not fail with "relation already exists" (e.g. re-running after
// a partial migration). The target is pre-seeded with a leftover, incompatible
// `public.orders`; a normal import (schema copy NOT skipped) must drop it,
// recreate it from the source schema, and stream the rows in.
func TestSchemaCopyDropsExistingTargetTable(t *testing.T) {
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
	seedSource(t, ctx, srcPort) // source public.orders: (id identity PK, v text) + 3 rows

	// Pre-create a leftover, incompatible public.orders on the target. Without the
	// pre-copy drop, the schema apply (CREATE TABLE public.orders …) would fail.
	tc := targetConn(t, ctx, primary, targetDB)
	defer tc.Close()
	_, err := tc.Query(ctx, "CREATE TABLE public.orders (id bigint PRIMARY KEY, junk integer NOT NULL)")
	require.NoError(t, err)

	mt, mtClose := migrationClient(t, primary)
	defer mtClose()

	createResp, err := mt.CreateMigration(ctx, &migratorpb.CreateMigrationRequest{
		SourceDsn:      sourceDSN(srcPort),
		TargetDatabase: targetDB,
		Objects:        objs("public.orders"),
		// copy_data defaults true; schema copy is NOT skipped, so it must drop first.
	})
	require.NoError(t, err)
	id := createResp.GetMigration().GetId()

	startResp, err := mt.StartMigration(ctx, &migratorpb.StartMigrationRequest{Id: id})
	require.NoError(t, err, "start (schema copy must drop the pre-existing target table); last_error=%s",
		startResp.GetMigration().GetLastError())

	require.Eventually(t, func() bool {
		resp, err := mt.GetMigrations(ctx, &migratorpb.GetMigrationsRequest{Id: id})
		if err != nil || len(resp.GetMigrations()) == 0 {
			return false
		}
		m := resp.GetMigrations()[0]
		require.Empty(t, m.GetLastError(), "migration must not error on a pre-existing target table")
		return m.GetCaughtUp()
	}, 60*time.Second, 500*time.Millisecond, "migration must catch up after dropping the pre-existing table")

	// The table was recreated from the source schema (the leftover `junk` column is
	// gone, the source `v` column is present) and the 3 rows landed.
	require.Eventually(t, func() bool {
		n, ok := countRows(t, ctx, tc, "public.orders")
		return ok && n == 3
	}, 30*time.Second, 500*time.Millisecond, "the 3 source rows must land in the recreated table")
	_, err = tc.Query(ctx, "SELECT v FROM public.orders LIMIT 1")
	require.NoError(t, err, "recreated table must have the source's schema (column v), not the leftover")
}
