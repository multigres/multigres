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

// TestSetMigrationDirection drives the symmetric switch: after an IMPORT catches
// up, set EXPORT and prove a write on the Multigres target replicates out to the
// old database; then set IMPORT and prove a write on the old database replicates
// back in; then prove setting the current direction is a no-op.
func TestSetMigrationDirection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Multigres Migrator switch e2e in short mode")
	}
	if !utils.HasPostgreSQLBinaries() {
		t.Skip("PostgreSQL client/server binaries not on PATH")
	}
	ctx := t.Context()

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(2),
		// EXPORT makes the Multigres target the logical publisher, which needs
		// wal_level=logical (and the non-temporary-slot admission) that
		// slot-based replication turns on — matching targetConnInfo's guardrail.
		shardsetup.WithMultipoolerExtraArgs("--enable-slot-based-replication=true"),
	)
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
	_, err = mt.StartMigration(ctx, &migratorpb.StartMigrationRequest{Id: id})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		resp, err := mt.GetMigrations(ctx, &migratorpb.GetMigrationsRequest{Id: id})
		return err == nil && len(resp.GetMigrations()) == 1 && resp.GetMigrations()[0].GetCaughtUp()
	}, 60*time.Second, 500*time.Millisecond, "IMPORT must catch up")

	tc := targetConn(t, ctx, primary, targetDB)
	defer tc.Close()

	// Switch to EXPORT: Multigres becomes the source of truth.
	exportResp, err := mt.SetMigrationDirection(ctx, &migratorpb.SetMigrationDirectionRequest{
		Id: id, Direction: migratorpb.MigrationDirection_MIGRATION_DIRECTION_EXPORT,
	})
	require.NoError(t, err)
	require.Equal(t, migratorpb.MigrationDirection_MIGRATION_DIRECTION_EXPORT, exportResp.GetMigration().GetActiveDirection())

	// A write on the Multigres target must reach the old database.
	_, err = tc.Query(ctx, "INSERT INTO public.orders (v) VALUES ('x')")
	require.NoError(t, err, "target must be write-safe in EXPORT (sequence advanced)")
	require.Eventually(t, func() bool {
		sc := dialSource(t, ctx, srcPort)
		defer sc.Close()
		n, ok := countRows(t, ctx, sc, "public.orders")
		t.Logf("source count after EXPORT write=%d ok=%v", n, ok)
		return ok && n == 4
	}, 30*time.Second, 500*time.Millisecond, "EXPORT: target write must reach the old database")

	// Switch back to IMPORT: the old database is the source of truth again.
	importResp, err := mt.SetMigrationDirection(ctx, &migratorpb.SetMigrationDirectionRequest{
		Id: id, Direction: migratorpb.MigrationDirection_MIGRATION_DIRECTION_IMPORT,
	})
	require.NoError(t, err)
	require.Equal(t, migratorpb.MigrationDirection_MIGRATION_DIRECTION_IMPORT, importResp.GetMigration().GetActiveDirection())

	// A write on the old database must reach the Multigres target again.
	sc := dialSource(t, ctx, srcPort)
	_, err = sc.Query(ctx, "INSERT INTO public.orders (v) VALUES ('y')")
	require.NoError(t, err)
	_ = sc.Close()
	require.Eventually(t, func() bool {
		n, ok := countRows(t, ctx, tc, "public.orders")
		t.Logf("target count after IMPORT write=%d ok=%v", n, ok)
		return ok && n == 5
	}, 30*time.Second, 500*time.Millisecond, "IMPORT: old-database write must reach the target")

	// Setting the current direction is a no-op.
	noop, err := mt.SetMigrationDirection(ctx, &migratorpb.SetMigrationDirectionRequest{
		Id: id, Direction: migratorpb.MigrationDirection_MIGRATION_DIRECTION_IMPORT,
	})
	require.NoError(t, err)
	require.Equal(t, migratorpb.MigrationDirection_MIGRATION_DIRECTION_IMPORT, noop.GetMigration().GetActiveDirection())

	_, err = mt.DropMigration(ctx, &migratorpb.DropMigrationRequest{Id: id, Force: true})
	require.NoError(t, err)
}
