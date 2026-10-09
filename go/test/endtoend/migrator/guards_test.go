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

	"github.com/stretchr/testify/require"

	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestMigrationDirectionIdempotence drives SetMigrationDirection's two
// self-guarding, no-error cases once a migration is EXPORTING: calling it
// again with IMPORT is a valid rollback (not rejected — there is no separate
// IMPORT-only "start" verb anymore), and calling it again with EXPORT (the
// current direction) is a no-op.
func TestMigrationDirectionIdempotence(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Multigres Migrator e2e in short mode")
	}
	if !utils.HasPostgreSQLBinaries() {
		t.Skip("PostgreSQL client/server binaries not on PATH")
	}
	ctx := t.Context()

	setup, cleanup := shardsetup.NewIsolated(
		t,
		shardsetup.WithMultipoolerCount(2),
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

	id := activatedMigration(t, ctx, mt, srcPort, targetDB)

	// Setting EXPORT again on an already-EXPORTING migration is a no-op.
	noopResp, err := mt.SetMigrationDirection(ctx, &migratorpb.SetMigrationDirectionRequest{Ref: idRef(id), Direction: migratorpb.MigrationDirection_MIGRATION_DIRECTION_EXPORT})
	require.NoError(t, err, "setting the current direction again must be a no-op, not an error")
	require.Equal(t, migratorpb.MigrationDirection_MIGRATION_DIRECTION_EXPORT, noopResp.GetStatus().GetActiveDirection())

	// Setting IMPORT on an EXPORTING migration is the rollback, not rejected —
	// there is no separate IMPORT-only "start" verb to guard against.
	rollbackResp, err := mt.SetMigrationDirection(ctx, &migratorpb.SetMigrationDirectionRequest{Ref: idRef(id), Direction: migratorpb.MigrationDirection_MIGRATION_DIRECTION_IMPORT})
	require.NoError(t, err, "setting IMPORT on an EXPORTING migration must roll back, not error")
	require.Equal(t, migratorpb.MigrationDirection_MIGRATION_DIRECTION_IMPORT, rollbackResp.GetStatus().GetActiveDirection())

	_, err = mt.DropMigration(ctx, &migratorpb.DropMigrationRequest{Ref: idRef(id), Force: true})
	require.NoError(t, err)
}
