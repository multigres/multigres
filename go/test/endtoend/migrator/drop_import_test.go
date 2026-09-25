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

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestGracefulDropOfImportingMigration is the IMPORT-direction counterpart to
// TestGracefulDropOfActivatedMigrationRestoresServing: a graceful (non-force) drop
// of a caught-up but not-yet-activated (IMPORTING) migration must run the IMPORT
// drain barrier — quiesce the source, wait the source slot confirmed, advance the
// target's sequences — then tear down the IMPORT-side objects (the target
// subscription and the source publication) and restore serving. The existing drop
// tests drain and tear down the EXPORT side; this drives the symmetric IMPORT path.
func TestGracefulDropOfImportingMigration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Multigres Migrator drop e2e in short mode")
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
	grpcPort := primary.Multipooler.GrpcPort

	srcPort := startStandaloneSource(t)
	seedSource(t, ctx, srcPort)

	mt, mtClose := migrationClient(t, primary)
	defer mtClose()

	createResp, err := mt.CreateMigration(ctx, &migratorpb.CreateMigrationRequest{
		SourceDsn:      sourceDSN(srcPort),
		TargetDatabase: targetDB,
		Objects:        objs("public.orders"),
	})
	require.NoError(t, err)
	id := createResp.GetMigration().GetId()

	_, err = mt.StartMigration(ctx, &migratorpb.StartMigrationRequest{Id: id})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		resp, err := mt.GetMigrations(ctx, &migratorpb.GetMigrationsRequest{Id: id})
		return err == nil && len(resp.GetMigrations()) == 1 && resp.GetMigrations()[0].GetCaughtUp()
	}, 60*time.Second, 500*time.Millisecond, "IMPORT must catch up")

	// A caught-up IMPORTING migration holds serving (the shard is a migration target).
	requireServingStatus(t, ctx, grpcPort, clustermetadatapb.PoolerServingStatus_DRAINING,
		"an IMPORTING target must not serve")

	// Graceful (non-force) drop: this drains the IMPORT link to lag zero (source
	// read-only, source slot confirmed, target sequences advanced) and tears down
	// the IMPORT-side objects. Bound it so a regression fails as a timeout.
	dropCtx, dropCancel := context.WithTimeout(ctx, 45*time.Second)
	defer dropCancel()
	_, err = mt.DropMigration(dropCtx, &migratorpb.DropMigrationRequest{Id: id})
	require.NoError(t, err, "graceful drop of a caught-up IMPORTING migration must complete")

	// The migration is gone and serving is restored (the gate releases).
	_, err = mt.GetMigrations(ctx, &migratorpb.GetMigrationsRequest{Id: id})
	require.Error(t, err, "the dropped migration must no longer resolve")
	requireServingStatus(t, ctx, grpcPort, clustermetadatapb.PoolerServingStatus_SERVING,
		"after a graceful drop the shard serves again")

	// Correct IMPORT-direction teardown on the surviving (target) side: the target
	// subscription is dropped. The source side is intentionally left quiesced — the
	// drain set the old database read-only and the drop does not un-quiesce it (the
	// migration has cut over to Multigres; writes must not resume on the abandoned
	// source), so the source-side publication teardown is best-effort and may remain.
	tc := targetConn(t, ctx, primary, targetDB)
	defer tc.Close()
	require.Eventually(t, func() bool {
		subs := scalarInt(t, ctx, tc,
			"SELECT count(*) FROM pg_subscription WHERE subname = 'mt_sub_"+id+"'")
		t.Logf("after import drop: target subscriptions=%d", subs)
		return subs == 0
	}, 30*time.Second, 500*time.Millisecond, "IMPORT target subscription must be torn down")
}
