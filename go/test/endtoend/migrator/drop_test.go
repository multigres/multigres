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
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/client"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestGracefulDropOfActivatedMigrationRestoresServing is the regression test for
// the DropMigration direction-loss bug. A graceful (non-force) drop overwrote the
// phase to COMPLETING before draining and tearing down, and directionOf(COMPLETING)
// falls through to the IMPORT default — so dropping an activated (EXPORTING)
// migration ran the IMPORT teardown path: it waited on a source-side slot that does
// not exist for an export and hung until the caller's deadline (DeadlineExceeded).
// Worse, the COMPLETING phase tripped the serving gate, so the shard was stranded
// non-serving (the gateway reported MTF01 "planned failover in progress") with no
// recovery. This drives the exact sequence and asserts the graceful drop completes,
// restores serving, and tears down the EXPORT-side objects (proving it took the
// correct, export, direction rather than the import one).
func TestGracefulDropOfActivatedMigrationRestoresServing(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Multigres Migrator drop e2e in short mode")
	}
	if !utils.HasPostgreSQLBinaries() {
		t.Skip("PostgreSQL client/server binaries not on PATH")
	}
	ctx := t.Context()

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(2),
		shardsetup.WithMultipoolerExtraArgs("--enable-slot-based-replication=true"),
	)
	defer cleanup()
	primary := setup.GetPrimary(t)
	shardsetup.WaitForManagerReady(t, primary.Multipooler)
	targetDB := waitForPrimaryDB(t, ctx, setup)
	grpcPort := primary.Multipooler.GrpcPort

	srcPort := startStandaloneSource(t)
	seedSource(t, ctx, srcPort)

	mt, mtClose := migrationClient(t, primary)
	defer mtClose()

	id := activatedMigration(t, ctx, mt, srcPort, targetDB)
	requireServingStatus(t, ctx, grpcPort, clustermetadatapb.PoolerServingStatus_SERVING,
		"activate (EXPORT) must make the target serve")

	// Graceful (non-force) drop of the activated (EXPORTING) migration. Before the
	// fix this hung on the wrong (import) drain and never returned; bound it so a
	// regression fails as a timeout rather than hanging the whole suite.
	dropCtx, dropCancel := context.WithTimeout(ctx, 45*time.Second)
	defer dropCancel()
	_, err := mt.DropMigration(dropCtx, &migratorpb.DropMigrationRequest{Id: id})
	require.NoError(t, err, "graceful drop of an EXPORTING migration must complete, not hang on the wrong drain")

	// The migration is gone.
	_, err = mt.GetMigrations(ctx, &migratorpb.GetMigrationsRequest{Id: id})
	require.Error(t, err, "the dropped migration must no longer resolve")

	// Serving is restored: the gate releases once the row is deleted.
	requireServingStatus(t, ctx, grpcPort, clustermetadatapb.PoolerServingStatus_SERVING,
		"after a graceful drop the shard serves again")

	// Correct-direction teardown: the EXPORT-side objects on the target — the
	// publication and the pre-created reverse slot — are gone. The bug's IMPORT path
	// would have tried to drop a target *subscription* instead and left these behind.
	tc := targetConn(t, ctx, primary, targetDB)
	defer tc.Close()
	require.Eventually(t, func() bool {
		pubs := scalarInt(t, ctx, tc,
			"SELECT count(*) FROM pg_publication WHERE pubname = 'mt_pub_"+id+"'")
		slots := scalarInt(t, ctx, tc,
			"SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'mt_sub_"+id+"'")
		t.Logf("after drop: target publications=%d reverse slots=%d", pubs, slots)
		return pubs == 0 && slots == 0
	}, 30*time.Second, 500*time.Millisecond, "EXPORT-side publication and reverse slot must be torn down")
}

// TestGracefulDropDrainFailurePreservesServing covers the restore-on-failure path:
// when the drain barrier cannot reach lag zero before the deadline, the drop must
// not leave the migration stranded in COMPLETING (which the serving gate treats as
// non-serving). The phase is rolled back to its streaming state so the shard keeps
// serving, and a later drop — once the link recovers — completes normally.
func TestGracefulDropDrainFailurePreservesServing(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Multigres Migrator drop e2e in short mode")
	}
	if !utils.HasPostgreSQLBinaries() {
		t.Skip("PostgreSQL client/server binaries not on PATH")
	}
	ctx := t.Context()

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(2),
		shardsetup.WithMultipoolerExtraArgs("--enable-slot-based-replication=true"),
	)
	defer cleanup()
	primary := setup.GetPrimary(t)
	shardsetup.WaitForManagerReady(t, primary.Multipooler)
	targetDB := waitForPrimaryDB(t, ctx, setup)
	grpcPort := primary.Multipooler.GrpcPort

	srcPort := startStandaloneSource(t)
	seedSource(t, ctx, srcPort)

	mt, mtClose := migrationClient(t, primary)
	defer mtClose()

	id := activatedMigration(t, ctx, mt, srcPort, targetDB)
	requireServingStatus(t, ctx, grpcPort, clustermetadatapb.PoolerServingStatus_SERVING,
		"activate (EXPORT) must make the target serve")

	// Stall the reverse EXPORT link so the drain cannot reach lag zero: disable the
	// source-side subscription (it stops consuming, freezing the target reverse
	// slot's confirmed_flush), then write on the target to push its LSN past that
	// frozen point.
	sc := dialSource(t, ctx, srcPort)
	_, err := sc.Query(ctx, "ALTER SUBSCRIPTION mt_sub_"+id+" DISABLE")
	require.NoError(t, err)
	_ = sc.Close()

	tc := targetConn(t, ctx, primary, targetDB)
	defer tc.Close()
	_, err = tc.Query(ctx, "INSERT INTO public.orders (v) VALUES ('drain-stall')")
	require.NoError(t, err, "target must be write-safe in EXPORT")

	// A graceful drop now cannot drain; it must fail rather than hang forever...
	dropCtx, dropCancel := context.WithTimeout(ctx, 10*time.Second)
	_, err = mt.DropMigration(dropCtx, &migratorpb.DropMigrationRequest{Id: id})
	dropCancel()
	require.Error(t, err, "a drop whose drain cannot reach lag zero must fail, not hang")

	// ...and the shard must keep serving: the phase was rolled back to EXPORTING, so
	// the serving gate releases again and the migration is still present.
	requireServingStatus(t, ctx, grpcPort, clustermetadatapb.PoolerServingStatus_SERVING,
		"a failed drain must not strand the shard non-serving")
	resp, err := mt.GetMigrations(ctx, &migratorpb.GetMigrationsRequest{Id: id})
	require.NoError(t, err, "the migration must survive a failed drain")
	require.Equal(t, migratorpb.MigrationPhase_MIGRATION_PHASE_EXPORTING,
		resp.GetMigrations()[0].GetPhase(), "phase must be rolled back to EXPORTING")

	// Recover the link and drop again — now it completes.
	sc = dialSource(t, ctx, srcPort)
	_, err = sc.Query(ctx, "ALTER SUBSCRIPTION mt_sub_"+id+" ENABLE")
	require.NoError(t, err)
	_ = sc.Close()

	retryCtx, retryCancel := context.WithTimeout(ctx, 45*time.Second)
	defer retryCancel()
	_, err = mt.DropMigration(retryCtx, &migratorpb.DropMigrationRequest{Id: id})
	require.NoError(t, err, "a graceful drop must succeed once the link recovers")
	requireServingStatus(t, ctx, grpcPort, clustermetadatapb.PoolerServingStatus_SERVING,
		"after the successful drop the shard serves again")
}

// activatedMigration creates, starts, and activates a one-table IMPORT migration,
// returning its id once it is EXPORTING (live). Shared setup for the drop tests.
func activatedMigration(t *testing.T, ctx context.Context, mt migratorpb.MigratorClient, srcPort int, targetDB string) string {
	t.Helper()
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

	exportResp, err := mt.ActivateMigration(ctx, &migratorpb.ActivateMigrationRequest{Id: id})
	require.NoError(t, err)
	require.Equal(t, migratorpb.MigrationDirection_MIGRATION_DIRECTION_EXPORT,
		exportResp.GetMigration().GetActiveDirection(), "activate must switch to EXPORT")
	return id
}

// scalarInt runs a single-integer query on the connection and returns the value.
func scalarInt(t *testing.T, ctx context.Context, conn *client.Conn, query string) int {
	t.Helper()
	res, err := conn.Query(ctx, query)
	require.NoError(t, err, "query: %s", query)
	require.NotEmpty(t, res)
	require.NotEmpty(t, res[0].Rows)
	n, err := strconv.Atoi(string(res[0].Rows[0].Values[0]))
	require.NoError(t, err, "scalar not an int: %s", query)
	return n
}
