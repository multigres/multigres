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
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestServingGate exercises the serving gate across the full workflow from the
// design document: a shard that is the target of an IMPORT migration must not
// serve client queries until it is activated (switched to EXPORT), and going back
// to IMPORT stops serving again. Serving is observed on the pooler's health stream
// (the signal the gateway consumes), not through a direct-to-Postgres connection —
// the gate lives in the pooler, so it advertises DRAINING while importing and
// SERVING once activated.
//
//	CREATED (migration exists) -> not serving (DRAINING)
//	IMPORTING -> not serving (DRAINING)
//	activate (EXPORT) -> serving (SERVING)
//	deactivate (IMPORT) -> not serving (DRAINING)
//	drop -> serving (standalone)
func TestServingGate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Multigres Migrator serving-gate e2e in short mode")
	}
	if !utils.HasPostgreSQLBinaries() {
		t.Skip("PostgreSQL client/server binaries not on PATH")
	}
	ctx := t.Context()

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(2),
		// EXPORT makes the Multigres target the logical publisher, which needs
		// slot-based replication (matching targetConnInfo's guardrail).
		shardsetup.WithMultipoolerExtraArgs("--enable-slot-based-replication=true"),
	)
	defer cleanup()
	primary := setup.GetPrimary(t)
	shardsetup.WaitForManagerReady(t, primary.Multipooler)
	targetDB := waitForPrimaryDB(t, ctx, setup)
	grpcPort := primary.Multipooler.GrpcPort

	// Baseline: with no migration, the shard serves.
	requireServingStatus(t, ctx, grpcPort, clustermetadatapb.PoolerServingStatus_SERVING,
		"shard should serve before any migration")

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

	// As soon as a migration exists on the shard — even CREATED, before it is
	// started — the pooler stops serving, so clients cannot change the database
	// while a migration is staged against it.
	requireServingStatus(t, ctx, grpcPort, clustermetadatapb.PoolerServingStatus_DRAINING,
		"a CREATED migration must hold serving (non-serving)")

	_, err = mt.StartMigration(ctx, &migratorpb.StartMigrationRequest{Id: id})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		resp, err := mt.GetMigrations(ctx, &migratorpb.GetMigrationsRequest{Id: id})
		return err == nil && len(resp.GetMigrations()) == 1 && resp.GetMigrations()[0].GetCaughtUp()
	}, 60*time.Second, 500*time.Millisecond, "IMPORT must catch up")

	// IMPORTING: the target must NOT serve, even once caught up.
	requireServingStatus(t, ctx, grpcPort, clustermetadatapb.PoolerServingStatus_DRAINING,
		"IMPORTING target must not serve (DRAINING) even when caught up")

	// activate -> EXPORTING: the target goes live.
	_, err = mt.ActivateMigration(ctx, &migratorpb.ActivateMigrationRequest{Id: id})
	require.NoError(t, err)
	requireServingStatus(t, ctx, grpcPort, clustermetadatapb.PoolerServingStatus_SERVING,
		"activate (EXPORT) must make the target serve")

	// deactivate -> IMPORTING: back to not serving.
	_, err = mt.DeactivateMigration(ctx, &migratorpb.DeactivateMigrationRequest{Id: id})
	require.NoError(t, err)
	requireServingStatus(t, ctx, grpcPort, clustermetadatapb.PoolerServingStatus_DRAINING,
		"deactivate (IMPORT) must stop serving again")

	// drop -> the migration is gone, so the gate releases and the shard serves as a
	// standalone shard. Force tears down immediately (skipping the caught-up/drain
	// barrier) — this test only cares that the serving hold clears after the drop.
	_, err = mt.DropMigration(ctx, &migratorpb.DropMigrationRequest{Id: id, Force: true})
	require.NoError(t, err)
	requireServingStatus(t, ctx, grpcPort, clustermetadatapb.PoolerServingStatus_SERVING,
		"after drop the shard serves again")
}

// requireServingStatus polls the pooler's health stream until its advertised
// serving status equals want. The serving gate reconciles on the postgres
// monitor's tick, so allow a generous window.
func requireServingStatus(t *testing.T, ctx context.Context, grpcPort int, want clustermetadatapb.PoolerServingStatus, msg string) {
	t.Helper()
	require.Eventually(t, func() bool {
		got, ok := poolerServingStatus(ctx, grpcPort)
		if ok {
			t.Logf("pooler serving_status=%s want=%s", got, want)
		}
		return ok && got == want
	}, 30*time.Second, 500*time.Millisecond, msg)
}

// poolerServingStatus reads the current serving status from the pooler's health
// stream (StreamPoolerHealth sends the current state immediately on connect).
func poolerServingStatus(ctx context.Context, grpcPort int) (clustermetadatapb.PoolerServingStatus, bool) {
	conn, err := grpc.NewClient(
		"passthrough:///localhost:"+strconv.Itoa(grpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return 0, false
	}
	defer conn.Close()

	sctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	stream, err := multipoolerservicepb.NewMultipoolerServiceClient(conn).
		StreamPoolerHealth(sctx, &multipoolerservicepb.StreamPoolerHealthRequest{})
	if err != nil {
		return 0, false
	}
	resp, err := stream.Recv()
	if err != nil {
		return 0, false
	}
	return resp.GetServingStatus(), true
}
