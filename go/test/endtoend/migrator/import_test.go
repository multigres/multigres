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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestImportHappyPathAndDrop drives a full IMPORT through the multipooler-hosted
// Migrator service: create -> start -> catch up -> stream -> drop (default
// drain), verifying rows land, streaming works, the source password never leaks,
// teardown removes the subscription, and the target's identity sequence is
// advanced so the standalone target can take writes without a PK collision.
func TestImportHappyPathAndDrop(t *testing.T) {
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
	require.NotEmpty(t, id)
	require.Equal(t, migratorpb.MigrationPhase_MIGRATION_PHASE_CREATED, createResp.GetMigration().GetPhase())

	startResp, err := mt.StartMigration(ctx, &migratorpb.StartMigrationRequest{Id: id})
	require.NoError(t, err, "start; last_error=%s", startResp.GetMigration().GetLastError())

	require.Eventually(t, func() bool {
		resp, err := mt.GetMigrations(ctx, &migratorpb.GetMigrationsRequest{Id: id})
		if err != nil || len(resp.GetMigrations()) == 0 {
			return false
		}
		m := resp.GetMigrations()[0]
		t.Logf("phase=%s ready=%d/%d caught_up=%v err=%q",
			m.GetPhase(), m.GetReadyRelations(), m.GetTotalRelations(), m.GetCaughtUp(), m.GetLastError())
		return m.GetCaughtUp()
	}, 60*time.Second, 500*time.Millisecond, "migration must catch up")

	// The status projection must never leak the source password.
	got, err := mt.GetMigrations(ctx, &migratorpb.GetMigrationsRequest{Id: id})
	require.NoError(t, err)
	require.NotContains(t, got.GetMigrations()[0].GetSource(), sourcePassword)

	tc := targetConn(t, ctx, primary, targetDB)
	defer tc.Close()
	require.Eventually(t, func() bool {
		n, ok := countRows(t, ctx, tc, "public.orders")
		return ok && n == 3
	}, 30*time.Second, 500*time.Millisecond, "initial 3 rows must land on the target")

	// Ongoing streaming: a new row on the source reaches the target.
	sc := dialSource(t, ctx, srcPort)
	_, err = sc.Query(ctx, "INSERT INTO public.orders (v) VALUES ('d')")
	require.NoError(t, err)
	_ = sc.Close()
	require.Eventually(t, func() bool {
		n, ok := countRows(t, ctx, tc, "public.orders")
		return ok && n == 4
	}, 30*time.Second, 500*time.Millisecond, "streamed insert must reach the target")

	// Drop with the default drain: quiesce source, drain, advance target
	// sequences, tear down.
	_, err = mt.DropMigration(ctx, &migratorpb.DropMigrationRequest{Id: id})
	require.NoError(t, err)

	// The subscription must be gone on the target.
	require.Eventually(t, func() bool {
		res, err := tc.Query(ctx, "SELECT count(*) FROM pg_subscription WHERE subname = 'mt_sub_"+id+"'")
		if err != nil || len(res) == 0 || len(res[0].Rows) == 0 {
			return false
		}
		return string(res[0].Rows[0].Values[0]) == "0"
	}, 30*time.Second, 500*time.Millisecond, "subscription must be dropped")

	// The target retains all data and its identity sequence was advanced past
	// the copied max, so a fresh insert does not collide on the primary key.
	_, err = tc.Query(ctx, "INSERT INTO public.orders (v) VALUES ('e')")
	require.NoError(t, err, "target must be write-safe after drop (sequence advanced)")
	n, ok := countRows(t, ctx, tc, "public.orders")
	require.True(t, ok)
	require.Equal(t, 5, n, "target retains migrated data plus the new row")
}

// TestDropForceVsDefault verifies drop's guardrails: the default refuses a
// not-started migration, while --force removes it.
func TestDropForceVsDefault(t *testing.T) {
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

	// Default drop on a CREATED (not started) migration is refused.
	_, err = mt.DropMigration(ctx, &migratorpb.DropMigrationRequest{Id: id})
	require.Error(t, err)

	// --force removes it regardless of phase.
	_, err = mt.DropMigration(ctx, &migratorpb.DropMigrationRequest{Id: id, Force: true})
	require.NoError(t, err)

	// It is gone.
	_, err = mt.GetMigrations(ctx, &migratorpb.GetMigrationsRequest{Id: id})
	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err))
}
