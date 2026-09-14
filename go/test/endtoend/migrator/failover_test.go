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

// TestTargetFailoverDuringMigration proves the core failover requirement: with a
// migration streaming, killing the target primary and letting multiorch promote
// a standby leaves the migration intact — the subscription and the migration row
// rode physical replication to the new primary, PostgreSQL auto-resumed apply,
// and the new primary's coordinator picks it up — so a post-failover source
// write still lands on the new primary with no data loss.
func TestTargetFailoverDuringMigration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Multigres Migrator failover e2e in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("no postgres binaries")
	}
	ctx := t.Context()

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiorchCount(3),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
	)
	defer cleanup()
	setup.StartMultiorchs(ctx, t)

	primary := setup.GetPrimary(t)
	oldPrimaryName := setup.PrimaryName
	targetDB := waitForPrimaryDB(t, ctx, setup)

	srcPort := startStandaloneSource(t)
	seedSource(t, ctx, srcPort)

	mt, mtClose := migrationClient(t, primary)
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
	}, 60*time.Second, 500*time.Millisecond, "migration must catch up before failover")
	mtClose()

	// A pre-failover source write.
	sc := dialSource(t, ctx, srcPort)
	_, err = sc.Query(ctx, "INSERT INTO public.orders (v) VALUES ('d')")
	require.NoError(t, err)
	_ = sc.Close()

	// Kill the target primary's postgres; multiorch promotes a standby.
	t.Logf("killing postgres on primary %s", oldPrimaryName)
	setup.KillPostgres(t, oldPrimaryName)
	newPrimaryName := shardsetup.WaitForNewPrimary(t, setup, oldPrimaryName, 60*time.Second)
	require.NotEmpty(t, newPrimaryName, "multiorch must elect a new primary")
	t.Logf("new primary: %s", newPrimaryName)
	newPrimary := setup.GetMultipoolerInstance(newPrimaryName)
	require.NotNil(t, newPrimary)
	shardsetup.WaitForManagerReady(t, newPrimary.Multipooler)

	// The migration is still visible on the new primary (row + subscription rode
	// physical replication; the coordinator is primary-gated and now runs here).
	mt2, mt2Close := migrationClient(t, newPrimary)
	defer mt2Close()
	require.Eventually(t, func() bool {
		resp, err := mt2.GetMigrations(ctx, &migratorpb.GetMigrationsRequest{Id: id})
		if err != nil || len(resp.GetMigrations()) != 1 {
			return false
		}
		t.Logf("post-failover phase=%s caught_up=%v err=%q", resp.GetMigrations()[0].GetPhase(),
			resp.GetMigrations()[0].GetCaughtUp(), resp.GetMigrations()[0].GetLastError())
		return true
	}, 60*time.Second, 500*time.Millisecond, "migration must be visible on the new primary")

	// A post-failover source write must reach the new primary — proving apply
	// resumed against the surviving slot on the promoted node.
	sc2 := dialSource(t, ctx, srcPort)
	_, err = sc2.Query(ctx, "INSERT INTO public.orders (v) VALUES ('e')")
	require.NoError(t, err)
	_ = sc2.Close()

	tc := targetConn(t, ctx, newPrimary, targetDB)
	defer tc.Close()
	require.Eventually(t, func() bool {
		n, ok := countRows(t, ctx, tc, "public.orders")
		t.Logf("new primary orders count=%d ok=%v", n, ok)
		return ok && n == 5
	}, 90*time.Second, 1*time.Second, "all 5 rows (incl. pre- and post-failover writes) must be on the new primary")
}
