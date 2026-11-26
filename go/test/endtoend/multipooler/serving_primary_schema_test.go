// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package multipooler

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	mdpb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/testpoll"
)

// The tests in this file check that a pooler brings the sidecar schemas and
// the sharding catalog up to date before it serves as the primary, whatever
// made it the primary. They simulate a catalog an older binary bootstrapped:
// without the catalog tables added since, and without the definition hash
// column, which an older binary did not have either.

// olderCatalogSQL turns the current catalog into one an older binary would
// have left: no routing_rule or admission_fence table (nothing references
// either), and no definition_hash.
const olderCatalogSQL = `DROP TABLE multigres.admission_fence, multigres.routing_rule;
ALTER TABLE multigres.catalog_state DROP COLUMN IF EXISTS definition_hash`

// newServingPrimarySetup starts a catalog shard with a gateway, multiorch and
// multiadmin, so a test can switch its primary and send application traffic.
func newServingPrimarySetup(t *testing.T) *shardsetup.ShardSetup {
	t.Helper()
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(2),
		shardsetup.WithMultiorchCount(1),
		shardsetup.WithMultigateway(),
		shardsetup.WithMultiadmin(),
		shardsetup.WithDatabase(routingE2EDatabase),
		shardsetup.WithCellName("test-cell"),
	)
	t.Cleanup(cleanup)
	setup.StartMultiorchs(t.Context(), t)
	setup.WaitForMultigatewayQueryServing(t)
	return setup
}

// connectToPooler opens a superuser connection to inst's postgres.
func connectToPooler(t *testing.T, inst *shardsetup.MultipoolerInstance) *sql.DB {
	t.Helper()
	db := connectToPostgresViaSocket(t, getPostgresSocketPath(inst.Pgctld.PoolerDir), inst.Pgctld.PgPort)
	t.Cleanup(func() { db.Close() })
	return db
}

// requestSwitchPrimary asks multiadmin to switch the catalog shard's primary.
func requestSwitchPrimary(ctx context.Context, t *testing.T, setup *shardsetup.ShardSetup) error {
	t.Helper()
	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", setup.MultiadminGrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	_, err = multiadminpb.NewMultiadminServiceClient(conn).SwitchPrimary(ctx, &multiadminpb.SwitchPrimaryRequest{
		ShardRef: &clustermetadatapb.ShardRef{Database: routingE2EDatabase, TableGroup: "default", Shard: "0-inf"},
		Reason:   "serving primary schema test",
	})
	return err
}

// catalogTablesExist reports whether db has both catalog tables olderCatalogSQL
// drops.
func catalogTablesExist(db *sql.DB) bool {
	var exist bool
	err := db.QueryRow(`SELECT to_regclass('multigres.routing_rule') IS NOT NULL
		AND to_regclass('multigres.admission_fence') IS NOT NULL`).Scan(&exist)
	return err == nil && exist
}

// gatewayWrite writes one row through the gateway, failing within timeout.
func gatewayWrite(ctx context.Context, gw *sql.DB, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	_, err := gw.ExecContext(ctx, `INSERT INTO serving_primary_schema_t (v) VALUES ('x')`)
	return err
}

// openGateway connects to the gateway as the test user and creates the table
// gatewayWrite writes to.
func openGateway(t *testing.T, setup *shardsetup.ShardSetup) *sql.DB {
	t.Helper()
	gw, err := sql.Open("postgres", shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5"))
	require.NoError(t, err)
	t.Cleanup(func() { gw.Close() })
	_, err = gw.ExecContext(t.Context(), `CREATE TABLE serving_primary_schema_t (id serial PRIMARY KEY, v text)`)
	require.NoError(t, err)
	return gw
}

// requireServingWithCurrentCatalog checks that inst serves as the primary on a
// current catalog: the tables are back, its routing snapshot reads, and the
// gateway's writes reach it.
func requireServingWithCurrentCatalog(t *testing.T, inst *shardsetup.MultipoolerInstance, gw *sql.DB) {
	t.Helper()
	db := connectToPooler(t, inst)
	require.Eventually(t, func() bool { return catalogTablesExist(db) },
		utils.ScaleTimeout(30*time.Second), 200*time.Millisecond,
		"the new primary should recreate the catalog tables before serving")
	waitForCatalogDefinition(t, db, "the new primary should record its definition hash")

	rc := newRoutingClients(t, inst.Multipooler.GrpcPort)
	require.Eventually(t, func() bool {
		_, err := rc.manager.GetRoutingSnapshot(t.Context(), &mdpb.GetRoutingSnapshotRequest{})
		return err == nil
	}, utils.ScaleTimeout(30*time.Second), 200*time.Millisecond, "the new primary should serve its routing snapshot")

	require.Eventually(t, func() bool { return gatewayWrite(t.Context(), gw, 5*time.Second) == nil },
		utils.ScaleTimeout(60*time.Second), 500*time.Millisecond, "the gateway should route writes to the new primary")
}

// TestServingPrimarySchema_UpgradeViaSwitchover is the rolling upgrade: the
// catalog an older binary bootstrapped reaches the new tables when a standby
// is promoted, whether its pooler had been open as a replica all along or was
// just restarted (a pod rolled to the new binary opens in recovery).
func TestServingPrimarySchema_UpgradeViaSwitchover(t *testing.T) {
	for _, tt := range []struct {
		name           string
		restartStandby bool
	}{
		{name: "standby open as a replica"},
		{name: "standby reopened in recovery", restartStandby: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			setup := newServingPrimarySetup(t)
			gw := openGateway(t, setup)
			require.NoError(t, gatewayWrite(t.Context(), gw, 10*time.Second))

			oldPrimary := setup.GetPrimary(t)
			standbys := setup.GetStandbys()
			require.Len(t, standbys, 1)
			standby := standbys[0]

			_, err := connectToPooler(t, oldPrimary).Exec(olderCatalogSQL)
			require.NoError(t, err)
			standbyDB := connectToPooler(t, standby)
			require.Eventually(t, func() bool { return !catalogTablesExist(standbyDB) },
				utils.ScaleTimeout(30*time.Second), 200*time.Millisecond, "the older catalog should replicate to the standby")

			if tt.restartStandby {
				restartMultipooler(t, standby)
			}

			ctx, cancel := context.WithTimeout(t.Context(), utils.ScaleTimeout(60*time.Second))
			defer cancel()
			require.NoError(t, requestSwitchPrimary(ctx, t, setup))
			setup.RequireRecovery(t, "multiorch", shardsetup.RecoveryScenarioPlannedFailover)
			newPrimary := setup.RefreshPrimary(t)
			require.Equal(t, standby.Name, newPrimary.Name)

			requireServingWithCurrentCatalog(t, newPrimary, gw)
		})
	}
}

// blockDDLSQL makes every DDL statement fail, from any role, until
// unblockDDLSQL drops the trigger (which the trigger does not block). A full
// catalog ensure runs DDL, so it fails while the trigger exists. The trigger
// is in the catalog, so it replicates and survives a postgres restart, which a
// lock held in a test session would not: multiorch answers a promoted pooler
// that does not serve by recruiting and promoting again, which restarts
// postgres.
const (
	blockDDLSQL = `CREATE FUNCTION public.serving_primary_block_ddl() RETURNS event_trigger
LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'DDL blocked by the test'; END $$;
CREATE EVENT TRIGGER serving_primary_block_ddl ON ddl_command_start
EXECUTE FUNCTION public.serving_primary_block_ddl()`
	unblockDDLSQL = `DROP EVENT TRIGGER IF EXISTS serving_primary_block_ddl`
)

// TestServingPrimarySchema_FailClosedThenRecover checks that a promoted
// pooler whose ensure fails does not serve as the primary, and that it starts
// serving, without a restart, once the ensure can succeed.
func TestServingPrimarySchema_FailClosedThenRecover(t *testing.T) {
	setup := newServingPrimarySetup(t)
	gw := openGateway(t, setup)
	require.NoError(t, gatewayWrite(t.Context(), gw, 10*time.Second))
	oldPrimary := setup.GetPrimary(t)
	dbs := map[string]*sql.DB{}
	for name, inst := range setup.Multipoolers {
		dbs[name] = connectToPooler(t, inst)
	}

	_, err := dbs[oldPrimary.Name].Exec(olderCatalogSQL + ";\n" + blockDDLSQL)
	require.NoError(t, err)
	for _, standby := range setup.GetStandbys() {
		require.Eventually(t, func() bool { return !catalogTablesExist(dbs[standby.Name]) },
			utils.ScaleTimeout(30*time.Second), 200*time.Millisecond, "the older catalog should replicate to the standby")
	}

	ctx, cancel := context.WithTimeout(t.Context(), utils.ScaleTimeout(60*time.Second))
	defer cancel()
	if err := requestSwitchPrimary(ctx, t, setup); err != nil {
		t.Logf("SwitchPrimary: %v", err)
	}

	// A promoted pooler's ensure fails, which shows the promotion reached it.
	require.Eventually(t, func() bool {
		for _, inst := range setup.Multipoolers {
			data, err := os.ReadFile(inst.Multipooler.LogFile)
			if err == nil && strings.Contains(string(data), "not serving as primary: preparing to serve failed") {
				return true
			}
		}
		return false
	}, utils.ScaleTimeout(60*time.Second), 500*time.Millisecond, "a promoted pooler's ensure should fail")

	// While the ensure fails, no pooler serves as the primary, and the
	// gateway's writes fail. That spans several retries: the monitor retries
	// every 5s.
	testpoll.Never(t, func() bool {
		_, ok := setup.TryFindPrimary(t)
		return ok
	}, 25*time.Second, time.Second, "no pooler should serve as the primary while its ensure fails")
	require.Error(t, gatewayWrite(t.Context(), gw, 5*time.Second), "writes should fail while no pooler serves as the primary")

	// Unblock DDL on whichever postgres is the primary: the monitor's next
	// retry succeeds, without a restart.
	require.Eventually(t, func() bool {
		for _, db := range dbs {
			if _, err := db.Exec(unblockDDLSQL); err == nil {
				return true
			}
		}
		return false
	}, utils.ScaleTimeout(60*time.Second), time.Second, "drop the event trigger on the primary postgres")
	var newPrimary *shardsetup.MultipoolerInstance
	require.Eventually(t, func() bool {
		inst, ok := setup.TryFindPrimary(t)
		newPrimary = inst
		return ok
	}, utils.ScaleTimeout(60*time.Second), time.Second, "a pooler should serve as the primary once its ensure succeeds")
	requireServingWithCurrentCatalog(t, newPrimary, gw)
}
