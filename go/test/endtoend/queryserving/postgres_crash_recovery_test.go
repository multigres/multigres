// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package queryserving

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// TestMultiGateway_PostgresCrashRecovery tests that SQL queries through multigateway recover
// after postgres crashes on the primary and is auto-restarted by the monitor.
func TestMultiGateway_PostgresCrashRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping PostgresCrashRecovery test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping cluster lifecycle tests")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")

	// Step 1: Verify baseline query works through multigateway.
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	defer db.Close()

	ctx := utils.WithTimeout(t, 10*time.Second)
	var result int
	err = db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
	require.NoError(t, err, "baseline query should succeed before crash")
	require.Equal(t, 1, result)
	t.Log("Baseline query through multigateway succeeded")

	// Step 2: Kill postgres on the primary via pgctld.
	// Use pgctld Stop RPC directly (instead of setup.KillPostgres) to handle
	// the case where postgres may already be stopped.
	primary := setup.GetPrimary(t)
	pgctldClient, err := shardsetup.NewPgctldClient(primary.Pgctld.GrpcPort)
	require.NoError(t, err, "failed to connect to pgctld")
	defer pgctldClient.Close()

	// Disable postgres restarts so the monitor does not restart postgres before
	// we can confirm it is stopped.
	primaryClient := setup.NewPrimaryClient(t)
	defer primaryClient.Close()

	_, err = primaryClient.Manager.SetPostgresRestartsEnabled(t.Context(), &multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: false})
	require.NoError(t, err, "failed to disable postgres restarts")
	defer func() {
		_, _ = primaryClient.Manager.SetPostgresRestartsEnabled(context.Background(), &multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: true})
	}()

	t.Logf("Stopping postgres on primary node %s via pgctld (immediate mode)", setup.PrimaryName)
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer stopCancel()
	_, err = pgctldClient.Stop(stopCtx, &pgctldpb.StopRequest{Mode: "immediate"})
	if err != nil {
		t.Logf("pgctld Stop returned error (postgres may already be stopped): %v", err)
	}

	// Confirm postgres is down before re-enabling restarts.
	require.Eventually(t, func() bool {
		statusCtx := utils.WithShortDeadline(t)
		resp, err := pgctldClient.Status(statusCtx, &pgctldpb.StatusRequest{})
		if err != nil {
			return false
		}
		return resp.Status != pgctldpb.ServerStatus_RUNNING
	}, 10*time.Second, 500*time.Millisecond, "Postgres should be stopped after kill")
	t.Log("Postgres confirmed stopped")

	// Re-enable restarts so the monitor can auto-restart postgres.
	_, err = primaryClient.Manager.SetPostgresRestartsEnabled(t.Context(), &multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: true})
	require.NoError(t, err, "failed to re-enable postgres restarts")

	// Step 3: Wait for the monitor to auto-restart postgres.

	t.Log("Waiting for postgres to be auto-restarted by monitor...")
	require.Eventually(t, func() bool {
		statusCtx := utils.WithShortDeadline(t)
		status, err := primaryClient.Manager.Status(statusCtx, &multipoolermanagerdatapb.StatusRequest{})
		if err != nil {
			return false
		}
		return status.Status.PostgresReady
	}, 30*time.Second, 500*time.Millisecond, "Postgres should be auto-restarted by monitor")
	t.Log("Postgres auto-restarted successfully")

	// Step 4: Verify queries through multigateway recover after postgres restart.
	freshDB, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	defer freshDB.Close()

	require.Eventually(t, func() bool {
		queryCtx := utils.WithShortDeadline(t)
		err := freshDB.QueryRowContext(queryCtx, "SELECT 1").Scan(&result)
		if err != nil {
			t.Logf("Query after restart failed (may still be recovering): %v", err)
			return false
		}
		return result == 1
	}, 30*time.Second, 1*time.Second,
		"Query through multigateway should succeed after postgres crash recovery on primary")

	t.Log("Query through multigateway succeeded after postgres crash recovery")
}
