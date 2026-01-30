// Copyright 2025 Supabase, Inc.
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

package multipooler

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// setupManager manages the shared test setup for tests in this package.
var setupManager = shardsetup.NewSharedSetupManager(func(t *testing.T) *shardsetup.ShardSetup {
	// Create a 2-node cluster for testing (primary + standby)
	return shardsetup.New(t, shardsetup.WithMultipoolerCount(2))
})

// TestMain sets the path and cleans up after all tests.
func TestMain(m *testing.M) {
	exitCode := shardsetup.RunTestMain(m)
	if exitCode != 0 {
		setupManager.DumpLogs()
	}
	setupManager.Cleanup()
	os.Exit(exitCode) //nolint:forbidigo // TestMain() is allowed to call os.Exit
}

// getSharedSetup returns the shared setup for tests.
func getSharedSetup(t *testing.T) *shardsetup.ShardSetup {
	t.Helper()
	return setupManager.Get(t)
}

// restoreAfterEmergencyDemotion restores a pooler to a working state after emergency demotion.
// Emergency demotion stops postgres and disables monitoring but doesn't update topology.
// This helper:
// 1. Restarts postgres as standby
// 2. Updates topology to REPLICA
// 3. Restarts the multipooler to pick up topology changes
// 4. Resets synchronous replication configuration (clears synchronous_standby_names)
func restoreAfterEmergencyDemotion(t *testing.T, setup *MultipoolerTestSetup, pgctld *ProcessInstance, multipooler *ProcessInstance, multipoolerName string) {
	t.Helper()

	// Step 1: Restart postgres as standby (emergency demotion stopped it)
	pgctldClient, err := shardsetup.NewPgctldClient(pgctld.GrpcPort)
	require.NoError(t, err)
	defer pgctldClient.Close()

	t.Logf("Restarting stopped postgres as standby for pooler %s...", multipoolerName)
	_, err = pgctldClient.Restart(utils.WithTimeout(t, 10*time.Second), &pgctldpb.RestartRequest{
		Mode:      "fast",
		AsStandby: true,
	})
	require.NoError(t, err, "Restart as standby should succeed on pooler: %s", multipoolerName)

	// Wait for postgres to be running
	require.Eventually(t, func() bool {
		statusResp, err := pgctldClient.Status(context.Background(), &pgctldpb.StatusRequest{})
		return err == nil && statusResp.Status == pgctldpb.ServerStatus_RUNNING
	}, 10*time.Second, 1*time.Second, "Postgres should be running after restart on pooler: %s", multipoolerName)

	// Step 2: Update topology to REPLICA (emergency demotion doesn't update topology)
	t.Logf("Updating topology to REPLICA for pooler %s...", multipoolerName)
	multipoolerRecord, err := setup.TopoServer.GetMultiPooler(context.Background(), &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      setup.CellName,
		Name:      multipoolerName,
	})
	require.NoError(t, err)
	multipoolerRecord.Type = clustermetadatapb.PoolerType_REPLICA
	err = setup.TopoServer.UpdateMultiPooler(context.Background(), multipoolerRecord)
	require.NoError(t, err, "Should update topology to REPLICA for pooler: %s", multipoolerName)

	// Step 3: Restart multipooler so it picks up the topology change
	t.Logf("Restarting multipooler %s to pick up topology change...", multipoolerName)
	multipooler.Stop()
	err = multipooler.Start(t)
	require.NoError(t, err, "Multipooler should restart successfully: %s", multipoolerName)

	// Wait for manager to be ready
	waitForManagerReady(t, setup, multipooler)

	// Step 4: Reset synchronous replication configuration
	// Clear synchronous_standby_names that may have been set when this was primary
	t.Logf("Resetting synchronous replication config for pooler %s...", multipoolerName)
	poolerClient, err := shardsetup.NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", multipooler.GrpcPort))
	require.NoError(t, err)
	defer poolerClient.Close()

	_, err = poolerClient.ExecuteQuery(context.Background(), "ALTER SYSTEM SET synchronous_standby_names = ''", 1)
	require.NoError(t, err, "Should clear synchronous_standby_names on pooler: %s", multipoolerName)

	_, err = poolerClient.ExecuteQuery(context.Background(), "SELECT pg_reload_conf()", 1)
	require.NoError(t, err, "Should reload postgres config on pooler: %s", multipoolerName)

	t.Logf("Pooler %s restored after emergency demotion", multipoolerName)
}
