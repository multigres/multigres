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
	"github.com/multigres/multigres/go/tools/s3mock"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// filesystemSetupManager manages the shared test setup for filesystem backend tests.
var filesystemSetupManager = shardsetup.NewSharedSetupManager(func(t *testing.T) *shardsetup.ShardSetup {
	// Create a 2-node cluster for testing (primary + standby) with filesystem backup
	return shardsetup.New(t, shardsetup.WithMultipoolerCount(2))
})

// sharedS3MockServer stores the s3mock server instance shared across all s3 backend tests.
// Cleaned up in TestMain after all tests complete.
var sharedS3MockServer *s3mock.Server

// s3SetupManager manages the shared test setup for s3mock backend tests.
// Creates an embedded s3mock server on first Get() call.
var s3SetupManager = shardsetup.NewSharedSetupManager(func(t *testing.T) *shardsetup.ShardSetup {
	t.Helper()

	// Create embedded s3mock server (only once, shared across all s3 backend tests)
	if sharedS3MockServer == nil {
		var err error
		sharedS3MockServer, err = s3mock.NewServer(0) // Port 0 = auto-assign available port
		if err != nil {
			t.Fatalf("Failed to start s3mock: %v", err)
		}

		// Create the "multigres" bucket for testing
		if err := sharedS3MockServer.CreateBucket("multigres"); err != nil {
			t.Fatalf("Failed to create multigres bucket: %v", err)
		}

		t.Logf("s3mock started at %s", sharedS3MockServer.Endpoint())
	}

	// Set dummy AWS credentials for pgBackRest (s3mock doesn't check them)
	// Save original values to restore after test
	origAccessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	origSecretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	os.Setenv("AWS_ACCESS_KEY_ID", "test-access-key")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
	t.Cleanup(func() {
		if origAccessKey == "" {
			os.Unsetenv("AWS_ACCESS_KEY_ID")
		} else {
			os.Setenv("AWS_ACCESS_KEY_ID", origAccessKey)
		}
		if origSecretKey == "" {
			os.Unsetenv("AWS_SECRET_ACCESS_KEY")
		} else {
			os.Setenv("AWS_SECRET_ACCESS_KEY", origSecretKey)
		}
	})

	// Create a 2-node cluster for testing (primary + standby) with s3mock backup
	return shardsetup.New(t,
		shardsetup.WithMultipoolerCount(2),
		shardsetup.WithS3Backup("multigres", "us-east-1", sharedS3MockServer.Endpoint()),
	)
})

// TestMain sets the path and cleans up after all tests.
func TestMain(m *testing.M) {
	exitCode := shardsetup.RunTestMain(m)
	if exitCode != 0 {
		filesystemSetupManager.DumpLogs()
		s3SetupManager.DumpLogs()
	}
	filesystemSetupManager.Cleanup()
	s3SetupManager.Cleanup()

	// Stop shared s3mock server if it was created
	if sharedS3MockServer != nil {
		if err := sharedS3MockServer.Stop(); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to stop s3mock: %v\n", err)
		}
	}

	os.Exit(exitCode) //nolint:forbidigo // TestMain() is allowed to call os.Exit
}

// getSharedSetup returns the shared setup for tests (filesystem backend).
func getSharedSetup(t *testing.T) *shardsetup.ShardSetup {
	t.Helper()
	return filesystemSetupManager.Get(t)
}

// availableBackends lists the backup backends available for testing.
// Both filesystem and s3mock backends are always available.
var availableBackends = []string{"filesystem", "s3"}

// getSetupForBackend returns the appropriate shared setup for the given backend.
func getSetupForBackend(t *testing.T, backendName string) *MultipoolerTestSetup {
	t.Helper()

	var setup *shardsetup.ShardSetup
	if backendName == "s3" {
		setup = s3SetupManager.Get(t)
	} else {
		setup = filesystemSetupManager.Get(t)
	}

	return newMultipoolerTestSetup(setup)
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
	restartCtx, restartCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer restartCancel()
	_, err = pgctldClient.Restart(restartCtx, &pgctldpb.RestartRequest{
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
	err = multipooler.Start(restartCtx, t)
	require.NoError(t, err, "Multipooler should restart successfully: %s", multipoolerName)

	// Wait for manager to be ready
	waitForManagerReady(t, setup, multipooler)

	// Step 4: Re-enable the monitor (emergency demotion disabled it)
	t.Logf("Re-enabling monitor for pooler %s...", multipoolerName)
	multipoolerClient, err := shardsetup.NewMultipoolerClient(multipooler.GrpcPort)
	require.NoError(t, err)
	defer multipoolerClient.Close()

	setMonitorCtx, setMonitorCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer setMonitorCancel()
	_, err = multipoolerClient.Manager.SetMonitor(setMonitorCtx, &multipoolermanagerdatapb.SetMonitorRequest{
		Enabled: true,
	})
	require.NoError(t, err, "Should re-enable monitor on pooler: %s", multipoolerName)

	// Step 5: Reset synchronous replication configuration
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
