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

// Package endtoend contains integration tests for multigres components.
//
// Leader reelection tests:
//   - TestDeadPrimaryRecovery: Verifies multiorch detects primary failure and
//     automatically elects a new leader from remaining standbys.
package multiorch

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestDeadPrimaryRecovery tests multiorch's ability to detect a primary failure
// and elect a new primary from the standbys.
func TestDeadPrimaryRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestDeadPrimaryRecovery test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end dead primary recovery test (short mode or no postgres binaries)")
	}

	// Create an isolated shard for this test
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiOrchCount(1),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
	)
	defer cleanup()

	// Configure replication on all standbys
	setup.SetupTest(t, shardsetup.WithoutCleanup())

	// Get the primary
	primary := setup.GetMultipoolerInstance("primary")
	require.NotNil(t, primary, "primary instance should exist")
	t.Logf("Initial primary: %s", primary.Name)

	// Kill postgres on the primary (multipooler stays running to report unhealthy status)
	t.Logf("Killing postgres on primary node %s to simulate database crash", primary.Name)
	setup.KillPostgres(t, "primary")

	// Wait for multiorch to detect failure and elect new primary
	t.Logf("Waiting for multiorch to detect primary failure and elect new leader...")

	newPrimaryName := waitForNewPrimary(t, setup, "primary", 10*time.Second)
	require.NotEmpty(t, newPrimaryName, "Expected multiorch to elect new primary automatically")
	t.Logf("New primary elected: %s", newPrimaryName)

	// Verify new primary is functional
	t.Run("verify new primary is functional", func(t *testing.T) {
		newPrimaryInst := setup.GetMultipoolerInstance(newPrimaryName)
		require.NotNil(t, newPrimaryInst, "new primary instance should exist")

		status := checkInitializationStatus(t, &nodeInstance{
			name:           newPrimaryName,
			grpcPort:       newPrimaryInst.Multipooler.GrpcPort,
			pgctldGrpcPort: newPrimaryInst.Pgctld.GrpcPort,
		})
		require.True(t, status.IsInitialized, "New primary should be initialized")
		require.Equal(t, clustermetadatapb.PoolerType_PRIMARY, status.PoolerType, "New leader should have PRIMARY pooler type")

		// Verify we can connect and query
		socketDir := filepath.Join(newPrimaryInst.Pgctld.DataDir, "pg_sockets")
		db := connectToPostgres(t, socketDir, newPrimaryInst.Pgctld.PgPort)
		defer db.Close()

		var result int
		err := db.QueryRow("SELECT 1").Scan(&result)
		require.NoError(t, err, "Should be able to query new primary")
		assert.Equal(t, 1, result)
	})
}

// waitForNewPrimary waits for a new primary (different from oldPrimaryName) to be elected.
func waitForNewPrimary(t *testing.T, setup *shardsetup.ShardSetup, oldPrimaryName string, timeout time.Duration) string {
	t.Helper()

	deadline := time.Now().Add(timeout)
	checkInterval := 2 * time.Second

	for time.Now().Before(deadline) {
		for name, inst := range setup.Multipoolers {
			if name == oldPrimaryName {
				continue
			}

			status := checkInitializationStatus(t, &nodeInstance{
				name:           name,
				grpcPort:       inst.Multipooler.GrpcPort,
				pgctldGrpcPort: inst.Pgctld.GrpcPort,
			})

			if status.IsInitialized && status.PoolerType == clustermetadatapb.PoolerType_PRIMARY {
				t.Logf("New primary elected: %s (pooler_type=%s)", name, status.PoolerType)
				return name
			}
		}
		t.Logf("Waiting for new primary election... (sleeping %v)", checkInterval)
		time.Sleep(checkInterval)
	}

	t.Fatalf("Timeout: new primary not elected within %v", timeout)
	return ""
}
