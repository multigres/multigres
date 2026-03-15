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
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestBufferPlannedFailover verifies that multigateway's failover buffering
// absorbs a planned failover with zero application-visible errors.
//
// The test:
//  1. Creates an isolated 3-node cluster with multiorch and multigateway (buffering enabled).
//  2. Starts continuous writes through multigateway.
//  3. Triggers a planned failover via BeginTerm (emergency demotion).
//  4. Waits for a new primary to be elected.
//  5. Asserts zero failed writes — the buffer should have held all in-flight
//     requests until the new primary appeared.
func TestBufferPlannedFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestBufferPlannedFailover in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping buffer failover test")
	}

	// Create an isolated cluster with buffering enabled on multigateway.
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiOrchCount(3),
		shardsetup.WithMultigatewayBuffering(),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
		shardsetup.WithPrimaryFailoverGracePeriod("0s", "0s"),
	)
	defer cleanup()

	setup.StartMultiOrchs(t.Context(), t)
	setup.WaitForMultigatewayQueryServing(t)

	primary := setup.GetPrimary(t)
	require.NotNil(t, primary, "primary should exist after bootstrap")
	t.Logf("Initial primary: %s", setup.PrimaryName)

	// Connect to multigateway for continuous writes.
	connStr := fmt.Sprintf(
		"host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable connect_timeout=5",
		setup.MultigatewayPgPort, shardsetup.TestPostgresPassword)
	gatewayDB, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	defer gatewayDB.Close()
	require.NoError(t, gatewayDB.Ping(), "failed to ping multigateway")

	// Start continuous writes.
	validator, validatorCleanup, err := shardsetup.NewWriterValidator(t, gatewayDB,
		shardsetup.WithWorkerCount(4),
		shardsetup.WithWriteInterval(10*time.Millisecond),
	)
	require.NoError(t, err)
	t.Cleanup(validatorCleanup)

	t.Logf("Starting continuous writes via multigateway (table: %s)...", validator.TableName())
	validator.Start(t)

	// Let writes accumulate before triggering failover.
	time.Sleep(500 * time.Millisecond)
	preSuccess, preFailed := validator.Stats()
	t.Logf("Pre-failover writes: %d successful, %d failed", preSuccess, preFailed)

	// Trigger planned failover via BeginTerm (emergency demotion).
	currentPrimary := setup.RefreshPrimary(t)
	require.NotNil(t, currentPrimary)
	currentPrimaryName := currentPrimary.Name

	primaryClient, err := shardsetup.NewMultipoolerClient(currentPrimary.Multipooler.GrpcPort)
	require.NoError(t, err)

	statusResp, err := primaryClient.Manager.Status(
		utils.WithTimeout(t, 5*time.Second), &multipoolermanagerdatapb.StatusRequest{})
	require.NoError(t, err)
	oldTerm := statusResp.Status.ConsensusTerm.TermNumber

	t.Logf("Triggering planned failover: BeginTerm on %s (current term %d)", currentPrimaryName, oldTerm)

	beginTermResp, err := primaryClient.Consensus.BeginTerm(
		utils.WithTimeout(t, 10*time.Second),
		&consensusdatapb.BeginTermRequest{
			Term: oldTerm + 1,
			CandidateId: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIORCH,
				Cell:      setup.CellName,
				Name:      "test-coordinator",
			},
			Action: consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_REVOKE,
		})
	primaryClient.Close()

	require.NoError(t, err, "BeginTerm should succeed")
	require.True(t, beginTermResp.Accepted, "primary should accept BeginTerm")
	t.Logf("BeginTerm accepted, emergency demotion triggered")

	// Wait for multiorch to elect a new primary.
	newPrimaryName := waitForNewPrimary(t, setup, currentPrimaryName, 20*time.Second)
	require.NotEmpty(t, newPrimaryName, "new primary should be elected after planned failover")
	t.Logf("New primary elected: %s", newPrimaryName)

	// Let writes continue against the new primary for a bit.
	time.Sleep(500 * time.Millisecond)

	// Stop writes and check results.
	validator.Stop()
	successfulWrites, failedWrites := validator.Stats()
	t.Logf("Final writes: %d successful, %d failed", successfulWrites, failedWrites)

	// The key assertion: buffering should absorb the failover with zero failures.
	assert.Zero(t, failedWrites,
		"buffering should absorb the planned failover with zero failed writes")
	assert.Greater(t, successfulWrites, 0,
		"should have some successful writes")
}

// waitForNewPrimary polls the cluster until a primary different from oldPrimaryName is found.
func waitForNewPrimary(t *testing.T, setup *shardsetup.ShardSetup, oldPrimaryName string, timeout time.Duration) string {
	t.Helper()

	check := func() string {
		for name, inst := range setup.Multipoolers {
			if name == oldPrimaryName {
				continue
			}
			client, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
			if err != nil {
				continue
			}
			resp, err := client.Manager.Status(
				utils.WithTimeout(t, 5*time.Second), &multipoolermanagerdatapb.StatusRequest{})
			client.Close()
			if err != nil {
				continue
			}
			if resp.Status.IsInitialized && resp.Status.PoolerType == clustermetadatapb.PoolerType_PRIMARY {
				return name
			}
		}
		return ""
	}

	if name := check(); name != "" {
		return name
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	deadline := time.After(timeout)
	for {
		select {
		case <-ticker.C:
			if name := check(); name != "" {
				return name
			}
			t.Log("Waiting for new primary election...")
		case <-deadline:
			t.Fatalf("timeout: new primary not elected within %v", timeout)
			return ""
		}
	}
}
