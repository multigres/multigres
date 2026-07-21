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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// TestManagerStatus_NodeIdentityAndConsensus verifies that MultipoolerManager.Status
// reports the right node identity (cell, name) plus consensus role (term,
// IsLeader) for both primary and standby. ConsensusStatus rides on the
// manager-service StatusResponse since the dedicated MultipoolerConsensus.Status
// RPC was removed.
func TestManagerStatus_NodeIdentityAndConsensus(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)
	waitForManagerReady(t, setup, setup.PrimaryMultipooler)
	waitForManagerReady(t, setup, setup.StandbyMultipooler)
	setupPoolerTest(t, setup, WithoutReplication())

	primaryClient, err := shardsetup.NewMultipoolerClient(setup.PrimaryMultipooler.GrpcPort)
	require.NoError(t, err)
	t.Cleanup(func() { primaryClient.Close() })

	standbyClient, err := shardsetup.NewMultipoolerClient(setup.StandbyMultipooler.GrpcPort)
	require.NoError(t, err)
	t.Cleanup(func() { standbyClient.Close() })

	t.Run("primary", func(t *testing.T) {
		resp, err := primaryClient.Manager.Status(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StatusRequest{})
		require.NoError(t, err, "Status should succeed on primary")
		require.NotNil(t, resp, "Response should not be nil")

		assert.Equal(t, setup.PrimaryMultipooler.Name, resp.GetConsensusStatus().GetId().GetName(), "PoolerId should match")
		assert.Equal(t, "test-cell", resp.GetConsensusStatus().GetId().GetCell(), "Cell should match")
		assert.Equal(t, consensus.ConsensusRoleLeader, consensus.SelfConsensusRole(resp.GetConsensusStatus()), "Primary should be consensus leader")
	})

	t.Run("standby", func(t *testing.T) {
		resp, err := standbyClient.Manager.Status(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StatusRequest{})
		require.NoError(t, err, "Status should succeed on standby")
		require.NotNil(t, resp, "Response should not be nil")

		assert.Equal(t, setup.StandbyMultipooler.Name, resp.GetConsensusStatus().GetId().GetName(), "PoolerId should match")
		assert.Equal(t, "test-cell", resp.GetConsensusStatus().GetId().GetCell(), "Cell should match")
		assert.NotEqual(t, consensus.ConsensusRoleLeader, consensus.SelfConsensusRole(resp.GetConsensusStatus()), "Standby should not be consensus leader")
	})
}

// TestMultipoolerPrimaryLSN verifies that a primary pooler reports a valid WAL position
// via the manager Status RPC.
func TestMultipoolerPrimaryLSN(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)
	setupPoolerTest(t, setup, WithoutReplication())
	waitForManagerReady(t, setup, setup.PrimaryMultipooler)

	primaryClient, err := shardsetup.NewMultipoolerClient(setup.PrimaryMultipooler.GrpcPort)
	require.NoError(t, err)
	t.Cleanup(func() { primaryClient.Close() })

	ctx := utils.WithTimeout(t, 5*time.Second)
	statusResp, err := primaryClient.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
	require.NoError(t, err)
	assert.NotEmpty(t, statusResp.GetConsensusStatus().GetCurrentPosition().GetFlushedLsn(), "Primary LSN should not be empty")
	assert.Contains(t, statusResp.GetConsensusStatus().GetCurrentPosition().GetFlushedLsn(), "/", "LSN should be in PostgreSQL format (e.g., 0/1234ABCD)")
}
