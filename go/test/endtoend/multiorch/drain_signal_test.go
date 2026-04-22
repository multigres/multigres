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

package multiorch

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
)

// TestDrainingReplica_StoppedReplication_MultiorchSkipsRecovery provokes a
// condition that would normally make ReplicaNotReplicating fire — a draining
// replica with paused WAL replay — and asserts multiorch emits NO Problems
// for that pooler over a 20s window. Proves the IsVoluntarilyDraining skip
// works against a real fault condition.
func TestDrainingReplica_StoppedReplication_MultiorchSkipsRecovery(t *testing.T) {
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiOrchCount(1),
	)
	t.Cleanup(cleanup)
	setup.StartMultiOrchs(t.Context(), t)

	replicas := setup.GetStandbys()
	require.NotEmpty(t, replicas)
	replicaA := replicas[0]

	client, err := shardsetup.NewMultipoolerClient(replicaA.Multipooler.GrpcPort)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()

	// Drain the replica.
	drainCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	resp, err := client.Manager.Drain(drainCtx, &multipoolermanagerdatapb.DrainRequest{})
	cancel()
	require.NoError(t, err)
	assert.False(t, resp.PrimaryPathTaken, "replica Drain should not take primary path")

	// Pause WAL replay — would normally fire ReplicaNotReplicating.
	stopCtx, stopCancel := context.WithTimeout(ctx, 5*time.Second)
	_, err = client.Manager.StopReplication(stopCtx, &multipoolermanagerdatapb.StopReplicationRequest{
		Mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_ONLY,
		Wait: true,
	})
	stopCancel()
	require.NoError(t, err)

	// Sanity: WAL replay is actually paused.
	sCtx, sCancel := context.WithTimeout(ctx, 2*time.Second)
	st, err := client.Manager.Status(sCtx, &multipoolermanagerdatapb.StatusRequest{})
	sCancel()
	require.NoError(t, err)
	require.NotNil(t, st.Status.ReplicationStatus)
	assert.True(t, st.Status.ReplicationStatus.IsWalReplayPaused, "WAL replay should be paused")

	// Connect to the multiorch and hold the no-problem assertion.
	require.NotEmpty(t, setup.MultiOrchInstances, "test requires a multiorch instance")
	var moInst *shardsetup.ProcessInstance
	for _, mi := range setup.MultiOrchInstances {
		moInst = mi
		break
	}
	moClient, err := shardsetup.NewMultiOrchClient(moInst.GrpcPort)
	require.NoError(t, err)
	t.Cleanup(func() { _ = moClient.Close() })

	replicaID := setup.GetMultipoolerID(replicaA.Name)
	require.NotNil(t, replicaID)

	assertNoShardProblemsForPooler(t, moClient, replicaID,
		"postgres", constants.DefaultTableGroup, constants.DefaultShard,
		20*time.Second, 1*time.Second)
}

// TestReplicaDrain_SignalPropagatesToMultiorch proves: (a) Drain on a replica
// publishes DRAINING observable via its own Status RPC within 5s, and (b)
// primary's consensus term / standby list stay unchanged over a 10s hold
// (no cohort-change triggered by drain — deferred work).
func TestReplicaDrain_SignalPropagatesToMultiorch(t *testing.T) {
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiOrchCount(1),
	)
	t.Cleanup(cleanup)
	setup.StartMultiOrchs(t.Context(), t)

	replicas := setup.GetStandbys()
	require.NotEmpty(t, replicas)
	replicaA := replicas[0]
	primary := setup.GetPrimary(t)

	replicaClient, err := shardsetup.NewMultipoolerClient(replicaA.Multipooler.GrpcPort)
	require.NoError(t, err)
	t.Cleanup(func() { _ = replicaClient.Close() })
	primaryClient, err := shardsetup.NewMultipoolerClient(primary.Multipooler.GrpcPort)
	require.NoError(t, err)
	t.Cleanup(func() { _ = primaryClient.Close() })

	ctx := context.Background()

	// Capture baseline consensus state from the primary.
	psCtx, psCancel := context.WithTimeout(ctx, 2*time.Second)
	before, err := primaryClient.Manager.Status(psCtx, &multipoolermanagerdatapb.StatusRequest{})
	psCancel()
	require.NoError(t, err)
	baseTerm := before.Status.ConsensusTerm.GetPrimaryTerm()
	require.NotNil(t, before.Status.PrimaryStatus, "primary should have PrimaryStatus populated")
	var baseStandbyIDs []string
	if sync := before.Status.PrimaryStatus.GetSyncReplicationConfig(); sync != nil {
		for _, id := range sync.StandbyIds {
			baseStandbyIDs = append(baseStandbyIDs, id.GetName())
		}
	}

	// Act: Drain replica.
	dCtx, dCancel := context.WithTimeout(ctx, 5*time.Second)
	resp, err := replicaClient.Manager.Drain(dCtx, &multipoolermanagerdatapb.DrainRequest{})
	dCancel()
	require.NoError(t, err)
	assert.False(t, resp.PrimaryPathTaken)

	// Assert (positive): signal visible in the replica's own Status within 5s.
	require.Eventually(t, func() bool {
		sCtx, sCancel := context.WithTimeout(ctx, 2*time.Second)
		defer sCancel()
		s, err := replicaClient.Manager.Status(sCtx, &multipoolermanagerdatapb.StatusRequest{})
		if err != nil || s.Status == nil || s.Status.QueryServingStatus == nil {
			return false
		}
		return s.Status.QueryServingStatus.Signal == clustermetadatapb.ServingSignal_SERVING_SIGNAL_DRAINING
	}, 5*time.Second, 200*time.Millisecond, "replica should self-report DRAINING")

	// Assert (negative): over 10s, primary's consensus term and standby list unchanged.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		cCtx, cCancel := context.WithTimeout(ctx, 2*time.Second)
		s, err := primaryClient.Manager.Status(cCtx, &multipoolermanagerdatapb.StatusRequest{})
		cCancel()
		require.NoError(t, err)
		assert.Equal(t, baseTerm, s.Status.ConsensusTerm.GetPrimaryTerm(), "primary term must not change")
		var currentStandbyIDs []string
		if sync := s.Status.GetPrimaryStatus().GetSyncReplicationConfig(); sync != nil {
			for _, id := range sync.StandbyIds {
				currentStandbyIDs = append(currentStandbyIDs, id.GetName())
			}
		}
		assert.ElementsMatch(t, baseStandbyIDs, currentStandbyIDs,
			"standby list must not change — cohort updates are deferred work")
		time.Sleep(1 * time.Second)
	}
}
