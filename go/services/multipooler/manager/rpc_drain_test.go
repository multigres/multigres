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

package manager

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multipooler/poolerserver"
)

// newDrainReadyManager returns a test manager prepared to accept Drain calls:
// state is Ready and actionLock is initialised. healthStreamer is left nil;
// enterDraining tolerates that.
func newDrainReadyManager(t *testing.T) *MultiPoolerManager {
	t.Helper()
	pm, _ := newTestManagerWithMock("tg", "shard")
	pm.mu.Lock()
	pm.state = ManagerStateReady
	pm.mu.Unlock()
	pm.actionLock = NewActionLock()
	return pm
}

func TestDrainState_DefaultActive(t *testing.T) {
	pm, _ := newTestManagerWithMock("tg", "shard")

	assert.Equal(t, clustermetadatapb.ServingSignal_SERVING_SIGNAL_ACTIVE, pm.currentServingSignal())

	signal, permanent, reason, startedAt := pm.drainSnapshot()
	assert.Equal(t, clustermetadatapb.ServingSignal_SERVING_SIGNAL_UNKNOWN, signal)
	assert.False(t, permanent)
	assert.Equal(t, clustermetadatapb.RemoveReason_REMOVE_REASON_UNKNOWN, reason)
	assert.True(t, startedAt.IsZero())
}

func TestDrainState_EnterDraining_Transitions(t *testing.T) {
	pm, _ := newTestManagerWithMock("tg", "shard")

	wasAlreadyDraining := pm.enterDraining(false, clustermetadatapb.RemoveReason_REMOVE_REASON_UNKNOWN)
	assert.False(t, wasAlreadyDraining)

	assert.Equal(t, clustermetadatapb.ServingSignal_SERVING_SIGNAL_DRAINING, pm.currentServingSignal())

	signal, permanent, _, startedAt := pm.drainSnapshot()
	assert.Equal(t, clustermetadatapb.ServingSignal_SERVING_SIGNAL_DRAINING, signal)
	assert.False(t, permanent)
	assert.False(t, startedAt.IsZero())
}

func TestDrainState_EnterDraining_UpgradesToPermanent(t *testing.T) {
	pm, _ := newTestManagerWithMock("tg", "shard")

	pm.enterDraining(false, clustermetadatapb.RemoveReason_REMOVE_REASON_UNKNOWN)
	_, _, _, firstStartedAt := pm.drainSnapshot()

	wasAlreadyDraining := pm.enterDraining(true, clustermetadatapb.RemoveReason_REMOVE_REASON_SCALE_DOWN)
	assert.True(t, wasAlreadyDraining)

	signal, permanent, reason, startedAt := pm.drainSnapshot()
	assert.Equal(t, clustermetadatapb.ServingSignal_SERVING_SIGNAL_DRAINING, signal)
	assert.True(t, permanent)
	assert.Equal(t, clustermetadatapb.RemoveReason_REMOVE_REASON_SCALE_DOWN, reason)
	assert.Equal(t, firstStartedAt, startedAt)
}

func TestDrainState_EnterDraining_NoDowngrade(t *testing.T) {
	pm, _ := newTestManagerWithMock("tg", "shard")

	pm.enterDraining(true, clustermetadatapb.RemoveReason_REMOVE_REASON_SCALE_DOWN)

	wasAlreadyDraining := pm.enterDraining(false, clustermetadatapb.RemoveReason_REMOVE_REASON_UNKNOWN)
	assert.True(t, wasAlreadyDraining)

	_, permanent, reason, _ := pm.drainSnapshot()
	assert.True(t, permanent, "permanent must not downgrade")
	assert.Equal(t, clustermetadatapb.RemoveReason_REMOVE_REASON_SCALE_DOWN, reason)
}

func TestDrainState_EnterDraining_ReasonLastWriterWins(t *testing.T) {
	pm, _ := newTestManagerWithMock("tg", "shard")

	pm.enterDraining(true, clustermetadatapb.RemoveReason_REMOVE_REASON_SCALE_DOWN)

	pm.enterDraining(true, clustermetadatapb.RemoveReason_REMOVE_REASON_DECOMMISSION)

	_, permanent, reason, _ := pm.drainSnapshot()
	assert.True(t, permanent)
	assert.Equal(t, clustermetadatapb.RemoveReason_REMOVE_REASON_DECOMMISSION, reason)
}

func TestDrain_ReplicaPath(t *testing.T) {
	pm := newDrainReadyManager(t)

	resp, err := pm.Drain(context.Background(), &multipoolermanagerdatapb.DrainRequest{
		RemoveFromTopology: false,
	})
	require.NoError(t, err)
	assert.False(t, resp.WasAlreadyDraining)
	assert.False(t, resp.PrimaryPathTaken)
	assert.False(t, resp.RemoveFromTopology)

	assert.Equal(t, clustermetadatapb.ServingSignal_SERVING_SIGNAL_DRAINING, pm.currentServingSignal())
}

func TestDrain_PermanentWritesTombstoneOnShutdown(t *testing.T) {
	pm := newDrainReadyManager(t)
	pm.isOpen = true
	ctx := context.Background()

	// Register the MultiPooler in the topology so UpdateMultiPoolerFields can find it.
	pm.multipooler.Id = pm.serviceID
	require.NoError(t, pm.topoClient.CreateMultiPooler(ctx, pm.multipooler))

	_, err := pm.Drain(ctx, &multipoolermanagerdatapb.DrainRequest{
		RemoveFromTopology: true,
		RemoveReason:       clustermetadatapb.RemoveReason_REMOVE_REASON_SCALE_DOWN,
	})
	require.NoError(t, err)

	pm.writeDrainTombstoneIfNeeded()

	info, err := pm.topoClient.GetMultiPooler(ctx, pm.serviceID)
	require.NoError(t, err)
	require.NotNil(t, info.MultiPooler.RemovedAt)
	assert.Equal(
		t,
		clustermetadatapb.RemoveReason_REMOVE_REASON_SCALE_DOWN,
		info.MultiPooler.RemoveReason,
	)
	assert.WithinDuration(t, time.Now(), info.MultiPooler.RemovedAt.AsTime(), 5*time.Second)
}

func TestObservesInvoluntaryDrain_PublishesDraining(t *testing.T) {
	pm := newDrainReadyManager(t)
	ctx := context.Background()

	pm.multipooler.Id = pm.serviceID
	pm.multipooler.Type = clustermetadatapb.PoolerType_REPLICA
	require.NoError(t, pm.topoClient.CreateMultiPooler(ctx, pm.multipooler))

	// Externally mark the pooler as involuntarily DRAINED in topology.
	_, err := pm.topoClient.UpdateMultiPoolerFields(ctx, pm.serviceID, func(mp *clustermetadatapb.MultiPooler) error {
		mp.Type = clustermetadatapb.PoolerType_DRAINED
		return nil
	})
	require.NoError(t, err)

	// Invoke the observer that the heartbeat would call periodically.
	pm.observeInvoluntaryDrain(ctx)

	assert.Equal(t, clustermetadatapb.ServingSignal_SERVING_SIGNAL_DRAINING, pm.currentServingSignal())
	_, permanent, _, _ := pm.drainSnapshot()
	assert.False(t, permanent, "involuntary drain must not flip the permanent flag")
}

func TestObservesInvoluntaryDrain_NonDrained_NoTransition(t *testing.T) {
	pm := newDrainReadyManager(t)
	ctx := context.Background()

	pm.multipooler.Id = pm.serviceID
	pm.multipooler.Type = clustermetadatapb.PoolerType_REPLICA
	require.NoError(t, pm.topoClient.CreateMultiPooler(ctx, pm.multipooler))

	pm.observeInvoluntaryDrain(ctx)

	assert.Equal(t, clustermetadatapb.ServingSignal_SERVING_SIGNAL_ACTIVE, pm.currentServingSignal())
}

func TestDrain_NonPermanent_NoTombstone(t *testing.T) {
	pm := newDrainReadyManager(t)
	ctx := context.Background()

	pm.multipooler.Id = pm.serviceID
	require.NoError(t, pm.topoClient.CreateMultiPooler(ctx, pm.multipooler))

	_, err := pm.Drain(ctx, &multipoolermanagerdatapb.DrainRequest{
		RemoveFromTopology: false,
	})
	require.NoError(t, err)

	pm.writeDrainTombstoneIfNeeded()

	info, err := pm.topoClient.GetMultiPooler(ctx, pm.serviceID)
	require.NoError(t, err)
	assert.Nil(t, info.MultiPooler.RemovedAt, "non-permanent drain must not write a tombstone")
}

func TestDrain_IdempotentAndPermanentUpgrade(t *testing.T) {
	pm := newDrainReadyManager(t)
	ctx := context.Background()

	resp, err := pm.Drain(ctx, &multipoolermanagerdatapb.DrainRequest{})
	require.NoError(t, err)
	assert.False(t, resp.WasAlreadyDraining)
	assert.False(t, resp.RemoveFromTopology)

	resp, err = pm.Drain(ctx, &multipoolermanagerdatapb.DrainRequest{
		RemoveFromTopology: true,
		RemoveReason:       clustermetadatapb.RemoveReason_REMOVE_REASON_SCALE_DOWN,
	})
	require.NoError(t, err)
	assert.True(t, resp.WasAlreadyDraining)
	assert.True(t, resp.RemoveFromTopology)
	_, _, reason, _ := pm.drainSnapshot()
	assert.Equal(t, clustermetadatapb.RemoveReason_REMOVE_REASON_SCALE_DOWN, reason)

	resp, err = pm.Drain(ctx, &multipoolermanagerdatapb.DrainRequest{
		RemoveFromTopology: false,
	})
	require.NoError(t, err)
	assert.True(t, resp.WasAlreadyDraining)
	assert.True(t, resp.RemoveFromTopology, "remove_from_topology must not downgrade")
	_, _, reason, _ = pm.drainSnapshot()
	assert.Equal(t, clustermetadatapb.RemoveReason_REMOVE_REASON_SCALE_DOWN, reason)

	resp, err = pm.Drain(ctx, &multipoolermanagerdatapb.DrainRequest{
		RemoveFromTopology: true,
		RemoveReason:       clustermetadatapb.RemoveReason_REMOVE_REASON_DECOMMISSION,
	})
	require.NoError(t, err)
	assert.True(t, resp.RemoveFromTopology)
	_, _, reason, _ = pm.drainSnapshot()
	assert.Equal(t, clustermetadatapb.RemoveReason_REMOVE_REASON_DECOMMISSION, reason)
}

// TestServingSignalPublishes verifies that setting DRAINING on the health
// streamer causes subscribers to receive a HealthState carrying the
// DRAINING signal.
func TestServingSignalPublishes(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	hs := newHealthStreamer(logger, nil, "tg", "0")

	ch := make(chan *poolerserver.HealthState, 10)
	hs.clients[ch] = struct{}{}

	hs.SetServingSignal(clustermetadatapb.ServingSignal_SERVING_SIGNAL_DRAINING)

	select {
	case state := <-ch:
		require.NotNil(t, state)
		assert.Equal(t, clustermetadatapb.ServingSignal_SERVING_SIGNAL_DRAINING, state.ServingSignal)
	case <-time.After(time.Second):
		t.Fatal("expected broadcast with DRAINING signal")
	}
}
