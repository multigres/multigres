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
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// newTopoSyncTestManager creates a minimal MultiPoolerManager for topo sync tests.
// It registers the multipooler in topo so that RegisterMultiPooler (update path) works.
// The caller provides the manager context and pooler type to indicate the test setting.
func newTopoSyncTestManager(t *testing.T, ctx context.Context, ts topoclient.Store, poolerType clustermetadatapb.PoolerType) *MultiPoolerManager {
	t.Helper()

	ctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)

	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "topo-sync-test",
	}

	multipooler := &clustermetadatapb.MultiPooler{
		Id:            serviceID,
		Database:      "testdb",
		Hostname:      "localhost",
		PortMap:       map[string]int32{"grpc": 8080},
		Type:          poolerType,
		ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
		TableGroup:    constants.DefaultTableGroup,
		Shard:         constants.DefaultShard,
	}

	// Pre-create the multipooler in topo so RegisterMultiPooler uses the update path.
	require.NoError(t, ts.CreateMultiPooler(context.Background(), multipooler))

	pm := &MultiPoolerManager{
		logger:      slog.New(slog.NewTextHandler(os.Stdout, nil)),
		topoClient:  ts,
		serviceID:   serviceID,
		multipooler: multipooler,
		config:      &Config{TopoClient: ts},
		ctx:         ctx,
		cancel:      cancel,
	}

	return pm
}

// poolerPathForID returns the topo path for a multipooler, used for error injection.
func poolerPathForID(id *clustermetadatapb.ID) string {
	return "poolers/" + topoclient.MultiPoolerIDString(id) + "/Pooler"
}

func TestTopoSyncRetry_SucceedsOnRetry(t *testing.T) {
	ctx := context.Background()
	ts, factory := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	pm := newTopoSyncTestManager(t, ctx, ts, clustermetadatapb.PoolerType_REPLICA)

	// Inject two one-time errors for Update operations so the first two attempts fail.
	factory.AddOneTimeOperationError(memorytopo.Update, poolerPathForID(pm.serviceID), assert.AnError)
	factory.AddOneTimeOperationError(memorytopo.Update, poolerPathForID(pm.serviceID), assert.AnError)

	// scheduleTopoSync transitions from idle → syncing and spawns a goroutine.
	pm.scheduleTopoSync()

	// The goroutine should eventually succeed and transition to idle.
	require.Eventually(t, func() bool {
		return pm.topoSyncState.Load() == topoSyncIdle
	}, 10*time.Second, 50*time.Millisecond, "topo sync should eventually succeed and return to idle")

	// Verify topo was actually updated.
	mpi, err := ts.GetMultiPooler(ctx, pm.serviceID)
	require.NoError(t, err)
	assert.Equal(t, pm.multipooler.Type, mpi.MultiPooler.Type)
}

func TestTopoSyncRetry_NewWorkSignaling(t *testing.T) {
	ctx := context.Background()
	ts, factory := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	pm := newTopoSyncTestManager(t, ctx, ts, clustermetadatapb.PoolerType_REPLICA)

	// Inject a persistent error initially, then clear it after signaling new work.
	factory.AddOperationError(memorytopo.Update, poolerPathForID(pm.serviceID), assert.AnError)

	// Start the sync goroutine from idle state.
	pm.scheduleTopoSync()

	// Wait for at least one failed retry attempt.
	time.Sleep(2 * time.Second)

	// Signal new work by updating local state and calling scheduleTopoSync.
	pm.mu.Lock()
	pm.multipooler.Type = clustermetadatapb.PoolerType_PRIMARY
	pm.mu.Unlock()

	pm.scheduleTopoSync() // syncing → needed

	// Clear errors so the next retry succeeds.
	factory.ClearOperationErrors()

	// The goroutine should eventually complete and transition to idle.
	require.Eventually(t, func() bool {
		return pm.topoSyncState.Load() == topoSyncIdle
	}, 10*time.Second, 50*time.Millisecond)

	// Verify the latest state was written (PRIMARY, not REPLICA).
	mpi, err := ts.GetMultiPooler(ctx, pm.serviceID)
	require.NoError(t, err)
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, mpi.MultiPooler.Type)
}

func TestTopoSyncRetry_ExitsWhenDirectWriteSucceeds(t *testing.T) {
	ctx := context.Background()
	ts, factory := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	pm := newTopoSyncTestManager(t, ctx, ts, clustermetadatapb.PoolerType_REPLICA)

	// Inject persistent error so the goroutine keeps retrying.
	factory.AddOperationError(memorytopo.Update, poolerPathForID(pm.serviceID), assert.AnError)

	// Start the sync goroutine.
	pm.scheduleTopoSync()

	// Wait for the goroutine to be running.
	require.Eventually(t, func() bool {
		s := pm.topoSyncState.Load()
		return s == topoSyncSyncing || s == topoSyncNeeded
	}, 5*time.Second, 10*time.Millisecond)

	// Simulate a direct topo write success clearing the flag.
	pm.topoSyncState.Store(topoSyncIdle)

	// The goroutine should exit because state is now idle.
	// Verify that no new goroutine is spawned by waiting and checking state.
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, topoSyncIdle, pm.topoSyncState.Load())
}

func TestTopoSyncRetry_StopsOnContextCancel(t *testing.T) {
	ctx := context.Background()
	ts, factory := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	managerCtx, managerCancel := context.WithCancel(context.Background())
	pm := newTopoSyncTestManager(t, managerCtx, ts, clustermetadatapb.PoolerType_REPLICA)

	factory.AddOperationError(memorytopo.Update, poolerPathForID(pm.serviceID), assert.AnError)

	// Start the sync goroutine from idle state.
	pm.scheduleTopoSync()

	// Wait for the goroutine to be running (at least one retry attempt).
	require.Eventually(t, func() bool {
		s := pm.topoSyncState.Load()
		return s == topoSyncSyncing || s == topoSyncNeeded
	}, 5*time.Second, 10*time.Millisecond)

	// Cancel the manager context.
	managerCancel()

	// The goroutine should exit and set state back to idle.
	require.Eventually(t, func() bool {
		return pm.topoSyncState.Load() == topoSyncIdle
	}, 5*time.Second, 50*time.Millisecond, "topo sync goroutine should exit on context cancel")
}

func TestScheduleTopoSync_StateTransitions(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	pm := newTopoSyncTestManager(t, ctx, ts, clustermetadatapb.PoolerType_REPLICA)

	// Test: idle → syncing (spawns goroutine, which succeeds and returns to idle)
	pm.topoSyncState.Store(topoSyncIdle)
	pm.scheduleTopoSync()

	require.Eventually(t, func() bool {
		return pm.topoSyncState.Load() == topoSyncIdle
	}, 5*time.Second, 10*time.Millisecond)

	// Test: needed → no change (already flagged, no goroutine spawned)
	pm.topoSyncState.Store(topoSyncNeeded)
	pm.scheduleTopoSync()
	assert.Equal(t, topoSyncNeeded, pm.topoSyncState.Load())
}
