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

package recovery

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/clustermetadata/topo/memorytopo"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/multiorch/config"
	"github.com/multigres/multigres/go/multiorch/store"
	"github.com/multigres/multigres/go/pb/clustermetadata"

	"google.golang.org/protobuf/types/known/durationpb"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// TestPollPooler_UpdatesStore_Primary tests that polling a PRIMARY pooler updates the store with correct health metrics
func TestPollPooler_UpdatesStore_Primary(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cfg := config.NewTestConfig(
		config.WithCell("zone1"),
		config.WithPoolerHealthCheckInterval(100*time.Millisecond),
		config.WithClusterMetadataRefreshInterval(5*time.Second),
		config.WithClusterMetadataRefreshTimeout(5*time.Second),
	)

	// Create fake RPC client with mock response for PRIMARY
	fakeClient := &rpcclient.FakeClient{
		StatusResponses: map[string]*multipoolermanagerdatapb.StatusResponse{
			"multipooler-zone1-pooler1": {
				Status: &multipoolermanagerdatapb.Status{
					PoolerType: clustermetadata.PoolerType_PRIMARY,
					PrimaryStatus: &multipoolermanagerdatapb.PrimaryStatus{
						Lsn:   "0/123ABC",
						Ready: true,
						ConnectedFollowers: []*clustermetadata.ID{
							{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "replica1"},
							{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "replica2"},
						},
					},
				},
			},
		},
	}

	re := NewEngine(
		ts,
		logger,
		cfg,
		[]config.WatchTarget{{Database: "mydb"}},
		fakeClient,
	)

	// Add a pooler to the store
	poolerID := &clustermetadata.ID{
		Component: clustermetadata.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "pooler1",
	}
	pooler := &store.PoolerHealth{
		MultiPooler: &clustermetadata.MultiPooler{
			Id:         poolerID,
			Database:   "mydb",
			TableGroup: "tg1",
			Shard:      "0",
			Type:       clustermetadata.PoolerType_PRIMARY,
			Hostname:   "host1",
			PortMap:    map[string]int32{"grpc": 5432},
		},
		IsUpToDate:       false,
		IsLastCheckValid: false,
	}
	poolerKey := topo.MultiPoolerIDString(poolerID)
	re.poolerStore.Set(poolerKey, pooler)

	// Poll the pooler
	re.pollPooler(ctx, poolerID, pooler, false /* forceDiscovery */)

	// Verify store was updated
	updated, ok := re.poolerStore.Get(poolerKey)
	require.True(t, ok, "pooler should exist in store")

	// Check that health check succeeded
	require.True(t, updated.IsLastCheckValid, "health check should be valid")
	require.True(t, updated.IsUpToDate, "health check should be up to date")
	require.False(t, updated.LastSeen.IsZero(), "LastSeen should be set")
	require.False(t, updated.LastCheckSuccessful.IsZero(), "LastCheckSuccessful should be set")

	// Check that PRIMARY-specific fields were populated
	require.Equal(t, clustermetadata.PoolerType_PRIMARY, updated.PoolerType, "should report PRIMARY type")
	require.Equal(t, "0/123ABC", updated.PrimaryLSN, "LSN should match response")
	require.True(t, updated.PrimaryReady, "should be ready")
	require.Len(t, updated.PrimaryConnectedFollowers, 2, "should have 2 connected followers")

	// Check that REPLICA fields are not populated
	require.Empty(t, updated.ReplicaLastReplayLSN, "replica fields should be empty for PRIMARY")
	require.Empty(t, updated.ReplicaLastReceiveLSN, "replica fields should be empty for PRIMARY")
	require.Zero(t, updated.ReplicaLagMillis, "replica lag should be 0 for PRIMARY")
}

// TestPollPooler_UpdatesStore_Replica tests that polling a REPLICA pooler updates the store with correct health metrics
func TestPollPooler_UpdatesStore_Replica(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cfg := config.NewTestConfig(
		config.WithCell("zone1"),
		config.WithPoolerHealthCheckInterval(100*time.Millisecond),
		config.WithClusterMetadataRefreshInterval(5*time.Second),
		config.WithClusterMetadataRefreshTimeout(5*time.Second),
	)

	// Create fake RPC client with mock response for REPLICA
	fakeClient := &rpcclient.FakeClient{
		StatusResponses: map[string]*multipoolermanagerdatapb.StatusResponse{
			"multipooler-zone1-replica1": {
				Status: &multipoolermanagerdatapb.Status{
					PoolerType: clustermetadata.PoolerType_REPLICA,
					ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
						LastReplayLsn:           "0/123ABC",
						LastReceiveLsn:          "0/123DEF",
						IsWalReplayPaused:       false,
						WalReplayPauseState:     "not paused",
						Lag:                     durationpb.New(500 * time.Millisecond),
						LastXactReplayTimestamp: "2025-01-19 20:00:00.000000+00",
						PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{
							Host: "primary-host",
							Port: 5432,
						},
					},
				},
			},
		},
	}

	re := NewEngine(
		ts,
		logger,
		cfg,
		[]config.WatchTarget{{Database: "mydb"}},
		fakeClient,
	)

	// Add a replica pooler to the store
	poolerID := &clustermetadata.ID{
		Component: clustermetadata.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "replica1",
	}
	pooler := &store.PoolerHealth{
		MultiPooler: &clustermetadata.MultiPooler{
			Id:         poolerID,
			Database:   "mydb",
			TableGroup: "tg1",
			Shard:      "0",
			Type:       clustermetadata.PoolerType_REPLICA,
			Hostname:   "replica-host",
			PortMap:    map[string]int32{"grpc": 5432},
		},
		IsUpToDate:       false,
		IsLastCheckValid: false,
	}
	poolerKey := topo.MultiPoolerIDString(poolerID)
	re.poolerStore.Set(poolerKey, pooler)

	// Poll the pooler
	re.pollPooler(ctx, poolerID, pooler, false /* forceDiscovery */)

	// Verify store was updated
	updated, ok := re.poolerStore.Get(poolerKey)
	require.True(t, ok, "pooler should exist in store")

	// Check that health check succeeded
	require.True(t, updated.IsLastCheckValid, "health check should be valid")
	require.True(t, updated.IsUpToDate, "health check should be up to date")
	require.False(t, updated.LastSeen.IsZero(), "LastSeen should be set")

	// Check that REPLICA-specific fields were populated
	require.Equal(t, clustermetadata.PoolerType_REPLICA, updated.PoolerType, "should report REPLICA type")
	require.Equal(t, "0/123ABC", updated.ReplicaLastReplayLSN, "replay LSN should match response")
	require.Equal(t, "0/123DEF", updated.ReplicaLastReceiveLSN, "receive LSN should match response")
	require.False(t, updated.ReplicaIsWalReplayPaused, "WAL replay should not be paused")
	require.Equal(t, "not paused", updated.ReplicaWalReplayPauseState)
	require.Equal(t, int64(500), updated.ReplicaLagMillis, "lag should be 500ms")
	require.Equal(t, "2025-01-19 20:00:00.000000+00", updated.ReplicaLastXactReplayTimestamp)
	require.NotNil(t, updated.ReplicaPrimaryConnInfo, "primary conn info should be set")
	require.Equal(t, "primary-host", updated.ReplicaPrimaryConnInfo.Host)
	require.Equal(t, int32(5432), updated.ReplicaPrimaryConnInfo.Port)

	// Check that PRIMARY fields are not populated
	require.Empty(t, updated.PrimaryLSN, "primary fields should be empty for REPLICA")
	require.False(t, updated.PrimaryReady, "primary ready should be false for REPLICA")
	require.Nil(t, updated.PrimaryConnectedFollowers, "primary followers should be nil for REPLICA")
}

// TestPollPooler_RPCFailure tests that polling failure is properly recorded in the store
func TestPollPooler_RPCFailure(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cfg := config.NewTestConfig(
		config.WithCell("zone1"),
		config.WithPoolerHealthCheckInterval(100*time.Millisecond),
		config.WithClusterMetadataRefreshInterval(5*time.Second),
		config.WithClusterMetadataRefreshTimeout(5*time.Second),
	)

	// Create fake RPC client that returns errors
	fakeClient := &rpcclient.FakeClient{
		Errors: map[string]error{
			"multipooler-zone1-failed-pooler": context.DeadlineExceeded,
		},
	}

	re := NewEngine(
		ts,
		logger,
		cfg,
		[]config.WatchTarget{{Database: "mydb"}},
		fakeClient,
	)

	// Add a pooler to the store
	poolerID := &clustermetadata.ID{
		Component: clustermetadata.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "failed-pooler",
	}
	pooler := &store.PoolerHealth{
		MultiPooler: &clustermetadata.MultiPooler{
			Id:         poolerID,
			Database:   "mydb",
			TableGroup: "tg1",
			Shard:      "0",
			Type:       clustermetadata.PoolerType_PRIMARY,
			Hostname:   "host1",
			PortMap:    map[string]int32{"grpc": 5432},
		},
		IsUpToDate:       false,
		IsLastCheckValid: true, // was previously valid
		LastSeen:         time.Now().Add(-1 * time.Hour),
	}
	poolerKey := topo.MultiPoolerIDString(poolerID)
	re.poolerStore.Set(poolerKey, pooler)

	// Poll the pooler (should fail)
	re.pollPooler(ctx, poolerID, pooler, false /* forceDiscovery */)

	// Verify store was updated with failure
	updated, ok := re.poolerStore.Get(poolerKey)
	require.True(t, ok, "pooler should exist in store")

	// Check that health check failed properly
	require.False(t, updated.IsLastCheckValid, "health check should be invalid after failure")
	require.True(t, updated.IsUpToDate, "should be marked up-to-date (no immediate retry)")
	require.False(t, updated.LastCheckAttempted.IsZero(), "LastCheckAttempted should be set")

	// LastSeen should remain from before (not updated on failure)
	require.WithinDuration(t, pooler.LastSeen, updated.LastSeen, 1*time.Second, "LastSeen should not be updated on failure")
}

// TestPollPooler_TypeMismatch tests behavior when reported type differs from topology type
func TestPollPooler_TypeMismatch(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cfg := config.NewTestConfig(
		config.WithCell("zone1"),
		config.WithPoolerHealthCheckInterval(100*time.Millisecond),
		config.WithClusterMetadataRefreshInterval(5*time.Second),
		config.WithClusterMetadataRefreshTimeout(5*time.Second),
	)

	// Create fake RPC client where pooler reports PRIMARY but topology says REPLICA
	fakeClient := &rpcclient.FakeClient{
		StatusResponses: map[string]*multipoolermanagerdatapb.StatusResponse{
			"multipooler-zone1-confused-pooler": {
				Status: &multipoolermanagerdatapb.Status{
					PoolerType: clustermetadata.PoolerType_PRIMARY, // Reports PRIMARY
					PrimaryStatus: &multipoolermanagerdatapb.PrimaryStatus{
						Lsn:   "0/FFFFFF",
						Ready: true,
					},
				},
			},
		},
	}

	re := NewEngine(
		ts,
		logger,
		cfg,
		[]config.WatchTarget{{Database: "mydb"}},
		fakeClient,
	)

	// Add a pooler with REPLICA type in topology
	poolerID := &clustermetadata.ID{
		Component: clustermetadata.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "confused-pooler",
	}
	pooler := &store.PoolerHealth{
		MultiPooler: &clustermetadata.MultiPooler{
			Id:         poolerID,
			Database:   "mydb",
			TableGroup: "tg1",
			Shard:      "0",
			Type:       clustermetadata.PoolerType_REPLICA, // Topology says REPLICA
			Hostname:   "host1",
			PortMap:    map[string]int32{"grpc": 5432},
		},
		IsUpToDate:       false,
		IsLastCheckValid: false,
	}
	poolerKey := topo.MultiPoolerIDString(poolerID)
	re.poolerStore.Set(poolerKey, pooler)

	// Poll the pooler
	re.pollPooler(ctx, poolerID, pooler, false /* forceDiscovery */)

	// Verify store was updated
	updated, ok := re.poolerStore.Get(poolerKey)
	require.True(t, ok, "pooler should exist in store")

	// Check that we captured the type mismatch
	require.Equal(t, clustermetadata.PoolerType_REPLICA, updated.MultiPooler.Type, "topology type should remain REPLICA")
	require.Equal(t, clustermetadata.PoolerType_PRIMARY, updated.PoolerType, "reported type should be PRIMARY")

	// Should have populated PRIMARY fields (what the pooler actually reports)
	require.Equal(t, "0/FFFFFF", updated.PrimaryLSN)
	require.True(t, updated.PrimaryReady)
}
