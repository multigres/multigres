// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/rpcclient"
	commontypes "github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

func TestEngine_UpdateDetectedProblems(t *testing.T) {
	ts := newTestTopoStore()
	defer ts.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	cfg := config.NewTestConfig(config.WithCell("zone1"))

	engine := NewEngine(
		ts,
		logger,
		cfg,
		[]config.WatchTarget{{Database: "testdb"}},
		&rpcclient.FakeClient{},
		newTestCoordinator(ts, &rpcclient.FakeClient{}, "zone1"),
	)

	// Test with empty problems
	engine.updateDetectedProblems(nil)
	data := engine.collectDetectedProblemsData()
	assert.Empty(t, data)

	// Test with multiple problems
	problems := []types.Problem{
		{
			CheckName: "PrimaryIsDead",
			Scope:     types.ScopePooler,
			ShardKey: commontypes.ShardKey{
				Database:   "testdb",
				TableGroup: "tg1",
				Shard:      "shard1",
			},
			PoolerID: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "pooler1",
			},
		},
		{
			CheckName: "ReplicaNotReplicating",
			Scope:     types.ScopePooler,
			ShardKey: commontypes.ShardKey{
				Database:   "testdb",
				TableGroup: "tg1",
				Shard:      "shard2",
			},
			PoolerID: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "pooler2",
			},
		},
	}

	engine.updateDetectedProblems(problems)
	data = engine.collectDetectedProblemsData()

	require.Len(t, data, 2)
	assert.Equal(t, "PrimaryIsDead", data[0].AnalysisType)
	assert.Equal(t, "testdb", data[0].DBNamespace)
	assert.Equal(t, "shard1", data[0].Shard)
	assert.Contains(t, data[0].EntityID, "pooler1")

	assert.Equal(t, "ReplicaNotReplicating", data[1].AnalysisType)
	assert.Equal(t, "testdb", data[1].DBNamespace)
	assert.Equal(t, "shard2", data[1].Shard)
	assert.Contains(t, data[1].EntityID, "pooler2")
}

func TestEngine_UpdateDetectedProblems_Replacement(t *testing.T) {
	ts := newTestTopoStore()
	defer ts.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	cfg := config.NewTestConfig(config.WithCell("zone1"))

	engine := NewEngine(
		ts,
		logger,
		cfg,
		[]config.WatchTarget{{Database: "testdb"}},
		&rpcclient.FakeClient{},
		newTestCoordinator(ts, &rpcclient.FakeClient{}, "zone1"),
	)

	// Add initial problems
	initialProblems := []types.Problem{
		{
			CheckName: "PrimaryIsDead",
			Scope:     types.ScopePooler,
			ShardKey: commontypes.ShardKey{
				Database:   "testdb",
				TableGroup: "tg1",
				Shard:      "shard1",
			},
			PoolerID: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "pooler1",
			},
		},
	}
	engine.updateDetectedProblems(initialProblems)

	// Replace with different problems
	newProblems := []types.Problem{
		{
			CheckName: "ReplicaNotReplicating",
			Scope:     types.ScopePooler,
			ShardKey: commontypes.ShardKey{
				Database:   "testdb",
				TableGroup: "tg1",
				Shard:      "shard2",
			},
			PoolerID: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "pooler2",
			},
		},
	}
	engine.updateDetectedProblems(newProblems)

	// Verify old problems are gone, new problems are present
	data := engine.collectDetectedProblemsData()
	require.Len(t, data, 1)
	assert.Equal(t, "ReplicaNotReplicating", data[0].AnalysisType)
	assert.Contains(t, data[0].EntityID, "pooler2")
}

func TestEngine_DetectedProblems_ThreadSafety(t *testing.T) {
	ts := newTestTopoStore()
	defer ts.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	cfg := config.NewTestConfig(config.WithCell("zone1"))

	engine := NewEngine(
		ts,
		logger,
		cfg,
		[]config.WatchTarget{{Database: "testdb"}},
		&rpcclient.FakeClient{},
		newTestCoordinator(ts, &rpcclient.FakeClient{}, "zone1"),
	)

	// Concurrent writers and readers
	var wg sync.WaitGroup
	const numWriters = 5
	const numReaders = 10

	// Writers
	for range numWriters {
		wg.Go(func() {
			for range 10 {
				problems := []types.Problem{
					{
						CheckName: "PrimaryIsDead",
						ShardKey: commontypes.ShardKey{
							Database:   "testdb",
							TableGroup: "tg1",
							Shard:      "shard1",
						},
						PoolerID: &clustermetadatapb.ID{
							Component: clustermetadatapb.ID_MULTIPOOLER,
							Cell:      "zone1",
							Name:      "pooler1",
						},
					},
				}
				engine.updateDetectedProblems(problems)
				time.Sleep(time.Millisecond)
			}
		})
	}

	// Readers
	for range numReaders {
		wg.Go(func() {
			for range 10 {
				data := engine.collectDetectedProblemsData()
				// Just verify we can read without panicking
				_ = len(data)
				time.Sleep(time.Millisecond)
			}
		})
	}

	wg.Wait()
}

func TestMetrics_RecoveryActionDuration_WithShardLabels(t *testing.T) {
	metrics, err := NewMetrics()
	require.NoError(t, err)
	require.NotNil(t, metrics)

	ctx := context.Background()

	// Record some durations with shard labels
	metrics.recoveryActionDuration.Record(ctx, 1.5, "FixReplication", "ReplicaNotReplicating", RecoveryActionStatusSuccess, "testdb", "shard1")
	metrics.recoveryActionDuration.Record(ctx, 2.3, "AppointLeader", "PrimaryIsDead", RecoveryActionStatusFailure, "testdb", "shard2")

	// Just verify the metrics API works correctly - we can't easily verify the labels
	// are actually recorded without setting up a full OTel collector, but this ensures
	// the method signatures are correct and no panics occur.
}

func TestMetrics_DetectedProblemsCallback(t *testing.T) {
	metrics, err := NewMetrics()
	require.NoError(t, err)
	require.NotNil(t, metrics)

	var capturedData []DetectedProblemData

	err = metrics.RegisterDetectedProblemsCallback(func() []DetectedProblemData {
		capturedData = []DetectedProblemData{
			{
				AnalysisType: "PrimaryIsDead",
				DBNamespace:  "testdb",
				Shard:        "shard1",
				EntityID:     "pooler1",
			},
		}
		return capturedData
	})
	require.NoError(t, err)

	// The callback registration should succeed.
	// Note: The callback won't actually be invoked unless we trigger OTel metric collection,
	// which requires a full setup with a metric reader. This test just verifies
	// the registration mechanism works.
	_ = capturedData // Prevent unused variable warning
}

func TestMetrics_DetectedProblemsCallback_Nil(t *testing.T) {
	metrics, err := NewMetrics()
	require.NoError(t, err)
	require.NotNil(t, metrics)

	// Nil callback should not error
	err = metrics.RegisterDetectedProblemsCallback(nil)
	require.NoError(t, err)
}

func TestEngine_CollectStreamHealthData(t *testing.T) {
	ts := newTestTopoStore()
	defer ts.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	cfg := config.NewTestConfig(config.WithCell("zone1"))

	engine := NewEngine(
		ts,
		logger,
		cfg,
		[]config.WatchTarget{{Database: "testdb"}},
		&rpcclient.FakeClient{},
		newTestCoordinator(ts, &rpcclient.FakeClient{}, "zone1"),
	)

	// Empty store returns no data.
	data := engine.collectStreamHealthData()
	assert.Empty(t, data)

	// Populate the store with two poolers with different stream states.
	engine.poolerStore.Set("zone1/pooler1", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"},
			Database: "testdb",
			Shard:    "shard1",
		},
		StreamConnected:         true,
		StreamSnapshotsReceived: 42,
	})
	engine.poolerStore.Set("zone1/pooler2", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler2"},
			Database: "testdb",
			Shard:    "shard1",
		},
		StreamConnected:         false,
		StreamSnapshotsReceived: 7,
	})

	data = engine.collectStreamHealthData()
	require.Len(t, data, 2)

	byID := make(map[string]StreamHealthData, len(data))
	for _, d := range data {
		byID[d.PoolerID] = d
	}

	p1, ok := byID["multipooler-zone1-pooler1"]
	require.True(t, ok, "pooler1 should be present")
	assert.True(t, p1.Connected)
	assert.Equal(t, int64(42), p1.SnapshotsReceived)
	assert.Equal(t, "testdb", p1.DBNamespace)
	assert.Equal(t, "shard1", p1.Shard)

	p2, ok := byID["multipooler-zone1-pooler2"]
	require.True(t, ok, "pooler2 should be present")
	assert.False(t, p2.Connected)
	assert.Equal(t, int64(7), p2.SnapshotsReceived)
}

func TestEngine_CollectStreamHealthData_SkipsNilMultiPooler(t *testing.T) {
	ts := newTestTopoStore()
	defer ts.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	cfg := config.NewTestConfig(config.WithCell("zone1"))

	engine := NewEngine(
		ts,
		logger,
		cfg,
		[]config.WatchTarget{{Database: "testdb"}},
		&rpcclient.FakeClient{},
		newTestCoordinator(ts, &rpcclient.FakeClient{}, "zone1"),
	)

	// An entry with nil MultiPooler should be silently skipped.
	engine.poolerStore.Set("zone1/broken", &multiorchdatapb.PoolerHealthState{
		MultiPooler:     nil,
		StreamConnected: true,
	})

	data := engine.collectStreamHealthData()
	assert.Empty(t, data)
}

func TestMetrics_StreamHealthCallback(t *testing.T) {
	metrics, err := NewMetrics()
	require.NoError(t, err)
	require.NotNil(t, metrics)

	// Registration with a valid getter should succeed.
	err = metrics.RegisterStreamHealthCallback(func() []StreamHealthData {
		return []StreamHealthData{
			{PoolerID: "zone1_pooler1", DBNamespace: "testdb", Shard: "shard1", Connected: true, SnapshotsReceived: 10},
			{PoolerID: "zone1_pooler2", DBNamespace: "testdb", Shard: "shard1", Connected: false, SnapshotsReceived: 3},
		}
	})
	require.NoError(t, err)
}

func TestMetrics_StreamHealthCallback_Nil(t *testing.T) {
	metrics, err := NewMetrics()
	require.NoError(t, err)
	require.NotNil(t, metrics)

	// Nil callback should not error.
	err = metrics.RegisterStreamHealthCallback(nil)
	require.NoError(t, err)
}
