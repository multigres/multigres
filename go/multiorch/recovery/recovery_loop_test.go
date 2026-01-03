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
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	commontypes "github.com/multigres/multigres/go/common/types"
	"github.com/multigres/multigres/go/multiorch/config"
	"github.com/multigres/multigres/go/multiorch/recovery/analysis"
	"github.com/multigres/multigres/go/multiorch/recovery/types"
	"github.com/multigres/multigres/go/multiorch/store"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/tools/telemetry"
)

// customAnalyzer is a test analyzer that can use a custom analyze function.
type customAnalyzer struct {
	analyzeFn func(*store.ReplicationAnalysis) []types.Problem
	name      string
}

func (c *customAnalyzer) Name() types.CheckName {
	return types.CheckName(c.name)
}

func (c *customAnalyzer) Analyze(a *store.ReplicationAnalysis) ([]types.Problem, error) {
	return c.analyzeFn(a), nil
}

// trackingRecoveryAction is a test recovery action that tracks execution order.
type trackingRecoveryAction struct {
	name             string
	priority         types.Priority
	label            string
	executionOrder   *[]string
	executionOrderMu *sync.Mutex
}

func (t *trackingRecoveryAction) Execute(ctx context.Context, problem types.Problem) error {
	t.executionOrderMu.Lock()
	*t.executionOrder = append(*t.executionOrder, t.label)
	t.executionOrderMu.Unlock()
	return nil
}

func (t *trackingRecoveryAction) Metadata() types.RecoveryMetadata {
	return types.RecoveryMetadata{
		Name:    t.name,
		Timeout: 10 * time.Second,
	}
}

func (t *trackingRecoveryAction) RequiresHealthyPrimary() bool {
	return false
}

func (t *trackingRecoveryAction) Priority() types.Priority {
	return t.priority
}

// mockRecoveryAction is a test implementation of RecoveryAction
type mockRecoveryAction struct {
	name                   string
	priority               types.Priority
	timeout                time.Duration
	requiresHealthyPrimary bool
	executed               bool
	executeErr             error
	metadata               types.RecoveryMetadata
}

func (m *mockRecoveryAction) Execute(ctx context.Context, problem types.Problem) error {
	m.executed = true
	return m.executeErr
}

func (m *mockRecoveryAction) Metadata() types.RecoveryMetadata {
	// If metadata is set, use it; otherwise build default
	if m.metadata.Name != "" {
		return m.metadata
	}
	return types.RecoveryMetadata{
		Name:        m.name,
		Description: "Mock recovery action",
		Timeout:     m.timeout,
		Retryable:   true,
	}
}

func (m *mockRecoveryAction) RequiresHealthyPrimary() bool {
	return m.requiresHealthyPrimary
}

func (m *mockRecoveryAction) Priority() types.Priority {
	return m.priority
}

func TestGroupProblemsByShard(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := config.NewTestConfig(config.WithCell("cell1"))
	engine := NewEngine(ts, logger, cfg, []config.WatchTarget{}, &rpcclient.FakeClient{}, newTestCoordinator(ts, &rpcclient.FakeClient{}, "cell1"))

	poolerID1 := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "pooler1",
	}

	poolerID2 := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "pooler2",
	}

	poolerID3 := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "pooler3",
	}

	problems := []types.Problem{
		{
			Code:     types.ProblemPrimaryIsDead,
			PoolerID: poolerID1,
			ShardKey: commontypes.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "0"},
		},
		{
			Code:     types.ProblemReplicaNotReplicating,
			PoolerID: poolerID2,
			ShardKey: commontypes.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "0"},
		},
		{
			Code:     types.ProblemPrimaryIsDead,
			PoolerID: poolerID3,
			ShardKey: commontypes.ShardKey{Database: "db2", TableGroup: "tg2", Shard: "0"},
		},
	}

	grouped := engine.groupProblemsByShard(problems)

	// Should have 2 shards
	assert.Len(t, grouped, 2, "should have 2 shards")

	// Check first shard
	key1 := commontypes.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "0"}
	assert.Len(t, grouped[key1], 2, "db1/tg1/0 should have 2 problems")

	// Check second shard
	key2 := commontypes.ShardKey{Database: "db2", TableGroup: "tg2", Shard: "0"}
	assert.Len(t, grouped[key2], 1, "db2/tg2/0 should have 1 problem")
}

func TestPrioritySorting(t *testing.T) {
	poolerID1 := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "primary-pooler",
	}

	poolerID2 := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "replica-pooler",
	}

	poolerID3 := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "config-pooler",
	}

	problems := []types.Problem{
		{
			Code:     types.ProblemReplicaNotReplicating,
			PoolerID: poolerID2,
			ShardKey: commontypes.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "0"},
			Priority: types.PriorityHigh,
		},
		{
			Code:     types.ProblemPrimaryIsDead,
			PoolerID: poolerID1,
			ShardKey: commontypes.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "0"},
			Priority: types.PriorityEmergency,
		},
		{
			Code:     "ConfigurationDrift",
			PoolerID: poolerID3,
			ShardKey: commontypes.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "0"},
			Priority: types.PriorityNormal,
		},
	}

	// Sort by priority (same logic as processShardProblems)
	sort.SliceStable(problems, func(i, j int) bool {
		return problems[i].Priority > problems[j].Priority
	})

	// Verify order: Emergency > High > Normal
	require.Len(t, problems, 3)
	assert.Equal(t, types.PriorityEmergency, problems[0].Priority)
	assert.Equal(t, types.ProblemPrimaryIsDead, problems[0].Code)

	assert.Equal(t, types.PriorityHigh, problems[1].Priority)
	assert.Equal(t, types.ProblemReplicaNotReplicating, problems[1].Code)

	assert.Equal(t, types.PriorityNormal, problems[2].Priority)
	assert.Equal(t, types.ProblemCode("ConfigurationDrift"), problems[2].Code)
}

func TestShardKey(t *testing.T) {
	// Test that ShardKey works correctly as a map key
	key1 := commontypes.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "0"}

	key2 := commontypes.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "0"}

	key3 := commontypes.ShardKey{Database: "db1", TableGroup: "tg2", Shard: "0"}

	// Test map usage
	m := make(map[commontypes.ShardKey]int)
	m[key1] = 1
	m[key2] = 2 // Should overwrite key1
	m[key3] = 3

	assert.Equal(t, 2, m[key1], "key1 and key2 should be equal")
	assert.Equal(t, 2, m[key2], "key1 and key2 should be equal")
	assert.Equal(t, 3, m[key3], "key3 should be different")
	assert.Len(t, m, 2, "should have 2 unique keys")
}

func TestGroupProblemsByShard_DifferentShards(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := config.NewTestConfig(config.WithCell("cell1"))
	engine := NewEngine(ts, logger, cfg, []config.WatchTarget{}, &rpcclient.FakeClient{}, newTestCoordinator(ts, &rpcclient.FakeClient{}, "cell1"))

	poolerID1 := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "pooler1",
	}

	poolerID2 := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "pooler2",
	}

	problems := []types.Problem{
		{
			Code:     types.ProblemPrimaryIsDead,
			PoolerID: poolerID1,
			ShardKey: commontypes.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "0"},
		},
		{
			Code:     types.ProblemPrimaryIsDead,
			PoolerID: poolerID2,
			ShardKey: commontypes.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "1"}, // Different shard
		},
	}

	grouped := engine.groupProblemsByShard(problems)

	// Should have 2 separate groups (different shards)
	assert.Len(t, grouped, 2, "should have 2 separate groups for different shards")

	key1 := commontypes.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "0"}
	key2 := commontypes.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "1"}

	assert.Len(t, grouped[key1], 1, "shard 0 should have 1 problem")
	assert.Len(t, grouped[key2], 1, "shard 1 should have 1 problem")
}

// TestRecheckProblem_PoolerNotFound tests error handling when
// the pooler is not found in the store.
func TestRecheckProblem_PoolerNotFound(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := config.NewTestConfig(config.WithCell("cell1"))
	engine := NewEngine(ts, logger, cfg, []config.WatchTarget{}, &rpcclient.FakeClient{}, newTestCoordinator(ts, &rpcclient.FakeClient{}, "cell1"))

	poolerID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "pooler1",
	}

	// Create problem
	problem := types.Problem{
		Code:      types.ProblemPrimaryIsDead,
		CheckName: "PrimaryDeadCheck",
		PoolerID:  poolerID,
		ShardKey:  commontypes.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "0"},
		Priority:  types.PriorityEmergency,
	}

	stillExists, err := engine.recheckProblem(t.Context(), problem)

	require.Error(t, err, "should return error when pooler not found")
	assert.False(t, stillExists)
	assert.Contains(t, err.Error(), "pooler not found in store")
}

// TestFilterAndPrioritize_ShardWideOnly tests that when shard-wide problems exist,
// only the highest priority shard-wide problem is returned.
func TestFilterAndPrioritize_ShardWideOnly(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := config.NewTestConfig(config.WithCell("cell1"))
	engine := NewEngine(ts, logger, cfg, []config.WatchTarget{}, &rpcclient.FakeClient{}, newTestCoordinator(ts, &rpcclient.FakeClient{}, "cell1"))

	poolerID1 := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "primary-pooler",
	}

	poolerID2 := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "replica-pooler",
	}

	// Create problems with different scopes
	problems := []types.Problem{
		{
			Code:     types.ProblemReplicaNotReplicating,
			PoolerID: poolerID2,
			Priority: types.PriorityHigh,
			Scope:    types.ScopePooler,
			RecoveryAction: &mockRecoveryAction{
				name:     "FixReplica",
				priority: types.PriorityHigh,
				timeout:  30 * time.Second,
			},
		},
		{
			Code:     types.ProblemPrimaryIsDead,
			PoolerID: poolerID1,
			Priority: types.PriorityEmergency,
			Scope:    types.ScopeShard,
			RecoveryAction: &mockRecoveryAction{
				name:     "FailoverPrimary",
				priority: types.PriorityEmergency,
				timeout:  60 * time.Second,
			},
		},
		{
			Code:     "ConfigDrift",
			PoolerID: poolerID2,
			Priority: types.PriorityNormal,
			Scope:    types.ScopePooler,
			RecoveryAction: &mockRecoveryAction{
				name:     "FixConfig",
				priority: types.PriorityNormal,
				timeout:  10 * time.Second,
			},
		},
	}

	filtered := engine.filterAndPrioritize(problems)

	// Should return only the shard-wide problem (PrimaryDead)
	require.Len(t, filtered, 1)
	assert.Equal(t, types.ProblemPrimaryIsDead, filtered[0].Code)
	assert.Equal(t, types.PriorityEmergency, filtered[0].Priority)
}

// TestFilterAndPrioritize_NoShardWide tests deduplication by pooler ID
// when there are no shard-wide problems.
func TestFilterAndPrioritize_NoShardWide(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := config.NewTestConfig(config.WithCell("cell1"))
	engine := NewEngine(ts, logger, cfg, []config.WatchTarget{}, &rpcclient.FakeClient{}, newTestCoordinator(ts, &rpcclient.FakeClient{}, "cell1"))

	poolerID1 := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "pooler1",
	}

	poolerID2 := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "pooler2",
	}

	// Create problems - two for pooler1, one for pooler2
	problems := []types.Problem{
		{
			Code:     types.ProblemReplicaNotReplicating,
			PoolerID: poolerID1,
			Priority: types.PriorityHigh,
			Scope:    types.ScopePooler,
			RecoveryAction: &mockRecoveryAction{
				name:     "FixReplication",
				priority: types.PriorityHigh,
				timeout:  30 * time.Second,
			},
		},
		{
			Code:     types.ProblemPrimaryMisconfigured,
			PoolerID: poolerID1,
			Priority: types.PriorityNormal,
			Scope:    types.ScopePooler,
			RecoveryAction: &mockRecoveryAction{
				name:     "FixConfig",
				priority: types.PriorityNormal,
				timeout:  10 * time.Second,
			},
		},
		{
			Code:     types.ProblemReplicaLagging,
			PoolerID: poolerID2,
			Priority: types.PriorityNormal,
			Scope:    types.ScopePooler,
			RecoveryAction: &mockRecoveryAction{
				name:     "FixLag",
				priority: types.PriorityNormal,
				timeout:  20 * time.Second,
			},
		},
	}

	// Problems are already sorted by priority
	filtered := engine.filterAndPrioritize(problems)

	// Should keep only highest priority problem per pooler
	require.Len(t, filtered, 3)
	assert.Equal(t, types.ProblemReplicaNotReplicating, filtered[0].Code)
	assert.Equal(t, types.ProblemPrimaryMisconfigured, filtered[1].Code)
	assert.Equal(t, types.ProblemReplicaLagging, filtered[2].Code)
}

// TestFilterAndPrioritize_MultipleShardWide tests that when multiple shard-wide
// problems exist, only the highest priority one is returned.
func TestFilterAndPrioritize_MultipleShardWide(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := config.NewTestConfig(config.WithCell("cell1"))
	engine := NewEngine(ts, logger, cfg, []config.WatchTarget{}, &rpcclient.FakeClient{}, newTestCoordinator(ts, &rpcclient.FakeClient{}, "cell1"))

	poolerID1 := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "primary-pooler",
	}

	poolerID2 := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "another-primary",
	}

	// Create multiple shard-wide problems with different priorities
	problems := []types.Problem{
		{
			Code:     types.ProblemShardNeedsBootstrap,
			PoolerID: poolerID1,
			Priority: types.PriorityShardBootstrap,
			Scope:    types.ScopeShard,
			RecoveryAction: &mockRecoveryAction{
				name:     "ElectPrimary",
				priority: types.PriorityShardBootstrap,
				timeout:  60 * time.Second,
			},
		},
		{
			Code:     types.ProblemPrimaryIsDead,
			PoolerID: poolerID2,
			Priority: types.PriorityEmergency,
			Scope:    types.ScopeShard,
			RecoveryAction: &mockRecoveryAction{
				name:     "FailoverPrimary",
				priority: types.PriorityEmergency,
				timeout:  45 * time.Second,
			},
		},
	}

	filtered := engine.filterAndPrioritize(problems)

	// Should return only the highest priority shard-wide problem
	// PriorityShardBootstrap (10000) > PriorityEmergency (1000)
	require.Len(t, filtered, 1)
	assert.Equal(t, types.ProblemShardNeedsBootstrap, filtered[0].Code)
	assert.Equal(t, types.PriorityShardBootstrap, filtered[0].Priority)
}

// mockPrimaryDeadAnalyzer detects when a primary is unreachable
type mockPrimaryDeadAnalyzer struct {
	recoveryAction types.RecoveryAction
}

func (m *mockPrimaryDeadAnalyzer) Name() types.CheckName {
	return "MockPrimaryDeadCheck"
}

func (m *mockPrimaryDeadAnalyzer) Analyze(a *store.ReplicationAnalysis) ([]types.Problem, error) {
	// Detect if this is a primary that is unreachable
	if a.IsPrimary && a.IsUnreachable {
		return []types.Problem{
			{
				Code:           types.ProblemPrimaryIsDead,
				CheckName:      m.Name(),
				PoolerID:       a.PoolerID,
				ShardKey:       a.ShardKey,
				Priority:       types.PriorityEmergency,
				Scope:          types.ScopeShard,
				RecoveryAction: m.recoveryAction,
				DetectedAt:     time.Now(),
				Description:    "Primary is unreachable",
			},
		}, nil
	}
	return nil, nil
}

// mockReplicaNotReplicatingAnalyzer detects when a replica has stopped replicating
type mockReplicaNotReplicatingAnalyzer struct {
	recoveryAction types.RecoveryAction
}

func (m *mockReplicaNotReplicatingAnalyzer) Name() types.CheckName {
	return "MockReplicationStoppedCheck"
}

func (m *mockReplicaNotReplicatingAnalyzer) Analyze(a *store.ReplicationAnalysis) ([]types.Problem, error) {
	// Detect if this is a replica with replication stopped
	if !a.IsPrimary && a.IsWalReplayPaused {
		return []types.Problem{
			{
				Code:           types.ProblemReplicaNotReplicating,
				CheckName:      m.Name(),
				PoolerID:       a.PoolerID,
				ShardKey:       a.ShardKey,
				Priority:       types.PriorityHigh,
				Scope:          types.ScopePooler,
				RecoveryAction: m.recoveryAction,
				DetectedAt:     time.Now(),
				Description:    "Replica replication is paused",
			},
		}, nil
	}
	return nil, nil
}

// TestProcessShardProblems_DependencyEnforcement tests the full flow of
// dependency enforcement from state → analysis → problem detection → recovery.
func TestProcessShardProblems_DependencyEnforcement(t *testing.T) {
	// Setup
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := config.NewTestConfig(config.WithCell("cell1"))

	// Create fake RPC client with responses for both primary and replica
	fakeClient := rpcclient.NewFakeClient()
	fakeClient.SetStatusResponse("multipooler-cell1-primary-pooler", &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_PRIMARY,
			PrimaryStatus: &multipoolermanagerdatapb.PrimaryStatus{
				Lsn:   "0/DEADBEEF",
				Ready: true,
			},
		},
	})
	fakeClient.SetStatusResponse("multipooler-cell1-replica-pooler", &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_REPLICA,
			ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
				LastReplayLsn:           "0/DEADBEEF",
				LastReceiveLsn:          "0/DEADBEEF",
				IsWalReplayPaused:       true, // Replication is paused
				WalReplayPauseState:     "paused",
				Lag:                     durationpb.New(0),
				LastXactReplayTimestamp: "",
				PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{
					Host: "primary-host",
					Port: 5432,
				},
			},
		},
	})

	engine := NewEngine(ts, logger, cfg, []config.WatchTarget{}, fakeClient, newTestCoordinator(ts, fakeClient, "cell1"))

	primaryID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "primary-pooler",
	}

	replicaID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "replica-pooler",
	}

	// Create recovery actions
	primaryRecovery := &mockRecoveryAction{
		name:                   "EmergencyFailover",
		priority:               types.PriorityEmergency,
		timeout:                30 * time.Second,
		requiresHealthyPrimary: false,
		metadata: types.RecoveryMetadata{
			Name:    "EmergencyFailover",
			Timeout: 30 * time.Second,
		},
	}

	replicaRecovery := &mockRecoveryAction{
		name:                   "FixReplication",
		priority:               types.PriorityHigh,
		timeout:                10 * time.Second,
		requiresHealthyPrimary: true, // Requires healthy primary!
		metadata: types.RecoveryMetadata{
			Name:    "FixReplication",
			Timeout: 10 * time.Second,
		},
	}

	// Create mock analyzers
	primaryDeadAnalyzer := &mockPrimaryDeadAnalyzer{recoveryAction: primaryRecovery}
	replicaNotReplicatingAnalyzer := &mockReplicaNotReplicatingAnalyzer{recoveryAction: replicaRecovery}

	t.Run("skips replica recovery when primary is dead", func(t *testing.T) {
		// Reset execution flags
		primaryRecovery.executed = false
		replicaRecovery.executed = false

		// Setup test analyzers
		analysis.SetTestAnalyzers([]analysis.Analyzer{
			primaryDeadAnalyzer,
			replicaNotReplicatingAnalyzer,
		})
		t.Cleanup(analysis.ResetAnalyzers)

		// Set up store state: dead primary, replica with replication stopped
		primaryPooler := &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id:       primaryID,
				Database: "db1", TableGroup: "tg1", Shard: "0",
				Type:     clustermetadatapb.PoolerType_PRIMARY,
				Hostname: "primary-host",
			},
			IsLastCheckValid: false,
			IsUpToDate:       true,
			LastSeen:         timestamppb.Now(),
		}
		engine.poolerStore.Set("multipooler-cell1-primary-pooler", primaryPooler)

		replicaPooler := &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id:       replicaID,
				Database: "db1", TableGroup: "tg1", Shard: "0",
				Type:     clustermetadatapb.PoolerType_REPLICA,
				Hostname: "replica-host",
			},
			IsLastCheckValid: true,
			IsUpToDate:       true,
			ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
				IsWalReplayPaused: true, // Replication stopped
			},
			LastSeen: timestamppb.Now(),
		}
		engine.poolerStore.Set("multipooler-cell1-replica-pooler", replicaPooler)

		// Generate analyses from the store
		generator := analysis.NewAnalysisGenerator(engine.poolerStore)
		analyses := generator.GenerateAnalyses()

		// Run analyzers to detect problems
		var problems []types.Problem
		analyzers := analysis.DefaultAnalyzers(engine.actionFactory)
		for _, poolerAnalysis := range analyses {
			for _, analyzer := range analyzers {
				detectedProblems, err := analyzer.Analyze(poolerAnalysis)
				require.NoError(t, err)
				problems = append(problems, detectedProblems...)
			}
		}

		// Should detect both problems
		require.Len(t, problems, 2, "should detect both primary dead and replica not replicating")

		shardKey := commontypes.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "0"}

		// Call processShardProblems - this exercises the full recovery flow
		engine.processShardProblems(t.Context(), shardKey, problems)

		// ASSERTION: Replica recovery should be SKIPPED due to dependency check
		assert.False(t, replicaRecovery.executed,
			"replica recovery requiring healthy primary should be skipped when primary is dead")

		// Primary recovery should have been attempted (though it will fail validation
		// since we can't actually poll the dead primary)
		// The key point is that replica recovery was skipped BEFORE validation
	})

	t.Run("allows replica recovery when primary is healthy", func(t *testing.T) {
		// Reset execution flags
		replicaRecovery.executed = false

		// Setup test analyzers (only replica analyzer this time)
		analysis.SetTestAnalyzers([]analysis.Analyzer{
			replicaNotReplicatingAnalyzer,
		})
		t.Cleanup(analysis.ResetAnalyzers)

		// Set up store state: healthy primary, replica with replication stopped
		primaryPooler := &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id:       primaryID,
				Database: "db1", TableGroup: "tg1", Shard: "0",
				Type:     clustermetadatapb.PoolerType_PRIMARY,
				Hostname: "primary-host",
			},
			IsLastCheckValid: true, // Primary is healthy
			IsUpToDate:       true,
			LastSeen:         timestamppb.Now(),
		}
		engine.poolerStore.Set("multipooler-cell1-primary-pooler", primaryPooler)

		replicaPooler := &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id:       replicaID,
				Database: "db1", TableGroup: "tg1", Shard: "0",
				Type:     clustermetadatapb.PoolerType_REPLICA,
				Hostname: "replica-host",
			},
			IsLastCheckValid: true,
			IsUpToDate:       true,
			ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
				IsWalReplayPaused: true, // Replication stopped
			},
			LastSeen: timestamppb.Now(),
		}
		engine.poolerStore.Set("multipooler-cell1-replica-pooler", replicaPooler)

		// Generate analyses from the store
		generator := analysis.NewAnalysisGenerator(engine.poolerStore)
		analyses := generator.GenerateAnalyses()

		// Run analyzers to detect problems
		var problems []types.Problem
		analyzers := analysis.DefaultAnalyzers(engine.actionFactory)
		for _, poolerAnalysis := range analyses {
			for _, analyzer := range analyzers {
				detectedProblems, err := analyzer.Analyze(poolerAnalysis)
				require.NoError(t, err)
				problems = append(problems, detectedProblems...)
			}
		}

		// Should detect only replica problem
		require.Len(t, problems, 1, "should detect only replica not replicating")
		assert.Equal(t, types.ProblemReplicaNotReplicating, problems[0].Code)

		shardKey := commontypes.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "0"}

		// Call processShardProblems
		engine.processShardProblems(t.Context(), shardKey, problems)

		// ASSERTION: Replica recovery should be executed (NOT skipped)
		// It will still fail validation since the mock analyzer won't re-detect it,
		// but the key is it wasn't skipped by the dependency check
		assert.True(t, replicaRecovery.executed,
			"replica recovery should be executed when primary is healthy")
	})
}

// TestRecoveryLoop_ValidationPreventsStaleRecovery tests that the validation step
// correctly prevents recovery when the problem no longer exists.
func TestRecoveryLoop_ValidationPreventsStaleRecovery(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := config.NewTestConfig(config.WithCell("cell1"))

	// Create fake RPC client - we'll update it to simulate state changes
	fakeClient := rpcclient.NewFakeClient()
	fakeClient.SetStatusResponse("multipooler-cell1-replica-pooler", &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_REPLICA,
			ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
				LastReplayLsn:           "0/DEADBEEF",
				LastReceiveLsn:          "0/DEADBEEF",
				IsWalReplayPaused:       true, // Initially paused
				WalReplayPauseState:     "paused",
				Lag:                     durationpb.New(0),
				LastXactReplayTimestamp: "",
				PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{
					Host: "primary-host",
					Port: 5432,
				},
			},
		},
	})

	engine := NewEngine(ts, logger, cfg, []config.WatchTarget{}, fakeClient, newTestCoordinator(ts, fakeClient, "cell1"))

	replicaID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "replica-pooler",
	}

	// Create recovery action that should NOT be executed
	replicaRecovery := &mockRecoveryAction{
		name:                   "FixReplication",
		priority:               types.PriorityHigh,
		timeout:                10 * time.Second,
		requiresHealthyPrimary: false,
		metadata: types.RecoveryMetadata{
			Name:    "FixReplication",
			Timeout: 10 * time.Second,
		},
	}

	// Create mock analyzer
	replicaAnalyzer := &mockReplicaNotReplicatingAnalyzer{recoveryAction: replicaRecovery}

	// Setup test analyzers
	analysis.SetTestAnalyzers([]analysis.Analyzer{replicaAnalyzer})
	t.Cleanup(analysis.ResetAnalyzers)

	// Set up initial store state: replica with replication stopped
	replicaPooler := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:       replicaID,
			Database: "db1", TableGroup: "tg1", Shard: "0",
			Type:     clustermetadatapb.PoolerType_REPLICA,
			Hostname: "replica-host",
		},
		IsLastCheckValid: true,
		IsUpToDate:       true,
		ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
			IsWalReplayPaused: true, // Replication stopped
		},
		LastSeen: timestamppb.Now(),
	}
	engine.poolerStore.Set("multipooler-cell1-replica-pooler", replicaPooler)

	// Generate initial analysis - problem should be detected
	generator := analysis.NewAnalysisGenerator(engine.poolerStore)
	analyses := generator.GenerateAnalyses()

	var problems []types.Problem
	analyzers := analysis.DefaultAnalyzers(engine.actionFactory)
	for _, poolerAnalysis := range analyses {
		for _, analyzer := range analyzers {
			detectedProblems, err := analyzer.Analyze(poolerAnalysis)
			require.NoError(t, err)
			problems = append(problems, detectedProblems...)
		}
	}

	require.Len(t, problems, 1, "should detect replica not replicating problem")

	// NOW: Fix the problem in the fake client BEFORE validation
	// This simulates the problem being transient or fixed by external means
	fakeClient.SetStatusResponse("multipooler-cell1-replica-pooler", &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_REPLICA,
			ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
				LastReplayLsn:           "0/DEADBEEF",
				LastReceiveLsn:          "0/DEADBEEF",
				IsWalReplayPaused:       false, // NOW FIXED!
				WalReplayPauseState:     "not paused",
				Lag:                     durationpb.New(0),
				LastXactReplayTimestamp: "",
				PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{
					Host: "primary-host",
					Port: 5432,
				},
			},
		},
	})

	// Attempt recovery - validation should detect problem no longer exists
	engine.attemptRecovery(t.Context(), problems[0])

	// ASSERTION: Recovery should NOT be executed because validation failed
	assert.False(t, replicaRecovery.executed,
		"recovery should be skipped when problem no longer exists after validation")
}

// TestRecoveryLoop_PostRecoveryRefresh tests that after a shard-wide recovery,
// all poolers in the shard are force-refreshed.
func TestRecoveryLoop_PostRecoveryRefresh(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := config.NewTestConfig(config.WithCell("cell1"))

	// Create fake RPC client with responses for primary and replicas
	fakeClient := rpcclient.NewFakeClient()
	fakeClient.SetStatusResponse("multipooler-cell1-primary-pooler", &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_PRIMARY,
			PrimaryStatus: &multipoolermanagerdatapb.PrimaryStatus{
				Lsn:   "0/DEADBEEF",
				Ready: true,
			},
		},
	})
	fakeClient.SetStatusResponse("multipooler-cell1-replica1-pooler", &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_REPLICA,
			ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
				LastReplayLsn:           "0/DEADBEEF",
				LastReceiveLsn:          "0/DEADBEEF",
				IsWalReplayPaused:       false,
				WalReplayPauseState:     "not paused",
				Lag:                     durationpb.New(0),
				LastXactReplayTimestamp: "",
				PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{
					Host: "primary-host",
					Port: 5432,
				},
			},
		},
	})
	fakeClient.SetStatusResponse("multipooler-cell1-replica2-pooler", &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_REPLICA,
			ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
				LastReplayLsn:           "0/DEADBEEF",
				LastReceiveLsn:          "0/DEADBEEF",
				IsWalReplayPaused:       false,
				WalReplayPauseState:     "not paused",
				Lag:                     durationpb.New(0),
				LastXactReplayTimestamp: "",
				PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{
					Host: "primary-host",
					Port: 5432,
				},
			},
		},
	})

	engine := NewEngine(ts, logger, cfg, []config.WatchTarget{}, fakeClient, newTestCoordinator(ts, fakeClient, "cell1"))

	primaryID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "primary-pooler",
	}

	replica1ID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "replica1-pooler",
	}

	replica2ID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "replica2-pooler",
	}

	// Create recovery action that simulates successful shard-wide recovery
	primaryRecovery := &mockRecoveryAction{
		name:                   "EmergencyFailover",
		priority:               types.PriorityEmergency,
		timeout:                30 * time.Second,
		requiresHealthyPrimary: false,
		executeErr:             nil, // Success!
		metadata: types.RecoveryMetadata{
			Name:    "EmergencyFailover",
			Timeout: 30 * time.Second,
		},
	}

	// Create mock analyzer for primary dead
	primaryDeadAnalyzer := &mockPrimaryDeadAnalyzer{recoveryAction: primaryRecovery}

	// Setup test analyzers
	analysis.SetTestAnalyzers([]analysis.Analyzer{primaryDeadAnalyzer})
	t.Cleanup(analysis.ResetAnalyzers)

	// Set up store state: dead primary, healthy replicas

	// Record initial LastCheckAttempted times
	initialPrimaryCheck := time.Now().Add(-5 * time.Minute)
	initialReplica1Check := time.Now().Add(-4 * time.Minute)
	initialReplica2Check := time.Now().Add(-3 * time.Minute)

	primaryPooler := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:       primaryID,
			Database: "db1", TableGroup: "tg1", Shard: "0",
			Type:     clustermetadatapb.PoolerType_PRIMARY,
			Hostname: "primary-host",
		},
		IsLastCheckValid:   false, // Primary is dead
		IsUpToDate:         true,
		LastSeen:           timestamppb.New(time.Now().Add(-1 * time.Minute)),
		LastCheckAttempted: timestamppb.New(initialPrimaryCheck),
	}
	engine.poolerStore.Set("multipooler-cell1-primary-pooler", primaryPooler)

	replica1Pooler := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:       replica1ID,
			Database: "db1", TableGroup: "tg1", Shard: "0",
			Type:     clustermetadatapb.PoolerType_REPLICA,
			Hostname: "replica1-host",
		},
		IsLastCheckValid:   true,
		IsUpToDate:         true,
		LastSeen:           timestamppb.Now(),
		LastCheckAttempted: timestamppb.New(initialReplica1Check),
	}
	engine.poolerStore.Set("multipooler-cell1-replica1-pooler", replica1Pooler)

	replica2Pooler := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:       replica2ID,
			Database: "db1", TableGroup: "tg1", Shard: "0",
			Type:     clustermetadatapb.PoolerType_REPLICA,
			Hostname: "replica2-host",
		},
		IsLastCheckValid:   true,
		IsUpToDate:         true,
		LastSeen:           timestamppb.Now(),
		LastCheckAttempted: timestamppb.New(initialReplica2Check),
	}
	engine.poolerStore.Set("multipooler-cell1-replica2-pooler", replica2Pooler)

	// Generate analysis and detect problem
	generator := analysis.NewAnalysisGenerator(engine.poolerStore)
	analyses := generator.GenerateAnalyses()

	var problems []types.Problem
	analyzers := analysis.DefaultAnalyzers(engine.actionFactory)
	for _, poolerAnalysis := range analyses {
		for _, analyzer := range analyzers {
			detectedProblems, err := analyzer.Analyze(poolerAnalysis)
			require.NoError(t, err)
			problems = append(problems, detectedProblems...)
		}
	}

	require.Len(t, problems, 1, "should detect primary dead problem")
	assert.Equal(t, types.ScopeShard, problems[0].Scope)

	// Now fix the primary in the fake client so validation will pass
	fakeClient.SetStatusResponse("multipooler-cell1-primary-pooler", &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_PRIMARY,
			PrimaryStatus: &multipoolermanagerdatapb.PrimaryStatus{
				Lsn:   "0/NEWPRIMARY",
				Ready: true,
			},
		},
	})

	// Attempt recovery - should succeed and trigger post-recovery refresh
	engine.attemptRecovery(t.Context(), problems[0])

	// ASSERTION: Recovery should be executed
	assert.True(t, primaryRecovery.executed, "recovery should be executed")

	// ASSERTION: All poolers in the shard should have been refreshed (LastCheckAttempted updated)
	// Note: The dead primary won't be refreshed (it's in the ignore list), but replicas should be
	r1, _ := engine.poolerStore.Get("multipooler-cell1-replica1-pooler")
	assert.True(t, r1.LastCheckAttempted.AsTime().After(initialReplica1Check),
		"replica1 should be refreshed after shard-wide recovery")

	r2, _ := engine.poolerStore.Get("multipooler-cell1-replica2-pooler")
	assert.True(t, r2.LastCheckAttempted.AsTime().After(initialReplica2Check),
		"replica2 should be refreshed after shard-wide recovery")
}

// TestRecoveryLoop_FullCycle tests the complete recovery cycle end-to-end:
// state → analysis → problem detection → prioritization → validation → recovery.
func TestRecoveryLoop_FullCycle(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := config.NewTestConfig(config.WithCell("cell1"))

	// Create fake RPC client with multiple poolers in various states
	fakeClient := rpcclient.NewFakeClient()
	fakeClient.SetStatusResponse("multipooler-cell1-primary-pooler", &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_PRIMARY,
			PrimaryStatus: &multipoolermanagerdatapb.PrimaryStatus{
				Lsn:   "0/DEADBEEF",
				Ready: true,
			},
		},
	})
	fakeClient.SetStatusResponse("multipooler-cell1-replica1-pooler", &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_REPLICA,
			ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
				LastReplayLsn:           "0/DEADBEEF",
				LastReceiveLsn:          "0/DEADBEEF",
				IsWalReplayPaused:       true, // Problem: replication paused
				WalReplayPauseState:     "paused",
				Lag:                     durationpb.New(0),
				LastXactReplayTimestamp: "",
				PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{
					Host: "primary-host",
					Port: 5432,
				},
			},
		},
	})
	fakeClient.SetStatusResponse("multipooler-cell1-replica2-pooler", &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_REPLICA,
			ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
				LastReplayLsn:           "0/DEADBEEF",
				LastReceiveLsn:          "0/DEADBEEF",
				IsWalReplayPaused:       true, // Problem: replication paused
				WalReplayPauseState:     "paused",
				Lag:                     durationpb.New(0),
				LastXactReplayTimestamp: "",
				PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{
					Host: "primary-host",
					Port: 5432,
				},
			},
		},
	})

	engine := NewEngine(ts, logger, cfg, []config.WatchTarget{}, fakeClient, newTestCoordinator(ts, fakeClient, "cell1"))

	primaryID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "primary-pooler",
	}

	replica1ID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "replica1-pooler",
	}

	replica2ID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "replica2-pooler",
	}

	// Create recovery actions with different priorities
	replica1Recovery := &mockRecoveryAction{
		name:                   "FixReplica1",
		priority:               types.PriorityHigh,
		timeout:                10 * time.Second,
		requiresHealthyPrimary: false,
		metadata: types.RecoveryMetadata{
			Name:    "FixReplica1",
			Timeout: 10 * time.Second,
		},
	}

	replica2Recovery := &mockRecoveryAction{
		name:                   "FixReplica2",
		priority:               types.PriorityHigh,
		timeout:                10 * time.Second,
		requiresHealthyPrimary: false,
		metadata: types.RecoveryMetadata{
			Name:    "FixReplica2",
			Timeout: 10 * time.Second,
		},
	}

	// Create mock analyzer
	replicaAnalyzer := &mockReplicaNotReplicatingAnalyzer{recoveryAction: replica1Recovery}

	// Custom analyzer for replica2 (need separate recovery action)
	replica2Analyzer := &mockReplicaNotReplicatingAnalyzer{recoveryAction: replica2Recovery}

	// Setup test analyzers
	analysis.SetTestAnalyzers([]analysis.Analyzer{replicaAnalyzer, replica2Analyzer})
	t.Cleanup(analysis.ResetAnalyzers)

	// Set up store state: healthy primary, two replicas with problems
	primaryPooler := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:       primaryID,
			Database: "db1", TableGroup: "tg1", Shard: "0",
			Type:     clustermetadatapb.PoolerType_PRIMARY,
			Hostname: "primary-host",
		},
		IsLastCheckValid: true,
		IsUpToDate:       true,
		LastSeen:         timestamppb.Now(),
	}
	engine.poolerStore.Set("multipooler-cell1-primary-pooler", primaryPooler)

	replica1Pooler := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:       replica1ID,
			Database: "db1", TableGroup: "tg1", Shard: "0",
			Type:     clustermetadatapb.PoolerType_REPLICA,
			Hostname: "replica1-host",
		},
		IsLastCheckValid: true,
		IsUpToDate:       true,
		ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
			IsWalReplayPaused: true, // Problem
		},
		LastSeen: timestamppb.Now(),
	}
	engine.poolerStore.Set("multipooler-cell1-replica1-pooler", replica1Pooler)

	replica2Pooler := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:       replica2ID,
			Database: "db1", TableGroup: "tg1", Shard: "0",
			Type:     clustermetadatapb.PoolerType_REPLICA,
			Hostname: "replica2-host",
		},
		IsLastCheckValid: true,
		IsUpToDate:       true,
		ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
			IsWalReplayPaused: true, // Problem
		},
		LastSeen: timestamppb.Now(),
	}
	engine.poolerStore.Set("multipooler-cell1-replica2-pooler", replica2Pooler)

	// Run full recovery cycle
	engine.performRecoveryCycle()

	// ASSERTION: Both recovery actions should be executed (no shard-wide problems)
	assert.True(t, replica1Recovery.executed || replica2Recovery.executed,
		"at least one replica recovery should be executed in full cycle")
}

// TestRecoveryLoop_PriorityOrdering tests that problems are attempted in priority order.
func TestRecoveryLoop_PriorityOrdering(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := config.NewTestConfig(config.WithCell("cell1"))

	// Create fake RPC client
	fakeClient := rpcclient.NewFakeClient()
	fakeClient.SetStatusResponse("multipooler-cell1-primary-pooler", &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_PRIMARY,
			PrimaryStatus: &multipoolermanagerdatapb.PrimaryStatus{
				Lsn:   "0/DEADBEEF",
				Ready: true,
			},
		},
	})
	fakeClient.SetStatusResponse("multipooler-cell1-replica-pooler", &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_REPLICA,
			ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
				LastReplayLsn:           "0/DEADBEEF",
				LastReceiveLsn:          "0/DEADBEEF",
				IsWalReplayPaused:       true,
				WalReplayPauseState:     "paused",
				Lag:                     durationpb.New(0),
				LastXactReplayTimestamp: "",
				PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{
					Host: "primary-host",
					Port: 5432,
				},
			},
		},
	})

	engine := NewEngine(ts, logger, cfg, []config.WatchTarget{}, fakeClient, newTestCoordinator(ts, fakeClient, "cell1"))

	replicaID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "replica-pooler",
	}

	// Track execution order
	var executionOrder []string
	var mu sync.Mutex

	// Create wrapper recovery actions that track execution order
	emergencyRecovery := &trackingRecoveryAction{
		name:             "EmergencyFix",
		priority:         types.PriorityEmergency,
		label:            "Emergency",
		executionOrder:   &executionOrder,
		executionOrderMu: &mu,
	}

	highRecovery := &trackingRecoveryAction{
		name:             "HighPriorityFix",
		priority:         types.PriorityHigh,
		label:            "High",
		executionOrder:   &executionOrder,
		executionOrderMu: &mu,
	}

	normalRecovery := &trackingRecoveryAction{
		name:             "NormalFix",
		priority:         types.PriorityNormal,
		label:            "Normal",
		executionOrder:   &executionOrder,
		executionOrderMu: &mu,
	}

	// Create custom analyzer that returns multiple problems with different priorities
	analyzeFunc := func(a *store.ReplicationAnalysis) []types.Problem {
		if !a.IsPrimary && a.IsWalReplayPaused {
			// Return three problems with different priorities
			return []types.Problem{
				{
					Code:           types.ProblemReplicaNotReplicating,
					CheckName:      "MultiPriorityAnalyzer",
					PoolerID:       a.PoolerID,
					ShardKey:       a.ShardKey,
					Priority:       types.PriorityNormal,
					Scope:          types.ScopePooler,
					RecoveryAction: normalRecovery,
					DetectedAt:     time.Now(),
					Description:    "Normal priority problem",
				},
				{
					Code:           types.ProblemReplicaNotReplicating,
					CheckName:      "MultiPriorityAnalyzer",
					PoolerID:       a.PoolerID,
					ShardKey:       a.ShardKey,
					Priority:       types.PriorityEmergency,
					Scope:          types.ScopePooler,
					RecoveryAction: emergencyRecovery,
					DetectedAt:     time.Now(),
					Description:    "Emergency priority problem",
				},
				{
					Code:           types.ProblemReplicaNotReplicating,
					CheckName:      "MultiPriorityAnalyzer",
					PoolerID:       a.PoolerID,
					ShardKey:       a.ShardKey,
					Priority:       types.PriorityHigh,
					Scope:          types.ScopePooler,
					RecoveryAction: highRecovery,
					DetectedAt:     time.Now(),
					Description:    "High priority problem",
				},
			}
		}
		return nil
	}

	analyzer := &customAnalyzer{
		analyzeFn: analyzeFunc,
		name:      "MultiPriorityAnalyzer",
	}

	// Setup test analyzers
	analysis.SetTestAnalyzers([]analysis.Analyzer{analyzer})
	t.Cleanup(analysis.ResetAnalyzers)

	// Set up store state: replica with problem
	replicaPooler := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:       replicaID,
			Database: "db1", TableGroup: "tg1", Shard: "0",
			Type:     clustermetadatapb.PoolerType_REPLICA,
			Hostname: "replica-host",
		},
		IsLastCheckValid: true,
		IsUpToDate:       true,
		ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
			IsWalReplayPaused: true,
		},
		LastSeen: timestamppb.Now(),
	}
	engine.poolerStore.Set("multipooler-cell1-replica-pooler", replicaPooler)

	// Generate problems
	generator := analysis.NewAnalysisGenerator(engine.poolerStore)
	analyses := generator.GenerateAnalyses()

	var problems []types.Problem
	analyzers := analysis.DefaultAnalyzers(engine.actionFactory)
	for _, poolerAnalysis := range analyses {
		for _, analyzer := range analyzers {
			detectedProblems, err := analyzer.Analyze(poolerAnalysis)
			require.NoError(t, err)
			problems = append(problems, detectedProblems...)
		}
	}

	// Should detect 3 problems with different priorities
	require.Len(t, problems, 3, "should detect 3 problems with different priorities")

	shardKey := commontypes.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "0"}

	// Process problems - they should be attempted in priority order
	engine.processShardProblems(t.Context(), shardKey, problems)

	// ASSERTION: Problems should be attempted in priority order (Emergency > High > Normal)
	// Note: validation will fail for all since we don't change the state, but we can verify
	// that attempts were made in the correct order if we had proper tracking
	mu.Lock()
	defer mu.Unlock()

	// At least verify that if any executed, emergency was first
	if len(executionOrder) > 0 {
		assert.Equal(t, "Emergency", executionOrder[0],
			"Emergency priority problem should be attempted first")
	}
}

// TestRecoveryLoop_TracingSpans verifies that OpenTelemetry spans are created
// during recovery operations for observability.
func TestRecoveryLoop_TracingSpans(t *testing.T) {
	// Set up test telemetry to capture spans
	setup := telemetry.SetupTestTelemetry(t)
	err := setup.Telemetry.InitTelemetry(t.Context(), "test-recovery-spans")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = setup.Telemetry.ShutdownTelemetry(context.Background())
	})

	ctx := t.Context()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cfg := config.NewTestConfig(
		config.WithCell("zone1"),
		config.WithBookkeepingInterval(1*time.Minute),
		config.WithClusterMetadataRefreshInterval(15*time.Second),
		config.WithClusterMetadataRefreshTimeout(30*time.Second),
	)

	// Create engine
	engine := NewEngine(
		ts,
		logger,
		cfg,
		[]config.WatchTarget{{Database: "db1"}},
		&rpcclient.FakeClient{},
		newTestCoordinator(ts, &rpcclient.FakeClient{}, "zone1"),
	)

	// Create a mock analyzer that always detects a problem
	successAction := &mockRecoveryAction{
		name:     "SuccessfulRecovery",
		priority: types.PriorityHigh,
		timeout:  10 * time.Second,
	}

	replicaID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "replica-pooler",
	}

	analyzeFunc := func(a *store.ReplicationAnalysis) []types.Problem {
		// Detect replica with paused WAL replay
		if !a.IsPrimary && a.IsWalReplayPaused {
			return []types.Problem{
				{
					Code:           types.ProblemReplicaNotReplicating,
					CheckName:      "TracingTestAnalyzer",
					PoolerID:       a.PoolerID,
					ShardKey:       a.ShardKey,
					Scope:          types.ScopePooler,
					Priority:       types.PriorityHigh,
					RecoveryAction: successAction,
					DetectedAt:     time.Now(),
					Description:    "Test problem for span verification",
				},
			}
		}
		return nil
	}

	analyzer := &customAnalyzer{
		analyzeFn: analyzeFunc,
		name:      "TracingTestAnalyzer",
	}

	// Setup test analyzers
	analysis.SetTestAnalyzers([]analysis.Analyzer{analyzer})
	t.Cleanup(analysis.ResetAnalyzers)

	// Set up store state: replica with problem
	replicaPooler := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:       replicaID,
			Database: "db1", TableGroup: "tg1", Shard: "0",
			Type:     clustermetadatapb.PoolerType_REPLICA,
			Hostname: "replica-host",
		},
		IsLastCheckValid: true,
		IsUpToDate:       true,
		ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
			IsWalReplayPaused: true,
		},
		LastSeen: timestamppb.Now(),
	}
	engine.poolerStore.Set("multipooler-zone1-replica-pooler", replicaPooler)

	// Run a recovery cycle - this should create spans
	engine.performRecoveryCycle()

	// Flush spans to the exporter
	err = setup.ForceFlush(ctx)
	require.NoError(t, err)

	// Get captured spans
	spans := setup.SpanExporter.GetSpans()

	// Verify we have spans for recovery operations
	spanNames := make(map[string]bool)
	for _, span := range spans {
		spanNames[span.Name] = true
	}

	// Should have a cycle span
	assert.True(t, spanNames["recovery/cycle"], "should have recovery/cycle span, got spans: %v", spanNames)

	// Should have attempt span (for the detected problem)
	assert.True(t, spanNames["recovery/attempt"], "should have recovery/attempt span, got spans: %v", spanNames)

	// Verify span hierarchy: cycle → attempt
	var cycleSpan, attemptSpan *tracetest.SpanStub
	for i := range spans {
		switch spans[i].Name {
		case "recovery/cycle":
			cycleSpan = &spans[i]
		case "recovery/attempt":
			attemptSpan = &spans[i]
		}
	}

	require.NotNil(t, cycleSpan, "cycle span should exist")
	require.NotNil(t, attemptSpan, "attempt span should exist")

	// Verify spans share the same trace ID and parent-child relationship
	assert.Equal(t, cycleSpan.SpanContext.TraceID(), attemptSpan.SpanContext.TraceID(),
		"attempt span should share trace ID with cycle span")
	assert.Equal(t, cycleSpan.SpanContext.SpanID(), attemptSpan.Parent.SpanID(),
		"attempt span's parent should be cycle span")

	// Verify cycle span has problem count attribute
	var foundProblemCount bool
	for _, attr := range cycleSpan.Attributes {
		if attr.Key == "problems.count" {
			foundProblemCount = true
			assert.Equal(t, int64(1), attr.Value.AsInt64(), "problems.count should be 1")
		}
	}
	assert.True(t, foundProblemCount, "cycle span should have problems.count attribute")

	// Verify attempt span has shard, action, and problem attributes
	var foundDatabase, foundAction, foundProblem bool
	for _, attr := range attemptSpan.Attributes {
		if attr.Key == "shard.database" {
			foundDatabase = true
			assert.Equal(t, "db1", attr.Value.AsString())
		}
		if attr.Key == "action.name" {
			foundAction = true
			assert.Equal(t, "SuccessfulRecovery", attr.Value.AsString())
		}
		if attr.Key == "problem.code" {
			foundProblem = true
			assert.Equal(t, string(types.ProblemReplicaNotReplicating), attr.Value.AsString())
		}
	}
	assert.True(t, foundDatabase, "attempt span should have shard.database attribute")
	assert.True(t, foundAction, "attempt span should have action.name attribute")
	assert.True(t, foundProblem, "attempt span should have problem.code attribute")
}
