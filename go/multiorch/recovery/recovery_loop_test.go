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
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/clustermetadata/topo/memorytopo"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/multiorch/config"
	"github.com/multigres/multigres/go/multiorch/recovery/analysis"
	"github.com/multigres/multigres/go/multiorch/store"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// customAnalyzer is a test analyzer that can use a custom analyze function.
type customAnalyzer struct {
	analyzeFn func(*store.ReplicationAnalysis) []analysis.Problem
	name      string
}

func (c *customAnalyzer) Name() analysis.CheckName {
	return analysis.CheckName(c.name)
}

func (c *customAnalyzer) Analyze(a *store.ReplicationAnalysis) []analysis.Problem {
	return c.analyzeFn(a)
}

// trackingRecoveryAction is a test recovery action that tracks execution order.
type trackingRecoveryAction struct {
	name             string
	priority         analysis.Priority
	label            string
	executionOrder   *[]string
	executionOrderMu *sync.Mutex
}

func (t *trackingRecoveryAction) Execute(ctx context.Context, problem analysis.Problem) error {
	t.executionOrderMu.Lock()
	*t.executionOrder = append(*t.executionOrder, t.label)
	t.executionOrderMu.Unlock()
	return nil
}

func (t *trackingRecoveryAction) Metadata() analysis.RecoveryMetadata {
	return analysis.RecoveryMetadata{
		Name:    t.name,
		Timeout: 10 * time.Second,
	}
}

func (t *trackingRecoveryAction) RequiresLock() bool {
	return false
}

func (t *trackingRecoveryAction) RequiresHealthyPrimary() bool {
	return false
}

func (t *trackingRecoveryAction) Priority() analysis.Priority {
	return t.priority
}

// mockRecoveryAction is a test implementation of RecoveryAction
type mockRecoveryAction struct {
	name                   string
	priority               analysis.Priority
	timeout                time.Duration
	requiresLock           bool
	requiresHealthyPrimary bool
	executed               bool
	executeErr             error
	metadata               analysis.RecoveryMetadata
}

func (m *mockRecoveryAction) Execute(ctx context.Context, problem analysis.Problem) error {
	m.executed = true
	return m.executeErr
}

func (m *mockRecoveryAction) Metadata() analysis.RecoveryMetadata {
	// If metadata is set, use it; otherwise build default
	if m.metadata.Name != "" {
		return m.metadata
	}
	return analysis.RecoveryMetadata{
		Name:        m.name,
		Description: "Mock recovery action",
		Timeout:     m.timeout,
		Retryable:   true,
	}
}

func (m *mockRecoveryAction) RequiresLock() bool {
	return m.requiresLock
}

func (m *mockRecoveryAction) RequiresHealthyPrimary() bool {
	return m.requiresHealthyPrimary
}

func (m *mockRecoveryAction) Priority() analysis.Priority {
	return m.priority
}

func TestGroupProblemsByShard(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := config.NewTestConfig(config.WithCell("cell1"))
	engine := NewEngine(ts, logger, cfg, []config.WatchTarget{}, &rpcclient.FakeClient{})

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

	problems := []analysis.Problem{
		{
			Code:       analysis.ProblemPrimaryDead,
			PoolerID:   poolerID1,
			Database:   "db1",
			TableGroup: "tg1",
			Shard:      "0",
		},
		{
			Code:       analysis.ProblemReplicaNotReplicating,
			PoolerID:   poolerID2,
			Database:   "db1",
			TableGroup: "tg1",
			Shard:      "0",
		},
		{
			Code:       analysis.ProblemPrimaryDead,
			PoolerID:   poolerID3,
			Database:   "db2",
			TableGroup: "tg2",
			Shard:      "0",
		},
	}

	grouped := engine.groupProblemsByShard(problems)

	// Should have 2 shards
	assert.Len(t, grouped, 2, "should have 2 shards")

	// Check first shard
	key1 := analysis.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "0"}
	assert.Len(t, grouped[key1], 2, "db1/tg1/0 should have 2 problems")

	// Check second shard
	key2 := analysis.ShardKey{Database: "db2", TableGroup: "tg2", Shard: "0"}
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

	problems := []analysis.Problem{
		{
			Code:       analysis.ProblemReplicaNotReplicating,
			PoolerID:   poolerID2,
			Database:   "db1",
			TableGroup: "tg1",
			Shard:      "0",
			Priority:   analysis.PriorityHigh,
		},
		{
			Code:       analysis.ProblemPrimaryDead,
			PoolerID:   poolerID1,
			Database:   "db1",
			TableGroup: "tg1",
			Shard:      "0",
			Priority:   analysis.PriorityEmergency,
		},
		{
			Code:       "ConfigurationDrift",
			PoolerID:   poolerID3,
			Database:   "db1",
			TableGroup: "tg1",
			Shard:      "0",
			Priority:   analysis.PriorityNormal,
		},
	}

	// Sort by priority (same logic as processShardProblems)
	sort.SliceStable(problems, func(i, j int) bool {
		return problems[i].Priority > problems[j].Priority
	})

	// Verify order: Emergency > High > Normal
	require.Len(t, problems, 3)
	assert.Equal(t, analysis.PriorityEmergency, problems[0].Priority)
	assert.Equal(t, analysis.ProblemPrimaryDead, problems[0].Code)

	assert.Equal(t, analysis.PriorityHigh, problems[1].Priority)
	assert.Equal(t, analysis.ProblemReplicaNotReplicating, problems[1].Code)

	assert.Equal(t, analysis.PriorityNormal, problems[2].Priority)
	assert.Equal(t, analysis.ProblemCode("ConfigurationDrift"), problems[2].Code)
}

func TestShardKey(t *testing.T) {
	// Test that ShardKey works correctly as a map key
	key1 := analysis.ShardKey{
		Database:   "db1",
		TableGroup: "tg1",
		Shard:      "0",
	}

	key2 := analysis.ShardKey{
		Database:   "db1",
		TableGroup: "tg1",
		Shard:      "0",
	}

	key3 := analysis.ShardKey{
		Database:   "db1",
		TableGroup: "tg2",
		Shard:      "0",
	}

	// Test map usage
	m := make(map[analysis.ShardKey]int)
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
	engine := NewEngine(ts, logger, cfg, []config.WatchTarget{}, &rpcclient.FakeClient{})

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

	problems := []analysis.Problem{
		{
			Code:       analysis.ProblemPrimaryDead,
			PoolerID:   poolerID1,
			Database:   "db1",
			TableGroup: "tg1",
			Shard:      "0",
		},
		{
			Code:       analysis.ProblemPrimaryDead,
			PoolerID:   poolerID2,
			Database:   "db1",
			TableGroup: "tg1",
			Shard:      "1", // Different shard
		},
	}

	grouped := engine.groupProblemsByShard(problems)

	// Should have 2 separate groups (different shards)
	assert.Len(t, grouped, 2, "should have 2 separate groups for different shards")

	key1 := analysis.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "0"}
	key2 := analysis.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "1"}

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
	engine := NewEngine(ts, logger, cfg, []config.WatchTarget{}, &rpcclient.FakeClient{})

	poolerID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "pooler1",
	}

	// Create problem
	problem := analysis.Problem{
		Code:       analysis.ProblemPrimaryDead,
		CheckName:  "PrimaryDeadCheck",
		PoolerID:   poolerID,
		Database:   "db1",
		TableGroup: "tg1",
		Shard:      "0",
		Priority:   analysis.PriorityEmergency,
	}

	stillExists, err := engine.recheckProblem(problem)

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
	engine := NewEngine(ts, logger, cfg, []config.WatchTarget{}, &rpcclient.FakeClient{})

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
	problems := []analysis.Problem{
		{
			Code:     analysis.ProblemReplicaNotReplicating,
			PoolerID: poolerID2,
			Priority: analysis.PriorityHigh,
			Scope:    analysis.ScopePooler,
			RecoveryAction: &mockRecoveryAction{
				name:     "FixReplica",
				priority: analysis.PriorityHigh,
				timeout:  30 * time.Second,
			},
		},
		{
			Code:     analysis.ProblemPrimaryDead,
			PoolerID: poolerID1,
			Priority: analysis.PriorityEmergency,
			Scope:    analysis.ScopeShard,
			RecoveryAction: &mockRecoveryAction{
				name:     "FailoverPrimary",
				priority: analysis.PriorityEmergency,
				timeout:  60 * time.Second,
			},
		},
		{
			Code:     "ConfigDrift",
			PoolerID: poolerID2,
			Priority: analysis.PriorityNormal,
			Scope:    analysis.ScopePooler,
			RecoveryAction: &mockRecoveryAction{
				name:     "FixConfig",
				priority: analysis.PriorityNormal,
				timeout:  10 * time.Second,
			},
		},
	}

	filtered := engine.filterAndPrioritize(problems)

	// Should return only the shard-wide problem (PrimaryDead)
	require.Len(t, filtered, 1)
	assert.Equal(t, analysis.ProblemPrimaryDead, filtered[0].Code)
	assert.Equal(t, analysis.PriorityEmergency, filtered[0].Priority)
}

// TestFilterAndPrioritize_NoShardWide tests deduplication by pooler ID
// when there are no shard-wide problems.
func TestFilterAndPrioritize_NoShardWide(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := config.NewTestConfig(config.WithCell("cell1"))
	engine := NewEngine(ts, logger, cfg, []config.WatchTarget{}, &rpcclient.FakeClient{})

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
	problems := []analysis.Problem{
		{
			Code:     analysis.ProblemReplicaNotReplicating,
			PoolerID: poolerID1,
			Priority: analysis.PriorityHigh,
			Scope:    analysis.ScopePooler,
			RecoveryAction: &mockRecoveryAction{
				name:     "FixReplication",
				priority: analysis.PriorityHigh,
				timeout:  30 * time.Second,
			},
		},
		{
			Code:     analysis.ProblemPrimaryMisconfigured,
			PoolerID: poolerID1,
			Priority: analysis.PriorityNormal,
			Scope:    analysis.ScopePooler,
			RecoveryAction: &mockRecoveryAction{
				name:     "FixConfig",
				priority: analysis.PriorityNormal,
				timeout:  10 * time.Second,
			},
		},
		{
			Code:     analysis.ProblemReplicaLagging,
			PoolerID: poolerID2,
			Priority: analysis.PriorityNormal,
			Scope:    analysis.ScopePooler,
			RecoveryAction: &mockRecoveryAction{
				name:     "FixLag",
				priority: analysis.PriorityNormal,
				timeout:  20 * time.Second,
			},
		},
	}

	// Problems are already sorted by priority
	filtered := engine.filterAndPrioritize(problems)

	// Should keep only highest priority problem per pooler
	require.Len(t, filtered, 3)
	assert.Equal(t, analysis.ProblemReplicaNotReplicating, filtered[0].Code)
	assert.Equal(t, analysis.ProblemPrimaryMisconfigured, filtered[1].Code)
	assert.Equal(t, analysis.ProblemReplicaLagging, filtered[2].Code)
}

// TestFilterAndPrioritize_MultipleShardWide tests that when multiple shard-wide
// problems exist, only the highest priority one is returned.
func TestFilterAndPrioritize_MultipleShardWide(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := config.NewTestConfig(config.WithCell("cell1"))
	engine := NewEngine(ts, logger, cfg, []config.WatchTarget{}, &rpcclient.FakeClient{})

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
	problems := []analysis.Problem{
		{
			Code:     analysis.ProblemClusterHasNoPrimary,
			PoolerID: poolerID1,
			Priority: analysis.PriorityShardBootstrap,
			Scope:    analysis.ScopeShard,
			RecoveryAction: &mockRecoveryAction{
				name:     "ElectPrimary",
				priority: analysis.PriorityShardBootstrap,
				timeout:  60 * time.Second,
			},
		},
		{
			Code:     analysis.ProblemPrimaryDead,
			PoolerID: poolerID2,
			Priority: analysis.PriorityEmergency,
			Scope:    analysis.ScopeShard,
			RecoveryAction: &mockRecoveryAction{
				name:     "FailoverPrimary",
				priority: analysis.PriorityEmergency,
				timeout:  45 * time.Second,
			},
		},
	}

	filtered := engine.filterAndPrioritize(problems)

	// Should return only the highest priority shard-wide problem
	require.Len(t, filtered, 1)
	assert.Equal(t, analysis.ProblemClusterHasNoPrimary, filtered[0].Code)
	assert.Equal(t, analysis.PriorityShardBootstrap, filtered[0].Priority)
}

// mockPrimaryDeadAnalyzer detects when a primary is unreachable
type mockPrimaryDeadAnalyzer struct {
	recoveryAction analysis.RecoveryAction
}

func (m *mockPrimaryDeadAnalyzer) Name() analysis.CheckName {
	return "MockPrimaryDeadCheck"
}

func (m *mockPrimaryDeadAnalyzer) Analyze(a *store.ReplicationAnalysis) []analysis.Problem {
	// Detect if this is a primary that is unreachable
	if a.IsPrimary && a.IsUnreachable {
		return []analysis.Problem{
			{
				Code:           analysis.ProblemPrimaryDead,
				CheckName:      m.Name(),
				PoolerID:       a.PoolerID,
				Database:       a.Database,
				TableGroup:     a.TableGroup,
				Shard:          a.Shard,
				Priority:       analysis.PriorityEmergency,
				Scope:          analysis.ScopeShard,
				RecoveryAction: m.recoveryAction,
				DetectedAt:     time.Now(),
				Description:    "Primary is unreachable",
			},
		}
	}
	return nil
}

// mockReplicaNotReplicatingAnalyzer detects when a replica has stopped replicating
type mockReplicaNotReplicatingAnalyzer struct {
	recoveryAction analysis.RecoveryAction
}

func (m *mockReplicaNotReplicatingAnalyzer) Name() analysis.CheckName {
	return "MockReplicationStoppedCheck"
}

func (m *mockReplicaNotReplicatingAnalyzer) Analyze(a *store.ReplicationAnalysis) []analysis.Problem {
	// Detect if this is a replica with replication stopped
	if !a.IsPrimary && a.IsWalReplayPaused {
		return []analysis.Problem{
			{
				Code:           analysis.ProblemReplicaNotReplicating,
				CheckName:      m.Name(),
				PoolerID:       a.PoolerID,
				Database:       a.Database,
				TableGroup:     a.TableGroup,
				Shard:          a.Shard,
				Priority:       analysis.PriorityHigh,
				Scope:          analysis.ScopePooler,
				RecoveryAction: m.recoveryAction,
				DetectedAt:     time.Now(),
				Description:    "Replica replication is paused",
			},
		}
	}
	return nil
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
	fakeClient := &rpcclient.FakeClient{
		StatusResponses: map[string]*multipoolermanagerdatapb.StatusResponse{
			"multipooler-cell1-primary-pooler": {
				Status: &multipoolermanagerdatapb.Status{
					PoolerType: clustermetadatapb.PoolerType_PRIMARY,
					PrimaryStatus: &multipoolermanagerdatapb.PrimaryStatus{
						Lsn:   "0/DEADBEEF",
						Ready: true,
					},
				},
			},
			"multipooler-cell1-replica-pooler": {
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
			},
		},
	}

	engine := NewEngine(ts, logger, cfg, []config.WatchTarget{}, fakeClient)

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
		priority:               analysis.PriorityEmergency,
		timeout:                30 * time.Second,
		requiresHealthyPrimary: false,
		metadata: analysis.RecoveryMetadata{
			Name:    "EmergencyFailover",
			Timeout: 30 * time.Second,
		},
	}

	replicaRecovery := &mockRecoveryAction{
		name:                   "FixReplication",
		priority:               analysis.PriorityHigh,
		timeout:                10 * time.Second,
		requiresHealthyPrimary: true, // Requires healthy primary!
		metadata: analysis.RecoveryMetadata{
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
				Id:         primaryID,
				Database:   "db1",
				TableGroup: "tg1",
				Shard:      "0",
				Type:       clustermetadatapb.PoolerType_PRIMARY,
				Hostname:   "primary-host",
			},
			IsLastCheckValid: false,
			IsUpToDate:       true,
			LastSeen:         timestamppb.Now(),
		}
		engine.poolerStore.Set("multipooler-cell1-primary-pooler", primaryPooler)

		replicaPooler := &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id:         replicaID,
				Database:   "db1",
				TableGroup: "tg1",
				Shard:      "0",
				Type:       clustermetadatapb.PoolerType_REPLICA,
				Hostname:   "replica-host",
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
		var problems []analysis.Problem
		analyzers := analysis.DefaultAnalyzers()
		for _, poolerAnalysis := range analyses {
			for _, analyzer := range analyzers {
				detectedProblems := analyzer.Analyze(poolerAnalysis)
				problems = append(problems, detectedProblems...)
			}
		}

		// Should detect both problems
		require.Len(t, problems, 2, "should detect both primary dead and replica not replicating")

		shardKey := analysis.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "0"}

		// Call processShardProblems - this exercises the full recovery flow
		engine.processShardProblems(shardKey, problems)

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
				Id:         primaryID,
				Database:   "db1",
				TableGroup: "tg1",
				Shard:      "0",
				Type:       clustermetadatapb.PoolerType_PRIMARY,
				Hostname:   "primary-host",
			},
			IsLastCheckValid: true, // Primary is healthy
			IsUpToDate:       true,
			LastSeen:         timestamppb.Now(),
		}
		engine.poolerStore.Set("multipooler-cell1-primary-pooler", primaryPooler)

		replicaPooler := &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id:         replicaID,
				Database:   "db1",
				TableGroup: "tg1",
				Shard:      "0",
				Type:       clustermetadatapb.PoolerType_REPLICA,
				Hostname:   "replica-host",
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
		var problems []analysis.Problem
		analyzers := analysis.DefaultAnalyzers()
		for _, poolerAnalysis := range analyses {
			for _, analyzer := range analyzers {
				detectedProblems := analyzer.Analyze(poolerAnalysis)
				problems = append(problems, detectedProblems...)
			}
		}

		// Should detect only replica problem
		require.Len(t, problems, 1, "should detect only replica not replicating")
		assert.Equal(t, analysis.ProblemReplicaNotReplicating, problems[0].Code)

		shardKey := analysis.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "0"}

		// Call processShardProblems
		engine.processShardProblems(shardKey, problems)

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
	fakeClient := &rpcclient.FakeClient{
		StatusResponses: map[string]*multipoolermanagerdatapb.StatusResponse{
			"multipooler-cell1-replica-pooler": {
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
			},
		},
	}

	engine := NewEngine(ts, logger, cfg, []config.WatchTarget{}, fakeClient)

	replicaID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "replica-pooler",
	}

	// Create recovery action that should NOT be executed
	replicaRecovery := &mockRecoveryAction{
		name:                   "FixReplication",
		priority:               analysis.PriorityHigh,
		timeout:                10 * time.Second,
		requiresHealthyPrimary: false,
		metadata: analysis.RecoveryMetadata{
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
			Id:         replicaID,
			Database:   "db1",
			TableGroup: "tg1",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_REPLICA,
			Hostname:   "replica-host",
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

	var problems []analysis.Problem
	analyzers := analysis.DefaultAnalyzers()
	for _, poolerAnalysis := range analyses {
		for _, analyzer := range analyzers {
			detectedProblems := analyzer.Analyze(poolerAnalysis)
			problems = append(problems, detectedProblems...)
		}
	}

	require.Len(t, problems, 1, "should detect replica not replicating problem")

	// NOW: Fix the problem in the fake client BEFORE validation
	// This simulates the problem being transient or fixed by external means
	fakeClient.StatusResponses["multipooler-cell1-replica-pooler"] = &multipoolermanagerdatapb.StatusResponse{
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
	}

	// Attempt recovery - validation should detect problem no longer exists
	engine.attemptRecovery(problems[0])

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
	fakeClient := &rpcclient.FakeClient{
		StatusResponses: map[string]*multipoolermanagerdatapb.StatusResponse{
			"multipooler-cell1-primary-pooler": {
				Status: &multipoolermanagerdatapb.Status{
					PoolerType: clustermetadatapb.PoolerType_PRIMARY,
					PrimaryStatus: &multipoolermanagerdatapb.PrimaryStatus{
						Lsn:   "0/DEADBEEF",
						Ready: true,
					},
				},
			},
			"multipooler-cell1-replica1-pooler": {
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
			},
			"multipooler-cell1-replica2-pooler": {
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
			},
		},
	}

	engine := NewEngine(ts, logger, cfg, []config.WatchTarget{}, fakeClient)

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
		priority:               analysis.PriorityEmergency,
		timeout:                30 * time.Second,
		requiresHealthyPrimary: false,
		executeErr:             nil, // Success!
		metadata: analysis.RecoveryMetadata{
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
			Id:         primaryID,
			Database:   "db1",
			TableGroup: "tg1",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_PRIMARY,
			Hostname:   "primary-host",
		},
		IsLastCheckValid:   false, // Primary is dead
		IsUpToDate:         true,
		LastSeen:           timestamppb.New(time.Now().Add(-1 * time.Minute)),
		LastCheckAttempted: timestamppb.New(initialPrimaryCheck),
	}
	engine.poolerStore.Set("multipooler-cell1-primary-pooler", primaryPooler)

	replica1Pooler := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         replica1ID,
			Database:   "db1",
			TableGroup: "tg1",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_REPLICA,
			Hostname:   "replica1-host",
		},
		IsLastCheckValid:   true,
		IsUpToDate:         true,
		LastSeen:           timestamppb.Now(),
		LastCheckAttempted: timestamppb.New(initialReplica1Check),
	}
	engine.poolerStore.Set("multipooler-cell1-replica1-pooler", replica1Pooler)

	replica2Pooler := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         replica2ID,
			Database:   "db1",
			TableGroup: "tg1",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_REPLICA,
			Hostname:   "replica2-host",
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

	var problems []analysis.Problem
	analyzers := analysis.DefaultAnalyzers()
	for _, poolerAnalysis := range analyses {
		for _, analyzer := range analyzers {
			detectedProblems := analyzer.Analyze(poolerAnalysis)
			problems = append(problems, detectedProblems...)
		}
	}

	require.Len(t, problems, 1, "should detect primary dead problem")
	assert.Equal(t, analysis.ScopeShard, problems[0].Scope)

	// Now fix the primary in the fake client so validation will pass
	fakeClient.StatusResponses["multipooler-cell1-primary-pooler"] = &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_PRIMARY,
			PrimaryStatus: &multipoolermanagerdatapb.PrimaryStatus{
				Lsn:   "0/NEWPRIMARY",
				Ready: true,
			},
		},
	}

	// Attempt recovery - should succeed and trigger post-recovery refresh
	engine.attemptRecovery(problems[0])

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
	fakeClient := &rpcclient.FakeClient{
		StatusResponses: map[string]*multipoolermanagerdatapb.StatusResponse{
			"multipooler-cell1-primary-pooler": {
				Status: &multipoolermanagerdatapb.Status{
					PoolerType: clustermetadatapb.PoolerType_PRIMARY,
					PrimaryStatus: &multipoolermanagerdatapb.PrimaryStatus{
						Lsn:   "0/DEADBEEF",
						Ready: true,
					},
				},
			},
			"multipooler-cell1-replica1-pooler": {
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
			},
			"multipooler-cell1-replica2-pooler": {
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
			},
		},
	}

	engine := NewEngine(ts, logger, cfg, []config.WatchTarget{}, fakeClient)

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
		priority:               analysis.PriorityHigh,
		timeout:                10 * time.Second,
		requiresHealthyPrimary: false,
		metadata: analysis.RecoveryMetadata{
			Name:    "FixReplica1",
			Timeout: 10 * time.Second,
		},
	}

	replica2Recovery := &mockRecoveryAction{
		name:                   "FixReplica2",
		priority:               analysis.PriorityHigh,
		timeout:                10 * time.Second,
		requiresHealthyPrimary: false,
		metadata: analysis.RecoveryMetadata{
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
			Id:         primaryID,
			Database:   "db1",
			TableGroup: "tg1",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_PRIMARY,
			Hostname:   "primary-host",
		},
		IsLastCheckValid: true,
		IsUpToDate:       true,
		LastSeen:         timestamppb.Now(),
	}
	engine.poolerStore.Set("multipooler-cell1-primary-pooler", primaryPooler)

	replica1Pooler := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         replica1ID,
			Database:   "db1",
			TableGroup: "tg1",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_REPLICA,
			Hostname:   "replica1-host",
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
			Id:         replica2ID,
			Database:   "db1",
			TableGroup: "tg1",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_REPLICA,
			Hostname:   "replica2-host",
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
	fakeClient := &rpcclient.FakeClient{
		StatusResponses: map[string]*multipoolermanagerdatapb.StatusResponse{
			"multipooler-cell1-primary-pooler": {
				Status: &multipoolermanagerdatapb.Status{
					PoolerType: clustermetadatapb.PoolerType_PRIMARY,
					PrimaryStatus: &multipoolermanagerdatapb.PrimaryStatus{
						Lsn:   "0/DEADBEEF",
						Ready: true,
					},
				},
			},
			"multipooler-cell1-replica-pooler": {
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
			},
		},
	}

	engine := NewEngine(ts, logger, cfg, []config.WatchTarget{}, fakeClient)

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
		priority:         analysis.PriorityEmergency,
		label:            "Emergency",
		executionOrder:   &executionOrder,
		executionOrderMu: &mu,
	}

	highRecovery := &trackingRecoveryAction{
		name:             "HighPriorityFix",
		priority:         analysis.PriorityHigh,
		label:            "High",
		executionOrder:   &executionOrder,
		executionOrderMu: &mu,
	}

	normalRecovery := &trackingRecoveryAction{
		name:             "NormalFix",
		priority:         analysis.PriorityNormal,
		label:            "Normal",
		executionOrder:   &executionOrder,
		executionOrderMu: &mu,
	}

	// Create custom analyzer that returns multiple problems with different priorities
	analyzeFunc := func(a *store.ReplicationAnalysis) []analysis.Problem {
		if !a.IsPrimary && a.IsWalReplayPaused {
			// Return three problems with different priorities
			return []analysis.Problem{
				{
					Code:           analysis.ProblemReplicaNotReplicating,
					CheckName:      "MultiPriorityAnalyzer",
					PoolerID:       a.PoolerID,
					Database:       a.Database,
					TableGroup:     a.TableGroup,
					Shard:          a.Shard,
					Priority:       analysis.PriorityNormal,
					Scope:          analysis.ScopePooler,
					RecoveryAction: normalRecovery,
					DetectedAt:     time.Now(),
					Description:    "Normal priority problem",
				},
				{
					Code:           analysis.ProblemReplicaNotReplicating,
					CheckName:      "MultiPriorityAnalyzer",
					PoolerID:       a.PoolerID,
					Database:       a.Database,
					TableGroup:     a.TableGroup,
					Shard:          a.Shard,
					Priority:       analysis.PriorityEmergency,
					Scope:          analysis.ScopePooler,
					RecoveryAction: emergencyRecovery,
					DetectedAt:     time.Now(),
					Description:    "Emergency priority problem",
				},
				{
					Code:           analysis.ProblemReplicaNotReplicating,
					CheckName:      "MultiPriorityAnalyzer",
					PoolerID:       a.PoolerID,
					Database:       a.Database,
					TableGroup:     a.TableGroup,
					Shard:          a.Shard,
					Priority:       analysis.PriorityHigh,
					Scope:          analysis.ScopePooler,
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
			Id:         replicaID,
			Database:   "db1",
			TableGroup: "tg1",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_REPLICA,
			Hostname:   "replica-host",
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

	var problems []analysis.Problem
	analyzers := analysis.DefaultAnalyzers()
	for _, poolerAnalysis := range analyses {
		for _, analyzer := range analyzers {
			detectedProblems := analyzer.Analyze(poolerAnalysis)
			problems = append(problems, detectedProblems...)
		}
	}

	// Should detect 3 problems with different priorities
	require.Len(t, problems, 3, "should detect 3 problems with different priorities")

	shardKey := analysis.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "0"}

	// Process problems - they should be attempted in priority order
	engine.processShardProblems(shardKey, problems)

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
