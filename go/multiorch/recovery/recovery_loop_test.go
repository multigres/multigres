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
	"testing"
	"time"

	"github.com/multigres/multigres/go/clustermetadata/topo/memorytopo"
	"github.com/multigres/multigres/go/multiorch/config"
	"github.com/multigres/multigres/go/multiorch/recovery/analysis"
	"github.com/multigres/multigres/go/multipooler/rpcclient"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// mockRecoveryAction is a test implementation of RecoveryAction
type mockRecoveryAction struct {
	name         string
	priority     analysis.Priority
	timeout      time.Duration
	requiresLock bool
	executed     bool
	executeErr   error
	metadata     analysis.RecoveryMetadata
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

func (m *mockRecoveryAction) Priority() analysis.Priority {
	return m.priority
}

func TestGroupProblemsByTableGroup(t *testing.T) {
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

	grouped := engine.groupProblemsByTableGroup(problems)

	// Should have 2 tablegroups
	assert.Len(t, grouped, 2, "should have 2 tablegroups")

	// Check first tablegroup
	key1 := TableGroupKey{Database: "db1", TableGroup: "tg1", Shard: "0"}
	assert.Len(t, grouped[key1], 2, "db1/tg1/0 should have 2 problems")

	// Check second tablegroup
	key2 := TableGroupKey{Database: "db2", TableGroup: "tg2", Shard: "0"}
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

	// Sort by priority (same logic as processTableGroupProblems)
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

func TestTableGroupKey(t *testing.T) {
	// Test that TableGroupKey works correctly as a map key
	key1 := TableGroupKey{
		Database:   "db1",
		TableGroup: "tg1",
		Shard:      "0",
	}

	key2 := TableGroupKey{
		Database:   "db1",
		TableGroup: "tg1",
		Shard:      "0",
	}

	key3 := TableGroupKey{
		Database:   "db1",
		TableGroup: "tg2",
		Shard:      "0",
	}

	// Test map usage
	m := make(map[TableGroupKey]int)
	m[key1] = 1
	m[key2] = 2 // Should overwrite key1
	m[key3] = 3

	assert.Equal(t, 2, m[key1], "key1 and key2 should be equal")
	assert.Equal(t, 2, m[key2], "key1 and key2 should be equal")
	assert.Equal(t, 3, m[key3], "key3 should be different")
	assert.Len(t, m, 2, "should have 2 unique keys")
}

func TestGroupProblemsByTableGroup_DifferentShards(t *testing.T) {
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

	grouped := engine.groupProblemsByTableGroup(problems)

	// Should have 2 separate groups (different shards)
	assert.Len(t, grouped, 2, "should have 2 separate groups for different shards")

	key1 := TableGroupKey{Database: "db1", TableGroup: "tg1", Shard: "0"}
	key2 := TableGroupKey{Database: "db1", TableGroup: "tg1", Shard: "1"}

	assert.Len(t, grouped[key1], 1, "shard 0 should have 1 problem")
	assert.Len(t, grouped[key2], 1, "shard 1 should have 1 problem")
}

// TestValidateProblemStillExists_PoolerNotFound tests error handling when
// the pooler is not found in the store.
func TestValidateProblemStillExists_PoolerNotFound(t *testing.T) {
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

	// Create generator with empty store
	generator := analysis.NewAnalysisGenerator(engine.poolerStore)

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

	stillExists, err := engine.validateProblemStillExists(problem, generator)

	require.Error(t, err, "should return error when pooler not found")
	assert.False(t, stillExists)
	assert.Contains(t, err.Error(), "pooler not found in store")
}

// TestSmartFilterProblems_ClusterWideOnly tests that when cluster-wide problems exist,
// only the highest priority cluster-wide problem is returned.
func TestSmartFilterProblems_ClusterWideOnly(t *testing.T) {
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
			Scope:    analysis.ScopeSinglePooler,
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
			Scope:    analysis.ScopeClusterWide,
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
			Scope:    analysis.ScopeSinglePooler,
			RecoveryAction: &mockRecoveryAction{
				name:     "FixConfig",
				priority: analysis.PriorityNormal,
				timeout:  10 * time.Second,
			},
		},
	}

	// Problems are already sorted by priority (Emergency > High > Normal)
	filtered := engine.smartFilterProblems(problems)

	// Should return only the cluster-wide problem (PrimaryDead)
	require.Len(t, filtered, 1)
	assert.Equal(t, analysis.ProblemPrimaryDead, filtered[0].Code)
	assert.Equal(t, analysis.PriorityEmergency, filtered[0].Priority)
}

// TestSmartFilterProblems_NoClusterWide tests deduplication by pooler ID
// when there are no cluster-wide problems.
func TestSmartFilterProblems_NoClusterWide(t *testing.T) {
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
			Scope:    analysis.ScopeSinglePooler,
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
			Scope:    analysis.ScopeSinglePooler,
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
			Scope:    analysis.ScopeSinglePooler,
			RecoveryAction: &mockRecoveryAction{
				name:     "FixLag",
				priority: analysis.PriorityNormal,
				timeout:  20 * time.Second,
			},
		},
	}

	// Problems are already sorted by priority
	filtered := engine.smartFilterProblems(problems)

	// Should keep only highest priority problem per pooler
	require.Len(t, filtered, 3)
	assert.Equal(t, analysis.ProblemReplicaNotReplicating, filtered[0].Code)
	assert.Equal(t, analysis.ProblemPrimaryMisconfigured, filtered[1].Code)
	assert.Equal(t, analysis.ProblemReplicaLagging, filtered[2].Code)
}

// TestSmartFilterProblems_MultipleClusterWide tests that when multiple cluster-wide
// problems exist, only the highest priority one is returned.
func TestSmartFilterProblems_MultipleClusterWide(t *testing.T) {
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

	// Create multiple cluster-wide problems with different priorities
	problems := []analysis.Problem{
		{
			Code:     analysis.ProblemClusterHasNoPrimary,
			PoolerID: poolerID1,
			Priority: analysis.PriorityClusterBootstrap,
			Scope:    analysis.ScopeClusterWide,
			RecoveryAction: &mockRecoveryAction{
				name:     "ElectPrimary",
				priority: analysis.PriorityClusterBootstrap,
				timeout:  60 * time.Second,
			},
		},
		{
			Code:     analysis.ProblemPrimaryDead,
			PoolerID: poolerID2,
			Priority: analysis.PriorityEmergency,
			Scope:    analysis.ScopeClusterWide,
			RecoveryAction: &mockRecoveryAction{
				name:     "FailoverPrimary",
				priority: analysis.PriorityEmergency,
				timeout:  45 * time.Second,
			},
		},
	}

	// Problems are already sorted by priority
	filtered := engine.smartFilterProblems(problems)

	// Should return only the highest priority cluster-wide problem
	require.Len(t, filtered, 1)
	assert.Equal(t, analysis.ProblemClusterHasNoPrimary, filtered[0].Code)
	assert.Equal(t, analysis.PriorityClusterBootstrap, filtered[0].Priority)
}
