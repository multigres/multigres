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

package coordinator

import (
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/multiorch/store"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestBuildSyncReplicationConfig(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	c := &Coordinator{logger: logger}

	// Default candidate for tests
	candidate := createTestPoolerHealth("primary", "cell-primary")

	t.Run("required_count=1 returns nil (async replication)", func(t *testing.T) {
		rule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 1,
			Description:   "Single node quorum",
		}

		standbys := []*store.PoolerHealth{
			createTestPoolerHealth("mp1", "cell1"),
			createTestPoolerHealth("mp2", "cell1"),
		}

		config, err := c.buildSyncReplicationConfig(rule, standbys, candidate)
		require.NoError(t, err)
		require.Nil(t, config, "Should return nil for required_count=1")
	})

	t.Run("no standbys returns nil (async replication)", func(t *testing.T) {
		rule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 2,
			Description:   "Two node quorum",
			AsyncFallback: clustermetadatapb.AsyncReplicationFallbackMode_ASYNC_REPLICATION_FALLBACK_MODE_ALLOW,
		}

		standbys := []*store.PoolerHealth{}

		config, err := c.buildSyncReplicationConfig(rule, standbys, candidate)
		require.NoError(t, err)
		require.Nil(t, config, "Should return nil when no standbys")
	})

	t.Run("required_count=2 with 1 standby configures num_sync=1", func(t *testing.T) {
		rule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 2,
			Description:   "Two node quorum",
		}

		standbys := []*store.PoolerHealth{
			createTestPoolerHealth("mp1", "cell1"),
		}

		config, err := c.buildSyncReplicationConfig(rule, standbys, candidate)
		require.NoError(t, err)
		require.NotNil(t, config)
		require.Equal(t, multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_WRITE, config.SynchronousCommit)
		require.Equal(t, multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY, config.SynchronousMethod)
		require.Equal(t, int32(1), config.NumSync, "num_sync should be required_count - 1")
		require.Len(t, config.StandbyIds, 1)
		require.Equal(t, "mp1", config.StandbyIds[0].Name)
		require.True(t, config.ReloadConfig)
	})

	t.Run("required_count=3 with 2 standbys configures num_sync=2", func(t *testing.T) {
		rule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 3,
			Description:   "Three node quorum",
		}

		standbys := []*store.PoolerHealth{
			createTestPoolerHealth("mp1", "cell1"),
			createTestPoolerHealth("mp2", "cell1"),
		}

		config, err := c.buildSyncReplicationConfig(rule, standbys, candidate)
		require.NoError(t, err)
		require.NotNil(t, config)
		require.Equal(t, int32(2), config.NumSync, "num_sync should be required_count - 1")
		require.Len(t, config.StandbyIds, 2)
	})

	t.Run("required_count=3 with 5 standbys configures num_sync=2", func(t *testing.T) {
		rule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 3,
			Description:   "Three node quorum",
		}

		standbys := []*store.PoolerHealth{
			createTestPoolerHealth("mp1", "cell1"),
			createTestPoolerHealth("mp2", "cell1"),
			createTestPoolerHealth("mp3", "cell1"),
			createTestPoolerHealth("mp4", "cell1"),
			createTestPoolerHealth("mp5", "cell1"),
		}

		config, err := c.buildSyncReplicationConfig(rule, standbys, candidate)
		require.NoError(t, err)
		require.NotNil(t, config)
		require.Equal(t, int32(2), config.NumSync, "num_sync should be required_count - 1, not capped by standbys")
		require.Len(t, config.StandbyIds, 5, "All standbys should be in the list")
	})

	t.Run("required_count=5 with 2 standbys caps num_sync at 2", func(t *testing.T) {
		rule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 5,
			Description:   "Five node quorum",
			AsyncFallback: clustermetadatapb.AsyncReplicationFallbackMode_ASYNC_REPLICATION_FALLBACK_MODE_ALLOW,
		}

		standbys := []*store.PoolerHealth{
			createTestPoolerHealth("mp1", "cell1"),
			createTestPoolerHealth("mp2", "cell1"),
		}

		config, err := c.buildSyncReplicationConfig(rule, standbys, candidate)
		require.NoError(t, err)
		require.NotNil(t, config)
		require.Equal(t, int32(2), config.NumSync, "num_sync should be capped at number of standbys")
		require.Len(t, config.StandbyIds, 2)
	})

	t.Run("MULTI_CELL_ANY_N with required_count=2 and 3 standbys", func(t *testing.T) {
		rule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_MULTI_CELL_ANY_N,
			RequiredCount: 2, // 2 cells required for quorum
			Description:   "Two cell quorum",
		}

		standbys := []*store.PoolerHealth{
			createTestPoolerHealth("mp1", "us-west-1a"),
			createTestPoolerHealth("mp2", "us-west-1b"),
			createTestPoolerHealth("mp3", "us-west-1c"),
		}

		config, err := c.buildSyncReplicationConfig(rule, standbys, candidate)
		require.NoError(t, err)
		require.NotNil(t, config)
		require.Equal(t, int32(1), config.NumSync, "num_sync should be required_count - 1")
		require.Len(t, config.StandbyIds, 3)
		require.Equal(t, multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY, config.SynchronousMethod)
	})

	t.Run("verifies all standby IDs are included", func(t *testing.T) {
		rule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 2,
			Description:   "Two node quorum",
		}

		standbys := []*store.PoolerHealth{
			createTestPoolerHealth("mp-alpha", "cell-a"),
			createTestPoolerHealth("mp-beta", "cell-b"),
			createTestPoolerHealth("mp-gamma", "cell-c"),
		}

		config, err := c.buildSyncReplicationConfig(rule, standbys, candidate)
		require.NoError(t, err)
		require.NotNil(t, config)
		require.Len(t, config.StandbyIds, 3)

		// Verify all standby IDs are present
		names := make(map[string]bool)
		for _, id := range config.StandbyIds {
			names[id.Name] = true
		}
		require.True(t, names["mp-alpha"])
		require.True(t, names["mp-beta"])
		require.True(t, names["mp-gamma"])
	})

	t.Run("uses REMOTE_WRITE commit level", func(t *testing.T) {
		rule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 2,
		}

		standbys := []*store.PoolerHealth{createTestPoolerHealth("mp1", "cell1")}

		config, err := c.buildSyncReplicationConfig(rule, standbys, candidate)
		require.NoError(t, err)
		require.NotNil(t, config)
		require.Equal(t, multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_WRITE, config.SynchronousCommit)
	})

	t.Run("uses ANY synchronous method", func(t *testing.T) {
		rule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 3,
		}

		standbys := []*store.PoolerHealth{
			createTestPoolerHealth("mp1", "cell1"),
			createTestPoolerHealth("mp2", "cell1"),
		}

		config, err := c.buildSyncReplicationConfig(rule, standbys, candidate)
		require.NoError(t, err)
		require.NotNil(t, config)
		require.Equal(t, multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY, config.SynchronousMethod)
	})

	t.Run("sets reload_config to true", func(t *testing.T) {
		rule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 2,
		}

		standbys := []*store.PoolerHealth{createTestPoolerHealth("mp1", "cell1")}

		config, err := c.buildSyncReplicationConfig(rule, standbys, candidate)
		require.NoError(t, err)
		require.NotNil(t, config)
		require.True(t, config.ReloadConfig)
	})

	// ========== MULTI_CELL_ANY_N Cell Filtering Tests ==========

	t.Run("MULTI_CELL_ANY_N excludes same-cell standbys", func(t *testing.T) {
		candidate := createTestPoolerHealth("primary", "us-west-1a")
		rule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_MULTI_CELL_ANY_N,
			RequiredCount: 2,
			Description:   "Two cell quorum",
		}

		standbys := []*store.PoolerHealth{
			createTestPoolerHealth("mp1", "us-west-1a"), // Same cell as primary - should be excluded
			createTestPoolerHealth("mp2", "us-west-1b"), // Different cell - should be included
			createTestPoolerHealth("mp3", "us-west-1c"), // Different cell - should be included
		}

		config, err := c.buildSyncReplicationConfig(rule, standbys, candidate)
		require.NoError(t, err)
		require.NotNil(t, config)
		require.Equal(t, int32(1), config.NumSync, "num_sync should be required_count - 1")
		require.Len(t, config.StandbyIds, 2, "Should only include standbys from different cells")

		// Verify excluded standby is not in the list
		for _, id := range config.StandbyIds {
			require.NotEqual(t, "mp1", id.Name, "mp1 should be excluded (same cell as primary)")
		}

		// Verify included standbys are present
		names := make(map[string]bool)
		for _, id := range config.StandbyIds {
			names[id.Name] = true
		}
		require.True(t, names["mp2"], "mp2 should be included (different cell)")
		require.True(t, names["mp3"], "mp3 should be included (different cell)")
	})

	t.Run("MULTI_CELL_ANY_N with all standbys in same cell returns nil with ALLOW mode", func(t *testing.T) {
		candidate := createTestPoolerHealth("primary", "us-west-1a")
		rule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_MULTI_CELL_ANY_N,
			RequiredCount: 2,
			Description:   "Two cell quorum",
			AsyncFallback: clustermetadatapb.AsyncReplicationFallbackMode_ASYNC_REPLICATION_FALLBACK_MODE_ALLOW,
		}

		standbys := []*store.PoolerHealth{
			createTestPoolerHealth("mp1", "us-west-1a"), // Same cell as primary
			createTestPoolerHealth("mp2", "us-west-1a"), // Same cell as primary
			createTestPoolerHealth("mp3", "us-west-1a"), // Same cell as primary
		}

		config, err := c.buildSyncReplicationConfig(rule, standbys, candidate)
		require.NoError(t, err)
		require.Nil(t, config, "Should return nil when all standbys are in same cell as primary with ALLOW mode")
	})

	t.Run("MULTI_CELL_ANY_N with mixed cells only includes different cells", func(t *testing.T) {
		candidate := createTestPoolerHealth("primary", "cell-a")
		rule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_MULTI_CELL_ANY_N,
			RequiredCount: 3,
			Description:   "Three cell quorum",
		}

		standbys := []*store.PoolerHealth{
			createTestPoolerHealth("mp1", "cell-a"), // Same cell - excluded
			createTestPoolerHealth("mp2", "cell-a"), // Same cell - excluded
			createTestPoolerHealth("mp3", "cell-b"), // Different cell - included
			createTestPoolerHealth("mp4", "cell-c"), // Different cell - included
			createTestPoolerHealth("mp5", "cell-d"), // Different cell - included
		}

		config, err := c.buildSyncReplicationConfig(rule, standbys, candidate)
		require.NoError(t, err)
		require.NotNil(t, config)
		require.Equal(t, int32(2), config.NumSync, "num_sync should be required_count - 1")
		require.Len(t, config.StandbyIds, 3, "Should only include 3 standbys from different cells")

		// Verify no same-cell standbys are included
		for _, id := range config.StandbyIds {
			require.NotEqual(t, "cell-a", id.Cell, "No standbys from primary's cell should be included")
		}
	})

	t.Run("MULTI_CELL_ANY_N insufficient different-cell standbys caps num_sync", func(t *testing.T) {
		candidate := createTestPoolerHealth("primary", "cell-a")
		rule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_MULTI_CELL_ANY_N,
			RequiredCount: 4, // Would need 3 standbys
			Description:   "Four cell quorum",
			AsyncFallback: clustermetadatapb.AsyncReplicationFallbackMode_ASYNC_REPLICATION_FALLBACK_MODE_ALLOW,
		}

		standbys := []*store.PoolerHealth{
			createTestPoolerHealth("mp1", "cell-a"), // Same cell - excluded
			createTestPoolerHealth("mp2", "cell-b"), // Different cell - included
			createTestPoolerHealth("mp3", "cell-c"), // Different cell - included
		}

		config, err := c.buildSyncReplicationConfig(rule, standbys, candidate)
		require.NoError(t, err)
		require.NotNil(t, config)
		require.Equal(t, int32(2), config.NumSync, "num_sync should be capped at available different-cell standbys")
		require.Len(t, config.StandbyIds, 2, "Should include all available different-cell standbys")
	})

	t.Run("ANY_N does NOT filter by cell (includes same-cell standbys)", func(t *testing.T) {
		candidate := createTestPoolerHealth("primary", "us-west-1a")
		rule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 2,
			Description:   "Two node quorum",
		}

		standbys := []*store.PoolerHealth{
			createTestPoolerHealth("mp1", "us-west-1a"), // Same cell as primary - should be included for ANY_N
			createTestPoolerHealth("mp2", "us-west-1b"),
		}

		config, err := c.buildSyncReplicationConfig(rule, standbys, candidate)
		require.NoError(t, err)
		require.NotNil(t, config)
		require.Len(t, config.StandbyIds, 2, "ANY_N should include all standbys regardless of cell")

		// Verify same-cell standby IS included for ANY_N
		names := make(map[string]bool)
		for _, id := range config.StandbyIds {
			names[id.Name] = true
		}
		require.True(t, names["mp1"], "mp1 should be included for ANY_N (cell filtering only for MULTI_CELL)")
		require.True(t, names["mp2"], "mp2 should be included")
	})

	t.Run("REJECT mode with no eligible standbys returns error", func(t *testing.T) {
		candidate := createTestPoolerHealth("primary", "us-west-1a")
		rule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_MULTI_CELL_ANY_N,
			RequiredCount: 2,
			Description:   "Multi-cell quorum",
			AsyncFallback: clustermetadatapb.AsyncReplicationFallbackMode_ASYNC_REPLICATION_FALLBACK_MODE_REJECT,
		}

		// All standbys in same cell as candidate
		standbys := []*store.PoolerHealth{
			createTestPoolerHealth("mp1", "us-west-1a"),
			createTestPoolerHealth("mp2", "us-west-1a"),
			createTestPoolerHealth("mp3", "us-west-1a"),
		}

		config, err := c.buildSyncReplicationConfig(rule, standbys, candidate)
		require.Error(t, err)
		require.Nil(t, config)
		require.Contains(t, err.Error(), "cannot establish synchronous replication")
		require.Contains(t, err.Error(), "async_fallback=REJECT")
	})

	t.Run("ALLOW mode with no eligible standbys returns nil (async fallback)", func(t *testing.T) {
		candidate := createTestPoolerHealth("primary", "us-west-1a")
		rule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_MULTI_CELL_ANY_N,
			RequiredCount: 2,
			Description:   "Multi-cell quorum",
			AsyncFallback: clustermetadatapb.AsyncReplicationFallbackMode_ASYNC_REPLICATION_FALLBACK_MODE_ALLOW,
		}

		// All standbys in same cell as candidate
		standbys := []*store.PoolerHealth{
			createTestPoolerHealth("mp1", "us-west-1a"),
			createTestPoolerHealth("mp2", "us-west-1a"),
			createTestPoolerHealth("mp3", "us-west-1a"),
		}

		config, err := c.buildSyncReplicationConfig(rule, standbys, candidate)
		require.NoError(t, err)
		require.Nil(t, config, "Should return nil and allow async replication")
	})

	t.Run("default (UNKNOWN) mode behaves like REJECT", func(t *testing.T) {
		candidate := createTestPoolerHealth("primary", "us-west-1a")
		rule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_MULTI_CELL_ANY_N,
			RequiredCount: 2,
			Description:   "Multi-cell quorum",
			AsyncFallback: clustermetadatapb.AsyncReplicationFallbackMode_ASYNC_REPLICATION_FALLBACK_MODE_UNKNOWN,
		}

		// All standbys in same cell as candidate
		standbys := []*store.PoolerHealth{
			createTestPoolerHealth("mp1", "us-west-1a"),
			createTestPoolerHealth("mp2", "us-west-1a"),
			createTestPoolerHealth("mp3", "us-west-1a"),
		}

		config, err := c.buildSyncReplicationConfig(rule, standbys, candidate)
		require.Error(t, err, "UNKNOWN should default to REJECT behavior")
		require.Nil(t, config)
		require.Contains(t, err.Error(), "async_fallback=REJECT")
	})

	// ========== Additional REJECT Mode Edge Cases ==========

	t.Run("REJECT mode with ANY_N and no standbys returns error", func(t *testing.T) {
		candidate := createTestPoolerHealth("primary", "us-west-1a")
		rule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 2,
			Description:   "ANY_N quorum requiring 2 nodes",
			AsyncFallback: clustermetadatapb.AsyncReplicationFallbackMode_ASYNC_REPLICATION_FALLBACK_MODE_REJECT,
		}

		// No standbys available
		standbys := []*store.PoolerHealth{}

		config, err := c.buildSyncReplicationConfig(rule, standbys, candidate)
		require.Error(t, err)
		require.Nil(t, config)
		require.Contains(t, err.Error(), "cannot establish synchronous replication")
		require.Contains(t, err.Error(), "async_fallback=REJECT")
	})

	t.Run("REJECT mode with ANY_N and insufficient standbys returns error", func(t *testing.T) {
		candidate := createTestPoolerHealth("primary", "us-west-1a")
		rule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 5, // Requires 4 standbys to achieve quorum
			Description:   "ANY_N quorum requiring 5 nodes",
			AsyncFallback: clustermetadatapb.AsyncReplicationFallbackMode_ASYNC_REPLICATION_FALLBACK_MODE_REJECT,
		}

		// Only 2 standbys available, insufficient for required_count=5
		standbys := []*store.PoolerHealth{
			createTestPoolerHealth("mp1", "us-west-1a"),
			createTestPoolerHealth("mp2", "us-west-1b"),
		}

		config, err := c.buildSyncReplicationConfig(rule, standbys, candidate)
		require.Error(t, err)
		require.Nil(t, config)
		require.Contains(t, err.Error(), "cannot establish synchronous replication")
		require.Contains(t, err.Error(), "async_fallback=REJECT")
		require.Contains(t, err.Error(), "required 4 standbys")
	})

	t.Run("REJECT mode with MULTI_CELL_ANY_N and insufficient different-cell standbys returns error", func(t *testing.T) {
		candidate := createTestPoolerHealth("primary", "cell-a")
		rule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_MULTI_CELL_ANY_N,
			RequiredCount: 4, // Requires 3 standbys in different cells
			Description:   "MULTI_CELL quorum requiring 4 cells",
			AsyncFallback: clustermetadatapb.AsyncReplicationFallbackMode_ASYNC_REPLICATION_FALLBACK_MODE_REJECT,
		}

		// Only 1 standby in different cell (2 are in same cell as candidate)
		standbys := []*store.PoolerHealth{
			createTestPoolerHealth("mp1", "cell-a"), // Same cell - excluded
			createTestPoolerHealth("mp2", "cell-a"), // Same cell - excluded
			createTestPoolerHealth("mp3", "cell-b"), // Different cell - included
		}

		config, err := c.buildSyncReplicationConfig(rule, standbys, candidate)
		require.Error(t, err)
		require.Nil(t, config)
		require.Contains(t, err.Error(), "cannot establish synchronous replication")
		require.Contains(t, err.Error(), "async_fallback=REJECT")
		require.Contains(t, err.Error(), "required 3 standbys")
	})
}
