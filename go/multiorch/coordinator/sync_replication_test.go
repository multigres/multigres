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

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestBuildSyncReplicationConfig(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	c := &Coordinator{logger: logger}

	t.Run("required_count=1 returns nil (async replication)", func(t *testing.T) {
		rule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 1,
			Description:   "Single node quorum",
		}

		standbys := []*Node{
			createTestNode("mp1", "cell1"),
			createTestNode("mp2", "cell1"),
		}

		config := c.buildSyncReplicationConfig(rule, standbys)
		require.Nil(t, config, "Should return nil for required_count=1")
	})

	t.Run("no standbys returns nil (async replication)", func(t *testing.T) {
		rule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 2,
			Description:   "Two node quorum",
		}

		standbys := []*Node{}

		config := c.buildSyncReplicationConfig(rule, standbys)
		require.Nil(t, config, "Should return nil when no standbys")
	})

	t.Run("required_count=2 with 1 standby configures num_sync=1", func(t *testing.T) {
		rule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 2,
			Description:   "Two node quorum",
		}

		standbys := []*Node{
			createTestNode("mp1", "cell1"),
		}

		config := c.buildSyncReplicationConfig(rule, standbys)
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

		standbys := []*Node{
			createTestNode("mp1", "cell1"),
			createTestNode("mp2", "cell1"),
		}

		config := c.buildSyncReplicationConfig(rule, standbys)
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

		standbys := []*Node{
			createTestNode("mp1", "cell1"),
			createTestNode("mp2", "cell1"),
			createTestNode("mp3", "cell1"),
			createTestNode("mp4", "cell1"),
			createTestNode("mp5", "cell1"),
		}

		config := c.buildSyncReplicationConfig(rule, standbys)
		require.NotNil(t, config)
		require.Equal(t, int32(2), config.NumSync, "num_sync should be required_count - 1, not capped by standbys")
		require.Len(t, config.StandbyIds, 5, "All standbys should be in the list")
	})

	t.Run("required_count=5 with 2 standbys caps num_sync at 2", func(t *testing.T) {
		rule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 5,
			Description:   "Five node quorum",
		}

		standbys := []*Node{
			createTestNode("mp1", "cell1"),
			createTestNode("mp2", "cell1"),
		}

		config := c.buildSyncReplicationConfig(rule, standbys)
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

		standbys := []*Node{
			createTestNode("mp1", "us-west-1a"),
			createTestNode("mp2", "us-west-1b"),
			createTestNode("mp3", "us-west-1c"),
		}

		config := c.buildSyncReplicationConfig(rule, standbys)
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

		standbys := []*Node{
			createTestNode("mp-alpha", "cell-a"),
			createTestNode("mp-beta", "cell-b"),
			createTestNode("mp-gamma", "cell-c"),
		}

		config := c.buildSyncReplicationConfig(rule, standbys)
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

		standbys := []*Node{createTestNode("mp1", "cell1")}

		config := c.buildSyncReplicationConfig(rule, standbys)
		require.NotNil(t, config)
		require.Equal(t, multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_WRITE, config.SynchronousCommit)
	})

	t.Run("uses ANY synchronous method", func(t *testing.T) {
		rule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 3,
		}

		standbys := []*Node{
			createTestNode("mp1", "cell1"),
			createTestNode("mp2", "cell1"),
		}

		config := c.buildSyncReplicationConfig(rule, standbys)
		require.NotNil(t, config)
		require.Equal(t, multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY, config.SynchronousMethod)
	})

	t.Run("sets reload_config to true", func(t *testing.T) {
		rule := &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 2,
		}

		standbys := []*Node{createTestNode("mp1", "cell1")}

		config := c.buildSyncReplicationConfig(rule, standbys)
		require.NotNil(t, config)
		require.True(t, config.ReloadConfig)
	})
}
