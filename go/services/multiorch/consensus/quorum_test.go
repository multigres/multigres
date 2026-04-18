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

package consensus

import (
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

func TestValidateAnyNQuorum(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	c := &Coordinator{logger: logger}

	t.Run("success - exact count", func(t *testing.T) {
		rule := topoclient.AtLeastN(2)

		cohort := []*multiorchdatapb.PoolerHealthState{
			createTestPoolerHealth("mp1", "cell1"),
			createTestPoolerHealth("mp2", "cell1"),
			createTestPoolerHealth("mp3", "cell1"),
		}

		recruited := []*multiorchdatapb.PoolerHealthState{
			cohort[0],
			cohort[1],
		}

		err := c.validateAtLeastNQuorum(rule, cohort, recruited)
		require.NoError(t, err)
	})

	t.Run("success - more than required", func(t *testing.T) {
		rule := topoclient.AtLeastN(2)

		cohort := []*multiorchdatapb.PoolerHealthState{
			createTestPoolerHealth("mp1", "cell1"),
			createTestPoolerHealth("mp2", "cell1"),
			createTestPoolerHealth("mp3", "cell1"),
		}

		recruited := cohort // All 3 recruited

		err := c.validateAtLeastNQuorum(rule, cohort, recruited)
		require.NoError(t, err)
	})

	t.Run("error - insufficient nodes", func(t *testing.T) {
		rule := topoclient.AtLeastN(3)

		cohort := []*multiorchdatapb.PoolerHealthState{
			createTestPoolerHealth("mp1", "cell1"),
			createTestPoolerHealth("mp2", "cell1"),
			createTestPoolerHealth("mp3", "cell1"),
		}

		recruited := []*multiorchdatapb.PoolerHealthState{
			cohort[0],
			cohort[1],
		}

		err := c.validateAtLeastNQuorum(rule, cohort, recruited)
		require.Error(t, err)
		require.Contains(t, err.Error(), "quorum not satisfied")
		require.Contains(t, err.Error(), "recruited 2 nodes, required 3")
	})

	t.Run("success - single node quorum", func(t *testing.T) {
		rule := topoclient.AtLeastN(1)

		cohort := []*multiorchdatapb.PoolerHealthState{
			createTestPoolerHealth("mp1", "cell1"),
		}

		recruited := cohort

		err := c.validateAtLeastNQuorum(rule, cohort, recruited)
		require.NoError(t, err)
	})
}

func TestValidateMultiCellQuorum(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	c := &Coordinator{logger: logger}

	t.Run("success - exactly required cells", func(t *testing.T) {
		rule := topoclient.MultiCellAtLeastN(2)

		recruited := []*multiorchdatapb.PoolerHealthState{
			createTestPoolerHealth("mp1", "us-west-1a"),
			createTestPoolerHealth("mp2", "us-west-1b"),
		}

		err := c.validateMultiCellQuorum(rule, recruited)
		require.NoError(t, err)
	})

	t.Run("success - more than required cells", func(t *testing.T) {
		rule := topoclient.MultiCellAtLeastN(2)

		recruited := []*multiorchdatapb.PoolerHealthState{
			createTestPoolerHealth("mp1", "us-west-1a"),
			createTestPoolerHealth("mp2", "us-west-1b"),
			createTestPoolerHealth("mp3", "us-west-1c"),
		}

		err := c.validateMultiCellQuorum(rule, recruited)
		require.NoError(t, err)
	})

	t.Run("success - multiple nodes from same cell", func(t *testing.T) {
		rule := topoclient.MultiCellAtLeastN(2)

		recruited := []*multiorchdatapb.PoolerHealthState{
			createTestPoolerHealth("mp1", "us-west-1a"),
			createTestPoolerHealth("mp2", "us-west-1a"), // Same cell as mp1
			createTestPoolerHealth("mp3", "us-west-1b"),
		}

		err := c.validateMultiCellQuorum(rule, recruited)
		require.NoError(t, err)
	})

	t.Run("error - insufficient cells", func(t *testing.T) {
		rule := topoclient.MultiCellAtLeastN(3)

		recruited := []*multiorchdatapb.PoolerHealthState{
			createTestPoolerHealth("mp1", "us-west-1a"),
			createTestPoolerHealth("mp2", "us-west-1b"),
		}

		err := c.validateMultiCellQuorum(rule, recruited)
		require.Error(t, err)
		require.Contains(t, err.Error(), "quorum not satisfied")
		require.Contains(t, err.Error(), "recruited nodes from 2 cells, required 3 cells")
	})

	t.Run("error - all nodes from same cell", func(t *testing.T) {
		rule := topoclient.MultiCellAtLeastN(2)

		recruited := []*multiorchdatapb.PoolerHealthState{
			createTestPoolerHealth("mp1", "us-west-1a"),
			createTestPoolerHealth("mp2", "us-west-1a"),
			createTestPoolerHealth("mp3", "us-west-1a"),
		}

		err := c.validateMultiCellQuorum(rule, recruited)
		require.Error(t, err)
		require.Contains(t, err.Error(), "quorum not satisfied")
		require.Contains(t, err.Error(), "recruited nodes from 1 cells, required 2 cells")
	})

	t.Run("success - single cell requirement", func(t *testing.T) {
		rule := topoclient.MultiCellAtLeastN(1)

		recruited := []*multiorchdatapb.PoolerHealthState{
			createTestPoolerHealth("mp1", "us-west-1a"),
		}

		err := c.validateMultiCellQuorum(rule, recruited)
		require.NoError(t, err)
	})
}

func TestValidateQuorum(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	c := &Coordinator{logger: logger}

	t.Run("AT_LEAST_N - delegates to validateAtLeastNQuorum", func(t *testing.T) {
		rule := topoclient.AtLeastN(2)

		cohort := []*multiorchdatapb.PoolerHealthState{
			createTestPoolerHealth("mp1", "cell1"),
			createTestPoolerHealth("mp2", "cell1"),
		}

		recruited := cohort

		err := c.ValidateQuorum(rule, cohort, recruited)
		require.NoError(t, err)
	})

	t.Run("MULTI_CELL_AT_LEAST_N - delegates to validateMultiCellQuorum", func(t *testing.T) {
		rule := topoclient.MultiCellAtLeastN(2)

		cohort := []*multiorchdatapb.PoolerHealthState{
			createTestPoolerHealth("mp1", "us-west-1a"),
			createTestPoolerHealth("mp2", "us-west-1b"),
		}

		recruited := cohort

		err := c.ValidateQuorum(rule, cohort, recruited)
		require.NoError(t, err)
	})

	t.Run("error - unknown quorum type", func(t *testing.T) {
		rule := &clustermetadatapb.DurabilityPolicy{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_UNKNOWN,
			RequiredCount: 2,
			Description:   "Unknown quorum type",
		}

		cohort := []*multiorchdatapb.PoolerHealthState{
			createTestPoolerHealth("mp1", "cell1"),
		}

		recruited := cohort

		err := c.ValidateQuorum(rule, cohort, recruited)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unknown quorum type")
	})
}

// createTestPoolerHealth creates a test pooler health with minimal configuration
func createTestPoolerHealth(name, cell string) *multiorchdatapb.PoolerHealthState {
	return &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      cell,
				Name:      name,
			},
		},
	}
}
