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

package consensus

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestIsManagedPooler(t *testing.T) {
	for mode, managed := range map[clustermetadatapb.PoolerManagementMode]bool{
		clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNSPECIFIED: true,
		clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_MANAGED:     true,
		clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED:   false,
		clustermetadatapb.PoolerManagementMode(99):                                false,
	} {
		t.Run(mode.String(), func(t *testing.T) {
			assert.Equal(t, managed, isManagedPooler(&clustermetadatapb.Multipooler{ManagementMode: mode}))
		})
	}
}

// poolersOfEveryMode returns a managed pooler, one of an unspecified mode (which reads as
// managed), an unmanaged one and one of a mode multiorch does not know, all in one shard.
func poolersOfEveryMode() (managed, unspecified, unmanaged, unknown *clustermetadatapb.Multipooler) {
	managed = poolerWithShard("zone1", "managed")
	managed.ManagementMode = clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_MANAGED
	unspecified = poolerWithShard("zone1", "unspecified")
	unmanaged = poolerWithShard("zone1", "unmanaged")
	unmanaged.ManagementMode = clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED
	unknown = poolerWithShard("zone1", "unknown")
	unknown.ManagementMode = clustermetadatapb.PoolerManagementMode(99)
	return managed, unspecified, unmanaged, unknown
}

// TestGetShardNodesLeavesOutPoolersItMayNotManage: the nodes of a shard are the poolers
// multiorch may manage; an unmanaged pooler, or one of an unknown mode, is not one of them.
func TestGetShardNodesLeavesOutPoolersItMayNotManage(t *testing.T) {
	managed, unspecified, unmanaged, unknown := poolersOfEveryMode()
	c, _ := newCertifiedTestCoordinator(t, rpcclient.NewFakeClient(),
		[]*clustermetadatapb.Multipooler{managed, unspecified, unmanaged, unknown})

	nodes, err := c.GetShardNodes(t.Context(), "zone1", "db1", "default", "0-inf")
	require.NoError(t, err)
	var names []string
	for _, n := range nodes {
		names = append(names, n.GetMultipooler().GetId().GetName())
	}
	assert.ElementsMatch(t, []string{"managed", "unspecified"}, names)
}

// TestRefreshShardConsensusStatusesLeavesOutPoolersItMayNotManage: the certified rule change
// asks only the poolers multiorch may manage for their consensus status.
func TestRefreshShardConsensusStatusesLeavesOutPoolersItMayNotManage(t *testing.T) {
	managed, unspecified, unmanaged, unknown := poolersOfEveryMode()
	fc := rpcclient.NewFakeClient()
	for _, p := range []*clustermetadatapb.Multipooler{managed, unspecified, unmanaged, unknown} {
		fc.SetStatusResponse(topoclient.ComponentIDString(p.Id), &multipoolermanagerdatapb.StatusResponse{
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{Id: p.Id},
		})
	}
	c, _ := newCertifiedTestCoordinator(t, fc, []*clustermetadatapb.Multipooler{managed, unspecified, unmanaged, unknown})

	statuses, err := c.refreshShardConsensusStatuses(t.Context(),
		&clustermetadatapb.ShardRef{Database: "db1", TableGroup: "default", Shard: "0-inf"})
	require.NoError(t, err)
	require.Len(t, statuses, 2)
	assert.Contains(t, statuses, topoclient.ClusterIDString(managed.Id))
	assert.Contains(t, statuses, topoclient.ClusterIDString(unspecified.Id))
}

// TestResolveCohortRejectsAMemberItMayNotManage: a certified rule change
// whose proposed cohort lists an unmanaged pooler, or one of an unknown mode,
// is refused before any Recruit or Promote is sent to it.
func TestResolveCohortRejectsAMemberItMayNotManage(t *testing.T) {
	managed, unspecified, unmanaged, unknown := poolersOfEveryMode()
	c, _ := newCertifiedTestCoordinator(t, rpcclient.NewFakeClient(),
		[]*clustermetadatapb.Multipooler{managed, unspecified, unmanaged, unknown})

	_, cohort, err := c.resolveCohort(t.Context(), []*clustermetadatapb.ID{managed.Id, unspecified.Id})
	require.NoError(t, err)
	require.Len(t, cohort, 2)

	for _, p := range []*clustermetadatapb.Multipooler{unmanaged, unknown} {
		_, _, err := c.resolveCohort(t.Context(), []*clustermetadatapb.ID{managed.Id, p.Id})
		require.Error(t, err, p.Id.Name)
		assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err), p.Id.Name)
	}
}
