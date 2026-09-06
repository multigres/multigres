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

package store

import (
	"testing"

	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

func TestCohortEligibleFollowerIDs(t *testing.T) {
	pooler := func(name string) *Pooler {
		return NewPooler(&multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{
				Id: &clustermetadatapb.ID{Cell: "zone1", Name: name},
			},
		}, nil)
	}
	names := func(ids []*clustermetadatapb.ID) []string {
		out := make([]string, 0, len(ids))
		for _, id := range ids {
			out = append(out, id.GetName())
		}
		return out
	}

	leader := pooler("p0")
	s1 := pooler("s1")
	s2 := pooler("s2")

	t.Run("every non-leader member is eligible", func(t *testing.T) {
		members := ShardMembers{
			Poolers: []*Pooler{leader, s1, s2},
			Leader:  leader,
		}
		// The leader is identified from consensus (members.Leader) and excluded;
		// every other member is a follower, regardless of topology role.
		require.ElementsMatch(t, []string{"s1", "s2"}, names(CohortEligibleFollowerIDs(members)))
	})

	t.Run("a member is included even before it streams", func(t *testing.T) {
		// A freshly discovered standby is not yet in the committed cohort / not
		// yet streaming; it must still be eligible so its slot is pre-created (the
		// whole point of the deadlock fix). Eligibility is membership, not role.
		members := ShardMembers{Poolers: []*Pooler{leader, s1}, Leader: leader}
		require.Equal(t, []string{"s1"}, names(CohortEligibleFollowerIDs(members)))
	})

	t.Run("single-node shard yields no followers", func(t *testing.T) {
		members := ShardMembers{Poolers: []*Pooler{leader}, Leader: leader}
		require.Empty(t, CohortEligibleFollowerIDs(members))
	})
}
