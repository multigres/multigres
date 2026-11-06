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
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

// TestAppointLeader_AppointsOnlyOutgoingCohortMembers covers a failover whose
// recruits include a caught-up pooler that was never admitted to the cohort,
// as a replacement pod is while it waits for cohort reconciliation. The old
// leader is gone; recruitment reaches every remaining shard member.
func TestAppointLeader_AppointsOnlyOutgoingCohortMembers(t *testing.T) {
	ids := map[string]*clustermetadatapb.ID{}
	for _, name := range []string{"leader", "b", "c", "replacement"} {
		ids[name] = &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: name}
	}
	outgoingRule := &clustermetadatapb.ShardRule{
		RuleNumber:       &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
		LeaderId:         ids["leader"],
		CohortMembers:    []*clustermetadatapb.ID{ids["leader"], ids["b"], ids["c"]},
		DurabilityPolicy: topoclient.AtLeastN(2),
	}

	run := func(t *testing.T, lsns map[string]string, resigning ...string) (*rpcclient.FakeClient, error) {
		ctx := context.Background()
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
		fakeClient := rpcclient.NewFakeClient()
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
		t.Cleanup(func() { _ = ts.Close() })
		c := NewCoordinator(&clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIORCH, Cell: "zone1", Name: "test-coordinator",
		}, ts, fakeClient, logger)

		var shard []*multiorchdatapb.PoolerHealthState
		for _, name := range []string{"b", "c", "replacement"} {
			id := ids[name]
			mp := createMockNode(fakeClient, name, 5, lsns[name], true, outgoingRule)
			position := &clustermetadatapb.PoolerPosition{
				Lsn:      lsns[name],
				Position: &clustermetadatapb.RulePosition{Decision: outgoingRule},
			}
			mp.ConsensusStatus.Id = id
			mp.ConsensusStatus.CurrentPosition = position
			for _, r := range resigning {
				if r == name {
					mp.AvailabilityStatus = &clustermetadatapb.AvailabilityStatus{
						LeadershipStatus: &clustermetadatapb.LeadershipStatus{
							LeaderTerm: 5,
							Signal:     clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION,
						},
					}
				}
			}
			fakeClient.RecruitResponses[topoclient.ComponentIDString(id)] = &consensusdatapb.RecruitResponse{
				ConsensusStatus: &clustermetadatapb.ConsensusStatus{Id: id, CurrentPosition: position},
			}
			require.NoError(t, ts.CreateMultipooler(ctx, mp.Multipooler))
			shard = append(shard, mp)
		}
		shardRef := &clustermetadatapb.ShardRef{Database: "postgres", TableGroup: "default", Shard: "0-inf"}
		return fakeClient, c.AppointLeader(ctx, shardRef, shard, "test_cohort_candidates")
	}
	promoted := func(fakeClient *rpcclient.FakeClient, name string) bool {
		_, ok := fakeClient.PromoteRequests[topoclient.ComponentIDString(ids[name])]
		return ok
	}

	t.Run("a tied non-member is not appointed", func(t *testing.T) {
		// Fitness would prefer the replacement: both members are resigning.
		fakeClient, err := run(t,
			map[string]string{"b": "0/2000000", "c": "0/2000000", "replacement": "0/2000000"},
			"b", "c")
		require.NoError(t, err)
		require.False(t, promoted(fakeClient, "replacement"), "a pooler outside the outgoing cohort must not be appointed")
		require.True(t, promoted(fakeClient, "b") || promoted(fakeClient, "c"), "a cohort member is appointed")
	})

	t.Run("only a non-member at the most advanced LSN fails closed", func(t *testing.T) {
		fakeClient, err := run(t,
			map[string]string{"b": "0/1000000", "c": "0/1000000", "replacement": "0/2000000"})
		require.ErrorContains(t, err, "no eligible leader is a member of the outgoing cohort")
		require.False(t, promoted(fakeClient, "replacement"))
	})
}
