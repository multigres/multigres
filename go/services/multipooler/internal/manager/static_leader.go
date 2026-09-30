// Copyright 2026 Supabase, Inc.
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

package manager

import (
	"context"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
)

// isStaticLeader reports whether this pooler is its shard's leader without
// consensus (Config.StaticLeader).
func (pm *MultipoolerManager) isStaticLeader() bool {
	return pm.config != nil && pm.config.StaticLeader
}

// roleConsensusStatus returns the consensus status this pooler derives its role
// from: for a static leader, a fixed status naming it leader (staticLeaderStatus);
// otherwise the consensus manager's cached status. The serving state manager and
// the postgres monitor both use it, so they always agree on the role.
func (pm *MultipoolerManager) roleConsensusStatus() *clustermetadatapb.ConsensusStatus {
	if pm.isStaticLeader() {
		return staticLeaderStatus(pm.serviceID)
	}
	return pm.consensusMgr.CachedConsensusStatus()
}

// promoteStaticLeaderLocked promotes this static leader's postgres from standby
// to primary. It takes the place of the coordinator's Promote, which a static
// leader never receives: it runs the same promotion (pg_promote, then clearing
// primary_conninfo and restore_command) but skips recruitment and the rule write,
// which exist to coordinate with other poolers and coordinators.
//
// TODO: write the static rule (leader and sole cohort member) to the rule store,
// as Promote does, so the persisted consensus state matches the role.
func (pm *MultipoolerManager) promoteStaticLeaderLocked(ctx context.Context) error {
	if err := actionlock.AssertActionLockHeld(ctx); err != nil {
		return err
	}
	state, err := pm.checkPromotionState(ctx)
	if err != nil {
		return err
	}
	// The position only scopes the post-promotion rewind-ready mark, which needs
	// a recorded primary and so is a no-op for a static leader.
	return pm.promoteStandbyToPrimary(ctx, state, pm.roleConsensusStatus().GetCurrentPosition().GetPosition())
}

// staticLeaderStatus returns a consensus status in which the pooler with the
// given ID is the active leader: a decided rule names it as leader and sole
// cohort member, with no pending proposal and no term revocation. The rule
// number only breaks ties between poolers claiming PRIMARY, which cannot
// happen with a single pooler, so a fixed value is enough.
func staticLeaderStatus(id *clustermetadatapb.ID) *clustermetadatapb.ConsensusStatus {
	return &clustermetadatapb.ConsensusStatus{
		Id: id,
		CurrentPosition: &clustermetadatapb.PoolerPosition{
			Position: &clustermetadatapb.RulePosition{
				Decision: &clustermetadatapb.ShardRule{
					RuleNumber:    &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
					LeaderId:      id,
					CohortMembers: []*clustermetadatapb.ID{id},
				},
			},
		},
	}
}
