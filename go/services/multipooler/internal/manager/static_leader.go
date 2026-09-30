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

	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
)

// promoteStaticLeaderLocked promotes this static leader's postgres from standby
// to primary. It takes the place of the coordinator's Promote, which a static
// leader (consensus.ConsensusManager.StartsAsPrimary) never receives: it runs
// the same promotion (pg_promote, then clearing primary_conninfo and
// restore_command) but skips recruitment and the rule write, which exist to
// coordinate with other poolers and coordinators.
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
	return pm.promoteStandbyToPrimary(ctx, state, pm.consensusMgr.CachedConsensusStatus().GetCurrentPosition().GetPosition())
}
