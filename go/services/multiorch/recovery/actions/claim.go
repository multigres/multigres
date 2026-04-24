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

package actions

import (
	"context"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/rpcclient"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
)

// claimFromPrimaryStatus builds a CoordinatorClaim from a primary pooler's
// reported consensus identity. Used by recovery actions that must drive
// term-carrying RPCs but don't hold their own CoordinatorClaim: they
// reconstitute the current primary's claim and forward it, so the targeted
// pooler validates against the same (term, coord, initiated_at) triple the
// primary is operating under.
//
// A healthy peer under that primary has the same stored triple -- ApplyClaim
// is idempotent. A peer behind on term accepts the advance. A peer that saw
// a different coordinator or a restarted coordinator at the same term is
// rejected, which is the correct signal that recovery can't safely repair
// this peer until the coordinator catches up.
func claimFromPrimaryStatus(
	ctx context.Context,
	client rpcclient.MultiPoolerClient,
	primary *clustermetadatapb.MultiPooler,
) (*consensusdatapb.CoordinatorClaim, error) {
	resp, err := client.ConsensusStatus(ctx, primary, &consensusdatapb.StatusRequest{})
	if err != nil {
		return nil, err
	}
	rev := resp.GetConsensusStatus().GetTermRevocation()
	if rev == nil || rev.GetAcceptedCoordinatorId() == nil || rev.GetCoordinatorInitiatedAt() == nil {
		return nil, mterrors.New(0,
			"primary consensus status missing term identity (accepted_coordinator_id/coordinator_initiated_at)")
	}
	return &consensusdatapb.CoordinatorClaim{
		Term:                   rev.GetRevokedBelowTerm(),
		CoordinatorId:          rev.GetAcceptedCoordinatorId(),
		CoordinatorInitiatedAt: rev.GetCoordinatorInitiatedAt(),
	}, nil
}
