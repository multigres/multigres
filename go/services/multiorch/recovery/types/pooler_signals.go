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

package types

// TODO: stateless helpers for interpreting consensus and availability signals
// (like PrimaryNeedsReplacement) live here to avoid an import cycle between the
// analysis and actions packages. Once these utilities are needed more broadly they
// should move to go/common/consensus or a similar shared package.

import (
	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

// PrimaryNeedsReplacement reports whether a primary pooler has voluntarily
// signalled that it should be replaced via a new election. It reads the
// self-reported AvailabilityStatus from the consensus status RPC.
//
// The staleness check (leadership_status.primary_term == consensus primary_term)
// ensures we don't act on a REQUESTING_DEMOTION signal left over from a previous
// election cycle that was never cleared (e.g. after a process restart).
//
// TODO: once the coordinator synthesizes AvailabilityStatus into
// PoolerHealthState directly (see clustermetadata.proto TODO), read from
// PoolerHealthState.AvailabilityStatus instead of ConsensusStatus so that
// coordinator-synthesized signals (e.g. TEMPORARILY_UNAVAILABLE for unreachable
// nodes) are handled here too.
func PrimaryNeedsReplacement(p *multiorchdatapb.PoolerHealthState) bool {
	leadershipStatus := p.ConsensusStatus.GetAvailabilityStatus().GetLeadershipStatus()
	if leadershipStatus == nil {
		return false
	}
	if leadershipStatus.Signal != clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION {
		return false
	}
	// Verify the signal is for the current primary term, not a stale one.
	primaryTerm := commonconsensus.PrimaryTerm(p.GetConsensusStatus().GetConsensusStatus())
	return leadershipStatus.PrimaryTerm != 0 && leadershipStatus.PrimaryTerm == primaryTerm
}
