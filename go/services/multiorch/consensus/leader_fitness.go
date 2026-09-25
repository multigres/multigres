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
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

// selectFittestLeader returns whichever candidate (already tied on WAL
// position) is most fit for leadership, or nil if candidates is empty.
// Exposed as a selection, not a sort or a comparator, so how fitness is
// computed — pairwise Less funcs chained today, a scored/keyed approach
// tomorrow — can change without touching the call site or mutating the
// caller's slice.
//
// Current ordering, two orthogonal tiebreakers applied in sequence:
//  1. Postgres readiness (postgresReadyLess): ready before not-ready — not
//     yet ready to promote (crash recovery still running, socket not open).
//  2. LeadershipSignal (poolerHealthStateLess): non-resigning before
//     REQUESTING_DEMOTION (node has explicitly asked to be replaced via
//     SwitchPrimary), then failover-slot readiness.
//
// TODO: this manually chains two Less funcs (poolerHealthStateLess itself
// already chains two more criteria internally). If a third candidate-fitness
// signal shows up, generalize to an ordered list of criteria instead of
// nested if-returns.
func selectFittestLeader(candidates []*clustermetadatapb.ConsensusStatus, healthByID map[string]*multiorchdatapb.PoolerHealthState) *clustermetadatapb.ConsensusStatus {
	if len(candidates) == 0 {
		return nil
	}
	availLess := postgresReadyLess(healthByID)
	signalLess := poolerHealthStateLess(healthByID)
	less := func(a, b *clustermetadatapb.ConsensusStatus) bool {
		if availLess(a, b) {
			return true
		}
		if availLess(b, a) {
			return false
		}
		return signalLess(a, b)
	}
	// Earliest-appearing candidate wins ties, matching sort.SliceStable's
	// stability: only a strictly-more-fit later candidate replaces it.
	best := candidates[0]
	for _, c := range candidates[1:] {
		if less(c, best) {
			best = c
		}
	}
	return best
}

// postgresReadyLess is the BuildSafeProposal tiebreaker for nodes tied at the
// highest LSN: postgres-ready nodes sort first. A missing health snapshot
// defaults to not-ready rather than assuming readiness we haven't observed.
//
// Recruited nodes still count toward outgoing-cohort quorum regardless of
// readiness; this only reorders which tied node is proposed as leader.
func postgresReadyLess(healthByID map[string]*multiorchdatapb.PoolerHealthState) func(a, b *clustermetadatapb.ConsensusStatus) bool {
	isReady := func(id *clustermetadatapb.ID) bool {
		h := healthByID[topoclient.ClusterIDString(id)]
		return h.GetStatus().GetPostgresReady()
	}
	return func(a, b *clustermetadatapb.ConsensusStatus) bool {
		return isReady(a.GetId()) && !isReady(b.GetId())
	}
}

// leadershipSignalPriority maps each leadership signal to its sort priority.
// Lower values sort first — nodes with higher priority values are deprioritised
// as leader candidates when LSNs are tied. Add new signals here to extend the
// ordering without touching poolerHealthStateLess.
var leadershipSignalPriority = map[clustermetadatapb.LeadershipSignal]int{
	clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_UNKNOWN:             0,
	clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_ACTIVE:              0,
	clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION: 1,
}

// poolerHealthStateLess returns a less function for sort.SliceStable that
// orders ConsensusStatus entries by leadershipSignalPriority. It is used in
// Coordinator.runFailover to prefer nodes with lower priority values among
// candidates that share the highest LSN.
//
// WAL position is always the primary criterion: a node with a higher-priority
// signal still wins if it holds a strictly higher LSN than every other node.
// This tiebreaker only affects the ordering of tied eligible leaders.
//
// Recruited nodes participate in the outgoing-cohort quorum check regardless of
// their leadership signal — this tiebreaker only affects which tied node is
// proposed as leader, not the quorum denominator.
func poolerHealthStateLess(healthByID map[string]*multiorchdatapb.PoolerHealthState) func(a, b *clustermetadatapb.ConsensusStatus) bool {
	leadershipSignal := func(cs *clustermetadatapb.ConsensusStatus) clustermetadatapb.LeadershipSignal {
		h := healthByID[topoclient.ClusterIDString(cs.GetId())]
		return h.GetAvailabilityStatus().GetLeadershipStatus().GetSignal()
	}
	failoverSlotsReady := func(cs *clustermetadatapb.ConsensusStatus) int32 {
		h := healthByID[topoclient.ClusterIDString(cs.GetId())]
		return h.GetStatus().GetFailoverSlotsReady()
	}
	return func(a, b *clustermetadatapb.ConsensusStatus) bool {
		sigA := leadershipSignal(a)
		sigB := leadershipSignal(b)
		if leadershipSignalPriority[sigA] != leadershipSignalPriority[sigB] {
			return leadershipSignalPriority[sigA] < leadershipSignalPriority[sigB]
		}
		// Slot-aware tiebreak among otherwise-equal candidates: prefer the one
		// with more failover-ready logical slots so a promotion keeps the most
		// subscribers resumable (see the durable slot-creation barrier). This
		// only reorders candidates that already tied on WAL position (the
		// EligibleLeaders set) and on leadership signal, so it never trades data
		// safety or a resign intent for slot readiness. Zero when slot-based
		// replication is off, leaving the ordering unchanged.
		return failoverSlotsReady(a) > failoverSlotsReady(b)
	}
}
