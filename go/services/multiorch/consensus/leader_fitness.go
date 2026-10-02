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
	"math/rand/v2"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

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
// Ordering:
//  1. Postgres readiness (postgresReadyLess): ready before not-ready — not
//     yet ready to promote (crash recovery still running, socket not open).
//  2. LeadershipSignal (poolerHealthStateLess): non-resigning before
//     REQUESTING_DEMOTION (node has explicitly asked to be replaced via
//     SwitchPrimary), then failover-slot readiness.
//  3. Remaining ties are broken uniformly at random via rng, not by position
//     — so a retried failover doesn't keep proposing the same candidate
//     forever if its Promote keeps failing for an unrelated reason.
//
// TODO: criteria 1-2 manually chain two Less funcs (poolerHealthStateLess
// itself already chains two more criteria internally). If a third fitness
// signal shows up, generalize to an ordered list of criteria instead of
// nested if-returns.
func selectFittestLeader(candidates []*clustermetadatapb.ConsensusStatus, healthByID map[string]*multiorchdatapb.PoolerHealthState, rng *rand.Rand) *clustermetadatapb.ConsensusStatus {
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
	// Collect every candidate tied for best (not just the first found), then
	// pick uniformly among them.
	tied := []*clustermetadatapb.ConsensusStatus{candidates[0]}
	for _, c := range candidates[1:] {
		switch {
		case less(c, tied[0]):
			tied = []*clustermetadatapb.ConsensusStatus{c}
		case !less(tied[0], c):
			tied = append(tied, c)
		}
	}
	return tied[rng.IntN(len(tied))]
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

// QuorumCommitStale reports whether a quorum-commit timestamp is older than
// staleAfter. A nil or epoch (Seconds == 0) timestamp means no evidence yet,
// so it's never stale -- GetSeconds() is nil-safe, so this covers both with
// one check.
//
// Shared by the analyzer and AppointLeaderAction's live recheck, so both use
// the same definition of "stale".
func QuorumCommitStale(quorumCommitTs *timestamppb.Timestamp, now time.Time, staleAfter time.Duration) bool {
	return quorumCommitTs.GetSeconds() != 0 && now.Sub(quorumCommitTs.AsTime()) > staleAfter
}

// DefaultQuorumCommitStaleAfter is the staleness threshold used by both the
// analyzer's AvailabilityPolicy and AppointLeaderAction's live recheck.
const DefaultQuorumCommitStaleAfter = 20 * time.Second
