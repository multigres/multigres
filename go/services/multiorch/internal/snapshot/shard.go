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

// Package snapshot holds the frozen, read-only view of a shard that multiorch
// judges and acts on. The store produces snapshots; nothing here depends on it.
package snapshot

import (
	"time"

	"google.golang.org/protobuf/proto"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

// Shard is a frozen view of one shard: every pooler's health as of a single
// capture, plus the inputs a judgment about the shard needs. Health snapshots
// are shared with the cache, which only replaces them and never edits one in
// place, so consumers must treat everything here as read-only.
type Shard struct {
	ShardKey *clustermetadatapb.ShardKey
	// Poolers holds the health snapshot of every pooler in the shard, captured
	// once, so all reads agree.
	Poolers []*multiorchdatapb.PoolerHealthState

	// Now is the evaluation timestamp (orchestrator clock) captured when this
	// snapshot was taken. Analyzers use it — together with AvailabilityPolicy — to judge
	// observation freshness explicitly, rather than reading pre-baked liveness
	// verdicts.
	Now time.Time

	// AvailabilityPolicy carries the availability thresholds in effect for this evaluation.
	AvailabilityPolicy AvailabilityPolicy

	// TombstoneIDs is the set of pooler IDs the cache has marked as SHUTDOWN
	// tombstones cluster-wide. Analyzers consult it to detect cohort members
	// that have explicitly drained (and therefore had their riders evicted
	// from the live cache, so they don't appear in Poolers) versus poolers
	// that are merely missing from the cache for transient reasons. Cohort
	// scope is enforced naturally: cohort membership is per-shard, so a
	// missing cohort member found here is necessarily a shutdown of THIS
	// shard's pooler.
	TombstoneIDs map[topoclient.ComponentID]struct{}

	// BootstrapDurabilityPolicy is the durability policy configured for this shard's database.
	// May be nil if not yet configured or not available.
	BootstrapDurabilityPolicy *clustermetadatapb.DurabilityPolicy
}

// HighestIntendedRule returns the highest rule position anyone has told us of,
// or nil if no rule is known. A rule can sit at three levels:
//
//   - decided: the decision half of a pooler's own position, settled;
//   - in the WAL: a pooler's own position, which may also carry an undecided
//     proposal (these are what "position" always means);
//   - intended: the above plus positions only advertised through a
//     ReplicationPrimary, which no pooler's WAL need hold yet.
//
// This is the intended level, so it is not called a position: a follower can
// learn of a newer leader via SetPrimary before that leader has promoted or
// recorded the rule.
//
// TODO: rename commonconsensus.HighestKnownRule to HighestIntendedRule and the
// ReplicationPrimary proto message to IntendedPrimary to match this vocabulary.
func (sa *Shard) HighestIntendedRule() *clustermetadatapb.RulePosition {
	statuses := make([]*clustermetadatapb.ConsensusStatus, 0, len(sa.Poolers))
	for _, p := range sa.Poolers {
		if cs := p.GetConsensusStatus(); cs != nil {
			statuses = append(statuses, cs)
		}
	}
	return commonconsensus.HighestKnownRule(statuses)
}

// IntendedLeaderID returns the ID the shard's rule names as leader, or nil if
// no rule is known. That is the leader the coordinator is installing, not
// necessarily one that is established or serving: the rule may still be
// undecided, and followers can learn of it through SetPrimary before the named
// leader has been promoted or has recorded it. Judge whether it is serving
// from its health snapshot, not from this ID alone.
func (sa *Shard) IntendedLeaderID() *clustermetadatapb.ID {
	return commonconsensus.PossiblyUndecidedRule(sa.HighestIntendedRule()).GetLeaderId()
}

// PoolerHealth returns the health snapshot for id, or nil if the shard has none.
func (sa *Shard) PoolerHealth(id *clustermetadatapb.ID) *multiorchdatapb.PoolerHealthState {
	if id == nil {
		return nil
	}
	for _, p := range sa.Poolers {
		if proto.Equal(p.GetMultipooler().GetId(), id) {
			return p
		}
	}
	return nil
}

// IntendedLeaderHealth returns the health snapshot of the pooler IntendedLeaderID
// names, or nil if no rule is known or we have no health for that pooler. The rule
// can name a leader we have never observed; in that case we don't know where to
// point replicas, so consumers that need the leader's host/port gate on the result
// being non-nil rather than on reachability — an unreachable-but-known leader is
// still the intended one.
func (sa *Shard) IntendedLeaderHealth() *multiorchdatapb.PoolerHealthState {
	return sa.PoolerHealth(sa.IntendedLeaderID())
}
