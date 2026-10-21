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

// Shard is a frozen view of one shard: every pooler's health, each read once
// at capture, plus the inputs a judgment about the shard needs. Health snapshots
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

// HighestIntendedRule returns the highest rule anyone has told us of, or an empty
// RuleTransition (IsEmpty) if no rule is known. It ranks at the intended level (see
// commonconsensus.HighestIntendedRule): a follower can learn of a newer leader via
// SetPrimary before that leader has promoted or recorded the rule.
//
// TODO: rename the ReplicationPrimary proto message to IntendedPrimary to match
// this vocabulary.
func (sa *Shard) HighestIntendedRule() commonconsensus.RuleTransition {
	statuses := make([]*clustermetadatapb.ConsensusStatus, 0, len(sa.Poolers))
	for _, p := range sa.Poolers {
		if cs := p.GetConsensusStatus(); cs != nil {
			statuses = append(statuses, cs)
		}
	}
	return commonconsensus.NewRuleTransition(commonconsensus.HighestIntendedRule(statuses))
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

// IntendedLeaderHealth returns the health snapshot of the pooler the highest
// intended rule names as leader (possibly only proposed), or nil if no rule is
// known or we have no health for that pooler. A named leader may never have been
// observed, so consumers that need its host and port gate on a non-nil result, not
// on reachability. Being named does not make it established or serving: judge that
// from the returned health.
func (sa *Shard) IntendedLeaderHealth() *multiorchdatapb.PoolerHealthState {
	return sa.PoolerHealth(sa.HighestIntendedRule().PossiblyUndecidedRule().GetLeaderId())
}
