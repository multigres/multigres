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
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/consensus"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// ConsensusManager owns the pooler's consensus state. It composes the two
// lower-level pieces in this package:
//
//   - promises: the durable term revocation (ConsensusPromises), persisted to
//     disk via pessimistic save-then-update.
//   - rules: the rule store (RuleStorer), which observes and writes the
//     current_rule row replicated through postgres WAL.
//
// and holds the in-memory, observational consensus state that is not durable:
// the recorded replication primary (what a coordinator last told this pooler
// via SetPrimary/Promote) and the time that leader was first observed. These
// live here rather than in ConsensusPromises, whose name promises durability.
//
// The promises/rules fields are unexported and reached through Promises()/Rules();
// callers build a manager with NewConsensusManager. Tests construct it the same
// way (via the manager package's test builder), so neither the fields nor a
// setter need to be exported.
//
// Locking: the observational fields use mu, which is deliberately separate from
// ConsensusPromises's own lock. ConsensusPromises's lock wraps disk writes, and
// GetReplicationPrimary is read on nearly every monitor tick; sharing one lock
// would serialize the latency-sensitive monitor loop behind term persistence.
//
// Later steps fold the remaining consensus state still scattered across the
// manager (leader resignation, cohort eligibility, suspected divergence, and
// the rewind backoff) into this type.
type ConsensusManager struct {
	promises *ConsensusPromises
	rules    RuleStorer

	mu sync.Mutex
	// replicationPrimary is held in memory only — see HighestKnownRule's proto
	// comment. Populated by RPCs that inform this pooler about the cluster state
	// (SetPrimary today; Promote via the same RecordTermPrimary entry point).
	// Restarts reset it to nil; coordinators re-inform the pooler.
	replicationPrimary *clustermetadatapb.ReplicationPrimary
	// leaderObservedAt is when RecordTermPrimary last saw the leader ID change (a
	// new primary was recorded). Zero until the first leader is recorded. Used to
	// measure how long a diverged follower's pg_rewind waited for that leader to
	// become rewind-ready: the delay is (rewind start) - leaderObservedAt, which
	// is ~0 when the same SetPrimary that learned the new leader could also rewind.
	leaderObservedAt time.Time
}

// NewConsensusManager builds a ConsensusManager over an already-constructed
// durable-promise store and rule store.
func NewConsensusManager(promises *ConsensusPromises, rules RuleStorer) *ConsensusManager {
	return &ConsensusManager{promises: promises, rules: rules}
}

// Promises returns the durable term-revocation store.
func (cm *ConsensusManager) Promises() *ConsensusPromises { return cm.promises }

// Rules returns the rule store.
func (cm *ConsensusManager) Rules() RuleStorer { return cm.rules }

// RecordTermPrimary updates the highest-known (rule, primary, rewind_ready)
// based on what this pooler has been told. A nil argument (or nil rule) is a
// no-op. Bump semantics:
//
//   - rule: updated only when strictly greater than the current value
//     (comparison by RuleNumber: coordinator_term then leader_subterm).
//
//   - primary: updated when the rule advances, OR when the supplied rule
//     equals the current rule but the primary's contact info has changed.
//     The same-rule-different-contact case covers a primary pooler being
//     re-homed (host or port changes) without a new election.
//
//   - rewind_ready: recorded alongside, and a change to it is itself enough to
//     record even at the same rule and contact — the leader completing its
//     post-promotion checkpoint flips rewind_ready false->true without a new
//     election, and a diverged follower gates its pg_rewind on that flip.
//
// Safe to call on no-op SetPrimary paths so the recorded values reflect
// everything the pooler has been told, regardless of whether postgres-side
// changes were applied.
func (cm *ConsensusManager) RecordTermPrimary(rp *clustermetadatapb.ReplicationPrimary) {
	rule := rp.GetRule()
	if rule == nil {
		return
	}
	primary := rp.GetPrimary()
	rewindReady := rp.GetRewindReady()
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cmp := consensus.CompareRuleNumbers(rule.GetRuleNumber(), cm.replicationPrimary.GetRule().GetRuleNumber())
	if cmp < 0 {
		return
	}
	if cmp == 0 &&
		(primary == nil || proto.Equal(primary, cm.replicationPrimary.GetPrimary())) &&
		rewindReady == cm.replicationPrimary.GetRewindReady() {
		return
	}
	// Build the next value by starting from the existing fields (via getters,
	// which are nil-safe) and overlaying the updates we want to apply.
	next := &clustermetadatapb.ReplicationPrimary{
		Rule:        cm.replicationPrimary.GetRule(),
		Primary:     cm.replicationPrimary.GetPrimary(),
		RewindReady: rewindReady,
	}
	if cmp > 0 {
		next.Rule = proto.Clone(rule).(*clustermetadatapb.ShardRule)
	}
	// Update primary only when one is supplied; an RPC that advances the rule
	// without re-stating the primary (or a future WAL-observation path) leaves
	// the previously-recorded contact info in place.
	if primary != nil {
		next.Primary = proto.Clone(primary).(*clustermetadatapb.PoolerAddress)
	}
	// Stamp the time the leader identity changed, so a subsequent rewind toward
	// this leader can report how long it waited for the leader to become
	// rewind-ready (~0 when this same call already advances to a rewind-ready
	// leader).
	if !proto.Equal(cm.replicationPrimary.GetPrimary().GetId(), next.GetPrimary().GetId()) {
		cm.leaderObservedAt = time.Now()
	}
	cm.replicationPrimary = next
}

// LeaderObservedAt returns when RecordTermPrimary last recorded a change of leader
// identity, or the zero time if no leader has been recorded yet.
func (cm *ConsensusManager) LeaderObservedAt() time.Time {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.leaderObservedAt
}

// GetReplicationPrimary returns a copy of the primary + rule this pooler has
// been told to use, or nil if it has never been informed. The returned message
// is the same one exposed in ConsensusStatus.
func (cm *ConsensusManager) GetReplicationPrimary() *clustermetadatapb.ReplicationPrimary {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.replicationPrimary == nil {
		return nil
	}
	return proto.Clone(cm.replicationPrimary).(*clustermetadatapb.ReplicationPrimary)
}

// MarkSelfRewindReady records that this pooler is safe to pg_rewind from: it has
// checkpointed onto its current timeline. The postgres monitor calls this once it
// observes the condition (and the async post-promotion checkpoint can flip it as
// soon as it completes). It is one-way: the flag auto-clears whenever
// RecordTermPrimary installs a fresh ReplicationPrimary — a new coordinator term,
// or a SetPrimary naming a different leader (resignation / restart-as-replica) —
// since the new record defaults rewind_ready to false. Returns true if the value
// changed, so callers can broadcast the new health promptly.
//
// selfID and expectedCoordinatorTerm guard against marking the wrong record:
// only the pooler the record names as primary may declare itself rewind-ready,
// and only at the coordinator term it expects. The term check matters for the
// async post-promotion checkpoint — by the time it completes a newer term (a
// re-promotion or a different leader) may have replaced the record, and we must
// not stamp rewind_ready onto that newer term whose checkpoint hasn't completed.
// Both checks happen under the lock so the decision is atomic. (Whether this
// pooler currently holds non-resigned leadership is the caller's concern —
// resignation state lives in the manager, not here.) No-op if no
// ReplicationPrimary has been recorded yet.
func (cm *ConsensusManager) MarkSelfRewindReady(selfID *clustermetadatapb.ID, expectedCoordinatorTerm int64) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.replicationPrimary == nil || cm.replicationPrimary.GetRewindReady() {
		return false
	}
	if !proto.Equal(cm.replicationPrimary.GetPrimary().GetId(), selfID) {
		return false
	}
	if cm.replicationPrimary.GetRule().GetRuleNumber().GetCoordinatorTerm() != expectedCoordinatorTerm {
		return false
	}
	next := proto.Clone(cm.replicationPrimary).(*clustermetadatapb.ReplicationPrimary)
	next.RewindReady = true
	cm.replicationPrimary = next
	return true
}
