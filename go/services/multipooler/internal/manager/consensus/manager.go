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
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/consensus"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
)

// Broadcaster pushes an immediate health snapshot to subscribers. It is
// satisfied by the manager's health streamer. ConsensusManager calls it after a
// change the coordinator should see without waiting for the next heartbeat
// (e.g. a resignation or cohort-eligibility flip).
type Broadcaster interface {
	Broadcast()
}

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
// Concurrency — each field documents its own access rule; in summary:
//
//   - promises, rules: set once at construction, immutable thereafter. Each has
//     its own internal synchronization (ConsensusPromises wraps a disk lock;
//     RuleStorer guards its cache), so callers just use the returned value.
//   - replicationPrimary, leaderObservedAt: guarded by mu. mu is deliberately
//     NOT shared with ConsensusPromises's lock: that lock wraps disk writes, and
//     GetReplicationPrimary is read on nearly every monitor tick — sharing would
//     serialize the latency-sensitive monitor loop behind term persistence.
//   - resignedLeaderAtTerm, cohortEligibility: atomics, NOT under mu. Production
//     writes go through the manager under the action lock (so writers are
//     serialized there); the lock-free readers (Status/health snapshot, monitor
//     decision phase) load them without any lock. The atomic makes those reads
//     safe; the action lock orders the writers. They are independent scalars and
//     never need to be read consistently with each other or with mu's fields.
//
// The mutating methods assert the action lock is held (writes are serialized by
// it), with one exception documented on MarkSelfRewindReady.
// SetResignedLeaderAtTerm and SetCohortEligibility push an immediate health
// broadcast through the injected Broadcaster on a real change (the coordinator
// must see those promptly); nothing holds mu or any lock across that broadcast.
type ConsensusManager struct {
	// promises and rules are immutable after construction (see Concurrency above).
	promises *ConsensusPromises
	rules    RuleStorer
	// broadcaster pushes an immediate health snapshot after a change the
	// coordinator should see promptly. Immutable after construction; may be nil
	// (then broadcasts are skipped — used by unit tests with no health stream).
	broadcaster Broadcaster

	// mu guards replicationPrimary and leaderObservedAt only.
	mu sync.Mutex
	// replicationPrimary is held in memory only — see HighestKnownRule's proto
	// comment. Populated by RPCs that inform this pooler about the cluster state
	// (SetPrimary today; Promote via the same RecordTermPrimary entry point).
	// Restarts reset it to nil; coordinators re-inform the pooler. Guarded by mu.
	replicationPrimary *clustermetadatapb.ReplicationPrimary
	// leaderObservedAt is when RecordTermPrimary last saw the leader ID change (a
	// new primary was recorded). Zero until the first leader is recorded. Used to
	// measure how long a diverged follower's pg_rewind waited for that leader to
	// become rewind-ready: the delay is (rewind start) - leaderObservedAt, which
	// is ~0 when the same SetPrimary that learned the new leader could also rewind.
	// Guarded by mu.
	leaderObservedAt time.Time
	// rewindWaitEmittedFor is the leaderObservedAt value the rewind-wait metric
	// was last emitted for, so a rewind that fails and is re-attempted against the
	// same leader is counted once. Touched only on the restart-as-standby path
	// under the action lock; guarded by mu for safe publication alongside
	// leaderObservedAt.
	rewindWaitEmittedFor time.Time

	// resignedLeaderAtTerm is the consensus term at which this node voluntarily
	// resigned as primary (via emergency-demote or graceful shutdown), or 0 if it
	// has not resigned. A non-zero value tells the coordinator to trigger an
	// immediate election. Cleared when this node is elected primary again. Atomic;
	// production writes are serialized by the action lock (see Concurrency above).
	resignedLeaderAtTerm atomic.Int64
	// cohortEligibility is this node's self-reported willingness to be a member of
	// the consensus cohort, holding a clustermetadatapb.CohortEligibilitySignal.
	// Atomic; production writes are serialized by the action lock.
	cohortEligibility atomic.Int32
	// suspectedDivergence marks that this node's WAL may have diverged from the
	// cluster's chosen history, so the next restart-as-standby should run
	// pg_rewind to drop potential phantom / non-durable WAL. Set when consensus
	// ends this node's term (failover) or a replica can't replicate despite
	// connecting. Atomic; production writes happen on action-lock paths, and the
	// lock-free health-status reader loads it.
	suspectedDivergence atomic.Bool
}

// Deps are the lower-level dependencies a production ConsensusManager builds its
// components from. ID is this pooler's identity (used to root the durable promise
// store and the sync-standby manager). LoadPromises asks the constructor to load
// the persisted term from disk (the consensus-enabled path). Broadcaster — the
// manager's health streamer — may be nil, in which case broadcasts are skipped.
type Deps struct {
	Logger       *slog.Logger
	QueryService executor.InternalQueryService
	PoolerDir    string
	ID           *clustermetadatapb.ID
	Broadcaster  Broadcaster
	LoadPromises bool
}

// NewConsensusManager builds a ConsensusManager and the components it owns — the
// durable promise store and the rule store (with its sync-standby manager) — from
// deps, so the consensus package owns its own wiring. When deps.LoadPromises is
// set it loads the persisted term from disk, returning an error on a read/parse
// failure. Tests that need to inject fakes use NewManagerForTesting.
func NewConsensusManager(deps Deps) (*ConsensusManager, error) {
	promises := NewConsensusPromises(deps.PoolerDir, deps.ID)
	if deps.LoadPromises {
		if _, err := promises.Load(); err != nil {
			return nil, fmt.Errorf("failed to load consensus state from disk: %w", err)
		}
	}
	rules := NewRuleStore(deps.Logger, deps.QueryService, NewSyncStandbyManager(deps.Logger, deps.QueryService, deps.ID))
	return newConsensusManager(promises, rules, deps.Broadcaster), nil
}

// newConsensusManager wires a ConsensusManager over already-constructed
// components. Shared by NewConsensusManager (production) and NewManagerForTesting
// (tests, which inject fakes). broadcaster may be nil, in which case broadcasts
// are skipped.
func newConsensusManager(promises *ConsensusPromises, rules RuleStorer, broadcaster Broadcaster) *ConsensusManager {
	cm := &ConsensusManager{promises: promises, rules: rules, broadcaster: broadcaster}
	// Default to ELIGIBLE — a fresh node is willing to join the cohort. (The
	// atomic's zero value is the proto's UNSPECIFIED, so set it explicitly.)
	cm.cohortEligibility.Store(int32(clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE))
	return cm
}

// broadcast pushes an immediate health snapshot, if a broadcaster is wired.
// Never call while holding cm.mu: the snapshot build reads mu-guarded fields.
func (cm *ConsensusManager) broadcast() {
	if cm.broadcaster != nil {
		cm.broadcaster.Broadcast()
	}
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
func (cm *ConsensusManager) RecordTermPrimary(ctx context.Context, rp *clustermetadatapb.ReplicationPrimary) error {
	if err := actionlock.AssertActionLockHeld(ctx); err != nil {
		return err
	}
	rule := rp.GetRule()
	if rule == nil {
		return nil
	}
	primary := rp.GetPrimary()
	rewindReady := rp.GetRewindReady()
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cmp := consensus.CompareRuleNumbers(rule.GetRuleNumber(), cm.replicationPrimary.GetRule().GetRuleNumber())
	if cmp < 0 {
		return nil
	}
	if cmp == 0 &&
		(primary == nil || proto.Equal(primary, cm.replicationPrimary.GetPrimary())) &&
		rewindReady == cm.replicationPrimary.GetRewindReady() {
		return nil
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
	return nil
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
// Unlike the other mutators, MarkSelfRewindReady does NOT assert the action
// lock: the async post-promotion checkpoint goroutine (see promoteStandbyToPrimary)
// calls it without holding the lock. Its safety comes from mu plus the selfID /
// expectedCoordinatorTerm guards, which ensure it only stamps the record it still
// owns. This lock-free writer is also why replicationPrimary needs mu, not just
// action-lock serialization.
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

// ResignedLeaderAtTerm returns the term at which this node requested demotion as
// primary, or 0 if it has not resigned.
func (cm *ConsensusManager) ResignedLeaderAtTerm() int64 {
	return cm.resignedLeaderAtTerm.Load()
}

// SetResignedLeaderAtTerm records that this node is requesting demotion as
// primary for the given term. When the value changes it pushes an immediate
// health broadcast so the coordinator can trigger an election without waiting
// for the next heartbeat. Requires the action lock (ctx must be an action-lock
// context), which serializes writers.
func (cm *ConsensusManager) SetResignedLeaderAtTerm(ctx context.Context, term int64) error {
	if err := actionlock.AssertActionLockHeld(ctx); err != nil {
		return err
	}
	if cm.resignedLeaderAtTerm.Swap(term) != term {
		cm.broadcast()
	}
	return nil
}

// ClearResignedLeaderAtTerm clears the leadership-demotion request (sets it to
// 0). Called when this node is appointed primary at a new term; that promotion
// flow broadcasts, so this does not. Requires the action lock.
func (cm *ConsensusManager) ClearResignedLeaderAtTerm(ctx context.Context) error {
	if err := actionlock.AssertActionLockHeld(ctx); err != nil {
		return err
	}
	cm.resignedLeaderAtTerm.Store(0)
	return nil
}

// CohortEligibility returns this node's self-reported cohort eligibility.
func (cm *ConsensusManager) CohortEligibility() clustermetadatapb.CohortEligibilitySignal {
	return clustermetadatapb.CohortEligibilitySignal(cm.cohortEligibility.Load())
}

// SetCohortEligibility records this node's cohort eligibility. When the value
// changes it pushes an immediate health broadcast so the coordinator sees the
// new value without waiting for the next heartbeat. Requires the action lock
// (ctx must be an action-lock context), which serializes writers.
func (cm *ConsensusManager) SetCohortEligibility(ctx context.Context, signal clustermetadatapb.CohortEligibilitySignal) error {
	if err := actionlock.AssertActionLockHeld(ctx); err != nil {
		return err
	}
	if cm.cohortEligibility.Swap(int32(signal)) != int32(signal) {
		cm.broadcast()
	}
	return nil
}

// SuspectedDivergence reports whether this node's WAL may have diverged from the
// cluster's chosen history (so the next restart-as-standby should pg_rewind).
func (cm *ConsensusManager) SuspectedDivergence() bool {
	return cm.suspectedDivergence.Load()
}

// SetSuspectedDivergence sets or clears the suspected-divergence flag, returning
// whether the value changed (so a caller clearing it can log the transition).
// Requires the action lock (ctx must be an action-lock context).
func (cm *ConsensusManager) SetSuspectedDivergence(ctx context.Context, suspected bool) (changed bool, err error) {
	if err := actionlock.AssertActionLockHeld(ctx); err != nil {
		return false, err
	}
	return cm.suspectedDivergence.Swap(suspected) != suspected, nil
}

// RewindWaitEmittedFor returns the leaderObservedAt value the rewind-wait metric
// was last emitted for.
func (cm *ConsensusManager) RewindWaitEmittedFor() time.Time {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.rewindWaitEmittedFor
}

// SetRewindWaitEmittedFor records the leaderObservedAt value the rewind-wait
// metric was just emitted for, so the same leader is not double-counted.
func (cm *ConsensusManager) SetRewindWaitEmittedFor(t time.Time) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.rewindWaitEmittedFor = t
}
