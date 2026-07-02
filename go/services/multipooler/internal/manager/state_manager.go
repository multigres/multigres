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
	"log/slog"
	"sync"

	"golang.org/x/sync/errgroup"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

// StateAware is implemented by components that need to react to serving state changes.
// Each component decides internally what action to take based on the target state.
// For example, a heartbeat component starts writing when it is the writable leader
// and serving, while a query service component adjusts which queries it accepts.
type StateAware interface {
	// OnStateChange is called when the effective serving state changes; the
	// component transitions to match it. The state carries the derived routing
	// role (writability) and the serving intent — see servingstate.State. A
	// component that needs consensus leadership reads it from the consensus
	// snapshot directly; the serving layer does not carry it.
	OnStateChange(ctx context.Context, state servingstate.State) error
}

// StateManager coordinates serving state transitions across components.
//
// The current state lives in the poolerRecord (Type and ServingStatus),
// which is the source of truth for topology.
//
// On Mutate, the manager fans out OnStateChange to all registered components
// in parallel, waits for completion, then updates the record via Mutate. The
// Mutate also schedules an asynchronous publish so callers cannot forget to
// reflect the new state to etcd.
type StateManager struct {
	mu     sync.Mutex
	logger *slog.Logger

	// Current state lives in record.Type() / .ServingStatus(). The StateManager
	// is the exclusive caller of record.Mutate for Type/ServingStatus.
	record *poolerRecord

	// postgresPrimary is the physical recovery state (!pg_is_in_recovery), fed by
	// the postgres monitor via Mutate's PostgresPrimary field. Unlike
	// Type/ServingStatus it is local-only and never published to topology. Guarded by mu.
	postgresPrimary bool

	// consensusStatus returns this pooler's live consensus snapshot (e.g.
	// ConsensusManager.CachedConsensusStatus). Injected — queried live, not stored —
	// so the derived routing role (write-safety) reflects a revocation or a new
	// committed/known-rule observation without any explicit set. See deriveRoutingRole.
	consensusStatus func() *clustermetadatapb.ConsensusStatus

	// lastFannedOut is the effective state most recently delivered to components,
	// or nil if none has been yet. Used to skip redundant fan-outs: a record-only
	// change (e.g. an obs rule-number bump) that leaves the {routingRole,
	// servingStatus} tuple identical must not re-notify components.
	lastFannedOut *servingstate.State

	// Registered components that react to state changes.
	components []StateAware
}

// deriveRoutingRole computes the write-safety routing role from the physical
// recovery state and the live consensus snapshot: PRIMARY iff postgres is out of
// recovery AND this pooler is the active consensus leader (committed, non-revoked,
// not superseded by a higher known rule — see commonconsensus.IsActiveLeader);
// REPLICA otherwise. cs may be nil (treated as not a leader). This is the single
// place the base facts (recovery, consensus) become the effective routing role.
func deriveRoutingRole(postgresPrimary bool, cs *clustermetadatapb.ConsensusStatus) servingstate.RoutingRole {
	if postgresPrimary && commonconsensus.IsActiveLeader(cs) {
		return servingstate.RoutingRolePrimary
	}
	return servingstate.RoutingRoleReplica
}

// routingLeadershipObs builds the self-leadership observation persisted to the
// record to reflect the routing role: non-nil (naming self at the committed rule)
// when routing PRIMARY, nil otherwise. It intentionally reads the *committed*
// rule — a routing PRIMARY is by definition the active committed leader
// (IsActiveLeader), so this both matches the routing role and carries write
// authority, never a not-yet-committed rule. Returns nil when not PRIMARY so the
// record's Type ⇔ SelfLeadership invariant holds (Type PRIMARY ⇔ obs present ⇔
// routing PRIMARY).
func routingLeadershipObs(routingRole servingstate.RoutingRole, cs *clustermetadatapb.ConsensusStatus) *clustermetadatapb.LeaderObservation {
	if routingRole != servingstate.RoutingRolePrimary {
		return nil
	}
	committed := cs.GetCurrentPosition().GetRule()
	return &clustermetadatapb.LeaderObservation{
		LeaderId:         cs.GetId(),
		LeaderRuleNumber: committed.GetRuleNumber(),
	}
}

// NewStateManager creates a new StateManager. consensusStatus returns the live
// consensus snapshot (e.g. ConsensusManager.CachedConsensusStatus), combined with
// the cached recovery state to derive the routing role / write-safety live.
func NewStateManager(
	logger *slog.Logger,
	record *poolerRecord,
	consensusStatus func() *clustermetadatapb.ConsensusStatus,
	components ...StateAware,
) *StateManager {
	return &StateManager{
		logger:          logger,
		record:          record,
		consensusStatus: consensusStatus,
		components:      components,
	}
}

// Register adds a component to be notified on state changes.
// This is used for components created after the manager (e.g., ReplTracker).
// Must not be called concurrently with Mutate.
func (ssm *StateManager) Register(component StateAware) {
	ssm.mu.Lock()
	defer ssm.mu.Unlock()
	ssm.components = append(ssm.components, component)
}

// RegisterAndSync adds a component and immediately syncs it to the current state.
// This is used for components created after the initial state transition (e.g.,
// ReplTracker is created after Open() has already transitioned to SERVING).
// The component is initialized from the record, which is the source of truth.
func (ssm *StateManager) RegisterAndSync(ctx context.Context, component StateAware) error {
	ssm.mu.Lock()
	defer ssm.mu.Unlock()
	ssm.components = append(ssm.components, component)
	return component.OnStateChange(ctx, servingstate.State{
		RoutingRole:   deriveRoutingRole(ssm.postgresPrimary, ssm.consensusStatus()),
		ServingStatus: ssm.record.ServingStatus(),
	})
}

// fanOutLocked notifies every registered component of the target effective state
// in parallel and waits for them to converge. Caller must hold mu.
func (ssm *StateManager) fanOutLocked(ctx context.Context, target servingstate.State) error {
	if ssm.lastFannedOut != nil && *ssm.lastFannedOut == target {
		// Components are already at this effective state; nothing to notify.
		return nil
	}
	g, ctx := errgroup.WithContext(ctx)
	for _, c := range ssm.components {
		g.Go(func() error {
			return c.OnStateChange(ctx, target)
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	// Record the delivered state only after every component converges, so a
	// partial failure re-fans-out on retry rather than being deduped away.
	saved := target
	ssm.lastFannedOut = &saved
	return nil
}

// servingStateMutation is the mutable view passed to Mutate. A callback changes
// any subset of fields and sees the previous values; fields it leaves alone are
// carried forward. These are the base facts callers poke: PostgresPrimary is the
// physical recovery state (local-only); ServingStatus is the serving intent
// (topology-backed).
//
// There is deliberately no leadership or PoolerType field. The routing role, the
// record's SelfLeadership observation, and PoolerType are all *derived* from the
// live consensus snapshot plus PostgresPrimary — see deriveRoutingRole /
// routingLeadershipObs. Callers change consensus (via the rule store) and poke
// recovery/serving here; leadership follows.
type servingStateMutation struct {
	// PostgresPrimary is the physical recovery state (!pg_is_in_recovery).
	// TODO: flip to an InRecovery field (invert the sense) so callers state the
	// base fact postgres reports directly, removing the double-negative ambiguity
	// ("primary" is overloaded; "in recovery" is what pg_is_in_recovery() means).
	PostgresPrimary bool
	ServingStatus   clustermetadatapb.PoolerServingStatus
}

// Mutate applies fn to the current state, fans the resulting effective state out
// to all components, and persists any topology-backed change to the poolerRecord.
// fn may change any subset of fields and read the previous values; fields it
// leaves alone are preserved.
//
// The action lock is always required: record.Mutate asserts it for topology
// writes, and routing physical-state changes through the same gate keeps a
// recovery flip from racing a role transition. It is asserted up front, before
// fanning out, so a caller without the lock cannot half-transition the components
// and only fail later at record.Mutate.
//
// PoolerType is derived from the leadership observation, and the poolerRecord
// enforces the Type ⇔ SelfLeadership invariant.
func (ssm *StateManager) Mutate(ctx context.Context, fn func(s *servingStateMutation)) error {
	if err := actionlock.AssertActionLockHeld(ctx); err != nil {
		return err
	}

	ssm.mu.Lock()
	defer ssm.mu.Unlock()

	cur := servingStateMutation{
		PostgresPrimary: ssm.postgresPrimary,
		ServingStatus:   ssm.record.ServingStatus(),
	}
	next := cur
	fn(&next)

	// The effective state components react to. routingRole is derived live from
	// recovery + the consensus snapshot, so it can change (the committed rule
	// lands, a revocation arrives) even when serving/recovery have not — comparing
	// the whole target against the last fan-out is what ensures OnStateChange fires
	// on ANY effective-state change.
	cs := ssm.consensusStatus()
	target := servingstate.State{
		RoutingRole:   deriveRoutingRole(next.PostgresPrimary, cs),
		ServingStatus: next.ServingStatus,
	}
	if ssm.lastFannedOut != nil && *ssm.lastFannedOut == target {
		// Nothing components react to changed; no fan-out, no record write. (An
		// obs rule-number bump that keeps the same routing role is published via
		// the health stream, not the record.)
		//
		// Still refresh the cached recovery fact: postgres may have left/entered
		// recovery without flipping the derived routing role (e.g. while this
		// pooler is not the active leader, so it stays REPLICA either way). The
		// cache is a physical observation independent of the fan-out, and
		// RegisterAndSync reads it to derive the role for a late-registered
		// component — leaving it stale could hand that component the wrong role.
		ssm.postgresPrimary = next.PostgresPrimary
		return nil
	}

	ssm.logger.InfoContext(ctx, "Applying serving state",
		"routing_role", target.RoutingRole, "status", target.ServingStatus, "postgres_primary", next.PostgresPrimary,
		"prev_status", cur.ServingStatus, "prev_postgres_primary", cur.PostgresPrimary)

	// Fan out first so a failed transition leaves the record untouched.
	if err := ssm.fanOutLocked(ctx, target); err != nil {
		return err
	}
	// Components converged — keep the local physical state in step with what they
	// received before the (fallible) record write.
	ssm.postgresPrimary = next.PostgresPrimary

	// Project the routing role onto the topology record: Type PRIMARY and a
	// self-leadership observation iff routing PRIMARY (writable). Type and obs move
	// together, preserving the record's Type ⇔ SelfLeadership invariant. Write only
	// when the projected Type or serving status actually differs from the record.
	//
	// TODO: this record projection could itself be a StateAware — give poolerRecord
	// an OnStateChange(State) that projects routing role onto Type/SelfLeadership and
	// register it as a component, so Mutate just fans out and owns no special-case
	// record write. It would need the ServingStatus (already on State) and to be
	// ordered so the record write follows component convergence.
	obs := routingLeadershipObs(target.RoutingRole, cs)
	nextType := poolerTypeForLeader(obs != nil)
	if nextType != ssm.record.Type() || next.ServingStatus != cur.ServingStatus {
		if err := ssm.record.Mutate(ctx, func(s *MutablePoolerRecordState) {
			s.Type = nextType
			s.SelfLeadership = obs
			s.ServingStatus = next.ServingStatus
		}); err != nil {
			return err
		}
	}
	return nil
}

// hasDrift reports whether the effective state last fanned out to components is
// stale relative to freshly-observed inputs — postgres recovery (postgresPrimary)
// and the live consensus snapshot, from which the routing role is derived, plus
// the record's serving status. When true, the monitor calls fixDrift to
// re-derive and re-fan-out under the action lock, so components (notably the
// query server's write gate) never lag a committed-rule, revocation, recovery,
// or serving change. hasDrift (detect) and fixDrift (correct) derive the routing
// role from the same inputs, so a drift can never be detected but not corrected.
//
// A never-fanned state and DRAINING both always report drift: DRAINING is a
// transient drain this loop owns, and reconciling re-verifies it (completing the
// DRAINING->SERVING transition once the node is healthy and role-aligned).
func (ssm *StateManager) hasDrift(postgresPrimary bool) bool {
	ssm.mu.Lock()
	defer ssm.mu.Unlock()
	if ssm.lastFannedOut == nil || ssm.record.ServingStatus() == clustermetadatapb.PoolerServingStatus_DRAINING {
		return true
	}
	observed := servingstate.State{
		RoutingRole:   deriveRoutingRole(postgresPrimary, ssm.consensusStatus()),
		ServingStatus: ssm.record.ServingStatus(),
	}
	return *ssm.lastFannedOut != observed
}

// fixDrift re-applies the effective state from a freshly-observed recovery flag
// (and the live consensus snapshot, read inside Mutate): it re-fans-out to
// components and re-projects the record. It is the correction paired with
// hasDrift's detection. A DRAINING status is completed to SERVING (the transient
// drain the monitor owns); any other status is preserved (DISABLED stays
// not-serving). Requires the action lock (asserted by Mutate).
func (ssm *StateManager) fixDrift(ctx context.Context, postgresPrimary bool) error {
	return ssm.Mutate(ctx, func(s *servingStateMutation) {
		s.PostgresPrimary = postgresPrimary
		if s.ServingStatus == clustermetadatapb.PoolerServingStatus_DRAINING {
			s.ServingStatus = clustermetadatapb.PoolerServingStatus_SERVING
		}
	})
}
