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
	"google.golang.org/protobuf/proto"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
	"github.com/multigres/multigres/go/services/multipooler/internal/pgmode"
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
	//
	// INVARIANT — OnStateChange must be a pure sink: it consumes the state
	// (transition, publish, project) and must NOT initiate a state change.
	// Specifically it must not call StateManager.Mutate/Recalc (they assert the
	// action lock and re-enter ssm.mu, which is held across the fan-out —
	// deadlock) nor trigger a consensus mutation that would loop back into a
	// recalc. Handlers run concurrently under ssm.mu; keep them non-blocking
	// beyond waiting on their own subsystem.
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
	//
	// TODO: move serving status (and lifecycle status) ownership into the
	// StateManager so the poolerRecord becomes a pure reflection of effective
	// state rather than its source of truth — the record would register as a
	// StateAware sink and republish in the background, and nothing in multipooler
	// would read pm.record.* for a decision. Routing role already works this way
	// (derived here, not stored); serving/lifecycle are the remaining SoT to flip.
	record *poolerRecord

	// pgMode is the physical postgres recovery mode (see PostgresMode), fed by the
	// postgres monitor via Mutate's PostgresMode field. Guarded by mu.
	pgMode pgmode.Mode

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

// deriveRoutingState computes the full routing state (role + qualifying rule)
// from the physical recovery mode and the live consensus snapshot. It is the
// low-level derivation — the full state is authoritative; extract .Role from the
// result only where the rule cannot travel (metric tags, log lines, backup
// annotations).
//
// Role is PRIMARY iff postgres is out of recovery AND this pooler is the active
// consensus leader (committed, non-revoked, not superseded by a higher known rule
// — see commonconsensus.IsActiveLeader); REPLICA otherwise. cs may be nil (treated
// as not a leader). The qualifying Rule is the *committed* rule naming this pooler
// when PRIMARY (write authority — never a not-yet-committed rule), and the
// highest rule this pooler has known when REPLICA (advisory — lets the gateway
// spot a stale routing primary). This is the single place the base facts
// (recovery, consensus) become the effective routing state components fan out on
// and the health streamer / topology record project.
func deriveRoutingState(pgMode pgmode.Mode, cs *clustermetadatapb.ConsensusStatus) servingstate.RoutingState {
	if pgMode.OutOfRecovery() && commonconsensus.IsActiveLeader(cs) {
		return servingstate.RoutingState{
			Role: servingstate.RoutingRolePrimary,
			// IsActiveLeader() returns false for undecided rules, so there's no undecided proposal to consider here.
			Rule: cs.GetCurrentPosition().GetPosition().GetDecision().GetRuleNumber(),
		}
	}
	return servingstate.RoutingState{
		Role: servingstate.RoutingRoleReplica,
		Rule: commonconsensus.PossiblyUndecidedRule(commonconsensus.HighestKnownRule([]*clustermetadatapb.ConsensusStatus{cs})).GetRuleNumber(),
	}
}

// RoutingRole returns this pooler's current routing role, derived live from the
// physical postgres mode and the consensus snapshot. This derivation is the
// source of truth for the routing role; the record's routing_state is a
// projection of it. Internal decisions should read the role here rather than
// through the derived PoolerType label.
func (ssm *StateManager) RoutingRole() clustermetadatapb.RoutingRole {
	ssm.mu.Lock()
	defer ssm.mu.Unlock()
	return deriveRoutingState(ssm.pgMode, ssm.consensusStatus()).Role.ToProto()
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
		Routing:       deriveRoutingState(ssm.pgMode, ssm.consensusStatus()),
		ServingStatus: ssm.record.ServingStatus(),
	})
}

// sameFanout reports whether two states are equivalent for fan-out dedup. It
// compares the full routing state (role + qualifying rule) and ServingStatus.
// The rule is included because the health stream carries the full routing state
// live: a replica's highest-known-rule bump should reach subscribers even with
// the role unchanged (the gateway uses it to spot a stale routing primary). The
// extra etcd churn this could cause is absorbed at the publish boundary —
// routingStateForPublish drops a replica's routing_state, so successive replica
// states dedup to an identical published form. Re-notifying transition-style
// components (query server, heartbeat) on a rule-only bump is idempotent: they
// gate on role + serving.
func sameFanout(a, b servingstate.State) bool {
	return a.ServingStatus == b.ServingStatus &&
		a.Routing.Role == b.Routing.Role &&
		proto.Equal(a.Routing.Rule, b.Routing.Rule)
}

// fanOutLocked notifies every registered component of the target effective state
// in parallel and waits for them to converge. Caller must hold mu.
func (ssm *StateManager) fanOutLocked(ctx context.Context, target servingstate.State) error {
	if ssm.lastFannedOut != nil && sameFanout(*ssm.lastFannedOut, target) {
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
// carried forward. These are the base facts callers poke: PostgresMode is the
// physical recovery mode (local-only); ServingStatus is the serving intent
// (topology-backed).
//
// There is deliberately no leadership or PoolerType field. The routing role, the
// record's SelfLeadership observation, and PoolerType are all *derived* from the
// live consensus snapshot plus PostgresMode — see deriveRoutingRole /
// routingLeadershipObs. Callers change consensus (via the rule store) and poke
// recovery/serving here; leadership follows.
type servingStateMutation struct {
	// PostgresMode is the physical recovery mode postgres reports
	// (pg_is_in_recovery): pgmode.Primary or pgmode.InRecovery.
	PostgresMode  pgmode.Mode
	ServingStatus clustermetadatapb.PoolerServingStatus
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
		PostgresMode:  ssm.pgMode,
		ServingStatus: ssm.record.ServingStatus(),
	}
	next := cur
	fn(&next)

	// The effective state components react to. routingRole is derived live from
	// recovery + the consensus snapshot, so it can change (the committed rule
	// lands, a revocation arrives) even when serving/recovery have not — comparing
	// the whole target against the last fan-out is what ensures OnStateChange fires
	// on ANY effective-state change.
	cs := ssm.consensusStatus()
	// The full routing state (role + qualifying rule) is derived here, so
	// components that advertise it (the health streamer, the record projection
	// below) publish it as a pure function of the fanned state — no explicit push.
	target := servingstate.State{
		Routing:       deriveRoutingState(next.PostgresMode, cs),
		ServingStatus: next.ServingStatus,
	}
	if ssm.lastFannedOut != nil && sameFanout(*ssm.lastFannedOut, target) {
		// The effective state (routing role + rule + serving) is unchanged; no
		// fan-out, no record write.
		//
		// Still refresh the cached recovery fact: postgres may have left/entered
		// recovery without flipping the derived routing role (e.g. while this
		// pooler is not the active leader, so it stays REPLICA either way). The
		// cache is a physical observation independent of the fan-out, and
		// RegisterAndSync reads it to derive the role for a late-registered
		// component — leaving it stale could hand that component the wrong role.
		ssm.pgMode = next.PostgresMode
		return nil
	}

	// This point is only reached when the effective state actually changed (the
	// dedup above returned otherwise), so log it as a transition with prev -> new
	// for each field. prev_routing_role comes from the last fan-out; it is unknown
	// on the first transition (nil lastFannedOut).
	prevRoutingRole := servingstate.RoutingRoleUnknown
	if ssm.lastFannedOut != nil {
		prevRoutingRole = ssm.lastFannedOut.Routing.Role
	}
	ssm.logger.InfoContext(ctx, "Serving state changed",
		"routing_role", target.Routing.Role.String(), "prev_routing_role", prevRoutingRole.String(),
		"status", target.ServingStatus, "prev_status", cur.ServingStatus,
		"postgres_mode", next.PostgresMode, "prev_postgres_mode", cur.PostgresMode)

	// Fan out first so a failed transition leaves the record untouched.
	if err := ssm.fanOutLocked(ctx, target); err != nil {
		return err
	}
	// Components converged — keep the local physical state in step with what they
	// received before the (fallible) record write.
	ssm.pgMode = next.PostgresMode

	// Project the full routing state onto the topology record. The record holds
	// the whole routing state (role + rule, replicas included); PoolerType is
	// derived from it at publish and only the writable PRIMARY's routing_state
	// reaches etcd (the publisher drops it for replicas — see routingStateForPublish).
	// Write only when the routing state or serving status actually differs.
	//
	// TODO: this record projection could itself be a StateAware — give poolerRecord
	// an OnStateChange(State) that projects the routing state and register it as a
	// component, so Mutate just fans out and owns no special-case record write.
	routingProto := target.Routing.ToProto()
	if !proto.Equal(routingProto, ssm.record.RoutingState()) || next.ServingStatus != cur.ServingStatus {
		if err := ssm.record.Mutate(ctx, func(s *MutablePoolerRecordState) {
			s.RoutingState = routingProto
			s.ServingStatus = next.ServingStatus
		}); err != nil {
			return err
		}
	}
	return nil
}

// hasDrift reports whether the effective state last fanned out to components is
// stale relative to freshly-observed inputs — postgres recovery (pgMode)
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
func (ssm *StateManager) hasDrift(pgMode pgmode.Mode) bool {
	ssm.mu.Lock()
	defer ssm.mu.Unlock()
	if ssm.lastFannedOut == nil || ssm.record.ServingStatus() == clustermetadatapb.PoolerServingStatus_DRAINING {
		return true
	}
	observed := servingstate.State{
		Routing:       deriveRoutingState(pgMode, ssm.consensusStatus()),
		ServingStatus: ssm.record.ServingStatus(),
	}
	// Compare with sameFanout (proto-aware), not raw struct equality: the routing
	// state carries a *RuleNumber pointer that == would compare by identity, so a
	// freshly-derived observed state would always look different and re-fan forever.
	return !sameFanout(*ssm.lastFannedOut, observed)
}

// fixDrift re-applies the effective state from a freshly-observed recovery flag
// (and the live consensus snapshot, read inside Mutate): it re-fans-out to
// components and re-projects the record. It is the correction paired with
// hasDrift's detection. A DRAINING status is completed to SERVING (the transient
// drain the monitor owns); any other status is preserved (DISABLED stays
// not-serving). Requires the action lock (asserted by Mutate).
func (ssm *StateManager) fixDrift(ctx context.Context, pgMode pgmode.Mode) error {
	return ssm.Mutate(ctx, func(s *servingStateMutation) {
		s.PostgresMode = pgMode
		if s.ServingStatus == clustermetadatapb.PoolerServingStatus_DRAINING {
			s.ServingStatus = clustermetadatapb.PoolerServingStatus_SERVING
		}
	})
}

// Recalc re-derives the effective state from the live consensus snapshot and the
// last-known postgres recovery mode, fanning out only if something components
// react to changed (deduped like any other Mutate). It exists for consensus-only
// transitions — a revocation, a superseding committed rule — that flip the
// routing role with no recovery or serving event to carry them, so components
// (the query gate, the health stream advertising writable) converge immediately
// instead of waiting for the monitor's next drift tick.
//
// It reuses the stored pgMode rather than taking a fresh one: a pure
// consensus change does not move postgres recovery mode, and any transition that
// does move it already flows through Mutate. Like Mutate it requires the action
// lock; today it is only called from consensus RPC handlers that already hold it.
func (ssm *StateManager) Recalc(ctx context.Context) error {
	return ssm.Mutate(ctx, func(*servingStateMutation) {})
}
