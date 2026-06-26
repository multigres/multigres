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

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

// StateAware is implemented by components that need to react to serving state changes.
// Each component decides internally what action to take based on the target state.
// For example, a heartbeat component starts writing when it is writable and
// serving, while a query service component adjusts which queries it accepts.
type StateAware interface {
	// OnStateChange is called when the effective serving state changes; the
	// component transitions to match it. See servingstate.State for the meaning
	// of each field (notably IsHighestKnownLeader for routing vs Writable for
	// write-safety).
	OnStateChange(ctx context.Context, state servingstate.State) error
}

// StateManager coordinates serving state transitions across components.
//
// The current state lives in the poolerRecord (Type and ServingStatus),
// which is the source of truth for topology.
//
// Writability is computed, not stored: the StateManager caches only inRecovery
// (the one input it can't derive — an async postgres observation it is told of)
// and recomputes writable = !inRecovery && committedLeader() live. So a
// revocation or a new committed-rule observation flips writability with no
// stored copy to update; callers poke the facts (recovery, leadership, serving),
// never writable directly.
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

	// inRecovery is whether postgres is in recovery (a standby — not a writable
	// backend), fed by the postgres monitor via Mutate's InRecovery field. It is
	// the one write-safety input the StateManager cannot compute itself (an async
	// postgres observation). Local-only, never published to topology. Guarded by mu.
	inRecovery bool

	// committedLeader reports whether our highest non-revoked committed rule names
	// us — the consensus half of write-safety. Injected (a downward capability,
	// queried live rather than stored) so the StateManager recomputes writability
	// from the rule store + revocation on demand: a revocation or new committed-rule
	// observation flips writable without any explicit set. See writableLocked.
	committedLeader func() bool

	// lastFannedOut is the effective state most recently delivered to components,
	// or nil if none has been yet. Used to skip redundant fan-outs: components
	// react to the (IsHighestKnownLeader, Writable, ServingStatus) tuple, and
	// distinct PoolerTypes (REPLICA vs UNKNOWN) collapse to the same tuple, so a
	// record-only change that leaves the tuple identical must not re-notify.
	lastFannedOut *servingstate.State

	// Registered components that react to state changes.
	components []StateAware
}

// NewStateManager creates a new StateManager. committedLeader is the live
// committed-leadership query (whether our highest non-revoked committed rule
// names us); the StateManager combines it with its cached recovery state to
// compute writability.
func NewStateManager(
	logger *slog.Logger,
	record *poolerRecord,
	committedLeader func() bool,
	components ...StateAware,
) *StateManager {
	return &StateManager{
		logger:          logger,
		record:          record,
		committedLeader: committedLeader,
		components:      components,
	}
}

// writableLocked computes the live write-safety signal: postgres out of recovery
// AND we are the highest non-revoked committed leader. Caller must hold mu.
func (ssm *StateManager) writableLocked() bool {
	return !ssm.inRecovery && ssm.committedLeader()
}

// effectiveStateLocked builds the servingstate.State components react to from the
// given mutation result. IsHighestKnownLeader is the self-leadership observation
// being non-nil (highest-known leadership, the PoolerType source); Writable is
// recomputed live from recovery + committed leadership. Caller must hold mu.
func (ssm *StateManager) effectiveStateLocked(m servingStateMutation) servingstate.State {
	return servingstate.State{
		IsHighestKnownLeader: m.SelfLeadership != nil,
		Writable:             !m.InRecovery && ssm.committedLeader(),
		ServingStatus:        m.ServingStatus,
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
		IsHighestKnownLeader: ssm.record.SelfLeadership() != nil,
		Writable:             ssm.writableLocked(),
		ServingStatus:        ssm.record.ServingStatus(),
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
// carried forward. SelfLeadership and ServingStatus are topology-backed (persisted
// to the poolerRecord); InRecovery is the local-only recovery observation. There
// is deliberately no Writable field — writability is derived from InRecovery and
// the committed-leadership query, never set directly.
//
// There is also no PoolerType field: the consensus-leadership observation is the
// source of truth, and PoolerType (PRIMARY iff a self-leadership obs is held,
// REPLICA otherwise) is derived from it when persisting the record.
type servingStateMutation struct {
	SelfLeadership *clustermetadatapb.LeaderObservation
	InRecovery     bool
	ServingStatus  clustermetadatapb.PoolerServingStatus
}

// Mutate applies fn to the current state, fans the resulting effective state out
// to all components, and persists any topology-backed change to the poolerRecord.
// fn may change any subset of fields and read the previous values; fields it
// leaves alone are preserved.
//
// The action lock is always required: record.Mutate asserts it for topology
// writes, and routing recovery changes through the same gate keeps a recovery
// flip from racing a role transition. It is asserted up front, before fanning
// out, so a caller without the lock cannot half-transition the components and
// only fail later at record.Mutate.
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
		SelfLeadership: ssm.record.SelfLeadership(),
		InRecovery:     ssm.inRecovery,
		ServingStatus:  ssm.record.ServingStatus(),
	}
	next := cur
	fn(&next)

	// Leadership drives the derived PoolerType, so the record changes when
	// leader-ness flips or serving changes. An obs-only change (same leader-ness,
	// new rule number) is published via UpdateLeaderObservation, not here.
	leaderChanged := (next.SelfLeadership != nil) != (cur.SelfLeadership != nil)
	servingChanged := next.ServingStatus != cur.ServingStatus
	recoveryChanged := next.InRecovery != cur.InRecovery

	// Writability is recomputed live, so the effective state can differ from what
	// was last fanned out even when fn changed nothing here (e.g. a revocation
	// flipped committed leadership). Compare the freshly computed effective state,
	// not just the mutated fields, so such a change still re-notifies.
	nextEffective := ssm.effectiveStateLocked(next)
	effectiveChanged := ssm.lastFannedOut == nil || *ssm.lastFannedOut != nextEffective
	if !leaderChanged && !servingChanged && !recoveryChanged && !effectiveChanged {
		return nil
	}

	ssm.logger.InfoContext(ctx, "Applying serving state",
		"leader", nextEffective.IsHighestKnownLeader, "status", nextEffective.ServingStatus,
		"writable", nextEffective.Writable, "in_recovery", next.InRecovery,
		"prev_leader", cur.SelfLeadership != nil, "prev_status", cur.ServingStatus, "prev_in_recovery", cur.InRecovery)

	// Fan out first so a failed transition leaves the record untouched. The
	// fan-out dedups on the effective tuple, so a recovery-only or record-only
	// change that leaves the tuple identical does not re-notify.
	if err := ssm.fanOutLocked(ctx, nextEffective); err != nil {
		return err
	}
	// Components converged — keep the local recovery state in step with what they
	// were computed from before the (fallible) record write.
	ssm.inRecovery = next.InRecovery

	if leaderChanged || servingChanged {
		if err := ssm.record.Mutate(ctx, func(s *MutablePoolerRecordState) {
			s.Type = poolerTypeForLeader(next.SelfLeadership != nil)
			s.SelfLeadership = next.SelfLeadership
			s.ServingStatus = next.ServingStatus
		}); err != nil {
			return err
		}
	}
	return nil
}

// lastAppliedWritable returns the write-safety value most recently delivered to
// components (what they currently believe), or false if nothing has been fanned
// out yet. The monitor compares its freshly recomputed writable against this to
// decide whether components are stale and a reconcile is needed.
//
// This deliberately returns the last-delivered value, NOT a live recompute: if it
// recomputed live it would already reflect a revocation or recovery change, agree
// with the monitor's fresh value, and the monitor would detect no drift — leaving
// components stuck at the old writability. Returning what was last delivered lets
// the monitor see the divergence and re-notify.
func (ssm *StateManager) lastAppliedWritable() bool {
	ssm.mu.Lock()
	defer ssm.mu.Unlock()
	if ssm.lastFannedOut == nil {
		return false
	}
	return ssm.lastFannedOut.Writable
}
