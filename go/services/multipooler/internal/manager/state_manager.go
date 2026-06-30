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
)

// StateAware is implemented by components that need to react to serving state changes.
// Each component decides internally what action to take based on the target state.
// For example, a heartbeat component starts writing when it is the writable leader
// and serving, while a query service component adjusts which queries it accepts.
type StateAware interface {
	// OnStateChange is called when the effective serving state changes; the
	// component transitions to match it.
	//
	// isConsensusLeader reports whether this pooler is the consensus-elected
	// leader (the shard's write target). postgresPrimary reports the physical
	// recovery state (!pg_is_in_recovery): being the leader does NOT imply
	// postgres is out of recovery, so components that need a writable backend
	// (heartbeat writer, LISTEN/NOTIFY) must require both. servingStatus is the
	// serving intent.
	OnStateChange(ctx context.Context, isConsensusLeader, postgresPrimary bool, servingStatus clustermetadatapb.PoolerServingStatus) error
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

	// lastFannedOut is the effective state most recently delivered to components,
	// or nil if none has been yet. Used to skip redundant fan-outs: components
	// react to the (leader, postgresPrimary, servingStatus) tuple, and distinct
	// PoolerTypes (REPLICA vs UNKNOWN) collapse to the same tuple, so a record-only
	// change that leaves the tuple identical must not re-notify components.
	lastFannedOut *effectiveState

	// Registered components that react to state changes.
	components []StateAware
}

// effectiveState is what registered components react to, distinct from the
// topology record's PoolerType/ServingStatus: leader is the consensus-leadership
// fact, postgresPrimary the physical recovery state.
type effectiveState struct {
	leader          bool
	postgresPrimary bool
	servingStatus   clustermetadatapb.PoolerServingStatus
}

// NewStateManager creates a new StateManager.
func NewStateManager(
	logger *slog.Logger,
	record *poolerRecord,
	components ...StateAware,
) *StateManager {
	return &StateManager{
		logger:     logger,
		record:     record,
		components: components,
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
	return component.OnStateChange(ctx, ssm.record.SelfLeadership() != nil, ssm.postgresPrimary, ssm.record.ServingStatus())
}

// fanOutLocked notifies every registered component of the target effective state
// in parallel and waits for them to converge. Caller must hold mu.
//
// Leadership — whether this pooler is the consensus-elected write target — is the
// self-leadership observation being non-nil, the consensus fact from which the
// PoolerType routing label is derived. Components react to that fact, not the label.
func (ssm *StateManager) fanOutLocked(ctx context.Context, target effectiveState) error {
	if ssm.lastFannedOut != nil && *ssm.lastFannedOut == target {
		// Components are already at this effective state; nothing to notify.
		return nil
	}
	g, ctx := errgroup.WithContext(ctx)
	for _, c := range ssm.components {
		g.Go(func() error {
			return c.OnStateChange(ctx, target.leader, target.postgresPrimary, target.servingStatus)
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
// to the poolerRecord); PostgresPrimary is local-only physical state.
//
// There is deliberately no PoolerType field: the consensus-leadership observation
// is the source of truth, and PoolerType (PRIMARY iff a self-leadership obs is
// held, REPLICA otherwise) is derived from it when persisting the record.
type servingStateMutation struct {
	SelfLeadership  *clustermetadatapb.LeaderObservation
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
		SelfLeadership:  ssm.record.SelfLeadership(),
		PostgresPrimary: ssm.postgresPrimary,
		ServingStatus:   ssm.record.ServingStatus(),
	}
	next := cur
	fn(&next)

	// Leadership drives the derived PoolerType, so the record changes when
	// leader-ness flips or serving changes. An obs-only change (same leader-ness,
	// new rule number) is published via UpdateLeaderObservation, not here.
	leaderChanged := (next.SelfLeadership != nil) != (cur.SelfLeadership != nil)
	servingChanged := next.ServingStatus != cur.ServingStatus
	primaryChanged := next.PostgresPrimary != cur.PostgresPrimary
	if !leaderChanged && !servingChanged && !primaryChanged {
		return nil
	}

	ssm.logger.InfoContext(ctx, "Applying serving state",
		"leader", next.SelfLeadership != nil, "status", next.ServingStatus, "postgres_primary", next.PostgresPrimary,
		"prev_leader", cur.SelfLeadership != nil, "prev_status", cur.ServingStatus, "prev_postgres_primary", cur.PostgresPrimary)

	// Fan out first so a failed transition leaves the record untouched. The
	// fan-out dedups on the effective tuple, so a primary-only or record-only
	// change that leaves the tuple identical does not re-notify.
	if err := ssm.fanOutLocked(ctx, effectiveState{
		leader:          next.SelfLeadership != nil,
		postgresPrimary: next.PostgresPrimary,
		servingStatus:   next.ServingStatus,
	}); err != nil {
		return err
	}
	// Components converged — keep the local physical state in step with what they
	// received before the (fallible) record write.
	ssm.postgresPrimary = next.PostgresPrimary

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

// isPostgresPrimary returns the physical recovery state last applied to
// components. The monitor compares its fresh observation against this (lock-free)
// to decide whether a sync under the action lock is needed.
func (ssm *StateManager) isPostgresPrimary() bool {
	ssm.mu.Lock()
	defer ssm.mu.Unlock()
	return ssm.postgresPrimary
}
