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
	"fmt"
	"log/slog"
	"sync"

	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// StateAware is implemented by components that need to react to serving state changes.
// Each component decides internally what action to take based on the target state.
// For example, a heartbeat component starts writing when (PRIMARY, SERVING) and
// stops otherwise, while a query service component adjusts which queries it accepts.
type StateAware interface {
	// OnStateChange is called when the serving state changes.
	// The component should transition to match the target state.
	OnStateChange(ctx context.Context, poolerType clustermetadatapb.PoolerType, servingStatus clustermetadatapb.PoolerServingStatus) error
}

// StateManager coordinates serving state transitions across components.
//
// The current state lives in the poolerRecord (Type, ServingStatus, and
// CurrentLeadership), which is the source of truth for topology.
//
// On SetState, the manager derives PoolerType and CurrentLeadership from
// the caller-supplied ShardRule, fans out OnStateChange to all registered
// components in parallel, waits for completion, then updates the record
// via Mutate. The Mutate also schedules an asynchronous publish so callers
// cannot forget to reflect the new state to etcd.
//
// Type derivation:
//   - rule names this pooler as leader → PRIMARY (CurrentLeadership set)
//   - rule exists but names someone else as leader → REPLICA. (Cohort
//     membership is not consulted: a pooler replicating from the leader
//     but not in the cohort is an observer; observers and cohort replicas
//     both publish as REPLICA.)
//   - rule == nil → UNKNOWN
//
// Callers choose the source of the rule explicitly: read from rule_store
// (durable, lags RPCs by postgres apply time), from consensusState (in
// memory, updated immediately by RPCs), or from a freshly-received RPC
// payload. Each call site knows which source reflects ground truth at
// that moment — there is no single blessed source.
type StateManager struct {
	mu     sync.Mutex
	logger *slog.Logger

	// Current state lives in record.Type() / .ServingStatus(). The StateManager
	// is the exclusive caller of record.Mutate for Type/ServingStatus.
	record *poolerRecord

	// Registered components that react to state changes.
	components []StateAware
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
// Must not be called concurrently with SetState.
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
	return component.OnStateChange(ctx, ssm.record.Type(), ssm.record.ServingStatus())
}

// SetState transitions all components to the target state and updates the
// record. PoolerType and CurrentLeadership are derived from rule (see type
// docs for the derivation rules); only the serving status is independently
// caller-controlled.
//
// rule may be nil when the pooler has not yet substantiated any role —
// that yields Type=UNKNOWN, which callers MUST pair with NOT_SERVING.
// Passing (nil, SERVING) returns an error rather than silently downgrading:
// a pooler with no rule substantiated cannot honestly claim to serve, and
// the caller is in a better position than SetState to log the reason.
//
// Returns an error if any component fails to transition. No-op if the
// derived Type, CurrentLeadership, and the requested ServingStatus all
// match the current record.
func (ssm *StateManager) SetState(ctx context.Context, rule *clustermetadatapb.ShardRule, servingStatus clustermetadatapb.PoolerServingStatus) error {
	ssm.mu.Lock()
	defer ssm.mu.Unlock()

	newType, newObs := deriveTypeAndObs(rule, ssm.record.Id())
	if newType == clustermetadatapb.PoolerType_UNKNOWN && servingStatus != clustermetadatapb.PoolerServingStatus_NOT_SERVING {
		return fmt.Errorf("SetState: cannot request servingStatus=%s with no rule observed (Type=UNKNOWN must be paired with NOT_SERVING)", servingStatus)
	}

	currentType := ssm.record.Type()
	currentStatus := ssm.record.ServingStatus()
	currentObs := ssm.record.CurrentLeadership()

	if currentType == newType && currentStatus == servingStatus && proto.Equal(currentObs, newObs) {
		ssm.logger.InfoContext(ctx, "Serving state unchanged, skipping",
			"type", newType, "status", servingStatus)
		return nil
	}

	ssm.logger.InfoContext(ctx, "Setting serving state",
		"target_type", newType, "target_status", servingStatus,
		"current_type", currentType, "current_status", currentStatus)

	g, ctx := errgroup.WithContext(ctx)
	for _, c := range ssm.components {
		g.Go(func() error {
			return c.OnStateChange(ctx, newType, servingStatus)
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	// All components converged — update the record and schedule a publish.
	// Requires the caller to hold the action lock; Mutate will return an
	// error otherwise. Mutate also validates the Type ↔ CurrentLeadership
	// invariant.
	if err := ssm.record.Mutate(ctx, func(s *MutablePoolerRecordState) {
		s.Type = newType
		s.ServingStatus = servingStatus
		s.CurrentLeadership = newObs
	}); err != nil {
		return err
	}

	ssm.logger.InfoContext(ctx, "Serving state converged",
		"type", newType, "status", servingStatus)

	return nil
}

// deriveTypeAndObs returns the PoolerType and LeaderObservation implied
// by rule for the pooler identified by selfID. See StateManager docs for
// the derivation rules.
func deriveTypeAndObs(rule *clustermetadatapb.ShardRule, selfID *clustermetadatapb.ID) (clustermetadatapb.PoolerType, *clustermetadatapb.LeaderObservation) {
	if rule == nil {
		return clustermetadatapb.PoolerType_UNKNOWN, nil
	}
	if proto.Equal(rule.GetLeaderId(), selfID) {
		return clustermetadatapb.PoolerType_PRIMARY, &clustermetadatapb.LeaderObservation{
			LeaderId:         selfID,
			LeaderRuleNumber: rule.GetRuleNumber(),
		}
	}
	return clustermetadatapb.PoolerType_REPLICA, nil
}
