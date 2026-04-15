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
// The current state lives in the multipooler record (multipooler.Type and
// multipooler.ServingStatus), which is the source of truth for topology.
//
// On SetState, the manager fans out OnStateChange to all registered components
// in parallel, waits for completion, then updates the multipooler record.
type StateManager struct {
	mu     sync.Mutex
	logger *slog.Logger

	// Current state lives in multipooler.Type / .ServingStatus.
	// This pointer is shared with MultiPoolerManager; the StateManager is the
	// exclusive writer of the Type and ServingStatus fields.
	multipooler *clustermetadatapb.MultiPooler

	// Registered components that react to state changes.
	components []StateAware
}

// NewStateManager creates a new StateManager.
func NewStateManager(
	logger *slog.Logger,
	multipooler *clustermetadatapb.MultiPooler,
	components ...StateAware,
) *StateManager {
	return &StateManager{
		logger:      logger,
		multipooler: multipooler,
		components:  components,
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

// Unregister removes a component so it will no longer receive OnStateChange
// calls. This is used during reopenConnections to remove stale components
// (e.g., old pubsub listeners, repl trackers, schema trackers) before
// registering their replacements.
// Must not be called concurrently with SetState.
func (ssm *StateManager) Unregister(component StateAware) {
	ssm.mu.Lock()
	defer ssm.mu.Unlock()
	for i, c := range ssm.components {
		if c == component {
			ssm.components = append(ssm.components[:i], ssm.components[i+1:]...)
			return
		}
	}
}

// RegisterAndSync adds a component and immediately syncs it to the current state.
// This is used for components created after the initial state transition (e.g.,
// ReplTracker is created after Open() has already transitioned to SERVING).
// The component is initialized from the multipooler record, which is the source of truth.
func (ssm *StateManager) RegisterAndSync(ctx context.Context, component StateAware) error {
	ssm.mu.Lock()
	defer ssm.mu.Unlock()
	ssm.components = append(ssm.components, component)
	return component.OnStateChange(ctx, ssm.multipooler.Type, ssm.multipooler.ServingStatus)
}

// SetState transitions all components to the given state in parallel.
// The multipooler record is updated only after all components converge.
// Returns an error if any component fails to transition.
// No-op if the state hasn't changed.
func (ssm *StateManager) SetState(ctx context.Context, poolerType clustermetadatapb.PoolerType, servingStatus clustermetadatapb.PoolerServingStatus) error {
	ssm.mu.Lock()
	defer ssm.mu.Unlock()

	if ssm.multipooler.Type == poolerType && ssm.multipooler.ServingStatus == servingStatus {
		ssm.logger.InfoContext(ctx, "Serving state unchanged, skipping",
			"type", poolerType, "status", servingStatus)
		return nil
	}

	ssm.logger.InfoContext(ctx, "Setting serving state",
		"target_type", poolerType, "target_status", servingStatus,
		"current_type", ssm.multipooler.Type, "current_status", ssm.multipooler.ServingStatus)

	g, ctx := errgroup.WithContext(ctx)
	for _, c := range ssm.components {
		g.Go(func() error {
			return c.OnStateChange(ctx, poolerType, servingStatus)
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	// All components converged — update the multipooler record (current state).
	ssm.multipooler.Type = poolerType
	ssm.multipooler.ServingStatus = servingStatus

	ssm.logger.InfoContext(ctx, "Serving state converged",
		"type", poolerType, "status", servingStatus)

	return nil
}
