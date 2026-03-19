// Copyright 2025 Supabase, Inc.
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

// Package poolerserver implements the multipooler gRPC server
package poolerserver

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/queryservice"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/connpoolmanager"
	"github.com/multigres/multigres/go/services/multipooler/executor"
)

// Sentinel errors for request gating.
var (
	// ErrNotServing is returned when the pooler is not in SERVING state.
	ErrNotServing = errors.New("pooler is not serving")
	// ErrShuttingDown is returned when the pooler is draining and the request
	// does not have allowOnShutdown=true.
	ErrShuttingDown = errors.New("pooler is shutting down")
)

const defaultGracePeriod = 5 * time.Second

// QueryPoolerServer is the core pooler implementation for query serving.
// It encapsulates the components required to manage query execution
// (e.g. pooling, execution, transactions).
//
// In the future, components like the transaction engine and query engine
// should be added here. The current executor is temporary until those
// engines are introduced.
//
// The lifecycle of the pooler is managed by the MultiPoolerManager.
// The connection pool manager (connpoolmanager) handles all database connections,
// including per-user connection pools with trust/peer authentication.
type QueryPoolerServer struct {
	logger      *slog.Logger
	poolManager connpoolmanager.PoolManager
	executor    *executor.Executor

	mu             sync.Mutex
	poolerType     clustermetadatapb.PoolerType
	servingStatus  clustermetadatapb.PoolerServingStatus
	shuttingDown   bool
	healthProvider HealthProvider

	requests    *RequestsWaiter
	gracePeriod time.Duration
}

// NewQueryPoolerServer creates a new QueryPoolerServer instance with the given pool manager
// and health provider.
// The pool manager must already be opened before calling this function.
// The health provider is used by StreamPoolerHealth to provide health updates to clients.
func NewQueryPoolerServer(logger *slog.Logger, poolManager connpoolmanager.PoolManager, poolerID *clustermetadatapb.ID, healthProvider HealthProvider) *QueryPoolerServer {
	var exec *executor.Executor
	if poolManager != nil {
		exec = executor.NewExecutor(logger, poolManager, poolerID)
	}

	return &QueryPoolerServer{
		logger:         logger,
		poolManager:    poolManager,
		executor:       exec,
		servingStatus:  clustermetadatapb.PoolerServingStatus_NOT_SERVING,
		healthProvider: healthProvider,
		requests:       NewRequestsWaiter(),
		// TODO: Create a flag for this
		gracePeriod: defaultGracePeriod,
	}
}

// OnStateChange transitions the query service to match the new serving state.
// Implements PoolerController interface.
//
// For SERVING transitions: straightforward state update.
// For NOT_SERVING transitions: two-phase shutdown:
//  1. Set shuttingDown=true to block new queries while allowing in-flight transactions to complete.
//  2. Wait for in-flight requests to drain (up to gracePeriod), then finalize to NOT_SERVING.
func (s *QueryPoolerServer) OnStateChange(ctx context.Context, poolerType clustermetadatapb.PoolerType, servingStatus clustermetadatapb.PoolerServingStatus) error {
	s.mu.Lock()

	s.logger.InfoContext(ctx, "Transitioning serving type",
		"pooler_type_from", s.poolerType, "pooler_type_to", poolerType,
		"status_from", s.servingStatus, "status_to", servingStatus)

	s.poolerType = poolerType

	if servingStatus == clustermetadatapb.PoolerServingStatus_SERVING {
		s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING
		s.shuttingDown = false
		s.mu.Unlock()
		return nil
	}

	// NOT_SERVING transition: begin two-phase shutdown.
	// Phase 1: set shuttingDown to block new non-shutdown requests and capture
	// the drain channel atomically. This prevents the TOCTOU race where a
	// StartRequest(allowOnShutdown=true) could slip in between setting
	// shuttingDown and reading the in-flight count, causing drain to skip.
	s.shuttingDown = true
	zeroCh := s.requests.ZeroChan()
	gracePeriod := s.gracePeriod
	s.mu.Unlock()

	// Phase 2: drain in-flight requests.
	// We wait on zeroCh captured above. Any allowOnShutdown=true requests that
	// start after we released the lock will call Add(1) which replaces zeroCh
	// inside RequestsWaiter, but we hold the old channel reference. The old
	// channel fires when the requests that were in-flight at snapshot time
	// complete. New shutdown-allowed requests (COMMIT/ROLLBACK) are operations
	// on those same transactions, so they will complete before or with them.
	select {
	case <-zeroCh:
		// Already drained (or was already at zero).
	default:
		s.logger.InfoContext(ctx, "Draining in-flight requests",
			"count", s.requests.GetCount(), "grace_period", gracePeriod)

		if gracePeriod > 0 {
			timer := time.NewTimer(gracePeriod)
			defer timer.Stop()
			select {
			case <-zeroCh:
				s.logger.InfoContext(ctx, "All in-flight requests drained")
			case <-timer.C:
				s.logger.WarnContext(ctx, "Grace period expired with in-flight requests",
					"remaining", s.requests.GetCount())
			case <-ctx.Done():
				s.logger.WarnContext(ctx, "Context cancelled during drain",
					"remaining", s.requests.GetCount())
			}
		} else {
			select {
			case <-zeroCh:
				s.logger.InfoContext(ctx, "All in-flight requests drained")
			case <-ctx.Done():
				s.logger.WarnContext(ctx, "Context cancelled during drain",
					"remaining", s.requests.GetCount())
			}
		}
	}

	// Finalize: set NOT_SERVING and clear shuttingDown.
	s.mu.Lock()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_NOT_SERVING
	s.shuttingDown = false
	s.mu.Unlock()

	return nil
}

// StartRequest gates an incoming request based on serving state.
// If allowOnShutdown is true, the request is allowed during the shutdown drain phase
// (e.g., COMMIT/ROLLBACK on existing reserved connections).
// Returns ErrNotServing if NOT_SERVING, or ErrShuttingDown if draining and !allowOnShutdown.
func (s *QueryPoolerServer) StartRequest(allowOnShutdown bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.servingStatus != clustermetadatapb.PoolerServingStatus_SERVING {
		return ErrNotServing
	}
	if s.shuttingDown && !allowOnShutdown {
		return ErrShuttingDown
	}

	s.requests.Add(1)
	return nil
}

// EndRequest signals that a request has completed.
func (s *QueryPoolerServer) EndRequest() {
	s.requests.Done()
}

// SetGracePeriod sets the maximum time to wait for in-flight requests during drain.
func (s *QueryPoolerServer) SetGracePeriod(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gracePeriod = d
}

// IsServing returns true if currently serving queries.
// Implements PoolerController interface.
func (s *QueryPoolerServer) IsServing() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.servingStatus == clustermetadatapb.PoolerServingStatus_SERVING
}

// IsHealthy checks if the controller is healthy.
// Implements PoolerController interface.
func (s *QueryPoolerServer) IsHealthy() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.executor == nil {
		return errors.New("executor not initialized")
	}

	if s.poolManager == nil {
		return errors.New("pool manager not initialized")
	}

	// The pool manager handles connection health internally.
	// If we have the executor and pool manager, we're healthy.
	return nil
}

// RegisterGRPCServices registers gRPC services (called by manager during startup).
// Implements PoolerController interface.
func (s *QueryPoolerServer) RegisterGRPCServices() {
	s.registerGRPCServices()
}

// StartServiceForTests is a convenience method for tests to initialize and start the pooler.
// Following Vitess pattern: "StartService is only used for testing."
// It sets the serving type to PRIMARY + SERVING.
func (s *QueryPoolerServer) StartServiceForTests() error {
	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()
	return s.OnStateChange(ctx, clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerServingStatus_SERVING)
}

// Executor returns the executor instance for use by gRPC service handlers.
// Implements PoolerController interface.
// Returns error if the pooler is not initialized.
func (s *QueryPoolerServer) Executor() (queryservice.QueryService, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.executor == nil {
		return nil, errors.New("executor not initialized - pool manager was nil")
	}

	return s.executor, nil
}

// PoolManager returns the pool manager instance.
// This is used by GetAuthCredentials to query pg_authid using an admin connection,
// which works even before the executor is fully initialized during bootstrap.
func (s *QueryPoolerServer) PoolManager() connpoolmanager.PoolManager {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.poolManager
}

// InternalQueryService returns the executor as an InternalQueryService for internal queries.
// Implements PoolerController interface.
func (s *QueryPoolerServer) InternalQueryService() executor.InternalQueryService {
	// Explicit nil check required: returning a nil *Executor directly would produce a
	// non-nil interface value wrapping a nil pointer, causing callers' == nil checks to
	// pass but method calls on the interface to panic.
	if s.executor == nil {
		return nil
	}
	return s.executor
}

// HealthProvider returns the health provider for streaming health updates.
func (s *QueryPoolerServer) HealthProvider() HealthProvider {
	return s.healthProvider
}
