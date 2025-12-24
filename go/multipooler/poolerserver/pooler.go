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
	"github.com/multigres/multigres/go/multipooler/connpoolmanager"
	"github.com/multigres/multigres/go/multipooler/executor"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

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
	servingStatus  clustermetadatapb.PoolerServingStatus
	healthProvider HealthProvider
}

// NewQueryPoolerServer creates a new QueryPoolerServer instance with the given pool manager
// and health provider.
// The pool manager must already be opened before calling this function.
// The health provider is used by StreamPoolerHealth to provide health updates to clients.
func NewQueryPoolerServer(logger *slog.Logger, poolManager connpoolmanager.PoolManager, healthProvider HealthProvider) *QueryPoolerServer {
	var exec *executor.Executor
	if poolManager != nil {
		exec = executor.NewExecutor(logger, poolManager)
	}

	return &QueryPoolerServer{
		logger:         logger,
		poolManager:    poolManager,
		executor:       exec,
		servingStatus:  clustermetadatapb.PoolerServingStatus_NOT_SERVING,
		healthProvider: healthProvider,
	}
}

// SetServingType transitions the serving state.
// Implements PoolerController interface.
func (s *QueryPoolerServer) SetServingType(ctx context.Context, servingStatus clustermetadatapb.PoolerServingStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.InfoContext(ctx, "Transitioning serving type", "from", s.servingStatus, "to", servingStatus)
	s.servingStatus = servingStatus

	// TODO: Implement state-specific behavior:
	// - SERVING: Accept all queries
	// - SERVING_RDONLY: Accept only read queries
	// - NOT_SERVING: Reject all queries
	// - DRAINED: Gracefully drain existing connections, reject new queries

	return nil
}

// IsServing returns true if currently serving queries.
// Implements PoolerController interface.
func (s *QueryPoolerServer) IsServing() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.servingStatus == clustermetadatapb.PoolerServingStatus_SERVING ||
		s.servingStatus == clustermetadatapb.PoolerServingStatus_SERVING_RDONLY
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
// It sets the serving type to SERVING.
func (s *QueryPoolerServer) StartServiceForTests() error {
	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()
	return s.SetServingType(ctx, clustermetadatapb.PoolerServingStatus_SERVING)
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
	return s.executor
}

// HealthProvider returns the health provider for streaming health updates.
func (s *QueryPoolerServer) HealthProvider() HealthProvider {
	return s.healthProvider
}
