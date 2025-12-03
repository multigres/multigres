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

// Package pooler implements the multipooler gRPC server
package poolerserver

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/queryservice"
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
// New subcomponents added to QueryPoolerServer should follow one of these
// initialization patterns: New -> InitDBConfig -> Init -> Open
// or: New -> InitDBConfig -> Open.
//
// Some subcomponents define Init methods. These usually perform one-time
// initialization and must be idempotent.
//
// Open and Close may be called repeatedly during the lifetime of a
// subcomponent and must also be idempotent.
type QueryPoolerServer struct {
	logger   *slog.Logger
	dbConfig *DBConfig
	executor *executor.Executor

	mu            sync.Mutex
	servingStatus clustermetadatapb.PoolerServingStatus
}

// NewMultiPooler creates a new multipooler instance.
func NewMultiPooler(logger *slog.Logger) *QueryPoolerServer {
	return &QueryPoolerServer{
		logger:        logger,
		servingStatus: clustermetadatapb.PoolerServingStatus_NOT_SERVING,
	}
}

// InitDBConfig initializes the controller with database configuration.
// This is called by MultiPoolerManager after it has the DB config available.
// Implements PoolerController interface. InitDBConfig is a continuation of New.
// However the db config is not initially available. For this reason, the initialization
// is done in two phases.
func (s *QueryPoolerServer) InitDBConfig(dbConfig *DBConfig) error {
	if dbConfig == nil {
		return fmt.Errorf("database config cannot be nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.dbConfig = dbConfig

	// Create the executor (but don't open yet - manager will call Open)
	execConfig := &executor.DBConfig{
		SocketFilePath: dbConfig.SocketFilePath,
		PoolerDir:      dbConfig.PoolerDir,
		Database:       dbConfig.Database,
		PgPort:         dbConfig.PgPort,
	}
	s.executor = executor.NewExecutor(s.logger, execConfig)

	s.logger.Info("MultiPooler initialized with database config",
		"socket", dbConfig.SocketFilePath,
		"database", dbConfig.Database)

	return nil
}

// Open opens the database connection via the executor.
// Following Vitess pattern: manager calls Open explicitly.
func (s *QueryPoolerServer) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.executor == nil {
		return fmt.Errorf("executor not initialized - call InitDBConfig first")
	}

	return s.executor.Open()
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
	exec := s.executor
	s.mu.Unlock()

	if exec == nil {
		return fmt.Errorf("executor not initialized")
	}

	return exec.IsHealthy()
}

// RegisterGRPCServices registers gRPC services (called by manager during startup).
// Implements PoolerController interface.
func (s *QueryPoolerServer) RegisterGRPCServices() {
	s.registerGRPCServices()
}

// StartServiceForTests is a convenience method for tests to initialize and start the pooler.
// Following Vitess pattern: "StartService is only used for testing."
// It calls InitDBConfig, Open, and SetServingType(SERVING).
//
// For production use, the manager handles initialization via:
//   - NewMultiPooler()
//   - InitDBConfig()
//   - Open()
//   - Register()
//   - SetServingType()
func (s *QueryPoolerServer) StartServiceForTests(dbConfig *DBConfig) error {
	if err := s.InitDBConfig(dbConfig); err != nil {
		return err
	}
	if err := s.Open(); err != nil {
		return err
	}
	// Set to SERVING state (tests assume the pooler is ready to serve)
	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()
	return s.SetServingType(ctx, clustermetadatapb.PoolerServingStatus_SERVING)
}

// Close closes the executor and releases resources.
// Implements PoolerController interface.
func (s *QueryPoolerServer) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Executor was never initialized, nothing to do
	if s.executor != nil {
		exec := s.executor
		ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
		defer cancel()
		return exec.Close(ctx)
	}
	return nil
}

// Executor returns the executor instance for use by gRPC service handlers.
// Implements PoolerController interface.
// Returns error if the pooler is not opened or unhealthy.
func (s *QueryPoolerServer) Executor() (queryservice.QueryService, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.executor == nil {
		return nil, fmt.Errorf("executor not initialized - call InitDBConfig first")
	}

	// Check if the executor is healthy (db connection is opened)
	if err := s.executor.IsHealthy(); err != nil {
		return nil, fmt.Errorf("executor not ready: %w", err)
	}

	return s.executor, nil
}
