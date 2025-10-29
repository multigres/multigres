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
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/multigres/multigres/go/multipooler/executor"
	"github.com/multigres/multigres/go/multipooler/manager"
	"github.com/multigres/multigres/go/multipooler/queryservice"
	"github.com/multigres/multigres/go/servenv"
)

// MultiPooler is the core pooler implementation
type MultiPooler struct {
	logger   *slog.Logger
	config   *manager.Config
	db       *sql.DB
	executor queryservice.QueryService
}

// NewMultiPooler creates a new multipooler instance
func NewMultiPooler(logger *slog.Logger, config *manager.Config) *MultiPooler {
	mp := &MultiPooler{
		logger: logger,
		config: config,
	}
	_ = mp.connectDB()
	mp.executor = executor.NewExecutor(logger, mp.db)
	return mp
}

// connectDB establishes a connection to PostgreSQL
func (s *MultiPooler) connectDB() error {
	if s.db != nil {
		return nil // Already connected
	}

	db, err := manager.CreateDBConnection(s.logger, s.config)
	if err != nil {
		return err
	}
	s.db = db

	// Test the connection
	if err := s.db.Ping(); err != nil {
		s.db.Close()
		s.db = nil
		return fmt.Errorf("failed to ping database: %w", err)
	}

	s.logger.Info("Connected to PostgreSQL", "socket_path", s.config.SocketFilePath, "database", s.config.Database)

	// Create the multigres sidecar schema if it doesn't exist, but only on primary databases
	// Note: Manager also creates this, but poolerserver might connect first
	ctx := context.Background()
	isPrimary, err := s.isPrimary(ctx)
	if err != nil {
		s.logger.Error("Failed to check if database is primary", "error", err)
		// Don't fail the connection if primary check fails
	} else if isPrimary {
		s.logger.Info("Creating sidecar schema on primary database")
		if err := manager.CreateSidecarSchema(s.db); err != nil {
			return fmt.Errorf("failed to create sidecar schema: %w", err)
		}
	} else {
		s.logger.Info("Skipping sidecar schema creation on replica")
	}

	return nil
}

// isPrimary checks if the connected database is a primary (not in recovery)
func (s *MultiPooler) isPrimary(ctx context.Context) (bool, error) {
	if s.db == nil {
		return false, fmt.Errorf("database connection not established")
	}

	var inRecovery bool
	err := s.db.QueryRowContext(ctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery)
	if err != nil {
		return false, fmt.Errorf("failed to query pg_is_in_recovery: %w", err)
	}

	// pg_is_in_recovery() returns true if standby, false if primary
	return !inRecovery, nil
}

// Close closes the database connection
func (s *MultiPooler) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// Start initializes the MultiPooler
func (s *MultiPooler) Start(senv *servenv.ServEnv) {
	senv.OnRun(func() {
		s.logger.Info("MultiPooler started")

		// Register all gRPC services that have registered themselves
		s.registerGRPCServices()
		s.logger.Info("MultiPooler gRPC services registered")
	})
}

// GetExecutor returns the executor instance for use by gRPC service handlers.
func (s *MultiPooler) GetExecutor() queryservice.QueryService {
	return s.executor
}
