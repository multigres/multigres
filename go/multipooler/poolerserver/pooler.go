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
	"strings"
	"time"

	"github.com/multigres/multigres/go/multipooler/heartbeat"
	"github.com/multigres/multigres/go/multipooler/manager"
	querypb "github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/servenv"
)

// MultiPooler is the core pooler implementation
type MultiPooler struct {
	logger      *slog.Logger
	config      *manager.Config
	db          *sql.DB
	replTracker *heartbeat.ReplTracker
}

// NewMultiPooler creates a new multipooler instance
func NewMultiPooler(logger *slog.Logger, config *manager.Config) *MultiPooler {
	return &MultiPooler{
		logger: logger,
		config: config,
	}
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

	// Create the multigres sidecar schema if it doesn't exist
	s.logger.Info("Creating sidecar database")
	if err := s.createSidecarDB(); err != nil {
		return fmt.Errorf("failed to create sidecar schema: %w", err)
	}

	// Start heartbeat tracking if not already started
	if s.replTracker == nil {
		s.logger.Info("Starting database heartbeat")
		ctx := context.Background()
		shardID := []byte("0") // default shard ID
		// TODO: populate this with a unique ID for the pooler
		poolerID := fmt.Sprintf("pooler-%d", s.config.PgPort)
		if err := s.startHeartbeat(ctx, shardID, poolerID); err != nil {
			s.logger.Error("Failed to start heartbeat", "error", err)
			// Don't fail the connection if heartbeat fails
		}
	}

	return nil
}

// createSidecarDB creates the multigres sidecar schema if it doesn't exist
func (s *MultiPooler) createSidecarDB() error {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	primary, err := s.isPrimary(ctx)
	if err != nil {
		return fmt.Errorf("failed to check if database is primary: %w", err)
	}
	if !primary {
		return nil
	}

	_, err = s.db.Exec("CREATE SCHEMA IF NOT EXISTS multigres")
	if err != nil {
		return fmt.Errorf("failed to create multigres schema: %w", err)
	}

	// Create the heartbeat table
	_, err = s.db.Exec(`
		CREATE TABLE IF NOT EXISTS multigres.heartbeat (
			shard_id BYTEA PRIMARY KEY,
			pooler_id TEXT NOT NULL,
			ts BIGINT NOT NULL
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create heartbeat table: %w", err)
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

// startHeartbeat starts the replication tracker and heartbeat writer if connected to a primary database
func (s *MultiPooler) startHeartbeat(ctx context.Context, shardID []byte, poolerID string) error {
	// Create the replication tracker
	s.replTracker = heartbeat.NewReplTracker(s.db, s.logger, shardID, poolerID)

	// Check if we're connected to a primary
	isPrimary, err := s.isPrimary(ctx)
	if err != nil {
		return fmt.Errorf("failed to check if database is primary: %w", err)
	}

	if isPrimary {
		s.logger.Info("Starting heartbeat writer - connected to primary database")
		s.replTracker.MakePrimary()
	} else {
		s.logger.Info("Not starting heartbeat writer - connected to standby database")
		s.replTracker.MakeNonPrimary()
	}

	return nil
}

// Close closes the database connection
func (s *MultiPooler) Close() error {
	if s.replTracker != nil {
		s.replTracker.Close()
	}
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

// ExecuteQuery executes a SQL query
func (s *MultiPooler) ExecuteQuery(ctx context.Context, query []byte, maxRows uint64) (*querypb.QueryResult, error) {
	// Log the incoming request
	s.logger.Info("ExecuteQuery called",
		"query_length", len(query),
		"max_rows", maxRows,
	)

	// Ensure database connection
	if err := s.connectDB(); err != nil {
		s.logger.Error("Failed to connect to database", "error", err)
		return nil, fmt.Errorf("database connection failed: %w", err)
	}

	// Convert query bytes to string
	queryString := string(query)

	// Log the actual query (be careful with sensitive data in production)
	s.logger.Debug("Executing query", "query", queryString)

	// Execute the query
	result, err := s.executeQuery(ctx, queryString, maxRows)
	if err != nil {
		s.logger.Error("Query execution failed", "error", err, "query", queryString)
		return nil, fmt.Errorf("query execution failed: %w", err)
	}

	return result, nil
}

// executeQuery executes a SQL query and returns the result
func (s *MultiPooler) executeQuery(ctx context.Context, query string, maxRows uint64) (*querypb.QueryResult, error) {
	// Determine if this is a SELECT query or a modification query
	trimmedQuery := strings.TrimSpace(strings.ToUpper(query))
	isSelect := strings.HasPrefix(trimmedQuery, "SELECT") ||
		strings.HasPrefix(trimmedQuery, "WITH") ||
		strings.HasPrefix(trimmedQuery, "SHOW") ||
		strings.HasPrefix(trimmedQuery, "EXPLAIN")

	if isSelect {
		return s.executeSelectQuery(ctx, query, maxRows)
	} else {
		return s.executeModifyQuery(ctx, query)
	}
}

// executeSelectQuery executes a SELECT query and returns rows
func (s *MultiPooler) executeSelectQuery(ctx context.Context, query string, maxRows uint64) (*querypb.QueryResult, error) {
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Get column information
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to get column types: %w", err)
	}

	// Build field information
	fields := make([]*querypb.Field, len(columns))
	for i, col := range columns {
		fields[i] = &querypb.Field{
			Name: col,
			Type: columnTypes[i].DatabaseTypeName(),
		}
	}

	// Read rows
	var resultRows []*querypb.Row
	scanValues := make([]interface{}, len(columns))
	scanPointers := make([]interface{}, len(columns))

	for i := range scanValues {
		scanPointers[i] = &scanValues[i]
	}

	rowCount := uint64(0)
	for rows.Next() && (maxRows == 0 || rowCount < maxRows) {
		if err := rows.Scan(scanPointers...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Convert values to bytes
		values := make([][]byte, len(columns))
		for i, val := range scanValues {
			if val == nil {
				values[i] = nil
			} else {
				values[i] = []byte(fmt.Sprintf("%v", val))
			}
		}

		resultRows = append(resultRows, &querypb.Row{Values: values})
		rowCount++
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading rows: %w", err)
	}

	return &querypb.QueryResult{
		Fields:       fields,
		RowsAffected: 0, // SELECT queries don't affect rows
		Rows:         resultRows,
	}, nil
}

// executeModifyQuery executes an INSERT, UPDATE, DELETE, or other modification query
func (s *MultiPooler) executeModifyQuery(ctx context.Context, query string) (*querypb.QueryResult, error) {
	result, err := s.db.ExecContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		// Some queries don't support RowsAffected, that's okay
		rowsAffected = 0
	}

	return &querypb.QueryResult{
		Fields:       []*querypb.Field{}, // No fields for modification queries
		RowsAffected: uint64(rowsAffected),
		Rows:         []*querypb.Row{}, // No rows for modification queries
	}, nil
}
