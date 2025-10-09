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

	"github.com/multigres/multigres/go/multipooler/manager"
	querypb "github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/servenv"
)

// MultiPooler is the core pooler implementation
type MultiPooler struct {
	logger *slog.Logger
	config *manager.Config
	db     *sql.DB
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
	// Note: Manager also creates this, but poolerserver might connect first
	s.logger.Info("Creating sidecar database")
	if err := manager.CreateSidecarSchema(s.db); err != nil {
		return fmt.Errorf("failed to create sidecar schema: %w", err)
	}

	return nil
}

// Close closes the database connection
func (s *MultiPooler) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// Start initializes the MultiPooler
func (s *MultiPooler) Start() {
	servenv.OnRun(func() {
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
