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

// Package server implements the multipooler gRPC server
package server

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"

	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	querypb "github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/servenv"

	"google.golang.org/grpc"
)

// RegisterService registers the MultiPooler gRPC service with the given configuration
func RegisterService(config *Config) {
	servenv.OnRun(func() {
		if servenv.GRPCServer == nil {
			return
		}

		logger := servenv.GetLogger()

		logger.Info("CLAUDE DEBUG: CustomRegisterService function called - checking service map for poolerquery and poolermanager")

		// Check if the pooler service should be registered
		poolerServiceEnabled := servenv.GRPCCheckServiceMap("poolerquery")
		logger.Info("Service map check result", "service", "poolerquery", "enabled", poolerServiceEnabled)
		if poolerServiceEnabled {
			server := NewMultiPoolerServer(logger, config)
			multipoolerpb.RegisterMultiPoolerServiceServer(servenv.GRPCServer, server)
			logger.Info("MultiPooler gRPC service registered")
		}

		// Check if the manager service should be registered
		managerServiceEnabled := servenv.GRPCCheckServiceMap("poolermanager")
		logger.Info("Service map check result", "service", "poolermanager", "enabled", managerServiceEnabled)
		if managerServiceEnabled {
			managerServer := NewMultiPoolerManagerServer(logger, config)
			multipoolermanagerpb.RegisterMultiPoolerManagerServer(servenv.GRPCServer, managerServer)
			logger.Info("MultiPoolerManager gRPC service registered")
		}
	})
}

// Config holds configuration for the MultiPooler server
type Config struct {
	SocketFilePath string
	PoolerDir      string
	PgPort         int
	Database       string
}

// MultiPoolerServer implements the MultiPoolerService gRPC interface
type MultiPoolerServer struct {
	multipoolerpb.UnimplementedMultiPoolerServiceServer
	logger *slog.Logger
	config *Config
	db     *sql.DB
}

// NewMultiPoolerServer creates a new multipooler gRPC server
func NewMultiPoolerServer(logger *slog.Logger, config *Config) *MultiPoolerServer {
	return &MultiPoolerServer{
		logger: logger,
		config: config,
	}
}

// RegisterWithGRPCServer registers the MultiPooler service with the provided gRPC server
func (s *MultiPoolerServer) RegisterWithGRPCServer(grpcServer *grpc.Server) {
	multipoolerpb.RegisterMultiPoolerServiceServer(grpcServer, s)
	s.logger.Info("MultiPooler service registered with gRPC server")
}

// createDBConnection establishes a new connection to PostgreSQL using the config
// This is a shared helper function used by both MultiPoolerServer and MultiPoolerManagerServer
func createDBConnection(logger *slog.Logger, config *Config) (*sql.DB, error) {
	// Debug: Log the configuration we received
	logger.Info("createDBConnection: Configuration received",
		"pooler_dir", config.PoolerDir,
		"pg_port", config.PgPort,
		"socket_file_path", config.SocketFilePath,
		"database", config.Database)

	var dsn string
	if config.PoolerDir != "" && config.PgPort != 0 {
		// Use pooler directory and port to construct socket path
		// PostgreSQL creates socket files as: {poolerDir}/pg_sockets/.s.PGSQL.{port}
		socketDir := filepath.Join(config.PoolerDir, "pg_sockets")
		port := fmt.Sprintf("%d", config.PgPort)

		dsn = fmt.Sprintf("user=postgres dbname=%s host=%s port=%s sslmode=disable",
			config.Database, socketDir, port)

		logger.Info("Unix socket connection via pooler directory",
			"pooler_dir", config.PoolerDir,
			"socket_dir", socketDir,
			"pg_port", config.PgPort,
			"dsn", dsn)
	} else if config.SocketFilePath != "" {
		// Fallback: use socket file path directly
		socketDir := filepath.Dir(config.SocketFilePath)
		socketFile := filepath.Base(config.SocketFilePath)

		// Extract port from socket filename (.s.PGSQL.PORT)
		port := "5432" // default
		if strings.HasPrefix(socketFile, ".s.PGSQL.") {
			if portStr := strings.TrimPrefix(socketFile, ".s.PGSQL."); portStr != "" {
				port = portStr
			}
		}

		dsn = fmt.Sprintf("user=postgres dbname=%s host=%s port=%s sslmode=disable",
			config.Database, socketDir, port)

		logger.Info("Unix socket connection via socket file path (fallback)",
			"original_socket_path", config.SocketFilePath,
			"socket_dir", socketDir,
			"socket_file", socketFile,
			"extracted_port", port,
			"dsn", dsn)
	} else {
		// Use TCP connection (fallback)
		dsn = fmt.Sprintf("user=postgres dbname=%s host=localhost port=5432 sslmode=disable",
			config.Database)
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	logger.Info("Connected to PostgreSQL", "socket_path", config.SocketFilePath, "database", config.Database)
	return db, nil
}

// connectDB establishes a connection to PostgreSQL
func (s *MultiPoolerServer) connectDB() error {
	if s.db != nil {
		return nil // Already connected
	}

	db, err := createDBConnection(s.logger, s.config)
	if err != nil {
		return err
	}
	s.db = db
	return nil
}

// Close closes the database connection
func (s *MultiPoolerServer) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// ExecuteQuery implements the ExecuteQuery RPC method
func (s *MultiPoolerServer) ExecuteQuery(ctx context.Context, req *multipoolerpb.ExecuteQueryRequest) (*multipoolerpb.ExecuteQueryResponse, error) {
	// Log the incoming request
	s.logger.Info("ExecuteQuery called",
		"query_length", len(req.Query),
		"max_rows", req.MaxRows,
		"caller_principal", req.CallerId.GetPrincipal(),
		"caller_component", req.CallerId.GetComponent(),
	)

	// Ensure database connection
	if err := s.connectDB(); err != nil {
		s.logger.Error("Failed to connect to database", "error", err)
		return nil, fmt.Errorf("database connection failed: %w", err)
	}

	// Convert query bytes to string
	queryString := string(req.Query)

	// Log the actual query (be careful with sensitive data in production)
	s.logger.Debug("Executing query", "query", queryString)

	// Execute the query
	result, err := s.executeQuery(ctx, queryString, req.MaxRows)
	if err != nil {
		s.logger.Error("Query execution failed", "error", err, "query", queryString)
		return nil, fmt.Errorf("query execution failed: %w", err)
	}

	return &multipoolerpb.ExecuteQueryResponse{
		Result: result,
	}, nil
}

// executeQuery executes a SQL query and returns the result
func (s *MultiPoolerServer) executeQuery(ctx context.Context, query string, maxRows uint64) (*querypb.QueryResult, error) {
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
func (s *MultiPoolerServer) executeSelectQuery(ctx context.Context, query string, maxRows uint64) (*querypb.QueryResult, error) {
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
func (s *MultiPoolerServer) executeModifyQuery(ctx context.Context, query string) (*querypb.QueryResult, error) {
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
