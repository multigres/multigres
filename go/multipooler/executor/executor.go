// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package executor implements query execution for multipooler.
// It provides the QueryService interface implementation that executes queries
// against PostgreSQL and streams results back to clients.
package executor

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/multigres/multigres/go/multipooler/dbconn"
	"github.com/multigres/multigres/go/multipooler/queryservice"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/pools/connpool"

	_ "github.com/lib/pq" // PostgreSQL driver
)

// DBConfig contains database connection parameters.
type DBConfig struct {
	SocketFilePath  string
	PoolerDir       string
	Database        string
	PgPort          int
	PoolEnabled     bool          // Enable connection pooling (default: true)
	PoolCapacity    int           // Max connections (default: 100)
	PoolMaxIdle     int           // Max idle connections (default: 50)
	PoolIdleTimeout time.Duration // Idle timeout (default: 5min)
	PoolMaxLifetime time.Duration // Max connection lifetime (default: 1hr)
}

// Executor implements the QueryService interface for executing queries against PostgreSQL.
type Executor struct {
	logger   *slog.Logger
	dbConfig *DBConfig
	db       *sql.DB                        // Used for non-pooled mode or driver initialization
	pool     *connpool.Pool[*dbconn.DBConn] // Connection pool
	dsn      string                         // DSN for creating new connections
	isOpen   atomic.Bool
}

// NewExecutor creates a new Executor instance.
func NewExecutor(logger *slog.Logger, dbConfig *DBConfig) *Executor {
	return &Executor{
		logger:   logger,
		dbConfig: dbConfig,
	}
}

// Open creates the database connection pool (or sql.DB if pooling is disabled).
func (e *Executor) Open() error {
	if e.isOpen.Load() {
		return nil
	}

	e.logger.Info("Executor: opening")

	if e.dbConfig == nil {
		return fmt.Errorf("database config not set")
	}

	// Create connection string using socket connection
	// PostgreSQL creates socket files as: {poolerDir}/pg_sockets/.s.PGSQL.{port}
	socketDir := filepath.Join(e.dbConfig.PoolerDir, "pg_sockets")
	port := fmt.Sprintf("%d", e.dbConfig.PgPort)

	e.dsn = fmt.Sprintf("user=postgres dbname=%s host=%s port=%s sslmode=disable",
		e.dbConfig.Database, socketDir, port)

	e.logger.Info("Executor: Unix socket connection",
		"pooler_dir", e.dbConfig.PoolerDir,
		"socket_dir", socketDir,
		"pg_port", e.dbConfig.PgPort)

	// Check if pooling is enabled (default to true if not explicitly disabled)
	poolEnabled := !(!e.dbConfig.PoolEnabled &&
		e.dbConfig.PoolCapacity == 0 &&
		e.dbConfig.PoolMaxIdle == 0)

	if poolEnabled {
		// Initialize connection pool
		e.logger.Info("Executor: initializing connection pool",
			"capacity", e.getPoolCapacity(),
			"max_idle", e.getPoolMaxIdle(),
			"idle_timeout", e.getPoolIdleTimeout(),
			"max_lifetime", e.getPoolMaxLifetime())

		// Create a factory function that creates new connections
		factory := func(ctx context.Context) (*dbconn.DBConn, error) {
			db, err := sql.Open("postgres", e.dsn)
			if err != nil {
				return nil, fmt.Errorf("failed to open database: %w", err)
			}

			conn, err := db.Conn(ctx)
			if err != nil {
				db.Close()
				return nil, fmt.Errorf("failed to get connection: %w", err)
			}

			return dbconn.NewDBConn(conn), nil
		}

		e.pool = connpool.NewPool(factory, connpool.Config{
			Capacity:    e.getPoolCapacity(),
			MaxIdle:     e.getPoolMaxIdle(),
			IdleTimeout: e.getPoolIdleTimeout(),
			MaxLifetime: e.getPoolMaxLifetime(),
		})

		// Test the pool by getting a connection
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		testPooled, err := e.pool.Get(ctx)
		if err != nil {
			return fmt.Errorf("failed to get test connection from pool: %w", err)
		}

		// Test ping
		err = testPooled.Conn().Exec(ctx, "SELECT 1")
		if err != nil {
			_ = e.pool.Put(testPooled)
			return fmt.Errorf("failed to ping database: %w", err)
		}

		// Return connection to pool
		if err := e.pool.Put(testPooled); err != nil {
			return fmt.Errorf("failed to return test connection to pool: %w", err)
		}

		e.logger.Info("Executor: connection pool initialized successfully")
	} else {
		// Fallback to sql.DB (non-pooled mode)
		e.logger.Info("Executor: connection pooling disabled, using sql.DB")

		db, err := sql.Open("postgres", e.dsn)
		if err != nil {
			return fmt.Errorf("failed to open database: %w", err)
		}

		// Test the connection
		if err := db.Ping(); err != nil {
			db.Close()
			return fmt.Errorf("failed to ping database: %w", err)
		}

		e.db = db
	}

	e.isOpen.Store(true)
	e.logger.Info("Executor opened database connection")

	return nil
}

// getPoolCapacity returns the pool capacity with default fallback.
func (e *Executor) getPoolCapacity() int {
	if e.dbConfig.PoolCapacity > 0 {
		return e.dbConfig.PoolCapacity
	}
	return 100 // Default capacity
}

// getPoolMaxIdle returns the max idle connections with default fallback.
func (e *Executor) getPoolMaxIdle() int {
	if e.dbConfig.PoolMaxIdle > 0 {
		return e.dbConfig.PoolMaxIdle
	}
	return 50 // Default max idle
}

// getPoolIdleTimeout returns the idle timeout with default fallback.
func (e *Executor) getPoolIdleTimeout() time.Duration {
	if e.dbConfig.PoolIdleTimeout > 0 {
		return e.dbConfig.PoolIdleTimeout
	}
	return 5 * time.Minute // Default idle timeout
}

// getPoolMaxLifetime returns the max connection lifetime with default fallback.
func (e *Executor) getPoolMaxLifetime() time.Duration {
	if e.dbConfig.PoolMaxLifetime > 0 {
		return e.dbConfig.PoolMaxLifetime
	}
	return 1 * time.Hour // Default max lifetime
}

// ExecuteQuery implements queryservice.QueryService.
func (e *Executor) ExecuteQuery(ctx context.Context, target *query.Target, sql string, options *query.ExecuteOptions) (*query.QueryResult, error) {
	if target == nil {
		target = &query.Target{}
	}
	e.logger.DebugContext(ctx, "executing query",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"query", sql)

	// TODO: Use ExecuteOptions for prepared statements, portals, session settings, etc.
	// Extract maxRows from options if provided
	maxRows := uint64(0)
	if options != nil {
		maxRows = options.MaxRows
	}

	// Execute the query and stream results
	return e.executeQuery(ctx, sql, maxRows)
}

// StreamExecute executes a query and streams results back via callback.
// This implements the queryservice.QueryService interface.
func (e *Executor) StreamExecute(
	ctx context.Context,
	target *query.Target,
	sql string,
	options *query.ExecuteOptions,
	callback func(context.Context, *query.QueryResult) error,
) error {
	// TODO: Use ExecuteOptions for prepared statements, portals, session settings, etc.
	// Execute the query and stream results
	// TODO(GuptaManan100): Actually stream the results from postgres.
	result, err := e.ExecuteQuery(ctx, target, sql, options)
	if err != nil {
		e.logger.ErrorContext(ctx, "query execution failed", "error", err, "query", sql)
		return fmt.Errorf("query execution failed: %w", err)
	}

	// Stream the result via callback
	if err := callback(ctx, result); err != nil {
		return err
	}

	return nil
}

// ReserveStreamExecute reserves a connection from the pool and executes a query on it.
// Returns ReservedState containing the connection ID and pooler ID for subsequent queries.
func (e *Executor) ReserveStreamExecute(
	ctx context.Context,
	target *query.Target,
	sql string,
	options *query.ExecuteOptions,
	callback func(context.Context, *query.QueryResult) error,
) (queryservice.ReservedState, error) {
	// TODO: Implement actual connection reservation logic
	// For now, just execute the query normally and return empty state
	e.logger.WarnContext(ctx, "ReserveStreamExecute not fully implemented, using regular StreamExecute")
	err := e.StreamExecute(ctx, target, sql, options, callback)
	return queryservice.ReservedState{}, err
}

// Describe returns metadata about a prepared statement or portal.
func (e *Executor) Describe(
	ctx context.Context,
	target *query.Target,
	options *query.ExecuteOptions,
) (*query.StatementDescription, error) {
	// TODO: Implement Describe to return field descriptions for prepared statements/portals
	e.logger.WarnContext(ctx, "Describe not yet implemented")
	return nil, fmt.Errorf("Describe not yet implemented")
}

// Close closes the executor and releases resources.
func (e *Executor) Close(ctx context.Context) error {
	if !e.isOpen.Swap(false) {
		return nil
	}

	// Close the pool if it exists
	if e.pool != nil {
		if err := e.pool.Close(); err != nil {
			return fmt.Errorf("failed to close connection pool: %w", err)
		}
		e.pool = nil
	}

	// Close sql.DB if it exists (non-pooled mode)
	if e.db != nil {
		if err := e.db.Close(); err != nil {
			return fmt.Errorf("failed to close database: %w", err)
		}
		e.db = nil
	}

	e.logger.InfoContext(ctx, "Executor: closed")
	return nil
}

// IsHealthy checks if the executor is healthy and can serve queries.
func (e *Executor) IsHealthy() error {
	if !e.isOpen.Load() {
		return fmt.Errorf("executor not open")
	}

	if e.pool != nil {
		// Pool mode - verify we can get a connection
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		pooled, err := e.pool.Get(ctx)
		if err != nil {
			return fmt.Errorf("failed to get connection from pool: %w", err)
		}
		defer func() {
			if putErr := e.pool.Put(pooled); putErr != nil {
				e.logger.Warn("failed to return connection to pool during health check", "error", putErr)
			}
		}()

		// Test the connection
		if err := pooled.Conn().Exec(ctx, "SELECT 1"); err != nil {
			return fmt.Errorf("connection health check failed: %w", err)
		}

		return nil
	}

	if e.db == nil {
		return fmt.Errorf("database connection not initialized")
	}

	if err := e.db.Ping(); err != nil {
		return fmt.Errorf("database ping failed: %w", err)
	}

	return nil
}

// executeQuery executes a SQL query and returns the result.
// This is the internal method that handles both SELECT and modification queries.
func (e *Executor) executeQuery(ctx context.Context, queryStr string, maxRows uint64) (*query.QueryResult, error) {
	// Determine if this is a SELECT query or a modification query
	trimmedQuery := strings.TrimSpace(strings.ToUpper(queryStr))
	isSelect := strings.HasPrefix(trimmedQuery, "SELECT") ||
		strings.HasPrefix(trimmedQuery, "WITH") ||
		strings.HasPrefix(trimmedQuery, "SHOW") ||
		strings.HasPrefix(trimmedQuery, "EXPLAIN")

	if isSelect {
		return e.executeSelectQuery(ctx, queryStr, maxRows)
	}
	return e.executeModifyQuery(ctx, queryStr)
}

// executeSelectQuery executes a SELECT query and returns rows.
func (e *Executor) executeSelectQuery(ctx context.Context, queryStr string, maxRows uint64) (*query.QueryResult, error) {
	var rows *sql.Rows
	var err error

	if e.pool != nil {
		// Use connection pool
		pooled, err := e.pool.Get(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get connection from pool: %w", err)
		}
		defer func() {
			if putErr := e.pool.Put(pooled); putErr != nil {
				e.logger.Warn("failed to return connection to pool after query", "error", putErr)
			}
		}()

		rows, err = pooled.Conn().Query(ctx, queryStr)
		if err != nil {
			return nil, fmt.Errorf("failed to execute query: %w", err)
		}
	} else {
		// Use sql.DB
		rows, err = e.db.QueryContext(ctx, queryStr)
		if err != nil {
			return nil, fmt.Errorf("failed to execute query: %w", err)
		}
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
	fields := make([]*query.Field, len(columns))
	for i, col := range columns {
		fields[i] = &query.Field{
			Name: col,
			Type: columnTypes[i].DatabaseTypeName(),
		}
	}

	// Read rows
	var resultRows []*query.Row
	scanValues := make([]any, len(columns))
	scanPointers := make([]any, len(columns))

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
				values[i] = fmt.Appendf(nil, "%v", val)
			}
		}

		resultRows = append(resultRows, &query.Row{Values: values})
		rowCount++
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading rows: %w", err)
	}

	// Generate command tag for SELECT
	commandTag := fmt.Sprintf("SELECT %d", rowCount)

	return &query.QueryResult{
		Fields:       fields,
		RowsAffected: 0, // SELECT queries don't affect rows
		Rows:         resultRows,
		CommandTag:   commandTag,
	}, nil
}

// executeModifyQuery executes an INSERT, UPDATE, DELETE, or other modification query.
func (e *Executor) executeModifyQuery(ctx context.Context, queryStr string) (*query.QueryResult, error) {
	var rowsAffected int64

	if e.pool != nil {
		// Use connection pool
		pooled, err := e.pool.Get(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get connection from pool: %w", err)
		}
		defer func() {
			if putErr := e.pool.Put(pooled); putErr != nil {
				e.logger.Warn("failed to return connection to pool after modify", "error", putErr)
			}
		}()

		err = pooled.Conn().Exec(ctx, queryStr)
		if err != nil {
			return nil, fmt.Errorf("failed to execute query: %w", err)
		}

		// Note: DBConn.Exec doesn't return RowsAffected
		// For now, we set it to 0. In the future, we might need to enhance
		// DBConn to return sql.Result
		rowsAffected = 0
	} else {
		// Use sql.DB
		result, err := e.db.ExecContext(ctx, queryStr)
		if err != nil {
			return nil, fmt.Errorf("failed to execute query: %w", err)
		}

		rowsAffected, err = result.RowsAffected()
		if err != nil {
			// Some queries don't support RowsAffected, that's okay
			rowsAffected = 0
		}
	}

	// Generate command tag based on query type
	commandTag := e.generateCommandTag(queryStr, uint64(rowsAffected))

	return &query.QueryResult{
		Fields:       []*query.Field{}, // No fields for modification queries
		RowsAffected: uint64(rowsAffected),
		Rows:         []*query.Row{}, // No rows for modification queries
		CommandTag:   commandTag,
	}, nil
}

// generateCommandTag generates a PostgreSQL command tag for the result.
func (e *Executor) generateCommandTag(queryStr string, rowsAffected uint64) string {
	trimmedQuery := strings.TrimSpace(strings.ToUpper(queryStr))

	switch {
	case strings.HasPrefix(trimmedQuery, "INSERT"):
		return fmt.Sprintf("INSERT 0 %d", rowsAffected)
	case strings.HasPrefix(trimmedQuery, "UPDATE"):
		return fmt.Sprintf("UPDATE %d", rowsAffected)
	case strings.HasPrefix(trimmedQuery, "DELETE"):
		return fmt.Sprintf("DELETE %d", rowsAffected)
	case strings.HasPrefix(trimmedQuery, "CREATE TABLE"):
		return "CREATE TABLE"
	case strings.HasPrefix(trimmedQuery, "DROP TABLE"):
		return "DROP TABLE"
	case strings.HasPrefix(trimmedQuery, "ALTER TABLE"):
		return "ALTER TABLE"
	case strings.HasPrefix(trimmedQuery, "CREATE INDEX"):
		return "CREATE INDEX"
	case strings.HasPrefix(trimmedQuery, "DROP INDEX"):
		return "DROP INDEX"
	default:
		// Generic command complete
		return "COMMAND"
	}
}

// Ensure Executor implements queryservice.QueryService
var _ queryservice.QueryService = (*Executor)(nil)
