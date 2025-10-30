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
	"strings"

	"github.com/multigres/multigres/go/multipooler/queryservice"
	"github.com/multigres/multigres/go/pb/query"
)

// Executor implements the QueryService interface for executing queries against PostgreSQL.
type Executor struct {
	logger *slog.Logger
	db     *sql.DB
}

// NewExecutor creates a new Executor instance.
func NewExecutor(logger *slog.Logger, db *sql.DB) *Executor {
	return &Executor{
		logger: logger,
		db:     db,
	}
}

// ExecuteQuery implements queryservice.QueryService.
func (e *Executor) ExecuteQuery(ctx context.Context, target *query.Target, sql string, maxRows uint64) (*query.QueryResult, error) {
	e.logger.Debug("executing query",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"query", sql)

	// Execute the query and stream results
	return e.executeQuery(ctx, sql, maxRows)
}

// StreamExecute executes a query and streams results back via callback.
// This implements the queryservice.QueryService interface.
func (e *Executor) StreamExecute(
	ctx context.Context,
	target *query.Target,
	sql string,
	callback func(context.Context, *query.QueryResult) error,
) error {
	// Execute the query and stream results
	// TODO(GuptaManan100): Actually stream the results from postgres.
	result, err := e.ExecuteQuery(ctx, target, sql, 0) // 0 = no max rows limit
	if err != nil {
		e.logger.Error("query execution failed", "error", err, "query", sql)
		return fmt.Errorf("query execution failed: %w", err)
	}

	// Stream the result via callback
	if err := callback(ctx, result); err != nil {
		return err
	}

	return nil
}

// Close closes the executor and releases resources.
func (e *Executor) Close(ctx context.Context) error {
	// Executor doesn't own the DB connection, so nothing to close
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
	rows, err := e.db.QueryContext(ctx, queryStr)
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
	fields := make([]*query.Field, len(columns))
	for i, col := range columns {
		fields[i] = &query.Field{
			Name: col,
			Type: columnTypes[i].DatabaseTypeName(),
		}
	}

	// Read rows
	var resultRows []*query.Row
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
	result, err := e.db.ExecContext(ctx, queryStr)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		// Some queries don't support RowsAffected, that's okay
		rowsAffected = 0
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
