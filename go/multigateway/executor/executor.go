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

package executor

import (
	"context"
	"log/slog"

	"github.com/multigres/multigres/go/multigateway/engine"
	"github.com/multigres/multigres/go/multigateway/handler"
	"github.com/multigres/multigres/go/multigateway/planner"
	"github.com/multigres/multigres/go/parser/ast"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/pgprotocol/server"
)

const (
	// TODO(GuptaManan100): Remove this and use discovery to find the table group and use that.
	DefaultTableGroup = "default"
)

// Executor is the query execution engine for multigateway.
// It handles query planning, routing to appropriate multipooler instances,
// and result streaming back to clients.
//
// The Executor depends only on the IExecute interface, not on concrete
// implementations like ScatterConn. This makes it easy to test by passing
// mock implementations.
type Executor struct {
	planner *planner.Planner
	exec    engine.IExecute
	logger  *slog.Logger
}

// NewExecutor creates a new executor instance.
// The IExecute parameter provides the execution backend (typically ScatterConn).
// This dependency injection pattern makes testing much easier.
func NewExecutor(exec engine.IExecute, logger *slog.Logger) *Executor {
	return &Executor{
		planner: planner.NewPlanner(DefaultTableGroup, logger),
		exec:    exec,
		logger:  logger,
	}
}

// StreamExecute executes a query and streams results back via the callback function.
// This method will eventually route queries to multipooler via gRPC.
//
// The callback function is invoked for each chunk of results. For large result sets,
// the callback may be invoked multiple times with partial results.
func (e *Executor) StreamExecute(
	ctx context.Context,
	conn *server.Conn,
	sql string,
	astStmt ast.Stmt,
	callback func(ctx context.Context, res *query.QueryResult) error,
) error {
	e.logger.DebugContext(ctx, "executing query",
		"query", sql,
		"user", conn.User(),
		"database", conn.Database(),
		"connection_id", conn.ConnectionID())

	// Step 1: Plan the query
	plan, err := e.planner.Plan(sql, conn)
	if err != nil {
		e.logger.ErrorContext(ctx, "query planning failed",
			"query", sql,
			"error", err)
		return err
	}

	e.logger.DebugContext(ctx, "query plan created",
		"plan", plan.String(),
		"tablegroup", plan.GetTableGroup())

	// Step 2: Execute the plan
	// Pass the IExecute implementation to the plan, which will pass it to the primitive
	err = plan.StreamExecute(ctx, e.exec, conn, callback)
	if err != nil {
		e.logger.ErrorContext(ctx, "query execution failed",
			"query", sql,
			"plan", plan.String(),
			"error", err)
		return err
	}

	e.logger.DebugContext(ctx, "query execution completed",
		"query", sql,
		"tablegroup", plan.GetTableGroup())

	return nil
}

// Ensure Executor implements handler.Executor interface.
var _ handler.Executor = (*Executor)(nil)
