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
	"fmt"
	"log/slog"

	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/engine"
	"github.com/multigres/multigres/go/services/multigateway/handler"
	"github.com/multigres/multigres/go/services/multigateway/planner"
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
	state *handler.MultiGatewayConnectionState,
	queryStr string,
	astStmt ast.Stmt,
	callback func(ctx context.Context, res *sqltypes.Result) error,
) error {
	e.logger.DebugContext(ctx, "executing query",
		"query", queryStr,
		"user", conn.User(),
		"database", conn.Database(),
		"connection_id", conn.ConnectionID())

	// Step 1: Plan the query (now with AST for better analysis)
	plan, err := e.planner.Plan(queryStr, astStmt, conn)
	if err != nil {
		e.logger.ErrorContext(ctx, "query planning failed",
			"query", queryStr,
			"error", err)
		return err
	}

	e.logger.DebugContext(ctx, "query plan created",
		"plan", plan.String(),
		"tablegroup", plan.GetTableGroup())

	// Step 2: Execute the plan
	// Pass the IExecute implementation to the plan, which will pass it to the primitive
	err = plan.StreamExecute(ctx, e.exec, conn, state, callback)
	if err != nil {
		e.logger.ErrorContext(ctx, "query execution failed",
			"query", queryStr,
			"plan", plan.String(),
			"error", err)
		return err
	}

	e.logger.DebugContext(ctx, "query execution completed",
		"query", queryStr,
		"tablegroup", plan.GetTableGroup())

	return nil
}

// PortalStreamExecute executes a portal and streams results back via the callback function.
func (e *Executor) PortalStreamExecute(
	ctx context.Context,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	portalInfo *preparedstatement.PortalInfo,
	maxRows int32,
	callback func(ctx context.Context, res *sqltypes.Result) error,
) error {
	e.logger.DebugContext(ctx, "executing portal",
		"portal", portalInfo.Portal.Name,
		"max_rows", maxRows,
		"user", conn.User(),
		"database", conn.Database(),
		"connection_id", conn.ConnectionID())

	// TODO: We will need to plan the query to find wether it can
	// be served by a single shard or not. For now, since we only
	// support unsharded, we don't have to do much.
	// We just send the query to the default table group.

	return e.exec.PortalStreamExecute(ctx, e.planner.GetDefaultTableGroup(), "", conn, state, portalInfo, maxRows, callback)
}

// Describe returns metadata about a prepared statement or portal.
func (e *Executor) Describe(
	ctx context.Context,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	portalInfo *preparedstatement.PortalInfo,
	preparedStatementInfo *preparedstatement.PreparedStatementInfo,
) (*query.StatementDescription, error) {
	e.logger.DebugContext(ctx, "describe",
		"user", conn.User(),
		"database", conn.Database(),
		"connection_id", conn.ConnectionID())

	// TODO: We will need to plan the query to find wether it can
	// be served by a single shard or not. For now, since we only
	// support unsharded, we don't have to do much.
	// We just send the query to the default table group.

	return e.exec.Describe(ctx, e.planner.GetDefaultTableGroup(), "", conn, state, portalInfo, preparedStatementInfo)
}

// ValidateStartupParams validates startup parameters by executing SELECT 1
// against the backend with the startup params applied as session settings.
// If any parameter is invalid, the backend rejects the SET and returns an error.
// Returns ParameterStatus values captured from the backend response.
func (e *Executor) ValidateStartupParams(
	ctx context.Context,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
) (map[string]string, error) {
	e.logger.DebugContext(ctx, "validating startup parameters",
		"user", conn.User(),
		"database", conn.Database(),
		"connection_id", conn.ConnectionID())

	var paramStatus map[string]string
	err := e.exec.StreamExecute(
		ctx,
		conn,
		e.planner.GetDefaultTableGroup(),
		"",
		"SELECT 1",
		state,
		func(_ context.Context, result *sqltypes.Result) error {
			if len(result.ParameterStatus) > 0 {
				paramStatus = result.ParameterStatus
			}
			return nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("invalid startup parameters: %w", err)
	}

	return paramStatus, nil
}

// Ensure Executor implements handler.Executor interface.
var _ handler.Executor = (*Executor)(nil)
