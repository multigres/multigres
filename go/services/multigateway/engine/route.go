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

package engine

import (
	"context"
	"fmt"

	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// Route is a primitive that routes a query to a specific tablegroup.
// It represents the simplest form of query execution - sending a query
// to a single target tablegroup.
type Route struct {
	// TableGroup is the target tablegroup for this query.
	TableGroup string

	// Query is the SQL query string to execute. For cached plans, this is the
	// normalized SQL template with $1, $2, ... placeholders.
	Query string

	// Shard is the target shard (empty string for unsharded or any shard).
	Shard string

	// NormalizedAST is the normalized AST with ParamRef placeholders.
	// It is set for cached plans and used together with bindVars to
	// reconstruct the final SQL at execution time. Nil for non-cached plans.
	NormalizedAST ast.Stmt

	// PreparedStatement, if set, is a gateway-managed prepared statement
	// that must be parsed on the backend connection before Query runs.
	// Used for wrapped EXECUTE forms (EXPLAIN EXECUTE, CREATE TABLE ... AS
	// EXECUTE) where Query references the prepared statement by its
	// canonical name (e.g., "stmt42"). The planner rewrites the user-facing
	// name ("p") to the canonical name and attaches the metadata here so
	// the multipooler can ensurePrepared() on the chosen backend connection.
	PreparedStatement *query.PreparedStatement
}

// NewRoute creates a new Route primitive.
// The astStmt parameter is stored as NormalizedAST for SQL reconstruction at
// execution time (substituting bind values into ParamRef placeholders). Pass
// nil for routes that don't need SQL reconstruction (e.g., non-cached plans).
func NewRoute(tableGroup, shard, query string, astStmt ast.Stmt) *Route {
	return &Route{
		TableGroup:    tableGroup,
		Shard:         shard,
		Query:         query,
		NormalizedAST: astStmt,
	}
}

// NewRouteWithPreparedStatement creates a Route that carries a gateway-managed
// prepared statement to be ensured on the backend connection before execution.
// See Route.PreparedStatement for details.
func NewRouteWithPreparedStatement(tableGroup, shard, sql string, ps *query.PreparedStatement) *Route {
	return &Route{
		TableGroup:        tableGroup,
		Shard:             shard,
		Query:             sql,
		PreparedStatement: ps,
	}
}

// StreamExecute executes the route by sending the query to the target tablegroup.
// It uses the IExecute interface to perform the actual execution, allowing for
// easy testing and decoupling from concrete execution implementations.
//
// If bindVars is non-empty and NormalizedAST is set, the final SQL is
// reconstructed by substituting the bind values into the normalized AST.
// Otherwise, the Route's Query string is sent as-is.
func (r *Route) StreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	bindVars []*ast.A_Const,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	query := r.Query
	if len(bindVars) > 0 && r.NormalizedAST != nil {
		query = ast.ReconstructSQL(r.NormalizedAST, bindVars)
	}
	// Execute the query through the execution interface.
	// We pass ctx (not conn.Context()) so that deadlines set by executeWithTimeout
	// propagate through gRPC to the multipooler for statement timeout enforcement.
	return exec.StreamExecute(
		ctx,
		conn,
		r.TableGroup,
		r.Shard,
		query,
		r.PreparedStatement,
		state,
		callback,
	)
}

// PortalStreamExecute reissues the portal against the route's tablegroup/shard
// so the multipooler receives the original query text (with $N placeholders)
// alongside the wire-format Bind values. The bindVars slice from
// StreamExecute is unused here — the portal carries its own binds.
func (r *Route) PortalStreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	portalInfo *preparedstatement.PortalInfo,
	maxRows int32,
	includeDescribe bool,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	return exec.PortalStreamExecute(ctx, r.TableGroup, r.Shard, conn, state, portalInfo, maxRows, includeDescribe, callback)
}

// GetTableGroup returns the target tablegroup.
func (r *Route) GetTableGroup() string {
	return r.TableGroup
}

// GetQuery returns the SQL query.
func (r *Route) GetQuery() string {
	return r.Query
}

// String returns a description of the route for debugging.
func (r *Route) String() string {
	return fmt.Sprintf("Route(tablegroup=%s, query=%s)", r.TableGroup, r.Query)
}

// Ensure Route implements Primitive interface.
var _ Primitive = (*Route)(nil)
