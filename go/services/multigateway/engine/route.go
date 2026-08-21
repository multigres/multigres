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

	// ExecuteSQLPreparedStatement, if set, describes a SQL-level EXECUTE
	// wrapper whose prepared-statement name must be resolved by the multipooler
	// through pooler-level consolidation before Query runs. Used for wrapped
	// EXECUTE forms (EXPLAIN EXECUTE, CREATE TABLE ... AS EXECUTE).
	ExecuteSQLPreparedStatement *query.ExecuteSqlPreparedStatement

	// KeepStructured opts this route out of opaque row passthrough, forcing the
	// multipooler to return structured Rows even when passthrough is enabled.
	// It is a static plan-build-time property (set at construction, no runtime
	// input), so it lives on the primitive and is folded into the multipooler's
	// ExecuteOptions by StreamExecute — not carried on the per-call PlanExecInfo.
	// Set by routes whose caller reads the result rows itself rather than
	// streaming them to the client (for example ResolveTrackSetConfig's
	// resolve projection).
	KeepStructured bool
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

// NewRouteWithExecuteSQLPreparedStatement creates a Route carrying a SQL-level
// EXECUTE wrapper to be materialized by the multipooler after pooler-level
// prepared-statement consolidation. See Route.ExecuteSQLPreparedStatement.
func NewRouteWithExecuteSQLPreparedStatement(tableGroup, shard, sql string, ps *query.ExecuteSqlPreparedStatement) *Route {
	return &Route{
		TableGroup:                  tableGroup,
		Shard:                       shard,
		Query:                       sql,
		ExecuteSQLPreparedStatement: ps,
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
	state *handler.MultigatewayConnectionState,
	bindVars []*ast.A_Const,
	info PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	query := r.Query
	if len(bindVars) > 0 && r.NormalizedAST != nil {
		query = ast.ReconstructSQL(r.NormalizedAST, bindVars)
	}
	// Execute the query through the execution interface, forwarding the plan's
	// reservation directives. We pass ctx (not conn.Context()) so that deadlines
	// set by executeWithTimeout propagate through gRPC to the multipooler for
	// statement timeout enforcement.
	return exec.StreamExecute(
		ctx,
		conn,
		r.TableGroup,
		r.Shard,
		query,
		r.ExecuteSQLPreparedStatement,
		state,
		info,
		r.KeepStructured,
		captureReportedSettings(info, callback),
	)
}

// captureReportedSettings wraps callback so GUC_REPORT values PostgreSQL
// attached to the routed result are also recorded onto the Sequence exchange
// (keyed by ParameterStatus display name) for a trailing silent tracker.
// PostgreSQL's report carries the CANONICAL value — SET datestyle = 'dmy' on
// a backend at 'German, YMD' reports 'German, DMY' — which is what must be
// tracked into the replayable session map; the client's partial literal
// under-describes the composite state and would drop the style component on
// pool rotation. The result itself is forwarded unchanged, so the
// client-facing ParameterStatus relay is unaffected. No-op outside a
// Sequence (nil exchange).
func captureReportedSettings(info PlanExecInfo, callback func(context.Context, *sqltypes.Result) error) func(context.Context, *sqltypes.Result) error {
	if info.Exchange == nil {
		return callback
	}
	return func(ctx context.Context, result *sqltypes.Result) error {
		for name, value := range result.ParameterStatus {
			info.Exchange.AddReportedSetting(name, value)
		}
		return callback(ctx, result)
	}
}

// PortalStreamExecute reissues the portal against the route's tablegroup/shard
// so the multipooler receives the original query text (with $N placeholders)
// alongside the wire-format Bind values. The bindVars slice from
// StreamExecute is unused here — the portal carries its own binds.
func (r *Route) PortalStreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	portalInfo *preparedstatement.PortalInfo,
	maxRows int32,
	includeDescribe bool,
	info PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	// The portal path normally reissues the client's original prepared statement.
	// But when this route carries a REWRITTEN query — e.g. a SessionStateBranch's
	// unpinned is_local:=true revert — reissuing the original portal would drop
	// the rewrite (the set_config would run is_local=false and persist on the
	// pooled backend, leaking across clients). r.Query holds the rewritten SQL
	// (== r.Query on a plain route, or the normalized/reverted form here); when it
	// differs from the portal's prepared statement, run the rewritten query with
	// the client's bind values instead. The rewrite keeps every $N in place, so
	// the portal's binds still apply.
	pi := portalInfo
	if psi := portalInfo.PreparedStatementInfo; psi != nil && r.Query != "" && r.Query != psi.GetQuery() {
		rewrittenPSI, err := preparedstatement.NewPreparedStatementInfo(&query.PreparedStatement{
			Name:       psi.GetName(),
			Query:      r.Query,
			ParamTypes: psi.GetParamTypes(),
		})
		if err != nil {
			return err
		}
		pi = preparedstatement.NewPortalInfo(rewrittenPSI, portalInfo.Portal)
	}
	return exec.PortalStreamExecute(ctx, r.TableGroup, r.Shard, conn, state, pi, maxRows, includeDescribe, info, r.KeepStructured, captureReportedSettings(info, callback))
}

// SilentRoute executes a gateway-synthesized statement on the target shard
// and swallows its result rows and command tag. It exists for reconciliation
// statements (for example the startup-parameter restore a pinned RESET
// routes) whose response must never reach the client: the client-visible
// response comes from a sibling primitive in the same Sequence, exactly as
// on the unpinned probe-then-track paths. Errors still propagate, so a
// failed reconciliation aborts the statement before any tracking runs.
//
// ParameterStatus is NOT swallowed: when the synthesized statement changes a
// GUC_REPORT variable (application_name, DateStyle, TimeZone, ...),
// PostgreSQL's report must still reach the client or its driver keeps a
// stale — after RESET ALL restores, actively wrong — cached value. The
// backend-canonicalized values are forwarded as their own tag-less result,
// which the wire server turns into bare ParameterStatus messages (dropping
// any whose value did not actually change).
type SilentRoute struct {
	route *Route
}

// NewSilentRoute creates a swallowed-output route for gateway-synthesized SQL.
func NewSilentRoute(tableGroup, shard, query string) *SilentRoute {
	return &SilentRoute{route: NewRoute(tableGroup, shard, query, nil)}
}

func (r *SilentRoute) StreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	_ []*ast.A_Const,
	info PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	return r.route.StreamExecute(ctx, exec, conn, state, nil, info,
		func(cbCtx context.Context, result *sqltypes.Result) error {
			if callback == nil || len(result.ParameterStatus) == 0 {
				return nil
			}
			return callback(cbCtx, &sqltypes.Result{ParameterStatus: result.ParameterStatus})
		})
}

// PortalStreamExecute runs the synthesized SQL exactly as on the simple path.
// The client's portal carries the ORIGINAL statement, so reissuing it here
// would execute the wrong query; the synthesized statement is gateway-built
// with no binds and needs none of the portal machinery.
func (r *SilentRoute) PortalStreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	_ *preparedstatement.PortalInfo,
	_ int32,
	_ bool,
	info PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	return r.StreamExecute(ctx, exec, conn, state, nil, info, callback)
}

// GetQuery returns the synthesized SQL (used by tests and plan debugging).
func (r *SilentRoute) GetQuery() string {
	return r.route.Query
}

// GetTableGroup returns the target tablegroup.
func (r *SilentRoute) GetTableGroup() string {
	return r.route.TableGroup
}

// String returns a description of the silent route for debugging.
func (r *SilentRoute) String() string {
	return fmt.Sprintf("SilentRoute(tablegroup=%s, query=%s)", r.route.TableGroup, r.route.Query)
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
