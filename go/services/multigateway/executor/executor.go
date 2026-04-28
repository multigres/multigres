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
	"time"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/engine"
	"github.com/multigres/multigres/go/services/multigateway/handler"
	"github.com/multigres/multigres/go/services/multigateway/plancache"
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
	planner   *planner.Planner
	exec      engine.IExecute
	logger    *slog.Logger
	planCache *plancache.PlanCache
}

// NewExecutor creates a new executor instance.
// The IExecute parameter provides the execution backend (typically ScatterConn).
// planCacheMemory controls the maximum memory in bytes for the plan cache (0 disables caching).
func NewExecutor(exec engine.IExecute, logger *slog.Logger, planCacheMemory int) *Executor {
	txnMetrics, err := engine.NewTransactionMetrics()
	if err != nil {
		logger.Warn("failed to initialise some transaction metrics", "error", err)
	}
	return &Executor{
		planner:   planner.NewPlanner(DefaultTableGroup, logger, txnMetrics),
		exec:      exec,
		logger:    logger,
		planCache: plancache.New(planCacheMemory),
	}
}

// StreamExecute executes a query and streams results back via the callback function.
//
// For cacheable statements (SELECT, INSERT, UPDATE, DELETE), the executor
// normalizes the query (replacing literals with $1, $2, ... placeholders)
// and checks the plan cache. On a cache hit, the cached plan is reused with
// the current query's bind variables. On a miss, the query is planned using
// the normalized SQL/AST, and the resulting plan is cached for future reuse.
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
) (*handler.ExecuteResult, error) {
	e.logger.DebugContext(ctx, "executing query",
		"query", queryStr,
		"user", conn.User(),
		"database", conn.Database(),
		"connection_id", conn.ConnectionID())

	planStart := time.Now()
	plan, bindVars, cacheHit, normalizedSQL, fingerprint, err := e.resolvePlan(ctx, queryStr, astStmt, conn)
	planTime := time.Since(planStart)
	if err != nil {
		e.logger.ErrorContext(ctx, "query planning failed",
			"query", queryStr,
			"error", err)
		return &handler.ExecuteResult{
			PlanTime:      planTime,
			NormalizedSQL: normalizedSQL,
			Fingerprint:   fingerprint,
		}, err
	}

	result := &handler.ExecuteResult{
		TablesUsed:    plan.TablesUsed,
		PlanType:      plan.Type,
		PlanTime:      planTime,
		CacheHit:      cacheHit,
		NormalizedSQL: normalizedSQL,
		Fingerprint:   fingerprint,
	}

	err = plan.StreamExecute(ctx, e.exec, conn, state, bindVars, callback)
	if err != nil {
		e.logger.ErrorContext(ctx, "query execution failed",
			"query", queryStr,
			"plan", plan.String(),
			"error", err)
	}
	return result, err
}

// resolvePlan obtains a query plan, using the plan cache when possible.
// Returns the plan, bind variables extracted during normalization (nil if none),
// whether the plan was a cache hit, the normalized SQL string (empty for
// non-cacheable statements), a stable fingerprint hash of that normalized SQL,
// and any planning error.
func (e *Executor) resolvePlan(
	ctx context.Context,
	queryStr string,
	astStmt ast.Stmt,
	conn *server.Conn,
) (*engine.Plan, []*ast.A_Const, bool, string, string, error) {
	if !isCacheable(astStmt) {
		plan, err := e.planner.Plan(queryStr, astStmt, conn)
		if err != nil {
			return nil, nil, false, "", "", err
		}
		e.logger.DebugContext(ctx, "query plan created (non-cacheable)",
			"plan", plan.String(),
			"tablegroup", plan.GetTableGroup())
		return plan, nil, false, "", "", nil
	}

	// Normalize: replace literals with $1, $2, ... placeholders.
	// If the query has no literals, NormalizedSQL equals the original SQL
	// and BindValues is empty — the plan is still cached by its SQL string.
	normResult := ast.Normalize(astStmt)
	normalizedSQL := normResult.NormalizedSQL
	fingerprint := normResult.Fingerprint()
	cacheKey := buildCacheKey(conn.Database(), normalizedSQL)
	var bindVars []*ast.A_Const
	if normResult.WasNormalized() {
		bindVars = normResult.BindValues
	}

	// Cache hit
	if cachedPlan, ok := e.planCache.Get(ctx, cacheKey); ok {
		e.logger.DebugContext(ctx, "plan cache hit",
			"normalized_query", normalizedSQL)
		return cachedPlan, bindVars, true, normalizedSQL, fingerprint, nil
	}

	// Cache miss — plan with normalized SQL/AST and cache the result.
	plan, err := e.planner.Plan(normalizedSQL, normResult.NormalizedAST, conn)
	if err != nil {
		return nil, nil, false, normalizedSQL, fingerprint, err
	}

	e.planCache.Put(cacheKey, plan)
	e.logger.DebugContext(ctx, "plan cache miss, planned and cached",
		"normalized_query", normalizedSQL,
		"plan", plan.String())
	return plan, bindVars, false, normalizedSQL, fingerprint, nil
}

// isCacheable returns true if the statement type is eligible for plan caching.
// Only DML statements that go through planDefault() are cacheable.
func isCacheable(stmt ast.Stmt) bool {
	switch stmt.NodeTag() {
	case ast.T_SelectStmt:
		// Exclude SELECT INTO — temp-table variants use a different primitive
		// (TempTableRoute), and non-temp variants are DDL-like (they create a
		// table), so caching their plans is not useful.
		if ss, ok := stmt.(*ast.SelectStmt); ok && ss.IntoClause != nil {
			return false
		}
		return true
	case ast.T_InsertStmt, ast.T_UpdateStmt, ast.T_DeleteStmt:
		return true
	default:
		return false
	}
}

// PortalStreamExecute executes a portal and streams results back via the callback function.
func (e *Executor) PortalStreamExecute(
	ctx context.Context,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	portalInfo *preparedstatement.PortalInfo,
	maxRows int32,
	callback func(ctx context.Context, res *sqltypes.Result) error,
) (*handler.ExecuteResult, error) {
	e.logger.DebugContext(ctx, "executing portal",
		"portal", portalInfo.Portal.Name,
		"max_rows", maxRows,
		"user", conn.User(),
		"database", conn.Database(),
		"connection_id", conn.ConnectionID())

	planStart := time.Now()
	astStmt := portalInfo.PreparedStatementInfo.AstStmt()

	// For cacheable DML (SELECT, INSERT, UPDATE, DELETE), use the plan cache
	// to resolve routing. The extended protocol query already has $1, $2, ...
	// placeholders — the same form as our normalized cache key — so portal
	// queries share cache entries with simple protocol queries.
	if isCacheable(astStmt) {
		plan, cacheHit, err := e.resolvePortalPlan(ctx, astStmt, conn)
		planTime := time.Since(planStart)
		normalizedSQL := astStmt.SqlString()
		fingerprint := ast.FingerprintSQL(normalizedSQL)
		if err != nil {
			e.logger.ErrorContext(ctx, "portal query planning failed",
				"query", portalInfo.PreparedStatementInfo.Query, "error", err)
			return &handler.ExecuteResult{
				PlanTime:      planTime,
				NormalizedSQL: normalizedSQL,
				Fingerprint:   fingerprint,
			}, err
		}

		// Hand off to the plan, which delegates to its root primitive's
		// PortalStreamExecute. Each primitive owns its portal-mode behavior:
		// Route reissues the portal to the multipooler, Sequence iterates
		// children (so any silent ApplySessionState prefix runs before the
		// trailing Route forwards), gateway-local primitives ignore
		// portalInfo and run their StreamExecute logic.
		err = plan.PortalStreamExecute(ctx, e.exec, conn, state, portalInfo, maxRows, callback)
		return &handler.ExecuteResult{
			TablesUsed:    plan.TablesUsed,
			PlanType:      plan.Type,
			PlanTime:      planTime,
			CacheHit:      cacheHit,
			NormalizedSQL: normalizedSQL,
			Fingerprint:   fingerprint,
		}, err
	}

	// Non-cacheable — check if the gateway needs to handle locally (e.g.,
	// SET/SHOW gateway-managed variables, LISTEN/NOTIFY, temp table DDL).
	plan, err := e.planner.PlanPortal(portalInfo, conn)
	planTime := time.Since(planStart)
	if err != nil {
		e.logger.ErrorContext(ctx, "portal query planning failed",
			"query", portalInfo.PreparedStatementInfo.Query,
			"error", err)
		return &handler.ExecuteResult{PlanTime: planTime}, err
	}
	if plan != nil {
		e.logger.DebugContext(ctx, "executing portal plan locally",
			"plan", plan.String())
		err = plan.StreamExecute(ctx, e.exec, conn, state, nil, callback)
		return &handler.ExecuteResult{
			TablesUsed: plan.TablesUsed,
			PlanType:   plan.Type,
			PlanTime:   planTime,
		}, err
	}

	// Non-cacheable, non-local — send directly to multipooler with defaults.
	err = e.exec.PortalStreamExecute(ctx, e.planner.GetDefaultTableGroup(), constants.DefaultShard, conn, state, portalInfo, maxRows, callback)
	return &handler.ExecuteResult{
		TablesUsed: ast.ExtractTablesUsed(astStmt),
		PlanType:   engine.PlanTypeRoute,
		PlanTime:   planTime,
	}, err
}

// resolvePortalPlan looks up or creates a cached plan for a portal's query.
// The AST's SqlString() is used as the normalized SQL portion of the cache key,
// producing the same canonical form as the simple protocol path. This ensures
// cross-protocol cache sharing regardless of casing or whitespace differences
// in the original query text.
func (e *Executor) resolvePortalPlan(
	ctx context.Context,
	astStmt ast.Stmt,
	conn *server.Conn,
) (*engine.Plan, bool, error) {
	normalizedSQL := astStmt.SqlString()
	cacheKey := buildCacheKey(conn.Database(), normalizedSQL)
	if cachedPlan, ok := e.planCache.Get(ctx, cacheKey); ok {
		e.logger.DebugContext(ctx, "portal plan cache hit", "query", normalizedSQL)
		return cachedPlan, true, nil
	}

	plan, err := e.planner.Plan(normalizedSQL, astStmt, conn)
	if err != nil {
		return nil, false, err
	}

	e.planCache.Put(cacheKey, plan)
	e.logger.DebugContext(ctx, "portal plan cache miss, planned and cached",
		"query", normalizedSQL, "plan", plan.String())
	return plan, false, nil
}

// buildCacheKey constructs the plan cache key from the database name and
// normalized SQL. Including the database prevents cross-database plan reuse
// (different databases may have different schemas and routing).
//
// TODO(GuptaManan100): When shard-aware routing is introduced and the planner
// starts resolving table names for shard selection, search_path will need to
// be included in the cache key as well, since it affects table name resolution.
func buildCacheKey(database, normalizedSQL string) string {
	return database + "\x00" + normalizedSQL
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

	// TODO: We will need to plan the query to find whether it can
	// be served by a single shard or not. For now, since we only
	// support unsharded, we don't have to do much.
	// We just send the query to the default table group.

	return e.exec.Describe(ctx, e.planner.GetDefaultTableGroup(), constants.DefaultShard, conn, state, portalInfo, preparedStatementInfo)
}

// ReleaseAll releases all reserved connections, regardless of reservation reason.
// Delegates to ReleaseAllReservedConnections which calls ReleaseReservedConnection
// on the multipooler for each reserved connection. The multipooler handles
// rollback, COPY abort, and portal release internally.
// Used for connection cleanup when a client disconnects.
func (e *Executor) ReleaseAll(
	ctx context.Context,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
) error {
	return e.exec.ReleaseAllReservedConnections(ctx, conn, state)
}

// Close shuts down the executor, releasing resources such as the plan cache.
func (e *Executor) Close() {
	e.planCache.Close()
}

// Ensure Executor implements handler.Executor interface.
var _ handler.Executor = (*Executor)(nil)
