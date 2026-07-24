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
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
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
	state *handler.MultigatewayConnectionState,
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
		plan, err := e.planner.Plan(queryStr, astStmt, conn, planner.PlanOptions{})
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
	plan, err := e.planner.Plan(normalizedSQL, normResult.NormalizedAST, conn, planner.PlanOptions{})
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
	state *handler.MultigatewayConnectionState,
	portalInfo *preparedstatement.PortalInfo,
	maxRows int32,
	includeDescribe bool,
	callback func(ctx context.Context, res *sqltypes.Result) error,
) (*handler.ExecuteResult, error) {
	e.logger.DebugContext(ctx, "executing portal",
		"portal", portalInfo.Portal.Name,
		"max_rows", maxRows,
		"user", conn.User(),
		"database", conn.Database(),
		"connection_id", conn.ConnectionID())

	planStart := time.Now()
	plan, cacheHit, normalizedSQL, fingerprint, err := e.resolvePortalPlan(ctx, portalInfo, conn)
	planTime := time.Since(planStart)
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
	// Route reissues the portal to the multipooler, Sequence iterates children
	// (so a Route can forward first and a silent ApplySessionState child can
	// track only after backend success), and gateway-local primitives ignore
	// portalInfo and run their StreamExecute logic. A plain Route reissuing the
	// portal is exactly what a raw forward to the multipooler would do, so
	// non-routable utility statements need no special-casing here.
	err = plan.PortalStreamExecute(ctx, e.exec, conn, state, portalInfo, maxRows, includeDescribe, callback)
	if err != nil {
		e.logger.ErrorContext(ctx, "portal query execution failed",
			"query", portalInfo.PreparedStatementInfo.Query,
			"plan", plan.String(), "error", err)
	}
	return &handler.ExecuteResult{
		TablesUsed:    plan.TablesUsed,
		PlanType:      plan.Type,
		PlanTime:      planTime,
		CacheHit:      cacheHit,
		NormalizedSQL: normalizedSQL,
		Fingerprint:   fingerprint,
	}, err
}

// resolvePortalPlan obtains a query plan for a portal, mirroring resolvePlan but
// for the extended protocol. The portal query already carries $1, $2, ...
// placeholders, so there is nothing to normalize: the AST's SqlString() is used
// directly as the cache key's SQL portion, producing the same canonical form as
// the simple protocol path so the two protocols share cache entries regardless
// of casing or whitespace in the original query text.
//
// Returns the plan, whether it was a cache hit, the normalized SQL (empty for
// non-cacheable statements), a fingerprint of that SQL, and any planning error.
func (e *Executor) resolvePortalPlan(
	ctx context.Context,
	portalInfo *preparedstatement.PortalInfo,
	conn *server.Conn,
) (*engine.Plan, bool, string, string, error) {
	astStmt := portalInfo.PreparedStatementInfo.AstStmt()

	// Non-cacheable statements (SET/SHOW, LISTEN/NOTIFY, DISCARD, temp/unlogged
	// DDL, transactions, cursors, PREPARE/EXECUTE/DEALLOCATE, plain DDL, ...) are
	// planned directly. Plan produces a gateway-local primitive where the
	// statement needs special handling and a plain Route otherwise — and a Route
	// in portal mode forwards the portal to the multipooler, which is what
	// non-routable statements want anyway.
	//
	// IsPortal is set only here, on the path that never caches. It gates exactly
	// one plan-time decision — the wrapped-EXECUTE unwrap (EXPLAIN EXECUTE /
	// CREATE TABLE AS EXECUTE), which is simple-protocol only and itself
	// non-cacheable. Keeping IsPortal off the cacheable branch makes the shared
	// plan cache protocol-agnostic by construction: every plan that can be cached
	// is built identically regardless of protocol, so a plan cached by one path
	// is always correct to serve to the other. The protocol difference lives in
	// the plan's PortalStreamExecute vs StreamExecute, never in its content.
	if !isCacheable(astStmt) {
		plan, err := e.planner.Plan(portalInfo.PreparedStatementInfo.Query, astStmt, conn, planner.PlanOptions{IsPortal: true})
		if err != nil {
			return nil, false, "", "", err
		}
		e.logger.DebugContext(ctx, "portal plan created (non-cacheable)",
			"plan", plan.String())
		return plan, false, "", "", nil
	}

	normalizedSQL := astStmt.SqlString()
	fingerprint := ast.FingerprintSQL(normalizedSQL)
	cacheKey := buildCacheKey(conn.Database(), normalizedSQL)
	if cachedPlan, ok := e.planCache.Get(ctx, cacheKey); ok {
		e.logger.DebugContext(ctx, "portal plan cache hit", "query", normalizedSQL)
		return cachedPlan, true, normalizedSQL, fingerprint, nil
	}

	// Cacheable DML is planned protocol-agnostically (zero-value PlanOptions, same
	// as the simple path) so the cached entry is shared safely across protocols.
	plan, err := e.planner.Plan(normalizedSQL, astStmt, conn, planner.PlanOptions{})
	if err != nil {
		return nil, false, normalizedSQL, fingerprint, err
	}

	e.planCache.Put(cacheKey, plan)
	e.logger.DebugContext(ctx, "portal plan cache miss, planned and cached",
		"query", normalizedSQL, "plan", plan.String())
	return plan, false, normalizedSQL, fingerprint, nil
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
	state *handler.MultigatewayConnectionState,
	portalInfo *preparedstatement.PortalInfo,
	preparedStatementInfo *preparedstatement.PreparedStatementInfo,
) (*query.StatementDescription, error) {
	e.logger.DebugContext(ctx, "describe",
		"user", conn.User(),
		"database", conn.Database(),
		"connection_id", conn.ConnectionID())

	// SHOW multigres.server_version is a gateway-only pseudo-variable with no backing
	// postgres GUC. Answer Describe locally rather than forwarding it, which the
	// backend would reject as an unrecognized configuration parameter. Execute
	// is already served locally via the planner (planVariableShowStmt).
	if stmt := describeAST(portalInfo, preparedStatementInfo); stmt != nil && engine.IsMultigresServerVersionShow(stmt) {
		return engine.MultigresServerVersionShowDescription(), nil
	}

	// TODO: We will need to plan the query to find whether it can
	// be served by a single shard or not. For now, since we only
	// support unsharded, we don't have to do much.
	// We just send the query to the default table group.

	return e.exec.Describe(ctx, e.planner.GetDefaultTableGroup(), constants.DefaultShard, conn, state, portalInfo, preparedStatementInfo)
}

// describeAST returns the parsed statement being described, from whichever of
// the portal or prepared-statement info the caller supplied (exactly one is
// non-nil: portal for Describe('P'), statement for Describe('S')). Returns nil
// when neither carries an AST (e.g. an empty statement).
func describeAST(portalInfo *preparedstatement.PortalInfo, preparedStatementInfo *preparedstatement.PreparedStatementInfo) ast.Stmt {
	switch {
	case portalInfo != nil:
		return portalInfo.AstStmt()
	case preparedStatementInfo != nil:
		return preparedStatementInfo.AstStmt()
	default:
		return nil
	}
}

// EagerParseInTransaction forces a backend Parse for SQL PREPARE / protocol
// Parse inside an explicit transaction. The actual carrier is the existing
// StreamExecute reservation path with force_unnamed_parse set; the multipooler
// runs unnamed Parse after replaying any deferred BEGIN.
func (e *Executor) EagerParseInTransaction(
	ctx context.Context,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	queryStr string,
	paramTypes []uint32,
) error {
	return e.exec.StreamExecute(ctx, conn, DefaultTableGroup, constants.DefaultShard, "", &query.ExecuteSqlPreparedStatement{
		PreparedStatement: &query.PreparedStatement{
			Query:      queryStr,
			ParamTypes: paramTypes,
		},
		ForceUnnamedParse: true,
	}, state, engine.PlanExecInfo{}, false, func(context.Context, *sqltypes.Result) error { return nil })
}

// StreamReplication routes a logical-replication connection to the PRIMARY
// pooler for the default tablegroup/shard and returns the live bidi stream.
// Replication bypasses query planning entirely, so this just forwards to the
// execution backend with the default routing target.
func (e *Executor) StreamReplication(
	ctx context.Context,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	init *multipoolerpb.StreamReplicationInit,
) (multipoolerpb.MultipoolerService_StreamReplicationClient, error) {
	e.logger.DebugContext(ctx, "stream replication",
		"user", conn.User(),
		"database", conn.Database(),
		"connection_id", conn.ConnectionID())

	return e.exec.StreamReplication(ctx, conn, e.planner.GetDefaultTableGroup(), constants.DefaultShard, state, init)
}

// ReleaseAll releases all reserved connections, regardless of reservation reason.
// Delegates to ReleaseAllReservedConnections which calls ReleaseReservedConnection
// on the multipooler for each reserved connection. The multipooler handles
// rollback, COPY abort, and portal release internally.
// Used for connection cleanup when a client disconnects.
func (e *Executor) ReleaseAll(
	ctx context.Context,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
) error {
	return e.exec.ReleaseAllReservedConnections(ctx, conn, state)
}

// Close shuts down the executor, releasing resources such as the plan cache.
func (e *Executor) Close() {
	e.planCache.Close()
}

// Ensure Executor implements handler.Executor interface.
var _ handler.Executor = (*Executor)(nil)
