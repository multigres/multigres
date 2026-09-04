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
	"sync"
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

	// slotBasedReplicationMu guards slotBasedReplicationEnabled and
	// lastSlotBasedReplicationEnabled together: invalidateOnSlotBasedReplicationTransition
	// needs the check, the flip, and the cache invalidation to happen as one
	// unit, so a concurrent request that finds the flag already up to date
	// is guaranteed the invalidation a sibling request made for that same
	// transition has already finished (see that method).
	slotBasedReplicationMu          sync.Mutex
	slotBasedReplicationEnabled     func() bool
	lastSlotBasedReplicationEnabled bool
}

// SetSlotBasedReplicationEnabled wires the dynamic getter that gates
// admitting a non-temporary logical failover replication slot created via
// plain SQL. Must be called before connections are accepted. A nil getter
// (the default) keeps the feature off, rejecting every non-temporary slot as
// before. See planner.Planner.SetSlotBasedReplicationEnabled.
//
// A plan admitted under this (or any other dynamic, plan-affecting) flag
// stays cached and gets served on later hits without re-running analysis.
// Multigateway's config-reload handler (CobraPreRunE/Init) calls
// InvalidatePlanCache asynchronously whenever the live config changes, which
// is the backstop for "nothing queries this for a while" — but the getter is
// also kept here so invalidateOnSlotBasedReplicationTransition can check it
// synchronously on every cacheable request, closing the gap between "the
// flag changed" and "the reload handler got scheduled" far tighter than the
// backstop alone would for a request already in flight.
func (e *Executor) SetSlotBasedReplicationEnabled(enabled func() bool) {
	e.slotBasedReplicationMu.Lock()
	e.slotBasedReplicationEnabled = enabled
	if enabled != nil {
		e.lastSlotBasedReplicationEnabled = enabled()
	}
	e.slotBasedReplicationMu.Unlock()
	e.planner.SetSlotBasedReplicationEnabled(enabled)
}

// invalidateOnSlotBasedReplicationTransition invalidates the plan cache the
// moment a request observes enable-slot-based-replication has changed since
// the last request observed it. resolvePlan/resolvePortalPlan call this
// before every cache lookup: a mutex lock is negligible next to the rest of
// planning, and checking here — not only inside Plan(), which only runs on a
// cache miss — means a cache hit can never serve a plan admitted under a
// since-flipped flag, no matter how long the asynchronous reload handler
// takes to get scheduled.
//
// The check, the flip, and the invalidation share one critical section on
// purpose: if they didn't, a request could observe the flag already flipped
// by a concurrent request (so skip invalidating itself, correctly assuming
// that sibling request handles it) and still race ahead to a cache lookup
// before that sibling's Invalidate call actually runs, serving a plan
// admitted under the stale value. Serializing the whole sequence means any
// request that finds the flag already up to date is guaranteed the
// invalidation for that transition has already completed.
//
// This still leaves one irreducible gap: the getter reads viper's live,
// externally-mutated config value, so a flip landing in the instant between
// this read and the subsequent planCache.Get could let that one request
// serve a plan from just before the flip. Closing that to zero would mean
// the config watcher itself synchronously invalidating the moment it
// observes a change, rather than today's buffered-notification handoff to a
// consumer goroutine — a change to the general dynamic-config mechanism,
// not something this function can do on its own. In practice the gap
// self-heals immediately: the very next request re-reads the getter fresh
// and observes the transition, since nothing here depends on the reload
// notification actually having arrived yet.
func (e *Executor) invalidateOnSlotBasedReplicationTransition() {
	e.slotBasedReplicationMu.Lock()
	defer e.slotBasedReplicationMu.Unlock()
	if e.slotBasedReplicationEnabled == nil {
		return
	}
	current := e.slotBasedReplicationEnabled()
	if e.lastSlotBasedReplicationEnabled != current {
		e.lastSlotBasedReplicationEnabled = current
		e.planCache.Invalidate()
	}
}

// InvalidatePlanCache discards every cached plan, so the next request for
// any statement is re-planned from scratch. Call this whenever a dynamic
// flag that affects planning decisions (e.g. enable-slot-based-replication)
// changes value — see SetSlotBasedReplicationEnabled.
func (e *Executor) InvalidatePlanCache() {
	e.planCache.Invalidate()
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
	plan, bindVars, cacheHit, normalizedSQL, fingerprint, err := e.resolvePlan(ctx, queryStr, astStmt, conn, state)
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
	state *handler.MultigatewayConnectionState,
) (*engine.Plan, []*ast.A_Const, bool, string, string, error) {
	// A unsafe connection is kept off the shared plan cache: its
	// planning depends on the per-connection opt-out (accept/reject decisions and
	// the ReasonUnsafeConnection pin), so a plan built for it must never be served to
	// another connection. Route it through the non-cacheable path with State, like
	// any other statement whose plan depends on live connection state.
	if !isCacheable(astStmt) || conn.UnsafeConnection() {
		plan, err := e.planner.Plan(queryStr, astStmt, conn, planner.PlanOptions{State: state})
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

	// See invalidateOnSlotBasedReplicationTransition: checked here, right
	// before the lookup, so a cache hit can never serve a plan admitted
	// under a since-flipped dynamic flag.
	e.invalidateOnSlotBasedReplicationTransition()

	// Cache hit
	if cachedPlan, ok := e.planCache.Get(ctx, cacheKey); ok {
		e.logger.DebugContext(ctx, "plan cache hit",
			"normalized_query", normalizedSQL)
		return cachedPlan, bindVars, true, normalizedSQL, fingerprint, nil
	}

	// Cache miss — plan with normalized SQL/AST and cache the result.
	//
	// The epoch is captured before planning, not read fresh at Put: planning
	// may read live, mutable state (e.g. a dynamic feature flag such as
	// enable-slot-based-replication) that Invalidate() is the designated
	// response to changing. If a reload bumps the epoch while this plan
	// (built under the pre-reload state) is still in flight, stamping with
	// the captured epoch — now behind the current one — means the entry is
	// immediately stale on the next Get, instead of Put silently caching a
	// decision made under a policy that no longer holds.
	epochAtPlan := e.planCache.Epoch()
	plan, err := e.planner.Plan(normalizedSQL, normResult.NormalizedAST, conn, planner.PlanOptions{})
	if err != nil {
		return nil, nil, false, normalizedSQL, fingerprint, err
	}

	e.planCache.PutAtEpoch(cacheKey, plan, epochAtPlan)
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
		if ss, ok := stmt.(*ast.SelectStmt); ok && ss.LeafIntoClause() != nil {
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
	plan, cacheHit, normalizedSQL, fingerprint, err := e.resolvePortalPlan(ctx, portalInfo, conn, state)
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
	state *handler.MultigatewayConnectionState,
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
	//
	// A unsafe connection is also forced down this path (mirroring resolvePlan):
	// its plan is built with the unsafe-statement rejections suppressed, so it
	// must never enter the shared cache where a normal connection could receive it
	// as a database-wide hit — that would bypass analyzeStatement's function
	// blocklist (e.g. a cached SELECT dblink(...) served to another client).
	if !isCacheable(astStmt) || conn.UnsafeConnection() {
		plan, err := e.planner.Plan(portalInfo.PreparedStatementInfo.Query, astStmt, conn, planner.PlanOptions{IsPortal: true, State: state})
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

	// See invalidateOnSlotBasedReplicationTransition: checked here, right
	// before the lookup, so a cache hit can never serve a plan admitted
	// under a since-flipped dynamic flag.
	e.invalidateOnSlotBasedReplicationTransition()

	if cachedPlan, ok := e.planCache.Get(ctx, cacheKey); ok {
		e.logger.DebugContext(ctx, "portal plan cache hit", "query", normalizedSQL)
		return cachedPlan, true, normalizedSQL, fingerprint, nil
	}

	// Cacheable DML is planned protocol-agnostically (zero-value PlanOptions, same
	// as the simple path) so the cached entry is shared safely across protocols.
	// Epoch captured before planning — see the matching comment in resolvePlan.
	epochAtPlan := e.planCache.Epoch()
	plan, err := e.planner.Plan(normalizedSQL, astStmt, conn, planner.PlanOptions{})
	if err != nil {
		return nil, false, normalizedSQL, fingerprint, err
	}

	e.planCache.PutAtEpoch(cacheKey, plan, epochAtPlan)
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

// ReleaseAll releases all reserved connections, regardless of reservation
// reason, including sticky ones (see protoutil.ReasonSetSeed) — this is a
// real client disconnect, so nothing about the connection's session is worth
// preserving. Delegates to ReleaseAllReservedConnections which calls
// ReleaseReservedConnection on the multipooler for each reserved connection.
// The multipooler handles rollback, COPY abort, and portal release internally.
// Used for connection cleanup when a client disconnects.
func (e *Executor) ReleaseAll(
	ctx context.Context,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
) error {
	return e.exec.ReleaseAllReservedConnections(ctx, conn, state, false)
}

// Close shuts down the executor, releasing resources such as the plan cache.
func (e *Executor) Close() {
	e.planCache.Close()
}

// Ensure Executor implements handler.Executor interface.
var _ handler.Executor = (*Executor)(nil)
