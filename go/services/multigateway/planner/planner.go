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

// Package planner handles query planning for multigateway.
// It analyzes SQL queries and creates execution plans with appropriate primitives.
package planner

import (
	"log/slog"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/services/multigateway/engine"
)

// Planner is responsible for creating query execution plans.
type Planner struct {
	// defaultTableGroup is the tablegroup to use when routing queries.
	// For Phase 1, all queries are routed to this tablegroup.
	defaultTableGroup string

	logger *slog.Logger

	// txnMetrics is injected into TransactionPrimitive at creation time
	// for recording transaction duration and count.
	txnMetrics *engine.TransactionMetrics
}

// NewPlanner creates a new query planner.
func NewPlanner(defaultTableGroup string, logger *slog.Logger, txnMetrics *engine.TransactionMetrics) *Planner {
	return &Planner{
		defaultTableGroup: defaultTableGroup,
		logger:            logger,
		txnMetrics:        txnMetrics,
	}
}

// PlannerOptions carries per-statement planning signals — derived from
// analyzing the statement (e.g. its expression tree) — that the routing
// builders fold into the plan they produce. Grouping them in a struct keeps
// builder signatures stable as new signals are added; the zero value means "no
// special routing", which is what the non-routing statement handlers pass.
type PlannerOptions struct {
	// PinForAdvisoryLock indicates the statement acquires a session-level
	// advisory lock, so its route must keep the backend pinned for the lock's
	// lifetime (AdvisoryLockRoute rather than a plain Route).
	PinForAdvisoryLock bool

	// RecheckForAdvisoryLock indicates the statement touches session-level
	// advisory locks (an acquire or a release), so the multipooler should
	// re-probe pg_locks afterward and unpin if none remain. It is a superset of
	// PinForAdvisoryLock: every acquire also wants a recheck (to catch a failed
	// pg_try_advisory_lock), and a bare release wants only the recheck.
	RecheckForAdvisoryLock bool
}

// Plan creates an execution plan for the given SQL query and AST.
//
// The planner analyzes the AST to determine query type and creates
// appropriate primitives. Uses PostgreSQL's utility.c dispatch pattern
// with switch on NodeTag for extensibility.
//
// Supported statement types:
// - VariableSetStmt: SET/RESET commands → ApplySessionState
// - Regular queries: Route only
//
// Future phases will add more statement handlers for:
// - TransactionStmt: BEGIN/COMMIT/ROLLBACK
// - SelectStmt: Query optimization and sharding
// - InsertStmt/UpdateStmt/DeleteStmt: Write operations
func (p *Planner) Plan(
	sql string,
	stmt ast.Stmt,
	conn *server.Conn,
) (*engine.Plan, error) {
	p.logger.Debug("planning query",
		"query", sql,
		"user", conn.User(),
		"database", conn.Database(),
		"default_tablegroup", p.defaultTableGroup,
		"statement_type", stmt.NodeTag())

	// Analyze the statement before dispatch: reject unsupported constructs
	// (Tier 2 statement types and blocklisted/misplaced FuncCalls) and gather
	// the planning signals — tracked set_config calls, advisory-lock pinning —
	// that the routing builders need. Running here (not in the executor) means
	// the plan cache short-circuits this whole pass: a cached plan is by
	// construction already analyzed and safe. The normalizer is configured to
	// preserve literals inside set_config calls so its args remain A_Const at
	// this point.
	analysis, err := analyzeStatement(stmt)
	if err != nil {
		return nil, err
	}

	// Handle wrapped EXECUTE forms (EXPLAIN EXECUTE / CREATE TABLE AS EXECUTE)
	// before normal dispatch. The wrapper's inner ExecuteStmt references a
	// gateway-managed prepared statement by user-facing name (e.g. "p"); we
	// rewrite it to the canonical name (e.g. "stmt42") and attach the
	// PreparedStatement metadata so the multipooler can ensurePrepared() on
	// the backend connection before running the query. See execute_unwrap.go.
	if unwrappedPlan, err := p.tryUnwrapWrappedExecute(sql, stmt, conn); err != nil {
		return nil, err
	} else if unwrappedPlan != nil {
		// A wrapped CREATE UNLOGGED TABLE ... AS EXECUTE returns here before the
		// main dispatch, so attach the failover warning on this path too.
		p.maybeWrapUnloggedWarning(sql, stmt, unwrappedPlan)
		unwrappedPlan.TablesUsed = ast.ExtractTablesUsed(stmt)
		unwrappedPlan.Type = primitiveName(unwrappedPlan.Primitive)
		return unwrappedPlan, nil
	}

	// Collect per-statement routing signals derived from the expression
	// analysis. These ride on ordinary queries that may simultaneously do other
	// things the planner tracks (e.g. set_config), so rather than diverting to a
	// dedicated path, the options are threaded into the normal routing builders
	// (planDefault / planSelectStmt), which fold them into whatever plan they
	// would otherwise produce.
	opts := PlannerOptions{
		PinForAdvisoryLock:     analysis.AcquiresSessionAdvisoryLock,
		RecheckForAdvisoryLock: analysis.AcquiresSessionAdvisoryLock || analysis.ReleasesSessionAdvisoryLock,
	}

	// Dispatch to appropriate planner function based on statement type
	// This follows PostgreSQL's utility.c pattern with switch on node tag
	var plan *engine.Plan

	switch stmt.NodeTag() {
	case ast.T_VariableSetStmt:
		plan, err = p.planVariableSetStmt(sql, stmt.(*ast.VariableSetStmt), conn)

	case ast.T_CopyStmt:
		plan, err = p.planCopyStmt(sql, stmt.(*ast.CopyStmt))

	case ast.T_TransactionStmt:
		plan, err = p.planTransactionStmt(sql, stmt.(*ast.TransactionStmt))

	case ast.T_VariableShowStmt:
		plan, err = p.planVariableShowStmt(sql, stmt.(*ast.VariableShowStmt), conn)

	case ast.T_PrepareStmt:
		plan, err = p.planPrepareStmt(sql, stmt.(*ast.PrepareStmt))

	case ast.T_ExecuteStmt:
		plan, err = p.planExecuteStmt(sql, stmt.(*ast.ExecuteStmt))

	case ast.T_DeallocateStmt:
		plan, err = p.planDeallocateStmt(sql, stmt.(*ast.DeallocateStmt))

	case ast.T_ListenStmt:
		return p.planListenStmt(sql, stmt.(*ast.ListenStmt))

	case ast.T_UnlistenStmt:
		return p.planUnlistenStmt(sql, stmt.(*ast.UnlistenStmt))

	case ast.T_NotifyStmt:
		return p.planNotifyStmt(sql)

	case ast.T_DiscardStmt:
		return p.planDiscardStmt(sql, stmt.(*ast.DiscardStmt), conn)

	case ast.T_CreateStmt:
		if cs := stmt.(*ast.CreateStmt); cs.Relation != nil && cs.Relation.RelPersistence == ast.RELPERSISTENCE_TEMP {
			return p.planTempTableCreation(sql, conn)
		}
		plan, err = p.planDefault(sql, stmt, conn, opts)

	case ast.T_CreateTableAsStmt:
		if cs := stmt.(*ast.CreateTableAsStmt); cs.Into != nil && cs.Into.Rel != nil && cs.Into.Rel.RelPersistence == ast.RELPERSISTENCE_TEMP {
			return p.planTempTableCreation(sql, conn)
		}
		plan, err = p.planDefault(sql, stmt, conn, opts)

	case ast.T_CreateSeqStmt:
		// Temp sequences are session-scoped like temp tables: nextval/currval
		// on later statements must land on the same backend connection.
		if cs := stmt.(*ast.CreateSeqStmt); cs.Sequence != nil && cs.Sequence.RelPersistence == ast.RELPERSISTENCE_TEMP {
			return p.planTempTableCreation(sql, conn)
		}
		plan, err = p.planDefault(sql, stmt, conn, opts)

	case ast.T_SelectStmt:
		ss := stmt.(*ast.SelectStmt)
		if ss.IntoClause != nil && ss.IntoClause.Rel != nil && ss.IntoClause.Rel.RelPersistence == ast.RELPERSISTENCE_TEMP {
			return p.planTempTableCreation(sql, conn)
		}
		plan, err = p.planSelectStmt(sql, ss, conn, analysis.SetConfigs, analysis.DynamicSetConfig, opts)

	case ast.T_ViewStmt:
		if vs := stmt.(*ast.ViewStmt); vs.View != nil && vs.View.RelPersistence == ast.RELPERSISTENCE_TEMP {
			return p.planTempTableCreation(sql, conn)
		}
		plan, err = p.planDefault(sql, stmt, conn, opts)

	case ast.T_DeclareCursorStmt:
		dcs := stmt.(*ast.DeclareCursorStmt)
		if dcs.Options&ast.CURSOR_OPT_HOLD != 0 {
			return p.planHoldCursorDeclare(sql, dcs)
		}
		plan, err = p.planDefault(sql, stmt, conn, opts)

	case ast.T_ClosePortalStmt:
		return p.planClosePortalStmt(sql, stmt.(*ast.ClosePortalStmt))

	default:
		// Default: simple route to PostgreSQL
		plan, err = p.planDefault(sql, stmt, conn, opts)
	}

	if err != nil {
		return nil, err
	}

	p.maybeWrapUnloggedWarning(sql, stmt, plan)

	plan.TablesUsed = ast.ExtractTablesUsed(stmt)
	plan.Type = primitiveName(plan.Primitive)

	return plan, nil
}

// maybeWrapUnloggedWarning prepends a WARNING notice to plan when stmt creates an
// UNLOGGED relation. Such statements route normally, but unlogged contents are
// never replicated and are lost on failover, so the warning points the user at the
// failover-behaviour doc. Tables and sequences get distinct messages. The caller
// recomputes plan.Type afterwards.
func (p *Planner) maybeWrapUnloggedWarning(sql string, stmt ast.Stmt, plan *engine.Plan) {
	var warning engine.Primitive
	switch {
	case isUnloggedCreate(stmt):
		warning = engine.NewUnloggedTableWarning(sql)
	case isUnloggedSequenceCreate(stmt):
		warning = engine.NewUnloggedSequenceWarning(sql)
	default:
		return
	}
	plan.Primitive = engine.NewSequence([]engine.Primitive{warning, plan.Primitive})
}

// isUnloggedCreate reports whether stmt creates an UNLOGGED table, across the
// plain CREATE TABLE, CREATE TABLE AS, and SELECT INTO forms.
func isUnloggedCreate(stmt ast.Stmt) bool {
	switch s := stmt.(type) {
	case *ast.CreateStmt:
		return s.Relation != nil && s.Relation.RelPersistence == ast.RELPERSISTENCE_UNLOGGED
	case *ast.CreateTableAsStmt:
		return s.Into != nil && s.Into.Rel != nil && s.Into.Rel.RelPersistence == ast.RELPERSISTENCE_UNLOGGED
	case *ast.SelectStmt:
		return s.IntoClause != nil && s.IntoClause.Rel != nil && s.IntoClause.Rel.RelPersistence == ast.RELPERSISTENCE_UNLOGGED
	}
	return false
}

// isUnloggedSequenceCreate reports whether stmt is CREATE UNLOGGED SEQUENCE.
func isUnloggedSequenceCreate(stmt ast.Stmt) bool {
	s, ok := stmt.(*ast.CreateSeqStmt)
	return ok && s.Sequence != nil && s.Sequence.RelPersistence == ast.RELPERSISTENCE_UNLOGGED
}

// planTempTableCreation creates a plan that routes through a reserved
// connection with ReasonTempTable. The reservation ensures the temp table
// persists across queries on the same session.
func (p *Planner) planTempTableCreation(sql string, conn *server.Conn) (*engine.Plan, error) {
	p.logger.Debug("planning temp table creation", "sql", sql)
	route := engine.NewTempTableRoute(p.defaultTableGroup, constants.DefaultShard, sql)
	return engine.NewPlan(sql, route), nil
}

// planHoldCursorDeclare creates a plan for `DECLARE ... WITH HOLD` cursors.
// WITH HOLD promotes the cursor to session-level state that must survive
// COMMIT; the cursor name is pinned on the reserved backend connection via
// ReasonPortal so the multipooler does not return the backend to the pool
// when the surrounding transaction commits.
func (p *Planner) planHoldCursorDeclare(sql string, stmt *ast.DeclareCursorStmt) (*engine.Plan, error) {
	p.logger.Debug("planning DECLARE WITH HOLD cursor",
		"cursor", stmt.PortalName, "sql", sql)
	route := engine.NewHoldCursorRoute(p.defaultTableGroup, constants.DefaultShard, sql, stmt.PortalName)
	plan := engine.NewPlan(sql, route)
	plan.Type = engine.PlanTypeHoldCursorRoute
	return plan, nil
}

// planClosePortalStmt creates a plan for `CLOSE <name>` / `CLOSE ALL`. The
// CloseCursorRoute forwards the CLOSE to PostgreSQL on the existing reserved
// backend and, when the named cursor was a `WITH HOLD` pin, asks the
// multipooler to drop the corresponding entry from the reserved
// connection's portal set.
func (p *Planner) planClosePortalStmt(sql string, stmt *ast.ClosePortalStmt) (*engine.Plan, error) {
	p.logger.Debug("planning CLOSE cursor", "cursor", stmt.PortalName, "sql", sql)
	var route *engine.CloseCursorRoute
	if stmt.PortalName == "" {
		route = engine.NewCloseAllCursorRoute(p.defaultTableGroup, constants.DefaultShard, sql)
	} else {
		route = engine.NewCloseCursorRoute(p.defaultTableGroup, constants.DefaultShard, sql, stmt.PortalName)
	}
	plan := engine.NewPlan(sql, route)
	plan.Type = engine.PlanTypeCloseCursorRoute
	return plan, nil
}

// planDefault creates a simple route plan for queries without special handling.
// This is the fallback for most SQL statements. opts.PinForAdvisoryLock routes
// the query through an AdvisoryLockRoute so the backend stays pinned for the
// session-level advisory lock's lifetime.
func (p *Planner) planDefault(sql string, stmt ast.Stmt, conn *server.Conn, opts PlannerOptions) (*engine.Plan, error) {
	plan := engine.NewPlan(sql, p.routePrimitive(sql, stmt, opts))

	p.logger.Debug("created default route plan",
		"plan", plan.String(),
		"tablegroup", p.defaultTableGroup)
	return plan, nil
}

// routePrimitive builds the routing primitive for an ordinary query: a plain
// Route, or an AdvisoryLockRoute wrapping it when the statement touches
// session-level advisory locks. Centralizing this lets the set_config Sequence
// path (planSelectStmt) and the bare-route path (planDefault) fold in the
// advisory handling the same way, so a query that both takes an advisory lock
// and tracks a set_config keeps both behaviors.
//
// We wrap on RecheckForAdvisoryLock (the superset): an acquire wants both a pin
// and a recheck, a bare release wants only the recheck. The wrapper carries the
// pin intent separately so a release doesn't reserve a connection.
func (p *Planner) routePrimitive(sql string, stmt ast.Stmt, opts PlannerOptions) engine.Primitive {
	route := engine.NewRoute(p.defaultTableGroup, constants.DefaultShard, sql, stmt)
	// Flag statements that change per-database/role session GUC defaults so the
	// multipooler refreshes pooled connections (which otherwise keep PostgreSQL's
	// session-start snapshot of those defaults). A statement is never both
	// advisory-lock and defaults-changing, so setting it on the base route is
	// correct whether or not it gets wrapped in an AdvisoryLockRoute below.
	// See connection_defaults.go.
	route.InvalidatesConnectionDefaults = statementChangesConnectionDefaults(stmt)
	if opts.RecheckForAdvisoryLock {
		return engine.NewAdvisoryLockRoute(route, opts.PinForAdvisoryLock)
	}
	return route
}

// PlanPortal creates an execution plan for the extended query protocol (portal path).
// Unlike Plan, which handles all statements, PlanPortal only returns a non-nil plan
// for statements that require local handling by the gateway. For all other statements,
// it returns (nil, nil) to indicate they should be sent to PostgreSQL via
// PortalStreamExecute with the portal's bound parameters.
//
// Statements that produce a plan are delegated to Plan to reuse existing planning logic.
// This covers any statement whose semantics cannot be preserved by a plain portal
// execute on a pooled backend connection — for example, gateway-managed session
// variables, LISTEN/UNLISTEN/NOTIFY, DISCARD, temp-table creation, and
// BEGIN/COMMIT/ROLLBACK.
func (p *Planner) PlanPortal(
	portalInfo *preparedstatement.PortalInfo,
	conn *server.Conn,
) (*engine.Plan, error) {
	stmt := portalInfo.PreparedStatementInfo.AstStmt()

	// Non-cacheable extended-protocol statements reach PlanPortal directly;
	// cacheable DML (SELECT/INSERT/UPDATE/DELETE) goes through
	// resolvePortalPlan → Plan instead, which runs the full analysis and builds
	// the routing primitive (including AdvisoryLockRoute). PlanPortal only
	// handles gateway-local statement types, so here we keep just the rejection
	// side of the analysis and discard the planning signals — none of the
	// statement types PlanPortal routes track set_config or acquire advisory
	// locks directly.
	if _, err := analyzeStatement(stmt); err != nil {
		return nil, err
	}

	switch stmt.NodeTag() {
	case ast.T_VariableSetStmt:
		setStmt := stmt.(*ast.VariableSetStmt)
		// Mirror planVariableSetStmt's local-vs-forward decision so the
		// extended-protocol path validates and tracks SET exactly like the
		// simple protocol. Gateway-managed variables and the locally-handled
		// kinds (plain SET, RESET, RESET ALL, SET TO DEFAULT) are planned here;
		// only SET LOCAL and SET TRANSACTION / SET ... FROM CURRENT — which the
		// backend is authoritative for — fall through to a plain portal execute.
		// Without this, a non-gateway SET over the extended protocol forwarded a
		// raw SET to the backend: it mutated backend state outside multipooler's
		// tracking (breaking a later RESET) and never tracked the setting for
		// pool-rotation replay.
		if isGatewayManagedVariable(setStmt.Name) ||
			(!setStmt.IsLocal && setStmt.Kind != ast.VAR_SET_MULTI && setStmt.Kind != ast.VAR_SET_CURRENT) {
			return p.Plan(portalInfo.PreparedStatementInfo.Query, stmt, conn)
		}
		return nil, nil

	case ast.T_VariableShowStmt:
		showStmt := stmt.(*ast.VariableShowStmt)
		if isGatewayManagedVariable(showStmt.Name) {
			return p.Plan(portalInfo.PreparedStatementInfo.Query, stmt, conn)
		}
		return nil, nil

	case ast.T_ListenStmt:
		return p.Plan(portalInfo.PreparedStatementInfo.Query, stmt, conn)

	case ast.T_UnlistenStmt:
		return p.Plan(portalInfo.PreparedStatementInfo.Query, stmt, conn)

	case ast.T_NotifyStmt:
		return p.Plan(portalInfo.PreparedStatementInfo.Query, stmt, conn)

	case ast.T_DiscardStmt:
		// DISCARD TEMP needs the DiscardTempPrimitive for reservation cleanup.
		return p.Plan(portalInfo.PreparedStatementInfo.Query, stmt, conn)

	case ast.T_CreateStmt:
		// UNLOGGED creations route normally but must carry the failover warning,
		// so delegate to Plan (which attaches it) rather than plain portal execute.
		if cs := stmt.(*ast.CreateStmt); isUnloggedCreate(stmt) ||
			(cs.Relation != nil && cs.Relation.RelPersistence == ast.RELPERSISTENCE_TEMP) {
			return p.Plan(portalInfo.PreparedStatementInfo.Query, stmt, conn)
		}
		return nil, nil

	case ast.T_CreateTableAsStmt:
		if cs := stmt.(*ast.CreateTableAsStmt); isUnloggedCreate(stmt) ||
			(cs.Into != nil && cs.Into.Rel != nil && cs.Into.Rel.RelPersistence == ast.RELPERSISTENCE_TEMP) {
			return p.Plan(portalInfo.PreparedStatementInfo.Query, stmt, conn)
		}
		return nil, nil

	case ast.T_SelectStmt:
		if ss := stmt.(*ast.SelectStmt); isUnloggedCreate(stmt) ||
			(ss.IntoClause != nil && ss.IntoClause.Rel != nil && ss.IntoClause.Rel.RelPersistence == ast.RELPERSISTENCE_TEMP) {
			return p.Plan(portalInfo.PreparedStatementInfo.Query, stmt, conn)
		}
		return nil, nil

	case ast.T_ViewStmt:
		if vs := stmt.(*ast.ViewStmt); vs.View != nil && vs.View.RelPersistence == ast.RELPERSISTENCE_TEMP {
			return p.Plan(portalInfo.PreparedStatementInfo.Query, stmt, conn)
		}
		return nil, nil

	case ast.T_CreateSeqStmt:
		// Temp sequences are session-scoped like temp tables: nextval/currval
		// on later statements must land on the same backend connection, so
		// CREATE TEMP SEQUENCE over the extended protocol must reserve too.
		// Unlogged sequences delegate to Plan so the failover warning is attached.
		if cs := stmt.(*ast.CreateSeqStmt); isUnloggedSequenceCreate(stmt) ||
			(cs.Sequence != nil && cs.Sequence.RelPersistence == ast.RELPERSISTENCE_TEMP) {
			return p.Plan(portalInfo.PreparedStatementInfo.Query, stmt, conn)
		}
		return nil, nil

	case ast.T_TransactionStmt:
		// BEGIN/COMMIT/ROLLBACK must run through the gateway's transaction
		// primitive — executing them as a normal portal on a pooled backend
		// connection leaks open (or aborted) transactions across clients when
		// the connection is recycled.
		return p.Plan(portalInfo.PreparedStatementInfo.Query, stmt, conn)

	case ast.T_DeclareCursorStmt:
		// DECLARE … WITH HOLD must go through HoldCursorRoute so the cursor
		// name is pinned on the reserved backend (ReasonPortal). Without
		// this case, an extended-protocol DECLARE WITH HOLD would land on a
		// pooled connection and the cursor would be lost on COMMIT.
		// Non-HOLD DECLARE is delegated through Plan too so the parser-driven
		// dispatch decides — non-HOLD falls through to planDefault there.
		return p.Plan(portalInfo.PreparedStatementInfo.Query, stmt, conn)

	case ast.T_ClosePortalStmt:
		// CLOSE / CLOSE ALL must go through CloseCursorRoute so HOLD-cursor
		// pin bookkeeping on the multipooler stays in sync — otherwise the
		// reserved backend would leak with a stale ReasonPortal.
		return p.Plan(portalInfo.PreparedStatementInfo.Query, stmt, conn)

	case ast.T_AlterDatabaseSetStmt, ast.T_AlterRoleSetStmt, ast.T_CreateExtensionStmt:
		// Statements that may change per-database/role session GUC defaults must
		// route through Plan (planDefault) so the resulting Route carries
		// InvalidatesConnectionDefaults; a bare portal execute would skip the
		// pooled-connection refresh. statementChangesConnectionDefaults filters
		// non-flagged CREATE EXTENSIONs back to a plain forward (the Route's flag
		// is simply false), so this only adds the gateway-local hop, not a
		// behavior change, for those.
		return p.Plan(portalInfo.PreparedStatementInfo.Query, stmt, conn)

	default:
		return nil, nil
	}
}

// SetDefaultTableGroup updates the default tablegroup for routing.
// This allows dynamic configuration changes.
func (p *Planner) SetDefaultTableGroup(tableGroup string) {
	p.defaultTableGroup = tableGroup
	p.logger.Info("default tablegroup updated", "tablegroup", tableGroup)
}

// GetDefaultTableGroup returns the current default tablegroup.
func (p *Planner) GetDefaultTableGroup() string {
	return p.defaultTableGroup
}

// primitiveName returns a short string identifying the primitive type.
// Used for observability (span attributes and query logs).
func primitiveName(p engine.Primitive) string {
	switch p.(type) {
	case *engine.Route:
		return engine.PlanTypeRoute
	case *engine.TempTableRoute:
		return engine.PlanTypeTempTableRoute
	case *engine.AdvisoryLockRoute:
		return engine.PlanTypeAdvisoryLockRoute
	case *engine.HoldCursorRoute:
		return engine.PlanTypeHoldCursorRoute
	case *engine.CloseCursorRoute:
		return engine.PlanTypeCloseCursorRoute
	case *engine.TransactionPrimitive:
		return engine.PlanTypeTransaction
	case *engine.CopyStatement:
		return engine.PlanTypeCopyStatement
	case *engine.ApplySessionState:
		return engine.PlanTypeApplySessionState
	case *engine.ResolveTrackSetConfig:
		return engine.PlanTypeResolveTrackSetConfig
	case *engine.GatewaySessionState:
		return engine.PlanTypeGatewaySessionState
	case *engine.GatewayShowVariable:
		return engine.PlanTypeGatewayShowVariable
	case *engine.ListenNotifyPrimitive:
		return engine.PlanTypeListenNotify
	case *engine.Sequence:
		return engine.PlanTypeSequence
	default:
		return engine.PlanTypeUnknown
	}
}

// planListenStmt creates a ListenNotify primitive for LISTEN.
func (p *Planner) planListenStmt(sql string, stmt *ast.ListenStmt) (*engine.Plan, error) {
	return engine.NewPlan(sql, engine.NewListenPrimitive(stmt.Conditionname, sql)), nil
}

// planUnlistenStmt creates a ListenNotify primitive for UNLISTEN.
func (p *Planner) planUnlistenStmt(sql string, stmt *ast.UnlistenStmt) (*engine.Plan, error) {
	if stmt.Conditionname == "*" || stmt.Conditionname == "" {
		return engine.NewPlan(sql, engine.NewUnlistenAllPrimitive(sql)), nil
	}
	return engine.NewPlan(sql, engine.NewUnlistenPrimitive(stmt.Conditionname, sql)), nil
}

// planNotifyStmt routes NOTIFY to the default table group as a regular query.
func (p *Planner) planNotifyStmt(sql string) (*engine.Plan, error) {
	return engine.NewPlan(sql, engine.NewRoute(p.defaultTableGroup, constants.DefaultShard, sql, nil)), nil
}
