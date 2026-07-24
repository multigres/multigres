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
	"strings"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
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

// PlanOptions carries planning inputs for a single Plan call.
//
// IsPortal is supplied by the caller to select the protocol; the remaining
// fields are per-statement routing signals that Plan derives from analyzing the
// statement (e.g. its expression tree) and threads into the routing builders,
// which fold them into the plan they produce. Grouping them keeps builder
// signatures stable as new signals are added; the zero value means "simple
// protocol, no special routing".
type PlanOptions struct {
	// IsPortal selects the extended (portal) query protocol. The single
	// behavioral difference today is that the wrapped-EXECUTE unwrap
	// (EXPLAIN EXECUTE / CREATE TABLE AS EXECUTE) is simple-protocol only: the
	// rewritten Route cannot carry its prepared-statement metadata through the
	// portal path, so in portal mode such statements route normally instead.
	//
	// INVARIANT: IsPortal must never change the plan produced for a cacheable
	// statement. The plan cache is shared across both protocols (keyed only by
	// database + normalized SQL), so a plan cached by one protocol may be served
	// to the other. Any IsPortal-conditional planning must therefore stay on a
	// non-cacheable path — as the unwrap does, since it only matches the
	// (non-cacheable) EXPLAIN EXECUTE / CREATE TABLE AS EXECUTE shapes. Protocol
	// differences for cacheable plans belong in PortalStreamExecute vs
	// StreamExecute, not in plan content.
	IsPortal bool

	// PinForAdvisoryLock indicates the statement acquires a session-level
	// advisory lock, so its route must keep the backend pinned for the lock's
	// lifetime (AdvisoryLockRoute rather than a plain Route). Derived by Plan.
	PinForAdvisoryLock bool

	// RecheckForAdvisoryLock indicates the statement touches session-level
	// advisory locks (an acquire or a release), so the multipooler should
	// re-probe pg_locks afterward and unpin if none remain. It is a superset of
	// PinForAdvisoryLock: every acquire also wants a recheck (to catch a failed
	// pg_try_advisory_lock), and a bare release wants only the recheck. Derived
	// by Plan.
	RecheckForAdvisoryLock bool

	// RewriteCurrentSetting indicates the statement has a gateway-managed
	// current_setting call that must be rewritten to return the gateway value.
	// Derived by Plan from analysis.NeedsCurrentSettingRewrite so the routing
	// builders can gate the rewrite without re-walking the tree.
	RewriteCurrentSetting bool
}

// Plan creates an execution plan for the given SQL query and AST.
//
// The planner analyzes the AST to determine query type and creates
// appropriate primitives. Uses PostgreSQL's utility.c dispatch pattern
// with switch on NodeTag for extensibility.
//
// Plan serves both query protocols. The simple protocol calls it with
// opts.IsPortal == false; the extended (portal) protocol calls it with
// opts.IsPortal == true. Both share the same dispatch and the same plan cache —
// the resulting plan's PortalStreamExecute / StreamExecute methods own the
// protocol-specific execution, so a Route, a gateway-local primitive, or a
// Sequence all behave correctly on either path. See PlanOptions.IsPortal for the
// one planning-time difference.
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
	opts PlanOptions,
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
	// attach a SQL prefix/suffix template plus PreparedStatement metadata so the
	// multipooler can resolve a pooler-consolidated backend name before running
	// the query. See execute_unwrap.go.
	//
	// Simple protocol only: the rewrite produces a Route carrying the SQL EXECUTE
	// template, but Route.PortalStreamExecute forwards the portal as-is and
	// ignores that metadata, so the unwrap is a no-op (or worse, an error if the
	// statement is missing) over the extended protocol. In portal mode these
	// wrapped forms fall through to normal dispatch and route like any query.
	if !opts.IsPortal {
		if unwrappedPlan, err := p.tryUnwrapWrappedExecute(sql, stmt, conn); err != nil {
			return nil, err
		} else if unwrappedPlan != nil {
			// A wrapped CREATE UNLOGGED TABLE ... AS EXECUTE returns here before the
			// main dispatch, so attach the failover warning on this path too.
			p.maybeWrapStatementWarning(sql, stmt, unwrappedPlan)
			unwrappedPlan.TablesUsed = ast.ExtractTablesUsed(stmt)
			unwrappedPlan.Type = planType(unwrappedPlan.Primitive, unwrappedPlan.ExecInfo)
			return unwrappedPlan, nil
		}
	}

	// Fold the per-statement routing signals derived from the expression
	// analysis onto opts. These ride on ordinary queries that may simultaneously
	// do other things the planner tracks (e.g. set_config), so rather than
	// diverting to a dedicated path, the options are threaded into the normal
	// routing builders (planDefault / planSelectStmt), which fold them into
	// whatever plan they would otherwise produce.
	opts.PinForAdvisoryLock = analysis.AcquiresSessionAdvisoryLock
	opts.RecheckForAdvisoryLock = analysis.AcquiresSessionAdvisoryLock || analysis.ReleasesSessionAdvisoryLock
	opts.RewriteCurrentSetting = analysis.NeedsCurrentSettingRewrite

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
		plan, err = p.planExecuteStmt(sql, stmt.(*ast.ExecuteStmt), conn)

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

	p.maybeWrapStatementWarning(sql, stmt, plan)

	plan.TablesUsed = ast.ExtractTablesUsed(stmt)
	plan.Type = planType(plan.Primitive, plan.ExecInfo)

	return plan, nil
}

// maybeWrapStatementWarning prepends a WARNING notice to plan when stmt has
// pooling/replication semantics the user should know about at CREATE time:
// UNLOGGED relations (contents lost on failover) and ON login event triggers
// (fire per pooled backend, not per client session). Such statements route
// normally; the warning points the user at the relevant doc. The caller
// recomputes plan.Type afterwards.
func (p *Planner) maybeWrapStatementWarning(sql string, stmt ast.Stmt, plan *engine.Plan) {
	var warning engine.Primitive
	switch {
	case isUnloggedCreate(stmt):
		warning = engine.NewUnloggedTableWarning(sql)
	case isUnloggedSequenceCreate(stmt):
		warning = engine.NewUnloggedSequenceWarning(sql)
	case isLoginEventTriggerCreate(stmt):
		warning = engine.NewLoginEventTriggerWarning(sql)
	default:
		return
	}
	plan.Primitive = engine.NewSequence([]engine.Primitive{warning, plan.Primitive})
}

// isLoginEventTriggerCreate reports whether stmt is CREATE EVENT TRIGGER ... ON
// login. The parser lowercases the unquoted event name; EqualFold also covers
// the quoted "LOGIN" spelling.
func isLoginEventTriggerCreate(stmt ast.Stmt) bool {
	s, ok := stmt.(*ast.CreateEventTrigStmt)
	return ok && strings.EqualFold(s.EventName, "login")
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
	plan := engine.NewPlan(sql, engine.NewRoute(p.defaultTableGroup, constants.DefaultShard, sql, nil))
	// The temp-table reservation is a static property of the plan: it routes as
	// a plain Route, but ExecInfo.TempTable tells the executor to reserve a
	// connection with ReasonTempTable.
	plan.ExecInfo.TempTable = true
	plan.Type = engine.PlanTypeTempTableRoute
	return plan, nil
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
	// The cursor name to pin is known at plan time, so it rides on the cached
	// plan's ExecInfo; HoldCursorRoute forwards it to the reservation.
	plan.ExecInfo.PinPortals = []string{stmt.PortalName}
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
// This is the fallback for most SQL statements. Advisory-lock pinning, when the
// statement touches a session-level advisory lock, rides on the plan's ExecInfo
// (see advisoryExecInfo) rather than a dedicated routing primitive.
func (p *Planner) planDefault(sql string, stmt ast.Stmt, conn *server.Conn, opts PlanOptions) (*engine.Plan, error) {
	prim, err := p.routePrimitive(sql, stmt, opts)
	if err != nil {
		return nil, err
	}
	plan := engine.NewPlan(sql, prim)
	plan.ExecInfo = advisoryExecInfo(opts)

	p.logger.Debug("created default route plan",
		"plan", plan.String(),
		"tablegroup", p.defaultTableGroup)
	return plan, nil
}

// routePrimitive builds the leading Route for a query. When analysis flagged a
// gateway-managed current_setting (opts.RewriteCurrentSetting), the call is
// rewritten out so it returns the gateway-owned value and the Route is wrapped in
// a GatewayManagedValueRoute that fills the synthetic value slots from gateway
// state at execute time (see rewriteGatewayManagedCurrentSetting); otherwise it is
// a plain Route over the original statement. The flag is set only when a rewrite is
// actually required, so the common case never walks the tree here.
func (p *Planner) routePrimitive(sql string, stmt ast.Stmt, opts PlanOptions) (engine.Primitive, error) {
	if opts.RewriteCurrentSetting {
		rewritten, reads, err := rewriteGatewayManagedCurrentSetting(stmt)
		if err != nil {
			return nil, err
		}
		if rewritten != nil {
			route := engine.NewRoute(p.defaultTableGroup, constants.DefaultShard, rewritten.SqlString(), rewritten)
			return engine.NewGatewayManagedValueRoute(route, nil, reads), nil
		}
	}
	return engine.NewRoute(p.defaultTableGroup, constants.DefaultShard, sql, stmt), nil
}

// advisoryExecInfo derives the plan-level reservation directives for a statement
// that touches session-level advisory locks. We track on RecheckForAdvisoryLock
// (the superset): an acquire wants both a pin and a recheck, a bare release
// wants only the recheck — so the pin is carried separately and a release does
// not reserve a connection. The zero value (no advisory) is the common case.
func advisoryExecInfo(opts PlanOptions) engine.PlanExecInfo {
	return engine.PlanExecInfo{
		AdvisoryLock:         opts.PinForAdvisoryLock,
		RecheckAdvisoryLocks: opts.RecheckForAdvisoryLock,
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

// planType returns the observability label for a plan. Temp-table and
// advisory-lock routing no longer have dedicated primitive types — they are a
// plain Route plus ExecInfo — so the label for a Route is refined from the
// plan's ExecInfo to preserve the previous TempTableRoute / AdvisoryLockRoute
// labels in spans and query logs.
func planType(p engine.Primitive, info engine.PlanExecInfo) string {
	if _, ok := p.(*engine.Route); ok {
		switch {
		case info.TempTable:
			return engine.PlanTypeTempTableRoute
		case info.AdvisoryLock || info.RecheckAdvisoryLocks:
			return engine.PlanTypeAdvisoryLockRoute
		}
	}
	return primitiveName(p)
}

// primitiveName returns a short string identifying the primitive type.
// Used for observability (span attributes and query logs).
func primitiveName(p engine.Primitive) string {
	switch p.(type) {
	case *engine.Route:
		return engine.PlanTypeRoute
	case *engine.HoldCursorRoute:
		return engine.PlanTypeHoldCursorRoute
	case *engine.CloseCursorRoute:
		return engine.PlanTypeCloseCursorRoute
	case *engine.TransactionPrimitive:
		return engine.PlanTypeTransaction
	case *engine.CopyStatement:
		return engine.PlanTypeCopyStatement
	case *engine.ApplySessionState, *engine.ReservationAware:
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
