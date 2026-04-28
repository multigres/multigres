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

	// Reject unsupported constructs before dispatch: Tier 2 statement types
	// (LOAD, ALTER SYSTEM, CREATE/DROP DATABASE, etc.) plus any blocklisted
	// or misplaced FuncCalls in expression trees. Running here (not in the
	// executor) means the plan cache short-circuits both checks: a cached
	// plan is by construction safe. The normalizer is configured to
	// preserve literals inside set_config calls so its args remain A_Const
	// at this point.
	exprResult, err := planUnsupportedConstructs(stmt)
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
		unwrappedPlan.TablesUsed = ast.ExtractTablesUsed(stmt)
		unwrappedPlan.Type = primitiveName(unwrappedPlan.Primitive)
		return unwrappedPlan, nil
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
		plan, err = p.planDefault(sql, stmt, conn)

	case ast.T_CreateTableAsStmt:
		if cs := stmt.(*ast.CreateTableAsStmt); cs.Into != nil && cs.Into.Rel != nil && cs.Into.Rel.RelPersistence == ast.RELPERSISTENCE_TEMP {
			return p.planTempTableCreation(sql, conn)
		}
		plan, err = p.planDefault(sql, stmt, conn)

	case ast.T_SelectStmt:
		ss := stmt.(*ast.SelectStmt)
		if ss.IntoClause != nil && ss.IntoClause.Rel != nil && ss.IntoClause.Rel.RelPersistence == ast.RELPERSISTENCE_TEMP {
			return p.planTempTableCreation(sql, conn)
		}
		plan, err = p.planSelectStmt(sql, ss, conn, exprResult.SetConfigs)

	case ast.T_ViewStmt:
		if vs := stmt.(*ast.ViewStmt); vs.View != nil && vs.View.RelPersistence == ast.RELPERSISTENCE_TEMP {
			return p.planTempTableCreation(sql, conn)
		}
		plan, err = p.planDefault(sql, stmt, conn)

	default:
		// Default: simple route to PostgreSQL
		plan, err = p.planDefault(sql, stmt, conn)
	}

	if err != nil {
		return nil, err
	}

	plan.TablesUsed = ast.ExtractTablesUsed(stmt)
	plan.Type = primitiveName(plan.Primitive)

	return plan, nil
}

// planTempTableCreation creates a plan that routes through a reserved
// connection with ReasonTempTable. The reservation ensures the temp table
// persists across queries on the same session.
func (p *Planner) planTempTableCreation(sql string, conn *server.Conn) (*engine.Plan, error) {
	p.logger.Debug("planning temp table creation", "sql", sql)
	route := engine.NewTempTableRoute(p.defaultTableGroup, constants.DefaultShard, sql)
	return engine.NewPlan(sql, route), nil
}

// planDefault creates a simple route plan for queries without special handling.
// This is the fallback for most SQL statements.
func (p *Planner) planDefault(sql string, stmt ast.Stmt, conn *server.Conn) (*engine.Plan, error) {
	route := engine.NewRoute(p.defaultTableGroup, constants.DefaultShard, sql, stmt)
	plan := engine.NewPlan(sql, route)

	p.logger.Debug("created default route plan",
		"plan", plan.String(),
		"tablegroup", p.defaultTableGroup)
	return plan, nil
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

	// Non-cacheable extended-protocol statements reach PlanPortal directly
	// (cacheable ones go through resolvePortalPlan → Plan, which does the
	// same checks), so both paths must share the same pre-dispatch rejection.
	// We throw away the set_config result here: PlanPortal only routes
	// gateway-local statement types, none of which are SELECTs that could
	// carry tracked set_configs.
	if _, err := planUnsupportedConstructs(stmt); err != nil {
		return nil, err
	}

	switch stmt.NodeTag() {
	case ast.T_VariableSetStmt:
		setStmt := stmt.(*ast.VariableSetStmt)
		if isGatewayManagedVariable(setStmt.Name) || setStmt.Kind == ast.VAR_RESET_ALL {
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
		if cs := stmt.(*ast.CreateStmt); cs.Relation != nil && cs.Relation.RelPersistence == ast.RELPERSISTENCE_TEMP {
			return p.Plan(portalInfo.PreparedStatementInfo.Query, stmt, conn)
		}
		return nil, nil

	case ast.T_CreateTableAsStmt:
		if cs := stmt.(*ast.CreateTableAsStmt); cs.Into != nil && cs.Into.Rel != nil && cs.Into.Rel.RelPersistence == ast.RELPERSISTENCE_TEMP {
			return p.Plan(portalInfo.PreparedStatementInfo.Query, stmt, conn)
		}
		return nil, nil

	case ast.T_SelectStmt:
		if ss := stmt.(*ast.SelectStmt); ss.IntoClause != nil && ss.IntoClause.Rel != nil && ss.IntoClause.Rel.RelPersistence == ast.RELPERSISTENCE_TEMP {
			return p.Plan(portalInfo.PreparedStatementInfo.Query, stmt, conn)
		}
		return nil, nil

	case ast.T_ViewStmt:
		if vs := stmt.(*ast.ViewStmt); vs.View != nil && vs.View.RelPersistence == ast.RELPERSISTENCE_TEMP {
			return p.Plan(portalInfo.PreparedStatementInfo.Query, stmt, conn)
		}
		return nil, nil

	case ast.T_TransactionStmt:
		// BEGIN/COMMIT/ROLLBACK must run through the gateway's transaction
		// primitive — executing them as a normal portal on a pooled backend
		// connection leaks open (or aborted) transactions across clients when
		// the connection is recycled.
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
	case *engine.TransactionPrimitive:
		return engine.PlanTypeTransaction
	case *engine.CopyStatement:
		return engine.PlanTypeCopyStatement
	case *engine.ApplySessionState:
		return engine.PlanTypeApplySessionState
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
