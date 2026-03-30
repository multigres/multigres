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

	// Dispatch to appropriate planner function based on statement type
	// This follows PostgreSQL's utility.c pattern with switch on node tag
	var plan *engine.Plan
	var err error

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
	// Future: Add more statement types here
	// case ast.T_SelectStmt:
	//     plan, err = p.planSelectStmt(sql, stmt.(*ast.SelectStmt), conn)

	default:
		// Default: simple route to PostgreSQL
		plan, err = p.planDefault(sql, conn)
	}

	if err != nil {
		return nil, err
	}

	plan.TablesUsed = ast.ExtractTablesUsed(stmt)
	plan.Type = primitiveName(plan.Primitive)
	return plan, nil
}

// planDefault creates a simple route plan for queries without special handling.
// This is the fallback for most SQL statements.
func (p *Planner) planDefault(sql string, conn *server.Conn) (*engine.Plan, error) {
	route := engine.NewRoute(p.defaultTableGroup, constants.DefaultShard, sql)
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
//
// Currently handles:
//   - Gateway-managed SET/SHOW/RESET (e.g., statement_timeout) — executed locally
//     without a PostgreSQL round-trip.
//   - RESET ALL — must be sent to PostgreSQL AND also reset gateway-managed variables.
//     The returned plan handles both via Sequence[Route, ApplySessionState].
func (p *Planner) PlanPortal(
	portalInfo *preparedstatement.PortalInfo,
	conn *server.Conn,
) (*engine.Plan, error) {
	stmt := portalInfo.PreparedStatementInfo.AstStmt()

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
	return engine.NewPlan(sql, engine.NewRoute(p.defaultTableGroup, constants.DefaultShard, sql)), nil
}
