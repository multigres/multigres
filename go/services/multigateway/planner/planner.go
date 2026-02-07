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
}

// NewPlanner creates a new query planner.
func NewPlanner(defaultTableGroup string, logger *slog.Logger) *Planner {
	return &Planner{
		defaultTableGroup: defaultTableGroup,
		logger:            logger,
	}
}

// Plan creates an execution plan for the given SQL query and AST.
//
// The planner analyzes the AST to determine query type and creates
// appropriate primitives. Uses PostgreSQL's utility.c dispatch pattern
// with switch on NodeTag for extensibility.
//
// Supported statement types:
// - VariableSetStmt: SET/RESET commands → Sequence[Route, ApplySessionState]
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
	switch stmt.NodeTag() {
	case ast.T_VariableSetStmt:
		return p.planVariableSetStmt(sql, stmt.(*ast.VariableSetStmt), conn)

	case ast.T_CopyStmt:
		return p.planCopyStmt(sql, stmt.(*ast.CopyStmt))

	case ast.T_TransactionStmt:
		return p.planTransactionStmt(sql, stmt.(*ast.TransactionStmt))

	// Future: Add more statement types here
	// case ast.T_SelectStmt:
	//     return p.planSelectStmt(sql, stmt.(*ast.SelectStmt), conn)

	default:
		// Default: simple route to PostgreSQL
		return p.planDefault(sql, conn)
	}
}

// planDefault creates a simple route plan for queries without special handling.
// This is the fallback for most SQL statements.
func (p *Planner) planDefault(sql string, conn *server.Conn) (*engine.Plan, error) {
	route := engine.NewRoute(p.defaultTableGroup, "", sql)
	plan := engine.NewPlan(sql, route)

	p.logger.Debug("created default route plan",
		"plan", plan.String(),
		"tablegroup", p.defaultTableGroup)
	return plan, nil
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
