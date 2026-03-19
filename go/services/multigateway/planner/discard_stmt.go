// Copyright 2026 Supabase, Inc.
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

package planner

import (
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/services/multigateway/engine"
)

// planDiscardStmt creates a plan for DISCARD statements.
//
// For DISCARD TEMP and DISCARD ALL, uses DiscardTempPrimitive which handles
// removing the temp table reservation reason on the multipooler side.
// For other DISCARD variants (PLANS, SEQUENCES), falls through to planDefault.
func (p *Planner) planDiscardStmt(
	sql string,
	stmt *ast.DiscardStmt,
	conn *server.Conn,
) (*engine.Plan, error) {
	switch stmt.Target {
	case ast.DISCARD_TEMP, ast.DISCARD_ALL:
		p.logger.Debug("planning discard temp statement",
			"target", stmt.Target.String(),
			"sql", sql)

		primitive := engine.NewDiscardTempPrimitive(sql, p.defaultTableGroup)
		plan := engine.NewPlan(sql, primitive)

		p.logger.Debug("created discard temp plan",
			"plan", plan.String())

		return plan, nil

	default:
		// DISCARD PLANS, DISCARD SEQUENCES — just route to PostgreSQL
		return p.planDefault(sql, conn)
	}
}
