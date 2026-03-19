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
// For DISCARD TEMP, uses DiscardTempPrimitive which handles removing the
// temp table reservation reason on the multipooler side.
// DISCARD ALL does NOT drop temp tables in PostgreSQL, so it routes normally.
// Other DISCARD variants (PLANS, SEQUENCES) also fall through to planDefault.
func (p *Planner) planDiscardStmt(
	sql string,
	stmt *ast.DiscardStmt,
	conn *server.Conn,
) (*engine.Plan, error) {
	if stmt.Target == ast.DISCARD_TEMP {
		p.logger.Debug("planning discard temp statement", "sql", sql)

		primitive := engine.NewDiscardTempPrimitive(sql, p.defaultTableGroup)
		plan := engine.NewPlan(sql, primitive)

		p.logger.Debug("created discard temp plan", "plan", plan.String())
		return plan, nil
	}

	// DISCARD ALL, DISCARD PLANS, DISCARD SEQUENCES — route to PostgreSQL.
	return p.planDefault(sql, conn)
}
