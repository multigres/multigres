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
//
// For DISCARD ALL, uses DiscardAllPrimitive, which resets the gateway's
// session state and releases any reserved connection back to the pool
// without forwarding the statement to a shared pooled backend (see that
// primitive for the rationale).
//
// DISCARD PLANS / DISCARD SEQUENCES carry no cursor or temp-table side
// effects and fall through to planDefault.
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

	if stmt.Target == ast.DISCARD_ALL {
		p.logger.Debug("planning discard all statement", "sql", sql)
		// DISCARD ALL is handled entirely at the gateway and is NOT
		// forwarded to a pooled backend. In the pooled model the
		// session state DISCARD ALL resets lives at the gateway
		// (prepared statements, session GUCs, LISTEN subscriptions);
		// the only backend-resident state is on a reserved connection,
		// which DiscardAllPrimitive releases back to the pool. Forwarding
		// the literal DISCARD ALL would run DEALLOCATE ALL on a shared
		// backend and desync the multipooler's prepared-statement
		// tracking. See DiscardAllPrimitive for the full rationale.
		primitive := engine.NewDiscardAllPrimitive(sql)
		plan := engine.NewPlan(sql, primitive)
		plan.Type = engine.PlanTypeDiscardAll
		return plan, nil
	}

	// DISCARD PLANS / DISCARD SEQUENCES — route to PostgreSQL.
	return p.planDefault(sql, stmt, conn, PlanOptions{})
}
