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
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/services/multigateway/engine"
)

// planDiscardStmt creates a plan for DISCARD statements.
//
// For DISCARD TEMP, uses DiscardTempPrimitive which handles removing the
// temp table reservation reason on the multipooler side.
//
// For DISCARD ALL, uses CloseCursorRoute(CloseAll) so any open
// `DECLARE … WITH HOLD` cursor pins are released alongside PG's
// session-level cleanup. PG documents DISCARD ALL as equivalent to
// `CLOSE ALL; SET SESSION AUTHORIZATION DEFAULT; RESET ALL; DEALLOCATE
// ALL; UNLISTEN *; SELECT pg_advisory_unlock_all(); DISCARD PLANS;
// DISCARD TEMP;` — the cursor-close half is what we mirror here so the
// reserved backend isn't leaked with a stale `ReasonPortal` after the
// session-level cursor wipe.
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
		// CloseCursorRoute snapshots OpenHoldCursorNames at execution
		// time and forwards them as release_portal_names, so any HOLD
		// pin on the reserved backend is drained before/with the
		// DISCARD ALL itself. The actual SQL forwarded to PG is
		// unchanged — PG handles every other DISCARD ALL side effect.
		route := engine.NewCloseAllCursorRoute(p.defaultTableGroup, constants.DefaultShard, sql)
		plan := engine.NewPlan(sql, route)
		plan.Type = engine.PlanTypeCloseCursorRoute
		return plan, nil
	}

	// DISCARD PLANS / DISCARD SEQUENCES — route to PostgreSQL.
	return p.planDefault(sql, stmt, conn)
}
