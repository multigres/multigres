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
	"errors"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/engine"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// planPrepareStmt creates a plan for PREPARE name [(types)] AS query.
// Delegates to conn.Handler().HandleParse at execution time.
func (p *Planner) planPrepareStmt(sql string, stmt *ast.PrepareStmt) (*engine.Plan, error) {
	innerQuery := engine.ExtractInnerQuery(stmt)
	if innerQuery == "" {
		return nil, errors.New("PREPARE: inner query is empty")
	}

	paramTypes := engine.ExtractParamTypeOids(stmt)
	prim := engine.NewPreparePrimitive(p.defaultTableGroup, stmt.Name, innerQuery, paramTypes)
	plan := engine.NewPlan(sql, prim)

	p.logger.Debug("created prepare plan", "name", stmt.Name, "inner_query", innerQuery)
	return plan, nil
}

// planExecuteStmt creates a plan for EXECUTE name [(params)].
// The primitive rewrites only the prepared-statement name to its canonical
// gateway-managed name at execution time, preserving argument expressions for
// PostgreSQL to evaluate.
//
// EXECUTE is non-cacheable, so this runs fresh with live session state on every
// execution — the pinned/unpinned decision below is therefore safe to bake into
// the plan (unlike the cacheable SELECT set_config path, which defers it to a
// SessionStateBranch at execute time). A prepared body runs VERBATIM on the
// backend, so a session-persisting set_config(..., false) in it would persist on
// a pooled backend and leak. On an unpinned session we rewrite the body's
// is_local false→true so the pooled backend reverts it (the value lives only in
// the gateway map, replayed at the next checkout — mirroring an unpinned SET);
// on a pinned session the reserved backend has no replay path, so the body runs
// verbatim and genuinely carries the change. Either way the value is tracked.
func (p *Planner) planExecuteStmt(sql string, stmt *ast.ExecuteStmt, conn *server.Conn, state *handler.MultigatewayConnectionState) (*engine.Plan, error) {
	var execInfo engine.PlanExecInfo
	var setConfigs []engine.SQLPreparedSetConfig
	var bodyOverride *query.PreparedStatement
	directConnection := conn != nil && conn.DirectConnection()
	if psi := conn.Handler().GetPreparedStatementInfo(conn.ConnectionID(), stmt.Name); psi != nil {
		analysis, err := analyzeSQLPreparedBody(psi.AstStmt(), directConnection)
		if err != nil {
			return nil, err
		}
		execInfo.AdvisoryLock = analysis.AcquiresSessionAdvisoryLock
		execInfo.RecheckAdvisoryLocks = analysis.AcquiresSessionAdvisoryLock || analysis.ReleasesSessionAdvisoryLock
		execInfo.TempTable = preparedBodyCreatesTempTable(psi.AstStmt())
		setConfigs = sqlPreparedSetConfigs(analysis.SetConfigs)

		// If the session is unpinned and the body carries a persisting ordinary
		// set_config, rewrite it to revert on the pooled backend. A pinned session
		// (or a body with nothing to flip) runs the registered body verbatim. A
		// body that reserves its own backend (temp table, advisory lock, ...)
		// counts as pinned too: it must persist on the backend it just pinned.
		pinned := sessionPinned(conn, state, p.defaultTableGroup, constants.DefaultShard) ||
			engine.StatementReservesBackend(execInfo)
		if !pinned {
			if reverted := rewriteSetConfigToRevert(psi.AstStmt()); reverted != nil {
				bodyOverride = &query.PreparedStatement{
					Name:       psi.Name,
					Query:      reverted.SqlString(),
					ParamTypes: psi.ParamTypes,
				}
			}
		}
	}

	prim := engine.NewExecutePrimitive(p.defaultTableGroup, stmt, setConfigs, bodyOverride)
	plan := engine.NewPlan(sql, prim)
	plan.ExecInfo = execInfo

	paramCount := 0
	if stmt.Params != nil {
		paramCount = stmt.Params.Len()
	}
	p.logger.Debug("created execute plan", "name", stmt.Name, "param_count", paramCount)
	return plan, nil
}

func preparedBodyCreatesTempTable(stmt ast.Stmt) bool {
	ss, ok := stmt.(*ast.SelectStmt)
	if !ok {
		return false
	}
	into := ss.LeafIntoClause()
	return into != nil && into.Rel != nil && into.Rel.RelPersistence == ast.RELPERSISTENCE_TEMP
}

func sqlPreparedSetConfigs(setConfigs []setConfigCall) []engine.SQLPreparedSetConfig {
	if len(setConfigs) == 0 {
		return nil
	}
	out := make([]engine.SQLPreparedSetConfig, 0, len(setConfigs))
	for _, sc := range setConfigs {
		out = append(out, engine.SQLPreparedSetConfig{
			Name:               sc.Name,
			Value:              sc.Value,
			ValueParam:         sc.ValueBind,
			IsLocalLiteralTrue: sc.IsLocalLiteralTrue,
			// Must be carried across: a dropped ValueIsNull leaves Value ""
			// looking like an explicit empty-string assignment, so the prepared
			// form would silently diverge from the identical direct statement
			// (the divergence validateSQLPreparedSetConfigs exists to prevent).
			ValueIsNull: sc.ValueIsNull,
		})
	}
	return out
}

// planDeallocateStmt creates a plan for DEALLOCATE name or DEALLOCATE ALL.
// Named DEALLOCATE delegates to conn.Handler().HandleClose.
// DEALLOCATE ALL uses the consolidator directly (no extended protocol equivalent).
func (p *Planner) planDeallocateStmt(sql string, stmt *ast.DeallocateStmt) (*engine.Plan, error) {
	var prim engine.Primitive
	if stmt.IsAll {
		prim = engine.NewDeallocateAllPrimitive(p.defaultTableGroup)
	} else {
		prim = engine.NewDeallocatePrimitive(p.defaultTableGroup, stmt.Name)
	}
	plan := engine.NewPlan(sql, prim)

	p.logger.Debug("created deallocate plan", "name", stmt.Name, "is_all", stmt.IsAll)
	return plan, nil
}
