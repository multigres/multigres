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
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/engine"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// preparedStatementOverride packages rewritten's deparsed SQL as a
// *query.PreparedStatement in place of psi's original body, preserving its
// name and param types — the shape planExecuteStmt sends to the multipooler
// as bodyOverride when the set_config revert applies. Returns nil, matching
// "no override needed", when rewritten is nil.
func preparedStatementOverride(psi *preparedstatement.PreparedStatementInfo, rewritten ast.Stmt) *query.PreparedStatement {
	if rewritten == nil {
		return nil
	}
	return &query.PreparedStatement{
		Name:       psi.Name,
		Query:      rewritten.SqlString(),
		ParamTypes: psi.ParamTypes,
	}
}

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
// EXECUTE is non-cacheable, so this runs fresh with live session state on
// every execution — the pinned/unpinned decision below is therefore safe to
// bake into the plan (unlike the cacheable SELECT set_config path, which
// defers it to a SessionStateBranch at execute time).
func (p *Planner) planExecuteStmt(sql string, stmt *ast.ExecuteStmt, conn *server.Conn, state *handler.MultigatewayConnectionState) (*engine.Plan, error) {
	var execInfo engine.PlanExecInfo
	var setConfigs []engine.SQLPreparedSetConfig
	var bodyOverride *query.PreparedStatement
	if psi := conn.Handler().GetPreparedStatementInfo(conn.ConnectionID(), stmt.Name); psi != nil {
		unsafeConnection := conn != nil && conn.UnsafeConnection()
		analysis, err := analyzeSQLPreparedBody(psi.AstStmt(), unsafeConnection, p.admitsFailoverSlots())
		if err != nil {
			return nil, err
		}
		execInfo = preparedBodyExecInfo(analysis, psi.AstStmt())
		setConfigs = sqlPreparedSetConfigs(analysis.SetConfigs)

		// A prepared body runs VERBATIM on the backend, so a session-persisting
		// set_config(..., false) in it would persist on a pooled backend — and
		// leak to whichever unrelated client checks out that connection next. On
		// an unpinned session, rewrite the body's is_local false→true so the
		// pooled backend reverts it itself; the value still reaches the gateway
		// map via setConfigs above, replayed at the next checkout, mirroring an
		// unpinned SET. A pinned session's reserved backend has no replay path,
		// so the body runs verbatim and genuinely carries the change. A body
		// that reserves its own backend (temp table, advisory lock, ...) counts
		// as pinned too: it must persist on the backend it just pinned.
		//
		// Only this path can do that safely, and only because it can record the
		// value: the wrapped-EXECUTE unwrapper's Route has no session-state
		// channel, so it refuses such a body outright rather than reverting a
		// value it cannot track (see tryUnwrapWrappedExecute).
		pinned := sessionPinned(conn, state, p.defaultTableGroup, constants.DefaultShard) ||
			engine.StatementReservesBackend(execInfo)
		if !pinned {
			if reverted := rewriteSetConfigToRevert(psi.AstStmt()); reverted != nil {
				bodyOverride = preparedStatementOverride(psi, reverted)
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

// preparedBodyExecInfo derives the reserved-connection directives for a
// prepared statement's body from its analysis, mirroring execInfoFromOpts's
// treatment of the equivalent PlanOptions fields for a direct (non-prepared)
// statement. Every caller that builds a plan from a prepared body's
// analysis — planExecuteStmt and the wrapped-EXECUTE unwrapper
// (tryUnwrapWrappedExecute) — goes through this rather than picking
// individual fields by hand: a body creating a temporary logical
// replication slot, acquiring a session advisory lock, or calling
// setseed(...) needs the same pinning a direct statement doing the same
// thing gets, and hand-copying each field at each call site is exactly how
// a future field only ends up wired into one of them.
func preparedBodyExecInfo(analysis *statementAnalysis, bodyStmt ast.Stmt) engine.PlanExecInfo {
	return engine.PlanExecInfo{
		AdvisoryLock:           analysis.AcquiresSessionAdvisoryLock,
		RecheckAdvisoryLocks:   analysis.AcquiresSessionAdvisoryLock || analysis.ReleasesSessionAdvisoryLock,
		TempTable:              preparedBodyCreatesTempTable(bodyStmt),
		LogicalReplicationSlot: analysis.CreatesLogicalReplicationSlot,
		SetSeed:                analysis.CallsSetSeed,
	}
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
