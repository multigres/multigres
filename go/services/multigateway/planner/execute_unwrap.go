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
	"strings"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multigateway/engine"
)

// tryUnwrapWrappedExecute detects statements of the form `EXPLAIN EXECUTE p`
// or `CREATE [TEMP] TABLE t AS EXECUTE p` and turns them into a plan that runs
// the substituted prepared body as an ordinary query.
//
// Background: multigateway stores SQL-level `PREPARE p AS ...` only in the
// gateway's consolidator keyed by the user name `p`. The backend session never
// sees a statement named `p`. Wrappers like `EXPLAIN EXECUTE p` or
// `CREATE TABLE ... AS EXECUTE p` would otherwise fall through to planDefault →
// raw StreamExecute, and the backend would reject them with `prepared statement
// "p" does not exist`.
//
// The fix: substitute the EXECUTE arguments into the prepared body (cast to the
// resolved parameter types) and splice the result in place of the ExecuteStmt
// node, so `EXPLAIN EXECUTE p(5)` becomes `EXPLAIN SELECT ...`. Unlike the old
// prefix/suffix template approach this works identically on the simple and
// extended protocols.
//
// PostgreSQL grammar guarantees at most one EXECUTE reference per parsed
// statement (ExecuteStmt is reachable only as the statement itself, as
// ExplainStmt.Query, or in CreateTableAsStmt.Query).
//
// Returns:
//   - (plan, nil) if the statement was a wrapped EXECUTE and was rewritten;
//   - (nil, nil) if no rewrite applies (caller should continue normal dispatch);
//   - (nil, err) if a wrapped EXECUTE referenced an unknown prepared statement.
func (p *Planner) tryUnwrapWrappedExecute(sql string, stmt ast.Stmt, conn *server.Conn) (*engine.Plan, error) {
	execStmt, isTemp, executes := findWrappedExecute(stmt)
	if execStmt == nil {
		return nil, nil
	}

	// Look up the user-visible prepared statement name via the Handler
	// interface. The handler's consolidator maps the user name to the
	// associated PreparedStatementInfo (carrying the resolved parameter types).
	psi := conn.Handler().GetPreparedStatementInfo(conn.ConnectionID(), execStmt.Name)
	if psi == nil {
		return nil, mterrors.NewInvalidPreparedStatementError(execStmt.Name)
	}

	var execInfo engine.PlanExecInfo
	if executes {
		// Re-analyzed here, not just at PREPARE time, because this statement
		// actually runs the prepared body as a side effect (CREATE TABLE AS
		// EXECUTE always; EXPLAIN ANALYZE EXECUTE because ANALYZE executes
		// the query — see explainAnalyzes), and the admissibility of what the
		// body does can have changed since PREPARE registered it.
		unsafeConnection := conn != nil && conn.UnsafeConnection()
		analysis, err := analyzeSQLPreparedBody(psi.AstStmt(), unsafeConnection, p.admitsFailoverSlots())
		if err != nil {
			return nil, err
		}
		execInfo = preparedBodyExecInfo(analysis, psi.AstStmt())

		// A tracked set_config in the body is refused rather than half-handled.
		// This route runs the substituted body as an ordinary query and, unlike
		// plain EXECUTE's PreparedStatementPrimitive (see NewExecutePrimitive's
		// setConfigs), has no channel for session-state tracking. Both ways of
		// proceeding are wrong: running the body verbatim persists the change on
		// a pooled backend, leaking it to whichever unrelated client checks out
		// that connection next, while rewriting it to revert (what
		// planExecuteStmt does, safely, because it *can* record the value) would
		// make the client's set_config a silent no-op — reverted on the backend
		// and recorded nowhere. Failing closed keeps the two forms honest: the
		// same body under a plain EXECUTE still works.
		if len(analysis.SetConfigs) > 0 || analysis.DynamicSetConfig {
			return nil, mterrors.NewFeatureNotSupported(
				"set_config inside a prepared statement is not supported under EXPLAIN ANALYZE EXECUTE or CREATE TABLE AS EXECUTE; use EXECUTE directly")
		}
	}

	substituted, err := engine.MaterializeWrappedExecute(stmt, execStmt, psi, nil)
	if err != nil {
		return nil, err
	}
	deparsedSQL := substituted.SqlString()
	p.logger.Debug("unwrapped wrapped EXECUTE",
		"user_name", execStmt.Name,
		"original", sql,
		"deparsed", deparsedSQL)

	// Run the substituted body as an ordinary query. ExecInfo composes two
	// independent reservation needs, both already false when the body never
	// runs (execInfo is the zero value, and findWrappedExecute never sets
	// isTemp for a non-executing shape — see its own doc comment): execInfo
	// above covers whatever the prepared body itself does (a temporary
	// logical replication slot, a session advisory lock, setseed(...), or
	// its own SELECT ... INTO TEMP); isTemp is the separate, wrapper-level
	// need — `CREATE TEMP TABLE t AS EXECUTE p` materializes its own temp
	// table regardless of what the body does.
	plan := engine.NewPlan(deparsedSQL,
		engine.NewRoute(p.defaultTableGroup, constants.DefaultShard, deparsedSQL, substituted))
	plan.ExecInfo = execInfo
	if isTemp {
		plan.ExecInfo.TempTable = true
	}
	return plan, nil
}

// findWrappedExecute returns the innermost ExecuteStmt inside a supported
// wrapper, whether the effective plan should use the temp-table-aware
// primitive, and whether the prepared body actually executes as a result of
// this statement running. Recognized shapes:
//
//   - ExplainStmt{Query: ExecuteStmt}
//     → plain Route; executes only if the EXPLAIN specifies ANALYZE (see
//     explainAnalyzes) — a bare EXPLAIN EXECUTE only plans the prepared
//     statement, it never runs it. Never temp-table (EXPLAIN never
//     materializes a table, even with ANALYZE, since there is no CTAS
//     target here to materialize into).
//   - CreateTableAsStmt{Query: ExecuteStmt}
//     → always executes (CREATE TABLE AS always runs its query); temp-table
//     if the CTAS target is TEMP.
//   - ExplainStmt{Query: CreateTableAsStmt{Query: ExecuteStmt}}
//     → executes, and temp-table if the CTAS target is TEMP, only if the
//     EXPLAIN specifies ANALYZE — EXPLAIN CREATE TABLE ... AS EXECUTE
//     without ANALYZE only plans it, exactly like the bare-EXECUTE case
//     above; only EXPLAIN ANALYZE actually executes and materializes the
//     table.
//
// Returns (nil, false, false) if the statement shape does not match a
// wrapped EXECUTE.
func findWrappedExecute(stmt ast.Stmt) (execStmt *ast.ExecuteStmt, isTemp bool, executes bool) {
	switch s := stmt.(type) {
	case *ast.ExplainStmt:
		analyzes := explainAnalyzes(s)
		// Direct EXPLAIN EXECUTE
		if es, ok := s.Query.(*ast.ExecuteStmt); ok {
			return es, false, analyzes
		}
		// EXPLAIN wrapping CREATE TABLE ... AS EXECUTE
		if ctas, ok := s.Query.(*ast.CreateTableAsStmt); ok {
			if es, ok := ctas.Query.(*ast.ExecuteStmt); ok {
				isTemp := analyzes && ctas.Into != nil && ctas.Into.Rel != nil && ctas.Into.Rel.RelPersistence == ast.RELPERSISTENCE_TEMP
				return es, isTemp, analyzes
			}
		}
	case *ast.CreateTableAsStmt:
		if es, ok := s.Query.(*ast.ExecuteStmt); ok {
			isTemp := s.Into != nil && s.Into.Rel != nil && s.Into.Rel.RelPersistence == ast.RELPERSISTENCE_TEMP
			return es, isTemp, true
		}
	}
	return nil, false, false
}

// explainAnalyzes reports whether stmt actually runs the statement it
// explains. Only ANALYZE makes EXPLAIN execute (performing every side effect
// a direct run would); a bare EXPLAIN only plans it and executes nothing.
//
// PostgreSQL's grammar parses the shorthand (`EXPLAIN ANALYZE ...`) and the
// bare option (`EXPLAIN (ANALYZE) ...`) to an "analyze" DefElem with no Arg,
// both meaning on. The option also takes an explicit boolean
// (`EXPLAIN (ANALYZE FALSE) ...`), which arrives as a String Arg — so the
// option's presence alone does not answer the question, and its value has to
// be read. A value this planner cannot resolve to a boolean is treated as
// executing, the fail-safe direction here: over-reserving a connection for a
// statement that turns out not to run costs a held connection, while
// under-reserving one that does run strands the slot or seed it creates on a
// backend nothing is tracking (see preparedBodyExecInfo).
func explainAnalyzes(stmt *ast.ExplainStmt) bool {
	if stmt.Options == nil {
		return false
	}
	for _, item := range stmt.Options.Items {
		de, ok := item.(*ast.DefElem)
		if !ok || !strings.EqualFold(de.Defname, "analyze") {
			continue
		}
		if de.Arg == nil {
			return true
		}
		s, ok := de.Arg.(*ast.String)
		if !ok {
			return true
		}
		analyzes, parsed := sqltypes.ParseBool(s.SVal)
		return !parsed || analyzes
	}
	return false
}
