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
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/services/multigateway/engine"
)

// tryUnwrapWrappedExecute detects statements of the form `EXPLAIN EXECUTE p`
// or `CREATE [TEMP] TABLE t AS EXECUTE p` and turns them into a plan that
// carries a SQL EXECUTE prefix/suffix template plus the gateway-managed
// prepared statement metadata.
//
// Background: multigateway stores SQL-level `PREPARE p AS ...` only in the
// gateway's consolidator keyed by the user name `p`. The backend session never
// sees a statement named `p`. Wrappers like `EXPLAIN EXECUTE p` or
// `CREATE TABLE ... AS EXECUTE p` would otherwise fall through to planDefault →
// raw StreamExecute, and the backend would reject them with `prepared statement
// "p" does not exist`.
//
// The fix: the gateway deparses the statement into SQL prefix/suffix around the
// inner ExecuteStmt.Name and attaches the PreparedStatement metadata. The
// multipooler's StreamExecute path resolves that metadata through its own
// pooler-level consolidator (ppstmt*) and materializes the final SQL before
// running it — so PostgreSQL evaluates the SQL EXECUTE wrapper normally while
// preserving prepared-statement consolidation across gateways.
//
// PostgreSQL grammar guarantees at most one EXECUTE reference per parsed
// statement (ExecuteStmt is a top-level production reachable only as the
// statement itself, as ExplainStmt.Query, or in CreateTableAsStmt.Query),
// so a single prefix/suffix pair covers every legal wrapped shape.
//
// Returns:
//   - (plan, nil) if the statement was a wrapped EXECUTE and was rewritten;
//   - (nil, nil) if no rewrite applies (caller should continue normal dispatch);
//   - (nil, err) if a wrapped EXECUTE referenced an unknown prepared statement.
func (p *Planner) tryUnwrapWrappedExecute(sql string, stmt ast.Stmt, conn *server.Conn) (*engine.Plan, error) {
	execStmt, isTemp := findWrappedExecute(stmt)
	if execStmt == nil {
		return nil, nil
	}

	// Look up the user-visible prepared statement name via the Handler
	// interface. The handler's consolidator maps the user name to a
	// canonical name and the associated PreparedStatementInfo.
	psi := conn.Handler().GetPreparedStatementInfo(conn.ConnectionID(), execStmt.Name)
	if psi == nil {
		return nil, mterrors.NewInvalidPreparedStatementError(execStmt.Name)
	}

	executeSQLPreparedStatement, err := engine.BuildExecuteSQLPreparedStatement(stmt, execStmt, psi.PreparedStatement)
	if err != nil {
		return nil, err
	}
	deparsedSQL := stmt.SqlString()
	p.logger.Debug("unwrapped wrapped EXECUTE",
		"user_name", execStmt.Name,
		"gateway_canonical_name", psi.Name,
		"original", sql,
		"deparsed", deparsedSQL,
		"sql_prefix", executeSQLPreparedStatement.SqlPrefix,
		"sql_suffix", executeSQLPreparedStatement.SqlSuffix)

	// Build a Route carrying the SQL EXECUTE template. For
	// `CREATE TEMP TABLE t AS EXECUTE p`, ExecInfo.TempTable makes the executor
	// run it on a temp-table-reserved connection; otherwise it goes through the
	// regular pool.
	plan := engine.NewPlan(deparsedSQL,
		engine.NewRouteWithExecuteSQLPreparedStatement(p.defaultTableGroup, constants.DefaultShard, deparsedSQL, executeSQLPreparedStatement))
	if isTemp {
		plan.ExecInfo.TempTable = true
	}
	return plan, nil
}

// findWrappedExecute returns the innermost ExecuteStmt inside a supported
// wrapper and true when the effective plan should use the temp-table-aware
// primitive. Recognized shapes:
//
//   - ExplainStmt{Query: ExecuteStmt}
//     → plain Route (EXPLAIN never materializes data, even with ANALYZE,
//     because ANALYZE EXECUTE re-runs the existing prepared statement)
//   - CreateTableAsStmt{Query: ExecuteStmt}
//     → TempTableRoute if the CTAS target is TEMP, else Route
//   - ExplainStmt{Query: CreateTableAsStmt{Query: ExecuteStmt}}
//     → TempTableRoute if the CTAS target is TEMP (EXPLAIN ANALYZE CREATE
//     TABLE ... actually executes and materializes the table), else Route
//
// Returns (nil, false) if the statement shape does not match a wrapped
// EXECUTE.
func findWrappedExecute(stmt ast.Stmt) (*ast.ExecuteStmt, bool) {
	switch s := stmt.(type) {
	case *ast.ExplainStmt:
		// Direct EXPLAIN EXECUTE
		if es, ok := s.Query.(*ast.ExecuteStmt); ok {
			return es, false
		}
		// EXPLAIN wrapping CREATE TABLE ... AS EXECUTE
		if ctas, ok := s.Query.(*ast.CreateTableAsStmt); ok {
			if es, ok := ctas.Query.(*ast.ExecuteStmt); ok {
				isTemp := ctas.Into != nil && ctas.Into.Rel != nil && ctas.Into.Rel.RelPersistence == ast.RELPERSISTENCE_TEMP
				return es, isTemp
			}
		}
	case *ast.CreateTableAsStmt:
		if es, ok := s.Query.(*ast.ExecuteStmt); ok {
			isTemp := s.Into != nil && s.Into.Rel != nil && s.Into.Rel.RelPersistence == ast.RELPERSISTENCE_TEMP
			return es, isTemp
		}
	}
	return nil, false
}
