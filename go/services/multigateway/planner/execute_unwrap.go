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
// attaches the gateway-managed prepared statement metadata to the Route.
//
// Background: multigateway stores SQL-level `PREPARE p AS ...` only in the
// gateway's consolidator keyed by the user name `p`, with a canonical name
// (e.g. `stmt42`) used for backend per-connection parse caching. The backend
// session never sees a statement named `p`. Top-level `EXECUTE p` works
// because the ExecutePrimitive routes through PortalStreamExecute, whose
// multipooler path calls ensurePrepared() on the chosen backend connection.
// But wrappers like `EXPLAIN EXECUTE p` or `CREATE TABLE ... AS EXECUTE p`
// would fall through to planDefault → raw StreamExecute, and the backend
// would reject them with `prepared statement "p" does not exist`.
//
// The fix: rewrite the inner ExecuteStmt.Name from the user name `p` to the
// canonical name `stmt42`, regenerate SQL via SqlString(), and attach the
// PreparedStatement metadata to the Route. The multipooler's StreamExecute
// path reads options.PreparedStatement and calls ensurePrepared() on the
// backend connection before running the rewritten SQL — so the backend sees
// a real SQL `EXECUTE stmt42` against a real parsed statement and normal
// plan-cache semantics apply.
//
// PostgreSQL grammar guarantees at most one EXECUTE reference per parsed
// statement (ExecuteStmt is a top-level production reachable only as the
// statement itself, as ExplainStmt.Query, or in CreateTableAsStmt.Query),
// so we only need to handle these two wrapper shapes.
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

	// Mutate the AST so the regenerated SQL references the canonical name.
	// The AST is freshly parsed per-query in handler.HandleQuery, so it's
	// safe to mutate in-place here.
	userName := execStmt.Name
	execStmt.Name = psi.Name
	rewrittenSQL := stmt.SqlString()
	p.logger.Debug("unwrapped wrapped EXECUTE",
		"user_name", userName,
		"canonical_name", psi.Name,
		"original", sql,
		"rewritten", rewrittenSQL)

	// Build the route. Use TempTableRoute for `CREATE TEMP TABLE t AS EXECUTE p`
	// so the query runs on a temp-table-reserved connection; otherwise use a
	// plain Route that goes through the regular pool.
	var prim engine.Primitive
	if isTemp {
		prim = engine.NewTempTableRouteWithPreparedStatement(
			p.defaultTableGroup, constants.DefaultShard, rewrittenSQL, psi.PreparedStatement)
	} else {
		prim = engine.NewRouteWithPreparedStatement(
			p.defaultTableGroup, constants.DefaultShard, rewrittenSQL, psi.PreparedStatement)
	}
	return engine.NewPlan(rewrittenSQL, prim), nil
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
