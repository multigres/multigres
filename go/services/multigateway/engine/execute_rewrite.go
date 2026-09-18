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

package engine

import (
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/preparedstatement"
)

// materializeExecute builds the concrete SQL for a SQL-level EXECUTE: it
// resolves any bound-parameter arguments to constants, substitutes the arguments
// into the prepared body (each cast to its resolved parameter type), and deparses
// the result. portalInfo may be nil for the simple protocol, where arguments are
// always literal expressions.
func materializeExecute(name string, body ast.Stmt, args *ast.NodeList, paramTypeOids []uint32, portalInfo *preparedstatement.PortalInfo) (string, error) {
	resolvedArgs, err := resolveExecuteArgs(args, portalInfo)
	if err != nil {
		return "", err
	}
	stmt, err := RewritePreparedBody(name, body, resolvedArgs, ParamTypeNamesFromOids(paramTypeOids))
	if err != nil {
		return "", err
	}
	return stmt.SqlString(), nil
}

// resolveExecuteArgs erases the *outer* layer of parameters from the EXECUTE
// arguments, leaving them as self-contained literals for RewritePreparedBody.
//
// Two independent scopes of $N can be in play, and they must be resolved in
// order. Consider `PREPARE p AS SELECT $1 * 2` executed over the extended
// protocol as: Parse "EXECUTE p($1)", Bind $1=21, Execute. There are now two
// unrelated $1s:
//
//   - outer $1 — a parameter of the EXECUTE *command itself*, supplied by the
//     wire Bind (its value, 21, lives in portalInfo). This is what a driver uses
//     to pass a runtime value into an EXECUTE without inlining a literal.
//   - inner $1 — a parameter of p's body (SELECT $1 * 2), filled by the EXECUTE
//     argument.
//
// They share the number but index into different things, so the outer layer must
// go first: this function turns `EXECUTE p($1)` + bind 21 into the equivalent of
// `EXECUTE p(21)` by decoding each bound value as text and substituting the
// literal, so RewritePreparedBody afterwards sees only the body's (inner) $N.
//
// The simple protocol has no Bind step, so its EXECUTE arguments are always
// literals (no outer $N) — that case takes the fast path unchanged. An argument
// that references $N with no bound-parameter context (portalInfo == nil) is a
// simple-protocol impossibility and is rejected.
func resolveExecuteArgs(args *ast.NodeList, portalInfo *preparedstatement.PortalInfo) (*ast.NodeList, error) {
	if args == nil || args.Len() == 0 {
		return args, nil
	}
	if !argsContainParamRef(args) {
		return args, nil
	}
	if portalInfo == nil {
		return nil, mterrors.NewFeatureNotSupported("EXECUTE argument references a parameter but the statement has no bound parameters")
	}

	resolved := ast.NewNodeList()
	for _, item := range args.Items {
		var rerr error
		newItem := ast.Rewrite(item, func(c *ast.Cursor) bool {
			pr, ok := c.Node().(*ast.ParamRef)
			if !ok {
				return true
			}
			txt, err := preparedstatement.DecodeBindAsText(portalInfo, pr, "EXECUTE argument")
			if err != nil {
				rerr = err
				return false
			}
			c.Replace(ast.NewA_Const(ast.NewString(txt), pr.Location()))
			return true
		}, nil)
		if rerr != nil {
			return nil, rerr
		}
		resolved.Append(newItem)
	}
	return resolved, nil
}

// argsContainParamRef reports whether any EXECUTE argument expression references
// a bound parameter ($N).
func argsContainParamRef(args *ast.NodeList) bool {
	found := false
	for _, item := range args.Items {
		ast.Rewrite(item, func(c *ast.Cursor) bool {
			if _, ok := c.Node().(*ast.ParamRef); ok {
				found = true
				return false
			}
			return true
		}, nil)
		if found {
			return true
		}
	}
	return false
}

// ParamTypeNamesFromOids maps the resolved parameter type OIDs recorded for a
// prepared statement to the cast-target TypeNames used by RewritePreparedBody.
// An unrecognized OID (0/unspecified, array, or user-defined) yields a nil entry,
// so the corresponding argument is substituted without a cast.
func ParamTypeNamesFromOids(oids []uint32) []*ast.TypeName {
	out := make([]*ast.TypeName, len(oids))
	for i, o := range oids {
		out[i] = ast.OidToTypeName(ast.Oid(o))
	}
	return out
}

// MaterializeWrappedExecute substitutes a SQL-level EXECUTE that is nested inside
// a wrapper statement (EXPLAIN EXECUTE, CREATE TABLE AS EXECUTE, or EXPLAIN of
// those) and returns a clone of the wrapper with the EXECUTE replaced by the
// substituted prepared body. wrapper is the full statement; execStmt is the
// ExecuteStmt node it contains; psi is the prepared statement being executed. The
// caller's AST is not mutated.
func MaterializeWrappedExecute(wrapper ast.Stmt, execStmt *ast.ExecuteStmt, psi *preparedstatement.PreparedStatementInfo, portalInfo *preparedstatement.PortalInfo) (ast.Stmt, error) {
	resolvedArgs, err := resolveExecuteArgs(execStmt.Params, portalInfo)
	if err != nil {
		return nil, err
	}
	substituted, err := RewritePreparedBody(execStmt.Name, psi.AstStmt(), resolvedArgs, ParamTypeNamesFromOids(psi.ResolvedParamTypeOids()))
	if err != nil {
		return nil, err
	}
	// Replace the ExecuteStmt node inside a clone of the wrapper with the
	// substituted body.
	clone := ast.CloneStmt(wrapper)
	replaced := false
	result := ast.Rewrite(clone, func(c *ast.Cursor) bool {
		if _, ok := c.Node().(*ast.ExecuteStmt); ok {
			c.Replace(substituted)
			replaced = true
			return false
		}
		return true
	}, nil)
	if !replaced {
		return nil, mterrors.NewFeatureNotSupported("EXECUTE: could not locate nested EXECUTE in wrapper statement")
	}
	stmt, ok := result.(ast.Stmt)
	if !ok {
		return nil, mterrors.NewFeatureNotSupported("EXECUTE: wrapped statement did not rewrite to a statement")
	}
	return stmt, nil
}

// RewritePreparedBody materializes a SQL-level EXECUTE into a concrete statement
// by substituting the EXECUTE argument expressions into the prepared statement
// body in place of its $N parameter references. The caller deparses the result
// and runs it as an ordinary query, so no backend PREPARE/EXECUTE pair is needed.
//
// This mirrors what PostgreSQL's EXECUTE does internally — coerce each argument
// to the prepared parameter's resolved type, then evaluate the body with those
// values — but does it as an AST rewrite.
//
// paramTypes carries the resolved parameter types recorded by the Describe
// performed at PREPARE time; paramTypes[i] is the type for $(i+1). A nil entry
// means the type is unknown, in which case the argument is substituted without
// a cast (wrapped in parentheses to preserve operator precedence). len(args)
// must equal len(paramTypes) — PostgreSQL requires EXECUTE to supply exactly the
// prepared parameter count.
//
// The prepared body is cloned and every argument is cloned per occurrence, so
// the input AST is never mutated and no node is aliased into multiple tree
// positions.
//
// Fidelity notes (accepted, documented divergences from real EXECUTE):
//   - The cast uses CAST(...) (explicit-context coercion), marginally more
//     permissive than EXECUTE's assignment-context coercion.
//   - A volatile argument referenced by more than one $N is evaluated once per
//     occurrence rather than exactly once.
func RewritePreparedBody(name string, body ast.Stmt, args *ast.NodeList, paramTypes []*ast.TypeName) (ast.Stmt, error) {
	nargs := 0
	if args != nil {
		nargs = args.Len()
	}
	if nargs != len(paramTypes) {
		return nil, mterrors.NewWrongNumberOfParametersError(name, len(paramTypes), nargs)
	}
	if body == nil {
		return nil, mterrors.NewFeatureNotSupported("EXECUTE: prepared statement has no body to execute")
	}

	// Clone so we never mutate the registered prepared-statement AST.
	cloned := ast.CloneStmt(body)

	var rewriteErr error
	result := ast.Rewrite(cloned, func(cursor *ast.Cursor) bool {
		pr, ok := cursor.Node().(*ast.ParamRef)
		if !ok {
			return true
		}
		// A body $N outside the supplied range should have been rejected by
		// PostgreSQL at PREPARE time; guard defensively rather than emit invalid
		// SQL. Report the same shape as a count mismatch.
		if pr.Number < 1 || pr.Number > nargs {
			rewriteErr = mterrors.NewWrongNumberOfParametersError(name, len(paramTypes), nargs)
			return false
		}
		// Clone the argument for this occurrence: the same $N may appear multiple
		// times, and each site must own an independent subtree.
		arg := ast.CloneNode(args.Items[pr.Number-1])

		var replacement ast.Node
		if tn := paramTypes[pr.Number-1]; tn != nil {
			// CAST renders as CAST(expr AS type) — self-delimiting, so it needs no
			// extra parentheses regardless of surrounding operator precedence.
			replacement = ast.NewTypeCast(arg, ast.CloneRefOfTypeName(tn), pr.Location())
		} else {
			// Unknown parameter type: substitute the argument bare, parenthesized to
			// preserve precedence against surrounding operators.
			replacement = ast.NewParenExpr(arg, pr.Location())
		}
		cursor.Replace(replacement)
		return true
	}, nil)

	if rewriteErr != nil {
		return nil, rewriteErr
	}
	stmt, ok := result.(ast.Stmt)
	if !ok {
		// A statement body always rewrites to a statement (the root is never a
		// bare $N), so this indicates a programming error rather than bad input.
		return nil, mterrors.NewFeatureNotSupported("EXECUTE: rewritten prepared body is not a statement")
	}
	return stmt, nil
}
