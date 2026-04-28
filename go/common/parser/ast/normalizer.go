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

package ast

import (
	"fmt"
	"strings"

	"github.com/cespare/xxhash/v2"
)

// NormalizeResult holds the output of AST normalization.
type NormalizeResult struct {
	// NormalizedSQL is the SQL string with literals replaced by $1, $2, ...
	// Used as the plan cache key.
	NormalizedSQL string

	// NormalizedAST is the cloned AST with A_Const replaced by ParamRef.
	// Used for planning on cache miss and for SQL reconstruction at execution time.
	NormalizedAST Stmt

	// BindValues holds the extracted literal A_Const values in parameter order.
	// BindValues[0] corresponds to $1, etc.
	BindValues []*A_Const
}

// WasNormalized reports whether any literals were replaced during normalization.
func (r *NormalizeResult) WasNormalized() bool {
	return len(r.BindValues) > 0
}

// Fingerprint returns a stable 16-character hex hash of the normalized SQL.
// Two queries with the same NormalizedSQL produce identical fingerprints,
// which makes the fingerprint suitable as a per-query-shape identifier for
// metrics aggregation and cache keys.
func (r *NormalizeResult) Fingerprint() string {
	return FingerprintSQL(r.NormalizedSQL)
}

// FingerprintSQL returns a stable 16-character hex hash of the given SQL
// string. Useful for callers that already have a normalized or
// placeholder-form SQL (e.g. extended-protocol queries whose Parse text
// already contains $N placeholders) and don't need to run the normalizer.
func FingerprintSQL(sql string) string {
	return fmt.Sprintf("%016x", xxhash.Sum64String(sql))
}

// Normalize replaces literal A_Const values in the AST with ParamRef ($1, $2, ...)
// placeholders and returns the normalized SQL string along with the extracted values.
//
// The input AST is cloned before modification — the original is not mutated.
// Only A_Const nodes in expression positions are replaced. Nodes inside
// VariableSetStmt, VariableShowStmt, and DefElem subtrees are skipped
// because their literal values carry semantic meaning that affects planning.
//
// The same applies inside built-in function calls whose arguments the planner
// inspects literally — currently just `set_config(name, value, is_local)`,
// where the planner rewrites a bare SELECT into the equivalent SET and needs
// the literal values to build the SessionSettings update. See
// go/services/multigateway/planner/unsafe_funccall.go.
//
// NULL constants (A_Const with Isnull=true) are NOT normalized because NULL
// is a keyword that affects query semantics (e.g., IS NULL vs IS $1).
func Normalize(stmt Stmt) *NormalizeResult {
	cloned := CloneNode(stmt).(Stmt)

	var (
		counter    int
		bindValues []*A_Const
	)

	// replaceLiteral converts an A_Const literal into a $N placeholder and
	// records the original value. Shared between the top-level walker and
	// the per-arg recursion below so they mint placeholders from the same
	// counter sequence.
	replaceLiteral := func(cursor *Cursor) bool {
		aConst, ok := cursor.Node().(*A_Const)
		if !ok {
			return true
		}
		if aConst.Isnull {
			return true
		}
		counter++
		bindValues = append(bindValues, aConst)
		cursor.Replace(NewParamRef(counter, aConst.Location()))
		return false
	}

	normalizedAST := Rewrite(cloned, func(cursor *Cursor) bool {
		node := cursor.Node()

		// Skip subtrees where literal values carry semantic meaning that
		// affects planning (e.g., SET timezone = 'UTC') — don't normalize them.
		switch n := node.(type) {
		case *VariableSetStmt, *VariableShowStmt, *DefElem:
			return false
		case *FuncCall:
			if isPlannerLiteralFunc(n.Funcname) {
				// set_config(name, value, is_local). The planner needs to
				// read is_local literally to decide whether to track the
				// call, so args[2] must stay an A_Const regardless. For the
				// is_local=true case we still want to parameterize name and
				// value so a hot per-request pattern (PostgREST-style)
				// collapses into a single plan-cache fingerprint; we walk
				// only those two arg slots and skip the rest.
				if setConfigIsLocalLiteralTrue(n) && n.Args != nil && n.Args.Len() == 3 {
					n.Args.Items[0] = Rewrite(n.Args.Items[0], replaceLiteral, nil)
					n.Args.Items[1] = Rewrite(n.Args.Items[1], replaceLiteral, nil)
				}
				return false
			}
		}

		return replaceLiteral(cursor)
	}, nil).(Stmt)

	return &NormalizeResult{
		NormalizedSQL: normalizedAST.SqlString(),
		NormalizedAST: normalizedAST,
		BindValues:    bindValues,
	}
}

// isPlannerLiteralFunc reports whether the planner inspects this function
// call's arguments as literal values and therefore needs normalization
// skipped for its subtree. Currently only `set_config(name, value, is_local)`
// qualifies; callers schema-qualified to pg_catalog resolve to the same entry.
//
// Keeping this predicate in the ast package (next to the normalizer) trades
// a little co-location for avoiding an import cycle — the planner package
// imports ast, not the other way around.
func isPlannerLiteralFunc(funcname *NodeList) bool {
	if funcname == nil {
		return false
	}
	switch funcname.Len() {
	case 1:
		return funcNamePartEquals(funcname.Items[0], "set_config")
	case 2:
		return funcNamePartEquals(funcname.Items[0], "pg_catalog") &&
			funcNamePartEquals(funcname.Items[1], "set_config")
	}
	return false
}

// funcNamePartEquals returns true iff the node is a *String whose value,
// lowercased, equals want. Used for FuncCall.Funcname items, which are
// always *String in a well-formed parse tree.
func funcNamePartEquals(n Node, want string) bool {
	s, ok := n.(*String)
	if !ok {
		return false
	}
	return strings.EqualFold(s.SVal, want)
}

// setConfigIsLocalLiteralTrue reports whether fc is a 3-arg call whose
// third argument is the literal boolean true. The normalizer uses this
// to allow parameterizing set_config args when is_local=true: those calls
// aren't tracked or rewritten by the planner, so their literals carry no
// planning-relevant meaning, and preserving them would churn the plan
// cache for hot patterns like PostgREST's per-request
// set_config('request.jwt.claims', '<dynamic JSON>', true).
//
// Anything other than a clean literal-true (false, missing, ParamRef,
// non-literal expression, TypeCast over a literal) returns false — the
// safe direction, since the planner-side validator can then still see
// the original literals and reject or accept on its own terms.
func setConfigIsLocalLiteralTrue(fc *FuncCall) bool {
	if fc == nil || fc.Args == nil || fc.Args.Len() != 3 {
		return false
	}
	c, ok := fc.Args.Items[2].(*A_Const)
	if !ok || c.Isnull {
		return false
	}
	b, ok := c.Val.(*Boolean)
	if !ok {
		return false
	}
	return b.BoolVal
}

// ReconstructSQL takes a normalized AST and bind values, and produces the
// final SQL with values substituted back in. This is used by Route at
// execution time to reconstruct the actual query from a cached plan.
//
// The normalized AST is cloned before modification — it is not mutated.
func ReconstructSQL(normalizedAST Stmt, bindValues []*A_Const) string {
	cloned := CloneNode(normalizedAST).(Stmt)

	result := Rewrite(cloned, func(cursor *Cursor) bool {
		paramRef, ok := cursor.Node().(*ParamRef)
		if !ok {
			return true
		}
		idx := paramRef.Number - 1 // ParamRef is 1-based
		if idx >= 0 && idx < len(bindValues) {
			cursor.Replace(bindValues[idx])
		}
		return false
	}, nil)

	return result.(Stmt).SqlString()
}
