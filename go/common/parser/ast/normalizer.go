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

// Normalize replaces literal A_Const values in the AST with ParamRef ($1, $2, ...)
// placeholders and returns the normalized SQL string along with the extracted values.
//
// The input AST is cloned before modification — the original is not mutated.
// Only A_Const nodes in expression positions are replaced. Nodes inside
// VariableSetStmt, VariableShowStmt, and DefElem subtrees are skipped
// because their literal values carry semantic meaning that affects planning.
//
// NULL constants (A_Const with Isnull=true) are NOT normalized because NULL
// is a keyword that affects query semantics (e.g., IS NULL vs IS $1).
func Normalize(stmt Stmt) *NormalizeResult {
	cloned := CloneNode(stmt).(Stmt)

	var (
		counter    int
		bindValues []*A_Const
	)

	normalizedAST := Rewrite(cloned, func(cursor *Cursor) bool {
		node := cursor.Node()

		// Skip subtrees where literal values carry semantic meaning that
		// affects planning (e.g., SET timezone = 'UTC') — don't normalize them.
		switch node.(type) {
		case *VariableSetStmt, *VariableShowStmt, *DefElem:
			return false
		}

		aConst, ok := node.(*A_Const)
		if !ok {
			return true
		}

		// Don't normalize NULL — it's a keyword, not a data literal.
		if aConst.Isnull {
			return true
		}

		counter++
		bindValues = append(bindValues, aConst)
		cursor.Replace(NewParamRef(counter, aConst.Location()))
		return false
	}, nil).(Stmt)

	return &NormalizeResult{
		NormalizedSQL: normalizedAST.SqlString(),
		NormalizedAST: normalizedAST,
		BindValues:    bindValues,
	}
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
