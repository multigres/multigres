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

import "github.com/multigres/multigres/go/common/parser/ast"

// ExtractTablesUsed walks the AST and returns deduplicated, schema-qualified
// table names from all RangeVar nodes. CTE names are excluded since they are
// virtual tables, not real ones. Returns nil for statements that don't
// reference tables (SET, SHOW, BEGIN, etc.).
func ExtractTablesUsed(stmt ast.Stmt) []string {
	if stmt == nil {
		return nil
	}

	// First pass: collect CTE names so we can exclude them from results.
	cteNames := make(map[string]struct{})
	ast.Rewrite(stmt, func(cursor *ast.Cursor) bool {
		cte, ok := cursor.Node().(*ast.CommonTableExpr)
		if ok && cte.Ctename != "" {
			cteNames[cte.Ctename] = struct{}{}
		}
		return true
	}, nil)

	// Second pass: collect real table names from RangeVar nodes.
	seen := make(map[string]struct{})
	var tables []string

	ast.Rewrite(stmt, func(cursor *ast.Cursor) bool {
		rv, ok := cursor.Node().(*ast.RangeVar)
		if !ok {
			return true
		}
		if rv.RelName == "" {
			return true
		}

		// Skip CTE references (unqualified names that match a CTE).
		if rv.SchemaName == "" {
			if _, isCTE := cteNames[rv.RelName]; isCTE {
				return true
			}
		}

		name := rv.RelName
		if rv.SchemaName != "" {
			name = rv.SchemaName + "." + name
		}

		if _, exists := seen[name]; !exists {
			seen[name] = struct{}{}
			tables = append(tables, name)
		}
		return true
	}, nil)

	return tables
}
