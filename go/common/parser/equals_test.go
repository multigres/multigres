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

package parser

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser/ast"
)

// TestEqualsNodeMatchesDeepEqual checks the generated ast.EqualsNode against
// reflect.DeepEqual over the parser corpora. Every query is parsed twice, so
// equality is decided over two independently built trees rather than one
// pointer compared with itself. Each statement is also compared with the
// statement of the previous case, which in these files is usually a close
// variant, so the unequal branch is exercised too.
func TestEqualsNodeMatchesDeepEqual(t *testing.T) {
	files := []string{
		"select_cases.json",
		"misc_cases.json",
		"ddl_cases.json",
		"dml_cases.json",
		"set_cases.json",
		"sharding_cases.json",
	}
	for _, file := range files {
		t.Run(file, func(t *testing.T) {
			var prev ast.Stmt
			for _, tcase := range readJSONTests(file) {
				if tcase.Query == "" {
					continue
				}
				first, err := ParseSQL(tcase.Query)
				if err != nil {
					continue
				}
				second, err := ParseSQL(tcase.Query)
				require.NoError(t, err)
				require.Len(t, second, len(first))

				for i, stmt := range first {
					assert.Truef(t, ast.EqualsNode(stmt, second[i]),
						"two parses of the same query are not equal\nquery: %s", tcase.Query)
					assert.Truef(t, ast.EqualsNode(stmt, ast.CloneNode(stmt)),
						"a statement is not equal to its clone\nquery: %s", tcase.Query)

					if prev != nil {
						assert.Equalf(t, reflect.DeepEqual(prev, stmt), ast.EqualsNode(prev, stmt),
							"EqualsNode disagrees with reflect.DeepEqual\nprevious: %s\nquery:    %s",
							prev.SqlString(), tcase.Query)
					}
					prev = stmt
				}
			}
		})
	}
}

// TestEqualsNodeDetectsChanges checks that a change anywhere in the tree,
// including a nested list element or a source position, makes trees unequal.
func TestEqualsNodeDetectsChanges(t *testing.T) {
	parseOne := func(sql string) ast.Stmt {
		stmts, err := ParseSQL(sql)
		require.NoError(t, err)
		require.Len(t, stmts, 1)
		return stmts[0]
	}

	base := parseOne("CREATE TABLE t (a int PRIMARY KEY, b text NOT NULL DEFAULT 'x')")

	tests := []struct {
		name  string
		other ast.Stmt
	}{
		{"different column name", parseOne("CREATE TABLE t (a int PRIMARY KEY, c text NOT NULL DEFAULT 'x')")},
		{"different column type", parseOne("CREATE TABLE t (a bigint PRIMARY KEY, b text NOT NULL DEFAULT 'x')")},
		{"different default", parseOne("CREATE TABLE t (a int PRIMARY KEY, b text NOT NULL DEFAULT 'y')")},
		{"missing constraint", parseOne("CREATE TABLE t (a int PRIMARY KEY, b text DEFAULT 'x')")},
		{"extra column", parseOne("CREATE TABLE t (a int PRIMARY KEY, b text NOT NULL DEFAULT 'x', c int)")},
		{"different statement type", parseOne("DROP TABLE t")},
		{"different source position", func() ast.Stmt {
			// Positions are compared like any other field.
			c := ast.CloneNode(base).(*ast.CreateStmt)
			c.Loc++
			return c
		}()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.False(t, ast.EqualsNode(base, tt.other))
			assert.False(t, ast.EqualsNode(tt.other, base))
		})
	}

	t.Run("nil handling", func(t *testing.T) {
		assert.True(t, ast.EqualsNode(nil, nil))
		assert.False(t, ast.EqualsNode(base, nil))
		assert.False(t, ast.EqualsNode(nil, base))
	})
}
