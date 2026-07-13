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

package handler

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser"
)

// foldOne parses sql, folds multigres.version() with the given version, and
// returns the re-rendered SQL — the transform foldGatewayFunctions applies.
func foldOne(t *testing.T, sql, version string) string {
	t.Helper()
	stmts, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	foldMultigresVersion(stmts[0], version)
	return stmts[0].SqlString()
}

// TestFoldMultigresVersion covers folding multigres.version() into a literal
// across the expression positions users can write it in.
func TestFoldMultigresVersion(t *testing.T) {
	const v = "Multigres 0.1.0-SNAPSHOT"

	tests := []struct {
		name string
		sql  string
		// wantContains are substrings the folded SQL must contain.
		wantContains []string
		// wantAbsent are substrings the folded SQL must NOT contain.
		wantAbsent []string
	}{
		{
			name:         "bare target gets version label",
			sql:          "SELECT multigres.version()",
			wantContains: []string{"'Multigres 0.1.0-SNAPSHOT'", "AS version"},
			wantAbsent:   []string{"multigres.version"},
		},
		{
			name:         "explicit alias preserved",
			sql:          "SELECT multigres.version() AS v",
			wantContains: []string{"'Multigres 0.1.0-SNAPSHOT'", "AS v"},
			wantAbsent:   []string{"multigres.version", "AS version"},
		},
		{
			name:         "in WHERE clause",
			sql:          "SELECT id FROM t WHERE created_by = multigres.version()",
			wantContains: []string{"'Multigres 0.1.0-SNAPSHOT'", "FROM t", "WHERE"},
			wantAbsent:   []string{"multigres.version"},
		},
		{
			name:         "mixed with a real column",
			sql:          "SELECT multigres.version(), id FROM t",
			wantContains: []string{"'Multigres 0.1.0-SNAPSHOT'", "AS version", "id", "FROM t"},
			wantAbsent:   []string{"multigres.version"},
		},
		{
			name:         "case-insensitive",
			sql:          "SELECT MULTIGRES.VERSION()",
			wantContains: []string{"'Multigres 0.1.0-SNAPSHOT'"},
			wantAbsent:   []string{"MULTIGRES.VERSION", "multigres.version"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := foldOne(t, tt.sql, v)
			for _, sub := range tt.wantContains {
				assert.Contains(t, got, sub, "folded: %s", got)
			}
			for _, sub := range tt.wantAbsent {
				assert.NotContains(t, got, sub, "folded: %s", got)
			}
		})
	}
}

// TestFoldMultigresVersion_NotFolded checks the cases that must route to
// PostgreSQL untouched.
func TestFoldMultigresVersion_NotFolded(t *testing.T) {
	const v = "Multigres 0.1.0-SNAPSHOT"

	for _, sql := range []string{
		"SELECT version()",            // bare version() → PostgreSQL's own
		"SELECT pg_catalog.version()", // pg_catalog qualified → PostgreSQL's own
		"SELECT multigres.version(1)", // args → not our niladic function
		"SELECT other.version()",      // wrong schema
		"SELECT multigres.other()",    // wrong function
	} {
		t.Run(sql, func(t *testing.T) {
			stmts, err := parser.ParseSQL(sql)
			require.NoError(t, err)
			changed := foldMultigresVersion(stmts[0], v)
			assert.False(t, changed, "should not fold %q", sql)
		})
	}
}

// TestFoldGatewayFunctionsInStatements covers the parsed-statement entry point
// used by the simple-query path (which reuses its parse rather than re-parsing).
func TestFoldGatewayFunctionsInStatements(t *testing.T) {
	t.Run("folds and reports change", func(t *testing.T) {
		stmts, err := parser.ParseSQL("SELECT multigres.version(); SELECT 1")
		require.NoError(t, err)
		require.Len(t, stmts, 2)

		assert.True(t, foldGatewayFunctionsInStatements(stmts))
		assert.NotContains(t, renderStatements(stmts), "multigres.version")
	})

	t.Run("no change when absent", func(t *testing.T) {
		stmts, err := parser.ParseSQL("SELECT 1; SELECT version()")
		require.NoError(t, err)
		assert.False(t, foldGatewayFunctionsInStatements(stmts))
	})
}

// TestFoldGatewayFunctions covers the string-level entry point, including the
// cheap substring gate and its pass-through behaviour.
func TestFoldGatewayFunctions(t *testing.T) {
	t.Run("query without the schema is returned unchanged (no parse)", func(t *testing.T) {
		sql := "SELECT id FROM t WHERE name = 'x'"
		assert.Equal(t, sql, foldGatewayFunctions(sql))
	})

	t.Run("unparseable input is returned unchanged", func(t *testing.T) {
		sql := "SELECT multigres.version() FROM" // trailing FROM → syntax error
		assert.Equal(t, sql, foldGatewayFunctions(sql))
	})

	t.Run("folds the real function", func(t *testing.T) {
		got := foldGatewayFunctions("SELECT multigres.version()")
		assert.NotContains(t, got, "multigres.version")
		assert.Contains(t, got, "AS version")
	})
}
