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

// Renderings of the two folds for version "V".
const (
	foldedText = "CAST('V' AS TEXT)"
	foldedFrom = "CAST((SELECT " + foldedText + " AS version) AS TEXT)"
)

// TestFoldMultigresVersion checks the rendered SQL of the fold in expression
// positions and as a FROM item, in the shapes PostgreSQL treats specially.
func TestFoldMultigresVersion(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		// Expression positions.
		{
			name: "bare target gets version label",
			sql:  "SELECT multigres.version()",
			want: "SELECT " + foldedText + " AS version",
		},
		{
			name: "explicit alias preserved",
			sql:  "SELECT multigres.version() AS v",
			want: "SELECT " + foldedText + " AS v",
		},
		{
			name: "in WHERE clause",
			sql:  "SELECT id FROM t WHERE created_by = multigres.version()",
			want: "SELECT id FROM t WHERE created_by = " + foldedText,
		},
		{
			name: "mixed with a real column",
			sql:  "SELECT multigres.version(), id FROM t",
			want: "SELECT " + foldedText + " AS version, id FROM t",
		},
		{
			name: "mixed case",
			sql:  "SELECT MULTIGRES.Version()",
			want: "SELECT " + foldedText + " AS version",
		},
		{
			name: "function argument",
			sql:  "SELECT pg_typeof(multigres.version())",
			want: "SELECT pg_typeof(" + foldedText + ")",
		},
		{
			name: "argument of CALL",
			sql:  "CALL p(multigres.version())",
			want: "CALL p(" + foldedText + ")",
		},

		// FROM items.
		{
			name: "FROM without alias",
			sql:  "SELECT * FROM multigres.version()",
			want: "SELECT * FROM " + foldedFrom,
		},
		{
			name: "FROM mixed case",
			sql:  "SELECT * FROM MULTIGRES.Version()",
			want: "SELECT * FROM " + foldedFrom,
		},
		{
			name: "FROM with table alias",
			sql:  "SELECT * FROM multigres.version() AS v",
			want: "SELECT * FROM " + foldedFrom + " AS v",
		},
		{
			name: "FROM with column alias",
			sql:  "SELECT * FROM multigres.version() AS v(x)",
			want: "SELECT * FROM " + foldedFrom + " AS v(x)",
		},
		{
			name: "FROM whole-row reference",
			sql:  "SELECT v FROM multigres.version() v",
			want: "SELECT v FROM " + foldedFrom + " AS v",
		},
		{
			name: "FROM WITH ORDINALITY",
			sql:  "SELECT * FROM multigres.version() WITH ORDINALITY",
			want: "SELECT * FROM " + foldedFrom + " WITH ORDINALITY",
		},
		{
			name: "ROWS FROM first of two",
			sql:  "SELECT * FROM ROWS FROM (multigres.version(), generate_series(1, 2))",
			want: "SELECT * FROM ROWS FROM (" + foldedFrom + ", generate_series(1, 2))",
		},
		{
			name: "ROWS FROM second of two, aliased, with ordinality",
			sql:  "SELECT * FROM ROWS FROM (generate_series(1, 2), multigres.version()) WITH ORDINALITY AS r",
			want: "SELECT * FROM ROWS FROM (generate_series(1, 2), " + foldedFrom + ") WITH ORDINALITY AS r",
		},
		{
			name: "ROWS FROM twice",
			sql:  "SELECT * FROM ROWS FROM (multigres.version(), multigres.version())",
			want: "SELECT * FROM ROWS FROM (" + foldedFrom + ", " + foldedFrom + ")",
		},
		{
			name: "LATERAL",
			sql:  "SELECT * FROM generate_series(1, 2) AS g, LATERAL multigres.version()",
			want: "SELECT * FROM generate_series(1, 2) AS g, LATERAL " + foldedFrom,
		},
		{
			name: "inside a JOIN",
			sql:  "SELECT * FROM multigres.version() JOIN pg_class ON false",
			want: "SELECT * FROM " + foldedFrom + " INNER JOIN pg_class ON FALSE",
		},
		{
			name: "nested subquery",
			sql:  "SELECT * FROM (SELECT * FROM multigres.version()) AS s",
			want: "SELECT * FROM (SELECT * FROM " + foldedFrom + ") AS s",
		},
		{
			name: "inside a CTE",
			sql:  "WITH c AS (SELECT * FROM multigres.version()) SELECT * FROM c",
			want: "WITH c AS (SELECT * FROM " + foldedFrom + ") SELECT * FROM c",
		},
		{
			name: "set operation",
			sql:  "SELECT * FROM multigres.version() UNION ALL SELECT 'x'",
			want: "SELECT * FROM " + foldedFrom + " UNION ALL SELECT 'x'",
		},
		{
			name: "column definition list is kept for the backend to reject",
			sql:  "SELECT * FROM multigres.version() AS v(x text)",
			want: "SELECT * FROM " + foldedFrom + " AS v (x TEXT)",
		},
		{
			name: "FROM and expression folds in one statement",
			sql:  "SELECT multigres.version(), v FROM multigres.version() AS v WHERE v = multigres.version()",
			want: "SELECT " + foldedText + " AS version, v FROM " + foldedFrom + " AS v WHERE v = " + foldedText,
		},
		{
			name: "expression inside another FROM function's arguments",
			sql:  "SELECT * FROM generate_series(1, length(multigres.version()))",
			want: "SELECT * FROM generate_series(1, length(" + foldedText + "))",
		},
		{
			name: "VALUES row",
			sql:  "VALUES (multigres.version())",
			want: "VALUES (" + foldedText + ")",
		},

		// Other statements.
		{
			name: "CREATE VIEW",
			sql:  "CREATE VIEW vv AS SELECT * FROM multigres.version()",
			want: "CREATE VIEW vv AS SELECT * FROM " + foldedFrom,
		},
		{
			name: "CREATE TABLE AS",
			sql:  "CREATE TABLE tt AS SELECT * FROM multigres.version()",
			want: "CREATE TABLE tt AS SELECT * FROM " + foldedFrom,
		},
		{
			name: "INSERT SELECT",
			sql:  "INSERT INTO tt SELECT * FROM multigres.version()",
			want: "INSERT INTO tt SELECT * FROM " + foldedFrom,
		},
		{
			name: "COPY query",
			sql:  "COPY (SELECT * FROM multigres.version()) TO STDOUT",
			want: "COPY (SELECT * FROM " + foldedFrom + ") TO STDOUT",
		},
		{
			name: "PREPARE",
			sql:  "PREPARE p AS SELECT * FROM multigres.version()",
			want: "PREPARE p AS SELECT * FROM " + foldedFrom,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := foldOne(t, tt.sql, "V")
			assert.Equal(t, tt.want, got)
			// The rendered SQL must itself parse: it is what the planner and
			// the backend see.
			_, err := parser.ParseSQL(got)
			assert.NoError(t, err, "folded SQL does not parse: %s", got)
		})
	}
}

// TestFoldMultigresVersion_NotFolded checks the cases that must route to
// PostgreSQL untouched.
func TestFoldMultigresVersion_NotFolded(t *testing.T) {
	const v = "Multigres 0.1.0-SNAPSHOT"

	for _, sql := range []string{
		"SELECT version()",                   // bare version() → PostgreSQL's own
		"SELECT pg_catalog.version()",        // pg_catalog qualified → PostgreSQL's own
		"SELECT multigres.version(1)",        // args → not our niladic function
		"SELECT other.version()",             // wrong schema
		"SELECT multigres.other()",           // wrong function
		"SELECT * FROM version()",            // bare version() as a FROM item
		"SELECT * FROM pg_catalog.version()", // pg_catalog qualified FROM item
		"SELECT * FROM multigres.version(1)", // FROM item with args
		"CALL multigres.version()",           // a procedure name, not an expression
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

	t.Run("unparsable input is returned unchanged", func(t *testing.T) {
		sql := "SELECT multigres.version() FROM" // trailing FROM → syntax error
		assert.Equal(t, sql, foldGatewayFunctions(sql))
	})

	t.Run("folds the real function", func(t *testing.T) {
		got := foldGatewayFunctions("SELECT multigres.version()")
		assert.NotContains(t, got, "multigres.version")
		assert.Contains(t, got, "AS version")
	})
}
