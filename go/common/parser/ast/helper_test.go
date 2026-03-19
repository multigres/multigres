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

package ast_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"
)

func TestExtractTablesUsed(t *testing.T) {
	tests := []struct {
		name   string
		sql    string
		expect []string
	}{
		{
			name:   "simple select",
			sql:    "SELECT * FROM users",
			expect: []string{"users"},
		},
		{
			name:   "schema-qualified select",
			sql:    "SELECT * FROM public.users",
			expect: []string{"public.users"},
		},
		{
			name:   "join",
			sql:    "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
			expect: []string{"users", "orders"},
		},
		{
			name:   "insert",
			sql:    "INSERT INTO orders (id) VALUES (1)",
			expect: []string{"orders"},
		},
		{
			name:   "update",
			sql:    "UPDATE users SET name = 'alice'",
			expect: []string{"users"},
		},
		{
			name:   "delete",
			sql:    "DELETE FROM users WHERE id = 1",
			expect: []string{"users"},
		},
		{
			name:   "update with from",
			sql:    "UPDATE users SET name = accounts.name FROM accounts WHERE users.id = accounts.user_id",
			expect: []string{"users", "accounts"},
		},
		{
			name:   "set statement",
			sql:    "SET statement_timeout = '5s'",
			expect: nil,
		},
		{
			name:   "begin",
			sql:    "BEGIN",
			expect: nil,
		},
		{
			name:   "select without from",
			sql:    "SELECT 1",
			expect: nil,
		},
		{
			name:   "subquery",
			sql:    "SELECT * FROM (SELECT * FROM users) AS u",
			expect: []string{"users"},
		},
		{
			name:   "duplicate tables",
			sql:    "SELECT * FROM users u1 JOIN users u2 ON u1.id = u2.id",
			expect: []string{"users"},
		},
		{
			name:   "cte",
			sql:    "WITH cte AS (SELECT * FROM orders) SELECT * FROM cte JOIN users ON cte.id = users.id",
			expect: []string{"orders", "users"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := parser.ParseSQL(tt.sql)
			require.NoError(t, err)
			require.Len(t, stmts, 1)

			got := ast.ExtractTablesUsed(stmts[0])
			assert.ElementsMatch(t, tt.expect, got)
		})
	}
}

func TestExtractTablesUsed_NilStmt(t *testing.T) {
	assert.Nil(t, ast.ExtractTablesUsed(nil))
}
