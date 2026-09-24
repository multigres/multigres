// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMigrationStatementsParseDeparse drives the Multigres-only migration and
// connection statements end to end: it parses each form through the real grammar
// and compares the deparsed SqlString against the expected canonical spelling.
//
// This is both the grammar-wiring check (the grammar must build the AST node the
// deparser expects) and a regression guard for the `FOR TABLE ...` object-list
// path, whose CreateMigrationStmt.SqlString previously panicked because it called
// the unimplemented PublicationObjSpec.SqlString instead of reusing the shared
// publication object-list renderer.
//
// Note: connection OPTIONS accept the reloptions spelling (`host 'h'`) while the
// deparser emits the DefElem spelling (`host = 'h'`), so the deparsed connection
// SQL is intentionally not re-parseable; the migration statements below use the
// `WITH (... = ...)` definition grammar and do round-trip.
func TestMigrationStatementsParseDeparse(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		// CONNECTIONS
		{
			name: "create connection options",
			in:   "CREATE CONNECTION src OPTIONS (host 'db.example.com', port '5432', dbname 'app')",
			want: "CREATE CONNECTION src OPTIONS (host = 'db.example.com', port = '5432', dbname = 'app')",
		},
		{
			name: "create connection if not exists",
			in:   "CREATE CONNECTION IF NOT EXISTS src OPTIONS (host 'h')",
			want: "CREATE CONNECTION IF NOT EXISTS src OPTIONS (host = 'h')",
		},
		{
			name: "alter connection add/set/drop options",
			in:   "ALTER CONNECTION src OPTIONS (SET host 'new', ADD sslmode 'require', DROP port)",
			want: "ALTER CONNECTION src OPTIONS (host = 'new', sslmode = 'require', port)",
		},
		{
			name: "drop connection",
			in:   "DROP CONNECTION src",
			want: "DROP CONNECTION src",
		},
		{
			name: "drop connection if exists list",
			in:   "DROP CONNECTION IF EXISTS a, b, c",
			want: "DROP CONNECTION IF EXISTS a, b, c",
		},
		{
			name: "show one connection",
			in:   "SHOW CONNECTION src",
			want: "SHOW CONNECTION src",
		},
		// MIGRATIONS
		{
			name: "create migration for all tables",
			in:   "CREATE MIGRATION m CONNECTION src FOR ALL TABLES",
			want: "CREATE MIGRATION m CONNECTION src FOR ALL TABLES",
		},
		{
			name: "create migration for single table",
			in:   "CREATE MIGRATION m CONNECTION src FOR TABLE t1",
			want: "CREATE MIGRATION m CONNECTION src FOR TABLE t1",
		},
		{
			name: "create migration for qualified table list",
			in:   "CREATE MIGRATION m CONNECTION src FOR TABLE t1, TABLE public.t2",
			want: "CREATE MIGRATION m CONNECTION src FOR TABLE t1, public.t2",
		},
		{
			name: "create migration for tables in schema",
			in:   "CREATE MIGRATION m CONNECTION src FOR TABLES IN SCHEMA s1",
			want: "CREATE MIGRATION m CONNECTION src FOR TABLES IN SCHEMA s1",
		},
		{
			name: "create migration with options",
			in:   "CREATE MIGRATION IF NOT EXISTS m CONNECTION src FOR ALL TABLES WITH (copy_data = true)",
			want: "CREATE MIGRATION IF NOT EXISTS m CONNECTION src FOR ALL TABLES WITH (copy_data = 'true')",
		},
		{
			name: "alter migration start",
			in:   "ALTER MIGRATION m START",
			want: "ALTER MIGRATION m START",
		},
		{
			name: "alter migration if exists activate",
			in:   "ALTER MIGRATION IF EXISTS m ACTIVATE",
			want: "ALTER MIGRATION IF EXISTS m ACTIVATE",
		},
		{
			name: "alter migration deactivate",
			in:   "ALTER MIGRATION m DEACTIVATE",
			want: "ALTER MIGRATION m DEACTIVATE",
		},
		{
			name: "alter migration set connection",
			in:   "ALTER MIGRATION m CONNECTION other",
			want: "ALTER MIGRATION m CONNECTION other",
		},
		{
			name: "alter migration set options",
			in:   "ALTER MIGRATION m SET (copy_data = false)",
			want: "ALTER MIGRATION m SET (copy_data = 'false')",
		},
		{
			name: "drop migration",
			in:   "DROP MIGRATION m",
			want: "DROP MIGRATION m",
		},
		{
			name: "drop migration if exists force",
			in:   "DROP MIGRATION IF EXISTS m FORCE",
			want: "DROP MIGRATION IF EXISTS m FORCE",
		},
		{
			name: "drop migration wait",
			in:   "DROP MIGRATION m WAIT",
			want: "DROP MIGRATION m WAIT",
		},
		{
			name: "drop migration wait with timeout",
			in:   "DROP MIGRATION m WAIT (30)",
			want: "DROP MIGRATION m WAIT (30)",
		},
		{
			name: "show one migration",
			in:   "SHOW MIGRATION m",
			want: "SHOW MIGRATION m",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := ParseSQL(tt.in)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			assert.Equal(t, tt.want, stmts[0].SqlString())
		})
	}
}
