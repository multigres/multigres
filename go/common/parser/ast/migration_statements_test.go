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

package ast

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// connOptions builds a `(host = 'h', port = '5432')`-style OPTIONS list.
func connOptions() *NodeList {
	return NewNodeList(
		NewDefElem("host", NewString("h")),
		NewDefElem("port", NewString("5432")),
	)
}

// tableObjects builds a `FOR` object list of the given unqualified table names.
func tableObjects(names ...string) *NodeList {
	list := NewNodeList()
	for _, n := range names {
		list.Append(NewPublicationObjSpecTable(
			PUBLICATIONOBJ_TABLE,
			NewPublicationTable(NewRangeVar(n, "", ""), nil, nil),
		))
	}
	return list
}

// TestMigrationStatementsSqlString exercises the hand-written deparse (SqlString)
// method of every migration/connection statement node, across each branch of the
// statement grammar (IF [NOT] EXISTS, option lists, FOR ALL TABLES vs object list,
// the ALTER MIGRATION actions, and the DROP MIGRATION FORCE/WAIT behaviours).
func TestMigrationStatementsSqlString(t *testing.T) {
	tests := []struct {
		name string
		node Node
		want string
	}{
		// CREATE CONNECTION
		{
			name: "create connection with options",
			node: NewCreateConnectionStmt("src", connOptions(), false),
			want: "CREATE CONNECTION src OPTIONS (host = 'h', port = '5432')",
		},
		{
			name: "create connection if not exists",
			node: NewCreateConnectionStmt("src", connOptions(), true),
			want: "CREATE CONNECTION IF NOT EXISTS src OPTIONS (host = 'h', port = '5432')",
		},
		{
			name: "create connection without options",
			node: NewCreateConnectionStmt("src", nil, false),
			want: "CREATE CONNECTION src",
		},
		// ALTER CONNECTION
		{
			name: "alter connection with options",
			node: NewAlterConnectionStmt("src", connOptions()),
			want: "ALTER CONNECTION src OPTIONS (host = 'h', port = '5432')",
		},
		{
			name: "alter connection without options",
			node: NewAlterConnectionStmt("src", nil),
			want: "ALTER CONNECTION src",
		},
		// DROP CONNECTION
		{
			name: "drop connection single",
			node: NewDropConnectionStmt(NewNodeList(NewString("src")), false),
			want: "DROP CONNECTION src",
		},
		{
			name: "drop connection if exists multiple",
			node: NewDropConnectionStmt(NewNodeList(NewString("a"), NewString("b"), NewString("c")), true),
			want: "DROP CONNECTION IF EXISTS a, b, c",
		},
		// SHOW CONNECTION(S)
		{
			name: "show all connections",
			node: NewShowConnectionsStmt(""),
			want: "SHOW CONNECTIONS",
		},
		{
			name: "show one connection",
			node: NewShowConnectionsStmt("src"),
			want: "SHOW CONNECTION src",
		},
		// CREATE MIGRATION
		{
			name: "create migration for all tables",
			node: NewCreateMigrationStmt("m", "src", &MigrationTables{ForAllTables: true}, nil, false),
			want: "CREATE MIGRATION m CONNECTION src FOR ALL TABLES",
		},
		{
			name: "create migration if not exists with options",
			node: NewCreateMigrationStmt("m", "src", &MigrationTables{ForAllTables: true}, connOptions(), true),
			want: "CREATE MIGRATION IF NOT EXISTS m CONNECTION src FOR ALL TABLES WITH (host = 'h', port = '5432')",
		},
		{
			name: "create migration for table list",
			node: NewCreateMigrationStmt("m", "src", &MigrationTables{Objects: tableObjects("t1", "t2")}, nil, false),
			want: "CREATE MIGRATION m CONNECTION src FOR TABLE t1, t2",
		},
		{
			name: "create migration for tables in schema",
			node: NewCreateMigrationStmt("m", "src",
				&MigrationTables{Objects: NewNodeList(NewPublicationObjSpecName(PUBLICATIONOBJ_TABLES_IN_SCHEMA, "s1"))},
				nil, false),
			want: "CREATE MIGRATION m CONNECTION src FOR TABLES IN SCHEMA s1",
		},
		// ALTER MIGRATION
		{
			name: "alter migration start",
			node: NewAlterMigrationStmt("m", false, &MigrationActionSpec{Action: MigrationActionStart}),
			want: "ALTER MIGRATION m START",
		},
		{
			name: "alter migration if exists activate",
			node: NewAlterMigrationStmt("m", true, &MigrationActionSpec{Action: MigrationActionActivate}),
			want: "ALTER MIGRATION IF EXISTS m ACTIVATE",
		},
		{
			name: "alter migration deactivate",
			node: NewAlterMigrationStmt("m", false, &MigrationActionSpec{Action: MigrationActionDeactivate}),
			want: "ALTER MIGRATION m DEACTIVATE",
		},
		{
			name: "alter migration set connection",
			node: NewAlterMigrationStmt("m", false, &MigrationActionSpec{Action: MigrationActionSetConnection, Connection: "other"}),
			want: "ALTER MIGRATION m CONNECTION other",
		},
		{
			name: "alter migration set options",
			node: NewAlterMigrationStmt("m", false, &MigrationActionSpec{
				Action:  MigrationActionSetOptions,
				Options: NewNodeList(NewDefElem("copy_data", NewString("false"))),
			}),
			want: "ALTER MIGRATION m SET (copy_data = 'false')",
		},
		// DROP MIGRATION
		{
			name: "drop migration plain",
			node: NewDropMigrationStmt(NewNodeList(NewString("m")), false, &MigrationDropBehavior{}),
			want: "DROP MIGRATION m",
		},
		{
			name: "drop migration if exists force",
			node: NewDropMigrationStmt(NewNodeList(NewString("m")), true, &MigrationDropBehavior{Force: true}),
			want: "DROP MIGRATION IF EXISTS m FORCE",
		},
		{
			name: "drop migration wait",
			node: NewDropMigrationStmt(NewNodeList(NewString("m")), false, &MigrationDropBehavior{Wait: true}),
			want: "DROP MIGRATION m WAIT",
		},
		{
			name: "drop migration wait with timeout",
			node: NewDropMigrationStmt(NewNodeList(NewString("m")), false, &MigrationDropBehavior{Wait: true, HasTimeout: true, WaitTimeout: 30}),
			want: "DROP MIGRATION m WAIT (30)",
		},
		// SHOW MIGRATION(S)
		{
			name: "show all migrations",
			node: NewShowMigrationsStmt(""),
			want: "SHOW MIGRATIONS",
		},
		{
			name: "show one migration",
			node: NewShowMigrationsStmt("m"),
			want: "SHOW MIGRATION m",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.node.SqlString())
		})
	}
}

// TestMigrationStatementListHelpers covers the nil/non-String fallback branches
// of the option- and name-list deparse helpers, which the statement grammar
// itself never reaches (it only builds non-nil lists of *String names).
func TestMigrationStatementListHelpers(t *testing.T) {
	assert.Equal(t, "()", optionListSQL(nil))
	assert.Equal(t, "", nameListSQL(nil))
	// A non-*String element falls through to its own SqlString rendering.
	assert.Equal(t, "x", nameListSQL(NewNodeList(NewRangeVar("x", "", ""))))
}

// TestMigrationStatementsStatementType checks the StatementType tag reported by
// each node, which drives gateway routing of these Multigres-only statements.
func TestMigrationStatementsStatementType(t *testing.T) {
	tests := []struct {
		node Node
		want string
	}{
		{NewCreateConnectionStmt("c", nil, false), "CREATE"},
		{NewAlterConnectionStmt("c", nil), "ALTER"},
		{NewDropConnectionStmt(NewNodeList(NewString("c")), false), "DROP"},
		{NewShowConnectionsStmt(""), "SHOW"},
		{NewCreateMigrationStmt("m", "c", &MigrationTables{ForAllTables: true}, nil, false), "CREATE"},
		{NewAlterMigrationStmt("m", false, &MigrationActionSpec{Action: MigrationActionStart}), "ALTER"},
		{NewDropMigrationStmt(NewNodeList(NewString("m")), false, &MigrationDropBehavior{}), "DROP"},
		{NewShowMigrationsStmt(""), "SHOW"},
	}
	for _, tt := range tests {
		st, ok := tt.node.(interface{ StatementType() string })
		if assert.True(t, ok, "node must expose StatementType") {
			assert.Equal(t, tt.want, st.StatementType())
		}
	}
}

// TestMigrationStatementsString checks the diagnostic String() form of each node
// (used in error messages and debug output), including the name and location.
func TestMigrationStatementsString(t *testing.T) {
	assert.Equal(t, "CreateConnectionStmt(src)@0", NewCreateConnectionStmt("src", nil, false).String())
	assert.Equal(t, "AlterConnectionStmt(src)@0", NewAlterConnectionStmt("src", nil).String())
	assert.Equal(t, "DropConnectionStmt@0", NewDropConnectionStmt(NewNodeList(NewString("a")), false).String())
	assert.Equal(t, "ShowConnectionsStmt(src)@0", NewShowConnectionsStmt("src").String())
	assert.Equal(t, "CreateMigrationStmt(m)@0", NewCreateMigrationStmt("m", "src", &MigrationTables{ForAllTables: true}, nil, false).String())
	assert.Equal(t, "AlterMigrationStmt(m)@0", NewAlterMigrationStmt("m", false, &MigrationActionSpec{Action: MigrationActionStart}).String())
	assert.Equal(t, "DropMigrationStmt@0", NewDropMigrationStmt(NewNodeList(NewString("m")), false, &MigrationDropBehavior{}).String())
	assert.Equal(t, "ShowMigrationsStmt(m)@0", NewShowMigrationsStmt("m").String())
}
