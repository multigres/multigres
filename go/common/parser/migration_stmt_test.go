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

	"github.com/multigres/multigres/go/common/parser/ast"
)

// parseOne parses a single statement and returns it, failing the test on error
// or on a statement count other than one.
func parseOne(t *testing.T, sql string) ast.Stmt {
	t.Helper()
	stmts, err := ParseSQL(sql)
	require.NoError(t, err, "parse %q", sql)
	require.Len(t, stmts, 1, "one statement for %q", sql)
	return stmts[0]
}

func TestParseConnectionStatements(t *testing.T) {
	create := parseOne(t, "CREATE CONNECTION onprem OPTIONS (host 'db.example.com', dbname 'app')")
	cc, ok := create.(*ast.CreateConnectionStmt)
	require.True(t, ok, "got %T", create)
	assert.Equal(t, "onprem", cc.Name)
	assert.False(t, cc.IfNotExists)
	require.NotNil(t, cc.Options)
	assert.Len(t, cc.Options.Items, 2)

	createINE := parseOne(t, "CREATE CONNECTION IF NOT EXISTS onprem OPTIONS (host 'h')")
	assert.True(t, createINE.(*ast.CreateConnectionStmt).IfNotExists)

	alter := parseOne(t, "ALTER CONNECTION onprem OPTIONS (SET host 'db2.example.com')")
	ac, ok := alter.(*ast.AlterConnectionStmt)
	require.True(t, ok, "got %T", alter)
	assert.Equal(t, "onprem", ac.Name)
	require.NotNil(t, ac.Options)

	drop := parseOne(t, "DROP CONNECTION onprem")
	dc, ok := drop.(*ast.DropConnectionStmt)
	require.True(t, ok, "got %T", drop)
	assert.False(t, dc.IfExists)
	assert.Len(t, dc.Names.Items, 1)

	dropIE := parseOne(t, "DROP CONNECTION IF EXISTS a, b")
	dc2 := dropIE.(*ast.DropConnectionStmt)
	assert.True(t, dc2.IfExists)
	assert.Len(t, dc2.Names.Items, 2)

	show := parseOne(t, "SHOW CONNECTION onprem")
	sc, ok := show.(*ast.ShowConnectionsStmt)
	require.True(t, ok, "got %T", show)
	assert.Equal(t, "onprem", sc.Name)
}

func TestParseCreateMigration(t *testing.T) {
	all := parseOne(t, "CREATE MIGRATION m CONNECTION onprem FOR ALL TABLES")
	cm, ok := all.(*ast.CreateMigrationStmt)
	require.True(t, ok, "got %T", all)
	assert.Equal(t, "m", cm.Name)
	assert.Equal(t, "onprem", cm.Connection)
	assert.True(t, cm.ForAllTables)
	assert.Nil(t, cm.Objects)

	tables := parseOne(t, "CREATE MIGRATION m CONNECTION onprem FOR TABLE orders, customers WITH (copy_data = true, sequence_margin = 1000)")
	cm2 := tables.(*ast.CreateMigrationStmt)
	assert.False(t, cm2.ForAllTables)
	require.NotNil(t, cm2.Objects)
	assert.Len(t, cm2.Objects.Items, 2)
	require.NotNil(t, cm2.Options)
	assert.Len(t, cm2.Options.Items, 2)

	schema := parseOne(t, "CREATE MIGRATION m CONNECTION onprem FOR TABLES IN SCHEMA public")
	cm3 := schema.(*ast.CreateMigrationStmt)
	require.NotNil(t, cm3.Objects)
	assert.Len(t, cm3.Objects.Items, 1)

	ine := parseOne(t, "CREATE MIGRATION IF NOT EXISTS m CONNECTION onprem FOR ALL TABLES")
	assert.True(t, ine.(*ast.CreateMigrationStmt).IfNotExists)

	qualified := parseOne(t, "CREATE MIGRATION m CONNECTION onprem FOR TABLE sales.orders (id, total) WHERE (total > 0)")
	cm4 := qualified.(*ast.CreateMigrationStmt)
	require.NotNil(t, cm4.Objects)
	assert.Len(t, cm4.Objects.Items, 1)
}

func TestParseAlterMigration(t *testing.T) {
	cases := []struct {
		sql    string
		action ast.MigrationAction
		conn   string
	}{
		{"ALTER MIGRATION m START", ast.MigrationActionStart, ""},
		{"ALTER MIGRATION m ACTIVATE", ast.MigrationActionActivate, ""},
		{"ALTER MIGRATION m DEACTIVATE", ast.MigrationActionDeactivate, ""},
		{"ALTER MIGRATION m CONNECTION other", ast.MigrationActionSetConnection, "other"},
		{"ALTER MIGRATION m SET (sequence_margin = 10)", ast.MigrationActionSetOptions, ""},
	}
	for _, c := range cases {
		stmt := parseOne(t, c.sql)
		am, ok := stmt.(*ast.AlterMigrationStmt)
		require.True(t, ok, "got %T for %q", stmt, c.sql)
		assert.Equal(t, "m", am.Name)
		assert.Equal(t, c.action, am.Action, c.sql)
		assert.Equal(t, c.conn, am.Connection, c.sql)
	}

	ie := parseOne(t, "ALTER MIGRATION IF EXISTS m START")
	assert.True(t, ie.(*ast.AlterMigrationStmt).IfExists)
}

func TestParseDropAndShowMigration(t *testing.T) {
	plain := parseOne(t, "DROP MIGRATION m").(*ast.DropMigrationStmt)
	assert.False(t, plain.Force)
	assert.False(t, plain.Wait)
	assert.Len(t, plain.Names.Items, 1)

	force := parseOne(t, "DROP MIGRATION m FORCE").(*ast.DropMigrationStmt)
	assert.True(t, force.Force)

	wait := parseOne(t, "DROP MIGRATION m WAIT").(*ast.DropMigrationStmt)
	assert.True(t, wait.Wait)
	assert.False(t, wait.HasTimeout)

	waitN := parseOne(t, "DROP MIGRATION m WAIT (30)").(*ast.DropMigrationStmt)
	assert.True(t, waitN.Wait)
	assert.True(t, waitN.HasTimeout)
	assert.Equal(t, 30, waitN.WaitTimeout)

	ie := parseOne(t, "DROP MIGRATION IF EXISTS a, b").(*ast.DropMigrationStmt)
	assert.True(t, ie.IfExists)
	assert.Len(t, ie.Names.Items, 2)

	show := parseOne(t, "SHOW MIGRATION orders_move").(*ast.ShowMigrationsStmt)
	assert.Equal(t, "orders_move", show.Name)
}

// TestParseMigrationFactoredForms exercises the factored grammar nonterminals
// (migration_tables, migration_action, migration_drop_behavior) across both the
// plain and IF [NOT] EXISTS productions, plus a mixed FOR object list.
func TestParseMigrationFactoredForms(t *testing.T) {
	// Mixed FOR list: tables and a schema interleaved (pub_obj_list with
	// CONTINUATION resolution).
	mixed := parseOne(t, "CREATE MIGRATION m CONNECTION c FOR TABLE a, TABLES IN SCHEMA s, TABLE b").(*ast.CreateMigrationStmt)
	assert.False(t, mixed.ForAllTables)
	require.NotNil(t, mixed.Objects)
	assert.Len(t, mixed.Objects.Items, 3)

	// ALTER MIGRATION IF EXISTS with the parameterized actions (shared
	// migration_action nonterminal on the IF EXISTS production).
	altConn := parseOne(t, "ALTER MIGRATION IF EXISTS m CONNECTION other").(*ast.AlterMigrationStmt)
	assert.True(t, altConn.IfExists)
	assert.Equal(t, ast.MigrationActionSetConnection, altConn.Action)
	assert.Equal(t, "other", altConn.Connection)

	altSet := parseOne(t, "ALTER MIGRATION IF EXISTS m SET (sequence_margin = 3)").(*ast.AlterMigrationStmt)
	assert.True(t, altSet.IfExists)
	assert.Equal(t, ast.MigrationActionSetOptions, altSet.Action)
	require.NotNil(t, altSet.Options)

	// DROP MIGRATION IF EXISTS with each behavior (shared
	// migration_drop_behavior nonterminal on the IF EXISTS production).
	dropForce := parseOne(t, "DROP MIGRATION IF EXISTS m FORCE").(*ast.DropMigrationStmt)
	assert.True(t, dropForce.IfExists)
	assert.True(t, dropForce.Force)

	dropWaitN := parseOne(t, "DROP MIGRATION IF EXISTS m WAIT (5)").(*ast.DropMigrationStmt)
	assert.True(t, dropWaitN.IfExists)
	assert.True(t, dropWaitN.Wait)
	assert.True(t, dropWaitN.HasTimeout)
	assert.Equal(t, 5, dropWaitN.WaitTimeout)
}

// TestMigrationsIdentifierStillWorks makes sure "migrations"/"connections" are
// still usable as ordinary identifiers (they are NOT keywords).
func TestMigrationsIdentifierStillWorks(t *testing.T) {
	for _, sql := range []string{
		"SELECT * FROM migrations",
		"SELECT * FROM connections",
		"SELECT migration, connection FROM t",
	} {
		_, err := ParseSQL(sql)
		assert.NoError(t, err, sql)
	}
}
