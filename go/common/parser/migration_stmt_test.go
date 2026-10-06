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
	assert.Equal(t, "CREATE", cc.StatementType())
	// Round-trips through SqlString and re-parses to the same shape.
	reparsedCC := parseOne(t, cc.SqlString()).(*ast.CreateConnectionStmt)
	assert.Equal(t, "onprem", reparsedCC.Name)
	require.NotNil(t, reparsedCC.Options)
	assert.Len(t, reparsedCC.Options.Items, 2)

	createINE := parseOne(t, "CREATE CONNECTION IF NOT EXISTS onprem OPTIONS (host 'h')")
	ccINE := createINE.(*ast.CreateConnectionStmt)
	assert.True(t, ccINE.IfNotExists)
	assert.Contains(t, ccINE.SqlString(), "IF NOT EXISTS")

	drop := parseOne(t, "DROP CONNECTION onprem")
	dc, ok := drop.(*ast.DropConnectionStmt)
	require.True(t, ok, "got %T", drop)
	assert.False(t, dc.IfExists)
	assert.Len(t, dc.Names.Items, 1)
	assert.Equal(t, "DROP", dc.StatementType())
	assert.Equal(t, "DROP CONNECTION onprem", dc.SqlString())

	dropIE := parseOne(t, "DROP CONNECTION IF EXISTS a, b")
	dc2 := dropIE.(*ast.DropConnectionStmt)
	assert.True(t, dc2.IfExists)
	assert.Len(t, dc2.Names.Items, 2)
	assert.Equal(t, "DROP CONNECTION IF EXISTS a, b", dc2.SqlString())

	show := parseOne(t, "SHOW CONNECTION onprem")
	sc, ok := show.(*ast.ShowConnectionsStmt)
	require.True(t, ok, "got %T", show)
	assert.Equal(t, "onprem", sc.Name)
	assert.Equal(t, "SHOW", sc.StatementType())
	assert.Equal(t, "SHOW CONNECTION onprem", sc.SqlString())

	// The bare plural "SHOW CONNECTIONS" has no dedicated grammar production —
	// it parses as an ordinary SHOW <guc-name> (see
	// docs/migration/migrator_sql_interface.md) — so the no-name branch of
	// ShowConnectionsStmt.SqlString is only reachable via direct construction.
	showAll := ast.NewShowConnectionsStmt("")
	assert.Equal(t, "SHOW CONNECTIONS", showAll.SqlString())
}

// TestParseAlterConnectionIsGone is the regression test for the removal of
// ALTER CONNECTION and UpdateConnection: connections are immutable once
// created (update is DROP + CREATE), so the statement must be a syntax error,
// not merely unimplemented.
func TestParseAlterConnectionIsGone(t *testing.T) {
	_, err := ParseSQL("ALTER CONNECTION onprem OPTIONS (SET host 'db2.example.com')")
	require.Error(t, err)
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
		sql string
		dir ast.MigrationDirection
	}{
		{"ALTER MIGRATION m PHASE IMPORT", ast.MigrationDirectionImport},
		{"ALTER MIGRATION m PHASE EXPORT", ast.MigrationDirectionExport},
	}
	for _, c := range cases {
		stmt := parseOne(t, c.sql)
		am, ok := stmt.(*ast.AlterMigrationStmt)
		require.True(t, ok, "got %T for %q", stmt, c.sql)
		assert.Equal(t, "m", am.Name)
		assert.Equal(t, c.dir, am.Direction, c.sql)
	}

	ie := parseOne(t, "ALTER MIGRATION IF EXISTS m PHASE IMPORT")
	assert.True(t, ie.(*ast.AlterMigrationStmt).IfExists)
}

// TestParseAlterMigrationSetConnectionIsGone is the regression test for the
// removal of UpdateMigration: ALTER MIGRATION's CONNECTION/SET subcommands
// (re-pointing the source connection, or changing sequence_margin/objects in
// place) are a candidate for future reintroduction (see the
// migration-foundation RFC) but are not currently implemented, so both must
// be syntax errors, not merely unimplemented — PHASE is the only
// ALTER MIGRATION action today.
func TestParseAlterMigrationSetConnectionIsGone(t *testing.T) {
	_, err := ParseSQL("ALTER MIGRATION m CONNECTION other")
	require.Error(t, err)

	_, err = ParseSQL("ALTER MIGRATION m SET (sequence_margin = 10)")
	require.Error(t, err)
}

// TestParseAlterMigrationPhaseCondOptions covers PHASE EXPORT's WHEN (cond)
// readiness gate and WITH (...) options (wait_timeout), and their SqlString
// round-trip.
func TestParseAlterMigrationPhaseCondOptions(t *testing.T) {
	// Bare PHASE EXPORT still parses with no condition or options.
	bare := parseOne(t, "ALTER MIGRATION m PHASE EXPORT").(*ast.AlterMigrationStmt)
	assert.Equal(t, ast.MigrationDirectionExport, bare.Direction)
	assert.Nil(t, bare.When)
	assert.Nil(t, bare.Options)
	assert.Equal(t, "ALTER MIGRATION m PHASE EXPORT", bare.SqlString())

	withCond := parseOne(t, "ALTER MIGRATION m PHASE EXPORT WHEN (lag_bytes < 8388608) WITH (wait_timeout = '30s')").(*ast.AlterMigrationStmt)
	require.NotNil(t, withCond.When)
	assert.Equal(t, "lag_bytes", withCond.When.Field)
	assert.Equal(t, ast.MigrationCondLT, withCond.When.Op)
	require.NotNil(t, withCond.Options)
	assert.Len(t, withCond.Options.Items, 1)
	// Round-trips through SqlString and re-parses to the same shape.
	rt := withCond.SqlString()
	reparsed := parseOne(t, rt).(*ast.AlterMigrationStmt)
	require.NotNil(t, reparsed.When)
	assert.Equal(t, "lag_bytes", reparsed.When.Field)
	assert.Equal(t, ast.MigrationCondLT, reparsed.When.Op)
	require.NotNil(t, reparsed.Options)
	assert.Len(t, reparsed.Options.Items, 1)

	// A not-yet-backed field/operator combination still parses (the gateway
	// rejects it at runtime with a typed feature_not_supported error, not the
	// parser).
	otherField := parseOne(t, "ALTER MIGRATION m PHASE EXPORT WHEN (total_relations >= 10)").(*ast.AlterMigrationStmt)
	require.NotNil(t, otherField.When)
	assert.Equal(t, "total_relations", otherField.When.Field)
	assert.Equal(t, ast.MigrationCondGE, otherField.When.Op)
}

func TestParseDropMigration(t *testing.T) {
	plain := parseOne(t, "DROP MIGRATION m").(*ast.DropMigrationStmt)
	assert.False(t, plain.Force)
	assert.Nil(t, plain.When)
	assert.Len(t, plain.Names.Items, 1)

	force := parseOne(t, "DROP MIGRATION m FORCE").(*ast.DropMigrationStmt)
	assert.True(t, force.Force)

	when := parseOne(t, "DROP MIGRATION m WHEN (lag_bytes = 0)").(*ast.DropMigrationStmt)
	require.NotNil(t, when.When)
	assert.Equal(t, "lag_bytes", when.When.Field)
	assert.Equal(t, ast.MigrationCondEQ, when.When.Op)
	assert.Nil(t, when.Options)

	whenWithTimeout := parseOne(t, "DROP MIGRATION m WHEN (lag_bytes = 0) WITH (wait_timeout = 30)").(*ast.DropMigrationStmt)
	require.NotNil(t, whenWithTimeout.When)
	require.NotNil(t, whenWithTimeout.Options)
	assert.Len(t, whenWithTimeout.Options.Items, 1)

	ie := parseOne(t, "DROP MIGRATION IF EXISTS a, b").(*ast.DropMigrationStmt)
	assert.True(t, ie.IfExists)
	assert.Len(t, ie.Names.Items, 2)
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

	// ALTER MIGRATION IF EXISTS on the shared migration_action nonterminal
	// (see TestParseAlterMigration's "ie" case for the non-IF-EXISTS form).
	altPhase := parseOne(t, "ALTER MIGRATION IF EXISTS m PHASE EXPORT WITH (wait_timeout = 5)").(*ast.AlterMigrationStmt)
	assert.True(t, altPhase.IfExists)
	assert.Equal(t, ast.MigrationDirectionExport, altPhase.Direction)
	require.NotNil(t, altPhase.Options)

	// DROP MIGRATION IF EXISTS with each behavior (shared
	// migration_drop_behavior nonterminal on the IF EXISTS production).
	dropForce := parseOne(t, "DROP MIGRATION IF EXISTS m FORCE").(*ast.DropMigrationStmt)
	assert.True(t, dropForce.IfExists)
	assert.True(t, dropForce.Force)

	dropWhen := parseOne(t, "DROP MIGRATION IF EXISTS m WHEN (lag_bytes = 0) WITH (wait_timeout = 5)").(*ast.DropMigrationStmt)
	assert.True(t, dropWhen.IfExists)
	require.NotNil(t, dropWhen.When)
	require.NotNil(t, dropWhen.Options)
	assert.Len(t, dropWhen.Options.Items, 1)
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
