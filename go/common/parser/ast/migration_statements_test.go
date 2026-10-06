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

// TestMigrationStatementsStringers covers the debug String() methods and
// StatementType() on the migration/connection statement nodes: these are
// exercised by neither the grammar parse tests (which only check parsed
// fields) nor the SqlString deparse tests (which check the rendered SQL, a
// different method).
func TestMigrationStatementsStringers(t *testing.T) {
	cc := NewCreateConnectionStmt("src", nil, false)
	assert.Contains(t, cc.String(), "CreateConnectionStmt(src)")
	assert.Equal(t, "CREATE", cc.StatementType())

	dc := NewDropConnectionStmt(NewNodeList(NewString("src")), false)
	assert.Contains(t, dc.String(), "DropConnectionStmt")
	assert.Equal(t, "DROP", dc.StatementType())

	sc := NewShowConnectionsStmt("src")
	assert.Contains(t, sc.String(), "ShowConnectionsStmt(src)")
	assert.Equal(t, "SHOW", sc.StatementType())

	cm := NewCreateMigrationStmt("m", "src", &MigrationTables{ForAllTables: true}, nil, false)
	assert.Contains(t, cm.String(), "CreateMigrationStmt(m)")
	assert.Equal(t, "CREATE", cm.StatementType())

	am := NewAlterMigrationStmt("m", false, &MigrationActionSpec{Direction: MigrationDirectionExport})
	assert.Contains(t, am.String(), "AlterMigrationStmt(m)")
	assert.Equal(t, "ALTER", am.StatementType())

	dm := NewDropMigrationStmt(NewNodeList(NewString("m")), false, &MigrationDropBehavior{})
	assert.Contains(t, dm.String(), "DropMigrationStmt")
	assert.Equal(t, "DROP", dm.StatementType())
}

// TestRedactIfCredentialBearing covers both branches: a CREATE CONNECTION
// statement (whose raw SQL carries a password literal) is redacted, and every
// other statement type is left alone. Every query-logging call site must
// redact through this function rather than reimplementing the check, so
// coverage here guards against a password ever reaching logs.
func TestRedactIfCredentialBearing(t *testing.T) {
	cc := NewCreateConnectionStmt("src", nil, false)
	redacted, ok := RedactIfCredentialBearing(cc)
	assert.True(t, ok)
	assert.Equal(t, "CREATE CONNECTION src <redacted>", redacted)

	dm := NewDropMigrationStmt(NewNodeList(NewString("m")), false, &MigrationDropBehavior{})
	redacted, ok = RedactIfCredentialBearing(dm)
	assert.False(t, ok)
	assert.Empty(t, redacted)
}

// TestDropMigrationStmtSqlString covers the DROP MIGRATION deparse path across
// its IfExists/Force/When/Options combinations; the parser grammar tests only
// assert the parsed fields, never SqlString's rendered output.
func TestDropMigrationStmtSqlString(t *testing.T) {
	tests := []struct {
		name string
		stmt *DropMigrationStmt
		want string
	}{
		{
			name: "plain",
			stmt: NewDropMigrationStmt(NewNodeList(NewString("m")), false, &MigrationDropBehavior{}),
			want: "DROP MIGRATION m",
		},
		{
			name: "if exists, multiple names",
			stmt: NewDropMigrationStmt(NewNodeList(NewString("a"), NewString("b")), true, &MigrationDropBehavior{}),
			want: "DROP MIGRATION IF EXISTS a, b",
		},
		{
			name: "force",
			stmt: NewDropMigrationStmt(NewNodeList(NewString("m")), false, &MigrationDropBehavior{Force: true}),
			want: "DROP MIGRATION m FORCE",
		},
		{
			name: "when",
			stmt: NewDropMigrationStmt(NewNodeList(NewString("m")), false, &MigrationDropBehavior{
				When: &MigrationCond{Field: "lag_bytes", Op: MigrationCondEQ, Value: NewInteger(0)},
			}),
			want: "DROP MIGRATION m WHEN (lag_bytes = 0)",
		},
		{
			name: "when with options",
			stmt: NewDropMigrationStmt(NewNodeList(NewString("m")), false, &MigrationDropBehavior{
				When:    &MigrationCond{Field: "lag_bytes", Op: MigrationCondEQ, Value: NewInteger(0)},
				Options: NewNodeList(NewDefElem("wait_timeout", NewInteger(30))),
			}),
			want: "DROP MIGRATION m WHEN (lag_bytes = 0) WITH (wait_timeout = 30)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.stmt.SqlString())
		})
	}
}

// TestMigrationDirectionAndCondOpString covers the default ("unknown value")
// branch of each stringer, which no grammar input can reach — the grammar
// only ever produces the named constants.
func TestMigrationDirectionAndCondOpString(t *testing.T) {
	assert.Equal(t, "IMPORT", MigrationDirectionImport.String())
	assert.Equal(t, "EXPORT", MigrationDirectionExport.String())
	assert.Empty(t, MigrationDirection(99).String())

	assert.Equal(t, "<", MigrationCondLT.String())
	assert.Equal(t, "<=", MigrationCondLE.String())
	assert.Equal(t, "=", MigrationCondEQ.String())
	assert.Equal(t, ">=", MigrationCondGE.String())
	assert.Equal(t, ">", MigrationCondGT.String())
	assert.Equal(t, "<>", MigrationCondNE.String())
	assert.Equal(t, "?", MigrationCondOp(99).String())
}

// TestOptionListHelpers covers the nil-list and non-default-action branches of
// the WITH/OPTIONS rendering helpers that no longer have a reachable grammar
// path: connectionOptionSQL's ADD/SET/DROP actions were only ever driven by
// ALTER CONNECTION, which is gone (see TestParseAlterConnectionIsGone) — the
// cases are kept for a possible future reintroduction (see
// MigrationActionSpec's doc comment) and are tested directly here instead.
func TestOptionListHelpers(t *testing.T) {
	assert.Equal(t, "()", optionListSQL(nil))
	assert.Equal(t, "()", connectionOptionListSQL(nil))
	assert.Empty(t, nameListSQL(nil))

	// A non-DefElem item is skipped rather than rendered.
	assert.Equal(t, "()", connectionOptionListSQL(NewNodeList(NewInteger(1))))

	assert.Equal(t, "ADD host 'h'", connectionOptionSQL(NewDefElemExtended("", "host", NewString("h"), DEFELEM_ADD)))
	assert.Equal(t, "SET host 'h'", connectionOptionSQL(NewDefElemExtended("", "host", NewString("h"), DEFELEM_SET)))
	assert.Equal(t, "DROP host", connectionOptionSQL(NewDefElemExtended("", "host", nil, DEFELEM_DROP)))

	// A non-*String item falls back to its own SqlString.
	assert.Equal(t, "1", nameListSQL(NewNodeList(NewInteger(1))))
}
