// PostgreSQL Database Management System
// (also known as Postgres, formerly known as Postgres95)
//
//	Portions Copyright (c) 2026, Supabase, Inc
//
//	Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//
//	Portions Copyright (c) 1994, The Regents of the University of California
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written agreement
// is hereby granted, provided that the above copyright notice and this
// paragraph and the following two paragraphs appear in all copies.

/*
 * Replication-protocol Parser - Command Parsing Tests
 *
 * Coverage:
 *   - Each in-scope replication command (IDENTIFY_SYSTEM, READ_REPLICATION_SLOT,
 *     SHOW, CREATE_REPLICATION_SLOT with parenthesized + legacy options,
 *     DROP_REPLICATION_SLOT with/without WAIT, ALTER_REPLICATION_SLOT,
 *     START_REPLICATION).
 *   - Out-of-scope physical commands are rejected with a syntax error.
 *   - IsReplicationCommand peek-only behavior matching PG's
 *     replication_scanner_is_replication_command.
 *
 * Style mirrors go/common/parser/lexer_strings_test.go: table-driven cases
 * with named t.Run subtests and testify assertions.
 */

package replparser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser/ast"
)

// TestParseReplicationCommand_Success covers each in-scope command and
// asserts both the concrete AST type and the salient fields. Patterned
// after go/common/parser/lexer_strings_test.go's table-driven style.
func TestParseReplicationCommand_Success(t *testing.T) {
	t.Run("IDENTIFY_SYSTEM", func(t *testing.T) {
		// repl_gram.y:127-132
		tests := []struct {
			name string
			in   string
		}{
			{"bare", "IDENTIFY_SYSTEM"},
			{"with trailing semicolon", "IDENTIFY_SYSTEM;"},
			{"lowercase", "identify_system"},
			{"with whitespace", "  IDENTIFY_SYSTEM\n"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				stmt, err := ParseReplicationCommand(tt.in)
				require.NoError(t, err)
				_, ok := stmt.(*ast.IdentifySystemCmd)
				assert.True(t, ok, "want *ast.IdentifySystemCmd, got %T", stmt)
			})
		}
	})

	t.Run("READ_REPLICATION_SLOT", func(t *testing.T) {
		// repl_gram.y:137-144
		stmt, err := ParseReplicationCommand("READ_REPLICATION_SLOT s1")
		require.NoError(t, err)
		c, ok := stmt.(*ast.ReadReplicationSlotCmd)
		require.True(t, ok, "got %T", stmt)
		assert.Equal(t, "s1", c.SlotName)
	})

	t.Run("SHOW", func(t *testing.T) {
		// repl_gram.y:149-160 — note SHOW returns *VariableShowStmt, not a
		// dedicated replication node, so the SQL SHOW path can handle it.
		tests := []struct {
			name string
			in   string
			want string
		}{
			{"unqualified", "SHOW server_version", "server_version"},
			{"qualified", "SHOW pg_catalog.server_version", "pg_catalog.server_version"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				stmt, err := ParseReplicationCommand(tt.in)
				require.NoError(t, err)
				c, ok := stmt.(*ast.VariableShowStmt)
				require.True(t, ok, "got %T", stmt)
				assert.Equal(t, tt.want, c.Name)
			})
		}
	})

	t.Run("CREATE_REPLICATION_SLOT parenthesized options", func(t *testing.T) {
		// repl_gram.y:192-202, with parenthesized options (repl_gram.y:206-208)
		stmt, err := ParseReplicationCommand(
			"CREATE_REPLICATION_SLOT s1 TEMPORARY LOGICAL pgoutput (two_phase 'true')")
		require.NoError(t, err)
		c, ok := stmt.(*ast.CreateReplicationSlotCmd)
		require.True(t, ok, "got %T", stmt)
		assert.Equal(t, "s1", c.SlotName)
		assert.Equal(t, "pgoutput", c.Plugin)
		assert.True(t, c.Temporary)
		assert.Equal(t, ast.ReplicationKindLogical, c.Kind)
		require.Len(t, c.Options, 1)
		assert.Equal(t, "two_phase", c.Options[0].Defname)
	})

	t.Run("CREATE_REPLICATION_SLOT legacy options", func(t *testing.T) {
		// repl_gram.y:217-243 — legacy unparenthesized options.
		stmt, err := ParseReplicationCommand(
			"CREATE_REPLICATION_SLOT s1 LOGICAL pgoutput RESERVE_WAL TWO_PHASE")
		require.NoError(t, err)
		c, ok := stmt.(*ast.CreateReplicationSlotCmd)
		require.True(t, ok, "got %T", stmt)
		assert.False(t, c.Temporary)
		require.Len(t, c.Options, 2)
		assert.Equal(t, "reserve_wal", c.Options[0].Defname)
		assert.Equal(t, "two_phase", c.Options[1].Defname)
	})

	t.Run("DROP_REPLICATION_SLOT", func(t *testing.T) {
		// repl_gram.y:246-263
		tests := []struct {
			name     string
			in       string
			wantWait bool
		}{
			{"without WAIT", "DROP_REPLICATION_SLOT s1", false},
			{"with WAIT", "DROP_REPLICATION_SLOT s1 WAIT", true},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				stmt, err := ParseReplicationCommand(tt.in)
				require.NoError(t, err)
				c, ok := stmt.(*ast.DropReplicationSlotCmd)
				require.True(t, ok, "got %T", stmt)
				assert.Equal(t, "s1", c.SlotName)
				assert.Equal(t, tt.wantWait, c.Wait)
			})
		}
	})

	t.Run("ALTER_REPLICATION_SLOT", func(t *testing.T) {
		// repl_gram.y:266-275
		stmt, err := ParseReplicationCommand("ALTER_REPLICATION_SLOT s1 (failover 'true')")
		require.NoError(t, err)
		c, ok := stmt.(*ast.AlterReplicationSlotCmd)
		require.True(t, ok, "got %T", stmt)
		assert.Equal(t, "s1", c.SlotName)
		require.Len(t, c.Options, 1)
		assert.Equal(t, "failover", c.Options[0].Defname)
	})

	t.Run("START_REPLICATION", func(t *testing.T) {
		// repl_gram.y:294-306 — LOGICAL only in our scope.
		tests := []struct {
			name           string
			in             string
			wantSlot       string
			wantStartPoint ast.XLogRecPtr
			wantOptCount   int
		}{
			{
				name:           "no options at 0/0",
				in:             "START_REPLICATION SLOT test_slot LOGICAL 0/0",
				wantSlot:       "test_slot",
				wantStartPoint: 0,
				wantOptCount:   0,
			},
			{
				name:           "with plugin options",
				in:             `START_REPLICATION SLOT s1 LOGICAL 0/16B3748 (proto_version '1', publication_names 'pub1')`,
				wantSlot:       "s1",
				wantStartPoint: 0x16B3748,
				wantOptCount:   2,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				stmt, err := ParseReplicationCommand(tt.in)
				require.NoError(t, err)
				c, ok := stmt.(*ast.StartReplicationCmd)
				require.True(t, ok, "got %T", stmt)
				assert.Equal(t, tt.wantSlot, c.SlotName)
				assert.Equal(t, tt.wantStartPoint, c.StartPoint)
				assert.Equal(t, ast.ReplicationKindLogical, c.Kind)
				assert.Len(t, c.Options, tt.wantOptCount)
			})
		}
	})
}

// TestParseReplicationCommand_RejectsPhysical verifies that physical-only
// commands and physical START_REPLICATION are out-of-scope syntax errors —
// the grammar in repl_gram.y for these productions was intentionally dropped
// in our port.
func TestParseReplicationCommand_RejectsPhysical(t *testing.T) {
	tests := []struct {
		name string
		in   string
	}{
		{"BASE_BACKUP", "BASE_BACKUP"},
		{"TIMELINE_HISTORY", "TIMELINE_HISTORY 1"},
		{"UPLOAD_MANIFEST", "UPLOAD_MANIFEST"},
		{"physical CREATE_REPLICATION_SLOT", "CREATE_REPLICATION_SLOT s1 PHYSICAL"},
		{"physical START_REPLICATION", "START_REPLICATION 0/0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseReplicationCommand(tt.in)
			assert.Error(t, err, "expected syntax error for %q", tt.in)
		})
	}
}

// TestParseReplicationCommand_GarbageInput verifies that non-replication
// input is rejected. (IsReplicationCommand is the cheaper gate; this
// confirms ParseReplicationCommand is also defensive.)
func TestParseReplicationCommand_GarbageInput(t *testing.T) {
	tests := []struct {
		name string
		in   string
	}{
		{"empty", ""},
		{"SQL select", "SELECT 1"},
		{"unterminated literal", `'unterminated`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseReplicationCommand(tt.in)
			assert.Error(t, err)
		})
	}
}

// TestIsReplicationCommand covers the first-token peek that the gateway
// uses to gate dispatch. Mirrors PG's replication_scanner_is_replication_command
// (repl_scanner.l:294-318).
func TestIsReplicationCommand(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want bool
	}{
		{"IDENTIFY_SYSTEM upper", "IDENTIFY_SYSTEM", true},
		{"IDENTIFY_SYSTEM lower", "identify_system", true},
		{"CREATE_REPLICATION_SLOT", "CREATE_REPLICATION_SLOT s1 LOGICAL pgoutput", true},
		{"DROP_REPLICATION_SLOT", "DROP_REPLICATION_SLOT s1", true},
		{"ALTER_REPLICATION_SLOT", "ALTER_REPLICATION_SLOT s1 (a 'b')", true},
		{"READ_REPLICATION_SLOT", "READ_REPLICATION_SLOT s1", true},
		{"START_REPLICATION", "START_REPLICATION SLOT s1 LOGICAL 0/0", true},
		{"SHOW", "SHOW server_version", true},
		{"SELECT 1", "SELECT 1", false},
		{"empty input", "", false},
		{"SQL-style comment", "-- a comment", false},
		// Physical-only keywords were intentionally NOT included in our
		// keyword map, so they tokenize as IDENT and IsReplicationCommand
		// returns false. (PG would return true; we don't, because we'd
		// also fail to parse them later.)
		{"BASE_BACKUP (out of scope)", "BASE_BACKUP", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsReplicationCommand(tt.in))
		})
	}
}
