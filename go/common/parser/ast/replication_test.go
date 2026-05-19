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
//
// IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
// DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
// LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
// DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.
//
// THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
// AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
// ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
// PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.

// Tests for replication command nodes (parser/ast/replication.go).
// Mirrors the style of utility_statements_test.go: table-driven where
// possible, t.Run subtests per node, testify assertions, and interface-
// compliance checks at the end of each node's subtest.

package ast

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestReplicationKind verifies the iota values follow PG's REPLICATION_KIND_*
// (replnodes.h:20-24): PHYSICAL=0, LOGICAL=1.
func TestReplicationKind(t *testing.T) {
	tests := []struct {
		kind ReplicationKind
		want int
	}{
		{ReplicationKindPhysical, 0},
		{ReplicationKindLogical, 1},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, int(tt.kind))
	}
}

// TestReplicationCmds groups the six logical-replication command nodes.
// Each subtest covers: NodeTag, field round-tripping, StatementType,
// SqlString, String, and interface compliance.
func TestReplicationCmds(t *testing.T) {
	t.Run("IdentifySystemCmd", func(t *testing.T) {
		stmt := NewIdentifySystemCmd()

		assert.Equal(t, T_IdentifySystemCmd, stmt.NodeTag())
		assert.Equal(t, "IDENTIFY_SYSTEM", stmt.StatementType())
		assert.Equal(t, "IDENTIFY_SYSTEM", stmt.SqlString())
		assert.Contains(t, stmt.String(), "T_IdentifySystemCmd")

		// Interface compliance
		var _ Node = stmt
		var _ Stmt = stmt
	})

	t.Run("CreateReplicationSlotCmd", func(t *testing.T) {
		opts := []*DefElem{NewDefElem("two_phase", NewBoolean(true))}
		stmt := NewCreateReplicationSlotCmd("slot1", ReplicationKindLogical, "pgoutput", true)
		stmt.Options = opts

		assert.Equal(t, T_CreateReplicationSlotCmd, stmt.NodeTag())
		assert.Equal(t, "CREATE_REPLICATION_SLOT", stmt.StatementType())
		assert.Equal(t, "CREATE_REPLICATION_SLOT", stmt.SqlString())
		assert.Equal(t, "slot1", stmt.SlotName)
		assert.Equal(t, ReplicationKindLogical, stmt.Kind)
		assert.Equal(t, "pgoutput", stmt.Plugin)
		assert.True(t, stmt.Temporary)
		assert.Equal(t, opts, stmt.Options)
		assert.Contains(t, stmt.String(), "T_CreateReplicationSlotCmd")

		// Defaults: a non-temporary, no-options construct.
		bare := NewCreateReplicationSlotCmd("s", ReplicationKindLogical, "p", false)
		assert.False(t, bare.Temporary)
		assert.Nil(t, bare.Options)

		var _ Node = stmt
		var _ Stmt = stmt
	})

	t.Run("DropReplicationSlotCmd", func(t *testing.T) {
		tests := []struct {
			name string
			slot string
			wait bool
		}{
			{"with WAIT", "slot1", true},
			{"without WAIT", "slot2", false},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				stmt := NewDropReplicationSlotCmd(tt.slot, tt.wait)

				assert.Equal(t, T_DropReplicationSlotCmd, stmt.NodeTag())
				assert.Equal(t, "DROP_REPLICATION_SLOT", stmt.StatementType())
				assert.Equal(t, "DROP_REPLICATION_SLOT", stmt.SqlString())
				assert.Equal(t, tt.slot, stmt.SlotName)
				assert.Equal(t, tt.wait, stmt.Wait)

				var _ Node = stmt
				var _ Stmt = stmt
			})
		}
	})

	t.Run("AlterReplicationSlotCmd", func(t *testing.T) {
		opts := []*DefElem{NewDefElem("failover", NewBoolean(true))}
		stmt := NewAlterReplicationSlotCmd("slot1", opts)

		assert.Equal(t, T_AlterReplicationSlotCmd, stmt.NodeTag())
		assert.Equal(t, "ALTER_REPLICATION_SLOT", stmt.StatementType())
		assert.Equal(t, "ALTER_REPLICATION_SLOT", stmt.SqlString())
		assert.Equal(t, "slot1", stmt.SlotName)
		assert.Equal(t, opts, stmt.Options)

		// Nil options is valid construction.
		bare := NewAlterReplicationSlotCmd("slot1", nil)
		assert.Nil(t, bare.Options)

		var _ Node = stmt
		var _ Stmt = stmt
	})

	t.Run("StartReplicationCmd", func(t *testing.T) {
		opts := []*DefElem{
			NewDefElem("proto_version", NewString("1")),
			NewDefElem("publication_names", NewString("pub1")),
		}
		stmt := NewStartReplicationCmd(ReplicationKindLogical, "slot1", 0, 0x16B3748, opts)

		assert.Equal(t, T_StartReplicationCmd, stmt.NodeTag())
		assert.Equal(t, "START_REPLICATION", stmt.StatementType())
		assert.Equal(t, "START_REPLICATION", stmt.SqlString())
		assert.Equal(t, ReplicationKindLogical, stmt.Kind)
		assert.Equal(t, "slot1", stmt.SlotName)
		assert.Equal(t, TimeLineID(0), stmt.Timeline)
		assert.Equal(t, XLogRecPtr(0x16B3748), stmt.StartPoint)
		assert.Equal(t, opts, stmt.Options)

		var _ Node = stmt
		var _ Stmt = stmt
	})

	t.Run("ReadReplicationSlotCmd", func(t *testing.T) {
		stmt := NewReadReplicationSlotCmd("slot1")

		assert.Equal(t, T_ReadReplicationSlotCmd, stmt.NodeTag())
		assert.Equal(t, "READ_REPLICATION_SLOT", stmt.StatementType())
		assert.Equal(t, "READ_REPLICATION_SLOT", stmt.SqlString())
		assert.Equal(t, "slot1", stmt.SlotName)

		var _ Node = stmt
		var _ Stmt = stmt
	})
}

// TestReplicationNodeTags verifies the six T_*Cmd NodeTags stringify
// distinctly (matches the convention of other ast tests verifying enum
// .String() round-trips).
func TestReplicationNodeTags(t *testing.T) {
	tests := []struct {
		tag  NodeTag
		want string
	}{
		{T_IdentifySystemCmd, "T_IdentifySystemCmd"},
		{T_CreateReplicationSlotCmd, "T_CreateReplicationSlotCmd"},
		{T_DropReplicationSlotCmd, "T_DropReplicationSlotCmd"},
		{T_AlterReplicationSlotCmd, "T_AlterReplicationSlotCmd"},
		{T_StartReplicationCmd, "T_StartReplicationCmd"},
		{T_ReadReplicationSlotCmd, "T_ReadReplicationSlotCmd"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.tag.String())
		})
	}
}
