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

// Exercises the asthelpergen-generated Clone and Rewrite cases for every
// replication command node. Without these tests the `case *FooCmd:` lines
// added to ast_clone.go / ast_rewrite.go when a new type is introduced are
// never hit, which (1) leaves them uncovered and (2) lets a stale generated
// file go unnoticed if a contributor forgets to run `make build-all`.

package ast_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser/ast"
)

// replicationNodeCases lists one populated instance of every in-scope
// replication AST node. Used to drive table-driven Clone and Rewrite tests.
func replicationNodeCases() []struct {
	name string
	node ast.Stmt
} {
	return []struct {
		name string
		node ast.Stmt
	}{
		{"IdentifySystemCmd", ast.NewIdentifySystemCmd()},
		{
			"CreateReplicationSlotCmd",
			func() ast.Stmt {
				c := ast.NewCreateReplicationSlotCmd("s1", ast.ReplicationKindLogical, "pgoutput", true)
				c.Options = []*ast.DefElem{ast.NewDefElem("two_phase", ast.NewBoolean(true))}
				return c
			}(),
		},
		{"DropReplicationSlotCmd", ast.NewDropReplicationSlotCmd("s1", true)},
		{
			"AlterReplicationSlotCmd",
			ast.NewAlterReplicationSlotCmd("s1", []*ast.DefElem{
				ast.NewDefElem("failover", ast.NewBoolean(true)),
			}),
		},
		{
			"StartReplicationCmd",
			ast.NewStartReplicationCmd(
				ast.ReplicationKindLogical, "s1", 0, 0x16B3748,
				[]*ast.DefElem{
					ast.NewDefElem("proto_version", ast.NewString("1")),
				},
			),
		},
		{"ReadReplicationSlotCmd", ast.NewReadReplicationSlotCmd("s1")},
	}
}

// TestCloneReplicationNodes runs every replication node through the generic
// `CloneNode` dispatch (which routes to the asthelpergen-emitted
// `case *FooCmd:` entries in ast_clone.go). For each, asserts the clone is a
// fresh pointer of the same concrete type with the same NodeTag.
func TestCloneReplicationNodes(t *testing.T) {
	for _, tt := range replicationNodeCases() {
		t.Run(tt.name, func(t *testing.T) {
			cloned := ast.CloneNode(tt.node)
			require.NotNil(t, cloned, "CloneNode should not return nil")
			assert.NotSame(t, tt.node, cloned, "clone must be a different instance")
			assert.IsType(t, tt.node, cloned, "clone must have the same concrete type")
			cs, ok := cloned.(ast.Stmt)
			require.True(t, ok, "clone must still satisfy ast.Stmt")
			assert.Equal(t, tt.node.NodeTag(), cs.NodeTag())
			assert.Equal(t, tt.node.StatementType(), cs.StatementType())
		})
	}
}

// TestCloneReplicationNodes_DeepCopy verifies that mutating the clone of
// a node with a slice field (Options) does not leak back into the original.
func TestCloneReplicationNodes_DeepCopy(t *testing.T) {
	orig := ast.NewCreateReplicationSlotCmd("s1", ast.ReplicationKindLogical, "pgoutput", false)
	orig.Options = []*ast.DefElem{
		ast.NewDefElem("two_phase", ast.NewBoolean(true)),
	}

	clone := ast.CloneRefOfCreateReplicationSlotCmd(orig)
	require.NotNil(t, clone)
	assert.NotSame(t, orig, clone)
	assert.NotSame(t, &orig.Options[0], &clone.Options[0],
		"Options slice header must not be shared")

	// Mutating the clone's options must not affect the original.
	clone.Options[0].Defname = "mutated"
	assert.Equal(t, "two_phase", orig.Options[0].Defname,
		"original must be unaffected by clone mutation")
}

// TestRewriteReplicationNodes runs every replication node through the
// `Rewrite` dispatch (which uses the rewriter table in ast_rewrite.go).
// Asserts the pre-visit callback fires at least once for each, confirming
// the new `case *FooCmd:` entries are reachable.
func TestRewriteReplicationNodes(t *testing.T) {
	for _, tt := range replicationNodeCases() {
		t.Run(tt.name, func(t *testing.T) {
			var visited int
			result := ast.Rewrite(tt.node, func(*ast.Cursor) bool {
				visited++
				return true
			}, nil)
			require.NotNil(t, result, "Rewrite must return a non-nil result")
			assert.GreaterOrEqual(t, visited, 1, "pre-visit callback must fire at least once")
		})
	}
}
