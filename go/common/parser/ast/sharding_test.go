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

// Deparsing of parsed statements is covered by the parser round-trip tests
// in testdata/sharding_cases.json; this covers what those cannot reach.
func TestTableGroupStmts(t *testing.T) {
	create := NewCreateTableGroupStmt("g", nil, false)
	assert.Equal(t, "CREATE TABLEGROUP", create.StatementType())
	assert.Equal(t, "CREATE TABLEGROUP g", create.SqlString())

	alter := NewAlterTableGroupRename("g", "h")
	assert.Equal(t, "ALTER TABLEGROUP", alter.StatementType())
	assert.Equal(t, "ALTER TABLEGROUP g RENAME TO h", alter.SqlString())

	alter.Action = AlterTableGroupAction(99)
	assert.Equal(t, "ALTER TABLEGROUP g", alter.SqlString())

	assert.True(t, IsTableGroupDDL(create))
	assert.False(t, IsTableGroupDDL(&DropStmt{RemoveType: OBJECT_TABLE}))
}
