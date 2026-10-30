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

import "strings"

// Multigres tablegroup DDL: CREATE TABLEGROUP, ALTER TABLEGROUP and DROP
// TABLEGROUP (the last is a DropStmt with OBJECT_TABLEGROUP). See
// docs/sharding/ddl.md for the syntax. These statements are parsed and
// deparsed only; the gateway rejects them until the tablegroup catalog
// exists (IsTableGroupDDL below).

// CreateTableGroupStmt is CREATE TABLEGROUP [IF NOT EXISTS] name
// [SHARDS ('range', ...)]. Shards is nil when the clause is omitted: a
// single 0-inf shard.
type CreateTableGroupStmt struct {
	BaseNode
	Name        string
	Shards      *NodeList // *String items, 'lower-upper' ranges
	IfNotExists bool
}

func NewCreateTableGroupStmt(name string, shards *NodeList, ifNotExists bool) *CreateTableGroupStmt {
	return &CreateTableGroupStmt{BaseNode: BaseNode{Tag: T_CreateTableGroupStmt}, Name: name, Shards: shards, IfNotExists: ifNotExists}
}

func (c *CreateTableGroupStmt) StatementType() string { return "CREATE TABLEGROUP" }

func (c *CreateTableGroupStmt) SqlString() string {
	s := "CREATE TABLEGROUP "
	if c.IfNotExists {
		s += "IF NOT EXISTS "
	}
	s += QuoteIdentifier(c.Name)
	if c.Shards != nil && c.Shards.Len() > 0 {
		s += " SHARDS " + shardRangeListSql(c.Shards)
	}
	return s
}

// AlterTableGroupAction selects the ALTER TABLEGROUP form.
type AlterTableGroupAction int

const (
	AlterTableGroupRename AlterTableGroupAction = iota
	AlterTableGroupReshard
)

// AlterTableGroupStmt is ALTER TABLEGROUP name {RENAME TO new_name |
// RESHARD (sources) INTO (targets)}.
type AlterTableGroupStmt struct {
	BaseNode
	Name    string
	Action  AlterTableGroupAction
	NewName string    // RENAME TO
	Sources *NodeList // RESHARD: *String ranges being replaced
	Targets *NodeList // RESHARD: *String ranges replacing them
}

func NewAlterTableGroupRename(name, newName string) *AlterTableGroupStmt {
	return &AlterTableGroupStmt{BaseNode: BaseNode{Tag: T_AlterTableGroupStmt}, Name: name, Action: AlterTableGroupRename, NewName: newName}
}

func NewAlterTableGroupReshard(name string, sources, targets *NodeList) *AlterTableGroupStmt {
	return &AlterTableGroupStmt{BaseNode: BaseNode{Tag: T_AlterTableGroupStmt}, Name: name, Action: AlterTableGroupReshard, Sources: sources, Targets: targets}
}

func (a *AlterTableGroupStmt) StatementType() string { return "ALTER TABLEGROUP" }

func (a *AlterTableGroupStmt) SqlString() string {
	s := "ALTER TABLEGROUP " + QuoteIdentifier(a.Name)
	switch a.Action {
	case AlterTableGroupRename:
		return s + " RENAME TO " + QuoteIdentifier(a.NewName)
	case AlterTableGroupReshard:
		return s + " RESHARD " + shardRangeListSql(a.Sources) + " INTO " + shardRangeListSql(a.Targets)
	}
	return s
}

func shardRangeListSql(l *NodeList) string {
	var parts []string
	if l != nil {
		for _, item := range l.Items {
			if str, ok := item.(*String); ok {
				parts = append(parts, QuoteStringLiteral(str.SVal))
			}
		}
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

// IsTableGroupDDL reports whether stmt is a CREATE, ALTER or DROP TABLEGROUP.
// The gateway uses this to reject such statements until they are
// implemented, instead of forwarding them to postgres where they would fail
// with a syntax error.
func IsTableGroupDDL(stmt Stmt) bool {
	switch s := stmt.(type) {
	case *CreateTableGroupStmt, *AlterTableGroupStmt:
		return true
	case *DropStmt:
		return s.RemoveType == OBJECT_TABLEGROUP
	}
	return false
}
