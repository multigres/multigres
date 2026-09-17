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

// Migration and connection statements. These are NOT part of PostgreSQL; they
// are a Multigres-specific SQL surface that the multigateway parses and handles
// in-gateway (translating them to migrator RPCs) rather than forwarding them to
// PostgreSQL. See docs/migration/migrator_sql_interface.md.

package ast

import (
	"fmt"
	"strings"
)

// ==============================================================================
// CONNECTIONS
// ==============================================================================

// CreateConnectionStmt represents `CREATE CONNECTION name OPTIONS (...)`.
type CreateConnectionStmt struct {
	BaseNode
	Name        string
	Options     *NodeList // *DefElem list (libpq connection options); may be nil
	IfNotExists bool
}

func NewCreateConnectionStmt(name string, options *NodeList, ifNotExists bool) *CreateConnectionStmt {
	return &CreateConnectionStmt{
		BaseNode:    BaseNode{Tag: T_CreateConnectionStmt},
		Name:        name,
		Options:     options,
		IfNotExists: ifNotExists,
	}
}

func (n *CreateConnectionStmt) String() string {
	return fmt.Sprintf("CreateConnectionStmt(%s)@%d", n.Name, n.Location())
}

func (n *CreateConnectionStmt) StatementType() string { return "CREATE" }

func (n *CreateConnectionStmt) SqlString() string {
	var b strings.Builder
	b.WriteString("CREATE CONNECTION ")
	if n.IfNotExists {
		b.WriteString("IF NOT EXISTS ")
	}
	b.WriteString(QuoteIdentifier(n.Name))
	if n.Options != nil {
		b.WriteString(" OPTIONS ")
		b.WriteString(optionListSQL(n.Options))
	}
	return b.String()
}

// AlterConnectionStmt represents `ALTER CONNECTION name OPTIONS (ADD|SET|DROP ...)`.
type AlterConnectionStmt struct {
	BaseNode
	Name    string
	Options *NodeList // *DefElem list with ADD/SET/DROP actions
}

func NewAlterConnectionStmt(name string, options *NodeList) *AlterConnectionStmt {
	return &AlterConnectionStmt{
		BaseNode: BaseNode{Tag: T_AlterConnectionStmt},
		Name:     name,
		Options:  options,
	}
}

func (n *AlterConnectionStmt) String() string {
	return fmt.Sprintf("AlterConnectionStmt(%s)@%d", n.Name, n.Location())
}

func (n *AlterConnectionStmt) StatementType() string { return "ALTER" }

func (n *AlterConnectionStmt) SqlString() string {
	var b strings.Builder
	b.WriteString("ALTER CONNECTION ")
	b.WriteString(QuoteIdentifier(n.Name))
	if n.Options != nil {
		b.WriteString(" OPTIONS ")
		b.WriteString(optionListSQL(n.Options))
	}
	return b.String()
}

// DropConnectionStmt represents `DROP CONNECTION [IF EXISTS] name [, ...]`.
type DropConnectionStmt struct {
	BaseNode
	Names    *NodeList // *String list
	IfExists bool
}

func NewDropConnectionStmt(names *NodeList, ifExists bool) *DropConnectionStmt {
	return &DropConnectionStmt{
		BaseNode: BaseNode{Tag: T_DropConnectionStmt},
		Names:    names,
		IfExists: ifExists,
	}
}

func (n *DropConnectionStmt) String() string {
	return fmt.Sprintf("DropConnectionStmt@%d", n.Location())
}

func (n *DropConnectionStmt) StatementType() string { return "DROP" }

func (n *DropConnectionStmt) SqlString() string {
	var b strings.Builder
	b.WriteString("DROP CONNECTION ")
	if n.IfExists {
		b.WriteString("IF EXISTS ")
	}
	b.WriteString(nameListSQL(n.Names))
	return b.String()
}

// ShowConnectionsStmt represents `SHOW CONNECTIONS` (Name == "") or
// `SHOW CONNECTION name`.
type ShowConnectionsStmt struct {
	BaseNode
	Name string // "" means all connections
}

func NewShowConnectionsStmt(name string) *ShowConnectionsStmt {
	return &ShowConnectionsStmt{
		BaseNode: BaseNode{Tag: T_ShowConnectionsStmt},
		Name:     name,
	}
}

func (n *ShowConnectionsStmt) String() string {
	return fmt.Sprintf("ShowConnectionsStmt(%s)@%d", n.Name, n.Location())
}

func (n *ShowConnectionsStmt) StatementType() string { return "SHOW" }

func (n *ShowConnectionsStmt) SqlString() string {
	if n.Name == "" {
		return "SHOW CONNECTIONS"
	}
	return "SHOW CONNECTION " + QuoteIdentifier(n.Name)
}

// ==============================================================================
// MIGRATIONS
// ==============================================================================

// MigrationTables carries the parsed table-selection clause of a CREATE
// MIGRATION (`FOR ALL TABLES`, or a `FOR` object list). Not an AST Node — a
// grammar-only value type.
type MigrationTables struct {
	ForAllTables bool
	Objects      *NodeList // *PublicationObjSpec list; nil when ForAllTables
}

// CreateMigrationStmt represents
// `CREATE MIGRATION name CONNECTION conn { FOR ALL TABLES | FOR obj [, ...] } [WITH (...)]`.
type CreateMigrationStmt struct {
	BaseNode
	Name         string
	Connection   string
	ForAllTables bool
	Objects      *NodeList // *PublicationObjSpec list; nil when ForAllTables
	Options      *NodeList // WITH options (*DefElem list); may be nil
	IfNotExists  bool
}

func NewCreateMigrationStmt(name, connection string, tables *MigrationTables, options *NodeList, ifNotExists bool) *CreateMigrationStmt {
	// Resolve the CONTINUATION entries the grammar emits for the ambiguous
	// bare-name forms in the FOR list, exactly as CREATE PUBLICATION does.
	if tables.Objects != nil {
		preprocessPubObjList(tables.Objects)
	}
	return &CreateMigrationStmt{
		BaseNode:     BaseNode{Tag: T_CreateMigrationStmt},
		Name:         name,
		Connection:   connection,
		ForAllTables: tables.ForAllTables,
		Objects:      tables.Objects,
		Options:      options,
		IfNotExists:  ifNotExists,
	}
}

func (n *CreateMigrationStmt) String() string {
	return fmt.Sprintf("CreateMigrationStmt(%s)@%d", n.Name, n.Location())
}

func (n *CreateMigrationStmt) StatementType() string { return "CREATE" }

func (n *CreateMigrationStmt) SqlString() string {
	var b strings.Builder
	b.WriteString("CREATE MIGRATION ")
	if n.IfNotExists {
		b.WriteString("IF NOT EXISTS ")
	}
	b.WriteString(QuoteIdentifier(n.Name))
	b.WriteString(" CONNECTION ")
	b.WriteString(QuoteIdentifier(n.Connection))
	if n.ForAllTables {
		b.WriteString(" FOR ALL TABLES")
	} else if n.Objects != nil {
		b.WriteString(" FOR ")
		parts := make([]string, 0, len(n.Objects.Items))
		for _, it := range n.Objects.Items {
			parts = append(parts, it.SqlString())
		}
		b.WriteString(strings.Join(parts, ", "))
	}
	if n.Options != nil {
		b.WriteString(" WITH ")
		b.WriteString(optionListSQL(n.Options))
	}
	return b.String()
}

// MigrationAction is the subcommand of ALTER MIGRATION.
type MigrationAction int

const (
	MigrationActionStart MigrationAction = iota
	MigrationActionActivate
	MigrationActionDeactivate
	MigrationActionSetConnection
	MigrationActionSetOptions
)

// MigrationActionSpec carries a parsed ALTER MIGRATION subcommand and its
// parameters. It lets the grammar reduce every action to one nonterminal, so
// the IF EXISTS split is expressed in a single place rather than duplicated per
// action. Not an AST Node — a grammar-only value type.
type MigrationActionSpec struct {
	Action     MigrationAction
	Connection string    // for MigrationActionSetConnection
	Options    *NodeList // for MigrationActionSetOptions (*DefElem list)
}

// AlterMigrationStmt represents
// `ALTER MIGRATION [IF EXISTS] name { START | ACTIVATE | DEACTIVATE | CONNECTION conn | SET (...) }`.
type AlterMigrationStmt struct {
	BaseNode
	Name       string
	IfExists   bool
	Action     MigrationAction
	Connection string    // for MigrationActionSetConnection
	Options    *NodeList // for MigrationActionSetOptions (*DefElem list)
}

func NewAlterMigrationStmt(name string, ifExists bool, action *MigrationActionSpec) *AlterMigrationStmt {
	return &AlterMigrationStmt{
		BaseNode:   BaseNode{Tag: T_AlterMigrationStmt},
		Name:       name,
		IfExists:   ifExists,
		Action:     action.Action,
		Connection: action.Connection,
		Options:    action.Options,
	}
}

func (n *AlterMigrationStmt) String() string {
	return fmt.Sprintf("AlterMigrationStmt(%s)@%d", n.Name, n.Location())
}

func (n *AlterMigrationStmt) StatementType() string { return "ALTER" }

func (n *AlterMigrationStmt) SqlString() string {
	var b strings.Builder
	b.WriteString("ALTER MIGRATION ")
	if n.IfExists {
		b.WriteString("IF EXISTS ")
	}
	b.WriteString(QuoteIdentifier(n.Name))
	switch n.Action {
	case MigrationActionStart:
		b.WriteString(" START")
	case MigrationActionActivate:
		b.WriteString(" ACTIVATE")
	case MigrationActionDeactivate:
		b.WriteString(" DEACTIVATE")
	case MigrationActionSetConnection:
		b.WriteString(" CONNECTION ")
		b.WriteString(QuoteIdentifier(n.Connection))
	case MigrationActionSetOptions:
		b.WriteString(" SET ")
		b.WriteString(optionListSQL(n.Options))
	}
	return b.String()
}

// MigrationDropBehavior carries the trailing `[FORCE | WAIT [(n)]]` of a
// DROP MIGRATION, so the grammar can express the IF EXISTS split once. Not an
// AST Node — a grammar-only value type.
type MigrationDropBehavior struct {
	Force       bool
	Wait        bool
	WaitTimeout int  // seconds; only meaningful when HasTimeout
	HasTimeout  bool // WAIT (n) was given
}

// DropMigrationStmt represents
// `DROP MIGRATION [IF EXISTS] name [, ...] [FORCE | WAIT [(n)]]`.
type DropMigrationStmt struct {
	BaseNode
	Names       *NodeList // *String list
	IfExists    bool
	Force       bool
	Wait        bool
	WaitTimeout int  // seconds; only meaningful when HasTimeout
	HasTimeout  bool // WAIT (n) was given
}

func NewDropMigrationStmt(names *NodeList, ifExists bool, behavior *MigrationDropBehavior) *DropMigrationStmt {
	return &DropMigrationStmt{
		BaseNode:    BaseNode{Tag: T_DropMigrationStmt},
		Names:       names,
		IfExists:    ifExists,
		Force:       behavior.Force,
		Wait:        behavior.Wait,
		WaitTimeout: behavior.WaitTimeout,
		HasTimeout:  behavior.HasTimeout,
	}
}

func (n *DropMigrationStmt) String() string {
	return fmt.Sprintf("DropMigrationStmt@%d", n.Location())
}

func (n *DropMigrationStmt) StatementType() string { return "DROP" }

func (n *DropMigrationStmt) SqlString() string {
	var b strings.Builder
	b.WriteString("DROP MIGRATION ")
	if n.IfExists {
		b.WriteString("IF EXISTS ")
	}
	b.WriteString(nameListSQL(n.Names))
	switch {
	case n.Force:
		b.WriteString(" FORCE")
	case n.Wait && n.HasTimeout:
		fmt.Fprintf(&b, " WAIT (%d)", n.WaitTimeout)
	case n.Wait:
		b.WriteString(" WAIT")
	}
	return b.String()
}

// ShowMigrationsStmt represents `SHOW MIGRATIONS` (Name == "") or
// `SHOW MIGRATION name`.
type ShowMigrationsStmt struct {
	BaseNode
	Name string // "" means all migrations
}

func NewShowMigrationsStmt(name string) *ShowMigrationsStmt {
	return &ShowMigrationsStmt{
		BaseNode: BaseNode{Tag: T_ShowMigrationsStmt},
		Name:     name,
	}
}

func (n *ShowMigrationsStmt) String() string {
	return fmt.Sprintf("ShowMigrationsStmt(%s)@%d", n.Name, n.Location())
}

func (n *ShowMigrationsStmt) StatementType() string { return "SHOW" }

func (n *ShowMigrationsStmt) SqlString() string {
	if n.Name == "" {
		return "SHOW MIGRATIONS"
	}
	return "SHOW MIGRATION " + QuoteIdentifier(n.Name)
}

// ==============================================================================
// helpers
// ==============================================================================

// optionListSQL renders a parenthesized option list "( a, b, ... )" from a
// NodeList, deparsing each element via its SqlString.
func optionListSQL(list *NodeList) string {
	if list == nil {
		return "()"
	}
	parts := make([]string, 0, len(list.Items))
	for _, it := range list.Items {
		parts = append(parts, it.SqlString())
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

// nameListSQL renders a comma-separated list of quoted identifiers from a
// NodeList of *String nodes.
func nameListSQL(list *NodeList) string {
	if list == nil {
		return ""
	}
	parts := make([]string, 0, len(list.Items))
	for _, it := range list.Items {
		if s, ok := it.(*String); ok {
			parts = append(parts, QuoteIdentifier(s.SVal))
		} else {
			parts = append(parts, it.SqlString())
		}
	}
	return strings.Join(parts, ", ")
}
