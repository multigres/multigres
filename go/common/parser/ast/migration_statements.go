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
		b.WriteString(connectionOptionListSQL(n.Options))
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

// RedactIfCredentialBearing reports whether stmt is a CREATE CONNECTION
// statement — the only statement type whose raw SQL text carries a source
// password as a literal — and if so returns a safe-to-log substitute in place
// of that raw text. Every site that might log a query's raw text (gateway
// query logging, plan string formatting, ...) must redact through this rather
// than reconstruct the check itself, so a password is never one missed call
// site away from reaching logs.
func RedactIfCredentialBearing(stmt Stmt) (redacted string, ok bool) {
	switch s := stmt.(type) {
	case *CreateConnectionStmt:
		return fmt.Sprintf("CREATE CONNECTION %s <redacted>", s.Name), true
	default:
		return "", false
	}
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
		// Reuse the CREATE PUBLICATION object-list renderer: PublicationObjSpec
		// has no SqlString of its own (calling it panics), and this keeps the
		// FOR clause identical to the publication grammar it mirrors.
		if list := formatPubObjList(n.Objects); list != "" {
			b.WriteString(" FOR ")
			b.WriteString(list)
		}
	}
	if n.Options != nil {
		b.WriteString(" WITH ")
		b.WriteString(optionListSQL(n.Options))
	}
	return b.String()
}

// MigrationDirection is the replication direction a PHASE action sets. The
// keyword's two values name the direction being set, not a literal
// MigrationPhase value — multigres.stat_migration's migration_phase column
// reports the derived steady state as IMPORTING/EXPORTING, not IMPORT/EXPORT
// (see docs/migration/migrator_sql_interface.md).
type MigrationDirection int

const (
	MigrationDirectionImport MigrationDirection = iota
	MigrationDirectionExport
)

func (d MigrationDirection) String() string {
	switch d {
	case MigrationDirectionImport:
		return "IMPORT"
	case MigrationDirectionExport:
		return "EXPORT"
	default:
		return ""
	}
}

// MigrationCondOp is a comparison operator in a migration WHEN (cond) clause.
type MigrationCondOp int

const (
	MigrationCondLT MigrationCondOp = iota
	MigrationCondLE
	MigrationCondEQ
	MigrationCondGE
	MigrationCondGT
	MigrationCondNE
)

func (op MigrationCondOp) String() string {
	switch op {
	case MigrationCondLT:
		return "<"
	case MigrationCondLE:
		return "<="
	case MigrationCondEQ:
		return "="
	case MigrationCondGE:
		return ">="
	case MigrationCondGT:
		return ">"
	case MigrationCondNE:
		return "<>"
	default:
		return "?"
	}
}

// MigrationCond is a WHEN (cond) clause shared by ALTER MIGRATION's PHASE
// action and DROP MIGRATION: "field OP constant" (field a
// multigres.stat_migration column, OP a comparison operator). The reverse
// "constant OP field" ordering the grammar could in principle also accept is
// deliberately not supported: allowing the grammar to reduce either ColId or
// def_arg first from the same leading token introduces reduce/reduce
// conflicts across the whole parser (confirmed: 155 of them), and every
// concrete example in the RFC this grammar implements is field-first anyway.
// Value is the constant as parsed (typically *Integer, *Float, or *String —
// mirrors DefElem.Arg) — size/duration parsing of it is a semantic-layer
// concern (see migration_ddl.go), not this grammar-only value type's. Not an
// AST Node.
type MigrationCond struct {
	Field string
	Op    MigrationCondOp
	Value Node
}

func (c *MigrationCond) SqlString() string {
	if c == nil {
		return ""
	}
	return fmt.Sprintf("%s %s %s", c.Field, c.Op, c.Value.SqlString())
}

// MigrationActionSpec carries a parsed ALTER MIGRATION PHASE action's
// parameters. It lets the grammar express the IF EXISTS split once rather than
// duplicating the action grammar per branch. Not an AST Node — a grammar-only
// value type.
//
// CONNECTION/SET subcommands (re-pointing a migration's source connection or
// changing its sequence_margin/objects in place) existed here previously,
// backed by the now-removed UpdateMigration RPC; they are a candidate for
// future reintroduction (see the migration-foundation RFC) but are not
// currently implemented.
type MigrationActionSpec struct {
	Direction MigrationDirection
	When      *MigrationCond // nil if omitted
	Options   *NodeList      // *DefElem list; may be nil
}

// AlterMigrationStmt represents
// `ALTER MIGRATION [IF EXISTS] name PHASE {IMPORT|EXPORT} [WHEN (cond)] [WITH (...)]`.
type AlterMigrationStmt struct {
	BaseNode
	Name      string
	IfExists  bool
	Direction MigrationDirection
	When      *MigrationCond // nil if omitted
	Options   *NodeList      // *DefElem list; may be nil
}

func NewAlterMigrationStmt(name string, ifExists bool, action *MigrationActionSpec) *AlterMigrationStmt {
	return &AlterMigrationStmt{
		BaseNode:  BaseNode{Tag: T_AlterMigrationStmt},
		Name:      name,
		IfExists:  ifExists,
		Direction: action.Direction,
		When:      action.When,
		Options:   action.Options,
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
	fmt.Fprintf(&b, " PHASE %s", n.Direction)
	if n.When != nil {
		b.WriteString(" WHEN (")
		b.WriteString(n.When.SqlString())
		b.WriteString(")")
	}
	if n.Options != nil && len(n.Options.Items) > 0 {
		b.WriteString(" WITH ")
		b.WriteString(optionListSQL(n.Options))
	}
	return b.String()
}

// MigrationDropBehavior carries the trailing `[FORCE | WHEN (cond)] [WITH (...)]`
// of a DROP MIGRATION, so the grammar can express the IF EXISTS split once.
// Not an AST Node — a grammar-only value type.
type MigrationDropBehavior struct {
	Force   bool
	When    *MigrationCond // nil if omitted (default: require caught up now)
	Options *NodeList      // WITH (...) (*DefElem list); e.g. wait_timeout
}

// DropMigrationStmt represents
// `DROP MIGRATION [IF EXISTS] name [, ...] [FORCE | [WHEN (cond)] [WITH (...)]]`.
type DropMigrationStmt struct {
	BaseNode
	Names    *NodeList // *String list
	IfExists bool
	Force    bool
	When     *MigrationCond // nil if omitted (default: require caught up now)
	Options  *NodeList      // WITH (...) (*DefElem list); e.g. wait_timeout
}

func NewDropMigrationStmt(names *NodeList, ifExists bool, behavior *MigrationDropBehavior) *DropMigrationStmt {
	return &DropMigrationStmt{
		BaseNode: BaseNode{Tag: T_DropMigrationStmt},
		Names:    names,
		IfExists: ifExists,
		Force:    behavior.Force,
		When:     behavior.When,
		Options:  behavior.Options,
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
	case n.When != nil:
		b.WriteString(" WHEN (")
		b.WriteString(n.When.SqlString())
		b.WriteString(")")
	}
	if n.Options != nil && len(n.Options.Items) > 0 {
		b.WriteString(" WITH ")
		b.WriteString(optionListSQL(n.Options))
	}
	return b.String()
}

// ==============================================================================
// helpers
// ==============================================================================

// optionListSQL renders a parenthesized option list "( a, b, ... )" from a
// NodeList, deparsing each element via its SqlString. Used for `WITH (...)`
// options (CREATE MIGRATION, ALTER MIGRATION ACTIVATE/SET), whose grammar is
// `migration_option [ = value ]` — DefElem.SqlString's "name = value" form.
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

// connectionOptionListSQL renders a parenthesized CONNECTION OPTIONS list.
// Unlike optionListSQL, this grammar (`connection_option 'value'`, optionally
// ADD/SET/DROP-prefixed) never uses "=" — reusing DefElem.SqlString's
// "name = value" form here would produce SQL the parser rejects. Mirrors the
// ADD/SET/DROP-prefixed rendering AlterTableStmt already uses for ALTER
// COLUMN ... OPTIONS (ddl_statements.go), which CONNECTION OPTIONS was
// modeled on (see docs/migration/migrator_sql_interface.md).
func connectionOptionListSQL(list *NodeList) string {
	if list == nil {
		return "()"
	}
	parts := make([]string, 0, len(list.Items))
	for _, it := range list.Items {
		d, ok := it.(*DefElem)
		if !ok {
			continue
		}
		parts = append(parts, connectionOptionSQL(d))
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

// connectionOptionSQL renders one CONNECTION OPTIONS element, including its
// ADD/SET/DROP action when present (DROP never carries a value).
func connectionOptionSQL(d *DefElem) string {
	name := QuoteIdentifier(d.Defname)
	switch d.Defaction {
	case DEFELEM_ADD:
		return "ADD " + name + " " + d.Arg.SqlString()
	case DEFELEM_SET:
		return "SET " + name + " " + d.Arg.SqlString()
	case DEFELEM_DROP:
		return "DROP " + name
	default:
		return name + " " + d.Arg.SqlString()
	}
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
