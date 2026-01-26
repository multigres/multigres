// PostgreSQL Database Management System
// (also known as Postgres, formerly known as Postgres95)
//
//	Portions Copyright (c) 2025, Supabase, Inc
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
package ast

import (
	"fmt"
	"strconv"
	"strings"
)

// ==============================================================================
// DDL FRAMEWORK - PostgreSQL parsenodes.h DDL implementation
// Ported from postgres/src/include/nodes/parsenodes.h
// ==============================================================================

// Supporting enums and types for DDL statements

// ObjectType represents the type of database object - ported from postgres/src/include/nodes/parsenodes.h:2256-2310
type ObjectType int

const (
	OBJECT_ACCESS_METHOD ObjectType = iota
	OBJECT_AGGREGATE
	OBJECT_AMOP
	OBJECT_AMPROC
	OBJECT_ATTRIBUTE // type's attribute, when distinct from column
	OBJECT_CAST
	OBJECT_COLUMN
	OBJECT_COLLATION
	OBJECT_CONVERSION
	OBJECT_DATABASE
	OBJECT_DEFAULT
	OBJECT_DEFACL
	OBJECT_DOMAIN
	OBJECT_DOMCONSTRAINT
	OBJECT_EVENT_TRIGGER
	OBJECT_EXTENSION
	OBJECT_FDW
	OBJECT_FOREIGN_SERVER
	OBJECT_FOREIGN_TABLE
	OBJECT_FUNCTION
	OBJECT_INDEX
	OBJECT_LANGUAGE
	OBJECT_LARGEOBJECT
	OBJECT_MATVIEW
	OBJECT_OPCLASS
	OBJECT_OPERATOR
	OBJECT_OPFAMILY
	OBJECT_PARAMETER_ACL
	OBJECT_POLICY
	OBJECT_PROCEDURE
	OBJECT_PUBLICATION
	OBJECT_PUBLICATION_NAMESPACE
	OBJECT_PUBLICATION_REL
	OBJECT_ROLE
	OBJECT_ROUTINE
	OBJECT_RULE
	OBJECT_SCHEMA
	OBJECT_SEQUENCE
	OBJECT_SUBSCRIPTION
	OBJECT_STATISTIC_EXT
	OBJECT_TABCONSTRAINT
	OBJECT_TABLE
	OBJECT_TABLESPACE
	OBJECT_TRANSFORM
	OBJECT_TRIGGER
	OBJECT_TSCONFIGURATION
	OBJECT_TSDICTIONARY
	OBJECT_TSPARSER
	OBJECT_TSTEMPLATE
	OBJECT_TYPE
	OBJECT_USER_MAPPING
	OBJECT_VIEW
)

func (o ObjectType) String() string {
	switch o {
	case OBJECT_ACCESS_METHOD:
		return "ACCESS METHOD"
	case OBJECT_AGGREGATE:
		return "AGGREGATE"
	case OBJECT_AMOP:
		return "AMOP"
	case OBJECT_AMPROC:
		return "AMPROC"
	case OBJECT_ATTRIBUTE:
		return "ATTRIBUTE"
	case OBJECT_CAST:
		return "CAST"
	case OBJECT_COLUMN:
		return "COLUMN"
	case OBJECT_COLLATION:
		return "COLLATION"
	case OBJECT_CONVERSION:
		return "CONVERSION"
	case OBJECT_DATABASE:
		return "DATABASE"
	case OBJECT_DEFAULT:
		return "DEFAULT"
	case OBJECT_DEFACL:
		return "DEFAULT PRIVILEGES"
	case OBJECT_DOMAIN:
		return "DOMAIN"
	case OBJECT_DOMCONSTRAINT:
		return "CONSTRAINT"
	case OBJECT_EVENT_TRIGGER:
		return "EVENT TRIGGER"
	case OBJECT_EXTENSION:
		return "EXTENSION"
	case OBJECT_FDW:
		return "FOREIGN DATA WRAPPER"
	case OBJECT_FOREIGN_SERVER:
		return "SERVER"
	case OBJECT_FOREIGN_TABLE:
		return "FOREIGN TABLE"
	case OBJECT_FUNCTION:
		return "FUNCTION"
	case OBJECT_INDEX:
		return "INDEX"
	case OBJECT_LANGUAGE:
		return "LANGUAGE"
	case OBJECT_LARGEOBJECT:
		return "LARGE OBJECT"
	case OBJECT_MATVIEW:
		return "MATERIALIZED VIEW"
	case OBJECT_OPCLASS:
		return "OPERATOR CLASS"
	case OBJECT_OPERATOR:
		return "OPERATOR"
	case OBJECT_OPFAMILY:
		return "OPERATOR FAMILY"
	case OBJECT_PARAMETER_ACL:
		return "PARAMETER"
	case OBJECT_POLICY:
		return "POLICY"
	case OBJECT_PROCEDURE:
		return "PROCEDURE"
	case OBJECT_PUBLICATION:
		return "PUBLICATION"
	case OBJECT_PUBLICATION_NAMESPACE:
		return "PUBLICATION NAMESPACE"
	case OBJECT_PUBLICATION_REL:
		return "PUBLICATION RELATION"
	case OBJECT_ROLE:
		return "ROLE"
	case OBJECT_ROUTINE:
		return "ROUTINE"
	case OBJECT_RULE:
		return "RULE"
	case OBJECT_SCHEMA:
		return "SCHEMA"
	case OBJECT_SEQUENCE:
		return "SEQUENCE"
	case OBJECT_SUBSCRIPTION:
		return "SUBSCRIPTION"
	case OBJECT_STATISTIC_EXT:
		return "STATISTICS"
	case OBJECT_TABCONSTRAINT:
		return "CONSTRAINT"
	case OBJECT_TABLE:
		return "TABLE"
	case OBJECT_TABLESPACE:
		return "TABLESPACE"
	case OBJECT_TRANSFORM:
		return "TRANSFORM"
	case OBJECT_TRIGGER:
		return "TRIGGER"
	case OBJECT_TSCONFIGURATION:
		return "TEXT SEARCH CONFIGURATION"
	case OBJECT_TSDICTIONARY:
		return "TEXT SEARCH DICTIONARY"
	case OBJECT_TSPARSER:
		return "TEXT SEARCH PARSER"
	case OBJECT_TSTEMPLATE:
		return "TEXT SEARCH TEMPLATE"
	case OBJECT_TYPE:
		return "TYPE"
	case OBJECT_VIEW:
		return "VIEW"
	default:
		return fmt.Sprintf("ObjectType(%d)", int(o))
	}
}

// DropBehavior represents CASCADE/RESTRICT behavior - ported from postgres/src/include/nodes/parsenodes.h:2329-2333
type DropBehavior int

const (
	DropRestrict DropBehavior = iota // drop fails if any dependent objects
	DropCascade                      // remove dependent objects too
)

func (d DropBehavior) String() string {
	switch d {
	case DropRestrict:
		return "RESTRICT"
	case DropCascade:
		return "CASCADE"
	default:
		return fmt.Sprintf("DropBehavior(%d)", int(d))
	}
}

// ConstrType represents types of constraints - ported from postgres/src/include/nodes/parsenodes.h:2697-2714
type ConstrType int

const (
	CONSTR_NULL ConstrType = iota // not standard SQL, but expected
	CONSTR_NOTNULL
	CONSTR_DEFAULT
	CONSTR_IDENTITY
	CONSTR_GENERATED
	CONSTR_CHECK
	CONSTR_PRIMARY
	CONSTR_UNIQUE
	CONSTR_EXCLUSION
	CONSTR_FOREIGN
	CONSTR_ATTR_DEFERRABLE // attributes for previous constraint node
	CONSTR_ATTR_NOT_DEFERRABLE
	CONSTR_ATTR_DEFERRED
	CONSTR_ATTR_IMMEDIATE
)

// Identity generation constants - ported from postgres/src/include/catalog/pg_attribute.h:233-234
const (
	ATTRIBUTE_IDENTITY_ALWAYS     byte = 'a' // GENERATED ALWAYS AS IDENTITY
	ATTRIBUTE_IDENTITY_BY_DEFAULT byte = 'd' // GENERATED BY DEFAULT AS IDENTITY
)

// Constraint Attribute Specification (CAS) bitmask constants
// Ported from postgres/src/include/parser/parse_node.h:46-52
const (
	CAS_NOT_DEFERRABLE      int = 0x01
	CAS_DEFERRABLE          int = 0x02
	CAS_INITIALLY_IMMEDIATE int = 0x04
	CAS_INITIALLY_DEFERRED  int = 0x08
	CAS_NOT_VALID           int = 0x10
	CAS_NO_INHERIT          int = 0x20
)

// Foreign Key Constraint Match Types
// Ported from postgres/src/include/nodes/parsenodes.h:2665-2667
const (
	FKCONSTR_MATCH_FULL    byte = 'f' // MATCH FULL
	FKCONSTR_MATCH_PARTIAL byte = 'p' // MATCH PARTIAL
	FKCONSTR_MATCH_SIMPLE  byte = 's' // MATCH SIMPLE (default)
)

// Replica Identity Types
// Ported from postgres/src/include/catalog/pg_class.h
const (
	REPLICA_IDENTITY_DEFAULT rune = 'd' // Use primary key
	REPLICA_IDENTITY_NOTHING rune = 'n' // No replica identity
	REPLICA_IDENTITY_FULL    rune = 'f' // Full row is replica identity
	REPLICA_IDENTITY_INDEX   rune = 'i' // Use specific index
)

// Foreign Key Constraint Action Types
// Ported from postgres/src/include/nodes/parsenodes.h:2670-2674
const (
	FKCONSTR_ACTION_NOACTION   byte = 'a' // NO ACTION
	FKCONSTR_ACTION_RESTRICT   byte = 'r' // RESTRICT
	FKCONSTR_ACTION_CASCADE    byte = 'c' // CASCADE
	FKCONSTR_ACTION_SETNULL    byte = 'n' // SET NULL
	FKCONSTR_ACTION_SETDEFAULT byte = 'd' // SET DEFAULT
)

// KeyAction represents a foreign key action with optional column list
// Ported from postgres/src/backend/parser/gram.y:142-146
type KeyAction struct {
	BaseNode
	Action byte      // FKCONSTR_ACTION_* constant
	Cols   *NodeList // optional column list for SET NULL/SET DEFAULT
}

// KeyActions represents both update and delete actions for foreign keys
// Ported from postgres/src/backend/parser/gram.y:148-152
type KeyActions struct {
	BaseNode
	UpdateAction *KeyAction
	DeleteAction *KeyAction
}

// SqlString returns the SQL representation of KeyAction
func (ka *KeyAction) SqlString() string {
	if ka == nil {
		return ""
	}

	var result string
	switch ka.Action {
	case FKCONSTR_ACTION_NOACTION:
		result = "NO ACTION"
	case FKCONSTR_ACTION_RESTRICT:
		result = "RESTRICT"
	case FKCONSTR_ACTION_CASCADE:
		result = "CASCADE"
	case FKCONSTR_ACTION_SETNULL:
		result = "SET NULL"
	case FKCONSTR_ACTION_SETDEFAULT:
		result = "SET DEFAULT"
	default:
		result = "NO ACTION"
	}

	// Add column list if present (only valid for SET NULL and SET DEFAULT)
	if ka.Cols != nil && len(ka.Cols.Items) > 0 {
		if ka.Action == FKCONSTR_ACTION_SETNULL || ka.Action == FKCONSTR_ACTION_SETDEFAULT {
			result += " ("
			var colsBuilder strings.Builder
			for i, col := range ka.Cols.Items {
				if i > 0 {
					colsBuilder.WriteString(", ")
				}
				colsBuilder.WriteString(col.SqlString())
			}
			result += colsBuilder.String()
			result += ")"
		}
	}

	return result
}

func (c ConstrType) String() string {
	switch c {
	case CONSTR_NULL:
		return "NULL"
	case CONSTR_NOTNULL:
		return "NOT_NULL"
	case CONSTR_DEFAULT:
		return "DEFAULT"
	case CONSTR_CHECK:
		return "CHECK"
	case CONSTR_PRIMARY:
		return "PRIMARY_KEY"
	case CONSTR_UNIQUE:
		return "UNIQUE"
	case CONSTR_FOREIGN:
		return "FOREIGN_KEY"
	default:
		return fmt.Sprintf("ConstrType(%d)", int(c))
	}
}

// ViewCheckOption represents WITH CHECK OPTION - ported from postgres/src/include/nodes/parsenodes.h:3773-3777
type ViewCheckOption int

const (
	NO_CHECK_OPTION ViewCheckOption = iota
	LOCAL_CHECK_OPTION
	CASCADED_CHECK_OPTION
)

func (v ViewCheckOption) String() string {
	switch v {
	case NO_CHECK_OPTION:
		return "NO_CHECK"
	case LOCAL_CHECK_OPTION:
		return "LOCAL"
	case CASCADED_CHECK_OPTION:
		return "CASCADED"
	default:
		return fmt.Sprintf("ViewCheckOption(%d)", int(v))
	}
}

// AlterTableType represents types of ALTER TABLE operations - ported from postgres/src/include/nodes/parsenodes.h:2348-2417
type AlterTableType int

const (
	AT_AddColumn                 AlterTableType = iota // add column
	AT_AddColumnToView                                 // implicitly via CREATE OR REPLACE VIEW
	AT_ColumnDefault                                   // alter column default
	AT_CookedColumnDefault                             // add a pre-cooked column default
	AT_DropNotNull                                     // alter column drop not null
	AT_SetNotNull                                      // alter column set not null
	AT_SetExpression                                   // alter column set expression
	AT_DropExpression                                  // alter column drop expression
	AT_CheckNotNull                                    // check column is already not null
	AT_SetStatistics                                   // alter column set statistics
	AT_SetOptions                                      // alter column set (options)
	AT_ResetOptions                                    // alter column reset (options)
	AT_SetStorage                                      // alter column set storage
	AT_SetCompression                                  // alter column set compression
	AT_DropColumn                                      // drop column
	AT_AddIndex                                        // add index
	AT_ReAddIndex                                      // internal to commands/tablecmds.c
	AT_AddConstraint                                   // add constraint
	AT_AddConstraintRecurse                            // internal to commands/tablecmds.c
	AT_ReAddConstraint                                 // internal to commands/tablecmds.c
	AT_ReAddDomainConstraint                           // internal to commands/tablecmds.c
	AT_AlterConstraint                                 // alter constraint
	AT_ValidateConstraint                              // validate constraint
	AT_AddIndexConstraint                              // add constraint using existing index
	AT_DropConstraint                                  // drop constraint
	AT_ReAddComment                                    // internal to commands/tablecmds.c
	AT_AlterColumnType                                 // alter column type
	AT_AlterColumnGenericOptions                       // alter column OPTIONS (...)
	AT_ChangeOwner                                     // change owner
	AT_ClusterOn                                       // CLUSTER ON
	AT_DropCluster                                     // SET WITHOUT CLUSTER
	AT_SetLogged                                       // SET LOGGED
	AT_SetUnLogged                                     // SET UNLOGGED
	AT_DropOids                                        // SET WITHOUT OIDS
	AT_SetAccessMethod                                 // SET ACCESS METHOD
	AT_SetTableSpace                                   // SET TABLESPACE
	AT_SetRelOptions                                   // SET (...)
	AT_ResetRelOptions                                 // RESET (...)
	AT_ReplaceRelOptions                               // replace reloption list in its entirety
	AT_EnableTrig                                      // ENABLE TRIGGER name
	AT_EnableAlwaysTrig                                // ENABLE ALWAYS TRIGGER name
	AT_EnableReplicaTrig                               // ENABLE REPLICA TRIGGER name
	AT_DisableTrig                                     // DISABLE TRIGGER name
	AT_EnableTrigAll                                   // ENABLE TRIGGER ALL
	AT_DisableTrigAll                                  // DISABLE TRIGGER ALL
	AT_EnableTrigUser                                  // ENABLE TRIGGER USER
	AT_DisableTrigUser                                 // DISABLE TRIGGER USER
	AT_EnableRule                                      // ENABLE RULE name
	AT_EnableAlwaysRule                                // ENABLE ALWAYS RULE name
	AT_EnableReplicaRule                               // ENABLE REPLICA RULE name
	AT_DisableRule                                     // DISABLE RULE name
	AT_AddInherit                                      // INHERIT parent
	AT_DropInherit                                     // NO INHERIT parent
	AT_AddOf                                           // OF <type_name>
	AT_DropOf                                          // NOT OF
	AT_ReplicaIdentity                                 // REPLICA IDENTITY
	AT_EnableRowSecurity                               // ENABLE ROW SECURITY
	AT_DisableRowSecurity                              // DISABLE ROW SECURITY
	AT_ForceRowSecurity                                // FORCE ROW SECURITY
	AT_NoForceRowSecurity                              // NO FORCE ROW SECURITY
	AT_GenericOptions                                  // OPTIONS (...)
	AT_AttachPartition                                 // ATTACH PARTITION
	AT_DetachPartition                                 // DETACH PARTITION
	AT_DetachPartitionFinalize                         // DETACH PARTITION ... FINALIZE
	AT_AddIdentity                                     // ADD IDENTITY
	AT_SetIdentity                                     // SET identity column options
	AT_DropIdentity                                    // DROP IDENTITY
	AT_ReAddStatistics                                 // internal to commands/tablecmds.c
)

func (a AlterTableType) String() string {
	switch a {
	case AT_AddColumn:
		return "ADD_COLUMN"
	case AT_DropColumn:
		return "DROP_COLUMN"
	case AT_ColumnDefault:
		return "COLUMN_DEFAULT"
	case AT_DropNotNull:
		return "DROP_NOT_NULL"
	case AT_SetNotNull:
		return "SET_NOT_NULL"
	case AT_AddConstraint:
		return "ADD_CONSTRAINT"
	case AT_DropConstraint:
		return "DROP_CONSTRAINT"
	case AT_AlterColumnType:
		return "ALTER_COLUMN_TYPE"
	case AT_ChangeOwner:
		return "CHANGE_OWNER"
	case AT_SetTableSpace:
		return "SET_TABLESPACE"
	case AT_EnableRule:
		return "ENABLE RULE"
	case AT_EnableAlwaysRule:
		return "ENABLE ALWAYS RULE"
	case AT_EnableReplicaRule:
		return "ENABLE REPLICA RULE"
	case AT_DisableRule:
		return "DISABLE RULE"
	default:
		return fmt.Sprintf("AlterTableType(%d)", int(a))
	}
}

// DefElemAction represents actions for DefElem - ported from postgres/src/include/nodes/parsenodes.h:803-809
type DefElemAction int

const (
	DEFELEM_UNSPEC DefElemAction = iota // no action given
	DEFELEM_SET                         // SET
	DEFELEM_ADD                         // ADD
	DEFELEM_DROP                        // DROP
)

func (d DefElemAction) String() string {
	switch d {
	case DEFELEM_UNSPEC:
		return "UNSPEC"
	case DEFELEM_SET:
		return "SET"
	case DEFELEM_ADD:
		return "ADD"
	case DEFELEM_DROP:
		return "DROP"
	default:
		return fmt.Sprintf("DefElemAction(%d)", int(d))
	}
}

// SortByDir represents sort direction - ported from postgres/src/include/nodes/parsenodes.h:57-62
type SortByDir int

const (
	SORTBY_DEFAULT SortByDir = iota
	SORTBY_ASC
	SORTBY_DESC
	SORTBY_USING // not used in indexes
)

func (s SortByDir) String() string {
	switch s {
	case SORTBY_DEFAULT:
		return "DEFAULT"
	case SORTBY_ASC:
		return "ASC"
	case SORTBY_DESC:
		return "DESC"
	case SORTBY_USING:
		return "USING"
	default:
		return fmt.Sprintf("SortByDir(%d)", int(s))
	}
}

// SortByNulls represents null ordering - ported from postgres/src/include/nodes/parsenodes.h:64-69
type SortByNulls int

const (
	SORTBY_NULLS_DEFAULT SortByNulls = iota
	SORTBY_NULLS_FIRST
	SORTBY_NULLS_LAST
)

func (s SortByNulls) String() string {
	switch s {
	case SORTBY_NULLS_DEFAULT:
		return "DEFAULT"
	case SORTBY_NULLS_FIRST:
		return "NULLS_FIRST"
	case SORTBY_NULLS_LAST:
		return "NULLS_LAST"
	default:
		return fmt.Sprintf("SortByNulls(%d)", int(s))
	}
}

// RoleSpecType represents types of role specifications - ported from postgres/src/include/nodes/parsenodes.h:383-389
type RoleSpecType int

const (
	ROLESPEC_CSTRING      RoleSpecType = iota // role name is stored as a C string
	ROLESPEC_CURRENT_ROLE                     // role spec is CURRENT_ROLE
	ROLESPEC_CURRENT_USER                     // role spec is CURRENT_USER
	ROLESPEC_SESSION_USER                     // role spec is SESSION_USER
	ROLESPEC_PUBLIC                           // role name is "public"
)

func (r RoleSpecType) String() string {
	switch r {
	case ROLESPEC_CSTRING:
		return "CSTRING"
	case ROLESPEC_CURRENT_ROLE:
		return "CURRENT_ROLE"
	case ROLESPEC_CURRENT_USER:
		return "CURRENT_USER"
	case ROLESPEC_SESSION_USER:
		return "SESSION_USER"
	case ROLESPEC_PUBLIC:
		return "PUBLIC"
	default:
		return fmt.Sprintf("RoleSpecType(%d)", int(r))
	}
}

// RoleSpec represents a role specification.
// Ported from postgres/src/include/nodes/parsenodes.h:401
type RoleSpec struct {
	BaseNode
	Roletype RoleSpecType // Type of this rolespec - postgres/src/include/nodes/parsenodes.h:403
	Rolename string       // filled only for ROLESPEC_CSTRING - postgres/src/include/nodes/parsenodes.h:404
}

// NewRoleSpec creates a new RoleSpec node.
func NewRoleSpec(roletype RoleSpecType, rolename string) *RoleSpec {
	return &RoleSpec{
		BaseNode: BaseNode{Tag: T_RoleSpec},
		Roletype: roletype,
		Rolename: rolename,
	}
}

func (r *RoleSpec) String() string {
	if r.Roletype == ROLESPEC_CSTRING {
		return fmt.Sprintf("RoleSpec(%s)@%d", r.Rolename, r.Location())
	}
	return fmt.Sprintf("RoleSpec(%s)@%d", r.Roletype, r.Location())
}

// SqlString returns the SQL representation of the role specification
func (r *RoleSpec) SqlString() string {
	switch r.Roletype {
	case ROLESPEC_CSTRING:
		return r.Rolename
	case ROLESPEC_CURRENT_USER:
		return "CURRENT_USER"
	case ROLESPEC_SESSION_USER:
		return "SESSION_USER"
	case ROLESPEC_CURRENT_ROLE:
		return "CURRENT_ROLE"
	case ROLESPEC_PUBLIC:
		return "PUBLIC"
	default:
		return r.Rolename
	}
}

// ==============================================================================
// CORE DDL SUPPORTING STRUCTURES
// ==============================================================================

// TypeName represents a type name specification.
// This is a placeholder implementation - full TypeName from parsenodes.h will be implemented later
type TypeName struct {
	BaseNode
	Names       *NodeList // qualified name (list of String)
	TypeOid     Oid       // type's OID (filled in by transformTypeName)
	Setof       bool      // is a set?
	PctType     bool      // %TYPE specified?
	Typmods     *NodeList // type modifier expression(s)
	Typemod     int32     // prespecified type modifier
	ArrayBounds *NodeList // array bounds
}

// stringsToNodeList converts a slice of strings to a NodeList of String nodes
func stringsToNodeList(names []string) *NodeList {
	if len(names) == 0 {
		return nil
	}
	nodeList := NewNodeList()
	for _, name := range names {
		nodeList.Append(NewString(name))
	}
	return nodeList
}

// nodeListToStrings converts a NodeList of String nodes back to a slice of strings
func nodeListToStrings(nodeList *NodeList) []string {
	if nodeList == nil {
		return nil
	}
	var names []string
	for _, item := range nodeList.Items {
		if str, ok := item.(*String); ok {
			names = append(names, QuoteIdentifier(str.SVal))
		}
	}
	return names
}

// GetNames returns the Names as a slice of strings for testing purposes
func (t *TypeName) GetNames() []string {
	return nodeListToStrings(t.Names)
}

// GetKeys returns the Keys as a slice of strings for testing purposes
func (c *Constraint) GetKeys() []string {
	return nodeListToStrings(c.Keys)
}

// GetIncluding returns the Including as a slice of strings for testing purposes
func (c *Constraint) GetIncluding() []string {
	return nodeListToStrings(c.Including)
}

// GetFkAttrs returns the FkAttrs as a slice of strings for testing purposes
func (c *Constraint) GetFkAttrs() []string {
	return nodeListToStrings(c.FkAttrs)
}

// GetPkAttrs returns the PkAttrs as a slice of strings for testing purposes
func (c *Constraint) GetPkAttrs() []string {
	return nodeListToStrings(c.PkAttrs)
}

// GetFkDelSetCols returns the FkDelSetCols as a slice of strings for testing purposes
func (c *Constraint) GetFkDelSetCols() []string {
	return nodeListToStrings(c.FkDelSetCols)
}

// NewTypeName creates a new TypeName node.
func NewTypeName(names []string) *TypeName {
	return &TypeName{
		BaseNode: BaseNode{Tag: T_TypeName},
		Names:    stringsToNodeList(names),
	}
}

func (t *TypeName) String() string {
	typeName := ""
	if t.Names != nil && t.Names.Len() > 0 {
		lastItem := t.Names.Items[t.Names.Len()-1]
		if str, ok := lastItem.(*String); ok {
			typeName = str.SVal
		}
	}
	return fmt.Sprintf("TypeName(%s)@%d", typeName, t.Location())
}

// SqlString returns the SQL representation of the TypeName
// normalizeTypeName converts PostgreSQL internal type names to standard SQL type names
func normalizeTypeName(nameParts []string) string {
	if len(nameParts) == 0 {
		return ""
	}

	// For qualified names, handle schema qualification
	if len(nameParts) > 1 {
		// Strip pg_catalog or public schema for built-in types only
		if len(nameParts) == 2 && (nameParts[0] == "pg_catalog" || nameParts[0] == "public") {
			typeName := nameParts[1]
			if isBuiltInType(typeName) {
				return normalizeSingleTypeName(typeName)
			}
		}

		// For other qualified names, quote each part individually if needed
		var quotedParts []string
		for _, part := range nameParts {
			quotedParts = append(quotedParts, QuoteIdentifier(part))
		}
		return strings.Join(quotedParts, ".")
	}

	// For single names, normalize if it's a built-in type
	return normalizeSingleTypeName(nameParts[0])
}

// isBuiltInType checks if a type name is a built-in PostgreSQL type
func isBuiltInType(typeName string) bool {
	switch strings.ToLower(typeName) {
	case "int4", "int", "int8", "bigint", "int2", "smallint",
		"float", "float4", "real", "float8", "double precision",
		"bool", "boolean", "bpchar", "char", "varchar", "text",
		"numeric", "decimal", "timestamp", "timestamptz",
		"time", "timetz", "date", "interval", "bytea",
		"uuid", "json", "jsonb", "xml":
		return true
	default:
		return false
	}
}

func normalizeSingleTypeName(typeName string) string {
	// Map PostgreSQL internal names to standard SQL names
	switch strings.ToLower(typeName) {
	case "int4", "int":
		return "INT"
	case "int8", "bigint":
		return "BIGINT"
	case "int2", "smallint":
		return "SMALLINT"
	case "float":
		return "FLOAT"
	case "float4", "real":
		return "REAL"
	case "float8":
		return "FLOAT8"
	case "double precision":
		return "DOUBLE PRECISION"
	case "bool", "boolean":
		return "BOOLEAN"
	case "bpchar", "char":
		return "CHAR"
	case "varchar":
		return "VARCHAR"
	case "text":
		return "TEXT"
	case "numeric":
		return "NUMERIC"
	case "decimal":
		return "DECIMAL"
	case "timestamp":
		return "TIMESTAMP"
	case "timestamptz":
		return "TIMESTAMPTZ"
	case "time":
		return "TIME"
	case "timetz":
		return "TIMETZ"
	case "date":
		return "DATE"
	case "interval":
		return "INTERVAL"
	case "bytea":
		return "BYTEA"
	case "uuid":
		return "UUID"
	case "json":
		return "JSON"
	case "jsonb":
		return "JSONB"
	case "xml":
		return "XML"
	default:
		return QuoteIdentifier(typeName)
	}
}

// intervalMaskToString converts an interval mask to its string representation
func intervalMaskToString(mask int) string {
	switch mask {
	case INTERVAL_MASK_YEAR:
		return "YEAR"
	case INTERVAL_MASK_MONTH:
		return "MONTH"
	case INTERVAL_MASK_DAY:
		return "DAY"
	case INTERVAL_MASK_HOUR:
		return "HOUR"
	case INTERVAL_MASK_MINUTE:
		return "MINUTE"
	case INTERVAL_MASK_SECOND:
		return "SECOND"
	case INTERVAL_MASK_YEAR | INTERVAL_MASK_MONTH:
		return "YEAR TO MONTH"
	case INTERVAL_MASK_DAY | INTERVAL_MASK_HOUR:
		return "DAY TO HOUR"
	case INTERVAL_MASK_DAY | INTERVAL_MASK_MINUTE:
		return "DAY TO MINUTE"
	case INTERVAL_MASK_DAY | INTERVAL_MASK_HOUR | INTERVAL_MASK_MINUTE:
		return "DAY TO MINUTE"
	case INTERVAL_MASK_DAY | INTERVAL_MASK_SECOND:
		return "DAY TO SECOND"
	case INTERVAL_MASK_DAY | INTERVAL_MASK_HOUR | INTERVAL_MASK_SECOND:
		return "DAY TO SECOND"
	case INTERVAL_MASK_DAY | INTERVAL_MASK_MINUTE | INTERVAL_MASK_SECOND:
		return "DAY TO SECOND"
	case INTERVAL_MASK_DAY | INTERVAL_MASK_HOUR | INTERVAL_MASK_MINUTE | INTERVAL_MASK_SECOND:
		return "DAY TO SECOND"
	case INTERVAL_MASK_HOUR | INTERVAL_MASK_MINUTE:
		return "HOUR TO MINUTE"
	case INTERVAL_MASK_HOUR | INTERVAL_MASK_SECOND:
		return "HOUR TO SECOND"
	case INTERVAL_MASK_HOUR | INTERVAL_MASK_MINUTE | INTERVAL_MASK_SECOND:
		return "HOUR TO SECOND"
	case INTERVAL_MASK_MINUTE | INTERVAL_MASK_SECOND:
		return "MINUTE TO SECOND"
	case INTERVAL_FULL_RANGE:
		return "FULL_RANGE" // Special marker for precision-only intervals
	default:
		return ""
	}
}

func (t *TypeName) SqlString() string {
	if t.Names == nil || t.Names.Len() == 0 {
		return ""
	}

	var result string

	// Add SETOF prefix if present
	if t.Setof {
		result = "SETOF "
	}

	// Collect name parts from the NodeList
	var nameParts []string
	for _, item := range t.Names.Items {
		if str, ok := item.(*String); ok {
			nameParts = append(nameParts, str.SVal)
		}
	}

	// Use normalizeTypeName to handle both single and qualified names
	typeName := normalizeTypeName(nameParts)

	// Add type modifiers if present
	if t.Typmods != nil && t.Typmods.Len() > 0 {
		var modStrs []string

		// Special handling for INTERVAL types
		if strings.ToLower(typeName) == "interval" && t.Typmods.Len() >= 1 {
			if firstMod, ok := t.Typmods.Items[0].(*Integer); ok {
				if firstMod.IVal == INTERVAL_FULL_RANGE {
					// Skip the INTERVAL_FULL_RANGE (first modifier) and only include the precision (second modifier)
					for i := 1; i < t.Typmods.Len(); i++ {
						if mod := t.Typmods.Items[i]; mod != nil {
							modStrs = append(modStrs, mod.SqlString())
						}
					}
				} else {
					// Convert interval mask to text representation
					intervalUnit := intervalMaskToString(firstMod.IVal)
					if intervalUnit != "" && intervalUnit != "FULL_RANGE" {
						// For specific units like "minute to second", use that instead of numeric
						typeName += " " + strings.ToLower(intervalUnit)

						// Add precision if present as second parameter
						if t.Typmods.Len() >= 2 {
							if precision, ok := t.Typmods.Items[1].(*Integer); ok {
								typeName += "(" + precision.SqlString() + ")"
							}
						}
						// Skip the normal modifier processing since we handled it above
						modStrs = nil
					} else {
						// Fallback to regular handling for unrecognized masks
						for _, mod := range t.Typmods.Items {
							if mod != nil {
								modStrs = append(modStrs, mod.SqlString())
							}
						}
					}
				}
			} else {
				// Regular handling if first modifier is not an Integer
				for _, mod := range t.Typmods.Items {
					if mod != nil {
						modStrs = append(modStrs, mod.SqlString())
					}
				}
			}
		} else {
			// Regular handling for non-INTERVAL types
			for _, mod := range t.Typmods.Items {
				if mod != nil {
					modStrs = append(modStrs, mod.SqlString())
				}
			}
		}

		if len(modStrs) > 0 {
			typeName += "(" + strings.Join(modStrs, ", ") + ")"
		}
	}

	result += typeName

	// Add array bounds if present
	if t.ArrayBounds != nil && t.ArrayBounds.Len() > 0 {
		var boundsBuilder strings.Builder
		for _, bound := range t.ArrayBounds.Items {
			if intBound, ok := bound.(*Integer); ok {
				if intBound.IVal == -1 {
					boundsBuilder.WriteString("[]")
				} else {
					boundsBuilder.WriteString(fmt.Sprintf("[%d]", intBound.IVal))
				}
			}
		}
		result += boundsBuilder.String()
	}

	return result
}

// CollateClause represents a COLLATE clause.
// This is a placeholder implementation - full CollateClause from parsenodes.h will be implemented later
type CollateClause struct {
	BaseNode
	Arg      Node      // input expression
	Collname *NodeList // possibly-qualified collation name
}

// NewCollateClause creates a new CollateClause node.
func NewCollateClause(collname *NodeList) *CollateClause {
	return &CollateClause{
		BaseNode: BaseNode{Tag: T_CollateClause, Loc: -1},
		Collname: collname,
	}
}

func (c *CollateClause) String() string {
	collName := ""
	if c.Collname != nil && c.Collname.Len() > 0 {
		// Get the last element (unqualified name)
		if lastNode := c.Collname.Items[c.Collname.Len()-1]; lastNode != nil {
			if strNode, ok := lastNode.(*String); ok {
				collName = strNode.SVal
			}
		}
	}
	return fmt.Sprintf("CollateClause(%s)@%d", collName, c.Location())
}

// SqlString generates SQL representation of a COLLATE clause
func (c *CollateClause) SqlString() string {
	var parts []string

	// Add the expression if present
	if c.Arg != nil {
		parts = append(parts, c.Arg.SqlString())
	}

	// Add COLLATE and collation name
	if c.Collname != nil && c.Collname.Len() > 0 {
		collNames := make([]string, 0, c.Collname.Len())
		for _, item := range c.Collname.Items {
			if strNode, ok := item.(*String); ok {
				// Use QuoteIdentifier to properly handle identifier quoting
				collNames = append(collNames, QuoteIdentifier(strNode.SVal))
			}
		}
		if len(collNames) > 0 {
			parts = append(parts, "COLLATE", strings.Join(collNames, "."))
		}
	}

	return strings.Join(parts, " ")
}

// ==============================================================================

// DefElem represents a generic name/value pair for options.
// Ported from postgres/src/include/nodes/parsenodes.h:811
type DefElem struct {
	BaseNode
	Defnamespace string        // NULL if unqualified name - postgres/src/include/nodes/parsenodes.h:813
	Defname      string        // postgres/src/include/nodes/parsenodes.h:814
	Arg          Node          // typically Integer, Float, String, or TypeName - postgres/src/include/nodes/parsenodes.h:815
	Defaction    DefElemAction // unspecified action, or SET/ADD/DROP - postgres/src/include/nodes/parsenodes.h:816
}

// NewDefElem creates a new DefElem node.
func NewDefElem(defname string, arg Node) *DefElem {
	return &DefElem{
		BaseNode:  BaseNode{Tag: T_DefElem},
		Defname:   defname,
		Arg:       arg,
		Defaction: DEFELEM_UNSPEC,
	}
}

// NewDefElemExtended creates a new DefElem node with a qualified name.
func NewDefElemExtended(defnamespace, defname string, arg Node, action DefElemAction) *DefElem {
	return &DefElem{
		BaseNode:     BaseNode{Tag: T_DefElem},
		Defnamespace: defnamespace,
		Defname:      defname,
		Arg:          arg,
		Defaction:    action,
	}
}

func (d *DefElem) String() string {
	action := ""
	if d.Defaction != DEFELEM_UNSPEC {
		action = fmt.Sprintf(" %s", d.Defaction)
	}
	return fmt.Sprintf("DefElem(%s%s)@%d", d.Defname, action, d.Location())
}

// SqlString returns the SQL representation of DefElem
func (d *DefElem) SqlString() string {
	if d.Arg != nil {
		argStr := d.Arg.SqlString()
		return fmt.Sprintf("%s = %s", QuoteIdentifier(d.Defname), argStr)
	}
	return QuoteIdentifier(d.Defname)
}

// SqlStringForFunction returns the SQL representation of DefElem for function options
// This handles the special formatting needed for ALTER FUNCTION statements
func (d *DefElem) SqlStringForFunction() string {
	switch d.Defname {
	case "volatility":
		if strNode, ok := d.Arg.(*String); ok {
			return strings.ToUpper(strNode.SVal)
		}
	case "strict":
		if boolNode, ok := d.Arg.(*Boolean); ok {
			if boolNode.BoolVal {
				return "STRICT"
			} else {
				return "CALLED ON NULL INPUT"
			}
		}
	case "leakproof":
		if boolNode, ok := d.Arg.(*Boolean); ok {
			if boolNode.BoolVal {
				return "LEAKPROOF"
			} else {
				return "NOT LEAKPROOF"
			}
		}
	case "security":
		if boolNode, ok := d.Arg.(*Boolean); ok {
			if boolNode.BoolVal {
				return "SECURITY DEFINER"
			} else {
				return "SECURITY INVOKER"
			}
		}
	case "cost":
		if d.Arg != nil {
			return "COST " + d.Arg.SqlString()
		}
	case "rows":
		if d.Arg != nil {
			return "ROWS " + d.Arg.SqlString()
		}
	case "parallel":
		if strNode, ok := d.Arg.(*String); ok {
			return "PARALLEL " + strings.ToUpper(strNode.SVal)
		}
	case "support":
		if d.Arg != nil {
			// For SUPPORT, we want the unquoted function name
			if nodeList, ok := d.Arg.(*NodeList); ok && nodeList.Len() > 0 {
				// Handle qualified names like schema.func_name
				nameStrs := make([]string, 0, nodeList.Len())
				for i := 0; i < nodeList.Len(); i++ {
					if strNode, ok := nodeList.Items[i].(*String); ok {
						nameStrs = append(nameStrs, strNode.SVal)
					}
				}
				return "SUPPORT " + strings.Join(nameStrs, ".")
			} else if strNode, ok := d.Arg.(*String); ok {
				return "SUPPORT " + strNode.SVal
			} else {
				return "SUPPORT " + d.Arg.SqlString()
			}
		}
	case "set":
		// Handle SET/RESET operations via FunctionSetResetClause
		if d.Arg != nil {
			return d.Arg.SqlString()
		}
	default:
		// For other options, fall back to the standard format
		return d.SqlString()
	}

	// Fallback for cases where Arg is nil or doesn't match expected type
	return d.Defname
}

// Constraint represents a constraint definition.
// Ported from postgres/src/include/nodes/parsenodes.h:2728
type Constraint struct {
	BaseNode
	Contype            ConstrType // see above - postgres/src/include/nodes/parsenodes.h:2731
	Conname            string     // Constraint name, or NULL if unnamed - postgres/src/include/nodes/parsenodes.h:2732
	Deferrable         bool       // DEFERRABLE? - postgres/src/include/nodes/parsenodes.h:2733
	Initdeferred       bool       // INITIALLY DEFERRED? - postgres/src/include/nodes/parsenodes.h:2734
	SkipValidation     bool       // skip validation of existing rows? - postgres/src/include/nodes/parsenodes.h:2735
	InitiallyValid     bool       // mark the new constraint as valid? - postgres/src/include/nodes/parsenodes.h:2736
	IsNoInherit        bool       // is constraint non-inheritable? - postgres/src/include/nodes/parsenodes.h:2737
	RawExpr            Node       // CHECK or DEFAULT expression, as untransformed parse tree - postgres/src/include/nodes/parsenodes.h:2738
	CookedExpr         string     // CHECK or DEFAULT expression, as nodeToString representation - postgres/src/include/nodes/parsenodes.h:2740
	GeneratedWhen      byte       // ALWAYS or BY DEFAULT - postgres/src/include/nodes/parsenodes.h:2742
	Inhcount           int        // initial inheritance count to apply - postgres/src/include/nodes/parsenodes.h:2743
	NullsNotDistinct   bool       // null treatment for UNIQUE constraints - postgres/src/include/nodes/parsenodes.h:2744
	Keys               *NodeList  // String nodes naming referenced key column(s) - postgres/src/include/nodes/parsenodes.h:2746
	Including          *NodeList  // String nodes naming referenced nonkey column(s) - postgres/src/include/nodes/parsenodes.h:2747
	Exclusions         *NodeList  // list of (IndexElem, operator name) pairs - postgres/src/include/nodes/parsenodes.h:2748
	Options            *NodeList  // options from WITH clause - postgres/src/include/nodes/parsenodes.h:2749
	Indexname          string     // existing index to use; otherwise NULL - postgres/src/include/nodes/parsenodes.h:2750
	Indexspace         string     // index tablespace; NULL for default - postgres/src/include/nodes/parsenodes.h:2751
	ResetDefaultTblspc bool       // reset default_tablespace prior to creating the index - postgres/src/include/nodes/parsenodes.h:2752
	AccessMethod       string     // access method to use for the index - postgres/src/include/nodes/parsenodes.h:2753
	WhereClause        Node       // partial index predicate - postgres/src/include/nodes/parsenodes.h:2754
	Pktable            *RangeVar  // Primary key table for FOREIGN KEY - postgres/src/include/nodes/parsenodes.h:2755
	FkAttrs            *NodeList  // Attributes of foreign key - postgres/src/include/nodes/parsenodes.h:2756
	PkAttrs            *NodeList  // Corresponding attrs in PK table - postgres/src/include/nodes/parsenodes.h:2757
	FkMatchtype        byte       // FULL, PARTIAL, SIMPLE - postgres/src/include/nodes/parsenodes.h:2758
	FkUpdAction        byte       // ON UPDATE action - postgres/src/include/nodes/parsenodes.h:2759
	FkDelAction        byte       // ON DELETE action - postgres/src/include/nodes/parsenodes.h:2760
	FkDelSetCols       *NodeList  // ON DELETE SET NULL/DEFAULT (column_list) - postgres/src/include/nodes/parsenodes.h:2761
	OldConpfeqop       []Oid      // pg_constraint.conpfeqop of my former self - postgres/src/include/nodes/parsenodes.h:2762
	OldPktableOid      Oid        // pg_class.oid of my former self - postgres/src/include/nodes/parsenodes.h:2763
}

// NewConstraint creates a new Constraint node.
func NewConstraint(contype ConstrType) *Constraint {
	return &Constraint{
		BaseNode:       BaseNode{Tag: T_Constraint},
		Contype:        contype,
		InitiallyValid: true, // Constraints are valid by default unless explicitly marked NOT VALID
	}
}

func (c *Constraint) String() string {
	name := c.Conname
	if name == "" {
		name = "unnamed"
	}
	return fmt.Sprintf("Constraint(%s %s)@%d", c.Contype, name, c.Location())
}

// SqlString generates SQL representation of a constraint
func (c *Constraint) SqlString() string {
	switch c.Contype {
	case CONSTR_NOTNULL:
		return "NOT NULL"
	case CONSTR_NULL:
		return "NULL"
	case CONSTR_DEFAULT:
		if c.RawExpr != nil {
			return "DEFAULT " + c.RawExpr.SqlString()
		}
		return "DEFAULT"
	case CONSTR_IDENTITY:
		var result strings.Builder
		result.WriteString("GENERATED ")
		switch c.GeneratedWhen {
		case ATTRIBUTE_IDENTITY_ALWAYS:
			result.WriteString("ALWAYS")
		case ATTRIBUTE_IDENTITY_BY_DEFAULT:
			result.WriteString("BY DEFAULT")
		}
		result.WriteString(" AS IDENTITY")

		// Add sequence options if present
		if c.Options != nil && len(c.Options.Items) > 0 {
			for _, item := range c.Options.Items {
				if defElem, ok := item.(*DefElem); ok {
					switch defElem.Defname {
					case "increment":
						if defElem.Arg != nil {
							result.WriteString(" SET INCREMENT BY " + defElem.Arg.SqlString())
						}
					case "start":
						if defElem.Arg != nil {
							result.WriteString(" SET START WITH " + defElem.Arg.SqlString())
						}
					case "restart":
						if defElem.Arg != nil {
							result.WriteString(" SET RESTART WITH " + defElem.Arg.SqlString())
						} else {
							result.WriteString(" RESTART")
						}
					case "maxvalue":
						if defElem.Arg != nil {
							result.WriteString(" SET MAXVALUE " + defElem.Arg.SqlString())
						}
					case "minvalue":
						if defElem.Arg != nil {
							result.WriteString(" SET MINVALUE " + defElem.Arg.SqlString())
						}
					case "cache":
						if defElem.Arg != nil {
							result.WriteString(" SET CACHE " + defElem.Arg.SqlString())
						}
					case "cycle":
						if defElem.Arg != nil {
							// Check if it's a boolean and handle accordingly
							if boolNode, ok := defElem.Arg.(*Boolean); ok {
								if boolNode.BoolVal {
									result.WriteString(" SET CYCLE")
								} else {
									result.WriteString(" SET NO CYCLE")
								}
							}
						}
					}
				}
			}
		}
		return result.String()
	case CONSTR_GENERATED:
		result := "GENERATED ALWAYS AS"
		if c.RawExpr != nil {
			result += " (" + c.RawExpr.SqlString() + ")"
		}
		result += " STORED"
		return result
	case CONSTR_PRIMARY:
		result := ""
		if c.Conname != "" {
			result = "CONSTRAINT " + c.Conname + " "
		}
		result += "PRIMARY KEY"
		if c.Keys != nil && c.Keys.Len() > 0 {
			result += " (" + strings.Join(nodeListToStrings(c.Keys), ", ") + ")"
		}
		if c.Indexname != "" {
			result += " USING INDEX " + QuoteIdentifier(c.Indexname)
		}
		return result
	case CONSTR_UNIQUE:
		result := ""
		if c.Conname != "" {
			result = "CONSTRAINT " + c.Conname + " "
		}
		result += "UNIQUE"
		if c.Keys != nil && c.Keys.Len() > 0 {
			result += " (" + strings.Join(nodeListToStrings(c.Keys), ", ") + ")"
		}
		if c.Indexname != "" {
			result += " USING INDEX " + QuoteIdentifier(c.Indexname)
		}
		return result
	case CONSTR_CHECK:
		result := ""
		if c.Conname != "" {
			result = "CONSTRAINT " + c.Conname + " "
		}
		result += "CHECK"
		if c.RawExpr != nil {
			result += " (" + c.RawExpr.SqlString() + ")"
		}

		// Add NO INHERIT if specified
		if c.IsNoInherit {
			result += " NO INHERIT"
		}

		// Add NOT VALID if specified (InitiallyValid = false means NOT VALID)
		if !c.InitiallyValid {
			result += " NOT VALID"
		}

		return result
	case CONSTR_FOREIGN:
		result := ""
		if c.Conname != "" {
			result = "CONSTRAINT " + c.Conname + " "
		}

		// For column-level constraints, don't include "FOREIGN KEY"
		// For table-level constraints, include it
		if c.FkAttrs != nil && c.FkAttrs.Len() > 0 {
			result += "FOREIGN KEY (" + strings.Join(nodeListToStrings(c.FkAttrs), ", ") + ")"
		} else {
			result += "REFERENCES"
		}
		if c.Pktable != nil {
			if c.FkAttrs != nil && c.FkAttrs.Len() > 0 {
				result += " REFERENCES " + c.Pktable.SqlString()
			} else {
				result += " " + c.Pktable.SqlString()
			}
			if c.PkAttrs != nil && c.PkAttrs.Len() > 0 {
				result += "(" + strings.Join(nodeListToStrings(c.PkAttrs), ", ") + ")"
			}
		}

		// Add MATCH clause if explicitly specified (non-zero and not default SIMPLE)
		if c.FkMatchtype != 0 && c.FkMatchtype != FKCONSTR_MATCH_SIMPLE {
			switch c.FkMatchtype {
			case FKCONSTR_MATCH_FULL:
				result += " MATCH FULL"
			case FKCONSTR_MATCH_PARTIAL:
				result += " MATCH PARTIAL"
			}
		}

		// Add foreign key actions (only if explicitly set and not default NO ACTION)
		if c.FkDelAction != 0 && c.FkDelAction != FKCONSTR_ACTION_NOACTION {
			switch c.FkDelAction {
			case FKCONSTR_ACTION_RESTRICT:
				result += " ON DELETE RESTRICT"
			case FKCONSTR_ACTION_CASCADE:
				result += " ON DELETE CASCADE"
			case FKCONSTR_ACTION_SETNULL:
				result += " ON DELETE SET NULL"
				if c.FkDelSetCols != nil && len(c.FkDelSetCols.Items) > 0 {
					result += " (" + strings.Join(nodeListToStrings(c.FkDelSetCols), ", ") + ")"
				}
			case FKCONSTR_ACTION_SETDEFAULT:
				result += " ON DELETE SET DEFAULT"
				if c.FkDelSetCols != nil && len(c.FkDelSetCols.Items) > 0 {
					result += " (" + strings.Join(nodeListToStrings(c.FkDelSetCols), ", ") + ")"
				}
			}
		}

		if c.FkUpdAction != 0 && c.FkUpdAction != FKCONSTR_ACTION_NOACTION {
			switch c.FkUpdAction {
			case FKCONSTR_ACTION_RESTRICT:
				result += " ON UPDATE RESTRICT"
			case FKCONSTR_ACTION_CASCADE:
				result += " ON UPDATE CASCADE"
			case FKCONSTR_ACTION_SETNULL:
				result += " ON UPDATE SET NULL"
			case FKCONSTR_ACTION_SETDEFAULT:
				result += " ON UPDATE SET DEFAULT"
			}
		}

		// Add DEFERRABLE and INITIALLY DEFERRED attributes
		if c.Deferrable {
			result += " DEFERRABLE"
			if c.Initdeferred {
				result += " INITIALLY DEFERRED"
			} else {
				result += " INITIALLY IMMEDIATE"
			}
		}

		// Add NOT VALID if specified (InitiallyValid = false means NOT VALID)
		if !c.InitiallyValid {
			result += " NOT VALID"
		}

		return result
	case CONSTR_EXCLUSION:
		result := ""
		if c.Conname != "" {
			result = "CONSTRAINT " + c.Conname + " "
		}
		result += "EXCLUDE"

		// Add access method if specified
		if c.AccessMethod != "" {
			result += " USING " + c.AccessMethod
		}

		// Add exclusion elements (column WITH operator pairs)
		if c.Exclusions != nil && c.Exclusions.Len() > 0 {
			var exclusionParts []string
			for _, item := range c.Exclusions.Items {
				if nodeList, ok := item.(*NodeList); ok && nodeList.Len() >= 2 {
					// Each exclusion is a pair: (IndexElem, operator name)
					if indexElem := nodeList.Items[0]; indexElem != nil {
						elemStr := indexElem.SqlString()
						if operList, ok := nodeList.Items[1].(*NodeList); ok && operList.Len() > 0 {
							// Get the operator
							if operStr, ok := operList.Items[0].(*String); ok {
								elemStr += " WITH " + operStr.SVal
							}
						}
						exclusionParts = append(exclusionParts, elemStr)
					}
				}
			}
			if len(exclusionParts) > 0 {
				result += " (" + strings.Join(exclusionParts, ", ") + ")"
			}
		}

		// Add WHERE clause if specified
		if c.WhereClause != nil {
			result += " WHERE " + c.WhereClause.SqlString()
		}

		return result
	}
	return ""
}

// ==============================================================================
// ALTER TABLE STATEMENTS
// ==============================================================================

// AlterTableStmt represents an ALTER TABLE statement.
// Ported from postgres/src/include/nodes/parsenodes.h:2339
type AlterTableStmt struct {
	BaseNode
	Relation  *RangeVar  // table to work on - postgres/src/include/nodes/parsenodes.h:2341
	Cmds      *NodeList  // list of subcommands - postgres/src/include/nodes/parsenodes.h:2342
	Objtype   ObjectType // type of object - postgres/src/include/nodes/parsenodes.h:2344
	MissingOk bool       // skip error if table missing - postgres/src/include/nodes/parsenodes.h:2345
}

// NewAlterTableStmt creates a new AlterTableStmt node.
func NewAlterTableStmt(relation *RangeVar, cmds *NodeList) *AlterTableStmt {
	return &AlterTableStmt{
		BaseNode: BaseNode{Tag: T_AlterTableStmt},
		Relation: relation,
		Cmds:     cmds,
		Objtype:  OBJECT_TABLE,
	}
}

func (a *AlterTableStmt) StatementType() string {
	return "AlterTableStmt"
}

func (a *AlterTableStmt) String() string {
	return a.SqlString()
}

// ReplicaIdentityStmt represents a REPLICA IDENTITY statement.
// Ported from postgres/src/include/nodes/parsenodes.h
type ReplicaIdentityStmt struct {
	BaseNode
	IdentityType rune   // replica identity type ('d', 'n', 'f', 'i')
	Name         string // index name for REPLICA_IDENTITY_INDEX, or empty
}

// NewReplicaIdentityStmt creates a new ReplicaIdentityStmt node.
func NewReplicaIdentityStmt(identityType rune, name string) *ReplicaIdentityStmt {
	return &ReplicaIdentityStmt{
		BaseNode:     BaseNode{Tag: T_ReplicaIdentityStmt},
		IdentityType: identityType,
		Name:         name,
	}
}

func (r *ReplicaIdentityStmt) StatementType() string {
	return "ReplicaIdentityStmt"
}

func (r *ReplicaIdentityStmt) String() string {
	if r.Name != "" {
		return fmt.Sprintf("ReplicaIdentityStmt(%c, %s)@%d", r.IdentityType, r.Name, r.Location())
	}
	return fmt.Sprintf("ReplicaIdentityStmt(%c)@%d", r.IdentityType, r.Location())
}

func (r *ReplicaIdentityStmt) SqlString() string {
	switch r.IdentityType {
	case REPLICA_IDENTITY_NOTHING:
		return "REPLICA IDENTITY NOTHING"
	case REPLICA_IDENTITY_FULL:
		return "REPLICA IDENTITY FULL"
	case REPLICA_IDENTITY_DEFAULT:
		return "REPLICA IDENTITY DEFAULT"
	case REPLICA_IDENTITY_INDEX:
		return "REPLICA IDENTITY USING INDEX " + r.Name
	default:
		return "REPLICA IDENTITY"
	}
}

// AlterTableMoveAllStmt represents moving all tables/indexes/views from one tablespace to another.
// Ported from postgres/src/include/nodes/parsenodes.h:2348-2356
type AlterTableMoveAllStmt struct {
	BaseNode
	OrigTablespacename string     // original tablespace name
	Objtype            ObjectType // Object type to move
	Roles              *NodeList  // List of roles to move objects of
	NewTablespacename  string     // new tablespace name
	Nowait             bool       // do not wait for locks
}

// NewAlterTableMoveAllStmt creates a new AlterTableMoveAllStmt node.
func NewAlterTableMoveAllStmt(origTablespace string, objtype ObjectType, newTablespace string) *AlterTableMoveAllStmt {
	return &AlterTableMoveAllStmt{
		BaseNode:           BaseNode{Tag: T_AlterTableMoveAllStmt},
		OrigTablespacename: origTablespace,
		Objtype:            objtype,
		NewTablespacename:  newTablespace,
	}
}

func (a *AlterTableMoveAllStmt) node() {}
func (a *AlterTableMoveAllStmt) stmt() {}

func (a *AlterTableMoveAllStmt) StatementType() string {
	return "AlterTableMoveAllStmt"
}

func (a *AlterTableMoveAllStmt) String() string {
	return fmt.Sprintf("AlterTableMoveAllStmt(%s->%s)@%d", a.OrigTablespacename, a.NewTablespacename, a.Location())
}

func (a *AlterTableMoveAllStmt) SqlString() string {
	var parts []string

	// Start with ALTER
	parts = append(parts, "ALTER")

	// Add object type
	switch a.Objtype {
	case OBJECT_TABLE:
		parts = append(parts, "TABLE")
	case OBJECT_INDEX:
		parts = append(parts, "INDEX")
	case OBJECT_MATVIEW:
		parts = append(parts, "MATERIALIZED VIEW")
	default:
		parts = append(parts, "TABLE")
	}

	// Add ALL IN TABLESPACE
	parts = append(parts, "ALL IN TABLESPACE", a.OrigTablespacename)

	// Add OWNED BY if roles specified
	if a.Roles != nil && a.Roles.Len() > 0 {
		parts = append(parts, "OWNED BY")
		// Extract role names from the NodeList
		roleNames := make([]string, 0, a.Roles.Len())
		for _, item := range a.Roles.Items {
			if roleSpec, ok := item.(*RoleSpec); ok {
				// Handle different role types
				if roleSpec.Rolename != "" {
					roleNames = append(roleNames, roleSpec.Rolename)
				} else {
					// Handle special roles like CURRENT_USER, etc.
					switch roleSpec.Roletype {
					case ROLESPEC_CURRENT_USER:
						roleNames = append(roleNames, "CURRENT_USER")
					case ROLESPEC_CURRENT_ROLE:
						roleNames = append(roleNames, "CURRENT_ROLE")
					case ROLESPEC_SESSION_USER:
						roleNames = append(roleNames, "SESSION_USER")
					}
				}
			}
		}
		parts = append(parts, strings.Join(roleNames, ", "))
	}

	// Add SET TABLESPACE
	parts = append(parts, "SET TABLESPACE", a.NewTablespacename)

	// Add NOWAIT if specified
	if a.Nowait {
		parts = append(parts, "NOWAIT")
	}

	return strings.Join(parts, " ")
}

func (a *AlterTableStmt) SqlString() string {
	var parts []string

	// Start with ALTER
	parts = append(parts, "ALTER")

	// Add object type
	switch a.Objtype {
	case OBJECT_TABLE:
		parts = append(parts, "TABLE")
	case OBJECT_INDEX:
		parts = append(parts, "INDEX")
	case OBJECT_SEQUENCE:
		parts = append(parts, "SEQUENCE")
	case OBJECT_VIEW:
		parts = append(parts, "VIEW")
	case OBJECT_MATVIEW:
		parts = append(parts, "MATERIALIZED VIEW")
	case OBJECT_FOREIGN_TABLE:
		parts = append(parts, "FOREIGN TABLE")
	case OBJECT_TYPE:
		parts = append(parts, "TYPE")
	default:
		parts = append(parts, "TABLE")
	}

	// Add IF EXISTS if specified
	if a.MissingOk {
		parts = append(parts, "IF EXISTS")
	}

	// Add relation name
	if a.Relation != nil {
		parts = append(parts, a.Relation.SqlString())
	}

	// Add commands
	if a.Cmds != nil && a.Cmds.Len() > 0 {
		var cmdStrs []string
		for _, item := range a.Cmds.Items {
			if cmd, ok := item.(*AlterTableCmd); ok && cmd != nil {
				if a.Objtype == OBJECT_TYPE {
					// For composite types, use ATTRIBUTE instead of COLUMN
					cmdStrs = append(cmdStrs, cmd.SqlStringForCompositeType())
				} else {
					cmdStrs = append(cmdStrs, cmd.SqlString())
				}
			}
		}
		if len(cmdStrs) > 0 {
			parts = append(parts, strings.Join(cmdStrs, ", "))
		}
	}

	return strings.Join(parts, " ")
}

// AlterTableCmd represents one subcommand of an ALTER TABLE.
// Ported from postgres/src/include/nodes/parsenodes.h:2426
type AlterTableCmd struct {
	BaseNode
	Subtype   AlterTableType // Type of table alteration to apply - postgres/src/include/nodes/parsenodes.h:2429
	Name      string         // column, constraint, or trigger to act on - postgres/src/include/nodes/parsenodes.h:2430
	Num       int16          // attribute number for columns referenced by number - postgres/src/include/nodes/parsenodes.h:2432
	Newowner  *RoleSpec      // postgres/src/include/nodes/parsenodes.h:2434
	Def       Node           // definition of new column, index, constraint, or parent table - postgres/src/include/nodes/parsenodes.h:2435
	Behavior  DropBehavior   // RESTRICT or CASCADE for DROP cases - postgres/src/include/nodes/parsenodes.h:2437
	MissingOk bool           // skip error if missing? - postgres/src/include/nodes/parsenodes.h:2438
	Recurse   bool           // exec-time recursion - postgres/src/include/nodes/parsenodes.h:2439
}

// NewAlterTableCmd creates a new AlterTableCmd node.
func NewAlterTableCmd(subtype AlterTableType, name string, def Node) *AlterTableCmd {
	return &AlterTableCmd{
		BaseNode: BaseNode{Tag: T_AlterTableCmd},
		Subtype:  subtype,
		Name:     name,
		Def:      def,
		Behavior: DropRestrict,
	}
}

func (a *AlterTableCmd) String() string {
	return fmt.Sprintf("AlterTableCmd(%s %s)@%d", a.Subtype, a.Name, a.Location())
}

func (a *AlterTableCmd) SqlString() string {
	var parts []string

	switch a.Subtype {
	case AT_AddColumn:
		parts = append(parts, "ADD COLUMN")
		if a.MissingOk {
			parts = append(parts, "IF NOT EXISTS")
		}
		if a.Def != nil {
			parts = append(parts, a.Def.SqlString())
		}

	case AT_DropColumn:
		parts = append(parts, "DROP COLUMN")
		if a.MissingOk {
			parts = append(parts, "IF EXISTS")
		}
		parts = append(parts, QuoteIdentifier(a.Name))
		if a.Behavior == DropCascade {
			parts = append(parts, "CASCADE")
		}

	case AT_AlterColumnType:
		parts = append(parts, "ALTER COLUMN", QuoteIdentifier(a.Name), "TYPE")
		if colDef, ok := a.Def.(*ColumnDef); ok && colDef != nil {
			// For ALTER COLUMN TYPE, we need to handle the type and USING clause specially
			if colDef.TypeName != nil {
				parts = append(parts, colDef.TypeName.SqlString())
			}
			// Add USING clause if specified (stored in RawDefault for ALTER COLUMN TYPE)
			if colDef.RawDefault != nil {
				parts = append(parts, "USING", colDef.RawDefault.SqlString())
			}
			// Add collation if specified
			if colDef.Collclause != nil {
				parts = append(parts, colDef.Collclause.SqlString())
			}
		} else if a.Def != nil {
			parts = append(parts, a.Def.SqlString())
		}

	case AT_ColumnDefault:
		parts = append(parts, "ALTER COLUMN", QuoteIdentifier(a.Name))
		if a.Def != nil {
			parts = append(parts, "SET DEFAULT", a.Def.SqlString())
		} else {
			parts = append(parts, "DROP DEFAULT")
		}

	case AT_SetNotNull:
		parts = append(parts, "ALTER COLUMN", QuoteIdentifier(a.Name), "SET NOT NULL")

	case AT_DropNotNull:
		parts = append(parts, "ALTER COLUMN", QuoteIdentifier(a.Name), "DROP NOT NULL")

	case AT_SetStatistics:
		parts = append(parts, "ALTER COLUMN")
		if a.Name != "" {
			parts = append(parts, QuoteIdentifier(a.Name))
		} else {
			parts = append(parts, strconv.Itoa(int(a.Num)))
		}
		parts = append(parts, "SET STATISTICS")
		if a.Def != nil {
			parts = append(parts, a.Def.SqlString())
		}

	case AT_SetExpression:
		parts = append(parts, "ALTER COLUMN", QuoteIdentifier(a.Name), "SET EXPRESSION AS")
		if a.Def != nil {
			parts = append(parts, "(", a.Def.SqlString(), ")")
		}

	case AT_DropExpression:
		parts = append(parts, "ALTER COLUMN", QuoteIdentifier(a.Name), "DROP EXPRESSION")
		if a.MissingOk {
			parts = append(parts, "IF EXISTS")
		}

	case AT_AddConstraint:
		parts = append(parts, "ADD")
		if a.Def != nil {
			// The constraint SqlString already includes CONSTRAINT keyword and name
			parts = append(parts, a.Def.SqlString())
		}

	case AT_DropConstraint:
		parts = append(parts, "DROP CONSTRAINT")
		if a.MissingOk {
			parts = append(parts, "IF EXISTS")
		}
		parts = append(parts, QuoteIdentifier(a.Name))
		if a.Behavior == DropCascade {
			parts = append(parts, "CASCADE")
		}

	case AT_ValidateConstraint:
		parts = append(parts, "VALIDATE CONSTRAINT", QuoteIdentifier(a.Name))

	case AT_SetStorage:
		parts = append(parts, "ALTER COLUMN", QuoteIdentifier(a.Name), "SET STORAGE")
		if a.Def != nil {
			// For storage modes, we want an identifier, not a quoted string literal
			if str, ok := a.Def.(*String); ok {
				parts = append(parts, QuoteIdentifier(str.SVal))
			} else {
				parts = append(parts, a.Def.SqlString())
			}
		}

	case AT_SetCompression:
		parts = append(parts, "ALTER COLUMN", QuoteIdentifier(a.Name), "SET COMPRESSION")
		if a.Def != nil {
			// For compression methods, we want an identifier, not a quoted string literal
			if str, ok := a.Def.(*String); ok {
				parts = append(parts, QuoteIdentifier(str.SVal))
			} else {
				parts = append(parts, a.Def.SqlString())
			}
		}

	case AT_SetOptions:
		parts = append(parts, "ALTER COLUMN", QuoteIdentifier(a.Name), "SET")
		if a.Def != nil {
			parts = append(parts, "("+a.Def.SqlString()+")")
		}

	case AT_ResetOptions:
		parts = append(parts, "ALTER COLUMN", QuoteIdentifier(a.Name), "RESET")
		if a.Def != nil {
			parts = append(parts, "("+a.Def.SqlString()+")")
		}

	case AT_ClusterOn:
		parts = append(parts, "CLUSTER ON", QuoteIdentifier(a.Name))

	case AT_DropCluster:
		parts = append(parts, "SET WITHOUT CLUSTER")

	case AT_SetLogged:
		parts = append(parts, "SET LOGGED")

	case AT_SetUnLogged:
		parts = append(parts, "SET UNLOGGED")

	case AT_SetTableSpace:
		parts = append(parts, "SET TABLESPACE", QuoteIdentifier(a.Name))

	case AT_ChangeOwner:
		parts = append(parts, "OWNER TO")
		if a.Newowner != nil {
			parts = append(parts, a.Newowner.SqlString())
		}

	case AT_AttachPartition:
		parts = append(parts, "ATTACH PARTITION")
		if a.Def != nil {
			// a.Def should be a PartitionCmd containing the partition name and bound spec
			parts = append(parts, a.Def.SqlString())
		}

	case AT_DetachPartition:
		parts = append(parts, "DETACH PARTITION")
		if a.Def != nil {
			// a.Def should be a PartitionCmd containing the partition name
			if partCmd, ok := a.Def.(*PartitionCmd); ok {
				if partCmd.Name != nil {
					parts = append(parts, partCmd.Name.SqlString())
				}
				if partCmd.Concurrent {
					parts = append(parts, "CONCURRENTLY")
				}
			}
		}

	case AT_DetachPartitionFinalize:
		parts = append(parts, "DETACH PARTITION")
		if a.Def != nil {
			// a.Def should be a PartitionCmd containing the partition name
			if partCmd, ok := a.Def.(*PartitionCmd); ok {
				if partCmd.Name != nil {
					parts = append(parts, partCmd.Name.SqlString(), "FINALIZE")
				}
			}
		}

	case AT_AddIdentity:
		parts = append(parts, "ALTER COLUMN", QuoteIdentifier(a.Name), "ADD")
		if a.Def != nil {
			// Handle identity constraint specially for ADD case
			if constraint, ok := a.Def.(*Constraint); ok && constraint.Contype == CONSTR_IDENTITY {
				// Build the identity specification with proper formatting for ADD
				result := "GENERATED "
				switch constraint.GeneratedWhen {
				case ATTRIBUTE_IDENTITY_ALWAYS:
					result += "ALWAYS"
				case ATTRIBUTE_IDENTITY_BY_DEFAULT:
					result += "BY DEFAULT"
				}
				result += " AS IDENTITY"

				// Add sequence options in parentheses (without SET keywords)
				if constraint.Options != nil && len(constraint.Options.Items) > 0 {
					var optParts []string
					for _, item := range constraint.Options.Items {
						if defElem, ok := item.(*DefElem); ok {
							switch defElem.Defname {
							case "increment":
								if defElem.Arg != nil {
									optParts = append(optParts, "INCREMENT BY "+defElem.Arg.SqlString())
								}
							case "start":
								if defElem.Arg != nil {
									optParts = append(optParts, "START WITH "+defElem.Arg.SqlString())
								}
							case "restart":
								if defElem.Arg != nil {
									optParts = append(optParts, "RESTART WITH "+defElem.Arg.SqlString())
								} else {
									optParts = append(optParts, "RESTART")
								}
							case "maxvalue":
								if defElem.Arg != nil {
									optParts = append(optParts, "MAXVALUE "+defElem.Arg.SqlString())
								}
							case "minvalue":
								if defElem.Arg != nil {
									optParts = append(optParts, "MINVALUE "+defElem.Arg.SqlString())
								}
							case "cache":
								if defElem.Arg != nil {
									optParts = append(optParts, "CACHE "+defElem.Arg.SqlString())
								}
							case "cycle":
								if defElem.Arg != nil {
									if boolNode, ok := defElem.Arg.(*Boolean); ok {
										if boolNode.BoolVal {
											optParts = append(optParts, "CYCLE")
										} else {
											optParts = append(optParts, "NO CYCLE")
										}
									}
								}
							}
						}
					}
					if len(optParts) > 0 {
						result += " (" + strings.Join(optParts, " ") + ")"
					}
				}
				parts = append(parts, result)
			} else {
				// Fallback to regular SqlString for non-identity constraints
				parts = append(parts, a.Def.SqlString())
			}
		}

	case AT_SetIdentity:
		parts = append(parts, "ALTER COLUMN", QuoteIdentifier(a.Name), "SET")
		if a.Def != nil {
			// Handle identity specifications - could be a Constraint or DefElems
			if constraint, ok := a.Def.(*Constraint); ok {
				parts = append(parts, constraint.SqlString())
			} else if nodeList, ok := a.Def.(*NodeList); ok {
				// Handle NodeList of DefElems for identity options
				var identityParts []string
				var hasGenerated bool

				// First pass - check for 'generated' option to determine ALWAYS vs BY DEFAULT
				for _, item := range nodeList.Items {
					if defElem, ok := item.(*DefElem); ok && defElem.Defname == "generated" {
						hasGenerated = true
						// For SET IDENTITY, we typically want BY DEFAULT unless specified otherwise
						identityParts = append(identityParts, "GENERATED BY DEFAULT")
						break
					}
				}

				if !hasGenerated {
					// Default to BY DEFAULT if no generated option specified
					identityParts = append(identityParts, "GENERATED BY DEFAULT")
				}

				// Second pass - handle sequence options
				for _, item := range nodeList.Items {
					if defElem, ok := item.(*DefElem); ok {
						switch defElem.Defname {
						case "increment":
							if defElem.Arg != nil {
								identityParts = append(identityParts, "SET INCREMENT BY "+defElem.Arg.SqlString())
							}
						case "start":
							if defElem.Arg != nil {
								identityParts = append(identityParts, "SET START WITH "+defElem.Arg.SqlString())
							}
						case "restart":
							if defElem.Arg != nil {
								identityParts = append(identityParts, "SET RESTART WITH "+defElem.Arg.SqlString())
							} else {
								identityParts = append(identityParts, "RESTART")
							}
						case "maxvalue":
							if defElem.Arg != nil {
								identityParts = append(identityParts, "SET MAXVALUE "+defElem.Arg.SqlString())
							}
						case "minvalue":
							if defElem.Arg != nil {
								identityParts = append(identityParts, "SET MINVALUE "+defElem.Arg.SqlString())
							}
						case "cache":
							if defElem.Arg != nil {
								identityParts = append(identityParts, "SET CACHE "+defElem.Arg.SqlString())
							}
						case "cycle":
							if defElem.Arg != nil {
								if boolNode, ok := defElem.Arg.(*Boolean); ok {
									if boolNode.BoolVal {
										identityParts = append(identityParts, "SET CYCLE")
									} else {
										identityParts = append(identityParts, "SET NO CYCLE")
									}
								}
							}
						// Skip the 'generated' option as it's handled above
						case "generated":
							// Already handled in first pass
						}
					}
				}
				parts = append(parts, strings.Join(identityParts, " "))
			} else {
				// Fallback to default SqlString
				parts = append(parts, a.Def.SqlString())
			}
		}

	case AT_DropIdentity:
		parts = append(parts, "ALTER COLUMN", QuoteIdentifier(a.Name), "DROP IDENTITY")

	case AT_ReplicaIdentity:
		if a.Def != nil {
			parts = append(parts, a.Def.SqlString())
		} else if a.Name != "" {
			parts = append(parts, "REPLICA IDENTITY USING INDEX", a.Name)
		} else {
			parts = append(parts, "REPLICA IDENTITY")
		}

	case AT_AlterConstraint:
		parts = append(parts, "ALTER CONSTRAINT")
		if constraint, ok := a.Def.(*Constraint); ok && constraint != nil {
			if constraint.Conname != "" {
				parts = append(parts, constraint.Conname)
			}
			// Add DEFERRABLE and INITIALLY DEFERRED attributes
			if constraint.Deferrable {
				parts = append(parts, "DEFERRABLE")
				if constraint.Initdeferred {
					parts = append(parts, "INITIALLY DEFERRED")
				} else {
					parts = append(parts, "INITIALLY IMMEDIATE")
				}
			} else {
				parts = append(parts, "NOT DEFERRABLE")
			}
		}

	case AT_AddInherit:
		parts = append(parts, "INHERIT")
		if rangeVar, ok := a.Def.(*RangeVar); ok && rangeVar != nil {
			parts = append(parts, rangeVar.SqlString())
		}

	case AT_DropInherit:
		parts = append(parts, "NO INHERIT")
		if rangeVar, ok := a.Def.(*RangeVar); ok && rangeVar != nil {
			parts = append(parts, rangeVar.SqlString())
		}

	case AT_AddOf:
		parts = append(parts, "OF")
		if a.Def != nil {
			parts = append(parts, a.Def.SqlString())
		}

	case AT_DropOf:
		parts = append(parts, "NOT OF")

	case AT_SetRelOptions:
		parts = append(parts, "SET")
		if a.Def != nil {
			parts = append(parts, "("+a.Def.SqlString()+")")
		}

	case AT_ResetRelOptions:
		parts = append(parts, "RESET")
		if a.Def != nil {
			parts = append(parts, "("+a.Def.SqlString()+")")
		}

	case AT_DropOids:
		parts = append(parts, "SET WITHOUT OIDS")

	case AT_SetAccessMethod:
		parts = append(parts, "SET ACCESS METHOD")
		if a.Name != "" {
			parts = append(parts, a.Name)
		} else {
			// When Name is empty, it means DEFAULT was explicitly specified
			parts = append(parts, "DEFAULT")
		}

	case AT_EnableRowSecurity:
		parts = append(parts, "ENABLE ROW LEVEL SECURITY")

	case AT_DisableRowSecurity:
		parts = append(parts, "DISABLE ROW LEVEL SECURITY")

	case AT_ForceRowSecurity:
		parts = append(parts, "FORCE ROW LEVEL SECURITY")

	case AT_NoForceRowSecurity:
		parts = append(parts, "NO FORCE ROW LEVEL SECURITY")

	case AT_AlterColumnGenericOptions:
		parts = append(parts, "ALTER COLUMN", QuoteIdentifier(a.Name), "OPTIONS")
		if a.Def != nil {
			// Handle DefElem with ADD/SET/DROP actions for column options
			if defElem, ok := a.Def.(*DefElem); ok {
				var optStr string
				switch defElem.Defaction {
				case DEFELEM_ADD:
					if defElem.Arg != nil {
						optStr = "ADD " + QuoteIdentifier(defElem.Defname) + " " + defElem.Arg.SqlString()
					} else {
						optStr = "ADD " + QuoteIdentifier(defElem.Defname)
					}
				case DEFELEM_SET:
					if defElem.Arg != nil {
						optStr = "SET " + QuoteIdentifier(defElem.Defname) + " " + defElem.Arg.SqlString()
					} else {
						optStr = "SET " + QuoteIdentifier(defElem.Defname)
					}
				case DEFELEM_DROP:
					optStr = "DROP " + QuoteIdentifier(defElem.Defname)
				default:
					// Fallback to regular format - defaction might be DEFELEM_UNSPEC or something else
					if defElem.Arg != nil {
						optStr = "ADD " + QuoteIdentifier(defElem.Defname) + " " + defElem.Arg.SqlString()
					} else {
						optStr = "ADD " + QuoteIdentifier(defElem.Defname)
					}
				}
				parts = append(parts, "("+optStr+")")
			} else if nodeList, ok := a.Def.(*NodeList); ok {
				// Handle NodeList of DefElems
				var optStrs []string
				for _, item := range nodeList.Items {
					if defElem, ok := item.(*DefElem); ok {
						var optStr string
						switch defElem.Defaction {
						case DEFELEM_ADD:
							if defElem.Arg != nil {
								optStr = "ADD " + QuoteIdentifier(defElem.Defname) + " " + defElem.Arg.SqlString()
							} else {
								optStr = "ADD " + QuoteIdentifier(defElem.Defname)
							}
						case DEFELEM_SET:
							if defElem.Arg != nil {
								optStr = "SET " + QuoteIdentifier(defElem.Defname) + " " + defElem.Arg.SqlString()
							} else {
								optStr = "SET " + QuoteIdentifier(defElem.Defname)
							}
						case DEFELEM_DROP:
							optStr = "DROP " + QuoteIdentifier(defElem.Defname)
						default:
							// Fallback - assume ADD if no action specified
							if defElem.Arg != nil {
								optStr = "ADD " + QuoteIdentifier(defElem.Defname) + " " + defElem.Arg.SqlString()
							} else {
								optStr = "ADD " + QuoteIdentifier(defElem.Defname)
							}
						}
						optStrs = append(optStrs, optStr)
					}
				}
				parts = append(parts, "("+strings.Join(optStrs, ", ")+")")
			} else {
				parts = append(parts, "("+a.Def.SqlString()+")")
			}
		}

	case AT_GenericOptions:
		parts = append(parts, "OPTIONS")
		if a.Def != nil {
			// Handle DefElem with ADD/SET/DROP actions for table-level options
			if defElem, ok := a.Def.(*DefElem); ok {
				var optStr string
				switch defElem.Defaction {
				case DEFELEM_ADD:
					if defElem.Arg != nil {
						optStr = "ADD " + QuoteIdentifier(defElem.Defname) + " " + defElem.Arg.SqlString()
					} else {
						optStr = "ADD " + QuoteIdentifier(defElem.Defname)
					}
				case DEFELEM_SET:
					if defElem.Arg != nil {
						optStr = "SET " + QuoteIdentifier(defElem.Defname) + " " + defElem.Arg.SqlString()
					} else {
						optStr = "SET " + QuoteIdentifier(defElem.Defname)
					}
				case DEFELEM_DROP:
					optStr = "DROP " + QuoteIdentifier(defElem.Defname)
				default:
					// Fallback - assume ADD if no action specified
					if defElem.Arg != nil {
						optStr = "ADD " + QuoteIdentifier(defElem.Defname) + " " + defElem.Arg.SqlString()
					} else {
						optStr = "ADD " + QuoteIdentifier(defElem.Defname)
					}
				}
				parts = append(parts, "("+optStr+")")
			} else if nodeList, ok := a.Def.(*NodeList); ok {
				// Handle NodeList of DefElems for multiple options
				var optStrs []string
				for _, item := range nodeList.Items {
					if defElem, ok := item.(*DefElem); ok {
						var optStr string
						switch defElem.Defaction {
						case DEFELEM_ADD:
							if defElem.Arg != nil {
								optStr = "ADD " + QuoteIdentifier(defElem.Defname) + " " + defElem.Arg.SqlString()
							} else {
								optStr = "ADD " + QuoteIdentifier(defElem.Defname)
							}
						case DEFELEM_SET:
							if defElem.Arg != nil {
								optStr = "SET " + QuoteIdentifier(defElem.Defname) + " " + defElem.Arg.SqlString()
							} else {
								optStr = "SET " + QuoteIdentifier(defElem.Defname)
							}
						case DEFELEM_DROP:
							optStr = "DROP " + QuoteIdentifier(defElem.Defname)
						default:
							// Fallback - assume ADD if no action specified
							if defElem.Arg != nil {
								optStr = "ADD " + QuoteIdentifier(defElem.Defname) + " " + defElem.Arg.SqlString()
							} else {
								optStr = "ADD " + QuoteIdentifier(defElem.Defname)
							}
						}
						optStrs = append(optStrs, optStr)
					}
				}
				parts = append(parts, "("+strings.Join(optStrs, ", ")+")")
			} else {
				parts = append(parts, "("+a.Def.SqlString()+")")
			}
		}

	case AT_EnableTrig:
		parts = append(parts, "ENABLE TRIGGER", QuoteIdentifier(a.Name))

	case AT_EnableAlwaysTrig:
		parts = append(parts, "ENABLE ALWAYS TRIGGER", QuoteIdentifier(a.Name))

	case AT_EnableReplicaTrig:
		parts = append(parts, "ENABLE REPLICA TRIGGER", QuoteIdentifier(a.Name))

	case AT_DisableTrig:
		parts = append(parts, "DISABLE TRIGGER", QuoteIdentifier(a.Name))

	case AT_EnableTrigAll:
		parts = append(parts, "ENABLE TRIGGER ALL")

	case AT_DisableTrigAll:
		parts = append(parts, "DISABLE TRIGGER ALL")

	case AT_EnableTrigUser:
		parts = append(parts, "ENABLE TRIGGER USER")

	case AT_DisableTrigUser:
		parts = append(parts, "DISABLE TRIGGER USER")

	case AT_EnableRule:
		parts = append(parts, "ENABLE RULE")
		if a.Name != "" {
			parts = append(parts, QuoteIdentifier(a.Name))
		}

	case AT_EnableAlwaysRule:
		parts = append(parts, "ENABLE ALWAYS RULE")
		if a.Name != "" {
			parts = append(parts, QuoteIdentifier(a.Name))
		}

	case AT_EnableReplicaRule:
		parts = append(parts, "ENABLE REPLICA RULE")
		if a.Name != "" {
			parts = append(parts, QuoteIdentifier(a.Name))
		}

	case AT_DisableRule:
		parts = append(parts, "DISABLE RULE")
		if a.Name != "" {
			parts = append(parts, QuoteIdentifier(a.Name))
		}

	default:
		// Fallback for unhandled subtypes
		parts = append(parts, fmt.Sprintf("/* %s */", a.Subtype.String()))
		if a.Name != "" {
			parts = append(parts, QuoteIdentifier(a.Name))
		}
	}

	return strings.Join(parts, " ")
}

// SqlStringForCompositeType returns the SQL string for composite type operations using ATTRIBUTE instead of COLUMN
func (a *AlterTableCmd) SqlStringForCompositeType() string {
	var parts []string

	switch a.Subtype {
	case AT_AddColumn:
		parts = append(parts, "ADD ATTRIBUTE")
		if a.MissingOk {
			parts = append(parts, "IF NOT EXISTS")
		}
		if a.Def != nil {
			parts = append(parts, a.Def.SqlString())
		}
		// Add CASCADE behavior if specified
		if a.Behavior == DropCascade {
			parts = append(parts, "CASCADE")
		}

	case AT_DropColumn:
		parts = append(parts, "DROP ATTRIBUTE")
		if a.MissingOk {
			parts = append(parts, "IF EXISTS")
		}
		parts = append(parts, QuoteIdentifier(a.Name))
		if a.Behavior == DropCascade {
			parts = append(parts, "CASCADE")
		}

	case AT_AlterColumnType:
		parts = append(parts, "ALTER ATTRIBUTE", QuoteIdentifier(a.Name), "TYPE")
		if colDef, ok := a.Def.(*ColumnDef); ok && colDef != nil {
			// For ALTER ATTRIBUTE, we only want the type part, not the full column definition
			if colDef.TypeName != nil {
				parts = append(parts, colDef.TypeName.SqlString())
			}
			// Add collation if specified
			if colDef.Collclause != nil {
				parts = append(parts, colDef.Collclause.SqlString())
			}
		} else if a.Def != nil {
			parts = append(parts, a.Def.SqlString())
		}
		// Add CASCADE or RESTRICT behavior if specified
		if a.Behavior == DropCascade {
			parts = append(parts, "CASCADE")
		}

	case AT_ColumnDefault:
		parts = append(parts, "ALTER ATTRIBUTE", a.Name)
		if a.Def != nil {
			parts = append(parts, "SET DEFAULT", a.Def.SqlString())
		} else {
			parts = append(parts, "DROP DEFAULT")
		}

	default:
		// For other operations, use the regular SqlString method
		return a.SqlString()
	}

	return strings.Join(parts, " ")
}

// ==============================================================================
// INDEX STATEMENTS
// ==============================================================================

// IndexStmt represents a CREATE INDEX statement.
// Ported from postgres/src/include/nodes/parsenodes.h:3348
type IndexStmt struct {
	BaseNode
	Idxname                  string    // name of new index, or NULL for default - postgres/src/include/nodes/parsenodes.h:3350
	Relation                 *RangeVar // relation to build index on - postgres/src/include/nodes/parsenodes.h:3351
	AccessMethod             string    // name of access method (eg. btree) - postgres/src/include/nodes/parsenodes.h:3352
	TableSpace               string    // tablespace, or NULL for default - postgres/src/include/nodes/parsenodes.h:3353
	IndexParams              *NodeList // columns to index: a list of IndexElem - postgres/src/include/nodes/parsenodes.h:3354
	IndexIncludingParams     *NodeList // additional columns to index - postgres/src/include/nodes/parsenodes.h:3355
	Options                  *NodeList // WITH clause options: a list of DefElem - postgres/src/include/nodes/parsenodes.h:3357
	WhereClause              Node      // qualification (partial-index predicate) - postgres/src/include/nodes/parsenodes.h:3358
	ExcludeOpNames           *NodeList // exclusion operator names, or NIL if none - postgres/src/include/nodes/parsenodes.h:3359
	Idxcomment               string    // comment to apply to index, or NULL - postgres/src/include/nodes/parsenodes.h:3360
	IndexOid                 Oid       // OID of an existing index, if any - postgres/src/include/nodes/parsenodes.h:3361
	OldNumber                uint32    // relfilenumber of existing storage, if any - postgres/src/include/nodes/parsenodes.h:3362
	OldCreateSubid           uint32    // rd_createSubid of existing storage, if any - postgres/src/include/nodes/parsenodes.h:3363
	OldFirstRelfilenodeSubid uint32    // rd_firstRelfilenodeSubid of existing storage - postgres/src/include/nodes/parsenodes.h:3364
	Unique                   bool      // is index unique? - postgres/src/include/nodes/parsenodes.h:3365
	NullsNotDistinct         bool      // null treatment in unique index - postgres/src/include/nodes/parsenodes.h:3366
	Primary                  bool      // is index a primary key? - postgres/src/include/nodes/parsenodes.h:3367
	Isconstraint             bool      // is it for a pkey/unique constraint? - postgres/src/include/nodes/parsenodes.h:3368
	Deferrable               bool      // is the constraint DEFERRABLE? - postgres/src/include/nodes/parsenodes.h:3369
	Initdeferred             bool      // is the constraint INITIALLY DEFERRED? - postgres/src/include/nodes/parsenodes.h:3370
	Transformed              bool      // true when transformIndexStmt is finished - postgres/src/include/nodes/parsenodes.h:3371
	Concurrent               bool      // should this be a concurrent index build? - postgres/src/include/nodes/parsenodes.h:3372
	IfNotExists              bool      // just do nothing if index already exists? - postgres/src/include/nodes/parsenodes.h:3373
	ResetDefaultTblspc       bool      // reset default_tablespace prior to creating the index - postgres/src/include/nodes/parsenodes.h:3374
}

// NewIndexStmt creates a new IndexStmt node.
func NewIndexStmt(idxname string, relation *RangeVar, indexParams *NodeList) *IndexStmt {
	return &IndexStmt{
		BaseNode:     BaseNode{Tag: T_IndexStmt},
		Idxname:      idxname,
		Relation:     relation,
		IndexParams:  indexParams,
		AccessMethod: "btree", // default access method
	}
}

func (i *IndexStmt) StatementType() string {
	return "IndexStmt"
}

func (i *IndexStmt) String() string {
	unique := ""
	if i.Unique {
		unique = " UNIQUE"
	}
	return fmt.Sprintf("IndexStmt(%s%s on %s)@%d", i.Idxname, unique, i.Relation.RelName, i.Location())
}

// IndexElem represents one index element (column or expression).
// Ported from postgres/src/include/nodes/parsenodes.h:780
type IndexElem struct {
	BaseNode
	Name          string      // name of attribute to index, or NULL - postgres/src/include/nodes/parsenodes.h:782
	Expr          Node        // expression to index, or NULL - postgres/src/include/nodes/parsenodes.h:783
	Indexcolname  string      // name for index column; NULL = default - postgres/src/include/nodes/parsenodes.h:784
	Collation     *NodeList   // name of collation; NIL = default - postgres/src/include/nodes/parsenodes.h:785
	Opclass       *NodeList   // name of desired opclass; NIL = default - postgres/src/include/nodes/parsenodes.h:786
	Opclassopts   *NodeList   // opclass-specific options, or NIL - postgres/src/include/nodes/parsenodes.h:787
	Ordering      SortByDir   // ASC/DESC/default - postgres/src/include/nodes/parsenodes.h:788
	NullsOrdering SortByNulls // FIRST/LAST/default - postgres/src/include/nodes/parsenodes.h:789
}

// NewIndexElem creates a new IndexElem node.
func NewIndexElem(name string) *IndexElem {
	return &IndexElem{
		BaseNode:      BaseNode{Tag: T_IndexElem},
		Name:          name,
		Ordering:      SORTBY_DEFAULT,
		NullsOrdering: SORTBY_NULLS_DEFAULT,
	}
}

func (i *IndexElem) String() string {
	target := i.Name
	if target == "" && i.Expr != nil {
		target = "expr"
	}
	order := ""
	if i.Ordering != SORTBY_DEFAULT {
		order = fmt.Sprintf(" %s", i.Ordering)
	}
	return fmt.Sprintf("IndexElem(%s%s)@%d", target, order, i.Location())
}

func (i *IndexElem) SqlString() string {
	var result string

	// Get the base column name or expression
	if i.Name != "" {
		result = QuoteIdentifier(i.Name)
	} else if i.Expr != nil {
		result = "(" + i.Expr.SqlString() + ")"
	} else {
		return ""
	}

	// Add collation if specified (before operator class)
	if i.Collation != nil && i.Collation.Len() > 0 {
		// Collation names should be output as identifiers, not string literals
		var collationParts []string
		for _, item := range i.Collation.Items {
			if strNode, ok := item.(*String); ok {
				// Quote as identifier if needed
				collationParts = append(collationParts, QuoteIdentifier(strNode.SVal))
			} else if item != nil {
				collationParts = append(collationParts, item.SqlString())
			}
		}
		if len(collationParts) > 0 {
			result += " COLLATE " + strings.Join(collationParts, ".")
		}
	}

	// Add operator class if specified (after collation)
	if i.Opclass != nil && i.Opclass.Len() > 0 {
		// Format operator class as identifier, not quoted string
		var opclassParts []string
		for _, item := range i.Opclass.Items {
			if strNode, ok := item.(*String); ok {
				opclassParts = append(opclassParts, strNode.SVal)
			} else if item != nil {
				opclassParts = append(opclassParts, item.SqlString())
			}
		}
		if len(opclassParts) > 0 {
			result += " " + strings.Join(opclassParts, ".")
		}
	}

	// Add operator class options if specified
	if i.Opclassopts != nil && i.Opclassopts.Len() > 0 {
		// Format operator class options with no spaces around '='
		var optParts []string
		for _, item := range i.Opclassopts.Items {
			if defElem, ok := item.(*DefElem); ok {
				if defElem.Arg != nil {
					optParts = append(optParts, fmt.Sprintf("%s = %s", defElem.Defname, defElem.Arg.SqlString()))
				} else {
					optParts = append(optParts, defElem.Defname)
				}
			} else if item != nil {
				optParts = append(optParts, item.SqlString())
			}
		}
		if len(optParts) > 0 {
			result += "(" + strings.Join(optParts, ", ") + ")"
		}
	}

	// Add ordering (ASC/DESC) if not default
	switch i.Ordering {
	case SORTBY_ASC:
		result += " asc"
	case SORTBY_DESC:
		result += " desc"
		// SORTBY_DEFAULT means no explicit ordering clause
	}

	// Add null ordering if not default
	switch i.NullsOrdering {
	case SORTBY_NULLS_FIRST:
		result += " nulls first"
	case SORTBY_NULLS_LAST:
		result += " nulls last"
		// SORTBY_NULLS_DEFAULT means no explicit nulls clause
	}

	return result
}

// ==============================================================================
// VIEW STATEMENTS
// ==============================================================================

// ViewStmt represents a CREATE VIEW statement.
// Ported from postgres/src/include/nodes/parsenodes.h:3740
type ViewStmt struct {
	BaseNode
	View            *RangeVar       // the view to be created - postgres/src/include/nodes/parsenodes.h:3742
	Aliases         *NodeList       // target column names - postgres/src/include/nodes/parsenodes.h:3743
	Query           Node            // the SELECT query (as a raw parse tree) - postgres/src/include/nodes/parsenodes.h:3744
	Replace         bool            // replace an existing view? - postgres/src/include/nodes/parsenodes.h:3745
	Options         *NodeList       // options from WITH clause - postgres/src/include/nodes/parsenodes.h:3746
	WithCheckOption ViewCheckOption // WITH CHECK OPTION - postgres/src/include/nodes/parsenodes.h:3747
}

// NewViewStmt creates a new ViewStmt node.
func NewViewStmt(view *RangeVar, query Node, replace bool) *ViewStmt {
	return &ViewStmt{
		BaseNode:        BaseNode{Tag: T_ViewStmt},
		View:            view,
		Query:           query,
		Replace:         replace,
		WithCheckOption: NO_CHECK_OPTION,
	}
}

func (v *ViewStmt) StatementType() string {
	return "ViewStmt"
}

func (v *ViewStmt) String() string {
	replace := ""
	if v.Replace {
		replace = " OR REPLACE"
	}
	return fmt.Sprintf("ViewStmt(%s%s)@%d", v.View.RelName, replace, v.Location())
}

// SqlString returns the SQL representation of ViewStmt
func (v *ViewStmt) SqlString() string {
	var parts []string

	// CREATE [OR REPLACE] [TEMP] [RECURSIVE] VIEW
	parts = append(parts, "CREATE")
	if v.Replace {
		parts = append(parts, "OR REPLACE")
	}

	// Add TEMP/TEMPORARY if needed
	if v.View != nil && v.View.RelPersistence == RELPERSISTENCE_TEMP {
		parts = append(parts, "TEMPORARY")
	} else if v.View != nil && v.View.RelPersistence == RELPERSISTENCE_UNLOGGED {
		parts = append(parts, "UNLOGGED")
	}

	// TODO: Add RECURSIVE support when we can detect recursive views
	// For now, we can't easily determine if a view is recursive from the ViewStmt

	parts = append(parts, "VIEW")

	// View name
	if v.View != nil {
		parts = append(parts, v.View.SqlString())
	}

	// Column aliases
	if v.Aliases != nil && v.Aliases.Len() > 0 {
		var aliasStrs []string
		for _, item := range v.Aliases.Items {
			if alias, ok := item.(*String); ok {
				aliasStrs = append(aliasStrs, alias.SVal)
			}
		}
		if len(aliasStrs) > 0 {
			parts = append(parts, fmt.Sprintf("(%s)", strings.Join(aliasStrs, ", ")))
		}
	}

	// WITH options
	if v.Options != nil && v.Options.Len() > 0 {
		parts = append(parts, "WITH (")
		var optStrs []string
		for _, item := range v.Options.Items {
			if opt, ok := item.(*DefElem); ok {
				optStrs = append(optStrs, opt.SqlString())
			}
		}
		parts = append(parts, strings.Join(optStrs, ", ")+")")
	}

	// AS query
	parts = append(parts, "AS")
	if v.Query != nil {
		parts = append(parts, v.Query.SqlString())
	}

	// WITH CHECK OPTION
	switch v.WithCheckOption {
	case LOCAL_CHECK_OPTION:
		parts = append(parts, "WITH LOCAL CHECK OPTION")
	case CASCADED_CHECK_OPTION:
		parts = append(parts, "WITH CHECK OPTION")
	case NO_CHECK_OPTION:
		// No check option
	}

	return strings.Join(parts, " ")
}

// ==============================================================================
// DOMAIN STATEMENTS
// ==============================================================================

// AlterDomainStmt represents an ALTER DOMAIN statement.
// Ported from postgres/src/include/nodes/parsenodes.h:2461
type AlterDomainStmt struct {
	BaseNode
	Subtype   byte         // T/N/O/C/X for alter type - postgres/src/include/nodes/parsenodes.h:2463
	TypeName  *NodeList    // domain to work on - postgres/src/include/nodes/parsenodes.h:2464
	Name      string       // column or constraint name to act on - postgres/src/include/nodes/parsenodes.h:2465
	Def       Node         // definition of default or constraint - postgres/src/include/nodes/parsenodes.h:2466
	Behavior  DropBehavior // RESTRICT or CASCADE for DROP cases - postgres/src/include/nodes/parsenodes.h:2467
	MissingOk bool         // skip error if missing? - postgres/src/include/nodes/parsenodes.h:2468
}

// NewAlterDomainStmt creates a new AlterDomainStmt node.
func NewAlterDomainStmt(subtype byte, typeName *NodeList) *AlterDomainStmt {
	return &AlterDomainStmt{
		BaseNode: BaseNode{Tag: T_AlterDomainStmt},
		Subtype:  subtype,
		TypeName: typeName,
		Behavior: DropRestrict,
	}
}

func (a *AlterDomainStmt) StatementType() string {
	return "AlterDomainStmt"
}

func (a *AlterDomainStmt) String() string {
	domainName := ""
	if a.TypeName != nil && len(a.TypeName.Items) > 0 {
		if str, ok := a.TypeName.Items[len(a.TypeName.Items)-1].(*String); ok {
			domainName = str.SVal // last part is the domain name
		}
	}
	return fmt.Sprintf("AlterDomainStmt(%s)@%d", domainName, a.Location())
}

// SqlString returns the SQL representation of ALTER DOMAIN statement.
func (a *AlterDomainStmt) SqlString() string {
	var parts []string
	parts = append(parts, "ALTER DOMAIN")

	// Add domain name
	if a.TypeName != nil && len(a.TypeName.Items) > 0 {
		var nameStrs []string
		for _, item := range a.TypeName.Items {
			if str, ok := item.(*String); ok {
				nameStrs = append(nameStrs, str.SVal)
			}
		}
		parts = append(parts, strings.Join(nameStrs, "."))
	}

	// Handle different subtype commands
	switch a.Subtype {
	case 'T': // SET DEFAULT or DROP DEFAULT
		if a.Def != nil {
			parts = append(parts, "SET DEFAULT")
			if defNode, ok := a.Def.(interface{ SqlString() string }); ok {
				parts = append(parts, defNode.SqlString())
			}
		} else {
			parts = append(parts, "DROP DEFAULT")
		}
	case 'N': // DROP NOT NULL
		parts = append(parts, "DROP NOT NULL")
	case 'O': // SET NOT NULL
		parts = append(parts, "SET NOT NULL")
	case 'C': // ADD CONSTRAINT
		parts = append(parts, "ADD")
		if a.Def != nil {
			if defNode, ok := a.Def.(interface{ SqlString() string }); ok {
				parts = append(parts, defNode.SqlString())
			}
		}
	case 'X': // DROP CONSTRAINT
		parts = append(parts, "DROP CONSTRAINT")
		if a.MissingOk {
			parts = append(parts, "IF EXISTS")
		}
		if a.Name != "" {
			parts = append(parts, a.Name)
		}
		if a.Behavior == DropCascade {
			parts = append(parts, "CASCADE")
		}
	case 'V': // VALIDATE CONSTRAINT
		parts = append(parts, "VALIDATE CONSTRAINT")
		if a.Name != "" {
			parts = append(parts, a.Name)
		}
	}

	return strings.Join(parts, " ")
}

// CreateDomainStmt represents a CREATE DOMAIN statement.
// Ported from postgres/src/include/nodes/parsenodes.h:3156
type CreateDomainStmt struct {
	BaseNode
	Domainname  *NodeList      // qualified name (list of String) - postgres/src/include/nodes/parsenodes.h:3158
	TypeName    *TypeName      // the base type - postgres/src/include/nodes/parsenodes.h:3159
	CollClause  *CollateClause // untransformed COLLATE spec, if any - postgres/src/include/nodes/parsenodes.h:3160
	Constraints *NodeList      // constraints (list of Constraint nodes) - postgres/src/include/nodes/parsenodes.h:3161
}

// NewCreateDomainStmt creates a new CreateDomainStmt node.
func NewCreateDomainStmt(domainname *NodeList, typeName *TypeName) *CreateDomainStmt {
	return &CreateDomainStmt{
		BaseNode:   BaseNode{Tag: T_CreateDomainStmt},
		Domainname: domainname,
		TypeName:   typeName,
	}
}

func (c *CreateDomainStmt) StatementType() string {
	return "CreateDomainStmt"
}

func (c *CreateDomainStmt) String() string {
	domainName := ""
	if c.Domainname != nil && len(c.Domainname.Items) > 0 {
		if str, ok := c.Domainname.Items[len(c.Domainname.Items)-1].(*String); ok {
			domainName = str.SVal
		}
	}
	return fmt.Sprintf("CreateDomainStmt(%s)@%d", domainName, c.Location())
}

// SqlString returns the SQL representation of CREATE DOMAIN statement.
func (c *CreateDomainStmt) SqlString() string {
	var parts []string
	parts = append(parts, "CREATE DOMAIN")

	// Add domain name
	if c.Domainname != nil && len(c.Domainname.Items) > 0 {
		var nameStrs []string
		for _, item := range c.Domainname.Items {
			if str, ok := item.(*String); ok {
				nameStrs = append(nameStrs, str.SVal)
			}
		}
		parts = append(parts, strings.Join(nameStrs, "."))
	}

	// Add AS keyword
	parts = append(parts, "AS")

	// Add type name
	if c.TypeName != nil {
		parts = append(parts, c.TypeName.SqlString())
	}

	// Add constraints
	if c.Constraints != nil {
		for _, item := range c.Constraints.Items {
			if constraint, ok := item.(*Constraint); ok {
				parts = append(parts, constraint.SqlString())
			}
		}
	}

	// Add collate clause
	if c.CollClause != nil {
		parts = append(parts, c.CollClause.SqlString())
	}

	return strings.Join(parts, " ")
}

// ==============================================================================
// SCHEMA STATEMENTS
// ==============================================================================

// CreateSchemaStmt represents a CREATE SCHEMA statement.
// Ported from postgres/src/include/nodes/parsenodes.h:2320
type CreateSchemaStmt struct {
	BaseNode
	Schemaname  string    // the name of the schema to create - postgres/src/include/nodes/parsenodes.h:2322
	Authrole    *RoleSpec // the owner of the created schema - postgres/src/include/nodes/parsenodes.h:2323
	SchemaElts  *NodeList // schema components (list of parsenodes) - postgres/src/include/nodes/parsenodes.h:2324
	IfNotExists bool      // just do nothing if schema already exists? - postgres/src/include/nodes/parsenodes.h:2325
}

// NewCreateSchemaStmt creates a new CreateSchemaStmt node.
func NewCreateSchemaStmt(schemaname string, ifNotExists bool) *CreateSchemaStmt {
	return &CreateSchemaStmt{
		BaseNode:    BaseNode{Tag: T_CreateSchemaStmt},
		Schemaname:  schemaname,
		IfNotExists: ifNotExists,
	}
}

func (c *CreateSchemaStmt) StatementType() string {
	return "CreateSchemaStmt"
}

func (c *CreateSchemaStmt) String() string {
	ifNotExists := ""
	if c.IfNotExists {
		ifNotExists = " IF NOT EXISTS"
	}
	return fmt.Sprintf("CreateSchemaStmt(%s%s)@%d", c.Schemaname, ifNotExists, c.Location())
}

// SqlString returns SQL representation of the CREATE SCHEMA statement
func (c *CreateSchemaStmt) SqlString() string {
	var parts []string

	parts = append(parts, "CREATE SCHEMA")

	if c.IfNotExists {
		parts = append(parts, "IF NOT EXISTS")
	}

	// Add schema name if specified
	if c.Schemaname != "" {
		parts = append(parts, QuoteIdentifier(c.Schemaname))
	}

	// Add AUTHORIZATION clause if specified
	if c.Authrole != nil {
		parts = append(parts, "AUTHORIZATION", c.Authrole.SqlString())
	}

	result := strings.Join(parts, " ")

	// Add embedded statements if any
	if c.SchemaElts != nil && c.SchemaElts.Len() > 0 {
		var stmtStrs []string
		for _, item := range c.SchemaElts.Items {
			if item != nil {
				stmtStrs = append(stmtStrs, item.SqlString())
			}
		}
		if len(stmtStrs) > 0 {
			result += " " + strings.Join(stmtStrs, " ")
		}
	}

	return result
}

// ==============================================================================
// EXTENSION STATEMENTS
// ==============================================================================

// CreateExtensionStmt represents a CREATE EXTENSION statement.
// Ported from postgres/src/include/nodes/parsenodes.h:2819
type CreateExtensionStmt struct {
	BaseNode
	Extname     string    // extension name - postgres/src/include/nodes/parsenodes.h:2821
	IfNotExists bool      // just do nothing if it already exists? - postgres/src/include/nodes/parsenodes.h:2822
	Options     *NodeList // List of DefElem nodes - postgres/src/include/nodes/parsenodes.h:2823
}

// NewCreateExtensionStmt creates a new CreateExtensionStmt node.
func NewCreateExtensionStmt(extname string, ifNotExists bool, options *NodeList) *CreateExtensionStmt {
	return &CreateExtensionStmt{
		BaseNode:    BaseNode{Tag: T_CreateExtensionStmt},
		Extname:     extname,
		IfNotExists: ifNotExists,
		Options:     options,
	}
}

func (c *CreateExtensionStmt) StatementType() string {
	return "CreateExtensionStmt"
}

func (c *CreateExtensionStmt) String() string {
	ifNotExists := ""
	if c.IfNotExists {
		ifNotExists = " IF NOT EXISTS"
	}
	return fmt.Sprintf("CreateExtensionStmt(%s%s)@%d", c.Extname, ifNotExists, c.Location())
}

// formatExtensionOption formats an extension option DefElem for SQL output
func formatExtensionOption(opt *DefElem) string {
	if opt == nil {
		return ""
	}

	switch opt.Defname {
	case "schema":
		if opt.Arg != nil {
			// Schema names are identifiers - use proper identifier quoting
			if s, ok := opt.Arg.(*String); ok {
				return "SCHEMA " + QuoteIdentifier(s.SVal)
			}
			return "SCHEMA " + opt.Arg.SqlString()
		}
		return ""
	case "version":
		if opt.Arg != nil {
			return "VERSION " + opt.Arg.SqlString()
		}
		return ""
	case "cascade":
		if b, ok := opt.Arg.(*Boolean); ok && b.BoolVal {
			return "CASCADE"
		}
		return ""
	default:
		// Fall back to default formatting
		return opt.SqlString()
	}
}

// SqlString returns the SQL representation of CreateExtensionStmt
func (c *CreateExtensionStmt) SqlString() string {
	var parts []string
	parts = append(parts, "CREATE EXTENSION")

	if c.IfNotExists {
		parts = append(parts, "IF NOT EXISTS")
	}

	parts = append(parts, QuoteIdentifier(c.Extname))

	// Add options if present
	if c.Options != nil && c.Options.Len() > 0 {
		for _, item := range c.Options.Items {
			if opt, ok := item.(*DefElem); ok && opt != nil {
				optStr := formatExtensionOption(opt)
				if optStr != "" {
					parts = append(parts, optStr)
				}
			}
		}
	}

	return strings.Join(parts, " ")
}

// AlterExtensionStmt represents an ALTER EXTENSION statement.
// Ported from postgres/src/include/nodes/parsenodes.h:2834
type AlterExtensionStmt struct {
	BaseNode
	Extname string    // extension name
	Options *NodeList // list of DefElem nodes
}

// NewAlterExtensionStmt creates a new AlterExtensionStmt node.
func NewAlterExtensionStmt(extname string, options *NodeList) *AlterExtensionStmt {
	return &AlterExtensionStmt{
		BaseNode: BaseNode{Tag: T_AlterExtensionStmt},
		Extname:  extname,
		Options:  options,
	}
}

func (a *AlterExtensionStmt) StatementType() string {
	return "AlterExtensionStmt"
}

func (a *AlterExtensionStmt) String() string {
	return fmt.Sprintf("AlterExtensionStmt(%s)@%d", a.Extname, a.Location())
}

// SqlString returns the SQL representation of AlterExtensionStmt
func (a *AlterExtensionStmt) SqlString() string {
	var parts []string
	parts = append(parts, "ALTER EXTENSION", a.Extname, "UPDATE")

	// Add options if present
	if a.Options != nil && a.Options.Len() > 0 {
		for _, item := range a.Options.Items {
			if opt, ok := item.(*DefElem); ok && opt != nil {
				// Special handling for ALTER EXTENSION TO option
				if opt.Defname == "to" && opt.Arg != nil {
					parts = append(parts, "TO", opt.Arg.SqlString())
				} else {
					parts = append(parts, opt.SqlString())
				}
			}
		}
	}

	return strings.Join(parts, " ")
}

// AlterExtensionContentsStmt represents an ALTER EXTENSION ADD/DROP statement.
// Ported from postgres/src/include/nodes/parsenodes.h:2842
type AlterExtensionContentsStmt struct {
	BaseNode
	Extname string // extension name
	Action  bool   // true = ADD, false = DROP
	Objtype int    // object type from ObjectType enum
	Object  Node   // qualified name of the object (can be String, NodeList, TypeName, etc.)
}

// NewAlterExtensionContentsStmt creates a new AlterExtensionContentsStmt node.
func NewAlterExtensionContentsStmt(extname string, action bool, objtype int, object Node) *AlterExtensionContentsStmt {
	return &AlterExtensionContentsStmt{
		BaseNode: BaseNode{Tag: T_AlterExtensionContentsStmt},
		Extname:  extname,
		Action:   action,
		Objtype:  objtype,
		Object:   object,
	}
}

func (a *AlterExtensionContentsStmt) StatementType() string {
	return "AlterExtensionContentsStmt"
}

func (a *AlterExtensionContentsStmt) String() string {
	action := "DROP"
	if a.Action {
		action = "ADD"
	}
	return fmt.Sprintf("AlterExtensionContentsStmt(%s %s)@%d", a.Extname, action, a.Location())
}

// SqlString returns the SQL representation of AlterExtensionContentsStmt
func (a *AlterExtensionContentsStmt) SqlString() string {
	var parts []string
	parts = append(parts, "ALTER EXTENSION", a.Extname)

	if a.Action {
		parts = append(parts, "ADD")
	} else {
		parts = append(parts, "DROP")
	}

	// Add object type and handle special formatting cases
	switch a.Objtype {
	case int(OBJECT_AGGREGATE):
		parts = append(parts, "AGGREGATE")
	case int(OBJECT_CAST):
		parts = append(parts, "CAST")
		// For CAST, the object is a NodeList with two TypeNames
		if nodeList, ok := a.Object.(*NodeList); ok && nodeList.Len() >= 2 {
			parts = append(parts, "(", nodeList.Items[0].SqlString(), "AS", nodeList.Items[1].SqlString(), ")")
			return strings.Join(parts, " ")
		}
	case int(OBJECT_DOMAIN):
		parts = append(parts, "DOMAIN")
	case int(OBJECT_FUNCTION):
		parts = append(parts, "FUNCTION")
	case int(OBJECT_OPCLASS):
		parts = append(parts, "OPERATOR CLASS")
		// For OPERATOR CLASS, need to handle "name USING method" format
		if nodeList, ok := a.Object.(*NodeList); ok && nodeList.Len() >= 2 {
			// First item is method name, rest are class name parts
			methodStr := nodeList.Items[0].SqlString()
			var nameStr strings.Builder
			for i := 1; i < nodeList.Len(); i++ {
				if i > 1 {
					nameStr.WriteString(".")
				}
				nameStr.WriteString(nodeList.Items[i].SqlString())
			}
			parts = append(parts, nameStr.String(), "USING", methodStr)
			return strings.Join(parts, " ")
		}
	case int(OBJECT_OPFAMILY):
		parts = append(parts, "OPERATOR FAMILY")
		// For OPERATOR FAMILY, need to handle "name USING method" format
		if nodeList, ok := a.Object.(*NodeList); ok && nodeList.Len() >= 2 {
			// First item is method name, rest are family name parts
			methodStr := nodeList.Items[0].SqlString()
			var nameStr strings.Builder
			for i := 1; i < nodeList.Len(); i++ {
				if i > 1 {
					nameStr.WriteString(".")
				}
				nameStr.WriteString(nodeList.Items[i].SqlString())
			}
			parts = append(parts, nameStr.String(), "USING", methodStr)
			return strings.Join(parts, " ")
		}
	case int(OBJECT_PROCEDURE):
		parts = append(parts, "PROCEDURE")
	case int(OBJECT_ROUTINE):
		parts = append(parts, "ROUTINE")
	case int(OBJECT_TYPE):
		parts = append(parts, "TYPE")
	case int(OBJECT_TABLE):
		parts = append(parts, "TABLE")
	case int(OBJECT_VIEW):
		parts = append(parts, "VIEW")
	default:
		// For other object types, use simple case conversion
		parts = append(parts, "OBJECT")
	}

	// Add object name for simple cases (non-special formatting)
	if a.Object != nil && a.Objtype != int(OBJECT_CAST) && a.Objtype != int(OBJECT_OPCLASS) && a.Objtype != int(OBJECT_OPFAMILY) {
		parts = append(parts, a.Object.SqlString())
	}

	return strings.Join(parts, " ")
}

// ==============================================================================
// DDL CONVENIENCE CONSTRUCTORS
// ==============================================================================

// NewPrimaryKeyConstraint creates a PRIMARY KEY constraint.
func NewPrimaryKeyConstraint(conname string, keys []string) *Constraint {
	constraint := NewConstraint(CONSTR_PRIMARY)
	constraint.Conname = conname
	constraint.Keys = stringsToNodeList(keys)
	return constraint
}

// NewForeignKeyConstraint creates a FOREIGN KEY constraint.
func NewForeignKeyConstraint(conname string, fkAttrs []string, pktable *RangeVar, pkAttrs []string) *Constraint {
	constraint := NewConstraint(CONSTR_FOREIGN)
	constraint.Conname = conname
	constraint.FkAttrs = stringsToNodeList(fkAttrs)
	constraint.Pktable = pktable
	constraint.PkAttrs = stringsToNodeList(pkAttrs)
	return constraint
}

// NewUniqueConstraint creates a UNIQUE constraint.
func NewUniqueConstraint(conname string, keys []string) *Constraint {
	constraint := NewConstraint(CONSTR_UNIQUE)
	constraint.Conname = conname
	constraint.Keys = stringsToNodeList(keys)
	return constraint
}

// NewUniqueConstraintNullsNotDistinct creates a UNIQUE constraint with NULLS NOT DISTINCT.
func NewUniqueConstraintNullsNotDistinct(conname string, keys []string) *Constraint {
	constraint := NewConstraint(CONSTR_UNIQUE)
	constraint.Conname = conname
	constraint.Keys = stringsToNodeList(keys)
	constraint.NullsNotDistinct = true
	return constraint
}

// NewCheckConstraint creates a CHECK constraint.
func NewCheckConstraint(conname string, rawExpr Node) *Constraint {
	constraint := NewConstraint(CONSTR_CHECK)
	constraint.Conname = conname
	constraint.RawExpr = rawExpr
	return constraint
}

// NewNotNullConstraint creates a NOT NULL constraint.
func NewNotNullConstraint(conname string) *Constraint {
	constraint := NewConstraint(CONSTR_NOTNULL)
	constraint.Conname = conname
	return constraint
}

// NewAddColumnCmd creates an ALTER TABLE ADD COLUMN command.
func NewAddColumnCmd(columnName string, columnDef Node) *AlterTableCmd {
	return NewAlterTableCmd(AT_AddColumn, columnName, columnDef)
}

// NewDropColumnCmd creates an ALTER TABLE DROP COLUMN command.
func NewDropColumnCmd(columnName string, behavior DropBehavior) *AlterTableCmd {
	cmd := NewAlterTableCmd(AT_DropColumn, columnName, nil)
	cmd.Behavior = behavior
	return cmd
}

// NewAddConstraintCmd creates an ALTER TABLE ADD CONSTRAINT command.
func NewAddConstraintCmd(constraint *Constraint) *AlterTableCmd {
	return NewAlterTableCmd(AT_AddConstraint, constraint.Conname, constraint)
}

// NewDropConstraintCmd creates an ALTER TABLE DROP CONSTRAINT command.
func NewDropConstraintCmd(constraintName string, behavior DropBehavior) *AlterTableCmd {
	cmd := NewAlterTableCmd(AT_DropConstraint, constraintName, nil)
	cmd.Behavior = behavior
	return cmd
}

// NewUniqueIndex creates a unique index statement.
func NewUniqueIndex(idxname string, relation *RangeVar, indexParams *NodeList) *IndexStmt {
	idx := NewIndexStmt(idxname, relation, indexParams)
	idx.Unique = true
	return idx
}

// NewBtreeIndexElem creates a B-tree index element with default ordering.
func NewBtreeIndexElem(columnName string) *IndexElem {
	return NewIndexElem(columnName)
}

// NewDescIndexElem creates a descending index element.
func NewDescIndexElem(columnName string) *IndexElem {
	elem := NewIndexElem(columnName)
	elem.Ordering = SORTBY_DESC
	return elem
}

// NewExpressionIndexElem creates an index element on an expression.
func NewExpressionIndexElem(expr Node) *IndexElem {
	elem := &IndexElem{
		BaseNode:      BaseNode{Tag: T_IndexElem},
		Expr:          expr,
		Ordering:      SORTBY_DEFAULT,
		NullsOrdering: SORTBY_NULLS_DEFAULT,
	}
	return elem
}

// SqlString returns the SQL representation of CREATE INDEX statement
func (i *IndexStmt) SqlString() string {
	var parts []string

	parts = append(parts, "CREATE")

	// Add UNIQUE if specified
	if i.Unique {
		parts = append(parts, "UNIQUE")
	}

	parts = append(parts, "INDEX")

	// Add CONCURRENTLY if specified
	if i.Concurrent {
		parts = append(parts, "CONCURRENTLY")
	}

	// Add IF NOT EXISTS if specified
	if i.IfNotExists {
		parts = append(parts, "IF NOT EXISTS")
	}

	// Add index name
	if i.Idxname != "" {
		parts = append(parts, QuoteIdentifier(i.Idxname))
	}

	// Add ON table
	parts = append(parts, "ON")
	if i.Relation != nil {
		parts = append(parts, i.Relation.SqlString())
	}

	// Add access method if specified
	if i.AccessMethod != "" {
		parts = append(parts, "USING", i.AccessMethod)
	}

	// Add index columns
	if i.IndexParams != nil && i.IndexParams.Len() > 0 {
		var columnParts []string
		for _, item := range i.IndexParams.Items {
			if param, ok := item.(*IndexElem); ok && param != nil {
				columnParts = append(columnParts, param.SqlString())
			}
		}
		parts = append(parts, "( "+strings.Join(columnParts, ", ")+" )")
	}

	// Add INCLUDE columns if specified
	if i.IndexIncludingParams != nil && i.IndexIncludingParams.Len() > 0 {
		var includeParts []string
		for _, item := range i.IndexIncludingParams.Items {
			if param, ok := item.(*IndexElem); ok && param != nil {
				includeParts = append(includeParts, param.SqlString())
			}
		}
		parts = append(parts, "INCLUDE", "("+strings.Join(includeParts, ", ")+")")
	}

	// Add NULLS NOT DISTINCT if specified
	if i.NullsNotDistinct {
		parts = append(parts, "NULLS NOT DISTINCT")
	}

	// Add WITH options if specified
	if i.Options != nil && i.Options.Len() > 0 {
		var optParts []string
		for _, item := range i.Options.Items {
			if opt, ok := item.(*DefElem); ok && opt != nil {
				optParts = append(optParts, opt.SqlString())
			}
		}
		if len(optParts) > 0 {
			parts = append(parts, "WITH", "("+strings.Join(optParts, ", ")+")")
		}
	}

	// Add tablespace if specified
	if i.TableSpace != "" {
		parts = append(parts, "TABLESPACE", i.TableSpace)
	}

	// Add WHERE clause if specified
	if i.WhereClause != nil {
		parts = append(parts, "WHERE", i.WhereClause.SqlString())
	}

	return strings.Join(parts, " ")
}

// ==============================================================================
// FOREIGN DATA WRAPPER AST NODES
// ==============================================================================

// CreateFdwStmt represents CREATE FOREIGN DATA WRAPPER statement
// Ported from postgres/src/include/nodes/parsenodes.h CreateFdwStmt
type CreateFdwStmt struct {
	BaseNode
	FdwName     string    `json:"fdwname"`      // foreign data wrapper name
	FuncOptions *NodeList `json:"func_options"` // HANDLER/VALIDATOR options
	Options     *NodeList `json:"options"`      // OPTIONS clause
}

// NewCreateFdwStmt creates a new CreateFdwStmt node
func NewCreateFdwStmt(fdwname string, funcOptions, options *NodeList) *CreateFdwStmt {
	return &CreateFdwStmt{
		BaseNode:    BaseNode{Tag: T_CreateFdwStmt},
		FdwName:     fdwname,
		FuncOptions: funcOptions,
		Options:     options,
	}
}

func (c *CreateFdwStmt) StatementType() string {
	return "CreateFdwStmt"
}

func (c *CreateFdwStmt) String() string {
	return fmt.Sprintf("CreateFdwStmt(%s)@%d", c.FdwName, c.Location())
}

// SqlString returns the SQL representation of CreateFdwStmt
func (c *CreateFdwStmt) SqlString() string {
	var parts []string
	parts = append(parts, "CREATE FOREIGN DATA WRAPPER", c.FdwName)

	// Add HANDLER/VALIDATOR options
	if c.FuncOptions != nil && c.FuncOptions.Len() > 0 {
		for _, item := range c.FuncOptions.Items {
			if opt, ok := item.(*DefElem); ok && opt != nil {
				// Special handling for HANDLER and VALIDATOR
				switch opt.Defname {
				case "handler":
					if opt.Arg != nil {
						if nodeList, ok := opt.Arg.(*NodeList); ok {
							// Handle qualified function names (schema.function)
							var funcParts []string
							for _, funcItem := range nodeList.Items {
								if strNode, ok := funcItem.(*String); ok {
									funcParts = append(funcParts, strNode.SVal)
								}
							}
							parts = append(parts, "HANDLER", strings.Join(funcParts, "."))
						} else {
							parts = append(parts, "HANDLER", opt.Arg.SqlString())
						}
					}
				case "validator":
					if opt.Arg != nil {
						if nodeList, ok := opt.Arg.(*NodeList); ok {
							// Handle qualified function names (schema.function)
							var funcParts []string
							for _, funcItem := range nodeList.Items {
								if strNode, ok := funcItem.(*String); ok {
									funcParts = append(funcParts, strNode.SVal)
								}
							}
							parts = append(parts, "VALIDATOR", strings.Join(funcParts, "."))
						} else {
							parts = append(parts, "VALIDATOR", opt.Arg.SqlString())
						}
					}
				default:
					// For other options, use the standard format
					parts = append(parts, opt.SqlString())
				}
			}
		}
	}

	// Add OPTIONS clause
	if c.Options != nil && c.Options.Len() > 0 {
		var optParts []string
		for _, item := range c.Options.Items {
			if opt, ok := item.(*DefElem); ok && opt != nil {
				// For generic options, use PostgreSQL format: key 'value' (no =)
				if opt.Arg != nil {
					optParts = append(optParts, QuoteIdentifier(opt.Defname)+" "+opt.Arg.SqlString())
				} else {
					optParts = append(optParts, QuoteIdentifier(opt.Defname))
				}
			}
		}
		parts = append(parts, "OPTIONS", "("+strings.Join(optParts, ", ")+")")
	}

	return strings.Join(parts, " ")
}

// AlterFdwStmt represents ALTER FOREIGN DATA WRAPPER statement
// Ported from postgres/src/include/nodes/parsenodes.h AlterFdwStmt
type AlterFdwStmt struct {
	BaseNode
	FdwName     string    `json:"fdwname"`      // foreign data wrapper name
	FuncOptions *NodeList `json:"func_options"` // HANDLER/VALIDATOR options
	Options     *NodeList `json:"options"`      // OPTIONS clause
}

// NewAlterFdwStmt creates a new AlterFdwStmt node
func NewAlterFdwStmt(fdwname string, funcOptions, options *NodeList) *AlterFdwStmt {
	return &AlterFdwStmt{
		BaseNode:    BaseNode{Tag: T_AlterFdwStmt},
		FdwName:     fdwname,
		FuncOptions: funcOptions,
		Options:     options,
	}
}

func (a *AlterFdwStmt) StatementType() string {
	return "AlterFdwStmt"
}

func (a *AlterFdwStmt) String() string {
	return fmt.Sprintf("AlterFdwStmt(%s)@%d", a.FdwName, a.Location())
}

// SqlString returns the SQL representation of AlterFdwStmt
func (a *AlterFdwStmt) SqlString() string {
	var parts []string
	parts = append(parts, "ALTER FOREIGN DATA WRAPPER", a.FdwName)

	// Add HANDLER/VALIDATOR options
	if a.FuncOptions != nil && a.FuncOptions.Len() > 0 {
		for _, item := range a.FuncOptions.Items {
			if opt, ok := item.(*DefElem); ok && opt != nil {
				// Special handling for HANDLER and VALIDATOR
				switch opt.Defname {
				case "handler":
					if opt.Arg != nil {
						if nodeList, ok := opt.Arg.(*NodeList); ok {
							// Handle qualified function names (schema.function)
							var funcParts []string
							for _, funcItem := range nodeList.Items {
								if strNode, ok := funcItem.(*String); ok {
									funcParts = append(funcParts, strNode.SVal)
								}
							}
							parts = append(parts, "HANDLER", strings.Join(funcParts, "."))
						} else {
							parts = append(parts, "HANDLER", opt.Arg.SqlString())
						}
					} else {
						// NO HANDLER case
						parts = append(parts, "NO HANDLER")
					}
				case "validator":
					if opt.Arg != nil {
						if nodeList, ok := opt.Arg.(*NodeList); ok {
							// Handle qualified function names (schema.function)
							var funcParts []string
							for _, funcItem := range nodeList.Items {
								if strNode, ok := funcItem.(*String); ok {
									funcParts = append(funcParts, strNode.SVal)
								}
							}
							parts = append(parts, "VALIDATOR", strings.Join(funcParts, "."))
						} else {
							parts = append(parts, "VALIDATOR", opt.Arg.SqlString())
						}
					} else {
						// NO VALIDATOR case
						parts = append(parts, "NO VALIDATOR")
					}
				default:
					// For other options, use the standard format
					parts = append(parts, opt.SqlString())
				}
			}
		}
	}

	// Add OPTIONS clause
	if a.Options != nil && a.Options.Len() > 0 {
		var optParts []string
		for _, item := range a.Options.Items {
			if opt, ok := item.(*DefElem); ok && opt != nil {
				// For ALTER options, include the action (ADD/SET/DROP) prefix
				var optStr string
				switch opt.Defaction {
				case DEFELEM_ADD:
					if opt.Arg != nil {
						optStr = "ADD " + QuoteIdentifier(opt.Defname) + " " + opt.Arg.SqlString()
					} else {
						optStr = "ADD " + QuoteIdentifier(opt.Defname)
					}
				case DEFELEM_SET:
					if opt.Arg != nil {
						optStr = "SET " + QuoteIdentifier(opt.Defname) + " " + opt.Arg.SqlString()
					} else {
						optStr = "SET " + QuoteIdentifier(opt.Defname)
					}
				case DEFELEM_DROP:
					optStr = "DROP " + QuoteIdentifier(opt.Defname)
				default:
					// DEFELEM_UNSPEC or other - use without action prefix
					if opt.Arg != nil {
						optStr = QuoteIdentifier(opt.Defname) + " " + opt.Arg.SqlString()
					} else {
						optStr = QuoteIdentifier(opt.Defname)
					}
				}
				optParts = append(optParts, optStr)
			}
		}
		parts = append(parts, "OPTIONS", "("+strings.Join(optParts, ", ")+")")
	}

	return strings.Join(parts, " ")
}

// AlterForeignServerStmt represents ALTER SERVER statement
// Ported from postgres/src/include/nodes/parsenodes.h AlterForeignServerStmt
type AlterForeignServerStmt struct {
	BaseNode
	Servername string    `json:"servername"`  // server name
	Version    string    `json:"version"`     // optional server version
	Options    *NodeList `json:"options"`     // OPTIONS clause
	HasVersion bool      `json:"has_version"` // whether version was specified
}

// NewAlterForeignServerStmt creates a new AlterForeignServerStmt node
func NewAlterForeignServerStmt(servername, version string, options *NodeList, hasVersion bool) *AlterForeignServerStmt {
	return &AlterForeignServerStmt{
		BaseNode:   BaseNode{Tag: T_AlterForeignServerStmt},
		Servername: servername,
		Version:    version,
		Options:    options,
		HasVersion: hasVersion,
	}
}

func (a *AlterForeignServerStmt) StatementType() string {
	return "AlterForeignServerStmt"
}

func (a *AlterForeignServerStmt) String() string {
	return fmt.Sprintf("AlterForeignServerStmt(%s)@%d", a.Servername, a.Location())
}

// SqlString returns the SQL representation of AlterForeignServerStmt
func (a *AlterForeignServerStmt) SqlString() string {
	var parts []string
	parts = append(parts, "ALTER SERVER", a.Servername)

	if a.HasVersion && a.Version != "" {
		parts = append(parts, "VERSION", "'"+a.Version+"'")
	}

	// Add OPTIONS clause
	if a.Options != nil && a.Options.Len() > 0 {
		var optParts []string
		for _, item := range a.Options.Items {
			if opt, ok := item.(*DefElem); ok && opt != nil {
				// For ALTER options, include the action (ADD/SET/DROP) prefix
				var optStr string
				switch opt.Defaction {
				case DEFELEM_ADD:
					if opt.Arg != nil {
						optStr = "ADD " + QuoteIdentifier(opt.Defname) + " " + opt.Arg.SqlString()
					} else {
						optStr = "ADD " + QuoteIdentifier(opt.Defname)
					}
				case DEFELEM_SET:
					if opt.Arg != nil {
						optStr = "SET " + QuoteIdentifier(opt.Defname) + " " + opt.Arg.SqlString()
					} else {
						optStr = "SET " + QuoteIdentifier(opt.Defname)
					}
				case DEFELEM_DROP:
					optStr = "DROP " + QuoteIdentifier(opt.Defname)
				default:
					// DEFELEM_UNSPEC or other - use without action prefix
					if opt.Arg != nil {
						optStr = QuoteIdentifier(opt.Defname) + " " + opt.Arg.SqlString()
					} else {
						optStr = QuoteIdentifier(opt.Defname)
					}
				}
				optParts = append(optParts, optStr)
			}
		}
		parts = append(parts, "OPTIONS", "("+strings.Join(optParts, ", ")+")")
	}

	return strings.Join(parts, " ")
}

// AlterUserMappingStmt represents ALTER USER MAPPING statement
// Ported from postgres/src/include/nodes/parsenodes.h AlterUserMappingStmt
type AlterUserMappingStmt struct {
	BaseNode
	User       *RoleSpec `json:"user"`       // user name or role
	Servername string    `json:"servername"` // server name
	Options    *NodeList `json:"options"`    // OPTIONS clause
}

// NewAlterUserMappingStmt creates a new AlterUserMappingStmt node
func NewAlterUserMappingStmt(user *RoleSpec, servername string, options *NodeList) *AlterUserMappingStmt {
	return &AlterUserMappingStmt{
		BaseNode:   BaseNode{Tag: T_AlterUserMappingStmt},
		User:       user,
		Servername: servername,
		Options:    options,
	}
}

func (a *AlterUserMappingStmt) StatementType() string {
	return "AlterUserMappingStmt"
}

func (a *AlterUserMappingStmt) String() string {
	return fmt.Sprintf("AlterUserMappingStmt(%s@%s)@%d", a.User.SqlString(), a.Servername, a.Location())
}

// SqlString returns the SQL representation of AlterUserMappingStmt
func (a *AlterUserMappingStmt) SqlString() string {
	var parts []string
	parts = append(parts, "ALTER USER MAPPING FOR", a.User.SqlString(), "SERVER", a.Servername)

	// Add OPTIONS clause
	if a.Options != nil && a.Options.Len() > 0 {
		var optParts []string
		for _, item := range a.Options.Items {
			if opt, ok := item.(*DefElem); ok && opt != nil {
				// For ALTER options, include the action (ADD/SET/DROP) prefix
				var optStr string
				switch opt.Defaction {
				case DEFELEM_ADD:
					if opt.Arg != nil {
						optStr = "ADD " + QuoteIdentifier(opt.Defname) + " " + opt.Arg.SqlString()
					} else {
						optStr = "ADD " + QuoteIdentifier(opt.Defname)
					}
				case DEFELEM_SET:
					if opt.Arg != nil {
						optStr = "SET " + QuoteIdentifier(opt.Defname) + " " + opt.Arg.SqlString()
					} else {
						optStr = "SET " + QuoteIdentifier(opt.Defname)
					}
				case DEFELEM_DROP:
					optStr = "DROP " + QuoteIdentifier(opt.Defname)
				default:
					// DEFELEM_UNSPEC or other - use without action prefix
					if opt.Arg != nil {
						optStr = QuoteIdentifier(opt.Defname) + " " + opt.Arg.SqlString()
					} else {
						optStr = QuoteIdentifier(opt.Defname)
					}
				}
				optParts = append(optParts, optStr)
			}
		}
		parts = append(parts, "OPTIONS", "("+strings.Join(optParts, ", ")+")")
	}

	return strings.Join(parts, " ")
}

// DropUserMappingStmt represents DROP USER MAPPING statement
// Ported from postgres/src/include/nodes/parsenodes.h DropUserMappingStmt
type DropUserMappingStmt struct {
	BaseNode
	User       *RoleSpec `json:"user"`       // user name or role
	Servername string    `json:"servername"` // server name
	MissingOk  bool      `json:"missing_ok"` // IF EXISTS option
}

// NewDropUserMappingStmt creates a new DropUserMappingStmt node
func NewDropUserMappingStmt(user *RoleSpec, servername string, missingOk bool) *DropUserMappingStmt {
	return &DropUserMappingStmt{
		BaseNode:   BaseNode{Tag: T_DropUserMappingStmt},
		User:       user,
		Servername: servername,
		MissingOk:  missingOk,
	}
}

func (d *DropUserMappingStmt) StatementType() string {
	return "DropUserMappingStmt"
}

func (d *DropUserMappingStmt) String() string {
	return fmt.Sprintf("DropUserMappingStmt(%s@%s)@%d", d.User.SqlString(), d.Servername, d.Location())
}

// SqlString returns the SQL representation of DropUserMappingStmt
func (d *DropUserMappingStmt) SqlString() string {
	var parts []string
	parts = append(parts, "DROP USER MAPPING")

	if d.MissingOk {
		parts = append(parts, "IF EXISTS")
	}

	parts = append(parts, "FOR", d.User.SqlString(), "SERVER", d.Servername)

	return strings.Join(parts, " ")
}

// Event trigger constants
type TriggerFires int

const (
	TRIGGER_FIRES_ON_ORIGIN TriggerFires = iota
	TRIGGER_FIRES_ON_REPLICA
	TRIGGER_FIRES_ALWAYS
	TRIGGER_DISABLED
)

// ==============================================================================
// EVENT TRIGGER STATEMENTS
// ==============================================================================

// CreateEventTrigStmt represents CREATE EVENT TRIGGER statement
// Ported from postgres/src/include/nodes/parsenodes.h CreateEventTrigStmt
type CreateEventTrigStmt struct {
	BaseNode
	TrigName   string    `json:"trigname"`   // event trigger name
	EventName  string    `json:"eventname"`  // event name (e.g., ddl_command_start)
	FuncName   *NodeList `json:"funcname"`   // function name
	WhenClause *NodeList `json:"whenclause"` // WHEN clause conditions
}

// NewCreateEventTrigStmt creates a new CreateEventTrigStmt node
func NewCreateEventTrigStmt(trigname string, eventname string, funcname *NodeList, whenclause *NodeList) *CreateEventTrigStmt {
	return &CreateEventTrigStmt{
		BaseNode:   BaseNode{Tag: T_CreateEventTrigStmt},
		TrigName:   trigname,
		EventName:  eventname,
		FuncName:   funcname,
		WhenClause: whenclause,
	}
}

func (c *CreateEventTrigStmt) StatementType() string {
	return "CreateEventTrigStmt"
}

func (c *CreateEventTrigStmt) String() string {
	return fmt.Sprintf("CreateEventTrigStmt(%s ON %s)@%d", c.TrigName, c.EventName, c.Location())
}

// SqlString returns the SQL representation of CreateEventTrigStmt
func (c *CreateEventTrigStmt) SqlString() string {
	var parts []string
	parts = append(parts, "CREATE EVENT TRIGGER", c.TrigName, "ON", c.EventName)

	// Add WHEN clause if present
	if c.WhenClause != nil && c.WhenClause.Len() > 0 {
		var whenParts []string
		for _, item := range c.WhenClause.Items {
			if defElem, ok := item.(*DefElem); ok {
				// Format as "defname IN (value1, value2, ...)"
				if defElem.Arg != nil {
					if valueList, ok := defElem.Arg.(*NodeList); ok {
						var values []string
						for _, val := range valueList.Items {
							if strVal, ok := val.(*String); ok {
								values = append(values, "'"+strVal.SVal+"'")
							}
						}
						whenParts = append(whenParts, defElem.Defname+" IN ("+strings.Join(values, ", ")+")")
					}
				}
			}
		}
		if len(whenParts) > 0 {
			parts = append(parts, "WHEN", strings.Join(whenParts, " AND "))
		}
	}

	parts = append(parts, "EXECUTE FUNCTION")
	if c.FuncName != nil {
		var funcParts []string
		for _, item := range c.FuncName.Items {
			if strNode, ok := item.(*String); ok {
				funcParts = append(funcParts, strNode.SVal)
			}
		}
		parts = append(parts, strings.Join(funcParts, ".")+"()")
	}

	return strings.Join(parts, " ")
}

// AlterEventTrigStmt represents ALTER EVENT TRIGGER statement
// Ported from postgres/src/include/nodes/parsenodes.h AlterEventTrigStmt
type AlterEventTrigStmt struct {
	BaseNode
	TrigName  string       `json:"trigname"`  // event trigger name
	TgEnabled TriggerFires `json:"tgenabled"` // enable/disable state
}

// NewAlterEventTrigStmt creates a new AlterEventTrigStmt node
func NewAlterEventTrigStmt(trigname string, tgenabled TriggerFires) *AlterEventTrigStmt {
	return &AlterEventTrigStmt{
		BaseNode:  BaseNode{Tag: T_AlterEventTrigStmt},
		TrigName:  trigname,
		TgEnabled: tgenabled,
	}
}

func (a *AlterEventTrigStmt) StatementType() string {
	return "AlterEventTrigStmt"
}

func (a *AlterEventTrigStmt) String() string {
	return fmt.Sprintf("AlterEventTrigStmt(%s)@%d", a.TrigName, a.Location())
}

// SqlString returns the SQL representation of AlterEventTrigStmt
func (a *AlterEventTrigStmt) SqlString() string {
	var parts []string
	parts = append(parts, "ALTER EVENT TRIGGER", a.TrigName)

	switch a.TgEnabled {
	case TRIGGER_FIRES_ON_ORIGIN:
		parts = append(parts, "ENABLE")
	case TRIGGER_FIRES_ON_REPLICA:
		parts = append(parts, "ENABLE REPLICA")
	case TRIGGER_FIRES_ALWAYS:
		parts = append(parts, "ENABLE ALWAYS")
	case TRIGGER_DISABLED:
		parts = append(parts, "DISABLE")
	}

	return strings.Join(parts, " ")
}

// ==============================================================================
// DATABASE STATEMENTS
// ==============================================================================

// CreatedbStmt represents a CREATE DATABASE statement.
// Ported from postgres/src/include/nodes/parsenodes.h:3787
type CreatedbStmt struct {
	BaseNode
	Dbname  string    // name of database to create - postgres/src/include/nodes/parsenodes.h:3789
	Options *NodeList // list of DefElem nodes - postgres/src/include/nodes/parsenodes.h:3790
}

// NewCreatedbStmt creates a new CreatedbStmt node.
func NewCreatedbStmt(dbname string, options *NodeList) *CreatedbStmt {
	return &CreatedbStmt{
		BaseNode: BaseNode{Tag: T_CreatedbStmt},
		Dbname:   dbname,
		Options:  options,
	}
}

func (c *CreatedbStmt) StatementType() string {
	return "CREATE DATABASE"
}

func (c *CreatedbStmt) String() string {
	return fmt.Sprintf("CreatedbStmt(%s)@%d", c.Dbname, c.Location())
}

// SqlString returns the SQL representation of CreatedbStmt
func (c *CreatedbStmt) SqlString() string {
	var parts []string
	parts = append(parts, "CREATE DATABASE", c.Dbname)

	// Add options if present
	if c.Options != nil && c.Options.Len() > 0 {
		var opts []string
		for _, item := range c.Options.Items {
			if opt, ok := item.(*DefElem); ok && opt != nil {
				opts = append(opts, c.formatDatabaseOption(opt))
			}
		}
		if len(opts) > 0 {
			parts = append(parts, "WITH", strings.Join(opts, " "))
		}
	}

	return strings.Join(parts, " ")
}

// formatDatabaseOption formats database options with proper PostgreSQL keywords
func (c *CreatedbStmt) formatDatabaseOption(opt *DefElem) string {
	// Map option names to their proper PostgreSQL keywords
	optName := opt.Defname
	switch optName {
	case "template":
		optName = "TEMPLATE"
	case "encoding":
		optName = "ENCODING"
	case "connection_limit":
		optName = "CONNECTION LIMIT"
	case "owner":
		optName = "OWNER"
	case "tablespace":
		optName = "TABLESPACE"
	case "location":
		optName = "LOCATION"
	default:
		optName = strings.ToUpper(optName)
	}

	if opt.Arg != nil {
		argStr := opt.Arg.SqlString()
		// For identifiers that should not be quoted
		if strNode, ok := opt.Arg.(*String); ok {
			switch opt.Defname {
			case "template", "owner", "tablespace":
				// These should be unquoted identifiers
				argStr = strNode.SVal
			case "encoding", "location":
				// These should remain as quoted strings
				argStr = strNode.SqlString()
			}
		}
		return fmt.Sprintf("%s = %s", optName, argStr)
	}
	return optName
}

// DropdbStmt represents a DROP DATABASE statement.
// Ported from postgres/src/include/nodes/parsenodes.h:3800
type DropdbStmt struct {
	BaseNode
	Dbname    string    // database to drop - postgres/src/include/nodes/parsenodes.h:3803
	MissingOk bool      // skip error if db is missing? - postgres/src/include/nodes/parsenodes.h:3804
	Options   *NodeList // currently only FORCE is supported - postgres/src/include/nodes/parsenodes.h:3805
}

// NewDropdbStmt creates a new DropdbStmt node.
func NewDropdbStmt(dbname string, missingOk bool, options *NodeList) *DropdbStmt {
	return &DropdbStmt{
		BaseNode:  BaseNode{Tag: T_DropdbStmt},
		Dbname:    dbname,
		MissingOk: missingOk,
		Options:   options,
	}
}

func (d *DropdbStmt) StatementType() string {
	return "DROP DATABASE"
}

func (d *DropdbStmt) String() string {
	ifExists := ""
	if d.MissingOk {
		ifExists = " IF EXISTS"
	}
	return fmt.Sprintf("DropdbStmt(%s%s)@%d", d.Dbname, ifExists, d.Location())
}

// SqlString returns the SQL representation of DropdbStmt
func (d *DropdbStmt) SqlString() string {
	var parts []string
	parts = append(parts, "DROP DATABASE")

	if d.MissingOk {
		parts = append(parts, "IF EXISTS")
	}

	parts = append(parts, d.Dbname)

	// Add options if present (e.g., FORCE)
	if d.Options != nil && d.Options.Len() > 0 {
		var opts []string
		for _, item := range d.Options.Items {
			if opt, ok := item.(*DefElem); ok && opt != nil {
				if opt.Defname == "force" {
					opts = append(opts, "FORCE")
				}
			}
		}
		if len(opts) > 0 {
			parts = append(parts, "WITH ("+strings.Join(opts, ", ")+")")
		}
	}

	return strings.Join(parts, " ")
}

// DropTableSpaceStmt represents a DROP TABLESPACE statement.
// Ported from postgres/src/include/nodes/parsenodes.h:2790
type DropTableSpaceStmt struct {
	BaseNode
	Tablespacename string // tablespace to drop - postgres/src/include/nodes/parsenodes.h:2792
	MissingOk      bool   // skip error if missing? - postgres/src/include/nodes/parsenodes.h:2793
}

// NewDropTableSpaceStmt creates a new DropTableSpaceStmt node.
func NewDropTableSpaceStmt(tablespacename string, missingOk bool) *DropTableSpaceStmt {
	return &DropTableSpaceStmt{
		BaseNode:       BaseNode{Tag: T_DropTableSpaceStmt},
		Tablespacename: tablespacename,
		MissingOk:      missingOk,
	}
}

func (d *DropTableSpaceStmt) StatementType() string {
	return "DROP TABLESPACE"
}

func (d *DropTableSpaceStmt) String() string {
	ifExists := ""
	if d.MissingOk {
		ifExists = " IF EXISTS"
	}
	return fmt.Sprintf("DropTableSpaceStmt(%s%s)@%d", d.Tablespacename, ifExists, d.Location())
}

// SqlString returns the SQL representation of DropTableSpaceStmt
func (d *DropTableSpaceStmt) SqlString() string {
	var parts []string
	parts = append(parts, "DROP TABLESPACE")

	if d.MissingOk {
		parts = append(parts, "IF EXISTS")
	}

	parts = append(parts, d.Tablespacename)

	return strings.Join(parts, " ")
}

// ==============================================================================
// OWNERSHIP STATEMENTS
// ==============================================================================

// DropOwnedStmt represents a DROP OWNED statement.
// Ported from postgres/src/include/nodes/parsenodes.h:4075
type DropOwnedStmt struct {
	BaseNode
	Roles    *NodeList    // list of RoleSpec - postgres/src/include/nodes/parsenodes.h:4078
	Behavior DropBehavior // CASCADE or RESTRICT - postgres/src/include/nodes/parsenodes.h:4079
}

// NewDropOwnedStmt creates a new DropOwnedStmt node.
func NewDropOwnedStmt(roles *NodeList, behavior DropBehavior) *DropOwnedStmt {
	return &DropOwnedStmt{
		BaseNode: BaseNode{Tag: T_DropOwnedStmt},
		Roles:    roles,
		Behavior: behavior,
	}
}

func (d *DropOwnedStmt) StatementType() string {
	return "DROP OWNED"
}

func (d *DropOwnedStmt) String() string {
	return fmt.Sprintf("DropOwnedStmt(%d roles)@%d", d.Roles.Len(), d.Location())
}

// SqlString returns the SQL representation of DropOwnedStmt
func (d *DropOwnedStmt) SqlString() string {
	var parts []string
	parts = append(parts, "DROP OWNED BY")

	// Add role list
	if d.Roles != nil {
		var roleNames []string
		for _, item := range d.Roles.Items {
			if role, ok := item.(*RoleSpec); ok {
				roleNames = append(roleNames, role.SqlString())
			}
		}
		parts = append(parts, strings.Join(roleNames, ", "))
	}

	// Add behavior
	parts = append(parts, d.Behavior.String())

	return strings.Join(parts, " ")
}

// ReassignOwnedStmt represents a REASSIGN OWNED statement.
// Ported from postgres/src/include/nodes/parsenodes.h:4084
type ReassignOwnedStmt struct {
	BaseNode
	Roles   *NodeList // list of RoleSpec - postgres/src/include/nodes/parsenodes.h:4087
	Newrole *RoleSpec // new owner - postgres/src/include/nodes/parsenodes.h:4088
}

// NewReassignOwnedStmt creates a new ReassignOwnedStmt node.
func NewReassignOwnedStmt(roles *NodeList, newrole *RoleSpec) *ReassignOwnedStmt {
	return &ReassignOwnedStmt{
		BaseNode: BaseNode{Tag: T_ReassignOwnedStmt},
		Roles:    roles,
		Newrole:  newrole,
	}
}

func (r *ReassignOwnedStmt) StatementType() string {
	return "REASSIGN OWNED"
}

func (r *ReassignOwnedStmt) String() string {
	return fmt.Sprintf("ReassignOwnedStmt(%d roles)@%d", r.Roles.Len(), r.Location())
}

// SqlString returns the SQL representation of ReassignOwnedStmt
func (r *ReassignOwnedStmt) SqlString() string {
	var parts []string
	parts = append(parts, "REASSIGN OWNED BY")

	// Add role list
	if r.Roles != nil {
		var roleNames []string
		for _, item := range r.Roles.Items {
			if role, ok := item.(*RoleSpec); ok {
				roleNames = append(roleNames, role.SqlString())
			}
		}
		parts = append(parts, strings.Join(roleNames, ", "))
	}

	parts = append(parts, "TO")

	if r.Newrole != nil {
		parts = append(parts, r.Newrole.SqlString())
	}

	return strings.Join(parts, " ")
}

// ==============================================================================
// FOREIGN SCHEMA STATEMENTS
// ==============================================================================

// ImportForeignSchemaType represents import qualification types for IMPORT FOREIGN SCHEMA
// Ported from postgres/src/include/nodes/parsenodes.h:4094-4100
type ImportForeignSchemaType int

const (
	FDW_IMPORT_SCHEMA_ALL      ImportForeignSchemaType = iota // all relations wanted
	FDW_IMPORT_SCHEMA_LIMIT_TO                                // include only listed tables in import
	FDW_IMPORT_SCHEMA_EXCEPT                                  // exclude listed tables from import
)

// String returns string representation of ImportForeignSchemaType
func (t ImportForeignSchemaType) String() string {
	switch t {
	case FDW_IMPORT_SCHEMA_ALL:
		return ""
	case FDW_IMPORT_SCHEMA_LIMIT_TO:
		return "LIMIT TO"
	case FDW_IMPORT_SCHEMA_EXCEPT:
		return "EXCEPT"
	default:
		return "UNKNOWN"
	}
}

// ImportForeignSchemaStmt represents an IMPORT FOREIGN SCHEMA statement
// Ported from postgres/src/include/nodes/parsenodes.h:4105-4115
type ImportForeignSchemaStmt struct {
	BaseNode
	ServerName   *String                 // FDW server name
	RemoteSchema *String                 // remote schema name to query
	LocalSchema  *String                 // local schema to create objects in
	ListType     ImportForeignSchemaType // type of table list
	TableList    *NodeList               // List of RangeVar
	Options      *NodeList               // list of options to pass to FDW
}

// NewImportForeignSchemaStmt creates a new ImportForeignSchemaStmt node
func NewImportForeignSchemaStmt(serverName, remoteSchema, localSchema *String,
	listType ImportForeignSchemaType, tableList, options *NodeList,
) *ImportForeignSchemaStmt {
	return &ImportForeignSchemaStmt{
		BaseNode:     BaseNode{Tag: T_ImportForeignSchemaStmt},
		ServerName:   serverName,
		RemoteSchema: remoteSchema,
		LocalSchema:  localSchema,
		ListType:     listType,
		TableList:    tableList,
		Options:      options,
	}
}

func (i *ImportForeignSchemaStmt) StatementType() string {
	return "IMPORT FOREIGN SCHEMA"
}

func (i *ImportForeignSchemaStmt) String() string {
	return fmt.Sprintf("ImportForeignSchemaStmt(%s from %s into %s)@%d",
		i.RemoteSchema.SVal, i.ServerName.SVal, i.LocalSchema.SVal, i.Location())
}

// SqlString returns the SQL representation of ImportForeignSchemaStmt
func (i *ImportForeignSchemaStmt) SqlString() string {
	var parts []string
	parts = append(parts, "IMPORT FOREIGN SCHEMA")

	if i.RemoteSchema != nil {
		parts = append(parts, QuoteIdentifier(i.RemoteSchema.SVal))
	}

	// Add import qualification
	if i.ListType != FDW_IMPORT_SCHEMA_ALL {
		parts = append(parts, i.ListType.String())
		if i.TableList != nil && i.TableList.Len() > 0 {
			var tableNames []string
			for _, item := range i.TableList.Items {
				if rv, ok := item.(*RangeVar); ok {
					tableNames = append(tableNames, rv.SqlString())
				}
			}
			parts = append(parts, "("+strings.Join(tableNames, ", ")+")")
		}
	}

	parts = append(parts, "FROM SERVER")
	if i.ServerName != nil {
		parts = append(parts, QuoteIdentifier(i.ServerName.SVal))
	}

	parts = append(parts, "INTO")
	if i.LocalSchema != nil {
		parts = append(parts, QuoteIdentifier(i.LocalSchema.SVal))
	}

	// Add options
	if i.Options != nil && i.Options.Len() > 0 {
		parts = append(parts, "OPTIONS")
		var optionStrings []string
		for _, item := range i.Options.Items {
			if opt, ok := item.(*DefElem); ok {
				// For foreign schema import, use "name 'value'" format instead of "name = 'value'"
				if opt.Arg != nil {
					optionStrings = append(optionStrings, QuoteIdentifier(opt.Defname)+" "+opt.Arg.SqlString())
				} else {
					optionStrings = append(optionStrings, QuoteIdentifier(opt.Defname))
				}
			}
		}
		parts = append(parts, "("+strings.Join(optionStrings, ", ")+")")
	}

	return strings.Join(parts, " ")
}
