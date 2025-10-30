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
	"strings"
)

// ==============================================================================
// ALTER Miscellaneous Statement Nodes - Phase 3J Implementation
// ==============================================================================

// Helper function to convert NodeList of strings to qualified name
func nodeListToQualifiedName(nodeList *NodeList) string {
	if nodeList == nil || nodeList.Len() == 0 {
		return ""
	}

	var parts []string
	for _, item := range nodeList.Items {
		if str, ok := item.(*String); ok {
			parts = append(parts, str.SVal)
		}
	}

	return FormatQualifiedName(parts...)
}

// AlterTSConfigType represents the type of ALTER TEXT SEARCH CONFIGURATION operation
// Ported from postgres/src/include/nodes/parsenodes.h:3700-3706
type AlterTSConfigType int

const (
	ALTER_TSCONFIG_ADD_MAPPING AlterTSConfigType = iota
	ALTER_TSCONFIG_ALTER_MAPPING_FOR_TOKEN
	ALTER_TSCONFIG_ALTER_MAPPING_REPLACE
	ALTER_TSCONFIG_REPLACE_DICT
	ALTER_TSCONFIG_REPLACE_DICT_FOR_TOKEN
	ALTER_TSCONFIG_DROP_MAPPING
)

func (a AlterTSConfigType) String() string {
	switch a {
	case ALTER_TSCONFIG_ADD_MAPPING:
		return "ADD MAPPING"
	case ALTER_TSCONFIG_ALTER_MAPPING_FOR_TOKEN:
		return "ALTER MAPPING FOR"
	case ALTER_TSCONFIG_ALTER_MAPPING_REPLACE:
		return "ALTER MAPPING REPLACE"
	case ALTER_TSCONFIG_REPLACE_DICT:
		return "REPLACE"
	case ALTER_TSCONFIG_REPLACE_DICT_FOR_TOKEN:
		return "REPLACE FOR"
	case ALTER_TSCONFIG_DROP_MAPPING:
		return "DROP MAPPING"
	default:
		return "UNKNOWN"
	}
}

// AlterObjectSchemaStmt represents ALTER ... SET SCHEMA statements
// Ported from postgres/src/include/nodes/parsenodes.h:3547-3554
type AlterObjectSchemaStmt struct {
	BaseNode
	ObjectType ObjectType `json:"objectType"` // OBJECT_TABLE, OBJECT_TYPE, etc
	Relation   *RangeVar  `json:"relation"`   // In case it's a table
	Object     Node       `json:"object"`     // In case it's some other object
	NewSchema  string     `json:"newSchema"`  // The new schema
	MissingOk  bool       `json:"missingOk"`  // Skip error if missing?
}

func (n *AlterObjectSchemaStmt) node() {}
func (n *AlterObjectSchemaStmt) stmt() {}

func (n *AlterObjectSchemaStmt) StatementType() string {
	return "ALTER"
}

func (n *AlterObjectSchemaStmt) String() string {
	return n.SqlString()
}

func (n *AlterObjectSchemaStmt) SqlString() string {
	var parts []string
	parts = append(parts, "ALTER")

	// Add object type
	parts = append(parts, n.ObjectType.String())

	// Add IF EXISTS if specified
	if n.MissingOk {
		parts = append(parts, "IF EXISTS")
	}

	// Add object name
	if n.Relation != nil {
		parts = append(parts, n.Relation.SqlString())
	} else if n.Object != nil {
		if nodeList, ok := n.Object.(*NodeList); ok {
			// Special handling for OPERATOR CLASS and OPERATOR FAMILY
			if n.ObjectType == OBJECT_OPCLASS || n.ObjectType == OBJECT_OPFAMILY {
				// Format as "name USING method" where first item is method, rest is name
				if nodeList.Len() >= 2 {
					methodStr := ""
					if str, ok := nodeList.Items[0].(*String); ok {
						methodStr = str.SVal
					} else {
						methodStr = nodeList.Items[0].SqlString()
					}

					var nameStr string
					for i := 1; i < nodeList.Len(); i++ {
						if i > 1 {
							nameStr += "."
						}
						if str, ok := nodeList.Items[i].(*String); ok {
							nameStr += str.SVal
						} else {
							nameStr += nodeList.Items[i].SqlString()
						}
					}
					parts = append(parts, nameStr, "USING", methodStr)
				} else {
					parts = append(parts, nodeListToQualifiedName(nodeList))
				}
			} else {
				parts = append(parts, nodeListToQualifiedName(nodeList))
			}
		} else if strv, ok := n.Object.(*String); ok {
			parts = append(parts, QuoteIdentifier(strv.SVal))
		} else {
			parts = append(parts, n.Object.SqlString())
		}
	}

	// Add SET SCHEMA clause
	parts = append(parts, "SET SCHEMA", QuoteIdentifier(n.NewSchema))

	return strings.Join(parts, " ")
}

// NewAlterObjectSchemaStmt creates a new AlterObjectSchemaStmt node
func NewAlterObjectSchemaStmt(objectType ObjectType, newSchema string) *AlterObjectSchemaStmt {
	return &AlterObjectSchemaStmt{
		BaseNode:   BaseNode{Tag: T_AlterObjectSchemaStmt},
		ObjectType: objectType,
		NewSchema:  newSchema,
	}
}

// AlterOperatorStmt represents ALTER OPERATOR statements
// Ported from postgres/src/include/nodes/parsenodes.h:3585-3589
type AlterOperatorStmt struct {
	BaseNode
	Opername *ObjectWithArgs `json:"opername"` // Operator name and argument types
	Options  *NodeList       `json:"options"`  // List of DefElem nodes
}

func (n *AlterOperatorStmt) node() {}
func (n *AlterOperatorStmt) stmt() {}

func (n *AlterOperatorStmt) StatementType() string {
	return "ALTER"
}

func (n *AlterOperatorStmt) String() string {
	return n.SqlString()
}

func (n *AlterOperatorStmt) SqlString() string {
	var parts []string
	parts = append(parts, "ALTER OPERATOR")

	if n.Opername != nil {
		parts = append(parts, n.Opername.SqlString())
	}

	if n.Options != nil && n.Options.Len() > 0 {
		parts = append(parts, "SET (")
		var optStrs []string
		for _, opt := range n.Options.Items {
			if defElem, ok := opt.(*DefElem); ok {
				optStrs = append(optStrs, defElem.SqlString())
			}
		}
		parts = append(parts, strings.Join(optStrs, ", "))
		parts = append(parts, ")")
	}

	return strings.Join(parts, " ")
}

// NewAlterOperatorStmt creates a new AlterOperatorStmt node
func NewAlterOperatorStmt(opername *ObjectWithArgs, options *NodeList) *AlterOperatorStmt {
	return &AlterOperatorStmt{
		BaseNode: BaseNode{Tag: T_AlterOperatorStmt},
		Opername: opername,
		Options:  options,
	}
}

// AlterObjectDependsStmt represents ALTER ... DEPENDS statements
// Ported from postgres/src/include/nodes/parsenodes.h:3556-3564
type AlterObjectDependsStmt struct {
	BaseNode
	ObjectType ObjectType `json:"objectType"` // OBJECT_FUNCTION, OBJECT_TRIGGER, etc
	Relation   *RangeVar  `json:"relation"`   // In case a table is involved
	Object     Node       `json:"object"`     // Name of the object
	Extname    *String    `json:"extname"`    // Extension name
	Remove     bool       `json:"remove"`     // Set true to remove dep rather than add
}

func (n *AlterObjectDependsStmt) node() {}
func (n *AlterObjectDependsStmt) stmt() {}

func (n *AlterObjectDependsStmt) StatementType() string {
	return "ALTER"
}

func (n *AlterObjectDependsStmt) String() string {
	return n.SqlString()
}

func (n *AlterObjectDependsStmt) SqlString() string {
	var parts []string
	parts = append(parts, "ALTER")

	// Add object type
	parts = append(parts, n.ObjectType.String())

	// Add object name
	if n.ObjectType == OBJECT_TRIGGER && n.Object != nil && n.Relation != nil {
		// For TRIGGER, format as "trigger_name ON table_name"
		if str, ok := n.Object.(*String); ok {
			parts = append(parts, QuoteIdentifier(str.SVal), "ON", n.Relation.SqlString())
		} else {
			parts = append(parts, n.Object.SqlString(), "ON", n.Relation.SqlString())
		}
	} else if n.Relation != nil {
		parts = append(parts, n.Relation.SqlString())
	} else if n.Object != nil {
		parts = append(parts, n.Object.SqlString())
	}

	// Add DEPENDS clause
	if n.Remove {
		parts = append(parts, "NO DEPENDS ON EXTENSION")
	} else {
		parts = append(parts, "DEPENDS ON EXTENSION")
	}

	if n.Extname != nil {
		parts = append(parts, QuoteIdentifier(n.Extname.SVal))
	}

	return strings.Join(parts, " ")
}

// NewAlterObjectDependsStmt creates a new AlterObjectDependsStmt node
func NewAlterObjectDependsStmt(objectType ObjectType, extname *String, remove bool) *AlterObjectDependsStmt {
	return &AlterObjectDependsStmt{
		BaseNode:   BaseNode{Tag: T_AlterObjectDependsStmt},
		ObjectType: objectType,
		Extname:    extname,
		Remove:     remove,
	}
}

// AlterCollationStmt represents ALTER COLLATION statements
// Ported from postgres/src/include/nodes/parsenodes.h:2510-2514
type AlterCollationStmt struct {
	BaseNode
	Collname *NodeList `json:"collname"` // Qualified name
}

func (n *AlterCollationStmt) node() {}
func (n *AlterCollationStmt) stmt() {}

func (n *AlterCollationStmt) StatementType() string {
	return "ALTER"
}

func (n *AlterCollationStmt) String() string {
	return n.SqlString()
}

func (n *AlterCollationStmt) SqlString() string {
	var parts []string
	parts = append(parts, "ALTER COLLATION")

	if n.Collname != nil {
		parts = append(parts, nodeListToQualifiedName(n.Collname))
	}

	parts = append(parts, "REFRESH VERSION")

	return strings.Join(parts, " ")
}

// NewAlterCollationStmt creates a new AlterCollationStmt node
func NewAlterCollationStmt(collname *NodeList) *AlterCollationStmt {
	return &AlterCollationStmt{
		BaseNode: BaseNode{Tag: T_AlterCollationStmt},
		Collname: collname,
	}
}

// AlterDatabaseStmt represents ALTER DATABASE statements
// Ported from postgres/src/include/nodes/parsenodes.h:3221-3226
type AlterDatabaseStmt struct {
	BaseNode
	Dbname  string    `json:"dbname"`  // Name of database to alter
	Options *NodeList `json:"options"` // List of DefElem nodes
}

func (n *AlterDatabaseStmt) node() {}
func (n *AlterDatabaseStmt) stmt() {}

func (n *AlterDatabaseStmt) StatementType() string {
	return "ALTER"
}

func (n *AlterDatabaseStmt) String() string {
	return n.SqlString()
}

func (n *AlterDatabaseStmt) SqlString() string {
	var parts []string
	parts = append(parts, "ALTER DATABASE", QuoteIdentifier(n.Dbname))

	if n.Options != nil && n.Options.Len() > 0 {
		var optStrs []string
		for _, opt := range n.Options.Items {
			if defElem, ok := opt.(*DefElem); ok {
				optStrs = append(optStrs, defElem.SqlString())
			}
		}
		parts = append(parts, strings.Join(optStrs, " "))
	}

	return strings.Join(parts, " ")
}

// NewAlterDatabaseStmt creates a new AlterDatabaseStmt node
func NewAlterDatabaseStmt(dbname string, options *NodeList) *AlterDatabaseStmt {
	return &AlterDatabaseStmt{
		BaseNode: BaseNode{Tag: T_AlterDatabaseStmt},
		Dbname:   dbname,
		Options:  options,
	}
}

// AlterDatabaseSetStmt represents ALTER DATABASE SET statements
// Ported from postgres/src/include/nodes/parsenodes.h:3238-3243
type AlterDatabaseSetStmt struct {
	BaseNode
	Dbname  string           `json:"dbname"`  // Database name
	Setstmt *VariableSetStmt `json:"setstmt"` // SET or RESET subcommand
}

func (n *AlterDatabaseSetStmt) node() {}
func (n *AlterDatabaseSetStmt) stmt() {}

func (n *AlterDatabaseSetStmt) StatementType() string {
	return "ALTER"
}

func (n *AlterDatabaseSetStmt) String() string {
	return n.SqlString()
}

func (n *AlterDatabaseSetStmt) SqlString() string {
	var parts []string
	parts = append(parts, "ALTER DATABASE", QuoteIdentifier(n.Dbname))

	if n.Setstmt != nil {
		parts = append(parts, n.Setstmt.SqlString())
	}

	return strings.Join(parts, " ")
}

// NewAlterDatabaseSetStmt creates a new AlterDatabaseSetStmt node
func NewAlterDatabaseSetStmt(dbname string, setstmt *VariableSetStmt) *AlterDatabaseSetStmt {
	return &AlterDatabaseSetStmt{
		BaseNode: BaseNode{Tag: T_AlterDatabaseSetStmt},
		Dbname:   dbname,
		Setstmt:  setstmt,
	}
}

// AlterDatabaseRefreshCollStmt represents ALTER DATABASE REFRESH COLLATION VERSION statements
// Ported from postgres/src/include/nodes/parsenodes.h
type AlterDatabaseRefreshCollStmt struct {
	BaseNode
	Dbname string `json:"dbname"` // Database name
}

func (n *AlterDatabaseRefreshCollStmt) node() {}
func (n *AlterDatabaseRefreshCollStmt) stmt() {}

func (n *AlterDatabaseRefreshCollStmt) StatementType() string {
	return "ALTER"
}

func (n *AlterDatabaseRefreshCollStmt) String() string {
	return n.SqlString()
}

func (n *AlterDatabaseRefreshCollStmt) SqlString() string {
	return "ALTER DATABASE " + QuoteIdentifier(n.Dbname) + " REFRESH COLLATION VERSION"
}

// NewAlterDatabaseRefreshCollStmt creates a new AlterDatabaseRefreshCollStmt node
func NewAlterDatabaseRefreshCollStmt(dbname string) *AlterDatabaseRefreshCollStmt {
	return &AlterDatabaseRefreshCollStmt{
		BaseNode: BaseNode{Tag: T_AlterDatabaseRefreshCollStmt},
		Dbname:   dbname,
	}
}

// AlterCompositeTypeStmt represents ALTER TYPE (composite) statements
// Note: This is actually handled by AlterTableStmt in PostgreSQL but
// represented here for grammar rule completeness
type AlterCompositeTypeStmt struct {
	BaseNode
	TypeName *NodeList `json:"typeName"` // Qualified type name (list of String)
	Cmds     *NodeList `json:"cmds"`     // List of alter type commands
}

func (n *AlterCompositeTypeStmt) node() {}
func (n *AlterCompositeTypeStmt) stmt() {}

func (n *AlterCompositeTypeStmt) StatementType() string {
	return "ALTER"
}

func (n *AlterCompositeTypeStmt) String() string {
	return n.SqlString()
}

func (n *AlterCompositeTypeStmt) SqlString() string {
	var parts []string
	parts = append(parts, "ALTER TYPE")

	if n.TypeName != nil {
		parts = append(parts, nodeListToQualifiedName(n.TypeName))
	}

	if n.Cmds != nil && n.Cmds.Len() > 0 {
		var cmdStrs []string
		for _, cmd := range n.Cmds.Items {
			if alterCmd, ok := cmd.(*AlterTableCmd); ok {
				cmdStrs = append(cmdStrs, alterCmd.SqlString())
			}
		}
		parts = append(parts, strings.Join(cmdStrs, ", "))
	}

	return strings.Join(parts, " ")
}

// NewAlterCompositeTypeStmt creates a new AlterCompositeTypeStmt node
func NewAlterCompositeTypeStmt(typeName *NodeList, cmds *NodeList) *AlterCompositeTypeStmt {
	return &AlterCompositeTypeStmt{
		BaseNode: BaseNode{Tag: T_AlterCompositeTypeStmt},
		TypeName: typeName,
		Cmds:     cmds,
	}
}

// AlterTSConfigurationStmt represents ALTER TEXT SEARCH CONFIGURATION statements
// Ported from postgres/src/include/nodes/parsenodes.h:3708-3721
type AlterTSConfigurationStmt struct {
	BaseNode
	Kind      AlterTSConfigType `json:"kind"`      // ALTER_TSCONFIG_ADD_MAPPING, etc
	Cfgname   *NodeList         `json:"cfgname"`   // Qualified name (list of String)
	Tokentype *NodeList         `json:"tokentype"` // List of String
	Dicts     *NodeList         `json:"dicts"`     // List of String
	Override  bool              `json:"override"`  // If true, replace rather than add
	Replace   bool              `json:"replace"`   // If true, replace rather than modify
	MissingOk bool              `json:"missingOk"` // For ALTER ... IF EXISTS
}

func (n *AlterTSConfigurationStmt) node() {}
func (n *AlterTSConfigurationStmt) stmt() {}

func (n *AlterTSConfigurationStmt) StatementType() string {
	return "ALTER"
}

func (n *AlterTSConfigurationStmt) String() string {
	return n.SqlString()
}

func (n *AlterTSConfigurationStmt) SqlString() string {
	var parts []string
	parts = append(parts, "ALTER TEXT SEARCH CONFIGURATION")

	if n.Cfgname != nil {
		parts = append(parts, nodeListToQualifiedName(n.Cfgname))
	}

	// Add operation type with special handling for ALTER MAPPING patterns
	if (n.Kind == ALTER_TSCONFIG_REPLACE_DICT || n.Kind == ALTER_TSCONFIG_REPLACE_DICT_FOR_TOKEN) && n.Dicts != nil && n.Dicts.Len() == 2 {
		// This is likely an ALTER MAPPING REPLACE that was parsed as REPLACE_DICT
		if n.Tokentype != nil && n.Tokentype.Len() > 0 {
			// Format: ALTER MAPPING FOR tokens REPLACE
			parts = append(parts, "ALTER MAPPING")

			// Add FOR tokens
			parts = append(parts, "FOR")
			var tokens []string
			for _, token := range n.Tokentype.Items {
				if str, ok := token.(*String); ok {
					tokens = append(tokens, str.SVal)
				} else if nodeList, ok := token.(*NodeList); ok {
					tokens = append(tokens, nodeListToQualifiedName(nodeList))
				}
			}
			parts = append(parts, strings.Join(tokens, ", "))

			// Add REPLACE
			parts = append(parts, "REPLACE")
		} else {
			// Format: ALTER MAPPING REPLACE
			parts = append(parts, "ALTER MAPPING REPLACE")
		}
	} else {
		// Standard operation types
		parts = append(parts, n.Kind.String())

		// Add IF EXISTS if specified (for DROP operations)
		if n.MissingOk && n.Kind == ALTER_TSCONFIG_DROP_MAPPING {
			parts = append(parts, "IF EXISTS")
		}

		// Add FOR token types if specified (for non-REPLACE operations)
		if n.Tokentype != nil && n.Tokentype.Len() > 0 {
			// Only add "FOR" if it's not already in the kind string
			kindStr := n.Kind.String()
			if !strings.Contains(kindStr, "FOR") {
				parts = append(parts, "FOR")
			}
			var tokens []string
			for _, token := range n.Tokentype.Items {
				if str, ok := token.(*String); ok {
					tokens = append(tokens, str.SVal)
				} else if nodeList, ok := token.(*NodeList); ok {
					tokens = append(tokens, nodeListToQualifiedName(nodeList))
				}
			}
			parts = append(parts, strings.Join(tokens, ", "))
		}
	}

	// Add WITH dictionaries if specified
	if n.Dicts != nil && n.Dicts.Len() > 0 {
		if (n.Kind == ALTER_TSCONFIG_ALTER_MAPPING_REPLACE ||
			((n.Kind == ALTER_TSCONFIG_REPLACE_DICT || n.Kind == ALTER_TSCONFIG_REPLACE_DICT_FOR_TOKEN) && n.Dicts.Len() == 2)) && n.Dicts.Len() == 2 {
			// Special format for ALTER MAPPING REPLACE: "old_dict WITH new_dict"
			var dicts []string
			for _, dict := range n.Dicts.Items {
				if str, ok := dict.(*String); ok {
					dicts = append(dicts, QuoteIdentifier(str.SVal))
				} else if nodeList, ok := dict.(*NodeList); ok {
					dicts = append(dicts, nodeListToQualifiedName(nodeList))
				}
			}
			if len(dicts) >= 2 {
				parts = append(parts, dicts[0], "WITH", dicts[1])
			}
		} else {
			// Standard format: "WITH dict1, dict2, ..."
			parts = append(parts, "WITH")
			var dicts []string
			for _, dict := range n.Dicts.Items {
				if str, ok := dict.(*String); ok {
					dicts = append(dicts, QuoteIdentifier(str.SVal))
				} else if nodeList, ok := dict.(*NodeList); ok {
					dicts = append(dicts, nodeListToQualifiedName(nodeList))
				}
			}
			parts = append(parts, strings.Join(dicts, ", "))
		}
	}

	return strings.Join(parts, " ")
}

// NewAlterTSConfigurationStmt creates a new AlterTSConfigurationStmt node
func NewAlterTSConfigurationStmt(kind AlterTSConfigType, cfgname *NodeList) *AlterTSConfigurationStmt {
	return &AlterTSConfigurationStmt{
		BaseNode: BaseNode{Tag: T_AlterTSConfigurationStmt},
		Kind:     kind,
		Cfgname:  cfgname,
	}
}

// AlterTSDictionaryStmt represents ALTER TEXT SEARCH DICTIONARY statements
// Ported from postgres/src/include/nodes/parsenodes.h:3693-3698
type AlterTSDictionaryStmt struct {
	BaseNode
	Dictname *NodeList `json:"dictname"` // Qualified name (list of String)
	Options  *NodeList `json:"options"`  // List of DefElem nodes
}

func (n *AlterTSDictionaryStmt) node() {}
func (n *AlterTSDictionaryStmt) stmt() {}

func (n *AlterTSDictionaryStmt) StatementType() string {
	return "ALTER"
}

func (n *AlterTSDictionaryStmt) String() string {
	return n.SqlString()
}

func (n *AlterTSDictionaryStmt) SqlString() string {
	var parts []string
	parts = append(parts, "ALTER TEXT SEARCH DICTIONARY")

	if n.Dictname != nil {
		parts = append(parts, nodeListToQualifiedName(n.Dictname))
	}

	if n.Options != nil && n.Options.Len() > 0 {
		var optStrs []string
		for _, opt := range n.Options.Items {
			if defElem, ok := opt.(*DefElem); ok {
				optStrs = append(optStrs, defElem.SqlString())
			}
		}
		parts = append(parts, "("+strings.Join(optStrs, ", ")+")")
	}

	return strings.Join(parts, " ")
}

// NewAlterTSDictionaryStmt creates a new AlterTSDictionaryStmt node
func NewAlterTSDictionaryStmt(dictname *NodeList, options *NodeList) *AlterTSDictionaryStmt {
	return &AlterTSDictionaryStmt{
		BaseNode: BaseNode{Tag: T_AlterTSDictionaryStmt},
		Dictname: dictname,
		Options:  options,
	}
}
