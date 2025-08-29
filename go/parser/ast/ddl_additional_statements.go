/*
 * PostgreSQL Additional DDL Statement AST Nodes
 *
 * This file contains AST node definitions for additional CREATE/ALTER statements
 * that are not already defined in other AST files.
 * Includes: TABLESPACE, ACCESS METHOD, ALTER STATISTICS, PUBLICATION, SUBSCRIPTION,
 * and ALTER OPERATOR FAMILY statements.
 *
 * Ported from postgres/src/include/nodes/parsenodes.h
 */

package ast

import (
	"fmt"
	"strings"
)

// =============================================================================
// TABLESPACE Statements
// =============================================================================

// CreateTableSpaceStmt represents CREATE TABLESPACE statement
type CreateTableSpaceStmt struct {
	BaseNode
	TablespaceName string    `json:"tablespacename"`
	Owner          *RoleSpec `json:"owner"`
	LocationPath   string    `json:"location"`
	Options        *NodeList `json:"options"`
}

func NewCreateTableSpaceStmt(name string, owner *RoleSpec, location string, options *NodeList) *CreateTableSpaceStmt {
	return &CreateTableSpaceStmt{
		BaseNode:       BaseNode{Tag: T_CreateTableSpaceStmt},
		TablespaceName: name,
		Owner:          owner,
		LocationPath:   location,
		Options:        options,
	}
}

func (c *CreateTableSpaceStmt) String() string {
	return fmt.Sprintf("CreateTableSpaceStmt(%s)@%d", c.TablespaceName, c.BaseNode.Location())
}

func (c *CreateTableSpaceStmt) StatementType() string {
	return "CreateTableSpaceStmt"
}

// SqlString returns the SQL representation of CREATE TABLESPACE statement
func (c *CreateTableSpaceStmt) SqlString() string {
	var parts []string
	parts = append(parts, "CREATE TABLESPACE", c.TablespaceName)
	
	if c.Owner != nil {
		parts = append(parts, "OWNER", c.Owner.SqlString())
	}
	
	if c.LocationPath != "" {
		parts = append(parts, "LOCATION", fmt.Sprintf("'%s'", c.LocationPath))
	}
	
	if c.Options != nil && c.Options.Len() > 0 {
		optionStrs := make([]string, 0, c.Options.Len())
		for i := 0; i < c.Options.Len(); i++ {
			if defElem, ok := c.Options.Items[i].(*DefElem); ok {
				optionStrs = append(optionStrs, defElem.SqlString())
			}
		}
		if len(optionStrs) > 0 {
			parts = append(parts, "WITH (", strings.Join(optionStrs, ", "), ")")
		}
	}
	
	return strings.Join(parts, " ")
}

// AlterTableSpaceStmt represents ALTER TABLESPACE statement
type AlterTableSpaceStmt struct {
	BaseNode
	TablespaceName string    `json:"tablespacename"`
	Options        *NodeList `json:"options"`
	IsReset        bool      `json:"isReset"`
}

func NewAlterTableSpaceStmt(name string, options *NodeList, isReset bool) *AlterTableSpaceStmt {
	return &AlterTableSpaceStmt{
		BaseNode:       BaseNode{Tag: T_AlterTableSpaceStmt},
		TablespaceName: name,
		Options:        options,
		IsReset:        isReset,
	}
}

func (a *AlterTableSpaceStmt) String() string {
	return fmt.Sprintf("AlterTableSpaceStmt(%s)@%d", a.TablespaceName, a.BaseNode.Location())
}

func (a *AlterTableSpaceStmt) StatementType() string {
	return "AlterTableSpaceStmt"
}

// SqlString returns the SQL representation of ALTER TABLESPACE statement
func (a *AlterTableSpaceStmt) SqlString() string {
	var parts []string
	parts = append(parts, "ALTER TABLESPACE", a.TablespaceName)
	
	if a.Options != nil && a.Options.Len() > 0 {
		optionStrs := make([]string, 0, a.Options.Len())
		for i := 0; i < a.Options.Len(); i++ {
			if defElem, ok := a.Options.Items[i].(*DefElem); ok {
				optionStrs = append(optionStrs, defElem.SqlString())
			}
		}
		if len(optionStrs) > 0 {
			if a.IsReset {
				parts = append(parts, "RESET (", strings.Join(optionStrs, ", "), ")")
			} else {
				parts = append(parts, "SET (", strings.Join(optionStrs, ", "), ")")
			}
		}
	}
	
	return strings.Join(parts, " ")
}

// =============================================================================
// ACCESS METHOD Statements
// =============================================================================

// AmType represents access method type
type AmType byte

const (
	AMTYPE_INDEX AmType = 'i' // Index access method
	AMTYPE_TABLE AmType = 't' // Table access method
)

// CreateAmStmt represents CREATE ACCESS METHOD statement
type CreateAmStmt struct {
	BaseNode
	AmName      string    `json:"amname"`
	HandlerName *NodeList `json:"handler_name"`
	AmType      AmType    `json:"amtype"`
}

func NewCreateAmStmt(name string, amType AmType, handler *NodeList) *CreateAmStmt {
	return &CreateAmStmt{
		BaseNode:    BaseNode{Tag: T_CreateAmStmt},
		AmName:      name,
		AmType:      amType,
		HandlerName: handler,
	}
}

func (c *CreateAmStmt) String() string {
	return fmt.Sprintf("CreateAmStmt(%s)@%d", c.AmName, c.BaseNode.Location())
}

func (c *CreateAmStmt) StatementType() string {
	return "CreateAmStmt"
}

// SqlString returns the SQL representation of CREATE ACCESS METHOD statement
func (c *CreateAmStmt) SqlString() string {
	var parts []string
	parts = append(parts, "CREATE ACCESS METHOD", c.AmName)
	
	var typeStr string
	switch c.AmType {
	case AMTYPE_INDEX:
		typeStr = "INDEX"
	case AMTYPE_TABLE:
		typeStr = "TABLE"
	}
	parts = append(parts, "TYPE", typeStr)
	
	if c.HandlerName != nil && c.HandlerName.Len() > 0 {
		handlerNames := make([]string, 0, c.HandlerName.Len())
		for i := 0; i < c.HandlerName.Len(); i++ {
			if strNode, ok := c.HandlerName.Items[i].(*String); ok {
				handlerNames = append(handlerNames, strNode.SVal)
			}
		}
		parts = append(parts, "HANDLER", strings.Join(handlerNames, "."))
	}
	
	return strings.Join(parts, " ")
}

// =============================================================================
// ALTER STATISTICS Statement
// =============================================================================

// AlterStatsStmt represents ALTER STATISTICS statement
type AlterStatsStmt struct {
	BaseNode
	DefNames      *NodeList `json:"defnames"`
	StxStatTarget Node      `json:"stxstattarget"`
	MissingOk     bool      `json:"missing_ok"`
}

func NewAlterStatsStmt(defnames *NodeList, target Node, missingOk bool) *AlterStatsStmt {
	return &AlterStatsStmt{
		BaseNode:      BaseNode{Tag: T_AlterStatsStmt},
		DefNames:      defnames,
		StxStatTarget: target,
		MissingOk:     missingOk,
	}
}

func (a *AlterStatsStmt) String() string {
	return fmt.Sprintf("AlterStatsStmt@%d", a.BaseNode.Location())
}

func (a *AlterStatsStmt) StatementType() string {
	return "AlterStatsStmt"
}

// SqlString returns the SQL representation of ALTER STATISTICS statement
func (a *AlterStatsStmt) SqlString() string {
	var parts []string
	parts = append(parts, "ALTER STATISTICS")
	
	if a.MissingOk {
		parts = append(parts, "IF EXISTS")
	}
	
	if a.DefNames != nil && a.DefNames.Len() > 0 {
		nameStrs := make([]string, 0, a.DefNames.Len())
		for i := 0; i < a.DefNames.Len(); i++ {
			if strNode, ok := a.DefNames.Items[i].(*String); ok {
				nameStrs = append(nameStrs, strNode.SVal)
			}
		}
		parts = append(parts, strings.Join(nameStrs, "."))
	}
	
	parts = append(parts, "SET STATISTICS")
	if a.StxStatTarget != nil {
		parts = append(parts, a.StxStatTarget.SqlString())
	}
	
	return strings.Join(parts, " ")
}

// =============================================================================
// PUBLICATION Statements
// =============================================================================

// AlterPublicationType represents types of ALTER PUBLICATION operations
type AlterPublicationType int

const (
	AP_SetOptions AlterPublicationType = iota
	AP_AddObjects
	AP_SetObjects
	AP_DropObjects
)

// PublicationObjSpecType represents types of publication objects
type PublicationObjSpecType int

const (
	PUBLICATIONOBJ_TABLE PublicationObjSpecType = iota
	PUBLICATIONOBJ_TABLES_IN_SCHEMA
	PUBLICATIONOBJ_TABLES_IN_CUR_SCHEMA
	PUBLICATIONOBJ_CONTINUATION
)

// PublicationTable represents a publication table specification
type PublicationTable struct {
	BaseNode
	Relation    *RangeVar `json:"relation"`    // relation to be published
	WhereClause Node      `json:"whereclause"` // qualifications
	Columns     *NodeList `json:"columns"`     // List of columns in a publication table
}

func NewPublicationTable(relation *RangeVar, whereClause Node, columns *NodeList) *PublicationTable {
	return &PublicationTable{
		BaseNode:    BaseNode{Tag: T_PublicationTable},
		Relation:    relation,
		WhereClause: whereClause,
		Columns:     columns,
	}
}

// PublicationObjSpec represents a publication object specification
// Ported from postgres/src/include/nodes/parsenodes.h:4151-4158
type PublicationObjSpec struct {
	BaseNode
	PubObjType PublicationObjSpecType `json:"pubobjtype"` // type of this publication object
	Name       string                 `json:"name"`       // object name
	PubTable   *PublicationTable      `json:"pubtable"`   // publication table details
}

// NewPublicationObjSpecName creates a PublicationObjSpec with just a name
func NewPublicationObjSpecName(objType PublicationObjSpecType, name string) *PublicationObjSpec {
	return &PublicationObjSpec{
		BaseNode:   BaseNode{Tag: T_PublicationObjSpec},
		PubObjType: objType,
		Name:       name,
		PubTable:   nil,
	}
}

// NewPublicationObjSpecTable creates a PublicationObjSpec with a PublicationTable
func NewPublicationObjSpecTable(objType PublicationObjSpecType, pubTable *PublicationTable) *PublicationObjSpec {
	return &PublicationObjSpec{
		BaseNode:   BaseNode{Tag: T_PublicationObjSpec},
		PubObjType: objType,
		Name:       "",
		PubTable:   pubTable,
	}
}

// NewPublicationObjSpec creates a basic PublicationObjSpec (for schema-only cases)
func NewPublicationObjSpec(objType PublicationObjSpecType) *PublicationObjSpec {
	return &PublicationObjSpec{
		BaseNode:   BaseNode{Tag: T_PublicationObjSpec},
		PubObjType: objType,
		Name:       "",
		PubTable:   nil,
	}
}

// CreatePublicationStmt represents CREATE PUBLICATION statement
type CreatePublicationStmt struct {
	BaseNode
	PubName      string    `json:"pubname"`
	PubObjects   *NodeList `json:"pubobjects"`
	ForAllTables bool      `json:"for_all_tables"`
	Options      *NodeList `json:"options"`
}

func NewCreatePublicationStmt(name string, objects *NodeList, forAllTables bool, options *NodeList) *CreatePublicationStmt {
	return &CreatePublicationStmt{
		BaseNode:     BaseNode{Tag: T_CreatePublicationStmt},
		PubName:      name,
		PubObjects:   objects,
		ForAllTables: forAllTables,
		Options:      options,
	}
}

func (c *CreatePublicationStmt) String() string {
	return fmt.Sprintf("CreatePublicationStmt(%s)@%d", c.PubName, c.BaseNode.Location())
}

func (c *CreatePublicationStmt) StatementType() string {
	return "CreatePublicationStmt"
}

// SqlString returns the SQL representation of CREATE PUBLICATION statement
func (c *CreatePublicationStmt) SqlString() string {
	var parts []string
	parts = append(parts, "CREATE PUBLICATION", c.PubName)
	
	if c.ForAllTables {
		parts = append(parts, "FOR ALL TABLES")
	} else if c.PubObjects != nil && c.PubObjects.Len() > 0 {
		parts = append(parts, "FOR TABLE")
		objStrs := make([]string, 0, c.PubObjects.Len())
		for i := 0; i < c.PubObjects.Len(); i++ {
			if pubObj, ok := c.PubObjects.Items[i].(*PublicationObjSpec); ok {
				if pubObj.PubTable != nil && pubObj.PubTable.Relation != nil {
					objStrs = append(objStrs, pubObj.PubTable.Relation.SqlString())
				} else if pubObj.Name != "" {
					objStrs = append(objStrs, pubObj.Name)
				}
			}
		}
		if len(objStrs) > 0 {
			parts = append(parts, strings.Join(objStrs, ", "))
		}
	}
	
	if c.Options != nil && c.Options.Len() > 0 {
		optionStrs := make([]string, 0, c.Options.Len())
		for i := 0; i < c.Options.Len(); i++ {
			if defElem, ok := c.Options.Items[i].(*DefElem); ok {
				optionStrs = append(optionStrs, defElem.SqlString())
			}
		}
		if len(optionStrs) > 0 {
			parts = append(parts, "WITH (", strings.Join(optionStrs, ", "), ")")
		}
	}
	
	return strings.Join(parts, " ")
}

// AlterPublicationStmt represents ALTER PUBLICATION statement
type AlterPublicationStmt struct {
	BaseNode
	PubName    string               `json:"pubname"`
	Options    *NodeList            `json:"options"`
	PubObjects *NodeList            `json:"pubobjects"`
	Action     AlterPublicationType `json:"action"`
}

func NewAlterPublicationStmt(name string, options, objects *NodeList, action AlterPublicationType) *AlterPublicationStmt {
	return &AlterPublicationStmt{
		BaseNode:   BaseNode{Tag: T_AlterPublicationStmt},
		PubName:    name,
		Options:    options,
		PubObjects: objects,
		Action:     action,
	}
}

func (a *AlterPublicationStmt) String() string {
	return fmt.Sprintf("AlterPublicationStmt(%s)@%d", a.PubName, a.BaseNode.Location())
}

func (a *AlterPublicationStmt) StatementType() string {
	return "AlterPublicationStmt"
}

// SqlString returns the SQL representation of ALTER PUBLICATION statement
func (a *AlterPublicationStmt) SqlString() string {
	var parts []string
	parts = append(parts, "ALTER PUBLICATION", a.PubName)
	
	switch a.Action {
	case AP_SetOptions:
		if a.Options != nil && a.Options.Len() > 0 {
			optionStrs := make([]string, 0, a.Options.Len())
			for i := 0; i < a.Options.Len(); i++ {
				if defElem, ok := a.Options.Items[i].(*DefElem); ok {
					optionStrs = append(optionStrs, defElem.SqlString())
				}
			}
			parts = append(parts, "SET (", strings.Join(optionStrs, ", "), ")")
		}
	case AP_AddObjects:
		parts = append(parts, "ADD TABLE")
		if a.PubObjects != nil && a.PubObjects.Len() > 0 {
			objStrs := make([]string, 0, a.PubObjects.Len())
			for i := 0; i < a.PubObjects.Len(); i++ {
				if pubObj, ok := a.PubObjects.Items[i].(*PublicationObjSpec); ok {
					if pubObj.PubTable != nil && pubObj.PubTable.Relation != nil {
						objStrs = append(objStrs, pubObj.PubTable.Relation.SqlString())
					}
				}
			}
			parts = append(parts, strings.Join(objStrs, ", "))
		}
	case AP_SetObjects:
		parts = append(parts, "SET TABLE")
		if a.PubObjects != nil && a.PubObjects.Len() > 0 {
			objStrs := make([]string, 0, a.PubObjects.Len())
			for i := 0; i < a.PubObjects.Len(); i++ {
				if pubObj, ok := a.PubObjects.Items[i].(*PublicationObjSpec); ok {
					if pubObj.PubTable != nil && pubObj.PubTable.Relation != nil {
						objStrs = append(objStrs, pubObj.PubTable.Relation.SqlString())
					}
				}
			}
			parts = append(parts, strings.Join(objStrs, ", "))
		}
	case AP_DropObjects:
		parts = append(parts, "DROP TABLE")
		if a.PubObjects != nil && a.PubObjects.Len() > 0 {
			objStrs := make([]string, 0, a.PubObjects.Len())
			for i := 0; i < a.PubObjects.Len(); i++ {
				if pubObj, ok := a.PubObjects.Items[i].(*PublicationObjSpec); ok {
					if pubObj.PubTable != nil && pubObj.PubTable.Relation != nil {
						objStrs = append(objStrs, pubObj.PubTable.Relation.SqlString())
					}
				}
			}
			parts = append(parts, strings.Join(objStrs, ", "))
		}
	}
	
	return strings.Join(parts, " ")
}

// =============================================================================
// SUBSCRIPTION Statements
// =============================================================================

// AlterSubscriptionType represents types of ALTER SUBSCRIPTION operations
type AlterSubscriptionType int

const (
	ALTER_SUBSCRIPTION_OPTIONS AlterSubscriptionType = iota
	ALTER_SUBSCRIPTION_CONNECTION
	ALTER_SUBSCRIPTION_REFRESH
	ALTER_SUBSCRIPTION_ADD_PUBLICATION
	ALTER_SUBSCRIPTION_DROP_PUBLICATION
	ALTER_SUBSCRIPTION_SET_PUBLICATION
	ALTER_SUBSCRIPTION_ENABLED
	ALTER_SUBSCRIPTION_SKIP
)

// CreateSubscriptionStmt represents CREATE SUBSCRIPTION statement
type CreateSubscriptionStmt struct {
	BaseNode
	SubName     string    `json:"subname"`
	ConnInfo    string    `json:"conninfo"`
	Publication *NodeList `json:"publication"`
	Options     *NodeList `json:"options"`
}

func NewCreateSubscriptionStmt(name, connInfo string, publication, options *NodeList) *CreateSubscriptionStmt {
	return &CreateSubscriptionStmt{
		BaseNode:    BaseNode{Tag: T_CreateSubscriptionStmt},
		SubName:     name,
		ConnInfo:    connInfo,
		Publication: publication,
		Options:     options,
	}
}

func (c *CreateSubscriptionStmt) String() string {
	return fmt.Sprintf("CreateSubscriptionStmt(%s)@%d", c.SubName, c.BaseNode.Location())
}

func (c *CreateSubscriptionStmt) StatementType() string {
	return "CreateSubscriptionStmt"
}

// SqlString returns the SQL representation of CREATE SUBSCRIPTION statement
func (c *CreateSubscriptionStmt) SqlString() string {
	var parts []string
	parts = append(parts, "CREATE SUBSCRIPTION", c.SubName)
	parts = append(parts, "CONNECTION", fmt.Sprintf("'%s'", c.ConnInfo))
	
	if c.Publication != nil && c.Publication.Len() > 0 {
		pubStrs := make([]string, 0, c.Publication.Len())
		for i := 0; i < c.Publication.Len(); i++ {
			if strNode, ok := c.Publication.Items[i].(*String); ok {
				pubStrs = append(pubStrs, fmt.Sprintf("'%s'", strNode.SVal))
			}
		}
		parts = append(parts, "PUBLICATION", strings.Join(pubStrs, ", "))
	}
	
	if c.Options != nil && c.Options.Len() > 0 {
		optionStrs := make([]string, 0, c.Options.Len())
		for i := 0; i < c.Options.Len(); i++ {
			if defElem, ok := c.Options.Items[i].(*DefElem); ok {
				optionStrs = append(optionStrs, defElem.SqlString())
			}
		}
		if len(optionStrs) > 0 {
			parts = append(parts, "WITH (", strings.Join(optionStrs, ", "), ")")
		}
	}
	
	return strings.Join(parts, " ")
}

// AlterSubscriptionStmt represents ALTER SUBSCRIPTION statement
type AlterSubscriptionStmt struct {
	BaseNode
	SubName     string                `json:"subname"`
	Kind        AlterSubscriptionType `json:"kind"`
	ConnInfo    string                `json:"conninfo"`
	Publication *NodeList             `json:"publication"`
	Options     *NodeList             `json:"options"`
}

func NewAlterSubscriptionStmt(name string, kind AlterSubscriptionType, connInfo string, publication, options *NodeList) *AlterSubscriptionStmt {
	return &AlterSubscriptionStmt{
		BaseNode:    BaseNode{Tag: T_AlterSubscriptionStmt},
		SubName:     name,
		Kind:        kind,
		ConnInfo:    connInfo,
		Publication: publication,
		Options:     options,
	}
}

func (a *AlterSubscriptionStmt) String() string {
	return fmt.Sprintf("AlterSubscriptionStmt(%s)@%d", a.SubName, a.BaseNode.Location())
}

func (a *AlterSubscriptionStmt) StatementType() string {
	return "AlterSubscriptionStmt"
}

// SqlString returns the SQL representation of ALTER SUBSCRIPTION statement
func (a *AlterSubscriptionStmt) SqlString() string {
	var parts []string
	parts = append(parts, "ALTER SUBSCRIPTION", a.SubName)
	
	switch a.Kind {
	case ALTER_SUBSCRIPTION_OPTIONS:
		if a.Options != nil && a.Options.Len() > 0 {
			optionStrs := make([]string, 0, a.Options.Len())
			for i := 0; i < a.Options.Len(); i++ {
				if defElem, ok := a.Options.Items[i].(*DefElem); ok {
					optionStrs = append(optionStrs, defElem.SqlString())
				}
			}
			parts = append(parts, "SET (", strings.Join(optionStrs, ", "), ")")
		}
	case ALTER_SUBSCRIPTION_CONNECTION:
		if a.ConnInfo != "" {
			parts = append(parts, "CONNECTION", fmt.Sprintf("'%s'", a.ConnInfo))
		}
	case ALTER_SUBSCRIPTION_REFRESH:
		parts = append(parts, "REFRESH PUBLICATION")
		if a.Options != nil && a.Options.Len() > 0 {
			optionStrs := make([]string, 0, a.Options.Len())
			for i := 0; i < a.Options.Len(); i++ {
				if defElem, ok := a.Options.Items[i].(*DefElem); ok {
					optionStrs = append(optionStrs, defElem.SqlString())
				}
			}
			parts = append(parts, "WITH (", strings.Join(optionStrs, ", "), ")")
		}
	case ALTER_SUBSCRIPTION_ADD_PUBLICATION:
		parts = append(parts, "ADD PUBLICATION")
		if a.Publication != nil && a.Publication.Len() > 0 {
			pubStrs := make([]string, 0, a.Publication.Len())
			for i := 0; i < a.Publication.Len(); i++ {
				if strNode, ok := a.Publication.Items[i].(*String); ok {
					pubStrs = append(pubStrs, fmt.Sprintf("'%s'", strNode.SVal))
				}
			}
			parts = append(parts, strings.Join(pubStrs, ", "))
		}
	case ALTER_SUBSCRIPTION_DROP_PUBLICATION:
		parts = append(parts, "DROP PUBLICATION")
		if a.Publication != nil && a.Publication.Len() > 0 {
			pubStrs := make([]string, 0, a.Publication.Len())
			for i := 0; i < a.Publication.Len(); i++ {
				if strNode, ok := a.Publication.Items[i].(*String); ok {
					pubStrs = append(pubStrs, fmt.Sprintf("'%s'", strNode.SVal))
				}
			}
			parts = append(parts, strings.Join(pubStrs, ", "))
		}
	case ALTER_SUBSCRIPTION_SET_PUBLICATION:
		parts = append(parts, "SET PUBLICATION")
		if a.Publication != nil && a.Publication.Len() > 0 {
			pubStrs := make([]string, 0, a.Publication.Len())
			for i := 0; i < a.Publication.Len(); i++ {
				if strNode, ok := a.Publication.Items[i].(*String); ok {
					pubStrs = append(pubStrs, fmt.Sprintf("'%s'", strNode.SVal))
				}
			}
			parts = append(parts, strings.Join(pubStrs, ", "))
		}
	case ALTER_SUBSCRIPTION_ENABLED:
		parts = append(parts, "ENABLE")
	case ALTER_SUBSCRIPTION_SKIP:
		parts = append(parts, "SKIP")
	}
	
	return strings.Join(parts, " ")
}

// =============================================================================
// ALTER OPERATOR FAMILY Statement
// =============================================================================

// AlterOpFamilyStmt represents ALTER OPERATOR FAMILY statement
type AlterOpFamilyStmt struct {
	BaseNode
	OpFamilyName *NodeList `json:"opfamilyname"`
	AmName       string    `json:"amname"`
	IsDrop       bool      `json:"isDrop"`
	Items        *NodeList `json:"items"`
}

func NewAlterOpFamilyStmt(name *NodeList, amName string, isDrop bool, items *NodeList) *AlterOpFamilyStmt {
	return &AlterOpFamilyStmt{
		BaseNode:     BaseNode{Tag: T_AlterOpFamilyStmt},
		OpFamilyName: name,
		AmName:       amName,
		IsDrop:       isDrop,
		Items:        items,
	}
}

func (a *AlterOpFamilyStmt) String() string {
	return fmt.Sprintf("AlterOpFamilyStmt@%d", a.BaseNode.Location())
}

func (a *AlterOpFamilyStmt) StatementType() string {
	return "AlterOpFamilyStmt"
}

// SqlString returns the SQL representation of ALTER OPERATOR FAMILY statement
func (a *AlterOpFamilyStmt) SqlString() string {
	var parts []string
	parts = append(parts, "ALTER OPERATOR FAMILY")
	
	if a.OpFamilyName != nil && a.OpFamilyName.Len() > 0 {
		nameStrs := make([]string, 0, a.OpFamilyName.Len())
		for i := 0; i < a.OpFamilyName.Len(); i++ {
			if strNode, ok := a.OpFamilyName.Items[i].(*String); ok {
				nameStrs = append(nameStrs, strNode.SVal)
			}
		}
		parts = append(parts, strings.Join(nameStrs, "."))
	}
	
	parts = append(parts, "USING", a.AmName)
	
	if a.IsDrop {
		parts = append(parts, "DROP")
	} else {
		parts = append(parts, "ADD")
	}
	
	// Add the items
	if a.Items != nil && a.Items.Len() > 0 {
		itemStrs := make([]string, 0, a.Items.Len())
		for i := 0; i < a.Items.Len(); i++ {
			if item, ok := a.Items.Items[i].(*CreateOpClassItem); ok {
				itemStrs = append(itemStrs, item.SqlString())
			}
		}
		if len(itemStrs) > 0 {
			parts = append(parts, strings.Join(itemStrs, ", "))
		}
	}
	
	return strings.Join(parts, " ")
}

// =============================================================================
// Supporting Types for OPERATOR CLASS
// =============================================================================

// OpClassItemType represents types of operator class items
type OpClassItemType int

const (
	OPCLASS_ITEM_OPERATOR    OpClassItemType = 1 // #define OPCLASS_ITEM_OPERATOR 1
	OPCLASS_ITEM_FUNCTION    OpClassItemType = 2 // #define OPCLASS_ITEM_FUNCTION 2
	OPCLASS_ITEM_STORAGETYPE OpClassItemType = 3 // #define OPCLASS_ITEM_STORAGETYPE 3
)
