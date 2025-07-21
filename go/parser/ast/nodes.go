// Package ast provides PostgreSQL AST node definitions and interfaces.
// Ported from postgres/src/include/nodes/nodes.h and related header files.
package ast

import (
	"fmt"
)

// NodeTag represents the type of an AST node.
// Ported from postgres/src/include/nodes/nodes.h:26-31 (NodeTag enum)
type NodeTag int

// NodeTag constants - ported from postgres/src/include/nodes/nodes.h:28-30
// These represent the fundamental node types in the PostgreSQL AST
const (
	T_Invalid NodeTag = iota // Ported from postgres/src/include/nodes/nodes.h:28

	// Basic node types - will be expanded in full implementation
	T_Node
	T_Query
	T_SelectStmt
	T_InsertStmt
	T_UpdateStmt
	T_DeleteStmt
	T_MergeStmt
	T_CreateStmt
	T_DropStmt
	T_AlterStmt
	
	// Expression nodes
	T_Expr
	T_Var
	T_Const
	T_Param
	T_Aggref
	T_WindowFunc
	T_FuncExpr
	T_OpExpr
	T_BoolExpr
	
	// List and utility nodes
	T_List
	T_ResTarget
	T_RangeVar
	T_ColumnRef
	T_AConst
	T_String
	T_Integer
	T_Float
	T_BitString
	T_Null
)

// String returns the string representation of a NodeTag.
// Used for debugging and error reporting.
func (nt NodeTag) String() string {
	switch nt {
	case T_Invalid:
		return "T_Invalid"
	case T_Node:
		return "T_Node"
	case T_Query:
		return "T_Query"
	case T_SelectStmt:
		return "T_SelectStmt"
	case T_InsertStmt:
		return "T_InsertStmt"
	case T_UpdateStmt:
		return "T_UpdateStmt"
	case T_DeleteStmt:
		return "T_DeleteStmt"
	case T_MergeStmt:
		return "T_MergeStmt"
	case T_CreateStmt:
		return "T_CreateStmt"
	case T_DropStmt:
		return "T_DropStmt"
	case T_AlterStmt:
		return "T_AlterStmt"
	case T_Expr:
		return "T_Expr"
	case T_Var:
		return "T_Var"
	case T_Const:
		return "T_Const"
	case T_Param:
		return "T_Param"
	case T_Aggref:
		return "T_Aggref"
	case T_WindowFunc:
		return "T_WindowFunc"
	case T_FuncExpr:
		return "T_FuncExpr"
	case T_OpExpr:
		return "T_OpExpr"
	case T_BoolExpr:
		return "T_BoolExpr"
	case T_List:
		return "T_List"
	case T_ResTarget:
		return "T_ResTarget"
	case T_RangeVar:
		return "T_RangeVar"
	case T_ColumnRef:
		return "T_ColumnRef"
	case T_AConst:
		return "T_AConst"
	case T_String:
		return "T_String"
	case T_Integer:
		return "T_Integer"
	case T_Float:
		return "T_Float"
	case T_BitString:
		return "T_BitString"
	case T_Null:
		return "T_Null"
	default:
		return fmt.Sprintf("NodeTag(%d)", int(nt))
	}
}

// Node is the base interface for all PostgreSQL AST nodes.
// Every node in the parse tree implements this interface.
// Ported from postgres/src/include/nodes/nodes.h:17-19 (base node concept)
type Node interface {
	// NodeTag returns the type tag for this node
	NodeTag() NodeTag
	
	// Location returns the byte offset in the source string where this node begins.
	// Returns -1 if location is not available.
	// Ported from postgres/src/include/nodes/parsenodes.h:6-12 location concept
	Location() int
	
	// String returns a string representation of the node (for debugging)
	String() string
}

// BaseNode provides a basic implementation of the Node interface.
// Other node types should embed this to get default implementations.
// Ported from postgres base node structure concept
type BaseNode struct {
	Tag NodeTag // Node type tag - ported from postgres/src/include/nodes/nodes.h:18
	Loc int     // Source location in bytes - ported from postgres/src/include/nodes/parsenodes.h:6-12
}

// NodeTag returns the node's type tag.
func (n *BaseNode) NodeTag() NodeTag {
	return n.Tag
}

// Location returns the node's source location.
func (n *BaseNode) Location() int {
	return n.Loc
}

// String returns a basic string representation.
func (n *BaseNode) String() string {
	return fmt.Sprintf("%s@%d", n.Tag, n.Loc)
}

// SetLocation sets the source location for this node.
// Used during parsing to track where nodes came from in the source.
func (n *BaseNode) SetLocation(location int) {
	n.Loc = location
}

// NodeList represents a list of nodes.
// This is a fundamental PostgreSQL concept used throughout the AST.
// Ported from postgres List structure concept
type NodeList struct {
	BaseNode
	Items []Node // List of nodes
}

// NewNodeList creates a new node list.
func NewNodeList(items ...Node) *NodeList {
	return &NodeList{
		BaseNode: BaseNode{Tag: T_List},
		Items:    items,
	}
}

// Append adds a node to the list.
func (l *NodeList) Append(node Node) {
	l.Items = append(l.Items, node)
}

// Len returns the number of items in the list.
func (l *NodeList) Len() int {
	return len(l.Items)
}

// String returns a string representation of the list.
func (l *NodeList) String() string {
	return fmt.Sprintf("List[%d items]@%d", len(l.Items), l.Location())
}

// Statement represents the base interface for all SQL statements.
// All top-level SQL constructs implement this interface.
// Ported from postgres statement node concept
type Statement interface {
	Node
	StatementType() string
}

// Expression represents the base interface for all SQL expressions.
// All expressions in WHERE clauses, SELECT lists, etc. implement this.
// Ported from postgres expression node concept
type Expression interface {
	Node
	ExpressionType() string
}

// Identifier represents a simple identifier (table name, column name, etc.).
// Ported from basic identifier concept used throughout postgres AST
type Identifier struct {
	BaseNode
	Name string // The identifier name
}

// NewIdentifier creates a new identifier node.
func NewIdentifier(name string) *Identifier {
	return &Identifier{
		BaseNode: BaseNode{Tag: T_String}, // Use T_String for simple identifiers
		Name:     name,
	}
}

// String returns the identifier name.
func (i *Identifier) String() string {
	return fmt.Sprintf("Identifier(%s)@%d", i.Name, i.Location())
}

// ExpressionType returns the expression type for Expression interface.
func (i *Identifier) ExpressionType() string {
	return "Identifier"
}

// Value represents a literal value in the AST.
// Ported from postgres constant value concept
type Value struct {
	BaseNode
	Val interface{} // The actual value (string, int, float, bool, nil)
}

// NewValue creates a new value node.
func NewValue(val interface{}) *Value {
	var tag NodeTag
	switch val.(type) {
	case string:
		tag = T_String
	case int, int32, int64:
		tag = T_Integer
	case float32, float64:
		tag = T_Float
	case nil:
		tag = T_Null
	default:
		tag = T_AConst // Generic constant
	}
	
	return &Value{
		BaseNode: BaseNode{Tag: tag},
		Val:      val,
	}
}

// String returns a string representation of the value.
func (v *Value) String() string {
	return fmt.Sprintf("Value(%v)@%d", v.Val, v.Location())
}

// ExpressionType returns the expression type for Expression interface.
func (v *Value) ExpressionType() string {
	return "Value"
}

// Helper functions for node creation and manipulation

// IsNode checks if a value implements the Node interface.
func IsNode(v interface{}) bool {
	_, ok := v.(Node)
	return ok
}

// NodeTagOf returns the NodeTag of a node, or T_Invalid if not a node.
func NodeTagOf(v interface{}) NodeTag {
	if node, ok := v.(Node); ok {
		return node.NodeTag()
	}
	return T_Invalid
}

// LocationOf returns the location of a node, or -1 if not a node or no location.
func LocationOf(v interface{}) int {
	if node, ok := v.(Node); ok {
		return node.Location()
	}
	return -1
}

// CastNode safely casts a value to a Node, returning nil if not a node.
func CastNode(v interface{}) Node {
	if node, ok := v.(Node); ok {
		return node
	}
	return nil
}

// NodeWalker is a function type for walking the AST.
// It receives a node and returns whether to continue walking.
type NodeWalker func(Node) bool

// WalkNodes recursively walks all nodes in an AST, calling the walker function.
// This is useful for analysis, transformation, and debugging.
func WalkNodes(node Node, walker NodeWalker) {
	if node == nil || !walker(node) {
		return
	}
	
	// Handle specific node types that contain other nodes
	switch n := node.(type) {
	case *NodeList:
		for _, item := range n.Items {
			if item != nil {
				WalkNodes(item, walker)
			}
		}
	// Additional node types will be handled as they're implemented
	default:
		// For now, we don't traverse into other node types
		// This will be expanded as we implement more complex AST structures
	}
}

// FindNodes finds all nodes of a specific type in an AST.
func FindNodes(root Node, targetTag NodeTag) []Node {
	var found []Node
	WalkNodes(root, func(node Node) bool {
		if node.NodeTag() == targetTag {
			found = append(found, node)
		}
		return true
	})
	return found
}

// PrintAST prints a simple representation of an AST for debugging.
func PrintAST(node Node, indent int) {
	if node == nil {
		return
	}
	
	prefix := ""
	for i := 0; i < indent; i++ {
		prefix += "  "
	}
	
	fmt.Printf("%s%s\n", prefix, node.String())
	
	// Print children for specific node types
	switch n := node.(type) {
	case *NodeList:
		for _, item := range n.Items {
			PrintAST(item, indent+1)
		}
	}
}