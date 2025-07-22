package ast

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNodeTag tests the NodeTag enum and its string representation.
// Validates that NodeTag constants match PostgreSQL concepts.
func TestNodeTag(t *testing.T) {
	tests := []struct {
		tag      NodeTag
		expected string
	}{
		{T_Invalid, "T_Invalid"},
		{T_Node, "T_Node"},
		{T_Query, "T_Query"},
		{T_SelectStmt, "T_SelectStmt"},
		{T_InsertStmt, "T_InsertStmt"},
		{T_List, "T_List"},
		{T_String, "T_String"},
		{T_Integer, "T_Integer"},
		{T_Null, "T_Null"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := tt.tag.String()
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestNodeTagUnknown tests unknown node tag handling.
func TestNodeTagUnknown(t *testing.T) {
	unknownTag := NodeTag(9999)
	result := unknownTag.String()
	assert.Equal(t, "NodeTag(9999)", result)
}

// TestBaseNode tests the basic node functionality.
func TestBaseNode(t *testing.T) {
	node := &BaseNode{
		Tag: T_Query,
		Loc: 100,
	}

	assert.Equal(t, T_Query, node.NodeTag())
	assert.Equal(t, 100, node.Location())
	assert.Equal(t, "T_Query@100", node.String())

	// Test location setting
	node.SetLocation(200)
	assert.Equal(t, 200, node.Location())
}

// TestIdentifier tests identifier node creation and functionality.
func TestIdentifier(t *testing.T) {
	ident := NewIdentifier("users")
	
	require.NotNil(t, ident)
	assert.Equal(t, T_String, ident.NodeTag())
	assert.Equal(t, "users", ident.Name)
	assert.Equal(t, "Identifier", ident.ExpressionType())
	assert.Contains(t, ident.String(), "users")
}

// TestValue tests the convenience NewValue function with type delegation.
func TestValue(t *testing.T) {
	tests := []struct {
		name        string
		value       interface{}
		expectedTag NodeTag
		expectedType string
		expectedStr string
	}{
		{"string_value", "hello", T_String, "String", "hello"},
		{"int_value", 42, T_Integer, "Integer", "42"},
		{"float_value", 3.14, T_Float, "Float", "3.14"},
		{"null_value", nil, T_Null, "Null", "NULL"},
		{"bool_value", true, T_Boolean, "Boolean", "true"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value := NewValue(tt.value)
			
			require.NotNil(t, value)
			assert.Equal(t, tt.expectedTag, value.NodeTag())
			assert.Contains(t, value.String(), tt.expectedStr)
			
			// Test that it's properly typed
			if expr, ok := value.(Expression); ok {
				assert.Equal(t, tt.expectedType, expr.ExpressionType())
			}
			
			// Test specific type assertions
			switch tt.expectedTag {
			case T_String:
				s, ok := value.(*String)
				assert.True(t, ok)
				assert.Equal(t, tt.value, s.SVal)
			case T_Integer:
				i, ok := value.(*Integer)
				assert.True(t, ok)
				assert.Equal(t, tt.value, i.IVal)
			case T_Float:
				f, ok := value.(*Float)
				assert.True(t, ok)
				assert.Equal(t, "3.14", f.FVal) // Float stores as string
			case T_Boolean:
				b, ok := value.(*Boolean)
				assert.True(t, ok)
				assert.Equal(t, tt.value, b.BoolVal)
			case T_Null:
				_, ok := value.(*Null)
				assert.True(t, ok)
			}
		})
	}
}

// TestTypedValues tests the new typed value node system.
func TestTypedValues(t *testing.T) {
	t.Run("Integer", func(t *testing.T) {
		value := NewInteger(42)
		require.NotNil(t, value)
		assert.Equal(t, T_Integer, value.NodeTag())
		assert.Equal(t, 42, value.IVal)
		assert.Equal(t, "Integer", value.ExpressionType())
		assert.Contains(t, value.String(), "42")
		assert.Equal(t, 42, IntVal(value))
		assert.True(t, value.IsValue())
	})

	t.Run("Float", func(t *testing.T) {
		value := NewFloat("3.14159")
		require.NotNil(t, value)
		assert.Equal(t, T_Float, value.NodeTag())
		assert.Equal(t, "3.14159", value.FVal)
		assert.Equal(t, "Float", value.ExpressionType())
		assert.Contains(t, value.String(), "3.14159")
		assert.InDelta(t, 3.14159, FloatVal(value), 0.0001)
		assert.True(t, value.IsValue())
	})

	t.Run("Boolean", func(t *testing.T) {
		value := NewBoolean(true)
		require.NotNil(t, value)
		assert.Equal(t, T_Boolean, value.NodeTag())
		assert.Equal(t, true, value.BoolVal)
		assert.Equal(t, "Boolean", value.ExpressionType())
		assert.Contains(t, value.String(), "true")
		assert.Equal(t, true, BoolVal(value))
		assert.True(t, value.IsValue())
	})

	t.Run("String", func(t *testing.T) {
		value := NewString("hello world")
		require.NotNil(t, value)
		assert.Equal(t, T_String, value.NodeTag())
		assert.Equal(t, "hello world", value.SVal)
		assert.Equal(t, "String", value.ExpressionType())
		assert.Contains(t, value.String(), "hello world")
		assert.Equal(t, "hello world", StrVal(value))
		assert.True(t, value.IsValue())
	})

	t.Run("BitString", func(t *testing.T) {
		value := NewBitString("10101010")
		require.NotNil(t, value)
		assert.Equal(t, T_BitString, value.NodeTag())
		assert.Equal(t, "10101010", value.BSVal)
		assert.Equal(t, "BitString", value.ExpressionType())
		assert.Contains(t, value.String(), "10101010")
		assert.True(t, value.IsValue())
	})

	t.Run("Null", func(t *testing.T) {
		value := NewNull()
		require.NotNil(t, value)
		assert.Equal(t, T_Null, value.NodeTag())
		assert.Equal(t, "Null", value.ExpressionType())
		assert.Contains(t, value.String(), "NULL")
		assert.True(t, value.IsValue())
	})
}

// TestNodeList tests the node list functionality.
func TestNodeList(t *testing.T) {
	// Create empty list
	list := NewNodeList()
	require.NotNil(t, list)
	assert.Equal(t, T_List, list.NodeTag())
	assert.Equal(t, 0, list.Len())

	// Create list with initial items
	id1 := NewIdentifier("col1")
	id2 := NewIdentifier("col2")
	list2 := NewNodeList(id1, id2)
	
	assert.Equal(t, 2, list2.Len())
	assert.Equal(t, id1, list2.Items[0])
	assert.Equal(t, id2, list2.Items[1])

	// Test append
	id3 := NewIdentifier("col3")
	list2.Append(id3)
	assert.Equal(t, 3, list2.Len())
	assert.Equal(t, id3, list2.Items[2])

	// Test string representation
	assert.Contains(t, list2.String(), "List[3 items]")
}

// TestIsNode tests the node type checking functions.
func TestIsNode(t *testing.T) {
	node := NewIdentifier("test")
	nonNode := "not a node"

	assert.True(t, IsNode(node))
	assert.False(t, IsNode(nonNode))
	assert.False(t, IsNode(nil))
}

// TestNodeTagOf tests getting node tags from values.
func TestNodeTagOf(t *testing.T) {
	node := NewIdentifier("test")
	nonNode := "not a node"

	assert.Equal(t, T_String, NodeTagOf(node))
	assert.Equal(t, T_Invalid, NodeTagOf(nonNode))
	assert.Equal(t, T_Invalid, NodeTagOf(nil))
}

// TestLocationOf tests getting locations from values.
func TestLocationOf(t *testing.T) {
	node := NewIdentifier("test")
	node.SetLocation(150)
	nonNode := "not a node"

	assert.Equal(t, 150, LocationOf(node))
	assert.Equal(t, -1, LocationOf(nonNode))
	assert.Equal(t, -1, LocationOf(nil))
}

// TestCastNode tests safe node casting.
func TestCastNode(t *testing.T) {
	node := NewIdentifier("test")
	nonNode := "not a node"

	result1 := CastNode(node)
	assert.Equal(t, node, result1)

	result2 := CastNode(nonNode)
	assert.Nil(t, result2)

	result3 := CastNode(nil)
	assert.Nil(t, result3)
}

// TestWalkNodes tests AST traversal functionality.
func TestWalkNodes(t *testing.T) {
	// Create a simple AST: List containing identifiers
	id1 := NewIdentifier("col1")
	id2 := NewIdentifier("col2")
	list := NewNodeList(id1, id2)

	// Walk and collect all visited nodes
	var visited []Node
	WalkNodes(list, func(node Node) bool {
		visited = append(visited, node)
		return true
	})

	// Should visit the list and both identifiers
	assert.Len(t, visited, 3)
	assert.Equal(t, list, visited[0])
	assert.Equal(t, id1, visited[1])
	assert.Equal(t, id2, visited[2])
}

// TestWalkNodesEarlyExit tests early exit from node walking.
func TestWalkNodesEarlyExit(t *testing.T) {
	id1 := NewIdentifier("col1")
	id2 := NewIdentifier("col2")
	list := NewNodeList(id1, id2)

	var visited []Node
	WalkNodes(list, func(node Node) bool {
		visited = append(visited, node)
		// Stop walking after the first node
		return false
	})

	// Should only visit the first node (list)
	assert.Len(t, visited, 1)
	assert.Equal(t, list, visited[0])
}

// TestFindNodes tests finding nodes by type.
func TestFindNodes(t *testing.T) {
	// Create AST with mixed node types
	id1 := NewIdentifier("col1")
	id2 := NewIdentifier("col2")  
	val1 := NewValue(42) // Use integer value to get T_Integer tag
	list := NewNodeList(id1, val1, id2)

	// Find all string nodes (identifiers use T_String)
	stringNodes := FindNodes(list, T_String)
	assert.Len(t, stringNodes, 2)
	assert.Equal(t, id1, stringNodes[0])
	assert.Equal(t, id2, stringNodes[1])

	// Find integer nodes
	intNodes := FindNodes(list, T_Integer)
	assert.Len(t, intNodes, 1)
	assert.Equal(t, val1, intNodes[0])

	// Find list nodes
	listNodes := FindNodes(list, T_List)
	assert.Len(t, listNodes, 1)
	assert.Equal(t, list, listNodes[0])

	// Find non-existent node type
	queryNodes := FindNodes(list, T_Query)
	assert.Empty(t, queryNodes)
}

// TestNodeInterfaces tests that nodes implement required interfaces.
func TestNodeInterfaces(t *testing.T) {
	// Test that basic nodes implement Node interface
	var node Node = NewIdentifier("test")
	assert.NotNil(t, node)
	assert.Equal(t, T_String, node.NodeTag())

	// Test that expressions implement Expression interface
	var expr Expression = NewIdentifier("test")
	assert.NotNil(t, expr)
	assert.Equal(t, "Identifier", expr.ExpressionType())

	var expr2 Expression = NewValue("test").(Expression)
	assert.NotNil(t, expr2)
	assert.Equal(t, "String", expr2.ExpressionType())
}

// TestComplexAST tests building and walking a more complex AST.
func TestComplexAST(t *testing.T) {
	// Create some simple identifiers and values
	id1 := NewIdentifier("col1")
	id2 := NewIdentifier("col2")
	val1 := NewValue(42)
	val2 := NewValue("test")
	
	// Build nested list structure
	innerList := NewNodeList(id1, val1)
	outerList := NewNodeList(innerList, id2, val2)
	
	// Walk the entire tree and count nodes
	nodeCount := 0
	WalkNodes(outerList, func(node Node) bool {
		nodeCount++
		return true
	})
	
	// Should visit: outerList, innerList, id1, val1, id2, val2
	assert.Equal(t, 6, nodeCount, "Should visit all nodes in nested structure")
	
	// Find all string nodes (identifiers)
	stringNodes := FindNodes(outerList, T_String)
	assert.Len(t, stringNodes, 3) // id1, id2, and val2 (string value)
	
	// Find all list nodes
	listNodes := FindNodes(outerList, T_List)
	assert.Len(t, listNodes, 2) // outerList and innerList
}