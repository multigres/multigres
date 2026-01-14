// Copyright 2025 Supabase, Inc.
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

// Rewrite traverses a syntax tree recursively, starting with root,
// and calling pre and post for each node as described below.
// Rewrite returns the syntax tree, possibly modified.
//
// If pre is not nil, it is called for each node before the node's
// children are traversed (pre-order). If pre returns false, no
// children are traversed, and post is not called for that node.
//
// If post is not nil, and a prior call of pre didn't return false,
// post is called for each node after its children are traversed
// (post-order). If post returns false, traversal is terminated and
// Rewrite returns immediately.
//
// Only fields that refer to AST nodes are considered children;
// i.e., fields of basic types (strings, []byte, etc.) are ignored.
//
// Example usage:
//
//	// Find all table references
//	tables := []string{}
//	Rewrite(stmt, func(cursor *Cursor) bool {
//	    if rv, ok := cursor.Node().(*RangeVar); ok {
//	        tables = append(tables, rv.RelName)
//	    }
//	    return true  // Continue traversal
//	}, nil)
//
//	// Replace all integer constants
//	Rewrite(stmt, func(cursor *Cursor) bool {
//	    if intVal, ok := cursor.Node().(*Integer); ok {
//	        if intVal.IVal == 42 {
//	            cursor.Replace(NewInteger(100))
//	        }
//	    }
//	    return true
//	}, nil)
func Rewrite(node Node, pre, post ApplyFunc) (result Node) {
	parent := &RootNode{node}

	// this is the root-replacer, used when the user replaces the root of the ast
	replacer := func(newNode Node, _ Node) {
		parent.Node = newNode
	}

	a := &application{
		pre:  pre,
		post: post,
	}

	a.rewriteNode(parent, node, replacer)

	return parent.Node
}

// RootNode is the root node of the AST when rewriting. It is the first element of the tree.
type RootNode struct {
	Node
}

// An ApplyFunc is invoked by Rewrite for each node n, even if n is nil,
// before and/or after the node's children, using a Cursor describing
// the current node and providing operations on it.
//
// The return value of ApplyFunc controls the syntax tree traversal.
// See Rewrite for details.
type ApplyFunc func(*Cursor) bool

// A Cursor describes a node encountered during Rewrite.
// Information about the node and its parent is available
// from the Node and Parent methods.
type Cursor struct {
	parent   Node
	replacer replacerFunc
	node     Node

	// marks that the node has been replaced, and the new node should be visited
	revisit bool
}

// Node returns the current Node.
func (c *Cursor) Node() Node { return c.node }

// Parent returns the parent of the current Node.
func (c *Cursor) Parent() Node { return c.parent }

// Replace replaces the current node in the parent field with this new object.
// The caller needs to make sure to not replace the object with something of
// the wrong type, or the visitor will panic.
func (c *Cursor) Replace(newNode Node) {
	c.replacer(newNode, c.parent)
	c.node = newNode
}

// ReplaceAndRevisit replaces the current node in the parent field with this new object.
// When used, this will abort the visitation of the current node - no post or children visited,
// and the new node visited.
func (c *Cursor) ReplaceAndRevisit(newNode Node) {
	c.replacer(newNode, c.parent)
	c.node = newNode
	c.revisit = true
}

type replacerFunc func(newNode, parent Node)

// application carries all the shared data so we can pass it around cheaply.
type application struct {
	pre, post ApplyFunc
	cur       Cursor
}
