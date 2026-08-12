// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ast

import "strings"

// ReplicationSlotFuncTemporaryArgIndex maps the pg_create_*_replication_slot
// builtins to the zero-based argument index of their `temporary` parameter:
//
//	pg_create_physical_replication_slot(slot_name, immediately_reserve DEFAULT false, temporary DEFAULT false)
//	pg_create_logical_replication_slot(slot_name, plugin, temporary DEFAULT false, twophase DEFAULT false)
//
// This is the single source of truth for both enforcement points that reject
// non-temporary replication slot creation: the planner's function-call check
// (go/services/multigateway/planner/unsafe_funccall.go) and
// FindNonTemporaryReplicationSlotCall below, used by the multigateway
// replication preamble to catch these calls when they arrive as arbitrary
// SQL over a replication=database connection. The two call sites can't share
// more than this map — the planner imports the handler package (for
// gateway-managed-variable lookups), so the handler can't import the planner
// back without an import cycle.
var ReplicationSlotFuncTemporaryArgIndex = map[string]int{
	"pg_create_physical_replication_slot": 2,
	"pg_create_logical_replication_slot":  2,
}

// FindNonTemporaryReplicationSlotCall walks stmt for a call to one of
// ReplicationSlotFuncTemporaryArgIndex's functions whose `temporary`
// argument is missing, non-literal, or false, and returns that function's
// name. Returns ("", false) if stmt contains no such call. Fails closed,
// matching the planner's own check: a missing, bound, or non-literal
// argument is treated the same as an explicit `false`.
func FindNonTemporaryReplicationSlotCall(stmt Stmt) (string, bool) {
	if stmt == nil {
		return "", false
	}
	var found string
	Rewrite(stmt, func(cursor *Cursor) bool {
		if found != "" {
			return false
		}
		fc, ok := cursor.Node().(*FuncCall)
		if !ok {
			return true
		}
		name := replicationSlotFuncCallName(fc.Funcname)
		temporaryArgIndex, isReplicationSlotFunc := ReplicationSlotFuncTemporaryArgIndex[name]
		if !isReplicationSlotFunc {
			return true
		}
		if fc.Args != nil && fc.Args.Len() > temporaryArgIndex {
			if isTemp, ok := literalBoolArg(fc.Args.Items[temporaryArgIndex]); ok && isTemp {
				return true
			}
		}
		found = name
		return false
	}, nil)
	return found, found != ""
}

// replicationSlotFuncCallName resolves a FuncCall's name for comparison
// against ReplicationSlotFuncTemporaryArgIndex, handling both the bare and
// pg_catalog-qualified forms.
func replicationSlotFuncCallName(funcname *NodeList) string {
	if funcname == nil {
		return ""
	}
	switch funcname.Len() {
	case 1:
		return stringNodeValue(funcname.Items[0])
	case 2:
		if stringNodeValue(funcname.Items[0]) != "pg_catalog" {
			return ""
		}
		return stringNodeValue(funcname.Items[1])
	}
	return ""
}

// stringNodeValue returns the lowercased value if n is a *String, or "" for
// anything else. FuncCall.Funcname items are always *String in a
// well-formed parse tree.
func stringNodeValue(n Node) string {
	s, ok := n.(*String)
	if !ok {
		return ""
	}
	return strings.ToLower(s.SVal)
}

// literalBoolArg reports whether n is a literal A_Const (after stripping any
// TypeCast) that unambiguously spells a boolean, for checking the
// `temporary` argument of a replication-slot-creating call.
//
// go/common/parser must be usable as a standalone library (see the
// parser-isolation lint rule), so this can't depend on
// go/common/sqltypes.ParseBool — unlike the planner's equivalent check
// (constBoolArg in unsafe_funccall.go), it doesn't replicate postgres's full
// unique-prefix boolean parsing (parse_bool_with_len), only exact
// case-insensitive matches on the common spellings. That's strictly more
// conservative, never less: an unrecognized spelling here is treated as
// non-literal by the caller and rejected, which is the safe direction for a
// check whose job is to fail closed.
func literalBoolArg(n Node) (isTrue bool, isLiteralBool bool) {
	tc, ok := n.(*TypeCast)
	for ok {
		n = tc.Arg
		tc, ok = n.(*TypeCast)
	}
	c, ok := n.(*A_Const)
	if !ok || c.Isnull {
		return false, false
	}
	switch v := c.Val.(type) {
	case *Boolean:
		return v.BoolVal, true
	case *String:
		switch strings.ToLower(strings.TrimSpace(v.SVal)) {
		case "true", "t", "yes", "y", "on", "1":
			return true, true
		case "false", "f", "no", "n", "off", "0":
			return false, true
		}
	}
	return false, false
}
