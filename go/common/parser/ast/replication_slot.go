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

// ReplicationSlotFuncFailoverArgIndex maps the logical slot builtin to the
// zero-based index of its `failover` parameter:
//
//	pg_create_logical_replication_slot(slot_name, plugin, temporary DEFAULT false, twophase DEFAULT false, failover DEFAULT false)
//
// Only the logical function has one — a physical slot cannot be a logical
// failover slot — so the physical builtin is deliberately absent.
var ReplicationSlotFuncFailoverArgIndex = map[string]int{
	"pg_create_logical_replication_slot": 4,
}

// ReplicationSlotTemporaryParamName and ReplicationSlotFailoverParamName are
// the pg_create_*_replication_slot builtins' parameter names, for resolving
// a call that passes the argument by name (temporary => true) rather than
// position. Both builtins name their temporary parameter "temporary"; only
// the logical one has "failover".
const (
	ReplicationSlotTemporaryParamName = "temporary"
	ReplicationSlotFailoverParamName  = "failover"
)

// FindNonTemporaryReplicationSlotCall walks stmt for a call to one of
// ReplicationSlotFuncTemporaryArgIndex's functions whose `temporary`
// argument is missing, non-literal, or false, and returns that function's
// name. Returns ("", false) if stmt contains no such call. Fails closed,
// matching the planner's own check: a missing, bound, or non-literal
// argument is treated the same as an explicit `false`.
func FindNonTemporaryReplicationSlotCall(stmt Stmt) (string, bool) {
	return findRejectableReplicationSlotCall(stmt, isTemporaryReplicationSlotCall)
}

// FindNonTemporaryNonFailoverReplicationSlotCall is like
// FindNonTemporaryReplicationSlotCall but additionally admits a non-temporary
// logical slot created with `failover => true`. Such a slot is safe to keep
// persistently because multigres syncs it to the standbys and can transition
// it across a primary failover. It returns the name of a replication-slot-
// creating call that is neither temporary nor an admissible failover slot, or
// ("", false) if none. Used by the multigateway replication preamble once the
// slot-based-replication feature is enabled.
func FindNonTemporaryNonFailoverReplicationSlotCall(stmt Stmt) (string, bool) {
	return findRejectableReplicationSlotCall(stmt, func(name string, fc *FuncCall) bool {
		return isTemporaryReplicationSlotCall(name, fc) || isFailoverReplicationSlotCall(name, fc)
	})
}

// findRejectableReplicationSlotCall walks stmt for a replication-slot-creating
// builtin that admissible reports false for, returning the first such call's
// name. admissible receives the resolved (lower-cased, pg_catalog-stripped)
// function name and its FuncCall and returns true when the call is allowed.
func findRejectableReplicationSlotCall(stmt Stmt, admissible func(name string, fc *FuncCall) bool) (string, bool) {
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
		if _, isReplicationSlotFunc := ReplicationSlotFuncTemporaryArgIndex[name]; !isReplicationSlotFunc {
			return true
		}
		if admissible(name, fc) {
			return true
		}
		found = name
		return false
	}, nil)
	return found, found != ""
}

// isTemporaryReplicationSlotCall reports whether fc's `temporary` argument is a
// literal true. Fails closed: a missing, bound, or non-literal argument reads
// as not-temporary.
func isTemporaryReplicationSlotCall(name string, fc *FuncCall) bool {
	return literalBoolArgAt(fc, ReplicationSlotFuncTemporaryArgIndex[name], ReplicationSlotTemporaryParamName)
}

// isFailoverReplicationSlotCall reports whether fc's `failover` argument is a
// literal true. Only the logical builtin has a failover parameter; for any
// other function it returns false. Fails closed, like
// isTemporaryReplicationSlotCall.
func isFailoverReplicationSlotCall(name string, fc *FuncCall) bool {
	idx, ok := ReplicationSlotFuncFailoverArgIndex[name]
	if !ok {
		return false
	}
	return literalBoolArgAt(fc, idx, ReplicationSlotFailoverParamName)
}

// literalBoolArgAt reports whether fc's paramName parameter (see FuncCallArg)
// is a literal boolean true.
func literalBoolArgAt(fc *FuncCall, positionalIndex int, paramName string) bool {
	arg, given := FuncCallArg(fc, positionalIndex, paramName)
	if !given {
		return false
	}
	isTrue, ok := literalBoolArg(arg)
	return ok && isTrue
}

// FuncCallArg resolves fc's parameter at positionalIndex, however the caller
// passed it: positionally or with name => value syntax (parsed as a
// *NamedArgExpr). given reports whether the parameter was specified at all;
// when false, the caller gave neither form and the parameter's default
// applies.
//
// Shared by this file's own literalBoolArgAt and the planner's equivalent
// check (funcCallBoolArg in
// go/services/multigateway/planner/unsafe_funccall.go) so both enforcement
// points resolve an argument's position identically and can only differ in
// how they parse the resolved node as a literal — literalBoolArg's doc
// comment explains why that part can't be shared too.
//
// The raw parser doesn't validate that named arguments only follow
// positional ones — PostgreSQL enforces that during catalog-aware semantic
// analysis, which this codebase (parser-only, no catalog) never performs —
// so this scans every item once rather than assuming a fixed layout: each
// non-named item advances a running positional counter, and a NamedArgExpr
// is matched by name regardless of where it appears.
func FuncCallArg(fc *FuncCall, positionalIndex int, paramName string) (arg Node, given bool) {
	if fc.Args == nil {
		return nil, false
	}
	positional := 0
	for _, item := range fc.Args.Items {
		if named, ok := item.(*NamedArgExpr); ok {
			if named.Name == paramName {
				return named.Arg, true
			}
			continue
		}
		if positional == positionalIndex {
			return item, true
		}
		positional++
	}
	return nil, false
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
