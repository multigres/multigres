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

package planner

import (
	"strings"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
)

// funcBlocklist lists built-in functions that must be rejected wherever they
// appear in an expression tree. Each one bypasses the pooler's isolation
// model along one of four axes: outbound network connections, server
// filesystem access, server shell execution, or arbitrary SQL execution via
// metadata-query helpers.
//
// Keys are lowercased unqualified function names. Schema-qualified calls
// (e.g. pg_catalog.dblink) resolve to the same entry via resolveFuncName.
var funcBlocklist = map[string]string{
	// Outbound database connections.
	"dblink":           "dblink is not supported: outbound database connections are not permitted through the connection pooler",
	"dblink_exec":      "dblink_exec is not supported: outbound database connections are not permitted through the connection pooler",
	"dblink_connect":   "dblink_connect is not supported: outbound database connections are not permitted through the connection pooler",
	"dblink_connect_u": "dblink_connect_u is not supported: outbound database connections are not permitted through the connection pooler",

	// Server filesystem read.
	"pg_read_file":        "pg_read_file is not supported: server filesystem access is not permitted through the connection pooler",
	"pg_read_binary_file": "pg_read_binary_file is not supported: server filesystem access is not permitted through the connection pooler",
	"pg_ls_dir":           "pg_ls_dir is not supported: server filesystem access is not permitted through the connection pooler",
	"pg_stat_file":        "pg_stat_file is not supported: server filesystem access is not permitted through the connection pooler",

	// Filesystem read/write via large objects.
	"lo_import": "lo_import is not supported: server filesystem access is not permitted through the connection pooler",
	"lo_export": "lo_export is not supported: server filesystem access is not permitted through the connection pooler",

	// Shell execution.
	"pg_execute_server_program": "pg_execute_server_program is not supported: server shell execution is not permitted through the connection pooler",

	// Arbitrary SQL execution via XML helpers.
	"query_to_xml":               "query_to_xml is not supported: arbitrary SQL execution via XML helpers is not permitted through the connection pooler",
	"query_to_xmlschema":         "query_to_xmlschema is not supported: arbitrary SQL execution via XML helpers is not permitted through the connection pooler",
	"query_to_xml_and_xmlschema": "query_to_xml_and_xmlschema is not supported: arbitrary SQL execution via XML helpers is not permitted through the connection pooler",
	"table_to_xml":               "table_to_xml is not supported: arbitrary SQL execution via XML helpers is not permitted through the connection pooler",
	"table_to_xmlschema":         "table_to_xmlschema is not supported: arbitrary SQL execution via XML helpers is not permitted through the connection pooler",
	"table_to_xml_and_xmlschema": "table_to_xml_and_xmlschema is not supported: arbitrary SQL execution via XML helpers is not permitted through the connection pooler",
	"cursor_to_xml":              "cursor_to_xml is not supported: arbitrary SQL execution via XML helpers is not permitted through the connection pooler",
	"cursor_to_xmlschema":        "cursor_to_xmlschema is not supported: arbitrary SQL execution via XML helpers is not permitted through the connection pooler",
}

// setConfigCall is one `set_config(name, value, is_local=false)` call the
// planner accepted as a tracked session-state update. The planner mints one
// per allowed position and uses the list to build the execution plan.
//
// Only is_local=false calls appear here: is_local=true is transaction-scoped,
// PG handles it directly, and there is nothing for the pooler to track.
type setConfigCall struct {
	Name  string
	Value string
}

// expressionCheckResult carries the output of inspectExpressionFuncCalls.
type expressionCheckResult struct {
	// SetConfigs are the set_config calls the planner accepted — i.e., they
	// appear in an allowed position (directly as a SelectStmt target-list
	// entry), with literal arguments and is_local=false. Ordering matches
	// the target-list positions left-to-right.
	SetConfigs []setConfigCall
}

// planUnsupportedConstructs runs the two pre-dispatch rejection checks that
// every planning path (simple `Plan()` and extended-protocol `PlanPortal()`)
// must apply: unsupported statement types (Tier 2) and the expression-level
// walker (blocklist + rogue set_config). Returns the accepted set_config
// calls for Plan's SELECT dispatch; PlanPortal ignores that result.
//
// Centralizing the pair here is the point — earlier versions called only
// planUnsupportedStmt from PlanPortal and silently let blocklisted function
// calls through on non-cacheable extended-protocol paths.
func planUnsupportedConstructs(stmt ast.Stmt) (*expressionCheckResult, error) {
	if err := planUnsupportedStmt(stmt); err != nil {
		return nil, err
	}
	return inspectExpressionFuncCalls(stmt)
}

// inspectExpressionFuncCalls walks every FuncCall in stmt and either:
//   - returns an error to reject the statement (blocklisted function call,
//     or a set_config in a disallowed position / with unsafe arguments), or
//   - returns a result describing any accepted set_config calls that the
//     caller should turn into session-state tracking.
//
// "Allowed position" for set_config means: the call sits directly as the
// Val of a ResTarget in the top-level SelectStmt's TargetList. That covers
// the two forms we want to support:
//
//	SELECT set_config('x', 'y', false)                  -- bare
//	SELECT set_config('x', 'y', false), * FROM t        -- mixed with a read
//
// Anything else — set_config buried inside another expression, a subquery,
// a CTE, a DEFAULT, a WHERE clause — gets rejected; the call's execution
// semantics in those positions (conditional evaluation, multiple-times
// evaluation, etc.) cannot be faithfully represented by a SET.
//
// Runs BEFORE statement-type dispatch but AFTER normalization — by the time
// we see stmt, non-set_config literals have become ParamRefs. The
// normalizer is configured to skip inside set_config's args so those
// remain A_Const here.
func inspectExpressionFuncCalls(stmt ast.Stmt) (*expressionCheckResult, error) {
	if stmt == nil {
		return &expressionCheckResult{}, nil
	}

	result := &expressionCheckResult{}
	allowedSetConfigs := collectTopLevelSetConfigs(stmt)

	var walkErr error
	ast.Rewrite(stmt, func(cursor *ast.Cursor) bool {
		if walkErr != nil {
			return false
		}
		fc, ok := cursor.Node().(*ast.FuncCall)
		if !ok {
			return true
		}
		name := resolveFuncName(fc.Funcname)
		if name == "" {
			return true
		}
		if msg, blocked := funcBlocklist[name]; blocked {
			walkErr = mterrors.NewFeatureNotSupported(msg)
			return false
		}
		if name != "set_config" {
			return true
		}

		if _, isAllowed := allowedSetConfigs[fc]; !isAllowed {
			walkErr = mterrors.NewFeatureNotSupported(
				"set_config is only supported as a top-level SELECT target list entry — use a SET statement, or set_config(..., true) for a transaction-scoped change")
			return false
		}

		setCfg, err := validateAcceptedSetConfig(fc)
		if err != nil {
			walkErr = err
			return false
		}
		if setCfg != nil {
			result.SetConfigs = append(result.SetConfigs, *setCfg)
		}
		// else is_local=true: leave it alone; PG executes it as a normal
		// transaction-scoped call and the pooler does not track it.
		return true
	}, nil)

	if walkErr != nil {
		return nil, walkErr
	}
	return result, nil
}

// collectTopLevelSetConfigs returns the set of FuncCall pointers that occupy
// an allowed position for set_config — "directly as the Val of a ResTarget
// in the top-level SelectStmt's TargetList". The identity set is used by
// inspectExpressionFuncCalls to distinguish allowed calls from the same
// function name appearing in a WHERE clause or subquery.
//
// This does NOT recurse into WithClause CTEs or set-operation subqueries:
// a CTE's target list isn't "top-level" for the outer statement. Only the
// outermost SelectStmt in simple form qualifies.
func collectTopLevelSetConfigs(stmt ast.Stmt) map[*ast.FuncCall]struct{} {
	allowed := make(map[*ast.FuncCall]struct{})
	ss, ok := stmt.(*ast.SelectStmt)
	if !ok || ss.Op != ast.SETOP_NONE {
		return allowed
	}
	if ss.TargetList == nil {
		return allowed
	}
	for _, item := range ss.TargetList.Items {
		rt, ok := item.(*ast.ResTarget)
		if !ok {
			continue
		}
		fc, ok := rt.Val.(*ast.FuncCall)
		if !ok {
			continue
		}
		if resolveFuncName(fc.Funcname) != "set_config" {
			continue
		}
		allowed[fc] = struct{}{}
	}
	return allowed
}

// validateAcceptedSetConfig verifies that an allowed-position set_config
// call has the expected literal arguments, and returns its (name, value)
// when is_local=false. Returns (nil, nil) when is_local=true — allowed but
// not tracked.
func validateAcceptedSetConfig(fc *ast.FuncCall) (*setConfigCall, error) {
	name, value, isLocal, ok := parseSetConfigArgs(fc)
	if !ok {
		return nil, mterrors.NewFeatureNotSupported(
			"set_config with non-literal arguments is not supported through the connection pooler")
	}
	if isLocal {
		return nil, nil
	}
	return &setConfigCall{Name: name, Value: value}, nil
}

// resolveFuncName returns the lowercased built-in name targeted by funcname,
// or "" if the call does not resolve to a built-in we care about.
//
// PostgreSQL's parser represents `set_config(...)` as a one-element name list
// and `pg_catalog.set_config(...)` as a two-element list; both target the
// same built-in, so the blocklist must fire on both. Calls schema-qualified
// to anything other than pg_catalog are user-defined and out of scope here.
func resolveFuncName(funcname *ast.NodeList) string {
	if funcname == nil {
		return ""
	}
	switch funcname.Len() {
	case 1:
		return lowerStringNode(funcname.Items[0])
	case 2:
		schema := lowerStringNode(funcname.Items[0])
		if schema != "pg_catalog" {
			return ""
		}
		return lowerStringNode(funcname.Items[1])
	}
	return ""
}

// lowerStringNode returns the lowercased value if n is a *ast.String, or ""
// otherwise. FuncCall.Funcname items are always *ast.String in a well-formed
// parse tree.
func lowerStringNode(n ast.Node) string {
	s, ok := n.(*ast.String)
	if !ok {
		return ""
	}
	return strings.ToLower(s.SVal)
}

// parseSetConfigArgs validates that fc matches the signature
//
//	set_config(name text, new_value text, is_local bool)
//
// with all three arguments as literal constants, and extracts them. Any
// deviation (wrong arity, non-literal arg, wrong literal type) yields
// ok=false; callers then reject or fall through as appropriate.
func parseSetConfigArgs(fc *ast.FuncCall) (name, value string, isLocal, ok bool) {
	if fc.Args == nil || fc.Args.Len() != 3 {
		return "", "", false, false
	}
	name, ok = constStringArg(fc.Args.Items[0])
	if !ok {
		return "", "", false, false
	}
	value, ok = constStringArg(fc.Args.Items[1])
	if !ok {
		return "", "", false, false
	}
	isLocal, ok = constBoolArg(fc.Args.Items[2])
	if !ok {
		return "", "", false, false
	}
	return name, value, isLocal, true
}

// constStringArg returns the underlying string value if n is a string-valued
// A_Const literal. PostgreSQL parses `'foo'` as A_Const{Val: String{"foo"}}.
func constStringArg(n ast.Node) (string, bool) {
	c, ok := n.(*ast.A_Const)
	if !ok || c.Isnull {
		return "", false
	}
	s, ok := c.Val.(*ast.String)
	if !ok {
		return "", false
	}
	return s.SVal, true
}

// constBoolArg returns the underlying boolean value if n is a boolean-valued
// A_Const literal. PostgreSQL parses `true`/`false` as A_Const{Val: Boolean}.
func constBoolArg(n ast.Node) (bool, bool) {
	c, ok := n.(*ast.A_Const)
	if !ok || c.Isnull {
		return false, false
	}
	b, ok := c.Val.(*ast.Boolean)
	if !ok {
		return false, false
	}
	return b.BoolVal, true
}
