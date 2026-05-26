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
	"strconv"
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
	// Outbound database connections. Cover the full dblink async-cursor
	// surface (dblink_open/_fetch/_close/_send_query/_get_result) as well as
	// the synchronous entry points so a caller cannot smuggle remote queries
	// through the asynchronous variant.
	"dblink":            "dblink is not supported: outbound database connections are not permitted through the connection pooler",
	"dblink_exec":       "dblink_exec is not supported: outbound database connections are not permitted through the connection pooler",
	"dblink_connect":    "dblink_connect is not supported: outbound database connections are not permitted through the connection pooler",
	"dblink_connect_u":  "dblink_connect_u is not supported: outbound database connections are not permitted through the connection pooler",
	"dblink_open":       "dblink_open is not supported: outbound database connections are not permitted through the connection pooler",
	"dblink_fetch":      "dblink_fetch is not supported: outbound database connections are not permitted through the connection pooler",
	"dblink_close":      "dblink_close is not supported: outbound database connections are not permitted through the connection pooler",
	"dblink_send_query": "dblink_send_query is not supported: outbound database connections are not permitted through the connection pooler",
	"dblink_get_result": "dblink_get_result is not supported: outbound database connections are not permitted through the connection pooler",

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

// setConfigCall is one `set_config(name, value, is_local)` call the planner
// accepted as a tracked session-state update. The planner mints one per
// allowed position and uses the list to build the execution plan.
//
// Two shapes:
//   - all-literal: Name and Value carry the parsed strings, *Bind fields
//     are nil, Bound is false. is_local was literal false (literal true
//     short-circuits below and never produces a setConfigCall).
//   - any-bound: at least one of NameBind/ValueBind/IsLocalBind is non-nil
//     and points at the parser-produced ParamRef for that slot. Name/Value
//     hold the parsed string for any slot that was NOT bound. Bound is
//     true — the planner emits the deferred-resolution primitive that
//     decodes the bound slots from the portal at execute time.
//
// `is_local=true` literals never produce a setConfigCall — validation
// returns (nil, nil) early so the call goes to PG via Route only, with no
// SessionSettings write. That's also why we don't need to carry a literal
// is_local: any returned setConfigCall implies is_local was literal false
// or bound, and the executor defaults to false when IsLocalBind is nil.
type setConfigCall struct {
	Name  string
	Value string

	NameBind    *ast.ParamRef
	ValueBind   *ast.ParamRef
	IsLocalBind *ast.ParamRef

	// Bound is true when at least one of *Bind is non-nil. Callers branch
	// on this to choose ApplySessionStateSilent (literal) vs
	// ApplySessionStateFromBind (deferred resolution).
	Bound bool
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
// normalizer skips inside set_config's args when is_local is literal false
// (so the validator can extract the name/value), but allows recursion when
// is_local is literal true (those calls are accepted but not tracked, and
// parameterizing them keeps the plan cache stable for hot patterns).
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
//
// SELECT ... INTO TEMP is also excluded: that shape dispatches to the
// reserved temp-table route, which would silently drop the tracked
// set_config and leave the gateway's session-state tracker stale relative
// to the backend. Rejecting at the walker yields the same "only supported
// as a top-level SELECT target list entry" error users already see for
// other unsupported positions.
func collectTopLevelSetConfigs(stmt ast.Stmt) map[*ast.FuncCall]struct{} {
	allowed := make(map[*ast.FuncCall]struct{})
	ss, ok := stmt.(*ast.SelectStmt)
	if !ok || ss.Op != ast.SETOP_NONE {
		return allowed
	}
	if ss.IntoClause != nil {
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
// call has the expected arguments and builds the setConfigCall the planner
// will turn into a SessionSettings tracking entry. Each slot may be a
// literal A_Const or a *ast.ParamRef — the latter is recorded as a *Bind
// for execute-time resolution from the portal's wire-protocol Bind values.
// Anything else (non-const non-ParamRef expression) errors out.
//
// is_local is inspected first because a literal `true` short-circuits:
// transaction-scoped calls are not tracked at all (PG executes them via
// Route, the gateway holds no state for them), so name/value need not be
// validated and the normalizer is allowed to parameterize their args (see
// isPlannerLiteralFunc / normalizer.go) — keeps the plan-cache fingerprint
// stable for hot patterns like PostgREST's set_config('request.jwt.claims',
// '<dynamic JSON>', true).
//
// A bound is_local cannot be short-circuited at plan time — the decision
// to track is deferred to executeSetWithBinds.
func validateAcceptedSetConfig(fc *ast.FuncCall) (*setConfigCall, error) {
	if fc.Args == nil || fc.Args.Len() != 3 {
		return nil, mterrors.NewFeatureNotSupported(
			"set_config requires three arguments: (name text, value text, is_local bool)")
	}

	sc := &setConfigCall{}

	if pr, isParam := unwrapTypeCast(fc.Args.Items[2]).(*ast.ParamRef); isParam {
		sc.IsLocalBind = pr
		sc.Bound = true
	} else if isLocal, ok := constBoolArg(fc.Args.Items[2]); ok {
		if isLocal {
			return nil, nil
		}
		// is_local literal false: fall through. No field to set — the
		// returned setConfigCall represents false implicitly via the
		// absence of IsLocalBind.
	} else {
		return nil, setConfigArgError(fc.Args.Items[2], "is_local")
	}

	if pr, isParam := unwrapTypeCast(fc.Args.Items[0]).(*ast.ParamRef); isParam {
		sc.NameBind = pr
		sc.Bound = true
	} else if name, ok := constStringArg(fc.Args.Items[0]); ok {
		sc.Name = name
	} else {
		return nil, setConfigArgError(fc.Args.Items[0], "name")
	}

	if pr, isParam := unwrapTypeCast(fc.Args.Items[1]).(*ast.ParamRef); isParam {
		sc.ValueBind = pr
		sc.Bound = true
	} else if value, ok := constStringArg(fc.Args.Items[1]); ok {
		sc.Value = value
	} else {
		return nil, setConfigArgError(fc.Args.Items[1], "value")
	}

	return sc, nil
}

// setConfigArgError builds the user-facing rejection for a set_config
// argument that was neither a literal nor a bound parameter. Bound
// parameters are no longer rejected — they go through the deferred
// resolution path in ApplySessionState. This error fires only on
// expression-shaped args (column refs, function calls, casts of non-const
// values, etc.) which can never be safely tracked.
func setConfigArgError(arg ast.Node, which string) error {
	return mterrors.NewFeatureNotSupported(
		"set_config " + which + " argument must be a literal constant or a bound parameter")
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

// unwrapTypeCast strips any number of TypeCast wrappers from n. PostgreSQL
// parses `'256MB'::text` as TypeCast{Arg: A_Const{String{"256MB"}}}, and
// users routinely write set_config args that way. Stripping the cast lets
// us look through to the literal underneath. Multiple layers (e.g.
// `'t'::text::bool`) are uncommon but handled by looping.
func unwrapTypeCast(n ast.Node) ast.Node {
	for {
		tc, ok := n.(*ast.TypeCast)
		if !ok {
			return n
		}
		n = tc.Arg
	}
}

// constStringArg returns the underlying string value if n is a string- or
// numeric-valued A_Const literal (after stripping any TypeCast). PG parses
// `'foo'` as A_Const{Val: String{"foo"}} and `100` as A_Const{Val:
// Integer{100}}; both are accepted because PG would implicitly cast the
// numeric to text when calling set_config(text, text, bool).
func constStringArg(n ast.Node) (string, bool) {
	c, ok := unwrapTypeCast(n).(*ast.A_Const)
	if !ok || c.Isnull {
		return "", false
	}
	switch v := c.Val.(type) {
	case *ast.String:
		return v.SVal, true
	case *ast.Integer:
		return strconv.Itoa(v.IVal), true
	case *ast.Float:
		return v.FVal, true
	}
	return "", false
}

// constBoolArg returns the underlying boolean value if n is a boolean-valued
// A_Const literal (after stripping any TypeCast). PG parses `true`/`false`
// as A_Const{Val: Boolean}, and `'t'::bool` as TypeCast{A_Const{String}};
// both forms are accepted. The accepted string spellings mirror PG's
// boolin() — t/true/y/yes/on/1 and f/false/n/no/off/0, case-insensitive —
// so users who write set_config(..., 'true') get the natural behavior.
func constBoolArg(n ast.Node) (bool, bool) {
	c, ok := unwrapTypeCast(n).(*ast.A_Const)
	if !ok || c.Isnull {
		return false, false
	}
	switch v := c.Val.(type) {
	case *ast.Boolean:
		return v.BoolVal, true
	case *ast.String:
		switch strings.ToLower(strings.TrimSpace(v.SVal)) {
		case "t", "true", "y", "yes", "on", "1":
			return true, true
		case "f", "false", "n", "no", "off", "0":
			return false, true
		}
	}
	return false, false
}
