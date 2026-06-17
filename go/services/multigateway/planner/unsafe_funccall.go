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
	"slices"
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
//     are nil. is_local was literal false (literal true short-circuits
//     below and never produces a setConfigCall).
//   - any-bound: at least one of NameBind/ValueBind/IsLocalBind is non-nil
//     and points at the parser-produced ParamRef for that slot. Name/Value
//     hold the parsed string for any slot that was NOT bound. The planner
//     emits the deferred-resolution primitive that decodes the bound slots
//     from the portal at execute time.
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
}

func (sc setConfigCall) hasBoundParams() bool {
	return sc.NameBind != nil || sc.ValueBind != nil || sc.IsLocalBind != nil
}

// sessionAdvisoryLockAcquireFuncs is the set of built-in functions that acquire
// a SESSION-level advisory lock. Holding one of these locks pins the backend to
// the client session: the lock lives on the backend that ran the call and
// survives transaction boundaries, so the gateway must keep routing the session
// to that backend until the lock is released.
//
// The transaction-scoped variants (pg_advisory_xact_lock, ...) are deliberately
// excluded — those locks are released at transaction end and never outlive a
// pooled backend's involvement in the session, so they need no pinning.
var sessionAdvisoryLockAcquireFuncs = map[string]struct{}{
	"pg_advisory_lock":            {},
	"pg_advisory_lock_shared":     {},
	"pg_try_advisory_lock":        {},
	"pg_try_advisory_lock_shared": {},
}

// sessionAdvisoryLockReleaseFuncs is the set of built-in functions that release
// a SESSION-level advisory lock. Seeing one of these is the signal to re-probe
// pg_locks and unpin if the session no longer holds any advisory lock —
// PostgreSQL is still the authority on the reference count, this just decides
// when to ask. pg_advisory_unlock / _shared are reference-counted single-key
// releases; pg_advisory_unlock_all drops everything. The transaction-scoped
// variants are excluded for the same reason as the acquire set.
var sessionAdvisoryLockReleaseFuncs = map[string]struct{}{
	"pg_advisory_unlock":        {},
	"pg_advisory_unlock_shared": {},
	"pg_advisory_unlock_all":    {},
}

// statementAnalysis carries the result of analyzing a statement before
// dispatch: the planning signals gathered from its expression tree (which
// set_config calls to track, whether it acquires a session-level advisory
// lock). Rejection of unsupported constructs happens as part of the same pass
// and surfaces as an error rather than a field here.
type statementAnalysis struct {
	// SetConfigs are the set_config calls the planner accepted — i.e., they
	// appear in an allowed position (directly as a SelectStmt target-list
	// entry), with literal arguments and is_local=false. Ordering matches
	// the target-list positions left-to-right.
	SetConfigs []setConfigCall

	// DynamicSetConfig is true when the statement is a SELECT whose target
	// list is entirely set_config(...) calls and at least one call has an
	// argument that can't be resolved at plan time (a column reference or
	// other expression rather than a literal or bound parameter) — the shape
	// pg_dump uses on PG17+:
	//
	//	SELECT set_config(name, 'view, foreign-table', false)
	//	FROM pg_settings WHERE name = 'restrict_nonsystem_relation_kind'
	//
	// We can't mint a literal SET to track up front, so the planner emits a
	// ResolveTrackSetConfig primitive that executes the argument projection
	// once to learn the concrete (name, value, is_local) tuples, tracks the
	// session-scoped ones, and applies them with literals. When this is set,
	// SetConfigs is empty — every call in the target list is handled by the
	// resolve path. See planResolveSetConfig.
	DynamicSetConfig bool

	// AcquiresSessionAdvisoryLock is true if any FuncCall in the statement is a
	// session-level advisory lock acquisition (see
	// sessionAdvisoryLockAcquireFuncs). The planner uses this to route the
	// statement through a reserved connection with ReasonSessionAdvisoryLock so
	// the backend is pinned for the lifetime of the lock.
	//
	// This is best-effort: it catches advisory locks taken directly in the
	// statement text (the overwhelming common case). Locks acquired indirectly —
	// inside a PL/pgSQL function body, a trigger, or dynamic SQL the parser
	// can't see — are not detected here and remain a pre-existing pooling
	// limitation, the same way temp tables created via dynamic SQL are.
	AcquiresSessionAdvisoryLock bool

	// ReleasesSessionAdvisoryLock is true if any FuncCall in the statement is a
	// session-level advisory unlock (see sessionAdvisoryLockReleaseFuncs). It's
	// the signal that the multipooler should re-probe pg_locks after this
	// statement and unpin the backend if no advisory lock remains. Same
	// best-effort caveat as AcquiresSessionAdvisoryLock: an unlock hidden in a
	// function body or dynamic SQL isn't seen here, so the session stays pinned
	// (conservatively) until the next observed advisory statement, DISCARD ALL,
	// or disconnect — never a leak.
	ReleasesSessionAdvisoryLock bool
}

// analyzeStatement is the single pre-dispatch analysis pass that `Plan()`
// applies on both the simple and extended-protocol paths. It does two things in
// one place:
//
//   - Rejects unsupported constructs: Tier 2 statement types (LOAD, ALTER
//     SYSTEM, CREATE/DROP DATABASE, ...), changes to cluster-managed GUCs (the
//     restricted-GUC guard, covering SET / ALTER ROLE / ALTER DATABASE and
//     set_config on those same GUCs), and blocklisted or misplaced FuncCalls in
//     expression trees. These surface as an error.
//   - Gathers planning signals from the expression tree (accepted set_config
//     calls, session-level advisory-lock acquisition) into statementAnalysis,
//     which the routing builders fold into the plan.
//
// This mirrors how Vitess separates Normalize (runs on every query, builds the
// cache key) from semantic Analyze (runs only when a query is actually being
// planned): the normalizer stays policy-free, and everything that depends on
// gateway routing policy lives here, on the cache-miss planning path.
//
// Centralizing both concerns here is the point — earlier versions ran only a
// statement-type rejection on the extended-protocol path and silently let
// blocklisted function calls through on non-cacheable portal queries.
func analyzeStatement(stmt ast.Stmt) (*statementAnalysis, error) {
	if err := rejectUnsupportedStatement(stmt); err != nil {
		return nil, err
	}
	if err := checkRestrictedGUCChange(stmt); err != nil {
		return nil, err
	}
	return analyzeFunctionCalls(stmt)
}

// analyzeFunctionCalls walks every FuncCall in stmt and either:
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
func analyzeFunctionCalls(stmt ast.Stmt) (*statementAnalysis, error) {
	if stmt == nil {
		return &statementAnalysis{}, nil
	}

	result := &statementAnalysis{}
	allowedSetConfigs := collectTopLevelSetConfigs(stmt)

	// accepted collects the set_config calls that sit in an allowed position,
	// in target-list order. We validate them after the walk so we can first
	// decide between the literal/bound fast path and the resolve-and-apply
	// path for dynamic arguments — a decision that depends on the whole
	// target list, not a single call.
	var accepted []*ast.FuncCall
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
		if _, isAdvisory := sessionAdvisoryLockAcquireFuncs[name]; isAdvisory {
			result.AcquiresSessionAdvisoryLock = true
			// Keep walking: a statement can mix an advisory lock with other
			// calls we still need to inspect (e.g. a blocklisted function).
			return true
		}
		if _, isUnlock := sessionAdvisoryLockReleaseFuncs[name]; isUnlock {
			result.ReleasesSessionAdvisoryLock = true
			return true
		}
		if name != "set_config" {
			return true
		}

		if _, isAllowed := allowedSetConfigs[fc]; !isAllowed {
			walkErr = mterrors.NewFeatureNotSupported(
				"set_config is only supported as a top-level SELECT target list entry — use a SET statement, or set_config(..., true) for a transaction-scoped change")
			return false
		}
		accepted = append(accepted, fc)
		return true
	}, nil)

	if walkErr != nil {
		return nil, walkErr
	}

	// Resolve-and-apply path: the whole target list is set_config(...) and at
	// least one call has a non-literal, non-bound argument the literal/bound
	// fast path can't track (pg_dump's column-reference name). Hand the
	// statement to the planner's ResolveTrackSetConfig primitive rather than
	// erroring in validateAcceptedSetConfig.
	if targetListAllSetConfig(stmt, allowedSetConfigs) && slices.ContainsFunc(accepted, setConfigNeedsDynamic) {
		// A cluster-managed GUC is still rejected when the name is a literal;
		// a dynamic name is a documented gap (matches validateAcceptedSetConfig).
		for _, fc := range accepted {
			if name, ok := constStringArg(fc.Args.Items[0]); ok {
				if err := restrictedGUCError(name); err != nil {
					return nil, err
				}
			}
		}
		result.DynamicSetConfig = true
		return result, nil
	}

	for _, fc := range accepted {
		setCfg, err := validateAcceptedSetConfig(fc)
		if err != nil {
			return nil, err
		}
		if setCfg != nil {
			result.SetConfigs = append(result.SetConfigs, *setCfg)
		}
		// else is_local=true: leave it alone; PG executes it as a normal
		// transaction-scoped call and the pooler does not track it.
	}
	return result, nil
}

// collectTopLevelSetConfigs returns the set of FuncCall pointers that occupy
// an allowed position for set_config — "directly as the Val of a ResTarget
// in the top-level SelectStmt's TargetList". The identity set is used by
// analyzeFunctionCalls to distinguish allowed calls from the same
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

// targetListAllSetConfig reports whether every entry in stmt's top-level
// target list is one of the allowed set_config calls — i.e. the SELECT does
// nothing but set_config(...). Only this shape takes the resolve-and-apply
// path: the projection that resolves the arguments (each call's args become
// output columns) must not have to also compute unrelated columns, and the
// synthesized apply query reproduces exactly the original's columns.
func targetListAllSetConfig(stmt ast.Stmt, allowed map[*ast.FuncCall]struct{}) bool {
	ss, ok := stmt.(*ast.SelectStmt)
	if !ok || ss.TargetList == nil || ss.TargetList.Len() == 0 {
		return false
	}
	for _, item := range ss.TargetList.Items {
		rt, ok := item.(*ast.ResTarget)
		if !ok {
			return false
		}
		fc, ok := rt.Val.(*ast.FuncCall)
		if !ok {
			return false
		}
		if _, isAllowed := allowed[fc]; !isAllowed {
			return false
		}
	}
	return true
}

// setConfigNeedsDynamic reports whether fc is a set_config call the literal/
// bound fast path cannot handle — i.e. it would otherwise error in
// validateAcceptedSetConfig. That is: it has exactly three arguments, is_local
// is not a literal true (those run transaction-scoped via Route and need no
// tracking — validateAcceptedSetConfig short-circuits them), and at least one
// argument is neither a literal constant nor a bound parameter (a column
// reference or other expression).
func setConfigNeedsDynamic(fc *ast.FuncCall) bool {
	if fc.Args == nil || fc.Args.Len() != 3 {
		// Wrong arity: let validateAcceptedSetConfig raise its specific error.
		return false
	}
	if isLocal, ok := constBoolArg(fc.Args.Items[2]); ok && isLocal {
		return false
	}
	for _, arg := range fc.Args.Items {
		if !isStaticSetConfigArg(arg) {
			return true
		}
	}
	return false
}

// isStaticSetConfigArg reports whether a set_config argument can be resolved
// at plan time: a literal A_Const or a bound parameter (ParamRef), after
// stripping any TypeCast. Anything else (a column reference, function call,
// operator expression, ...) must be evaluated by PostgreSQL.
func isStaticSetConfigArg(n ast.Node) bool {
	switch unwrapTypeCast(n).(type) {
	case *ast.ParamRef, *ast.A_Const:
		return true
	}
	return false
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

	// Reject set_config targeting a cluster-managed GUC regardless of
	// is_local — it is just another reachable path for the override blocked in
	// checkRestrictedGUCChange. The normalizer keeps the name literal (see
	// normalizer.go) so we can read it here on the cached and is_local=true
	// paths too. A bound or otherwise non-literal name is a documented gap: we
	// let it through rather than reject blindly.
	if name, ok := constStringArg(fc.Args.Items[0]); ok {
		if err := restrictedGUCError(name); err != nil {
			return nil, err
		}
	}

	sc := &setConfigCall{}

	if pr, isParam := unwrapTypeCast(fc.Args.Items[2]).(*ast.ParamRef); isParam {
		sc.IsLocalBind = pr
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
	} else if name, ok := constStringArg(fc.Args.Items[0]); ok {
		sc.Name = name
	} else {
		return nil, setConfigArgError(fc.Args.Items[0], "name")
	}

	if pr, isParam := unwrapTypeCast(fc.Args.Items[1]).(*ast.ParamRef); isParam {
		sc.ValueBind = pr
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
