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
	"github.com/multigres/multigres/go/services/multigateway/handler"
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
//     are nil. is_local was either literal false or — for a gateway-managed
//     variable only — literal true (see IsLocalLiteralTrue).
//   - any-bound: at least one of NameBind/ValueBind/IsLocalBind is non-nil
//     and points at the ParamRef for that slot (parser-produced for the
//     extended protocol, normalizer-produced for cacheable simple queries).
//     Name/Value hold the parsed string for any slot that was NOT bound.
//     The planner emits the deferred-resolution primitive that resolves the
//     bound slots at execute time — from the portal's wire Bind values or
//     from the normalizer-extracted bindVars, depending on the path.
//
// `is_local=true` literals produce a setConfigCall only for gateway-managed
// variables, tracked as a transaction-local override so SHOW matches the
// `SET LOCAL <gmv>` statement form. For ordinary variables validation
// returns (nil, nil) early and the call goes to PG via Route only, with no
// SessionSettings write. A literal is_local therefore travels as
// IsLocalLiteralTrue; the executor treats is_local as false when both
// IsLocalBind is nil and IsLocalLiteralTrue is false.
type setConfigCall struct {
	Name  string
	Value string

	NameBind    *ast.ParamRef
	ValueBind   *ast.ParamRef
	IsLocalBind *ast.ParamRef

	// IsLocalLiteralTrue marks a call whose is_local argument is the literal
	// `true`. Normally such a call is not tracked (it short-circuits below),
	// but for a gateway-managed variable it is tracked as a transaction-local
	// override so SHOW matches the `SET LOCAL <gmv>` statement form. Mutually
	// exclusive with IsLocalBind (a bound is_local is resolved at execute time).
	IsLocalLiteralTrue bool
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
	// list is entirely set_config(...) calls in the narrow pg_dump PG17+ shape:
	// at least one name argument is pg_settings.name, while value and is_local
	// arguments remain static.
	//
	//	SELECT set_config(name, 'view, foreign-table', false)
	//	FROM pg_settings WHERE name = 'restrict_nonsystem_relation_kind'
	//
	// We can't mint a literal SET to track up front, so the planner emits a
	// ResolveTrackSetConfig primitive that executes the pg_settings projection
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

	// UsesPgListeningChannels is true if the statement calls
	// pg_catalog.pg_listening_channels(). The planner virtualizes only simple
	// supported forms from gateway LISTEN state and rejects complex forms rather
	// than routing them to an arbitrary pooled backend.
	UsesPgListeningChannels bool
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
// (so the validator can extract the name/value), but parameterizes the
// value when is_local is literal true: name and is_local stay literal, so
// the gateway-managed check below still works; ordinary calls go untracked,
// and the collapsed value keeps the plan cache stable for hot patterns.
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
		if name == "pg_listening_channels" {
			result.UsesPgListeningChannels = true
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
	// fast path can't track. This path is intentionally narrow: it exists for
	// pg_dump's PG17+ pg_settings probe, where only the GUC name is dynamic
	// (`pg_settings.name`) and value/is_local remain static. Arbitrary dynamic
	// expressions would be evaluated in a separate resolve statement before the
	// synthesized apply statement, which can break PostgreSQL's single-statement
	// atomicity and argument type checking. Reject those shapes instead of trying
	// to emulate them.
	if targetListAllSetConfig(stmt, allowedSetConfigs) && slices.ContainsFunc(accepted, setConfigNeedsDynamic) {
		if err := validateDynamicSetConfigShape(stmt, accepted); err != nil {
			return nil, err
		}
		// A cluster-managed GUC is still rejected when the name is a literal;
		// the only supported dynamic name is pg_settings.name.
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
		// else is_local=true on an ordinary variable: leave it alone; PG
		// executes it as a normal transaction-scoped call and the pooler
		// does not track it. (Gateway-managed names DO produce a setCfg —
		// see validateAcceptedSetConfig.)
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

// validateDynamicSetConfigShape accepts only the pg_dump-safe dynamic shape:
// every set_config value and is_local argument must be static, and any dynamic
// name must be the name column from pg_settings. This keeps the resolve step a
// side-effect-free catalog read and avoids evaluating arbitrary expressions as
// ordinary SELECT outputs before re-applying them as text literals.
func validateDynamicSetConfigShape(stmt ast.Stmt, accepted []*ast.FuncCall) error {
	ss, ok := stmt.(*ast.SelectStmt)
	if !ok {
		return mterrors.NewFeatureNotSupported("dynamic set_config is only supported in SELECT statements")
	}

	pgSettingsQualifiers, pgSettingsOK := pgSettingsNameQualifiers(ss)
	for _, fc := range accepted {
		if fc.Args == nil || fc.Args.Len() != 3 {
			return mterrors.NewFeatureNotSupported(
				"set_config requires three arguments: (name text, value text, is_local bool)")
		}

		nameArg := fc.Args.Items[0]
		if !isStaticSetConfigArg(nameArg) {
			if !pgSettingsOK || !isPgSettingsNameColumnRef(nameArg, pgSettingsQualifiers) {
				return mterrors.NewFeatureNotSupported(
					"dynamic set_config name argument is only supported for pg_settings.name")
			}
		} else if !isDynamicTextSetConfigArg(nameArg) {
			return mterrors.NewFeatureNotSupported(
				"dynamic set_config name argument must be a text literal, bound text parameter, or pg_settings.name")
		}
		if !isDynamicTextSetConfigArg(fc.Args.Items[1]) {
			if !isStaticSetConfigArg(fc.Args.Items[1]) {
				return setConfigArgError(fc.Args.Items[1], "value")
			}
			return mterrors.NewFeatureNotSupported(
				"dynamic set_config value argument must be a text literal or bound text parameter")
		}
		if !isDynamicBoolSetConfigArg(fc.Args.Items[2]) {
			if !isStaticSetConfigArg(fc.Args.Items[2]) {
				return setConfigArgError(fc.Args.Items[2], "is_local")
			}
			return mterrors.NewFeatureNotSupported(
				"dynamic set_config is_local argument must be a literal boolean")
		}
	}

	acceptedSet := make(map[*ast.FuncCall]struct{}, len(accepted))
	for _, fc := range accepted {
		acceptedSet[fc] = struct{}{}
	}
	var walkErr error
	ast.Rewrite(stmt, func(cursor *ast.Cursor) bool {
		if walkErr != nil {
			return false
		}
		fc, ok := cursor.Node().(*ast.FuncCall)
		if !ok {
			return true
		}
		if _, isSetConfigTarget := acceptedSet[fc]; isSetConfigTarget {
			return true
		}
		walkErr = mterrors.NewFeatureNotSupported(
			"dynamic set_config only supports simple pg_settings lookups; function calls outside set_config are not supported")
		return false
	}, nil)
	return walkErr
}

// pgSettingsNameQualifiers returns the allowed qualifiers for pg_settings.name
// in the current SELECT, plus whether the FROM clause is the simple pg_settings
// scan used by pg_dump. We require a single RangeVar so the resolve projection
// cannot hide side effects in FROM functions or joins.
func pgSettingsNameQualifiers(ss *ast.SelectStmt) (map[string]struct{}, bool) {
	if ss.FromClause == nil || ss.FromClause.Len() != 1 {
		return nil, false
	}
	rv, ok := ss.FromClause.Items[0].(*ast.RangeVar)
	if !ok {
		return nil, false
	}
	if !strings.EqualFold(rv.RelName, "pg_settings") {
		return nil, false
	}
	if rv.CatalogName != "" || (rv.SchemaName != "" && !strings.EqualFold(rv.SchemaName, "pg_catalog")) {
		return nil, false
	}

	qualifiers := map[string]struct{}{"pg_settings": {}}
	if rv.Alias != nil && rv.Alias.AliasName != "" {
		qualifiers[strings.ToLower(rv.Alias.AliasName)] = struct{}{}
	}
	return qualifiers, true
}

func isDynamicTextSetConfigArg(n ast.Node) bool {
	switch c := n.(type) {
	case *ast.TypeCast:
		if !isDynamicTextType(c.TypeName) {
			return false
		}
		return isDynamicTextSetConfigArg(c.Arg)
	case *ast.ParamRef:
		return true
	case *ast.A_Const:
		if c.Isnull {
			return false
		}
		_, ok := c.Val.(*ast.String)
		return ok
	default:
		return false
	}
}

func isDynamicBoolSetConfigArg(n ast.Node) bool {
	switch c := n.(type) {
	case *ast.TypeCast:
		if !isDynamicBoolType(c.TypeName) {
			return false
		}
		return isDynamicBoolSetConfigArg(c.Arg)
	case *ast.A_Const:
		if c.Isnull {
			return false
		}
		switch v := c.Val.(type) {
		case *ast.Boolean:
			return true
		case *ast.String:
			switch strings.ToLower(strings.TrimSpace(v.SVal)) {
			case "t", "true", "y", "yes", "on", "1",
				"f", "false", "n", "no", "off", "0":
				return true
			}
		}
		return false
	default:
		return false
	}
}

func isDynamicTextType(typeName *ast.TypeName) bool {
	switch dynamicTypeNameOID(typeName) {
	case ast.TEXTOID, ast.VARCHAROID:
		return true
	}
	return false
}

func isDynamicBoolType(typeName *ast.TypeName) bool {
	return dynamicTypeNameOID(typeName) == ast.BOOLOID
}

func dynamicTypeNameOID(typeName *ast.TypeName) ast.Oid {
	if typeName == nil {
		return ast.InvalidOid
	}
	if typeName.TypeOid != ast.InvalidOid {
		return typeName.TypeOid
	}
	if typeName.Names == nil || typeName.Names.Len() == 0 {
		return ast.InvalidOid
	}
	parts := make([]string, 0, typeName.Names.Len())
	for _, item := range typeName.Names.Items {
		name := lowerStringNode(item)
		if name == "" {
			return ast.InvalidOid
		}
		if name != "pg_catalog" {
			parts = append(parts, name)
		}
	}
	if len(parts) == 0 {
		return ast.InvalidOid
	}
	return ast.TypeNameToOid(strings.Join(parts, " "))
}

func isPgSettingsNameColumnRef(n ast.Node, qualifiers map[string]struct{}) bool {
	if tc, ok := n.(*ast.TypeCast); ok {
		if !isDynamicTextType(tc.TypeName) {
			return false
		}
		n = tc.Arg
	}
	ref, ok := n.(*ast.ColumnRef)
	if !ok || ref.Fields == nil {
		return false
	}
	parts := make([]string, 0, ref.Fields.Len())
	for _, field := range ref.Fields.Items {
		s, ok := field.(*ast.String)
		if !ok {
			return false
		}
		parts = append(parts, strings.ToLower(s.SVal))
	}
	switch len(parts) {
	case 1:
		return parts[0] == "name"
	case 2:
		_, ok := qualifiers[parts[0]]
		return ok && parts[1] == "name"
	default:
		return false
	}
}

// validateAcceptedSetConfig verifies that an allowed-position set_config
// call has the expected arguments and builds the setConfigCall the planner
// will turn into a SessionSettings tracking entry. Each slot may be a
// literal A_Const or a *ast.ParamRef — the latter is recorded as a *Bind
// for execute-time resolution from the portal's wire-protocol Bind values.
// Anything else (non-const non-ParamRef expression) errors out.
//
// is_local is inspected first because a literal `true` usually
// short-circuits: transaction-scoped calls on ordinary variables are not
// tracked at all (PG executes them via Route, the gateway holds no state
// for them), so name/value need not be validated and the normalizer is
// allowed to parameterize their value (see isPlannerLiteralFunc /
// normalizer.go) — keeps the plan-cache fingerprint stable for hot patterns
// like PostgREST's set_config('request.jwt.claims', '<dynamic JSON>', true).
// Gateway-managed variables are the exception: literal-true IS tracked
// (IsLocalLiteralTrue) so SHOW matches SET LOCAL, and the parameterized
// value is resolved from the execution's bindVars at execute time.
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
			// is_local literal true. For an ordinary variable we do not track
			// it: PostgreSQL executes the call transaction-scoped via the
			// paired Route and the gateway holds no state (which also keeps
			// the plan cache compact for hot PostgREST set_config(...,true)
			// patterns). For a gateway-managed variable we DO track it as a
			// transaction-local override, so SHOW matches the `SET LOCAL <gmv>`
			// statement form. The normalizer keeps the name literal even on the
			// is_local=true path, so the GMV check below is reliable.
			if name, ok := constStringArg(fc.Args.Items[0]); !ok || !handler.IsGatewayManagedVariable(name) {
				return nil, nil
			}
			sc.IsLocalLiteralTrue = true
		}
		// is_local literal false: fall through. No field to set — the
		// returned setConfigCall represents false implicitly via the
		// absence of IsLocalBind and IsLocalLiteralTrue.
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
