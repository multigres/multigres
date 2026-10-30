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
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgsettings"
	"github.com/multigres/multigres/go/common/sqltypes"
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

// replicationSlotFuncs lists the pg_create_*_replication_slot builtins and
// the zero-based argument index of their `temporary` parameter. Outside a
// TEMPORARY (session-scoped) slot, only a logical slot registered for
// failover (see ast.ReplicationSlotFuncFailoverArgIndex) is safe, and only
// once slot-based replication is enabled — see
// rejectNonTemporaryReplicationSlot. This is the same map
// ast.FindNonTemporaryReplicationSlotCall uses for the equivalent check on
// arbitrary SQL sent over a replication=database connection (see
// go/services/multigateway/handler/replication_preamble.go) — kept as a
// single shared definition in the ast package (this package can't import
// handler's checks back, since planner already imports handler for
// gateway-managed-variable lookups) so the two enforcement points can never
// drift apart on which functions/argument index are covered.
var replicationSlotFuncs = ast.ReplicationSlotFuncTemporaryArgIndex

// rejectNonTemporaryReplicationSlot fails closed: if the temporary argument
// is missing (the default is false) or isn't a literal boolean true, reject
// — unless admitFailoverSlots is true and the call spells out a literal
// failover=true, which multigres keeps in sync with the standbys and can
// carry across a primary failover (see
// go/services/multipooler/internal/manager/logical_slots.go).
//
// The failover argument must be explicit. An omitted one is rejected like
// any other non-temporary call with the feature off, deliberately: the
// alternative — admitting it and having the caller inject failover=true
// into the SQL before it reaches postgres — makes admission a promise about
// a rewrite some later code path has to remember to perform, which every
// plan-construction path can silently break, and which freezes into any
// definition postgres stores and re-invokes later (a view body, a column
// DEFAULT, a CHECK constraint). Requiring the client to write
// `failover => true` keeps this a pure predicate: what postgres runs is
// exactly what the client wrote, and this function's answer is the whole
// decision rather than half of one. The walsender CREATE_REPLICATION_SLOT
// command form does auto-mark (see the replication preamble's
// rewriteCreateReplicationSlotAddFailover) because its grammar is fixed and
// small enough to patch as text, and because a real subscriber sends it
// without ever seeing this planner.
//
// A non-literal/bound temporary or failover argument is rejected too — this
// is a safety constraint, not a convenience one, so we don't guess: a bound
// temporary could resolve to true at execute time, and PostgreSQL rejects a
// slot created with both temporary=true and failover=true.
func rejectNonTemporaryReplicationSlot(name string, temporaryArgIndex int, fc *ast.FuncCall, admitFailoverSlots bool) error {
	isTemp, temporaryIsLiteral, temporaryGiven := funcCallBoolArg(fc, temporaryArgIndex, ast.ReplicationSlotTemporaryParamName)
	if temporaryIsLiteral && isTemp {
		return nil
	}
	if temporaryGiven && !temporaryIsLiteral {
		return replicationSlotNotTemporaryError(name)
	}
	if admitFailoverSlots {
		if failoverArgIndex, ok := ast.ReplicationSlotFuncFailoverArgIndex[name]; ok {
			isFailover, isLiteral, _ := funcCallBoolArg(fc, failoverArgIndex, ast.ReplicationSlotFailoverParamName)
			if isLiteral && isFailover {
				return nil
			}
		}
	}
	return replicationSlotNotTemporaryError(name)
}

func replicationSlotNotTemporaryError(name string) error {
	return mterrors.NewNonTemporaryReplicationSlotError(name, "temporary=true")
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

	// ValueIsNull marks a call whose value argument is the literal NULL.
	// set_config is not STRICT: set_config(name, NULL, false) resets the
	// parameter to its default and returns that default, so the gateway must
	// track a REMOVAL rather than a value (see syntheticSetStmt, which emits
	// VAR_RESET for this shape). Mutually exclusive with ValueBind — a bound
	// NULL is only knowable at execute time and is handled there.
	ValueIsNull bool
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

// sessionSetSeedFuncs is the set of built-in functions that seed this
// backend's PRNG. The seed is backend-local state with no reset command, not
// even DISCARD ALL, so the backend must stay pinned to the client session for
// the session's lifetime once one of these is called, or a later
// random()/random_normal() landing on a different pooled backend would
// silently break reproducibility.
var sessionSetSeedFuncs = map[string]struct{}{
	"setseed": {},
}

// logicalReplicationSlotCreateFuncs is the set of built-in functions that
// create a logical replication slot via plain SQL (as opposed to the
// replication-protocol CREATE_REPLICATION_SLOT command, which is handled
// separately at connection-open time — see protoutil.ReasonLogicalReplication).
// Supabase Realtime's Postgres Changes / CDC-RLS polling extension creates a
// TEMPORARY slot this way and then polls it repeatedly on the same client
// session for the session's entire lifetime; a temporary slot only exists on
// the backend that created it, so the gateway must keep routing the session
// to that same backend from here on.
var logicalReplicationSlotCreateFuncs = map[string]struct{}{
	"pg_create_logical_replication_slot": {},
}

// isTemporarySlotCreate reports whether fc's `temporary` argument (the third
// positional argument to pg_create_logical_replication_slot) indicates the
// slot being created is temporary — the case that needs backend pinning. A
// persistent (non-temporary) slot is visible from any backend and survives
// independently of the connection that created it, so it doesn't need one.
//
// `temporary` defaults to false when omitted — whether left out entirely or
// (with a failover-registered slot) only its sibling `failover` argument was
// given by name — so an omitted temporary is definitively persistent. When
// the argument is given but isn't a literal boolean (e.g. a bound
// parameter), its value can't be resolved at plan time; this conservatively
// treats that case as temporary so a slot that turns out temporary at
// execute time is never left unpinned — the cost of an unnecessary pin is a
// held connection, not the data-unavailability bug this detection exists to
// prevent.
func isTemporarySlotCreate(fc *ast.FuncCall) bool {
	isTemp, isLiteral, given := funcCallBoolArg(fc, 2, ast.ReplicationSlotTemporaryParamName)
	if !given {
		return false
	}
	if !isLiteral {
		return true
	}
	return isTemp
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

	// CreatesLogicalReplicationSlot is true if any FuncCall in the statement is
	// pg_create_logical_replication_slot(...) with a temporary slot (see
	// logicalReplicationSlotCreateFuncs and isTemporarySlotCreate). The planner
	// uses this to route the statement through a reserved connection with
	// ReasonLogicalReplication so the backend is pinned for the session's
	// lifetime — a temporary slot only exists on that one backend, unlike a
	// persistent slot, which needs no pinning. This is acquire-only: unlike
	// AcquiresSessionAdvisoryLock, there is no matching "release" detection or
	// recheck — the reservation persists until DISCARD ALL or session
	// teardown, mirroring TempTable rather than the advisory-lock pattern.
	CreatesLogicalReplicationSlot bool

	// CallsSetSeed is true if any FuncCall in the statement is setseed(...)
	// (see sessionSetSeedFuncs). The planner uses this to route the statement
	// through a reserved connection with ReasonSetSeed so the backend is
	// pinned for the rest of the session. This is acquire-only, like
	// CreatesLogicalReplicationSlot, but the reservation it requests is sticky
	// (see protoutil.ReasonSetSeed): it survives DISCARD ALL and is released
	// only at the connection's real teardown, never an explicit unpin.
	//
	// Best-effort, same caveat as AcquiresSessionAdvisoryLock: a setseed()
	// call hidden in a PL/pgSQL function body or dynamic SQL is not detected
	// here.
	CallsSetSeed bool

	// NeedsCurrentSettingRewrite is true when the statement is a value-evaluating
	// DML statement (see stmtRewritableForCurrentSetting) that contains at least
	// one current_setting('<gmv>', …) call over a literal gateway-managed name.
	// It's decided here, on the walk this pass already does, so the routing
	// builders can gate the (mutating) rewrite on it and the common case — no such
	// call — skips a second tree walk entirely. See rewriteGatewayManagedCurrentSetting.
	NeedsCurrentSettingRewrite bool
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
//
// unsafeConnection is the per-connection opt-out: when set, every unsafe-statement
// rejection layer (Tier 1 body analysis, the Tier 2 statement blocklist, the
// restricted-GUC guard, and the expression-level function blocklist) is
// suppressed, because the connection has its own dedicated, quarantined backend.
// The planning signals — tracked set_config calls, advisory-lock pinning,
// current_setting rewrites — are still gathered, so routing stays correct; only
// the "reject this statement" behavior is relaxed.
//
// admitFailoverSlots reports whether the slot-based-replication feature is on
// (see Planner.SetSlotBasedReplicationEnabled); it is not affected by
// unsafeConnection — see rejectNonTemporaryReplicationSlot. It is forced
// false unless isImmediatelyExecutedForFailoverAdmission(stmt) confirms this
// exact analysis pass is the one deciding what postgres runs — see that
// function's doc comment for why an allowlist, not a blocklist, is the only
// version of this check that stays correct as the grammar this planner
// supports grows.
func analyzeStatement(stmt ast.Stmt, unsafeConnection bool, admitFailoverSlots bool) (*statementAnalysis, error) {
	if !isImmediatelyExecutedForFailoverAdmission(stmt) {
		admitFailoverSlots = false
	}
	if !unsafeConnection {
		if err := rejectUnsupportedStatement(stmt); err != nil {
			return nil, err
		}
		if err := checkRestrictedGUCChange(stmt); err != nil {
			return nil, err
		}
		if err := analyzeProceduralBody(stmt); err != nil {
			return nil, err
		}
	}
	if err := checkTempSchemaQualifiedCreate(stmt); err != nil {
		return nil, err
	}
	if ps, ok := stmt.(*ast.PrepareStmt); ok {
		if _, err := analyzeSQLPreparedBody(ps.Query, unsafeConnection, admitFailoverSlots); err != nil {
			return nil, err
		}
		// PREPARE analyzes but does not execute the body, so advisory/temp/set_config
		// effects are applied later by SQL EXECUTE.
		return &statementAnalysis{}, nil
	}
	return analyzeFunctionCalls(stmt, !unsafeConnection, admitFailoverSlots)
}

// isImmediatelyExecutedForFailoverAdmission reports whether stmt's
// pg_create_logical_replication_slot(...) calls, if any, are guaranteed to
// run under the enable-slot-based-replication value this exact analysis
// pass observed — the precondition for admitting one via the failover path
// at all (see rejectNonTemporaryReplicationSlot). That holds two ways:
//
//   - The statement runs its own query to completion as part of executing:
//     an ordinary DML statement, COPY (including COPY (query) TO ...), or
//     an EXECUTE whose own argument expressions (not its referenced body)
//     embed the call.
//   - The statement is a PREPARE: its body doesn't execute yet, but every
//     future EXECUTE of it independently re-runs this same analysis with a
//     freshly-read flag value (see planExecuteStmt and the wrapped-EXECUTE
//     unwrapper, tryUnwrapWrappedExecute) before ever sending it to
//     postgres — so a flag flip between PREPARE and EXECUTE is still
//     correctly observed at the point that matters.
//
// Everything else defaults to false, deliberately not enumerated by name.
// Two distinct families land there, for the same underlying reason:
//
//   - Definitions PostgreSQL stores and re-invokes indefinitely — a view or
//     materialized view body, a column DEFAULT or CHECK constraint, a
//     GENERATED ALWAYS AS expression, an index expression, a rule, a
//     row-level-security policy, a SQL-language function or trigger body.
//   - Statements that defer their own query to a later command in the same
//     session: DECLARE ... CURSOR evaluates its query at FETCH, and WITH
//     HOLD at COMMIT, both arbitrarily long after this analysis ran.
//
// In every case the admission decision would be made under a flag reading
// that no longer has to hold when the call actually executes, with no
// further text for this planner to ever re-examine. Naming each such shape
// individually would only ever cover the ones already thought of, so
// anything not on the short list above is presumed deferred and gets the
// same literal-temporary=true-only policy the feature has when disabled,
// mirroring analyzeBodyFragment's identical treatment of PL/pgSQL bodies.
func isImmediatelyExecutedForFailoverAdmission(stmt ast.Stmt) bool {
	switch stmt.NodeTag() {
	case ast.T_SelectStmt, ast.T_InsertStmt, ast.T_UpdateStmt, ast.T_DeleteStmt,
		ast.T_CopyStmt, ast.T_ExecuteStmt, ast.T_PrepareStmt:
		return true
	}
	return false
}

func analyzeSQLPreparedBody(query ast.Node, unsafeConnection bool, admitFailoverSlots bool) (*statementAnalysis, error) {
	stmt, ok := query.(ast.Stmt)
	if !ok || stmt == nil {
		return &statementAnalysis{}, nil
	}
	if !unsafeConnection {
		if err := rejectUnsupportedStatement(stmt); err != nil {
			return nil, err
		}
		if err := checkRestrictedGUCChange(stmt); err != nil {
			return nil, err
		}
		if err := analyzeProceduralBody(stmt); err != nil {
			return nil, err
		}
	}
	if err := checkTempSchemaQualifiedCreate(stmt); err != nil {
		return nil, err
	}
	analysis, err := analyzeFunctionCalls(stmt, !unsafeConnection, admitFailoverSlots)
	if err != nil {
		return nil, err
	}
	if err := validateSQLPreparedSetConfigs(analysis); err != nil {
		return nil, err
	}
	return analysis, nil
}

func validateSQLPreparedSetConfigs(analysis *statementAnalysis) error {
	if analysis == nil {
		return nil
	}
	if analysis.DynamicSetConfig {
		return mterrors.NewFeatureNotSupported("dynamic set_config is not supported inside SQL PREPARE")
	}
	for _, sc := range analysis.SetConfigs {
		if sc.NameBind != nil {
			return mterrors.NewFeatureNotSupported("set_config name argument inside SQL PREPARE must be a literal constant")
		}
		if sc.IsLocalBind != nil {
			return mterrors.NewFeatureNotSupported("set_config is_local argument inside SQL PREPARE must be a literal boolean")
		}
		// A gateway-managed variable must never reach a backend, but a
		// prepared body executes there VERBATIM — the direct path's
		// gateway-managed rewrite cannot apply to a body registered
		// pooler-side as-is, and the release label (built from
		// SessionSettings) structurally cannot describe a gateway-managed
		// value. Rejected regardless of is_local so the prepared form cannot
		// silently diverge from the identical direct statement, which the
		// gateway rewrites and handles itself.
		if handler.IsGatewayManagedVariable(sc.Name) {
			return mterrors.NewFeatureNotSupported(fmt.Sprintf(
				"set_config on gateway-managed variable %q is not supported inside SQL PREPARE", sc.Name))
		}
	}
	return nil
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
//
// reject controls the safety rejections (blocklisted call, set_config in a
// disallowed position, cluster-managed GUC via set_config). When false — the
// unsafe-connection opt-out — those are skipped and the offending call simply
// goes untracked, while the planning signals (accepted set_config, advisory
// locks, current_setting) are still gathered so routing stays correct.
// admitFailoverSlots is independent of reject — see
// rejectNonTemporaryReplicationSlot.
func analyzeFunctionCalls(stmt ast.Stmt, reject bool, admitFailoverSlots bool) (*statementAnalysis, error) {
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
	var hasGatewayManagedCurrentSetting bool
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
			if reject {
				walkErr = mterrors.NewFeatureNotSupported(msg)
				return false
			}
			// unsafe-connection: operator accepts the risk; leave the call
			// alone (it goes to PG untracked) and keep walking.
			return true
		}
		if temporaryArgIndex, isReplicationSlotFunc := replicationSlotFuncs[name]; isReplicationSlotFunc {
			if err := rejectNonTemporaryReplicationSlot(name, temporaryArgIndex, fc, admitFailoverSlots); err != nil {
				walkErr = err
				return false
			}
			// Accepted: either a literal temporary=true, or (with
			// admitFailoverSlots) a persistent logical slot with an explicit
			// literal failover=true, survived the check above.
			// isTemporarySlotCreate below independently re-derives temporary
			// from fc, so a call accepted via the failover path correctly
			// reads as not-temporary here and skips pinning — a failover slot
			// is visible from any backend, like any other persistent slot. A
			// temporary
			// logical replication slot only exists on the backend that
			// created it, so pin the connection for the session's lifetime
			// (see logicalReplicationSlotCreateFuncs/CreatesLogicalReplicationSlot).
			// This can never fire for pg_create_physical_replication_slot,
			// which isn't in logicalReplicationSlotCreateFuncs — nothing
			// yet reads from a physical slot mid-session the way Realtime's
			// CDC-RLS poller does for logical slots. isTemporarySlotCreate is
			// still called (rather than assumed true from the check above)
			// so this stays correct independent of that check's exact
			// fail-closed semantics.
			if _, isSlotCreate := logicalReplicationSlotCreateFuncs[name]; isSlotCreate && isTemporarySlotCreate(fc) {
				result.CreatesLogicalReplicationSlot = true
			}
			return true
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
		if _, isSetSeed := sessionSetSeedFuncs[name]; isSetSeed {
			result.CallsSetSeed = true
			return true
		}
		if name == "current_setting" {
			// Note (don't rewrite here) whether the statement reads a GMV via
			// current_setting; the routing builder does the actual rewrite on a
			// clone. Collecting it on this walk means the no-match common case
			// needs no second traversal.
			if _, isGMV := gatewayManagedCurrentSettingName(fc); isGMV {
				hasGatewayManagedCurrentSetting = true
			}
			return true
		}
		if name != "set_config" {
			return true
		}

		if _, isAllowed := allowedSetConfigs[fc]; !isAllowed {
			// set_config outside a top-level SELECT target — e.g. in a WHERE
			// clause, subquery, or CTE. Its conditional / repeated evaluation
			// semantics there can't be mirrored into a tracked SET, so we don't
			// try. When the feature is on, a transaction-scoped call (is_local=true)
			// on an ordinary GUC reverts at transaction end and leaves nothing for
			// the pooler to track, so it may pass straight through to the backend
			// untracked — this unblocks PostgREST's mutation row-count trick, which
			// calls set_config('pgrst.inserted', …, true) inside an INSERT ... WHERE;
			// other shapes are rejected. In unsafe-connection (reject=false)
			// nothing is rejected: it all goes to PG as written.
			if reject {
				if err := allowTransactionLocalSetConfig(fc); err != nil {
					walkErr = err
					return false
				}
			}
			return true
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
		err := validateDynamicSetConfigShape(stmt, accepted)
		if err != nil && reject {
			return nil, err
		}
		if err == nil {
			// A cluster-managed GUC is still rejected when the name is a literal;
			// the only supported dynamic name is pg_settings.name. Suppressed in
			// unsafe-connection.
			if reject {
				for _, fc := range accepted {
					if name, ok := constStringArg(fc.Args.Items[0]); ok {
						if err := restrictedGUCError(name); err != nil {
							return nil, err
						}
					}
				}
			}
			result.DynamicSetConfig = true
			return result, nil
		}
		// unsafe-connection with an unsupported dynamic shape: don't reject and
		// don't synthesize the resolve-and-apply plan. Fall through to the
		// per-call loop, which lets each set_config pass to PG untracked.
	}

	for _, fc := range accepted {
		setCfg, err := validateAcceptedSetConfig(fc, reject)
		if err != nil {
			if reject {
				return nil, err
			}
			// unsafe-connection: an untrackable set_config is not rejected; it
			// goes to PG as written, just untracked.
			continue
		}
		if setCfg != nil {
			result.SetConfigs = append(result.SetConfigs, *setCfg)
		}
		// else is_local=true on an ordinary variable: leave it alone; PG
		// executes it as a normal transaction-scoped call and the pooler
		// does not track it. (Gateway-managed names DO produce a setCfg —
		// see validateAcceptedSetConfig.)
	}
	// A GMV current_setting is only rewritten where the call is evaluated for a
	// result (see stmtRewritableForCurrentSetting); a stored, re-evaluable
	// definition (CREATE VIEW, CREATE MATERIALIZED VIEW) keeps the call so it isn't
	// frozen to the creating session's value. The DynamicSetConfig path returned
	// earlier, so its ResolveTrackSetConfig plan (which we don't wrap) leaves the
	// flag false.
	result.NeedsCurrentSettingRewrite = hasGatewayManagedCurrentSetting && stmtRewritableForCurrentSetting(stmt)
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

// allowTransactionLocalSetConfig decides whether a set_config call sitting
// outside a top-level SELECT target (in a WHERE clause, subquery, CTE, ...) may
// pass through to the backend untracked. It returns nil to allow the
// pass-through, or a rejection error otherwise.
//
// A call qualifies only when it is unambiguously transaction-scoped and
// harmless for the pooler to ignore:
//   - exactly three arguments;
//   - is_local is the literal boolean true, so PostgreSQL reverts it at
//     transaction end and no untracked backend session state survives the
//     statement — the same reasoning the top-level path uses to leave
//     is_local=true calls untracked (see validateAcceptedSetConfig); and
//   - name is a literal constant that is neither a cluster-managed GUC nor a
//     gateway-managed variable.
//
// Everything else fails closed with the original "top-level SELECT target"
// message: a persistent (is_local=false) change would leak untracked backend
// state; a bound or otherwise non-literal is_local / name can't be resolved at
// plan time; a cluster-managed GUC must never be assigned (its own message is
// preserved); and a gateway-managed variable must never reach the backend,
// since the gateway — not PostgreSQL — is the authority on its value.
//
// search_path is additionally value-restricted here: transaction scoping bounds
// the GUC, not the objects created while it is in effect (see below).
func allowTransactionLocalSetConfig(fc *ast.FuncCall) error {
	reject := mterrors.NewFeatureNotSupported(
		"set_config is only supported as a top-level SELECT target list entry — use a SET statement, or set_config(..., true) for a transaction-scoped change")

	if fc.Args == nil || fc.Args.Len() != 3 {
		return reject
	}
	if isLocal, ok := constBoolArg(fc.Args.Items[2]); !ok || !isLocal {
		return reject
	}
	name, ok := constStringArg(fc.Args.Items[0])
	if !ok {
		return reject
	}
	if err := restrictedGUCError(name); err != nil {
		return err
	}
	if handler.IsGatewayManagedVariable(name) {
		return reject
	}

	// search_path is value-restricted, and is_local=true does NOT make it safe
	// here. The GUC change reverts at transaction end, but anything created
	// under it does not: with pg_temp as the effective creation target, an
	// unqualified CREATE inside the same transaction lands in the pooled
	// backend's temporary namespace and survives the COMMIT. That object
	// carries no TEMP keyword and no pg_temp qualification, so
	// planTempTableCreation and checkTempSchemaQualifiedCreate both miss it —
	// no ReasonTempTable, no MarkTempTainted — and the backend returns to the
	// pool holding it.
	//
	// Unlike every other set_config surface, this path emits no primitive, so
	// there is no execute-time re-check to fall back on (contrast
	// engine.resolveSetConfig / resolvePreparedSetConfig). A value that cannot
	// be read at plan time therefore fails closed.
	if strings.EqualFold(name, "search_path") {
		value, ok := constStringArg(fc.Args.Items[1])
		if !ok {
			return reject
		}
		if err := pgsettings.RejectTempSchemaSearchPath(value); err != nil {
			return err
		}
	}
	return nil
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
			_, ok := sqltypes.ParseBool(v.SVal)
			return ok
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
func validateAcceptedSetConfig(fc *ast.FuncCall, reject bool) (*setConfigCall, error) {
	if fc.Args == nil || fc.Args.Len() != 3 {
		return nil, mterrors.NewFeatureNotSupported(
			"set_config requires three arguments: (name text, value text, is_local bool)")
	}

	// Reject set_config targeting a cluster-managed GUC regardless of
	// is_local — it is just another reachable path for the override blocked in
	// checkRestrictedGUCChange. The normalizer keeps the name literal (see
	// normalizer.go) so we can read it here on the cached and is_local=true
	// paths too. A bound or otherwise non-literal name is a documented gap: we
	// let it through rather than reject blindly. Suppressed in unsafe-connection.
	if reject {
		if name, ok := constStringArg(fc.Args.Items[0]); ok {
			if err := restrictedGUCError(name); err != nil {
				return nil, err
			}

			// search_path values must be vetted for pg_temp (see
			// pgsettings.RejectTempSchemaSearchPath). A literal value is checked
			// here; a bound value is resolved and re-checked at execute time by
			// resolveSetConfig, which runs during the Sequence's prepare phase —
			// before the paired Route reaches the backend — on every is_local
			// shape (false, bound, or literal true via the vet-only entry built
			// below).
			if strings.EqualFold(name, "search_path") {
				if value, ok := constStringArg(fc.Args.Items[1]); ok {
					if err := pgsettings.RejectTempSchemaSearchPath(value); err != nil {
						return nil, err
					}
				}
			}
		}
	}

	sc := &setConfigCall{}

	if pr, isParam := unwrapTypeCast(fc.Args.Items[2]).(*ast.ParamRef); isParam {
		// A bound is_local can resolve to false at execute time. For an
		// ordinary variable that would persist real session state on the pooled
		// backend the routed query already executed on — untrackable divergence
		// from the gateway's authoritative session map. Only gateway-managed
		// variables (whose call is rewritten out of the routed query entirely)
		// support a bound is_local.
		if name, ok := constStringArg(fc.Args.Items[0]); !ok || !handler.IsGatewayManagedVariable(name) {
			return nil, mterrors.NewFeatureNotSupported(
				"set_config is_local argument must be a boolean literal for this variable")
		}
		sc.IsLocalBind = pr
	} else if isLocal, ok := constBoolArg(fc.Args.Items[2]); ok {
		if isLocal {
			// is_local literal true. For an ordinary variable with nothing
			// left to vet we do not track it: PostgreSQL executes the call
			// transaction-scoped via the paired Route and the gateway holds no
			// state. For a gateway-managed variable we DO track it as a
			// transaction-local override, so SHOW matches the `SET LOCAL <gmv>`
			// statement form.
			//
			// Bound slots that still need vetting get a vet-only entry
			// instead of the bare passthrough: IsLocalLiteralTrue plus the
			// bind refs captured below produce an ApplySessionStateFromBind
			// whose resolveSetConfig runs during the Sequence's prepare phase
			// — before the Route reaches the backend — rejects a name
			// resolving to a gateway-managed or restricted GUC and a
			// search_path value naming pg_temp, then tracks nothing
			// (shouldTrack=false for a transaction-scoped ordinary variable).
			// This keeps the PostgREST hot path `set_config($1, $2, true)`
			// working under a single cached plan.
			name, nameIsLiteral := constStringArg(fc.Args.Items[0])
			_, valueIsLiteral := constStringArg(fc.Args.Items[1])
			// A literal NULL value needs no vetting: set_config(..., NULL, ...)
			// resets the parameter to its default, which is server/admin
			// configuration rather than a client-supplied value, so it can
			// never carry a client-injected pg_temp.
			valueIsLiteral = valueIsLiteral || isNullConstArg(fc.Args.Items[1])
			switch {
			case !nameIsLiteral:
				// Bound name: vet-only. (A non-ParamRef expression name is
				// rejected by the capture below — it cannot be resolved at
				// execute time.)
				sc.IsLocalLiteralTrue = true
			case handler.IsGatewayManagedVariable(name):
				// Tracked transaction-local override.
				sc.IsLocalLiteralTrue = true
			case strings.EqualFold(name, "search_path") && !valueIsLiteral:
				// Literal search_path name with a bound value: vet-only, so
				// the resolved value is checked for pg_temp before routing.
				sc.IsLocalLiteralTrue = true
			default:
				// Ordinary variable, everything vetted at plan time:
				// untracked passthrough, no primitive, plan cache compact.
				// A transaction-scoped reset (literal NULL value) lands here
				// too: PostgreSQL scopes it to the transaction, so there is
				// nothing for the gateway to track.
				return nil, nil
			}
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
	} else if isNullConstArg(fc.Args.Items[1]) {
		// set_config(name, NULL, false) is a RESET: PostgreSQL is not STRICT
		// here — it clears the parameter, returns the restored default, and
		// the gateway must track the removal so pool replay stops asserting
		// the old value. Reaching this point implies is_local is the literal
		// false (bound is_local is gateway-managed only, and literal true
		// returned above), so the reset is always session-scoped and
		// syntheticSetStmt can emit VAR_RESET unconditionally.
		//
		// Two shapes stay fail-closed rather than guess:
		//   - a bound name, which the VAR_RESET synthetic cannot resolve (it
		//     would reset a placeholder and silently drift from the backend);
		//   - a gateway-managed variable, whose value the gateway owns and
		//     for which no per-variable reset primitive exists.
		name, nameIsLiteral := constStringArg(fc.Args.Items[0])
		if !nameIsLiteral {
			return nil, setConfigArgError(fc.Args.Items[1], "value")
		}
		if handler.IsGatewayManagedVariable(name) {
			return nil, mterrors.NewFeatureNotSupported(fmt.Sprintf(
				"set_config(%q, NULL, ...) is not supported under connection pooling; use RESET %s", name, name))
		}
		sc.ValueIsNull = true
	} else {
		return nil, setConfigArgError(fc.Args.Items[1], "value")
	}

	return sc, nil
}

// isNullConstArg reports whether n is the literal NULL (after stripping any
// TypeCast), the shape `set_config(name, NULL, false)` uses to reset a
// parameter. Distinguished from constStringArg's failure cases so a NULL can
// be given PostgreSQL's reset semantics instead of a rejection.
func isNullConstArg(n ast.Node) bool {
	c, ok := unwrapTypeCast(n).(*ast.A_Const)
	return ok && c.Isnull
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
		return sqltypes.ParseBool(v.SVal)
	}
	return false, false
}

// funcCallBoolArg resolves fc's paramName parameter, however the caller
// passed it: positionally (at positionalIndex) or with name => value syntax.
// given reports whether the parameter was specified at all, positionally or
// by name; when it's false, the caller gave neither form and the
// parameter's default applies. isLiteral reports whether a given argument
// resolved to a literal boolean (isTrue); a given argument that isn't
// literal is bound, and its value can't be known at plan time — that's a
// different case from omitted and callers that care about the default (e.g.
// isTemporarySlotCreate) must not conflate the two.
//
// Argument location is delegated to ast.FuncCallArg, shared with the
// replication preamble's equivalent check, so the two enforcement points can
// never resolve a positional-vs-named argument differently — only the
// literal-parsing step below is planner-specific (constBoolArg supports a
// broader set of string spellings than the ast package can, per
// literalBoolArg's doc comment).
func funcCallBoolArg(fc *ast.FuncCall, positionalIndex int, paramName string) (isTrue bool, isLiteral bool, given bool) {
	arg, given := ast.FuncCallArg(fc, positionalIndex, paramName)
	if !given {
		return false, false, false
	}
	isTrue, isLiteral = constBoolArg(arg)
	return isTrue, isLiteral, true
}
