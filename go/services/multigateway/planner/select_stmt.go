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

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/services/multigateway/engine"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// planSelectStmt plans a SELECT. When the target list contains one or more
// set_config(...) calls the walker accepted, the plan also tracks those in
// SessionSettings so the change survives pool rotation.
//
// A session-persisting set_config mirrors an unpinned SET: it must never leave
// state on a pooled backend, but a reserved (pinned) backend has no pool-replay
// path and must carry it for real. Which shape is correct depends on live
// session state, yet SELECT plans are cached (keyed on SQL only), so the
// decision cannot be baked in. The plan therefore carries BOTH shapes under a
// SessionStateBranch that picks at execute time:
//
//		Sequence[SessionStateBranch{pinnedRoute, unpinnedRoute}, ApplySessionState per call]
//
//	  - pinnedRoute runs the tracked set_config calls verbatim (is_local as
//	    written), so the reserved backend genuinely applies the change.
//	  - unpinnedRoute flips each ordinary tracked set_config's is_local false→true,
//	    so it reverts at statement end and leaves nothing on the pooled backend;
//	    the value lives only in the gateway map and is replayed at the next
//	    checkout.
//
// Both routes have gateway-managed set_config calls rewritten out (a
// gateway-managed GUC must never persist on any backend) and share the trailing
// silent ApplySessionState primitives, which record the value into the gateway
// map only after the route succeeds — so a rejected SELECT never leaves a GUC
// recorded when the backend never applied it. When there is no ordinary
// persisting set_config to flip, the two shapes are identical and no branch is
// built.
//
// For literal-arg calls the tracker carries the value directly. For calls with
// one or more bound-parameter args (extended-protocol shape
// `SELECT set_config('search_path', $1, false)`) it is built via
// NewApplySessionStateFromBind, which defers per-slot resolution to execute time
// when the portal's Bind values become available — keeping the plan cache hit
// for repeated executions of the same prepared statement regardless of bind
// values.
func (p *Planner) planSelectStmt(
	sql string,
	stmt *ast.SelectStmt,
	conn *server.Conn,
	setConfigs []setConfigCall,
	dynamicSetConfig bool,
	opts PlanOptions,
) (*engine.Plan, error) {
	if dynamicSetConfig {
		return p.planResolveSetConfig(sql, stmt, opts)
	}
	if len(setConfigs) == 0 {
		return p.planDefault(sql, stmt, conn, opts)
	}

	// Rewrite gateway-managed set_config calls out of the query sent to the
	// backend: a gateway-managed set_config must NOT run there, or the real GUC
	// persists on the pooled connection and leaks across clients. A literal value
	// becomes its canonical constant; a bound value ($N) becomes a bare `$N`
	// projection canonicalized at execute time by a GatewayManagedValueRoute.
	// Ordinary set_config calls stay in the routed query.
	rewritten, bound, err := rewriteGatewayManagedSetConfig(stmt)
	if err != nil {
		return nil, err
	}
	// A gateway-managed current_setting in the same SELECT is rewritten to the
	// gateway value too, on whichever AST the backend will run (the set_config
	// clone if there was one, else the original). Its synthetic value slots ride on
	// the same GatewayManagedValueRoute as the bound set_config values. Gated on the
	// analysis flag so we only walk when a rewrite is actually required.
	var routeAST ast.Stmt = stmt
	if rewritten != nil {
		routeAST = rewritten
	}
	var reads []engine.GatewayManagedSettingRead
	if opts.RewriteCurrentSetting {
		csRewritten, csReads, err := rewriteGatewayManagedCurrentSetting(routeAST)
		if err != nil {
			return nil, err
		}
		if csRewritten != nil {
			routeAST = csRewritten
			reads = csReads
		}
	}
	// buildRoute wraps a route AST in a GatewayManagedValueRoute when there are
	// gateway-managed bound values or current_setting reads to canonicalize at
	// execute time; otherwise it is a plain Route. The routed SQL is the AST's own
	// deparse — the original `sql` still contains any gateway-managed call that
	// was rewritten out of the AST, so it must never reach the backend. Advisory-
	// lock pinning rides on the plan's ExecInfo (set below); Sequence forwards it
	// so a `SELECT set_config(...), pg_advisory_lock(...)` both pins the backend
	// for the lock and tracks the session setting on success.
	buildRoute := func(routeStmt ast.Stmt) engine.Primitive {
		route := engine.NewRoute(p.defaultTableGroup, constants.DefaultShard, routeStmt.SqlString(), routeStmt)
		if len(bound) > 0 || len(reads) > 0 {
			return engine.NewGatewayManagedValueRoute(route, bound, reads)
		}
		return route
	}

	// The pinned route runs the base AST verbatim (ordinary set_config keeps its
	// is_local, persisting on the reserved backend). The unpinned route flips each
	// ordinary tracked set_config's is_local false→true so it reverts on the
	// pooled backend. When nothing is flipped (only gateway-managed calls, or
	// only is_local-true calls), the two are identical and no branch is needed.
	primitives := make([]engine.Primitive, 0, len(setConfigs)+1)
	if revertedAST := rewriteSetConfigToRevert(routeAST); revertedAST == nil {
		primitives = append(primitives, buildRoute(routeAST))
	} else {
		primitives = append(primitives, engine.NewSessionStateBranch(
			p.defaultTableGroup, constants.DefaultShard, sql,
			buildRoute(routeAST), buildRoute(revertedAST)))
	}
	for _, sc := range setConfigs {
		base := syntheticSetStmt(sc)
		if sc.hasBoundParams() {
			refs := &engine.BoundSetConfigRefs{
				NameParam:    sc.NameBind,
				ValueParam:   sc.ValueBind,
				IsLocalParam: sc.IsLocalBind,
			}
			primitives = append(primitives, engine.NewApplySessionStateFromBind(sql, base, refs))
		} else {
			primitives = append(primitives, engine.NewApplySessionStateSilent(sql, base))
		}
	}
	plan := engine.NewPlan(sql, engine.NewSequence(primitives))
	plan.ExecInfo = execInfoFromOpts(opts)
	return plan, nil
}

// rewriteSetConfigToRevert flips each ordinary (non-gateway-managed) tracked
// set_config(name, value, false) call in stmt's target list to is_local := true,
// so the call reverts at statement end and leaves no session state on the
// backend it runs on. It is a pure AST transform — it does not observe session
// state; the caller decides whether to use the reverting variant (a pooled
// backend reverts; a reserved backend keeps the verbatim persisting form).
// Gateway-managed calls are already removed from the routed query
// (rewriteGatewayManagedSetConfig); a literal is_local true call already reverts
// and is left untouched.
//
// Returns a rewritten clone when at least one call was flipped, else nil (the
// caller then routes the base AST unchanged). It never mutates stmt: it clones
// on the first flip, so a cached plan's shared tree is left intact.
func rewriteSetConfigToRevert(stmt ast.Stmt) ast.Stmt {
	ss, ok := stmt.(*ast.SelectStmt)
	if !ok || ss.TargetList == nil {
		return nil
	}
	var clone *ast.SelectStmt
	for i, item := range ss.TargetList.Items {
		rt, ok := item.(*ast.ResTarget)
		if !ok {
			continue
		}
		fc, ok := rt.Val.(*ast.FuncCall)
		if !ok || resolveFuncName(fc.Funcname) != "set_config" || fc.Args == nil || fc.Args.Len() != 3 {
			continue
		}
		// Only a literal is_local false persists and needs flipping. A literal
		// true already reverts; a bound is_local occurs only for gateway-managed
		// calls, which are removed from the routed query upstream.
		if isLocal, ok := constBoolArg(fc.Args.Items[2]); !ok || isLocal {
			continue
		}
		// Defensive: gateway-managed calls are removed upstream, so a literal
		// gateway-managed name should never remain here.
		if name, ok := constStringArg(fc.Args.Items[0]); ok && handler.IsGatewayManagedVariable(name) {
			continue
		}
		if clone == nil {
			clone = ast.CloneRefOfSelectStmt(ss)
		}
		cfc := clone.TargetList.Items[i].(*ast.ResTarget).Val.(*ast.FuncCall)
		cfc.Args.Items[2] = ast.NewA_Const(ast.NewBoolean(true), 0)
	}
	if clone == nil {
		return nil
	}
	return clone
}

// planResolveSetConfig plans the narrow dynamic SELECT set_config shape the
// analyzer accepts for pg_dump: the target list is entirely set_config(...), at
// least one name argument is pg_settings.name, and every value/is_local argument
// is static. We can't mint a literal SET to track before reading pg_settings, so
// the plan is a single ResolveTrackSetConfig primitive that:
//
//  1. runs an "unroll" projection — the SELECT with each set_config(a, b, c)
//     target replaced by its three arguments a, b, c — once, to learn the
//     concrete (name, value, is_local) tuples per row (side-effect-free
//     pg_settings read, no set_config side effect);
//  2. prepares and validates gateway tracking actions without mutating state;
//  3. applies all tuples with literals (a synthesized set_config(...) over the
//     captured values) and forwards that authoritative result to the client;
//  4. records the prepared tracking actions only after PostgreSQL accepts the
//     synthesized apply query.
//
// Running the projection once is essential: re-running the original dynamic
// query could resolve to different rows/values after a concurrent catalog
// change, so the first resolution is the source of truth.
func (p *Planner) planResolveSetConfig(sql string, stmt *ast.SelectStmt, opts PlanOptions) (*engine.Plan, error) {
	// Clone before mutating: the AST may be a cached plan's normalized tree
	// shared across executions.
	unroll := ast.CloneRefOfSelectStmt(stmt)

	aliases, err := rewriteToUnrollProjection(unroll)
	if err != nil {
		return nil, err
	}

	// The resolve projection runs through an ordinary Route (bindVar
	// reconstruction included); advisory-lock pinning rides on the plan's
	// ExecInfo, which ResolveTrackSetConfig forwards to this Route at exec time
	// (that's the query that actually evaluates the set_config args, including
	// any pg_advisory_lock call). The resolve primitive just reads the rows the
	// route streams back.
	resolveRoute := engine.NewRoute(p.defaultTableGroup, constants.DefaultShard, unroll.SqlString(), unroll)
	// The resolve projection's rows are read by ResolveTrackSetConfig itself, not
	// streamed to the client, so opt this route out of opaque row passthrough.
	resolveRoute.KeepStructured = true

	prim := engine.NewResolveTrackSetConfig(p.defaultTableGroup, constants.DefaultShard, sql, resolveRoute, unroll, aliases)
	plan := engine.NewPlan(sql, prim)
	plan.ExecInfo = execInfoFromOpts(opts)
	return plan, nil
}

// rewriteGatewayManagedSetConfig rewrites every gateway-managed set_config call in
// stmt's target list out of the query that will be routed to a backend, so the
// real GUC is never set (and leaked) there — the gateway owns these variables and
// the sibling ApplySessionState primitives update its state.
//
// Each such call is replaced, in a clone, by its value: a literal value becomes
// its canonical constant (computed here, at plan time), and a bound value ($N)
// becomes a bare projection whose slot GatewayManagedValueRoute canonicalizes at
// execute time. When the value param is referenced only once, its own slot is
// reused (keeping the param in the AST, so portal bind-decoding is trivial); when it
// is shared with another use, a fresh synthetic slot is allocated that reads the
// same value but is canonicalized independently, so the other use is untouched.
// Either way the set_config never reaches the backend. is_local doesn't matter —
// the call is removed regardless.
//
// Returns (rewrittenClone, boundValues, nil) when at least one call was rewritten;
// (nil, nil, nil) when there was nothing to rewrite (caller routes the original);
// (nil, nil, err) when a literal value is invalid (mirrors set_config's set-time
// validation) or a gateway-managed call has a non-literal, non-bound value. The
// latter is unreachable — the analyzer rejects such a value or routes it through
// ResolveTrackSetConfig — so it fails closed as an internal-invariant error rather
// than leaving the call for the backend (which would leak the real GUC).
func rewriteGatewayManagedSetConfig(stmt *ast.SelectStmt) (*ast.SelectStmt, []engine.GatewayManagedBoundValue, error) {
	if stmt.TargetList == nil {
		return nil, nil, nil
	}
	paramCounts := countParamRefs(stmt)
	// Synthetic value slots (for shared params, below) are numbered past the highest
	// param the client sent, so they can't collide with a real bind.
	maxParam := 0
	for n := range paramCounts {
		if n > maxParam {
			maxParam = n
		}
	}

	var clone *ast.SelectStmt
	var bound []engine.GatewayManagedBoundValue
	for i, item := range stmt.TargetList.Items {
		rt, ok := item.(*ast.ResTarget)
		if !ok {
			continue
		}
		fc, ok := rt.Val.(*ast.FuncCall)
		if !ok || resolveFuncName(fc.Funcname) != "set_config" || fc.Args == nil || fc.Args.Len() != 3 {
			continue
		}
		name, ok := constStringArg(fc.Args.Items[0])
		if !ok || !handler.IsGatewayManagedVariable(name) {
			continue
		}

		// Determine the replacement projection for this target.
		var replacement ast.Node
		var record *engine.GatewayManagedBoundValue
		if pr, isParam := unwrapTypeCast(fc.Args.Items[1]).(*ast.ParamRef); isParam {
			// The projection reads the canonical value from `target`, sourced from the
			// call's own value param. Normally target == source: the value param is
			// reused as the projection and canonicalized in place. But when that param
			// is *also* referenced elsewhere, canonicalizing it in place would corrupt
			// the other use — so allocate a fresh synthetic slot that reads from the
			// source but is canonicalized independently, leaving the original param
			// untouched (and never letting the set_config reach the backend).
			target := pr.Number
			if paramCounts[pr.Number] != 1 {
				maxParam++
				target = maxParam
			}
			// Fresh ParamRef so the clone doesn't share a node with the original
			// AST (which may be a cached plan's tree).
			replacement = ast.NewParamRef(target, 0)
			record = &engine.GatewayManagedBoundValue{Param: target, SourceParam: pr.Number, Name: name}
		} else if value, ok := constStringArg(fc.Args.Items[1]); ok {
			canonical, err := handler.GatewayManagedCanonicalValue(name, value)
			if err != nil {
				return nil, nil, err
			}
			replacement = ast.NewA_Const(ast.NewString(canonical), 0)
		} else {
			// Unreachable for a gateway-managed variable: the analyzer rejects an
			// expression-valued set_config (mixed target list → setConfigArgError) or
			// routes it through ResolveTrackSetConfig (all-set_config target list →
			// DynamicSetConfig), using the same literal/bound classification as above.
			// So a non-literal, non-bound value never gets here. If one ever does,
			// analyzer and planner have diverged — fail closed rather than leave the
			// gateway-managed set_config for the backend, which would persist the real
			// GUC and leak it across pooled clients.
			return nil, nil, resolveSetConfigBug(fmt.Sprintf(
				"gateway-managed set_config %q reached the rewrite with a non-literal, non-bound value", name))
		}

		if clone == nil {
			clone = ast.CloneRefOfSelectStmt(stmt)
		}
		crt := clone.TargetList.Items[i].(*ast.ResTarget)
		crt.Val = replacement
		// Preserve the output column name. set_config's default is "set_config";
		// a bare projection would otherwise be reported as "?column?".
		if crt.Name == "" {
			crt.Name = "set_config"
		}
		if record != nil {
			bound = append(bound, *record)
		}
	}
	if clone == nil {
		return nil, nil, nil
	}
	return clone, bound, nil
}

func countParamRefs(stmt *ast.SelectStmt) map[int]int {
	counts := map[int]int{}
	ast.Rewrite(stmt, func(cursor *ast.Cursor) bool {
		if pr, ok := cursor.Node().(*ast.ParamRef); ok {
			counts[pr.Number]++
		}
		return true
	}, nil)
	return counts
}

// rewriteToUnrollProjection rewrites ss in place: its target list (already
// validated by the analyzer to be entirely set_config(name, value, is_local)
// calls) is replaced with a flat projection of every call's three arguments,
// in order. The FROM/WHERE/GROUP BY/... clauses are kept verbatim so the
// projection resolves the same rows the original would have. It returns the
// per-call output-column aliases (ResTarget.Name, "" when the call had no
// explicit AS) so the apply step can reproduce the original's column names.
func rewriteToUnrollProjection(ss *ast.SelectStmt) ([]string, error) {
	if ss.TargetList == nil || ss.TargetList.Len() == 0 {
		return nil, resolveSetConfigBug("target list is empty")
	}

	aliases := make([]string, 0, ss.TargetList.Len())
	projected := ast.NewNodeList()
	for _, item := range ss.TargetList.Items {
		rt, ok := item.(*ast.ResTarget)
		if !ok {
			return nil, resolveSetConfigBug(fmt.Sprintf("target is a %T, not a ResTarget", item))
		}
		fc, ok := rt.Val.(*ast.FuncCall)
		if !ok || fc.Args == nil || fc.Args.Len() != 3 {
			return nil, resolveSetConfigBug("target is not a three-argument set_config call")
		}
		aliases = append(aliases, rt.Name)
		for _, arg := range fc.Args.Items {
			projected.Append(ast.NewResTarget("", arg))
		}
	}
	ss.TargetList = projected
	return aliases, nil
}

// resolveSetConfigBug builds an internal-error diagnostic for an invariant the
// analyzer was supposed to guarantee before the planner reached this code (the
// target list being entirely three-argument set_config calls). Reaching it
// means analyzer and planner disagree — a bug — so it surfaces as SQLSTATE
// XX000 with a report-this-bug hint rather than a bare Go error.
func resolveSetConfigBug(detail string) error {
	return mterrors.NewPgError("ERROR", mterrors.PgSSInternalError,
		"internal error building set_config plan (please report this as a bug)", detail)
}

// syntheticSetStmt builds a VariableSetStmt equivalent to `SET name = value`
// for use inside ApplySessionState. Same shape as what the real
// VariableSetStmt path feeds into the primitive, so execution is identical.
//
// For slots that were bound parameters, a `__bind_$N__` placeholder is
// used. These placeholders are overwritten at execute time when
// executeSetWithBinds resolves the actual value from the portal — they
// exist only so SqlString-style debug output stays structurally valid and
// so an accidental literal-mode execute would surface as an obvious bug
// rather than silently leaking the placeholder into SessionSettings.
func syntheticSetStmt(sc setConfigCall) *ast.VariableSetStmt {
	name := sc.Name
	if sc.NameBind != nil {
		name = fmt.Sprintf("__bind_$%d__", sc.NameBind.Number)
	}
	value := sc.Value
	if sc.ValueBind != nil {
		value = fmt.Sprintf("__bind_$%d__", sc.ValueBind.Number)
	}
	return &ast.VariableSetStmt{
		BaseNode: ast.BaseNode{Tag: ast.T_VariableSetStmt},
		Kind:     ast.VAR_SET_VALUE,
		Name:     name,
		// IsLocal is set for a set_config(..., true) that produced a
		// setConfigCall: a gateway-managed variable (tracked as a
		// transaction-local override, parity with SET LOCAL <gmv>) or a
		// vet-only entry with bound name/value (resolveSetConfig vets the
		// resolved slots, sees isLocal=true here, and tracks nothing).
		// Fully-vetted ordinary is_local=true calls never produce a
		// setConfigCall, so this is false for them.
		IsLocal: sc.IsLocalLiteralTrue,
		Args: ast.NewNodeList(
			ast.NewA_Const(ast.NewString(value), 0),
		),
	}
}
