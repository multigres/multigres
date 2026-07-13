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
// With set_configs present the plan is always
//
//	Sequence[Route(rewritten SQL), ApplySessionState per call]
//
// The Route sends the query to PG and streams the result back, but any
// gateway-managed set_config is rewritten out of that query first (it must not
// run on the backend, or the real GUC leaks across pooled clients); ordinary
// set_config calls still execute on PG normally. The Sequence precomputes the
// backend's post-success session settings and attaches them to the Route for
// multipooler recycle bookkeeping, but only after the Route succeeds do the
// silent ApplySessionState primitives update the gateway tracker. This preserves
// PostgreSQL semantics on statement errors: a rejected SELECT must not leave a
// session GUC recorded in the gateway when the backend never applied it.
//
// For literal-arg calls the primitive carries the value directly. For
// calls with one or more bound-parameter args (extended-protocol shape
// `SELECT set_config('search_path', $1, false)`) the primitive is built
// via NewApplySessionStateFromBind, which defers per-slot resolution to
// execute time when the portal's Bind values become available — keeping
// the plan cache hit for repeated executions of the same prepared
// statement regardless of bind values.
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

	primitives := make([]engine.Primitive, 0, len(setConfigs)+1)
	// The leading route runs the SELECT itself — but with gateway-managed
	// set_config calls rewritten out of the query sent to the backend. A
	// gateway-managed set_config must NOT run on the backend: it would persist the
	// real GUC on the pooled connection and leak it across clients. A literal value
	// becomes its canonical constant; a bound value ($N) becomes a bare `$N`
	// projection canonicalized at execute time by a GatewayManagedValueRoute.
	// Ordinary set_config calls stay in the routed query. Advisory-lock pinning
	// rides on the plan's ExecInfo (set below); Sequence forwards it to this Route,
	// so a `SELECT set_config(...), pg_advisory_lock(...)` both pins the backend for
	// the lock and, if the SELECT succeeds, tracks the session setting via the
	// silent ApplySessionState primitives below.
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
	if rewritten != nil || len(reads) > 0 {
		route := engine.NewRoute(p.defaultTableGroup, constants.DefaultShard, routeAST.SqlString(), routeAST)
		if len(bound) > 0 || len(reads) > 0 {
			primitives = append(primitives, engine.NewGatewayManagedValueRoute(route, bound, reads))
		} else {
			primitives = append(primitives, route)
		}
	} else {
		primitives = append(primitives, engine.NewRoute(p.defaultTableGroup, constants.DefaultShard, sql, stmt))
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
	plan.ExecInfo = advisoryExecInfo(opts)
	return plan, nil
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

	prim := engine.NewResolveTrackSetConfig(p.defaultTableGroup, constants.DefaultShard, sql, resolveRoute, unroll, aliases)
	plan := engine.NewPlan(sql, prim)
	plan.ExecInfo = advisoryExecInfo(opts)
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

// countParamRefs returns how many times each $N appears in stmt.
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
		// IsLocal is set only for a gateway-managed set_config(..., true), which
		// the executor applies as a transaction-local override (parity with
		// SET LOCAL <gmv>). Ordinary is_local=true calls never produce a
		// setConfigCall, so this is false for them.
		IsLocal: sc.IsLocalLiteralTrue,
		Args: ast.NewNodeList(
			ast.NewA_Const(ast.NewString(value), 0),
		),
	}
}
