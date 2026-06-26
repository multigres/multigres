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
)

// planSelectStmt plans a SELECT. When the target list contains one or more
// set_config(...) calls the walker accepted, the plan also tracks those in
// SessionSettings so the change survives pool rotation.
//
// With set_configs present the plan is always
//
//	Sequence[Route(original SQL), ApplySessionState per call]
//
// The Route sends the unmodified query to PG, which executes set_config
// normally and streams the result back. Only after that succeeds do the silent
// ApplySessionState primitives update the gateway tracker. This preserves
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
	// The leading route runs the SELECT itself. Advisory-lock pinning rides on
	// the plan's ExecInfo (set below); Sequence forwards it to this Route, so a
	// `SELECT set_config(...), pg_advisory_lock(...)` both pins the backend for
	// the lock and, if the SELECT succeeds, tracks the session setting via the
	// silent ApplySessionState primitives below.
	primitives = append(primitives, engine.NewRoute(p.defaultTableGroup, constants.DefaultShard, sql, stmt))
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

// planResolveSetConfig plans a SELECT whose target list is entirely
// set_config(...) calls where at least one argument can't be resolved at plan
// time (the analyzer set DynamicSetConfig). We can't mint a literal SET to
// track, so the plan is a single ResolveTrackSetConfig primitive that:
//
//  1. runs an "unroll" projection — the SELECT with each set_config(a, b, c)
//     target replaced by its three arguments a, b, c — once, to learn the
//     concrete (name, value, is_local) tuples per row (pure read, no
//     set_config side effect);
//  2. prepares and validates gateway tracking actions without mutating state;
//  3. applies all tuples with literals (a synthesized set_config(...) over the
//     captured values) and forwards that authoritative result to the client;
//  4. records the prepared tracking actions only after PostgreSQL accepts the
//     synthesized apply query.
//
// Running the projection once is essential: re-running the original dynamic
// query could resolve to different rows/values (concurrent catalog change,
// volatile FROM), so the first resolution is the source of truth.
//
// opts carries advisory-lock pinning intent. A set_config argument can itself
// acquire a session-level advisory lock (e.g.
// set_config('x', pg_try_advisory_lock(1)::text, false)); that call runs when
// the resolve projection evaluates the arguments, so the primitive must pin the
// backend the same way routePrimitive would for an ordinary query.
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
