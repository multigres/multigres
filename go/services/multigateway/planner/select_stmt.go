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
//	Sequence[ApplySessionState per call, Route(original SQL)]
//
// The ApplySessionState primitive runs silently — it updates the tracker
// without emitting anything to the client; the Route then sends the
// unmodified query to PG, which executes set_config normally and streams
// the result back. The extra round-trip matters less than keeping the
// planning logic uniform — we don't try to short-circuit the bare
// `SELECT set_config(...)` case.
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
) (*engine.Plan, error) {
	if len(setConfigs) == 0 {
		return p.planDefault(sql, stmt, conn)
	}

	primitives := make([]engine.Primitive, 0, len(setConfigs)+1)
	for _, sc := range setConfigs {
		base := syntheticSetStmt(sc)
		if sc.Bound {
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
	primitives = append(primitives, engine.NewRoute(p.defaultTableGroup, constants.DefaultShard, sql, stmt))
	return engine.NewPlan(sql, engine.NewSequence(primitives)), nil
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
		Args: ast.NewNodeList(
			ast.NewA_Const(ast.NewString(value), 0),
		),
	}
}
