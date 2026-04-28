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
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/services/multigateway/engine"
)

// planSelectStmt plans a SELECT. When the target list contains one or more
// set_config(literal, literal, false) calls the walker accepted, the plan
// also tracks those in SessionSettings so the change survives pool rotation.
//
// With set_configs present the plan is always
//
//	Sequence[silent ApplySessionState per call, Route(original SQL)]
//
// The silent primitives update the tracker without emitting anything to
// the client; the Route then sends the unmodified query to PG, which
// executes set_config normally and streams the result back. The extra
// round-trip matters less than keeping the planning logic uniform — we
// don't try to short-circuit the bare `SELECT set_config(...)` case.
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
		primitives = append(primitives, engine.NewApplySessionStateSilent(sql, syntheticSetStmt(sc)))
	}
	primitives = append(primitives, engine.NewRoute(p.defaultTableGroup, constants.DefaultShard, sql, stmt))
	return engine.NewPlan(sql, engine.NewSequence(primitives)), nil
}

// syntheticSetStmt builds a VariableSetStmt equivalent to `SET name = value`
// for use inside ApplySessionState. Same shape as what the real
// VariableSetStmt path feeds into the primitive, so execution is identical.
func syntheticSetStmt(sc setConfigCall) *ast.VariableSetStmt {
	return &ast.VariableSetStmt{
		BaseNode: ast.BaseNode{Tag: ast.T_VariableSetStmt},
		Kind:     ast.VAR_SET_VALUE,
		Name:     sc.Name,
		Args: ast.NewNodeList(
			ast.NewA_Const(ast.NewString(sc.Value), 0),
		),
	}
}
