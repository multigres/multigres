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
	"errors"

	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/services/multigateway/engine"
)

// planPrepareStmt creates a plan for PREPARE name [(types)] AS query.
// Delegates to conn.Handler().HandleParse at execution time.
func (p *Planner) planPrepareStmt(sql string, stmt *ast.PrepareStmt) (*engine.Plan, error) {
	innerQuery := engine.ExtractInnerQuery(stmt)
	if innerQuery == "" {
		return nil, errors.New("PREPARE: inner query is empty")
	}

	paramTypes := engine.ExtractParamTypeOids(stmt)
	prim := engine.NewPreparePrimitive(p.defaultTableGroup, stmt.Name, innerQuery, paramTypes)
	plan := engine.NewPlan(sql, prim)

	p.logger.Debug("created prepare plan", "name", stmt.Name, "inner_query", innerQuery)
	return plan, nil
}

// planExecuteStmt creates a plan for EXECUTE name [(params)].
// Delegates to conn.Handler().HandleBind + exec.PortalStreamExecute at execution time.
func (p *Planner) planExecuteStmt(sql string, stmt *ast.ExecuteStmt) (*engine.Plan, error) {
	params, err := engine.ExtractExecuteParams(stmt)
	if err != nil {
		return nil, err
	}

	prim := engine.NewExecutePrimitive(p.defaultTableGroup, stmt.Name, params)
	plan := engine.NewPlan(sql, prim)

	p.logger.Debug("created execute plan", "name", stmt.Name, "param_count", len(params))
	return plan, nil
}

// planDeallocateStmt creates a plan for DEALLOCATE name or DEALLOCATE ALL.
// Named DEALLOCATE delegates to conn.Handler().HandleClose.
// DEALLOCATE ALL uses the consolidator directly (no extended protocol equivalent).
func (p *Planner) planDeallocateStmt(sql string, stmt *ast.DeallocateStmt) (*engine.Plan, error) {
	var prim engine.Primitive
	if stmt.IsAll {
		prim = engine.NewDeallocateAllPrimitive(p.defaultTableGroup)
	} else {
		prim = engine.NewDeallocatePrimitive(p.defaultTableGroup, stmt.Name)
	}
	plan := engine.NewPlan(sql, prim)

	p.logger.Debug("created deallocate plan", "name", stmt.Name, "is_all", stmt.IsAll)
	return plan, nil
}
