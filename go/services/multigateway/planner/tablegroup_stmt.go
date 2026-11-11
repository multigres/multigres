// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package planner

import (
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/services/multigateway/engine"
)

// errTableGroupDDLDisabled rejects tablegroup DDL on a gateway with no
// tablegroup admin. Without a provisioner the catalog would record the intent
// and the statement would wait until the creation timed out.
func errTableGroupDDLDisabled() error {
	return mterrors.NewFeatureNotSupported("tablegroup DDL is not enabled on this cluster")
}

func (p *Planner) planCreateTableGroup(sql string, stmt *ast.CreateTableGroupStmt) (*engine.Plan, error) {
	if p.tableGroups == nil {
		return nil, errTableGroupDDLDisabled()
	}
	var shards []string
	if stmt.Shards != nil {
		for _, item := range stmt.Shards.Items {
			if s, ok := item.(*ast.String); ok {
				shards = append(shards, s.SVal)
			}
		}
	}
	prim := engine.NewCreateTableGroup(p.tableGroups, sql, stmt.Name, shards, stmt.IfNotExists)
	return p.tableGroupPlan(sql, prim), nil
}

func (p *Planner) planDropTableGroup(sql string, stmt *ast.DropStmt) (*engine.Plan, error) {
	if p.tableGroups == nil {
		return nil, errTableGroupDDLDisabled()
	}
	if stmt.Behavior == ast.DropCascade {
		return nil, mterrors.NewFeatureNotSupported("DROP TABLEGROUP ... CASCADE is not supported; drop or move the tablegroup's tables first")
	}
	name, ok := dropTableGroupName(stmt)
	if !ok {
		return nil, mterrors.NewFeatureNotSupported("DROP TABLEGROUP takes exactly one tablegroup name")
	}
	prim := engine.NewDropTableGroup(p.tableGroups, sql, name, stmt.MissingOk)
	return p.tableGroupPlan(sql, prim), nil
}

// dropTableGroupName returns the single name the grammar puts in a DROP
// TABLEGROUP: one list holding one string.
func dropTableGroupName(stmt *ast.DropStmt) (string, bool) {
	if stmt.Objects == nil || stmt.Objects.Len() != 1 {
		return "", false
	}
	names, ok := stmt.Objects.Items[0].(*ast.NodeList)
	if !ok || names.Len() != 1 {
		return "", false
	}
	s, ok := names.Items[0].(*ast.String)
	if !ok {
		return "", false
	}
	return s.SVal, true
}

func (p *Planner) tableGroupPlan(sql string, prim *engine.TableGroupDDL) *engine.Plan {
	plan := engine.NewPlan(sql, prim)
	plan.Type = planType(plan.Primitive, plan.ExecInfo)
	return plan
}
