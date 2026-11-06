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
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/services/multigateway/engine"
)

// errClusterViewUnsupported rejects a statement that references a cluster
// view (multigres.multigateways, multigres.multipoolers) from a plan path
// that would forward the statement text to the backend unfolded.
var errClusterViewUnsupported = mterrors.NewFeatureNotSupported(
	"multigres views cannot be combined with this statement")

// CheckClusterViewStatement rejects statements that name a cluster view in a
// way the fold must not serve: writes to a view, and definitions that would
// persist one snapshot (a view or materialized view over it). It is run
// before planning, and by the executor before an in-transaction Parse, so
// the client gets the gateway's rejection rather than a backend error.
func CheckClusterViewStatement(stmt ast.Stmt) error {
	if rv := clusterViewWriteTarget(stmt); rv != nil {
		return mterrors.NewFeatureNotSupported(
			constants.MultigresSchema + "." + rv.RelName + " is a read-only view")
	}
	if !engine.ContainsClusterView(stmt) {
		return nil
	}
	switch s := stmt.(type) {
	case *ast.ViewStmt:
		return mterrors.NewFeatureNotSupported("a view cannot be defined over a multigres view")
	case *ast.CreateTableAsStmt:
		if s.ObjType == ast.OBJECT_MATVIEW {
			return mterrors.NewFeatureNotSupported("a materialized view cannot be defined over a multigres view")
		}
	}
	return nil
}

// noClusterView guards the dispatch branches in Plan that return before
// wrapClusterViews runs (temp-table creation, holdable cursors): their
// primitives forward the statement text unfolded, so a view reference there
// is rejected rather than sent to the backend. With the views off nothing
// is a view and the statement goes to the backend as written.
func (p *Planner) noClusterView(sql string, stmt ast.Stmt) error {
	if p.ClusterViewsEnabled() && engine.MayReferenceClusterView(sql) && engine.ContainsClusterView(stmt) {
		return errClusterViewUnsupported
	}
	return nil
}

// wrapClusterViews turns a plain Route whose statement reads a cluster view
// into a ClusterViewRoute, which folds the gateway's current snapshot into
// the statement at each execution (see engine/cluster_view.go). Any other
// plan is returned as is. A view reference combined with a construct the
// planner handles with a non-Route primitive (tracked set_config, COPY, ...)
// is rejected as unsupported rather than silently routing the unfolded name
// to the backend.
func (p *Planner) wrapClusterViews(sql string, stmt ast.Stmt, plan *engine.Plan) (*engine.Plan, error) {
	if !p.ClusterViewsEnabled() || !engine.MayReferenceClusterView(sql) || !engine.ContainsClusterView(stmt) {
		return plan, nil
	}
	route, ok := plan.Primitive.(*engine.Route)
	if !ok || route.NormalizedAST == nil {
		return nil, errClusterViewUnsupported
	}
	plan.Primitive = engine.NewClusterViewRoute(route, p.clusterViews)
	return plan, nil
}

// clusterViewWriteTarget returns the cluster view a data-modifying statement
// targets, or nil.
func clusterViewWriteTarget(stmt ast.Stmt) *ast.RangeVar {
	var targets []ast.Node
	switch s := stmt.(type) {
	case *ast.InsertStmt:
		targets = []ast.Node{s.Relation}
	case *ast.UpdateStmt:
		targets = []ast.Node{s.Relation}
	case *ast.DeleteStmt:
		targets = []ast.Node{s.Relation}
	case *ast.MergeStmt:
		targets = []ast.Node{s.Relation}
	case *ast.TruncateStmt:
		if s.Relations != nil {
			targets = s.Relations.Items
		}
	default:
		return nil
	}
	for _, t := range targets {
		if rv, ok := t.(*ast.RangeVar); ok && engine.IsClusterViewRelation(rv) {
			return rv
		}
	}
	return nil
}
