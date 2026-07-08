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

package engine

import (
	"context"
	"fmt"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// mterrorsRewriteBug builds an internal-error diagnostic for an invariant the
// planner was supposed to guarantee before this primitive ran. Reaching it means
// planner and executor disagree.
func mterrorsRewriteBug(detail string) error {
	return mterrors.NewPgError("ERROR", mterrors.PgSSInternalError,
		"internal error rewriting set_config query (please report this as a bug)", detail)
}

// GatewayManagedBoundValue names the bind slot a gateway-managed set_config value
// projects through. The planner rewrote the call to a bare `$Param` projection at
// plan time; before the query runs, the raw value read from slot SourceParam is
// canonicalized (e.g. '1000' → '1s') and written into slot Param so the client sees
// the same string PostgreSQL's set_config would return.
//
// Usually Param == SourceParam: the call's own value param is reused as the
// projection and canonicalized in place. When that param is *also* referenced
// elsewhere in the query, canonicalizing it in place would corrupt the other use,
// so the planner allocates a fresh synthetic slot (Param != SourceParam) that reads
// its value from SourceParam but is canonicalized independently — leaving the
// original param untouched. The synthetic slot exists only inside the gateway; it is
// never decoded from the client's portal.
type GatewayManagedBoundValue struct {
	// Param is the 1-based bind slot the projection reads (holds the canonical value
	// after canonicalize runs).
	Param int
	// SourceParam is the 1-based bind slot holding the client's raw value. Equal to
	// Param unless a synthetic slot was allocated to avoid corrupting a shared param.
	SourceParam int
	// Name is the gateway-managed variable (e.g. "statement_timeout").
	Name string
}

// isSynthetic reports whether Param is a gateway-allocated slot (not a client bind).
func (v GatewayManagedBoundValue) isSynthetic() bool {
	return v.Param != v.SourceParam
}

// GatewayManagedValueRoute canonicalizes the bound gateway-managed set_config
// values in a query, then routes it. The query rewrite itself already happened at
// plan time: each gateway-managed set_config('<gmv>', …) call was replaced by its
// value — a canonical constant for a literal, or a bare `$N` projection for a bound
// value — so the set_config never runs on the backend, the real GUC is never
// persisted there, and it can't leak across pooled clients (the gateway state is
// updated by the sibling ApplySessionState primitives).
//
// This primitive only exists for the bound-value case: its one runtime job is to
// replace each `$N` value slot with its canonical form (e.g. '1000' → '1s') before
// the wrapped Route reconstructs and executes the query. Literal-only rewrites need
// no execute-time work and route through a plain Route instead. The Route handles
// the simple and extended protocols and bindVar reconstruction like any other query.
type GatewayManagedValueRoute struct {
	route  *Route
	values []GatewayManagedBoundValue
}

// NewGatewayManagedValueRoute wraps route (built from the plan-time-rewritten AST)
// with the bound value slots that need canonicalizing at execute time.
func NewGatewayManagedValueRoute(route *Route, values []GatewayManagedBoundValue) *GatewayManagedValueRoute {
	return &GatewayManagedValueRoute{route: route, values: values}
}

// StreamExecute runs on the simple-query path, where any non-gateway-managed
// literals arrive as normalizer bindVars.
func (r *GatewayManagedValueRoute) StreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	bindVars []*ast.A_Const,
	info PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	bindVars, err := r.canonicalize(bindVars)
	if err != nil {
		return err
	}
	return r.route.StreamExecute(ctx, exec, conn, state, bindVars, info, callback)
}

// PortalStreamExecute runs on the extended-protocol path. It decodes the portal's
// binds, canonicalizes the gateway-managed value slots, and runs the rewritten
// query through the Route as a simple execution — never reissuing the portal,
// which would run the original set_config on the backend.
func (r *GatewayManagedValueRoute) PortalStreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	portalInfo *preparedstatement.PortalInfo,
	_ int32,
	includeDescribe bool,
	info PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	bindVars, err := r.bindVarsFromPortal(portalInfo)
	if err != nil {
		return err
	}
	bindVars, err = r.canonicalize(bindVars)
	if err != nil {
		return err
	}
	// The Route runs a simple execution, whose result carries Fields. Emit
	// RowDescription from this Execute only when a portal Describe was folded in;
	// a separate Describe already sent it, so forwarding Fields again would be a
	// duplicate 'T' that breaks the client's extended-protocol state machine.
	if !includeDescribe {
		callback = stripResultFields(callback)
	}
	return r.route.StreamExecute(ctx, exec, conn, state, bindVars, info, callback)
}

// canonicalize returns a copy of bindVars in which each gateway-managed value slot
// (Param) holds the canonical form of the raw value read from its SourceParam (the
// string PostgreSQL's set_config would return). The result is grown to fit any
// synthetic Param slots the planner allocated past the client's bind count. Returns
// bindVars unchanged when there are no bound values.
func (r *GatewayManagedValueRoute) canonicalize(bindVars []*ast.A_Const) ([]*ast.A_Const, error) {
	if len(r.values) == 0 {
		return bindVars, nil
	}
	n := len(bindVars)
	for _, v := range r.values {
		if v.Param > n {
			n = v.Param
		}
	}
	out := make([]*ast.A_Const, n)
	copy(out, bindVars)
	for _, v := range r.values {
		src := v.SourceParam - 1
		if src < 0 || src >= len(bindVars) || bindVars[src] == nil {
			return nil, mterrorsRewriteBug(fmt.Sprintf("source bind $%d out of range for %q", v.SourceParam, v.Name))
		}
		canonical, err := handler.GatewayManagedCanonicalValue(v.Name, extractConstValue(bindVars[src]))
		if err != nil {
			return nil, err
		}
		out[v.Param-1] = ast.NewA_Const(ast.NewString(canonical), 0)
	}
	return out, nil
}

// bindVarsFromPortal decodes the portal's Bind values into a positional slice
// (index 0 is $1). It decodes every $N referenced in the (rewritten) route AST plus
// every SourceParam a synthetic slot reads from — the latter may have been rewritten
// out of the AST entirely, but its value is still needed to canonicalize. Synthetic
// Param slots are left nil (the client never sent them); canonicalize fills them.
// Returns nil when there are no parameters.
func (r *GatewayManagedValueRoute) bindVarsFromPortal(portalInfo *preparedstatement.PortalInfo) ([]*ast.A_Const, error) {
	maxNum := 0
	// refs is keyed by param number so a source param synthesized below can't
	// duplicate one already found in the AST.
	refs := map[int]*ast.ParamRef{}
	ast.Rewrite(r.route.NormalizedAST, func(cursor *ast.Cursor) bool {
		if pr, ok := cursor.Node().(*ast.ParamRef); ok {
			refs[pr.Number] = pr
			if pr.Number > maxNum {
				maxNum = pr.Number
			}
		}
		return true
	}, nil)

	// Synthetic slots aren't decoded from the portal, but their source params must
	// be — even when the source no longer appears in the routed AST (e.g. every use
	// of it was a gateway-managed set_config value that got rewritten out).
	synthetic := map[int]bool{}
	for _, v := range r.values {
		if !v.isSynthetic() {
			continue
		}
		synthetic[v.Param] = true
		if v.Param > maxNum {
			maxNum = v.Param
		}
		if _, ok := refs[v.SourceParam]; !ok {
			refs[v.SourceParam] = ast.NewParamRef(v.SourceParam, 0)
		}
		if v.SourceParam > maxNum {
			maxNum = v.SourceParam
		}
	}
	if maxNum == 0 {
		return nil, nil
	}
	bindVars := make([]*ast.A_Const, maxNum)
	for num, pr := range refs {
		if synthetic[num] {
			continue
		}
		text, err := preparedstatement.DecodeBindAsText(portalInfo, pr, "set_config rewrite parameter")
		if err != nil {
			return nil, err
		}
		bindVars[num-1] = ast.NewA_Const(ast.NewString(text), 0)
	}
	return bindVars, nil
}

// stripResultFields wraps callback so each forwarded result carries no Fields —
// used on the extended path when a separate Describe already sent RowDescription.
func stripResultFields(callback func(context.Context, *sqltypes.Result) error) func(context.Context, *sqltypes.Result) error {
	return func(ctx context.Context, res *sqltypes.Result) error {
		if res != nil && res.Fields != nil {
			stripped := *res
			stripped.Fields = nil
			res = &stripped
		}
		return callback(ctx, res)
	}
}

// GetTableGroup returns the wrapped route's tablegroup.
func (r *GatewayManagedValueRoute) GetTableGroup() string {
	return r.route.GetTableGroup()
}

// GetQuery returns the wrapped route's SQL.
func (r *GatewayManagedValueRoute) GetQuery() string {
	return r.route.GetQuery()
}

// String returns a description for debugging.
func (r *GatewayManagedValueRoute) String() string {
	return fmt.Sprintf("GatewayManagedValueRoute(%s)", r.route.GetQuery())
}

// Ensure GatewayManagedValueRoute implements Primitive.
var _ Primitive = (*GatewayManagedValueRoute)(nil)
