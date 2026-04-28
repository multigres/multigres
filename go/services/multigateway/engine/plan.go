// Copyright 2025 Supabase, Inc.
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

	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// Plan type constants identify the root primitive for observability.
const (
	PlanTypeRoute               = "Route"
	PlanTypeTransaction         = "Transaction"
	PlanTypeCopyStatement       = "CopyStatement"
	PlanTypeApplySessionState   = "ApplySessionState"
	PlanTypeGatewaySessionState = "GatewaySessionState"
	PlanTypeGatewayShowVariable = "GatewayShowVariable"
	PlanTypeListenNotify        = "ListenNotify"
	PlanTypeSequence            = "Sequence"
	PlanTypeTempTableRoute      = "TempTableRoute"
	PlanTypeUnknown             = "Unknown"
)

// Plan represents a query execution plan.
// It contains the root primitive and metadata about the query.
type Plan struct {
	// Original is the original SQL query string.
	Original string

	// Primitive is the root execution primitive.
	// In Phase 1, this will always be a Route primitive.
	Primitive Primitive

	// TablesUsed contains deduplicated, schema-qualified table names
	// referenced by this query. Nil for statements that don't reference
	// tables (SET, SHOW, BEGIN, etc.).
	TablesUsed []string

	// Type is the name of the root primitive (e.g. "Route", "Transaction").
	// Used for observability: span attributes and structured query logs.
	Type string
}

// NewPlan creates a new query plan.
func NewPlan(original string, primitive Primitive) *Plan {
	return &Plan{
		Original:  original,
		Primitive: primitive,
	}
}

// StreamExecute executes the plan by calling the root primitive's StreamExecute.
// bindVars contains literal values extracted during normalization; nil for non-cached paths.
func (p *Plan) StreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	bindVars []*ast.A_Const,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	return p.Primitive.StreamExecute(ctx, exec, conn, state, bindVars, callback)
}

// PortalStreamExecute executes the plan on the extended-protocol portal path.
// Delegates to the root primitive so each primitive owns its own portal-mode
// behavior — Route forwards the portal to the backend, Sequence iterates,
// gateway-local primitives just delegate to StreamExecute with no binds.
func (p *Plan) PortalStreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	portalInfo *preparedstatement.PortalInfo,
	maxRows int32,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	return p.Primitive.PortalStreamExecute(ctx, exec, conn, state, portalInfo, maxRows, callback)
}

// GetTableGroup returns the target tablegroup from the primitive.
func (p *Plan) GetTableGroup() string {
	return p.Primitive.GetTableGroup()
}

// CachedSize returns the approximate memory cost of this plan in bytes.
// Used by the theine cache to enforce memory-based capacity limits.
// Can be refined to return actual byte size.
// TODO: Generate cached size
func (p *Plan) CachedSize(_ bool) int64 {
	// Plan struct overhead + pointer/interface/slice headers.
	size := int64(256)
	size += int64(len(p.Original))
	size += int64(len(p.Type))
	for _, t := range p.TablesUsed {
		size += int64(len(t)) + 16 // string header + content
	}
	if r, ok := p.Primitive.(*Route); ok {
		size += int64(len(r.Query)) + int64(len(r.TableGroup)) + int64(len(r.Shard))
		// NormalizedAST is a cloned AST tree. Rough estimate: ~10x the query
		// string length accounts for node structs, pointers, and metadata.
		if r.NormalizedAST != nil {
			size += int64(len(r.Query)) * 10
		}
	}
	return size
}

// String returns a string representation of the plan for debugging.
func (p *Plan) String() string {
	return fmt.Sprintf("Plan{original=%q, primitive=%s}", p.Original, p.Primitive.String())
}
