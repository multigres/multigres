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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"
)

func constText(s string) *ast.A_Const {
	return ast.NewA_Const(ast.NewString(s), 0)
}

// TestGatewayManagedValueRoute_Canonicalize covers the two shapes the planner
// produces for a bound gateway-managed set_config value: reusing the value's own
// slot (Param == SourceParam) and routing it through a fresh synthetic slot
// (Param != SourceParam) when the source param is shared with another use.
func TestGatewayManagedValueRoute_Canonicalize(t *testing.T) {
	t.Run("in-place: reuse the value slot", func(t *testing.T) {
		r := &GatewayManagedValueRoute{
			values: []GatewayManagedBoundValue{{Param: 1, SourceParam: 1, Name: "statement_timeout"}},
		}
		out, err := r.resolveSlots(nil, []*ast.A_Const{constText("1000")})
		require.NoError(t, err)
		require.Len(t, out, 1)
		assert.Equal(t, "1s", extractConstValue(out[0]), "the value slot is canonicalized in place")
	})

	t.Run("synthetic slot: source param left untouched", func(t *testing.T) {
		// $1 is shared (used elsewhere), so the projection reads from a synthetic $2
		// canonicalized independently. $1 itself must be preserved for its other use.
		r := &GatewayManagedValueRoute{
			values: []GatewayManagedBoundValue{{Param: 2, SourceParam: 1, Name: "statement_timeout"}},
		}
		out, err := r.resolveSlots(nil, []*ast.A_Const{constText("1000")})
		require.NoError(t, err)
		require.Len(t, out, 2, "the slice grows to fit the synthetic slot")
		assert.Equal(t, "1000", extractConstValue(out[0]), "the shared source param is untouched")
		assert.Equal(t, "1s", extractConstValue(out[1]), "the synthetic slot holds the canonical value")
	})

	t.Run("no bound values: bindVars returned unchanged", func(t *testing.T) {
		r := &GatewayManagedValueRoute{}
		in := []*ast.A_Const{constText("x")}
		out, err := r.resolveSlots(nil, in)
		require.NoError(t, err)
		assert.Equal(t, in, out)
	})

	t.Run("invalid value: canonicalize surfaces the error", func(t *testing.T) {
		r := &GatewayManagedValueRoute{
			values: []GatewayManagedBoundValue{{Param: 1, SourceParam: 1, Name: "statement_timeout"}},
		}
		_, err := r.resolveSlots(nil, []*ast.A_Const{constText("not-a-duration")})
		require.Error(t, err, "an invalid value fails rather than leaking or being accepted")
	})

	t.Run("source out of range: internal error, no silent pass", func(t *testing.T) {
		r := &GatewayManagedValueRoute{
			values: []GatewayManagedBoundValue{{Param: 2, SourceParam: 5, Name: "statement_timeout"}},
		}
		_, err := r.resolveSlots(nil, []*ast.A_Const{constText("1000")})
		require.Error(t, err)
	})
}

func routeForAST(t *testing.T, sql string) *Route {
	t.Helper()
	stmts, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	return NewRoute("g", "s", sql, stmts[0])
}

// TestGatewayManagedValueRoute_BindVarsFromPortal exercises the portal-decode path
// for the shared-param (synthetic-slot) case — the highest-risk new branch, where a
// synthetic slot's SourceParam must be decoded from the portal even when it no
// longer appears in the routed AST, and the synthetic slot itself must NOT be
// decoded (the client never sent it).
func TestGatewayManagedValueRoute_BindVarsFromPortal(t *testing.T) {
	t.Run("synthetic slot: source stays in AST via its other use", func(t *testing.T) {
		// Routed AST after rewrite of `set_config('statement_timeout', $1, false), abs($1)`.
		r := &GatewayManagedValueRoute{
			route:  routeForAST(t, "SELECT $2 AS set_config, abs($1)"),
			values: []GatewayManagedBoundValue{{Param: 2, SourceParam: 1, Name: "statement_timeout"}},
		}
		// The portal is the original prepared statement: one client param, $1.
		portalInfo := buildBoundPortalInfo(t,
			"SELECT set_config('statement_timeout', $1, false), abs($1)",
			[]uint32{uint32(ast.TEXTOID)}, [][]byte{[]byte("1000")}, []int16{0})

		bindVars, err := r.bindVarsFromPortal(portalInfo)
		require.NoError(t, err)
		require.Len(t, bindVars, 2)
		assert.Equal(t, "1000", extractConstValue(bindVars[0]), "the client's $1 is decoded")
		assert.Nil(t, bindVars[1], "the synthetic $2 is not decoded from the portal — resolveSlots fills it")

		// End-to-end through resolveSlots: $2 gets the canonical value, $1 is preserved.
		out, err := r.resolveSlots(nil, bindVars)
		require.NoError(t, err)
		require.Len(t, out, 2)
		assert.Equal(t, "1000", extractConstValue(out[0]))
		assert.Equal(t, "1s", extractConstValue(out[1]))
	})

	t.Run("synthetic slots: source rewritten out of AST entirely", func(t *testing.T) {
		// Both uses of $1 are gateway-managed values, so $1 is gone from the routed
		// AST — but its value is still needed to canonicalize both synthetic slots.
		r := &GatewayManagedValueRoute{
			route: routeForAST(t, "SELECT $2 AS set_config, $3 AS set_config"),
			values: []GatewayManagedBoundValue{
				{Param: 2, SourceParam: 1, Name: "statement_timeout"},
				{Param: 3, SourceParam: 1, Name: "idle_session_timeout"},
			},
		}
		portalInfo := buildBoundPortalInfo(t,
			"SELECT set_config('statement_timeout', $1, false), set_config('idle_session_timeout', $1, false)",
			[]uint32{uint32(ast.TEXTOID)}, [][]byte{[]byte("1000")}, []int16{0})

		bindVars, err := r.bindVarsFromPortal(portalInfo)
		require.NoError(t, err)
		require.Len(t, bindVars, 3)
		assert.Equal(t, "1000", extractConstValue(bindVars[0]), "the source $1 is decoded even though it's absent from the routed AST")

		out, err := r.resolveSlots(nil, bindVars)
		require.NoError(t, err)
		require.Len(t, out, 3)
		assert.Equal(t, "1s", extractConstValue(out[1]), "statement_timeout canonicalized")
		assert.Equal(t, "1s", extractConstValue(out[2]), "idle_session_timeout canonicalized")
	})
}
