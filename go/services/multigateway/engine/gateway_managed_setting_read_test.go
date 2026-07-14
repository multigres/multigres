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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// TestGatewayManagedValueRoute_SettingReads covers the current_setting fill: a read
// slot is filled from gateway connection state (the SHOW value), independent of any
// client bind, and the slice grows to fit a synthetic slot numbered past the binds.
func TestGatewayManagedValueRoute_SettingReads(t *testing.T) {
	state := &handler.MultigatewayConnectionState{}
	state.SetStatementTimeout(2 * time.Second)

	t.Run("read slot filled from gateway state", func(t *testing.T) {
		r := &GatewayManagedValueRoute{
			reads: []GatewayManagedSettingRead{{Param: 1, Name: "statement_timeout"}},
		}
		out, err := r.resolveSlots(state, nil)
		require.NoError(t, err)
		require.Len(t, out, 1)
		assert.Equal(t, "2s", extractConstValue(out[0]),
			"the read slot holds the gateway effective value, not the backend GUC")
	})

	t.Run("read slot numbered past a client bind", func(t *testing.T) {
		// $1 is a client bind; the current_setting slot is $2.
		r := &GatewayManagedValueRoute{
			reads: []GatewayManagedSettingRead{{Param: 2, Name: "statement_timeout"}},
		}
		out, err := r.resolveSlots(state, []*ast.A_Const{constText("client")})
		require.NoError(t, err)
		require.Len(t, out, 2, "the slice grows to fit the synthetic read slot")
		assert.Equal(t, "client", extractConstValue(out[0]), "the client bind is untouched")
		assert.Equal(t, "2s", extractConstValue(out[1]), "the read slot holds the gateway value")
	})

	t.Run("reads and set_config values coexist", func(t *testing.T) {
		// $1 client value (canonicalized set_config), $2 current_setting read.
		r := &GatewayManagedValueRoute{
			values: []GatewayManagedBoundValue{{Param: 1, SourceParam: 1, Name: "statement_timeout"}},
			reads:  []GatewayManagedSettingRead{{Param: 2, Name: "statement_timeout"}},
		}
		out, err := r.resolveSlots(state, []*ast.A_Const{constText("1000")})
		require.NoError(t, err)
		require.Len(t, out, 2)
		assert.Equal(t, "1s", extractConstValue(out[0]), "the set_config value is canonicalized")
		assert.Equal(t, "2s", extractConstValue(out[1]), "the current_setting read is filled from state")
	})
}

// TestGatewayManagedValueRoute_BindVarsFromPortal_SkipsReadSlot verifies the portal
// decode leaves a current_setting read slot nil — it appears in the routed AST as
// $N but is never a client bind, so resolveSlots (not the portal) fills it.
func TestGatewayManagedValueRoute_BindVarsFromPortal_SkipsReadSlot(t *testing.T) {
	// Routed AST after rewrite of `SELECT current_setting('statement_timeout'), $1`:
	// the read became $2, the client param $1 stays.
	r := &GatewayManagedValueRoute{
		route: routeForAST(t, "SELECT $2 AS current_setting, $1"),
		reads: []GatewayManagedSettingRead{{Param: 2, Name: "statement_timeout"}},
	}
	portalInfo := buildBoundPortalInfo(t,
		"SELECT current_setting('statement_timeout'), $1",
		[]uint32{uint32(ast.TEXTOID)}, [][]byte{[]byte("client")}, []int16{0})

	bindVars, err := r.bindVarsFromPortal(portalInfo)
	require.NoError(t, err)
	require.Len(t, bindVars, 2)
	assert.Equal(t, "client", extractConstValue(bindVars[0]), "the client's $1 is decoded")
	assert.Nil(t, bindVars[1], "the read slot $2 is not decoded from the portal — resolveSlots fills it")
}
