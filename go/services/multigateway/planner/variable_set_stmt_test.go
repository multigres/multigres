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
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/services/multigateway/engine"
)

func TestPlanVariableSetStmt_SET(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	stmt := &ast.VariableSetStmt{
		Kind: ast.VAR_SET_VALUE,
		Name: "work_mem",
		Args: &ast.NodeList{Items: []ast.Node{&ast.A_Const{Val: &ast.String{SVal: "256MB"}}}},
	}

	plan, err := p.planVariableSetStmt("SET work_mem = '256MB'", stmt, testConn.Conn)
	require.NoError(t, err)
	require.NotNil(t, plan)

	// SET var = value is validated on a backend then tracked locally, so the
	// plan is Sequence[ValidateSetting, ApplySessionState].
	seq, ok := plan.Primitive.(*engine.Sequence)
	require.True(t, ok, "expected Sequence primitive, got %T", plan.Primitive)
	require.Len(t, seq.Primitives, 2, "expected [ValidateSetting, ApplySessionState]")
	_, ok = seq.Primitives[0].(*engine.ValidateSetting)
	assert.True(t, ok, "first primitive should be ValidateSetting (validate on backend), got %T", seq.Primitives[0])
	_, ok = seq.Primitives[1].(*engine.ApplySessionState)
	assert.True(t, ok, "second primitive should be ApplySessionState (track + emit SET), got %T", seq.Primitives[1])
}

func TestPlanVariableSetStmt_RESET(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	stmt := &ast.VariableSetStmt{
		Kind: ast.VAR_RESET,
		Name: "work_mem",
	}

	plan, err := p.planVariableSetStmt("RESET work_mem", stmt, testConn.Conn)
	require.NoError(t, err)
	require.NotNil(t, plan)

	_, ok := plan.Primitive.(*engine.ApplySessionState)
	assert.True(t, ok, "expected ApplySessionState primitive")
}

func TestPlanVariableSetStmt_TransactionOnlyVariablesPassThrough(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	tests := []struct {
		name string
		sql  string
	}{
		{name: "RESET transaction_isolation", sql: "RESET transaction_isolation"},
		{name: "RESET transaction_read_only", sql: "RESET transaction_read_only"},
		{name: "RESET transaction_deferrable", sql: "RESET transaction_deferrable"},
		{name: "SET TRANSACTION SNAPSHOT", sql: "SET TRANSACTION SNAPSHOT 'FFF-FFF-F'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan, err := planPortal(t, p, testConn.Conn, tt.sql)
			require.NoError(t, err)
			require.NotNil(t, plan)
			_, ok := plan.Primitive.(*engine.Route)
			assert.True(t, ok, "transaction-only variable must route to PostgreSQL, got %T", plan.Primitive)
		})
	}
}

func TestPlanVariableSetStmt_RESET_ALL(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	stmt := &ast.VariableSetStmt{
		Kind: ast.VAR_RESET_ALL,
	}

	plan, err := p.planVariableSetStmt("RESET ALL", stmt, testConn.Conn)
	require.NoError(t, err)
	require.NotNil(t, plan)

	_, ok := plan.Primitive.(*engine.ApplySessionState)
	assert.True(t, ok, "expected ApplySessionState primitive")
}

func TestPlanVariableSetStmt_SET_LOCAL_PassesThrough(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	stmt := &ast.VariableSetStmt{
		Kind:    ast.VAR_SET_VALUE,
		Name:    "work_mem",
		IsLocal: true,
		Args:    &ast.NodeList{Items: []ast.Node{&ast.A_Const{Val: &ast.String{SVal: "256MB"}}}},
	}

	plan, err := p.planVariableSetStmt("SET LOCAL work_mem = '256MB'", stmt, testConn.Conn)
	require.NoError(t, err)
	require.NotNil(t, plan)

	// SET LOCAL should produce a plain Route, not ApplySessionState
	_, ok := plan.Primitive.(*engine.ApplySessionState)
	assert.False(t, ok, "SET LOCAL should not produce ApplySessionState")
}

func TestPlanVariableSetStmt_SET_DEFAULT_TreatedAsReset(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	stmt := &ast.VariableSetStmt{
		Kind: ast.VAR_SET_DEFAULT,
		Name: "work_mem",
	}

	plan, err := p.planVariableSetStmt("SET work_mem TO DEFAULT", stmt, testConn.Conn)
	require.NoError(t, err)
	require.NotNil(t, plan)

	// VAR_SET_DEFAULT should produce an ApplySessionState with RESET kind
	prim, ok := plan.Primitive.(*engine.ApplySessionState)
	assert.True(t, ok, "expected ApplySessionState primitive")
	assert.Equal(t, ast.VAR_RESET, prim.VariableStmt.Kind)
}

func TestPlanVariableSetStmt_SET_TIME_ZONE_DEFAULT_TreatedAsReset(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	stmt := &ast.VariableSetStmt{
		Kind: ast.VAR_SET_DEFAULT,
		Name: "timezone",
	}

	plan, err := p.planVariableSetStmt("SET TIME ZONE DEFAULT", stmt, testConn.Conn)
	require.NoError(t, err)
	require.NotNil(t, plan)

	prim, ok := plan.Primitive.(*engine.ApplySessionState)
	assert.True(t, ok, "expected ApplySessionState primitive")
	assert.Equal(t, ast.VAR_RESET, prim.VariableStmt.Kind)
	assert.Equal(t, "timezone", prim.VariableStmt.Name)
}

func TestPlanVariableSetStmt_SET_MULTI_PassesThrough(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	stmt := &ast.VariableSetStmt{
		Kind: ast.VAR_SET_MULTI,
		Name: "TRANSACTION",
	}

	plan, err := p.planVariableSetStmt("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", stmt, testConn.Conn)
	require.NoError(t, err)
	require.NotNil(t, plan)

	// SET TRANSACTION should pass through to PG (Route), not be handled locally
	_, ok := plan.Primitive.(*engine.ApplySessionState)
	assert.False(t, ok, "SET TRANSACTION should not produce ApplySessionState")
}

func TestPlanVariableSetStmt_SET_CURRENT_PassesThrough(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	stmt := &ast.VariableSetStmt{
		Kind: ast.VAR_SET_CURRENT,
		Name: "search_path",
	}

	plan, err := p.planVariableSetStmt("SET search_path FROM CURRENT", stmt, testConn.Conn)
	require.NoError(t, err)
	require.NotNil(t, plan)

	// SET FROM CURRENT should pass through to PG (Route)
	_, ok := plan.Primitive.(*engine.ApplySessionState)
	assert.False(t, ok, "SET FROM CURRENT should not produce ApplySessionState")
}

// TestPlanPortal_SET pins that the extended-protocol path plans SET/RESET the
// same way the simple protocol does: plain SET validates + tracks (Sequence),
// RESET tracks locally, and SET LOCAL / SET TRANSACTION route as a plain Route
// (which reissues the portal to the authoritative backend). Producing a Sequence
// for a plain SET — rather than a bare Route — is what keeps a raw SET from
// mutating a pooled backend outside multipooler's tracking and skipping
// pool-rotation replay.
func TestPlanPortal_SET(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	t.Run("plain SET is planned (validate + track)", func(t *testing.T) {
		plan, err := planPortal(t, p, testConn.Conn, "SET work_mem = '256MB'")
		require.NoError(t, err)
		require.NotNil(t, plan, "non-gateway SET must be planned, not forwarded raw to a pooled backend")
		seq, ok := plan.Primitive.(*engine.Sequence)
		require.True(t, ok, "expected Sequence, got %T", plan.Primitive)
		require.Len(t, seq.Primitives, 2)
		_, ok = seq.Primitives[0].(*engine.ValidateSetting)
		assert.True(t, ok, "first primitive should be ValidateSetting, got %T", seq.Primitives[0])
	})

	t.Run("RESET is planned", func(t *testing.T) {
		plan, err := planPortal(t, p, testConn.Conn, "RESET work_mem")
		require.NoError(t, err)
		require.NotNil(t, plan, "RESET must be planned so it clears local tracking")
		_, ok := plan.Primitive.(*engine.ApplySessionState)
		assert.True(t, ok, "expected ApplySessionState, got %T", plan.Primitive)
	})

	t.Run("SET LOCAL routes to PG", func(t *testing.T) {
		plan, err := planPortal(t, p, testConn.Conn, "SET LOCAL work_mem = '256MB'")
		require.NoError(t, err)
		require.NotNil(t, plan)
		_, ok := plan.Primitive.(*engine.Route)
		assert.True(t, ok, "SET LOCAL must route as a plain Route to the authoritative backend, got %T", plan.Primitive)
	})

	t.Run("SET TRANSACTION routes to PG", func(t *testing.T) {
		plan, err := planPortal(t, p, testConn.Conn, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
		require.NoError(t, err)
		require.NotNil(t, plan)
		_, ok := plan.Primitive.(*engine.Route)
		assert.True(t, ok, "SET TRANSACTION must route as a plain Route to the backend, got %T", plan.Primitive)
	})
}
