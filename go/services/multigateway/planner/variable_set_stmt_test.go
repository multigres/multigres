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
	p := NewPlanner("default", logger)
	testConn := server.NewTestConn(&bytes.Buffer{})

	stmt := &ast.VariableSetStmt{
		Kind: ast.VAR_SET_VALUE,
		Name: "work_mem",
		Args: &ast.NodeList{Items: []ast.Node{&ast.A_Const{Val: &ast.String{SVal: "256MB"}}}},
	}

	plan, err := p.planVariableSetStmt("SET work_mem = '256MB'", stmt, testConn.Conn)
	require.NoError(t, err)
	require.NotNil(t, plan)

	// The primitive should be an ApplySessionState
	_, ok := plan.Primitive.(*engine.ApplySessionState)
	assert.True(t, ok, "expected ApplySessionState primitive")
}

func TestPlanVariableSetStmt_RESET(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger)
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

func TestPlanVariableSetStmt_RESET_ALL(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger)
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
	p := NewPlanner("default", logger)
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
	p := NewPlanner("default", logger)
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

func TestPlanVariableSetStmt_SET_MULTI_PassesThrough(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger)
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
	p := NewPlanner("default", logger)
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
