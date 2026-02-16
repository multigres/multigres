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

	"github.com/multigres/multigres/go/common/mterrors"
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

	// The primitive should be a SessionStateRoute
	_, ok := plan.Primitive.(*engine.SessionStateRoute)
	assert.True(t, ok, "expected SessionStateRoute primitive")
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

	_, ok := plan.Primitive.(*engine.SessionStateRoute)
	assert.True(t, ok, "expected SessionStateRoute primitive")
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

	_, ok := plan.Primitive.(*engine.SessionStateRoute)
	assert.True(t, ok, "expected SessionStateRoute primitive")
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

	// SET LOCAL should produce a plain Route, not SessionStateRoute
	_, ok := plan.Primitive.(*engine.SessionStateRoute)
	assert.False(t, ok, "SET LOCAL should not produce SessionStateRoute")
}

func TestPlanVariableSetStmt_UnsupportedKind(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger)
	testConn := server.NewTestConn(&bytes.Buffer{})

	stmt := &ast.VariableSetStmt{
		Kind: ast.VAR_SET_DEFAULT,
		Name: "work_mem",
	}

	_, err := p.planVariableSetStmt("SET work_mem TO DEFAULT", stmt, testConn.Conn)
	require.Error(t, err)

	var pgDiag *mterrors.PgDiagnostic
	require.ErrorAs(t, err, &pgDiag)
	assert.Equal(t, "0A000", pgDiag.Code)
}
