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
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// collectCallback returns a callback that appends results to the given slice.
func collectCallback(results *[]*sqltypes.Result) func(context.Context, *sqltypes.Result) error {
	return func(_ context.Context, r *sqltypes.Result) error {
		*results = append(*results, r)
		return nil
	}
}

func TestApplySessionState_SET_UpdatesStateAndReturnsSynthetic(t *testing.T) {
	testConn := server.NewTestConn(&bytes.Buffer{})
	state := &handler.MultiGatewayConnectionState{}
	ctx := context.Background()

	stmt := &ast.VariableSetStmt{
		Kind: ast.VAR_SET_VALUE,
		Name: "work_mem",
		Args: &ast.NodeList{Items: []ast.Node{&ast.A_Const{Val: &ast.String{SVal: "256MB"}}}},
	}

	ssr := NewApplySessionState("SET work_mem = '256MB'", stmt)

	var results []*sqltypes.Result
	err := ssr.StreamExecute(ctx, nil, testConn.Conn, state, nil, collectCallback(&results))
	require.NoError(t, err)

	val, exists := state.GetSessionVariable("work_mem")
	assert.True(t, exists)
	assert.Equal(t, "256MB", val)

	// Should receive synthetic CommandComplete with SET tag
	require.Len(t, results, 1)
	assert.Equal(t, "SET", results[0].CommandTag)
}

func TestApplySessionState_SET_InvalidParam_Succeeds(t *testing.T) {
	// Invalid params are accepted locally — errors surface on next query.
	testConn := server.NewTestConn(&bytes.Buffer{})
	state := &handler.MultiGatewayConnectionState{}
	ctx := context.Background()

	stmt := &ast.VariableSetStmt{
		Kind: ast.VAR_SET_VALUE,
		Name: "totally_invalid_variable",
		Args: &ast.NodeList{Items: []ast.Node{&ast.A_Const{Val: &ast.String{SVal: "whatever"}}}},
	}

	ssr := NewApplySessionState("SET totally_invalid_variable = 'whatever'", stmt)

	var results []*sqltypes.Result
	err := ssr.StreamExecute(ctx, nil, testConn.Conn, state, nil, collectCallback(&results))
	require.NoError(t, err, "SET with invalid param should succeed locally")

	val, exists := state.GetSessionVariable("totally_invalid_variable")
	assert.True(t, exists)
	assert.Equal(t, "whatever", val)

	require.Len(t, results, 1)
	assert.Equal(t, "SET", results[0].CommandTag)
}

func TestApplySessionState_RESET_NeverSetVariable(t *testing.T) {
	// RESET of a variable that was never SET should succeed (matches PostgreSQL behaviour).
	state := &handler.MultiGatewayConnectionState{}
	testConn := server.NewTestConn(&bytes.Buffer{})
	ctx := context.Background()

	stmt := &ast.VariableSetStmt{
		Kind: ast.VAR_RESET,
		Name: "never_set_var",
	}

	ssr := NewApplySessionState("RESET never_set_var", stmt)

	var results []*sqltypes.Result
	err := ssr.StreamExecute(ctx, nil, testConn.Conn, state, nil, collectCallback(&results))
	require.NoError(t, err, "RESET of never-set variable should succeed")

	_, exists := state.GetSessionVariable("never_set_var")
	assert.False(t, exists)

	require.Len(t, results, 1)
	assert.Equal(t, "RESET", results[0].CommandTag)
}

func TestApplySessionState_RESET_UpdatesStateAndReturnsSynthetic(t *testing.T) {
	state := &handler.MultiGatewayConnectionState{}
	state.SetSessionVariable("work_mem", "256MB")

	testConn := server.NewTestConn(&bytes.Buffer{})
	ctx := context.Background()

	stmt := &ast.VariableSetStmt{
		Kind: ast.VAR_RESET,
		Name: "work_mem",
	}

	ssr := NewApplySessionState("RESET work_mem", stmt)

	var results []*sqltypes.Result
	err := ssr.StreamExecute(ctx, nil, testConn.Conn, state, nil, collectCallback(&results))
	require.NoError(t, err)

	// Variable should be removed
	_, exists := state.GetSessionVariable("work_mem")
	assert.False(t, exists, "variable should be removed after RESET")

	// Should receive synthetic CommandComplete
	require.Len(t, results, 1)
	assert.Equal(t, "RESET", results[0].CommandTag)
}

func TestApplySessionState_RESET_ALL_ClearsAllVariables(t *testing.T) {
	state := &handler.MultiGatewayConnectionState{}
	state.SetSessionVariable("work_mem", "256MB")
	state.SetSessionVariable("search_path", "myschema")
	state.SetSessionVariable("statement_timeout", "30s")

	testConn := server.NewTestConn(&bytes.Buffer{})
	ctx := context.Background()

	stmt := &ast.VariableSetStmt{
		Kind: ast.VAR_RESET_ALL,
	}

	ssr := NewApplySessionState("RESET ALL", stmt)

	var results []*sqltypes.Result
	err := ssr.StreamExecute(ctx, nil, testConn.Conn, state, nil, collectCallback(&results))
	require.NoError(t, err)

	// All variables should be cleared
	_, exists := state.GetSessionVariable("work_mem")
	assert.False(t, exists)
	_, exists = state.GetSessionVariable("search_path")
	assert.False(t, exists)
	_, exists = state.GetSessionVariable("statement_timeout")
	assert.False(t, exists)

	// Should receive synthetic CommandComplete
	require.Len(t, results, 1)
	assert.Equal(t, "RESET", results[0].CommandTag)
}

func TestApplySessionState_UnsupportedKind(t *testing.T) {
	testConn := server.NewTestConn(&bytes.Buffer{})
	state := &handler.MultiGatewayConnectionState{}
	ctx := context.Background()

	stmt := &ast.VariableSetStmt{
		Kind: ast.VAR_SET_DEFAULT,
		Name: "work_mem",
	}

	ssr := NewApplySessionState("SET work_mem TO DEFAULT", stmt)

	var results []*sqltypes.Result
	err := ssr.StreamExecute(ctx, nil, testConn.Conn, state, nil, collectCallback(&results))
	require.Error(t, err)

	var pgDiag *mterrors.PgDiagnostic
	require.ErrorAs(t, err, &pgDiag)
	assert.Equal(t, "0A000", pgDiag.Code)
}

func TestApplySessionState_GetTableGroup(t *testing.T) {
	stmt := &ast.VariableSetStmt{Kind: ast.VAR_SET_VALUE, Name: "x"}
	ssr := NewApplySessionState("SET x = 1", stmt)
	assert.Equal(t, "", ssr.GetTableGroup(), "SET/RESET are local-only, no tablegroup")
}

func TestApplySessionState_GetQuery(t *testing.T) {
	stmt := &ast.VariableSetStmt{Kind: ast.VAR_SET_VALUE, Name: "x"}
	ssr := NewApplySessionState("SET x = 1", stmt)
	assert.Equal(t, "SET x = 1", ssr.GetQuery())
}

func TestApplySessionState_String(t *testing.T) {
	stmt := &ast.VariableSetStmt{
		Kind: ast.VAR_SET_VALUE,
		Name: "work_mem",
		Args: &ast.NodeList{Items: []ast.Node{&ast.A_Const{Val: &ast.String{SVal: "256MB"}}}},
	}
	ssr := NewApplySessionState("SET work_mem = '256MB'", stmt)
	result := ssr.String()
	assert.Contains(t, result, "ApplySessionState")
}

func TestExtractVariableValue(t *testing.T) {
	tests := []struct {
		name     string
		args     *ast.NodeList
		expected string
	}{
		{
			name:     "nil args",
			args:     nil,
			expected: "",
		},
		{
			name:     "empty args",
			args:     &ast.NodeList{},
			expected: "",
		},
		{
			name: "string constant",
			args: &ast.NodeList{Items: []ast.Node{
				&ast.A_Const{Val: &ast.String{SVal: "myschema"}},
			}},
			expected: "myschema",
		},
		{
			name: "integer constant",
			args: &ast.NodeList{Items: []ast.Node{
				&ast.A_Const{Val: &ast.Integer{IVal: 42}},
			}},
			expected: "42",
		},
		{
			name: "float constant",
			args: &ast.NodeList{Items: []ast.Node{
				&ast.A_Const{Val: &ast.Float{FVal: "3.14"}},
			}},
			expected: "3.14",
		},
		{
			name: "bare string node",
			args: &ast.NodeList{Items: []ast.Node{
				&ast.String{SVal: "public"},
			}},
			expected: "public",
		},
		{
			name: "bare integer node",
			args: &ast.NodeList{Items: []ast.Node{
				&ast.Integer{IVal: 7},
			}},
			expected: "7",
		},
		{
			name: "multiple values joined with comma",
			args: &ast.NodeList{Items: []ast.Node{
				&ast.String{SVal: "public"},
				&ast.String{SVal: "pg_catalog"},
			}},
			expected: "public, pg_catalog",
		},
		{
			name: "fallback node uses SqlString",
			args: &ast.NodeList{Items: []ast.Node{
				&ast.Float{FVal: "2.5"},
			}},
			expected: "2.5",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := extractVariableValue(tc.args)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestExtractConstValue(t *testing.T) {
	tests := []struct {
		name     string
		input    *ast.A_Const
		expected string
	}{
		{
			name:     "nil const",
			input:    nil,
			expected: "",
		},
		{
			name:     "nil val",
			input:    &ast.A_Const{Val: nil},
			expected: "",
		},
		{
			name:     "string val",
			input:    &ast.A_Const{Val: &ast.String{SVal: "hello"}},
			expected: "hello",
		},
		{
			name:     "integer val",
			input:    &ast.A_Const{Val: &ast.Integer{IVal: 99}},
			expected: "99",
		},
		{
			name:     "float val",
			input:    &ast.A_Const{Val: &ast.Float{FVal: "1.5"}},
			expected: "1.5",
		},
		{
			name:     "fallback val uses SqlString",
			input:    &ast.A_Const{Val: &ast.Boolean{BoolVal: true}},
			expected: "TRUE",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := extractConstValue(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}
