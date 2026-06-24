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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
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
	state := &handler.MultigatewayConnectionState{}
	ctx := context.Background()

	stmt := &ast.VariableSetStmt{
		Kind: ast.VAR_SET_VALUE,
		Name: "work_mem",
		Args: &ast.NodeList{Items: []ast.Node{&ast.A_Const{Val: &ast.String{SVal: "256MB"}}}},
	}

	ssr := NewApplySessionState("SET work_mem = '256MB'", stmt)

	var results []*sqltypes.Result
	err := ssr.StreamExecute(ctx, nil, testConn.Conn, state, nil, PlanExecInfo{}, collectCallback(&results))
	require.NoError(t, err)

	val, exists := state.GetSessionVariable("work_mem")
	assert.True(t, exists)
	assert.Equal(t, "256MB", val)

	// Should receive synthetic CommandComplete with SET tag
	require.Len(t, results, 1)
	assert.Equal(t, "SET", results[0].CommandTag)
}

func TestApplySessionState_RoleSessionAuthorizationTracking(t *testing.T) {
	testConn := server.NewTestConn(&bytes.Buffer{})
	state := &handler.MultigatewayConnectionState{}
	ctx := context.Background()

	setRole := NewApplySessionState("SET ROLE child", &ast.VariableSetStmt{
		Kind: ast.VAR_SET_VALUE,
		Name: "role",
		Args: &ast.NodeList{Items: []ast.Node{&ast.A_Const{Val: &ast.String{SVal: "child"}}}},
	})
	var results []*sqltypes.Result
	require.NoError(t, setRole.StreamExecute(ctx, nil, testConn.Conn, state, nil, PlanExecInfo{}, collectCallback(&results)))
	role, ok := state.GetSessionVariable("role")
	require.True(t, ok)
	require.Equal(t, "child", role)

	setSessionAuth := NewApplySessionState("SET SESSION AUTHORIZATION parent", &ast.VariableSetStmt{
		Kind: ast.VAR_SET_VALUE,
		Name: "session_authorization",
		Args: &ast.NodeList{Items: []ast.Node{&ast.A_Const{Val: &ast.String{SVal: "parent"}}}},
	})
	require.NoError(t, setSessionAuth.StreamExecute(ctx, nil, testConn.Conn, state, nil, PlanExecInfo{}, collectCallback(&results)))
	sessionAuth, ok := state.GetSessionVariable("session_authorization")
	require.True(t, ok)
	require.Equal(t, "parent", sessionAuth)
	_, ok = state.GetSessionVariable("role")
	require.False(t, ok, "SET SESSION AUTHORIZATION resets any prior SET ROLE")
}

func TestApplySessionState_RoleValueNoneResetsTrackedRole(t *testing.T) {
	testConn := server.NewTestConn(&bytes.Buffer{})
	state := &handler.MultigatewayConnectionState{}
	ctx := context.Background()

	setRoleNone := NewApplySessionState("SET ROLE 'none'", &ast.VariableSetStmt{
		Kind: ast.VAR_SET_VALUE,
		Name: "role",
		Args: &ast.NodeList{Items: []ast.Node{&ast.A_Const{Val: &ast.String{SVal: "none"}}}},
	})
	var results []*sqltypes.Result
	require.NoError(t, setRoleNone.StreamExecute(ctx, nil, testConn.Conn, state, nil, PlanExecInfo{}, collectCallback(&results)))

	_, ok := state.GetSessionVariable("role")
	require.False(t, ok, "PostgreSQL treats role value \"none\" as RESET ROLE")
	require.Len(t, results, 1)
	require.Equal(t, "SET", results[0].CommandTag)
}

func TestResolveTrackSetConfig_RoleSessionAuthorizationSemantics(t *testing.T) {
	state := &handler.MultigatewayConnectionState{}
	state.SetSessionVariable("role", "child")
	conn := server.NewTestConn(&bytes.Buffer{}).Conn

	resolver := &ResolveTrackSetConfig{Aliases: []string{"set_config"}}
	actions, err := resolver.prepareTrackActions(conn, state, []*sqltypes.Row{{Values: []sqltypes.Value{
		[]byte("session_authorization"), []byte("parent"), []byte("false"),
	}}})
	require.NoError(t, err)
	for _, action := range actions {
		action()
	}

	sessionAuth, ok := state.GetSessionVariable("session_authorization")
	require.True(t, ok)
	require.Equal(t, "parent", sessionAuth)
	_, ok = state.GetSessionVariable("role")
	require.False(t, ok, "set_config('session_authorization', ...) clears active role")

	state.SetSessionVariable("role", "child")
	actions, err = resolver.prepareTrackActions(conn, state, []*sqltypes.Row{{Values: []sqltypes.Value{
		[]byte("role"), []byte("none"), []byte("false"),
	}}})
	require.NoError(t, err)
	for _, action := range actions {
		action()
	}
	_, ok = state.GetSessionVariable("role")
	require.False(t, ok, "set_config('role', 'none', false) resets role")
}

func TestApplySessionState_SetRoleDefaultResetsTrackedRole(t *testing.T) {
	testConn := server.NewTestConn(&bytes.Buffer{})
	state := &handler.MultigatewayConnectionState{}
	state.SetSessionVariable("role", "child")

	setRoleDefault := NewApplySessionState("SET ROLE DEFAULT", &ast.VariableSetStmt{
		Kind: ast.VAR_SET_DEFAULT,
		Name: "role",
	})
	var results []*sqltypes.Result
	require.NoError(t, setRoleDefault.StreamExecute(context.Background(), nil, testConn.Conn, state, nil, PlanExecInfo{}, collectCallback(&results)))

	_, ok := state.GetSessionVariable("role")
	require.False(t, ok)
	require.Len(t, results, 1)
	require.Equal(t, "SET", results[0].CommandTag)
}

func TestApplySessionState_ResetAllPreservesRoleSessionAuthorization(t *testing.T) {
	testConn := server.NewTestConn(&bytes.Buffer{})
	state := &handler.MultigatewayConnectionState{}
	state.InitStatementTimeout(30 * time.Second)
	state.SetSessionVariable("session_authorization", "parent")
	state.SetSessionVariable("role", "child")
	state.SetSessionVariable("search_path", "public")
	state.SetStatementTimeout(5 * time.Second)

	resetAll := NewApplySessionState("RESET ALL", &ast.VariableSetStmt{Kind: ast.VAR_RESET_ALL})
	var results []*sqltypes.Result
	require.NoError(t, resetAll.StreamExecute(context.Background(), nil, testConn.Conn, state, nil, PlanExecInfo{}, collectCallback(&results)))

	sessionAuth, ok := state.GetSessionVariable("session_authorization")
	require.True(t, ok)
	require.Equal(t, "parent", sessionAuth)
	role, ok := state.GetSessionVariable("role")
	require.True(t, ok)
	require.Equal(t, "child", role)
	_, ok = state.GetSessionVariable("search_path")
	require.False(t, ok)
	require.Equal(t, 30*time.Second, state.GetStatementTimeout(), "RESET ALL resets gateway-managed variables")
	require.Len(t, results, 1)
	require.Equal(t, "RESET", results[0].CommandTag)
}

func TestApplySessionState_ResetSessionAuthorizationClearsRole(t *testing.T) {
	testConn := server.NewTestConn(&bytes.Buffer{})
	state := &handler.MultigatewayConnectionState{}
	state.SetSessionVariable("session_authorization", "parent")
	state.SetSessionVariable("role", "child")

	resetSessionAuth := NewApplySessionState("RESET SESSION AUTHORIZATION", &ast.VariableSetStmt{
		Kind: ast.VAR_RESET,
		Name: "session_authorization",
	})
	var results []*sqltypes.Result
	require.NoError(t, resetSessionAuth.StreamExecute(context.Background(), nil, testConn.Conn, state, nil, PlanExecInfo{}, collectCallback(&results)))

	_, ok := state.GetSessionVariable("session_authorization")
	require.False(t, ok)
	_, ok = state.GetSessionVariable("role")
	require.False(t, ok)
	require.Len(t, results, 1)
	require.Equal(t, "RESET", results[0].CommandTag)
}

// TestApplySessionState_SetConfig_GatewayManagedRoutesToGatewayState verifies
// set_config of a gateway-managed variable: it must update gateway-local
// state (visible via SHOW) and must NOT land in SessionSettings, otherwise it
// would be replayed to backends on pool rotation. Mirrors the silent
// ApplySessionState the planner builds for
// `SELECT set_config('statement_timeout', '5s', false)`.
func TestApplySessionState_SetConfig_GatewayManagedRoutesToGatewayState(t *testing.T) {
	testConn := server.NewTestConn(&bytes.Buffer{})
	state := &handler.MultigatewayConnectionState{}
	ctx := context.Background()

	stmt := &ast.VariableSetStmt{
		Kind: ast.VAR_SET_VALUE,
		Name: "statement_timeout",
		Args: &ast.NodeList{Items: []ast.Node{&ast.A_Const{Val: &ast.String{SVal: "5s"}}}},
	}
	ssr := NewApplySessionStateSilent("SELECT set_config('statement_timeout', '5s', false)", stmt)

	var results []*sqltypes.Result
	err := ssr.StreamExecute(ctx, nil, testConn.Conn, state, nil, PlanExecInfo{}, collectCallback(&results))
	require.NoError(t, err)

	// Silent: a sibling Route owns the client-facing result.
	assert.Empty(t, results)
	// Routed to gateway-managed state.
	assert.Equal(t, 5*time.Second, state.GetStatementTimeout())
	// NOT written to SessionSettings (would be replayed on pool rotation).
	_, exists := state.GetSessionVariable("statement_timeout")
	assert.False(t, exists)
	assert.Nil(t, state.GetSessionSettings())
}

// TestApplySessionState_SetConfig_GatewayManagedLocalInTransaction verifies that
// set_config('<gmv>', v, true) inside a transaction applies a transaction-local
// gateway override (parity with SET LOCAL <gmv>), visible via SHOW and kept out
// of SessionSettings.
func TestApplySessionState_SetConfig_GatewayManagedLocalInTransaction(t *testing.T) {
	testConn := server.NewTestConn(&bytes.Buffer{})
	testConn.Conn.SetTxnStatus(protocol.TxnStatusInBlock)
	state := &handler.MultigatewayConnectionState{}

	stmt := &ast.VariableSetStmt{
		Kind:    ast.VAR_SET_VALUE,
		Name:    "statement_timeout",
		IsLocal: true,
		Args:    &ast.NodeList{Items: []ast.Node{&ast.A_Const{Val: &ast.String{SVal: "7s"}}}},
	}
	ssr := NewApplySessionStateSilent("SELECT set_config('statement_timeout', '7s', true)", stmt)

	var results []*sqltypes.Result
	err := ssr.StreamExecute(context.Background(), nil, testConn.Conn, state, nil, PlanExecInfo{}, collectCallback(&results))
	require.NoError(t, err)
	assert.Empty(t, results)
	assert.Equal(t, 7*time.Second, state.GetStatementTimeout())
	_, exists := state.GetSessionVariable("statement_timeout")
	assert.False(t, exists)
}

// TestApplySessionState_SetConfig_GatewayManagedLocalOutsideTxnIsNoOp verifies
// that set_config('<gmv>', v, true) outside a transaction does NOT apply a
// gateway override. PostgreSQL scopes such a change to the implicit
// single-statement transaction, and applying it to gateway state would leak it
// for the connection's lifetime (no COMMIT/ROLLBACK fires to clear it).
func TestApplySessionState_SetConfig_GatewayManagedLocalOutsideTxnIsNoOp(t *testing.T) {
	testConn := server.NewTestConn(&bytes.Buffer{}) // idle: not in a transaction
	state := &handler.MultigatewayConnectionState{}
	state.InitStatementTimeout(30 * time.Second)

	stmt := &ast.VariableSetStmt{
		Kind:    ast.VAR_SET_VALUE,
		Name:    "statement_timeout",
		IsLocal: true,
		Args:    &ast.NodeList{Items: []ast.Node{&ast.A_Const{Val: &ast.String{SVal: "7s"}}}},
	}
	ssr := NewApplySessionStateSilent("SELECT set_config('statement_timeout', '7s', true)", stmt)

	var results []*sqltypes.Result
	err := ssr.StreamExecute(context.Background(), nil, testConn.Conn, state, nil, PlanExecInfo{}, collectCallback(&results))
	require.NoError(t, err)
	assert.Equal(t, 30*time.Second, state.GetStatementTimeout(),
		"local override must not be applied/leaked outside a transaction")
	_, exists := state.GetSessionVariable("statement_timeout")
	assert.False(t, exists)
}

// TestApplySessionState_SetConfig_StatementTimeoutInvalidErrors confirms an
// invalid gateway-managed value surfaces an error when the tracker executes,
// matching PostgreSQL's set-time check.
func TestApplySessionState_SetConfig_StatementTimeoutInvalidErrors(t *testing.T) {
	testConn := server.NewTestConn(&bytes.Buffer{})
	state := &handler.MultigatewayConnectionState{}

	stmt := &ast.VariableSetStmt{
		Kind: ast.VAR_SET_VALUE,
		Name: "statement_timeout",
		Args: &ast.NodeList{Items: []ast.Node{&ast.A_Const{Val: &ast.String{SVal: "not-a-duration"}}}},
	}
	ssr := NewApplySessionStateSilent("SELECT set_config('statement_timeout', 'not-a-duration', false)", stmt)

	var results []*sqltypes.Result
	err := ssr.StreamExecute(context.Background(), nil, testConn.Conn, state, nil, PlanExecInfo{}, collectCallback(&results))
	require.Error(t, err)
	assert.Empty(t, results)
}

func TestApplySessionState_SET_InvalidParam_Succeeds(t *testing.T) {
	// Invalid params are accepted locally — errors surface on next query.
	testConn := server.NewTestConn(&bytes.Buffer{})
	state := &handler.MultigatewayConnectionState{}
	ctx := context.Background()

	stmt := &ast.VariableSetStmt{
		Kind: ast.VAR_SET_VALUE,
		Name: "totally_invalid_variable",
		Args: &ast.NodeList{Items: []ast.Node{&ast.A_Const{Val: &ast.String{SVal: "whatever"}}}},
	}

	ssr := NewApplySessionState("SET totally_invalid_variable = 'whatever'", stmt)

	var results []*sqltypes.Result
	err := ssr.StreamExecute(ctx, nil, testConn.Conn, state, nil, PlanExecInfo{}, collectCallback(&results))
	require.NoError(t, err, "SET with invalid param should succeed locally")

	val, exists := state.GetSessionVariable("totally_invalid_variable")
	assert.True(t, exists)
	assert.Equal(t, "whatever", val)

	require.Len(t, results, 1)
	assert.Equal(t, "SET", results[0].CommandTag)
}

func TestApplySessionState_RESET_NeverSetVariable(t *testing.T) {
	// RESET of a variable that was never SET should succeed (matches PostgreSQL behaviour).
	state := &handler.MultigatewayConnectionState{}
	testConn := server.NewTestConn(&bytes.Buffer{})
	ctx := context.Background()

	stmt := &ast.VariableSetStmt{
		Kind: ast.VAR_RESET,
		Name: "never_set_var",
	}

	ssr := NewApplySessionState("RESET never_set_var", stmt)

	var results []*sqltypes.Result
	err := ssr.StreamExecute(ctx, nil, testConn.Conn, state, nil, PlanExecInfo{}, collectCallback(&results))
	require.NoError(t, err, "RESET of never-set variable should succeed")

	_, exists := state.GetSessionVariable("never_set_var")
	assert.False(t, exists)

	require.Len(t, results, 1)
	assert.Equal(t, "RESET", results[0].CommandTag)
}

func TestApplySessionState_RESET_UpdatesStateAndReturnsSynthetic(t *testing.T) {
	state := &handler.MultigatewayConnectionState{}
	state.SetSessionVariable("work_mem", "256MB")

	testConn := server.NewTestConn(&bytes.Buffer{})
	ctx := context.Background()

	stmt := &ast.VariableSetStmt{
		Kind: ast.VAR_RESET,
		Name: "work_mem",
	}

	ssr := NewApplySessionState("RESET work_mem", stmt)

	var results []*sqltypes.Result
	err := ssr.StreamExecute(ctx, nil, testConn.Conn, state, nil, PlanExecInfo{}, collectCallback(&results))
	require.NoError(t, err)

	// Variable should be removed
	_, exists := state.GetSessionVariable("work_mem")
	assert.False(t, exists, "variable should be removed after RESET")

	// Should receive synthetic CommandComplete
	require.Len(t, results, 1)
	assert.Equal(t, "RESET", results[0].CommandTag)
}

func TestApplySessionState_RESET_ALL_ClearsAllVariables(t *testing.T) {
	state := &handler.MultigatewayConnectionState{}
	state.InitStatementTimeout(30 * time.Second)
	state.InitIdleSessionTimeout(0)
	state.SetSessionVariable("work_mem", "256MB")
	state.SetSessionVariable("search_path", "myschema")
	state.SetSessionVariable("statement_timeout", "30s")
	state.SetSessionVariable("idle_session_timeout", "30s")
	state.SetStatementTimeout(5 * time.Second)
	state.SetIdleSessionTimeout(5 * time.Second)

	testConn := server.NewTestConn(&bytes.Buffer{})
	ctx := context.Background()

	stmt := &ast.VariableSetStmt{
		Kind: ast.VAR_RESET_ALL,
	}

	ssr := NewApplySessionState("RESET ALL", stmt)

	var results []*sqltypes.Result
	err := ssr.StreamExecute(ctx, nil, testConn.Conn, state, nil, PlanExecInfo{}, collectCallback(&results))
	require.NoError(t, err)

	// All variables should be cleared
	_, exists := state.GetSessionVariable("work_mem")
	assert.False(t, exists)
	_, exists = state.GetSessionVariable("search_path")
	assert.False(t, exists)
	_, exists = state.GetSessionVariable("statement_timeout")
	assert.False(t, exists)
	_, exists = state.GetSessionVariable("idle_session_timeout")
	assert.False(t, exists)
	assert.Equal(t, 30*time.Second, state.GetStatementTimeout(), "RESET ALL should reset gateway-managed statement_timeout")
	assert.Equal(t, time.Duration(0), state.GetIdleSessionTimeout(), "RESET ALL should reset gateway-managed idle_session_timeout")

	// Should receive synthetic CommandComplete
	require.Len(t, results, 1)
	assert.Equal(t, "RESET", results[0].CommandTag)
}

func TestApplySessionState_UnsupportedKind(t *testing.T) {
	testConn := server.NewTestConn(&bytes.Buffer{})
	state := &handler.MultigatewayConnectionState{}
	ctx := context.Background()

	stmt := &ast.VariableSetStmt{
		Kind: ast.VAR_SET_MULTI,
		Name: "TRANSACTION",
	}

	ssr := NewApplySessionState("SET TRANSACTION READ ONLY", stmt)

	var results []*sqltypes.Result
	err := ssr.StreamExecute(ctx, nil, testConn.Conn, state, nil, PlanExecInfo{}, collectCallback(&results))
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

func TestGatewaySessionState_SETLOCAL_OutsideTxnNoOpsWithWarning(t *testing.T) {
	// PostgreSQL: SET LOCAL outside a transaction block emits
	//   WARNING: 25P01 — SET LOCAL can only be used in transaction blocks
	// and the value is discarded immediately because the implicit autocommit
	// transaction commits right after the statement.
	//
	// Multigateway must mirror this: skip the gateway-state mutation and
	// surface the WARNING as a NoticeResponse. Without this guard, isLocalSet
	// would persist for the lifetime of the connection because no
	// COMMIT/ROLLBACK ever fires to clear it.
	testConn := server.NewTestConn(&bytes.Buffer{})
	require.False(t, testConn.IsInTransaction(), "TestConn defaults to TxnStatusIdle")

	state := handler.NewMultigatewayConnectionState()
	state.InitStatementTimeout(30 * time.Second)
	ctx := context.Background()

	prim := NewStatementTimeoutSet("SET LOCAL statement_timeout = '100ms'", 100*time.Millisecond, true /*isLocal*/)

	var results []*sqltypes.Result
	err := prim.StreamExecute(ctx, nil, testConn.Conn, state, nil, PlanExecInfo{}, collectCallback(&results))
	require.NoError(t, err)

	// State must NOT have absorbed the LOCAL value.
	assert.Equal(t, 30*time.Second, state.GetStatementTimeout(),
		"SET LOCAL outside txn must not update gateway state")

	// Should receive a synthetic CommandComplete with a NoticeResponse attached.
	require.Len(t, results, 1)
	assert.Equal(t, "SET", results[0].CommandTag)
	require.Len(t, results[0].Notices, 1, "should attach a single NoticeResponse")
	notice := results[0].Notices[0]
	assert.Equal(t, "WARNING", notice.Severity)
	assert.Equal(t, mterrors.PgSSNoActiveTransaction, notice.Code)
	assert.Equal(t, "SET LOCAL can only be used in transaction blocks", notice.Message)
	assert.Equal(t, byte('N'), notice.MessageType, "must be NoticeResponse, not ErrorResponse")
}

func TestGatewaySessionState_SETLOCAL_InsideTxnUpdatesState(t *testing.T) {
	// Sanity check: inside a transaction, SET LOCAL still flows to gateway state.
	testConn := server.NewTestConn(&bytes.Buffer{})
	testConn.SetTxnStatus(protocol.TxnStatusInBlock)

	state := handler.NewMultigatewayConnectionState()
	state.InitStatementTimeout(30 * time.Second)
	ctx := context.Background()

	prim := NewStatementTimeoutSet("SET LOCAL statement_timeout = '100ms'", 100*time.Millisecond, true)

	var results []*sqltypes.Result
	err := prim.StreamExecute(ctx, nil, testConn.Conn, state, nil, PlanExecInfo{}, collectCallback(&results))
	require.NoError(t, err)

	assert.Equal(t, 100*time.Millisecond, state.GetStatementTimeout())
	require.Len(t, results, 1)
	assert.Empty(t, results[0].Notices, "inside-txn SET LOCAL emits no warning")
}

func TestGatewaySessionState_IdleSessionTimeoutVariants(t *testing.T) {
	t.Run("SET", func(t *testing.T) {
		testConn := server.NewTestConn(&bytes.Buffer{})
		state := handler.NewMultigatewayConnectionState()
		state.InitIdleSessionTimeout(30 * time.Second)
		prim := NewIdleSessionTimeoutSet("SET idle_session_timeout = '5s'", 5*time.Second, false)

		var results []*sqltypes.Result
		err := prim.StreamExecute(context.Background(), nil, testConn.Conn, state, nil, PlanExecInfo{}, collectCallback(&results))
		require.NoError(t, err)
		assert.Equal(t, 5*time.Second, state.GetIdleSessionTimeout())
		require.Len(t, results, 1)
		assert.Equal(t, "SET", results[0].CommandTag)
	})

	t.Run("SET LOCAL inside transaction", func(t *testing.T) {
		testConn := server.NewTestConn(&bytes.Buffer{})
		testConn.SetTxnStatus(protocol.TxnStatusInBlock)
		state := handler.NewMultigatewayConnectionState()
		state.InitIdleSessionTimeout(30 * time.Second)
		state.SetIdleSessionTimeout(5 * time.Second)
		prim := NewIdleSessionTimeoutSet("SET LOCAL idle_session_timeout = '250ms'", 250*time.Millisecond, true)

		var results []*sqltypes.Result
		err := prim.StreamExecute(context.Background(), nil, testConn.Conn, state, nil, PlanExecInfo{}, collectCallback(&results))
		require.NoError(t, err)
		assert.Equal(t, 250*time.Millisecond, state.GetIdleSessionTimeout())
		state.ResetAllLocalGUCs()
		assert.Equal(t, 5*time.Second, state.GetIdleSessionTimeout())
		require.Len(t, results, 1)
		assert.Empty(t, results[0].Notices)
	})

	t.Run("RESET", func(t *testing.T) {
		testConn := server.NewTestConn(&bytes.Buffer{})
		state := handler.NewMultigatewayConnectionState()
		state.InitIdleSessionTimeout(30 * time.Second)
		state.SetIdleSessionTimeout(5 * time.Second)
		prim := NewGatewaySessionStateReset("RESET idle_session_timeout", "idle_session_timeout", false, true)

		var results []*sqltypes.Result
		err := prim.StreamExecute(context.Background(), nil, testConn.Conn, state, nil, PlanExecInfo{}, collectCallback(&results))
		require.NoError(t, err)
		assert.Equal(t, 30*time.Second, state.GetIdleSessionTimeout())
		require.Len(t, results, 1)
		assert.Equal(t, "RESET", results[0].CommandTag)
	})

	t.Run("SET LOCAL TO DEFAULT inside transaction", func(t *testing.T) {
		testConn := server.NewTestConn(&bytes.Buffer{})
		testConn.SetTxnStatus(protocol.TxnStatusInBlock)
		state := handler.NewMultigatewayConnectionState()
		state.InitIdleSessionTimeout(30 * time.Second)
		state.SetIdleSessionTimeout(5 * time.Second)
		prim := NewGatewaySessionStateReset("SET LOCAL idle_session_timeout TO DEFAULT", "idle_session_timeout", true, false)

		var results []*sqltypes.Result
		err := prim.StreamExecute(context.Background(), nil, testConn.Conn, state, nil, PlanExecInfo{}, collectCallback(&results))
		require.NoError(t, err)
		assert.Equal(t, 30*time.Second, state.GetIdleSessionTimeout())
		state.ResetAllLocalGUCs()
		assert.Equal(t, 5*time.Second, state.GetIdleSessionTimeout())
		require.Len(t, results, 1)
		assert.Equal(t, "SET", results[0].CommandTag)
	})
}

func TestGatewayShowVariable_IdleSessionTimeout(t *testing.T) {
	state := handler.NewMultigatewayConnectionState()
	state.SetIdleSessionTimeout(5 * time.Second)
	prim := NewGatewayShowVariable("SHOW idle_session_timeout", "idle_session_timeout")

	var results []*sqltypes.Result
	err := prim.StreamExecute(context.Background(), nil, nil, state, nil, PlanExecInfo{}, collectCallback(&results))
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "SHOW", results[0].CommandTag)
	require.Len(t, results[0].Fields, 1)
	assert.Equal(t, "idle_session_timeout", results[0].Fields[0].Name)
	require.Len(t, results[0].Rows, 1)
	assert.Equal(t, []byte("5s"), []byte(results[0].Rows[0].Values[0]))
}

func TestGatewaySessionState_SETLOCAL_TODefault_PreservesSession(t *testing.T) {
	// SET LOCAL var TO DEFAULT must mask the session value with the default for
	// the duration of the transaction without destroying the session value.
	// Pre-fix behavior: this primitive called ResetStatementTimeout(),
	// destroying the session value.
	testConn := server.NewTestConn(&bytes.Buffer{})
	testConn.SetTxnStatus(protocol.TxnStatusInBlock)

	state := handler.NewMultigatewayConnectionState()
	state.InitStatementTimeout(30 * time.Second)
	state.SetStatementTimeout(5 * time.Second)
	ctx := context.Background()

	// Mirrors what the planner emits for `SET LOCAL statement_timeout TO DEFAULT`.
	prim := NewGatewaySessionStateReset("SET LOCAL statement_timeout TO DEFAULT", "statement_timeout", true /*isLocal*/, false /*isResetStmt: source is VAR_SET_DEFAULT*/)

	var results []*sqltypes.Result
	err := prim.StreamExecute(ctx, nil, testConn.Conn, state, nil, PlanExecInfo{}, collectCallback(&results))
	require.NoError(t, err)

	assert.Equal(t, 30*time.Second, state.GetStatementTimeout(),
		"inside txn: effective value is default, masking session")
	state.ResetAllLocalGUCs()
	assert.Equal(t, 5*time.Second, state.GetStatementTimeout(),
		"after txn end: session value is restored, not lost")
	require.Len(t, results, 1)
	assert.Empty(t, results[0].Notices, "inside-txn LOCAL TO DEFAULT emits no warning")
	assert.Equal(t, "SET", results[0].CommandTag,
		`SET LOCAL var TO DEFAULT must return CommandTag "SET" (not "RESET"), matching PostgreSQL`)
}

func TestGatewaySessionState_RESET_NonLocalStillClearsSession(t *testing.T) {
	// Plain RESET (non-LOCAL) should still clear the session-level override.
	// Regression check: thread isLocal through without breaking the existing
	// session-RESET behavior.
	testConn := server.NewTestConn(&bytes.Buffer{})

	state := handler.NewMultigatewayConnectionState()
	state.InitStatementTimeout(30 * time.Second)
	state.SetStatementTimeout(5 * time.Second)
	ctx := context.Background()

	prim := NewGatewaySessionStateReset("RESET statement_timeout", "statement_timeout", false /*isLocal*/, true /*isResetStmt*/)

	var results []*sqltypes.Result
	err := prim.StreamExecute(ctx, nil, testConn.Conn, state, nil, PlanExecInfo{}, collectCallback(&results))
	require.NoError(t, err)

	assert.Equal(t, 30*time.Second, state.GetStatementTimeout(),
		"RESET clears the session override and reverts to default")
	require.Len(t, results, 1)
	assert.Equal(t, "RESET", results[0].CommandTag)
}

func TestGatewaySessionState_SETToDEFAULT_NonLocalReturnsSETTag(t *testing.T) {
	// `SET var TO DEFAULT` (non-LOCAL, VAR_SET_DEFAULT) clears the session
	// override like RESET, but PostgreSQL returns CommandTag "SET" — only the
	// literal `RESET var` syntax returns "RESET". The planner sets
	// isResetStmt=false for VAR_SET_DEFAULT.
	testConn := server.NewTestConn(&bytes.Buffer{})

	state := handler.NewMultigatewayConnectionState()
	state.InitStatementTimeout(30 * time.Second)
	state.SetStatementTimeout(5 * time.Second)
	ctx := context.Background()

	prim := NewGatewaySessionStateReset("SET statement_timeout TO DEFAULT", "statement_timeout", false /*isLocal*/, false /*isResetStmt: source is VAR_SET_DEFAULT*/)

	var results []*sqltypes.Result
	err := prim.StreamExecute(ctx, nil, testConn.Conn, state, nil, PlanExecInfo{}, collectCallback(&results))
	require.NoError(t, err)

	assert.Equal(t, 30*time.Second, state.GetStatementTimeout(),
		"SET ... TO DEFAULT clears the session override, same effect as RESET")
	require.Len(t, results, 1)
	assert.Equal(t, "SET", results[0].CommandTag,
		`SET var TO DEFAULT must return CommandTag "SET" (not "RESET"), matching PostgreSQL`)
}

func TestGatewaySessionState_SETLOCALToDEFAULT_OutsideTxnReturnsSETTag(t *testing.T) {
	// `SET LOCAL var TO DEFAULT` outside a transaction emits the 25P01 warning
	// AND must return CommandTag "SET" (not "RESET") because the source
	// statement was VAR_SET_DEFAULT, not VAR_RESET.
	testConn := server.NewTestConn(&bytes.Buffer{})
	require.False(t, testConn.IsInTransaction(), "TestConn defaults to TxnStatusIdle")

	state := handler.NewMultigatewayConnectionState()
	state.InitStatementTimeout(30 * time.Second)
	state.SetStatementTimeout(5 * time.Second)
	ctx := context.Background()

	prim := NewGatewaySessionStateReset("SET LOCAL statement_timeout TO DEFAULT", "statement_timeout", true /*isLocal*/, false /*isResetStmt*/)

	var results []*sqltypes.Result
	err := prim.StreamExecute(ctx, nil, testConn.Conn, state, nil, PlanExecInfo{}, collectCallback(&results))
	require.NoError(t, err)

	// State must NOT have changed: outside-txn SET LOCAL is a no-op.
	assert.Equal(t, 5*time.Second, state.GetStatementTimeout(),
		"outside-txn SET LOCAL (any form) must not mutate gateway state")

	require.Len(t, results, 1)
	assert.Equal(t, "SET", results[0].CommandTag,
		`SET LOCAL var TO DEFAULT (even outside txn) must return CommandTag "SET"`)
	require.Len(t, results[0].Notices, 1)
	assert.Equal(t, mterrors.PgSSNoActiveTransaction, results[0].Notices[0].Code)
}
