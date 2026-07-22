// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

type preparedPrimitiveHandler struct {
	recordingHandler
	info *preparedstatement.PreparedStatementInfo
}

func (h *preparedPrimitiveHandler) GetPreparedStatementInfo(uint32, string) *preparedstatement.PreparedStatementInfo {
	return h.info
}

func newPreparedPrimitiveConn(t *testing.T, preparedSQL string) (*PreparedStatementPrimitive, *preparedPrimitiveHandler) {
	t.Helper()
	psi, err := preparedstatement.NewPreparedStatementInfo(&query.PreparedStatement{Name: "p", Query: preparedSQL})
	require.NoError(t, err)
	parsed, err := parser.ParseSQL("EXECUTE p('value')")
	require.NoError(t, err)
	h := &preparedPrimitiveHandler{info: psi}
	return NewExecutePrimitive("default", parsed[0].(*ast.ExecuteStmt), nil), h
}

func TestSQLPreparedExecuteArgumentResolution(t *testing.T) {
	portal := buildBoundPortalInfo(t, "SELECT $1", []uint32{uint32(ast.TEXTOID)}, [][]byte{[]byte("bound")}, nil)
	tests := []struct {
		name    string
		arg     ast.Node
		portal  *preparedstatement.PortalInfo
		want    string
		wantErr string
	}{
		{name: "constant", arg: ast.NewA_Const(ast.NewString("literal"), 0), want: "literal"},
		{name: "null", arg: ast.NewA_ConstNull(0), wantErr: "cannot be NULL"},
		{name: "string", arg: ast.NewString("literal"), want: "literal"},
		{name: "integer", arg: ast.NewInteger(42), want: "42"},
		{name: "cast", arg: ast.NewTypeCast(ast.NewInteger(7), nil, 0), want: "7"},
		{name: "bound", arg: ast.NewParamRef(1, 0), portal: portal, want: "bound"},
		{name: "bound without portal", arg: ast.NewParamRef(1, 0), wantErr: "literal constant or a bound text parameter"},
		{name: "unsupported", arg: ast.NewBoolean(true), wantErr: "literal constant or a bound text parameter"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := executeArgAsText(tt.arg, tt.portal, "argument")
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSQLPreparedSetConfigResolution(t *testing.T) {
	p := &PreparedStatementPrimitive{executeStmt: &ast.ExecuteStmt{
		Name:   "p",
		Params: ast.NewNodeList(ast.NewString("value")),
	}}

	resolved, err := p.resolvePreparedSetConfig(SQLPreparedSetConfig{Name: "work_mem", IsLocalLiteralTrue: true}, nil)
	require.NoError(t, err)
	assert.False(t, resolved.shouldTrack)

	resolved, err = p.resolvePreparedSetConfig(SQLPreparedSetConfig{Name: "application_name", ValueParam: ast.NewParamRef(1, 0)}, nil)
	require.NoError(t, err)
	assert.Equal(t, resolvedSetConfig{name: "application_name", value: "value", shouldTrack: true}, resolved)

	_, err = p.resolvePreparedSetConfig(SQLPreparedSetConfig{Name: "application_name", ValueParam: ast.NewParamRef(2, 0)}, nil)
	require.ErrorContains(t, err, "EXECUTE supplies 1 argument")

	assert.Zero(t, executeArgCount(nil))
	assert.Zero(t, executeArgCount(&ast.ExecuteStmt{}))
	assert.Equal(t, 1, executeArgCount(p.executeStmt))
}

func TestSQLPreparedSetConfigTrackingBranches(t *testing.T) {
	state := handler.NewMultigatewayConnectionState()
	conn := newDiscardTestConn(t, &recordingHandler{})

	p := &PreparedStatementPrimitive{setConfigs: []SQLPreparedSetConfig{
		{Name: "work_mem", Value: "64MB", IsLocalLiteralTrue: true},
		{Name: "application_name", Value: "prepared"},
	}}
	actions, info, err := p.prepareSetConfigTracking(conn, state, nil, PlanExecInfo{})
	require.NoError(t, err)
	require.Len(t, actions, 1)
	assert.True(t, info.HasPostQuerySessionSettings)
	assert.Equal(t, "prepared", info.PostQuerySessionSettings["application_name"])
	actions[0]()
	got, ok := state.GetSessionVariable("application_name")
	require.True(t, ok)
	assert.Equal(t, "prepared", got)

	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	p.setConfigs = []SQLPreparedSetConfig{{Name: "statement_timeout", Value: "1s", IsLocalLiteralTrue: true}}
	actions, info, err = p.prepareSetConfigTracking(conn, state, nil, PlanExecInfo{})
	require.NoError(t, err)
	require.Len(t, actions, 1)
	assert.False(t, info.HasPostQuerySessionSettings)
	actions[0]()

	p.setConfigs = []SQLPreparedSetConfig{{Name: "statement_timeout", Value: "invalid"}}
	_, _, err = p.prepareSetConfigTracking(conn, state, nil, PlanExecInfo{})
	require.Error(t, err)

	p.executeStmt = &ast.ExecuteStmt{Name: "p"}
	p.setConfigs = []SQLPreparedSetConfig{{Name: "application_name", ValueParam: ast.NewParamRef(1, 0)}}
	_, _, err = p.prepareSetConfigTracking(conn, state, nil, PlanExecInfo{})
	require.ErrorContains(t, err, "EXECUTE supplies 0 argument")
}

func TestBuiltinArrayTypeOid(t *testing.T) {
	for scalar, array := range map[ast.Oid]ast.Oid{
		ast.BOOLOID:        ast.BOOLARRAYOID,
		ast.BYTEAOID:       ast.BYTEAARRAYOID,
		ast.NAMEOID:        ast.NAMEARRAYOID,
		ast.INT2OID:        ast.INT2ARRAYOID,
		ast.INT4OID:        ast.INT4ARRAYOID,
		ast.INT8OID:        ast.INT8ARRAYOID,
		ast.FLOAT4OID:      ast.FLOAT4ARRAYOID,
		ast.FLOAT8OID:      ast.FLOAT8ARRAYOID,
		ast.TEXTOID:        ast.TEXTARRAYOID,
		ast.VARCHAROID:     ast.VARCHARARRAYOID,
		ast.DATEOID:        ast.DATEARRAYOID,
		ast.TIMEOID:        ast.TIMEARRAYOID,
		ast.TIMESTAMPOID:   ast.TIMESTAMPARRAYOID,
		ast.TIMESTAMPTZOID: ast.TIMESTAMPTZARRAYOID,
		ast.JSONOID:        ast.JSONARRAYOID,
		ast.JSONBOID:       ast.JSONBARRAYOID,
	} {
		assert.Equal(t, array, builtinArrayTypeOid(scalar))
	}
	assert.Equal(t, ast.InvalidOid, builtinArrayTypeOid(ast.InvalidOid))
}

func TestPreparedStatementPrimitiveExecuteErrorsAndPortalDispatch(t *testing.T) {
	p, h := newPreparedPrimitiveConn(t, "SELECT 1")
	conn := newDiscardTestConn(t, h)
	state := handler.NewMultigatewayConnectionState()

	p.setConfigs = []SQLPreparedSetConfig{{Name: "statement_timeout", Value: "invalid"}}
	err := p.StreamExecute(context.Background(), &mockIExecute{}, conn, state, nil, PlanExecInfo{}, nil)
	require.Error(t, err)

	p.setConfigs = nil
	execErr := errors.New("execute failed")
	err = p.StreamExecute(context.Background(), &mockIExecute{streamExecuteErr: execErr}, conn, state, nil, PlanExecInfo{}, nil)
	require.ErrorIs(t, err, execErr)

	h.info = &preparedstatement.PreparedStatementInfo{}
	err = p.StreamExecute(context.Background(), &mockIExecute{}, conn, state, nil, PlanExecInfo{}, nil)
	require.ErrorContains(t, err, "prepared statement is nil")

	p, h = newPreparedPrimitiveConn(t, "SELECT 1")
	conn = newDiscardTestConn(t, h)
	require.NoError(t, p.PortalStreamExecute(context.Background(), &mockIExecute{}, conn, state, nil, 0, false, PlanExecInfo{}, nil))

	prepare := NewPreparePrimitive("default", "p", "SELECT 1", nil)
	require.NoError(t, prepare.PortalStreamExecute(context.Background(), &mockIExecute{}, newDiscardTestConn(t, &recordingHandler{}), state, nil, 0, false, PlanExecInfo{}, func(context.Context, *sqltypes.Result) error { return nil }))
}
