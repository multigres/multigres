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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
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

func (h *preparedPrimitiveHandler) HandleParse(_ context.Context, _ *server.Conn, name, sql string, paramTypes []uint32) error {
	info, err := preparedstatement.NewPreparedStatementInfo(&query.PreparedStatement{Name: name, Query: sql, ParamTypes: paramTypes})
	h.info = info
	return err
}

func newPreparedPrimitiveConn(t *testing.T, preparedSQL string) (*PreparedStatementPrimitive, *preparedPrimitiveHandler) {
	t.Helper()
	psi, err := preparedstatement.NewPreparedStatementInfo(&query.PreparedStatement{Name: "p", Query: preparedSQL})
	require.NoError(t, err)
	parsed, err := parser.ParseSQL("EXECUTE p('value')")
	require.NoError(t, err)
	h := &preparedPrimitiveHandler{info: psi}
	return NewExecutePrimitive("default", "EXECUTE p('value')", parsed[0].(*ast.ExecuteStmt), nil, nil), h
}

func TestSQLPreparedExecuteArgumentResolution(t *testing.T) {
	portal := buildBoundPortalInfo(t, "SELECT $1", []uint32{uint32(ast.TEXTOID)}, [][]byte{[]byte("bound")}, nil)
	nullPortal := buildBoundPortalInfo(t, "SELECT $1", []uint32{uint32(ast.TEXTOID)}, [][]byte{nil}, nil)
	tests := []struct {
		name     string
		arg      ast.Node
		portal   *preparedstatement.PortalInfo
		want     string
		wantNull bool
		wantErr  string
	}{
		{name: "constant", arg: ast.NewA_Const(ast.NewString("literal"), 0), want: "literal"},
		// NULL is reported, not rejected: set_config is not STRICT, so the
		// caller maps it to PostgreSQL's reset-to-default semantics.
		{name: "null", arg: ast.NewA_ConstNull(0), wantNull: true},
		{name: "bound null", arg: ast.NewParamRef(1, 0), portal: nullPortal, wantNull: true},
		{name: "string", arg: ast.NewString("literal"), want: "literal"},
		{name: "integer", arg: ast.NewInteger(42), want: "42"},
		{name: "cast", arg: ast.NewTypeCast(ast.NewInteger(7), nil, 0), want: "7"},
		{name: "bound", arg: ast.NewParamRef(1, 0), portal: portal, want: "bound"},
		{name: "bound without portal", arg: ast.NewParamRef(1, 0), wantErr: "literal constant or a bound text parameter"},
		{name: "unsupported", arg: ast.NewBoolean(true), wantErr: "literal constant or a bound text parameter"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, isNull, err := executeArgAsTextOrNull(tt.arg, tt.portal, "argument")
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantNull, isNull)
			if !tt.wantNull {
				assert.Equal(t, tt.want, got)
			}
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

	// search_path resolved from an EXECUTE argument must be vetted for pg_temp
	// (the SQL PREPARE/EXECUTE half of the guard; the wire-protocol half lives
	// in resolveSetConfig).
	pgTemp := &PreparedStatementPrimitive{executeStmt: &ast.ExecuteStmt{
		Name:   "p",
		Params: ast.NewNodeList(ast.NewString("pg_temp, public")),
	}}
	_, err = pgTemp.resolvePreparedSetConfig(SQLPreparedSetConfig{Name: "search_path", ValueParam: ast.NewParamRef(1, 0)}, nil)
	require.ErrorContains(t, err, "pg_temp")

	// The guard runs before the untracked is_local=true early return, so even
	// that (planner-unreachable) shape cannot slip a pg_temp value through.
	_, err = pgTemp.resolvePreparedSetConfig(SQLPreparedSetConfig{Name: "search_path", Value: "pg_temp", IsLocalLiteralTrue: true}, nil)
	require.ErrorContains(t, err, "pg_temp")

	// Benign search_path values resolve normally.
	resolved, err = p.resolvePreparedSetConfig(SQLPreparedSetConfig{Name: "search_path", ValueParam: ast.NewParamRef(1, 0)}, nil)
	require.NoError(t, err)
	assert.Equal(t, resolvedSetConfig{name: "search_path", value: "value", shouldTrack: true}, resolved)

	// A literal NULL in the PREPARE body resolves to a reset, exactly like a
	// NULL EXECUTE argument. Without ValueIsNull the zero-valued Value ("")
	// would be tracked as an explicit empty string the backend never had.
	resolved, err = p.resolvePreparedSetConfig(SQLPreparedSetConfig{Name: "work_mem", ValueIsNull: true}, nil)
	require.NoError(t, err)
	assert.Equal(t, resolvedSetConfig{name: "work_mem", shouldTrack: true, isReset: true}, resolved)

	// Same for search_path: the reset restores the admin default, so the
	// pg_temp vet has nothing to inspect and must not reject it.
	resolved, err = p.resolvePreparedSetConfig(SQLPreparedSetConfig{Name: "search_path", ValueIsNull: true}, nil)
	require.NoError(t, err)
	assert.Equal(t, resolvedSetConfig{name: "search_path", shouldTrack: true, isReset: true}, resolved)

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
	actions, _, err := p.prepareSetConfigTracking(conn, state, nil, PlanExecInfo{})
	require.NoError(t, err)
	require.Len(t, actions, 1)
	actions[0]()
	got, ok := state.GetSessionVariable("application_name")
	require.True(t, ok)
	assert.Equal(t, "prepared", got)

	// A literal NULL in the PREPARE body must REMOVE the tracked entry, not
	// write "". Tracking "" would replay as SET application_name = '' onto the
	// next pooled backend — rejected by PostgreSQL for most GUCs, and silently
	// wrong for search_path.
	p.setConfigs = []SQLPreparedSetConfig{{Name: "application_name", ValueIsNull: true}}
	actions, _, err = p.prepareSetConfigTracking(conn, state, nil, PlanExecInfo{})
	require.NoError(t, err)
	require.Len(t, actions, 1)
	actions[0]()
	_, ok = state.GetSessionVariable("application_name")
	assert.False(t, ok, "a NULL value must drop the tracked entry, matching PostgreSQL's reset")

	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	p.setConfigs = []SQLPreparedSetConfig{{Name: "statement_timeout", Value: "1s", IsLocalLiteralTrue: true}}
	actions, _, err = p.prepareSetConfigTracking(conn, state, nil, PlanExecInfo{})
	require.NoError(t, err)
	require.Len(t, actions, 1)
	actions[0]()

	p.setConfigs = []SQLPreparedSetConfig{{Name: "statement_timeout", Value: "invalid"}}
	_, _, err = p.prepareSetConfigTracking(conn, state, nil, PlanExecInfo{})
	require.Error(t, err)

	p.executeStmt = &ast.ExecuteStmt{Name: "p"}
	p.setConfigs = []SQLPreparedSetConfig{{Name: "application_name", ValueParam: ast.NewParamRef(1, 0)}}
	_, _, err = p.prepareSetConfigTracking(conn, state, nil, PlanExecInfo{})
	require.ErrorContains(t, err, "EXECUTE supplies 0 argument")
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

	prepare := NewPreparePrimitive("default", "p", "SELECT 1", 0, nil, nil)
	require.NoError(t, prepare.PortalStreamExecute(context.Background(), &mockIExecute{}, newDiscardTestConn(t, &preparedPrimitiveHandler{}), state, nil, 0, false, PlanExecInfo{}, func(context.Context, *sqltypes.Result) error { return nil }))
}

func TestTranslatePrepareBodyPosition(t *testing.T) {
	// Regression: the backend validates only the PREPARE body, so its position is
	// body-relative. Reported against the client's text it must clear the
	// `PREPARE foo (xml) AS ` prefix. Taken from pg_regress xml.sql, where the
	// caret landed 21 columns short (the prefix width).
	const sql = `PREPARE foo (xml) AS SELECT xmlconcat('<foo/>', $1);`
	bodyOffset := strings.Index(sql, "SELECT")
	require.Equal(t, 21, bodyOffset)

	backendErr := &mterrors.PgDiagnostic{Message: "unsupported XML feature", Position: 26}
	translated := translatePrepareBodyPosition(backendErr, bodyOffset)

	var diagnostic *mterrors.PgDiagnostic
	require.ErrorAs(t, translated, &diagnostic)
	assert.Equal(t, int32(47), diagnostic.Position, "position must be body-relative + body offset")
	assert.Equal(t, int32(26), backendErr.Position, "translation must not mutate the original diagnostic")

	// A position-less diagnostic and a zero offset both pass through untouched.
	noPos := &mterrors.PgDiagnostic{Message: "boom"}
	assert.Same(t, noPos, translatePrepareBodyPosition(noPos, bodyOffset))
	assert.Same(t, backendErr, translatePrepareBodyPosition(backendErr, 0))
}

func TestExtractParamTypesQualifiedTypesDeferToBackend(t *testing.T) {
	// A non-pg_catalog qualifier can shadow a builtin name (CREATE DOMAIN
	// s.int4 AS text), so only unqualified and pg_catalog-qualified names may
	// take the static-OID fast path; everything else must go to the backend.
	parsed, err := parser.ParseSQL(`PREPARE p (s.int4, pg_catalog.int4, int4, s.custom) AS SELECT 1`)
	require.NoError(t, err)
	oids, names := ExtractParamTypes(parsed[0].(*ast.PrepareStmt))

	assert.Equal(t, []uint32{0, uint32(ast.INT4OID), uint32(ast.INT4OID), 0}, oids)
	require.Len(t, names, 4)
	assert.Equal(t, "s.int4", names[0].Name)
	assert.Equal(t, "s.custom", names[3].Name)
}
