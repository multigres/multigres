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
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	pgClient "github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

type resolveRowsPrimitive struct {
	rows []*sqltypes.Row
	err  error
}

func (p *resolveRowsPrimitive) StreamExecute(
	ctx context.Context,
	_ IExecute,
	_ *server.Conn,
	_ *handler.MultigatewayConnectionState,
	_ []*ast.A_Const,
	_ PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	if p.err != nil {
		return p.err
	}
	return callback(ctx, &sqltypes.Result{Rows: p.rows, CommandTag: "SELECT 1"})
}

func (p *resolveRowsPrimitive) PortalStreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	_ *preparedstatement.PortalInfo,
	_ int32,
	_ bool,
	info PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	return p.StreamExecute(ctx, exec, conn, state, nil, info, callback)
}

func (p *resolveRowsPrimitive) GetTableGroup() string { return "" }
func (p *resolveRowsPrimitive) GetQuery() string      { return "resolve" }
func (p *resolveRowsPrimitive) String() string        { return "resolveRowsPrimitive" }

type resolveApplyExec struct {
	applyCalls int
	applySQL   string
	applyErr   error
	onCallback func()
}

func (e *resolveApplyExec) StreamExecute(
	ctx context.Context,
	_ *server.Conn,
	_ string,
	_ string,
	sql string,
	_ *query.ExecuteSqlPreparedStatement,
	_ *handler.MultigatewayConnectionState,
	_ PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	e.applyCalls++
	e.applySQL = sql
	if e.applyErr != nil {
		return e.applyErr
	}
	if e.onCallback != nil {
		e.onCallback()
	}
	return callback(ctx, &sqltypes.Result{CommandTag: "SELECT 1"})
}

func (e *resolveApplyExec) PortalStreamExecute(context.Context, string, string, *server.Conn, *handler.MultigatewayConnectionState, *preparedstatement.PortalInfo, int32, bool, PlanExecInfo, func(context.Context, *sqltypes.Result) error) error {
	return nil
}

func (e *resolveApplyExec) Describe(context.Context, string, string, *server.Conn, *handler.MultigatewayConnectionState, *preparedstatement.PortalInfo, *preparedstatement.PreparedStatementInfo) (*query.StatementDescription, error) {
	return nil, nil
}

func (e *resolveApplyExec) CopyInitiate(context.Context, *server.Conn, string, string, string, *handler.MultigatewayConnectionState, func(context.Context, *sqltypes.Result) error) (int16, []int16, error) {
	return 0, nil, nil
}

func (e *resolveApplyExec) CopySendData(context.Context, *server.Conn, string, string, *handler.MultigatewayConnectionState, []byte) error {
	return nil
}

func (e *resolveApplyExec) CopyFinalize(context.Context, *server.Conn, string, string, *handler.MultigatewayConnectionState, []byte, func(context.Context, *sqltypes.Result) error) error {
	return nil
}

func (e *resolveApplyExec) CopyAbort(context.Context, *server.Conn, string, string, *handler.MultigatewayConnectionState) error {
	return nil
}

func (e *resolveApplyExec) CopyOutInitiate(context.Context, *server.Conn, string, string, string, *handler.MultigatewayConnectionState) (int16, []int16, []*mterrors.PgDiagnostic, error) {
	return 0, nil, nil, nil
}

func (e *resolveApplyExec) CopyOutStream(context.Context, *server.Conn, string, string, *handler.MultigatewayConnectionState, func(pgClient.CopyOutMessage) error) (*sqltypes.Result, error) {
	return nil, nil
}

func (e *resolveApplyExec) ConcludeTransaction(context.Context, *server.Conn, *handler.MultigatewayConnectionState, multipoolerpb.TransactionConclusion, []string, bool, bool, func(context.Context, *sqltypes.Result) error) error {
	return nil
}

func (e *resolveApplyExec) DiscardTempTables(context.Context, *server.Conn, *handler.MultigatewayConnectionState, func(context.Context, *sqltypes.Result) error) error {
	return nil
}

func (e *resolveApplyExec) ReleaseAllReservedConnections(context.Context, *server.Conn, *handler.MultigatewayConnectionState) error {
	return nil
}

func setConfigResolveRow(name, value string, isLocal bool) *sqltypes.Row {
	local := "f"
	if isLocal {
		local = "t"
	}
	return &sqltypes.Row{Values: []sqltypes.Value{[]byte(name), []byte(value), []byte(local)}}
}

func newResolveSetConfigForTest(rows ...*sqltypes.Row) *ResolveTrackSetConfig {
	return NewResolveTrackSetConfig("", "", "SELECT set_config(...)", &resolveRowsPrimitive{rows: rows}, nil, []string{""})
}

func TestResolveTrackSetConfig_PrevalidatesGatewayManagedBeforeApply(t *testing.T) {
	prim := newResolveSetConfigForTest(setConfigResolveRow("statement_timeout", "5 seconds", false))
	exec := &resolveApplyExec{}
	conn := server.NewTestConn(&bytes.Buffer{}).Conn
	state := handler.NewMultigatewayConnectionState()
	state.InitStatementTimeout(30 * time.Second)

	callbacks := 0
	err := prim.StreamExecute(context.Background(), exec, conn, state, nil, PlanExecInfo{}, func(context.Context, *sqltypes.Result) error {
		callbacks++
		return nil
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), `invalid value for parameter "statement_timeout": "5 seconds"`)
	assert.Equal(t, 0, exec.applyCalls, "invalid gateway-managed value must fail before backend apply streams a result")
	assert.Equal(t, 0, callbacks, "client-visible apply callback must not run on prevalidation failure")
	assert.Equal(t, 30*time.Second, state.GetStatementTimeout(), "gateway state must not drift on rejected value")
}

func TestResolveTrackSetConfig_TracksGatewayManagedOnlyAfterApplySuccess(t *testing.T) {
	prim := newResolveSetConfigForTest(setConfigResolveRow("statement_timeout", "1min", false))
	exec := &resolveApplyExec{}
	conn := server.NewTestConn(&bytes.Buffer{}).Conn
	state := handler.NewMultigatewayConnectionState()
	state.InitStatementTimeout(30 * time.Second)
	exec.onCallback = func() {
		assert.Equal(t, 30*time.Second, state.GetStatementTimeout(), "tracking must wait until backend apply returns")
	}

	callbacks := 0
	err := prim.StreamExecute(context.Background(), exec, conn, state, nil, PlanExecInfo{}, func(context.Context, *sqltypes.Result) error {
		callbacks++
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, 1, exec.applyCalls)
	assert.Contains(t, exec.applySQL, "statement_timeout")
	assert.Equal(t, 1, callbacks)
	assert.Equal(t, time.Minute, state.GetStatementTimeout())
}

func TestResolveTrackSetConfig_DoesNotTrackWhenApplyFails(t *testing.T) {
	prim := newResolveSetConfigForTest(setConfigResolveRow("statement_timeout", "1min", false))
	applyErr := errors.New("backend rejected apply")
	exec := &resolveApplyExec{applyErr: applyErr}
	conn := server.NewTestConn(&bytes.Buffer{}).Conn
	state := handler.NewMultigatewayConnectionState()
	state.InitStatementTimeout(30 * time.Second)

	err := prim.StreamExecute(context.Background(), exec, conn, state, nil, PlanExecInfo{}, func(context.Context, *sqltypes.Result) error {
		t.Fatal("callback must not run when apply returns an error")
		return nil
	})

	require.ErrorIs(t, err, applyErr)
	assert.Equal(t, 1, exec.applyCalls)
	assert.Equal(t, 30*time.Second, state.GetStatementTimeout(), "state changes must be delayed until backend apply succeeds")
}
