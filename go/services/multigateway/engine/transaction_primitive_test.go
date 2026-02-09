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
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// txMockIExecute is a mock IExecute for testing TransactionPrimitive.
type txMockIExecute struct {
	streamExecuteErr   error
	streamExecuteSQL   []string
	streamExecuteCount int
	callbackResult     *sqltypes.Result
}

func (m *txMockIExecute) StreamExecute(
	_ context.Context,
	_ *server.Conn,
	_ string,
	_ string,
	sql string,
	_ *handler.MultiGatewayConnectionState,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	m.streamExecuteSQL = append(m.streamExecuteSQL, sql)
	m.streamExecuteCount++
	if m.streamExecuteErr != nil {
		return m.streamExecuteErr
	}
	if m.callbackResult != nil {
		return callback(context.Background(), m.callbackResult)
	}
	return nil
}

func (m *txMockIExecute) PortalStreamExecute(context.Context, string, string, *server.Conn, *handler.MultiGatewayConnectionState, *preparedstatement.PortalInfo, int32, func(context.Context, *sqltypes.Result) error) error {
	return nil
}

func (m *txMockIExecute) Describe(context.Context, string, string, *server.Conn, *handler.MultiGatewayConnectionState, *preparedstatement.PortalInfo, *preparedstatement.PreparedStatementInfo) (*query.StatementDescription, error) {
	return nil, nil
}

func (m *txMockIExecute) CopyInitiate(context.Context, *server.Conn, string, string, string, *handler.MultiGatewayConnectionState, func(context.Context, *sqltypes.Result) error) (int16, []int16, error) {
	return 0, nil, nil
}

func (m *txMockIExecute) CopySendData(context.Context, *server.Conn, string, string, *handler.MultiGatewayConnectionState, []byte) error {
	return nil
}

func (m *txMockIExecute) CopyFinalize(context.Context, *server.Conn, string, string, *handler.MultiGatewayConnectionState, []byte, func(context.Context, *sqltypes.Result) error) error {
	return nil
}

func (m *txMockIExecute) CopyAbort(context.Context, *server.Conn, string, string, *handler.MultiGatewayConnectionState) error {
	return nil
}

// newTestReservedState creates a state with a reserved connection on the given tableGroup.
func newTestReservedState(tableGroup string) *handler.MultiGatewayConnectionState {
	state := handler.NewMultiGatewayConnectionState()
	state.SetTransactionState(handler.TxStateInTransaction)
	target := &query.Target{
		TableGroup: tableGroup,
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	state.StoreReservedConnection(target, queryservice.ReservedState{
		ReservedConnectionId: 100,
		PoolerID:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
	})
	return state
}

func TestTransactionPrimitive_Begin_SetsStateAndReturnsSyntheticResult(t *testing.T) {
	mockExec := &txMockIExecute{}
	state := handler.NewMultiGatewayConnectionState()
	var callbackResult *sqltypes.Result

	tp := NewTransactionPrimitive(ast.TRANS_STMT_BEGIN, "BEGIN", "tg1")
	err := tp.StreamExecute(context.Background(), mockExec, nil, state, func(_ context.Context, r *sqltypes.Result) error {
		callbackResult = r
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, handler.TxStateInTransaction, state.GetTransactionState())
	require.Equal(t, 0, mockExec.streamExecuteCount, "BEGIN should not call backend")
	require.NotNil(t, callbackResult)
	require.Equal(t, "BEGIN", callbackResult.CommandTag)
}

func TestTransactionPrimitive_StartTransaction(t *testing.T) {
	mockExec := &txMockIExecute{}
	state := handler.NewMultiGatewayConnectionState()
	var callbackResult *sqltypes.Result

	tp := NewTransactionPrimitive(ast.TRANS_STMT_START, "START TRANSACTION", "tg1")
	err := tp.StreamExecute(context.Background(), mockExec, nil, state, func(_ context.Context, r *sqltypes.Result) error {
		callbackResult = r
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, handler.TxStateInTransaction, state.GetTransactionState())
	require.Equal(t, 0, mockExec.streamExecuteCount, "START should not call backend")
	require.NotNil(t, callbackResult)
	require.Equal(t, "BEGIN", callbackResult.CommandTag)
}

func TestTransactionPrimitive_Commit_NoReservedConnections(t *testing.T) {
	mockExec := &txMockIExecute{}
	state := handler.NewMultiGatewayConnectionState()
	state.SetTransactionState(handler.TxStateInTransaction)
	var callbackResult *sqltypes.Result

	tp := NewTransactionPrimitive(ast.TRANS_STMT_COMMIT, "COMMIT", "tg1")
	err := tp.StreamExecute(context.Background(), mockExec, nil, state, func(_ context.Context, r *sqltypes.Result) error {
		callbackResult = r
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, handler.TxStateIdle, state.GetTransactionState())
	require.Equal(t, 0, mockExec.streamExecuteCount, "No backend call when no reserved connections")
	require.NotNil(t, callbackResult)
	require.Equal(t, "COMMIT", callbackResult.CommandTag)
}

func TestTransactionPrimitive_Commit_WithReservedConnections(t *testing.T) {
	mockExec := &txMockIExecute{
		callbackResult: &sqltypes.Result{CommandTag: "COMMIT"},
	}
	state := newTestReservedState("tg1")

	tp := NewTransactionPrimitive(ast.TRANS_STMT_COMMIT, "COMMIT", "tg1")
	err := tp.StreamExecute(context.Background(), mockExec, nil, state, func(_ context.Context, _ *sqltypes.Result) error {
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, handler.TxStateIdle, state.GetTransactionState())
	require.Equal(t, 1, mockExec.streamExecuteCount)
	require.Equal(t, []string{"COMMIT"}, mockExec.streamExecuteSQL)
}

func TestTransactionPrimitive_Commit_StreamExecuteError(t *testing.T) {
	mockExec := &txMockIExecute{
		streamExecuteErr: errors.New("commit failed"),
	}
	state := newTestReservedState("tg1")

	tp := NewTransactionPrimitive(ast.TRANS_STMT_COMMIT, "COMMIT", "tg1")
	err := tp.StreamExecute(context.Background(), mockExec, nil, state, func(_ context.Context, _ *sqltypes.Result) error {
		return nil
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "commit failed")
	// State should still be reset to Idle regardless of error
	require.Equal(t, handler.TxStateIdle, state.GetTransactionState())
}

func TestTransactionPrimitive_Rollback_NoReservedConnections(t *testing.T) {
	mockExec := &txMockIExecute{}
	state := handler.NewMultiGatewayConnectionState()
	state.SetTransactionState(handler.TxStateInTransaction)
	var callbackResult *sqltypes.Result

	tp := NewTransactionPrimitive(ast.TRANS_STMT_ROLLBACK, "ROLLBACK", "tg1")
	err := tp.StreamExecute(context.Background(), mockExec, nil, state, func(_ context.Context, r *sqltypes.Result) error {
		callbackResult = r
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, handler.TxStateIdle, state.GetTransactionState())
	require.Equal(t, 0, mockExec.streamExecuteCount, "No backend call when no reserved connections")
	require.NotNil(t, callbackResult)
	require.Equal(t, "ROLLBACK", callbackResult.CommandTag)
}

func TestTransactionPrimitive_Rollback_WithReservedConnections(t *testing.T) {
	mockExec := &txMockIExecute{
		callbackResult: &sqltypes.Result{CommandTag: "ROLLBACK"},
	}
	state := newTestReservedState("tg1")

	tp := NewTransactionPrimitive(ast.TRANS_STMT_ROLLBACK, "ROLLBACK", "tg1")
	err := tp.StreamExecute(context.Background(), mockExec, nil, state, func(_ context.Context, _ *sqltypes.Result) error {
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, handler.TxStateIdle, state.GetTransactionState())
	require.Equal(t, 1, mockExec.streamExecuteCount)
	require.Equal(t, []string{"ROLLBACK"}, mockExec.streamExecuteSQL)
}

func TestTransactionPrimitive_Rollback_StreamExecuteError(t *testing.T) {
	mockExec := &txMockIExecute{
		streamExecuteErr: errors.New("rollback failed"),
	}
	state := newTestReservedState("tg1")

	tp := NewTransactionPrimitive(ast.TRANS_STMT_ROLLBACK, "ROLLBACK", "tg1")
	err := tp.StreamExecute(context.Background(), mockExec, nil, state, func(_ context.Context, _ *sqltypes.Result) error {
		return nil
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "rollback failed")
	// State should still be reset to Idle regardless of error
	require.Equal(t, handler.TxStateIdle, state.GetTransactionState())
}

func TestTransactionPrimitive_Savepoint_PassThrough(t *testing.T) {
	mockExec := &txMockIExecute{
		callbackResult: &sqltypes.Result{CommandTag: "SAVEPOINT"},
	}
	state := handler.NewMultiGatewayConnectionState()
	state.SetTransactionState(handler.TxStateInTransaction)

	tp := NewTransactionPrimitive(ast.TRANS_STMT_SAVEPOINT, "SAVEPOINT sp1", "tg1")
	err := tp.StreamExecute(context.Background(), mockExec, nil, state, func(_ context.Context, _ *sqltypes.Result) error {
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, 1, mockExec.streamExecuteCount)
	require.Equal(t, []string{"SAVEPOINT sp1"}, mockExec.streamExecuteSQL)
}

func TestTransactionPrimitive_StringAndGetters(t *testing.T) {
	tp := NewTransactionPrimitive(ast.TRANS_STMT_BEGIN, "BEGIN", "my_tg")

	require.Equal(t, "Transaction(BEGIN)", tp.String())
	require.Equal(t, "my_tg", tp.GetTableGroup())
	require.Equal(t, "BEGIN", tp.GetQuery())
}
