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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	pgClient "github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

func newTxTestConn() *server.Conn {
	return server.NewTestConn(&bytes.Buffer{}).Conn
}

// txMockIExecute is a mock IExecute for testing TransactionPrimitive.
type txMockIExecute struct {
	streamExecuteErr   error
	streamExecuteSQL   []string
	streamExecuteResv  []PlanExecInfo
	streamExecuteCount int
	callbackResult     *sqltypes.Result

	concludeTransactionErr                error
	concludeTransactionCount              int
	concludeTransactionConclusion         multipoolerpb.TransactionConclusion
	concludeTransactionReleasePortalNames []string
}

func (m *txMockIExecute) StreamExecute(
	_ context.Context,
	_ *server.Conn,
	_ string,
	_ string,
	sql string,
	_ *query.ExecuteSqlPreparedStatement,
	_ *handler.MultigatewayConnectionState,
	info PlanExecInfo,
	_ bool,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	m.streamExecuteSQL = append(m.streamExecuteSQL, sql)
	m.streamExecuteResv = append(m.streamExecuteResv, info)
	m.streamExecuteCount++
	if m.streamExecuteErr != nil {
		return m.streamExecuteErr
	}
	if m.callbackResult != nil {
		return callback(context.Background(), m.callbackResult)
	}
	return nil
}

func (m *txMockIExecute) PortalStreamExecute(context.Context, string, string, *server.Conn, *handler.MultigatewayConnectionState, *preparedstatement.PortalInfo, int32, bool, PlanExecInfo, bool, func(context.Context, *sqltypes.Result) error) error {
	return nil
}

func (m *txMockIExecute) Describe(context.Context, string, string, *server.Conn, *handler.MultigatewayConnectionState, *preparedstatement.PortalInfo, *preparedstatement.PreparedStatementInfo) (*query.StatementDescription, error) {
	return nil, nil
}

func (m *txMockIExecute) CopyInitiate(context.Context, *server.Conn, string, string, string, *handler.MultigatewayConnectionState, func(context.Context, *sqltypes.Result) error) (int16, []int16, error) {
	return 0, nil, nil
}

func (m *txMockIExecute) CopySendData(context.Context, *server.Conn, string, string, *handler.MultigatewayConnectionState, []byte) error {
	return nil
}

func (m *txMockIExecute) CopyFinalize(context.Context, *server.Conn, string, string, *handler.MultigatewayConnectionState, []byte, func(context.Context, *sqltypes.Result) error) error {
	return nil
}

func (m *txMockIExecute) CopyAbort(context.Context, *server.Conn, string, string, *handler.MultigatewayConnectionState) error {
	return nil
}

func (m *txMockIExecute) CopyOutInitiate(context.Context, *server.Conn, string, string, string, *handler.MultigatewayConnectionState) (int16, []int16, []*mterrors.PgDiagnostic, error) {
	return 0, nil, nil, nil
}

func (m *txMockIExecute) CopyOutStream(context.Context, *server.Conn, string, string, *handler.MultigatewayConnectionState, func(pgClient.CopyOutMessage) error) (*sqltypes.Result, error) {
	return nil, nil
}

func (m *txMockIExecute) ConcludeTransaction(
	ctx context.Context,
	_ *server.Conn,
	state *handler.MultigatewayConnectionState,
	conclusion multipoolerpb.TransactionConclusion,
	releasePortalNames []string,
	_ bool,
	chain bool,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	m.concludeTransactionCount++
	m.concludeTransactionConclusion = conclusion
	m.concludeTransactionReleasePortalNames = append([]string(nil), releasePortalNames...)
	if m.concludeTransactionErr != nil {
		// On error, clear all shard states (matches ScatterConn behavior)
		state.ClearAllReservedConnections()
		return m.concludeTransactionErr
	}
	// Simulate successful conclude. AND CHAIN keeps the backend transaction
	// reservation; ordinary COMMIT/ROLLBACK releases it.
	if !chain {
		state.ClearAllReservedConnections()
	}
	var commandTag string
	if conclusion == multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_COMMIT {
		commandTag = "COMMIT"
	} else {
		commandTag = "ROLLBACK"
	}
	return callback(ctx, &sqltypes.Result{CommandTag: commandTag})
}

func (m *txMockIExecute) DiscardTempTables(ctx context.Context, conn *server.Conn, state *handler.MultigatewayConnectionState, callback func(context.Context, *sqltypes.Result) error) error {
	return nil
}

func (m *txMockIExecute) ReleaseAllReservedConnections(context.Context, *server.Conn, *handler.MultigatewayConnectionState) error {
	return nil
}

// newTestReservedState creates a state with a reserved connection on the given tableGroup.
// It also sets the conn's txn status to InBlock (in transaction).
func newTestReservedState(tableGroup string, conn *server.Conn) *handler.MultigatewayConnectionState {
	state := handler.NewMultigatewayConnectionState()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	target := protoutil.NewTarget("", tableGroup, "", query.Mode_MODE_WRITABLE)
	state.SetReservedConnection(target, &query.ReservedState{
		ReservedConnectionId: 100,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonTransaction,
	})
	return state
}

func TestTransactionPrimitive_Begin_SetsStateAndReturnsSyntheticResult(t *testing.T) {
	mockExec := &txMockIExecute{}
	state := handler.NewMultigatewayConnectionState()
	conn := newTxTestConn()
	var callbackResult *sqltypes.Result

	tp := NewTransactionPrimitive(ast.TRANS_STMT_BEGIN, "", false, "BEGIN", "tg1", nil)
	err := tp.StreamExecute(context.Background(), mockExec, conn, state, nil, PlanExecInfo{}, func(_ context.Context, r *sqltypes.Result) error {
		callbackResult = r
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, protocol.TxnStatusInBlock, conn.TxnStatus())
	require.Equal(t, 0, mockExec.streamExecuteCount, "BEGIN should not call backend")
	require.NotNil(t, callbackResult)
	require.Equal(t, "BEGIN", callbackResult.CommandTag)
	require.False(t, state.TxnStartTime.IsZero(), "BEGIN should set TxnStartTime")
}

func TestTransactionPrimitive_StartTransaction(t *testing.T) {
	mockExec := &txMockIExecute{}
	state := handler.NewMultigatewayConnectionState()
	conn := newTxTestConn()
	var callbackResult *sqltypes.Result

	tp := NewTransactionPrimitive(ast.TRANS_STMT_START, "", false, "START TRANSACTION", "tg1", nil)
	err := tp.StreamExecute(context.Background(), mockExec, conn, state, nil, PlanExecInfo{}, func(_ context.Context, r *sqltypes.Result) error {
		callbackResult = r
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, protocol.TxnStatusInBlock, conn.TxnStatus())
	require.Equal(t, 0, mockExec.streamExecuteCount, "START should not call backend")
	require.NotNil(t, callbackResult)
	require.Equal(t, "BEGIN", callbackResult.CommandTag)
}

func TestTransactionPrimitive_Commit_NoReservedConnections(t *testing.T) {
	mockExec := &txMockIExecute{}
	state := handler.NewMultigatewayConnectionState()
	state.TxnStartTime = time.Now()
	conn := newTxTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	var callbackResult *sqltypes.Result

	tp := NewTransactionPrimitive(ast.TRANS_STMT_COMMIT, "", false, "COMMIT", "tg1", nil)
	err := tp.StreamExecute(context.Background(), mockExec, conn, state, nil, PlanExecInfo{}, func(_ context.Context, r *sqltypes.Result) error {
		callbackResult = r
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, protocol.TxnStatusIdle, conn.TxnStatus())
	require.Equal(t, 0, mockExec.streamExecuteCount, "No backend call when no reserved connections")
	require.NotNil(t, callbackResult)
	require.Equal(t, "COMMIT", callbackResult.CommandTag)
	require.Empty(t, callbackResult.Notices, "no warning when the session was in a transaction")
	require.True(t, state.TxnStartTime.IsZero(), "COMMIT should clear TxnStartTime")
}

// PostgreSQL emits WARNING 25P01 "there is no transaction in progress" when
// COMMIT/END or ROLLBACK/ABORT runs outside a transaction block, and still
// returns the usual command tag. Verify the gateway matches.
func TestTransactionPrimitive_ConclusionOutsideTxnWarns(t *testing.T) {
	for _, tc := range []struct {
		kind ast.TransactionStmtKind
		tag  string
	}{
		{ast.TRANS_STMT_COMMIT, "COMMIT"},
		{ast.TRANS_STMT_ROLLBACK, "ROLLBACK"},
	} {
		t.Run(tc.tag, func(t *testing.T) {
			mockExec := &txMockIExecute{}
			state := handler.NewMultigatewayConnectionState()
			conn := newTxTestConn()
			conn.SetTxnStatus(protocol.TxnStatusIdle) // not in a transaction
			var callbackResult *sqltypes.Result

			tp := NewTransactionPrimitive(tc.kind, "", false, tc.tag, "tg1", nil)
			err := tp.StreamExecute(context.Background(), mockExec, conn, state, nil, PlanExecInfo{}, func(_ context.Context, r *sqltypes.Result) error {
				callbackResult = r
				return nil
			})

			require.NoError(t, err)
			require.NotNil(t, callbackResult)
			require.Equal(t, tc.tag, callbackResult.CommandTag)
			require.Len(t, callbackResult.Notices, 1)
			notice := callbackResult.Notices[0]
			require.Equal(t, "WARNING", notice.Severity)
			require.Equal(t, "25P01", notice.Code)
			require.Equal(t, "there is no transaction in progress", notice.Message)
		})
	}
}

// PostgreSQL converts COMMIT into ROLLBACK when the transaction is in a
// failed state, so any SET / RESET issued before the failure must revert.
// Verify the gateway matches: COMMIT in TxnStatusFailed restores
// SessionSettings from the BEGIN-level snapshot.
func TestTransactionPrimitive_CommitAndChain_NoReservedConnectionsPreservesInheritedOptions(t *testing.T) {
	mockExec := &txMockIExecute{}
	state := handler.NewMultigatewayConnectionState()
	state.PendingBeginQuery = "START TRANSACTION ISOLATION LEVEL REPEATABLE READ READ WRITE DEFERRABLE"
	state.ActiveTransactionBeginQuery = state.PendingBeginQuery
	state.TxnStartTime = time.Now()
	conn := newTxTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	var callbackResult *sqltypes.Result

	tp := NewTransactionPrimitive(ast.TRANS_STMT_COMMIT, "", true, "COMMIT AND CHAIN", "tg1", nil)
	err := tp.StreamExecute(context.Background(), mockExec, conn, state, nil, PlanExecInfo{}, func(_ context.Context, r *sqltypes.Result) error {
		callbackResult = r
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, protocol.TxnStatusInBlock, conn.TxnStatus())
	require.Equal(t, 0, mockExec.concludeTransactionCount, "No backend call when no reserved connections")
	require.NotNil(t, callbackResult)
	require.Equal(t, "COMMIT", callbackResult.CommandTag)
	require.Equal(t, "START TRANSACTION ISOLATION LEVEL REPEATABLE READ READ WRITE DEFERRABLE", state.PendingBeginQuery)
	require.Equal(t, state.PendingBeginQuery, state.ActiveTransactionBeginQuery)
	require.False(t, state.TxnStartTime.IsZero(), "chained transaction should start a new timer")
}

func TestTransactionPrimitive_RollbackAndChain_NoReservedConnectionsPreservesInheritedOptions(t *testing.T) {
	mockExec := &txMockIExecute{}
	state := handler.NewMultigatewayConnectionState()
	state.PendingBeginQuery = "START TRANSACTION ISOLATION LEVEL SERIALIZABLE READ WRITE NOT DEFERRABLE"
	state.ActiveTransactionBeginQuery = state.PendingBeginQuery
	state.TxnStartTime = time.Now()
	conn := newTxTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	var callbackResult *sqltypes.Result

	tp := NewTransactionPrimitive(ast.TRANS_STMT_ROLLBACK, "", true, "ROLLBACK AND CHAIN", "tg1", nil)
	err := tp.StreamExecute(context.Background(), mockExec, conn, state, nil, PlanExecInfo{}, func(_ context.Context, r *sqltypes.Result) error {
		callbackResult = r
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, protocol.TxnStatusInBlock, conn.TxnStatus())
	require.Equal(t, 0, mockExec.concludeTransactionCount, "No backend call when no reserved connections")
	require.NotNil(t, callbackResult)
	require.Equal(t, "ROLLBACK", callbackResult.CommandTag)
	require.Equal(t, "START TRANSACTION ISOLATION LEVEL SERIALIZABLE READ WRITE NOT DEFERRABLE", state.PendingBeginQuery)
	require.Equal(t, state.PendingBeginQuery, state.ActiveTransactionBeginQuery)
	require.False(t, state.TxnStartTime.IsZero(), "chained transaction should start a new timer")
}

func TestTransactionPrimitive_CommitAndChain_WithReservedConnectionKeepsBackendChained(t *testing.T) {
	mockExec := &txMockIExecute{}
	conn := newTxTestConn()
	state := newTestReservedState("tg1", conn)
	state.ActiveTransactionBeginQuery = "START TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY DEFERRABLE"
	state.TxnStartTime = time.Now()
	var callbackResult *sqltypes.Result

	tp := NewTransactionPrimitive(ast.TRANS_STMT_COMMIT, "", true, "COMMIT AND CHAIN", "tg1", nil)
	err := tp.StreamExecute(context.Background(), mockExec, conn, state, nil, PlanExecInfo{}, func(_ context.Context, r *sqltypes.Result) error {
		callbackResult = r
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, protocol.TxnStatusInBlock, conn.TxnStatus())
	require.Equal(t, 1, mockExec.concludeTransactionCount)
	require.NotNil(t, callbackResult)
	require.Equal(t, "COMMIT", callbackResult.CommandTag)
	require.NotEmpty(t, state.ShardStates, "successful AND CHAIN keeps the reserved backend transaction")
	require.Empty(t, state.PendingBeginQuery, "backend already executed COMMIT AND CHAIN")
	require.Equal(t, "START TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY DEFERRABLE", state.ActiveTransactionBeginQuery)
}

func TestTransactionPrimitive_CommitAndChain_ConcludeErrorFailsClosed(t *testing.T) {
	mockExec := &txMockIExecute{concludeTransactionErr: errors.New("commit failed")}
	conn := newTxTestConn()
	state := newTestReservedState("tg1", conn)
	state.ActiveTransactionBeginQuery = "START TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE"
	state.TxnStartTime = time.Now()

	tp := NewTransactionPrimitive(ast.TRANS_STMT_COMMIT, "", true, "COMMIT AND CHAIN", "tg1", nil)
	err := tp.StreamExecute(context.Background(), mockExec, conn, state, nil, PlanExecInfo{}, func(context.Context, *sqltypes.Result) error {
		return nil
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "commit failed")
	require.Equal(t, protocol.TxnStatusIdle, conn.TxnStatus())
	require.Empty(t, state.ShardStates, "failed conclude clears the lost backend reservation")
	require.Empty(t, state.PendingBeginQuery, "lost backend-backed chain must not be transparently replayed")
	require.Empty(t, state.ActiveTransactionBeginQuery)
	require.True(t, state.TxnStartTime.IsZero())
	require.Equal(t, 0, state.SavepointDepth())
}

func TestTransactionPrimitive_RollbackAndChain_ConcludeErrorFailsClosed(t *testing.T) {
	mockExec := &txMockIExecute{concludeTransactionErr: errors.New("rollback failed")}
	conn := newTxTestConn()
	state := newTestReservedState("tg1", conn)
	state.ActiveTransactionBeginQuery = "START TRANSACTION ISOLATION LEVEL REPEATABLE READ READ WRITE NOT DEFERRABLE"
	state.TxnStartTime = time.Now()

	tp := NewTransactionPrimitive(ast.TRANS_STMT_ROLLBACK, "", true, "ROLLBACK AND CHAIN", "tg1", nil)
	err := tp.StreamExecute(context.Background(), mockExec, conn, state, nil, PlanExecInfo{}, func(context.Context, *sqltypes.Result) error {
		return nil
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "rollback failed")
	require.Equal(t, protocol.TxnStatusIdle, conn.TxnStatus())
	require.Empty(t, state.ShardStates, "failed conclude clears the lost backend reservation")
	require.Empty(t, state.PendingBeginQuery, "lost backend-backed chain must not be transparently replayed")
	require.Empty(t, state.ActiveTransactionBeginQuery)
	require.True(t, state.TxnStartTime.IsZero())
	require.Equal(t, 0, state.SavepointDepth())
}

func TestTransactionPrimitive_CommitAndChain_OutsideTransactionErrors(t *testing.T) {
	mockExec := &txMockIExecute{}
	state := handler.NewMultigatewayConnectionState()
	conn := newTxTestConn()

	tp := NewTransactionPrimitive(ast.TRANS_STMT_COMMIT, "", true, "COMMIT AND CHAIN", "tg1", nil)
	err := tp.StreamExecute(context.Background(), mockExec, conn, state, nil, PlanExecInfo{}, func(context.Context, *sqltypes.Result) error {
		return nil
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "COMMIT AND CHAIN can only be used in transaction blocks")
	require.Equal(t, protocol.TxnStatusIdle, conn.TxnStatus())
}

func TestTransactionPrimitive_Commit_FailedTxn_RevertsSessionSettings(t *testing.T) {
	mockExec := &txMockIExecute{}
	state := handler.NewMultigatewayConnectionState()
	state.SetSessionVariable("datestyle", "ISO, MDY")
	state.BeginTransaction()
	state.SetSessionVariable("datestyle", "German")

	conn := newTxTestConn()
	conn.SetTxnStatus(protocol.TxnStatusFailed)

	tp := NewTransactionPrimitive(ast.TRANS_STMT_COMMIT, "", false, "COMMIT", "tg1", nil)
	err := tp.StreamExecute(context.Background(), mockExec, conn, state, nil, PlanExecInfo{}, func(_ context.Context, _ *sqltypes.Result) error {
		return nil
	})

	require.NoError(t, err)
	v, ok := state.GetSessionVariable("datestyle")
	require.True(t, ok)
	require.Equal(t, "ISO, MDY", v, "COMMIT on aborted txn must revert SET like ROLLBACK")
	require.Equal(t, 0, state.SavepointDepth())
}

func TestTransactionPrimitive_Commit_WithReservedConnections(t *testing.T) {
	mockExec := &txMockIExecute{}
	conn := newTxTestConn()
	state := newTestReservedState("tg1", conn)

	tp := NewTransactionPrimitive(ast.TRANS_STMT_COMMIT, "", false, "COMMIT", "tg1", nil)
	err := tp.StreamExecute(context.Background(), mockExec, conn, state, nil, PlanExecInfo{}, func(_ context.Context, _ *sqltypes.Result) error {
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, protocol.TxnStatusIdle, conn.TxnStatus())
	require.Equal(t, 1, mockExec.concludeTransactionCount, "Should call ConcludeTransaction")
	require.Equal(t, multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_COMMIT, mockExec.concludeTransactionConclusion)
	require.Equal(t, 0, mockExec.streamExecuteCount, "Should not use StreamExecute for COMMIT")
	require.Empty(t, state.ShardStates, "ShardStates should be cleared after COMMIT")
}

func TestTransactionPrimitive_Commit_ConcludeTransactionError(t *testing.T) {
	mockExec := &txMockIExecute{
		concludeTransactionErr: errors.New("commit failed"),
	}
	conn := newTxTestConn()
	state := newTestReservedState("tg1", conn)

	tp := NewTransactionPrimitive(ast.TRANS_STMT_COMMIT, "", false, "COMMIT", "tg1", nil)
	err := tp.StreamExecute(context.Background(), mockExec, conn, state, nil, PlanExecInfo{}, func(_ context.Context, _ *sqltypes.Result) error {
		return nil
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "commit failed")
	// State should still be reset to Idle regardless of error
	require.Equal(t, protocol.TxnStatusIdle, conn.TxnStatus())
	require.Empty(t, state.ShardStates, "ShardStates should be cleared even on error")
}

func TestTransactionPrimitive_Rollback_NoReservedConnections(t *testing.T) {
	mockExec := &txMockIExecute{}
	state := handler.NewMultigatewayConnectionState()
	state.TxnStartTime = time.Now()
	conn := newTxTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	var callbackResult *sqltypes.Result

	tp := NewTransactionPrimitive(ast.TRANS_STMT_ROLLBACK, "", false, "ROLLBACK", "tg1", nil)
	err := tp.StreamExecute(context.Background(), mockExec, conn, state, nil, PlanExecInfo{}, func(_ context.Context, r *sqltypes.Result) error {
		callbackResult = r
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, protocol.TxnStatusIdle, conn.TxnStatus())
	require.Equal(t, 0, mockExec.streamExecuteCount, "No backend call when no reserved connections")
	require.NotNil(t, callbackResult)
	require.Equal(t, "ROLLBACK", callbackResult.CommandTag)
	require.True(t, state.TxnStartTime.IsZero(), "ROLLBACK should clear TxnStartTime")
}

func TestTransactionPrimitive_Rollback_WithReservedConnections(t *testing.T) {
	mockExec := &txMockIExecute{}
	conn := newTxTestConn()
	state := newTestReservedState("tg1", conn)

	tp := NewTransactionPrimitive(ast.TRANS_STMT_ROLLBACK, "", false, "ROLLBACK", "tg1", nil)
	err := tp.StreamExecute(context.Background(), mockExec, conn, state, nil, PlanExecInfo{}, func(_ context.Context, _ *sqltypes.Result) error {
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, protocol.TxnStatusIdle, conn.TxnStatus())
	require.Equal(t, 1, mockExec.concludeTransactionCount, "Should call ConcludeTransaction")
	require.Equal(t, multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_ROLLBACK, mockExec.concludeTransactionConclusion)
	require.Equal(t, 0, mockExec.streamExecuteCount, "Should not use StreamExecute for ROLLBACK")
	require.Empty(t, state.ShardStates, "ShardStates should be cleared after ROLLBACK")
}

func TestTransactionPrimitive_Rollback_ConcludeTransactionError(t *testing.T) {
	mockExec := &txMockIExecute{
		concludeTransactionErr: errors.New("rollback failed"),
	}
	conn := newTxTestConn()
	state := newTestReservedState("tg1", conn)

	tp := NewTransactionPrimitive(ast.TRANS_STMT_ROLLBACK, "", false, "ROLLBACK", "tg1", nil)
	err := tp.StreamExecute(context.Background(), mockExec, conn, state, nil, PlanExecInfo{}, func(_ context.Context, _ *sqltypes.Result) error {
		return nil
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "rollback failed")
	// State should still be reset to Idle regardless of error
	require.Equal(t, protocol.TxnStatusIdle, conn.TxnStatus())
	require.Empty(t, state.ShardStates, "ShardStates should be cleared even on error")
}

func TestTransactionPrimitive_PrepareTransaction_SuccessEndsTransactionAndReleasesReservation(t *testing.T) {
	mockExec := &txMockIExecute{
		callbackResult: &sqltypes.Result{CommandTag: "PREPARE TRANSACTION"},
	}
	conn := newTxTestConn()
	state := newTestReservedState("tg1", conn)
	state.SetSessionVariable("datestyle", "ISO, MDY")
	state.BeginTransaction()
	state.SetSessionVariable("datestyle", "German")

	var callbackResult *sqltypes.Result
	tp := NewTransactionPrimitive(ast.TRANS_STMT_PREPARE, "", false, "PREPARE TRANSACTION 'gid'", "tg1", nil)
	err := tp.StreamExecute(context.Background(), mockExec, conn, state, nil, PlanExecInfo{}, func(_ context.Context, r *sqltypes.Result) error {
		callbackResult = r
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, protocol.TxnStatusIdle, conn.TxnStatus())
	require.Equal(t, 1, mockExec.streamExecuteCount)
	require.Equal(t, []string{"PREPARE TRANSACTION 'gid'"}, mockExec.streamExecuteSQL)
	require.Equal(t, 1, mockExec.concludeTransactionCount, "PREPARE should release the transaction reservation")
	require.Equal(t, multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_COMMIT, mockExec.concludeTransactionConclusion)
	require.Empty(t, state.ShardStates, "transaction reservation should be cleared after PREPARE")
	require.NotNil(t, callbackResult)
	require.Equal(t, "PREPARE TRANSACTION", callbackResult.CommandTag)
	v, ok := state.GetSessionVariable("datestyle")
	require.True(t, ok)
	require.Equal(t, "German", v, "successful PREPARE keeps committed session SET state")
	require.Equal(t, 0, state.SavepointDepth())
}

func TestTransactionPrimitive_CommitPrepared_PassThrough(t *testing.T) {
	mockExec := &txMockIExecute{
		callbackResult: &sqltypes.Result{CommandTag: "COMMIT PREPARED"},
	}
	conn := newTxTestConn()
	state := handler.NewMultigatewayConnectionState()

	var callbackResult *sqltypes.Result
	tp := NewTransactionPrimitive(ast.TRANS_STMT_COMMIT_PREPARED, "", false, "COMMIT PREPARED 'gid'", "tg1", nil)
	err := tp.StreamExecute(context.Background(), mockExec, conn, state, nil, PlanExecInfo{}, func(_ context.Context, r *sqltypes.Result) error {
		callbackResult = r
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, 1, mockExec.streamExecuteCount)
	require.Equal(t, []string{"COMMIT PREPARED 'gid'"}, mockExec.streamExecuteSQL)
	require.Equal(t, 0, mockExec.concludeTransactionCount)
	require.NotNil(t, callbackResult)
	require.Equal(t, "COMMIT PREPARED", callbackResult.CommandTag)
}

func TestTransactionPrimitive_RollbackPrepared_PassThrough(t *testing.T) {
	mockExec := &txMockIExecute{
		callbackResult: &sqltypes.Result{CommandTag: "ROLLBACK PREPARED"},
	}
	conn := newTxTestConn()
	state := handler.NewMultigatewayConnectionState()

	var callbackResult *sqltypes.Result
	tp := NewTransactionPrimitive(ast.TRANS_STMT_ROLLBACK_PREPARED, "", false, "ROLLBACK PREPARED 'gid'", "tg1", nil)
	err := tp.StreamExecute(context.Background(), mockExec, conn, state, nil, PlanExecInfo{}, func(_ context.Context, r *sqltypes.Result) error {
		callbackResult = r
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, 1, mockExec.streamExecuteCount)
	require.Equal(t, []string{"ROLLBACK PREPARED 'gid'"}, mockExec.streamExecuteSQL)
	require.Equal(t, 0, mockExec.concludeTransactionCount)
	require.NotNil(t, callbackResult)
	require.Equal(t, "ROLLBACK PREPARED", callbackResult.CommandTag)
}

func TestTransactionPrimitive_PrepareTransaction_CleanupFailureDoesNotFailSuccessfulPrepare(t *testing.T) {
	cleanupErr := errors.New("cleanup failed")
	mockExec := &txMockIExecute{
		callbackResult:         &sqltypes.Result{CommandTag: "PREPARE TRANSACTION"},
		concludeTransactionErr: cleanupErr,
	}
	conn := newTxTestConn()
	state := newTestReservedState("tg1", conn)
	state.SetSessionVariable("datestyle", "ISO, MDY")
	state.BeginTransaction()
	state.SetSessionVariable("datestyle", "German")

	var callbackResult *sqltypes.Result
	tp := NewTransactionPrimitive(ast.TRANS_STMT_PREPARE, "", false, "PREPARE TRANSACTION 'gid'", "tg1", nil)
	err := tp.StreamExecute(context.Background(), mockExec, conn, state, nil, PlanExecInfo{}, func(_ context.Context, r *sqltypes.Result) error {
		callbackResult = r
		return nil
	})

	require.NoError(t, err, "cleanup failure must not turn a durable PREPARE into client-visible failure")
	require.NotNil(t, callbackResult)
	require.Equal(t, "PREPARE TRANSACTION", callbackResult.CommandTag)
	require.Equal(t, protocol.TxnStatusIdle, conn.TxnStatus())
	require.Equal(t, 1, mockExec.streamExecuteCount)
	require.Equal(t, 1, mockExec.concludeTransactionCount, "cleanup should still be attempted")
	require.Equal(t, multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_COMMIT, mockExec.concludeTransactionConclusion)
	require.Empty(t, state.ShardStates, "failed cleanup should clear the lost reservation locally")
	v, ok := state.GetSessionVariable("datestyle")
	require.True(t, ok)
	require.Equal(t, "German", v, "successful PREPARE keeps committed session SET state")
	require.Equal(t, 0, state.SavepointDepth())
}

func TestTransactionPrimitive_PrepareTransaction_BackendRollbackTagRollsBackGatewayState(t *testing.T) {
	mockExec := &txMockIExecute{
		callbackResult: &sqltypes.Result{CommandTag: "ROLLBACK"},
	}
	conn := newTxTestConn()
	state := newTestReservedState("tg1", conn)
	conn.SetTxnStatus(protocol.TxnStatusFailed)
	state.SetSessionVariable("datestyle", "ISO, MDY")
	state.AddOpenHoldCursor("pre_existing")
	state.BeginTransaction()
	state.SetSessionVariable("datestyle", "German")
	state.AddOpenHoldCursor("declared_in_txn")

	var callbackResult *sqltypes.Result
	tp := NewTransactionPrimitive(ast.TRANS_STMT_PREPARE, "", false, "PREPARE TRANSACTION 'gid'", "tg1", nil)
	err := tp.StreamExecute(context.Background(), mockExec, conn, state, nil, PlanExecInfo{}, func(_ context.Context, r *sqltypes.Result) error {
		callbackResult = r
		return nil
	})

	require.NoError(t, err)
	require.NotNil(t, callbackResult)
	require.Equal(t, "ROLLBACK", callbackResult.CommandTag)
	require.Equal(t, protocol.TxnStatusIdle, conn.TxnStatus(), "PREPARE returning ROLLBACK ends the failed transaction")
	require.Equal(t, 1, mockExec.streamExecuteCount)
	require.Equal(t, 1, mockExec.concludeTransactionCount, "PREPARE returning ROLLBACK should drop the transaction reservation")
	require.Equal(t, multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_ROLLBACK, mockExec.concludeTransactionConclusion)
	require.Equal(t, []string{"declared_in_txn"}, mockExec.concludeTransactionReleasePortalNames)
	require.Empty(t, state.ShardStates, "transaction reservation should be cleared after ROLLBACK-tag PREPARE")
	require.True(t, state.HasOpenHoldCursor("pre_existing"), "ROLLBACK-tag PREPARE should preserve HOLD cursors that pre-date the transaction")
	require.False(t, state.HasOpenHoldCursor("declared_in_txn"), "ROLLBACK-tag PREPARE should drop HOLD cursors declared in the transaction")
	v, ok := state.GetSessionVariable("datestyle")
	require.True(t, ok)
	require.Equal(t, "ISO, MDY", v, "ROLLBACK-tag PREPARE reverts transaction-local session SET state")
	require.Equal(t, 0, state.SavepointDepth())
}

func TestTransactionPrimitive_PrepareTransaction_FailureRollsBackGatewayStateAndReleasesReservation(t *testing.T) {
	prepareErr := errors.New("prepared transactions are disabled")
	mockExec := &txMockIExecute{streamExecuteErr: prepareErr}
	conn := newTxTestConn()
	state := newTestReservedState("tg1", conn)
	state.SetSessionVariable("datestyle", "ISO, MDY")
	state.AddOpenHoldCursor("pre_existing")
	state.BeginTransaction()
	state.SetSessionVariable("datestyle", "German")
	state.AddOpenHoldCursor("declared_in_txn")

	var callbackCalled bool
	tp := NewTransactionPrimitive(ast.TRANS_STMT_PREPARE, "", false, "PREPARE TRANSACTION 'gid'", "tg1", nil)
	err := tp.StreamExecute(context.Background(), mockExec, conn, state, nil, PlanExecInfo{}, func(_ context.Context, _ *sqltypes.Result) error {
		callbackCalled = true
		return nil
	})

	require.ErrorIs(t, err, prepareErr)
	require.False(t, callbackCalled, "failing PREPARE should surface only the backend error")
	require.Equal(t, protocol.TxnStatusIdle, conn.TxnStatus(), "failed PREPARE ends the transaction")
	require.Equal(t, 1, mockExec.streamExecuteCount)
	require.Equal(t, 1, mockExec.concludeTransactionCount, "failed PREPARE should drop the transaction reservation")
	require.Equal(t, multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_ROLLBACK, mockExec.concludeTransactionConclusion)
	require.Equal(t, []string{"declared_in_txn"}, mockExec.concludeTransactionReleasePortalNames)
	require.Empty(t, state.ShardStates, "transaction reservation should be cleared after failed PREPARE")
	require.True(t, state.HasOpenHoldCursor("pre_existing"), "failed PREPARE should preserve HOLD cursors that pre-date the transaction")
	require.False(t, state.HasOpenHoldCursor("declared_in_txn"), "failed PREPARE should drop HOLD cursors declared in the transaction")
	v, ok := state.GetSessionVariable("datestyle")
	require.True(t, ok)
	require.Equal(t, "ISO, MDY", v, "failed PREPARE reverts transaction-local session SET state")
	require.Equal(t, 0, state.SavepointDepth())
}

func TestTransactionPrimitive_Savepoint_PassThrough(t *testing.T) {
	mockExec := &txMockIExecute{
		callbackResult: &sqltypes.Result{CommandTag: "SAVEPOINT"},
	}
	state := handler.NewMultigatewayConnectionState()
	conn := newTxTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	tp := NewTransactionPrimitive(ast.TRANS_STMT_SAVEPOINT, "sp1", false, "SAVEPOINT sp1", "tg1", nil)
	err := tp.StreamExecute(context.Background(), mockExec, conn, state, nil, PlanExecInfo{}, func(_ context.Context, _ *sqltypes.Result) error {
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, 1, mockExec.streamExecuteCount)
	require.Equal(t, []string{"SAVEPOINT sp1"}, mockExec.streamExecuteSQL)
}

func TestTransactionPrimitive_StringAndGetters(t *testing.T) {
	tp := NewTransactionPrimitive(ast.TRANS_STMT_BEGIN, "", false, "BEGIN", "my_tg", nil)

	require.Equal(t, "Transaction(BEGIN)", tp.String())
	require.Equal(t, "my_tg", tp.GetTableGroup())
	require.Equal(t, "BEGIN", tp.GetQuery())
}
