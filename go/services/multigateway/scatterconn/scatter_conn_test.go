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

package scatterconn

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// mockGateway is a mock implementation of poolergateway.Gateway for testing.
type mockGateway struct {
	// StreamExecute tracking
	streamExecuteCalled      bool
	streamExecuteSQL         string
	streamExecuteOpts        *query.ExecuteOptions
	streamExecuteErr         error
	streamExecuteReturnState queryservice.ReservedState

	// ReserveStreamExecute tracking
	reserveStreamExecuteCalled bool
	reserveStreamExecuteSQL    string
	reserveStreamExecuteResult queryservice.ReservedState
	reserveStreamExecuteErr    error

	// QueryServiceByID tracking
	queryServiceByIDCalled bool
	queryServiceByIDErr    error

	// ConcludeTransaction tracking
	concludeTransactionResult      *sqltypes.Result
	concludeTransactionReturnState queryservice.ReservedState
	concludeTransactionErr         error

	// CopyFinalize tracking
	copyFinalizeResult      *sqltypes.Result
	copyFinalizeReturnState queryservice.ReservedState
	copyFinalizeErr         error

	// CopyAbort tracking
	copyAbortReturnState queryservice.ReservedState
	copyAbortErr         error

	// Callback control
	callbackResult *sqltypes.Result
}

// StreamExecute implements queryservice.QueryService.
func (m *mockGateway) StreamExecute(_ context.Context, _ *query.Target, sql string, opts *query.ExecuteOptions, callback func(context.Context, *sqltypes.Result) error) (queryservice.ReservedState, error) {
	m.streamExecuteCalled = true
	m.streamExecuteSQL = sql
	m.streamExecuteOpts = opts
	if m.streamExecuteErr != nil {
		return m.streamExecuteReturnState, m.streamExecuteErr
	}
	if m.callbackResult != nil {
		if err := callback(context.Background(), m.callbackResult); err != nil {
			return m.streamExecuteReturnState, err
		}
	}
	return m.streamExecuteReturnState, nil
}

// ReserveStreamExecute implements queryservice.QueryService.
func (m *mockGateway) ReserveStreamExecute(_ context.Context, _ *query.Target, sql string, _ *query.ExecuteOptions, _ *multipoolerpb.ReservationOptions, callback func(context.Context, *sqltypes.Result) error) (queryservice.ReservedState, error) {
	m.reserveStreamExecuteCalled = true
	m.reserveStreamExecuteSQL = sql
	if m.reserveStreamExecuteErr != nil {
		return queryservice.ReservedState{}, m.reserveStreamExecuteErr
	}
	if m.callbackResult != nil {
		_ = callback(context.Background(), m.callbackResult)
	}
	return m.reserveStreamExecuteResult, nil
}

// QueryServiceByID implements poolergateway.Gateway.
func (m *mockGateway) QueryServiceByID(_ context.Context, _ *clustermetadatapb.ID, _ *query.Target) (queryservice.QueryService, error) {
	m.queryServiceByIDCalled = true
	if m.queryServiceByIDErr != nil {
		return nil, m.queryServiceByIDErr
	}
	return m, nil
}

// Unused interface methods.
func (m *mockGateway) ExecuteQuery(context.Context, *query.Target, string, *query.ExecuteOptions) (*sqltypes.Result, queryservice.ReservedState, error) {
	return nil, queryservice.ReservedState{}, nil
}

func (m *mockGateway) PortalStreamExecute(context.Context, *query.Target, *query.PreparedStatement, *query.Portal, *query.ExecuteOptions, func(context.Context, *sqltypes.Result) error) (queryservice.ReservedState, error) {
	return queryservice.ReservedState{}, nil
}

func (m *mockGateway) Describe(context.Context, *query.Target, *query.PreparedStatement, *query.Portal, *query.ExecuteOptions) (*query.StatementDescription, error) {
	return nil, nil
}

func (m *mockGateway) Close() error { return nil }

func (m *mockGateway) CopyReady(context.Context, *query.Target, string, *query.ExecuteOptions, *multipoolerpb.ReservationOptions) (int16, []int16, queryservice.ReservedState, error) {
	return 0, nil, queryservice.ReservedState{}, nil
}

func (m *mockGateway) CopySendData(context.Context, *query.Target, []byte, *query.ExecuteOptions) error {
	return nil
}

func (m *mockGateway) CopyFinalize(_ context.Context, _ *query.Target, _ []byte, _ *query.ExecuteOptions) (*sqltypes.Result, queryservice.ReservedState, error) {
	return m.copyFinalizeResult, m.copyFinalizeReturnState, m.copyFinalizeErr
}

func (m *mockGateway) CopyAbort(_ context.Context, _ *query.Target, _ string, _ *query.ExecuteOptions) (queryservice.ReservedState, error) {
	return m.copyAbortReturnState, m.copyAbortErr
}

func (m *mockGateway) ReleaseReservedConnection(context.Context, *query.Target, *query.ExecuteOptions) error {
	return nil
}

func (m *mockGateway) ConcludeTransaction(_ context.Context, _ *query.Target, _ *query.ExecuteOptions, _ multipoolerpb.TransactionConclusion) (*sqltypes.Result, queryservice.ReservedState, error) {
	return m.concludeTransactionResult, m.concludeTransactionReturnState, m.concludeTransactionErr
}

// newTestConn creates a test server.Conn for ScatterConn tests.
func newTestConn() *server.Conn {
	return server.NewTestConn(&bytes.Buffer{}).Conn
}

func TestScatterConn_Case1_ExistingReservedConnection(t *testing.T) {
	gw := &mockGateway{
		callbackResult: &sqltypes.Result{CommandTag: "SELECT 1"},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultiGatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	target := &query.Target{
		TableGroup: "tg1",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	state.SetReservedConnection(target, queryservice.ReservedState{
		ReservedConnectionId: 42,
		PoolerID:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonTransaction,
	})

	err := sc.StreamExecute(context.Background(), conn, "tg1", "", "SELECT 1", state,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.NoError(t, err)
	require.True(t, gw.queryServiceByIDCalled, "should use QueryServiceByID for existing reserved connection")
	require.True(t, gw.streamExecuteCalled, "should call StreamExecute on the found query service")
	require.Equal(t, "SELECT 1", gw.streamExecuteSQL)
	require.Equal(t, uint64(42), gw.streamExecuteOpts.ReservedConnectionId)
	require.False(t, gw.reserveStreamExecuteCalled, "should not call ReserveStreamExecute")
}

func TestScatterConn_Case2_InTransactionNoReservedConn(t *testing.T) {
	gw := &mockGateway{
		reserveStreamExecuteResult: queryservice.ReservedState{
			ReservedConnectionId: 77,
			PoolerID:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		},
		callbackResult: &sqltypes.Result{CommandTag: "SELECT 1"},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultiGatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	err := sc.StreamExecute(context.Background(), conn, "tg1", "", "SELECT 1", state,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.NoError(t, err)
	require.True(t, gw.reserveStreamExecuteCalled, "should call ReserveStreamExecute")
	require.Equal(t, "SELECT 1", gw.reserveStreamExecuteSQL)
	require.False(t, gw.queryServiceByIDCalled)

	// Verify state was updated with the reserved connection
	target := &query.Target{
		TableGroup: "tg1",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	ss := state.GetMatchingShardState(target)
	require.NotNil(t, ss)
	require.Equal(t, int64(77), ss.ReservedConnectionId)
}

func TestScatterConn_Case2_ReserveError(t *testing.T) {
	gw := &mockGateway{
		reserveStreamExecuteErr: errors.New("reserve failed"),
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultiGatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	err := sc.StreamExecute(context.Background(), conn, "tg1", "", "SELECT 1", state,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.Error(t, err)
	require.Contains(t, err.Error(), "reserve failed")
	// State should not be updated
	target := &query.Target{
		TableGroup: "tg1",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	require.Nil(t, state.GetMatchingShardState(target))
}

func TestScatterConn_Case3_NotInTransaction(t *testing.T) {
	gw := &mockGateway{
		callbackResult: &sqltypes.Result{CommandTag: "SELECT 1"},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultiGatewayConnectionState()
	// TxState is Idle (default)

	err := sc.StreamExecute(context.Background(), newTestConn(), "tg1", "", "SELECT 1", state,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.NoError(t, err)
	require.True(t, gw.streamExecuteCalled, "should call regular StreamExecute")
	require.Equal(t, "SELECT 1", gw.streamExecuteSQL)
	require.False(t, gw.reserveStreamExecuteCalled, "should not call ReserveStreamExecute")
	require.False(t, gw.queryServiceByIDCalled, "should not call QueryServiceByID")
}

func TestScatterConn_Case3_StreamExecuteError(t *testing.T) {
	gw := &mockGateway{
		streamExecuteErr: errors.New("query failed"),
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultiGatewayConnectionState()

	err := sc.StreamExecute(context.Background(), newTestConn(), "tg1", "", "SELECT 1", state,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.Error(t, err)
	require.Contains(t, err.Error(), "query failed")
}

func TestScatterConn_StreamExecute_ReservedConn_UpdatesShardState(t *testing.T) {
	// When StreamExecute succeeds on a reserved connection and the multipooler says
	// the connection is still active, shard state should be updated with the
	// authoritative reservation reasons.
	gw := &mockGateway{
		callbackResult: &sqltypes.Result{CommandTag: "SELECT 1"},
		streamExecuteReturnState: queryservice.ReservedState{
			ReservedConnectionId: 42,
			PoolerID:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
			ReservationReasons:   protoutil.ReasonTransaction | protoutil.ReasonTempTable,
		},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultiGatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	target := &query.Target{
		TableGroup: "tg1",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	state.SetReservedConnection(target, queryservice.ReservedState{
		ReservedConnectionId: 42,
		PoolerID:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonTransaction,
	})

	err := sc.StreamExecute(context.Background(), conn, "tg1", "", "SELECT 1", state,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.NoError(t, err)
	ss := state.GetMatchingShardState(target)
	require.NotNil(t, ss)
	require.Equal(t, int64(42), ss.ReservedConnectionId)
	require.Equal(t, protoutil.ReasonTransaction|protoutil.ReasonTempTable, ss.ReservationReasons)
}

func TestScatterConn_StreamExecute_ReservedConn_DestroyedSetsTxnFailed(t *testing.T) {
	// When StreamExecute returns a zero ReservedState (connection destroyed) while
	// in a transaction, the shard state must be cleared and TxnStatus set to Failed.
	gw := &mockGateway{
		streamExecuteErr:         errors.New("reserved connection 42 not found"),
		streamExecuteReturnState: queryservice.ReservedState{}, // zero → destroyed
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultiGatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	target := &query.Target{
		TableGroup: "tg1",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	state.SetReservedConnection(target, queryservice.ReservedState{
		ReservedConnectionId: 42,
		PoolerID:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonTransaction,
	})

	err := sc.StreamExecute(context.Background(), conn, "tg1", "", "SELECT 1", state,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.Error(t, err)
	// Shard state must be cleared
	require.Nil(t, state.GetMatchingShardState(target))
	// Transaction status must be set to Failed
	require.Equal(t, protocol.TxnStatusFailed, conn.TxnStatus())
}

func TestScatterConn_ConcludeTransaction_RollbackOnDestroyedConn(t *testing.T) {
	// ROLLBACK on a destroyed connection should succeed with a synthetic result
	// and not propagate an error.
	gw := &mockGateway{
		concludeTransactionErr: errors.New("reserved connection not found"),
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultiGatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	target := &query.Target{
		TableGroup: "tg1",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	state.SetReservedConnection(target, queryservice.ReservedState{
		ReservedConnectionId: 42,
		PoolerID:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonTransaction,
	})

	var callbackResult *sqltypes.Result
	err := sc.ConcludeTransaction(context.Background(), conn, state,
		multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_ROLLBACK,
		func(_ context.Context, result *sqltypes.Result) error {
			callbackResult = result
			return nil
		})

	require.NoError(t, err, "ROLLBACK on destroyed connection should not error")
	require.NotNil(t, callbackResult)
	require.Equal(t, "ROLLBACK", callbackResult.CommandTag)
	// Shard state must be cleared
	require.Nil(t, state.GetMatchingShardState(target))
}

func TestScatterConn_ConcludeTransaction_CommitStillReserved(t *testing.T) {
	// After COMMIT, if the connection is still reserved (e.g., temp tables),
	// the shard state should be updated with the new reasons.
	poolerID := &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}
	gw := &mockGateway{
		concludeTransactionResult: &sqltypes.Result{CommandTag: "COMMIT"},
		concludeTransactionReturnState: queryservice.ReservedState{
			ReservedConnectionId: 42,
			PoolerID:             poolerID,
			ReservationReasons:   protoutil.ReasonTempTable,
		},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultiGatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	target := &query.Target{
		TableGroup: "tg1",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	state.SetReservedConnection(target, queryservice.ReservedState{
		ReservedConnectionId: 42,
		PoolerID:             poolerID,
		ReservationReasons:   protoutil.ReasonTransaction | protoutil.ReasonTempTable,
	})

	var callbackResult *sqltypes.Result
	err := sc.ConcludeTransaction(context.Background(), conn, state,
		multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_COMMIT,
		func(_ context.Context, result *sqltypes.Result) error {
			callbackResult = result
			return nil
		})

	require.NoError(t, err)
	require.NotNil(t, callbackResult)
	require.Equal(t, "COMMIT", callbackResult.CommandTag)
	// Shard state should still exist but with updated reasons
	ss := state.GetMatchingShardState(target)
	require.NotNil(t, ss, "shard state should still exist")
	require.Equal(t, protoutil.ReasonTempTable, ss.ReservationReasons)
}

func TestScatterConn_CopyFinalize_ErrorClearsShardState(t *testing.T) {
	// CopyFinalize error should clear all shard state because the
	// multipooler destroys the connection on all error paths.
	gw := &mockGateway{
		copyFinalizeErr: errors.New("COPY operation failed"),
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultiGatewayConnectionState()
	conn := newTestConn()

	target := &query.Target{
		TableGroup: "tg1",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	state.SetReservedConnection(target, queryservice.ReservedState{
		ReservedConnectionId: 42,
		PoolerID:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonCopy | protoutil.ReasonTransaction,
	})

	err := sc.CopyFinalize(context.Background(), conn, "tg1", "", state, nil,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.Error(t, err)
	require.Contains(t, err.Error(), "COPY operation failed")
	// All shard state should be cleared
	require.Nil(t, state.GetMatchingShardState(target))
}

func TestScatterConn_CopyFinalize_ErrorSetsTxnFailed(t *testing.T) {
	// When CopyFinalize fails while in a transaction, TxnStatus should be set to Failed
	// so subsequent queries are rejected until ROLLBACK.
	gw := &mockGateway{
		copyFinalizeErr: errors.New("COPY operation failed"),
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultiGatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	target := &query.Target{
		TableGroup: "tg1",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	state.SetReservedConnection(target, queryservice.ReservedState{
		ReservedConnectionId: 42,
		PoolerID:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonCopy | protoutil.ReasonTransaction,
	})

	err := sc.CopyFinalize(context.Background(), conn, "tg1", "", state, nil,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.Error(t, err)
	require.Contains(t, err.Error(), "COPY operation failed")
	// Shard state must be cleared
	require.Nil(t, state.GetMatchingShardState(target))
	// Transaction status must be set to Failed (defense-in-depth)
	require.Equal(t, protocol.TxnStatusFailed, conn.TxnStatus())
}

func TestScatterConn_CopyFinalize_SuccessStillReserved(t *testing.T) {
	// After CopyFinalize, if the connection is still reserved (e.g., in a transaction),
	// the shard state should be updated with the authoritative reasons.
	poolerID := &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}
	gw := &mockGateway{
		copyFinalizeResult: &sqltypes.Result{CommandTag: "COPY 10"},
		copyFinalizeReturnState: queryservice.ReservedState{
			ReservedConnectionId: 42,
			PoolerID:             poolerID,
			ReservationReasons:   protoutil.ReasonTransaction,
		},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultiGatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	target := &query.Target{
		TableGroup: "tg1",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	state.SetReservedConnection(target, queryservice.ReservedState{
		ReservedConnectionId: 42,
		PoolerID:             poolerID,
		ReservationReasons:   protoutil.ReasonCopy | protoutil.ReasonTransaction,
	})

	err := sc.CopyFinalize(context.Background(), conn, "tg1", "", state, nil,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.NoError(t, err)
	ss := state.GetMatchingShardState(target)
	require.NotNil(t, ss, "shard state should still exist")
	require.Equal(t, protoutil.ReasonTransaction, ss.ReservationReasons)
}

func TestScatterConn_CopyAbort_StillReserved(t *testing.T) {
	// After CopyAbort, if the connection is still reserved (e.g., in a transaction),
	// the shard state should be updated with the authoritative reasons.
	poolerID := &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}
	gw := &mockGateway{
		copyAbortReturnState: queryservice.ReservedState{
			ReservedConnectionId: 42,
			PoolerID:             poolerID,
			ReservationReasons:   protoutil.ReasonTransaction,
		},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultiGatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	target := &query.Target{
		TableGroup: "tg1",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	state.SetReservedConnection(target, queryservice.ReservedState{
		ReservedConnectionId: 42,
		PoolerID:             poolerID,
		ReservationReasons:   protoutil.ReasonCopy | protoutil.ReasonTransaction,
	})

	err := sc.CopyAbort(context.Background(), conn, "tg1", "", state)

	require.NoError(t, err)
	ss := state.GetMatchingShardState(target)
	require.NotNil(t, ss, "shard state should still exist")
	require.Equal(t, protoutil.ReasonTransaction, ss.ReservationReasons)
}

func TestScatterConn_CopyAbort_ConnectionDestroyed(t *testing.T) {
	// After CopyAbort with a destroyed connection (zero ReservedState),
	// the shard state should be cleared.
	gw := &mockGateway{
		copyAbortReturnState: queryservice.ReservedState{}, // zero → destroyed
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultiGatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	target := &query.Target{
		TableGroup: "tg1",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	state.SetReservedConnection(target, queryservice.ReservedState{
		ReservedConnectionId: 42,
		PoolerID:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonCopy | protoutil.ReasonTransaction,
	})

	err := sc.CopyAbort(context.Background(), conn, "tg1", "", state)

	require.NoError(t, err)
	// Shard state must be cleared
	require.Nil(t, state.GetMatchingShardState(target))
	// Transaction status must be set to Failed (was in a transaction)
	require.Equal(t, protocol.TxnStatusFailed, conn.TxnStatus())
}
