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
	querypb "github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// mockGateway is a mock implementation of poolergateway.Gateway for testing.
type mockGateway struct {
	// StreamExecute tracking
	streamExecuteCalled      bool
	streamExecuteSQL         string
	streamExecuteOpts        *querypb.ExecuteOptions
	streamExecuteErr         error
	streamExecuteReturnState *querypb.ReservedState

	// QueryServiceByID tracking
	queryServiceByIDCalled bool
	queryServiceByIDErr    error

	// ConcludeTransaction tracking
	concludeTransactionResult      *sqltypes.Result
	concludeTransactionReturnState *querypb.ReservedState
	concludeTransactionErr         error

	// CopyFinalize tracking
	copyFinalizeResult      *sqltypes.Result
	copyFinalizeReturnState *querypb.ReservedState
	copyFinalizeErr         error

	// CopyAbort tracking
	copyAbortReturnState *querypb.ReservedState
	copyAbortErr         error

	// Callback control
	callbackResult *sqltypes.Result
}

// StreamExecute implements queryservice.QueryService.
func (m *mockGateway) StreamExecute(_ context.Context, _ *querypb.Target, sql string, opts *querypb.ExecuteOptions, callback func(context.Context, *sqltypes.Result) error) (*querypb.ReservedState, error) {
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

// QueryServiceByID implements poolergateway.Gateway.
func (m *mockGateway) QueryServiceByID(_ context.Context, _ *clustermetadatapb.ID, _ *querypb.Target) (queryservice.QueryService, error) {
	m.queryServiceByIDCalled = true
	if m.queryServiceByIDErr != nil {
		return nil, m.queryServiceByIDErr
	}
	return m, nil
}

// Unused interface methods.
func (m *mockGateway) ExecuteQuery(context.Context, *querypb.Target, string, *querypb.ExecuteOptions) (*sqltypes.Result, *querypb.ReservedState, error) {
	return nil, nil, nil
}

func (m *mockGateway) PortalStreamExecute(context.Context, *querypb.Target, *querypb.PreparedStatement, *querypb.Portal, *querypb.ExecuteOptions, func(context.Context, *sqltypes.Result) error) (*querypb.ReservedState, error) {
	return nil, nil
}

func (m *mockGateway) Describe(context.Context, *querypb.Target, *querypb.PreparedStatement, *querypb.Portal, *querypb.ExecuteOptions) (*querypb.StatementDescription, error) {
	return nil, nil
}

func (m *mockGateway) Close() error { return nil }

func (m *mockGateway) CopyReady(context.Context, *querypb.Target, string, *querypb.ExecuteOptions, *multipoolerpb.ReservationOptions) (int16, []int16, *querypb.ReservedState, error) {
	return 0, nil, nil, nil
}

func (m *mockGateway) CopySendData(context.Context, *querypb.Target, []byte, *querypb.ExecuteOptions) error {
	return nil
}

func (m *mockGateway) CopyFinalize(_ context.Context, _ *querypb.Target, _ []byte, _ *querypb.ExecuteOptions) (*sqltypes.Result, *querypb.ReservedState, error) {
	return m.copyFinalizeResult, m.copyFinalizeReturnState, m.copyFinalizeErr
}

func (m *mockGateway) CopyAbort(_ context.Context, _ *querypb.Target, _ string, _ *querypb.ExecuteOptions) (*querypb.ReservedState, error) {
	return m.copyAbortReturnState, m.copyAbortErr
}

func (m *mockGateway) ReleaseReservedConnection(context.Context, *querypb.Target, *querypb.ExecuteOptions) error {
	return nil
}

func (m *mockGateway) ConcludeTransaction(_ context.Context, _ *querypb.Target, _ *querypb.ExecuteOptions, _ multipoolerpb.TransactionConclusion) (*sqltypes.Result, *querypb.ReservedState, error) {
	return m.concludeTransactionResult, m.concludeTransactionReturnState, m.concludeTransactionErr
}

func (m *mockGateway) DiscardTempTables(ctx context.Context, target *querypb.Target, options *querypb.ExecuteOptions) (*sqltypes.Result, *querypb.ReservedState, error) {
	return nil, nil, nil
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

	target := &querypb.Target{
		TableGroup: "tg1",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonTransaction,
	})

	err := sc.StreamExecute(context.Background(), conn, "tg1", "", "SELECT 1", state,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.NoError(t, err)
	require.True(t, gw.queryServiceByIDCalled, "should use QueryServiceByID for existing reserved connection")
	require.True(t, gw.streamExecuteCalled, "should call StreamExecute on the found query service")
	require.Equal(t, "SELECT 1", gw.streamExecuteSQL)
	require.Equal(t, uint64(42), gw.streamExecuteOpts.ReservedConnectionId)
}

func TestScatterConn_Case2_InTransactionNoReservedConn(t *testing.T) {
	gw := &mockGateway{
		streamExecuteReturnState: &querypb.ReservedState{
			ReservedConnectionId: 77,
			PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
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
	require.True(t, gw.streamExecuteCalled, "should call StreamExecute")
	require.Equal(t, "SELECT 1", gw.streamExecuteSQL)
	require.NotEqual(t, uint32(0), gw.streamExecuteOpts.ReservationReasons, "should set ReservationReasons")
	require.False(t, gw.queryServiceByIDCalled)

	// Verify state was updated with the reserved connection
	target := &querypb.Target{
		TableGroup: "tg1",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	ss := state.GetMatchingShardState(target)
	require.NotNil(t, ss)
	require.Equal(t, uint64(77), ss.ReservedState.GetReservedConnectionId())
}

func TestScatterConn_Case2_ReserveError(t *testing.T) {
	gw := &mockGateway{
		streamExecuteErr: errors.New("reserve failed"),
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultiGatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	err := sc.StreamExecute(context.Background(), conn, "tg1", "", "SELECT 1", state,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.Error(t, err)
	require.Contains(t, err.Error(), "reserve failed")
	require.True(t, gw.streamExecuteCalled, "should call StreamExecute")
	require.NotEqual(t, uint32(0), gw.streamExecuteOpts.ReservationReasons, "should set ReservationReasons")
	// State should not be updated
	target := &querypb.Target{
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
		streamExecuteReturnState: &querypb.ReservedState{
			ReservedConnectionId: 42,
			PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
			ReservationReasons:   protoutil.ReasonTransaction | protoutil.ReasonTempTable,
		},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultiGatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	target := &querypb.Target{
		TableGroup: "tg1",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonTransaction,
	})

	err := sc.StreamExecute(context.Background(), conn, "tg1", "", "SELECT 1", state,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.NoError(t, err)
	ss := state.GetMatchingShardState(target)
	require.NotNil(t, ss)
	require.Equal(t, uint64(42), ss.ReservedState.GetReservedConnectionId())
	require.Equal(t, protoutil.ReasonTransaction|protoutil.ReasonTempTable, ss.ReservedState.GetReservationReasons())
}

func TestScatterConn_StreamExecute_ReservedConn_DestroyedSetsTxnFailed(t *testing.T) {
	// When StreamExecute returns a nil ReservedState (connection destroyed) while
	// in a transaction, the shard state must be cleared and TxnStatus set to Failed.
	gw := &mockGateway{
		streamExecuteErr: errors.New("reserved connection 42 not found"),
		// nil streamExecuteReturnState → destroyed
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultiGatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	target := &querypb.Target{
		TableGroup: "tg1",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
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

	target := &querypb.Target{
		TableGroup: "tg1",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
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

func TestScatterConn_ConcludeTransaction_CommitOnDestroyedConn(t *testing.T) {
	// COMMIT on a destroyed connection must propagate the error because the
	// client needs to know their COMMIT didn't happen (data may not be persisted).
	gw := &mockGateway{
		concludeTransactionErr: errors.New("reserved connection not found"),
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultiGatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	target := &querypb.Target{
		TableGroup: "tg1",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonTransaction,
	})

	err := sc.ConcludeTransaction(context.Background(), conn, state,
		multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_COMMIT,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.Error(t, err, "COMMIT on destroyed connection must propagate error")
	require.Contains(t, err.Error(), "conclude transaction failed")
	// Shard state must be cleared
	require.Nil(t, state.GetMatchingShardState(target))
}

func TestScatterConn_ConcludeTransaction_CommitStillReserved(t *testing.T) {
	// After COMMIT, if the connection is still reserved (e.g., temp tables),
	// the shard state should be updated with the new reasons.
	poolerID := &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}
	gw := &mockGateway{
		concludeTransactionResult: &sqltypes.Result{CommandTag: "COMMIT"},
		concludeTransactionReturnState: &querypb.ReservedState{
			ReservedConnectionId: 42,
			PoolerId:             poolerID,
			ReservationReasons:   protoutil.ReasonTempTable,
		},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultiGatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	target := &querypb.Target{
		TableGroup: "tg1",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             poolerID,
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
	require.Equal(t, protoutil.ReasonTempTable, ss.ReservedState.GetReservationReasons())
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

	target := &querypb.Target{
		TableGroup: "tg1",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
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

	target := &querypb.Target{
		TableGroup: "tg1",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
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
		copyFinalizeReturnState: &querypb.ReservedState{
			ReservedConnectionId: 42,
			PoolerId:             poolerID,
			ReservationReasons:   protoutil.ReasonTransaction,
		},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultiGatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	target := &querypb.Target{
		TableGroup: "tg1",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             poolerID,
		ReservationReasons:   protoutil.ReasonCopy | protoutil.ReasonTransaction,
	})

	err := sc.CopyFinalize(context.Background(), conn, "tg1", "", state, nil,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.NoError(t, err)
	ss := state.GetMatchingShardState(target)
	require.NotNil(t, ss, "shard state should still exist")
	require.Equal(t, protoutil.ReasonTransaction, ss.ReservedState.GetReservationReasons())
}

func TestScatterConn_CopyAbort_StillReserved(t *testing.T) {
	// After CopyAbort, if the connection is still reserved (e.g., in a transaction),
	// the shard state should be updated with the authoritative reasons.
	poolerID := &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}
	gw := &mockGateway{
		copyAbortReturnState: &querypb.ReservedState{
			ReservedConnectionId: 42,
			PoolerId:             poolerID,
			ReservationReasons:   protoutil.ReasonTransaction,
		},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultiGatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	target := &querypb.Target{
		TableGroup: "tg1",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             poolerID,
		ReservationReasons:   protoutil.ReasonCopy | protoutil.ReasonTransaction,
	})

	err := sc.CopyAbort(context.Background(), conn, "tg1", "", state)

	require.NoError(t, err)
	ss := state.GetMatchingShardState(target)
	require.NotNil(t, ss, "shard state should still exist")
	require.Equal(t, protoutil.ReasonTransaction, ss.ReservedState.GetReservationReasons())
}

func TestScatterConn_CopyAbort_ConnectionDestroyed(t *testing.T) {
	// After CopyAbort with a destroyed connection (nil ReservedState),
	// the shard state should be cleared.
	gw := &mockGateway{
		// nil copyAbortReturnState → destroyed
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultiGatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	target := &querypb.Target{
		TableGroup: "tg1",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonCopy | protoutil.ReasonTransaction,
	})

	err := sc.CopyAbort(context.Background(), conn, "tg1", "", state)

	require.NoError(t, err)
	// Shard state must be cleared
	require.Nil(t, state.GetMatchingShardState(target))
	// Transaction status must be set to Failed (was in a transaction)
	require.Equal(t, protocol.TxnStatusFailed, conn.TxnStatus())
}
