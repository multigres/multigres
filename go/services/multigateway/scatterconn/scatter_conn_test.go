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
	streamExecuteCalled bool
	streamExecuteSQL    string
	streamExecuteOpts   *query.ExecuteOptions
	streamExecuteErr    error

	// ReserveStreamExecute tracking
	reserveStreamExecuteCalled bool
	reserveStreamExecuteSQL    string
	reserveStreamExecuteResult queryservice.ReservedState
	reserveStreamExecuteErr    error

	// QueryServiceByID tracking
	queryServiceByIDCalled bool
	queryServiceByIDErr    error

	// Callback control
	callbackResult *sqltypes.Result
}

// StreamExecute implements queryservice.QueryService.
func (m *mockGateway) StreamExecute(_ context.Context, _ *query.Target, sql string, opts *query.ExecuteOptions, callback func(context.Context, *sqltypes.Result) error) error {
	m.streamExecuteCalled = true
	m.streamExecuteSQL = sql
	m.streamExecuteOpts = opts
	if m.streamExecuteErr != nil {
		return m.streamExecuteErr
	}
	if m.callbackResult != nil {
		return callback(context.Background(), m.callbackResult)
	}
	return nil
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
func (m *mockGateway) ExecuteQuery(context.Context, *query.Target, string, *query.ExecuteOptions) (*sqltypes.Result, error) {
	return nil, nil
}

func (m *mockGateway) PortalStreamExecute(context.Context, *query.Target, *query.PreparedStatement, *query.Portal, *query.ExecuteOptions, func(context.Context, *sqltypes.Result) error) (queryservice.ReservedState, error) {
	return queryservice.ReservedState{}, nil
}

func (m *mockGateway) Describe(context.Context, *query.Target, *query.PreparedStatement, *query.Portal, *query.ExecuteOptions) (*query.StatementDescription, error) {
	return nil, nil
}

func (m *mockGateway) Close(context.Context) error { return nil }

func (m *mockGateway) CopyReady(context.Context, *query.Target, string, *query.ExecuteOptions) (int16, []int16, queryservice.ReservedState, error) {
	return 0, nil, queryservice.ReservedState{}, nil
}

func (m *mockGateway) CopySendData(context.Context, *query.Target, []byte, *query.ExecuteOptions) error {
	return nil
}

func (m *mockGateway) CopyFinalize(context.Context, *query.Target, []byte, *query.ExecuteOptions) (*sqltypes.Result, error) {
	return nil, nil
}

func (m *mockGateway) CopyAbort(context.Context, *query.Target, string, *query.ExecuteOptions) error {
	return nil
}

func (m *mockGateway) ReleaseReservedConnection(context.Context, *query.Target, *query.ExecuteOptions) error {
	return nil
}

func (m *mockGateway) ConcludeTransaction(context.Context, *query.Target, *query.ExecuteOptions, multipoolerpb.TransactionConclusion) (*sqltypes.Result, uint32, error) {
	return nil, 0, nil
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
	state.StoreReservedConnection(target, queryservice.ReservedState{
		ReservedConnectionId: 42,
		PoolerID:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
	}, protoutil.ReasonTransaction)

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
