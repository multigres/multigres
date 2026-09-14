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

package grpcpoolerservice

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/sqltypes"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
)

// The streaming handlers hold back the most recent data message so the
// reservation state rides on it instead of a separate trailing message. These
// tests pin the resulting wire sequence: what the gateway receives, in order.

type mockPortalStream struct {
	ctx  context.Context
	sent []*multipoolerpb.PortalStreamExecuteResponse
}

func (m *mockPortalStream) SetHeader(metadata.MD) error  { return nil }
func (m *mockPortalStream) SendHeader(metadata.MD) error { return nil }
func (m *mockPortalStream) SetTrailer(metadata.MD)       {}
func (m *mockPortalStream) SendMsg(any) error            { return nil }
func (m *mockPortalStream) RecvMsg(any) error            { return nil }
func (m *mockPortalStream) Context() context.Context {
	if m.ctx == nil {
		return context.Background()
	}
	return m.ctx
}

func (m *mockPortalStream) Send(resp *multipoolerpb.PortalStreamExecuteResponse) error {
	m.sent = append(m.sent, resp)
	return nil
}

var _ multipoolerpb.MultipoolerService_PortalStreamExecuteServer = (*mockPortalStream)(nil)

type mockExecStream struct {
	ctx  context.Context
	sent []*multipoolerpb.StreamExecuteResponse
}

func (m *mockExecStream) SetHeader(metadata.MD) error  { return nil }
func (m *mockExecStream) SendHeader(metadata.MD) error { return nil }
func (m *mockExecStream) SetTrailer(metadata.MD)       {}
func (m *mockExecStream) SendMsg(any) error            { return nil }
func (m *mockExecStream) RecvMsg(any) error            { return nil }
func (m *mockExecStream) Context() context.Context {
	if m.ctx == nil {
		return context.Background()
	}
	return m.ctx
}

func (m *mockExecStream) Send(resp *multipoolerpb.StreamExecuteResponse) error {
	m.sent = append(m.sent, resp)
	return nil
}

var _ multipoolerpb.MultipoolerService_StreamExecuteServer = (*mockExecStream)(nil)

// mockStreamQueryService reuses the COPY mock for the unused methods and lets a
// test script the streaming callbacks and the returned reservation state.
type mockStreamQueryService struct {
	mockCopyQueryService
	results  []*sqltypes.Result
	reserved *query.ReservedState
	err      error
	// afterCallback, when set, runs after the i-th callback returns. Tests use
	// it to observe what has been sent at that point.
	afterCallback func(i int)
}

func (m *mockStreamQueryService) drive(ctx context.Context, callback func(context.Context, *sqltypes.Result) error) (*query.ReservedState, error) {
	for i, r := range m.results {
		if err := callback(ctx, r); err != nil {
			return m.reserved, err
		}
		if m.afterCallback != nil {
			m.afterCallback(i)
		}
	}
	return m.reserved, m.err
}

// intermediateChunk models a batch the client flushes mid-result when it
// passes the streaming size threshold: rows, no CommandTag.
func intermediateChunk(rows string) *sqltypes.Result {
	return &sqltypes.Result{PassthroughBlock: []byte(rows), PassthroughRowCount: 1}
}

func (m *mockStreamQueryService) StreamExecute(ctx context.Context, _ *query.Target, _ string, _ *query.ExecuteOptions, _ *query.ReservationOptions, callback func(context.Context, *sqltypes.Result) error) (*query.ReservedState, error) {
	return m.drive(ctx, callback)
}

func (m *mockStreamQueryService) PortalStreamExecute(ctx context.Context, _ *query.Target, _ *query.PreparedStatement, _ *query.Portal, _ *query.ExecuteOptions, _ *multipoolerpb.PortalExecuteOptions, _ *query.ReservationOptions, callback func(context.Context, *sqltypes.Result) error) (*query.ReservedState, error) {
	return m.drive(ctx, callback)
}

func rowResult(tag string) *sqltypes.Result {
	return &sqltypes.Result{CommandTag: tag}
}

func portalReq() *multipoolerpb.PortalStreamExecuteRequest {
	return &multipoolerpb.PortalStreamExecuteRequest{
		Target:  protoutil.NewTarget("", "tg", "", query.Mode_MODE_UNSPECIFIED),
		Options: &query.ExecuteOptions{User: "postgres"},
	}
}

func TestPortalStreamExecute_ReservedStateRidesOnLastResult(t *testing.T) {
	stream := &mockPortalStream{}
	exec := &mockStreamQueryService{
		results:  []*sqltypes.Result{rowResult("SELECT 1")},
		reserved: &query.ReservedState{ReservedConnectionId: 42},
	}

	err := (&poolerService{}).portalStreamExecuteTo(stream, exec, portalReq())
	require.NoError(t, err)

	// One statement on a reserved connection: exactly one message, carrying
	// both the result and the reservation state.
	require.Len(t, stream.sent, 1)
	require.Equal(t, "SELECT 1", stream.sent[0].GetResult().GetResult().GetCommandTag())
	require.Equal(t, uint64(42), stream.sent[0].GetReservedState().GetReservedConnectionId())
}

func TestPortalStreamExecute_IntermediateChunksStreamImmediately(t *testing.T) {
	// A large result: two batches flushed mid-result (no CommandTag), then the
	// terminal chunk from CommandComplete. Intermediate chunks must reach the
	// stream as soon as they are produced; only the terminal chunk waits so the
	// reservation state can ride on it.
	stream := &mockPortalStream{}
	exec := &mockStreamQueryService{
		results:  []*sqltypes.Result{intermediateChunk("rows-1"), intermediateChunk("rows-2"), rowResult("SELECT 3")},
		reserved: &query.ReservedState{ReservedConnectionId: 7},
	}
	sentAfter := map[int]int{}
	exec.afterCallback = func(i int) { sentAfter[i] = len(stream.sent) }

	err := (&poolerService{}).portalStreamExecuteTo(stream, exec, portalReq())
	require.NoError(t, err)

	require.Equal(t, 1, sentAfter[0], "first intermediate chunk sent before the next one is produced")
	require.Equal(t, 2, sentAfter[1], "second intermediate chunk sent before the next one is produced")
	require.Equal(t, 2, sentAfter[2], "terminal chunk held until execution finishes")

	require.Len(t, stream.sent, 3)
	require.Equal(t, []byte("rows-1"), stream.sent[0].GetResult().GetResult().GetPassthroughBlock())
	require.Equal(t, []byte("rows-2"), stream.sent[1].GetResult().GetResult().GetPassthroughBlock())
	require.Equal(t, "SELECT 3", stream.sent[2].GetResult().GetResult().GetCommandTag())
	require.Nil(t, stream.sent[0].GetReservedState())
	require.Nil(t, stream.sent[1].GetReservedState())
	require.Equal(t, uint64(7), stream.sent[2].GetReservedState().GetReservedConnectionId())
}

func TestPortalStreamExecute_SuspendedPortalSendsReservedStateSeparately(t *testing.T) {
	// A portal with MaxRows hits PortalSuspended: the client flushes the rows
	// without a CommandTag and execution ends. There is no terminal chunk to
	// carry the reservation state, so it goes out on its own, as before.
	stream := &mockPortalStream{}
	exec := &mockStreamQueryService{
		results:  []*sqltypes.Result{intermediateChunk("rows-1")},
		reserved: &query.ReservedState{ReservedConnectionId: 8},
	}

	err := (&poolerService{}).portalStreamExecuteTo(stream, exec, portalReq())
	require.NoError(t, err)

	require.Len(t, stream.sent, 2)
	require.Equal(t, []byte("rows-1"), stream.sent[0].GetResult().GetResult().GetPassthroughBlock())
	require.Nil(t, stream.sent[0].GetReservedState())
	require.Nil(t, stream.sent[1].GetResult())
	require.Equal(t, uint64(8), stream.sent[1].GetReservedState().GetReservedConnectionId())
}

func TestStreamExecute_MultiStatementOnlyLastCarriesReservedState(t *testing.T) {
	// Simple-query path with several statements in one string: one terminal
	// (tagged) chunk per statement. Each is released when the next statement
	// produces output; only the last one carries the reservation state.
	stream := &mockExecStream{}
	exec := &mockStreamQueryService{
		results:  []*sqltypes.Result{rowResult("SELECT 1"), rowResult("UPDATE 2"), rowResult("SELECT 3")},
		reserved: &query.ReservedState{ReservedConnectionId: 7},
	}
	req := &multipoolerpb.StreamExecuteRequest{
		Target:  protoutil.NewTarget("", "tg", "", query.Mode_MODE_UNSPECIFIED),
		Query:   "select 1; update t set x = 1; select 3",
		Options: &query.ExecuteOptions{User: "postgres"},
	}

	err := (&poolerService{}).streamExecuteTo(stream, exec, req)
	require.NoError(t, err)

	require.Len(t, stream.sent, 3)
	for i, tag := range []string{"SELECT 1", "UPDATE 2", "SELECT 3"} {
		require.Equal(t, tag, stream.sent[i].GetResult().GetResult().GetCommandTag(), "statement order must be preserved")
	}
	require.Nil(t, stream.sent[0].GetReservedState())
	require.Nil(t, stream.sent[1].GetReservedState())
	require.Equal(t, uint64(7), stream.sent[2].GetReservedState().GetReservedConnectionId())
}

func TestPortalStreamExecute_PooledConnectionSendsNoReservedState(t *testing.T) {
	stream := &mockPortalStream{}
	exec := &mockStreamQueryService{
		results:  []*sqltypes.Result{rowResult("SELECT 1")},
		reserved: &query.ReservedState{}, // pooled: no reserved connection
	}

	err := (&poolerService{}).portalStreamExecuteTo(stream, exec, portalReq())
	require.NoError(t, err)

	require.Len(t, stream.sent, 1)
	require.Nil(t, stream.sent[0].GetReservedState())
}

func TestPortalStreamExecute_NoticesStayAheadOfTheirData(t *testing.T) {
	stream := &mockPortalStream{}
	exec := &mockStreamQueryService{
		results: []*sqltypes.Result{
			rowResult("chunk-1"),
			{
				CommandTag: "chunk-2",
				Notices:    []*mterrors.PgDiagnostic{{MessageType: 'N', Severity: "NOTICE", Message: "heads up"}},
			},
		},
		reserved: &query.ReservedState{ReservedConnectionId: 9},
	}

	err := (&poolerService{}).portalStreamExecuteTo(stream, exec, portalReq())
	require.NoError(t, err)

	// chunk-1, then the notice belonging to chunk-2, then chunk-2 (+ reserved state).
	require.Len(t, stream.sent, 3)
	require.Equal(t, "chunk-1", stream.sent[0].GetResult().GetResult().GetCommandTag())
	require.Equal(t, "heads up", stream.sent[1].GetResult().GetDiagnostic().GetMessage())
	require.Nil(t, stream.sent[1].GetReservedState())
	require.Equal(t, "chunk-2", stream.sent[2].GetResult().GetResult().GetCommandTag())
	require.Equal(t, uint64(9), stream.sent[2].GetReservedState().GetReservedConnectionId())
}

func TestPortalStreamExecute_ErrorStillDeliversPendingResultAndReservedState(t *testing.T) {
	stream := &mockPortalStream{}
	exec := &mockStreamQueryService{
		results:  []*sqltypes.Result{rowResult("partial")},
		reserved: &query.ReservedState{ReservedConnectionId: 5},
		err:      errors.New("relation does not exist"),
	}

	err := (&poolerService{}).portalStreamExecuteTo(stream, exec, portalReq())
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.NotEqual(t, codes.OK, st.Code())

	// The chunk produced before the failure is delivered, and it carries the
	// authoritative reservation state the gateway needs for the aborted backend.
	require.Len(t, stream.sent, 1)
	require.Equal(t, "partial", stream.sent[0].GetResult().GetResult().GetCommandTag())
	require.Equal(t, uint64(5), stream.sent[0].GetReservedState().GetReservedConnectionId())
}

func TestPortalStreamExecute_ErrorWithoutDataSendsReservedStateAlone(t *testing.T) {
	stream := &mockPortalStream{}
	exec := &mockStreamQueryService{
		reserved: &query.ReservedState{ReservedConnectionId: 5},
		err:      errors.New("boom"),
	}

	err := (&poolerService{}).portalStreamExecuteTo(stream, exec, portalReq())
	require.Error(t, err)

	require.Len(t, stream.sent, 1)
	require.Nil(t, stream.sent[0].GetResult())
	require.Equal(t, uint64(5), stream.sent[0].GetReservedState().GetReservedConnectionId())
}

func TestStreamExecute_ReservedStateRidesOnLastResult(t *testing.T) {
	stream := &mockExecStream{}
	exec := &mockStreamQueryService{
		results:  []*sqltypes.Result{rowResult("chunk-1"), rowResult("chunk-2")},
		reserved: &query.ReservedState{ReservedConnectionId: 11},
	}
	req := &multipoolerpb.StreamExecuteRequest{
		Target:  protoutil.NewTarget("", "tg", "", query.Mode_MODE_UNSPECIFIED),
		Query:   "select 1",
		Options: &query.ExecuteOptions{User: "postgres"},
	}

	err := (&poolerService{}).streamExecuteTo(stream, exec, req)
	require.NoError(t, err)

	require.Len(t, stream.sent, 2)
	require.Equal(t, "chunk-1", stream.sent[0].GetResult().GetResult().GetCommandTag())
	require.Nil(t, stream.sent[0].GetReservedState())
	require.Equal(t, "chunk-2", stream.sent[1].GetResult().GetResult().GetCommandTag())
	require.Equal(t, uint64(11), stream.sent[1].GetReservedState().GetReservedConnectionId())
}
