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

package poolergateway

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
)

// mockBidiStream is a mock implementation of grpc.BidiStreamingClient for testing.
type mockBidiStream struct {
	// Configurable behavior
	sendErr       error
	recvErr       error
	recvResponse  *multipoolerservice.CopyBidiExecuteResponse
	recvErrors    []error
	recvResponses []*multipoolerservice.CopyBidiExecuteResponse

	// Track calls for verification
	closeSendCalled atomic.Bool
	recvCalled      atomic.Bool
	sendCalled      atomic.Bool
	sentRequests    []*multipoolerservice.CopyBidiExecuteRequest
	recvIdx         int
}

func (m *mockBidiStream) Send(req *multipoolerservice.CopyBidiExecuteRequest) error {
	m.sendCalled.Store(true)
	m.sentRequests = append(m.sentRequests, req)
	return m.sendErr
}

func (m *mockBidiStream) Recv() (*multipoolerservice.CopyBidiExecuteResponse, error) {
	m.recvCalled.Store(true)
	if m.recvIdx < len(m.recvErrors) && m.recvErrors[m.recvIdx] != nil {
		err := m.recvErrors[m.recvIdx]
		m.recvIdx++
		return nil, err
	}
	if m.recvIdx < len(m.recvResponses) {
		resp := m.recvResponses[m.recvIdx]
		m.recvIdx++
		return resp, nil
	}
	if m.recvErr != nil {
		return nil, m.recvErr
	}
	return m.recvResponse, nil
}

func (m *mockBidiStream) CloseSend() error {
	m.closeSendCalled.Store(true)
	return nil
}

// Required interface methods (not used in tests but needed for interface compliance)
func (m *mockBidiStream) Header() (metadata.MD, error) { return nil, nil }
func (m *mockBidiStream) Trailer() metadata.MD         { return nil }
func (m *mockBidiStream) Context() context.Context     { return context.Background() }
func (m *mockBidiStream) SendMsg(msg any) error        { return nil }
func (m *mockBidiStream) RecvMsg(msg any) error        { return nil }

// Ensure mockBidiStream implements the interface
var _ grpc.BidiStreamingClient[multipoolerservice.CopyBidiExecuteRequest, multipoolerservice.CopyBidiExecuteResponse] = (*mockBidiStream)(nil)

// mockMultiPoolerServiceClient is a mock implementation of MultiPoolerServiceClient.
type mockMultiPoolerServiceClient struct {
	// CopyBidiExecute behavior
	bidiStream    *mockBidiStream
	bidiStreamErr error

	// ConcludeTransaction behavior
	concludeResponse *multipoolerservice.ConcludeTransactionResponse
	concludeErr      error
}

func (m *mockMultiPoolerServiceClient) CopyBidiExecute(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[multipoolerservice.CopyBidiExecuteRequest, multipoolerservice.CopyBidiExecuteResponse], error) {
	if m.bidiStreamErr != nil {
		return nil, m.bidiStreamErr
	}
	return m.bidiStream, nil
}

func (m *mockMultiPoolerServiceClient) StreamReplication(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[multipoolerservice.StreamReplicationRequest, multipoolerservice.StreamReplicationResponse], error) {
	return nil, nil
}

// Other methods not used in CopyReady tests
func (m *mockMultiPoolerServiceClient) ExecuteQuery(ctx context.Context, in *multipoolerservice.ExecuteQueryRequest, opts ...grpc.CallOption) (*multipoolerservice.ExecuteQueryResponse, error) {
	return nil, nil
}

func (m *mockMultiPoolerServiceClient) StreamExecute(ctx context.Context, in *multipoolerservice.StreamExecuteRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[multipoolerservice.StreamExecuteResponse], error) {
	return nil, nil
}

func (m *mockMultiPoolerServiceClient) PortalStreamExecute(ctx context.Context, in *multipoolerservice.PortalStreamExecuteRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[multipoolerservice.PortalStreamExecuteResponse], error) {
	return nil, nil
}

func (m *mockMultiPoolerServiceClient) Describe(ctx context.Context, in *multipoolerservice.DescribeRequest, opts ...grpc.CallOption) (*multipoolerservice.DescribeResponse, error) {
	return nil, nil
}

func (m *mockMultiPoolerServiceClient) GetAuthCredentials(ctx context.Context, in *multipoolerservice.GetAuthCredentialsRequest, opts ...grpc.CallOption) (*multipoolerservice.GetAuthCredentialsResponse, error) {
	return nil, nil
}

func (m *mockMultiPoolerServiceClient) ConcludeTransaction(ctx context.Context, in *multipoolerservice.ConcludeTransactionRequest, opts ...grpc.CallOption) (*multipoolerservice.ConcludeTransactionResponse, error) {
	if m.concludeErr != nil {
		return nil, m.concludeErr
	}
	return m.concludeResponse, nil
}

func (m *mockMultiPoolerServiceClient) ReleaseReservedConnection(ctx context.Context, in *multipoolerservice.ReleaseReservedConnectionRequest, opts ...grpc.CallOption) (*multipoolerservice.ReleaseReservedConnectionResponse, error) {
	return &multipoolerservice.ReleaseReservedConnectionResponse{}, nil
}

func (m *mockMultiPoolerServiceClient) StreamPoolerHealth(ctx context.Context, in *multipoolerservice.StreamPoolerHealthRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[multipoolerservice.StreamPoolerHealthResponse], error) {
	return nil, nil
}

func (m *mockMultiPoolerServiceClient) StreamNotifications(ctx context.Context, in *multipoolerservice.StreamNotificationsRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[multipoolerservice.StreamNotificationsResponse], error) {
	return nil, nil
}

func (m *mockMultiPoolerServiceClient) DiscardTempTables(ctx context.Context, in *multipoolerservice.DiscardTempTablesRequest, opts ...grpc.CallOption) (*multipoolerservice.DiscardTempTablesResponse, error) {
	return nil, nil
}

// Ensure mockMultiPoolerServiceClient implements the interface
var _ multipoolerservice.MultiPoolerServiceClient = (*mockMultiPoolerServiceClient)(nil)

// newTestGRPCQueryService creates a grpcQueryService with a mock client for testing.
func newTestGRPCQueryService(client multipoolerservice.MultiPoolerServiceClient) *grpcQueryService {
	return &grpcQueryService{
		client:      client,
		logger:      slog.Default(),
		poolerID:    "test-pooler",
		copyStreams: make(map[uint64]multipoolerservice.MultiPoolerService_CopyBidiExecuteClient),
	}
}

// TestCopyReady_CopyBidiExecuteError tests that when CopyBidiExecute fails to create a stream,
// no cleanup is needed (stream was never created).
func TestCopyReady_CopyBidiExecuteError(t *testing.T) {
	mockClient := &mockMultiPoolerServiceClient{
		bidiStreamErr: errors.New("failed to create stream"),
	}

	svc := newTestGRPCQueryService(mockClient)

	_, _, _, err := svc.CopyReady(
		context.Background(),
		&query.Target{TableGroup: "test"},
		"COPY t FROM STDIN",
		&query.ExecuteOptions{},
		nil,
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to start bidirectional execute stream")
	// copyStreams should be empty
	require.Len(t, svc.copyStreams, 0)
}

// TestCopyReady_SendInitiateError tests that when Send(INITIATE) fails,
// the stream is properly cleaned up via defer.
func TestCopyReady_SendInitiateError(t *testing.T) {
	mockStream := &mockBidiStream{
		sendErr: errors.New("send failed"),
	}
	mockClient := &mockMultiPoolerServiceClient{
		bidiStream: mockStream,
	}

	svc := newTestGRPCQueryService(mockClient)

	_, _, _, err := svc.CopyReady(
		context.Background(),
		&query.Target{TableGroup: "test"},
		"COPY t FROM STDIN",
		&query.ExecuteOptions{},
		nil,
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to send INITIATE")
	// Verify cleanup was called
	require.True(t, mockStream.closeSendCalled.Load(), "CloseSend should be called on error")
	require.True(t, mockStream.recvCalled.Load(), "Recv should be called to drain stream on error")
	// copyStreams should be empty
	require.Len(t, svc.copyStreams, 0)
}

// TestCopyReady_RecvReadyError tests that when Recv() fails after Send succeeds,
// the stream is properly cleaned up via defer.
func TestCopyReady_RecvReadyError(t *testing.T) {
	mockStream := &mockBidiStream{
		recvErr: errors.New("recv failed"),
	}
	mockClient := &mockMultiPoolerServiceClient{
		bidiStream: mockStream,
	}

	svc := newTestGRPCQueryService(mockClient)

	_, _, _, err := svc.CopyReady(
		context.Background(),
		&query.Target{TableGroup: "test"},
		"COPY t FROM STDIN",
		&query.ExecuteOptions{},
		nil,
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to receive READY response")
	// Verify cleanup was called
	require.True(t, mockStream.closeSendCalled.Load(), "CloseSend should be called on error")
	// Note: recvCalled will be true from the failed Recv() attempt, then called again in defer
	require.True(t, mockStream.recvCalled.Load(), "Recv should be called")
	// copyStreams should be empty
	require.Len(t, svc.copyStreams, 0)
}

// TestCopyReady_ErrorPhaseResponse tests that when the response has ERROR phase,
// the stream is properly cleaned up via defer.
func TestCopyReady_ErrorPhaseResponse(t *testing.T) {
	mockStream := &mockBidiStream{
		recvResponse: &multipoolerservice.CopyBidiExecuteResponse{
			Phase: multipoolerservice.CopyBidiExecuteResponse_ERROR,
			Error: "some backend error",
		},
	}
	mockClient := &mockMultiPoolerServiceClient{
		bidiStream: mockStream,
	}

	svc := newTestGRPCQueryService(mockClient)

	_, _, rs, err := svc.CopyReady(
		context.Background(),
		&query.Target{TableGroup: "test"},
		"COPY t FROM STDIN",
		&query.ExecuteOptions{},
		nil,
	)

	require.Error(t, err)
	// CopyReady now surfaces the underlying PG error un-wrapped so the gateway
	// re-emits a verbatim ErrorResponse. Without a PgDiagnostic attached the
	// fallback path returns the bare resp.Error text.
	require.Contains(t, err.Error(), "some backend error")
	require.Nil(t, rs, "ReservedState should be nil when multipooler did not attach one")
	// Verify cleanup was called
	require.True(t, mockStream.closeSendCalled.Load(), "CloseSend should be called on error")
	// copyStreams should be empty
	require.Len(t, svc.copyStreams, 0)
}

// TestCopyReady_ErrorPhasePropagatesReservedState verifies that when the
// multipooler reports a COPY initiation failure but attaches the surviving
// ReservedState (because the reserved connection is still alive for an
// unrelated reason like a transaction or temp table), the gateway client
// returns that state alongside the error so the scatter layer can keep
// tracking the connection.
func TestCopyReady_ErrorPhasePropagatesReservedState(t *testing.T) {
	survivingState := &query.ReservedState{
		ReservedConnectionId: 12345,
		ReservationReasons:   1, // any non-zero reason from protoutil bitmap
	}
	mockStream := &mockBidiStream{
		recvResponse: &multipoolerservice.CopyBidiExecuteResponse{
			Phase:         multipoolerservice.CopyBidiExecuteResponse_ERROR,
			Error:         "column \"xyz\" of relation \"x\" does not exist",
			ReservedState: survivingState,
		},
	}
	mockClient := &mockMultiPoolerServiceClient{
		bidiStream: mockStream,
	}

	svc := newTestGRPCQueryService(mockClient)

	_, _, rs, err := svc.CopyReady(
		context.Background(),
		&query.Target{TableGroup: "test"},
		"COPY t (xyz) FROM STDIN",
		&query.ExecuteOptions{ReservedConnectionId: 12345},
		nil,
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "column \"xyz\"")
	require.NotNil(t, rs, "ReservedState must be propagated when multipooler attaches it")
	require.Equal(t, uint64(12345), rs.GetReservedConnectionId())
	// Stream still gets cleaned up by the defer; the conn lives on at the multipooler.
	require.True(t, mockStream.closeSendCalled.Load(), "CloseSend should still be called")
	require.Len(t, svc.copyStreams, 0)
}

// TestCopyReady_UnexpectedPhaseResponse tests that when the response has an unexpected phase,
// the stream is properly cleaned up via defer.
func TestCopyReady_UnexpectedPhaseResponse(t *testing.T) {
	mockStream := &mockBidiStream{
		recvResponse: &multipoolerservice.CopyBidiExecuteResponse{
			Phase: multipoolerservice.CopyBidiExecuteResponse_RESULT, // Wrong phase, expected READY
		},
	}
	mockClient := &mockMultiPoolerServiceClient{
		bidiStream: mockStream,
	}

	svc := newTestGRPCQueryService(mockClient)

	_, _, _, err := svc.CopyReady(
		context.Background(),
		&query.Target{TableGroup: "test"},
		"COPY t FROM STDIN",
		&query.ExecuteOptions{},
		nil,
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "expected READY")
	// Verify cleanup was called
	require.True(t, mockStream.closeSendCalled.Load(), "CloseSend should be called on error")
	// copyStreams should be empty
	require.Len(t, svc.copyStreams, 0)
}

// TestCopyReady_Success tests that on success, the stream is added to copyStreams
// and NOT cleaned up by the defer.
func TestCopyReady_Success(t *testing.T) {
	mockStream := &mockBidiStream{
		recvResponse: &multipoolerservice.CopyBidiExecuteResponse{
			Phase: multipoolerservice.CopyBidiExecuteResponse_READY,
			ReservedState: &query.ReservedState{
				ReservedConnectionId: 12345,
			},
			Format:        0,
			ColumnFormats: []int32{0, 0, 0},
		},
	}
	mockClient := &mockMultiPoolerServiceClient{
		bidiStream: mockStream,
	}

	svc := newTestGRPCQueryService(mockClient)

	format, columnFormats, reservedState, err := svc.CopyReady(
		context.Background(),
		&query.Target{TableGroup: "test"},
		"COPY t FROM STDIN",
		&query.ExecuteOptions{},
		nil,
	)

	require.NoError(t, err)
	require.Equal(t, int16(0), format)
	require.Equal(t, []int16{0, 0, 0}, columnFormats)
	require.Equal(t, uint64(12345), reservedState.GetReservedConnectionId())

	// Verify cleanup was NOT called (success path)
	require.False(t, mockStream.closeSendCalled.Load(), "CloseSend should NOT be called on success")

	// Stream should be in copyStreams map
	require.Len(t, svc.copyStreams, 1)
	_, exists := svc.copyStreams[12345]
	require.True(t, exists, "Stream should be stored in copyStreams with reserved connection ID")
}

func TestCopyOutReady_Success(t *testing.T) {
	mockStream := &mockBidiStream{
		recvResponse: &multipoolerservice.CopyBidiExecuteResponse{
			Phase: multipoolerservice.CopyBidiExecuteResponse_READY,
			ReservedState: &query.ReservedState{
				ReservedConnectionId: 54321,
			},
			Format:        1,
			ColumnFormats: []int32{0, 1},
			Notices: []*query.PgDiagnostic{
				mterrors.PgDiagnosticToProto(&mterrors.PgDiagnostic{MessageType: 'N', Severity: "NOTICE", Message: "before-copy"}),
			},
		},
	}
	mockClient := &mockMultiPoolerServiceClient{bidiStream: mockStream}
	svc := newTestGRPCQueryService(mockClient)

	format, columnFormats, notices, reservedState, err := svc.CopyOutReady(
		context.Background(),
		&query.Target{TableGroup: "test"},
		"COPY t TO STDOUT",
		&query.ExecuteOptions{},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, int16(1), format)
	require.Equal(t, []int16{0, 1}, columnFormats)
	require.Equal(t, uint64(54321), reservedState.GetReservedConnectionId())
	require.Len(t, notices, 1)
	require.Equal(t, "before-copy", notices[0].Message)
	require.False(t, mockStream.closeSendCalled.Load(), "CloseSend should NOT be called on success")
	_, exists := svc.copyStreams[54321]
	require.True(t, exists, "stream should be stored for subsequent CopyOutStream")
	require.Len(t, mockStream.sentRequests, 1)
	require.Equal(t, multipoolerservice.CopyBidiExecuteRequest_TO_STDOUT, mockStream.sentRequests[0].GetDirection())
}

func TestCopyOutReady_ErrorPhasePropagatesReservedState(t *testing.T) {
	survivingState := &query.ReservedState{
		ReservedConnectionId: 123,
		ReservationReasons:   protoutil.ReasonTransaction,
	}
	mockStream := &mockBidiStream{
		recvResponse: &multipoolerservice.CopyBidiExecuteResponse{
			Phase:         multipoolerservice.CopyBidiExecuteResponse_ERROR,
			Error:         "copy rejected",
			ReservedState: survivingState,
		},
	}
	mockClient := &mockMultiPoolerServiceClient{bidiStream: mockStream}
	svc := newTestGRPCQueryService(mockClient)

	_, _, _, rs, err := svc.CopyOutReady(
		context.Background(),
		&query.Target{TableGroup: "test"},
		"COPY t TO STDOUT",
		&query.ExecuteOptions{ReservedConnectionId: 123},
		nil,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "copy rejected")
	require.NotNil(t, rs)
	require.Equal(t, uint64(123), rs.GetReservedConnectionId())
	require.True(t, mockStream.closeSendCalled.Load(), "error path should close the stream")
	require.Len(t, svc.copyStreams, 0, "error path should not keep the stream")
}

func TestCopyOutStream_SuccessWithDataNoticesAndResult(t *testing.T) {
	const connID = uint64(777)
	mockStream := &mockBidiStream{
		recvResponses: []*multipoolerservice.CopyBidiExecuteResponse{
			{
				Phase: multipoolerservice.CopyBidiExecuteResponse_DATA,
				Notices: []*query.PgDiagnostic{
					mterrors.PgDiagnosticToProto(&mterrors.PgDiagnostic{MessageType: 'N', Severity: "NOTICE", Message: "row notice"}),
				},
			},
			{
				Phase: multipoolerservice.CopyBidiExecuteResponse_DATA,
				Data:  []byte("payload"),
			},
			{
				Phase: multipoolerservice.CopyBidiExecuteResponse_RESULT,
				Result: (&sqltypes.Result{
					CommandTag:   "COPY 1",
					RowsAffected: 1,
				}).ToProto(),
				Notices: []*query.PgDiagnostic{
					mterrors.PgDiagnosticToProto(&mterrors.PgDiagnostic{MessageType: 'N', Severity: "NOTICE", Message: "final notice"}),
				},
				ReservedState: &query.ReservedState{ReservedConnectionId: connID},
			},
		},
	}
	svc := newTestGRPCQueryService(&mockMultiPoolerServiceClient{bidiStream: mockStream})
	svc.copyStreams[connID] = mockStream

	var messages []client.CopyOutMessage
	result, rs, err := svc.CopyOutStream(
		context.Background(),
		&query.Target{TableGroup: "test"},
		&query.ExecuteOptions{ReservedConnectionId: connID},
		func(msg client.CopyOutMessage) error {
			messages = append(messages, msg)
			return nil
		},
	)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "COPY 1", result.CommandTag)
	require.Equal(t, uint64(1), result.RowsAffected)
	require.Len(t, result.Notices, 1)
	require.Equal(t, "final notice", result.Notices[0].Message)
	require.NotNil(t, rs)
	require.Equal(t, connID, rs.GetReservedConnectionId())
	require.Len(t, messages, 2)
	require.Equal(t, "row notice", messages[0].Notice.Message)
	require.Equal(t, []byte("payload"), messages[1].Data)
	require.True(t, mockStream.closeSendCalled.Load(), "result path should close send")
	_, exists := svc.copyStreams[connID]
	require.False(t, exists, "stream should be removed after terminal RESULT")
}

func TestCopyOutStream_CallbackErrorClosesAndRemovesStream(t *testing.T) {
	const connID = uint64(888)
	mockStream := &mockBidiStream{
		recvResponse: &multipoolerservice.CopyBidiExecuteResponse{
			Phase: multipoolerservice.CopyBidiExecuteResponse_DATA,
			Data:  []byte("payload"),
		},
	}
	svc := newTestGRPCQueryService(&mockMultiPoolerServiceClient{bidiStream: mockStream})
	svc.copyStreams[connID] = mockStream

	_, _, err := svc.CopyOutStream(
		context.Background(),
		&query.Target{TableGroup: "test"},
		&query.ExecuteOptions{ReservedConnectionId: connID},
		func(client.CopyOutMessage) error {
			return errors.New("client write failed")
		},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "client write failed")
	require.True(t, mockStream.closeSendCalled.Load())
	_, exists := svc.copyStreams[connID]
	require.False(t, exists)
}

func TestCopyOutStream_ErrorPhasePropagatesState(t *testing.T) {
	const connID = uint64(999)
	mockStream := &mockBidiStream{
		recvResponse: &multipoolerservice.CopyBidiExecuteResponse{
			Phase: multipoolerservice.CopyBidiExecuteResponse_ERROR,
			Error: "backend copy error",
			ReservedState: &query.ReservedState{
				ReservedConnectionId: connID,
			},
		},
	}
	svc := newTestGRPCQueryService(&mockMultiPoolerServiceClient{bidiStream: mockStream})
	svc.copyStreams[connID] = mockStream

	_, rs, err := svc.CopyOutStream(
		context.Background(),
		&query.Target{TableGroup: "test"},
		&query.ExecuteOptions{ReservedConnectionId: connID},
		func(client.CopyOutMessage) error { return nil },
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "backend copy error")
	require.NotNil(t, rs)
	require.Equal(t, connID, rs.GetReservedConnectionId())
	_, exists := svc.copyStreams[connID]
	require.False(t, exists)
}

func TestCopyOutStream_RecvErrorRemovesStream(t *testing.T) {
	const connID = uint64(1001)
	mockStream := &mockBidiStream{recvErr: errors.New("recv failed")}
	svc := newTestGRPCQueryService(&mockMultiPoolerServiceClient{bidiStream: mockStream})
	svc.copyStreams[connID] = mockStream

	_, _, err := svc.CopyOutStream(
		context.Background(),
		&query.Target{TableGroup: "test"},
		&query.ExecuteOptions{ReservedConnectionId: connID},
		func(client.CopyOutMessage) error { return nil },
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to receive COPY frame")
	_, exists := svc.copyStreams[connID]
	require.False(t, exists)
}

func TestCopyAbort_HandlesStreamLifecycle(t *testing.T) {
	const connID = uint64(2024)
	mockStream := &mockBidiStream{
		recvResponse: &multipoolerservice.CopyBidiExecuteResponse{
			Phase: multipoolerservice.CopyBidiExecuteResponse_ERROR,
			ReservedState: &query.ReservedState{
				ReservedConnectionId: connID,
			},
		},
	}
	svc := newTestGRPCQueryService(&mockMultiPoolerServiceClient{bidiStream: mockStream})
	svc.copyStreams[connID] = mockStream

	rs, err := svc.CopyAbort(
		context.Background(),
		&query.Target{TableGroup: "test"},
		"abort requested",
		&query.ExecuteOptions{ReservedConnectionId: connID},
	)
	require.NoError(t, err)
	require.NotNil(t, rs)
	require.Equal(t, connID, rs.GetReservedConnectionId())
	require.True(t, mockStream.sendCalled.Load())
	require.True(t, mockStream.closeSendCalled.Load())
	require.True(t, mockStream.recvCalled.Load())
	require.Len(t, mockStream.sentRequests, 1)
	require.Equal(t, multipoolerservice.CopyBidiExecuteRequest_FAIL, mockStream.sentRequests[0].GetPhase())
	require.Equal(t, "abort requested", mockStream.sentRequests[0].GetErrorMessage())
	_, exists := svc.copyStreams[connID]
	require.False(t, exists, "abort should remove stream from map")
}

func TestCopyAbort_RecvEOFStillSucceeds(t *testing.T) {
	const connID = uint64(3030)
	mockStream := &mockBidiStream{
		recvErr: io.EOF,
	}
	svc := newTestGRPCQueryService(&mockMultiPoolerServiceClient{bidiStream: mockStream})
	svc.copyStreams[connID] = mockStream

	rs, err := svc.CopyAbort(
		context.Background(),
		&query.Target{TableGroup: "test"},
		"abort requested",
		&query.ExecuteOptions{ReservedConnectionId: connID},
	)
	require.NoError(t, err)
	require.Nil(t, rs)
	require.True(t, mockStream.sendCalled.Load())
	require.True(t, mockStream.closeSendCalled.Load())
}

// TestCopyFinalize_ErrorPhasePropagatesReservedState verifies CopyFinalize
// passes the multipooler's surviving ReservedState back through the error
// return when PostgreSQL rejects the COPY at CopyDone (e.g., constraint
// violation) but the underlying reserved connection is still alive because
// of an unrelated reason like a wrapping transaction.
func TestCopyFinalize_ErrorPhasePropagatesReservedState(t *testing.T) {
	const connID = uint64(99)
	survivingState := &query.ReservedState{
		ReservedConnectionId: connID,
		ReservationReasons:   1,
	}
	mockStream := &mockBidiStream{
		recvResponse: &multipoolerservice.CopyBidiExecuteResponse{
			Phase:         multipoolerservice.CopyBidiExecuteResponse_ERROR,
			Error:         "constraint violation",
			ReservedState: survivingState,
		},
	}
	mockClient := &mockMultiPoolerServiceClient{
		bidiStream: mockStream,
	}

	svc := newTestGRPCQueryService(mockClient)
	// Pre-register stream as if CopyReady had succeeded earlier.
	svc.copyStreams[connID] = mockStream

	_, rs, err := svc.CopyFinalize(
		context.Background(),
		&query.Target{TableGroup: "test"},
		nil,
		&query.ExecuteOptions{ReservedConnectionId: connID},
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "constraint violation")
	require.NotNil(t, rs, "ReservedState must be propagated when multipooler attaches it")
	require.Equal(t, connID, rs.GetReservedConnectionId())
	// CopyFinalize removes the stream regardless of outcome.
	require.NotContains(t, svc.copyStreams, connID)
}

// TestCopyFinalize_ErrorPhaseWithoutReservedState verifies CopyFinalize
// returns nil ReservedState when the multipooler reports a connection-level
// failure and does not attach a surviving state.
func TestCopyFinalize_ErrorPhaseWithoutReservedState(t *testing.T) {
	const connID = uint64(123)
	mockStream := &mockBidiStream{
		recvResponse: &multipoolerservice.CopyBidiExecuteResponse{
			Phase: multipoolerservice.CopyBidiExecuteResponse_ERROR,
			Error: "broken pipe",
		},
	}
	mockClient := &mockMultiPoolerServiceClient{
		bidiStream: mockStream,
	}

	svc := newTestGRPCQueryService(mockClient)
	svc.copyStreams[connID] = mockStream

	_, rs, err := svc.CopyFinalize(
		context.Background(),
		&query.Target{TableGroup: "test"},
		nil,
		&query.ExecuteOptions{ReservedConnectionId: connID},
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "broken pipe")
	require.Nil(t, rs, "ReservedState should be nil when multipooler did not attach one")
	require.NotContains(t, svc.copyStreams, connID)
}

// --- ConcludeTransaction tests ---

func TestConcludeTransaction_Commit(t *testing.T) {
	mockClient := &mockMultiPoolerServiceClient{
		concludeResponse: &multipoolerservice.ConcludeTransactionResponse{
			Result: (&sqltypes.Result{CommandTag: "COMMIT"}).ToProto(),
		},
	}

	svc := newTestGRPCQueryService(mockClient)

	result, reservedState, err := svc.ConcludeTransaction(
		context.Background(),
		&query.Target{TableGroup: "test"},
		&query.ExecuteOptions{ReservedConnectionId: 42},
		multipoolerservice.TransactionConclusion_TRANSACTION_CONCLUSION_COMMIT,
		nil,
		false,
		false,
	)

	require.NoError(t, err)
	require.Equal(t, "COMMIT", result.CommandTag)
	require.Equal(t, uint32(0), reservedState.GetReservationReasons())
}

func TestConcludeTransaction_Rollback(t *testing.T) {
	mockClient := &mockMultiPoolerServiceClient{
		concludeResponse: &multipoolerservice.ConcludeTransactionResponse{
			Result: (&sqltypes.Result{CommandTag: "ROLLBACK"}).ToProto(),
		},
	}

	svc := newTestGRPCQueryService(mockClient)

	result, reservedState, err := svc.ConcludeTransaction(
		context.Background(),
		&query.Target{TableGroup: "test"},
		&query.ExecuteOptions{ReservedConnectionId: 42},
		multipoolerservice.TransactionConclusion_TRANSACTION_CONCLUSION_ROLLBACK,
		nil,
		false,
		false,
	)

	require.NoError(t, err)
	require.Equal(t, "ROLLBACK", result.CommandTag)
	require.Equal(t, uint32(0), reservedState.GetReservationReasons())
}

func TestConcludeTransaction_StillReserved(t *testing.T) {
	poolerID := &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}
	mockClient := &mockMultiPoolerServiceClient{
		concludeResponse: &multipoolerservice.ConcludeTransactionResponse{
			Result: (&sqltypes.Result{CommandTag: "COMMIT"}).ToProto(),
			ReservedState: &query.ReservedState{
				ReservationReasons:   protoutil.ReasonPortal,
				ReservedConnectionId: 42,
				PoolerId:             poolerID,
			},
		},
	}

	svc := newTestGRPCQueryService(mockClient)

	result, reservedState, err := svc.ConcludeTransaction(
		context.Background(),
		&query.Target{TableGroup: "test"},
		&query.ExecuteOptions{ReservedConnectionId: 42},
		multipoolerservice.TransactionConclusion_TRANSACTION_CONCLUSION_COMMIT,
		nil,
		false,
		false,
	)

	require.NoError(t, err)
	require.Equal(t, "COMMIT", result.CommandTag)
	require.NotEqual(t, uint64(0), reservedState.GetReservedConnectionId(), "connection should still be reserved")
	require.Equal(t, protoutil.ReasonPortal, reservedState.GetReservationReasons())
}

func TestConcludeTransaction_Error(t *testing.T) {
	mockClient := &mockMultiPoolerServiceClient{
		concludeErr: errors.New("conclude failed"),
	}

	svc := newTestGRPCQueryService(mockClient)

	_, _, err := svc.ConcludeTransaction(
		context.Background(),
		&query.Target{TableGroup: "test"},
		&query.ExecuteOptions{ReservedConnectionId: 42},
		multipoolerservice.TransactionConclusion_TRANSACTION_CONCLUSION_COMMIT,
		nil,
		false,
		false,
	)

	require.Error(t, err)
	// The grpc client now passes the multipooler error through FromGRPC so the
	// underlying PostgreSQL diagnostic surfaces unwrapped. For a generic mock
	// error we keep its original message instead of an internal RPC prefix.
	require.Contains(t, err.Error(), "conclude failed")
}
