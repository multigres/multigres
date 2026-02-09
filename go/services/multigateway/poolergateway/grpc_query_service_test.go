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

	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
)

// mockBidiStream is a mock implementation of grpc.BidiStreamingClient for testing.
type mockBidiStream struct {
	// Configurable behavior
	sendErr      error
	recvErr      error
	recvResponse *multipoolerservice.CopyBidiExecuteResponse

	// Track calls for verification
	closeSendCalled atomic.Bool
	recvCalled      atomic.Bool
	sendCalled      atomic.Bool
}

func (m *mockBidiStream) Send(req *multipoolerservice.CopyBidiExecuteRequest) error {
	m.sendCalled.Store(true)
	return m.sendErr
}

func (m *mockBidiStream) Recv() (*multipoolerservice.CopyBidiExecuteResponse, error) {
	m.recvCalled.Store(true)
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

// mockReserveStream is a mock implementation of grpc.ServerStreamingClient for ReserveStreamExecute.
type mockReserveStream struct {
	responses []*multipoolerservice.ReserveStreamExecuteResponse
	index     int
	recvErr   error
	grpc.ClientStream
}

func (m *mockReserveStream) Recv() (*multipoolerservice.ReserveStreamExecuteResponse, error) {
	if m.recvErr != nil {
		return nil, m.recvErr
	}
	if m.index >= len(m.responses) {
		return nil, io.EOF
	}
	resp := m.responses[m.index]
	m.index++
	return resp, nil
}

func (m *mockReserveStream) Header() (metadata.MD, error) { return nil, nil }
func (m *mockReserveStream) Trailer() metadata.MD         { return nil }
func (m *mockReserveStream) Context() context.Context     { return context.Background() }
func (m *mockReserveStream) SendMsg(msg any) error        { return nil }
func (m *mockReserveStream) RecvMsg(msg any) error        { return nil }
func (m *mockReserveStream) CloseSend() error             { return nil }

// mockMultiPoolerServiceClient is a mock implementation of MultiPoolerServiceClient.
type mockMultiPoolerServiceClient struct {
	// CopyBidiExecute behavior
	bidiStream    *mockBidiStream
	bidiStreamErr error

	// ReserveStreamExecute behavior
	reserveStream    *mockReserveStream
	reserveStreamErr error

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

func (m *mockMultiPoolerServiceClient) ReserveStreamExecute(ctx context.Context, in *multipoolerservice.ReserveStreamExecuteRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[multipoolerservice.ReserveStreamExecuteResponse], error) {
	if m.reserveStreamErr != nil {
		return nil, m.reserveStreamErr
	}
	return m.reserveStream, nil
}

func (m *mockMultiPoolerServiceClient) ConcludeTransaction(ctx context.Context, in *multipoolerservice.ConcludeTransactionRequest, opts ...grpc.CallOption) (*multipoolerservice.ConcludeTransactionResponse, error) {
	if m.concludeErr != nil {
		return nil, m.concludeErr
	}
	return m.concludeResponse, nil
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

	_, _, _, err := svc.CopyReady(
		context.Background(),
		&query.Target{TableGroup: "test"},
		"COPY t FROM STDIN",
		&query.ExecuteOptions{},
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "COPY initiation failed")
	require.Contains(t, err.Error(), "some backend error")
	// Verify cleanup was called
	require.True(t, mockStream.closeSendCalled.Load(), "CloseSend should be called on error")
	// copyStreams should be empty
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
			Phase:                multipoolerservice.CopyBidiExecuteResponse_READY,
			ReservedConnectionId: 12345,
			Format:               0,
			ColumnFormats:        []int32{0, 0, 0},
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
	)

	require.NoError(t, err)
	require.Equal(t, int16(0), format)
	require.Equal(t, []int16{0, 0, 0}, columnFormats)
	require.Equal(t, uint64(12345), reservedState.ReservedConnectionId)

	// Verify cleanup was NOT called (success path)
	require.False(t, mockStream.closeSendCalled.Load(), "CloseSend should NOT be called on success")

	// Stream should be in copyStreams map
	require.Len(t, svc.copyStreams, 1)
	_, exists := svc.copyStreams[12345]
	require.True(t, exists, "Stream should be stored in copyStreams with reserved connection ID")
}

// --- ReserveStreamExecute tests ---

func TestReserveStreamExecute_Success(t *testing.T) {
	poolerID := &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}
	mockClient := &mockMultiPoolerServiceClient{
		reserveStream: &mockReserveStream{
			responses: []*multipoolerservice.ReserveStreamExecuteResponse{
				{
					Result:               (&sqltypes.Result{CommandTag: "SELECT 1"}).ToProto(),
					ReservedConnectionId: 42,
					PoolerId:             poolerID,
				},
			},
		},
	}

	svc := newTestGRPCQueryService(mockClient)

	var callbackResults []*sqltypes.Result
	reservedState, err := svc.ReserveStreamExecute(
		context.Background(),
		&query.Target{TableGroup: "test"},
		"SELECT 1",
		&query.ExecuteOptions{User: "testuser"},
		protoutil.NewTransactionReservationOptions(),
		func(_ context.Context, r *sqltypes.Result) error {
			callbackResults = append(callbackResults, r)
			return nil
		},
	)

	require.NoError(t, err)
	require.Equal(t, uint64(42), reservedState.ReservedConnectionId)
	require.Equal(t, poolerID.Cell, reservedState.PoolerID.Cell)
	require.Len(t, callbackResults, 1)
	require.Equal(t, "SELECT 1", callbackResults[0].CommandTag)
}

func TestReserveStreamExecute_StreamError(t *testing.T) {
	mockClient := &mockMultiPoolerServiceClient{
		reserveStreamErr: errors.New("rpc failed"),
	}

	svc := newTestGRPCQueryService(mockClient)

	_, err := svc.ReserveStreamExecute(
		context.Background(),
		&query.Target{TableGroup: "test"},
		"SELECT 1",
		&query.ExecuteOptions{},
		protoutil.NewTransactionReservationOptions(),
		func(_ context.Context, _ *sqltypes.Result) error { return nil },
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to start reserve stream execute")
}

func TestReserveStreamExecute_RecvError(t *testing.T) {
	mockClient := &mockMultiPoolerServiceClient{
		reserveStream: &mockReserveStream{
			recvErr: errors.New("recv failed"),
		},
	}

	svc := newTestGRPCQueryService(mockClient)

	_, err := svc.ReserveStreamExecute(
		context.Background(),
		&query.Target{TableGroup: "test"},
		"SELECT 1",
		&query.ExecuteOptions{},
		protoutil.NewTransactionReservationOptions(),
		func(_ context.Context, _ *sqltypes.Result) error { return nil },
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "reserve stream receive error")
}

// --- ConcludeTransaction tests ---

func TestConcludeTransaction_Commit(t *testing.T) {
	mockClient := &mockMultiPoolerServiceClient{
		concludeResponse: &multipoolerservice.ConcludeTransactionResponse{
			Result:           (&sqltypes.Result{CommandTag: "COMMIT"}).ToProto(),
			RemainingReasons: 0,
		},
	}

	svc := newTestGRPCQueryService(mockClient)

	result, remainingReasons, err := svc.ConcludeTransaction(
		context.Background(),
		&query.Target{TableGroup: "test"},
		&query.ExecuteOptions{ReservedConnectionId: 42},
		multipoolerservice.TransactionConclusion_TRANSACTION_CONCLUSION_COMMIT,
	)

	require.NoError(t, err)
	require.Equal(t, "COMMIT", result.CommandTag)
	require.Equal(t, uint32(0), remainingReasons)
}

func TestConcludeTransaction_Rollback(t *testing.T) {
	mockClient := &mockMultiPoolerServiceClient{
		concludeResponse: &multipoolerservice.ConcludeTransactionResponse{
			Result:           (&sqltypes.Result{CommandTag: "ROLLBACK"}).ToProto(),
			RemainingReasons: 0,
		},
	}

	svc := newTestGRPCQueryService(mockClient)

	result, remainingReasons, err := svc.ConcludeTransaction(
		context.Background(),
		&query.Target{TableGroup: "test"},
		&query.ExecuteOptions{ReservedConnectionId: 42},
		multipoolerservice.TransactionConclusion_TRANSACTION_CONCLUSION_ROLLBACK,
	)

	require.NoError(t, err)
	require.Equal(t, "ROLLBACK", result.CommandTag)
	require.Equal(t, uint32(0), remainingReasons)
}

func TestConcludeTransaction_StillReserved(t *testing.T) {
	mockClient := &mockMultiPoolerServiceClient{
		concludeResponse: &multipoolerservice.ConcludeTransactionResponse{
			Result:           (&sqltypes.Result{CommandTag: "COMMIT"}).ToProto(),
			RemainingReasons: protoutil.ReasonPortal,
		},
	}

	svc := newTestGRPCQueryService(mockClient)

	result, remainingReasons, err := svc.ConcludeTransaction(
		context.Background(),
		&query.Target{TableGroup: "test"},
		&query.ExecuteOptions{ReservedConnectionId: 42},
		multipoolerservice.TransactionConclusion_TRANSACTION_CONCLUSION_COMMIT,
	)

	require.NoError(t, err)
	require.Equal(t, "COMMIT", result.CommandTag)
	require.NotEqual(t, uint32(0), remainingReasons, "connection should still be reserved")
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
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "conclude transaction failed")
}
