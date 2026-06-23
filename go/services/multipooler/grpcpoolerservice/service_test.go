// Copyright 2025 Supabase, Inc.
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

package grpcpoolerservice

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/sqltypes"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
)

func TestGetAuthCredentials_Validation(t *testing.T) {
	srv := &poolerService{pooler: nil}

	t.Run("missing username", func(t *testing.T) {
		req := &multipoolerpb.GetAuthCredentialsRequest{
			Database: "testdb",
			Username: "",
		}
		_, err := srv.GetAuthCredentials(context.Background(), req)
		require.Error(t, err)

		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "username is required")
	})

	t.Run("missing database", func(t *testing.T) {
		req := &multipoolerpb.GetAuthCredentialsRequest{
			Database: "",
			Username: "testuser",
		}
		_, err := srv.GetAuthCredentials(context.Background(), req)
		require.Error(t, err)

		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "database is required")
	})

	t.Run("nil pooler", func(t *testing.T) {
		req := &multipoolerpb.GetAuthCredentialsRequest{
			Database: "testdb",
			Username: "testuser",
		}
		_, err := srv.GetAuthCredentials(context.Background(), req)
		require.Error(t, err)

		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.Unavailable, st.Code())
		assert.Contains(t, st.Message(), "pooler not initialized")
	})
}

func TestStreamPoolerHealth_Validation(t *testing.T) {
	t.Run("nil pooler", func(t *testing.T) {
		srv := &poolerService{pooler: nil}
		req := &multipoolerpb.StreamPoolerHealthRequest{}

		// Create a mock stream that captures the error
		mockStream := &mockHealthStream{ctx: context.Background()}
		err := srv.StreamPoolerHealth(req, mockStream)

		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.Unavailable, st.Code())
		assert.Contains(t, st.Message(), "pooler not initialized")
	})
}

// mockHealthStream is a minimal mock for testing StreamPoolerHealth validation.
type mockHealthStream struct {
	multipoolerpb.MultiPoolerService_StreamPoolerHealthServer
	ctx context.Context
}

func (m *mockHealthStream) Context() context.Context {
	return m.ctx
}

func (m *mockHealthStream) Send(*multipoolerpb.StreamPoolerHealthResponse) error {
	return nil
}

type mockCopyBidiStream struct {
	ctx           context.Context
	sendErrAtCall int
	sendCalls     int
	sent          []*multipoolerpb.CopyBidiExecuteResponse
}

func (m *mockCopyBidiStream) SetHeader(metadata.MD) error  { return nil }
func (m *mockCopyBidiStream) SendHeader(metadata.MD) error { return nil }
func (m *mockCopyBidiStream) SetTrailer(metadata.MD)       {}
func (m *mockCopyBidiStream) Context() context.Context {
	if m.ctx == nil {
		return context.Background()
	}
	return m.ctx
}
func (m *mockCopyBidiStream) SendMsg(any) error { return nil }
func (m *mockCopyBidiStream) RecvMsg(any) error { return nil }

func (m *mockCopyBidiStream) Send(resp *multipoolerpb.CopyBidiExecuteResponse) error {
	m.sendCalls++
	if m.sendErrAtCall > 0 && m.sendCalls == m.sendErrAtCall {
		return errors.New("send failed")
	}
	m.sent = append(m.sent, resp)
	return nil
}

func (m *mockCopyBidiStream) Recv() (*multipoolerpb.CopyBidiExecuteRequest, error) {
	return nil, errors.New("not used by copyBidiExecuteToStdout")
}

var _ multipoolerpb.MultiPoolerService_CopyBidiExecuteServer = (*mockCopyBidiStream)(nil)

type mockCopyQueryService struct {
	copyOutReadyFn  func(context.Context, *query.Target, string, *query.ExecuteOptions, *query.ReservationOptions) (int16, []int16, []*mterrors.PgDiagnostic, *query.ReservedState, error)
	copyOutStreamFn func(context.Context, *query.Target, *query.ExecuteOptions, func(client.CopyOutMessage) error) (*sqltypes.Result, *query.ReservedState, error)
	copyAbortFn     func(context.Context, *query.Target, string, *query.ExecuteOptions) (*query.ReservedState, error)
}

func (m *mockCopyQueryService) ExecuteQuery(context.Context, *query.Target, string, *query.ExecuteOptions) (*sqltypes.Result, *query.ReservedState, error) {
	return nil, nil, nil
}

func (m *mockCopyQueryService) StreamExecute(context.Context, *query.Target, string, *query.ExecuteOptions, *query.ReservationOptions, func(context.Context, *sqltypes.Result) error) (*query.ReservedState, error) {
	return nil, nil
}

func (m *mockCopyQueryService) PortalStreamExecute(context.Context, *query.Target, *query.PreparedStatement, *query.Portal, *query.ExecuteOptions, *multipoolerpb.PortalExecuteOptions, *query.ReservationOptions, func(context.Context, *sqltypes.Result) error) (*query.ReservedState, error) {
	return nil, nil
}

func (m *mockCopyQueryService) Describe(context.Context, *query.Target, *query.PreparedStatement, *query.Portal, *query.ExecuteOptions) (*query.StatementDescription, error) {
	return nil, nil
}

func (m *mockCopyQueryService) Close() error { return nil }

func (m *mockCopyQueryService) CopyReady(context.Context, *query.Target, string, *query.ExecuteOptions, *query.ReservationOptions) (int16, []int16, *query.ReservedState, error) {
	return 0, nil, nil, nil
}

func (m *mockCopyQueryService) CopySendData(context.Context, *query.Target, []byte, *query.ExecuteOptions) error {
	return nil
}

func (m *mockCopyQueryService) CopyFinalize(context.Context, *query.Target, []byte, *query.ExecuteOptions) (*sqltypes.Result, *query.ReservedState, error) {
	return nil, nil, nil
}

func (m *mockCopyQueryService) CopyAbort(ctx context.Context, target *query.Target, errorMsg string, options *query.ExecuteOptions) (*query.ReservedState, error) {
	if m.copyAbortFn != nil {
		return m.copyAbortFn(ctx, target, errorMsg, options)
	}
	return nil, nil
}

func (m *mockCopyQueryService) CopyOutReady(ctx context.Context, target *query.Target, copyQuery string, options *query.ExecuteOptions, reservationOptions *query.ReservationOptions) (int16, []int16, []*mterrors.PgDiagnostic, *query.ReservedState, error) {
	if m.copyOutReadyFn == nil {
		return 0, nil, nil, nil, nil
	}
	return m.copyOutReadyFn(ctx, target, copyQuery, options, reservationOptions)
}

func (m *mockCopyQueryService) CopyOutStream(ctx context.Context, target *query.Target, options *query.ExecuteOptions, onMessage func(client.CopyOutMessage) error) (*sqltypes.Result, *query.ReservedState, error) {
	if m.copyOutStreamFn == nil {
		return nil, nil, nil
	}
	return m.copyOutStreamFn(ctx, target, options, onMessage)
}

func (m *mockCopyQueryService) ConcludeTransaction(context.Context, *query.Target, *query.ExecuteOptions, multipoolerpb.TransactionConclusion, []string, bool) (*sqltypes.Result, *query.ReservedState, error) {
	return nil, nil, nil
}

func (m *mockCopyQueryService) DiscardTempTables(context.Context, *query.Target, *query.ExecuteOptions) (*sqltypes.Result, *query.ReservedState, error) {
	return nil, nil, nil
}

func (m *mockCopyQueryService) ReleaseReservedConnection(context.Context, *query.Target, *query.ExecuteOptions) error {
	return nil
}

func TestCopyBidiExecuteToStdout_Success(t *testing.T) {
	stream := &mockCopyBidiStream{}
	svc := &poolerService{}

	exec := &mockCopyQueryService{
		copyOutReadyFn: func(context.Context, *query.Target, string, *query.ExecuteOptions, *query.ReservationOptions) (int16, []int16, []*mterrors.PgDiagnostic, *query.ReservedState, error) {
			return 1, []int16{0, 1}, []*mterrors.PgDiagnostic{
				{MessageType: 'N', Severity: "NOTICE", Message: "init-notice"},
			}, &query.ReservedState{ReservedConnectionId: 42}, nil
		},
		copyOutStreamFn: func(_ context.Context, _ *query.Target, _ *query.ExecuteOptions, onMessage func(client.CopyOutMessage) error) (*sqltypes.Result, *query.ReservedState, error) {
			if err := onMessage(client.CopyOutMessage{Notice: &mterrors.PgDiagnostic{MessageType: 'N', Severity: "NOTICE", Message: "mid-notice"}}); err != nil {
				return nil, nil, err
			}
			if err := onMessage(client.CopyOutMessage{Data: []byte("row")}); err != nil {
				return nil, nil, err
			}
			return &sqltypes.Result{
				CommandTag: "COPY 1",
				Notices: []*mterrors.PgDiagnostic{
					{MessageType: 'N', Severity: "NOTICE", Message: "final-notice"},
				},
			}, &query.ReservedState{ReservedConnectionId: 42}, nil
		},
	}

	req := &multipoolerpb.CopyBidiExecuteRequest{
		Target:  protoutil.NewTarget("", "tg", "", query.Mode_MODE_UNSPECIFIED),
		Query:   "COPY t TO STDOUT",
		Options: &query.ExecuteOptions{User: "postgres"},
	}

	err := svc.copyBidiExecuteToStdout(context.Background(), stream, exec, req)
	require.NoError(t, err)
	require.Len(t, stream.sent, 4)
	require.Equal(t, multipoolerpb.CopyBidiExecuteResponse_READY, stream.sent[0].GetPhase())
	require.Equal(t, int32(1), stream.sent[0].GetFormat())
	require.Len(t, stream.sent[0].GetNotices(), 1)
	require.Equal(t, "init-notice", stream.sent[0].GetNotices()[0].GetMessage())
	require.Equal(t, multipoolerpb.CopyBidiExecuteResponse_DATA, stream.sent[1].GetPhase())
	require.Equal(t, "mid-notice", stream.sent[1].GetNotices()[0].GetMessage())
	require.Equal(t, multipoolerpb.CopyBidiExecuteResponse_DATA, stream.sent[2].GetPhase())
	require.Equal(t, []byte("row"), stream.sent[2].GetData())
	require.Equal(t, multipoolerpb.CopyBidiExecuteResponse_RESULT, stream.sent[3].GetPhase())
	require.Equal(t, "COPY 1", stream.sent[3].GetResult().GetCommandTag())
	require.Len(t, stream.sent[3].GetNotices(), 1)
	require.Equal(t, "final-notice", stream.sent[3].GetNotices()[0].GetMessage())
}

func TestCopyBidiExecuteToStdout_ReadySendFailureCallsCopyAbort(t *testing.T) {
	stream := &mockCopyBidiStream{sendErrAtCall: 1}
	svc := &poolerService{}
	abortCalled := false

	exec := &mockCopyQueryService{
		copyOutReadyFn: func(context.Context, *query.Target, string, *query.ExecuteOptions, *query.ReservationOptions) (int16, []int16, []*mterrors.PgDiagnostic, *query.ReservedState, error) {
			return 0, []int16{0}, nil, &query.ReservedState{ReservedConnectionId: 55}, nil
		},
		copyAbortFn: func(_ context.Context, _ *query.Target, _ string, options *query.ExecuteOptions) (*query.ReservedState, error) {
			abortCalled = true
			require.Equal(t, uint64(55), options.GetReservedConnectionId())
			return nil, nil
		},
	}

	req := &multipoolerpb.CopyBidiExecuteRequest{
		Target:  protoutil.NewTarget("", "tg", "", query.Mode_MODE_UNSPECIFIED),
		Query:   "COPY t TO STDOUT",
		Options: &query.ExecuteOptions{User: "postgres"},
	}

	err := svc.copyBidiExecuteToStdout(context.Background(), stream, exec, req)
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Internal, st.Code())
	require.True(t, abortCalled, "CopyAbort should run when sending READY fails")
}

func TestCopyBidiExecuteToStdout_StreamErrorSendsErrorPhase(t *testing.T) {
	stream := &mockCopyBidiStream{}
	svc := &poolerService{}

	exec := &mockCopyQueryService{
		copyOutReadyFn: func(context.Context, *query.Target, string, *query.ExecuteOptions, *query.ReservationOptions) (int16, []int16, []*mterrors.PgDiagnostic, *query.ReservedState, error) {
			return 0, []int16{0}, nil, &query.ReservedState{ReservedConnectionId: 91}, nil
		},
		copyOutStreamFn: func(context.Context, *query.Target, *query.ExecuteOptions, func(client.CopyOutMessage) error) (*sqltypes.Result, *query.ReservedState, error) {
			return nil, &query.ReservedState{ReservedConnectionId: 91}, errors.New("copy stream failed")
		},
	}

	req := &multipoolerpb.CopyBidiExecuteRequest{
		Target:  protoutil.NewTarget("", "tg", "", query.Mode_MODE_UNSPECIFIED),
		Query:   "COPY t TO STDOUT",
		Options: &query.ExecuteOptions{User: "postgres"},
	}

	err := svc.copyBidiExecuteToStdout(context.Background(), stream, exec, req)
	require.Error(t, err)
	require.Len(t, stream.sent, 2)
	require.Equal(t, multipoolerpb.CopyBidiExecuteResponse_READY, stream.sent[0].GetPhase())
	require.Equal(t, multipoolerpb.CopyBidiExecuteResponse_ERROR, stream.sent[1].GetPhase())
	require.Contains(t, stream.sent[1].GetError(), "copy stream failed")
}
