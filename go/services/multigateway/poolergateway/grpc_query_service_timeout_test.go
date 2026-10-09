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

package poolergateway

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/queryrpc"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
)

// drainingStream stands in for a multipooler that has been asked to cancel
// the backend and is still waiting for it to stop: Recv blocks until either
// the pooler reports the cancellation (done) or the RPC itself is torn down
// (ctx).
type drainingStream struct {
	grpc.ServerStreamingClient[multipoolerservice.StreamExecuteResponse]
	ctx  context.Context
	done <-chan struct{}
	err  error
	// rpcErrAtDone records whether the RPC was still alive when the pooler
	// reported (nil) or had already been torn down (non-nil).
	rpcErrAtDone error
}

func (s *drainingStream) Recv() (*multipoolerservice.StreamExecuteResponse, error) {
	select {
	case <-s.done:
		s.rpcErrAtDone = s.ctx.Err()
		return nil, s.err
	case <-s.ctx.Done():
		return nil, status.Error(codes.Canceled, "rpc torn down")
	}
}

func (s *drainingStream) Context() context.Context { return s.ctx }
func (s *drainingStream) Header() (metadata.MD, error) {
	return nil, nil
}
func (s *drainingStream) Trailer() metadata.MD { return nil }

func TestStreamExecute_StatementDeadlineWaitsForBackendCancel(t *testing.T) {
	const timeout = 50 * time.Millisecond
	// The "backend" only honors the cancel this long after the deadline.
	const cancelLatency = 100 * time.Millisecond

	drained := make(chan struct{})
	var rpcCtx context.Context
	var sentOptions *query.ExecuteOptions
	var stream *drainingStream
	mockClient := &mockMultipoolerServiceClient{
		streamExecuteFn: func(ctx context.Context, in *multipoolerservice.StreamExecuteRequest) (grpc.ServerStreamingClient[multipoolerservice.StreamExecuteResponse], error) {
			rpcCtx = ctx
			sentOptions = in.GetOptions()
			stream = &drainingStream{
				ctx:  ctx,
				done: drained,
				err:  status.Error(codes.DeadlineExceeded, "pooler statement timeout"),
			}
			return stream, nil
		},
	}
	svc := newTestGRPCQueryService(mockClient)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	deadline, _ := ctx.Deadline()

	go func() {
		<-ctx.Done()
		time.Sleep(cancelLatency)
		close(drained)
	}()

	start := time.Now()
	_, err := svc.StreamExecute(ctx, protoutil.NewTarget("", "test", "", query.Mode_MODE_UNSPECIFIED),
		"SELECT slow()", &query.ExecuteOptions{}, nil,
		func(context.Context, *sqltypes.Result) error { return nil })
	elapsed := time.Since(start)

	// The pooler got the remaining budget, not the RPC deadline.
	require.NotNil(t, sentOptions.GetStatementTimeout())
	require.InDelta(t, timeout, sentOptions.GetStatementTimeout().AsDuration(), float64(20*time.Millisecond))
	rpcDeadline, ok := rpcCtx.Deadline()
	require.True(t, ok)
	require.WithinDuration(t, deadline.Add(constants.StatementCancelDrainGrace), rpcDeadline, time.Millisecond)

	// The error only surfaces once the pooler reported the backend stopped.
	require.GreaterOrEqual(t, elapsed, timeout+cancelLatency)
	require.NoError(t, stream.rpcErrAtDone, "RPC must not be torn down at the statement deadline")
	var diag *mterrors.PgDiagnostic
	require.ErrorAs(t, err, &diag)
	require.Equal(t, mterrors.PgSSQueryCanceled, diag.Code)
}

func TestStreamExecute_ExplicitCancelStillTearsDownRPC(t *testing.T) {
	var rpcCtx context.Context
	mockClient := &mockMultipoolerServiceClient{
		streamExecuteFn: func(ctx context.Context, in *multipoolerservice.StreamExecuteRequest) (grpc.ServerStreamingClient[multipoolerservice.StreamExecuteResponse], error) {
			rpcCtx = ctx
			return &drainingStream{ctx: ctx, done: make(chan struct{})}, nil
		},
	}
	svc := newTestGRPCQueryService(mockClient)

	// A statement deadline is set, but the client cancels first (CancelRequest,
	// connection close): the RPC must go down immediately, not at deadline+grace.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, err := svc.StreamExecute(ctx, protoutil.NewTarget("", "test", "", query.Mode_MODE_UNSPECIFIED),
		"SELECT slow()", &query.ExecuteOptions{}, nil,
		func(context.Context, *sqltypes.Result) error { return nil })

	require.Error(t, err)
	require.Less(t, time.Since(start), time.Second)
	require.Error(t, rpcCtx.Err())
}

func TestStatementRPCContext_NoDeadlinePassesThrough(t *testing.T) {
	options := &query.ExecuteOptions{StatementTimeout: nil}
	ctx := context.Background()
	rpcCtx, cancel := statementRPCContext(ctx, options)
	defer cancel()
	require.Equal(t, ctx, rpcCtx)
	require.Nil(t, options.GetStatementTimeout())
}

// slowCancelServer is a pooler whose backend takes cancelLatency to honor the
// statement_timeout it was handed: the operation returns DeadlineExceeded only
// after that, the way the real executor does after cancel-and-drain.
type slowCancelServer struct {
	multipoolerservice.UnimplementedMultipoolerServiceServer
	cancelLatency time.Duration
	streams       atomic.Int32
	// opCtxErrAtReturn records the server-side operation context state when
	// the timed-out operation returned: non-nil means the gateway had already
	// torn the stream down instead of waiting.
	opCtxErrAtReturn error
}

func (s *slowCancelServer) ExecuteStream(stream multipoolerservice.MultipoolerService_ExecuteStreamServer) error {
	s.streams.Add(1)
	return queryrpc.Serve(stream, func(req *multipoolerservice.StreamExecuteRequest, out multipoolerservice.MultipoolerService_StreamExecuteServer) error {
		budget := req.GetOptions().GetStatementTimeout().AsDuration()
		if budget <= 0 {
			return nil
		}
		time.Sleep(budget + s.cancelLatency)
		s.opCtxErrAtReturn = out.Context().Err()
		return status.Error(codes.DeadlineExceeded, "canceling statement due to statement timeout")
	})
}

func TestReusedStream_StatementDeadlineWaitsForBackendCancel(t *testing.T) {
	const timeout = 50 * time.Millisecond
	const cancelLatency = 100 * time.Millisecond

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s := &slowCancelServer{cancelLatency: cancelLatency}
	server := grpc.NewServer()
	multipoolerservice.RegisterMultipoolerServiceServer(server, s)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)
	qs := queryServiceAt(t, lis.Addr().String())
	t.Cleanup(func() { require.NoError(t, qs.Close()) })

	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	start := time.Now()
	_, err = qs.StreamExecute(ctx, &query.Target{}, "SELECT slow()", &query.ExecuteOptions{}, nil, noopStreamCallback)
	elapsed := time.Since(start)

	var diag *mterrors.PgDiagnostic
	require.ErrorAs(t, err, &diag)
	require.Equal(t, mterrors.PgSSQueryCanceled, diag.Code)
	require.GreaterOrEqual(t, elapsed, timeout+cancelLatency, "error must wait for the backend to stop")
	require.NoError(t, s.opCtxErrAtReturn, "gateway must not tear the stream down at the statement deadline")

	// The timed-out operation completed normally, so the lease is reused rather
	// than discarded: the next statement rides the same stream.
	_, err = qs.StreamExecute(t.Context(), &query.Target{}, "SELECT 1", &query.ExecuteOptions{}, nil, noopStreamCallback)
	require.NoError(t, err)
	require.EqualValues(t, 1, s.streams.Load())
}

func TestConcludeTransaction_StatementDeadlineHandedToPooler(t *testing.T) {
	mockClient := &mockMultipoolerServiceClient{
		concludeResponse: &multipoolerservice.ConcludeTransactionResponse{
			Result: &query.QueryResult{CommandTag: "COMMIT"},
		},
	}
	svc := newTestGRPCQueryService(mockClient)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	deadline, _ := ctx.Deadline()

	_, _, err := svc.ConcludeTransaction(ctx, protoutil.NewTarget("", "test", "", query.Mode_MODE_UNSPECIFIED),
		&query.ExecuteOptions{ReservedConnectionId: 1}, multipoolerservice.TransactionConclusion_TRANSACTION_CONCLUSION_COMMIT,
		nil, false, false, nil)
	require.NoError(t, err)

	// COMMIT can wait (synchronous replication, for one), so it gets the same
	// treatment as a statement body: the budget goes to the pooler and the RPC
	// outlives the statement deadline.
	require.InDelta(t, time.Minute, mockClient.concludeReq.GetOptions().GetStatementTimeout().AsDuration(), float64(time.Second))
	rpcDeadline, ok := mockClient.concludeCtx.Deadline()
	require.True(t, ok)
	require.WithinDuration(t, deadline.Add(constants.StatementCancelDrainGrace), rpcDeadline, time.Millisecond)
}
