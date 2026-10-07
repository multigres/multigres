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
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/callerid"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/queryrpc"
	"github.com/multigres/multigres/go/common/sqltypes"
	pb "github.com/multigres/multigres/go/pb/multipoolerservice"
	querypb "github.com/multigres/multigres/go/pb/query"
)

type operationTestServer struct {
	pb.UnimplementedMultipoolerServiceServer
	call    func(context.Context, proto.Message) (proto.Message, error)
	streams atomic.Int32
	custom  func(pb.MultipoolerService_ExecuteStreamServer) error
}

func (s *operationTestServer) ExecuteStream(stream pb.MultipoolerService_ExecuteStreamServer) error {
	s.streams.Add(1)
	if s.custom != nil {
		return s.custom(stream)
	}
	return queryrpc.Serve(stream, s)
}

func (s *operationTestServer) StreamExecute(req *pb.StreamExecuteRequest, out pb.MultipoolerService_StreamExecuteServer) error {
	response, err := s.call(out.Context(), req)
	if response != nil {
		if sendErr := out.Send(response.(*pb.StreamExecuteResponse)); sendErr != nil {
			return sendErr
		}
	}
	return err
}

func (s *operationTestServer) PortalStreamExecute(req *pb.PortalStreamExecuteRequest, out pb.MultipoolerService_PortalStreamExecuteServer) error {
	response, err := s.call(out.Context(), req)
	if response != nil {
		if sendErr := out.Send(response.(*pb.PortalStreamExecuteResponse)); sendErr != nil {
			return sendErr
		}
	}
	return err
}

func (s *operationTestServer) ExecuteQuery(ctx context.Context, req *pb.ExecuteQueryRequest) (*pb.ExecuteQueryResponse, error) {
	response, err := s.call(ctx, req)
	if response == nil {
		return nil, err
	}
	return response.(*pb.ExecuteQueryResponse), err
}

func (s *operationTestServer) Describe(ctx context.Context, req *pb.DescribeRequest) (*pb.DescribeResponse, error) {
	response, err := s.call(ctx, req)
	if response == nil {
		return nil, err
	}
	return response.(*pb.DescribeResponse), err
}

func (s *operationTestServer) ConcludeTransaction(ctx context.Context, req *pb.ConcludeTransactionRequest) (*pb.ConcludeTransactionResponse, error) {
	response, err := s.call(ctx, req)
	if response == nil {
		return nil, err
	}
	return response.(*pb.ConcludeTransactionResponse), err
}

func (s *operationTestServer) DiscardTempTables(ctx context.Context, req *pb.DiscardTempTablesRequest) (*pb.DiscardTempTablesResponse, error) {
	response, err := s.call(ctx, req)
	if response == nil {
		return nil, err
	}
	return response.(*pb.DiscardTempTablesResponse), err
}

func (s *operationTestServer) ReleaseReservedConnection(ctx context.Context, req *pb.ReleaseReservedConnectionRequest) (*pb.ReleaseReservedConnectionResponse, error) {
	response, err := s.call(ctx, req)
	if response == nil {
		return nil, err
	}
	return response.(*pb.ReleaseReservedConnectionResponse), err
}

func queryServiceForOperations(t *testing.T, s *operationTestServer) *grpcQueryService {
	t.Helper()
	client, pool := connectStreamPool(t, s)
	return &grpcQueryService{client: client, executeStreams: pool, logger: slog.Default()}
}

// Exercise the public query-service methods, not just the envelope helpers.
// Caller identity and reservation/transaction controls must reach existing
// handlers unchanged, with response conversion still performed by the gateway.
func TestAllQueryOperationsReuseOneStream(t *testing.T) {
	cid := callerid.New("alice", "reuse-test")
	ctx := callerid.NewContext(t.Context(), cid)
	target := &querypb.Target{Mode: querypb.Mode_MODE_WRITABLE}
	options := &querypb.ExecuteOptions{ReservedConnectionId: 42, MaxRows: 1}
	prepared := &querypb.PreparedStatement{Name: "stmt", Query: "SELECT $1", ParamTypes: []uint32{23}, ForceReparse: true}
	portal := &querypb.Portal{Name: "cursor", PreparedStatementName: "stmt", ParamLengths: []int64{1}, ParamValues: []byte("7"), ResultFormats: []int32{1}}
	portalOptions := &pb.PortalExecuteOptions{IncludeDescribe: true}
	reservation := &querypb.ReservationOptions{Reasons: 1, BeginQuery: "BEGIN READ ONLY"}
	state := &querypb.ReservedState{ReservedConnectionId: 42}
	result := &querypb.QueryResult{CommandTag: "SELECT 1"}
	payload := &querypb.QueryResultPayload{Payload: &querypb.QueryResultPayload_Result{Result: result}}
	var callbacks int
	callback := func(_ context.Context, r *sqltypes.Result) error {
		callbacks++
		require.Equal(t, "SELECT 1", r.CommandTag)
		return nil
	}
	seen := make(chan proto.Message, 7)
	s := &operationTestServer{call: func(_ context.Context, req proto.Message) (proto.Message, error) {
		seen <- req
		switch req.(type) {
		case *pb.StreamExecuteRequest:
			return &pb.StreamExecuteResponse{Result: payload, ReservedState: state}, nil
		case *pb.PortalStreamExecuteRequest:
			return &pb.PortalStreamExecuteResponse{Result: payload, ReservedState: state}, nil
		case *pb.ExecuteQueryRequest:
			return &pb.ExecuteQueryResponse{Result: result, ReservedState: state}, nil
		case *pb.DescribeRequest:
			return &pb.DescribeResponse{Description: &querypb.StatementDescription{HasFields: true}}, nil
		case *pb.ConcludeTransactionRequest:
			return &pb.ConcludeTransactionResponse{Result: result, ReservedState: state}, nil
		case *pb.DiscardTempTablesRequest:
			return &pb.DiscardTempTablesResponse{Result: result, ReservedState: state}, nil
		case *pb.ReleaseReservedConnectionRequest:
			return &pb.ReleaseReservedConnectionResponse{ReservedState: state}, nil
		default:
			return nil, status.Error(codes.Internal, "unexpected request")
		}
	}}
	qs := queryServiceForOperations(t, s)
	check := func(want proto.Message, gotState *querypb.ReservedState, err error) {
		t.Helper()
		require.NoError(t, err)
		require.True(t, proto.Equal(state, gotState))
		require.True(t, proto.Equal(want, <-seen), "request fields changed")
	}
	got, err := qs.StreamExecute(ctx, target, "SELECT 1", options, reservation, callback)
	check(&pb.StreamExecuteRequest{Query: "SELECT 1", Target: target, Options: options, ReservationOptions: reservation, CallerId: cid}, got, err)
	got, err = qs.PortalStreamExecute(ctx, target, prepared, portal, options, portalOptions, reservation, callback)
	check(&pb.PortalStreamExecuteRequest{Target: target, PreparedStatement: prepared, Portal: portal, Options: options, PortalOptions: portalOptions, ReservationOptions: reservation, CallerId: cid}, got, err)
	res, got, err := qs.ExecuteQuery(ctx, target, "SELECT 1", options)
	check(&pb.ExecuteQueryRequest{Query: "SELECT 1", Target: target, Options: options, CallerId: cid}, got, err)
	require.Equal(t, "SELECT 1", res.CommandTag)
	description, err := qs.Describe(ctx, target, prepared, portal, options)
	require.NoError(t, err)
	require.NotNil(t, description.Fields, "zero-column RowDescription must survive")
	require.True(t, proto.Equal(&pb.DescribeRequest{Target: target, PreparedStatement: prepared, Portal: portal, Options: options, CallerId: cid}, <-seen))
	snapshot := map[string]string{"search_path": "public"}
	res, got, err = qs.ConcludeTransaction(ctx, target, options, pb.TransactionConclusion_TRANSACTION_CONCLUSION_COMMIT, []string{"cursor"}, true, true, snapshot)
	check(&pb.ConcludeTransactionRequest{Target: target, Options: options, Conclusion: pb.TransactionConclusion_TRANSACTION_CONCLUSION_COMMIT, ReleasePortalNames: []string{"cursor"}, ReleaseAllPortals: true, Chain: true, RollbackSessionSettings: &pb.SessionSettingsSnapshot{Vars: snapshot}, CallerId: cid}, got, err)
	require.Equal(t, "SELECT 1", res.CommandTag)
	_, got, err = qs.DiscardTempTables(ctx, target, options)
	check(&pb.DiscardTempTablesRequest{Target: target, Options: options, CallerId: cid}, got, err)
	got, err = qs.ReleaseReservedConnection(ctx, target, options, true)
	check(&pb.ReleaseReservedConnectionRequest{Target: target, Options: options, KeepStickyReservations: true, CallerId: cid}, got, err)
	require.Equal(t, 2, callbacks)
	require.Equal(t, int32(1), s.streams.Load())
}

func TestUnaryCompletionPreservesErrorDetailsAndDoesNotReplay(t *testing.T) {
	reserved := &querypb.ReservedState{ReservedConnectionId: 42}
	diagnostic := mterrors.NewPgError("ERROR", "23505", "deferred constraint failed", "original detail")
	original, err := status.Convert(mterrors.ToGRPC(diagnostic)).WithDetails(reserved)
	require.NoError(t, err)
	var calls atomic.Int32
	s := &operationTestServer{call: func(context.Context, proto.Message) (proto.Message, error) {
		calls.Add(1)
		return nil, original.Err()
	}}
	qs := queryServiceForOperations(t, s)
	_, got, err := qs.ConcludeTransaction(t.Context(), nil, &querypb.ExecuteOptions{ReservedConnectionId: 42}, pb.TransactionConclusion_TRANSACTION_CONCLUSION_COMMIT, nil, false, false, nil)
	require.Error(t, err)
	require.True(t, proto.Equal(reserved, got))
	var diag *mterrors.PgDiagnostic
	require.ErrorAs(t, err, &diag)
	require.Equal(t, diagnostic, diag)
	require.False(t, mterrors.IsPreExecutionUnavailable(err))
	// A handler error completes an operation; its healthy transport can be reused.
	_, _, err = qs.ExecuteQuery(t.Context(), nil, "again", nil)
	require.Error(t, err)
	require.Equal(t, int32(2), calls.Load())
	require.Equal(t, int32(1), s.streams.Load())
}

func TestNewOperationsFallbackBeforeSubmission(t *testing.T) {
	for _, outgoingMetadata := range []bool{false, true} {
		name := "old peer"
		if outgoingMetadata {
			name = "metadata"
		}
		t.Run(name, func(t *testing.T) {
			s := &operationTestServer{call: func(_ context.Context, req proto.Message) (proto.Message, error) {
				return &pb.ExecuteQueryResponse{Result: &querypb.QueryResult{CommandTag: "SELECT 1"}}, nil
			}}
			var submitted atomic.Bool
			s.custom = func(stream pb.MultipoolerService_ExecuteStreamServer) error {
				if err := stream.Send(&pb.ExecuteStreamResponse{Ready: true}); err != nil {
					return err
				}
				if _, err := stream.Recv(); err == nil {
					submitted.Store(true)
				}
				return nil
			}
			qs := queryServiceForOperations(t, s)
			ctx := t.Context()
			if outgoingMetadata {
				ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("x-test", "value"))
			}
			response, _, err := qs.ExecuteQuery(ctx, nil, "query", nil)
			require.NoError(t, err)
			require.Equal(t, "SELECT 1", response.CommandTag)
			require.False(t, submitted.Load(), "fallback must precede submission")
			if outgoingMetadata {
				require.Zero(t, s.streams.Load())
			} else {
				require.Equal(t, int32(1), s.streams.Load())
			}
		})
	}
}

func TestUnaryWaitsForCompletionAndRejectsMalformedReplies(t *testing.T) {
	response := &pb.ExecuteStreamResponse{Result: &pb.ExecuteStreamResponse_ExecuteQuery{ExecuteQuery: &pb.ExecuteQueryResponse{}}}
	cases := []struct {
		name   string
		frames []*pb.ExecuteStreamResponse
		code   codes.Code
	}{
		{"missing result", []*pb.ExecuteStreamResponse{{Completion: &statuspb.Status{}}}, codes.Internal},
		{"extra result", []*pb.ExecuteStreamResponse{response, response}, codes.Internal},
		{"wrong result", []*pb.ExecuteStreamResponse{{Result: &pb.ExecuteStreamResponse_Describe{Describe: &pb.DescribeResponse{}}}}, codes.Internal},
		{"result then EOF", []*pb.ExecuteStreamResponse{response}, codes.Unavailable},
		{"post-submission unavailable", []*pb.ExecuteStreamResponse{{Completion: &statuspb.Status{Code: int32(codes.Unavailable)}}}, codes.Unavailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &operationTestServer{custom: func(stream pb.MultipoolerService_ExecuteStreamServer) error {
				if err := stream.Send(&pb.ExecuteStreamResponse{Ready: true, SupportedOperations: []pb.ExecuteStreamOperation{pb.ExecuteStreamOperation_EXECUTE_QUERY}}); err != nil {
					return err
				}
				if _, err := stream.Recv(); err != nil {
					return err
				}
				for _, frame := range tc.frames {
					if err := stream.Send(frame); err != nil {
						return err
					}
				}
				return nil
			}}
			qs := queryServiceForOperations(t, s)
			_, err := callUnary(t.Context(), qs, &pb.ExecuteQueryRequest{}, qs.client.ExecuteQuery)
			require.Equal(t, tc.code, status.Code(err))
			require.False(t, mterrors.IsPreExecutionUnavailable(mterrors.FromGRPC(err)))
			require.Equal(t, int32(1), s.streams.Load())
		})
	}
}

// Transport refactor only: caller deadlines still cancel the active stream
// immediately. The future wait-for-cancel change must be reviewed separately.
func TestUnaryDeadlineRetainsImmediateCancellation(t *testing.T) {
	started, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer close(release)
	s := &operationTestServer{call: func(ctx context.Context, _ proto.Message) (proto.Message, error) {
		close(started)
		<-ctx.Done()
		close(cancelled)
		<-release
		return nil, status.FromContextError(ctx.Err()).Err()
	}}
	qs := queryServiceForOperations(t, s)
	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := callUnary(ctx, qs, &pb.ExecuteQueryRequest{}, qs.client.ExecuteQuery); result <- err }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not start")
	}
	select {
	case err := <-result:
		require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	case <-time.After(5 * time.Second):
		t.Fatal("caller waited for cleanup")
	}
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("backend operation was not cancelled")
	}
}

func TestPortalCanSuspendAndResumeOnReusableStream(t *testing.T) {
	var calls atomic.Int32
	s := &operationTestServer{call: func(_ context.Context, req proto.Message) (proto.Message, error) {
		portal := req.(*pb.PortalStreamExecuteRequest)
		tag := ""
		if calls.Add(1) == 2 {
			tag = "SELECT 2"
		}
		return &pb.PortalStreamExecuteResponse{Result: &querypb.QueryResultPayload{Payload: &querypb.QueryResultPayload_Result{Result: &querypb.QueryResult{CommandTag: tag}}}, ReservedState: &querypb.ReservedState{ReservedConnectionId: portal.Options.ReservedConnectionId}}, nil
	}}
	qs := queryServiceForOperations(t, s)
	for _, tag := range []string{"", "SELECT 2"} {
		got, err := qs.PortalStreamExecute(t.Context(), nil, &querypb.PreparedStatement{Query: "SELECT 1"}, &querypb.Portal{Name: "cursor"}, &querypb.ExecuteOptions{ReservedConnectionId: 42, MaxRows: 1}, nil, nil,
			func(_ context.Context, result *sqltypes.Result) error {
				require.Equal(t, tag, result.CommandTag)
				return nil
			})
		require.NoError(t, err)
		require.Equal(t, uint64(42), got.ReservedConnectionId)
	}
	require.Equal(t, int32(1), s.streams.Load())
}

func TestUnaryResultDoesNotReleaseLeaseBeforeCompletion(t *testing.T) {
	responseSent, complete := make(chan struct{}), make(chan struct{})
	defer close(complete)
	s := &operationTestServer{custom: func(stream pb.MultipoolerService_ExecuteStreamServer) error {
		if err := stream.Send(&pb.ExecuteStreamResponse{Ready: true, SupportedOperations: []pb.ExecuteStreamOperation{pb.ExecuteStreamOperation_EXECUTE_QUERY}}); err != nil {
			return err
		}
		if _, err := stream.Recv(); err != nil {
			return err
		}
		if err := stream.Send(&pb.ExecuteStreamResponse{Result: &pb.ExecuteStreamResponse_ExecuteQuery{ExecuteQuery: &pb.ExecuteQueryResponse{}}}); err != nil {
			return err
		}
		close(responseSent)
		select {
		case <-complete:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
		return stream.Send(&pb.ExecuteStreamResponse{Completion: &statuspb.Status{}})
	}}
	qs := queryServiceForOperations(t, s)
	result := make(chan error, 1)
	go func() {
		_, err := callUnary(t.Context(), qs, &pb.ExecuteQueryRequest{}, qs.client.ExecuteQuery)
		result <- err
	}()
	select {
	case <-responseSent:
	case <-time.After(5 * time.Second):
		t.Fatal("result was not sent")
	}
	select {
	case err := <-result:
		t.Fatalf("returned without completion: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	qs.executeStreams.mu.Lock()
	idle := len(qs.executeStreams.idle)
	qs.executeStreams.mu.Unlock()
	require.Zero(t, idle)
	complete <- struct{}{}
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("completion was not received")
	}
}
