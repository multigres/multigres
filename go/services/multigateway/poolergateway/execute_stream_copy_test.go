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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/sqltypes"
	pb "github.com/multigres/multigres/go/pb/multipoolerservice"
	querypb "github.com/multigres/multigres/go/pb/query"
)

func TestReusableCopyLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	state := &querypb.ReservedState{ReservedConnectionId: 42}
	options := &querypb.ExecuteOptions{ReservedConnectionId: 42, User: "copy-user"}
	s := &operationTestServer{}
	s.call = func(_ context.Context, _ proto.Message) (proto.Message, error) {
		return &pb.ExecuteQueryResponse{Result: (&sqltypes.Result{CommandTag: "SELECT 1"}).ToProto()}, nil
	}
	var calls atomic.Int32
	s.copy = func(stream pb.MultipoolerService_CopyBidiExecuteServer) error {
		calls.Add(1)
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		if !proto.Equal(options, req.Options) {
			return status.Error(codes.Internal, "lost COPY options")
		}
		if err := stream.Send(&pb.CopyBidiExecuteResponse{Phase: pb.CopyBidiExecuteResponse_READY, ReservedState: state, ColumnFormats: []int32{0}}); err != nil {
			return err
		}
		if req.Direction == pb.CopyBidiExecuteRequest_TO_STDOUT {
			if err := stream.Send(&pb.CopyBidiExecuteResponse{Phase: pb.CopyBidiExecuteResponse_DATA, Data: []byte("one\n")}); err != nil {
				return err
			}
		} else {
			req, err = stream.Recv()
			if err != nil {
				return err
			}
			if req.Phase == pb.CopyBidiExecuteRequest_FAIL {
				if err := stream.Send(&pb.CopyBidiExecuteResponse{Phase: pb.CopyBidiExecuteResponse_ERROR, Error: req.ErrorMessage, ReservedState: state}); err != nil {
					return err
				}
				return status.Error(codes.Aborted, "COPY aborted")
			}
			if string(req.Data) != "one\n" {
				return status.Error(codes.Internal, "lost COPY data")
			}
			req, err = stream.Recv()
			if err != nil {
				return err
			}
			if req.Phase != pb.CopyBidiExecuteRequest_DONE || string(req.Data) != "two\n" {
				return status.Error(codes.Internal, "lost COPY final data")
			}
		}
		return stream.Send(&pb.CopyBidiExecuteResponse{Phase: pb.CopyBidiExecuteResponse_RESULT, Result: (&sqltypes.Result{CommandTag: "COPY 2", RowsAffected: 2}).ToProto(), ReservedState: state})
	}
	g := queryServiceForOperations(t, s)
	target := &querypb.Target{}
	for range 2 {
		_, formats, reserved, err := g.CopyReady(ctx, target, "COPY t FROM STDIN", options, nil)
		require.NoError(t, err)
		require.Equal(t, []int16{0}, formats)
		require.True(t, proto.Equal(state, reserved))
		require.NoError(t, g.CopySendData(ctx, target, []byte("one\n"), options))
		result, reserved, err := g.CopyFinalize(ctx, target, []byte("two\n"), options)
		require.NoError(t, err)
		require.Equal(t, uint64(2), result.RowsAffected)
		require.True(t, proto.Equal(state, reserved))
		_, _, _, reserved, err = g.CopyOutReady(ctx, target, "COPY t TO STDOUT", options, nil)
		require.NoError(t, err)
		require.True(t, proto.Equal(state, reserved))
		var data []byte
		result, _, err = g.CopyOutStream(ctx, target, options, func(msg client.CopyOutMessage) error { data = append(data, msg.Data...); return nil })
		require.NoError(t, err)
		require.Equal(t, "one\n", string(data))
		require.Equal(t, "COPY 2", result.CommandTag)
		_, _, _, err = g.CopyReady(ctx, target, "COPY t FROM STDIN", options, nil)
		require.NoError(t, err)
		reserved, err = g.CopyAbort(ctx, target, "stop", options)
		require.NoError(t, err)
		require.True(t, proto.Equal(state, reserved))
		_, _, err = g.ExecuteQuery(ctx, target, "SELECT 1", options)
		require.NoError(t, err)
	}
	require.Empty(t, g.copyStreams)
	require.Equal(t, int32(6), calls.Load())
	require.Equal(t, int32(1), s.streams.Load(), "COPY and SQL should share one transport")
}

func TestReusableCopyEarlyErrorDrainsQueuedInput(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s := &operationTestServer{copy: func(stream pb.MultipoolerService_CopyBidiExecuteServer) error {
		if _, err := stream.Recv(); err != nil {
			return err
		}
		if err := stream.Send(&pb.CopyBidiExecuteResponse{Phase: pb.CopyBidiExecuteResponse_ERROR, Error: "bad input", ReservedState: &querypb.ReservedState{ReservedConnectionId: 7}, ErrorDiagnostic: &querypb.PgDiagnostic{Code: "23505", Message: "duplicate"}}); err != nil {
			return err
		}
		return status.Error(codes.InvalidArgument, "duplicate")
	}, call: func(context.Context, proto.Message) (proto.Message, error) { return &pb.ExecuteQueryResponse{}, nil }}
	g := queryServiceForOperations(t, s)
	stream, err := g.openCopy(ctx, &pb.CopyBidiExecuteRequest{Phase: pb.CopyBidiExecuteRequest_INITIATE})
	require.NoError(t, err)
	require.NoError(t, stream.Send(&pb.CopyBidiExecuteRequest{Phase: pb.CopyBidiExecuteRequest_DATA, Data: []byte("queued")}))
	require.NoError(t, stream.Send(&pb.CopyBidiExecuteRequest{Phase: pb.CopyBidiExecuteRequest_DONE}))
	response, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, uint64(7), response.GetReservedState().GetReservedConnectionId())
	var diagnostic *mterrors.PgDiagnostic
	require.ErrorAs(t, copyErrorFromResp(response), &diagnostic)
	_, _, err = g.ExecuteQuery(ctx, &querypb.Target{}, "SELECT 1", &querypb.ExecuteOptions{})
	require.NoError(t, err)
	require.Equal(t, int32(1), s.streams.Load())
}

func TestReusableCopyWaitsForCompletion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	finish := make(chan struct{})
	terminalSent := make(chan struct{})
	s := &operationTestServer{copy: func(stream pb.MultipoolerService_CopyBidiExecuteServer) error {
		if err := stream.Send(&pb.CopyBidiExecuteResponse{Phase: pb.CopyBidiExecuteResponse_RESULT}); err != nil {
			return err
		}
		close(terminalSent)
		select {
		case <-finish:
			return nil
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}}
	g := queryServiceForOperations(t, s)
	stream, err := g.openCopy(ctx, &pb.CopyBidiExecuteRequest{})
	require.NoError(t, err)
	result := make(chan error, 1)
	go func() { _, err := stream.Recv(); result <- err }()
	<-terminalSent
	select {
	case err := <-result:
		t.Fatalf("returned before handler completion: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(finish)
	require.NoError(t, <-result)
}

func TestReusableCopyMalformedCompletion(t *testing.T) {
	for _, scenario := range []string{"missing_terminal", "transport_eof", "extra_result", "wrong_operation"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			s := &operationTestServer{custom: func(stream pb.MultipoolerService_ExecuteStreamServer) error {
				if err := stream.Send(&pb.ExecuteStreamResponse{Ready: true, SupportedOperations: []pb.ExecuteStreamOperation{pb.ExecuteStreamOperation_COPY_BIDI_EXECUTE}}); err != nil {
					return err
				}
				if _, err := stream.Recv(); err != nil {
					return err
				}
				if scenario == "missing_terminal" {
					return stream.Send(&pb.ExecuteStreamResponse{Completion: &statuspb.Status{}})
				}
				if scenario == "wrong_operation" {
					return stream.Send(&pb.ExecuteStreamResponse{Result: &pb.ExecuteStreamResponse_ExecuteQuery{ExecuteQuery: &pb.ExecuteQueryResponse{}}})
				}
				frame := &pb.ExecuteStreamResponse{Result: &pb.ExecuteStreamResponse_CopyBidiExecute{CopyBidiExecute: &pb.CopyBidiExecuteResponse{Phase: pb.CopyBidiExecuteResponse_RESULT}}}
				if err := stream.Send(frame); err != nil {
					return err
				}
				if scenario == "extra_result" {
					return stream.Send(frame)
				}
				return nil
			}}
			g := queryServiceForOperations(t, s)
			stream, err := g.openCopy(ctx, &pb.CopyBidiExecuteRequest{})
			require.NoError(t, err)
			_, err = stream.Recv()
			require.Error(t, err)
			g.executeStreams.mu.Lock()
			idle := len(g.executeStreams.idle)
			g.executeStreams.mu.Unlock()
			require.Zero(t, idle)
		})
	}
}

func TestReusableCopyFallback(t *testing.T) {
	var calls atomic.Int32
	s := &operationTestServer{custom: func(stream pb.MultipoolerService_ExecuteStreamServer) error {
		if err := stream.Send(&pb.ExecuteStreamResponse{Ready: true, SupportedOperations: []pb.ExecuteStreamOperation{pb.ExecuteStreamOperation_STREAM_EXECUTE}}); err != nil {
			return err
		}
		_, err := stream.Recv()
		return err
	}, copy: func(stream pb.MultipoolerService_CopyBidiExecuteServer) error {
		calls.Add(1)
		if _, err := stream.Recv(); err != nil {
			return err
		}
		return stream.Send(&pb.CopyBidiExecuteResponse{Phase: pb.CopyBidiExecuteResponse_RESULT})
	}}
	g := queryServiceForOperations(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := g.openCopy(ctx, &pb.CopyBidiExecuteRequest{})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.NoError(t, err)
	require.NoError(t, stream.CloseSend())
	require.Equal(t, int32(1), calls.Load())
}

func TestReusableCopyAdmissionFailure(t *testing.T) {
	s := &operationTestServer{copy: func(pb.MultipoolerService_CopyBidiExecuteServer) error {
		return status.Error(codes.ResourceExhausted, "pool full")
	}, call: func(context.Context, proto.Message) (proto.Message, error) { return &pb.ExecuteQueryResponse{}, nil }}
	g := queryServiceForOperations(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _, _, err := g.CopyReady(ctx, &querypb.Target{}, "COPY t FROM STDIN", &querypb.ExecuteOptions{}, nil)
	require.ErrorContains(t, err, "pool full")
	_, _, err = g.ExecuteQuery(ctx, &querypb.Target{}, "SELECT 1", &querypb.ExecuteOptions{})
	require.NoError(t, err)
	require.Equal(t, int32(1), s.streams.Load())
}

func TestReusableCopyAbandonedOutput(t *testing.T) {
	exited := make(chan struct{})
	s := &operationTestServer{copy: func(stream pb.MultipoolerService_CopyBidiExecuteServer) error {
		defer close(exited)
		if err := stream.Send(&pb.CopyBidiExecuteResponse{Phase: pb.CopyBidiExecuteResponse_READY, ReservedState: &querypb.ReservedState{ReservedConnectionId: 1}}); err != nil {
			return err
		}
		if err := stream.Send(&pb.CopyBidiExecuteResponse{Phase: pb.CopyBidiExecuteResponse_DATA, Data: []byte("one")}); err != nil {
			return err
		}
		<-stream.Context().Done()
		return stream.Context().Err()
	}}
	g := queryServiceForOperations(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	options := &querypb.ExecuteOptions{ReservedConnectionId: 1}
	_, _, _, _, err := g.CopyOutReady(ctx, &querypb.Target{}, "COPY t TO STDOUT", options, nil)
	require.NoError(t, err)
	_, _, err = g.CopyOutStream(ctx, &querypb.Target{}, options, func(client.CopyOutMessage) error { return status.Error(codes.Canceled, "client disconnected") })
	require.ErrorContains(t, err, "client disconnected")
	select {
	case <-exited:
	case <-ctx.Done():
		t.Fatal("abandoned COPY handler did not stop")
	}
	require.Empty(t, g.copyStreams)
	g.executeStreams.mu.Lock()
	idle := len(g.executeStreams.idle)
	g.executeStreams.mu.Unlock()
	require.Zero(t, idle)
}

func TestReusableCopyCancellation(t *testing.T) {
	entered := make(chan struct{})
	exited := make(chan struct{})
	s := &operationTestServer{copy: func(stream pb.MultipoolerService_CopyBidiExecuteServer) error {
		defer close(exited)
		if _, err := stream.Recv(); err != nil {
			return err
		}
		close(entered)
		_, err := stream.Recv() // Waiting for input must be interrupted by cancellation.
		return err
	}}
	g := queryServiceForOperations(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := g.openCopy(ctx, &pb.CopyBidiExecuteRequest{})
	require.NoError(t, err)
	<-entered
	cancel()
	_, err = stream.Recv()
	require.Equal(t, codes.Canceled, status.Code(err))
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("COPY receive leaked after cancellation")
	}
	g.executeStreams.mu.Lock()
	idle := len(g.executeStreams.idle)
	g.executeStreams.mu.Unlock()
	require.Zero(t, idle)
}

func TestReusableCopyRejectsUnexpectedInput(t *testing.T) {
	s := &operationTestServer{copy: func(stream pb.MultipoolerService_CopyBidiExecuteServer) error {
		if _, err := stream.Recv(); err != nil {
			return err
		}
		_, err := stream.Recv()
		return err
	}}
	client, _ := connectStreamPool(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.ExecuteStream(ctx)
	require.NoError(t, err)
	_, err = stream.Recv()
	require.NoError(t, err)
	require.NoError(t, stream.Send(&pb.ExecuteStreamRequest{Operation: &pb.ExecuteStreamRequest_CopyBidiExecute{CopyBidiExecute: &pb.CopyBidiExecuteRequest{}}}))
	require.NoError(t, stream.Send(&pb.ExecuteStreamRequest{Operation: &pb.ExecuteStreamRequest_ExecuteQuery{ExecuteQuery: &pb.ExecuteQueryRequest{Query: "must not execute"}}}))
	_, err = stream.Recv()
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestReusableCopyMalformedResponseUnblocksSender(t *testing.T) {
	sending := make(chan struct{})
	s := &operationTestServer{copy: func(stream pb.MultipoolerService_CopyBidiExecuteServer) error {
		if _, err := stream.Recv(); err != nil {
			return err
		}
		<-sending
		if err := stream.Send(&pb.CopyBidiExecuteResponse{Phase: -1}); err != nil {
			return err
		}
		<-stream.Context().Done()
		return stream.Context().Err()
	}}
	g := queryServiceForOperations(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := g.openCopy(ctx, &pb.CopyBidiExecuteRequest{})
	require.NoError(t, err)
	sent := make(chan error, 1)
	go func() {
		close(sending)
		for {
			if err := stream.Send(&pb.CopyBidiExecuteRequest{Phase: pb.CopyBidiExecuteRequest_DATA, Data: make([]byte, 1024*1024)}); err != nil {
				sent <- err
				return
			}
		}
	}()
	_, err = stream.Recv()
	require.ErrorContains(t, err, "invalid COPY response phase")
	select {
	case <-sent:
	case <-ctx.Done():
		t.Fatal("sender remained blocked after malformed response")
	}
	require.NoError(t, ctx.Err(), "cleanup must not depend on the caller deadline")
}

func TestReusableCopyEarlyUploadFailureRetiresTransport(t *testing.T) {
	s := &operationTestServer{copy: func(stream pb.MultipoolerService_CopyBidiExecuteServer) error {
		if _, err := stream.Recv(); err != nil {
			return err
		}
		if err := stream.Send(&pb.CopyBidiExecuteResponse{Phase: pb.CopyBidiExecuteResponse_READY, ReservedState: &querypb.ReservedState{ReservedConnectionId: 9}}); err != nil {
			return err
		}
		if _, err := stream.Recv(); err != nil {
			return err
		}
		if err := stream.Send(&pb.CopyBidiExecuteResponse{Phase: pb.CopyBidiExecuteResponse_ERROR, Error: "backend failed", ReservedState: &querypb.ReservedState{ReservedConnectionId: 9}}); err != nil {
			return err
		}
		return status.Error(codes.Internal, "backend failed")
	}, call: func(context.Context, proto.Message) (proto.Message, error) { return &pb.ExecuteQueryResponse{}, nil }}
	g := queryServiceForOperations(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := g.openCopy(ctx, &pb.CopyBidiExecuteRequest{})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.NoError(t, err)
	// Do not read another response yet: an uploading client must be interrupted
	// even if it will only read the final result after sending all its data.
	for i := 0; ; i++ {
		err = stream.Send(&pb.CopyBidiExecuteRequest{Phase: pb.CopyBidiExecuteRequest_DATA, Data: make([]byte, 64*1024)})
		if err != nil {
			break
		}
		if i == 1000 {
			t.Fatal("continued accepting input after backend failure")
		}
	}
	response, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, "backend failed", response.Error)
	require.Equal(t, uint64(9), response.GetReservedState().GetReservedConnectionId())
	require.NoError(t, ctx.Err())
	_, _, err = g.ExecuteQuery(ctx, &querypb.Target{}, "SELECT 1", &querypb.ExecuteOptions{})
	require.NoError(t, err)
	require.Equal(t, int32(2), s.streams.Load(), "early upload failure must retire the stream")
}
