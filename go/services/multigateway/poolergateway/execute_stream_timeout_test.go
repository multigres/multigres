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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/queryrpc"
	"github.com/multigres/multigres/go/common/sqltypes"
	pb "github.com/multigres/multigres/go/pb/multipoolerservice"
	querypb "github.com/multigres/multigres/go/pb/query"
)

func TestOperationTimeoutWaitsForCleanup(t *testing.T) {
	requests := []proto.Message{
		&pb.StreamExecuteRequest{}, &pb.PortalStreamExecuteRequest{}, &pb.ExecuteQueryRequest{},
		&pb.DescribeRequest{}, &pb.ConcludeTransactionRequest{}, &pb.DiscardTempTablesRequest{},
		&pb.ReleaseReservedConnectionRequest{},
	}
	for _, request := range requests {
		t.Run(string(request.ProtoReflect().Descriptor().Name()), func(t *testing.T) {
			cancelled, cleanup := make(chan struct{}), make(chan struct{})
			defer close(cleanup)
			g := queryServiceForOperations(t, &operationTestServer{call: func(ctx context.Context, _ proto.Message) (proto.Message, error) {
				<-ctx.Done()
				close(cancelled)
				<-cleanup
				return nil, status.FromContextError(ctx.Err()).Err()
			}})
			ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
			defer cancel()
			r, used, err := g.executeStreams.open(ctx, queryrpc.Request(request))
			require.NoError(t, err)
			require.True(t, used)
			defer r.Release()
			result := make(chan error, 1)
			go func() { _, err := r.recv(); result <- err }()
			select {
			case <-cancelled:
			case <-time.After(time.Second):
				t.Fatal("backend not cancelled")
			}
			select {
			case err := <-result:
				t.Fatalf("returned before cleanup: %v", err)
			case <-time.After(10 * time.Millisecond):
			}
			require.NoError(t, r.lease.stream.Context().Err(), "deadline killed the acknowledgement transport")
			cleanup <- struct{}{}
			select {
			case err := <-result:
				require.Equal(t, codes.DeadlineExceeded, status.Code(err))
				require.True(t, r.done)
			case <-time.After(time.Second):
				t.Fatal("completion not delivered")
			}
		})
	}
}

func TestOperationTimeoutWithoutCompletionIsFatal(t *testing.T) {
	cleanup := make(chan struct{})
	defer close(cleanup)
	g := queryServiceForOperations(t, &operationTestServer{call: func(ctx context.Context, _ proto.Message) (proto.Message, error) {
		<-ctx.Done()
		<-cleanup
		return nil, ctx.Err()
	}})
	g.executeStreams.cleanupGrace = 20 * time.Millisecond
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
	defer cancel()
	_, _, err := g.ExecuteQuery(ctx, &querypb.Target{}, "SELECT 1", &querypb.ExecuteOptions{})
	var diagnostic *mterrors.PgDiagnostic
	require.ErrorAs(t, err, &diagnostic)
	require.Equal(t, "FATAL", diagnostic.Severity)
	require.Equal(t, mterrors.PgSSConnectionFailure, diagnostic.Code)
	require.Empty(t, g.executeStreams.idle)
}

func TestStreamingCallbackTimeoutWaitsForCleanup(t *testing.T) {
	for _, portal := range []bool{false, true} {
		name := "sql"
		if portal {
			name = "portal"
		}
		t.Run(name, func(t *testing.T) {
			entered, cleanup := make(chan struct{}), make(chan struct{})
			defer close(cleanup)
			g := queryServiceForOperations(t, &operationTestServer{custom: func(stream pb.MultipoolerService_ExecuteStreamServer) error {
				if err := stream.Send(&pb.ExecuteStreamResponse{Ready: true, SupportedOperations: []pb.ExecuteStreamOperation{pb.ExecuteStreamOperation_STREAM_EXECUTE, pb.ExecuteStreamOperation_PORTAL_STREAM_EXECUTE}}); err != nil {
					return err
				}
				if _, err := stream.Recv(); err != nil {
					return err
				}
				payload := &querypb.QueryResultPayload{Payload: &querypb.QueryResultPayload_Result{Result: &querypb.QueryResult{}}}
				frame := &pb.ExecuteStreamResponse{}
				if portal {
					frame.Result = &pb.ExecuteStreamResponse_PortalStreamExecute{PortalStreamExecute: &pb.PortalStreamExecuteResponse{Result: payload}}
				} else {
					frame.Result = &pb.ExecuteStreamResponse_Response{Response: &pb.StreamExecuteResponse{Result: payload}}
				}
				if err := stream.Send(frame); err != nil {
					return err
				}
				<-cleanup
				return stream.Send(&pb.ExecuteStreamResponse{Completion: status.New(codes.DeadlineExceeded, "timeout").Proto()})
			}})
			ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
			defer cancel()
			result := make(chan error, 1)
			callback := func(ctx context.Context, _ *sqltypes.Result) error { close(entered); <-ctx.Done(); return ctx.Err() }
			go func() {
				var err error
				if portal {
					_, err = g.PortalStreamExecute(ctx, &querypb.Target{}, &querypb.PreparedStatement{}, &querypb.Portal{}, &querypb.ExecuteOptions{}, nil, nil, callback)
				} else {
					_, err = g.StreamExecute(ctx, &querypb.Target{}, "SELECT 1", &querypb.ExecuteOptions{}, nil, callback)
				}
				result <- err
			}()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("callback not entered")
			}
			<-ctx.Done()
			select {
			case err := <-result:
				t.Fatalf("callback bypassed cleanup: %v", err)
			case <-time.After(10 * time.Millisecond):
			}
			cleanup <- struct{}{}
			select {
			case err := <-result:
				var diagnostic *mterrors.PgDiagnostic
				require.ErrorAs(t, err, &diagnostic)
				require.Equal(t, "ERROR", diagnostic.Severity)
				require.Contains(t, diagnostic.Message, "statement timeout")
			case <-time.After(time.Second):
				t.Fatal("callback cleanup did not complete")
			}
		})
	}
}

func TestLegacyTimeoutIsFatal(t *testing.T) {
	g := queryServiceForOperations(t, &operationTestServer{call: func(ctx context.Context, _ proto.Message) (proto.Message, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}})
	g.executeStreams = nil
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
	defer cancel()
	_, _, err := g.ExecuteQuery(ctx, &querypb.Target{}, "SELECT 1", &querypb.ExecuteOptions{})
	var diagnostic *mterrors.PgDiagnostic
	require.ErrorAs(t, err, &diagnostic)
	require.Equal(t, "FATAL", diagnostic.Severity)
}

func TestCopyTimeoutInterruptsInputAndWaitsForCleanup(t *testing.T) {
	cancelled, cleanup := make(chan struct{}), make(chan struct{})
	defer close(cleanup)
	g := queryServiceForCopy(t, &operationTestServer{copy: func(stream pb.MultipoolerService_CopyBidiExecuteServer) error {
		if _, err := stream.Recv(); err != nil {
			return err
		}
		if err := stream.Send(&pb.CopyBidiExecuteResponse{Phase: pb.CopyBidiExecuteResponse_READY, ReservedState: &querypb.ReservedState{ReservedConnectionId: 42}}); err != nil {
			return err
		}
		_, err := stream.Recv()
		close(cancelled)
		<-cleanup
		if sendErr := stream.Send(&pb.CopyBidiExecuteResponse{Phase: pb.CopyBidiExecuteResponse_ERROR, Error: "aborted", ReservedState: &querypb.ReservedState{ReservedConnectionId: 42}}); sendErr != nil {
			return sendErr
		}
		return err
	}})
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
	defer cancel()
	stream, err := g.openCopy(ctx, &pb.CopyBidiExecuteRequest{})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.NoError(t, err)
	result := make(chan *pb.CopyBidiExecuteResponse, 1)
	failures := make(chan error, 1)
	go func() { response, err := stream.Recv(); result <- response; failures <- err }()
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("COPY input read ignored the operation deadline")
	}
	select {
	case <-result:
		t.Fatal("COPY returned before cleanup")
	case <-time.After(10 * time.Millisecond):
	}
	cleanup <- struct{}{}
	select {
	case response := <-result:
		require.NoError(t, <-failures)
		require.Equal(t, uint64(42), response.GetReservedState().GetReservedConnectionId())
		require.Contains(t, response.ErrorDiagnostic.Message, "statement timeout")
	case <-time.After(time.Second):
		t.Fatal("COPY completion not received")
	}
}

func TestCopyOutputCallbackTimeoutWaitsForCleanup(t *testing.T) {
	cleanup := make(chan struct{})
	defer close(cleanup)
	g := queryServiceForCopy(t, &operationTestServer{copy: func(stream pb.MultipoolerService_CopyBidiExecuteServer) error {
		state := &querypb.ReservedState{ReservedConnectionId: 42}
		if err := stream.Send(&pb.CopyBidiExecuteResponse{Phase: pb.CopyBidiExecuteResponse_READY, ReservedState: state}); err != nil {
			return err
		}
		if err := stream.Send(&pb.CopyBidiExecuteResponse{Phase: pb.CopyBidiExecuteResponse_DATA, Data: []byte("row")}); err != nil {
			return err
		}
		<-stream.Context().Done()
		<-cleanup
		if err := stream.Send(&pb.CopyBidiExecuteResponse{Phase: pb.CopyBidiExecuteResponse_ERROR, Error: "cancelled", ReservedState: state}); err != nil {
			return err
		}
		return stream.Context().Err()
	}})
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
	defer cancel()
	target, options := &querypb.Target{}, &querypb.ExecuteOptions{ReservedConnectionId: 42}
	_, _, _, _, err := g.CopyOutReady(ctx, target, "COPY t TO STDOUT", options, nil)
	require.NoError(t, err)
	entered := make(chan struct{})
	result := make(chan error, 1)
	states := make(chan *querypb.ReservedState, 1)
	go func() {
		_, state, err := g.CopyOutStream(ctx, target, options, func(client.CopyOutMessage) error { close(entered); <-ctx.Done(); return ctx.Err() })
		states <- state
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("COPY callback not entered")
	}
	<-ctx.Done()
	select {
	case err := <-result:
		t.Fatalf("COPY callback bypassed cleanup: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	cleanup <- struct{}{}
	select {
	case err := <-result:
		require.Equal(t, uint64(42), (<-states).GetReservedConnectionId())
		var diagnostic *mterrors.PgDiagnostic
		require.ErrorAs(t, err, &diagnostic)
		require.Equal(t, "ERROR", diagnostic.Severity)
		require.Contains(t, diagnostic.Message, "statement timeout")
	case <-time.After(time.Second):
		t.Fatal("COPY did not finish cleanup")
	}
}

func TestCopyMissingCompletionRemainsFatal(t *testing.T) {
	cleanup := make(chan struct{})
	defer close(cleanup)
	g := queryServiceForCopy(t, &operationTestServer{copy: func(stream pb.MultipoolerService_CopyBidiExecuteServer) error {
		if err := stream.Send(&pb.CopyBidiExecuteResponse{Phase: pb.CopyBidiExecuteResponse_READY, ReservedState: &querypb.ReservedState{ReservedConnectionId: 42}}); err != nil {
			return err
		}
		<-stream.Context().Done()
		<-cleanup
		return stream.Context().Err()
	}})
	g.executeStreams.cleanupGrace = 20 * time.Millisecond
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
	defer cancel()
	target, options := &querypb.Target{}, &querypb.ExecuteOptions{ReservedConnectionId: 42}
	_, _, _, _, err := g.CopyOutReady(ctx, target, "COPY t TO STDOUT", options, nil)
	require.NoError(t, err)
	_, _, err = g.CopyOutStream(ctx, target, options, func(client.CopyOutMessage) error { return nil })
	var diagnostic *mterrors.PgDiagnostic
	require.ErrorAs(t, err, &diagnostic)
	require.Equal(t, "FATAL", diagnostic.Severity)
}

func TestStreamingCallbackErrorIdentity(t *testing.T) {
	for _, portal := range []bool{false, true} {
		name := "sql"
		if portal {
			name = "portal"
		}
		t.Run(name, func(t *testing.T) {
			g := queryServiceForOperations(t, &operationTestServer{call: func(context.Context, proto.Message) (proto.Message, error) {
				payload := &querypb.QueryResultPayload{Payload: &querypb.QueryResultPayload_Result{Result: &querypb.QueryResult{}}}
				if portal {
					return &pb.PortalStreamExecuteResponse{Result: payload}, nil
				}
				return &pb.StreamExecuteResponse{Result: payload}, nil
			}})
			callbackErr := errors.New("callback stopped")
			callback := func(context.Context, *sqltypes.Result) error { return callbackErr }
			var err error
			if portal {
				_, err = g.PortalStreamExecute(t.Context(), &querypb.Target{}, &querypb.PreparedStatement{}, &querypb.Portal{}, &querypb.ExecuteOptions{}, nil, nil, callback)
			} else {
				_, err = g.StreamExecute(t.Context(), &querypb.Target{}, "SELECT 1", &querypb.ExecuteOptions{}, nil, callback)
			}
			require.ErrorIs(t, err, callbackErr)
		})
	}
}
