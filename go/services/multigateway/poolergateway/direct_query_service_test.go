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
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/queryrpc"
	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/sqltypes"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/pb/multipoolerservice"
	querypb "github.com/multigres/multigres/go/pb/query"
)

// fakeLocalQueryService stands in for the pooler's query service. Methods the
// tests do not use panic through the nil embedded interface.
type fakeLocalQueryService struct {
	queryservice.QueryService
	executeQuery  func(ctx context.Context, sql string) (*sqltypes.Result, *querypb.ReservedState, error)
	streamExecute func(ctx context.Context, callback func(context.Context, *sqltypes.Result) error) (*querypb.ReservedState, error)
	closed        bool
	replication   bool
}

func (f *fakeLocalQueryService) ExecuteQuery(ctx context.Context, _ *querypb.Target, sql string, _ *querypb.ExecuteOptions) (*sqltypes.Result, *querypb.ReservedState, error) {
	return f.executeQuery(ctx, sql)
}

func (f *fakeLocalQueryService) StreamExecute(ctx context.Context, _ *querypb.Target, _ string, _ *querypb.ExecuteOptions, _ *querypb.ReservationOptions, callback func(context.Context, *sqltypes.Result) error) (*querypb.ReservedState, error) {
	return f.streamExecute(ctx, callback)
}

func (f *fakeLocalQueryService) Close() error {
	f.closed = true
	return nil
}

func (f *fakeLocalQueryService) StreamReplication(context.Context, *multipoolerservice.StreamReplicationInit) (multipoolerservice.MultipoolerService_StreamReplicationClient, error) {
	f.replication = true
	return nil, nil
}

func TestDirectQueryService_PassesThrough(t *testing.T) {
	want := &sqltypes.Result{Rows: []*sqltypes.Row{{Values: []sqltypes.Value{[]byte("1")}}}}
	local := &fakeLocalQueryService{
		executeQuery: func(context.Context, string) (*sqltypes.Result, *querypb.ReservedState, error) {
			return want, &querypb.ReservedState{ReservedConnectionId: 7}, nil
		},
		streamExecute: func(ctx context.Context, callback func(context.Context, *sqltypes.Result) error) (*querypb.ReservedState, error) {
			if err := callback(ctx, want); err != nil {
				return nil, err
			}
			return &querypb.ReservedState{ReservedConnectionId: 8}, nil
		},
	}
	grpcQS := &fakeLocalQueryService{}
	qs := newDirectQueryService(local, grpcQS)

	result, state, err := qs.ExecuteQuery(t.Context(), &querypb.Target{}, "select 1", nil)
	require.NoError(t, err)
	assert.Same(t, want, result)
	assert.EqualValues(t, 7, state.GetReservedConnectionId())

	var got []*sqltypes.Result
	state, err = qs.StreamExecute(t.Context(), &querypb.Target{}, "select 1", nil, nil, func(_ context.Context, r *sqltypes.Result) error {
		got = append(got, r)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, []*sqltypes.Result{want}, got)
	assert.EqualValues(t, 8, state.GetReservedConnectionId())

	// A callback error comes back as the callback returned it.
	callbackErr := errors.New("client went away")
	_, err = qs.StreamExecute(t.Context(), &querypb.Target{}, "select 1", nil, nil, func(context.Context, *sqltypes.Result) error {
		return callbackErr
	})
	assert.ErrorIs(t, err, callbackErr)

	// Replication and Close go to the gRPC query service, not the pooler's.
	_, err = qs.StreamReplication(t.Context(), &multipoolerservice.StreamReplicationInit{})
	require.NoError(t, err)
	assert.True(t, grpcQS.replication)
	require.NoError(t, qs.Close())
	assert.True(t, grpcQS.closed)
	assert.False(t, local.closed)
}

// errorPoolerServer serves ExecuteQuery from local over gRPC, converting its
// error as the pooler's gRPC handler does.
type errorPoolerServer struct {
	multipoolerservice.UnimplementedMultipoolerServiceServer
	local queryservice.QueryService
}

func (s *errorPoolerServer) ExecuteStream(stream multipoolerservice.MultipoolerService_ExecuteStreamServer) error {
	return queryrpc.Serve(stream, s)
}

func (s *errorPoolerServer) ExecuteQuery(ctx context.Context, req *multipoolerservice.ExecuteQueryRequest) (*multipoolerservice.ExecuteQueryResponse, error) {
	res, state, err := s.local.ExecuteQuery(ctx, req.Target, req.Query, req.Options)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolerservice.ExecuteQueryResponse{Result: res.ToProto(), ReservedState: state}, nil
}

// clientError is what the client sees of err: its code, and the diagnostic
// the gateway sends when there is one.
type clientError struct {
	code     mtrpcpb.Code
	sqlState string
	severity string
	message  string
}

func clientErrorOf(err error) clientError {
	ce := clientError{code: mterrors.Code(err)}
	var diag *mterrors.PgDiagnostic
	if errors.As(err, &diag) {
		ce.sqlState, ce.severity, ce.message = diag.SQLSTATE(), diag.Severity, diag.Message
	}
	return ce
}

// TestDirectQueryService_SameClientErrorAsGRPC checks that a failure gives the
// client the same error on the direct path as on the gRPC path.
func TestDirectQueryService_SameClientErrorAsGRPC(t *testing.T) {
	waitForCtx := func(ctx context.Context, _ string) (*sqltypes.Result, *querypb.ReservedState, error) {
		<-ctx.Done()
		return nil, nil, ctx.Err()
	}
	tests := []struct {
		name    string
		execute func(ctx context.Context, sql string) (*sqltypes.Result, *querypb.ReservedState, error)
		ctx     func(t *testing.T) context.Context
	}{
		{
			name: "statement timeout",
			ctx: func(t *testing.T) context.Context {
				ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
				t.Cleanup(cancel)
				return ctx
			},
			execute: waitForCtx,
		},
		{
			name: "cancellation",
			ctx: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(t.Context())
				time.AfterFunc(50*time.Millisecond, cancel)
				return ctx
			},
			execute: waitForCtx,
		},
		{
			name: "planned failover",
			execute: func(context.Context, string) (*sqltypes.Result, *querypb.ReservedState, error) {
				return nil, nil, mterrors.MTF01.New()
			},
		},
		{
			name: "postgres error",
			execute: func(context.Context, string) (*sqltypes.Result, *querypb.ReservedState, error) {
				return nil, nil, mterrors.NewPgError("ERROR", "42P01", `relation "missing" does not exist`, "")
			},
		},
		{
			name: "plain error",
			execute: func(context.Context, string) (*sqltypes.Result, *querypb.ReservedState, error) {
				return nil, nil, mterrors.New(mtrpcpb.Code_INTERNAL, "something broke")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			local := &fakeLocalQueryService{executeQuery: tt.execute}
			ctx := func() context.Context {
				if tt.ctx == nil {
					return t.Context()
				}
				return tt.ctx(t)
			}

			lis, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			server := grpc.NewServer()
			multipoolerservice.RegisterMultipoolerServiceServer(server, &errorPoolerServer{local: local})
			go func() { _ = server.Serve(lis) }()
			t.Cleanup(server.Stop)
			grpcQS := queryServiceAt(t, lis.Addr().String())

			_, _, grpcErr := grpcQS.ExecuteQuery(ctx(), &querypb.Target{}, "select 1", nil)
			require.Error(t, grpcErr)
			_, _, directErr := newDirectQueryService(local, grpcQS).ExecuteQuery(ctx(), &querypb.Target{}, "select 1", nil)
			require.Error(t, directErr)

			assert.Equal(t, clientErrorOf(grpcErr), clientErrorOf(directErr), "gRPC: %v\ndirect: %v", grpcErr, directErr)
		})
	}
}
