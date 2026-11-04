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
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/common/callerid"
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
	queryservice.Pooler
	executeQuery  func(ctx context.Context, sql string) (*sqltypes.Result, *querypb.ReservedState, error)
	streamExecute func(ctx context.Context, callback func(context.Context, *sqltypes.Result) error) (*querypb.ReservedState, error)
	getAuth       func(ctx context.Context) (*multipoolerservice.GetAuthCredentialsResponse, error)
	closed        bool
	replication   bool
}

func (f *fakeLocalQueryService) GetAuthCredentials(ctx context.Context, _ *multipoolerservice.GetAuthCredentialsRequest) (*multipoolerservice.GetAuthCredentialsResponse, error) {
	return f.getAuth(ctx)
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

func (f *fakeLocalQueryService) StreamReplication(context.Context, *multipoolerservice.StreamReplicationInit) (queryservice.ReplicationStream, error) {
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
	qs := newDirectQueryService(local)

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

	// Replication goes to the pooler directly; Close leaves the pooler open.
	_, err = qs.StreamReplication(t.Context(), &multipoolerservice.StreamReplicationInit{})
	require.NoError(t, err)
	assert.True(t, local.replication)
	require.NoError(t, qs.Close())
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

func (s *errorPoolerServer) GetAuthCredentials(ctx context.Context, req *multipoolerservice.GetAuthCredentialsRequest) (*multipoolerservice.GetAuthCredentialsResponse, error) {
	resp, err := s.local.(queryservice.Pooler).GetAuthCredentials(ctx, req)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return resp, nil
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
			_, _, directErr := newDirectQueryService(local).ExecuteQuery(ctx(), &querypb.Target{}, "select 1", nil)
			require.Error(t, directErr)

			assert.Equal(t, clientErrorOf(grpcErr), clientErrorOf(directErr), "gRPC: %v\ndirect: %v", grpcErr, directErr)
		})
	}
}

// TestDirectQueryService_AnnotatesCaller checks that a direct call puts the
// caller's identity on the current span, as the pooler's gRPC handler puts it
// on its operation span, so Minigres traces still attribute a query to its
// caller.
func TestDirectQueryService_AnnotatesCaller(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	local := &fakeLocalQueryService{executeQuery: func(context.Context, string) (*sqltypes.Result, *querypb.ReservedState, error) {
		return &sqltypes.Result{}, nil, nil
	}}
	qs := newDirectQueryService(local)

	ctx := callerid.NewContext(t.Context(), callerid.New("alice", "checkout-service"))
	ctx, span := tp.Tracer("test").Start(ctx, "Route.StreamExecute")
	_, _, err := qs.ExecuteQuery(ctx, &querypb.Target{}, "select 1", nil)
	require.NoError(t, err)
	span.End()

	spans := exporter.GetSpans()
	require.Len(t, spans, 1)
	assert.Contains(t, spans[0].Attributes, attribute.String(callerid.KeyAuthenticatedUser, "alice"))
	assert.Contains(t, spans[0].Attributes, attribute.String(callerid.KeyApplicationName, "checkout-service"))
}

// TestDirectQueryService_SameAuthErrorAsGRPC checks that each failure of the
// credential lookup gives the gateway the same error on the direct path as on
// the gRPC path, so its handling (buffering on unavailable, the client
// message) is the same.
func TestDirectQueryService_SameAuthErrorAsGRPC(t *testing.T) {
	tests := map[string]error{
		"invalid request":  mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "username is required"),
		"unavailable":      mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE, "failed to get admin connection: %v", errors.New("pool closed")),
		"not found":        mterrors.Errorf(mtrpcpb.Code_NOT_FOUND, "user %q not found", "u"),
		"login disabled":   mterrors.NewPgError("FATAL", mterrors.PgSSInvalidAuthSpec, `role "u" is not permitted to log in`, ""),
		"password expired": mterrors.NewPgError("FATAL", mterrors.PgSSAuthFailed, `password authentication failed for user "u"`, ""),
		"database error":   mterrors.Errorf(mtrpcpb.Code_INTERNAL, "failed to get role password: %v", errors.New("boom")),
		"planned failover": mterrors.MTF01.New(),
	}
	for name, lookupErr := range tests {
		t.Run(name, func(t *testing.T) {
			local := &fakeLocalQueryService{getAuth: func(context.Context) (*multipoolerservice.GetAuthCredentialsResponse, error) {
				return nil, lookupErr
			}}
			lis, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			server := grpc.NewServer()
			multipoolerservice.RegisterMultipoolerServiceServer(server, &errorPoolerServer{local: local})
			go func() { _ = server.Serve(lis) }()
			t.Cleanup(server.Stop)
			grpcQS := queryServiceAt(t, lis.Addr().String())
			req := &multipoolerservice.GetAuthCredentialsRequest{Username: "u", Database: "d"}

			_, grpcErr := grpcQS.GetAuthCredentials(t.Context(), req)
			require.Error(t, grpcErr)
			_, directErr := newDirectQueryService(local).GetAuthCredentials(t.Context(), req)
			require.Error(t, directErr)

			assert.Equal(t, clientErrorOf(grpcErr), clientErrorOf(directErr), "gRPC: %v\ndirect: %v", grpcErr, directErr)
		})
	}
}
