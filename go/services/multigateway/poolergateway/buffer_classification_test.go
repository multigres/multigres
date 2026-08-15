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
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/common/topoclient/poolerwatch"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/pb/multipoolerservice"
	querypb "github.com/multigres/multigres/go/pb/query"
)

// deadTCPAddr returns an address that deterministically refuses connections:
// a port the kernel just granted and that nothing listens on anymore. Dialing
// it reproduces the production stale-endpoint failure ("Error while dialing
// ... connect: connection refused") without any timing dependence.
func deadTCPAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	require.NoError(t, lis.Close())
	return addr
}

// queryServiceAt builds a grpcQueryService over a real (lazy) gRPC client
// connection to addr, mirroring how poolerConnection wires it in production.
func queryServiceAt(t *testing.T, addr string) queryservice.QueryService {
	t.Helper()
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })
	return newGRPCQueryService(cc, "test-pooler", slog.New(slog.DiscardHandler))
}

func noopStreamCallback(context.Context, *sqltypes.Result) error { return nil }

// TestGRPCQueryService_StreamStartDialFailureIsPreExecution pins the liveness
// half of the buffering boundary: a transport UNAVAILABLE surfaced by the
// stream constructor (NewStream/pick failure — the request message never left
// the gateway) must carry the pre-execution marker so classifyError buffers it
// during a failover instead of returning it to the client.
func TestGRPCQueryService_StreamStartDialFailureIsPreExecution(t *testing.T) {
	qs := queryServiceAt(t, deadTCPAddr(t))
	target := &querypb.Target{Mode: querypb.Mode_MODE_WRITABLE}

	_, err := qs.StreamExecute(t.Context(), target, "SELECT 1", nil, nil, noopStreamCallback)
	require.Error(t, err)
	assert.True(t, mterrors.IsPreExecutionUnavailable(err),
		"dial failure at stream start must be marked pre-execution, got: %v", err)

	_, err = qs.PortalStreamExecute(t.Context(), target, nil, &querypb.Portal{Name: "p0"},
		nil, nil, nil, noopStreamCallback)
	require.Error(t, err)
	assert.True(t, mterrors.IsPreExecutionUnavailable(err),
		"portal dial failure at stream start must be marked pre-execution, got: %v", err)
}

// TestIsClientDrivenFailure pins which errors are kept OUT of the
// failed_unbuffered alarm: cancellations and timeouts the client caused are
// expected during a failover and are not classification gaps, in both their
// raw-context and FromGRPC-synthesized (57014 query_canceled) shapes.
func TestIsClientDrivenFailure(t *testing.T) {
	assert.True(t, isClientDrivenFailure(context.Canceled))
	assert.True(t, isClientDrivenFailure(context.DeadlineExceeded))
	assert.True(t, isClientDrivenFailure(mterrors.FromGRPC(status.Error(codes.Canceled, "context canceled"))))
	assert.True(t, isClientDrivenFailure(mterrors.FromGRPC(status.Error(codes.DeadlineExceeded, "context deadline exceeded"))))

	assert.False(t, isClientDrivenFailure(mterrors.FromGRPC(status.Error(codes.Unavailable, "connection refused"))),
		"transport failures are exactly what the alarm exists for")
	assert.False(t, isClientDrivenFailure(mterrors.MTB01.New()))

	// A gateway without a buffer records nothing and must not panic.
	(&PoolerGateway{}).recordUnbufferedFailure(t.Context(), nil, errors.New("boom"))
}

// TestMarkStreamStartFailure_NonUnavailablePassesThrough pins that only
// UNAVAILABLE gets the pre-execution upgrade: any other status from stream
// creation passes through unmarked.
func TestMarkStreamStartFailure_NonUnavailablePassesThrough(t *testing.T) {
	err := markStreamStartFailure(status.Error(codes.Internal, "boom"))
	require.Error(t, err)
	assert.False(t, mterrors.IsPreExecutionUnavailable(err))
	assert.Equal(t, mtrpcpb.Code_INTERNAL, mterrors.Code(err))
}

// TestWithBuffering_UnbufferedFailurePassthrough exercises the two actionFail
// exits of withBuffering that feed the classification-gap alarm: a
// non-bufferable inner error on a leader-routed target, and a non-bufferable
// getConnection error on a replica-routed target. Both must surface to the
// caller unchanged.
func TestWithBuffering_UnbufferedFailurePassthrough(t *testing.T) {
	failoverBuffer := newTestFailoverBuffer(t, 10)
	lb := newTestLBWithLeaderServing(t, "zone1", failoverBuffer.StopBuffering)
	pg := &PoolerGateway{loadBalancer: lb, buffer: failoverBuffer, logger: slog.New(slog.DiscardHandler)}

	// Leader-routed inner failure: a plain query error is actionFail.
	primary := createTestMultipooler("primary", "zone1", constants.DefaultTableGroup,
		constants.DefaultShard, clustermetadatapb.PoolerType_PRIMARY)
	addPoolerForTest(t, lb, primary)
	simulateHealthUpdate(connForTest(t, lb, primary), clustermetadatapb.PoolerServingStatus_SERVING,
		primary.Id, &clustermetadatapb.RuleNumber{CoordinatorTerm: 1})
	target := protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup,
		constants.DefaultShard, querypb.Mode_MODE_WRITABLE)
	innerErr := errors.New("boom")
	err := pg.withBuffering(t.Context(), target, true, false, func(*poolerConnection) error { return innerErr })
	assert.ErrorIs(t, err, innerErr)

	// Replica-routed getConnection failure: no replica exists, and replica
	// traffic never buffers, so the UNAVAILABLE surfaces directly.
	replicaTarget := protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup,
		constants.DefaultShard, querypb.Mode_MODE_INCONSISTENT)
	err = pg.withBuffering(t.Context(), replicaTarget, true, false, func(*poolerConnection) error { return nil })
	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err))
}

// midStreamFailServer delivers one result frame and then fails the stream:
// the client-side error surfaces at Recv, after the statement may have
// started executing on the backend.
type midStreamFailServer struct {
	multipoolerservice.UnimplementedMultipoolerServiceServer
}

func (s *midStreamFailServer) StreamExecute(
	_ *multipoolerservice.StreamExecuteRequest,
	stream grpc.ServerStreamingServer[multipoolerservice.StreamExecuteResponse],
) error {
	if err := stream.Send(&multipoolerservice.StreamExecuteResponse{}); err != nil {
		return err
	}
	return status.Error(codes.Unavailable, "transport is closing")
}

// TestGRPCQueryService_MidStreamUnavailableIsNotPreExecution pins the safety
// half of the boundary: an UNAVAILABLE after the request reached the pooler
// must NOT be marked retry-safe — buffering and replaying it could
// double-apply a write.
func TestGRPCQueryService_MidStreamUnavailableIsNotPreExecution(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	multipoolerservice.RegisterMultipoolerServiceServer(srv, &midStreamFailServer{})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	qs := queryServiceAt(t, lis.Addr().String())
	_, err = qs.StreamExecute(t.Context(), &querypb.Target{Mode: querypb.Mode_MODE_WRITABLE},
		"SELECT 1", nil, nil, noopStreamCallback)
	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err))
	assert.False(t, mterrors.IsPreExecutionUnavailable(err),
		"an UNAVAILABLE after data has streamed must not be marked pre-execution, got: %v", err)
}

// promotedPoolerServer is a fake pooler that serves a controllable health
// stream (to drive leadership) and succeeds every StreamExecute immediately,
// so a drained retry against it completes.
type promotedPoolerServer struct {
	*controllableHealthServer
}

func (s *promotedPoolerServer) StreamExecute(
	_ *multipoolerservice.StreamExecuteRequest,
	_ grpc.ServerStreamingServer[multipoolerservice.StreamExecuteResponse],
) error {
	return nil
}

// newTestLBFrozenHealth is newTestLBWithLeaderServing with one twist: the
// pooler named frozenPooler gets no health-update callback, so nothing —
// including its own failing health stream — can ever retract its leader
// claim. This is the deterministic version of the production stale-endpoint
// window: the pod is dead, but the gateway has not yet observed the health
// stream error, so routing still targets the dead address.
func newTestLBFrozenHealth(t *testing.T, onLeaderServing func(*clustermetadatapb.ShardKey), frozenPooler string) *loadBalancer {
	t.Helper()
	logger := slog.New(slog.DiscardHandler)
	ctx := t.Context()
	dialOpt := grpc.WithTransportCredentials(insecure.NewCredentials())
	cache := poolerwatch.New(ctx, poolerwatch.Config[*poolerConnection]{
		Logger: logger,
	})
	lb := newLoadBalancer(loadBalancerOpts{
		Ctx:             ctx,
		LocalCell:       "zone1",
		Logger:          logger,
		DialOpt:         dialOpt,
		Cache:           cache,
		OnLeaderServing: onLeaderServing,
	})
	cache.Start(poolerwatch.Hooks[*poolerConnection]{
		OnLive: func(p *clustermetadatapb.Multipooler, _ *poolerConnection) *poolerConnection {
			cb := lb.onPoolerHealthUpdate
			if p.GetId().GetName() == frozenPooler {
				cb = nil
			}
			conn, err := newPoolerConnection(ctx, p, logger, dialOpt, cb)
			if err != nil {
				t.Errorf("newPoolerConnection failed: %v", err)
				return nil
			}
			return conn
		},
		OnUpdate: func(_, curr *clustermetadatapb.Multipooler, conn *poolerConnection) {
			if conn != nil {
				conn.UpdatePoolerInfo(curr)
			}
		},
		OnGone: func(p *clustermetadatapb.Multipooler, conn *poolerConnection, _ poolerwatch.GoneReason) {
			if conn != nil {
				_ = conn.Shutdown()
			}
			lb.onPoolerGone(p)
		},
	})
	t.Cleanup(func() { cache.Shutdown() })
	return lb
}

// TestPoolerGateway_BuffersDialFailureAgainstStalePrimary reproduces the
// field failure this boundary exists for: a failover leaves the gateway
// routing writes at a leader whose endpoint is already dead, and the
// resulting dial failure must join the failover buffer and drain against the
// promoted primary — not surface UNAVAILABLE to the client, which is exactly
// what happened before stream-start failures carried the pre-execution
// marker.
func TestPoolerGateway_BuffersDialFailureAgainstStalePrimary(t *testing.T) {
	failoverBuffer := newTestFailoverBuffer(t, 10)
	lb := newTestLBFrozenHealth(t, failoverBuffer.StopBuffering, "stale")
	pg := &PoolerGateway{loadBalancer: lb, buffer: failoverBuffer, logger: slog.New(slog.DiscardHandler)}
	target := protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup,
		constants.DefaultShard, querypb.Mode_MODE_WRITABLE)

	// The stale primary: still the routing leader, but its address refuses
	// connections.
	stale := createTestMultipooler("stale", "zone1", constants.DefaultTableGroup,
		constants.DefaultShard, clustermetadatapb.PoolerType_PRIMARY)
	deadHost, deadPort, err := net.SplitHostPort(deadTCPAddr(t))
	require.NoError(t, err)
	stale.Hostname = deadHost
	stalePort, err := net.LookupPort("tcp", deadPort)
	require.NoError(t, err)
	stale.PortMap["grpc"] = int32(stalePort)
	addPoolerForTest(t, lb, stale)
	setLeaderForTest(t, lb, constants.DefaultPostgresDatabase, constants.DefaultTableGroup,
		constants.DefaultShard, stale.Id, &clustermetadatapb.RuleNumber{CoordinatorTerm: 1})

	requestCtx, cancelRequest := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancelRequest()
	var attempts atomic.Int32
	done := make(chan error, 1)
	go func() {
		done <- pg.withBuffering(requestCtx, target, true, false, func(conn *poolerConnection) error {
			attempts.Add(1)
			_, err := conn.QueryService().StreamExecute(requestCtx, target, "SELECT 1", nil, nil, noopStreamCallback)
			return err
		})
	}()

	// The dial failure must arm the buffer. Before the fix, the request
	// returned UNAVAILABLE to the client right here.
	for {
		select {
		case err := <-done:
			t.Fatalf("request failed instead of buffering: %v", err)
		default:
		}
		probeCtx, cancelProbe := context.WithCancel(requestCtx)
		cancelProbe()
		_, err := failoverBuffer.WaitIfAlreadyBuffering(probeCtx, target.GetShardKey())
		if errors.Is(err, context.Canceled) {
			break
		}
		require.NoError(t, err)
		time.Sleep(time.Millisecond)
	}

	// Failover completes: a promoted pooler appears and its own live health
	// stream self-claims PRIMARY at a later term, which stops buffering and
	// drains the held request against the new leader.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	healthSrv := newControllableHealthServer()
	srv := grpc.NewServer()
	multipoolerservice.RegisterMultipoolerServiceServer(srv, &promotedPoolerServer{healthSrv})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	promoted := createTestMultipooler("promoted", "zone1", constants.DefaultTableGroup,
		constants.DefaultShard, clustermetadatapb.PoolerType_PRIMARY)
	promoted.Hostname = "127.0.0.1"
	promoted.PortMap["grpc"] = int32(lis.Addr().(*net.TCPAddr).Port)
	addPoolerForTest(t, lb, promoted)
	waitForStreamOpened(t, healthSrv)
	healthSrv.responseCh <- &multipoolerservice.StreamPoolerHealthResponse{
		PoolerId:      promoted.Id,
		ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
		RoutingState: &clustermetadatapb.RoutingState{
			Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY,
			Rule: &clustermetadatapb.RuleNumber{CoordinatorTerm: 2},
		},
	}

	select {
	case err := <-done:
		require.NoError(t, err, "buffered request must drain cleanly against the promoted primary")
	case <-time.After(8 * time.Second):
		t.Fatal("buffered request did not drain after promotion")
	}
	assert.GreaterOrEqual(t, attempts.Load(), int32(2),
		"the dial failure must have been retried after buffering, not surfaced")
}
