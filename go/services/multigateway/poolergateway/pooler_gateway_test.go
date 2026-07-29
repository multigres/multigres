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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	pgclient "github.com/multigres/multigres/go/common/pgprotocol/client"
	pgserver "github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/queryservice"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/auth"
	gatewaybuffer "github.com/multigres/multigres/go/services/multigateway/buffer"
	gatewayhandler "github.com/multigres/multigres/go/services/multigateway/handler"
	"github.com/multigres/multigres/go/tools/viperutil"
)

func TestTranslatePreExecutionUnavailable(t *testing.T) {
	original := mterrors.NewPgError("ERROR", "57P01", "terminating connection due to administrator command", "")
	translated := translatePreExecutionUnavailable(mterrors.MarkPreExecutionUnavailable(original))

	assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(translated))
	var diagnostic *mterrors.PgDiagnostic
	require.ErrorAs(t, translated, &diagnostic)
	assert.Same(t, original, diagnostic)
}

func TestClassifyError(t *testing.T) {
	primaryTarget := &query.Target{Mode: query.Mode_MODE_WRITABLE}
	replicaTarget := &query.Target{Mode: query.Mode_MODE_INCONSISTENT}

	tests := []struct {
		name               string
		err                error
		target             *query.Target
		retryReadOnlyError bool
		want               errorAction
	}{
		{
			name:   "MTF01 on PRIMARY triggers buffering",
			err:    mterrors.MTF01.New(),
			target: primaryTarget,
			want:   actionBuffer,
		},
		{
			name:   "MTF01 on REPLICA does not buffer",
			err:    mterrors.MTF01.New(),
			target: replicaTarget,
			want:   actionFail,
		},
		{
			name:   "pre-execution failure on PRIMARY triggers buffering",
			err:    mterrors.MarkPreExecutionUnavailable(errors.New("connection refused")),
			target: primaryTarget,
			want:   actionBuffer,
		},
		{
			name:   "pre-execution failure on REPLICA does not buffer",
			err:    mterrors.MarkPreExecutionUnavailable(errors.New("connection refused")),
			target: replicaTarget,
			want:   actionFail,
		},
		{
			name:   "no writable primary buffers leader traffic",
			err:    newNoWritablePrimaryError("no leader observed"),
			target: primaryTarget,
			want:   actionBuffer,
		},
		{
			name:   "no writable primary does not buffer replica traffic",
			err:    newNoWritablePrimaryError("no leader observed"),
			target: replicaTarget,
			want:   actionFail,
		},
		{
			name:   "generic error on PRIMARY does not buffer",
			err:    errors.New("connection refused"),
			target: primaryTarget,
			want:   actionFail,
		},
		{
			name:   "nil error on PRIMARY does not buffer",
			err:    nil,
			target: primaryTarget,
			want:   actionFail,
		},
		{
			name:               "read_only_sql_transaction on retryable PRIMARY triggers buffering",
			err:                mterrors.NewPgError("ERROR", mterrors.PgSSReadOnlyTransaction, "cannot execute INSERT in a read-only transaction", ""),
			target:             primaryTarget,
			retryReadOnlyError: true,
			want:               actionBuffer,
		},
		{
			name:   "read_only_sql_transaction on stateful PRIMARY does not buffer",
			err:    mterrors.NewPgError("ERROR", mterrors.PgSSReadOnlyTransaction, "cannot execute INSERT in a read-only transaction", ""),
			target: primaryTarget,
			want:   actionFail,
		},
		{
			name:               "read_only_sql_transaction on REPLICA does not buffer",
			err:                mterrors.NewPgError("ERROR", mterrors.PgSSReadOnlyTransaction, "cannot execute INSERT in a read-only transaction", ""),
			target:             replicaTarget,
			retryReadOnlyError: true,
			want:               actionFail,
		},
		{
			name:               "BEGIN READ WRITE recovery rejection on retryable PRIMARY triggers buffering",
			err:                mterrors.NewPgError("ERROR", mterrors.PgSSFeatureNotSupported, "cannot set transaction read-write mode during recovery", ""),
			target:             primaryTarget,
			retryReadOnlyError: true,
			want:               actionBuffer,
		},
		{
			name:   "BEGIN READ WRITE recovery rejection on established transaction does not buffer",
			err:    mterrors.NewPgError("ERROR", mterrors.PgSSFeatureNotSupported, "cannot set transaction read-write mode during recovery", ""),
			target: primaryTarget,
			want:   actionFail,
		},
		{
			name:               "unrelated 0A000 on PRIMARY does not buffer",
			err:                mterrors.NewPgError("ERROR", mterrors.PgSSFeatureNotSupported, "cached plan must not change result type", ""),
			target:             primaryTarget,
			retryReadOnlyError: true,
			want:               actionFail,
		},
		{
			name:   "other MT error on PRIMARY does not buffer",
			err:    mterrors.MTB01.New(),
			target: primaryTarget,
			want:   actionFail,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyError(tt.err, tt.target, tt.retryReadOnlyError)
			assert.Equal(t, tt.want, got)
		})
	}
}

func newTestFailoverBuffer(t *testing.T, size int) *gatewaybuffer.Buffer {
	t.Helper()
	config := gatewaybuffer.NewConfig(viperutil.NewRegistry())
	config.Enabled.Set(true)
	config.Window.Set(5 * time.Second)
	config.Size.Set(size)
	config.MaxFailoverDuration.Set(5 * time.Second)
	config.MinTimeBetweenFailovers.Set(0)
	config.DrainConcurrency.Set(1)
	failoverBuffer := gatewaybuffer.New(t.Context(), config, slog.New(slog.DiscardHandler))
	t.Cleanup(failoverBuffer.Shutdown)
	return failoverBuffer
}

func TestPoolerGateway_QuietGatewayBuffersUntilPrimaryAppears(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	failoverBuffer := newTestFailoverBuffer(t, 10)

	lb := newTestLBWithLeaderServing(t, "zone1", failoverBuffer.StopBuffering)
	pg := &PoolerGateway{loadBalancer: lb, buffer: failoverBuffer, logger: logger}
	target := protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup, constants.DefaultShard, query.Mode_MODE_WRITABLE)

	requestCtx, cancelRequest := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelRequest()
	selected := make(chan *poolerConnection, 1)
	done := make(chan error, 1)
	go func() {
		done <- pg.withBuffering(requestCtx, target, true, false, func(conn *poolerConnection) error {
			selected <- conn
			return nil
		})
	}()

	// This gateway saw no query during the drain. Its first arrival must arm the
	// buffer on the load balancer's no-primary result instead of failing fast.
	for {
		select {
		case err := <-done:
			t.Fatalf("request returned before a primary appeared: %v", err)
		default:
		}
		probeCtx, cancel := context.WithCancel(requestCtx)
		cancel()
		_, err := failoverBuffer.WaitIfAlreadyBuffering(probeCtx, target.GetShardKey())
		if errors.Is(err, context.Canceled) {
			break
		}
		require.NoError(t, err)
		time.Sleep(time.Millisecond)
	}

	primary := createTestMultipooler("primary", "zone1", constants.DefaultTableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_PRIMARY)
	addPoolerForTest(t, lb, primary)
	simulateHealthUpdate(connForTest(t, lb, primary), clustermetadatapb.PoolerServingStatus_SERVING,
		primary.Id, &clustermetadatapb.RuleNumber{CoordinatorTerm: 1})

	require.NoError(t, <-done)
	assert.Equal(t, poolerID(primary), (<-selected).ID())
}

func TestPoolerGateway_TopologyUpdateDoesNotDrainStaleHealth(t *testing.T) {
	failoverBuffer := newTestFailoverBuffer(t, 10)

	lb := newTestLBWithLeaderServing(t, "zone1", failoverBuffer.StopBuffering)
	oldPrimary := createTestMultipooler("old", "zone1", constants.DefaultTableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_PRIMARY)
	addPoolerForTest(t, lb, oldPrimary)
	simulateHealthUpdate(connForTest(t, lb, oldPrimary), clustermetadatapb.PoolerServingStatus_SERVING,
		oldPrimary.Id, &clustermetadatapb.RuleNumber{CoordinatorTerm: 1})

	type waitResult struct {
		retryDone gatewaybuffer.RetryDoneFunc
		err       error
	}
	result := make(chan waitResult, 1)
	go func() {
		retryDone, err := failoverBuffer.WaitForFailoverEnd(t.Context(), oldPrimary.GetShardKey())
		result <- waitResult{retryDone: retryDone, err: err}
	}()
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := failoverBuffer.WaitIfAlreadyBuffering(ctx, oldPrimary.GetShardKey())
		return errors.Is(err, context.Canceled)
	}, time.Second, time.Millisecond)

	// Topology changes before the corresponding health update. Re-evaluating the
	// old PRIMARY/SERVING health here would drain toward the restarted standby.
	updated := createTestMultipooler("old", "zone1", constants.DefaultTableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_REPLICA)
	addPoolerForTest(t, lb, updated)
	select {
	case got := <-result:
		t.Fatalf("topology-only update drained buffer: %v", got.err)
	case <-time.After(20 * time.Millisecond):
	}

	newPrimary := createTestMultipooler("new", "zone1", constants.DefaultTableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_PRIMARY)
	addPoolerForTest(t, lb, newPrimary)
	simulateHealthUpdate(connForTest(t, lb, newPrimary), clustermetadatapb.PoolerServingStatus_SERVING,
		newPrimary.Id, &clustermetadatapb.RuleNumber{CoordinatorTerm: 2})

	got := <-result
	require.NoError(t, got.err)
	require.NotNil(t, got.retryDone)
	got.retryDone()
}

func TestPoolerGateway_FailedDrainRearmsBuffer(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	// One request fills the buffer, exercising slot release before re-arm.
	failoverBuffer := newTestFailoverBuffer(t, 1)

	lb := newTestLBWithLeaderServing(t, "zone1", failoverBuffer.StopBuffering)
	oldPrimary := createTestMultipooler("old", "zone1", constants.DefaultTableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_PRIMARY)
	addPoolerForTest(t, lb, oldPrimary)
	simulateHealthUpdate(connForTest(t, lb, oldPrimary), clustermetadatapb.PoolerServingStatus_SERVING,
		oldPrimary.Id, &clustermetadatapb.RuleNumber{CoordinatorTerm: 1})

	pg := &PoolerGateway{loadBalancer: lb, buffer: failoverBuffer, logger: logger}
	target := protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup, constants.DefaultShard, query.Mode_MODE_WRITABLE)
	attempted := make(chan string, constants.MaxBufferingRetries+1)
	done := make(chan error, 1)
	go func() {
		done <- pg.withBuffering(t.Context(), target, true, true, func(conn *poolerConnection) error {
			name := conn.PoolerInfo().GetId().GetName()
			attempted <- name
			if name == "old" {
				return mterrors.NewPgError("ERROR", mterrors.PgSSReadOnlyTransaction, "cannot execute INSERT in a read-only transaction", "")
			}
			return nil
		})
	}()

	require.Equal(t, "old", <-attempted)

	// Simulate a stale PRIMARY/SERVING observation. StopBuffering is retried until
	// the first error has armed the buffer, then the drained retry reaches old.
	var secondAttempt string
	require.Eventually(t, func() bool {
		failoverBuffer.StopBuffering(target.GetShardKey())
		select {
		case secondAttempt = <-attempted:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
	require.Equal(t, "old", secondAttempt)

	newPrimary := createTestMultipooler("new", "zone1", constants.DefaultTableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_PRIMARY)
	addPoolerForTest(t, lb, newPrimary)
	simulateHealthUpdate(connForTest(t, lb, newPrimary), clustermetadatapb.PoolerServingStatus_SERVING,
		newPrimary.Id, &clustermetadatapb.RuleNumber{CoordinatorTerm: 2})

	var gotErr error
	require.Eventually(t, func() bool {
		// The health update can race the DRAINING -> BUFFERING re-arm, so repeat its
		// drain signal until the new generation is visible.
		failoverBuffer.StopBuffering(target.GetShardKey())
		select {
		case gotErr = <-done:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
	require.NoError(t, gotErr)
	lastAttempt := ""
	for len(attempted) > 0 {
		lastAttempt = <-attempted
	}
	require.Equal(t, "new", lastAttempt)
}

func TestGetAuthCredentials_InfrastructureFailureCarriesCannotConnectNow(t *testing.T) {
	tests := []struct {
		name    string
		authErr error
	}{
		{
			name:    "PostgreSQL unavailable behind pooler",
			authErr: status.Error(codes.Unavailable, "failed to connect to PostgreSQL socket"),
		},
		{
			name:    "pooler reports planned failover",
			authErr: mterrors.ToGRPC(mterrors.MTF01.New()),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lb := newTestLB(t, "zone1")
			primary := createTestMultipooler("primary", "zone1", constants.DefaultTableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_PRIMARY)
			addPoolerForTest(t, lb, primary)

			conn := connForTest(t, lb, primary)
			require.NotNil(t, conn)
			conn.cancel()
			<-conn.checkConnDone
			conn.client = &mockMultipoolerServiceClient{authErr: tt.authErr}
			setLeaderForTest(t, lb, constants.DefaultPostgresDatabase, constants.DefaultTableGroup, constants.DefaultShard,
				primary.Id, &clustermetadatapb.RuleNumber{CoordinatorTerm: 1})

			pg := &PoolerGateway{loadBalancer: lb, logger: slog.Default()}
			_, err := pg.GetAuthCredentials(t.Context(), &multipoolerpb.GetAuthCredentialsRequest{
				Database: constants.DefaultPostgresDatabase,
				Username: "postgres",
			})
			require.Error(t, err)
			assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err))

			var diagnostic *mterrors.PgDiagnostic
			require.ErrorAs(t, err, &diagnostic)
			assert.Equal(t, mterrors.PgSSCannotConnectNow, diagnostic.Code)
			assert.Equal(t, "database is temporarily unavailable; please retry", diagnostic.Message)
		})
	}
}

func TestGetAuthCredentials_FailoverBufferTimeoutCarriesCannotConnectNow(t *testing.T) {
	lb := newTestLB(t, "zone1")
	primary := createTestMultipooler("primary", "zone1", constants.DefaultTableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_PRIMARY)
	addPoolerForTest(t, lb, primary)

	conn := connForTest(t, lb, primary)
	require.NotNil(t, conn)
	conn.cancel()
	<-conn.checkConnDone
	conn.client = &mockMultipoolerServiceClient{authErr: status.Error(codes.Unavailable, "failed to get admin connection")}
	setLeaderForTest(t, lb, constants.DefaultPostgresDatabase, constants.DefaultTableGroup, constants.DefaultShard,
		primary.Id, &clustermetadatapb.RuleNumber{CoordinatorTerm: 1})

	bufferConfig := gatewaybuffer.NewConfig(viperutil.NewRegistry())
	bufferConfig.Enabled.Set(true)
	bufferConfig.Window.Set(20 * time.Millisecond)
	bufferConfig.Size.Set(1)
	bufferConfig.MaxFailoverDuration.Set(time.Second)
	bufferConfig.MinTimeBetweenFailovers.Set(0)
	bufferConfig.DrainConcurrency.Set(1)
	logger := slog.New(slog.DiscardHandler)
	failoverBuffer := gatewaybuffer.New(t.Context(), bufferConfig, logger)
	t.Cleanup(failoverBuffer.Shutdown)

	pg := &PoolerGateway{loadBalancer: lb, buffer: failoverBuffer, logger: logger}
	_, err := pg.GetAuthCredentials(t.Context(), &multipoolerpb.GetAuthCredentialsRequest{
		Database: constants.DefaultPostgresDatabase,
		Username: "postgres",
	})
	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err))
	assert.Contains(t, err.Error(), "failover buffer timeout")

	var diagnostic *mterrors.PgDiagnostic
	require.ErrorAs(t, err, &diagnostic)
	assert.Equal(t, mterrors.PgSSCannotConnectNow, diagnostic.Code)
	assert.Equal(t, "database is temporarily unavailable; please retry", diagnostic.Message)
}

func TestGetAuthCredentials_NoWritablePrimaryReachesClientAsCannotConnectNow(t *testing.T) {
	lb := newTestLB(t, "zone1")
	logger := slog.New(slog.DiscardHandler)
	pg := &PoolerGateway{loadBalancer: lb, logger: logger}
	credentialProvider := auth.NewPoolerCredentialProvider(pg, nil)

	listener, err := pgserver.NewListener(pgserver.ListenerConfig{
		Address:               "127.0.0.1:0",
		Handler:               gatewayhandler.NewMultigatewayHandler(nil, logger, 0),
		CredentialProvider:    credentialProvider,
		AuthenticationTimeout: 5 * time.Second,
		Logger:                logger,
	})
	require.NoError(t, err)
	serveErr := make(chan error, 1)
	go func() { serveErr <- listener.Serve() }()
	t.Cleanup(func() {
		require.NoError(t, listener.Close())
		require.NoError(t, <-serveErr)
	})

	addr, ok := listener.Addr().(*net.TCPAddr)
	require.True(t, ok)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	clientConn, err := pgclient.Connect(ctx, ctx, &pgclient.Config{
		Host:     addr.IP.String(),
		Port:     addr.Port,
		User:     "postgres",
		Password: "credentials-are-not-consulted",
		Database: constants.DefaultPostgresDatabase,
		SSLMode:  pgclient.SSLModeDisable,
	})
	if clientConn != nil {
		require.NoError(t, clientConn.Close())
	}
	require.Error(t, err)

	var diagnostic *mterrors.PgDiagnostic
	require.ErrorAs(t, err, &diagnostic)
	assert.Equal(t, "FATAL", diagnostic.Severity)
	assert.Equal(t, mterrors.PgSSCannotConnectNow, diagnostic.Code)
	assert.Equal(t, "no writable primary is currently available", diagnostic.Message)
	assert.NotEqual(t, mterrors.PgSSAuthFailed, diagnostic.Code)
}

// TestIsSingleQuery covers the classification that decides whether a request
// skips proactive failover buffering. Only a request with no existing reserved
// connection AND that will not create one is a single query. The scenarios are
// labelled by the handler input that produces each (reservedConnID, willReserve)
// pair, so this also documents what each handler passes:
//
//   - StreamExecute:        willReserve = ReservationOptions != nil
//   - ExecuteQuery/Describe: willReserve = false (no reservation path)
//   - PortalStreamExecute:   willReserve = MaxRows > 0 (suspendable cursor)
//   - CopyReady/CopyOutReady/GetAuthCredentials: always proactively buffered
//     (pass singleQuery=false directly; not via this helper)
func TestIsSingleQuery(t *testing.T) {
	tests := []struct {
		name           string
		reservedConnID uint64
		willReserve    bool
		want           bool
	}{
		{"StreamExecute autocommit (no reservation, no conn)", 0, false, true},
		{"ExecuteQuery/Describe standalone (no conn)", 0, false, true},
		{"PortalStreamExecute fetch-all (MaxRows==0, no conn)", 0, false, true},
		{"StreamExecute new transaction (reservation requested)", 0, true, false},
		{"PortalStreamExecute cursor (MaxRows>0)", 0, true, false},
		{"on an existing reserved connection (never a single query)", 42, false, false},
		{"existing reserved conn + would reserve", 42, true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isSingleQuery(tt.reservedConnID, tt.willReserve))
		})
	}
}

func TestRetryReadOnlyError(t *testing.T) {
	txn := func(begin string) *query.ReservationOptions {
		return &query.ReservationOptions{Reasons: protoutil.ReasonTransaction, BeginQuery: begin}
	}
	readOnlyDefault := &query.ExecuteOptions{SessionSettings: map[string]string{"default_transaction_read_only": "on"}}
	readOnlyPrefixDefault := &query.ExecuteOptions{SessionSettings: map[string]string{"default_transaction_read_only": "tr"}}

	tests := []struct {
		name           string
		reservedConnID uint64
		willReserve    bool
		opts           *query.ReservationOptions
		execOptions    *query.ExecuteOptions
		want           bool
	}{
		{"single autocommit query", 0, false, nil, nil, true},
		{"single autocommit query with read-only default", 0, false, nil, readOnlyDefault, false},
		{"single autocommit query with read-only prefix default", 0, false, nil, readOnlyPrefixDefault, false},
		{"deferred read-write transaction", 0, true, txn("START TRANSACTION READ WRITE"), nil, true},
		{"deferred read-write transaction overrides read-only default", 0, true, txn("START TRANSACTION READ WRITE"), readOnlyDefault, true},
		{"deferred plain transaction", 0, true, txn("BEGIN"), nil, true},
		{"deferred plain transaction with read-only default", 0, true, txn("BEGIN"), readOnlyDefault, false},
		{"deferred read-only transaction", 0, true, txn("START TRANSACTION READ ONLY"), nil, false},
		{"deferred read-only transaction with semicolon", 0, true, txn("START TRANSACTION READ ONLY;"), nil, false},
		{"deferred read-only transaction with isolation", 0, true, txn("START TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY;"), nil, false},
		{"deferred read-write transaction with isolation", 0, true, txn("START TRANSACTION ISOLATION LEVEL READ COMMITTED READ WRITE;"), nil, true},
		{"deferred transaction uses last read-only mode", 0, true, txn("BEGIN READ WRITE READ ONLY"), nil, false},
		{"deferred transaction uses last read-write mode", 0, true, txn("BEGIN READ ONLY READ WRITE"), nil, true},
		{"deferred transaction with unknown begin", 0, true, txn(""), nil, false},
		{"existing reserved transaction", 42, false, nil, nil, false},
		{"non-transaction reservation", 0, true, &query.ReservationOptions{Reasons: protoutil.ReasonTempTable}, nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, retryReadOnlyError(tt.reservedConnID, tt.willReserve, tt.opts, tt.execOptions))
		})
	}
}

// fakeStreamReplicationQueryService records the init passed to
// StreamReplication and returns a canned stream/error. It embeds
// queryservice.QueryService so the other (unused) methods satisfy the
// interface without explicit stubs.
type fakeStreamReplicationQueryService struct {
	queryservice.QueryService

	gotInit *multipoolerpb.StreamReplicationInit
	stream  multipoolerpb.MultipoolerService_StreamReplicationClient
	err     error
}

func (f *fakeStreamReplicationQueryService) StreamReplication(
	_ context.Context,
	init *multipoolerpb.StreamReplicationInit,
) (multipoolerpb.MultipoolerService_StreamReplicationClient, error) {
	f.gotInit = init
	return f.stream, f.err
}

// Close overrides the embedded (nil) QueryService so the cache's OnGone
// Shutdown does not panic on cleanup.
func (f *fakeStreamReplicationQueryService) Close() error { return nil }

// TestPoolerGateway_StreamReplication_RoutesToPrimary verifies that the
// gateway forces PRIMARY routing for replication, resolves the leader's
// connection, and delegates to that connection's QueryService — returning
// whatever stream the connection returned and leaving the caller's target
// untouched.
func TestPoolerGateway_StreamReplication_RoutesToPrimary(t *testing.T) {
	lb := newTestLB(t, "zone1")
	pg := &PoolerGateway{loadBalancer: lb, logger: slog.Default()}

	// Add a primary and mark it the leader so PRIMARY routing resolves.
	primary := createTestMultipooler("primary1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	addPoolerForTest(t, lb, primary)
	setLeaderForTest(t, lb, constants.DefaultPostgresDatabase, constants.DefaultTableGroup, "0",
		primary.Id, &clustermetadatapb.RuleNumber{CoordinatorTerm: 1})

	// Swap the cached connection's QueryService for a fake that records the init.
	conn := connForTest(t, lb, primary)
	require.NotNil(t, conn)
	wantStream := multipoolerpb.MultipoolerService_StreamReplicationClient(nil)
	fake := &fakeStreamReplicationQueryService{stream: wantStream}
	conn.queryService = fake

	// The caller's target carries a follower-eligible mode; the gateway must
	// force leader (WRITABLE) routing without mutating the caller's proto.
	callerTarget := protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup, "0", query.Mode_MODE_INCONSISTENT)
	init := &multipoolerpb.StreamReplicationInit{Target: callerTarget}

	stream, err := pg.StreamReplication(t.Context(), init)
	require.NoError(t, err)
	assert.Equal(t, wantStream, stream, "should return the connection's stream")

	// The connection's QueryService received the init.
	require.NotNil(t, fake.gotInit)

	// The sent init's target mode must be forced to WRITABLE, matching the
	// routing decision above — not just used locally to pick a connection.
	// The pooler's own leader-freshness check (checkTargetLocked in
	// go/services/multipooler/internal/poolerserver/pooler.go) only fires for
	// WRITABLE/CONSISTENT targets, so if this were still INCONSISTENT (the
	// caller's original mode), a demoted pooler would silently admit the
	// stream instead of rejecting it.
	assert.Equal(t, query.Mode_MODE_WRITABLE, fake.gotInit.GetTarget().GetMode(),
		"the sent init's target mode must be forced to WRITABLE")

	// The caller's target proto was not mutated in place.
	assert.Equal(t, query.Mode_MODE_INCONSISTENT, callerTarget.Mode,
		"caller's target must not be mutated")
}

// TestPoolerGateway_StreamReplication_NilTargetDoesNotPanic verifies that an
// Init with no Target set (init.GetTarget() == nil) is handled by
// substituting an empty Target rather than panicking on the subsequent
// target.Mode assignment. No pooler is registered, so routing still fails,
// but that failure must be a clean UNAVAILABLE error, not a nil dereference.
func TestPoolerGateway_StreamReplication_NilTargetDoesNotPanic(t *testing.T) {
	lb := newTestLB(t, "zone1")
	pg := &PoolerGateway{loadBalancer: lb, logger: slog.Default()}

	init := &multipoolerpb.StreamReplicationInit{}

	var stream multipoolerpb.MultipoolerService_StreamReplicationClient
	var err error
	assert.NotPanics(t, func() {
		stream, err = pg.StreamReplication(t.Context(), init)
	})
	require.Error(t, err)
	assert.Nil(t, stream)
}

// TestPoolerGateway_StreamReplication_NoLeaderPropagatesError verifies that
// when no leader is observed (load balancer returns UNAVAILABLE), the error
// is propagated and no stream is returned.
func TestPoolerGateway_StreamReplication_NoLeaderPropagatesError(t *testing.T) {
	lb := newTestLB(t, "zone1")
	pg := &PoolerGateway{loadBalancer: lb, logger: slog.Default()}

	init := &multipoolerpb.StreamReplicationInit{
		Target: protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup, "0", query.Mode_MODE_WRITABLE),
	}

	stream, err := pg.StreamReplication(t.Context(), init)
	require.Error(t, err)
	assert.Nil(t, stream)
	assert.True(t, mterrors.Code(err) == mtrpcpb.Code_UNAVAILABLE,
		"no-leader error should be UNAVAILABLE, got %v", err)
}
