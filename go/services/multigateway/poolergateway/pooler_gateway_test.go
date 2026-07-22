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
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/auth"
	gatewaybuffer "github.com/multigres/multigres/go/services/multigateway/buffer"
	gatewayhandler "github.com/multigres/multigres/go/services/multigateway/handler"
	"github.com/multigres/multigres/go/tools/viperutil"
)

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
	conn.client = &mockMultipoolerServiceClient{authErr: mterrors.ToGRPC(mterrors.MTF01.New())}
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
