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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/queryservice"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
)

func TestClassifyError(t *testing.T) {
	primaryTarget := &query.Target{
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	replicaTarget := &query.Target{
		PoolerType: clustermetadatapb.PoolerType_REPLICA,
	}

	tests := []struct {
		name   string
		err    error
		target *query.Target
		want   errorAction
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
			name:   "read_only_sql_transaction on PRIMARY triggers buffering",
			err:    mterrors.NewPgError("ERROR", mterrors.PgSSReadOnlyTransaction, "cannot execute INSERT in a read-only transaction", ""),
			target: primaryTarget,
			want:   actionBuffer,
		},
		{
			name:   "read_only_sql_transaction on REPLICA does not buffer",
			err:    mterrors.NewPgError("ERROR", mterrors.PgSSReadOnlyTransaction, "cannot execute INSERT in a read-only transaction", ""),
			target: replicaTarget,
			want:   actionFail,
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
			got := classifyError(tt.err, tt.target)
			assert.Equal(t, tt.want, got)
		})
	}
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

// fakeStreamReplicationQueryService records the init passed to
// StreamReplication and returns a canned stream/error. It embeds
// queryservice.QueryService so the other (unused) methods satisfy the
// interface without explicit stubs.
type fakeStreamReplicationQueryService struct {
	queryservice.QueryService

	gotInit *multipoolerservice.StreamReplicationInit
	stream  multipoolerservice.MultiPoolerService_StreamReplicationClient
	err     error
}

func (f *fakeStreamReplicationQueryService) StreamReplication(
	_ context.Context,
	init *multipoolerservice.StreamReplicationInit,
) (multipoolerservice.MultiPoolerService_StreamReplicationClient, error) {
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
	primary := createTestMultiPooler("primary1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	addPoolerForTest(t, lb, primary)
	setLeaderForTest(t, lb, constants.DefaultTableGroup, "0",
		&clustermetadatapb.LeaderObservation{LeaderId: primary.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1}})

	// Swap the cached connection's QueryService for a fake that records the init.
	conn := connForTest(t, lb, primary)
	require.NotNil(t, conn)
	wantStream := multipoolerservice.MultiPoolerService_StreamReplicationClient(nil)
	fake := &fakeStreamReplicationQueryService{stream: wantStream}
	conn.queryService = fake

	// The caller's target carries a non-PRIMARY type; the gateway must force
	// PRIMARY for routing without mutating the caller's proto.
	callerTarget := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		Shard:      "0",
		PoolerType: clustermetadatapb.PoolerType_REPLICA,
	}
	init := &multipoolerservice.StreamReplicationInit{Target: callerTarget}

	stream, err := pg.StreamReplication(t.Context(), init)
	require.NoError(t, err)
	assert.Equal(t, wantStream, stream, "should return the connection's stream")

	// The connection's QueryService received the init.
	require.NotNil(t, fake.gotInit)

	// The caller's target proto was not mutated in place.
	assert.Equal(t, clustermetadatapb.PoolerType_REPLICA, callerTarget.PoolerType,
		"caller's target must not be mutated")
}

// TestPoolerGateway_StreamReplication_NoLeaderPropagatesError verifies that
// when no leader is observed (load balancer returns UNAVAILABLE), the error
// is propagated and no stream is returned.
func TestPoolerGateway_StreamReplication_NoLeaderPropagatesError(t *testing.T) {
	lb := newTestLB(t, "zone1")
	pg := &PoolerGateway{loadBalancer: lb, logger: slog.Default()}

	init := &multipoolerservice.StreamReplicationInit{
		Target: &query.Target{
			TableGroup: constants.DefaultTableGroup,
			Shard:      "0",
			PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		},
	}

	stream, err := pg.StreamReplication(t.Context(), init)
	require.Error(t, err)
	assert.Nil(t, stream)
	assert.True(t, mterrors.Code(err) == mtrpcpb.Code_UNAVAILABLE,
		"no-leader error should be UNAVAILABLE, got %v", err)
}
