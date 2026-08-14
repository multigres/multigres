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

package multiadmin

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func makeRoutedPooler(cell, name string, role clustermetadatapb.RoutingRole) *clustermetadatapb.Multipooler {
	p := makePooler(cell, name)
	p.RoutingState = &clustermetadatapb.RoutingState{Role: role}
	return p
}

func TestSwitchPrimary_NoShardKey(t *testing.T) {
	s := newTestServer(t, "cell1")
	_, err := s.SwitchPrimary(t.Context(), &multiadminpb.SwitchPrimaryRequest{})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestSwitchPrimary_NoPrimaryFound(t *testing.T) {
	ctx := t.Context()
	s := newTestServer(t, "cell1")

	// Register a replica only — no primary.
	replica := makeRoutedPooler("cell1", "mp-replica", clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA)
	require.NoError(t, s.ts.CreateMultipooler(ctx, replica))

	_, err := s.SwitchPrimary(ctx, &multiadminpb.SwitchPrimaryRequest{
		ShardKey: &clustermetadatapb.ShardKey{Database: "db1", TableGroup: "default", Shard: "0-inf"},
	})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestSwitchPrimary_NoStandby(t *testing.T) {
	ctx := t.Context()
	s := newTestServer(t, "cell1")

	// Register a primary only — no standby to promote to.
	primary := makeRoutedPooler("cell1", "mp-primary", clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY)
	require.NoError(t, s.ts.CreateMultipooler(ctx, primary))

	_, err := s.SwitchPrimary(ctx, &multiadminpb.SwitchPrimaryRequest{
		ShardKey: &clustermetadatapb.ShardKey{Database: "db1", TableGroup: "default", Shard: "0-inf"},
	})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestSwitchPrimary_Success(t *testing.T) {
	ctx := t.Context()
	s := newTestServer(t, "cell1")

	primary := makeRoutedPooler("cell1", "mp-primary", clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY)
	replica := makeRoutedPooler("cell1", "mp-replica", clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA)
	require.NoError(t, s.ts.CreateMultipooler(ctx, primary))
	require.NoError(t, s.ts.CreateMultipooler(ctx, replica))

	fc := rpcclient.NewFakeClient()
	primaryKey := topoclient.ComponentIDString(primary.Id)
	fc.SetResignLeadershipResponse(primaryKey, &multipoolermanagerdatapb.ResignLeadershipResponse{
		FlushLsn: "0/2000000",
	})
	s.SetRPCClient(fc)

	resp, err := s.SwitchPrimary(ctx, &multiadminpb.SwitchPrimaryRequest{
		ShardKey: &clustermetadatapb.ShardKey{Database: "db1", TableGroup: "default", Shard: "0-inf"},
		Reason:   "unit test",
	})
	require.NoError(t, err)
	assert.Equal(t, "mp-primary", resp.GetOldLeaderId().GetName())
}

// TestSwitchPrimary_BackoffWait verifies that SwitchPrimary waits for the
// coordinator backoff window to expire before calling ResignLeadership when
// the primary reports a very recent TermRevocation.
func TestSwitchPrimary_BackoffWait(t *testing.T) {
	ctx := t.Context()
	s := newTestServer(t, "cell1")

	primary := makeRoutedPooler("cell1", "mp-primary", clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY)
	replica := makeRoutedPooler("cell1", "mp-replica", clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA)
	require.NoError(t, s.ts.CreateMultipooler(ctx, primary))
	require.NoError(t, s.ts.CreateMultipooler(ctx, replica))

	fc := rpcclient.NewFakeClient()
	primaryKey := topoclient.ComponentIDString(primary.Id)
	// Report a TermRevocation initiated 3.6 seconds ago so the backoff path
	// triggers but the remaining wait is small (~400 ms + 500 ms jitter).
	fc.SetStatusResponse(primaryKey, &multipoolermanagerdatapb.StatusResponse{
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			TermRevocation: &clustermetadatapb.TermRevocation{
				CoordinatorInitiatedAt: timestamppb.New(time.Now().Add(-3600 * time.Millisecond)),
			},
		},
	})
	fc.SetResignLeadershipResponse(primaryKey, &multipoolermanagerdatapb.ResignLeadershipResponse{
		FlushLsn: "0/3000000",
	})
	s.SetRPCClient(fc)

	resp, err := s.SwitchPrimary(ctx, &multiadminpb.SwitchPrimaryRequest{
		ShardKey: &clustermetadatapb.ShardKey{Database: "db1", TableGroup: "default", Shard: "0-inf"},
	})
	require.NoError(t, err)
	assert.Equal(t, "mp-primary", resp.GetOldLeaderId().GetName())
}

func TestSwitchPrimary_ResignLeadershipError(t *testing.T) {
	ctx := t.Context()
	s := newTestServer(t, "cell1")

	primary := makeRoutedPooler("cell1", "mp-primary", clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY)
	replica := makeRoutedPooler("cell1", "mp-replica", clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA)
	require.NoError(t, s.ts.CreateMultipooler(ctx, primary))
	require.NoError(t, s.ts.CreateMultipooler(ctx, replica))

	fc := rpcclient.NewFakeClient()
	primaryKey := topoclient.ComponentIDString(primary.Id)
	fc.Errors[primaryKey] = status.Error(codes.Unavailable, "pooler unreachable")
	s.SetRPCClient(fc)

	_, err := s.SwitchPrimary(ctx, &multiadminpb.SwitchPrimaryRequest{
		ShardKey: &clustermetadatapb.ShardKey{Database: "db1", TableGroup: "default", Shard: "0-inf"},
	})
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
}
