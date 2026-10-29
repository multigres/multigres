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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/protoutil"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerservice "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
)

func unmanagedPooler(name, cell, tableGroup string) *clustermetadatapb.Multipooler {
	p := createTestMultipooler(name, cell, tableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_PRIMARY)
	p.ManagementMode = clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED
	return p
}

// reportHealth reports a live health snapshot for an unmanaged pooler's connection: it
// reports role PRIMARY (with no rule) while it serves, that is while its backend is ready, and
// none otherwise.
func reportHealth(conn *poolerConnection, status clustermetadatapb.PoolerServingStatus) {
	resp := &multipoolerservice.StreamPoolerHealthResponse{PoolerId: conn.PoolerInfo().Id, ServingStatus: status}
	if status == clustermetadatapb.PoolerServingStatus_SERVING {
		resp.RoutingState = &clustermetadatapb.RoutingState{Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY}
	}
	conn.processHealthResponse(resp)
}

func writableTarget(tableGroup string) *query.Target {
	return protoutil.NewTarget(constants.DefaultPostgresDatabase, tableGroup, constants.DefaultShard, query.Mode_MODE_WRITABLE)
}

// TestAppRoutingRewrite: the application targets of the routed database are pointed at the
// application tablegroup, and the credential lookups follow them. Other databases, and a
// gateway without routing, are left alone.
func TestAppRoutingRewrite(t *testing.T) {
	r := NewAppRouting("appdb", "destTG")

	mine := protoutil.NewTarget("appdb", constants.DefaultTableGroup, constants.DefaultShard, query.Mode_MODE_WRITABLE)
	original := mine.GetShardRef()
	r.Rewrite(mine)
	assert.Equal(t, "destTG", mine.GetShardRef().GetTableGroup())
	assert.Equal(t, "appdb", mine.GetShardRef().GetDatabase())
	assert.Equal(t, constants.DefaultShard, mine.GetShardRef().GetShard())
	assert.Equal(t, constants.DefaultTableGroup, original.GetTableGroup(), "the key is replaced, not edited: a buffered request may hold the old one")

	other := protoutil.NewTarget("elsewhere", constants.DefaultTableGroup, constants.DefaultShard, query.Mode_MODE_WRITABLE)
	r.Rewrite(other)
	assert.Equal(t, constants.DefaultTableGroup, other.GetShardRef().GetTableGroup(), "another database is left alone")

	assert.Equal(t, "destTG", r.AuthTableGroup("appdb"))
	assert.Equal(t, constants.DefaultTableGroup, r.AuthTableGroup("elsewhere"), "a database the routing does not follow keeps the default")

	// A nil AppRouting is a no-op, so ordinary gateways are unaffected.
	var none *AppRouting
	untouched := writableTarget(constants.DefaultTableGroup)
	none.Rewrite(untouched)
	assert.Equal(t, constants.DefaultTableGroup, untouched.GetShardRef().GetTableGroup())
	assert.Equal(t, constants.DefaultTableGroup, none.AuthTableGroup("appdb"))
}

// TestRequestsAreServedByTheApplicationTablegroup: with routing set, a request that names the
// default tablegroup is served by a pooler of the application tablegroup.
func TestRequestsAreServedByTheApplicationTablegroup(t *testing.T) {
	lb := newTestLB(t, "zone1")
	pg := &PoolerGateway{loadBalancer: lb, logger: slog.New(slog.DiscardHandler)}
	src := unmanagedPooler("src", "zone1", "migrateTG")
	addPoolerForTest(t, lb, src)
	reportHealth(connForTest(t, lb, src), clustermetadatapb.PoolerServingStatus_SERVING)
	pg.SetAppRouting(NewAppRouting(constants.DefaultPostgresDatabase, "migrateTG"))

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var served *poolerConnection
	require.NoError(t, pg.withBuffering(ctx, writableTarget(constants.DefaultTableGroup), true, false, func(conn *poolerConnection) error {
		served = conn
		return nil
	}))
	require.NotNil(t, served)
	assert.Equal(t, poolerID(src), served.ID())
}
