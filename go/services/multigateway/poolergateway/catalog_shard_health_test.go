// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package poolergateway

import (
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/multipoolerservice"
)

func healthTestConn(tableGroup, shard string) *poolerConnection {
	mp := createTestMultipooler("p1", "zone1", tableGroup, shard, clustermetadatapb.PoolerType_PRIMARY)
	pc := &poolerConnection{logger: slog.New(slog.DiscardHandler), health: &poolerHealth{LastError: errPoolerUninitialized}}
	pc.poolerInfo.Store(&topoclient.MultipoolerInfo{Multipooler: mp})
	return pc
}

func reportHealth(pc *poolerConnection, role clustermetadatapb.RoutingRole, routingVersion int64) {
	pc.processHealthResponse(&multipoolerservice.StreamPoolerHealthResponse{
		ServingStatus:  clustermetadatapb.PoolerServingStatus_SERVING,
		RoutingState:   &clustermetadatapb.RoutingState{Role: role, Rule: &clustermetadatapb.RuleNumber{CoordinatorTerm: 3, LeaderSubterm: 1}},
		RoutingVersion: routingVersion,
		PlanEpoch:      2,
	})
}

func TestCatalogShardHealth(t *testing.T) {
	t.Run("the live catalog primary reports its version", func(t *testing.T) {
		pc := healthTestConn(constants.DefaultTableGroup, constants.DefaultShard)
		reportHealth(pc, clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY, 7)
		assert.Equal(t, int64(2), pc.Health().PlanEpoch)

		db, primary, rule, version, ok := catalogShardHealth(pc)
		require.True(t, ok)
		assert.Equal(t, constants.DefaultPostgresDatabase, db)
		assert.True(t, primary)
		assert.Equal(t, int64(3), rule.GetCoordinatorTerm())
		assert.Equal(t, int64(1), rule.GetLeaderSubterm())
		assert.Equal(t, int64(7), version)
	})
	t.Run("a replica is seen but reports nothing", func(t *testing.T) {
		pc := healthTestConn(constants.DefaultTableGroup, constants.DefaultShard)
		reportHealth(pc, clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA, 7)

		_, primary, rule, version, ok := catalogShardHealth(pc)
		require.True(t, ok)
		assert.False(t, primary)
		assert.Nil(t, rule)
		assert.Zero(t, version)
	})
	t.Run("a stale stream is not a primary report", func(t *testing.T) {
		pc := healthTestConn(constants.DefaultTableGroup, constants.DefaultShard)
		reportHealth(pc, clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY, 7)
		pc.setHealthError(errors.New("stream stalled"))
		assert.Equal(t, int64(7), pc.Health().RoutingVersion, "the error keeps the last report")

		_, primary, rule, version, ok := catalogShardHealth(pc)
		require.True(t, ok)
		assert.False(t, primary)
		assert.Nil(t, rule)
		assert.Zero(t, version)
	})
	t.Run("other shards are not catalog shards", func(t *testing.T) {
		pc := healthTestConn("commerce", constants.DefaultShard)
		reportHealth(pc, clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY, 7)
		_, _, _, _, ok := catalogShardHealth(pc)
		assert.False(t, ok)
	})
}
