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

package consensus

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

var routingPrimaryShard = &clustermetadatapb.ShardRef{Database: "db", TableGroup: "default", Shard: "0-inf"}

// addRoutingPooler registers a pooler of shard in cell with role (no routing
// state when UNKNOWN), under coordinator term term.
func addRoutingPooler(t *testing.T, ts topoclient.Store, cell, name string, shard *clustermetadatapb.ShardRef,
	role clustermetadatapb.RoutingRole, term int64, lifecycle clustermetadatapb.PoolerLifecycleStatus,
) {
	t.Helper()
	mp := &clustermetadatapb.Multipooler{
		Id:              &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: cell, Name: name},
		ShardRef:        shard,
		LifecycleStatus: &clustermetadatapb.PoolerLifecycle{Status: lifecycle},
	}
	if role != clustermetadatapb.RoutingRole_ROUTING_ROLE_UNKNOWN {
		mp.RoutingState = &clustermetadatapb.RoutingState{Role: role, Rule: &clustermetadatapb.RuleNumber{CoordinatorTerm: term}}
	}
	require.NoError(t, ts.CreateMultipooler(t.Context(), mp))
}

func TestFindRoutingPrimary(t *testing.T) {
	const (
		primary  = clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY
		replica  = clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA
		active   = clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE
		shutdown = clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN
	)
	ts, _ := memorytopo.NewServerAndFactory(t.Context(), "cell1", "cell2")
	otherShard := &clustermetadatapb.ShardRef{Database: "db", TableGroup: "default", Shard: "0-80"}
	otherDB := &clustermetadatapb.ShardRef{Database: "other", TableGroup: "default", Shard: "0-inf"}

	_, err := FindRoutingPrimary(t.Context(), ts, routingPrimaryShard)
	assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err), "no pooler at all")

	addRoutingPooler(t, ts, "cell1", "replica", routingPrimaryShard, replica, 3, active)
	addRoutingPooler(t, ts, "cell1", "gone", routingPrimaryShard, primary, 9, shutdown)
	addRoutingPooler(t, ts, "cell1", "other-shard", otherShard, primary, 9, active)
	addRoutingPooler(t, ts, "cell1", "other-db", otherDB, primary, 9, active)
	_, err = FindRoutingPrimary(t.Context(), ts, routingPrimaryShard)
	assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err),
		"a replica, a shut-down primary and primaries of other shards are not the shard's primary")
	assert.Contains(t, err.Error(), "no routing primary of db/default/0-inf is registered")

	// Two poolers advertise PRIMARY: the one under the higher rule wins,
	// whichever cell it is in.
	addRoutingPooler(t, ts, "cell1", "deposed", routingPrimaryShard, primary, 4, active)
	addRoutingPooler(t, ts, "cell2", "leader", routingPrimaryShard, primary, 5, active)
	got, err := FindRoutingPrimary(t.Context(), ts, routingPrimaryShard)
	require.NoError(t, err)
	assert.Equal(t, "leader", got.GetId().GetName())
}

func TestFindRoutingPrimary_UnreadableTopology(t *testing.T) {
	ts, factory := memorytopo.NewServerAndFactory(t.Context(), "cell1")
	factory.SetError(errors.New("etcd unreachable"))
	_, err := FindRoutingPrimary(t.Context(), ts, routingPrimaryShard)
	assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err))
	assert.Contains(t, err.Error(), "etcd unreachable")
}

// cellFailingStore fails the multipooler lookup of one cell.
type cellFailingStore struct {
	topoclient.Store
	cell string
}

func (s cellFailingStore) GetMultipoolersByCell(ctx context.Context, cell string, opt *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
	if cell == s.cell {
		return nil, errors.New("etcd unreachable")
	}
	return s.Store.GetMultipoolersByCell(ctx, cell, opt)
}

// A cell that cannot be read may hold the primary under a higher rule, so the
// best of the cells that were read is not returned: during a failover it is
// the deposed primary.
func TestFindRoutingPrimary_UnreadableCell(t *testing.T) {
	const (
		primary = clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY
		active  = clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE
	)
	ts, _ := memorytopo.NewServerAndFactory(t.Context(), "cell1", "cell2")
	addRoutingPooler(t, ts, "cell1", "deposed", routingPrimaryShard, primary, 4, active)
	addRoutingPooler(t, ts, "cell2", "leader", routingPrimaryShard, primary, 5, active)

	_, err := FindRoutingPrimary(t.Context(), cellFailingStore{Store: ts, cell: "cell2"}, routingPrimaryShard)
	assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err))
	assert.Contains(t, err.Error(), "cells [cell2] could not be read")

	got, err := FindRoutingPrimary(t.Context(), ts, routingPrimaryShard)
	require.NoError(t, err)
	assert.Equal(t, "leader", got.GetId().GetName())
}
