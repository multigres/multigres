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

package multigateway

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multigateway/poolergateway"
)

// newTestPooler builds a Multipooler proto for status tests. SelfLeadership
// is left unset by default; set it via withSelfLeader for leader scenarios.
func newTestPooler(cell, name, tableGroup, shard string, lifecycle clustermetadatapb.PoolerLifecycleStatus) *clustermetadatapb.Multipooler {
	return &clustermetadatapb.Multipooler{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      cell,
			Name:      name,
		},
		Hostname: name + ".example.com",
		PortMap:  map[string]int32{"grpc": 50051},
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   "db",
			TableGroup: tableGroup,
			Shard:      shard,
		},
		Type:            clustermetadatapb.PoolerType_PRIMARY,
		LifecycleStatus: &clustermetadatapb.PoolerLifecycle{Status: lifecycle},
	}
}

// withSelfLeader attaches a self_leadership observation naming the pooler
// itself as leader at the given coordinator term. This mirrors the topology
// record of a real PRIMARY pooler and is how the gateway learns of a leader
// at discovery time.
func withSelfLeader(p *clustermetadatapb.Multipooler, coordinatorTerm int64) *clustermetadatapb.Multipooler {
	p.RoutingState = &clustermetadatapb.RoutingState{
		Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY,
		Rule: &clustermetadatapb.RuleNumber{CoordinatorTerm: coordinatorTerm},
	}
	return p
}

// newTestGateway wires a Multigateway with just enough machinery for
// collectCellStatuses: a PoolerGateway backed by memorytopo, no listeners,
// no buffer. Cells passed in are pre-created in topology.
func newTestGateway(t *testing.T, cells ...string) (*Multigateway, topoclient.Store) {
	t.Helper()
	ctx := t.Context()
	ts, _ := memorytopo.NewServerAndFactory(ctx, cells...)
	t.Cleanup(func() { ts.Close() })

	pg := poolergateway.NewPoolerGateway(poolergateway.PoolerGatewayOpts{
		Ctx:       ctx,
		Source:    ts,
		LocalCell: "zone1",
		Logger:    slog.New(slog.DiscardHandler),
		DialOpt:   grpc.WithTransportCredentials(insecure.NewCredentials()),
	})
	t.Cleanup(func() { _ = pg.Close() })

	mg := &Multigateway{poolerGateway: pg}
	return mg, ts
}

// waitForPoolerCount blocks until pg.PoolerCount() reaches want.
func waitForPoolerCount(t *testing.T, mg *Multigateway, want int) {
	t.Helper()
	require.Eventually(t, func() bool {
		return mg.poolerGateway.PoolerCount() == want
	}, 3*time.Second, 5*time.Millisecond, "expected PoolerCount=%d, got %d", want, mg.poolerGateway.PoolerCount())
}

// findPoolerStatus returns the PoolerStatus for the given pooler name in the
// given cell, or fails the test if not present.
func findPoolerStatus(t *testing.T, statuses []CellStatus, cell, name string) PoolerStatus {
	t.Helper()
	for _, cs := range statuses {
		if cs.Cell != cell {
			continue
		}
		for _, p := range cs.Poolers {
			if p.Name == name {
				return p
			}
		}
	}
	t.Fatalf("pooler %s/%s not found in statuses: %+v", cell, name, statuses)
	return PoolerStatus{}
}

func TestCollectCellStatuses_Empty(t *testing.T) {
	mg, _ := newTestGateway(t /* no cells */)
	// Give the topology watch a moment to start so it has a chance to populate.
	// Without cells there's nothing to populate, so the result should be empty.
	statuses := mg.collectCellStatuses()
	assert.Empty(t, statuses)
}

func TestCollectCellStatuses_MultipleCellsSortedAlphabetically(t *testing.T) {
	mg, ts := newTestGateway(t, "zone2", "zone1", "zone3")
	ctx := t.Context()

	// Create one pooler per cell so each cell has a non-empty list.
	poolers := []*clustermetadatapb.Multipooler{
		newTestPooler("zone2", "b", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE),
		newTestPooler("zone1", "a1", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE),
		newTestPooler("zone1", "a2", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE),
		newTestPooler("zone3", "c", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE),
	}
	for _, p := range poolers {
		require.NoError(t, ts.CreateMultipooler(ctx, p))
	}
	waitForPoolerCount(t, mg, len(poolers))

	statuses := mg.collectCellStatuses()
	require.Len(t, statuses, 3)

	// Cells sorted alphabetically.
	assert.Equal(t, "zone1", statuses[0].Cell)
	assert.Equal(t, "zone2", statuses[1].Cell)
	assert.Equal(t, "zone3", statuses[2].Cell)

	// LastRefresh populated on every cell.
	for _, cs := range statuses {
		assert.False(t, cs.LastRefresh.IsZero(), "LastRefresh must be populated for cell %s", cs.Cell)
	}

	// Per-cell pooler order is sorted by serialized component ID (which has
	// cell prefix + name); within zone1, a1 sorts before a2.
	require.Len(t, statuses[0].Poolers, 2)
	assert.Equal(t, "a1", statuses[0].Poolers[0].Name)
	assert.Equal(t, "a2", statuses[0].Poolers[1].Name)

	require.Len(t, statuses[1].Poolers, 1)
	assert.Equal(t, "b", statuses[1].Poolers[0].Name)

	require.Len(t, statuses[2].Poolers, 1)
	assert.Equal(t, "c", statuses[2].Poolers[0].Name)

	// Lifecycle label round-trips through the renderer.
	assert.Equal(t, "active", statuses[0].Poolers[0].Lifecycle)
}

// TestCollectCellStatuses_EtcdSelfLeadershipDoesNotImplyLeader verifies that a
// pooler's topology self_leadership record does NOT make the gateway report it
// as a leader. Leadership is derived only from live health-stream observations
// now — the gateway deliberately no longer trusts etcd for leader identity (see
// the load balancer's move off topology for routing). Since this test's poolers
// have no reachable health stream, they render as plain followers even though
// their etcd record names them leader.
//
// The positive leader / stale-leader / follower derivation is exercised where
// live health can be simulated: poolergateway.TestLoadBalancer_LeadershipFor.
func TestCollectCellStatuses_EtcdSelfLeadershipDoesNotImplyLeader(t *testing.T) {
	mg, ts := newTestGateway(t, "zone1")
	ctx := t.Context()

	leader := withSelfLeader(
		newTestPooler("zone1", "leader1", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE),
		1,
	)
	require.NoError(t, ts.CreateMultipooler(ctx, leader))
	waitForPoolerCount(t, mg, 1)

	statuses := mg.collectCellStatuses()
	ps := findPoolerStatus(t, statuses, "zone1", "leader1")
	assert.Equal(t, "follower", ps.Leadership,
		"etcd self_leadership must not make the gateway report a leader; only live health does")
}

// TestCollectCellStatuses_TombstoneHasEmptyLeadership verifies that a pooler
// recorded as SHUTDOWN in topology shows up in CellStatuses (as a tombstone) but
// reports an empty leadership string — there is no rider in the cache for it,
// so LeadershipForID returns "".
func TestCollectCellStatuses_TombstoneHasEmptyLeadership(t *testing.T) {
	mg, ts := newTestGateway(t, "zone1")
	ctx := t.Context()

	// Create a SHUTDOWN pooler. The cache's "cold-shutdown discovery"
	// path records it as a tombstone — no rider, no OnLive — but it still
	// appears in CellStatuses for operator visibility.
	tombstone := newTestPooler("zone1", "tomb1", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN)
	require.NoError(t, ts.CreateMultipooler(ctx, tombstone))

	// Wait until the tombstone shows up in collectCellStatuses (since PoolerCount
	// reflects only live entries, we poll the status directly).
	require.Eventually(t, func() bool {
		for _, cs := range mg.collectCellStatuses() {
			for _, p := range cs.Poolers {
				if p.Name == "tomb1" {
					return true
				}
			}
		}
		return false
	}, 2*time.Second, 5*time.Millisecond, "tombstone pooler did not appear in CellStatuses")

	statuses := mg.collectCellStatuses()
	ps := findPoolerStatus(t, statuses, "zone1", "tomb1")
	assert.Equal(t, "", ps.Leadership, "tombstone pooler must have empty leadership")
	assert.Equal(t, "shutdown", ps.Lifecycle)
}
