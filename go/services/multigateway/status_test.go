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

// newTestPooler builds a MultiPooler proto for status tests. SelfLeadership
// is left unset by default; set it via withSelfLeader for leader scenarios.
func newTestPooler(cell, name, tableGroup, shard string, lifecycle clustermetadatapb.PoolerLifecycleStatus) *clustermetadatapb.MultiPooler {
	return &clustermetadatapb.MultiPooler{
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
func withSelfLeader(p *clustermetadatapb.MultiPooler, coordinatorTerm int64) *clustermetadatapb.MultiPooler {
	p.SelfLeadership = &clustermetadatapb.LeaderObservation{
		LeaderId:         p.Id,
		LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: coordinatorTerm},
	}
	return p
}

// newTestGateway wires a MultiGateway with just enough machinery for
// collectCellStatuses: a PoolerGateway backed by memorytopo, no listeners,
// no buffer. Cells passed in are pre-created in topology.
func newTestGateway(t *testing.T, cells ...string) (*MultiGateway, topoclient.Store) {
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

	mg := &MultiGateway{poolerGateway: pg}
	return mg, ts
}

// waitForPoolerCount blocks until pg.PoolerCount() reaches want.
func waitForPoolerCount(t *testing.T, mg *MultiGateway, want int) {
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
	poolers := []*clustermetadatapb.MultiPooler{
		newTestPooler("zone2", "b", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE),
		newTestPooler("zone1", "a1", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE),
		newTestPooler("zone1", "a2", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE),
		newTestPooler("zone3", "c", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE),
	}
	for _, p := range poolers {
		require.NoError(t, ts.CreateMultiPooler(ctx, p))
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

func TestCollectCellStatuses_LeaderReportsLeader(t *testing.T) {
	mg, ts := newTestGateway(t, "zone1")
	ctx := t.Context()

	// The pooler's own self_leadership names itself as leader; the gateway
	// folds this into the shard summary on OnLive, so leadershipFor will
	// return "leader" for it.
	leader := withSelfLeader(
		newTestPooler("zone1", "leader1", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE),
		1,
	)
	require.NoError(t, ts.CreateMultiPooler(ctx, leader))
	waitForPoolerCount(t, mg, 1)

	statuses := mg.collectCellStatuses()
	ps := findPoolerStatus(t, statuses, "zone1", "leader1")
	assert.Equal(t, "leader", ps.Leadership)
}

func TestCollectCellStatuses_StaleLeader(t *testing.T) {
	mg, ts := newTestGateway(t, "zone1")
	ctx := t.Context()

	// Two poolers in the same shard. `oldLeader` has self_leadership at term 1.
	// `newLeader` has self_leadership at term 2 — its higher rule number wins
	// when the shard summary is built. From the gateway's view:
	//   - newLeader is the consensus leader (reported as "leader"),
	//   - oldLeader still believes itself the leader (its own self_leadership
	//     names itself) but consensus has moved on — reported as "stale-leader".
	oldLeader := withSelfLeader(
		newTestPooler("zone1", "old", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE),
		1,
	)
	newLeader := withSelfLeader(
		newTestPooler("zone1", "new", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE),
		2,
	)
	require.NoError(t, ts.CreateMultiPooler(ctx, oldLeader))
	require.NoError(t, ts.CreateMultiPooler(ctx, newLeader))
	waitForPoolerCount(t, mg, 2)

	// Wait for both to be reflected in the shard summary; the LB merges each
	// self_leadership observation on OnLive synchronously, so by the time
	// PoolerCount=2 the summary is up to date.
	require.Eventually(t, func() bool {
		statuses := mg.collectCellStatuses()
		var newRole, oldRole string
		for _, cs := range statuses {
			for _, p := range cs.Poolers {
				if p.Name == "new" {
					newRole = p.Leadership
				}
				if p.Name == "old" {
					oldRole = p.Leadership
				}
			}
		}
		return newRole == "leader" && oldRole == "stale-leader"
	}, 2*time.Second, 5*time.Millisecond, "expected new=leader, old=stale-leader")
}

// TestCollectCellStatuses_GhostHasEmptyLeadership verifies that a pooler
// recorded as SHUTDOWN in topology shows up in CellStatuses (as a ghost) but
// reports an empty leadership string — there is no rider in the cache for it,
// so LeadershipForID returns "".
func TestCollectCellStatuses_GhostHasEmptyLeadership(t *testing.T) {
	mg, ts := newTestGateway(t, "zone1")
	ctx := t.Context()

	// Create a SHUTDOWN pooler. The cache's "cold-shutdown discovery"
	// path records it as a ghost — no rider, no OnLive — but it still
	// appears in CellStatuses for operator visibility.
	ghost := newTestPooler("zone1", "ghost1", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN)
	require.NoError(t, ts.CreateMultiPooler(ctx, ghost))

	// Wait until the ghost shows up in collectCellStatuses (since PoolerCount
	// reflects only live entries, we poll the status directly).
	require.Eventually(t, func() bool {
		for _, cs := range mg.collectCellStatuses() {
			for _, p := range cs.Poolers {
				if p.Name == "ghost1" {
					return true
				}
			}
		}
		return false
	}, 2*time.Second, 5*time.Millisecond, "ghost pooler did not appear in CellStatuses")

	statuses := mg.collectCellStatuses()
	ps := findPoolerStatus(t, statuses, "zone1", "ghost1")
	assert.Equal(t, "", ps.Leadership, "ghost pooler must have empty leadership")
	assert.Equal(t, "shutdown", ps.Lifecycle)
}
