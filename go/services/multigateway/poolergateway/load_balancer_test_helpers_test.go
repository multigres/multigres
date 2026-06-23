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
	"log/slog"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/poolerwatch"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// newTestLB constructs a loadBalancer wired to an in-memory pooler cache that
// mirrors the production hook contract from multigateway init.go: OnLive
// constructs a *poolerConnection (with insecure transport for tests) and
// merges any topology self_leadership; OnUpdate refreshes pooler info and
// re-merges; OnGone closes the connection. The cache uses no topology
// Source, so it is driven by SeedForTest / DeleteForTest in tests.
func newTestLB(t *testing.T, localCell string) *loadBalancer {
	return newTestLBWithLeaderServing(t, localCell, nil)
}

// newTestLBWithLeaderServing is the variant of newTestLB used by tests that
// need to observe the OnLeaderServing callback (the buffer-drain trigger).
// Pass nil to behave exactly like newTestLB.
func newTestLBWithLeaderServing(t *testing.T, localCell string, onLeaderServing func(*clustermetadatapb.ShardKey)) *loadBalancer {
	t.Helper()
	logger := slog.Default()
	ctx := t.Context()
	dialOpt := grpc.WithTransportCredentials(insecure.NewCredentials())
	cache := poolerwatch.New(ctx, poolerwatch.Config[*poolerConnection]{
		Logger: logger,
	})
	lb := newLoadBalancer(loadBalancerOpts{
		Ctx:             ctx,
		LocalCell:       localCell,
		Logger:          logger,
		DialOpt:         dialOpt,
		Cache:           cache,
		OnLeaderServing: onLeaderServing,
	})
	cache.Start(poolerwatch.Hooks[*poolerConnection]{
		OnLive: func(p *clustermetadatapb.MultiPooler, _ *poolerConnection) *poolerConnection {
			conn, err := newPoolerConnection(ctx, p, logger, dialOpt, lb.onPoolerHealthUpdate)
			if err != nil {
				t.Errorf("newPoolerConnection failed: %v", err)
				return nil
			}
			lb.mergeTopologyLeader(p)
			lb.notifyIfLeaderServing(p, conn)
			return conn
		},
		OnUpdate: func(_, curr *clustermetadatapb.MultiPooler, conn *poolerConnection) {
			if conn == nil {
				return
			}
			conn.UpdatePoolerInfo(curr)
			lb.mergeTopologyLeader(curr)
			lb.notifyIfLeaderServing(curr, conn)
		},
		OnGone: func(p *clustermetadatapb.MultiPooler, conn *poolerConnection, _ poolerwatch.GoneReason) {
			if conn != nil {
				_ = conn.Shutdown()
			}
			lb.onPoolerGone(p)
		},
	})
	t.Cleanup(func() { cache.Shutdown() })
	return lb
}

// addPoolerForTest drives a topology upsert through the cache, firing OnLive
// (which constructs the *poolerConnection rider) or OnUpdate.
func addPoolerForTest(t *testing.T, lb *loadBalancer, p *clustermetadatapb.MultiPooler) {
	t.Helper()
	poolerwatch.SeedForTest(t, lb.cache, p)
}

// removePoolerForTest evicts a pooler from the cache, firing OnGone (which
// closes the connection).
func removePoolerForTest(t *testing.T, lb *loadBalancer, id topoclient.ComponentID) {
	t.Helper()
	poolerwatch.DeleteForTest(t, lb.cache, id)
}

// connForTest returns the cached *poolerConnection rider for the given
// pooler, or nil if absent. Used by tests that need to call into the
// connection (e.g. simulateHealthUpdate).
func connForTest(t *testing.T, lb *loadBalancer, p *clustermetadatapb.MultiPooler) *poolerConnection {
	t.Helper()
	conn, ok := lb.cache.GetRider(topoclient.ComponentIDString(p.Id))
	if !ok {
		return nil
	}
	return conn
}

// setLeaderForTest installs a LeaderObservation directly into the LB's
// per-shard leader map. Used by tests that need to model a peer observation
// without wiring a second connection.
func setLeaderForTest(t *testing.T, lb *loadBalancer, tableGroup, shard string, obs *clustermetadatapb.LeaderObservation) {
	t.Helper()
	lb.mu.Lock()
	defer lb.mu.Unlock()
	key := shardKey{tableGroup: tableGroup, shard: shard}
	lb.shards[key] = &shardSummary{
		shardKey: &clustermetadatapb.ShardKey{
			TableGroup: tableGroup,
			Shard:      shard,
		},
		leaderObs: obs,
	}
}
