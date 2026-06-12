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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/multigres/multigres/go/common/constants"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
)

// poolerID returns the expected ID format for a pooler.
// Uses the same format as LoadBalancer internally.
func poolerID(pooler *clustermetadatapb.MultiPooler) string {
	return poolerIDString(pooler.Id)
}

func createTestMultiPooler(name, cell, tableGroup, shard string, poolerType clustermetadatapb.PoolerType) *clustermetadatapb.MultiPooler {
	return &clustermetadatapb.MultiPooler{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      cell,
			Name:      name,
		},
		Hostname: name + ".example.com",
		ShardKey: &clustermetadatapb.ShardKey{
			TableGroup: tableGroup,
			Shard:      shard,
		},
		Type: poolerType,
		PortMap: map[string]int32{
			"grpc": 50051,
		},
	}
}

// withSelfLeadership sets the pooler's self_leadership observation naming itself
// as leader at the given coordinator term. This mirrors a real leader's topology
// record (Type=PRIMARY ⇒ self_leadership set), which is how the gateway learns a
// shard's leader from etcd at discovery time.
func withSelfLeadership(p *clustermetadatapb.MultiPooler, coordinatorTerm int64) *clustermetadatapb.MultiPooler {
	p.SelfLeadership = &clustermetadatapb.LeaderObservation{
		LeaderId:         p.Id,
		LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: coordinatorTerm},
	}
	return p
}

func TestLoadBalancer_AddRemovePooler(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer(context.Background(), "zone1", logger, grpc.WithTransportCredentials(insecure.NewCredentials()))

	// Initially empty
	assert.Equal(t, 0, lb.ConnectionCount())

	// Add a pooler
	pooler := createTestMultiPooler("pooler1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	err := lb.AddPooler(pooler)
	require.NoError(t, err)
	assert.Equal(t, 1, lb.ConnectionCount())

	// Adding same pooler again is a no-op (but updates info)
	err = lb.AddPooler(pooler)
	require.NoError(t, err)
	assert.Equal(t, 1, lb.ConnectionCount())

	// Updating pooler type (simulating topology update from UNKNOWN to PRIMARY)
	poolerUpdated := createTestMultiPooler("pooler1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	err = lb.AddPooler(poolerUpdated)
	require.NoError(t, err)
	assert.Equal(t, 1, lb.ConnectionCount(), "should still have only one connection")

	// Verify the type was updated via GetConnection
	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		Shard:      "0",
		PoolerType: clustermetadatapb.PoolerType_REPLICA,
	}
	conn, err := lb.GetConnection(target)
	require.NoError(t, err)
	assert.Equal(t, clustermetadatapb.PoolerType_REPLICA, conn.PoolerInfo().Type, "pooler type should be updated")

	// Remove the pooler
	lb.RemovePooler(poolerID(pooler))
	assert.Equal(t, 0, lb.ConnectionCount())

	// Removing non-existent pooler is a no-op
	lb.RemovePooler("multipooler-zone1-nonexistent")
	assert.Equal(t, 0, lb.ConnectionCount())
}

func TestLoadBalancer_GetConnection_Primary(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer(context.Background(), "zone1", logger, grpc.WithTransportCredentials(insecure.NewCredentials()))

	// Add a primary and simulate health update to populate cache
	primary := createTestMultiPooler("primary1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, lb.AddPooler(primary))

	lb.mu.Lock()
	connPrimary := lb.connections[poolerID(primary)]
	lb.mu.Unlock()
	simulateHealthUpdate(connPrimary, clustermetadatapb.PoolerServingStatus_SERVING,
		&clustermetadatapb.LeaderObservation{LeaderId: primary.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1}})

	// Should find the primary via cache
	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		Shard:      "0",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	conn, err := lb.GetConnection(target)
	require.NoError(t, err)
	assert.Equal(t, poolerID(primary), conn.ID())
}

func TestLoadBalancer_GetConnection_ReplicaPreferLocalCell(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer(context.Background(), "zone1", logger, grpc.WithTransportCredentials(insecure.NewCredentials()))

	// Add replicas in both cells
	localReplica := createTestMultiPooler("local-replica", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	remoteReplica := createTestMultiPooler("remote-replica", "zone2", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	require.NoError(t, lb.AddPooler(localReplica))
	require.NoError(t, lb.AddPooler(remoteReplica))

	// Should prefer local cell for replicas
	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		PoolerType: clustermetadatapb.PoolerType_REPLICA,
	}
	conn, err := lb.GetConnection(target)
	require.NoError(t, err)
	assert.Equal(t, poolerID(localReplica), conn.ID(), "Should prefer local cell for replicas")
}

func TestLoadBalancer_GetConnection_CrossCellPrimary(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer(context.Background(), "zone1", logger, grpc.WithTransportCredentials(insecure.NewCredentials()))

	// Add primary only in remote cell and simulate health update
	remotePrimary := createTestMultiPooler("remote-primary", "zone2", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, lb.AddPooler(remotePrimary))

	lb.mu.Lock()
	connRemote := lb.connections[poolerID(remotePrimary)]
	lb.mu.Unlock()
	simulateHealthUpdate(connRemote, clustermetadatapb.PoolerServingStatus_SERVING,
		&clustermetadatapb.LeaderObservation{LeaderId: remotePrimary.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1}})

	// Should find primary in remote cell via cache
	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		Shard:      "0",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	conn, err := lb.GetConnection(target)
	require.NoError(t, err)
	assert.Equal(t, poolerID(remotePrimary), conn.ID(), "Should find primary in remote cell")
}

func TestLoadBalancer_GetConnection_NilTarget(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer(context.Background(), "zone1", logger, grpc.WithTransportCredentials(insecure.NewCredentials()))

	_, err := lb.GetConnection(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "target cannot be nil")
}

func TestLoadBalancer_GetConnection_NoMatch(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer(context.Background(), "zone1", logger, grpc.WithTransportCredentials(insecure.NewCredentials()))

	// Add a primary (a real PRIMARY record carries self_leadership).
	primary := withSelfLeadership(createTestMultiPooler("primary1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY), 1)
	require.NoError(t, lb.AddPooler(primary))

	// Request a replica - should not find one
	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		PoolerType: clustermetadatapb.PoolerType_REPLICA,
	}
	_, err := lb.GetConnection(target)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no pooler found")
}

func TestLoadBalancer_GetConnection_ShardMatch(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer(context.Background(), "zone1", logger, grpc.WithTransportCredentials(insecure.NewCredentials()))

	// Add primaries for different shards and simulate health updates
	shard0 := createTestMultiPooler("primary-shard0", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	shard1 := createTestMultiPooler("primary-shard1", "zone1", constants.DefaultTableGroup, "1", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, lb.AddPooler(shard0))
	require.NoError(t, lb.AddPooler(shard1))

	lb.mu.Lock()
	connShard0 := lb.connections[poolerID(shard0)]
	connShard1 := lb.connections[poolerID(shard1)]
	lb.mu.Unlock()
	simulateHealthUpdate(connShard0, clustermetadatapb.PoolerServingStatus_SERVING,
		&clustermetadatapb.LeaderObservation{LeaderId: shard0.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1}})
	simulateHealthUpdate(connShard1, clustermetadatapb.PoolerServingStatus_SERVING,
		&clustermetadatapb.LeaderObservation{LeaderId: shard1.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1}})

	// Request specific shard — should find correct primary via cache
	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		Shard:      "1",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	conn, err := lb.GetConnection(target)
	require.NoError(t, err)
	assert.Equal(t, poolerID(shard1), conn.ID())
}

func TestLoadBalancer_Close(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer(context.Background(), "zone1", logger, grpc.WithTransportCredentials(insecure.NewCredentials()))

	// Add some poolers
	pooler1 := createTestMultiPooler("pooler1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	pooler2 := createTestMultiPooler("pooler2", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	require.NoError(t, lb.AddPooler(pooler1))
	require.NoError(t, lb.AddPooler(pooler2))
	assert.Equal(t, 2, lb.ConnectionCount())

	// Close should remove all connections
	err := lb.Close()
	require.NoError(t, err)
	assert.Equal(t, 0, lb.ConnectionCount())
}

// TODO: Add concurrent access tests:
// - TestLoadBalancer_ConcurrentAddRemove: Multiple goroutines adding/removing poolers
// - TestLoadBalancer_ConcurrentGetConnection: GetConnection while poolers are being added/removed
// - TestLoadBalancer_RemoveWhileInUse: Remove a pooler that's currently being used for a query

// simulateHealthUpdate simulates receiving a health update from the stream.
// This uses the same code path as real health updates, ensuring any callbacks are triggered.
func simulateHealthUpdate(conn *PoolerConnection, status clustermetadatapb.PoolerServingStatus, observation *clustermetadatapb.LeaderObservation) {
	info := conn.PoolerInfo()
	conn.processHealthResponse(&multipoolerservice.StreamPoolerHealthResponse{
		Target: &query.Target{
			TableGroup: info.GetShardKey().GetTableGroup(),
			Shard:      info.GetShardKey().GetShard(),
			PoolerType: info.Type,
		},
		PoolerId:          info.Id,
		ServingStatus:     status,
		LeaderObservation: observation,
	})
}

func TestLoadBalancer_PrimaryCaching(t *testing.T) {
	t.Run("highest term wins", func(t *testing.T) {
		logger := slog.Default()
		lb := NewLoadBalancer(context.Background(), "zone1", logger, grpc.WithTransportCredentials(insecure.NewCredentials()))

		primary1 := createTestMultiPooler("primary1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
		primary2 := createTestMultiPooler("primary2", "zone2", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
		replica1 := createTestMultiPooler("replica1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)

		require.NoError(t, lb.AddPooler(primary1))
		require.NoError(t, lb.AddPooler(primary2))
		require.NoError(t, lb.AddPooler(replica1))

		lb.mu.Lock()
		connPrimary1 := lb.connections[poolerID(primary1)]
		connPrimary2 := lb.connections[poolerID(primary2)]
		connReplica1 := lb.connections[poolerID(replica1)]
		lb.mu.Unlock()

		// primary1 thinks primary1 is leader with term 5
		simulateHealthUpdate(connPrimary1,
			clustermetadatapb.PoolerServingStatus_SERVING,
			&clustermetadatapb.LeaderObservation{LeaderId: primary1.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 5}})

		// primary2 thinks primary2 is leader with term 10 (higher)
		simulateHealthUpdate(connPrimary2,
			clustermetadatapb.PoolerServingStatus_SERVING,
			&clustermetadatapb.LeaderObservation{LeaderId: primary2.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 10}})

		// replica1 also thinks primary2 is leader with term 10
		simulateHealthUpdate(connReplica1,
			clustermetadatapb.PoolerServingStatus_SERVING,
			&clustermetadatapb.LeaderObservation{LeaderId: primary2.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 10}})

		target := &query.Target{
			TableGroup: constants.DefaultTableGroup,
			Shard:      "0",
			PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		}
		conn, err := lb.GetConnection(target)
		require.NoError(t, err)
		assert.Equal(t, poolerID(primary2), conn.ID(), "Should select primary with highest term")
	})

	t.Run("replica reports higher term primary", func(t *testing.T) {
		logger := slog.Default()
		lb := NewLoadBalancer(context.Background(), "zone1", logger, grpc.WithTransportCredentials(insecure.NewCredentials()))

		primary1 := createTestMultiPooler("primary1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
		primary2 := createTestMultiPooler("primary2", "zone2", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
		replica1 := createTestMultiPooler("replica1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)

		require.NoError(t, lb.AddPooler(primary1))
		require.NoError(t, lb.AddPooler(primary2))
		require.NoError(t, lb.AddPooler(replica1))

		lb.mu.Lock()
		connPrimary1 := lb.connections[poolerID(primary1)]
		connPrimary2 := lb.connections[poolerID(primary2)]
		connReplica1 := lb.connections[poolerID(replica1)]
		lb.mu.Unlock()

		// primary1 thinks primary1 is leader with term 15
		simulateHealthUpdate(connPrimary1,
			clustermetadatapb.PoolerServingStatus_SERVING,
			&clustermetadatapb.LeaderObservation{LeaderId: primary1.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 15}})

		// primary2 thinks primary2 is leader with term 12 (stale)
		simulateHealthUpdate(connPrimary2,
			clustermetadatapb.PoolerServingStatus_NOT_SERVING,
			&clustermetadatapb.LeaderObservation{LeaderId: primary2.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 12}})

		// replica1 observed the new leader (primary1) with term 20 (highest)
		simulateHealthUpdate(connReplica1,
			clustermetadatapb.PoolerServingStatus_SERVING,
			&clustermetadatapb.LeaderObservation{LeaderId: primary1.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 20}})

		target := &query.Target{
			TableGroup: constants.DefaultTableGroup,
			Shard:      "0",
			PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		}
		conn, err := lb.GetConnection(target)
		require.NoError(t, err)
		assert.Equal(t, poolerID(primary1), conn.ID(), "Should trust replica's observation with highest term")
	})

	t.Run("no observations uses topology self_leadership", func(t *testing.T) {
		logger := slog.Default()
		lb := NewLoadBalancer(context.Background(), "zone1", logger, grpc.WithTransportCredentials(insecure.NewCredentials()))

		primary := withSelfLeadership(createTestMultiPooler("primary1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY), 1)
		replica := createTestMultiPooler("replica1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)

		require.NoError(t, lb.AddPooler(primary))
		require.NoError(t, lb.AddPooler(replica))

		// No health updates — but the primary's self_leadership record names it
		// the leader, so the gateway can route before any health stream connects.
		target := &query.Target{
			TableGroup: constants.DefaultTableGroup,
			Shard:      "0",
			PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		}
		conn, err := lb.GetConnection(target)
		require.NoError(t, err)
		assert.Equal(t, poolerID(primary), conn.ID(),
			"Should return the self_leadership-named primary before health stream connects")
	})

	t.Run("leader identity preserved on pooler removal", func(t *testing.T) {
		logger := slog.Default()
		lb := NewLoadBalancer(context.Background(), "zone1", logger, grpc.WithTransportCredentials(insecure.NewCredentials()))

		primary := createTestMultiPooler("primary1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
		require.NoError(t, lb.AddPooler(primary))

		lb.mu.Lock()
		connPrimary := lb.connections[poolerID(primary)]
		lb.mu.Unlock()

		// Health update populates the leader entry with a real (term > 0)
		// observation.
		simulateHealthUpdate(connPrimary,
			clustermetadatapb.PoolerServingStatus_SERVING,
			&clustermetadatapb.LeaderObservation{LeaderId: primary.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1}})

		// Verify routing
		target := &query.Target{
			TableGroup: constants.DefaultTableGroup,
			Shard:      "0",
			PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		}
		conn, err := lb.GetConnection(target)
		require.NoError(t, err)
		assert.Equal(t, poolerID(primary), conn.ID())

		// Remove the pooler — leader identity must persist; GetConnection
		// distinguishes "known but not connected" (transient, after removal)
		// from "no leader observed yet" (operational, never saw consensus).
		lb.RemovePooler(poolerID(primary))

		_, err = lb.GetConnection(target)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not connected",
			"Leader identity must persist; error should distinguish from 'no leader observed yet'")
	})
}

func TestLoadBalancer_PrimaryCachedFromDiscovery(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer(context.Background(), "zone1", logger, grpc.WithTransportCredentials(insecure.NewCredentials()))

	// AddPooler with a self_leadership record seeds the cache — no health update needed
	primary := withSelfLeadership(createTestMultiPooler("primary1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY), 1)
	require.NoError(t, lb.AddPooler(primary))

	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		Shard:      "0",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	conn, err := lb.GetConnection(target)
	require.NoError(t, err)
	assert.Equal(t, poolerID(primary), conn.ID(), "Should find primary seeded from self_leadership")
}

func TestLoadBalancer_KnownLeaderSurvivesTopologyDemotion(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer(context.Background(), "zone1", logger, grpc.WithTransportCredentials(insecure.NewCredentials()))

	pooler := withSelfLeadership(createTestMultiPooler("pooler1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY), 5)
	require.NoError(t, lb.AddPooler(pooler))

	// Health stream confirms the same leader at the same rule.
	lb.mu.Lock()
	conn := lb.connections[poolerID(pooler)]
	lb.mu.Unlock()
	simulateHealthUpdate(conn, clustermetadatapb.PoolerServingStatus_SERVING,
		&clustermetadatapb.LeaderObservation{LeaderId: pooler.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 5}})

	// The pooler is re-discovered demoted: Type=REPLICA and self_leadership
	// cleared. mergeTopologyLeaderLocked must NOT erase the known leader — only
	// a higher observation from a new leader supersedes it.
	poolerAsReplica := createTestMultiPooler("pooler1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	require.NoError(t, lb.AddPooler(poolerAsReplica))

	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		Shard:      "0",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	conn2, err := lb.GetConnection(target)
	require.NoError(t, err)
	assert.Equal(t, poolerID(pooler), conn2.ID(),
		"Known leader must persist until a higher observation supersedes it")
}

func TestLoadBalancer_UnknownTypePrimarySelection(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer(context.Background(), "zone1", logger, grpc.WithTransportCredentials(insecure.NewCredentials()))

	// Create UNKNOWN-type poolers (simulating initial discovery before multiorch assigns types)
	unknown1 := createTestMultiPooler("pooler1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_UNKNOWN)
	unknown2 := createTestMultiPooler("pooler2", "zone2", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_UNKNOWN)

	require.NoError(t, lb.AddPooler(unknown1))
	require.NoError(t, lb.AddPooler(unknown2))

	lb.mu.Lock()
	connUnknown1 := lb.connections[poolerID(unknown1)]
	connUnknown2 := lb.connections[poolerID(unknown2)]
	lb.mu.Unlock()

	t.Run("UNKNOWN poolers without observations return error", func(t *testing.T) {
		// No LeaderObservation set - simulates initial state before health stream
		simulateHealthUpdate(connUnknown1, clustermetadatapb.PoolerServingStatus_SERVING, nil)
		simulateHealthUpdate(connUnknown2, clustermetadatapb.PoolerServingStatus_SERVING, nil)

		target := &query.Target{
			TableGroup: constants.DefaultTableGroup,
			Shard:      "0",
			PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		}
		_, err := lb.GetConnection(target)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no leader observed yet",
			"Should not fall back to UNKNOWN type poolers")
	})

	t.Run("UNKNOWN poolers with observation cached via health callback", func(t *testing.T) {
		// Both UNKNOWN poolers point to each other (pathological case)
		simulateHealthUpdate(connUnknown1,
			clustermetadatapb.PoolerServingStatus_SERVING,
			&clustermetadatapb.LeaderObservation{LeaderId: unknown2.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 10}})
		simulateHealthUpdate(connUnknown2,
			clustermetadatapb.PoolerServingStatus_SERVING,
			&clustermetadatapb.LeaderObservation{LeaderId: unknown1.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 5}})

		target := &query.Target{
			TableGroup: constants.DefaultTableGroup,
			Shard:      "0",
			PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		}
		// The health callback caches the observed primary with highest term (unknown2 at term 10).
		// The cache returns the identified pooler regardless of type —
		// the observation is authoritative.
		conn, err := lb.GetConnection(target)
		require.NoError(t, err)
		assert.Equal(t, poolerID(unknown2), conn.ID(),
			"Observation takes precedence over pooler type")
	})
}

func TestLoadBalancer_SelectReplicaByLocalityAndServingStatus(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer(context.Background(), "zone1", logger, grpc.WithTransportCredentials(insecure.NewCredentials()))

	// Create replicas in different cells
	localReplica1 := createTestMultiPooler("local-replica1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	localReplica2 := createTestMultiPooler("local-replica2", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	remoteReplica := createTestMultiPooler("remote-replica", "zone2", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)

	require.NoError(t, lb.AddPooler(localReplica1))
	require.NoError(t, lb.AddPooler(localReplica2))
	require.NoError(t, lb.AddPooler(remoteReplica))

	lb.mu.Lock()
	connLocal1 := lb.connections[poolerID(localReplica1)]
	connLocal2 := lb.connections[poolerID(localReplica2)]
	connRemote := lb.connections[poolerID(remoteReplica)]
	lb.mu.Unlock()

	t.Run("prefers local serving over remote serving", func(t *testing.T) {
		simulateHealthUpdate(connLocal1, clustermetadatapb.PoolerServingStatus_NOT_SERVING, nil)
		simulateHealthUpdate(connLocal2, clustermetadatapb.PoolerServingStatus_SERVING, nil)
		simulateHealthUpdate(connRemote, clustermetadatapb.PoolerServingStatus_SERVING, nil)

		target := &query.Target{
			TableGroup: constants.DefaultTableGroup,
			PoolerType: clustermetadatapb.PoolerType_REPLICA,
		}
		conn, err := lb.GetConnection(target)
		require.NoError(t, err)
		assert.Equal(t, poolerID(localReplica2), conn.ID(),
			"Should prefer local serving replica over remote serving")
	})

	t.Run("falls back to remote serving when no local serving", func(t *testing.T) {
		simulateHealthUpdate(connLocal1, clustermetadatapb.PoolerServingStatus_NOT_SERVING, nil)
		simulateHealthUpdate(connLocal2, clustermetadatapb.PoolerServingStatus_NOT_SERVING, nil)
		simulateHealthUpdate(connRemote, clustermetadatapb.PoolerServingStatus_SERVING, nil)

		target := &query.Target{
			TableGroup: constants.DefaultTableGroup,
			PoolerType: clustermetadatapb.PoolerType_REPLICA,
		}
		conn, err := lb.GetConnection(target)
		require.NoError(t, err)
		assert.Equal(t, poolerID(remoteReplica), conn.ID(),
			"Should fall back to remote serving when no local serving")
	})

	t.Run("falls back to local not-serving when no serving", func(t *testing.T) {
		simulateHealthUpdate(connLocal1, clustermetadatapb.PoolerServingStatus_NOT_SERVING, nil)
		simulateHealthUpdate(connLocal2, clustermetadatapb.PoolerServingStatus_NOT_SERVING, nil)
		simulateHealthUpdate(connRemote, clustermetadatapb.PoolerServingStatus_NOT_SERVING, nil)

		target := &query.Target{
			TableGroup: constants.DefaultTableGroup,
			PoolerType: clustermetadatapb.PoolerType_REPLICA,
		}
		conn, err := lb.GetConnection(target)
		require.NoError(t, err)
		// Should pick one of the local not-serving replicas
		assert.Equal(t, "zone1", conn.Cell(),
			"Should fall back to local not-serving when no serving replicas")
	})
}

// TestLoadBalancer_LeaderObservationBeforeConnection covers the silent-drop
// race from the failover archive: a higher-term LeaderObservation arrives for
// pooler X before X has been added via AddPooler. With the old shape, the
// observation was dropped because the connection didn't exist yet. With the
// new shape, leader identity is recorded regardless and routing works the
// moment X is added.
func TestLoadBalancer_LeaderObservationBeforeConnection(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer(context.Background(), "zone1", logger, grpc.WithTransportCredentials(insecure.NewCredentials()))

	// observer is in the shard but is NOT the leader; we use its health stream
	// to deliver an observation naming a pooler we have not added yet.
	observer := createTestMultiPooler("observer", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	require.NoError(t, lb.AddPooler(observer))

	lb.mu.Lock()
	connObserver := lb.connections[poolerID(observer)]
	lb.mu.Unlock()

	// The future leader exists in topology but has not been added yet.
	futureLeader := createTestMultiPooler("future-leader", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)

	// observer reports that future-leader is the consensus leader at term 7.
	simulateHealthUpdate(connObserver,
		clustermetadatapb.PoolerServingStatus_SERVING,
		&clustermetadatapb.LeaderObservation{LeaderId: futureLeader.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 7}})

	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		Shard:      "0",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}

	// Leader identity must be recorded even though we have no connection.
	_, err := lb.GetConnection(target)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not connected",
		"Identity must be recorded; the gateway should distinguish 'known but unreachable' from 'unknown'")

	// AddPooler the leader. No additional observation needed — routing must
	// work immediately, proving identity was not dropped at observation time.
	require.NoError(t, lb.AddPooler(futureLeader))
	conn, err := lb.GetConnection(target)
	require.NoError(t, err)
	assert.Equal(t, poolerID(futureLeader), conn.ID())
}

// TestLoadBalancer_StalePrimaryTypeDoesNotEvict is the regression for the
// failover archive: a demoted-then-restarted pooler re-asserts Type=PRIMARY
// in topology. The gateway must not redirect traffic to it; the known leader
// at the higher term wins.
func TestLoadBalancer_StalePrimaryTypeDoesNotEvict(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer(context.Background(), "zone1", logger, grpc.WithTransportCredentials(insecure.NewCredentials()))

	demoted := createTestMultiPooler("demoted", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	newLeader := createTestMultiPooler("new-leader", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, lb.AddPooler(demoted))
	require.NoError(t, lb.AddPooler(newLeader))

	lb.mu.Lock()
	connNewLeader := lb.connections[poolerID(newLeader)]
	lb.mu.Unlock()

	// new-leader's health stream confirms itself as leader at term 2 (the
	// post-failover state).
	simulateHealthUpdate(connNewLeader,
		clustermetadatapb.PoolerServingStatus_SERVING,
		&clustermetadatapb.LeaderObservation{LeaderId: newLeader.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 2}})

	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		Shard:      "0",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	conn, err := lb.GetConnection(target)
	require.NoError(t, err)
	assert.Equal(t, poolerID(newLeader), conn.ID())

	// Now the demoted pooler's stale topology record flips to Type=PRIMARY
	// (its pod restarted before multiorch corrected etcd). The gateway sees
	// the same connection re-asserting itself as PRIMARY.
	demotedReasserted := createTestMultiPooler("demoted", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, lb.AddPooler(demotedReasserted))

	// Both connections must still be present — discovery does not evict, and
	// the LoadBalancer ignores the topology hint for leader identity.
	assert.Equal(t, 2, lb.ConnectionCount(),
		"Stale Type=PRIMARY assertion must NOT cause the real leader's connection to be dropped")

	// Routing must still go to new-leader, the consensus-elected term-2 leader.
	conn, err = lb.GetConnection(target)
	require.NoError(t, err)
	assert.Equal(t, poolerID(newLeader), conn.ID(),
		"Stale topology must not redirect PRIMARY traffic away from the consensus-elected leader")
}

// TestLoadBalancer_TopologySelfLeadershipMergesByRule verifies that
// self_leadership observations read from topology are reconciled by rule
// number: a higher rule supersedes the known leader, while a stale lower rule
// is ignored.
func TestLoadBalancer_TopologySelfLeadershipMergesByRule(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer(context.Background(), "zone1", logger, grpc.WithTransportCredentials(insecure.NewCredentials()))

	oldLeader := withSelfLeadership(createTestMultiPooler("old-leader", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY), 1)
	newLeader := withSelfLeadership(createTestMultiPooler("new-leader", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY), 2)
	require.NoError(t, lb.AddPooler(oldLeader))
	require.NoError(t, lb.AddPooler(newLeader))

	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		Shard:      "0",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}

	// new-leader's self_leadership (rule 2) supersedes old-leader's (rule 1).
	conn, err := lb.GetConnection(target)
	require.NoError(t, err)
	assert.Equal(t, poolerID(newLeader), conn.ID(), "higher-rule self_leadership should win")

	// Re-discovering old-leader's stale rule-1 record must not move the leader back.
	require.NoError(t, lb.AddPooler(withSelfLeadership(createTestMultiPooler("old-leader", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY), 1)))
	conn, err = lb.GetConnection(target)
	require.NoError(t, err)
	assert.Equal(t, poolerID(newLeader), conn.ID(), "stale lower-rule self_leadership must be ignored")
}

// TestLoadBalancer_ReplicaCandidatesExcludeLeader verifies that REPLICA
// selection consults the LeaderObservation instead of topology Type. A pooler
// that consensus has elected leader must never be considered a replica
// candidate, even if its topology Type still says REPLICA.
func TestLoadBalancer_ReplicaCandidatesExcludeLeader(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer(context.Background(), "zone1", logger, grpc.WithTransportCredentials(insecure.NewCredentials()))

	// All three are Type=REPLICA in topology; one is actually leader.
	a := createTestMultiPooler("pooler-a", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	b := createTestMultiPooler("pooler-b", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	c := createTestMultiPooler("pooler-c", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	require.NoError(t, lb.AddPooler(a))
	require.NoError(t, lb.AddPooler(b))
	require.NoError(t, lb.AddPooler(c))

	lb.mu.Lock()
	connA := lb.connections[poolerID(a)]
	lb.mu.Unlock()

	// a observes itself as leader; b and c are eligible replica candidates.
	simulateHealthUpdate(connA,
		clustermetadatapb.PoolerServingStatus_SERVING,
		&clustermetadatapb.LeaderObservation{LeaderId: a.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1}})

	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		Shard:      "0",
		PoolerType: clustermetadatapb.PoolerType_REPLICA,
	}

	// Run several iterations: the leader must never be selected, regardless of
	// which non-leader the load balancer picks at random.
	for range 50 {
		conn, err := lb.GetConnection(target)
		require.NoError(t, err)
		assert.NotEqual(t, poolerID(a), conn.ID(),
			"Known leader (pooler-a) must never be returned as a REPLICA candidate")
	}
}

func TestLoadBalancerListener(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer(context.Background(), "zone1", logger, grpc.WithTransportCredentials(insecure.NewCredentials()))
	listener := NewLoadBalancerListener(lb)

	// OnPoolerChanged should add pooler
	pooler := createTestMultiPooler("pooler1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	listener.OnPoolerChanged(pooler)
	assert.Equal(t, 1, lb.ConnectionCount())

	// OnPoolerRemoved should remove pooler
	listener.OnPoolerRemoved(pooler)
	assert.Equal(t, 0, lb.ConnectionCount())
}

// TestLoadBalancer_StaleLeaderExcludedFromReplicas verifies that a stale leader
// — a pooler whose own health observation still names it the leader at a rule
// older than the confirmed leader's — is not selected for replica reads, while
// a replica that already tracks the new leader is.
func TestLoadBalancer_StaleLeaderExcludedFromReplicas(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer(context.Background(), "zone1", logger, grpc.WithTransportCredentials(insecure.NewCredentials()))

	newLeader := withSelfLeadership(createTestMultiPooler("new-leader", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY), 2)
	stale := createTestMultiPooler("stale", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	replica := createTestMultiPooler("replica", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	require.NoError(t, lb.AddPooler(newLeader))
	require.NoError(t, lb.AddPooler(stale))
	require.NoError(t, lb.AddPooler(replica))

	lb.mu.Lock()
	connStale := lb.connections[poolerID(stale)]
	connReplica := lb.connections[poolerID(replica)]
	lb.mu.Unlock()

	// stale still believes it is the leader at the old rule 1; replica already
	// tracks the new leader at rule 2.
	simulateHealthUpdate(connStale, clustermetadatapb.PoolerServingStatus_SERVING,
		&clustermetadatapb.LeaderObservation{LeaderId: stale.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1}})
	simulateHealthUpdate(connReplica, clustermetadatapb.PoolerServingStatus_SERVING,
		&clustermetadatapb.LeaderObservation{LeaderId: newLeader.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 2}})

	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		Shard:      "0",
		PoolerType: clustermetadatapb.PoolerType_REPLICA,
	}
	// Only `replica` is eligible: new-leader is the leader (self_leadership) and
	// stale believes itself the leader, so both are excluded.
	for range 10 {
		conn, err := lb.GetConnection(target)
		require.NoError(t, err)
		assert.Equal(t, poolerID(replica), conn.ID(),
			"reads must avoid both the leader and the stale leader")
	}
}

// TestLoadBalancer_CentrallyKnownLeaderUnknownToItselfEligibleAsReplica
// documents the inverse of the stale-leader case: a pooler the central leaders
// map knows to be the leader — populated by a peer's health-stream observation
// — is still eligible for replica reads when its own view does not yet name it
// the leader (etcd self_leadership absent and its own health stream not yet
// reported). matchesReplicaTarget consults only the per-connection
// believesSelfLeader, never the central map, so the leader is not excluded here.
//
// This is benign: a read served by the actual current leader sees the most
// up-to-date data, so it cannot violate read consistency. The window is also
// narrow — it closes the moment the leader's own health stream reports or its
// etcd record gains self_leadership. The dangerous direction (serving from a
// stale leader on an older rule) is covered by the test above.
func TestLoadBalancer_CentrallyKnownLeaderUnknownToItselfEligibleAsReplica(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer(context.Background(), "zone1", logger, grpc.WithTransportCredentials(insecure.NewCredentials()))

	// leader's etcd record lags: discovered without self_leadership, and its own
	// health stream has not reported, so believesSelfLeader(leader) is false.
	leader := createTestMultiPooler("leader", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, lb.AddPooler(leader))

	// A peer's health stream has already named `leader` the shard leader at rule
	// 2, recorded in the central map. Set it directly rather than wiring a second
	// connection, mirroring how onPoolerHealthUpdate would populate it.
	key := shardKey{tableGroup: constants.DefaultTableGroup, shard: "0"}
	lb.mu.Lock()
	lb.leaders[key] = &clustermetadatapb.LeaderObservation{
		LeaderId:         leader.Id,
		LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 2},
	}
	lb.mu.Unlock()

	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		Shard:      "0",
		PoolerType: clustermetadatapb.PoolerType_REPLICA,
	}
	// `leader` is the only connection and is eligible despite the central map
	// naming it the leader, so it is returned rather than erroring as no-candidate.
	conn, err := lb.GetConnection(target)
	require.NoError(t, err)
	assert.Equal(t, poolerID(leader), conn.ID(),
		"a centrally-known leader unaware of its own leadership is eligible for replica reads")
}
