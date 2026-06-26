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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
)

// poolerID returns the expected ID format for a pooler.
// Uses the same format as loadBalancer internally.
func poolerID(pooler *clustermetadatapb.MultiPooler) topoclient.ComponentID {
	return topoclient.ComponentIDString(pooler.Id)
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
			Database:   constants.DefaultPostgresDatabase,
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
	lb := newTestLB(t, "zone1")

	// Initially empty
	assert.Equal(t, 0, lb.connectionCount())

	// Add a pooler
	pooler := createTestMultiPooler("pooler1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	addPoolerForTest(t, lb, pooler)
	assert.Equal(t, 1, lb.connectionCount())

	// Adding same pooler again is a no-op (but updates info)
	addPoolerForTest(t, lb, pooler)
	assert.Equal(t, 1, lb.connectionCount())

	// Updating pooler type (simulating topology update from UNKNOWN to PRIMARY)
	poolerUpdated := createTestMultiPooler("pooler1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	addPoolerForTest(t, lb, poolerUpdated)
	assert.Equal(t, 1, lb.connectionCount(), "should still have only one connection")

	// Verify the type was updated via GetConnection
	target := protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup, "0", query.Mode_MODE_INCONSISTENT)
	conn, err := lb.getConnection(target)
	require.NoError(t, err)
	assert.Equal(t, clustermetadatapb.PoolerType_REPLICA, conn.PoolerInfo().Type, "pooler type should be updated")

	// Remove the pooler
	removePoolerForTest(t, lb, poolerID(pooler))
	assert.Equal(t, 0, lb.connectionCount())

	// Removing non-existent pooler is a no-op
	removePoolerForTest(t, lb, "multipooler-zone1-nonexistent")
	assert.Equal(t, 0, lb.connectionCount())
}

func TestLoadBalancer_GetConnection_Primary(t *testing.T) {
	lb := newTestLB(t, "zone1")

	// Add a primary and simulate health update to populate cache
	primary := createTestMultiPooler("primary1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	addPoolerForTest(t, lb, primary)

	connPrimary := connForTest(t, lb, primary)
	simulateHealthUpdate(connPrimary, clustermetadatapb.PoolerServingStatus_SERVING,
		&clustermetadatapb.LeaderObservation{LeaderId: primary.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1}})

	// Should find the primary via cache
	target := protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup, "0", query.Mode_MODE_WRITABLE)
	conn, err := lb.getConnection(target)
	require.NoError(t, err)
	assert.Equal(t, poolerID(primary), conn.ID())
}

func TestLoadBalancer_GetConnection_ReplicaPreferLocalCell(t *testing.T) {
	lb := newTestLB(t, "zone1")

	// Add replicas in both cells
	localReplica := createTestMultiPooler("local-replica", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	remoteReplica := createTestMultiPooler("remote-replica", "zone2", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	addPoolerForTest(t, lb, localReplica)
	addPoolerForTest(t, lb, remoteReplica)

	// Should prefer local cell for replicas
	target := protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup, "", query.Mode_MODE_INCONSISTENT)
	conn, err := lb.getConnection(target)
	require.NoError(t, err)
	assert.Equal(t, poolerID(localReplica), conn.ID(), "Should prefer local cell for replicas")
}

func TestLoadBalancer_GetConnection_CrossCellPrimary(t *testing.T) {
	lb := newTestLB(t, "zone1")

	// Add primary only in remote cell and simulate health update
	remotePrimary := createTestMultiPooler("remote-primary", "zone2", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	addPoolerForTest(t, lb, remotePrimary)

	connRemote := connForTest(t, lb, remotePrimary)
	simulateHealthUpdate(connRemote, clustermetadatapb.PoolerServingStatus_SERVING,
		&clustermetadatapb.LeaderObservation{LeaderId: remotePrimary.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1}})

	// Should find primary in remote cell via cache
	target := protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup, "0", query.Mode_MODE_WRITABLE)
	conn, err := lb.getConnection(target)
	require.NoError(t, err)
	assert.Equal(t, poolerID(remotePrimary), conn.ID(), "Should find primary in remote cell")
}

func TestLoadBalancer_GetConnection_NilTarget(t *testing.T) {
	lb := newTestLB(t, "zone1")

	_, err := lb.getConnection(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "target cannot be nil")
}

func TestLoadBalancer_GetConnection_NoMatch(t *testing.T) {
	lb := newTestLB(t, "zone1")

	// Add a primary (a real PRIMARY record carries self_leadership).
	primary := withSelfLeadership(createTestMultiPooler("primary1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY), 1)
	addPoolerForTest(t, lb, primary)

	// Request a replica - should not find one
	target := protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup, "", query.Mode_MODE_INCONSISTENT)
	_, err := lb.getConnection(target)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no pooler found")
}

func TestLoadBalancer_GetConnection_ShardMatch(t *testing.T) {
	lb := newTestLB(t, "zone1")

	// Add primaries for different shards and simulate health updates
	shard0 := createTestMultiPooler("primary-shard0", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	shard1 := createTestMultiPooler("primary-shard1", "zone1", constants.DefaultTableGroup, "1", clustermetadatapb.PoolerType_PRIMARY)
	addPoolerForTest(t, lb, shard0)
	addPoolerForTest(t, lb, shard1)

	connShard0 := connForTest(t, lb, shard0)
	connShard1 := connForTest(t, lb, shard1)
	simulateHealthUpdate(connShard0, clustermetadatapb.PoolerServingStatus_SERVING,
		&clustermetadatapb.LeaderObservation{LeaderId: shard0.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1}})
	simulateHealthUpdate(connShard1, clustermetadatapb.PoolerServingStatus_SERVING,
		&clustermetadatapb.LeaderObservation{LeaderId: shard1.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1}})

	// Request specific shard — should find correct primary via cache
	target := protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup, "1", query.Mode_MODE_WRITABLE)
	conn, err := lb.getConnection(target)
	require.NoError(t, err)
	assert.Equal(t, poolerID(shard1), conn.ID())
}

// TODO: Add concurrent access tests:
// - TestLoadBalancer_ConcurrentAddRemove: Multiple goroutines adding/removing poolers
// - TestLoadBalancer_ConcurrentGetConnection: GetConnection while poolers are being added/removed
// - TestLoadBalancer_RemoveWhileInUse: Remove a pooler that's currently being used for a query

// simulateHealthUpdate simulates receiving a health update from the stream.
// This uses the same code path as real health updates, ensuring any callbacks are triggered.
func simulateHealthUpdate(conn *poolerConnection, status clustermetadatapb.PoolerServingStatus, observation *clustermetadatapb.LeaderObservation) {
	info := conn.PoolerInfo()
	conn.processHealthResponse(&multipoolerservice.StreamPoolerHealthResponse{
		PoolerId:          info.Id,
		ServingStatus:     status,
		LeaderObservation: observation,
	})
}

// TestLoadBalancer_WriteResumeWaitsForWritable verifies the buffer-drain gate
// (notifyLeaderServingFromSummary → onLeaderServing) does NOT resume write
// traffic for a leader that is the observed leader and SERVING but not yet
// writable (still in recovery mid-promotion). It resumes only once the leader
// reports writable.
func TestLoadBalancer_WriteResumeWaitsForWritable(t *testing.T) {
	lb := newTestLB(t, "zone1")

	var resumed int
	lb.onLeaderServing = func(_ *clustermetadatapb.ShardKey) { resumed++ }

	p := createTestMultiPooler("primary1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	addPoolerForTest(t, lb, p)
	conn := connForTest(t, lb, p)

	obs := &clustermetadatapb.LeaderObservation{
		LeaderId:         p.Id,
		LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
	}
	injectHealth := func(writable bool) {
		conn.processHealthResponse(&multipoolerservice.StreamPoolerHealthResponse{
			PoolerId:          p.Id,
			ServingStatus:     clustermetadatapb.PoolerServingStatus_SERVING,
			LeaderObservation: obs,
			Writable:          writable,
		})
	}

	// Observed leader, SERVING, but still in recovery: hold.
	injectHealth(false)
	assert.Equal(t, 0, resumed, "write traffic must stay buffered until the leader is writable")

	// Out of recovery: writable → resume.
	injectHealth(true)
	assert.Equal(t, 1, resumed, "resume once the leader reports writable")
}

func TestLoadBalancer_PrimaryCaching(t *testing.T) {
	t.Run("highest term wins", func(t *testing.T) {
		lb := newTestLB(t, "zone1")

		primary1 := createTestMultiPooler("primary1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
		primary2 := createTestMultiPooler("primary2", "zone2", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
		replica1 := createTestMultiPooler("replica1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)

		addPoolerForTest(t, lb, primary1)
		addPoolerForTest(t, lb, primary2)
		addPoolerForTest(t, lb, replica1)

		connPrimary1 := connForTest(t, lb, primary1)
		connPrimary2 := connForTest(t, lb, primary2)
		connReplica1 := connForTest(t, lb, replica1)

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

		target := protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup, "0", query.Mode_MODE_WRITABLE)
		conn, err := lb.getConnection(target)
		require.NoError(t, err)
		assert.Equal(t, poolerID(primary2), conn.ID(), "Should select primary with highest term")
	})

	t.Run("replica reports higher term primary", func(t *testing.T) {
		lb := newTestLB(t, "zone1")

		primary1 := createTestMultiPooler("primary1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
		primary2 := createTestMultiPooler("primary2", "zone2", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
		replica1 := createTestMultiPooler("replica1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)

		addPoolerForTest(t, lb, primary1)
		addPoolerForTest(t, lb, primary2)
		addPoolerForTest(t, lb, replica1)

		connPrimary1 := connForTest(t, lb, primary1)
		connPrimary2 := connForTest(t, lb, primary2)
		connReplica1 := connForTest(t, lb, replica1)

		// primary1 thinks primary1 is leader with term 15
		simulateHealthUpdate(connPrimary1,
			clustermetadatapb.PoolerServingStatus_SERVING,
			&clustermetadatapb.LeaderObservation{LeaderId: primary1.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 15}})

		// primary2 thinks primary2 is leader with term 12 (stale)
		simulateHealthUpdate(connPrimary2,
			clustermetadatapb.PoolerServingStatus_DISABLED,
			&clustermetadatapb.LeaderObservation{LeaderId: primary2.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 12}})

		// replica1 observed the new leader (primary1) with term 20 (highest)
		simulateHealthUpdate(connReplica1,
			clustermetadatapb.PoolerServingStatus_SERVING,
			&clustermetadatapb.LeaderObservation{LeaderId: primary1.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 20}})

		target := protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup, "0", query.Mode_MODE_WRITABLE)
		conn, err := lb.getConnection(target)
		require.NoError(t, err)
		assert.Equal(t, poolerID(primary1), conn.ID(), "Should trust replica's observation with highest term")
	})

	t.Run("no observations uses topology self_leadership", func(t *testing.T) {
		lb := newTestLB(t, "zone1")

		primary := withSelfLeadership(createTestMultiPooler("primary1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY), 1)
		replica := createTestMultiPooler("replica1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)

		addPoolerForTest(t, lb, primary)
		addPoolerForTest(t, lb, replica)

		// No health updates — but the primary's self_leadership record names it
		// the leader, so the gateway can route before any health stream connects.
		target := protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup, "0", query.Mode_MODE_WRITABLE)
		conn, err := lb.getConnection(target)
		require.NoError(t, err)
		assert.Equal(t, poolerID(primary), conn.ID(),
			"Should return the self_leadership-named primary before health stream connects")
	})

	t.Run("leader identity preserved while another pooler remains in shard", func(t *testing.T) {
		lb := newTestLB(t, "zone1")

		primary := createTestMultiPooler("primary1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
		replica := createTestMultiPooler("replica1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
		addPoolerForTest(t, lb, primary)
		addPoolerForTest(t, lb, replica)

		connPrimary := connForTest(t, lb, primary)

		// Health update populates the leader entry with a real (term > 0)
		// observation.
		simulateHealthUpdate(connPrimary,
			clustermetadatapb.PoolerServingStatus_SERVING,
			&clustermetadatapb.LeaderObservation{LeaderId: primary.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1}})

		// Verify routing
		target := protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup, "0", query.Mode_MODE_WRITABLE)
		conn, err := lb.getConnection(target)
		require.NoError(t, err)
		assert.Equal(t, poolerID(primary), conn.ID())

		// Remove the primary pooler — leader identity must persist as long as
		// some pooler from the shard remains in the cache. GetConnection
		// distinguishes "known but not connected" (transient, after removal)
		// from "no leader observed yet" (operational, never saw consensus).
		removePoolerForTest(t, lb, poolerID(primary))

		_, err = lb.getConnection(target)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not connected",
			"Leader identity must persist while another pooler remains in the shard")
	})
}

func TestLoadBalancer_PrimaryCachedFromDiscovery(t *testing.T) {
	lb := newTestLB(t, "zone1")

	// AddPooler with a self_leadership record seeds the cache — no health update needed
	primary := withSelfLeadership(createTestMultiPooler("primary1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY), 1)
	addPoolerForTest(t, lb, primary)

	target := protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup, "0", query.Mode_MODE_WRITABLE)
	conn, err := lb.getConnection(target)
	require.NoError(t, err)
	assert.Equal(t, poolerID(primary), conn.ID(), "Should find primary seeded from self_leadership")
}

func TestLoadBalancer_KnownLeaderSurvivesTopologyDemotion(t *testing.T) {
	lb := newTestLB(t, "zone1")

	pooler := withSelfLeadership(createTestMultiPooler("pooler1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY), 5)
	addPoolerForTest(t, lb, pooler)

	// Health stream confirms the same leader at the same rule.
	conn := connForTest(t, lb, pooler)
	simulateHealthUpdate(conn, clustermetadatapb.PoolerServingStatus_SERVING,
		&clustermetadatapb.LeaderObservation{LeaderId: pooler.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 5}})

	// The pooler is re-discovered demoted: Type=REPLICA and self_leadership
	// cleared. mergeTopologyLeaderLocked must NOT erase the known leader — only
	// a higher observation from a new leader supersedes it.
	poolerAsReplica := createTestMultiPooler("pooler1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	addPoolerForTest(t, lb, poolerAsReplica)

	target := protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup, "0", query.Mode_MODE_WRITABLE)
	conn2, err := lb.getConnection(target)
	require.NoError(t, err)
	assert.Equal(t, poolerID(pooler), conn2.ID(),
		"Known leader must persist until a higher observation supersedes it")
}

func TestLoadBalancer_UnknownTypePrimarySelection(t *testing.T) {
	lb := newTestLB(t, "zone1")

	// Create UNKNOWN-type poolers (simulating initial discovery before multiorch assigns types)
	unknown1 := createTestMultiPooler("pooler1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_UNKNOWN)
	unknown2 := createTestMultiPooler("pooler2", "zone2", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_UNKNOWN)

	addPoolerForTest(t, lb, unknown1)
	addPoolerForTest(t, lb, unknown2)

	connUnknown1 := connForTest(t, lb, unknown1)
	connUnknown2 := connForTest(t, lb, unknown2)

	t.Run("UNKNOWN poolers without observations return error", func(t *testing.T) {
		// No LeaderObservation set - simulates initial state before health stream
		simulateHealthUpdate(connUnknown1, clustermetadatapb.PoolerServingStatus_SERVING, nil)
		simulateHealthUpdate(connUnknown2, clustermetadatapb.PoolerServingStatus_SERVING, nil)

		target := protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup, "0", query.Mode_MODE_WRITABLE)
		_, err := lb.getConnection(target)
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

		target := protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup, "0", query.Mode_MODE_WRITABLE)
		// The health callback caches the observed primary with highest term (unknown2 at term 10).
		// The cache returns the identified pooler regardless of type —
		// the observation is authoritative.
		conn, err := lb.getConnection(target)
		require.NoError(t, err)
		assert.Equal(t, poolerID(unknown2), conn.ID(),
			"Observation takes precedence over pooler type")
	})
}

func TestLoadBalancer_SelectReplicaByLocalityAndServingStatus(t *testing.T) {
	lb := newTestLB(t, "zone1")

	// Create replicas in different cells
	localReplica1 := createTestMultiPooler("local-replica1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	localReplica2 := createTestMultiPooler("local-replica2", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	remoteReplica := createTestMultiPooler("remote-replica", "zone2", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)

	addPoolerForTest(t, lb, localReplica1)
	addPoolerForTest(t, lb, localReplica2)
	addPoolerForTest(t, lb, remoteReplica)

	connLocal1 := connForTest(t, lb, localReplica1)
	connLocal2 := connForTest(t, lb, localReplica2)
	connRemote := connForTest(t, lb, remoteReplica)

	t.Run("prefers local serving over remote serving", func(t *testing.T) {
		simulateHealthUpdate(connLocal1, clustermetadatapb.PoolerServingStatus_DISABLED, nil)
		simulateHealthUpdate(connLocal2, clustermetadatapb.PoolerServingStatus_SERVING, nil)
		simulateHealthUpdate(connRemote, clustermetadatapb.PoolerServingStatus_SERVING, nil)

		target := protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup, "", query.Mode_MODE_INCONSISTENT)
		conn, err := lb.getConnection(target)
		require.NoError(t, err)
		assert.Equal(t, poolerID(localReplica2), conn.ID(),
			"Should prefer local serving replica over remote serving")
	})

	t.Run("falls back to remote serving when no local serving", func(t *testing.T) {
		simulateHealthUpdate(connLocal1, clustermetadatapb.PoolerServingStatus_DISABLED, nil)
		simulateHealthUpdate(connLocal2, clustermetadatapb.PoolerServingStatus_DISABLED, nil)
		simulateHealthUpdate(connRemote, clustermetadatapb.PoolerServingStatus_SERVING, nil)

		target := protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup, "", query.Mode_MODE_INCONSISTENT)
		conn, err := lb.getConnection(target)
		require.NoError(t, err)
		assert.Equal(t, poolerID(remoteReplica), conn.ID(),
			"Should fall back to remote serving when no local serving")
	})

	t.Run("falls back to local not-serving when no serving", func(t *testing.T) {
		simulateHealthUpdate(connLocal1, clustermetadatapb.PoolerServingStatus_DISABLED, nil)
		simulateHealthUpdate(connLocal2, clustermetadatapb.PoolerServingStatus_DISABLED, nil)
		simulateHealthUpdate(connRemote, clustermetadatapb.PoolerServingStatus_DISABLED, nil)

		target := protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup, "", query.Mode_MODE_INCONSISTENT)
		conn, err := lb.getConnection(target)
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
	lb := newTestLB(t, "zone1")

	// observer is in the shard but is NOT the leader; we use its health stream
	// to deliver an observation naming a pooler we have not added yet.
	observer := createTestMultiPooler("observer", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	addPoolerForTest(t, lb, observer)

	connObserver := connForTest(t, lb, observer)

	// The future leader exists in topology but has not been added yet.
	futureLeader := createTestMultiPooler("future-leader", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)

	// observer reports that future-leader is the consensus leader at term 7.
	simulateHealthUpdate(connObserver,
		clustermetadatapb.PoolerServingStatus_SERVING,
		&clustermetadatapb.LeaderObservation{LeaderId: futureLeader.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 7}})

	target := protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup, "0", query.Mode_MODE_WRITABLE)

	// Leader identity must be recorded even though we have no connection.
	_, err := lb.getConnection(target)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not connected",
		"Identity must be recorded; the gateway should distinguish 'known but unreachable' from 'unknown'")

	// AddPooler the leader. No additional observation needed — routing must
	// work immediately, proving identity was not dropped at observation time.
	addPoolerForTest(t, lb, futureLeader)
	conn, err := lb.getConnection(target)
	require.NoError(t, err)
	assert.Equal(t, poolerID(futureLeader), conn.ID())
}

// TestLoadBalancer_StalePrimaryTypeDoesNotEvict is the regression for the
// failover archive: a demoted-then-restarted pooler re-asserts Type=PRIMARY
// in topology. The gateway must not redirect traffic to it; the known leader
// at the higher term wins.
func TestLoadBalancer_StalePrimaryTypeDoesNotEvict(t *testing.T) {
	lb := newTestLB(t, "zone1")

	demoted := createTestMultiPooler("demoted", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	newLeader := createTestMultiPooler("new-leader", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	addPoolerForTest(t, lb, demoted)
	addPoolerForTest(t, lb, newLeader)

	connNewLeader := connForTest(t, lb, newLeader)

	// new-leader's health stream confirms itself as leader at term 2 (the
	// post-failover state).
	simulateHealthUpdate(connNewLeader,
		clustermetadatapb.PoolerServingStatus_SERVING,
		&clustermetadatapb.LeaderObservation{LeaderId: newLeader.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 2}})

	target := protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup, "0", query.Mode_MODE_WRITABLE)
	conn, err := lb.getConnection(target)
	require.NoError(t, err)
	assert.Equal(t, poolerID(newLeader), conn.ID())

	// Now the demoted pooler's stale topology record flips to Type=PRIMARY
	// (its pod restarted before multiorch corrected etcd). The gateway sees
	// the same connection re-asserting itself as PRIMARY.
	demotedReasserted := createTestMultiPooler("demoted", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	addPoolerForTest(t, lb, demotedReasserted)

	// Both connections must still be present — discovery does not evict, and
	// the loadBalancer ignores the topology hint for leader identity.
	assert.Equal(t, 2, lb.connectionCount(),
		"Stale Type=PRIMARY assertion must NOT cause the real leader's connection to be dropped")

	// Routing must still go to new-leader, the consensus-elected term-2 leader.
	conn, err = lb.getConnection(target)
	require.NoError(t, err)
	assert.Equal(t, poolerID(newLeader), conn.ID(),
		"Stale topology must not redirect PRIMARY traffic away from the consensus-elected leader")
}

// TestLoadBalancer_TopologySelfLeadershipMergesByRule verifies that
// self_leadership observations read from topology are reconciled by rule
// number: a higher rule supersedes the known leader, while a stale lower rule
// is ignored.
func TestLoadBalancer_TopologySelfLeadershipMergesByRule(t *testing.T) {
	lb := newTestLB(t, "zone1")

	oldLeader := withSelfLeadership(createTestMultiPooler("old-leader", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY), 1)
	newLeader := withSelfLeadership(createTestMultiPooler("new-leader", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY), 2)
	addPoolerForTest(t, lb, oldLeader)
	addPoolerForTest(t, lb, newLeader)

	target := protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup, "0", query.Mode_MODE_WRITABLE)

	// new-leader's self_leadership (rule 2) supersedes old-leader's (rule 1).
	conn, err := lb.getConnection(target)
	require.NoError(t, err)
	assert.Equal(t, poolerID(newLeader), conn.ID(), "higher-rule self_leadership should win")

	// Re-discovering old-leader's stale rule-1 record must not move the leader back.
	addPoolerForTest(t, lb, withSelfLeadership(createTestMultiPooler("old-leader", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY), 1))
	conn, err = lb.getConnection(target)
	require.NoError(t, err)
	assert.Equal(t, poolerID(newLeader), conn.ID(), "stale lower-rule self_leadership must be ignored")
}

// TestLoadBalancer_ReplicaCandidatesExcludeLeader verifies that REPLICA
// selection consults the LeaderObservation instead of topology Type. A pooler
// that consensus has elected leader must never be considered a replica
// candidate, even if its topology Type still says REPLICA.
func TestLoadBalancer_ReplicaCandidatesExcludeLeader(t *testing.T) {
	lb := newTestLB(t, "zone1")

	// All three are Type=REPLICA in topology; one is actually leader.
	a := createTestMultiPooler("pooler-a", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	b := createTestMultiPooler("pooler-b", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	c := createTestMultiPooler("pooler-c", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	addPoolerForTest(t, lb, a)
	addPoolerForTest(t, lb, b)
	addPoolerForTest(t, lb, c)

	connA := connForTest(t, lb, a)

	// a observes itself as leader; b and c are eligible replica candidates.
	simulateHealthUpdate(connA,
		clustermetadatapb.PoolerServingStatus_SERVING,
		&clustermetadatapb.LeaderObservation{LeaderId: a.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1}})

	target := protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup, "0", query.Mode_MODE_INCONSISTENT)

	// Run several iterations: the leader must never be selected, regardless of
	// which non-leader the load balancer picks at random.
	for range 50 {
		conn, err := lb.getConnection(target)
		require.NoError(t, err)
		assert.NotEqual(t, poolerID(a), conn.ID(),
			"Known leader (pooler-a) must never be returned as a REPLICA candidate")
	}
}

// TestLoadBalancer_StaleLeaderExcludedFromReplicas verifies that a stale leader
// — a pooler whose own health observation still names it the leader at a rule
// older than the confirmed leader's — is not selected for replica reads, while
// a replica that already tracks the new leader is.
func TestLoadBalancer_StaleLeaderExcludedFromReplicas(t *testing.T) {
	lb := newTestLB(t, "zone1")

	newLeader := withSelfLeadership(createTestMultiPooler("new-leader", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY), 2)
	stale := createTestMultiPooler("stale", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	replica := createTestMultiPooler("replica", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	addPoolerForTest(t, lb, newLeader)
	addPoolerForTest(t, lb, stale)
	addPoolerForTest(t, lb, replica)

	connStale := connForTest(t, lb, stale)
	connReplica := connForTest(t, lb, replica)

	// stale still believes it is the leader at the old rule 1; replica already
	// tracks the new leader at rule 2.
	simulateHealthUpdate(connStale, clustermetadatapb.PoolerServingStatus_SERVING,
		&clustermetadatapb.LeaderObservation{LeaderId: stale.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1}})
	simulateHealthUpdate(connReplica, clustermetadatapb.PoolerServingStatus_SERVING,
		&clustermetadatapb.LeaderObservation{LeaderId: newLeader.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 2}})

	target := protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup, "0", query.Mode_MODE_INCONSISTENT)
	// Only `replica` is eligible: new-leader is the leader (self_leadership) and
	// stale believes itself the leader, so both are excluded.
	for range 10 {
		conn, err := lb.getConnection(target)
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
	lb := newTestLB(t, "zone1")

	// leader's etcd record lags: discovered without self_leadership, and its own
	// health stream has not reported, so believesSelfLeader(leader) is false.
	leader := createTestMultiPooler("leader", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	addPoolerForTest(t, lb, leader)

	// A peer's health stream has already named `leader` the shard leader at rule
	// 2, recorded in the central map. Set it directly rather than wiring a second
	// connection, mirroring how onPoolerHealthUpdate would populate it.
	setLeaderForTest(t, lb, constants.DefaultPostgresDatabase, constants.DefaultTableGroup, "0", &clustermetadatapb.LeaderObservation{
		LeaderId:         leader.Id,
		LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 2},
	})

	target := protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup, "0", query.Mode_MODE_INCONSISTENT)
	// `leader` is the only connection and is eligible despite the central map
	// naming it the leader, so it is returned rather than erroring as no-candidate.
	conn, err := lb.getConnection(target)
	require.NoError(t, err)
	assert.Equal(t, poolerID(leader), conn.ID(),
		"a centrally-known leader unaware of its own leadership is eligible for replica reads")
}

// TestLoadBalancer_LeadershipFor verifies the three roles reported for the
// admin/status page: the shard's consensus leader, a stale leader that still
// believes itself the leader at an older rule, and a plain follower.
func TestLoadBalancer_LeadershipFor(t *testing.T) {
	lb := newTestLB(t, "zone1")

	leader := withSelfLeadership(createTestMultiPooler("leader", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY), 2)
	stale := createTestMultiPooler("stale", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	follower := createTestMultiPooler("follower", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	addPoolerForTest(t, lb, leader)
	addPoolerForTest(t, lb, stale)
	addPoolerForTest(t, lb, follower)

	connLeader := connForTest(t, lb, leader)
	connStale := connForTest(t, lb, stale)
	connFollower := connForTest(t, lb, follower)

	// stale still believes it leads at the old rule 1; follower already tracks
	// the new leader at rule 2.
	simulateHealthUpdate(connStale, clustermetadatapb.PoolerServingStatus_SERVING,
		&clustermetadatapb.LeaderObservation{LeaderId: stale.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1}})
	simulateHealthUpdate(connFollower, clustermetadatapb.PoolerServingStatus_SERVING,
		&clustermetadatapb.LeaderObservation{LeaderId: leader.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 2}})

	assert.Equal(t, leadershipLeader, lb.leadershipFor(connLeader))
	assert.Equal(t, leadershipStaleLeader, lb.leadershipFor(connStale))
	assert.Equal(t, leadershipFollower, lb.leadershipFor(connFollower))
}

// TestLoadBalancer_ShardSummaryAutoClear verifies that removing the last
// pooler from a shard clears the shardSummary entry.
func TestLoadBalancer_ShardSummaryAutoClear(t *testing.T) {
	lb := newTestLB(t, "zone1")

	primary := withSelfLeadership(createTestMultiPooler("primary", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY), 1)
	addPoolerForTest(t, lb, primary)

	lb.mu.Lock()
	require.Len(t, lb.shards, 1, "shard should be tracked after adding a pooler")
	lb.mu.Unlock()

	removePoolerForTest(t, lb, poolerID(primary))

	lb.mu.Lock()
	assert.Empty(t, lb.shards, "shardSummary should be cleared once no poolers remain in the shard")
	lb.mu.Unlock()
}

// TestLoadBalancer_OnLeaderServingRequiresSelfNamedLeader is the regression
// guard for the buffer-drain race: a pooler can be SERVING (as a REPLICA)
// while consensus has just named it the new leader. Until the pooler's own
// broadcast names itself as leader, draining the failover buffer toward it
// would route writes to a queryServer that still rejects WRITABLE traffic
// with MTF01. We must only call OnLeaderServing once the pooler's most
// recent health snapshot self-identifies as leader — the gateway-side
// replacement for the dropped Target.PoolerType == PRIMARY check.
func TestLoadBalancer_OnLeaderServingRequiresSelfNamedLeader(t *testing.T) {
	var calls []*clustermetadatapb.ShardKey
	lb := newTestLBWithLeaderServing(t, "zone1", func(sk *clustermetadatapb.ShardKey) {
		calls = append(calls, sk)
	})

	// The eventual leader is also the observer here — it carries the
	// LeaderObservation that names itself, but the broadcast naming itself
	// only arrives after consensus completes the rule change.
	leader := withSelfLeadership(createTestMultiPooler("leader", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY), 2)
	addPoolerForTest(t, lb, leader)
	connLeader := connForTest(t, lb, leader)

	// First broadcast: pooler is SERVING but its broadcast still names the
	// OLD leader. This is the pre-promotion snapshot. Must not drain the
	// buffer — the pooler hasn't acknowledged being leader yet.
	oldLeaderID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "old-leader",
	}
	simulateHealthUpdate(connLeader,
		clustermetadatapb.PoolerServingStatus_SERVING,
		&clustermetadatapb.LeaderObservation{LeaderId: oldLeaderID, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 2}})
	assert.Empty(t, calls,
		"OnLeaderServing must not fire while the pooler's broadcast names a different leader")

	// Second broadcast: pooler now names itself in the broadcast and is
	// writable (postgres out of recovery). This is the post-OnStateChange,
	// post-promotion snapshot. Drain the buffer.
	connLeader.processHealthResponse(&multipoolerservice.StreamPoolerHealthResponse{
		PoolerId:          leader.Id,
		ServingStatus:     clustermetadatapb.PoolerServingStatus_SERVING,
		LeaderObservation: &clustermetadatapb.LeaderObservation{LeaderId: leader.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 2}},
		Writable:          true,
	})
	require.Len(t, calls, 1, "OnLeaderServing must fire once the pooler self-identifies as leader")
	assert.Equal(t, constants.DefaultTableGroup, calls[0].GetTableGroup())
	assert.Equal(t, "0", calls[0].GetShard())
}
