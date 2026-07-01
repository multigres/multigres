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
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
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

	// Add a primary that self-attests as leader on its health stream, so it is
	// excluded from replica reads.
	primary := createTestMultiPooler("primary1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	addPoolerForTest(t, lb, primary)
	simulateHealthUpdate(connForTest(t, lb, primary), clustermetadatapb.PoolerServingStatus_SERVING,
		&clustermetadatapb.LeaderObservation{LeaderId: primary.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1}})

	// Request a replica - should not find one (only the primary exists, and a
	// self-attesting leader is excluded from replica reads).
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

// TestLoadBalancer_WriteResumeWaitsForServingSelfNamedLeader verifies the
// buffer-drain gate (notifyLeaderServingFromSummary → onLeaderServing) does NOT
// resume write traffic while the leader's own broadcast names a different leader
// (pre-promotion), and DOES resume once the elected primary's own health
// snapshot self-identifies as leader AND is SERVING. Under the new model a
// self-naming LeaderObservation implies writability, so there is no separate
// writable gate — obs-presence is the writable signal.
func TestLoadBalancer_WriteResumeWaitsForServingSelfNamedLeader(t *testing.T) {
	lb := newTestLB(t, "zone1")

	var resumed int
	lb.onLeaderServing = func(_ *clustermetadatapb.ShardKey) { resumed++ }

	p := createTestMultiPooler("primary1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	addPoolerForTest(t, lb, p)
	conn := connForTest(t, lb, p)

	// SERVING, but the broadcast still names the OLD leader (pre-promotion):
	// no primary claim for this pooler → hold the buffer.
	oldLeaderID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "old-leader",
	}
	simulateHealthUpdate(conn, clustermetadatapb.PoolerServingStatus_SERVING,
		&clustermetadatapb.LeaderObservation{LeaderId: oldLeaderID, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 5}})
	assert.Equal(t, 0, resumed, "write traffic must stay buffered while the broadcast names a different leader")

	// The pooler's own broadcast now names itself as leader and is SERVING.
	// A self-naming observation implies writability → resume.
	simulateHealthUpdate(conn, clustermetadatapb.PoolerServingStatus_SERVING,
		&clustermetadatapb.LeaderObservation{LeaderId: p.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 5}})
	assert.Equal(t, 1, resumed, "resume once the leader observes itself as the serving primary")
}

// TestLoadBalancer_ConsistentBuffersUntilLeaderWritable verifies that CONSISTENT
// reads route to the writable leader, not to an appointed-but-not-yet-established
// one. A pooler appointed leader by consensus does not advertise itself until it
// is actually writable (out of recovery and its committed rule is active), so it
// carries no routing-primary claim. Until then CONSISTENT (like WRITABLE) has no
// leader to route to and returns UNAVAILABLE — the gateway buffers rather than
// serving read-your-writes traffic from a leader that may not yet have replayed
// the latest commits. Once the leader self-reports writable, CONSISTENT routes to
// it.
func TestLoadBalancer_ConsistentBuffersUntilLeaderWritable(t *testing.T) {
	lb := newTestLB(t, "zone1")

	p := createTestMultiPooler("appointee", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	addPoolerForTest(t, lb, p)
	conn := connForTest(t, lb, p)

	// Appointed leader, SERVING, but still establishing: its broadcast names the
	// prior leader (not yet writable itself), so it makes no routing-primary claim.
	oldLeaderID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "old-leader",
	}
	simulateHealthUpdate(conn, clustermetadatapb.PoolerServingStatus_SERVING,
		&clustermetadatapb.LeaderObservation{LeaderId: oldLeaderID, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 4}})

	consistent := protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup, "0", query.Mode_MODE_CONSISTENT)
	_, err := lb.getConnection(consistent)
	require.Error(t, err, "CONSISTENT must not route while no leader is writable")
	assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err),
		"a not-yet-established leader must buffer, not serve CONSISTENT reads")

	// The appointee establishes itself: out of recovery and active committed
	// leader, so it now advertises itself as the writable primary.
	simulateHealthUpdate(conn, clustermetadatapb.PoolerServingStatus_SERVING,
		&clustermetadatapb.LeaderObservation{LeaderId: p.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 5}})

	got, err := lb.getConnection(consistent)
	require.NoError(t, err, "CONSISTENT routes once the leader is writable")
	assert.Equal(t, poolerID(p), got.ID())
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

	t.Run("no health observation means no writable leader", func(t *testing.T) {
		lb := newTestLB(t, "zone1")

		// A PRIMARY topology record with self_leadership no longer seeds routing:
		// the primary set is driven purely by live health. With no self-naming
		// health observation, the shard has no writable leader and WRITABLE
		// routing must buffer.
		primary := withSelfLeadership(createTestMultiPooler("primary1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY), 1)
		replica := createTestMultiPooler("replica1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)

		addPoolerForTest(t, lb, primary)
		addPoolerForTest(t, lb, replica)

		target := protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup, "0", query.Mode_MODE_WRITABLE)
		_, err := lb.getConnection(target)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no leader observed yet",
			"self_leadership topology alone must not seed a writable leader")
	})

	t.Run("removing the elected primary falls back to the other overlapping primary", func(t *testing.T) {
		lb := newTestLB(t, "zone1")

		// Failover overlap: two poolers both currently claim primary via their
		// own health streams, at different rules. The higher-rule one is elected.
		primary1 := createTestMultiPooler("primary1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
		primary2 := createTestMultiPooler("primary2", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
		addPoolerForTest(t, lb, primary1)
		addPoolerForTest(t, lb, primary2)

		connPrimary1 := connForTest(t, lb, primary1)
		connPrimary2 := connForTest(t, lb, primary2)

		simulateHealthUpdate(connPrimary1,
			clustermetadatapb.PoolerServingStatus_SERVING,
			&clustermetadatapb.LeaderObservation{LeaderId: primary1.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1}})
		simulateHealthUpdate(connPrimary2,
			clustermetadatapb.PoolerServingStatus_SERVING,
			&clustermetadatapb.LeaderObservation{LeaderId: primary2.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 2}})

		target := protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup, "0", query.Mode_MODE_WRITABLE)
		conn, err := lb.getConnection(target)
		require.NoError(t, err)
		assert.Equal(t, poolerID(primary2), conn.ID(), "highest-rule primary is elected")

		// The elected primary leaves: onPoolerGone retracts its claim, and the
		// remaining overlapping primary takes over rather than the shard going
		// leaderless.
		removePoolerForTest(t, lb, poolerID(primary2))

		conn, err = lb.getConnection(target)
		require.NoError(t, err)
		assert.Equal(t, poolerID(primary1), conn.ID(),
			"retracting the elected primary must fall back to the other overlapping primary")
	})
}

// TestLoadBalancer_PrimaryLearnedFromHealth verifies that the writable leader is
// learned from a live self-naming health observation (not from topology
// discovery / self_leadership, which no longer seeds routing under the new
// model). Once the primary attests to itself on its health stream, WRITABLE
// routing resolves to it.
func TestLoadBalancer_PrimaryLearnedFromHealth(t *testing.T) {
	lb := newTestLB(t, "zone1")

	primary := createTestMultiPooler("primary1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	addPoolerForTest(t, lb, primary)

	connPrimary := connForTest(t, lb, primary)
	simulateHealthUpdate(connPrimary,
		clustermetadatapb.PoolerServingStatus_SERVING,
		&clustermetadatapb.LeaderObservation{LeaderId: primary.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1}})

	target := protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup, "0", query.Mode_MODE_WRITABLE)
	conn, err := lb.getConnection(target)
	require.NoError(t, err)
	assert.Equal(t, poolerID(primary), conn.ID(), "Should route to the primary learned from its health observation")
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

	t.Run("routing follows the self-naming health observation regardless of Type", func(t *testing.T) {
		// unknown2 attests to ITSELF as the writable primary on its health
		// stream; unknown1 (a peer) reports unknown2 as leader, which does not
		// make unknown1 a primary. Routing follows the self-naming observation
		// even though the topology Type is UNKNOWN — Type is irrelevant to
		// routing now.
		simulateHealthUpdate(connUnknown1,
			clustermetadatapb.PoolerServingStatus_SERVING,
			&clustermetadatapb.LeaderObservation{LeaderId: unknown2.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 10}})
		simulateHealthUpdate(connUnknown2,
			clustermetadatapb.PoolerServingStatus_SERVING,
			&clustermetadatapb.LeaderObservation{LeaderId: unknown2.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 10}})

		target := protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup, "0", query.Mode_MODE_WRITABLE)
		conn, err := lb.getConnection(target)
		require.NoError(t, err)
		assert.Equal(t, poolerID(unknown2), conn.ID(),
			"routing follows the self-naming primary regardless of topology Type")
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

// TestLoadBalancer_LeaderObservationBeforeConnection covers the "known but not
// connected" routing case: a pooler is known to be the writable primary for a
// shard, but the gateway does not yet hold a connection to it. WRITABLE routing
// must distinguish "known but unreachable" (buffer, transient) from "no leader
// observed yet". Under the new model the primary set is keyed by the
// self-naming pooler, so we seed the future leader's own claim directly (as
// onPoolerHealthUpdate would once its health stream connects); once the pooler
// is added, routing resolves to it.
func TestLoadBalancer_LeaderObservationBeforeConnection(t *testing.T) {
	lb := newTestLB(t, "zone1")

	// observer is in the shard but is NOT the leader; it keeps a shardSummary
	// alive so the shard is tracked before the leader connects.
	observer := createTestMultiPooler("observer", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	addPoolerForTest(t, lb, observer)

	// The future leader exists in topology but has not been added yet.
	futureLeader := createTestMultiPooler("future-leader", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)

	// future-leader's own primary claim (rule 7) is recorded before we hold a
	// connection to it — the identity the gateway must not drop.
	setLeaderForTest(t, lb, constants.DefaultPostgresDatabase, constants.DefaultTableGroup, "0",
		&clustermetadatapb.LeaderObservation{LeaderId: futureLeader.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 7}})

	target := protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup, "0", query.Mode_MODE_WRITABLE)

	// Leader identity is recorded even though we have no connection.
	_, err := lb.getConnection(target)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not connected",
		"Identity must be recorded; the gateway should distinguish 'known but unreachable' from 'unknown'")

	// Add the leader. No additional observation needed — routing must work
	// immediately, proving identity was not dropped.
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

// TestLoadBalancer_HealthObservationsMergeByRule verifies that concurrent
// self-naming primary claims delivered on health streams are reconciled by rule
// number: during a failover overlap two poolers both claim primary, and the
// higher-rule one is elected. When the elected primary stops naming itself
// (demoted — its next health snapshot names another leader) the set falls back
// to the remaining primary; when that one goes away too, the shard has no
// writable leader.
func TestLoadBalancer_HealthObservationsMergeByRule(t *testing.T) {
	lb := newTestLB(t, "zone1")

	oldLeader := createTestMultiPooler("old-leader", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	newLeader := createTestMultiPooler("new-leader", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	addPoolerForTest(t, lb, oldLeader)
	addPoolerForTest(t, lb, newLeader)

	connOld := connForTest(t, lb, oldLeader)
	connNew := connForTest(t, lb, newLeader)

	// Both poolers currently claim primary via their own health streams; the
	// higher rule (new-leader at rule 2) is elected over old-leader at rule 1.
	simulateHealthUpdate(connOld, clustermetadatapb.PoolerServingStatus_SERVING,
		&clustermetadatapb.LeaderObservation{LeaderId: oldLeader.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1}})
	simulateHealthUpdate(connNew, clustermetadatapb.PoolerServingStatus_SERVING,
		&clustermetadatapb.LeaderObservation{LeaderId: newLeader.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 2}})

	target := protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup, "0", query.Mode_MODE_WRITABLE)
	conn, err := lb.getConnection(target)
	require.NoError(t, err)
	assert.Equal(t, poolerID(newLeader), conn.ID(), "higher-rule self-naming observation should win")

	// new-leader retracts: its next health snapshot names old-leader instead of
	// itself, so its primary claim is cleared and routing falls back to
	// old-leader, the only remaining self-naming primary.
	simulateHealthUpdate(connNew, clustermetadatapb.PoolerServingStatus_SERVING,
		&clustermetadatapb.LeaderObservation{LeaderId: oldLeader.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 2}})
	conn, err = lb.getConnection(target)
	require.NoError(t, err)
	assert.Equal(t, poolerID(oldLeader), conn.ID(), "retracting the elected primary falls back to the remaining primary")

	// old-leader goes away too: onPoolerGone retracts its claim and the shard
	// has no writable leader, so WRITABLE routing buffers.
	removePoolerForTest(t, lb, poolerID(oldLeader))
	_, err = lb.getConnection(target)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no leader observed yet",
		"with no self-naming primary left, the shard has no writable leader")
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

// TestLoadBalancer_LeadershipFor verifies the three roles reported for the
// admin/status page: the shard's consensus leader, a stale leader that still
// believes itself the leader at an older rule, and a plain follower.
func TestLoadBalancer_LeadershipFor(t *testing.T) {
	lb := newTestLB(t, "zone1")

	leader := createTestMultiPooler("leader", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	stale := createTestMultiPooler("stale", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	follower := createTestMultiPooler("follower", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	addPoolerForTest(t, lb, leader)
	addPoolerForTest(t, lb, stale)
	addPoolerForTest(t, lb, follower)

	connLeader := connForTest(t, lb, leader)
	connStale := connForTest(t, lb, stale)
	connFollower := connForTest(t, lb, follower)

	// leader's own health stream names itself as the writable primary at rule 2,
	// electing it into the shard's primary set; stale still believes it leads at
	// the old rule 1; follower tracks the new leader at rule 2.
	simulateHealthUpdate(connLeader, clustermetadatapb.PoolerServingStatus_SERVING,
		&clustermetadatapb.LeaderObservation{LeaderId: leader.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 2}})
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

	primary := createTestMultiPooler("primary", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	addPoolerForTest(t, lb, primary)

	// A shardSummary is created from the first live health observation, not from
	// topology add — the primary set is driven purely by health under the new
	// model. Deliver a self-naming observation so the shard is tracked.
	connPrimary := connForTest(t, lb, primary)
	simulateHealthUpdate(connPrimary, clustermetadatapb.PoolerServingStatus_SERVING,
		&clustermetadatapb.LeaderObservation{LeaderId: primary.Id, LeaderRuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1}})

	lb.mu.Lock()
	shardCount := len(lb.shards)
	lb.mu.Unlock()
	require.Equal(t, 1, shardCount, "shard should be tracked after a health observation")

	removePoolerForTest(t, lb, poolerID(primary))

	lb.mu.Lock()
	shardCount = len(lb.shards)
	lb.mu.Unlock()
	assert.Zero(t, shardCount, "shardSummary should be cleared once no poolers remain in the shard")
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
	})
	require.Len(t, calls, 1, "OnLeaderServing must fire once the pooler self-identifies as leader")
	assert.Equal(t, constants.DefaultTableGroup, calls[0].GetTableGroup())
	assert.Equal(t, "0", calls[0].GetShard())
}
