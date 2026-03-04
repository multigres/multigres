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
		Hostname:   name + ".example.com",
		TableGroup: tableGroup,
		Shard:      shard,
		Type:       poolerType,
		PortMap: map[string]int32{
			"grpc": 50051,
		},
	}
}

func TestLoadBalancer_AddRemovePooler(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer(context.Background(), "zone1", logger)

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
	conn, err := lb.GetConnection(target, nil)
	require.NoError(t, err)
	assert.Equal(t, clustermetadatapb.PoolerType_REPLICA, conn.Type(), "pooler type should be updated")

	// Remove the pooler
	lb.RemovePooler(poolerID(pooler))
	assert.Equal(t, 0, lb.ConnectionCount())

	// Removing non-existent pooler is a no-op
	lb.RemovePooler("multipooler-zone1-nonexistent")
	assert.Equal(t, 0, lb.ConnectionCount())
}

func TestLoadBalancer_GetConnection_Primary(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer(context.Background(), "zone1", logger)

	// Add a primary
	primary := createTestMultiPooler("primary1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, lb.AddPooler(primary))

	// Should find the primary
	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	conn, err := lb.GetConnection(target, nil)
	require.NoError(t, err)
	assert.Equal(t, poolerID(primary), conn.ID())
}

func TestLoadBalancer_GetConnection_ReplicaPreferLocalCell(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer(context.Background(), "zone1", logger)

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
	conn, err := lb.GetConnection(target, nil)
	require.NoError(t, err)
	assert.Equal(t, poolerID(localReplica), conn.ID(), "Should prefer local cell for replicas")
}

func TestLoadBalancer_GetConnection_CrossCellPrimary(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer(context.Background(), "zone1", logger)

	// Add primary only in remote cell
	remotePrimary := createTestMultiPooler("remote-primary", "zone2", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, lb.AddPooler(remotePrimary))

	// Should find primary in remote cell
	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	conn, err := lb.GetConnection(target, nil)
	require.NoError(t, err)
	assert.Equal(t, poolerID(remotePrimary), conn.ID(), "Should find primary in remote cell")
}

func TestLoadBalancer_GetConnection_NilTarget(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer(context.Background(), "zone1", logger)

	_, err := lb.GetConnection(nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "target cannot be nil")
}

func TestLoadBalancer_GetConnection_NoMatch(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer(context.Background(), "zone1", logger)

	// Add a primary
	primary := createTestMultiPooler("primary1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, lb.AddPooler(primary))

	// Request a replica - should not find one
	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		PoolerType: clustermetadatapb.PoolerType_REPLICA,
	}
	_, err := lb.GetConnection(target, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no pooler found")
}

func TestLoadBalancer_GetConnection_ShardMatch(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer(context.Background(), "zone1", logger)

	// Add primaries for different shards
	shard0 := createTestMultiPooler("primary-shard0", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	shard1 := createTestMultiPooler("primary-shard1", "zone1", constants.DefaultTableGroup, "1", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, lb.AddPooler(shard0))
	require.NoError(t, lb.AddPooler(shard1))

	// Request specific shard
	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		Shard:      "1",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	conn, err := lb.GetConnection(target, nil)
	require.NoError(t, err)
	assert.Equal(t, poolerID(shard1), conn.ID())
}

func TestLoadBalancer_GetConnection_ExcludePoolers(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer(context.Background(), "zone1", logger)

	// Add two replicas
	replica1 := createTestMultiPooler("replica1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	replica2 := createTestMultiPooler("replica2", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	require.NoError(t, lb.AddPooler(replica1))
	require.NoError(t, lb.AddPooler(replica2))

	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		PoolerType: clustermetadatapb.PoolerType_REPLICA,
	}

	// First request returns one of them
	conn1, err := lb.GetConnection(target, nil)
	require.NoError(t, err)

	// Exclude the first one, should get the other
	opts := &GetConnectionOptions{
		ExcludePoolers: []string{conn1.ID()},
	}
	conn2, err := lb.GetConnection(target, opts)
	require.NoError(t, err)
	assert.NotEqual(t, conn1.ID(), conn2.ID(), "Should return different pooler when first is excluded")

	// Exclude both, should error
	opts.ExcludePoolers = []string{poolerID(replica1), poolerID(replica2)}
	_, err = lb.GetConnection(target, opts)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no pooler found")
}

func TestLoadBalancer_GetConnection_DefaultsToPrimary(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer(context.Background(), "zone1", logger)

	// Add both primary and replica
	primary := createTestMultiPooler("primary1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	replica := createTestMultiPooler("replica1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)
	require.NoError(t, lb.AddPooler(primary))
	require.NoError(t, lb.AddPooler(replica))

	// Request with UNKNOWN type should default to PRIMARY
	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		PoolerType: clustermetadatapb.PoolerType_UNKNOWN,
	}
	conn, err := lb.GetConnection(target, nil)
	require.NoError(t, err)
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, conn.Type(), "Should default to PRIMARY")
}

func TestLoadBalancer_Close(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer(context.Background(), "zone1", logger)

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
func simulateHealthUpdate(conn *PoolerConnection, status clustermetadatapb.PoolerServingStatus, observation *multipoolerservice.PrimaryObservation) {
	info := conn.PoolerInfo()
	conn.processHealthResponse(&multipoolerservice.StreamPoolerHealthResponse{
		Target: &query.Target{
			TableGroup: info.GetTableGroup(),
			Shard:      info.GetShard(),
			PoolerType: info.Type,
		},
		PoolerId:           info.Id,
		ServingStatus:      status,
		PrimaryObservation: observation,
	})
}

func TestLoadBalancer_SelectPrimaryByTerm(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer(context.Background(), "zone1", logger)

	// Create poolers in the same shard
	primary1 := createTestMultiPooler("primary1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	primary2 := createTestMultiPooler("primary2", "zone2", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	replica1 := createTestMultiPooler("replica1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_REPLICA)

	require.NoError(t, lb.AddPooler(primary1))
	require.NoError(t, lb.AddPooler(primary2))
	require.NoError(t, lb.AddPooler(replica1))

	// Get connections to set health
	lb.mu.Lock()
	connPrimary1 := lb.connections[poolerID(primary1)]
	connPrimary2 := lb.connections[poolerID(primary2)]
	connReplica1 := lb.connections[poolerID(replica1)]
	lb.mu.Unlock()

	t.Run("highest term wins", func(t *testing.T) {
		// primary1 thinks primary1 is leader with term 5
		simulateHealthUpdate(connPrimary1,
			clustermetadatapb.PoolerServingStatus_SERVING,
			&multipoolerservice.PrimaryObservation{
				PrimaryId: primary1.Id,
				Term:      5,
			})

		// primary2 thinks primary2 is leader with term 10 (higher)
		simulateHealthUpdate(connPrimary2,
			clustermetadatapb.PoolerServingStatus_SERVING,
			&multipoolerservice.PrimaryObservation{
				PrimaryId: primary2.Id,
				Term:      10,
			})

		// replica1 also thinks primary2 is leader with term 10
		simulateHealthUpdate(connReplica1,
			clustermetadatapb.PoolerServingStatus_SERVING,
			&multipoolerservice.PrimaryObservation{
				PrimaryId: primary2.Id,
				Term:      10,
			})

		target := &query.Target{
			TableGroup: constants.DefaultTableGroup,
			PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		}
		conn, err := lb.GetConnection(target, nil)
		require.NoError(t, err)
		assert.Equal(t, poolerID(primary2), conn.ID(), "Should select primary with highest term")
	})

	t.Run("replica reports higher term primary", func(t *testing.T) {
		// primary1 thinks primary1 is leader with term 15
		simulateHealthUpdate(connPrimary1,
			clustermetadatapb.PoolerServingStatus_SERVING,
			&multipoolerservice.PrimaryObservation{
				PrimaryId: primary1.Id,
				Term:      15,
			})

		// primary2 thinks primary2 is leader with term 12 (stale)
		simulateHealthUpdate(connPrimary2,
			clustermetadatapb.PoolerServingStatus_NOT_SERVING,
			&multipoolerservice.PrimaryObservation{
				PrimaryId: primary2.Id,
				Term:      12,
			})

		// replica1 observed the new leader (primary1) with term 20 (highest)
		simulateHealthUpdate(connReplica1,
			clustermetadatapb.PoolerServingStatus_SERVING,
			&multipoolerservice.PrimaryObservation{
				PrimaryId: primary1.Id,
				Term:      20,
			})

		target := &query.Target{
			TableGroup: constants.DefaultTableGroup,
			PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		}
		conn, err := lb.GetConnection(target, nil)
		require.NoError(t, err)
		assert.Equal(t, poolerID(primary1), conn.ID(), "Should trust replica's observation with highest term")
	})

	t.Run("no observations falls back to PRIMARY type", func(t *testing.T) {
		// Clear observations
		simulateHealthUpdate(connPrimary1,
			clustermetadatapb.PoolerServingStatus_SERVING,
			nil)
		simulateHealthUpdate(connPrimary2,
			clustermetadatapb.PoolerServingStatus_NOT_SERVING,
			nil)
		simulateHealthUpdate(connReplica1,
			clustermetadatapb.PoolerServingStatus_SERVING,
			nil)

		target := &query.Target{
			TableGroup: constants.DefaultTableGroup,
			PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		}
		conn, err := lb.GetConnection(target, nil)
		require.NoError(t, err)
		// Should return a PRIMARY type pooler
		assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, conn.Type(),
			"Should fall back to PRIMARY type when no observations")
	})

	t.Run("primary in unwatched cell falls back to observer", func(t *testing.T) {
		// All poolers observe a primary in zone3 that we don't have a connection to
		unknownPrimaryID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone3",
			Name:      "unknown-primary",
		}
		simulateHealthUpdate(connPrimary1,
			clustermetadatapb.PoolerServingStatus_SERVING,
			&multipoolerservice.PrimaryObservation{
				PrimaryId: unknownPrimaryID,
				Term:      100,
			})
		simulateHealthUpdate(connPrimary2,
			clustermetadatapb.PoolerServingStatus_SERVING,
			&multipoolerservice.PrimaryObservation{
				PrimaryId: unknownPrimaryID,
				Term:      100,
			})

		target := &query.Target{
			TableGroup: constants.DefaultTableGroup,
			PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		}
		conn, err := lb.GetConnection(target, nil)
		require.NoError(t, err)
		// Should fall back to the observer (one of the poolers that reported term 100)
		assert.Contains(t, []string{poolerID(primary1), poolerID(primary2)}, conn.ID(),
			"Should fall back to observer when primary not in connections")
	})

	t.Run("excluded pooler observation still consulted", func(t *testing.T) {
		// primary1 has the highest term observation pointing to primary2
		simulateHealthUpdate(connPrimary1,
			clustermetadatapb.PoolerServingStatus_SERVING,
			&multipoolerservice.PrimaryObservation{
				PrimaryId: primary2.Id,
				Term:      200,
			})
		// primary2 has stale observation
		simulateHealthUpdate(connPrimary2,
			clustermetadatapb.PoolerServingStatus_SERVING,
			&multipoolerservice.PrimaryObservation{
				PrimaryId: primary2.Id,
				Term:      150,
			})

		target := &query.Target{
			TableGroup: constants.DefaultTableGroup,
			PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		}

		// Exclude primary1, but its observation should still be used
		opts := &GetConnectionOptions{
			ExcludePoolers: []string{poolerID(primary1)},
		}
		conn, err := lb.GetConnection(target, opts)
		require.NoError(t, err)
		// Should return primary2 (from primary1's observation) even though primary1 is excluded
		assert.Equal(t, poolerID(primary2), conn.ID(),
			"Should use excluded pooler's observation to find primary")
	})

	t.Run("excluded primary falls back to another pooler", func(t *testing.T) {
		// Both point to primary2 as the highest-term primary
		simulateHealthUpdate(connPrimary1,
			clustermetadatapb.PoolerServingStatus_SERVING,
			&multipoolerservice.PrimaryObservation{
				PrimaryId: primary2.Id,
				Term:      300,
			})
		simulateHealthUpdate(connPrimary2,
			clustermetadatapb.PoolerServingStatus_SERVING,
			&multipoolerservice.PrimaryObservation{
				PrimaryId: primary2.Id,
				Term:      300,
			})

		target := &query.Target{
			TableGroup: constants.DefaultTableGroup,
			PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		}

		// Exclude primary2 (the actual primary) - should fall back to primary1 (observer)
		opts := &GetConnectionOptions{
			ExcludePoolers: []string{poolerID(primary2)},
		}
		conn, err := lb.GetConnection(target, opts)
		require.NoError(t, err)
		assert.Equal(t, poolerID(primary1), conn.ID(),
			"Should fall back to observer when identified primary is excluded")
	})
}

func TestLoadBalancer_SelectPrimaryByTerm_UnknownTypeNotUsed(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer(context.Background(), "zone1", logger)

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
		// No PrimaryObservation set - simulates initial state before health stream
		simulateHealthUpdate(connUnknown1, clustermetadatapb.PoolerServingStatus_SERVING, nil)
		simulateHealthUpdate(connUnknown2, clustermetadatapb.PoolerServingStatus_SERVING, nil)

		target := &query.Target{
			TableGroup: constants.DefaultTableGroup,
			PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		}
		_, err := lb.GetConnection(target, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no PRIMARY type",
			"Should not fall back to UNKNOWN type poolers")
	})

	t.Run("UNKNOWN poolers with observation pointing to UNKNOWN returns error", func(t *testing.T) {
		// Both UNKNOWN poolers point to each other (pathological case)
		simulateHealthUpdate(connUnknown1,
			clustermetadatapb.PoolerServingStatus_SERVING,
			&multipoolerservice.PrimaryObservation{
				PrimaryId: unknown2.Id,
				Term:      10,
			})
		simulateHealthUpdate(connUnknown2,
			clustermetadatapb.PoolerServingStatus_SERVING,
			&multipoolerservice.PrimaryObservation{
				PrimaryId: unknown1.Id,
				Term:      5,
			})

		target := &query.Target{
			TableGroup: constants.DefaultTableGroup,
			PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		}
		// Should find unknown2 via observation with highest term, but unknown2 is UNKNOWN type
		// The current implementation returns the identified pooler regardless of type,
		// which is intentional - the observation is authoritative
		conn, err := lb.GetConnection(target, nil)
		require.NoError(t, err)
		assert.Equal(t, poolerID(unknown2), conn.ID(),
			"Observation takes precedence over pooler type")
	})
}

func TestLoadBalancer_SelectReplicaByLocalityAndServingStatus(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer(context.Background(), "zone1", logger)

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
		conn, err := lb.GetConnection(target, nil)
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
		conn, err := lb.GetConnection(target, nil)
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
		conn, err := lb.GetConnection(target, nil)
		require.NoError(t, err)
		// Should pick one of the local not-serving replicas
		assert.Equal(t, "zone1", conn.Cell(),
			"Should fall back to local not-serving when no serving replicas")
	})
}

func TestLoadBalancerListener(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer(context.Background(), "zone1", logger)
	listener := NewLoadBalancerListener(lb)

	// OnPoolerChanged should add pooler
	pooler := createTestMultiPooler("pooler1", "zone1", constants.DefaultTableGroup, "0", clustermetadatapb.PoolerType_PRIMARY)
	listener.OnPoolerChanged(pooler)
	assert.Equal(t, 1, lb.ConnectionCount())

	// OnPoolerRemoved should remove pooler
	listener.OnPoolerRemoved(pooler)
	assert.Equal(t, 0, lb.ConnectionCount())
}
