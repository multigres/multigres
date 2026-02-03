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
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/query"
)

// poolerID returns the expected ID format for a pooler
func poolerID(pooler *clustermetadatapb.MultiPooler) string {
	return topoclient.MultiPoolerIDString(pooler.Id)
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
	lb := NewLoadBalancer("zone1", logger)

	// Initially empty
	assert.Equal(t, 0, lb.ConnectionCount())

	// Add a pooler
	pooler := createTestMultiPooler("pooler1", "zone1", constants.DefaultTableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_PRIMARY)
	err := lb.AddPooler(pooler)
	require.NoError(t, err)
	assert.Equal(t, 1, lb.ConnectionCount())

	// Adding same pooler again is a no-op
	err = lb.AddPooler(pooler)
	require.NoError(t, err)
	assert.Equal(t, 1, lb.ConnectionCount())

	// Remove the pooler
	lb.RemovePooler("zone1/pooler1")
	assert.Equal(t, 0, lb.ConnectionCount())

	// Removing non-existent pooler is a no-op
	lb.RemovePooler("zone1/nonexistent")
	assert.Equal(t, 0, lb.ConnectionCount())
}

func TestLoadBalancer_GetConnection_Primary(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer("zone1", logger)

	// Add a primary
	primary := createTestMultiPooler("primary1", "zone1", constants.DefaultTableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, lb.AddPooler(primary))

	// Should find the primary
	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	conn, err := lb.GetConnection(target)
	require.NoError(t, err)
	assert.Equal(t, poolerID(primary), conn.ID())
}

func TestLoadBalancer_GetConnection_ReplicaPreferLocalCell(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer("zone1", logger)

	// Add replicas in both cells
	localReplica := createTestMultiPooler("local-replica", "zone1", constants.DefaultTableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_REPLICA)
	remoteReplica := createTestMultiPooler("remote-replica", "zone2", constants.DefaultTableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_REPLICA)
	require.NoError(t, lb.AddPooler(localReplica))
	require.NoError(t, lb.AddPooler(remoteReplica))

	// Should prefer local cell for replicas
	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
		PoolerType: clustermetadatapb.PoolerType_REPLICA,
	}
	conn, err := lb.GetConnection(target)
	require.NoError(t, err)
	assert.Equal(t, poolerID(localReplica), conn.ID(), "Should prefer local cell for replicas")
}

func TestLoadBalancer_GetConnection_CrossCellPrimary(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer("zone1", logger)

	// Add primary only in remote cell
	remotePrimary := createTestMultiPooler("remote-primary", "zone2", constants.DefaultTableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, lb.AddPooler(remotePrimary))

	// Should find primary in remote cell
	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	conn, err := lb.GetConnection(target)
	require.NoError(t, err)
	assert.Equal(t, poolerID(remotePrimary), conn.ID(), "Should find primary in remote cell")
}

func TestLoadBalancer_GetConnection_NoMatch(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer("zone1", logger)

	// Add a primary
	primary := createTestMultiPooler("primary1", "zone1", constants.DefaultTableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, lb.AddPooler(primary))

	// Request a replica - should not find one
	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
		PoolerType: clustermetadatapb.PoolerType_REPLICA,
	}
	_, err := lb.GetConnection(target)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no pooler found")
}

func TestLoadBalancer_GetConnection_ShardMatch(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer("zone1", logger)

	// Add primaries for different shards
	shard0 := createTestMultiPooler("primary-shard0", "zone1", constants.DefaultTableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_PRIMARY)
	shard1 := createTestMultiPooler("primary-shard1", "zone1", constants.DefaultTableGroup, "1", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, lb.AddPooler(shard0))
	require.NoError(t, lb.AddPooler(shard1))

	// Request specific shard
	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		Shard:      "1",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	conn, err := lb.GetConnection(target)
	require.NoError(t, err)
	assert.Equal(t, poolerID(shard1), conn.ID())
}

func TestLoadBalancer_GetConnection_DefaultsToPrimary(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer("zone1", logger)

	// Add both primary and replica
	primary := createTestMultiPooler("primary1", "zone1", constants.DefaultTableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_PRIMARY)
	replica := createTestMultiPooler("replica1", "zone1", constants.DefaultTableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_REPLICA)
	require.NoError(t, lb.AddPooler(primary))
	require.NoError(t, lb.AddPooler(replica))

	// Request with UNKNOWN type should default to PRIMARY
	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
		PoolerType: clustermetadatapb.PoolerType_UNKNOWN,
	}
	conn, err := lb.GetConnection(target)
	require.NoError(t, err)
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, conn.Type(), "Should default to PRIMARY")
}

func TestLoadBalancer_Close(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer("zone1", logger)

	// Add some poolers
	pooler1 := createTestMultiPooler("pooler1", "zone1", constants.DefaultTableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_PRIMARY)
	pooler2 := createTestMultiPooler("pooler2", "zone1", constants.DefaultTableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_REPLICA)
	require.NoError(t, lb.AddPooler(pooler1))
	require.NoError(t, lb.AddPooler(pooler2))
	assert.Equal(t, 2, lb.ConnectionCount())

	// Close should remove all connections
	err := lb.Close()
	require.NoError(t, err)
	assert.Equal(t, 0, lb.ConnectionCount())
}

func TestLoadBalancer_ConcurrentAddRemove(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer("zone1", logger)

	const numGoroutines = 10
	const numPoolersPerGoroutine = 5

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Each goroutine adds its poolers then spawns removal goroutines
	for goroutineID := range numGoroutines {
		go func() {
			defer wg.Done()

			// Add poolers and spawn concurrent removals
			for j := range numPoolersPerGoroutine {
				pooler := createTestMultiPooler(
					fmt.Sprintf("pooler-%d-%d", goroutineID, j),
					"zone1",
					constants.DefaultTableGroup,
					constants.DefaultShard,
					clustermetadatapb.PoolerType_REPLICA,
				)
				err := lb.AddPooler(pooler)
				require.NoError(t, err)

				// Spawn goroutine to remove this pooler
				poolerRemoveID := fmt.Sprintf("zone1/pooler-%d-%d", goroutineID, j)
				wg.Add(1)
				go func(id string) {
					defer wg.Done()
					lb.RemovePooler(id)
				}(poolerRemoveID)
			}
		}()
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// All poolers should be removed
	assert.Equal(t, 0, lb.ConnectionCount(),
		"all poolers should be removed after concurrent add/remove")
}

func TestLoadBalancer_ConcurrentGetConnection(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer("zone1", logger)

	// Add stable poolers that won't be removed
	const numStablePoolers = 5
	for i := range numStablePoolers {
		pooler := createTestMultiPooler(
			fmt.Sprintf("stable-pooler-%d", i),
			"zone1",
			constants.DefaultTableGroup,
			"0",
			clustermetadatapb.PoolerType_REPLICA,
		)
		require.NoError(t, lb.AddPooler(pooler))
	}

	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
		PoolerType: clustermetadatapb.PoolerType_REPLICA,
	}

	const numReaders = 20
	const numDynamicOps = 5
	var wg sync.WaitGroup
	wg.Add(numReaders + numDynamicOps)

	// Track successful reads
	successfulReads := atomic.Int64{}

	// Multiple goroutines reading (GetConnection)
	for range numReaders {
		go func() {
			defer wg.Done()
			for range 10 {
				conn, err := lb.GetConnection(target)
				if err == nil {
					assert.NotNil(t, conn)
					successfulReads.Add(1)
				}
			}
		}()
	}

	// Goroutines performing dynamic add/remove operations
	// Each adds a pooler then immediately removes it
	for opID := range numDynamicOps {
		go func() {
			defer wg.Done()
			poolerName := fmt.Sprintf("dynamic-pooler-%d", opID)
			pooler := createTestMultiPooler(
				poolerName,
				"zone1",
				constants.DefaultTableGroup,
				constants.DefaultShard,
				clustermetadatapb.PoolerType_REPLICA,
			)
			_ = lb.AddPooler(pooler)
			// Small delay to increase chance of concurrent GetConnection calls
			time.Sleep(time.Millisecond)
			lb.RemovePooler("zone1/" + poolerName)
		}()
	}

	// Wait for all goroutines
	wg.Wait()

	// Verify stable state: original poolers still present
	assert.Equal(t, numStablePoolers, lb.ConnectionCount(),
		"should still have all stable poolers after concurrent operations")

	// Verify reads were successful
	assert.Greater(t, successfulReads.Load(), int64(0),
		"should have had successful GetConnection calls")
}

// TODO: TestLoadBalancer_RemoveWhileInUse would require integration testing
// with actual query execution to verify that removing a pooler mid-query
// doesn't cause issues. This is better suited for end-to-end tests.

func TestLoadBalancerListener(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer("zone1", logger)
	listener := NewLoadBalancerListener(lb)

	// OnPoolerChanged should add pooler
	pooler := createTestMultiPooler("pooler1", "zone1", constants.DefaultTableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_PRIMARY)
	listener.OnPoolerChanged(pooler)
	assert.Equal(t, 1, lb.ConnectionCount())

	// OnPoolerRemoved should remove pooler
	listener.OnPoolerRemoved(pooler)
	assert.Equal(t, 0, lb.ConnectionCount())
}

func TestLoadBalancer_GetConnection_MultipleReplicasSameCell(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer("zone1", logger)

	// Add multiple replicas in the same cell
	replica1 := createTestMultiPooler("replica1", "zone1", constants.DefaultTableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_REPLICA)
	replica2 := createTestMultiPooler("replica2", "zone1", constants.DefaultTableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_REPLICA)
	replica3 := createTestMultiPooler("replica3", "zone1", constants.DefaultTableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_REPLICA)
	require.NoError(t, lb.AddPooler(replica1))
	require.NoError(t, lb.AddPooler(replica2))
	require.NoError(t, lb.AddPooler(replica3))

	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
		PoolerType: clustermetadatapb.PoolerType_REPLICA,
	}

	// Should return one of the local replicas
	// The selection is deterministic based on map iteration order in this case
	conn, err := lb.GetConnection(target)
	require.NoError(t, err)
	assert.Contains(t, []string{
		poolerID(replica1),
		poolerID(replica2),
		poolerID(replica3),
	}, conn.ID(), "Should return one of the local replicas")
}

func TestLoadBalancer_GetConnection_NilTarget(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer("zone1", logger)

	// Add a pooler
	pooler := createTestMultiPooler("pooler1", "zone1", constants.DefaultTableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, lb.AddPooler(pooler))

	// Calling with nil target should not panic and should return error
	_, err := lb.GetConnection(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "target cannot be nil")
}

func TestLoadBalancer_GetConnection_EmptyTableGroup(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer("zone1", logger)

	// Add a pooler for default tablegroup
	pooler := createTestMultiPooler("pooler1", "zone1", constants.DefaultTableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, lb.AddPooler(pooler))

	// Request with empty tablegroup should not match
	target := &query.Target{
		TableGroup: "",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	_, err := lb.GetConnection(target)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no pooler found")
}

func TestLoadBalancer_GetConnection_MultipleRemoteReplicas(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer("zone1", logger)

	// Add replicas only in remote cells (no local replica)
	remote1 := createTestMultiPooler("remote1", "zone2", constants.DefaultTableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_REPLICA)
	remote2 := createTestMultiPooler("remote2", "zone3", constants.DefaultTableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_REPLICA)
	require.NoError(t, lb.AddPooler(remote1))
	require.NoError(t, lb.AddPooler(remote2))

	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
		PoolerType: clustermetadatapb.PoolerType_REPLICA,
	}

	// Should return one of the remote replicas (falls back when no local replica)
	conn, err := lb.GetConnection(target)
	require.NoError(t, err)
	assert.Contains(t, []string{
		poolerID(remote1),
		poolerID(remote2),
	}, conn.ID(), "Should return a remote replica when no local replica available")
}

func TestLoadBalancer_GetConnection_TablegroupMismatch(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer("zone1", logger)

	// Add a pooler for default tablegroup
	pooler := createTestMultiPooler("pooler1", "zone1", constants.DefaultTableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, lb.AddPooler(pooler))

	// Request a different tablegroup
	target := &query.Target{
		TableGroup: "other-tablegroup",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	_, err := lb.GetConnection(target)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no pooler found")
}

func TestLoadBalancer_GetConnection_ShardMismatch(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer("zone1", logger)

	// Add pooler for shard 0
	pooler := createTestMultiPooler("pooler1", "zone1", constants.DefaultTableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, lb.AddPooler(pooler))

	// Request shard 1 (which doesn't exist)
	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		Shard:      "1",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	_, err := lb.GetConnection(target)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no pooler found")
}

func TestLoadBalancer_GetConnection_MultiplePrimaries(t *testing.T) {
	logger := slog.Default()
	lb := NewLoadBalancer("zone1", logger)

	// Add multiple primaries (shouldn't happen in practice, but test the behavior)
	primary1 := createTestMultiPooler("primary1", "zone1", constants.DefaultTableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_PRIMARY)
	primary2 := createTestMultiPooler("primary2", "zone2", constants.DefaultTableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, lb.AddPooler(primary1))
	require.NoError(t, lb.AddPooler(primary2))

	target := &query.Target{
		TableGroup: constants.DefaultTableGroup,
		Shard:      constants.DefaultShard,
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}

	// Should return one of them (first one found in map iteration)
	conn, err := lb.GetConnection(target)
	require.NoError(t, err)
	assert.Contains(t, []string{
		poolerID(primary1),
		poolerID(primary2),
	}, conn.ID(), "Should return one of the primaries")
}
