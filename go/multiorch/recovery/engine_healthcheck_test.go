// Copyright 2025 Supabase, Inc.
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

package recovery

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/clustermetadata/topo/memorytopo"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/multiorch/config"
	"github.com/multigres/multigres/go/multiorch/store"
	"github.com/multigres/multigres/go/pb/clustermetadata"
)

// TestRecoveryEngine_HealthCheckQueue tests that outdated poolers are queued and health checked
func TestRecoveryEngine_HealthCheckQueue(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// Create engine with very short health check interval for testing
	cfg := config.NewTestConfig(
		config.WithCell("zone1"),
		config.WithPoolerHealthCheckInterval(100*time.Millisecond), // health check interval - short for testing
		config.WithHealthCheckWorkers(1),                           // single worker for testing
		config.WithClusterMetadataRefreshInterval(50*time.Millisecond),
		config.WithClusterMetadataRefreshTimeout(5*time.Second),
	)

	re := NewEngine(
		ts,
		logger,
		cfg,
		[]config.WatchTarget{{Database: "mydb"}},
		&rpcclient.FakeClient{},
	)

	// Add poolers to topology
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"},
		Database: "mydb", TableGroup: "tg1", Shard: "0",
		Hostname: "host1",
	}))
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler2"},
		Database: "mydb", TableGroup: "tg1", Shard: "1",
		Hostname: "host2",
	}))

	// Start the engine - discovery should add poolers to store
	err := re.Start()
	require.NoError(t, err, "failed to start recovery engine")
	defer re.Stop()

	// Wait for discovery to add poolers to store
	require.Eventually(t, func() bool {
		return re.poolerStore.Len() == 2
	}, 1*time.Second, 50*time.Millisecond, "poolers should be discovered")

	key1 := poolerKey("zone1", "pooler1")
	key2 := poolerKey("zone1", "pooler2")

	// Wait for health check ticker to queue and process poolers
	// Health checks should update LastCheckAttempted
	require.Eventually(t, func() bool {
		p1, ok := re.poolerStore.Get(key1)
		if !ok {
			return false
		}
		return !p1.LastCheckAttempted.IsZero()
	}, 1*time.Second, 50*time.Millisecond, "pooler1 should be health checked via queue")

	require.Eventually(t, func() bool {
		p2, ok := re.poolerStore.Get(key2)
		if !ok {
			return false
		}
		return !p2.LastCheckAttempted.IsZero()
	}, 1*time.Second, 50*time.Millisecond, "pooler2 should be health checked via queue")

	// Verify both poolers have been health checked
	pooler1After, ok := re.poolerStore.Get(key1)
	require.True(t, ok, "pooler1 should still be in store")
	require.False(t, pooler1After.LastCheckAttempted.IsZero(), "pooler1 should have LastCheckAttempted set")

	pooler2After, ok := re.poolerStore.Get(key2)
	require.True(t, ok, "pooler2 should still be in store")
	require.False(t, pooler2After.LastCheckAttempted.IsZero(), "pooler2 should have LastCheckAttempted set")

	// Verify queue is working - outdated poolers get re-queued
	// Update pooler1's LastCheckAttempted to be old
	pooler1After.LastCheckAttempted = time.Now().Add(-200 * time.Millisecond)
	re.poolerStore.Set(key1, pooler1After)
	firstCheckTime := pooler1After.LastCheckAttempted

	// Wait for health check ticker to re-queue and re-check pooler1
	require.Eventually(t, func() bool {
		p1, ok := re.poolerStore.Get(key1)
		if !ok {
			return false
		}
		// LastCheckAttempted should be updated (newer than firstCheckTime)
		return p1.LastCheckAttempted.After(firstCheckTime)
	}, 1*time.Second, 50*time.Millisecond, "pooler1 should be re-checked after becoming outdated")
}

// TestRecoveryEngine_HealthCheckQueueDeduplication tests that the queue properly deduplicates entries
func TestRecoveryEngine_HealthCheckQueueDeduplication(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg := config.NewTestConfig(
		config.WithCell("zone1"),
		config.WithPoolerHealthCheckInterval(100*time.Millisecond),
		config.WithHealthCheckWorkers(2),
		config.WithClusterMetadataRefreshInterval(50*time.Millisecond),
		config.WithClusterMetadataRefreshTimeout(5*time.Second),
	)

	re := NewEngine(
		ts,
		logger,
		cfg,
		[]config.WatchTarget{{Database: "mydb"}},
		&rpcclient.FakeClient{},
	)

	// Add pooler to topology
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"},
		Database: "mydb", TableGroup: "tg1", Shard: "0",
		Hostname: "host1",
	}))

	err := re.Start()
	require.NoError(t, err)
	defer re.Stop()

	// Wait for pooler to be discovered
	require.Eventually(t, func() bool {
		return re.poolerStore.Len() == 1
	}, 1*time.Second, 50*time.Millisecond)

	key1 := poolerKey("zone1", "pooler1")

	// Manually push the same pooler multiple times
	re.healthCheckQueue.Push(key1)
	re.healthCheckQueue.Push(key1)
	re.healthCheckQueue.Push(key1)

	// Queue should only have 1 instance (deduplication)
	queueLen := re.healthCheckQueue.QueueLen()
	require.Equal(t, 1, queueLen, "queue should deduplicate entries")

	// Wait for health check to complete
	require.Eventually(t, func() bool {
		return re.healthCheckQueue.QueueLen() == 0
	}, 1*time.Second, 50*time.Millisecond, "queue should be drained after health check")
}

// TestRecoveryEngine_HealthCheckWorkerPool tests that multiple workers process poolers concurrently
func TestRecoveryEngine_HealthCheckWorkerPool(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg := config.NewTestConfig(
		config.WithCell("zone1"),
		config.WithPoolerHealthCheckInterval(50*time.Millisecond),
		config.WithHealthCheckWorkers(10), // multiple workers
		config.WithClusterMetadataRefreshInterval(50*time.Millisecond),
		config.WithClusterMetadataRefreshTimeout(5*time.Second),
	)

	re := NewEngine(
		ts,
		logger,
		cfg,
		[]config.WatchTarget{{Database: "mydb"}},
		&rpcclient.FakeClient{},
	)

	// Add multiple poolers
	for i := 0; i < 20; i++ {
		require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
			Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: fmt.Sprintf("pooler-%d", i)},
			Database: "mydb", TableGroup: "tg1", Shard: "0",
			Hostname: "host1",
		}))
	}

	err := re.Start()
	require.NoError(t, err)
	defer re.Stop()

	// Wait for all poolers to be discovered
	require.Eventually(t, func() bool {
		return re.poolerStore.Len() == 20
	}, 2*time.Second, 50*time.Millisecond, "all poolers should be discovered")

	// Wait for all poolers to be health checked
	// With 10 workers, this should happen relatively quickly
	require.Eventually(t, func() bool {
		checkedCount := 0
		re.poolerStore.Range(func(key string, p *store.PoolerHealth) bool {
			if !p.LastCheckAttempted.IsZero() {
				checkedCount++
			}
			return true
		})
		return checkedCount == 20
	}, 2*time.Second, 50*time.Millisecond, "all poolers should be health checked")
}
