// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package recovery

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient/poolerwatch"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/config"
)

// newBookkeepingTestEngine constructs a recovery engine wired against a
// memorytopo, without calling Start (so no watch goroutines run). Tests
// poke the engine's poolerCache directly via SeedTombstoneForTest and
// invoke runBookkeeping / cleanupOldShutdownEntries inline.
func newBookkeepingTestEngine(t *testing.T) *Engine {
	t.Helper()
	ts := newTestTopoStore()
	t.Cleanup(func() { _ = ts.Close() })
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := config.NewTestConfig(
		config.WithCell("zone1"),
		config.WithBookkeepingInterval(1*time.Hour),
		config.WithHealthCheckWorkers(0),
	)
	re := NewEngine(
		ts,
		logger,
		cfg,
		[]config.WatchTarget{{Database: "db"}},
		&rpcclient.FakeClient{},
		newTestCoordinator(ts, &rpcclient.FakeClient{}, "zone1"),
	)
	return re
}

// registerPooler creates a topology entry the bookkeeping cleanup can later
// delete. Returns the created pooler so callers can derive its ID.
func registerPooler(t *testing.T, re *Engine, cell, name string) *clustermetadatapb.Multipooler {
	t.Helper()
	p := &clustermetadatapb.Multipooler{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER, Cell: cell, Name: name,
		},
		ShardKey: &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
		Hostname: name + ".local",
	}
	require.NoError(t, re.ts.CreateMultipooler(context.Background(), p))
	return p
}

func TestCleanupOldShutdownEntries_OldTombstoneRemoved(t *testing.T) {
	re := newBookkeepingTestEngine(t)

	p := registerPooler(t, re, "zone1", "old")
	// SHUTDOWN tombstone whose age exceeds shutdownEtcdCleanupAge by a wide
	// margin — exactly the case bookkeeping was built to clean up.
	poolerwatch.SeedTombstoneForTest(t, re.poolerCache, p.Id,
		time.Now().Add(-shutdownEtcdCleanupAge-time.Hour))

	// Sanity: the pooler is in topo before cleanup.
	_, err := re.ts.GetMultipooler(context.Background(), p.Id)
	require.NoError(t, err, "pooler should exist in topology before cleanup")

	re.cleanupOldShutdownEntries()

	_, err = re.ts.GetMultipooler(context.Background(), p.Id)
	require.Error(t, err, "topology entry should have been hard-deleted")
}

func TestCleanupOldShutdownEntries_YoungTombstoneRetained(t *testing.T) {
	re := newBookkeepingTestEngine(t)

	p := registerPooler(t, re, "zone1", "young")
	// Tombstone created well within the cleanup-age window — bookkeeping must
	// leave it alone so a recently-shutdown pooler is not prematurely purged.
	poolerwatch.SeedTombstoneForTest(t, re.poolerCache, p.Id, time.Now())

	re.cleanupOldShutdownEntries()

	_, err := re.ts.GetMultipooler(context.Background(), p.Id)
	require.NoError(t, err, "young tombstone must not trigger topology deletion")
}

func TestCleanupOldShutdownEntries_MixedTombstones(t *testing.T) {
	// Two tombstones, only the older one is past the cleanup threshold.
	// Confirms bookkeeping cleans the old one without sweeping the young one.
	re := newBookkeepingTestEngine(t)

	old := registerPooler(t, re, "zone1", "old")
	young := registerPooler(t, re, "zone1", "young")

	poolerwatch.SeedTombstoneForTest(t, re.poolerCache, old.Id,
		time.Now().Add(-shutdownEtcdCleanupAge-time.Minute))
	poolerwatch.SeedTombstoneForTest(t, re.poolerCache, young.Id, time.Now())

	re.cleanupOldShutdownEntries()

	_, err := re.ts.GetMultipooler(context.Background(), old.Id)
	assert.Error(t, err, "old tombstone's topology entry must be deleted")
	_, err = re.ts.GetMultipooler(context.Background(), young.Id)
	assert.NoError(t, err, "young tombstone's topology entry must remain")
}

func TestCleanupOldShutdownEntries_NoTombstones(t *testing.T) {
	// No tombstones in the cache — bookkeeping must be a no-op (no panics, no
	// RPCs). Important because a healthy cluster routinely has nothing to
	// clean up on most ticks.
	re := newBookkeepingTestEngine(t)
	re.cleanupOldShutdownEntries()
}
