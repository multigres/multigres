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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/constants"
	commontypes "github.com/multigres/multigres/go/common/types"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

func TestForgetLongUnseenInstances_BrokenEntries(t *testing.T) {
	ctx := context.Background()
	engine := newTestEngine(ctx, t)

	// Add broken entries
	engine.poolerStore.Set("broken-nil-info", nil)
	engine.poolerStore.Set("broken-nil-multipooler", &multiorchdatapb.PoolerHealthState{
		MultiPooler: nil,
	})
	engine.poolerStore.Set("broken-nil-id", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadata.MultiPooler{
			Id: nil,
		},
	})

	require.Equal(t, 3, engine.poolerStore.Len())

	// Run forget
	engine.forgetLongUnseenInstances()

	// All broken entries should be removed
	require.Equal(t, 0, engine.poolerStore.Len())
}

func TestForgetLongUnseenInstances_NeverSeen(t *testing.T) {
	ctx := context.Background()
	engine := newTestEngine(ctx, t)

	now := time.Now()
	threshold := 4 * time.Hour

	// Add pooler that was never successfully health checked, discovered > 4 hours ago
	oldPooler := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadata.MultiPooler{
			Id: &clustermetadata.ID{
				Component: clustermetadata.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "old-pooler",
			},
			Database:   "db1",
			TableGroup: constants.DefaultTableGroup,
			Shard:      "-",
		},
		LastCheckAttempted: timestamppb.New(now.Add(-threshold - time.Hour)), // > 4 hours ago
		LastSeen:           nil,                                              // nil = never seen
	}
	engine.poolerStore.Set("zone1/old-pooler", oldPooler)

	// Add pooler that was never health checked, but discovered recently
	recentPooler := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadata.MultiPooler{
			Id: &clustermetadata.ID{
				Component: clustermetadata.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "recent-pooler",
			},
			Database: "db1",
		},
		LastCheckAttempted: timestamppb.New(now.Add(-time.Hour)), // Only 1 hour ago
		LastSeen:           nil,                                  // nil = never seen
	}
	engine.poolerStore.Set("zone1/recent-pooler", recentPooler)

	// Add pooler with no attempts yet (should be skipped)
	noAttempts := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadata.MultiPooler{
			Id: &clustermetadata.ID{
				Component: clustermetadata.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "no-attempts",
			},
			Database: "db1",
		},
		LastCheckAttempted: nil, // No attempts yet
		LastSeen:           nil, // Never seen
	}
	engine.poolerStore.Set("zone1/no-attempts", noAttempts)

	require.Equal(t, 3, engine.poolerStore.Len())

	// Run forget
	engine.forgetLongUnseenInstances()

	// Only the old pooler should be forgotten
	require.Equal(t, 2, engine.poolerStore.Len())

	// Verify the right ones remain
	_, ok := engine.poolerStore.Get("zone1/old-pooler")
	require.False(t, ok, "old pooler should be forgotten")

	_, ok = engine.poolerStore.Get("zone1/recent-pooler")
	require.True(t, ok, "recent pooler should remain")

	_, ok = engine.poolerStore.Get("zone1/no-attempts")
	require.True(t, ok, "no-attempts pooler should remain")
}

func TestForgetLongUnseenInstances_LongUnseen(t *testing.T) {
	ctx := context.Background()
	engine := newTestEngine(ctx, t)

	now := time.Now()
	threshold := 4 * time.Hour

	// Add pooler that was healthy but not seen in > 4 hours
	oldHealthyPooler := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadata.MultiPooler{
			Id: &clustermetadata.ID{
				Component: clustermetadata.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "old-healthy",
			},
			Database: "db1",
		},
		LastSeen:            timestamppb.New(now.Add(-threshold - time.Hour)), // > 4 hours ago
		LastCheckAttempted:  timestamppb.New(now.Add(-threshold - time.Hour)),
		LastCheckSuccessful: timestamppb.New(now.Add(-threshold - time.Hour)),
		IsUpToDate:          true,
	}
	engine.poolerStore.Set("zone1/old-healthy", oldHealthyPooler)

	// Add pooler that was healthy and seen recently
	recentHealthyPooler := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadata.MultiPooler{
			Id: &clustermetadata.ID{
				Component: clustermetadata.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "recent-healthy",
			},
			Database: "db1",
		},
		LastSeen:            timestamppb.New(now.Add(-time.Hour)), // Only 1 hour ago
		LastCheckAttempted:  timestamppb.New(now.Add(-time.Hour)),
		LastCheckSuccessful: timestamppb.New(now.Add(-time.Hour)),
		IsUpToDate:          true,
	}
	engine.poolerStore.Set("zone1/recent-healthy", recentHealthyPooler)

	require.Equal(t, 2, engine.poolerStore.Len())

	// Run forget
	engine.forgetLongUnseenInstances()

	// Only the old healthy pooler should be forgotten
	require.Equal(t, 1, engine.poolerStore.Len())

	// Verify the right ones remain
	_, ok := engine.poolerStore.Get("zone1/old-healthy")
	require.False(t, ok, "old healthy pooler should be forgotten")

	_, ok = engine.poolerStore.Get("zone1/recent-healthy")
	require.True(t, ok, "recent healthy pooler should remain")
}

func TestForgetLongUnseenInstances_MixedScenario(t *testing.T) {
	ctx := context.Background()
	engine := newTestEngine(ctx, t)

	now := time.Now()
	threshold := 4 * time.Hour

	// Add various poolers covering all cases
	cases := map[string]*multiorchdatapb.PoolerHealthState{
		"broken": nil,
		"never-seen-old": {
			MultiPooler: &clustermetadata.MultiPooler{
				Id: &clustermetadata.ID{
					Component: clustermetadata.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "never-seen-old",
				},
			},
			LastCheckAttempted: timestamppb.New(now.Add(-threshold - time.Hour)),
			LastSeen:           nil,
		},
		"never-seen-recent": {
			MultiPooler: &clustermetadata.MultiPooler{
				Id: &clustermetadata.ID{
					Component: clustermetadata.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "never-seen-recent",
				},
			},
			LastCheckAttempted: timestamppb.New(now.Add(-time.Hour)),
			LastSeen:           nil,
		},
		"long-unseen": {
			MultiPooler: &clustermetadata.MultiPooler{
				Id: &clustermetadata.ID{
					Component: clustermetadata.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "long-unseen",
				},
			},
			LastSeen: timestamppb.New(now.Add(-threshold - time.Hour)),
		},
		"healthy": {
			MultiPooler: &clustermetadata.MultiPooler{
				Id: &clustermetadata.ID{
					Component: clustermetadata.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "healthy",
				},
			},
			LastSeen: timestamppb.New(now.Add(-time.Minute)),
		},
	}

	for key, info := range cases {
		engine.poolerStore.Set(key, info)
	}

	require.Equal(t, 5, engine.poolerStore.Len())

	// Run forget
	engine.forgetLongUnseenInstances()

	// Should keep: never-seen-recent, healthy (2 total)
	// Should forget: broken, never-seen-old, long-unseen (3 total)
	require.Equal(t, 2, engine.poolerStore.Len())

	// Verify
	_, ok := engine.poolerStore.Get("broken")
	require.False(t, ok)

	_, ok = engine.poolerStore.Get("never-seen-old")
	require.False(t, ok)

	_, ok = engine.poolerStore.Get("long-unseen")
	require.False(t, ok)

	_, ok = engine.poolerStore.Get("never-seen-recent")
	require.True(t, ok)

	_, ok = engine.poolerStore.Get("healthy")
	require.True(t, ok)
}

func TestForgetLongUnseenInstances_EmptyStore(t *testing.T) {
	ctx := context.Background()
	engine := newTestEngine(ctx, t)

	// Run forget on empty store (should not panic)
	engine.forgetLongUnseenInstances()

	require.Equal(t, 0, engine.poolerStore.Len())
}

func TestRunBookkeeping(t *testing.T) {
	ctx := context.Background()
	engine := newTestEngine(ctx, t)

	now := time.Now()
	threshold := 4 * time.Hour

	// Add an old pooler that should be forgotten
	oldPooler := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadata.MultiPooler{
			Id: &clustermetadata.ID{
				Component: clustermetadata.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "old",
			},
		},
		LastCheckAttempted: timestamppb.New(now.Add(-threshold - time.Hour)),
		LastSeen:           nil,
	}
	engine.poolerStore.Set("zone1/old", oldPooler)

	require.Equal(t, 1, engine.poolerStore.Len())

	// Run bookkeeping
	engine.runBookkeeping()

	// Old pooler should be forgotten (reloadConfigs runs in goroutine, so use Eventually)
	require.Eventually(t, func() bool {
		return engine.poolerStore.Len() == 0
	}, 1*time.Second, 10*time.Millisecond, "old pooler should be forgotten")
}

func TestAudit(t *testing.T) {
	ctx := context.Background()
	engine := newTestEngine(ctx, t)

	// Just verify audit doesn't panic (output is logged)
	engine.audit("test-type", "pooler-1", commontypes.ShardKey{Database: "db1", TableGroup: "default", Shard: "-"}, "test message")
}
