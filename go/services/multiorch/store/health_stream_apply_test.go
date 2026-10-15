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

package store

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestHealthStream_applySnapshot(t *testing.T) {
	poolerID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "p1"}
	shardKey := &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}

	newStream := func(t *testing.T) (*HealthStream, *Pooler) {
		t.Helper()
		cache := NewTestCache(t)
		pooler := NewPooler(&multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{Id: poolerID, ShardKey: shardKey},
		}, nil)
		id := SeedCache(t, cache, pooler)
		return &HealthStream{
			factory:  &HealthStreamFactory{logger: slog.New(slog.DiscardHandler)},
			cache:    cache,
			poolerID: id,
		}, pooler
	}

	t.Run("a snapshot with no status is skipped", func(t *testing.T) {
		hs, pooler := newStream(t)

		hs.applySnapshot(t.Context(), pooler, &multipoolermanagerdatapb.ManagerHealthSnapshot{})

		assert.Nil(t, pooler.Health().GetLastSeen(), "nothing may be recorded for an empty snapshot")
		assert.Zero(t, pooler.Health().GetStreamSnapshotsReceived())
	})

	t.Run("a snapshot is written through the shared field path", func(t *testing.T) {
		hs, pooler := newStream(t)

		hs.applySnapshot(t.Context(), pooler, &multipoolermanagerdatapb.ManagerHealthSnapshot{
			Status: &multipoolermanagerdatapb.StatusResponse{
				Status: &multipoolermanagerdatapb.Status{PostgresReady: true},
			},
		})

		got, ok := hs.cache.GetRider(hs.poolerID)
		require.True(t, ok)
		health := got.Health()
		assert.True(t, health.GetStatus().GetPostgresReady())
		assert.NotNil(t, health.GetLastSeen())
		assert.NotNil(t, health.GetLastPostgresReadyTime())
		assert.EqualValues(t, 1, health.GetStreamSnapshotsReceived())
	})
}
