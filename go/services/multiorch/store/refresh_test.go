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
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/services/multiorch/internal/snapshot"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestPooler_refreshNow(t *testing.T) {
	ctx := t.Context()
	leaderID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"}

	newPooler := func() *Pooler {
		return NewPooler(&multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{Id: leaderID, Type: clustermetadatapb.PoolerType_PRIMARY},
		}, nil)
	}

	t.Run("writes the fresh status into the cache on success", func(t *testing.T) {
		pooler := newPooler()
		servingStatus := &clustermetadatapb.ConsensusStatus{Id: leaderID}
		fakeClient := rpcclient.NewFakeClient()
		fakeClient.SetStatusResponse("multipooler-cell1-primary", &multipoolermanagerdatapb.StatusResponse{
			ConsensusStatus: servingStatus,
			Status: &multipoolermanagerdatapb.Status{
				PostgresReady:  true,
				PostgresStatus: multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_STANDBY,
			},
		})

		err := pooler.refreshNow(ctx, fakeClient)

		require.NoError(t, err)
		health := pooler.Health()
		assert.True(t, health.GetStatus().GetPostgresReady())
		assert.Equal(t, multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_STANDBY, health.GetStatus().GetPostgresStatus())
		assert.True(t, proto.Equal(health.GetConsensusStatus(), servingStatus))
		assert.NotNil(t, health.GetLastSeen())
		assert.NotNil(t, health.GetLastPostgresReadyTime())
		// Identity fields the Status RPC doesn't carry must survive untouched.
		assert.Equal(t, "primary", health.GetMultipooler().GetId().GetName())
	})

	t.Run("leaves LastPostgresReadyTime alone when postgres is not ready", func(t *testing.T) {
		pooler := newPooler()
		fakeClient := rpcclient.NewFakeClient()
		fakeClient.SetStatusResponse("multipooler-cell1-primary", &multipoolermanagerdatapb.StatusResponse{
			Status: &multipoolermanagerdatapb.Status{PostgresReady: false},
		})

		err := pooler.refreshNow(ctx, fakeClient)

		require.NoError(t, err)
		assert.False(t, pooler.Health().GetStatus().GetPostgresReady())
		assert.Nil(t, pooler.Health().GetLastPostgresReadyTime())
	})

	t.Run("leaves the cache untouched when the pooler is unreachable", func(t *testing.T) {
		pooler := newPooler()
		fakeClient := rpcclient.NewFakeClient()
		fakeClient.Errors["multipooler-cell1-primary"] = errors.New("connection refused")

		err := pooler.refreshNow(ctx, fakeClient)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "unreachable during health refresh")
		assert.Nil(t, pooler.Health().GetStatus())
		assert.Nil(t, pooler.Health().GetLastSeen())
	})
}

func TestRefreshRecentInParallel(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.DiscardHandler)
	ready := &multipoolermanagerdatapb.StatusResponse{Status: &multipoolermanagerdatapb.Status{PostgresReady: true}}

	poolerSeen := func(name string, lastSeen *timestamppb.Timestamp) *Pooler {
		return NewPooler(&multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{
				Id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: name},
			},
			LastSeen: lastSeen,
		}, nil)
	}
	recent := func() *timestamppb.Timestamp { return timestamppb.New(time.Now().Add(-time.Second)) }
	stale := func() *timestamppb.Timestamp {
		return timestamppb.New(time.Now().Add(-2 * snapshot.DefaultObservationFreshness))
	}

	t.Run("refreshes a recently seen pooler and skips stale or never-seen ones", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		for _, name := range []string{"fresh", "stale", "never"} {
			fakeClient.SetStatusResponse(topoclient.ComponentID("multipooler-cell1-"+name), ready)
		}
		fresh, staleOne, never := poolerSeen("fresh", recent()), poolerSeen("stale", stale()), poolerSeen("never", nil)

		result := RefreshRecentInParallel(ctx, fakeClient, []*Pooler{fresh, staleOne, never}, time.Second, logger)

		assert.Equal(t, RefreshResult{Refreshed: 1, Skipped: 2}, result)
		assert.True(t, fresh.Health().GetStatus().GetPostgresReady(), "a recently seen pooler is refreshed")
		assert.Nil(t, staleOne.Health().GetStatus(), "a pooler we have not heard from lately is not retried")
		assert.Nil(t, never.Health().GetStatus(), "a never-observed pooler is not contacted")
		assert.Equal(t, []string{"Status(multipooler-cell1-fresh)"}, fakeClient.GetCallLog(),
			"only the recently seen pooler may be contacted")
	})

	t.Run("one failing pooler does not stop the others from refreshing", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		fakeClient.SetStatusResponse("multipooler-cell1-ok", ready)
		fakeClient.Errors["multipooler-cell1-down"] = errors.New("connection refused")
		ok, down := poolerSeen("ok", recent()), poolerSeen("down", recent())

		result := RefreshRecentInParallel(ctx, fakeClient, []*Pooler{down, ok}, time.Second, logger)

		assert.Equal(t, RefreshResult{Refreshed: 1, Failed: 1}, result)
		assert.True(t, ok.Health().GetStatus().GetPostgresReady())
		assert.Nil(t, down.Health().GetStatus(), "the failed pooler keeps its cached state")
	})

	t.Run("a slow pooler is cut off at the overall timeout without blocking the rest", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		fakeClient.SetStatusResponse("multipooler-cell1-fast", ready)
		fakeClient.SetStatusResponseWithDelay("multipooler-cell1-slow", ready, 10*time.Second)
		fast, slow := poolerSeen("fast", recent()), poolerSeen("slow", recent())

		start := time.Now()
		result := RefreshRecentInParallel(ctx, fakeClient, []*Pooler{slow, fast}, 100*time.Millisecond, logger)

		assert.Equal(t, RefreshResult{Refreshed: 1, Failed: 1}, result, "the cut-off pooler counts as failed")
		assert.Less(t, time.Since(start), 5*time.Second, "the refresh must return at the timeout, not wait for the slow pooler")
		assert.True(t, fast.Health().GetStatus().GetPostgresReady(), "the fast pooler is refreshed in parallel")
		assert.Nil(t, slow.Health().GetStatus(), "the pooler that did not answer in time keeps its cached state")
	})
}

func TestPooler_applyStatusResponseSkipsWhenAStreamedSnapshotIsNewer(t *testing.T) {
	leaderID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"}
	requestedAt := time.Now().Add(-time.Second)
	response := &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{PostgresReady: true},
	}
	newPooler := func(lastSeen time.Time) *Pooler {
		return NewPooler(&multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{Id: leaderID},
			LastSeen:    timestamppb.New(lastSeen),
			Status:      &multipoolermanagerdatapb.Status{PostgresReady: false},
		}, nil)
	}

	t.Run("a snapshot that landed after the request is kept", func(t *testing.T) {
		pooler := newPooler(time.Now()) // streamed snapshot arrived while the RPC was in flight
		require.True(t, pooler.applyStatusResponse(response, timestamppb.Now(), requestedAt))
		assert.False(t, pooler.Health().GetStatus().GetPostgresReady(), "a slow reply must not overwrite the newer snapshot")
	})

	t.Run("an older cached snapshot is replaced", func(t *testing.T) {
		pooler := newPooler(requestedAt.Add(-time.Minute))
		require.True(t, pooler.applyStatusResponse(response, timestamppb.Now(), requestedAt))
		assert.True(t, pooler.Health().GetStatus().GetPostgresReady())
	})
}
