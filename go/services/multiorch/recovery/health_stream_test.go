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

package recovery

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// makeSnapshot wraps a Status into a HealthStreamResponse snapshot.
func makeSnapshot(status *multipoolermanagerdatapb.Status) *multipoolermanagerdatapb.ManagerHealthStreamResponse {
	return &multipoolermanagerdatapb.ManagerHealthStreamResponse{
		Snapshot: &multipoolermanagerdatapb.ManagerHealthSnapshot{
			Status: &multipoolermanagerdatapb.StatusResponse{Status: status},
		},
	}
}

// newTestHealthStream creates a HealthStream wired to the given FakeClient and store.
func newTestHealthStream(ctx context.Context, fakeClient *rpcclient.FakeClient, poolerStore *store.PoolerStore, opts ...Option) *HealthStream {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return NewHealthStream(ctx, fakeClient, poolerStore, logger, opts...)
}

// seedPooler adds a minimal pooler entry to the store and returns its key.
func seedPooler(poolerStore *store.PoolerStore, poolerID *clustermetadata.ID, poolerType clustermetadata.PoolerType) string {
	key := topoclient.MultiPoolerIDString(poolerID)
	poolerStore.Set(key, &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadata.MultiPooler{
			Id:         poolerID,
			Database:   "mydb",
			TableGroup: "tg1",
			Shard:      "0",
			Type:       poolerType,
			Hostname:   "host1",
			PortMap:    map[string]int32{"grpc": 5432},
		},
	})
	return key
}

// waitForStart drains the Sent channel until a start message arrives.
func waitForStart(t *testing.T, sent <-chan *multipoolermanagerdatapb.ManagerHealthStreamClientMessage) {
	t.Helper()
	select {
	case msg := <-sent:
		require.NotNil(t, msg.GetStart(), "first message should be a start message")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for start message")
	}
}

// waitForPoll drains the Sent channel until a poll message arrives.
func waitForPoll(t *testing.T, sent <-chan *multipoolermanagerdatapb.ManagerHealthStreamClientMessage) {
	t.Helper()
	select {
	case msg := <-sent:
		require.NotNil(t, msg.GetPoll(), "expected a poll message")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for poll message")
	}
}

// TestHealthStream_UpdatesStore_Primary tests that a PRIMARY snapshot is applied to the store.
func TestHealthStream_UpdatesStore_Primary(t *testing.T) {
	ctx := t.Context()

	fakeClient := rpcclient.NewFakeClient()
	streamCh := make(chan *rpcclient.FakeManagerHealthStream, 1)
	fakeClient.OnManagerHealthStream = func(_ string, s *rpcclient.FakeManagerHealthStream) {
		streamCh <- s
	}

	poolerStore := store.NewPoolerStore(fakeClient, slog.Default())
	sm := newTestHealthStream(ctx, fakeClient, poolerStore)

	poolerID := &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"}
	key := seedPooler(poolerStore, poolerID, clustermetadata.PoolerType_PRIMARY)

	sm.Start(key)

	stream := <-streamCh
	waitForStart(t, stream.Sent)

	stream.Ch <- makeSnapshot(&multipoolermanagerdatapb.Status{
		PoolerType: clustermetadata.PoolerType_PRIMARY,
		PrimaryStatus: &multipoolermanagerdatapb.PrimaryStatus{
			Lsn:   "0/123ABC",
			Ready: true,
			ConnectedFollowers: []*clustermetadata.ID{
				{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "replica1"},
				{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "replica2"},
			},
		},
	})

	require.Eventually(t, func() bool {
		s, ok := poolerStore.Get(key)
		return ok && s.IsLastCheckValid
	}, 2*time.Second, 10*time.Millisecond, "snapshot should be applied")

	updated, _ := poolerStore.Get(key)
	require.True(t, updated.IsUpToDate)
	require.NotNil(t, updated.LastSeen)
	require.NotNil(t, updated.LastCheckSuccessful)
	require.Equal(t, clustermetadata.PoolerType_PRIMARY, updated.PoolerType)
	require.NotNil(t, updated.PrimaryStatus)
	require.Equal(t, "0/123ABC", updated.PrimaryStatus.Lsn)
	require.True(t, updated.PrimaryStatus.Ready)
	require.Len(t, updated.PrimaryStatus.ConnectedFollowers, 2)
	require.Nil(t, updated.ReplicationStatus)
}

// TestHealthStream_UpdatesStore_Replica tests that a REPLICA snapshot is applied to the store.
func TestHealthStream_UpdatesStore_Replica(t *testing.T) {
	ctx := t.Context()

	fakeClient := rpcclient.NewFakeClient()
	streamCh := make(chan *rpcclient.FakeManagerHealthStream, 1)
	fakeClient.OnManagerHealthStream = func(_ string, s *rpcclient.FakeManagerHealthStream) {
		streamCh <- s
	}

	poolerStore := store.NewPoolerStore(fakeClient, slog.Default())
	sm := newTestHealthStream(ctx, fakeClient, poolerStore)

	poolerID := &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "replica1"}
	key := seedPooler(poolerStore, poolerID, clustermetadata.PoolerType_REPLICA)

	sm.Start(key)

	stream := <-streamCh
	waitForStart(t, stream.Sent)

	stream.Ch <- makeSnapshot(&multipoolermanagerdatapb.Status{
		PoolerType: clustermetadata.PoolerType_REPLICA,
		ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
			LastReplayLsn:           "0/123ABC",
			LastReceiveLsn:          "0/123DEF",
			IsWalReplayPaused:       false,
			WalReplayPauseState:     "not paused",
			Lag:                     durationpb.New(500 * time.Millisecond),
			LastXactReplayTimestamp: "2025-01-19 20:00:00.000000+00",
			PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{
				Host: "primary-host",
				Port: 5432,
			},
		},
	})

	require.Eventually(t, func() bool {
		s, ok := poolerStore.Get(key)
		return ok && s.IsLastCheckValid
	}, 2*time.Second, 10*time.Millisecond)

	updated, _ := poolerStore.Get(key)
	require.Equal(t, clustermetadata.PoolerType_REPLICA, updated.PoolerType)
	require.NotNil(t, updated.ReplicationStatus)
	require.Equal(t, "0/123ABC", updated.ReplicationStatus.LastReplayLsn)
	require.Equal(t, "0/123DEF", updated.ReplicationStatus.LastReceiveLsn)
	require.False(t, updated.ReplicationStatus.IsWalReplayPaused)
	require.Equal(t, "not paused", updated.ReplicationStatus.WalReplayPauseState)
	require.Equal(t, int64(500), updated.ReplicationStatus.Lag.AsDuration().Milliseconds())
	require.Equal(t, "2025-01-19 20:00:00.000000+00", updated.ReplicationStatus.LastXactReplayTimestamp)
	require.NotNil(t, updated.ReplicationStatus.PrimaryConnInfo)
	require.Equal(t, "primary-host", updated.ReplicationStatus.PrimaryConnInfo.Host)
	require.Equal(t, int32(5432), updated.ReplicationStatus.PrimaryConnInfo.Port)
	require.Nil(t, updated.PrimaryStatus)
}

// TestHealthStream_Poll tests that Poll() sends a poll message and a subsequent snapshot is applied.
func TestHealthStream_Poll(t *testing.T) {
	ctx := t.Context()

	fakeClient := rpcclient.NewFakeClient()
	streamCh := make(chan *rpcclient.FakeManagerHealthStream, 1)
	fakeClient.OnManagerHealthStream = func(_ string, s *rpcclient.FakeManagerHealthStream) {
		streamCh <- s
	}

	poolerStore := store.NewPoolerStore(fakeClient, slog.Default())
	sm := newTestHealthStream(ctx, fakeClient, poolerStore)

	poolerID := &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"}
	key := seedPooler(poolerStore, poolerID, clustermetadata.PoolerType_PRIMARY)

	sm.Start(key)

	stream := <-streamCh
	waitForStart(t, stream.Sent)

	// Inject initial snapshot.
	stream.Ch <- makeSnapshot(&multipoolermanagerdatapb.Status{
		PoolerType:    clustermetadata.PoolerType_PRIMARY,
		PrimaryStatus: &multipoolermanagerdatapb.PrimaryStatus{Lsn: "0/AAAAAA", Ready: true},
	})
	require.Eventually(t, func() bool {
		s, ok := poolerStore.Get(key)
		return ok && s.PrimaryStatus != nil && s.PrimaryStatus.Lsn == "0/AAAAAA"
	}, 2*time.Second, 10*time.Millisecond, "initial snapshot should be applied")

	// Trigger a poll.
	require.NoError(t, sm.Poll(key))
	waitForPoll(t, stream.Sent)

	// Inject updated snapshot (as if pooler responded to the poll).
	stream.Ch <- makeSnapshot(&multipoolermanagerdatapb.Status{
		PoolerType:    clustermetadata.PoolerType_PRIMARY,
		PrimaryStatus: &multipoolermanagerdatapb.PrimaryStatus{Lsn: "0/BBBBBB", Ready: true},
	})

	require.Eventually(t, func() bool {
		s, ok := poolerStore.Get(key)
		return ok && s.PrimaryStatus != nil && s.PrimaryStatus.Lsn == "0/BBBBBB"
	}, 2*time.Second, 10*time.Millisecond, "polled snapshot should be applied")
}

// TestHealthStream_Disconnect tests that a stream disconnection marks the pooler unreachable.
func TestHealthStream_Disconnect(t *testing.T) {
	ctx := t.Context()

	fakeClient := rpcclient.NewFakeClient()
	streamCh := make(chan *rpcclient.FakeManagerHealthStream, 2) // buffer for reconnect
	fakeClient.OnManagerHealthStream = func(_ string, s *rpcclient.FakeManagerHealthStream) {
		streamCh <- s
	}

	poolerStore := store.NewPoolerStore(fakeClient, slog.Default())
	sm := newTestHealthStream(ctx, fakeClient, poolerStore)

	poolerID := &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "failed-pooler"}
	lastSeenTime := time.Now().Add(-1 * time.Hour)
	key := topoclient.MultiPoolerIDString(poolerID)
	poolerStore.Set(key, &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadata.MultiPooler{
			Id: poolerID, Database: "mydb", TableGroup: "tg1", Shard: "0",
			Type: clustermetadata.PoolerType_PRIMARY, Hostname: "host1",
			PortMap: map[string]int32{"grpc": 5432},
		},
		IsLastCheckValid: true,
		LastSeen:         timestamppb.New(lastSeenTime),
	})

	sm.Start(key)

	stream := <-streamCh
	waitForStart(t, stream.Sent)

	// Inject one snapshot so the stream is "connected" with valid data.
	stream.Ch <- makeSnapshot(&multipoolermanagerdatapb.Status{
		PoolerType:    clustermetadata.PoolerType_PRIMARY,
		PostgresReady: true,
	})
	require.Eventually(t, func() bool {
		s, ok := poolerStore.Get(key)
		return ok && s.IsLastCheckValid
	}, 2*time.Second, 10*time.Millisecond)

	// Close the stream to simulate a disconnect.
	close(stream.Ch)

	// The store should be marked unreachable.
	require.Eventually(t, func() bool {
		s, ok := poolerStore.Get(key)
		return ok && !s.IsLastCheckValid && !s.StreamConnected
	}, 2*time.Second, 10*time.Millisecond, "pooler should be marked unreachable after disconnect")

	// LastSeen should remain from the last successful snapshot, not cleared.
	s, _ := poolerStore.Get(key)
	require.NotNil(t, s.LastSeen)
}

// TestHealthStream_ConcurrentWatcherUpdate tests that a topology update written by the
// PoolerWatcher while a snapshot is being applied is not overwritten (DoUpdate semantics).
func TestHealthStream_ConcurrentWatcherUpdate(t *testing.T) {
	ctx := t.Context()

	fakeClient := rpcclient.NewFakeClient()
	streamCh := make(chan *rpcclient.FakeManagerHealthStream, 1)
	fakeClient.OnManagerHealthStream = func(_ string, s *rpcclient.FakeManagerHealthStream) {
		streamCh <- s
	}

	poolerStore := store.NewPoolerStore(fakeClient, slog.Default())
	sm := newTestHealthStream(ctx, fakeClient, poolerStore)

	poolerID := &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"}
	key := seedPooler(poolerStore, poolerID, clustermetadata.PoolerType_REPLICA)

	sm.Start(key)

	stream := <-streamCh
	waitForStart(t, stream.Sent)

	// Concurrently promote the pooler in the topology (simulates PoolerWatcher update)
	// before the snapshot is applied.
	go func() {
		time.Sleep(5 * time.Millisecond)
		poolerStore.DoUpdate(key, func(existing *multiorchdatapb.PoolerHealthState) *multiorchdatapb.PoolerHealthState {
			existing.MultiPooler.Type = clustermetadata.PoolerType_PRIMARY
			return existing
		})
	}()

	stream.Ch <- makeSnapshot(&multipoolermanagerdatapb.Status{
		PoolerType:      clustermetadata.PoolerType_REPLICA,
		PostgresRunning: true,
	})

	require.Eventually(t, func() bool {
		s, ok := poolerStore.Get(key)
		return ok && s.IsLastCheckValid
	}, 2*time.Second, 10*time.Millisecond)

	result, _ := poolerStore.Get(key)
	// The watcher's topology promotion must be preserved.
	require.Equal(t, clustermetadata.PoolerType_PRIMARY, result.MultiPooler.Type,
		"watcher's topology update should not be overwritten by snapshot")
	// Health fields from the snapshot should still be applied.
	require.True(t, result.IsLastCheckValid)
	require.True(t, result.IsUpToDate)
}

// TestHealthStream_DeletedDuringStream tests that a pooler deleted from the store while a
// snapshot is in-flight is not resurrected by the apply.
func TestHealthStream_DeletedDuringStream(t *testing.T) {
	ctx := t.Context()

	fakeClient := rpcclient.NewFakeClient()
	streamCh := make(chan *rpcclient.FakeManagerHealthStream, 1)
	fakeClient.OnManagerHealthStream = func(_ string, s *rpcclient.FakeManagerHealthStream) {
		streamCh <- s
	}

	poolerStore := store.NewPoolerStore(fakeClient, slog.Default())
	sm := newTestHealthStream(ctx, fakeClient, poolerStore)

	poolerID := &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"}
	key := seedPooler(poolerStore, poolerID, clustermetadata.PoolerType_PRIMARY)

	sm.Start(key)

	stream := <-streamCh
	waitForStart(t, stream.Sent)

	// Delete the pooler from the store before the snapshot arrives.
	poolerStore.Delete(key)

	stream.Ch <- makeSnapshot(&multipoolermanagerdatapb.Status{
		PoolerType:      clustermetadata.PoolerType_PRIMARY,
		PostgresRunning: true,
	})

	// Give applySnapshot time to run.
	time.Sleep(100 * time.Millisecond)

	_, ok := poolerStore.Get(key)
	require.False(t, ok, "deleted pooler should not be resurrected by a snapshot")
}

// TestHealthStream_LastPostgresReadyTime tests that LastPostgresReadyTime is set/preserved correctly.
func TestHealthStream_LastPostgresReadyTime(t *testing.T) {
	t.Run("sets LastPostgresReadyTime when PostgresReady is true", func(t *testing.T) {
		ctx := t.Context()

		fakeClient := rpcclient.NewFakeClient()
		streamCh := make(chan *rpcclient.FakeManagerHealthStream, 1)
		fakeClient.OnManagerHealthStream = func(_ string, s *rpcclient.FakeManagerHealthStream) {
			streamCh <- s
		}

		poolerStore := store.NewPoolerStore(fakeClient, slog.Default())
		sm := newTestHealthStream(ctx, fakeClient, poolerStore)
		poolerID := &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"}
		key := seedPooler(poolerStore, poolerID, clustermetadata.PoolerType_PRIMARY)

		sm.Start(key)
		stream := <-streamCh
		waitForStart(t, stream.Sent)

		before := time.Now()
		stream.Ch <- makeSnapshot(&multipoolermanagerdatapb.Status{
			PoolerType:    clustermetadata.PoolerType_PRIMARY,
			PostgresReady: true,
		})

		require.Eventually(t, func() bool {
			s, ok := poolerStore.Get(key)
			return ok && s.LastPostgresReadyTime != nil
		}, 2*time.Second, 10*time.Millisecond)

		updated, _ := poolerStore.Get(key)
		require.True(t, updated.LastPostgresReadyTime.AsTime().After(before))
	})

	t.Run("preserves LastPostgresReadyTime when PostgresReady is false", func(t *testing.T) {
		ctx := t.Context()

		fakeClient := rpcclient.NewFakeClient()
		streamCh := make(chan *rpcclient.FakeManagerHealthStream, 1)
		fakeClient.OnManagerHealthStream = func(_ string, s *rpcclient.FakeManagerHealthStream) {
			streamCh <- s
		}

		poolerStore := store.NewPoolerStore(fakeClient, slog.Default())
		sm := newTestHealthStream(ctx, fakeClient, poolerStore)
		poolerID := &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler2"}
		key := topoclient.MultiPoolerIDString(poolerID)
		lastReadyTime := timestamppb.New(time.Now().Add(-10 * time.Second))
		poolerStore.Set(key, &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadata.MultiPooler{
				Id: poolerID, Database: "mydb", TableGroup: "tg1", Shard: "0",
				Type: clustermetadata.PoolerType_PRIMARY, Hostname: "host2",
				PortMap: map[string]int32{"grpc": 5432},
			},
			LastPostgresReadyTime: lastReadyTime,
		})

		sm.Start(key)
		stream := <-streamCh
		waitForStart(t, stream.Sent)

		stream.Ch <- makeSnapshot(&multipoolermanagerdatapb.Status{
			PoolerType:    clustermetadata.PoolerType_PRIMARY,
			PostgresReady: false,
		})

		require.Eventually(t, func() bool {
			s, ok := poolerStore.Get(key)
			return ok && s.IsLastCheckValid
		}, 2*time.Second, 10*time.Millisecond)

		updated, _ := poolerStore.Get(key)
		require.NotNil(t, updated.LastPostgresReadyTime)
		require.WithinDuration(t, lastReadyTime.AsTime(), updated.LastPostgresReadyTime.AsTime(), time.Second,
			"LastPostgresReadyTime should not change when PostgresReady is false")
	})
}

// TestHealthStream_StalenessTimeout verifies that a stream that stops sending messages
// is detected and triggers a reconnect. This exercises the application-level watchdog
// in streamOnce, which catches "live TCP but silent server goroutine" failures that
// gRPC keepalive does not cover.
func TestHealthStream_StalenessTimeout(t *testing.T) {
	ctx := t.Context()

	fakeClient := rpcclient.NewFakeClient()
	// Buffer 2: first stream + reconnect stream.
	streamCh := make(chan *rpcclient.FakeManagerHealthStream, 2)
	fakeClient.OnManagerHealthStream = func(_ string, s *rpcclient.FakeManagerHealthStream) {
		streamCh <- s
	}

	poolerStore := store.NewPoolerStore(fakeClient, slog.Default())
	// Use a very short staleness timeout so the test completes quickly.
	sm := newTestHealthStream(ctx, fakeClient, poolerStore, WithStalenessTimeout(100*time.Millisecond))

	poolerID := &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "silent-pooler"}
	key := seedPooler(poolerStore, poolerID, clustermetadata.PoolerType_PRIMARY)

	sm.Start(key)

	// First stream connection.
	stream := <-streamCh
	waitForStart(t, stream.Sent)

	// Send one snapshot so the stream is marked connected.
	stream.Ch <- makeSnapshot(&multipoolermanagerdatapb.Status{
		PoolerType:    clustermetadata.PoolerType_PRIMARY,
		PostgresReady: true,
	})
	require.Eventually(t, func() bool {
		s, ok := poolerStore.Get(key)
		return ok && s.IsLastCheckValid
	}, 2*time.Second, 10*time.Millisecond, "initial snapshot should be applied")

	// Now let the stream go silent — don't close it, don't send anything.
	// The staleness watchdog should fire after 100ms and trigger a reconnect.

	// Wait for the pooler to be marked unreachable.
	require.Eventually(t, func() bool {
		s, ok := poolerStore.Get(key)
		return ok && !s.IsLastCheckValid
	}, 2*time.Second, 10*time.Millisecond, "pooler should be marked unreachable after staleness timeout")

	// The stream manager should reconnect — a second stream must be dialled.
	select {
	case <-streamCh:
		// Reconnect stream arrived — staleness detection and reconnect work correctly.
	case <-time.After(2 * time.Second):
		t.Fatal("expected reconnect after staleness timeout, but no second stream was opened")
	}
}

// TestHealthStream_TypeMismatch tests that when a pooler reports a different type than topology,
// both are preserved: MultiPooler.Type stays as topology, PoolerType reflects what the pooler reports.
func TestHealthStream_TypeMismatch(t *testing.T) {
	ctx := t.Context()

	fakeClient := rpcclient.NewFakeClient()
	streamCh := make(chan *rpcclient.FakeManagerHealthStream, 1)
	fakeClient.OnManagerHealthStream = func(_ string, s *rpcclient.FakeManagerHealthStream) {
		streamCh <- s
	}

	poolerStore := store.NewPoolerStore(fakeClient, slog.Default())
	sm := newTestHealthStream(ctx, fakeClient, poolerStore)

	poolerID := &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "confused-pooler"}
	// Topology says REPLICA.
	key := seedPooler(poolerStore, poolerID, clustermetadata.PoolerType_REPLICA)

	sm.Start(key)
	stream := <-streamCh
	waitForStart(t, stream.Sent)

	// Pooler reports PRIMARY (type mismatch).
	stream.Ch <- makeSnapshot(&multipoolermanagerdatapb.Status{
		PoolerType: clustermetadata.PoolerType_PRIMARY,
		PrimaryStatus: &multipoolermanagerdatapb.PrimaryStatus{
			Lsn:   "0/FFFFFF",
			Ready: true,
		},
	})

	require.Eventually(t, func() bool {
		s, ok := poolerStore.Get(key)
		return ok && s.IsLastCheckValid
	}, 2*time.Second, 10*time.Millisecond)

	updated, _ := poolerStore.Get(key)
	require.Equal(t, clustermetadata.PoolerType_REPLICA, updated.MultiPooler.Type,
		"topology type should remain REPLICA")
	require.Equal(t, clustermetadata.PoolerType_PRIMARY, updated.PoolerType,
		"reported type should be PRIMARY")
	require.NotNil(t, updated.PrimaryStatus)
	require.Equal(t, "0/FFFFFF", updated.PrimaryStatus.Lsn)
	require.True(t, updated.PrimaryStatus.Ready)
}
