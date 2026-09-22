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

package grpcserver

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchpb "github.com/multigres/multigres/go/pb/multiorch"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/consensus"
	"github.com/multigres/multigres/go/services/multiorch/recovery"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// newTestEngine builds a real Engine watching exactly one database, with an
// empty pooler cache - enough to exercise GetShardStatus's validation and
// watch-target logic without needing cache seeding (which the recovery
// package keeps unexported; full happy-path coverage with real problems is
// integration-test territory, see /mt-dev integration multiorch).
func newTestEngine(t *testing.T, watchTargets []config.WatchTarget) *recovery.Engine {
	t.Helper()
	ts := memorytopo.NewServer(context.Background(), "zone1")
	t.Cleanup(func() { ts.Close() })
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := config.NewTestConfig(config.WithCell("zone1"))
	fakeClient := &rpcclient.FakeClient{}
	coordID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Cell: "zone1", Name: "test-coordinator"}
	coordinator := consensus.NewCoordinator(coordID, ts, fakeClient, logger)
	return recovery.NewEngine(ts, logger, cfg, watchTargets, fakeClient, coordinator)
}

func TestGetShardStatus_RequiresShardKey(t *testing.T) {
	engine := newTestEngine(t, []config.WatchTarget{{Database: "db"}})
	s := NewMultiorchServer(engine, nil, slog.Default())

	_, err := s.GetShardStatus(t.Context(), &multiorchpb.ShardStatusRequest{})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestGetShardStatus_NotFoundForUnwatchedShard(t *testing.T) {
	engine := newTestEngine(t, []config.WatchTarget{{Database: "other-db"}})
	s := NewMultiorchServer(engine, nil, slog.Default())

	_, err := s.GetShardStatus(t.Context(), &multiorchpb.ShardStatusRequest{
		ShardKey: &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
	})
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestGetShardStatus_EmptyForWatchedShardWithNoProblems(t *testing.T) {
	engine := newTestEngine(t, []config.WatchTarget{{Database: "db"}})
	s := NewMultiorchServer(engine, nil, slog.Default())

	resp, err := s.GetShardStatus(t.Context(), &multiorchpb.ShardStatusRequest{
		ShardKey: &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
	})
	require.NoError(t, err)
	require.Empty(t, resp.Problems)
	require.Empty(t, resp.PoolerHealths)
}

func TestGetWatchedShards_DelegatesToEngine(t *testing.T) {
	engine := newTestEngine(t, []config.WatchTarget{{Database: "db"}})
	s := NewMultiorchServer(engine, nil, slog.Default())

	resp, err := s.GetWatchedShards(t.Context(), &multiorchpb.GetWatchedShardsRequest{})
	require.NoError(t, err)
	require.Empty(t, resp.ShardKeys, "no poolers seeded, so no shards are known yet")
}

func testProblemState(resolved bool) recovery.ProblemState {
	now := time.Now()
	state := recovery.ProblemState{
		LastKnown: types.Problem{
			Code:        "LeaderUnhealthy",
			CheckName:   "LeaderNeedsReplacement",
			PoolerID:    &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler-1"},
			ShardKey:    &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
			Description: "leader postgres unhealthy",
			Priority:    1000,
			Scope:       types.ScopeShard,
		},
		BrokenSince:     now.Add(-time.Hour),
		OccurrenceCount: 3,
	}
	if resolved {
		state.ResolvedSince = now.Add(-time.Minute)
	}
	return state
}

func TestDetectedProblemFromState_BasicFields(t *testing.T) {
	state := testProblemState(false)
	dp := detectedProblemFromState(state, recovery.ActionAttemptHistory{}, nil)

	require.Equal(t, "LeaderUnhealthy", dp.Code)
	require.Equal(t, "LeaderNeedsReplacement", dp.CheckName)
	require.Equal(t, "pooler-1", dp.PoolerId.GetName())
	require.Equal(t, "db", dp.ShardKey.GetDatabase())
	require.Equal(t, "leader postgres unhealthy", dp.Description)
	require.Equal(t, int32(1000), dp.Priority)
	require.Equal(t, string(types.ScopeShard), dp.Scope)
	require.Equal(t, state.BrokenSince.Unix(), dp.BrokenSince.AsTime().Unix())
	require.Equal(t, state.BrokenSince.Unix(), dp.DetectedAt.AsTime().Unix(), "detected_at mirrors broken_since")
	require.Equal(t, int32(3), dp.OccurrenceCount)
}

func TestDetectedProblemFromState_ResolvedVsActive(t *testing.T) {
	active := detectedProblemFromState(testProblemState(false), recovery.ActionAttemptHistory{}, nil)
	require.Nil(t, active.ResolvedSince, "an active problem must not carry a resolved_since")

	resolvedState := testProblemState(true)
	resolved := detectedProblemFromState(resolvedState, recovery.ActionAttemptHistory{}, nil)
	require.NotNil(t, resolved.ResolvedSince)
	require.Equal(t, resolvedState.ResolvedSince.Unix(), resolved.ResolvedSince.AsTime().Unix())
}

func TestDetectedProblemFromState_AttemptHistory(t *testing.T) {
	now := time.Now()
	hist := recovery.ActionAttemptHistory{
		TotalAttempts: 7,
		RecentAttempts: []recovery.AttemptRecord{
			{At: now.Add(-time.Minute), TriggeringCode: "LeaderUnhealthy", CompletedAt: now.Add(-time.Second * 30), Error: "boom"},
			{At: now}, // still in progress: no CompletedAt
		},
	}

	dp := detectedProblemFromState(testProblemState(false), hist, nil)
	require.Equal(t, int32(7), dp.TotalAttempts, "total_attempts reflects all-time count, not just the ring size")
	require.Len(t, dp.RecentAttempts, 2)

	require.NotNil(t, dp.RecentAttempts[0].CompletedAt)
	require.Equal(t, "boom", dp.RecentAttempts[0].Error)
	require.Equal(t, "LeaderUnhealthy", dp.RecentAttempts[0].TriggeringCode)

	require.Nil(t, dp.RecentAttempts[1].CompletedAt, "an in-progress attempt must not carry completed_at")
	require.Empty(t, dp.RecentAttempts[1].Error)
}

func TestDetectedProblemFromState_NextEligibleAttempt(t *testing.T) {
	withoutGate := detectedProblemFromState(testProblemState(false), recovery.ActionAttemptHistory{}, nil)
	require.Nil(t, withoutGate.NextEligibleAttemptAt, "unset when there's nothing to wait on")

	readyAt := time.Now().Add(30 * time.Second)
	withGate := detectedProblemFromState(testProblemState(false), recovery.ActionAttemptHistory{}, &readyAt)
	require.NotNil(t, withGate.NextEligibleAttemptAt)
	require.Equal(t, readyAt.Unix(), withGate.NextEligibleAttemptAt.AsTime().Unix())
}
