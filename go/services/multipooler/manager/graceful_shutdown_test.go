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

package manager

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// recordingPgctldClient records each Stop call and returns whatever the
// supplied stopFn dictates per mode. Other methods panic — keep usage scoped
// to graceful-shutdown tests.
type recordingPgctldClient struct {
	stubPgctldClient
	mu     sync.Mutex
	calls  []string
	stopFn func(mode string) error
}

func (r *recordingPgctldClient) Stop(_ context.Context, req *pgctldpb.StopRequest, _ ...grpc.CallOption) (*pgctldpb.StopResponse, error) {
	r.mu.Lock()
	r.calls = append(r.calls, req.GetMode())
	r.mu.Unlock()
	if r.stopFn != nil {
		if err := r.stopFn(req.GetMode()); err != nil {
			return nil, err
		}
	}
	return &pgctldpb.StopResponse{}, nil
}

func (r *recordingPgctldClient) modesCalled() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.calls))
	copy(out, r.calls)
	return out
}

// newGracefulShutdownTestManager constructs a MultiPoolerManager wired with
// stubs sufficient to exercise GracefulShutdown without needing topology,
// gRPC services, or a real connection pool. The healthStreamer is constructed
// because broadcastHealth is called on it; other subsystems are nil and must
// not be touched by the code under test.
func newGracefulShutdownTestManager(t *testing.T, pgctldClient pgctldpb.PgCtldClient) *MultiPoolerManager {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))

	id := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test",
	}
	return &MultiPoolerManager{
		logger:         logger,
		serviceID:      id,
		config:         &Config{},
		pgctldClient:   pgctldClient,
		healthStreamer: newHealthStreamer(logger, id, "tg", "0"),
		actionLock:     NewActionLock(),
		// Fresh on-disk consensus state at a temp dir. announceLeaderResignationLocked
		// reads primary term via primaryTermLocked -> getConsensusStatus -> ConsensusState;
		// without this it nil-pointers. With no revocation file it reads as term 0,
		// matching the "not currently the leader" path so the test exercises a no-op
		// announce.
		consensusState: NewConsensusState(t.TempDir(), id),
	}
}

// TestGracefulShutdown_StopSucceedsOnSmart verifies the escalation stops at
// the first mode that succeeds.
func TestGracefulShutdown_StopSucceedsOnSmart(t *testing.T) {
	pgctld := &recordingPgctldClient{}
	pm := newGracefulShutdownTestManager(t, pgctld)

	pm.GracefulShutdown(context.Background())

	assert.Equal(t, []string{"smart"}, pgctld.modesCalled(),
		"smart should succeed on first try; no escalation expected")
}

// TestGracefulShutdown_StopEscalatesThroughModes verifies the escalation chain
// when each preceding mode fails.
func TestGracefulShutdown_StopEscalatesThroughModes(t *testing.T) {
	pgctld := &recordingPgctldClient{
		stopFn: func(mode string) error {
			if mode == "immediate" {
				return nil
			}
			return errors.New("pgctld stop failed")
		},
	}
	pm := newGracefulShutdownTestManager(t, pgctld)

	pm.GracefulShutdown(context.Background())

	assert.Equal(t, []string{"smart", "fast", "immediate"}, pgctld.modesCalled(),
		"escalation should walk smart -> fast -> immediate when each preceding mode fails")
}

// TestGracefulShutdown_StopAllModesFail verifies we still return without
// panicking even when every mode fails.
func TestGracefulShutdown_StopAllModesFail(t *testing.T) {
	pgctld := &recordingPgctldClient{
		stopFn: func(string) error { return errors.New("pgctld stop failed") },
	}
	pm := newGracefulShutdownTestManager(t, pgctld)

	pm.GracefulShutdown(context.Background())

	assert.Equal(t, []string{"smart", "fast", "immediate"}, pgctld.modesCalled(),
		"all three modes should have been attempted")
}

// TestGracefulShutdown_NilPgctldClient verifies GracefulShutdown returns
// cleanly when pgctld is not configured (the announce path still runs and
// must not panic).
func TestGracefulShutdown_NilPgctldClient(t *testing.T) {
	pm := newGracefulShutdownTestManager(t, nil)

	require.NotPanics(t, func() {
		pm.GracefulShutdown(context.Background())
	})
}

// TestGracefulShutdown_AnnouncesBeforeStop is a regression test for the
// ordering bug where primaryTermLocked was called after pgctld.Stop:
// primaryTermLocked reads from postgres (via rules.observePosition), so it
// would silently no-op once postgres was down and the coordinator would have
// to wait for stream-EOF + LeaderIsDead grace period instead of firing
// LeaderResignedAnalyzer on REQUESTING_DEMOTION.
//
// The fake ruleStore is configured to fail observePosition the moment
// pgctld.Stop has been called. If GracefulShutdown stops postgres before
// announcing, observePosition fails and resignedLeaderAtTerm stays 0 — the
// assertion that resignedLeaderAtTerm == primary_term would then fail.
func TestGracefulShutdown_AnnouncesBeforeStop(t *testing.T) {
	const primaryTerm int64 = 42

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	id := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test",
	}

	// rules returns a position where this pooler is leader at term=primaryTerm,
	// but only until pgctld.Stop is first called — afterwards observePosition
	// fails, mirroring the "postgres is down" reality.
	rules := &fakeRuleStore{
		pos: &clustermetadatapb.PoolerPosition{
			Rule: &clustermetadatapb.ShardRule{
				LeaderId:   id,
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: primaryTerm},
			},
		},
	}

	pgctld := &recordingPgctldClient{
		stopFn: func(string) error {
			// Simulate postgres going away: observePosition starts failing.
			rules.mu.Lock()
			rules.observeErr = errors.New("postgres unreachable: process stopped")
			rules.mu.Unlock()
			return nil
		},
	}

	pm := &MultiPoolerManager{
		logger:         logger,
		serviceID:      id,
		config:         &Config{},
		pgctldClient:   pgctld,
		healthStreamer: newHealthStreamer(logger, id, "tg", "0"),
		actionLock:     NewActionLock(),
		consensusState: NewConsensusState(t.TempDir(), id),
		rules:          rules,
	}

	pm.GracefulShutdown(context.Background())

	require.Equal(t, []string{"smart"}, pgctld.modesCalled(),
		"pgctld.Stop should have been called once (smart succeeded)")

	pm.mu.Lock()
	got := pm.resignedLeaderAtTerm
	pm.mu.Unlock()
	require.Equal(t, primaryTerm, got,
		"resignedLeaderAtTerm must be set to the primary term; if 0, the announce was "+
			"sequenced AFTER pgctld.Stop and observePosition failed for the dead postgres")
}
