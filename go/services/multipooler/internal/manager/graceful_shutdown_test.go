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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/consensus"
)

// assertRecent fails the test if ts is nil or older than 5 seconds ago.
// Used by lifecycle tests to verify that a write timestamp was set in the
// recent past — a coarse sanity check rather than a precise comparison
// (proto Timestamps round-trip through topology and can lose nanosecond
// precision; we just want "this was written, not zero").
func assertRecent(t *testing.T, ts *timestamppb.Timestamp, msg string) {
	t.Helper()
	require.NotNil(t, ts, msg+": timestamp is nil")
	delta := time.Since(ts.AsTime())
	assert.Less(t, delta, 5*time.Second, msg+": timestamp not recent (delta %s)", delta)
}

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
// because setCohortEligibility -> broadcastHealth is called on it; other
// subsystems are nil and must not be touched by the code under test.
func newGracefulShutdownTestManager(t *testing.T, pgctldClient pgctldpb.PgCtldClient) *MultiPoolerManager {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))

	id := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test",
	}
	hs := newHealthStreamer(logger, id, "tg", "0")
	// Match the production default set by topoclient.NewMultiPooler so the
	// record's LifecycleStatus reads as STARTING from the start. (Real boot wires
	// this in via NewMultiPoolerManager(multiPooler).)
	record := newRecordFromProto(&clustermetadatapb.MultiPooler{
		Id: id,
		LifecycleStatus: &clustermetadatapb.PoolerLifecycle{
			Status: clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_STARTING,
		},
	})
	return &MultiPoolerManager{
		logger:         logger,
		serviceID:      id,
		config:         &Config{},
		pgctldClient:   pgctldClient,
		healthStreamer: hs,
		actionLock:     actionlock.NewActionLock(),
		// consensusMgr is required: GracefulShutdown drives cohort eligibility
		// through it (pm.consensusMgr.SetCohortEligibility).
		consensusMgr: consensus.NewManagerForTesting(t, id, consensus.NewConsensusPromises("", id), &fakeRuleStore{}, hs),
		record:       record,
		// GracefulShutdown transitions serving state to DISABLED; wire a real
		// StateManager (fanning out to the healthStreamer) so the shutdown path
		// runs as in production rather than relying on a nil-check.
		stateManager: NewStateManager(logger, record, func() bool { return true }, hs),
	}
}

// TestGracefulShutdown_StopSucceedsOnFast verifies the escalation stops at
// the first mode that succeeds.
func TestGracefulShutdown_StopSucceedsOnFast(t *testing.T) {
	pgctld := &recordingPgctldClient{}
	pm := newGracefulShutdownTestManager(t, pgctld)

	pm.GracefulShutdown(context.Background())

	assert.Equal(t, []string{"fast"}, pgctld.modesCalled(),
		"fast should succeed on first try; no escalation expected")
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

	assert.Equal(t, []string{"fast", "immediate"}, pgctld.modesCalled(),
		"escalation should walk fast -> immediate when fast fails")
}

// TestGracefulShutdown_StopAllModesFail verifies we still return without
// panicking even when every mode fails.
func TestGracefulShutdown_StopAllModesFail(t *testing.T) {
	pgctld := &recordingPgctldClient{
		stopFn: func(string) error { return errors.New("pgctld stop failed") },
	}
	pm := newGracefulShutdownTestManager(t, pgctld)

	pm.GracefulShutdown(context.Background())

	assert.Equal(t, []string{"fast", "immediate"}, pgctld.modesCalled(),
		"both modes should have been attempted")
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

// TestGracefulShutdown_AnnouncesStopping verifies the in-memory lifecycle
// flips to STOPPING under the action lock at the top of GracefulShutdown,
// before any blocking work.
func TestGracefulShutdown_AnnouncesStopping(t *testing.T) {
	pm := newGracefulShutdownTestManager(t, nil)
	seedRecordLifecycleForTest(t, pm, clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE)

	pm.GracefulShutdown(context.Background())

	assert.Equal(t,
		clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_STOPPING,
		pm.record.Snapshot().GetLifecycleStatus().GetStatus(),
		"GracefulShutdown must announce STOPPING in its in-memory state")
}

// seedRecordLifecycleForTest mutates the record's LifecycleStatus to the
// given value, briefly holding the action lock. Tests that need to put the
// manager into a specific lifecycle state before exercising shutdown call
// this directly because record.Mutate requires an action-locked context.
func seedRecordLifecycleForTest(t *testing.T, pm *MultiPoolerManager, status clustermetadatapb.PoolerLifecycleStatus) {
	t.Helper()
	lockCtx, err := pm.actionLock.Acquire(t.Context(), "test-seed-lifecycle")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)
	require.NoError(t, pm.record.Mutate(lockCtx, func(s *MutablePoolerRecordState) {
		s.LifecycleStatus = &clustermetadatapb.PoolerLifecycle{Status: status}
	}))
}

// TestGracefulShutdown_AnnouncesStoppingInTopology verifies that GracefulShutdown
// records STOPPING + reason + updated timestamp in topology, so operators see
// the announcement immediately rather than only after shutdown completes. The
// publisher is driven synchronously here via publishIfNeeded so the assertion
// doesn't depend on the goroutine clock.
func TestGracefulShutdown_AnnouncesStoppingInTopology(t *testing.T) {
	ctx := t.Context()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	id := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test",
	}
	mp := topoclient.NewMultiPooler(id.Name, id.Cell, "localhost")
	mp.ShardKey = &clustermetadatapb.ShardKey{TableGroup: "tg", Shard: "0"}
	mp.LifecycleStatus.Status = clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE
	require.NoError(t, ts.CreateMultiPooler(ctx, mp))

	pm := newGracefulShutdownTestManager(t, nil)
	pm.serviceID = id
	pm.topoClient = ts
	record, err := newPoolerRecord(pm.logger, ts, mp)
	require.NoError(t, err)
	pm.record = record

	pm.GracefulShutdown(ctx)

	// Drain the scheduled publish synchronously.
	pm.record.publishIfNeeded(ctx)

	stored, err := ts.GetMultiPooler(ctx, id)
	require.NoError(t, err)
	assert.Equal(t,
		clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_STOPPING,
		stored.GetLifecycleStatus().GetStatus(),
		"topology lifecycle must announce STOPPING")
	assert.Equal(t, "shutting down", stored.GetLifecycleStatus().GetReason(),
		"announcement should carry the canonical reason string")
	assertRecent(t, stored.GetLifecycleStatus().GetUpdated(),
		"STOPPING announcement should set the updated timestamp")
}

// TestMarkPoolerActive_TransitionsLifecycle verifies the STARTING → ACTIVE
// transition: markPoolerActive flips the record's LifecycleStatus to ACTIVE
// via record.Mutate and the publisher reflects it to topology. publishIfNeeded
// is driven synchronously here so the assertion doesn't race the goroutine
// clock.
func TestMarkPoolerActive_TransitionsLifecycle(t *testing.T) {
	ctx := t.Context()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	id := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test",
	}
	mp := topoclient.NewMultiPooler(id.Name, id.Cell, "localhost")
	mp.ShardKey = &clustermetadatapb.ShardKey{TableGroup: "tg", Shard: "0"}
	require.Equal(t,
		clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_STARTING,
		mp.GetLifecycleStatus().GetStatus(),
		"NewMultiPooler factory should default to STARTING")
	require.Equal(t, "process starting", mp.GetLifecycleStatus().GetReason(),
		"NewMultiPooler factory should annotate STARTING with a reason")
	assertRecent(t, mp.GetLifecycleStatus().GetUpdated(),
		"NewMultiPooler factory should set the updated timestamp")
	require.NoError(t, ts.CreateMultiPooler(ctx, mp))

	pm := newGracefulShutdownTestManager(t, nil)
	pm.serviceID = id
	pm.topoClient = ts
	record, err := newPoolerRecord(pm.logger, ts, mp)
	require.NoError(t, err)
	pm.record = record
	require.Equal(t,
		clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_STARTING,
		pm.record.Snapshot().GetLifecycleStatus().GetStatus(),
		"test manager should default to STARTING")

	pm.markPoolerActive(ctx)

	assert.Equal(t,
		clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE,
		pm.record.Snapshot().GetLifecycleStatus().GetStatus(),
		"in-memory lifecycle must be ACTIVE after markPoolerActive")

	// Drain the scheduled publish synchronously so we can assert against
	// topology without racing the publisher goroutine.
	pm.record.publishIfNeeded(ctx)

	stored, err := ts.GetMultiPooler(ctx, id)
	require.NoError(t, err)
	assert.Equal(t,
		clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE,
		stored.GetLifecycleStatus().GetStatus(),
		"topology lifecycle must be ACTIVE after markPoolerActive")
	assert.Equal(t, "pooler active", stored.GetLifecycleStatus().GetReason(),
		"markPoolerActive should annotate the topology write with a reason")
	assertRecent(t, stored.GetLifecycleStatus().GetUpdated(),
		"markPoolerActive should set the updated timestamp")
}

// TestMarkPoolerActive_Idempotent verifies that calling markPoolerActive
// on an already-ACTIVE pooler is a no-op. The pgMonitor invokes this on
// every tick once postgres is up; the cheap pre-check must short-circuit
// before record.Mutate (and the action lock) is touched.
func TestMarkPoolerActive_Idempotent(t *testing.T) {
	ctx := t.Context()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	id := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test",
	}
	mp := topoclient.NewMultiPooler(id.Name, id.Cell, "localhost")
	mp.ShardKey = &clustermetadatapb.ShardKey{TableGroup: "tg", Shard: "0"}
	mp.LifecycleStatus.Status = clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE
	require.NoError(t, ts.CreateMultiPooler(ctx, mp))

	pm := newGracefulShutdownTestManager(t, nil)
	pm.serviceID = id
	pm.topoClient = ts
	record, err := newPoolerRecord(pm.logger, ts, mp)
	require.NoError(t, err)
	pm.record = record

	pm.markPoolerActive(ctx)

	assert.Equal(t,
		clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE,
		pm.record.Snapshot().GetLifecycleStatus().GetStatus(),
		"in-memory lifecycle stays ACTIVE on idempotent call")

	// The cheap pre-check must short-circuit before record.Mutate; the
	// publisher's desired stays at the seeded ACTIVE so publishIfNeeded
	// either no-ops or writes the same value back.
	pm.record.publishIfNeeded(ctx)

	stored, err := ts.GetMultiPooler(ctx, id)
	require.NoError(t, err)
	assert.Equal(t,
		clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE,
		stored.GetLifecycleStatus().GetStatus(),
		"topology lifecycle stays ACTIVE on idempotent call")
	assert.Equal(t, mp.GetLifecycleStatus().GetUpdated().GetSeconds(), stored.GetLifecycleStatus().GetUpdated().GetSeconds(),
		"topology Updated seconds must not change on idempotent call")
	assert.Equal(t, mp.GetLifecycleStatus().GetUpdated().GetNanos(), stored.GetLifecycleStatus().GetUpdated().GetNanos(),
		"topology Updated nanos must not change on idempotent call")
}

// TestGracefulShutdown_AdvertisesCohortIneligibleBeforeStop is a regression
// test for the ordering guarantee that cohort-ineligibility is broadcast
// before pgctld.Stop. If the order flipped, the broadcast would race against
// stream EOF and the coordinator would have to fall back to LeaderIsDead's
// grace period instead of firing LeaderResignedAnalyzer immediately on the
// INELIGIBLE signal.
//
// pgctld.Stop's callback captures the cohort eligibility state at the moment
// of the call, so the assertion fails if the announce was sequenced after
// the stop.
func TestGracefulShutdown_AdvertisesCohortIneligibleBeforeStop(t *testing.T) {
	pm := newGracefulShutdownTestManager(t, nil)

	var atStopSignal clustermetadatapb.CohortEligibilitySignal
	pgctld := &recordingPgctldClient{
		stopFn: func(string) error {
			atStopSignal = pm.consensusMgr.CohortEligibility()
			return nil
		},
	}
	pm.pgctldClient = pgctld

	pm.GracefulShutdown(context.Background())

	require.Equal(t, []string{"fast"}, pgctld.modesCalled(),
		"pgctld.Stop should have been called once (fast succeeded)")
	require.Equal(t,
		clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE,
		atStopSignal,
		"cohort eligibility must be INELIGIBLE before pgctld.Stop runs; if it was "+
			"the default ELIGIBLE, the announce was sequenced AFTER stop and the "+
			"broadcast races stream EOF")

	finalSignal := pm.consensusMgr.CohortEligibility()
	require.Equal(t,
		clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE,
		finalSignal,
		"cohort eligibility must remain INELIGIBLE after GracefulShutdown returns")
}
