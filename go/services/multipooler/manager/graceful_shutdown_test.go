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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/services/multipooler/connpoolmanager"

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

// recordingPoolMgr is a minimal connpoolmanager.PoolManager that tracks when
// Close was called. Other methods inherit the embedded nil interface and
// panic if called — keep usage scoped to graceful-shutdown tests.
type recordingPoolMgr struct {
	connpoolmanager.PoolManager
	mu        sync.Mutex
	closedAt  time.Time
	closeHook func()
}

func (r *recordingPoolMgr) Close() {
	r.mu.Lock()
	if r.closedAt.IsZero() {
		r.closedAt = time.Now()
	}
	hook := r.closeHook
	r.mu.Unlock()
	if hook != nil {
		hook()
	}
}

func (r *recordingPoolMgr) closedAtTime() time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closedAt
}

// newGracefulShutdownTestManager constructs a MultiPoolerManager wired with
// stubs sufficient to exercise GracefulShutdown without needing topology,
// gRPC services, or a real connection pool. The healthStreamer is constructed
// because broadcastHealth is called on it; other subsystems are nil and must
// not be touched by the code under test. servingState is nil — graceful
// shutdown's NOT_SERVING transition is a no-op in these tests; the integration
// tests cover the full state-machine path.
func newGracefulShutdownTestManager(t *testing.T, cfg *Config, pgctldClient pgctldpb.PgCtldClient, poolMgr connpoolmanager.PoolManager) *MultiPoolerManager {
	t.Helper()

	applyGracefulShutdownDefaults(cfg)
	require.NoError(t, validateGracefulShutdownConfig(cfg))

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))

	id := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test",
	}
	pm := &MultiPoolerManager{
		logger:         logger,
		serviceID:      id,
		config:         cfg,
		pgctldClient:   pgctldClient,
		connPoolMgr:    poolMgr,
		healthStreamer: newHealthStreamer(logger, id, "tg", "0"),
		actionLock:     NewActionLock(),
	}
	return pm
}

// readLifecycleSignal returns the lifecycle signal under pm.mu so the test
// never races the production setter.
func readLifecycleSignal(pm *MultiPoolerManager) clustermetadatapb.LifecycleSignal {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return pm.lifecycleSignal
}

func TestValidateGracefulShutdownConfig(t *testing.T) {
	t.Run("zero config gets defaults filled and passes", func(t *testing.T) {
		cfg := &Config{}
		applyGracefulShutdownDefaults(cfg)
		require.NoError(t, validateGracefulShutdownConfig(cfg))
	})

	t.Run("negative timeout rejected", func(t *testing.T) {
		cfg := &Config{
			GracefulShutdownFastTimeout: -1 * time.Second,
		}
		applyGracefulShutdownDefaults(cfg)
		err := validateGracefulShutdownConfig(cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "non-negative")
	})

	t.Run("post-announce budget exceeding announced deadline rejected", func(t *testing.T) {
		// Each individual timeout is fine, but their sum exceeds the
		// announced deadline. The validator must catch this so the
		// safety-net cannot fire while pgctld is still legitimately
		// running.
		cfg := &Config{
			GracefulShutdownFastTimeout:          25 * time.Second,
			GracefulShutdownImmediateTimeout:     25 * time.Second,
			GracefulShutdownFinalSnapshotTimeout: 25 * time.Second,
		}
		applyGracefulShutdownDefaults(cfg)
		err := validateGracefulShutdownConfig(cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exceeds announced deadline")
	})
}

// TestGracefulShutdown_HappyPath verifies the full sequence when fast
// shutdown succeeds. Asserts pgctld is called exactly once with fast and
// that the final lifecycle state is STOPPED.
func TestGracefulShutdown_HappyPath(t *testing.T) {
	pgctld := &recordingPgctldClient{}
	pm := newGracefulShutdownTestManager(t,
		&Config{},
		pgctld,
		&recordingPoolMgr{},
	)

	pm.GracefulShutdown(context.Background())

	assert.Equal(t, []string{"fast"}, pgctld.modesCalled(),
		"fast should succeed on first try; no escalation expected")

	assert.Equal(t, clustermetadatapb.LifecycleSignal_LIFECYCLE_SIGNAL_STOPPED, readLifecycleSignal(pm),
		"final lifecycle state must be STOPPED")
}

// TestGracefulShutdown_NeverCallsSmart locks in that smart is no longer in
// the escalation chain. The pooler does the equivalent of smart's
// "wait for clients to disconnect" itself in the drain + Close phases, so
// invoking smart again would just be redundant timeout budget.
func TestGracefulShutdown_NeverCallsSmart(t *testing.T) {
	pgctld := &recordingPgctldClient{
		stopFn: func(_ string) error { return errors.New("force escalation") },
	}
	pm := newGracefulShutdownTestManager(t, &Config{}, pgctld, &recordingPoolMgr{})

	pm.GracefulShutdown(context.Background())

	for _, mode := range pgctld.modesCalled() {
		assert.NotEqual(t, "smart", mode, "smart must never be invoked")
	}
}

// TestGracefulShutdown_EscalatesFastToImmediate verifies that when fast
// fails, immediate is tried. Both modes should appear in the call log in
// order; smart must not appear at all.
func TestGracefulShutdown_EscalatesFastToImmediate(t *testing.T) {
	pgctld := &recordingPgctldClient{
		stopFn: func(mode string) error {
			if mode == "immediate" {
				return nil // immediate succeeds
			}
			return errors.New("simulated failure")
		},
	}
	pm := newGracefulShutdownTestManager(t,
		&Config{},
		pgctld,
		&recordingPoolMgr{},
	)

	pm.GracefulShutdown(context.Background())

	assert.Equal(t, []string{"fast", "immediate"}, pgctld.modesCalled(),
		"both modes should be tried in order, with no smart")

	assert.Equal(t, clustermetadatapb.LifecycleSignal_LIFECYCLE_SIGNAL_STOPPED, readLifecycleSignal(pm))
}

// TestGracefulShutdown_AllModesFail verifies that even when every pgctld.Stop
// invocation errors, the final STOPPED snapshot is still emitted. The
// orchestrator must always learn we've left, regardless of pgctld's state.
func TestGracefulShutdown_AllModesFail(t *testing.T) {
	pgctld := &recordingPgctldClient{
		stopFn: func(mode string) error {
			return errors.New("simulated failure")
		},
	}
	pm := newGracefulShutdownTestManager(t,
		&Config{},
		pgctld,
		&recordingPoolMgr{},
	)

	pm.GracefulShutdown(context.Background())

	assert.Equal(t, []string{"fast", "immediate"}, pgctld.modesCalled())

	assert.Equal(t, clustermetadatapb.LifecycleSignal_LIFECYCLE_SIGNAL_STOPPED, readLifecycleSignal(pm),
		"STOPPED must be set even if every pgctld.Stop fails")
}

// TestGracefulShutdown_ClosesPoolBeforePgctld verifies the order: the pool
// manager's Close runs before any pgctld.Stop call, so postgres has no
// lingering pooler connections when shutdown actually runs.
func TestGracefulShutdown_ClosesPoolBeforePgctld(t *testing.T) {
	var firstPgctldAt atomic.Int64

	pgctld := &recordingPgctldClient{
		stopFn: func(_ string) error {
			firstPgctldAt.CompareAndSwap(0, time.Now().UnixNano())
			return nil
		},
	}
	pool := &recordingPoolMgr{}

	pm := newGracefulShutdownTestManager(t, &Config{}, pgctld, pool)

	pm.GracefulShutdown(context.Background())

	closedAt := pool.closedAtTime()
	require.False(t, closedAt.IsZero(), "Close must have been called")

	pgctldAt := time.Unix(0, firstPgctldAt.Load())
	require.False(t, pgctldAt.IsZero(), "pgctld.Stop must have been called")

	assert.True(t, !closedAt.After(pgctldAt),
		"connection pool Close must precede pgctld.Stop (close=%s, pgctld=%s)",
		closedAt, pgctldAt)
}

// TestGracefulShutdown_LifecycleSignalSetOnlyAtEnd verifies that the
// lifecycle signal is NOT set during the slow phases (drain, pool close,
// pgctld.Stop). It is set to STOPPED exactly once, right before the terminal
// broadcast closes subscriber channels. The test uses a pgctld stub that
// snapshots the manager's lifecycle state at the moment Stop is called; at
// that point the signal must still be UNKNOWN.
func TestGracefulShutdown_LifecycleSignalSetOnlyAtEnd(t *testing.T) {
	var (
		observedSignal clustermetadatapb.LifecycleSignal
		observedOnce   sync.Once
	)
	var pm *MultiPoolerManager

	pgctld := &recordingPgctldClient{
		stopFn: func(_ string) error {
			observedOnce.Do(func() {
				observedSignal = readLifecycleSignal(pm)
			})
			return nil
		},
	}
	pm = newGracefulShutdownTestManager(t, &Config{}, pgctld, &recordingPoolMgr{})

	pm.GracefulShutdown(context.Background())

	assert.Equal(t, clustermetadatapb.LifecycleSignal_LIFECYCLE_SIGNAL_UNKNOWN, observedSignal,
		"lifecycle must still be UNKNOWN when pgctld.Stop is called")
	assert.Equal(t, clustermetadatapb.LifecycleSignal_LIFECYCLE_SIGNAL_STOPPED, readLifecycleSignal(pm),
		"final lifecycle state must be STOPPED")
}

// TestGracefulShutdown_SafetyNetExits verifies that if the goroutine running
// GracefulShutdown overruns the announced deadline, the process-exit hook
// fires. We swap gracefulShutdownExitFunc for a capture and configure pgctld
// to block past the announced deadline.
func TestGracefulShutdown_SafetyNetExits(t *testing.T) {
	// Save and restore the package-level exit hook.
	origExit := gracefulShutdownExitFunc
	defer func() { gracefulShutdownExitFunc = origExit }()

	var exited atomic.Int32
	exitedCh := make(chan struct{}, 1)
	gracefulShutdownExitFunc = func(code int) {
		if exited.CompareAndSwap(0, 1) {
			exitedCh <- struct{}{}
		}
	}

	// pgctld stop blocks past the announced deadline (60s) so GracefulShutdown
	// overruns. The safety net must still fire.
	pgctld := &recordingPgctldClient{
		stopFn: func(_ string) error {
			time.Sleep(70 * time.Second)
			return nil
		},
	}
	pm := newGracefulShutdownTestManager(t, &Config{}, pgctld, &recordingPoolMgr{})

	// Run shutdown in a goroutine because pgctld will block. The safety net
	// should fire well before the goroutine returns naturally.
	go pm.GracefulShutdown(context.Background())

	select {
	case <-exitedCh:
		// Safety-net fired; great.
	case <-time.After(75 * time.Second):
		t.Fatal("safety-net os.Exit hook never fired")
	}
}

// TestGracefulShutdown_SafetyNetFlushesTelemetry verifies that the safety-net
// invokes the configured FlushTelemetry hook before calling the exit func,
// and that it does so even when the hook returns an error.
func TestGracefulShutdown_SafetyNetFlushesTelemetry(t *testing.T) {
	origExit := gracefulShutdownExitFunc
	defer func() { gracefulShutdownExitFunc = origExit }()

	// Ordering channel so we can assert flush-before-exit.
	events := make(chan string, 4)

	gracefulShutdownExitFunc = func(int) {
		events <- "exit"
	}

	pgctld := &recordingPgctldClient{
		stopFn: func(_ string) error {
			time.Sleep(70 * time.Second)
			return nil
		},
	}
	cfg := &Config{
		FlushTelemetry: func(context.Context) error {
			events <- "flush"
			return errors.New("simulated flush failure") // must not block exit
		},
	}
	pm := newGracefulShutdownTestManager(t, cfg, pgctld, &recordingPoolMgr{})

	go pm.GracefulShutdown(context.Background())

	// Expect "flush" then "exit", both within the safety-net + flush-timeout
	// window (60s + 2s + slack).
	var got []string
	for range 2 {
		select {
		case ev := <-events:
			got = append(got, ev)
		case <-time.After(75 * time.Second):
			t.Fatalf("safety-net did not complete; got events so far: %v", got)
		}
	}
	assert.Equal(t, []string{"flush", "exit"}, got,
		"safety-net must flush telemetry before forcing exit")
}

// TestGracefulShutdown_NilPgctldClient verifies the sequence completes (and
// reaches STOPPED) even when no pgctld client is wired. The orchestrator
// must still see the final snapshot in this degraded mode.
func TestGracefulShutdown_NilPgctldClient(t *testing.T) {
	pm := newGracefulShutdownTestManager(t,
		&Config{},
		nil, // no pgctld client
		&recordingPoolMgr{},
	)

	pm.GracefulShutdown(context.Background())

	assert.Equal(t, clustermetadatapb.LifecycleSignal_LIFECYCLE_SIGNAL_STOPPED, readLifecycleSignal(pm))
}

// TestGracefulShutdown_TerminalBroadcastClosesSubscriberChannels verifies that
// the final STOPPED broadcast closes subscriber channels so gRPC stream
// handlers return cleanly via the channel-close path. Without this,
// grpcServer.GracefulStop would block waiting for stream contexts that nothing
// cancels, and the process would hang until servenv's --onterm-timeout
// force-kill expires.
func TestGracefulShutdown_TerminalBroadcastClosesSubscriberChannels(t *testing.T) {
	pm := newGracefulShutdownTestManager(t, &Config{}, &recordingPgctldClient{}, &recordingPoolMgr{})

	// Subscribe before shutdown so we have a real channel to observe.
	_, ch := pm.healthStreamer.subscribe()

	pm.GracefulShutdown(context.Background())

	// Drain the channel and verify it closes. Buffered snapshots come first;
	// the close arrives once they've been read.
	closed := false
	for !closed {
		select {
		case _, ok := <-ch:
			if !ok {
				closed = true
			}
		case <-time.After(2 * time.Second):
			t.Fatal("subscriber channel never closed after terminal broadcast")
		}
	}
	// Sanity: a follow-up Broadcast should not deadlock or panic because the
	// client map is empty after Shutdown closed it.
	pm.healthStreamer.Broadcast()
}
