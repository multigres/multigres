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
	"os"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// gracefulShutdownExitFunc is the function called when the safety-net deadline
// fires while the graceful-shutdown goroutine is still running. Overridable
// for tests; defaults to os.Exit so production builds force-exit the process
// when the announced deadline elapses.
//
// os.Exit is deliberate here: the safety net exists precisely because the
// regular cleanup path failed to complete in time, so running cleanup hooks
// would just hang again. The orchestrator's stream-EOF path relies on this
// process actually exiting so failover can proceed.
//
//nolint:forbidigo // intentional bypass; see comment above
var gracefulShutdownExitFunc = os.Exit

// safetyNetTelemetryFlushTimeout bounds how long the safety-net callback waits
// for FlushTelemetry to drain OTel pipelines before calling os.Exit. Tight
// because the safety-net already fires AT the announced deadline; anything
// past it delays process exit (and therefore the orchestrator's
// post-EOF failover) by the flush duration. OTel exporters run on
// independent goroutines, so the flush typically succeeds even when the
// main GracefulShutdown goroutine is hung.
const safetyNetTelemetryFlushTimeout = 2 * time.Second

// Defaults for graceful-shutdown timeouts. Exported because services/multipooler
// uses them as flag defaults too — keeping a single source of truth across the
// flag layer (init.go) and the manager-side fallback used by tests.
//
// Smart shutdown is intentionally absent: the pooler drains and closes its
// pool itself in the steps that precede pgctld.Stop, so smart's "wait for
// clients" semantics would just be redundant timeout budget. The escalation
// chain is fast → immediate.
const (
	DefaultGracefulShutdownFastTimeout          = 5 * time.Second
	DefaultGracefulShutdownImmediateTimeout     = 5 * time.Second
	DefaultGracefulShutdownFinalSnapshotTimeout = 5 * time.Second
)

// gracefulShutdownAnnouncedDeadline is the wall-clock budget the pooler
// allows itself before the safety-net force-exits the process. The pooler
// must be done — process exited or at least postgres stopped — by the time
// this elapses.
//
// 60s matches the default that pg_ctl stop uses for stopping the server, so
// operators reasoning about postgres shutdown will already have 60s as a
// mental baseline. The actual end-to-end shutdown is typically much faster
// (~15s in the common case): drain (bounded by --connpool-drain-grace-period,
// default 3s) + pool close + pgctld.Stop fast (5s) + final snapshot (5s).
// The 60s ceiling exists for outliers (e.g. PoolCloseTimeout taking longer
// than expected, or escalation to immediate).
const gracefulShutdownAnnouncedDeadline = 60 * time.Second

// Compile-time assertion that the announced deadline meets the floor in
// constants.MinShutdownDeadline. If gracefulShutdownAnnouncedDeadline ever
// drops below that floor, the subtraction goes negative and conversion to
// uint fails to compile.
const _ uint = uint(gracefulShutdownAnnouncedDeadline - constants.MinShutdownDeadline)

// Compile-time assertion that the safety-net telemetry flush budget fits
// inside the announced deadline. If safetyNetTelemetryFlushTimeout ever
// grows larger than gracefulShutdownAnnouncedDeadline, the subtraction in
// the safety-net's AfterFunc offset would underflow and the safety-net
// would fire immediately at GracefulShutdown start.
const _ uint = uint(gracefulShutdownAnnouncedDeadline - safetyNetTelemetryFlushTimeout)

// applyGracefulShutdownDefaults fills in zero-valued timeouts with their
// defaults. A zero value is treated as "unset" rather than "skip this stage."
// Callers that legitimately want to skip a stage (e.g. tests) can pass a
// negative value, which validation will then reject.
func applyGracefulShutdownDefaults(cfg *Config) {
	if cfg.GracefulShutdownFastTimeout == 0 {
		cfg.GracefulShutdownFastTimeout = DefaultGracefulShutdownFastTimeout
	}
	if cfg.GracefulShutdownImmediateTimeout == 0 {
		cfg.GracefulShutdownImmediateTimeout = DefaultGracefulShutdownImmediateTimeout
	}
	if cfg.GracefulShutdownFinalSnapshotTimeout == 0 {
		cfg.GracefulShutdownFinalSnapshotTimeout = DefaultGracefulShutdownFinalSnapshotTimeout
	}
}

// validateGracefulShutdownConfig returns an error when the configured
// graceful-shutdown timeouts are nonsensical: any negative value, or a
// post-announce budget (fast + immediate + final-snapshot) that does not
// fit inside the announced shutdown deadline.
//
// applyGracefulShutdownDefaults must run before this so zero values do not
// cause spurious rejection.
func validateGracefulShutdownConfig(cfg *Config) error {
	if cfg.GracefulShutdownFastTimeout < 0 ||
		cfg.GracefulShutdownImmediateTimeout < 0 ||
		cfg.GracefulShutdownFinalSnapshotTimeout < 0 {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			"graceful-shutdown-* timeouts must be non-negative")
	}

	postAnnounce := cfg.GracefulShutdownFastTimeout +
		cfg.GracefulShutdownImmediateTimeout +
		cfg.GracefulShutdownFinalSnapshotTimeout
	if postAnnounce > gracefulShutdownAnnouncedDeadline {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"graceful shutdown post-announce budget %s exceeds announced deadline %s; lower the --graceful-shutdown-* timeouts",
			postAnnounce, gracefulShutdownAnnouncedDeadline)
	}
	return nil
}

// setLifecycleStatus stores the new lifecycle signal under pm.mu. It does NOT
// broadcast — the caller is responsible for invoking healthStreamer.Shutdown
// immediately after, so the terminal STOPPED state reaches subscribers via
// the same call that closes their channels.
func (pm *MultiPoolerManager) setLifecycleStatus(signal clustermetadatapb.LifecycleSignal) {
	pm.withLock(func() {
		pm.lifecycleSignal = signal
	})
}

// GracefulShutdown drains the pooler, closes the connection pool, stops
// Postgres via pgctld, broadcasts the terminal STOPPED snapshot, and marks
// itself DRAINED in topology. Wired as a servenv OnTermSync hook so it runs
// on SIGTERM within the lameduck window.
//
// The sequence is:
//
//  1. Arm a safety-net timer at gracefulShutdownAnnouncedDeadline that
//     force-exits the process if this method has not returned by the
//     deadline. Without this we could linger indefinitely after SIGTERM and
//     risk staying alive serving writes long after the operator asked us to
//     leave. Operators who want to exit faster than the deadline should
//     send SIGKILL.
//
//  2. Transition serving status to NOT_SERVING via the existing OnStateChange
//     path. This rejects new gateway queries, waits for in-flight queries to
//     drain (bounded by --connpool-drain-grace-period), and force-closes
//     reserved connections on grace-period timeout.
//
//  3. Close the connection pool. Idle backends are released immediately;
//     any straggling borrows are disposed when they return (bounded by
//     PoolCloseTimeout). With drain already done in step 2, this typically
//     returns near-instantly.
//
//  4. Stop Postgres via pgctld.Stop, escalating fast → immediate. Smart is
//     skipped: the pooler has already drained and closed its connections,
//     so smart's "wait for clients" semantics would just be redundant.
//
//  5. Set lifecycleSignal=STOPPED and broadcast as a terminal snapshot that
//     also closes subscriber channels, so the orchestrator can fail over
//     against a leader that has cleanly resigned.
func (pm *MultiPoolerManager) GracefulShutdown(ctx context.Context) {
	deadline := gracefulShutdownAnnouncedDeadline

	// Safety-net force-exit. Stops if GracefulShutdown returns first.
	//
	// Before force-exiting, do a best-effort flush of telemetry pipelines so
	// OTel batchers get a chance to drain the spans/metrics/logs from the
	// shutdown attempt. OTel exporters run on independent goroutines, so the
	// flush typically succeeds even when the main goroutine is hung on
	// something else.
	//
	// However, we will not wait indefinitely: a hung shutdown could otherwise
	// keep this pooler alive serving writes long after the operator asked us
	// to leave, which would risk a split-brain window with a newly elected
	// primary.
	//
	// The safety-net fires safetyNetTelemetryFlushTimeout *before* the
	// announced deadline, so that flush + os.Exit complete by the deadline.
	// Without this offset the process could linger past the deadline by the
	// flush duration.
	safetyNetFireAt := deadline - safetyNetTelemetryFlushTimeout
	safetyNet := time.AfterFunc(safetyNetFireAt, func() {
		pm.logger.ErrorContext(ctx, "graceful shutdown approaching announced deadline; forcing process exit",
			"deadline", deadline,
			"flush_budget", safetyNetTelemetryFlushTimeout)
		if pm.config != nil && pm.config.FlushTelemetry != nil {
			// Background ctx is intentional: the caller's ctx is likely
			// already cancelled (that may be why GracefulShutdown is hung).
			// The flush has its own bounded timeout below.
			//nolint:gocritic // intentional; see comment above
			flushCtx, cancel := context.WithTimeout(context.Background(), safetyNetTelemetryFlushTimeout)
			if err := pm.config.FlushTelemetry(flushCtx); err != nil {
				pm.logger.ErrorContext(ctx, "best-effort telemetry flush before force-exit failed",
					"error", err)
			}
			cancel()
		}
		gracefulShutdownExitFunc(1)
	})
	defer safetyNet.Stop()

	pm.logger.InfoContext(ctx, "graceful shutdown starting", "deadline", deadline)

	// Phase 1: drain. Reject new gateway queries, wait for in-flight to
	// finish (bounded by --connpool-drain-grace-period), force-close
	// reserved on grace-period timeout.
	if pm.servingState != nil {
		if err := pm.servingState.SetState(ctx, pm.multipooler.Type, clustermetadatapb.PoolerServingStatus_NOT_SERVING); err != nil {
			pm.logger.WarnContext(ctx, "transition to NOT_SERVING returned error; proceeding with shutdown",
				"error", err)
		}
	}

	// Phase 2: close the connection pool. Idle backends are released
	// immediately so postgres has no lingering pooler clients; borrows that
	// returned naturally during phase 1 are already gone, and any remaining
	// non-reserved borrows are disposed when they return (bounded by
	// PoolCloseTimeout). Action lock not needed for pool teardown.
	if pm.connPoolMgr != nil {
		pm.connPoolMgr.Close()
	}

	// Phase 3: stop Postgres.
	pm.stopPostgresWithEscalation(ctx)

	// Phase 4: announce completion. STOPPED is the orchestrator's failover
	// trigger; healthStreamer.Shutdown broadcasts it and then closes every
	// subscriber channel so gRPC stream handlers return cleanly via the
	// channel-close path, releasing servenv's grpcServer.GracefulStop wait
	// without forcing the lameduck-period timeout.
	pm.setLifecycleStatus(clustermetadatapb.LifecycleSignal_LIFECYCLE_SIGNAL_STOPPED)
	pm.healthStreamer.Shutdown(pm.config.GracefulShutdownFinalSnapshotTimeout)

	// Phase 5: mark this pooler's topology entry as Type=DRAINED,
	// ServingStatus=NOT_SERVING. Purely cosmetic — failover keys off
	// LeadershipStatus, not Type — but `multigres getpoolers` reads Type
	// and should not keep showing a stopped pooler as its last live type.
	// Best-effort: a topology-write failure is logged but does not block
	// the rest of shutdown.
	pm.markSelfDrainedInTopo(ctx)

	pm.closeLocked("graceful shutdown complete")
}

// markSelfDrainedInTopo writes Type=DRAINED, ServingStatus=NOT_SERVING to
// this pooler's topology entry. Best-effort: errors are logged and ignored.
// Uses a bounded fresh context rather than the caller's ctx, since by this
// point GracefulShutdown is past every announcement step and a topology
// hiccup should not delay process exit by more than the timeout below.
func (pm *MultiPoolerManager) markSelfDrainedInTopo(ctx context.Context) {
	if pm.topoClient == nil || pm.multipooler == nil || pm.multipooler.Id == nil {
		return
	}
	// Background ctx is intentional: the caller's ctx is the OnTermSync
	// hook ctx and may be near expiry by this point. The bounded timeout
	// below caps the topology write so a stuck etcd cannot delay exit.
	//nolint:gocritic // intentional; see comment above
	writeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	markFunc := func(mp *clustermetadatapb.MultiPooler) error {
		mp.Type = clustermetadatapb.PoolerType_DRAINED
		mp.ServingStatus = clustermetadatapb.PoolerServingStatus_NOT_SERVING
		return nil
	}

	if _, err := pm.topoClient.UpdateMultiPoolerFields(writeCtx, pm.multipooler.Id, markFunc); err != nil {
		pm.logger.WarnContext(ctx, "failed to mark self DRAINED in topology",
			"error", err)
	}
}

// stopPostgresWithEscalation stops the Postgres process by invoking
// pgctld.Stop with fast, then immediate modes. Each mode gets its configured
// timeout enforced via context.WithTimeout (in addition to pgctld's own
// pg_ctl -t bound, which only times out pg_ctl's wait, not Postgres itself).
// Returns when fast succeeds, or after immediate has been attempted
// regardless of outcome.
//
// Smart shutdown is deliberately not in the chain: by the time this runs,
// the pooler has already drained in-flight queries (in OnStateChange) and
// closed its connection pool, so smart's "wait for clients to disconnect"
// semantics would just be a redundant timeout. Going straight to fast
// terminates any external clients (e.g. pgbackrest) decisively.
//
// Acquires pm.actionLock for the duration of the escalation: pgctld.Stop is
// gated behind the protectedPgctldClient action-lock check, and holding the
// lock here also serialises against concurrent consensus operations
// (BeginTerm REVOKE, EmergencyDemote, Promote) so they don't race with our
// pgctld calls. If the lock cannot be acquired (parent ctx expired), logs
// the error and returns; the safety-net deadline will eventually force exit.
//
// A configured timeout of zero or negative is treated as "skip this stage."
func (pm *MultiPoolerManager) stopPostgresWithEscalation(ctx context.Context) {
	if pm.pgctldClient == nil {
		pm.logger.ErrorContext(ctx, "pgctld client not available; skipping pgctld.Stop escalation")
		return
	}

	lockCtx, err := pm.actionLock.Acquire(ctx, "GracefulShutdown")
	if err != nil {
		pm.logger.ErrorContext(ctx, "failed to acquire action lock for shutdown; pgctld.Stop will not run",
			"error", err)
		return
	}
	defer pm.actionLock.Release(lockCtx)

	cfg := pm.config
	modes := []struct {
		name    string
		timeout time.Duration
	}{
		{"fast", cfg.GracefulShutdownFastTimeout},
		{"immediate", cfg.GracefulShutdownImmediateTimeout},
	}

	for _, m := range modes {
		if m.timeout <= 0 {
			pm.logger.InfoContext(lockCtx, "pgctld.Stop stage skipped (timeout <= 0)", "mode", m.name)
			continue
		}

		req := &pgctldpb.StopRequest{
			Mode:    m.name,
			Timeout: durationpb.New(m.timeout),
		}

		stepCtx, cancel := context.WithTimeout(lockCtx, m.timeout)
		_, err := pm.pgctldClient.Stop(stepCtx, req)

		cancel()

		if err == nil {
			pm.logger.InfoContext(lockCtx, "pgctld.Stop succeeded", "mode", m.name)
			return
		}

		pm.logger.WarnContext(lockCtx, "pgctld.Stop failed; escalating to next mode",
			"mode", m.name,
			"timeout", m.timeout,
			"error", err)
	}
	pm.logger.ErrorContext(lockCtx, "all pgctld.Stop modes exhausted without success")
}
