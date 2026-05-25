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
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// pgctld.Stop is escalated through these modes in order. Each mode gets its
// own bounded timeout; the total fits inside servenv's --onterm-timeout (20s
// default). Operators who need a longer shutdown can raise --onterm-timeout;
// if both modes fail we log and return and servenv's onterm-timeout
// enforcement eventually forces the process to move on.
//
// "smart" mode is intentionally absent. Smart waits for every postgres
// client connection to disconnect, and our own connection pool keeps
// backends open until process exit — so smart would always time out
// waiting for them. fast sends SIGTERM to postgres which terminates those
// backends directly, achieving the same end state with no wasted wait.
var pgctldStopModes = []struct {
	name    string
	timeout time.Duration
}{
	{"fast", 10 * time.Second},
	{"immediate", 5 * time.Second},
}

// GracefulShutdown publishes REQUESTING_DEMOTION on the health stream (for a
// current leader) and then stops Postgres. Registered as a servenv OnTermSync
// hook so it runs on SIGTERM bounded by --onterm-timeout.
//
// The announcement is sequenced before pgctld.Stop because reading the
// primary term requires querying postgres for the current rule position;
// doing it post-stop would silently no-op and force the coordinator to wait
// for stream EOF + LeaderIsDead grace period instead of firing
// LeaderResignedAnalyzer immediately.
//
// The topology Type=DRAINED transition happens after this returns via the
// existing OnClose -> mp.Shutdown -> tr.Unregister chain registered in
// services/multipooler/init.go.
//
// The action lock is held for the whole sequence: pgctld.Stop is gated behind
// the protectedPgctldClient action-lock check, and announcing the resignation
// involves reading consensus state and writing resignedLeaderAtTerm under the
// same lock — holding it across both serialises against any concurrent
// consensus operation (Recruit, Propose, etc.).
func (pm *MultiPoolerManager) GracefulShutdown(ctx context.Context) {
	pm.logger.InfoContext(ctx, "graceful shutdown starting")

	lockCtx, err := pm.actionLock.Acquire(ctx, "GracefulShutdown")
	if err != nil {
		pm.logger.ErrorContext(ctx, "failed to acquire action lock for graceful shutdown",
			"error", err)
		return
	}
	defer pm.actionLock.Release(lockCtx)

	// Announce STOPPING in topology before any blocking work. Operators see
	// the announcement immediately; the actual teardown happens in the rest
	// of this function and in the StopTopoRegistration call that follows in
	// OnClose. STOPPING is observability-only — the orchestrator does not
	// react to this value behaviourally; the authoritative cleanup signal is
	// LIFECYCLE_SHUTDOWN written by StopTopoRegistration. The Mutate
	// schedules an async publish; a transient publish failure is recovered
	// by the publisher's 30 s retry tick.
	if err := pm.record.Mutate(lockCtx, func(s *MutablePoolerRecordState) {
		s.LifecycleStatus = &clustermetadatapb.PoolerLifecycle{
			Status:  clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_STOPPING,
			Reason:  "shutting down",
			Updated: timestamppb.Now(),
		}
	}); err != nil {
		pm.logger.WarnContext(lockCtx, "failed to announce STOPPING lifecycle",
			"error", err)
	}

	// Transition to NOT_SERVING so the gateway sees a clean rejection for new
	// queries while in-flight transactions are allowed to complete (bounded by
	// --connpool-drain-grace-period). SetState fans out OnStateChange to the
	// in-process components (query service, connection pool, heartbeat,
	// health streamer) and routes the topology update through record.Mutate
	// so the publisher reflects NOT_SERVING during the drain window —
	// without it, the entry would still read SERVING in topology until the
	// OnClose StopTopoRegistration runs at the very end of shutdown.
	//
	// Best-effort: a failure here is logged but doesn't block the rest of
	// shutdown.
	if pm.servingState != nil {
		if err := pm.servingState.SetState(lockCtx, pm.record.Type(), clustermetadatapb.PoolerServingStatus_NOT_SERVING); err != nil {
			pm.logger.WarnContext(lockCtx, "transition to NOT_SERVING returned error; proceeding with shutdown",
				"error", err)
		}
	}

	// If we are the leader, announce REQUESTING_DEMOTION before stopping
	// postgres. primaryTermLocked reads the current rule position from
	// postgres, so the read must happen while postgres is still alive;
	// running it post-stop would fail and the coordinator would have to wait
	// for stream EOF + LeaderIsDead grace period instead. No-op for
	// non-leaders and partially-initialized managers (consensus not wired).
	// Mirrors the primary-demote pattern in Recruit (rpc_consensus.go).
	if pm.consensusState != nil && pm.rules != nil {
		primaryTerm, err := pm.primaryTermLocked(lockCtx)
		switch {
		case err != nil:
			pm.logger.WarnContext(lockCtx, "could not read primary term for resignation announcement; coordinator will fail over via stream EOF",
				"error", err)
		case primaryTerm != 0:
			if err := pm.setResignedLeaderAtTerm(lockCtx, primaryTerm); err != nil {
				pm.logger.WarnContext(lockCtx, "failed to record resigned primary term",
					"error", err)
			}
		}
	}

	pm.stopPostgresLocked(lockCtx)

	// Signal long-lived subscribers (health-stream gRPC handlers) that the
	// manager is shutting down. Their cleanup goroutines close subscriber
	// channels, which makes the gRPC handlers return Unavailable and unblocks
	// servenv's parallel grpcServer.GracefulStop hook. Without this, the
	// handlers sit in `select { <-pollTicker.C }` forever and GracefulStop
	// only completes when servenv's --onterm-timeout fires.
	//
	// Nil guard: some unit tests construct MultiPoolerManager via struct
	// literal without going through NewMultiPoolerManager.
	if pm.shutdownCancel != nil {
		pm.shutdownCancel()
	}

	pm.logger.InfoContext(lockCtx, "graceful shutdown sequence complete")
}

// stopPostgresLocked stops postgres via pgctld using fast → immediate.
// Caller must hold the action lock.
func (pm *MultiPoolerManager) stopPostgresLocked(ctx context.Context) {
	if pm.pgctldClient == nil {
		pm.logger.ErrorContext(ctx, "pgctld client not available; skipping pgctld.Stop")
		return
	}

	for _, m := range pgctldStopModes {
		req := &pgctldpb.StopRequest{
			Mode:    m.name,
			Timeout: durationpb.New(m.timeout),
		}
		stepCtx, cancel := context.WithTimeout(ctx, m.timeout)
		_, err := pm.pgctldClient.Stop(stepCtx, req)
		cancel()
		if err == nil {
			pm.logger.InfoContext(ctx, "pgctld.Stop succeeded", "mode", m.name)
			return
		}
		pm.logger.WarnContext(ctx, "pgctld.Stop failed; escalating",
			"mode", m.name, "timeout", m.timeout, "error", err)
	}
	pm.logger.ErrorContext(ctx, "all pgctld.Stop modes exhausted without success")
}
