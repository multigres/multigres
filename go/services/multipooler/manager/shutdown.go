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

package manager

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/multigres/multigres/go/common/constants"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// performGracefulShutdown runs the graceful shutdown sequence exactly once (sync.Once).
// It may be called concurrently from OnTermSync and from InitiateGracefulShutdown; the
// second caller returns immediately without re-running the shutdown logic.
//
// PRIMARY path: signal STOPPING so multiorch can orchestrate EmergencyDemote, then wait
// for postgres to stop (or fall back to stopping it directly on timeout).
// REPLICA path: stop postgres directly — no failover coordination needed.
func (pm *MultiPoolerManager) performGracefulShutdown(ctx context.Context) {
	pm.shutdownOnce.Do(func() {
		poolerType := pm.getPoolerType()
		pm.logger.InfoContext(ctx, "graceful shutdown: beginning", "pooler_type", poolerType.String())
		if poolerType == clustermetadatapb.PoolerType_PRIMARY {
			if msg, attr := pm.awaitEmergencyDemote(ctx); msg != "" {
				pm.logger.WarnContext(ctx, fmt.Sprintf("graceful shutdown: %s; stopping postgres directly", msg), attr)
			}
		}
		pm.stopPostgresDirectly(ctx)
	})
}

// awaitEmergencyDemote is called on SIGTERM when this pooler is a PRIMARY.
// It signals PoolerType_STOPPING via the health stream so multiorch can orchestrate
// a controlled failover (EmergencyDemote + AppointLeader), then waits for postgres
// to stop. The caller is responsible for stopping postgres afterwards (directly on
// timeout or when the signal fails).
func (pm *MultiPoolerManager) awaitEmergencyDemote(ctx context.Context) (string, slog.Attr) {
	// Signal STOPPING so the health stream snapshot delivered to multiorch
	// triggers PrimaryIsStoppingAnalyzer → ShutdownPrimaryAction → EmergencyDemote.
	if err := pm.servingState.SetState(ctx,
		clustermetadatapb.PoolerType_STOPPING,
		clustermetadatapb.PoolerServingStatus_NOT_SERVING,
	); err != nil {
		return "failed to signal STOPPING", slog.String("error", err.Error())
	}
	pm.logger.InfoContext(ctx, "OnTermSync: signalled STOPPING; waiting for multiorch to call EmergencyDemote",
		"timeout", constants.PrimaryShutdownSignalTimeout)

	// Poll until postgres stops (EmergencyDemote will do this) or timeout.
	deadline := time.Now().Add(constants.PrimaryShutdownSignalTimeout)
	for time.Now().Before(deadline) {
		if !pm.isPostgresRunning(ctx) {
			// EmergencyDemote already stopped postgres — caller's stopPostgresDirectly
			// will be a harmless no-op.
			return "", slog.Attr{}
		}
		select {
		case <-ctx.Done():
			return "context cancelled", slog.Attr{}
		case <-time.After(constants.PrimaryShutdownPollInterval):
		}
	}

	return "timed out waiting for EmergencyDemote",
		slog.Duration("timeout", constants.PrimaryShutdownSignalTimeout)
}

// stopPostgresDirectly asks pgctld to stop postgres with a "fast" shutdown.
// Errors are logged but not returned — callers proceed regardless.
func (pm *MultiPoolerManager) stopPostgresDirectly(ctx context.Context) {
	if pm.pgctldClient == nil {
		pm.logger.WarnContext(ctx, "OnTermSync: pgctld client not available; cannot stop postgres")
		return
	}
	if _, err := pm.pgctldClient.Stop(ctx, &pgctldpb.StopRequest{Mode: "fast"}); err != nil {
		pm.logger.WarnContext(ctx, "OnTermSync: failed to stop postgres", "error", err)
		return
	}
	pm.logger.InfoContext(ctx, "OnTermSync: postgres stopped")
}
