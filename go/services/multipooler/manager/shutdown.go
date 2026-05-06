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
	"time"

	"github.com/multigres/multigres/go/common/constants"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// performGracefulShutdown runs the graceful shutdown sequence exactly once (sync.Once).
// It may be called concurrently from OnTermSync and from InitiateGracefulShutdown; the
// second caller returns immediately without re-running the shutdown logic.
//
// PRIMARY path: signal STOPPING via the health stream so multiorch can run the
// LeaderIsStopping recovery (EmergencyDemote + AppointLeader). Wait until either
// EmergencyDemote returns locally (demotedC closed) or the hard-stop timer fires,
// then stop postgres directly.
//
// REPLICA path: stop postgres directly — no failover coordination needed.
func (pm *MultiPoolerManager) performGracefulShutdown(ctx context.Context) {
	pm.shutdownOnce.Do(func() {
		poolerType := pm.getPoolerType()
		pm.logger.InfoContext(ctx, "graceful shutdown: beginning", "pooler_type", poolerType.String())
		if poolerType == clustermetadatapb.PoolerType_PRIMARY {
			pm.awaitPrimaryDemotion(ctx)
		}
		pm.stopPostgresDirectly(ctx)
	})
}

// awaitPrimaryDemotion is called on graceful shutdown when this pooler is a PRIMARY.
// It signals PoolerType_STOPPING via the health stream so multiorch can orchestrate
// the controlled failover (EmergencyDemote + AppointLeader), then waits for either
// the demotion to complete locally or the hard-stop timer to fire — whichever comes
// first. The caller stops postgres afterwards in either case.
//
// In the happy path (multiorch is healthy, drain is fast), demotedC is closed at
// the end of emergencyDemoteLocked and we exit within drain-time + small margin.
// If multiorch is slow/dead/partitioned, the timer bounds the wait; postgres is
// stopped, the health stream closes when this process exits, and multiorch's
// LeaderIsDeadAnalyzer takes over to elect a new primary via the dead-leader path.
func (pm *MultiPoolerManager) awaitPrimaryDemotion(ctx context.Context) {
	// Signal STOPPING so the next health-stream snapshot delivered to multiorch
	// triggers LeaderIsStoppingAnalyzer → ShutdownPrimaryAction → EmergencyDemote.
	if err := pm.servingState.SetState(ctx,
		clustermetadatapb.PoolerType_STOPPING,
		clustermetadatapb.PoolerServingStatus_NOT_SERVING,
	); err != nil {
		pm.logger.WarnContext(ctx, "graceful shutdown: failed to signal STOPPING; stopping postgres directly",
			"error", err)
		return
	}
	pm.logger.InfoContext(ctx, "graceful shutdown: signalled STOPPING; waiting for EmergencyDemote",
		"timeout", constants.PrimaryShutdownSignalTimeout)

	select {
	case <-pm.demotedC:
		pm.logger.InfoContext(ctx, "graceful shutdown: EmergencyDemote completed locally")
	case <-time.After(constants.PrimaryShutdownSignalTimeout):
		pm.logger.WarnContext(ctx, "graceful shutdown: timed out waiting for EmergencyDemote",
			"timeout", constants.PrimaryShutdownSignalTimeout)
	case <-ctx.Done():
		pm.logger.WarnContext(ctx, "graceful shutdown: context cancelled while waiting for EmergencyDemote")
	}
}

// stopPostgresDirectly asks pgctld to stop postgres with a "fast" shutdown.
// Errors are logged but not returned — callers proceed regardless.
func (pm *MultiPoolerManager) stopPostgresDirectly(ctx context.Context) {
	if pm.pgctldClient == nil {
		pm.logger.WarnContext(ctx, "graceful shutdown: pgctld client not available; cannot stop postgres")
		return
	}
	if _, err := pm.pgctldClient.Stop(ctx, &pgctldpb.StopRequest{Mode: "fast"}); err != nil {
		pm.logger.WarnContext(ctx, "graceful shutdown: failed to stop postgres", "error", err)
		return
	}
	pm.logger.InfoContext(ctx, "graceful shutdown: postgres stopped")
}
