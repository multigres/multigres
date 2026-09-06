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

	"google.golang.org/protobuf/types/known/timestamppb"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// markPoolerQuarantinedLocked latches this pooler into LIFECYCLE_QUARANTINED.
//
// The pooler process is up but its postgres is unrecoverably failing to start
// (the classic case is a diverged standby FATAL-looping), so it cannot
// self-heal and must be replaced. Unlike the transient "postgres momentarily
// down" state, quarantine is a durable verdict:
//
//   - It is published on the pooler's own topology record (lifecycle_status),
//     the signal the operator watches to drive replacement (delete pod + wipe
//     PVC + restore-from-backup) and the orchestrator observes to drop the node.
//   - It flips cohort eligibility to INELIGIBLE so the coordinator stops trying
//     to include the node in consensus.
//   - It disables further local restart attempts so the node stops
//     FATAL-looping; a replacement pod comes up clean and re-bootstraps.
//
// The verdict does not auto-clear within a process lifetime — recovery happens
// by replacing the pod, which starts a fresh manager. The caller must hold the
// action lock (record.Mutate asserts this); the monitor calls it from within
// monitorPostgresIteration, which already holds the lock.
func (pm *MultipoolerManager) markPoolerQuarantinedLocked(ctx context.Context, reason string) {
	// We run both these steps idempotently so that if the first succeeds but
	// the second fails, we can retry on the next monitor tick without
	// re-logging the same warning or re-writing the same lifecycle status. It
	// is critical that both steps succeed eventually, so the operator sees a
	// quarantined server that is not a primary.
	var stateChanged bool

	// Idempotent: skip quarantining if already quarantined. This avoids
	// repeatedly writing the same lifecycle status to the record and logging
	// the same warning on every monitor tick. The operator/orchestrator only
	// needs to see the first warning and the first lifecycle-status update;
	// subsequent ticks are just noise.
	if pm.record.Snapshot().GetLifecycleStatus().GetStatus() != clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_QUARANTINED {
		setLifecycleStatus := func(s *MutablePoolerRecordState) {
			s.LifecycleStatus = &clustermetadatapb.PoolerLifecycle{
				Status:  clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_QUARANTINED,
				Reason:  reason,
				Updated: timestamppb.Now(),
			}
		}
		if err := pm.record.Mutate(ctx, setLifecycleStatus); err != nil {
			pm.logger.ErrorContext(ctx, "failed to mark pooler quarantined; will retry next tick", "error", err)
			return
		}
		pm.logger.WarnContext(ctx, "pooler quarantined: postgres is unrecoverable, node needs replacement", "reason", reason)
		stateChanged = true
	}

	// Idempotent: Stop participating in consensus so the coordinator drops us
	// from the cohort rather than repeatedly trying (and failing) to recruit
	// us. If we failed to set the cohort eligibility on a previous tick, we
	// will retry here. It is critical to avoid the coordinator repeatedly
	// trying to recruit a node that is known to be unrecoverable and also
	// ensure that the orchestrator elects a new primary.
	//
	// If the node is quarantined but still eligible, the coordinator will keep
	// trying to recruit it and the orchestrator will not elect a new primary.
	if pm.consensusMgr.CohortEligibility() != clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE {
		if err := pm.consensusMgr.SetCohortEligibility(ctx, clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE); err != nil {
			pm.logger.ErrorContext(ctx, "failed to mark cohort ineligible on quarantine", "error", err)
		} else {
			// Only count this as a change on success, so a failed set does not
			// trigger a broadcast for state that did not actually change; the
			// next tick retries (see trackRecoveryOutcome).
			stateChanged = true
		}
	}

	// Idempotent: Stop hammering doomed start attempts. The node is being
	// replaced; a fresh pod restarts with this flag clear.
	pm.postgresRestartsDisabled.Store(true)

	// Publish the transition immediately rather than waiting for the next
	// heartbeat so the orchestrator/operator see the durable verdict promptly.
	// We only broadcast if the state changed, to avoid spamming the network
	// with unnecessary broadcasts.
	if stateChanged {
		pm.broadcastHealth()
	}
}

// now returns the current time via the injectable clock, defaulting to
// time.Now when nowFn is unset. Used by the unrecoverable classifier so its
// timeout gate is deterministic in tests.
func (pm *MultipoolerManager) now() time.Time {
	if pm.nowFn != nil {
		return pm.nowFn()
	}
	return time.Now()
}
