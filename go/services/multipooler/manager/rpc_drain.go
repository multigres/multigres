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

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/eventlog"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// tombstoneWriteTimeout bounds the topology update attempted on shutdown after
// a permanent drain. Shutdown must not hang; etcd errors are logged and
// swallowed.
const tombstoneWriteTimeout = 2 * time.Second

// writeDrainTombstoneIfNeeded writes a best-effort tombstone (removed_at +
// remove_reason) to this pooler's MultiPooler topology record when the
// manager is permanently draining. Called from Shutdown. Errors are logged,
// not returned — the signal published over streaming health is the
// authoritative channel; the tombstone is an etcd-backed hint for consumers
// that reconnect after this pooler is gone.
func (pm *MultiPoolerManager) writeDrainTombstoneIfNeeded() {
	pm.mu.Lock()
	signal := pm.drainSignal
	permanent := pm.drainPermanent
	reason := pm.drainReason
	pm.mu.Unlock()

	if signal != clustermetadatapb.ServingSignal_SERVING_SIGNAL_DRAINING || !permanent {
		return
	}

	if pm.topoClient == nil || pm.serviceID == nil {
		pm.logger.Warn("Skipping drain tombstone: topo client or service ID unavailable")
		return
	}

	// Shutdown has no incoming context to inherit; start a fresh bounded one so
	// the write cannot hang past tombstoneWriteTimeout even if etcd is slow.
	//nolint:gocritic // legitimate background entry point during shutdown
	ctx, cancel := context.WithTimeout(context.Background(), tombstoneWriteTimeout)
	defer cancel()

	now := timestamppb.Now()
	_, err := pm.topoClient.UpdateMultiPoolerFields(ctx, pm.serviceID, func(mp *clustermetadatapb.MultiPooler) error {
		mp.RemovedAt = now
		mp.RemoveReason = reason
		return nil
	})
	if err != nil {
		pm.logger.Warn("Failed to write drain tombstone on shutdown; continuing", "error", err)
		return
	}
	pm.logger.Info("Wrote drain tombstone on shutdown", "reason", reason.String())
}

// Drain initiates voluntary drain of this pooler. See
// multipoolermanagerservice.proto for semantics. The RPC is idempotent:
// calling on an already-draining node returns the current state.
//
// This implementation covers the replica (non-primary) path. The primary
// path — restart as standby, signal resignation — lands in a later phase.
// Likewise, forced termination of lingering client connections after
// drain_timeout is out of scope here; the plan defers server-side
// drain-aware query-serving behaviour.
func (pm *MultiPoolerManager) Drain(ctx context.Context, req *multipoolermanagerdatapb.DrainRequest) (_ *multipoolermanagerdatapb.DrainResponse, retErr error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}

	ctx, err := pm.actionLock.Acquire(ctx, "Drain")
	if err != nil {
		return nil, err
	}
	defer pm.actionLock.Release(ctx)

	removeFromTopology := req.GetRemoveFromTopology()
	reason := req.GetRemoveReason()
	nodeName := pm.serviceID.GetName()
	reasonStr := drainReasonString(removeFromTopology, reason)

	eventlog.Emit(ctx, pm.logger, eventlog.Started, eventlog.NodeDrain{NodeName: nodeName, Reason: reasonStr})
	defer func() {
		if retErr == nil {
			eventlog.Emit(ctx, pm.logger, eventlog.Success, eventlog.NodeDrain{NodeName: nodeName, Reason: reasonStr})
		} else {
			eventlog.Emit(ctx, pm.logger, eventlog.Failed, eventlog.NodeDrain{NodeName: nodeName, Reason: reasonStr}, "error", retErr)
		}
	}()

	wasAlreadyDraining := pm.enterDraining(removeFromTopology, reason)

	// Primary path: reuse the EmergencyDemote core to transition this node to
	// standby and signal resignation so the coordinator can trigger an
	// immediate election.
	primaryPathTaken := false
	var connectionsTerminated int32
	if pm.getPoolerType() == clustermetadatapb.PoolerType_PRIMARY {
		drainTimeout := req.GetDrainTimeout().AsDuration()
		if drainTimeout <= 0 {
			drainTimeout = 5 * time.Second
		}
		result, err := pm.demoteToStandbyLocked(ctx, drainTimeout)
		if err != nil {
			return nil, err
		}
		primaryPathTaken = true
		connectionsTerminated = result.connectionsTerminated
	}

	_, effectiveRemoveFromTopology, _, _ := pm.drainSnapshot()

	return &multipoolermanagerdatapb.DrainResponse{
		WasAlreadyDraining:    wasAlreadyDraining,
		PrimaryPathTaken:      primaryPathTaken,
		ConnectionsTerminated: connectionsTerminated,
		RemoveFromTopology:    effectiveRemoveFromTopology,
	}, nil
}

// drainReasonString maps drain intent to the string value stored on the
// NodeDrain event. Used by the event log consumers to distinguish
// voluntary drains from the existing involuntary drain flow ("rewind_not_feasible").
func drainReasonString(removeFromTopology bool, reason clustermetadatapb.RemoveReason) string {
	if !removeFromTopology {
		return "voluntary_temporary"
	}
	switch reason {
	case clustermetadatapb.RemoveReason_REMOVE_REASON_SCALE_DOWN:
		return "voluntary_permanent_scale_down"
	case clustermetadatapb.RemoveReason_REMOVE_REASON_DECOMMISSION:
		return "voluntary_permanent_decommission"
	case clustermetadatapb.RemoveReason_REMOVE_REASON_REPLACED:
		return "voluntary_permanent_replaced"
	case clustermetadatapb.RemoveReason_REMOVE_REASON_OPERATOR_REQUESTED:
		return "voluntary_permanent_operator_requested"
	default:
		return "voluntary_permanent_unknown"
	}
}

// enterDraining transitions the manager into DRAINING. Idempotent: returns
// wasAlreadyDraining=true when called on a manager already draining.
// Upgrades permanent from false to true (never the reverse). When already
// permanent, a non-UNKNOWN reason replaces the stored one (last-writer-wins).
//
// On the first transition to DRAINING, the healthStreamer is notified so the
// signal flows into the next published HealthState immediately.
func (pm *MultiPoolerManager) enterDraining(permanent bool, reason clustermetadatapb.RemoveReason) (wasAlreadyDraining bool) {
	pm.mu.Lock()
	if pm.drainSignal == clustermetadatapb.ServingSignal_SERVING_SIGNAL_DRAINING {
		if permanent && !pm.drainPermanent {
			pm.drainPermanent = true
			pm.drainReason = reason
		} else if pm.drainPermanent && reason != clustermetadatapb.RemoveReason_REMOVE_REASON_UNKNOWN {
			pm.drainReason = reason
		}
		pm.mu.Unlock()
		return true
	}
	pm.drainSignal = clustermetadatapb.ServingSignal_SERVING_SIGNAL_DRAINING
	pm.drainPermanent = permanent
	if permanent {
		pm.drainReason = reason
	}
	pm.drainStartedAt = time.Now()
	pm.mu.Unlock()

	if pm.healthStreamer != nil {
		pm.healthStreamer.SetServingSignal(clustermetadatapb.ServingSignal_SERVING_SIGNAL_DRAINING)
	}
	return false
}

// currentServingSignal returns the signal to publish in HealthState. Treats
// the zero/UNKNOWN value as ACTIVE so consumers see a stable signal from
// process start.
func (pm *MultiPoolerManager) currentServingSignal() clustermetadatapb.ServingSignal {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if pm.drainSignal == clustermetadatapb.ServingSignal_SERVING_SIGNAL_DRAINING {
		return clustermetadatapb.ServingSignal_SERVING_SIGNAL_DRAINING
	}
	return clustermetadatapb.ServingSignal_SERVING_SIGNAL_ACTIVE
}

// drainSnapshot returns the current drain-state values for consumers that
// need the full picture (tombstone writer, tests).
func (pm *MultiPoolerManager) drainSnapshot() (signal clustermetadatapb.ServingSignal, permanent bool, reason clustermetadatapb.RemoveReason, startedAt time.Time) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return pm.drainSignal, pm.drainPermanent, pm.drainReason, pm.drainStartedAt
}
