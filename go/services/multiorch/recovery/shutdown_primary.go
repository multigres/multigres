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

package recovery

import (
	"context"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/multigres/multigres/go/common/eventlog"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/topoclient"
	commontypes "github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multiorchpb "github.com/multigres/multigres/go/pb/multiorch"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// shutdownPrimaryCallback wraps ShutdownPrimary with the signature expected by
// analysis.ShutdownPrimaryFunc so it can be injected into the action factory without
// exposing the full proto request type to the analysis sub-package.
func (re *Engine) shutdownPrimaryCallback(
	ctx context.Context,
	primaryID *clustermetadatapb.ID,
	database, tableGroup, shard string,
) error {
	_, err := re.ShutdownPrimary(ctx, &multiorchpb.ShutdownPrimaryRequest{
		Database:     database,
		TableGroup:   tableGroup,
		Shard:        shard,
		PrimaryId:    primaryID,
		DrainTimeout: durationpb.New(10 * time.Second),
		Reason:       "SIGTERM",
	})
	return err
}

// isPrimaryCandidate returns true if the pooler type is currently reported to
// be in PRIMARY or REPLICA state. Both types are valid candidates for promotion
// to PRIMARY during the ShutdownPrimary flow, which excludes the old primary
// from consideration (since the pooler type is STOPPING).
//
// We deliberately exclude UNKNOWN (or any other unknown types that we don't know
// are valid) to be conservative in the face of missing or stale health data. If
// a pooler has no health status, we don't want to risk electing it as the new
// primary only to discover it's unhealthy when we try to promote it.
func (re *Engine) isPrimaryCandidate(pooler *multiorchdatapb.PoolerHealthState) bool {
	if pooler.MultiPooler == nil {
		return false
	}

	switch pooler.GetStatus().GetPoolerType() {
	case clustermetadatapb.PoolerType_PRIMARY, clustermetadatapb.PoolerType_REPLICA:
		return true
	case clustermetadatapb.PoolerType_STOPPING, clustermetadatapb.PoolerType_DRAINED:
		return false
	default:
		re.logger.Warn("isPrimaryCandidate: unknown pooler type, skipping",
			"pooler_id", topoclient.MultiPoolerIDString(pooler.MultiPooler.Id),
			"pooler_type", pooler.GetStatus().GetPoolerType())
		return false
	}
}

// ShutdownPrimary orchestrates a graceful primary switchover for the given shard.
//
// Triggered when a primary multipooler signals PoolerType_STOPPING on the health
// stream (e.g. on SIGTERM). The pooler has already stopped accepting writes by
// the time this runs; EmergencyDemote drains remaining connections and leaves
// PostgreSQL running in NOT_SERVING state — the multipooler stops PostgreSQL
// itself after this RPC returns. The old primary is then marked DRAINED in
// topology so getpoolers never shows two PRIMARY nodes simultaneously and the
// node is excluded from future elections.
//
// See the ShutdownPrimary RPC in multiorchservice.proto for the full sequence.
func (re *Engine) ShutdownPrimary(ctx context.Context, req *multiorchpb.ShutdownPrimaryRequest) (_ *multiorchpb.ShutdownPrimaryResponse, retErr error) {
	shardKey := &clustermetadatapb.ShardKey{
		Database:   req.Database,
		TableGroup: req.TableGroup,
		Shard:      req.Shard,
	}
	primaryIDStr := topoclient.MultiPoolerIDString(req.PrimaryId)
	start := time.Now()

	re.logger.InfoContext(ctx, "ShutdownPrimary: starting graceful switchover",
		"shard_key", commontypes.FormatShardKey(shardKey),
		"primary", primaryIDStr,
		"drain_timeout", req.DrainTimeout.AsDuration(),
		"reason", req.Reason)

	// Step 1: Validate that the given pooler is currently PRIMARY or STOPPING.
	primary, err := re.shutdownValidatePrimary(shardKey, req.PrimaryId)
	if err != nil {
		return nil, err
	}

	// Telemetry: record duration and emit demotion events.
	eventlog.Emit(ctx, re.logger, eventlog.Started, eventlog.PrimaryDemotion{
		NodeName: primaryIDStr,
		Reason:   "shutdown",
	})
	defer func() {
		duration := time.Since(start)
		status := RecoveryActionStatusSuccess
		if retErr != nil {
			status = RecoveryActionStatusFailure
			eventlog.Emit(ctx, re.logger, eventlog.Failed, eventlog.PrimaryDemotion{
				NodeName: primaryIDStr,
				Reason:   "shutdown",
			}, "error", retErr)
		} else {
			eventlog.Emit(ctx, re.logger, eventlog.Success, eventlog.PrimaryDemotion{
				NodeName: primaryIDStr,
				Reason:   "shutdown",
			})
		}
		re.metrics.recoveryActionDuration.Record(ctx,
			float64(duration.Milliseconds()),
			"ShutdownPrimary", "ShutdownPrimary",
			status,
			shardKey.Database, shardKey.Shard)
	}()

	// Step 2: Drain the primary via EmergencyDemote. With sync replication,
	// drain blocks until in-flight commits are ACK'd by sync standbys, so the
	// committed WAL is durable on sync standbys when this returns.
	//
	// EmergencyDemote is best-effort here: if it fails (e.g. the dying pooler's
	// hard-stop timer fired and the process exited mid-RPC) we log and proceed.
	// The cohort filter excludes STOPPING/DRAINED so the dying primary cannot be
	// re-elected, and AppointLeader operates against the standbys regardless of
	// whether the drain succeeded.
	re.logger.InfoContext(ctx, "ShutdownPrimary: draining primary via EmergencyDemote",
		"primary", primaryIDStr)
	demoteResp, err := re.rpcClient.EmergencyDemote(ctx, primary.MultiPooler,
		&multipoolermanagerdatapb.EmergencyDemoteRequest{
			// force=true bypasses term validation: we are initiating this demotion
			// deliberately, not responding to a detected failure.
			Force:        true,
			DrainTimeout: req.DrainTimeout,
			// restart_server_as_standby=false: the multipooler is exiting on SIGTERM
			// and will stop postgres directly. Restarting as standby would just be
			// undone moments later.
			RestartServerAsStandby: false,
		})
	if err != nil {
		re.logger.WarnContext(ctx, "ShutdownPrimary: EmergencyDemote failed; proceeding with election",
			"primary", primaryIDStr, "error", err)
	} else {
		re.logger.InfoContext(ctx, "ShutdownPrimary: primary drained",
			"primary", primaryIDStr,
			"final_lsn", demoteResp.LsnPosition,
			"connections_terminated", demoteResp.ConnectionsTerminated)
	}

	// Step 2b: Mark the old primary as DRAINED in topology so that getpoolers
	// does not show two PRIMARYs and the node is excluded from future elections.
	// The pooler sets STOPPING only on its in-memory health stream; it exits
	// before its monitor loop can rewrite the etcd record.
	if _, err := re.ts.UpdateMultiPoolerFields(ctx, req.PrimaryId, func(mp *clustermetadatapb.MultiPooler) error {
		mp.Type = clustermetadatapb.PoolerType_DRAINED
		return nil
	}); err != nil {
		re.logger.WarnContext(ctx, "ShutdownPrimary: failed to mark old primary as DRAINED in topology (non-fatal)",
			"primary", primaryIDStr, "error", err)
	}

	// Step 3: Build the election cohort, excluding STOPPING and DRAINED poolers.
	cohort := re.poolerStore.FindPoolersInShard(shardKey)
	filteredCohort := make([]*multiorchdatapb.PoolerHealthState, 0, len(cohort))
	for _, p := range cohort {
		if re.isPrimaryCandidate(p) {
			filteredCohort = append(filteredCohort, p)
		}
	}
	// Step 4: Elect a new primary from the cohort.
	re.logger.InfoContext(ctx, "ShutdownPrimary: electing new primary",
		"shard_key", commontypes.FormatShardKey(shardKey),
		"cohort_size", len(filteredCohort))
	if err := re.coordinator.AppointLeader(ctx, shardKey.Shard, filteredCohort, shardKey.Database, "ShutdownPrimary"); err != nil {
		return nil, mterrors.Wrap(err, "AppointLeader failed")
	}

	// Step 5: Find the newly elected primary and return its ID.
	newPrimary, err := re.shutdownFindNewPrimary(ctx, shardKey, primaryIDStr)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to find new primary after election")
	}

	newPrimaryIDStr := topoclient.MultiPoolerIDString(newPrimary.MultiPooler.Id)
	re.logger.InfoContext(ctx, "ShutdownPrimary: switchover complete",
		"old_primary", primaryIDStr,
		"new_primary", newPrimaryIDStr)

	return &multiorchpb.ShutdownPrimaryResponse{
		NewPrimaryId: newPrimary.MultiPooler.Id,
	}, nil
}

// shutdownValidatePrimary looks up the pooler in the store and validates that it
// is currently acting as PRIMARY or STOPPING in the given shard.
func (re *Engine) shutdownValidatePrimary(shardKey *clustermetadatapb.ShardKey, id *clustermetadatapb.ID) (*multiorchdatapb.PoolerHealthState, error) {
	if id == nil {
		return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "primary_id is required")
	}
	idStr := topoclient.MultiPoolerIDString(id)
	pooler, ok := re.poolerStore.Get(idStr)
	if !ok {
		return nil, mterrors.Errorf(mtrpcpb.Code_NOT_FOUND, "pooler %s not found in store", idStr)
	}
	if pooler.MultiPooler == nil {
		return nil, mterrors.Errorf(mtrpcpb.Code_NOT_FOUND, "pooler %s has no metadata", idStr)
	}
	if pooler.MultiPooler.Database != shardKey.Database ||
		pooler.MultiPooler.TableGroup != shardKey.TableGroup ||
		pooler.MultiPooler.Shard != shardKey.Shard {
		return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"pooler %s does not belong to shard %s", idStr, commontypes.FormatShardKey(shardKey))
	}
	// Observed type from streaming; fall back to topology type if not yet seen.
	poolerType := pooler.GetStatus().GetPoolerType()
	if poolerType == clustermetadatapb.PoolerType_UNKNOWN {
		poolerType = pooler.MultiPooler.Type
	}
	// Accept PRIMARY or STOPPING — the pooler may have already set STOPPING before
	// we received the request.
	if poolerType != clustermetadatapb.PoolerType_PRIMARY && poolerType != clustermetadatapb.PoolerType_STOPPING {
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"pooler %s is not PRIMARY or STOPPING (current type: %s)", idStr, poolerType)
	}
	return pooler, nil
}

// shutdownFindNewPrimary polls shard poolers for fresh state and returns the
// pooler in the shard that is now PRIMARY, excluding the old primary.
func (re *Engine) shutdownFindNewPrimary(ctx context.Context, shardKey *clustermetadatapb.ShardKey, oldPrimaryIDStr string) (*multiorchdatapb.PoolerHealthState, error) {
	re.pollAndWaitForNewSnapshots(ctx, shardKey)

	var newPrimary *multiorchdatapb.PoolerHealthState
	re.poolerStore.Range(func(_ string, p *multiorchdatapb.PoolerHealthState) bool {
		if p == nil || p.MultiPooler == nil || p.MultiPooler.Id == nil {
			return true
		}
		if p.MultiPooler.Database != shardKey.Database ||
			p.MultiPooler.TableGroup != shardKey.TableGroup ||
			p.MultiPooler.Shard != shardKey.Shard {
			return true
		}
		if topoclient.MultiPoolerIDString(p.MultiPooler.Id) == oldPrimaryIDStr {
			return true
		}
		t := p.GetStatus().GetPoolerType()
		if t == clustermetadatapb.PoolerType_UNKNOWN {
			t = p.MultiPooler.Type
		}
		if t == clustermetadatapb.PoolerType_PRIMARY {
			newPrimary = p
			return false
		}
		return true
	})

	if newPrimary == nil {
		return nil, mterrors.Errorf(mtrpcpb.Code_NOT_FOUND,
			"no new primary found in shard %s after election", commontypes.FormatShardKey(shardKey))
	}
	return newPrimary, nil
}
