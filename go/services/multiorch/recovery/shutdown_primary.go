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
	"sync"
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
		Database:              database,
		TableGroup:            tableGroup,
		Shard:                 shard,
		PrimaryId:             primaryID,
		DrainTimeout:          durationpb.New(10 * time.Second),
		StandbyCatchupTimeout: durationpb.New(30 * time.Second),
		Reason:                "SIGTERM",
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
// the time this runs; EmergencyDemote drains remaining connections and stops
// PostgreSQL. After stopping the primary, its topology type is updated to REPLICA
// so getpoolers never shows two PRIMARY nodes simultaneously.
//
// See the ShutdownPrimary RPC in multiorchservice.proto for the full sequence.
func (re *Engine) ShutdownPrimary(ctx context.Context, req *multiorchpb.ShutdownPrimaryRequest) (_ *multiorchpb.ShutdownPrimaryResponse, retErr error) {
	shardKey := commontypes.ShardKey{
		Database:   req.Database,
		TableGroup: req.TableGroup,
		Shard:      req.Shard,
	}
	primaryIDStr := topoclient.MultiPoolerIDString(req.PrimaryId)
	start := time.Now()

	re.logger.InfoContext(ctx, "ShutdownPrimary: starting graceful switchover",
		"shard_key", shardKey.String(),
		"primary", primaryIDStr,
		"drain_timeout", req.DrainTimeout.AsDuration(),
		"standby_catchup_timeout", req.StandbyCatchupTimeout.AsDuration(),
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

	// Step 2: Read the primary's current WAL LSN while it is still running.
	// The gateway has already stopped routing new writes (STOPPING signal), so
	// this LSN is a safe target for standby catchup. If the Status call fails
	// we skip the standby wait and proceed directly to EmergencyDemote.
	currentLSN := re.shutdownGetPrimaryLSN(ctx, primary.MultiPooler, primaryIDStr)

	// Step 3: Wait for standbys to replay up to the primary's current LSN.
	// The primary is still up and streaming WAL during this window, so standbys
	// should catch up quickly. AppointLeader picks the most-advanced node if
	// any standby times out.
	if currentLSN != "" {
		re.shutdownWaitForStandbys(ctx, shardKey, primaryIDStr, currentLSN, req.StandbyCatchupTimeout)
	}

	// Step 4: Stop the primary. Standbys have caught up (or timed out), so
	// EmergencyDemote is fast: few or no active writes to drain.
	re.logger.InfoContext(ctx, "ShutdownPrimary: stopping primary via EmergencyDemote",
		"primary", primaryIDStr)
	demoteResp, err := re.rpcClient.EmergencyDemote(ctx, primary.MultiPooler,
		&multipoolermanagerdatapb.EmergencyDemoteRequest{
			// force=true bypasses term validation: we are initiating this demotion
			// deliberately, not responding to a detected failure.
			Force:        true,
			DrainTimeout: req.DrainTimeout,
		})
	if err != nil {
		return nil, mterrors.Wrap(err, "EmergencyDemote failed")
	}
	re.logger.InfoContext(ctx, "ShutdownPrimary: primary stopped",
		"primary", primaryIDStr,
		"final_lsn", demoteResp.LsnPosition,
		"connections_terminated", demoteResp.ConnectionsTerminated)

	// Step 4b: Update the old primary's topology type to REPLICA so that
	// getpoolers does not show two PRIMARYs after the new one is elected.
	// The pooler sets STOPPING only on its in-memory health stream; it exits
	// before its monitor loop can rewrite the etcd record to REPLICA.
	if _, err := re.ts.UpdateMultiPoolerFields(ctx, req.PrimaryId, func(mp *clustermetadatapb.MultiPooler) error {
		mp.Type = clustermetadatapb.PoolerType_REPLICA
		return nil
	}); err != nil {
		re.logger.WarnContext(ctx, "ShutdownPrimary: failed to update old primary type to REPLICA in topology (non-fatal)",
			"primary", primaryIDStr, "error", err)
	}

	// Step 5: Build the election cohort, excluding STOPPING and DRAINED poolers.
	cohort := re.poolerStore.FindPoolersInShard(shardKey)
	filteredCohort := make([]*multiorchdatapb.PoolerHealthState, 0, len(cohort))
	for _, p := range cohort {
		if re.isPrimaryCandidate(p) {
			filteredCohort = append(filteredCohort, p)
		}
	}
	// Step 6: Elect a new primary from the cohort.
	re.logger.InfoContext(ctx, "ShutdownPrimary: electing new primary",
		"shard_key", shardKey.String(),
		"cohort_size", len(filteredCohort))
	if err := re.coordinator.AppointLeader(ctx, shardKey.Shard, filteredCohort, shardKey.Database, "ShutdownPrimary"); err != nil {
		return nil, mterrors.Wrap(err, "AppointLeader failed")
	}

	// Step 7: Find the newly elected primary and return its ID.
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
func (re *Engine) shutdownValidatePrimary(shardKey commontypes.ShardKey, id *clustermetadatapb.ID) (*multiorchdatapb.PoolerHealthState, error) {
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
			"pooler %s does not belong to shard %s", idStr, shardKey.String())
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

// shutdownWaitForStandbys calls WaitForLSN on each REPLICA in the shard in
// parallel. Timeouts and errors are logged as warnings but do not abort the
// switchover — catchup is best-effort per the ShutdownPrimary contract.
func (re *Engine) shutdownWaitForStandbys(
	ctx context.Context,
	shardKey commontypes.ShardKey,
	excludeIDStr string,
	targetLSN string,
	timeout *durationpb.Duration,
) {
	var standbys []*multiorchdatapb.PoolerHealthState
	re.poolerStore.Range(func(_ string, p *multiorchdatapb.PoolerHealthState) bool {
		if p == nil || p.MultiPooler == nil || p.MultiPooler.Id == nil {
			return true
		}
		if p.MultiPooler.Database != shardKey.Database ||
			p.MultiPooler.TableGroup != shardKey.TableGroup ||
			p.MultiPooler.Shard != shardKey.Shard {
			return true
		}
		if topoclient.MultiPoolerIDString(p.MultiPooler.Id) == excludeIDStr {
			return true
		}
		t := p.GetStatus().GetPoolerType()
		if t == clustermetadatapb.PoolerType_UNKNOWN {
			t = p.MultiPooler.Type
		}
		if t == clustermetadatapb.PoolerType_REPLICA {
			standbys = append(standbys, p)
		}
		return true
	})

	if len(standbys) == 0 {
		re.logger.WarnContext(ctx, "ShutdownPrimary: no standbys found, skipping catchup wait",
			"shard_key", shardKey.String())
		return
	}

	re.logger.InfoContext(ctx, "ShutdownPrimary: waiting for standbys to catch up",
		"shard_key", shardKey.String(),
		"standbys", len(standbys),
		"target_lsn", targetLSN)

	// Enforce the catchup deadline via context so WaitForLSN calls are
	// cancelled when the window expires — even if the primary has died and
	// WAL replay on standbys has stalled.
	waitCtx, cancel := context.WithTimeout(ctx, timeout.AsDuration())
	defer cancel()

	var wg sync.WaitGroup
	for _, s := range standbys {
		wg.Add(1)
		go func(standby *multiorchdatapb.PoolerHealthState) {
			defer wg.Done()
			standbyIDStr := topoclient.MultiPoolerIDString(standby.MultiPooler.Id)
			_, err := re.rpcClient.WaitForLSN(waitCtx, standby.MultiPooler,
				&multipoolermanagerdatapb.WaitForLSNRequest{
					TargetLsn: targetLSN,
					Timeout:   timeout,
				})
			if err != nil {
				re.logger.WarnContext(ctx, "ShutdownPrimary: standby did not catch up in time (proceeding anyway)",
					"standby", standbyIDStr, "error", err)
			} else {
				re.logger.InfoContext(ctx, "ShutdownPrimary: standby caught up",
					"standby", standbyIDStr, "lsn", targetLSN)
			}
		}(s)
	}
	wg.Wait()
}

// shutdownGetPrimaryLSN calls Status on the primary to get its current WAL LSN
// while it is still running. Returns an empty string (and logs a warning) if
// the call fails — callers skip the standby wait in that case.
func (re *Engine) shutdownGetPrimaryLSN(ctx context.Context, mp *clustermetadatapb.MultiPooler, primaryIDStr string) string {
	resp, err := re.rpcClient.Status(ctx, mp, &multipoolermanagerdatapb.StatusRequest{})
	if err != nil {
		re.logger.WarnContext(ctx, "ShutdownPrimary: could not read primary LSN, skipping standby wait",
			"primary", primaryIDStr, "error", err)
		return ""
	}
	lsn := resp.GetStatus().GetPrimaryStatus().GetLsn()
	if lsn == "" {
		re.logger.WarnContext(ctx, "ShutdownPrimary: primary LSN is empty, skipping standby wait",
			"primary", primaryIDStr)
	}
	return lsn
}

// shutdownFindNewPrimary polls shard poolers for fresh state and returns the
// pooler in the shard that is now PRIMARY, excluding the old primary.
func (re *Engine) shutdownFindNewPrimary(ctx context.Context, shardKey commontypes.ShardKey, oldPrimaryIDStr string) (*multiorchdatapb.PoolerHealthState, error) {
	re.pollShardPoolers(shardKey)

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
			"no new primary found in shard %s after election", shardKey.String())
	}
	return newPrimary, nil
}
