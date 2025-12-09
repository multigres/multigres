// Copyright 2025 Supabase, Inc.
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

package actions

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/multiorch/recovery/types"
	"github.com/multigres/multigres/go/multiorch/store"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// Compile-time assertion that FixReplicationAction implements types.RecoveryAction.
var _ types.RecoveryAction = (*FixReplicationAction)(nil)

// FixReplicationAction handles replication configuration and repair for replicas.
//
// This action addresses the following problem codes:
//   - ProblemReplicaNotReplicating: Replication is not configured at all
//
// Future problem codes (TODO):
//   - ProblemReplicaWrongPrimary: Replica is pointing to a stale/wrong primary
//   - ProblemReplicaLagging: Replication is configured but lag is excessive
//
// The action:
// Re-verifies the problem still exists (fresh RPC calls)
// Identifies the current primary from topology
// Configures the replica's primary_conninfo to point to the primary
// Starts WAL replay if paused
// Verifies replication is streaming
type FixReplicationAction struct {
	rpcClient   rpcclient.MultiPoolerClient
	poolerStore *store.PoolerStore
	topoStore   topoclient.Store
	logger      *slog.Logger
}

// NewFixReplicationAction creates a new fix replication action.
func NewFixReplicationAction(
	rpcClient rpcclient.MultiPoolerClient,
	poolerStore *store.PoolerStore,
	topoStore topoclient.Store,
	logger *slog.Logger,
) *FixReplicationAction {
	return &FixReplicationAction{
		rpcClient:   rpcClient,
		poolerStore: poolerStore,
		topoStore:   topoStore,
		logger:      logger,
	}
}

// Execute performs replication fix for a replica that is not replicating.
func (a *FixReplicationAction) Execute(ctx context.Context, problem types.Problem) error {
	a.logger.InfoContext(ctx, "executing fix replication action",
		"shard_key", problem.ShardKey.String(),
		"pooler", problem.PoolerID.Name,
		"problem_code", string(problem.Code))

	a.logger.InfoContext(ctx, "acquiring recovery lock", "shard_key", problem.ShardKey.String())

	// Find the affected replica
	replica, err := a.poolerStore.FindPoolerByID(problem.PoolerID)
	if err != nil {
		return mterrors.Wrap(err, "failed to find affected replica")
	}

	// Get all poolers in this shard to find the primary
	poolers := a.poolerStore.FindPoolersInShard(problem.ShardKey)
	if len(poolers) == 0 {
		return fmt.Errorf("no poolers found for shard %s", problem.ShardKey)
	}

	// Find a healthy primary in the shard
	primary, err := a.poolerStore.FindHealthyPrimary(ctx, poolers)
	if err != nil {
		return mterrors.Wrap(err, "failed to find primary")
	}

	a.logger.InfoContext(ctx, "found primary for replication",
		"primary", primary.MultiPooler.Id.Name,
		"replica", replica.MultiPooler.Id.Name)

	// Re-verify the problem still exists
	needsFix, _, err := a.verifyProblemExists(ctx, replica, primary)
	if err != nil {
		return mterrors.Wrap(err, "failed to verify replication status")
	}
	if !needsFix {
		a.logger.InfoContext(ctx, "replication already configured correctly, problem resolved",
			"shard_key", problem.ShardKey.String(),
			"pooler", problem.PoolerID.Name)
		return nil
	}

	// Dispatch to the appropriate fix based on the problem
	switch problem.Code {
	case types.ProblemReplicaNotReplicating, types.ProblemReplicaNotInStandbyList:
		return a.fixNotReplicating(ctx, replica, primary)

	// TODO: Future problem codes to handle
	// case types.ProblemReplicaWrongPrimary:
	//     return a.fixWrongPrimary(ctx, replica, primary, currentStatus)
	// case types.ProblemReplicaLagging:
	//     return a.fixReplicaLagging(ctx, replica, primary, currentStatus)
	// case types.ProblemReplicaMisconfigured:
	//     return a.fixMisconfigured(ctx, replica, primary, currentStatus)

	default:
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"unsupported problem code for fix replication: %s", problem.Code)
	}
}

// fixNotReplicating handles the case where replication is not set up at all.
// This is the most basic case: the replica has no primary_conninfo configured.
func (a *FixReplicationAction) fixNotReplicating(
	ctx context.Context,
	replica *multiorchdatapb.PoolerHealthState,
	primary *multiorchdatapb.PoolerHealthState,
) error {
	a.logger.InfoContext(ctx, "fixing replication: not configured",
		"replica", replica.MultiPooler.Id.Name,
		"primary", primary.MultiPooler.Id.Name)

	// Get the primary's postgres port
	primaryPort, ok := primary.MultiPooler.PortMap["postgres"]
	if !ok {
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"primary %s has no postgres port configured", primary.MultiPooler.Id.Name)
	}

	// Get the current consensus term from the primary
	consensusResp, err := a.rpcClient.ConsensusStatus(ctx, primary.MultiPooler, &consensusdatapb.StatusRequest{})
	if err != nil {
		return mterrors.Wrap(err, "failed to get consensus status from primary")
	}

	consensusTerm := consensusResp.CurrentTerm

	a.logger.InfoContext(ctx, "got consensus term from primary",
		"primary", primary.MultiPooler.Id.Name,
		"consensus_term", consensusTerm)

	// Configure primary_conninfo on the replica
	req := &multipoolermanagerdatapb.SetPrimaryConnInfoRequest{
		Host:                  primary.MultiPooler.Hostname,
		Port:                  primaryPort,
		StopReplicationBefore: true,
		StartReplicationAfter: true,
		CurrentTerm:           consensusTerm,
		Force:                 false, // Don't force, respect consensus term
	}

	a.logger.InfoContext(ctx, "setting primary connection info",
		"replica", replica.MultiPooler.Id.Name,
		"primary_host", req.Host,
		"primary_port", req.Port,
		"consensus_term", consensusTerm)

	_, err = a.rpcClient.SetPrimaryConnInfo(ctx, replica.MultiPooler, req)
	if err != nil {
		return mterrors.Wrap(err, "failed to set primary connection info")
	}

	a.logger.InfoContext(ctx, "successfully configured primary connection info",
		"replica", replica.MultiPooler.Id.Name)

	// Add replica to the primary's synchronous standby list if it's a REPLICA type
	if replica.MultiPooler.Type == clustermetadatapb.PoolerType_REPLICA {
		a.logger.InfoContext(ctx, "adding replica to primary's synchronous standby list",
			"replica", replica.MultiPooler.Id.Name,
			"primary", primary.MultiPooler.Id.Name)

		updateReq := &multipoolermanagerdatapb.UpdateSynchronousStandbyListRequest{
			Operation:     multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_ADD,
			StandbyIds:    []*clustermetadatapb.ID{replica.MultiPooler.Id},
			ReloadConfig:  true,
			ConsensusTerm: consensusTerm,
			Force:         false,
		}

		_, err = a.rpcClient.UpdateSynchronousStandbyList(ctx, primary.MultiPooler, updateReq)
		if err != nil {
			return mterrors.Wrap(err, "failed to add replica to synchronous standby list")
		}

		a.logger.InfoContext(ctx, "successfully added replica to synchronous standby list",
			"replica", replica.MultiPooler.Id.Name)
	}

	// Verify replication is now working
	if err := a.verifyReplicationStarted(ctx, replica); err != nil {
		// Log but don't fail - the configuration is set, streaming may take a moment
		a.logger.WarnContext(ctx, "replication configured but streaming not yet verified",
			"replica", replica.MultiPooler.Id.Name,
			"error", err)
	}

	a.logger.InfoContext(ctx, "fix replication action completed successfully",
		"replica", replica.MultiPooler.Id.Name,
		"primary", primary.MultiPooler.Id.Name)

	return nil
}

// verifyProblemExists re-checks whether the replication problem still exists.
// Returns true if the problem persists, false if already resolved.
func (a *FixReplicationAction) verifyProblemExists(
	ctx context.Context,
	replica *multiorchdatapb.PoolerHealthState,
	primary *multiorchdatapb.PoolerHealthState,
) (bool, *multipoolermanagerdatapb.StandbyReplicationStatus, error) {
	// Get current replication status from the replica
	statusResp, err := a.rpcClient.StandbyReplicationStatus(ctx, replica.MultiPooler,
		&multipoolermanagerdatapb.StandbyReplicationStatusRequest{})
	if err != nil {
		return false, nil, mterrors.Wrap(err, "failed to get standby replication status")
	}

	status := statusResp.Status
	if status == nil {
		// No status means we can't determine state, assume problem exists
		return true, nil, nil
	}

	// Check if primary_conninfo is configured
	if status.PrimaryConnInfo == nil || status.PrimaryConnInfo.Host == "" {
		// No primary_conninfo - problem exists
		a.logger.InfoContext(ctx, "replica has no primary_conninfo configured",
			"replica", replica.MultiPooler.Id.Name)
		return true, status, nil
	}

	// Check if pointing to the right primary
	expectedHost := primary.MultiPooler.Hostname
	expectedPort := primary.MultiPooler.PortMap["postgres"]

	if status.PrimaryConnInfo.Host != expectedHost ||
		status.PrimaryConnInfo.Port != expectedPort {
		// Wrong primary - this would be ProblemReplicaWrongPrimary
		a.logger.InfoContext(ctx, "replica pointing to wrong primary",
			"replica", replica.MultiPooler.Id.Name,
			"current_host", status.PrimaryConnInfo.Host,
			"current_port", status.PrimaryConnInfo.Port,
			"expected_host", expectedHost,
			"expected_port", expectedPort)
		return true, status, nil
	}

	// Check if WAL replay is paused (might need to resume)
	if status.IsWalReplayPaused {
		a.logger.InfoContext(ctx, "replica has WAL replay paused",
			"replica", replica.MultiPooler.Id.Name)
		return true, status, nil
	}

	// Replication appears to be configured correctly
	a.logger.InfoContext(ctx, "replication already configured correctly",
		"replica", replica.MultiPooler.Id.Name,
		"last_receive_lsn", status.LastReceiveLsn,
		"last_replay_lsn", status.LastReplayLsn)

	return false, status, nil
}

// verifyReplicationStarted checks that replication is actively streaming.
func (a *FixReplicationAction) verifyReplicationStarted(ctx context.Context, replica *multiorchdatapb.PoolerHealthState) error {
	// Brief delay to allow WAL receiver to connect
	time.Sleep(500 * time.Millisecond)

	statusResp, err := a.rpcClient.StandbyReplicationStatus(ctx, replica.MultiPooler,
		&multipoolermanagerdatapb.StandbyReplicationStatusRequest{})
	if err != nil {
		return mterrors.Wrap(err, "failed to get replication status after fix")
	}

	status := statusResp.Status
	if status == nil {
		return mterrors.Errorf(mtrpcpb.Code_INTERNAL, "no replication status returned")
	}

	// Check that we have a receive LSN (indicates WAL receiver is connected)
	if status.LastReceiveLsn == "" {
		return mterrors.Errorf(mtrpcpb.Code_INTERNAL,
			"WAL receiver not streaming (no receive LSN)")
	}

	a.logger.InfoContext(ctx, "verified replication is streaming",
		"replica", replica.MultiPooler.Id.Name,
		"last_receive_lsn", status.LastReceiveLsn,
		"last_replay_lsn", status.LastReplayLsn)

	return nil
}

// RecoveryAction interface implementation

func (a *FixReplicationAction) RequiresHealthyPrimary() bool {
	return true // Cannot fix replica replication without a healthy primary
}

func (a *FixReplicationAction) Metadata() types.RecoveryMetadata {
	return types.RecoveryMetadata{
		Name:        "FixReplication",
		Description: "Configure or repair replication on a replica",
		Timeout:     45 * time.Second,
		LockTimeout: 15 * time.Second,
		Retryable:   true,
	}
}

func (a *FixReplicationAction) Priority() types.Priority {
	return types.PriorityHigh
}

// =============================================================================
// TODO: Future replication problem handlers
// =============================================================================
//
// The following are replication problems we should handle in future PRs:
//
// ProblemReplicaWrongPrimary
//    - Replica is connected to a stale primary (e.g., after failover)
//    - Fix: Update primary_conninfo to point to new primary, restart streaming
//    - Consider: We need to handle timeline changes.
//
// ProblemReplicaLagging
//    - Replication is working but lag exceeds threshold
//    - Causes to investigate:
//      a) Network congestion between primary and replica
//      b) Replica CPU/IO saturation (can't keep up with replay)
//      c) Long-running queries on replica blocking replay
//      d) Checkpoint/vacuum activity on primary generating excessive WAL
//      e) Synchronous replication bottleneck
//    - Fix: Depends on root cause; short-term we might not fix them, automatically
//           should understand why replication is broken.
// ProblemReplicaTimelineDiverged
//    - Replica is on wrong timeline.
//    - Fix: pg_rewind or full re-clone from primary
//    - Critical: Verify data integrity
//
// ProblemWalReceiverCrashing
//    - WAL receiver process repeatedly crashing
//    - Causes: Bad WAL segment, memory issues, bugs
//    - Fix: May need to skip corrupted WAL or re-clone
//
// ProblemReplicaSlotMissing
//    - NOTE: We are not creating a replication slot right now, so we might need to revisit this.
//    - Replication slot on primary was dropped
//    - Symptoms: Replica can't stream, gets "replication slot does not exist"
//    - Fix: Create new slot, may need to re-clone if WAL recycled
//
// ProblemSynchronousStandbyMisconfigured
//    - synchronous_standby_names doesn't match actual standbys
//    - Symptoms: Primary waiting indefinitely for sync confirmation
//    - Fix: Update synchronous_standby_names to match reality
