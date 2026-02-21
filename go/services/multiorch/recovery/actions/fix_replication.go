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
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"

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
//   - ProblemReplicaNotInStandbyList: Replica is replicating but not in standby list
//
// Future problem codes (TODO):
//   - ProblemReplicaWrongPrimary: Replica is pointing to a stale/wrong primary
//   - ProblemReplicaLagging: Replication is configured but lag is excessive
//
// The action:
//   - Re-verifies the problem still exists (fresh RPC calls)
//   - Identifies the current primary from topology
//   - Configures the replica's primary_conninfo to point to the primary
//   - Adds the replica to the primary's synchronous standby list
//   - Verifies replication is streaming
//
// Idempotency:
// This action is fully idempotent. If multiple multiorch instances race to fix
// the same problem, the end result will be identical. The underlying RPC operations
// (SetPrimaryConnInfo, UpdateSynchronousStandbyList) are implemented as idempotent
// operations at the pooler level and serialized by action locks on the poolers,
// so concurrent calls are safe and produce the same final state.
type FixReplicationAction struct {
	config      *config.Config
	rpcClient   rpcclient.MultiPoolerClient
	poolerStore *store.PoolerStore
	topoStore   topoclient.Store
	logger      *slog.Logger
}

// NewFixReplicationAction creates a new fix replication action.
func NewFixReplicationAction(
	cfg *config.Config,
	rpcClient rpcclient.MultiPoolerClient,
	poolerStore *store.PoolerStore,
	topoStore topoclient.Store,
	logger *slog.Logger,
) *FixReplicationAction {
	return &FixReplicationAction{
		config:      cfg,
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
	needsFix, _, err := a.verifyProblemExists(ctx, replica, primary, problem.Code)
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
// It checks for timeline divergence first before starting replication to avoid
// the race where PostgreSQL starts, connects to primary, and updates its timeline.
func (a *FixReplicationAction) fixNotReplicating(
	ctx context.Context,
	replica *multiorchdatapb.PoolerHealthState,
	primary *multiorchdatapb.PoolerHealthState,
) error {
	a.logger.InfoContext(ctx, "fixing replication: not configured",
		"replica", replica.MultiPooler.Id.Name,
		"primary", primary.MultiPooler.Id.Name)

	// Get the current consensus term from the primary
	consensusResp, err := a.rpcClient.ConsensusStatus(ctx, primary.MultiPooler, &consensusdatapb.StatusRequest{})
	if err != nil {
		return mterrors.Wrap(err, "failed to get consensus status from primary")
	}
	consensusTerm := consensusResp.CurrentTerm

	// Configure primary_conninfo on the replica
	req := &consensusdatapb.SetPrimaryConnInfoRequest{
		Primary:               primary.MultiPooler,
		StopReplicationBefore: true,
		StartReplicationAfter: true,
		CurrentTerm:           consensusTerm,
		Force:                 false,
	}

	_, err = a.rpcClient.SetPrimaryConnInfo(ctx, replica.MultiPooler, req)
	if err != nil {
		return mterrors.Wrap(err, "failed to set primary connection info")
	}

	// Verify replication started
	err = a.verifyReplicationStarted(ctx, replica)
	if err != nil {
		a.logger.WarnContext(ctx, "replication did not start after configuration",
			"replica", replica.MultiPooler.Id.Name,
			"primary", primary.MultiPooler.Id.Name)
		if rewindErr := a.tryPgRewind(ctx, primary, replica); rewindErr != nil {
			return mterrors.Wrap(rewindErr, "pg_rewind failed for diverged timelines")
		}
		return mterrors.Wrap(err, "replication did not start after configuration")
	}

	// Add replica to the primary's synchronous standby list if it's a REPLICA type
	if replica.MultiPooler.Type == clustermetadatapb.PoolerType_REPLICA {
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
	}

	a.logger.InfoContext(ctx, "fix replication action completed successfully",
		"replica", replica.MultiPooler.Id.Name,
		"primary", primary.MultiPooler.Id.Name)

	return nil
}

// tryPgRewind attempts to repair a replica using pg_rewind.
// RewindToSource will:
// 1. Stop postgres
// 2. Check if rewind is needed (dry-run)
// 3. Run actual rewind if needed
// 4. Start postgres
// If pg_rewind is not feasible (missing WAL), it marks the pooler as DRAINED.
func (a *FixReplicationAction) tryPgRewind(
	ctx context.Context,
	primary *multiorchdatapb.PoolerHealthState,
	replica *multiorchdatapb.PoolerHealthState,
) error {
	a.logger.InfoContext(ctx, "attempting pg_rewind",
		"replica", replica.MultiPooler.Id.Name,
		"primary", primary.MultiPooler.Id.Name)

	// Call RewindToSource - it handles the entire flow atomically
	rewindReq := &multipoolermanagerdatapb.RewindToSourceRequest{
		Source: primary.MultiPooler,
	}
	rewindResp, err := a.rpcClient.RewindToSource(ctx, replica.MultiPooler, rewindReq)
	if err != nil {
		a.logger.WarnContext(ctx, "pg_rewind RPC failed, marking as DRAINED",
			"replica", replica.MultiPooler.Id.Name,
			"error", err)
		return a.markPoolerDrained(ctx, replica)
	}
	if !rewindResp.Success {
		a.logger.WarnContext(ctx, "pg_rewind not feasible, marking as DRAINED",
			"replica", replica.MultiPooler.Id.Name,
			"error", rewindResp.ErrorMessage)
		return a.markPoolerDrained(ctx, replica)
	}

	if rewindResp.RewindPerformed {
		a.logger.InfoContext(ctx, "pg_rewind completed successfully - servers were diverged",
			"replica", replica.MultiPooler.Id.Name)
	} else {
		a.logger.InfoContext(ctx, "pg_rewind not needed - timelines are compatible",
			"replica", replica.MultiPooler.Id.Name)
	}

	return nil
}

// verifyProblemExists re-checks whether the replication problem still exists.
// Returns true if the problem persists, false if already resolved.
func (a *FixReplicationAction) verifyProblemExists(
	ctx context.Context,
	replica *multiorchdatapb.PoolerHealthState,
	primary *multiorchdatapb.PoolerHealthState,
	problemCode types.ProblemCode,
) (bool, *multipoolermanagerdatapb.StandbyReplicationStatus, error) {
	switch problemCode {
	case types.ProblemReplicaNotReplicating:
		return a.verifyReplicaNotReplicating(ctx, replica, primary)

	case types.ProblemReplicaNotInStandbyList:
		return a.verifyReplicaNotInStandbyList(ctx, replica, primary)

	default:
		return false, nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"unsupported problem code for verifyProblemExists: %s", problemCode)
	}
}

// verifyReplicaNotReplicating checks if the replica still has no replication configured.
func (a *FixReplicationAction) verifyReplicaNotReplicating(
	ctx context.Context,
	replica *multiorchdatapb.PoolerHealthState,
	primary *multiorchdatapb.PoolerHealthState,
) (bool, *multipoolermanagerdatapb.StandbyReplicationStatus, error) {
	status, err := a.getReplicationStatus(ctx, replica)
	if err != nil {
		return false, nil, err
	}
	if status == nil {
		// No status means we can't determine state, assume problem exists
		return true, nil, nil
	}

	// Check if primary_conninfo is configured
	if status.PrimaryConnInfo == nil || status.PrimaryConnInfo.Host == "" {
		a.logger.InfoContext(ctx, "replica has no primary_conninfo configured",
			"replica", replica.MultiPooler.Id.Name)
		return true, status, nil
	}

	// Check if pointing to the right primary
	expectedHost := primary.MultiPooler.Hostname
	expectedPort := primary.MultiPooler.PortMap["postgres"]

	// TODO: Do we need to verify timeline_id matches the primary's timeline?
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

	a.logger.InfoContext(ctx, "replication already configured correctly",
		"replica", replica.MultiPooler.Id.Name,
		"last_receive_lsn", status.LastReceiveLsn,
		"last_replay_lsn", status.LastReplayLsn)

	return false, status, nil
}

// verifyReplicaNotInStandbyList checks if the replica is still not in the standby list.
func (a *FixReplicationAction) verifyReplicaNotInStandbyList(
	ctx context.Context,
	replica *multiorchdatapb.PoolerHealthState,
	primary *multiorchdatapb.PoolerHealthState,
) (bool, *multipoolermanagerdatapb.StandbyReplicationStatus, error) {
	status, err := a.getReplicationStatus(ctx, replica)
	if err != nil {
		return false, nil, err
	}

	inList, err := a.isReplicaInStandbyList(ctx, replica, primary)
	if err != nil {
		return false, nil, mterrors.Wrap(err, "failed to check standby list")
	}
	if !inList {
		a.logger.InfoContext(ctx, "replica not in primary's standby list",
			"replica", replica.MultiPooler.Id.Name,
			"primary", primary.MultiPooler.Id.Name)
		return true, status, nil
	}

	a.logger.InfoContext(ctx, "replica already in standby list",
		"replica", replica.MultiPooler.Id.Name,
		"primary", primary.MultiPooler.Id.Name)

	return false, status, nil
}

// getReplicationStatus gets the current replication status from the replica.
func (a *FixReplicationAction) getReplicationStatus(
	ctx context.Context,
	replica *multiorchdatapb.PoolerHealthState,
) (*multipoolermanagerdatapb.StandbyReplicationStatus, error) {
	statusResp, err := a.rpcClient.StandbyReplicationStatus(ctx, replica.MultiPooler,
		&multipoolermanagerdatapb.StandbyReplicationStatusRequest{})
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to get standby replication status")
	}
	return statusResp.Status, nil
}

// isReplicaInStandbyList checks if the replica is in the primary's synchronous standby list.
func (a *FixReplicationAction) isReplicaInStandbyList(
	ctx context.Context,
	replica *multiorchdatapb.PoolerHealthState,
	primary *multiorchdatapb.PoolerHealthState,
) (bool, error) {
	// Get the primary's status to check the standby list
	primaryStatusResp, err := a.rpcClient.PrimaryStatus(ctx, primary.MultiPooler,
		&multipoolermanagerdatapb.PrimaryStatusRequest{})
	if err != nil {
		return false, mterrors.Wrap(err, "failed to get primary status")
	}

	if primaryStatusResp.Status == nil ||
		primaryStatusResp.Status.SyncReplicationConfig == nil {
		// No sync replication config means no standby list
		return false, nil
	}

	// Check if replica is in the standby list
	replicaID := replica.MultiPooler.Id
	for _, standbyID := range primaryStatusResp.Status.SyncReplicationConfig.StandbyIds {
		if standbyID.Cell == replicaID.Cell && standbyID.Name == replicaID.Name {
			return true, nil
		}
	}

	return false, nil
}

// verifyReplicationStarted checks that replication is actively streaming.
// It polls a few times to allow the WAL receiver to connect.
func (a *FixReplicationAction) verifyReplicationStarted(ctx context.Context, replica *multiorchdatapb.PoolerHealthState) error {
	const maxAttempts = 5
	const pollInterval = 500 * time.Millisecond

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return mterrors.Wrap(ctx.Err(), "context cancelled while verifying replication")
		case <-ticker.C:
		}

		statusResp, err := a.rpcClient.StandbyReplicationStatus(ctx, replica.MultiPooler,
			&multipoolermanagerdatapb.StandbyReplicationStatusRequest{})
		if err != nil {
			lastErr = mterrors.Wrap(err, "failed to get replication status after fix")
			continue
		}

		status := statusResp.Status
		if status == nil {
			lastErr = mterrors.Errorf(mtrpcpb.Code_INTERNAL, "no replication status returned")
			continue
		}

		// Check WAL receiver status first - this is the live connection state
		if status.WalReceiverStatus != "streaming" {
			lastErr = mterrors.Errorf(mtrpcpb.Code_INTERNAL,
				"WAL receiver not streaming (status: %s)", status.WalReceiverStatus)
			continue
		}

		// Also verify we have a receive LSN (sanity check)
		if status.LastReceiveLsn == "" {
			lastErr = mterrors.Errorf(mtrpcpb.Code_INTERNAL,
				"WAL receiver streaming but no receive LSN")
			continue
		}

		a.logger.InfoContext(ctx, "verified replication is streaming",
			"replica", replica.MultiPooler.Id.Name,
			"wal_receiver_status", status.WalReceiverStatus,
			"last_receive_lsn", status.LastReceiveLsn,
			"last_replay_lsn", status.LastReplayLsn)

		return nil
	}

	return mterrors.Wrap(lastErr, "replication did not start after polling")
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

func (a *FixReplicationAction) GracePeriod() *types.GracePeriodConfig {
	// No grace period needed, execute immediately
	return nil
}

// markPoolerDrained marks a pooler as DRAINED in the topology.
func (a *FixReplicationAction) markPoolerDrained(ctx context.Context, pooler *multiorchdatapb.PoolerHealthState) error {
	a.logger.InfoContext(ctx, "marking pooler as DRAINED", "pooler", pooler.MultiPooler.Id.Name)
	_, err := a.topoStore.UpdateMultiPoolerFields(ctx, pooler.MultiPooler.Id, func(mp *clustermetadatapb.MultiPooler) error {
		mp.Type = clustermetadatapb.PoolerType_DRAINED
		return nil
	})
	if err != nil {
		return mterrors.Wrap(err, "failed to mark pooler as DRAINED")
	}
	return nil
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
