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
	"log/slog"
	"time"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/eventlog"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
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
//   - Re-verifies the problem still exists (fresh RPC calls)
//   - Identifies the current primary from topology
//   - Configures the replica's primary_conninfo to point to the primary
//   - Verifies replication is streaming
//
// Cohort membership (adding/removing the replica to the primary's
// synchronous standby list) is managed separately by ReconcileCohortAction.
//
// Idempotency:
// This action is fully idempotent. If multiple multiorch instances race to fix
// the same problem, the end result will be identical. The underlying RPC
// operations (SetPrimary) are implemented as idempotent operations
// at the pooler level and serialized by action locks on the poolers, so
// concurrent calls are safe and produce the same final state.

// Default polling parameters for verifyReplicationStarted.
const (
	DefaultVerifyMaxAttempts  = 10
	DefaultVerifyPollInterval = 500 * time.Millisecond
)

type FixReplicationAction struct {
	config      *config.Config
	rpcClient   rpcclient.MultiPoolerClient
	poolerStore *store.PoolerCache
	logger      *slog.Logger

	// Polling parameters for verifyReplicationStarted.
	verifyMaxAttempts  int
	verifyPollInterval time.Duration
}

// NewFixReplicationAction creates a new fix replication action.
func NewFixReplicationAction(
	cfg *config.Config,
	rpcClient rpcclient.MultiPoolerClient,
	poolerStore *store.PoolerCache,
	logger *slog.Logger,
) *FixReplicationAction {
	maxAttempts := DefaultVerifyMaxAttempts
	pollInterval := DefaultVerifyPollInterval
	if cfg != nil {
		timeout := cfg.GetVerifyReplicationTimeout()
		if timeout > 0 {
			maxAttempts = max(int(timeout/DefaultVerifyPollInterval), 1)
		}
	}
	return &FixReplicationAction{
		config:             cfg,
		rpcClient:          rpcClient,
		poolerStore:        poolerStore,
		logger:             logger,
		verifyMaxAttempts:  maxAttempts,
		verifyPollInterval: pollInterval,
	}
}

// Execute performs replication fix for a replica that is not replicating.
func (a *FixReplicationAction) Execute(ctx context.Context, problem types.Problem) error {
	a.logger.InfoContext(ctx, "executing fix replication action",
		"shard_key", problem.ShardKey.String(),
		"pooler", problem.PoolerID.Name,
		"problem_code", string(problem.Code))

	// Find the affected replica
	replica, err := store.FindPoolerByID(a.poolerStore, problem.PoolerID)
	if err != nil {
		return mterrors.Wrap(err, "failed to find affected replica")
	}

	members := store.FindShardMembers(a.poolerStore, problem.ShardKey)
	leader := members.Leader
	if leader == nil {
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"no consensus leader known for shard %s", problem.ShardKey)
	}

	a.logger.InfoContext(ctx, "found primary for replication",
		"primary", leader.Health().MultiPooler.Id.Name,
		"replica", replica.Health().MultiPooler.Id.Name)

	// Re-verify the problem still exists
	needsFix, _, err := a.verifyProblemExists(ctx, replica, leader, problem.Code)
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
	case types.ProblemReplicaNotReplicating:
		return a.fixNotReplicating(ctx, replica, leader, members.HighestKnownRule)

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
	replica *store.Pooler,
	leader *store.Pooler,
	highestKnownRule *clustermetadatapb.ShardRule,
) (retErr error) {
	a.logger.InfoContext(ctx, "fixing replication: not configured",
		"replica", replica.Health().MultiPooler.Id.Name,
		"primary", leader.Health().MultiPooler.Id.Name)
	eventlog.Emit(ctx, a.logger, eventlog.Started, eventlog.NodeJoin{
		NodeName: replica.Health().MultiPooler.Id.Name,
	})
	defer func() {
		if retErr == nil {
			eventlog.Emit(ctx, a.logger, eventlog.Success, eventlog.NodeJoin{
				NodeName: replica.Health().MultiPooler.Id.Name,
			})
		} else {
			eventlog.Emit(ctx, a.logger, eventlog.Failed, eventlog.NodeJoin{
				NodeName: replica.Health().MultiPooler.Id.Name,
			}, "error", retErr)
		}
	}()

	// Configure primary_conninfo on the replica via SetPrimary. The rule is the
	// shard's highest-known rule (authoritative — the leader may not yet know it
	// holds that rule), the contact is the leader's topology address, and we
	// relay the leader's self-reported rewind_ready so a diverged replica defers
	// its pg_rewind until the leader has checkpointed onto its current timeline.
	setPrimaryReq := &consensusdatapb.SetPrimaryRequest{
		ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
			Rule:        highestKnownRule,
			Primary:     topoclient.PoolerAddressFor(leader.Health().MultiPooler),
			RewindReady: commonconsensus.ReplicationPrimaryOrNil(leader.Health().GetConsensusStatus()).GetRewindReady(),
		},
	}
	if _, err := a.rpcClient.SetPrimary(ctx, replica.Health().MultiPooler, setPrimaryReq); err != nil {
		return mterrors.Wrap(err, "SetPrimary RPC failed")
	}

	// Verify replication started
	err := a.verifyReplicationStarted(ctx, replica)
	if err != nil {
		a.logger.WarnContext(ctx, "replication did not start after configuration",
			"replica", replica.Health().MultiPooler.Id.Name,
			"primary", leader.Health().MultiPooler.Id.Name)

		// Re-check the primary's latest health-stream state before running pg_rewind.
		// pg_rewind stops the replica's postgres before contacting the source; if the
		// primary postgres is no longer running the stop will leave two nodes down.
		// Return an error for retry — the next cycle will detect PrimaryIsDead.
		primaryKey := topoclient.ComponentIDString(leader.Health().MultiPooler.Id)
		latest, ok := a.poolerStore.GetRider(primaryKey)
		if !ok || !latest.Health().GetStatus().GetPostgresReady() {
			return mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE,
				"primary postgres not running, skipping pg_rewind to avoid leaving two nodes down")
		}

		// Defer pg_rewind until the leader has checkpointed onto its current
		// timeline. Rewinding from a leader whose control file still advertises a
		// stale checkpoint timeline stamps that stale timeline into this replica's
		// minRecoveryPoint and FATALs on startup. The leader self-reports readiness
		// in its published ReplicationPrimary (auto-cleared on any new term, so a
		// true value is current). Return for retry; the next cycle re-checks once
		// the leader's post-promotion checkpoint completes.
		if !commonconsensus.ReplicationPrimaryOrNil(latest.Health().GetConsensusStatus()).GetRewindReady() {
			return mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE,
				"leader %s not yet rewind-ready (checkpoint pending on its current timeline); deferring pg_rewind",
				leader.Health().MultiPooler.Id.Name)
		}

		if rewindErr := a.tryPgRewind(ctx, leader, replica); rewindErr != nil {
			return mterrors.Wrap(rewindErr, "pg_rewind failed")
		}
		// Re-verify replication after rewind. RewindToSource restarts
		// PostgreSQL as a standby and re-establishes primary_conninfo,
		// which pg_rewind may have cleared by syncing postgresql.auto.conf
		// from the source.
		if verifyErr := a.verifyReplicationStarted(ctx, replica); verifyErr != nil {
			return mterrors.Wrap(verifyErr, "replication did not start after pg_rewind")
		}
	}

	// Cohort membership (adding the replica to synchronous_standby_names) is
	// managed by ReconcileCohortAction separately. By the time this action
	// returns, the replica is replicating; the cohort analyzer will pick it up
	// on the next cycle and promote adding it to the cohort.

	a.logger.InfoContext(ctx, "fix replication action completed successfully",
		"replica", replica.Health().MultiPooler.Id.Name,
		"primary", leader.Health().MultiPooler.Id.Name)

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
	primary *store.Pooler,
	replica *store.Pooler,
) error {
	a.logger.InfoContext(ctx, "attempting pg_rewind",
		"replica", replica.Health().MultiPooler.Id.Name,
		"primary", primary.Health().MultiPooler.Id.Name)

	// Call RewindToSource - it handles the entire flow atomically
	rewindReq := &multipoolermanagerdatapb.RewindToSourceRequest{
		Source: primary.Health().MultiPooler,
	}
	rewindResp, err := a.rpcClient.RewindToSource(ctx, replica.Health().MultiPooler, rewindReq)
	if err != nil {
		// RPC failure (e.g. primary postgres unreachable) is transient — do not
		// drain the pooler. Return an error so the next recovery cycle retries.
		a.logger.WarnContext(ctx, "pg_rewind RPC failed, will retry next cycle",
			"replica", replica.Health().MultiPooler.Id.Name,
			"error", err)
		return mterrors.Wrap(err, "pg_rewind RPC failed")
	}
	if !rewindResp.Success {
		// pg_rewind is not feasible (e.g. the required WAL has been recycled): the
		// replica cannot rejoin and needs replacement.
		//
		// TODO: signal this durably via the pooler's lifecycle stage so the
		// provisioner and orch treat the pooler as broken/needs-replacement. Orch
		// must not write the pooler's topology Type — the pooler owns its own record
		// and would clobber an external write. For now we surface an error so the
		// failure is visible; the next recovery cycle will retry.
		a.logger.WarnContext(ctx, "pg_rewind not feasible; pooler needs replacement",
			"replica", replica.Health().MultiPooler.Id.Name,
			"error", rewindResp.ErrorMessage)
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"pg_rewind not feasible for %s: %s", replica.Health().MultiPooler.Id.Name, rewindResp.ErrorMessage)
	}

	if rewindResp.RewindPerformed {
		a.logger.InfoContext(ctx, "pg_rewind completed successfully - servers were diverged",
			"replica", replica.Health().MultiPooler.Id.Name)
	} else {
		a.logger.InfoContext(ctx, "pg_rewind not needed - timelines are compatible",
			"replica", replica.Health().MultiPooler.Id.Name)
	}

	return nil
}

// verifyProblemExists re-checks whether the replication problem still exists.
// Returns true if the problem persists, false if already resolved.
func (a *FixReplicationAction) verifyProblemExists(
	ctx context.Context,
	replica *store.Pooler,
	primary *store.Pooler,
	problemCode types.ProblemCode,
) (bool, *multipoolermanagerdatapb.StandbyReplicationStatus, error) {
	switch problemCode {
	case types.ProblemReplicaNotReplicating:
		return a.verifyReplicaNotReplicating(ctx, replica, primary)

	default:
		return false, nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"unsupported problem code for verifyProblemExists: %s", problemCode)
	}
}

// verifyReplicaNotReplicating checks if the replica still has no replication configured.
func (a *FixReplicationAction) verifyReplicaNotReplicating(
	ctx context.Context,
	replica *store.Pooler,
	primary *store.Pooler,
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
			"replica", replica.Health().MultiPooler.Id.Name)
		return true, status, nil
	}

	// Check if pointing to the right primary
	expectedHost := primary.Health().MultiPooler.Hostname
	expectedPort := primary.Health().MultiPooler.PortMap["postgres"]

	// TODO: Do we need to verify timeline_id matches the primary's timeline?
	if status.PrimaryConnInfo.Host != expectedHost ||
		status.PrimaryConnInfo.Port != expectedPort {
		// Wrong primary - this would be ProblemReplicaWrongPrimary
		a.logger.InfoContext(ctx, "replica pointing to wrong primary",
			"replica", replica.Health().MultiPooler.Id.Name,
			"current_host", status.PrimaryConnInfo.Host,
			"current_port", status.PrimaryConnInfo.Port,
			"expected_host", expectedHost,
			"expected_port", expectedPort)
		return true, status, nil
	}

	// Check if WAL replay is paused (might need to resume)
	if status.IsWalReplayPaused {
		a.logger.InfoContext(ctx, "replica has WAL replay paused",
			"replica", replica.Health().MultiPooler.Id.Name)
		return true, status, nil
	}

	// Check that the WAL receiver is actually streaming. primary_conninfo being set
	// is not sufficient — the WAL receiver may have failed to start (e.g. timeline
	// divergence) or a previous pg_rewind attempt may have left conninfo on disk
	// while the receiver is not running. Without this check orch would consider the
	// problem resolved and never retry.
	if status.WalReceiverStatus != "streaming" {
		a.logger.InfoContext(ctx, "replica has primary_conninfo configured but WAL receiver is not streaming",
			"replica", replica.Health().MultiPooler.Id.Name,
			"wal_receiver_status", status.WalReceiverStatus)
		return true, status, nil
	}

	a.logger.InfoContext(ctx, "replication already configured correctly",
		"replica", replica.Health().MultiPooler.Id.Name,
		"last_receive_lsn", status.LastReceiveLsn,
		"last_replay_lsn", status.LastReplayLsn)

	return false, status, nil
}

// getReplicationStatus gets the current replication status from the replica.
func (a *FixReplicationAction) getReplicationStatus(
	ctx context.Context,
	replica *store.Pooler,
) (*multipoolermanagerdatapb.StandbyReplicationStatus, error) {
	statusResp, err := a.rpcClient.Status(ctx, replica.Health().MultiPooler, &multipoolermanagerdatapb.StatusRequest{})
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to get replication status")
	}
	if statusResp.Status == nil {
		return nil, nil
	}
	return statusResp.Status.ReplicationStatus, nil
}

// verifyReplicationStarted checks that replication is actively streaming.
// It polls a few times to allow the WAL receiver to connect.
func (a *FixReplicationAction) verifyReplicationStarted(ctx context.Context, replica *store.Pooler) error {
	ticker := time.NewTicker(a.verifyPollInterval)
	defer ticker.Stop()

	var lastErr error
	for attempt := 1; attempt <= a.verifyMaxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return mterrors.Wrap(ctx.Err(), "context cancelled while verifying replication")
		case <-ticker.C:
		}

		statusResp, err := a.rpcClient.Status(ctx, replica.Health().MultiPooler, &multipoolermanagerdatapb.StatusRequest{})
		if err != nil {
			lastErr = mterrors.Wrap(err, "failed to get replication status after fix")
			continue
		}

		var status *multipoolermanagerdatapb.StandbyReplicationStatus
		if statusResp.Status != nil {
			status = statusResp.Status.ReplicationStatus
		}
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
			"replica", replica.Health().MultiPooler.Id.Name,
			"wal_receiver_status", status.WalReceiverStatus,
			"last_receive_lsn", status.LastReceiveLsn,
			"last_replay_lsn", status.LastReplayLsn)

		return nil
	}

	return mterrors.Wrap(lastErr, "replication did not start after polling")
}

// RecoveryAction interface implementation

func (a *FixReplicationAction) RequiresHealthyLeader() bool {
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

func (a *FixReplicationAction) GracePeriod() *types.GracePeriodConfig {
	// No grace period needed, execute immediately
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
