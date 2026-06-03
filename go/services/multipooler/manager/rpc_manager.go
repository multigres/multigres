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

package manager

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// broadcastHealth broadcasts the current health state to all subscribers.
//
// This should be called whenever there is a state change that clients should be
// aware of (e.g., PostgreSQL availability, replication status, etc.). Clients
// will receive the latest health snapshot immediately if they are connected, or
// upon their next connection if they are not currently connected.
func (pm *MultiPoolerManager) broadcastHealth() {
	if pm.healthStreamer != nil {
		pm.healthStreamer.Broadcast()
	}
}

// WaitForLSN waits for PostgreSQL server to reach a specific LSN position
func (pm *MultiPoolerManager) WaitForLSN(ctx context.Context, targetLsn string) error {
	if err := pm.checkReady(); err != nil {
		return err
	}

	// Check REPLICA guardrails (pooler type and recovery mode)
	if err := pm.checkReplicaGuardrails(ctx); err != nil {
		return err
	}

	// Wait for the standby to replay WAL up to the target LSN
	// We use a polling approach to check if the replay LSN has reached the target
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			pm.logger.ErrorContext(ctx, "WaitForLSN context cancelled or timed out",
				"target_lsn", targetLsn,
				"error", ctx.Err())
			return mterrors.Wrap(ctx.Err(), "context cancelled or timed out while waiting for LSN")

		case <-ticker.C:
			// Check if the standby has replayed up to the target LSN
			reachedTarget, err := pm.checkLSNReached(ctx, targetLsn)
			if err != nil {
				pm.logger.ErrorContext(ctx, "Failed to check replay LSN", "error", err)
				return err
			}

			if reachedTarget {
				pm.logger.InfoContext(ctx, "Standby reached target LSN", "target_lsn", targetLsn)
				return nil
			}
		}
	}
}

// setPrimaryConnInfoLocked sets the primary connection info for a standby server.
// This function assumes the action lock is already held by the caller.
//
// Refuses with FAILED_PRECONDITION when StopReplication previously cleared
// primary_conninfo and set the manual-stop flag — every conninfo writer
// (SetTermPrimary's standby branch and the postgres-monitor self-heal)
// funnels through here, so this single check is what keeps the admin pause
// honored against routine reconciliation. Use StartReplication to clear the
// flag before rewriting conninfo. demoteStalePrimaryLocked clears the flag itself before reaching
// this point — a stale-primary detection is an escalated event that
// supersedes an older admin pause.
func (pm *MultiPoolerManager) setPrimaryConnInfoLocked(ctx context.Context, host string, port int32, stopReplicationBefore, startReplicationAfter bool) error {
	if err := AssertActionLockHeld(ctx); err != nil {
		return err
	}

	if err := pm.checkReady(); err != nil {
		return err
	}

	if pm.walReceiverManuallyStopped.Load() {
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			"replication manually stopped via StopReplication; call StartReplication first")
	}

	// Guardrail: Check if the PostgreSQL instance is in recovery (standby mode)
	isPrimary, err := pm.isPrimary(ctx)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to check if instance is in recovery", "error", err)
		return mterrors.Wrap(err, "failed to check recovery status")
	}

	if isPrimary {
		pm.logger.ErrorContext(ctx, "setPrimaryConnInfo called on non-standby instance", "service_id", pm.serviceID.String())
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			fmt.Sprintf("operation not allowed: the PostgreSQL instance is not in standby mode (service_id: %s)", pm.serviceID.String()))
	}

	appName := pm.servicePoolerID

	// Optionally stop replication before making changes
	if stopReplicationBefore {
		_, err := pm.pauseReplication(ctx, multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_ONLY, false)
		if err != nil {
			return err
		}
	}

	// Build primary_conninfo connection string
	// Format: host=<host> port=<port> user=<user> application_name=<name> [passfile=<path>]
	// The heartbeat_interval is converted to keepalives_interval/keepalives_idle.
	// passfile points libpq at the pgpass file written at manager startup so the
	// standby can authenticate to the primary via SCRAM without embedding the
	// password in postgresql.auto.conf. It is omitted when pgpassPath is unset
	// (early startup or unit tests that bypass loadShardConfigFromGlobalTopo).
	user := constants.DefaultPostgresUser
	if pm.connPoolMgr != nil {
		user = pm.connPoolMgr.PgUser()
	}
	connInfo := fmt.Sprintf("host=%s port=%d user=%s application_name=%s",
		host, port, user, appName.appName)
	if pm.pgpassPath != "" {
		connInfo += " passfile=" + pm.pgpassPath
	}

	// Set primary_conninfo using ALTER SYSTEM
	if err = pm.setPrimaryConnInfo(ctx, connInfo); err != nil {
		return err
	}

	// Reload PostgreSQL configuration to apply changes
	if err = pm.reloadPostgresConfig(ctx); err != nil {
		return err
	}

	// Optionally start replication after making changes.
	// Note: If replication was already running when setPrimaryConnInfoLocked was called,
	// even if we don't set startReplicationAfter to true, replication will be running.
	if startReplicationAfter {
		// Wait for database to be available after restart
		if err := pm.waitForDatabaseConnection(ctx); err != nil {
			pm.logger.ErrorContext(ctx, "Failed to reconnect to database after restart", "error", err)
			return mterrors.Wrap(err, "failed to reconnect to database")
		}

		pm.logger.InfoContext(ctx, "Starting replication after setting primary_conninfo")
		if err := pm.resumeWALReplay(ctx); err != nil {
			return err
		}
	}

	pm.logger.InfoContext(ctx, "setPrimaryConnInfo completed successfully",
		"host", host,
		"port", port,
		"stop_replication_before", stopReplicationBefore,
		"start_replication_after", startReplicationAfter)

	return nil
}

// StartReplication starts WAL replay on standby (calls pg_wal_replay_resume).
// As the counterpart to StopReplication, it also clears any in-memory
// "manually stopped" signal that StopReplication may have set. Clearing
// flips this pooler's published CohortEligibility back to ELIGIBLE and
// re-enables the postgres-monitor self-heal of primary_conninfo, which in
// turn lets routine reconciliation re-establish the WAL receiver.
//
// StartReplication itself does not rewrite primary_conninfo — that happens
// via the monitor's self-heal (or via orch's FixReplicationAction) once
// eligibility flips.
func (pm *MultiPoolerManager) StartReplication(ctx context.Context) error {
	if err := pm.checkReady(); err != nil {
		return err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	ctx, err := pm.actionLock.Acquire(ctx, "StartReplication")
	if err != nil {
		return err
	}
	defer pm.actionLock.Release(ctx)

	started := pm.walReceiverManuallyStopped.CompareAndSwap(true, false)

	// Check REPLICA guardrails (pooler type and recovery mode)
	if err = pm.checkReplicaGuardrails(ctx); err != nil {
		return err
	}

	// Resume WAL replay on the standby
	if err := pm.resumeWALReplay(ctx); err != nil {
		return err
	}

	if started {
		pm.broadcastHealth()
	}

	return nil
}

// StopReplication stops replication based on the specified mode
func (pm *MultiPoolerManager) StopReplication(ctx context.Context, mode multipoolermanagerdatapb.ReplicationPauseMode, wait bool) error {
	if err := pm.checkReady(); err != nil {
		return err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	ctx, err := pm.actionLock.Acquire(ctx, "StopReplication")
	if err != nil {
		return err
	}
	defer pm.actionLock.Release(ctx)

	// Check REPLICA guardrails (pooler type and recovery mode)
	if err = pm.checkReplicaGuardrails(ctx); err != nil {
		return err
	}

	_, err = pm.pauseReplication(ctx, mode, wait)
	if err != nil {
		return err
	}

	// Modes that clear primary_conninfo are an explicit admin signal that
	// replication should stay stopped. Mark this so the postgres monitor
	// does not "self-heal" the cleared conninfo back to the recorded primary,
	// and so this pooler publishes COHORT_ELIGIBILITY_INELIGIBLE while
	// stopped. Cleared the next time something re-establishes the primary
	// link (SetTermPrimary / setPrimaryConnInfoLocked / demoteStalePrimaryLocked).
	switch mode {
	case multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_RECEIVER_ONLY,
		multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_AND_RECEIVER:
		if pm.walReceiverManuallyStopped.CompareAndSwap(false, true) {
			pm.broadcastHealth()
		}
	}

	return nil
}

// StandbyReplicationStatus gets the current replication status of the standby
func (pm *MultiPoolerManager) StandbyReplicationStatus(ctx context.Context) (*multipoolermanagerdatapb.StandbyReplicationStatus, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}

	// Check REPLICA guardrails (pooler type and recovery mode)
	if err := pm.checkReplicaGuardrails(ctx); err != nil {
		return nil, err
	}

	// Query all replication status fields
	status, err := pm.queryReplicationStatus(ctx)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to get replication status", "error", err)
		return nil, err
	}

	return status, nil
}

// Status gets unified status that works for both PRIMARY and REPLICA poolers.
// This RPC works even when the database connection is unavailable - fields that require
// database access will be nil/empty in that case. This allows callers to always get
// initialization status without needing a separate RPC.
func (pm *MultiPoolerManager) Status(ctx context.Context) (*multipoolermanagerdatapb.StatusResponse, error) {
	poolerStatus := &multipoolermanagerdatapb.Status{
		PoolerType:       pm.getPoolerType(),
		IsInitialized:    pm.isInitialized(ctx),
		HasDataDirectory: pm.hasDataDirectory(),
		PostgresReady:    pm.isPostgresReady(ctx),
		PostgresRunning:  pm.isPostgresRunning(ctx),
		PostgresStatus:   pm.getServerStatus(ctx),
		ShardId:          pm.getShardID(),
	}

	if action, duration := pm.actionLock.ActiveAction(); action != multipoolermanagerdatapb.PostgresAction_POSTGRES_ACTION_UNSPECIFIED {
		poolerStatus.PostgresAction = action
		poolerStatus.PostgresActionDuration = durationpb.New(duration)
	}

	// Get WAL position (ignore errors, just return empty string)
	walPosition, _ := pm.getWALPosition(ctx)
	poolerStatus.WalPosition = walPosition

	// Get cohort members from the current rule (best-effort).
	if pos, err := pm.rules.observePosition(ctx); err != nil {
		pm.logger.WarnContext(ctx, "Failed to read current rule for status", "error", err)
	} else if pos.Rule != nil {
		poolerStatus.CohortMembers = pos.Rule.CohortMembers
	}

	resp := &multipoolermanagerdatapb.StatusResponse{
		Status: poolerStatus,
	}

	if cs, err := pm.getInconsistentConsensusStatus(ctx); err == nil {
		resp.ConsensusStatus = cs
	}
	resp.AvailabilityStatus = pm.buildAvailabilityStatus()

	// Try to get detailed status based on PostgreSQL role
	isPrimary, err := pm.isPrimary(ctx)
	if err != nil {
		// Can't determine role - return what we have
		pm.logger.WarnContext(ctx, "Failed to check PostgreSQL role, returning partial status", "error", err)
		return resp, nil
	}

	// Populate role-specific status
	if isPrimary {
		// Acting as primary - get primary status (skip guardrails since we already checked isPrimary)
		primaryStatus, err := pm.getPrimaryStatusInternal(ctx)
		if err != nil {
			pm.logger.WarnContext(ctx, "Failed to get primary status", "error", err)
			// Return partial status instead of error
			return resp, nil
		}
		poolerStatus.PrimaryStatus = primaryStatus
		return resp, nil
	}
	// Acting as standby - get replication status (skip guardrails since we already checked isPrimary)
	replStatus, err := pm.getStandbyStatusInternal(ctx)
	if err != nil {
		pm.logger.WarnContext(ctx, "Failed to get standby replication status", "error", err)
		// Return partial status instead of error
		return resp, nil
	}
	poolerStatus.ReplicationStatus = replStatus
	return resp, nil
}

// ResetReplication resets the standby's connection to its primary by clearing primary_conninfo
// and reloading PostgreSQL configuration. This effectively disconnects the replica from the primary
// and prevents it from acknowledging commits, making it unavailable for synchronous replication
// until reconfigured.
func (pm *MultiPoolerManager) ResetReplication(ctx context.Context) error {
	if err := pm.checkReady(); err != nil {
		return err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	ctx, err := pm.actionLock.Acquire(ctx, "ResetReplication")
	if err != nil {
		return err
	}
	defer pm.actionLock.Release(ctx)

	// Check REPLICA guardrails (pooler type and recovery mode)
	if err = pm.checkReplicaGuardrails(ctx); err != nil {
		return err
	}

	// Pause the receiver (clear primary_conninfo) and wait for disconnect
	_, err = pm.pauseReplication(ctx, multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_RECEIVER_ONLY, true /* wait */)
	if err != nil {
		return err
	}

	return nil
}

// UpdateConsensusRule updates PostgreSQL synchronous_standby_names by adding
// or removing members. It is idempotent and only valid when synchronous
// replication is already configured.
//
// expectedOutgoingRule provides compare-and-swap semantics: the operation
// proceeds only if this pooler's current recorded rule matches the given
// RuleNumber. If they differ (the caller's view is stale), the operation
// fails — the caller should re-read state and retry.
func (pm *MultiPoolerManager) UpdateConsensusRule(ctx context.Context, operation multipoolermanagerdatapb.CohortUpdateOperation, standbyIDs []*clustermetadatapb.ID, expectedOutgoingRule *clustermetadatapb.RuleNumber, coordinatorID *clustermetadatapb.ID) error {
	if err := pm.checkReady(); err != nil {
		return err
	}

	// Validate operation
	if operation == multipoolermanagerdatapb.CohortUpdateOperation_COHORT_UPDATE_OPERATION_UNSPECIFIED {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "operation must be specified")
	}

	if expectedOutgoingRule == nil {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			"expected_outgoing_rule is required (compare-and-swap guard)")
	}

	// Validate standby IDs using the shared validation function
	requestedApplicationNames, err := validateStandbyIDs(standbyIDs)
	if err != nil {
		return err
	}

	// Pre-compute history fields before acquiring the lock.
	leaderID := pm.servicePoolerID

	ctx, err = pm.actionLock.Acquire(ctx, "UpdateConsensusRule")
	if err != nil {
		return err
	}
	defer pm.actionLock.Release(ctx)

	// Check PRIMARY guardrails (pooler type and non-recovery mode)
	if err = pm.checkPrimaryGuardrails(ctx); err != nil {
		return err
	}

	// === Parse Current Configuration ===

	// Read current cohort from the rule store (authoritative source of truth).
	pos, err := pm.rules.observePosition(ctx)
	if err != nil {
		return err
	}
	currentCohort := pos.GetRule().GetCohortMembers()

	// Check if synchronous replication is configured (i.e. the primary already
	// has a cohort recorded from a previous Propose/promotion).
	if len(currentCohort) == 0 {
		pm.logger.ErrorContext(ctx, "UpdateConsensusRule requires synchronous replication to be configured")
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			"empty cohort -- shard bootstrap needed")
	}

	// Convert current cohort IDs to pooler IDs for set operations.
	currentApplicationNames, err := toPoolerIDs(currentCohort)
	if err != nil {
		return err
	}

	// === Apply Operation ===

	var updatedStandbys []poolerID
	switch operation {
	case multipoolermanagerdatapb.CohortUpdateOperation_COHORT_UPDATE_OPERATION_ADD:
		updatedStandbys = applyAddOperation(currentApplicationNames, requestedApplicationNames)

	case multipoolermanagerdatapb.CohortUpdateOperation_COHORT_UPDATE_OPERATION_REMOVE:
		updatedStandbys = applyRemoveOperation(currentApplicationNames, requestedApplicationNames)

	default:
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			"unsupported operation: "+operation.String())
	}

	// Validate that the final list is not empty
	if len(updatedStandbys) == 0 {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			"resulting standby list cannot be empty after operation")
	}

	// Check if there are any changes (idempotent).
	if poolerIDSetEqual(currentApplicationNames, updatedStandbys) {
		return nil
	}

	operationName := standbyUpdateOperationName(operation)

	// Insert history before applying GUCs
	// Rationale: we want to ensure that a new cohort is advertised
	// before this primary can accept ACKs from it.
	// This is for safe replica joining of the cluster.
	// It will ensure multiorch can discover the new cohort during a failure.
	coordID := coordinatorID
	if coordID == nil {
		coordID = pm.serviceID
	}
	updatedStandbyIDs := make([]*clustermetadatapb.ID, len(updatedStandbys))
	for i, p := range updatedStandbys {
		updatedStandbyIDs[i] = p.id
	}
	// The new rule inherits the expected coordinator term — we're not
	// changing the leader, just amending its cohort. The rule store assigns
	// a fresh leader_subterm.
	standbyUpdate := newRuleUpdate(
		expectedOutgoingRule.GetCoordinatorTerm(),
		coordID,
		"replication_config",
		"UpdateConsensusRule: "+operationName,
		time.Now()).
		withLeader(leaderID.id).
		withCohort(updatedStandbyIDs).
		withOperation(operationName).
		withPreviousRule(
			expectedOutgoingRule.GetCoordinatorTerm(),
			expectedOutgoingRule.GetLeaderSubterm())
	if _, err := pm.rules.updateRule(ctx, standbyUpdate); err != nil {
		return mterrors.Wrap(err, "failed to record replication config history")
	}

	pm.logger.InfoContext(ctx, "UpdateConsensusRule completed successfully",
		"operation", operation,
		"old_cohort", currentCohort,
		"new_cohort", updatedStandbyIDs,
		"expected_outgoing_rule", expectedOutgoingRule)

	// Push an immediate health snapshot so orchestrators learn about the changed
	// synchronous standby list without waiting for the next 30-second heartbeat.
	pm.broadcastHealth()
	return nil
}

// getPrimaryStatusInternal gets primary status without guardrail checks.
// Called by Status() which has already verified the PostgreSQL role.
func (pm *MultiPoolerManager) getPrimaryStatusInternal(ctx context.Context) (*multipoolermanagerdatapb.PrimaryStatus, error) {
	status := &multipoolermanagerdatapb.PrimaryStatus{}

	// Get current LSN
	lsn, err := pm.getPrimaryLSN(ctx)
	if err != nil {
		return nil, err
	}
	status.Lsn = lsn
	status.Ready = true

	// Get connected followers from pg_stat_replication
	followers, err := pm.getConnectedFollowerIDs(ctx)
	if err != nil {
		return nil, err
	}
	status.ConnectedFollowers = followers

	// Get synchronous replication configuration
	syncConfig, err := pm.getSynchronousReplicationConfig(ctx)
	if err != nil {
		return nil, err
	}
	status.SyncReplicationConfig = syncConfig

	return status, nil
}

// getStandbyStatusInternal gets standby replication status without guardrail checks.
// Called by Status() which has already verified the PostgreSQL role.
func (pm *MultiPoolerManager) getStandbyStatusInternal(ctx context.Context) (*multipoolermanagerdatapb.StandbyReplicationStatus, error) {
	return pm.queryReplicationStatus(ctx)
}

// PrimaryStatus gets the status of the leader server
func (pm *MultiPoolerManager) PrimaryStatus(ctx context.Context) (*multipoolermanagerdatapb.PrimaryStatus, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}

	// Check PRIMARY guardrails (pooler type and non-recovery mode)
	if err := pm.checkPrimaryGuardrails(ctx); err != nil {
		return nil, err
	}

	status, err := pm.getPrimaryStatusInternal(ctx)
	if err != nil {
		return nil, err
	}

	return status, nil
}

// PrimaryPosition gets the current LSN position of the leader
func (pm *MultiPoolerManager) PrimaryPosition(ctx context.Context) (string, error) {
	if err := pm.checkReady(); err != nil {
		return "", err
	}

	// Check PRIMARY guardrails (pooler type and non-recovery mode)
	if err := pm.checkPrimaryGuardrails(ctx); err != nil {
		return "", err
	}

	// Get current primary LSN position
	return pm.getPrimaryLSN(ctx)
}

// StopReplicationAndGetStatus stops PostgreSQL replication (replay and/or receiver based on mode) and returns the status
func (pm *MultiPoolerManager) StopReplicationAndGetStatus(ctx context.Context, mode multipoolermanagerdatapb.ReplicationPauseMode, wait bool) (*multipoolermanagerdatapb.StandbyReplicationStatus, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	ctx, err := pm.actionLock.Acquire(ctx, "StopReplicationAndGetStatus")
	if err != nil {
		return nil, err
	}
	defer pm.actionLock.Release(ctx)

	// Check REPLICA guardrails (pooler type and recovery mode)
	if err = pm.checkReplicaGuardrails(ctx); err != nil {
		return nil, err
	}

	status, err := pm.pauseReplication(ctx, mode, wait)
	if err != nil {
		return nil, err
	}

	pm.logger.InfoContext(ctx, "StopReplicationAndGetStatus completed",
		"last_replay_lsn", status.LastReplayLsn,
		"last_receive_lsn", status.LastReceiveLsn,
		"is_paused", status.IsWalReplayPaused,
		"pause_state", status.WalReplayPauseState,
		"primary_conn_info", status.PrimaryConnInfo)

	return status, nil
}

// changeTypeLocked updates the pooler type without acquiring the action lock.
// The caller MUST already hold the action lock.
func (pm *MultiPoolerManager) changeTypeLocked(ctx context.Context, poolerType clustermetadatapb.PoolerType) error {
	if err := AssertActionLockHeld(ctx); err != nil {
		return err
	}

	pm.logger.InfoContext(ctx, "changeTypeLocked called", "pooler_type", poolerType.String(), "service_id", pm.serviceID.String())

	// Use the serving state manager to transition components and update the pooler record.
	// The serving status stays SERVING during type changes (the node remains available). Mutate
	// inside StateManager schedules an async publish to topology.
	if err := pm.servingState.SetState(ctx, poolerType, clustermetadatapb.PoolerServingStatus_SERVING); err != nil {
		return mterrors.Wrap(err, "failed to set serving state")
	}

	pm.logger.InfoContext(ctx, "Pooler type updated successfully", "new_type", poolerType.String(), "service_id", pm.serviceID.String())
	return nil
}

// emergencyDemoteLocked performs the core demotion logic.
// REQUIRES: action lock must already be held by the caller.
// This is used for emergency demote operations.
// We won't try to perform a graceful switchover in this case.
// We will drain this pooler and stop postgres.
// This should only be called during ungraceful shutdown.
// MultiOrch will try to contact all nodes in the cohort.
// In the case that the dead primary received the RPC, it should just
// shut down itself.
func (pm *MultiPoolerManager) emergencyDemoteLocked(ctx context.Context, consensusTerm int64, drainTimeout time.Duration) error {
	// Verify action lock is held
	if err := AssertActionLockHeld(ctx); err != nil {
		return err
	}

	// === Validation & State Check ===

	// Guard rail: Demote can only be called on a PRIMARY
	if err := pm.checkPrimaryGuardrails(ctx); err != nil {
		return err
	}

	// Check current demotion state
	state, err := pm.checkDemotionState(ctx)
	if err != nil {
		return err
	}

	// If everything is already complete, return early (fully idempotent)
	if state.isNotServing && state.isReplicaInTopology && state.isReadOnly {
		return nil
	}

	// Transition to NOT_SERVING — rejects all queries and stops heartbeat.
	// This ensures no new writes arrive while we drain existing connections.
	if err := pm.setNotServing(ctx, state); err != nil {
		return err
	}

	// Drain write connections

	if err := pm.drainWriteActivity(ctx, drainTimeout); err != nil {
		return err
	}

	// Terminate Remaining Write Connections

	connectionsTerminated, err := pm.terminateWriteConnections(ctx)
	if err != nil {
		// Log but don't fail - connections will eventually timeout
		pm.logger.WarnContext(ctx, "Failed to terminate write connections", "error", err)
	}

	// Capture State & Make PostgreSQL Read-Only
	finalLSN, err := pm.getPrimaryLSN(ctx)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to capture final LSN", "error", err)
		return err
	}

	// Signal voluntary resignation so the coordinator can trigger an immediate
	// election without waiting for a heartbeat timeout. Use this node's own
	// primary_term (not the incoming consensusTerm) so the coordinator can
	// correlate the signal with the term at which this node was elected.
	// setResignedLeaderAtTerm broadcasts internally on a change so multiorch
	// sees leadership_status.REQUESTING_DEMOTION before the next periodic
	// health stream interval fires.
	if primaryTerm, err := pm.primaryTermLocked(ctx); err == nil && primaryTerm != 0 {
		if err := pm.setResignedLeaderAtTerm(ctx, primaryTerm); err != nil {
			return mterrors.Wrap(err, "failed to set resigned primary term")
		}
	}

	// Restart PostgreSQL as standby. Unlike the old stop-only path, this keeps
	// the node in the cluster as a replication target, avoiding timeline divergence
	// in most cases. The coordinator still uses pg_rewind for nodes that diverged.
	if err := pm.restartPostgresAsStandby(ctx, state); err != nil {
		return err
	}

	pm.healthStreamer.UpdateLeaderObservation(nil)

	// Suppress the postgres monitor until a rewind completes; the monitor would
	// otherwise restart postgres on this demoted node.
	pm.rewindPending.Store(true)

	pm.logger.InfoContext(ctx, "Demote completed successfully",
		"final_lsn", finalLSN,
		"consensus_term", consensusTerm,
		"connections_terminated", connectionsTerminated)

	return nil
}

// UndoDemote undoes a demotion
func (pm *MultiPoolerManager) UndoDemote(ctx context.Context) error {
	if err := pm.checkReady(); err != nil {
		return err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	ctx, err := pm.actionLock.Acquire(ctx, "UndoDemote")
	if err != nil {
		return err
	}
	defer pm.actionLock.Release(ctx)

	pm.logger.InfoContext(ctx, "UndoDemote called")
	return mterrors.New(mtrpcpb.Code_UNIMPLEMENTED, "method UndoDemote not implemented")
}

// RewindToSource pg_rewinds this server against source and brings it back as a
// standby. The heavy lifting (stop, rewind, restart-as-standby, resume) is
// shared with the stale-primary demote path via stopRewindRestartAsStandbyLocked.
func (pm *MultiPoolerManager) RewindToSource(ctx context.Context, source *clustermetadatapb.MultiPooler) (*multipoolermanagerdatapb.RewindToSourceResponse, error) {
	if err := pm.checkReady(); err != nil {
		return nil, mterrors.Wrap(err, "multipooler not ready")
	}

	if source == nil || source.PortMap == nil {
		return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "source pooler or port_map is nil")
	}
	if source.Hostname == "" {
		return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "source hostname is required")
	}
	port, ok := source.PortMap["postgres"]
	if !ok {
		return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "postgres port not found in source pooler's port map")
	}

	pm.logger.InfoContext(ctx, "RewindToSource RPC called", "source", source.Id.Name)

	ctx, err := pm.actionLock.Acquire(ctx, "RewindToSource")
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to acquire action lock")
	}
	defer pm.actionLock.Release(ctx)

	// RewindToSource is an explicit "this WAL is suspect, rewind it" request from
	// the caller; raise rewindPending so restartAsStandbyLocked runs the
	// pg_rewind dry-run.
	pm.rewindPending.Store(true)
	rewindPerformed, err := pm.restartAsStandbyLocked(ctx, source.Hostname, port)
	if err != nil {
		return nil, err
	}

	pm.logger.InfoContext(ctx, "RewindToSource completed successfully",
		"rewind_performed", rewindPerformed)
	return &multipoolermanagerdatapb.RewindToSourceResponse{
		Success:         true,
		RewindPerformed: rewindPerformed,
	}, nil
}

// SetPostgresRestartsEnabled enables or disables automatic PostgreSQL restarts by the monitor.
// When disabled, the monitor continues to run and detect problems but will not auto-restart
// a stopped PostgreSQL instance. Used by tests and demos during controlled failovers.
func (pm *MultiPoolerManager) SetPostgresRestartsEnabled(ctx context.Context, req *multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest) (*multipoolermanagerdatapb.SetPostgresRestartsEnabledResponse, error) {
	pm.postgresRestartsDisabled.Store(!req.Enabled)
	pm.logger.InfoContext(ctx, "SetPostgresRestartsEnabled RPC called", "enabled", req.Enabled)
	return &multipoolermanagerdatapb.SetPostgresRestartsEnabledResponse{}, nil
}

// ====================================================================================
// Helper methods for stale-primary demotion (used by SetTermPrimary)
// ====================================================================================

// pgctldStopWithEscalation walks pgctldStopModes calling pgctld.Stop, returning
// nil as soon as a mode succeeds or postgres is already stopped, or the last
// error if every mode fails. Caller is responsible for any Pause()/resume()
// or other lifecycle bookkeeping; this function only drives pgctld.
func (pm *MultiPoolerManager) pgctldStopWithEscalation(ctx context.Context) error {
	if pm.pgctldClient == nil {
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "pgctld client not initialized")
	}

	var lastErr error
	for _, m := range pgctldStopModes {
		stepCtx, cancel := context.WithTimeout(ctx, m.timeout)
		_, err := pm.pgctldClient.Stop(stepCtx, &pgctldpb.StopRequest{
			Mode:    m.name,
			Timeout: durationpb.New(m.timeout),
		})
		cancel()
		if err == nil {
			pm.logger.InfoContext(ctx, "pgctld.Stop succeeded", "mode", m.name)
			return nil
		}
		// Treat "already stopped" as success: handles races where postgres
		// stopped between our check and the stop call, and earlier-mode
		// escalation that already brought postgres down.
		errMsg := err.Error()
		if strings.Contains(errMsg, "not running") ||
			strings.Contains(errMsg, "no child processes") ||
			strings.Contains(errMsg, "no such process") {
			pm.logger.InfoContext(ctx, "Postgres already stopped, continuing",
				"mode", m.name, "error", errMsg)
			return nil
		}
		lastErr = err
		pm.logger.WarnContext(ctx, "pgctld.Stop failed; escalating",
			"mode", m.name, "timeout", m.timeout, "error", err)
	}
	return mterrors.Wrap(lastErr, "failed to stop postgres after fast/immediate escalation")
}

// restartAsStandbyLocked is the shared core of RewindToSource and the
// stale-primary branch of SetTermPrimary: it pauses the manager, stops
// postgres, runs pg_rewind against source iff rewindPending is set
// (patching pgbackrest paths in postgresql.auto.conf after the rewind
// copies them from source), then restarts postgres as standby and
// resumes the manager.
//
// Gating on rewindPending: callers raise the flag when this node's WAL may
// have diverged from the cluster's chosen history (emergencyDemoteLocked
// sets it after an emergency demote; SetTermPrimary's stale-primary branch
// and RewindToSource set it before calling here). When the flag is clear we
// skip even the pg_rewind dry-run — the WAL is trusted and we just need to
// come back as a standby. The flag is cleared as soon as pg_rewind returns
// success; pg_rewind is idempotent on an already-rewound target, so a
// failure later in this function or a caller retry is safe — the next
// invocation will simply skip the rewind step.
//
// The manager is guaranteed to be resumed before this function returns, even
// on error paths — that's the whole reason for the Pause/defer-resume
// envelope.
//
// Caller must hold the action lock.
func (pm *MultiPoolerManager) restartAsStandbyLocked(
	ctx context.Context,
	sourceHost string,
	sourcePort int32,
) (rewindPerformed bool, err error) {
	if err := AssertActionLockHeld(ctx); err != nil {
		return false, err
	}
	if pm.pgctldClient == nil {
		return false, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "pgctld client not initialized")
	}

	wantRewind := pm.rewindPending.Load()
	pm.logger.InfoContext(ctx, "Pausing manager and stopping PostgreSQL to restart as standby",
		"source_host", sourceHost, "source_port", sourcePort, "rewind_pending", wantRewind)
	resume := pm.Pause(ctx)
	defer resume(ctx) // safety net; explicit resume() below after restart succeeds

	if err := pm.pgctldStopWithEscalation(ctx); err != nil {
		return false, mterrors.Wrap(err, "stop postgres")
	}

	if wantRewind {
		rewindPerformed, err = pm.runPgRewind(ctx, sourceHost, sourcePort)
		if err != nil {
			return false, mterrors.Wrap(err, "pg_rewind")
		}
		// pg_rewind is idempotent on an already-rewound target (a re-run's
		// dry-run detects no divergence and skips), so clearing as soon as
		// pg_rewind returns is safe even if the restart or reconnect below
		// fails: the next attempt will skip pg_rewind and just restart.
		pm.rewindPending.Store(false)
		// pg_rewind copies postgresql.auto.conf from source, baking source's
		// own pooler paths into pgbackrest commands (restore_command,
		// archive_command). Patch them back to this pooler's paths before
		// restart so postgres reads the corrected file. Best-effort: log and
		// continue on error rather than abort the demote.
		if err := pm.fixPgBackRestPaths(ctx); err != nil {
			pm.logger.WarnContext(ctx, "Failed to fix pgbackrest paths after pg_rewind, continuing anyway", "error", err)
		}
	}

	// Restart as standby. pgctld writes standby.signal before launching; this is
	// redundant on the divergence path (runPgRewind passes -R, which already
	// wrote it) but necessary on the no-divergence and no-rewind paths where
	// postgres was just stopped and needs to come back as a standby.
	if _, err := pm.pgctldClient.Restart(ctx, &pgctldpb.RestartRequest{
		Mode:      "fast",
		AsStandby: true,
	}); err != nil {
		return false, mterrors.Wrap(err, "restart postgres as standby")
	}

	// Resume the manager now that postgres is back up. The deferred resume()
	// remains in place as a safety net for the path below.
	resume(ctx)

	if err := pm.waitForDatabaseConnection(ctx); err != nil {
		return false, mterrors.Wrap(err, "wait for database after restart as standby")
	}

	// Sanity check: postgres must come back in recovery mode.
	inRecovery, err := pm.isInRecovery(ctx)
	if err != nil {
		return false, mterrors.Wrap(err, "verify standby status after restart")
	}
	if !inRecovery {
		return false, mterrors.New(mtrpcpb.Code_INTERNAL, "server not in recovery mode after restart as standby")
	}

	// If pg_rewind ran, postgresql.auto.conf was overwritten from source (a
	// primary, which has no primary_conninfo), so the WAL receiver has no
	// target. Restore primary_conninfo before returning so the caller's
	// "restart as standby of source" expectation actually holds. Discovered in
	// the mg-scale12 AZ-outage test (2026-05-29).
	if rewindPerformed {
		if err := pm.setPrimaryConnInfoLocked(ctx, sourceHost, sourcePort, false, false); err != nil {
			return false, mterrors.Wrap(err, "restore primary_conninfo after pg_rewind")
		}
	}

	return rewindPerformed, nil
}

// runPgRewind runs pg_rewind to sync with source.
// Returns true if rewind was performed, false if not needed.
func (pm *MultiPoolerManager) runPgRewind(ctx context.Context, sourceHost string, sourcePort int32) (bool, error) {
	if pm.pgctldClient == nil {
		return false, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "pgctld client not initialized")
	}

	// Get application name for replication connection
	pid := pm.servicePoolerID

	pm.logger.InfoContext(ctx, "Running pg_rewind dry-run (may do crash recovery)",
		"source_host", sourceHost, "source_port", sourcePort)

	// Dry-run to check if rewind is needed
	dryRunReq := &pgctldpb.PgRewindRequest{
		SourceHost:      sourceHost,
		SourcePort:      sourcePort,
		DryRun:          true,
		ApplicationName: pid.appName,
	}
	dryRunResp, err := pm.pgctldClient.PgRewind(ctx, dryRunReq)
	if err != nil {
		if dryRunResp != nil {
			pm.logger.ErrorContext(ctx, "pg_rewind dry-run failed", "error", err, "output", dryRunResp.Output)
		}
		return false, mterrors.Wrap(err, "pg_rewind dry-run failed")
	}

	// Check if servers diverged
	if dryRunResp.Output != "" && strings.Contains(dryRunResp.Output, "servers diverged at") {
		pm.logger.InfoContext(ctx, "Servers diverged, running pg_rewind with -R flag")

		rewindReq := &pgctldpb.PgRewindRequest{
			SourceHost:      sourceHost,
			SourcePort:      sourcePort,
			DryRun:          false,
			ApplicationName: pid.appName,
			ExtraArgs:       []string{"-R"},
		}
		rewindResp, err := pm.pgctldClient.PgRewind(ctx, rewindReq)
		if err != nil {
			if rewindResp != nil {
				pm.logger.ErrorContext(ctx, "pg_rewind failed", "error", err, "output", rewindResp.Output)
			}
			return false, mterrors.Wrap(err, "pg_rewind failed")
		}

		pm.logger.InfoContext(ctx, "pg_rewind completed")
		return true, nil
	}

	pm.logger.InfoContext(ctx, "No divergence, skipping rewind")
	return false, nil
}

// fixPgBackRestPaths fixes the pgbackrest paths in postgresql.auto.conf
// After pg_rewind, the restore_command and archive_command may have paths from another pooler
// This function updates them to point to the current pooler's directories
func (pm *MultiPoolerManager) fixPgBackRestPaths(ctx context.Context) error {
	pm.mu.Lock()
	poolerDir := pm.record.PoolerDir()
	pm.mu.Unlock()

	if poolerDir == "" {
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "pooler directory not set")
	}

	autoConfPath := filepath.Join(postgresDataDir(), "postgresql.auto.conf")

	pm.logger.InfoContext(ctx, "Fixing pgbackrest paths in postgresql.auto.conf", "file", autoConfPath)

	// Read the file
	content, err := os.ReadFile(autoConfPath)
	if err != nil {
		return mterrors.Wrap(err, "failed to read postgresql.auto.conf")
	}

	// Replace all occurrences of old pooler paths with current pooler paths
	// We need to fix: --config, --lock-path, --log-path, --pg1-path
	// These paths follow the pattern: /some/path/pooler-X/data/...
	// We want to replace them with: /some/path/pooler-current/data/...

	// Extract current pooler dir path pattern
	// poolerDir is like: /tmp/test_12345/pooler-1/data
	// We want to match patterns like: /tmp/test_12345/pooler-X/data
	baseDir := filepath.Dir(filepath.Dir(poolerDir)) // Go up two levels to get base directory

	// Use regex to replace pooler-X paths with current pooler paths
	// Pattern matches: /path/to/pooler-<anything>/data
	re := regexp.MustCompile(regexp.QuoteMeta(baseDir) + `/pooler-[^/]+/data`)
	newContent := re.ReplaceAllString(string(content), poolerDir)

	// Write the file back
	if err := os.WriteFile(autoConfPath, []byte(newContent), 0o600); err != nil {
		return mterrors.Wrap(err, "failed to write postgresql.auto.conf")
	}

	pm.logger.InfoContext(ctx, "Successfully fixed pgbackrest paths in postgresql.auto.conf")
	return nil
}
