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
	"github.com/multigres/multigres/go/common/eventlog"
	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"

	"github.com/multigres/multigres/go/services/multipooler/poolerserver"
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

// SetPrimaryConnInfo sets the primary connection info for a standby server
func (pm *MultiPoolerManager) SetPrimaryConnInfo(ctx context.Context, primary *clustermetadatapb.MultiPooler, stopReplicationBefore, startReplicationAfter bool, currentTerm int64, force bool) error {
	if err := pm.checkReady(); err != nil {
		return err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	ctx, err := pm.actionLock.Acquire(ctx, "SetPrimaryConnInfo")
	if err != nil {
		return err
	}
	defer pm.actionLock.Release(ctx)

	// Validate and update consensus term following consensus rules
	if err = pm.validateAndUpdateTerm(ctx, currentTerm, force); err != nil {
		return err
	}

	// Extract host and port from the MultiPooler (nil means clear the config)
	var host string
	var port int32
	if primary != nil {
		host = primary.Hostname
		var ok bool
		port, ok = primary.PortMap["postgres"]
		if !ok {
			return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
				"primary %s has no postgres port configured", primary.Id.Name)
		}
	}

	// Store primary pooler ID (nil if clearing)
	pm.mu.Lock()
	if primary != nil {
		pm.primaryPoolerID = primary.Id
		pm.primaryHost = host
		pm.primaryPort = port
	} else {
		pm.primaryPoolerID = nil
		pm.primaryHost = ""
		pm.primaryPort = 0
	}
	pm.mu.Unlock()

	// Call the locked version that assumes action lock is already held
	if err := pm.setPrimaryConnInfoLocked(ctx, host, port, stopReplicationBefore, startReplicationAfter); err != nil {
		return err
	}

	// Push an immediate health snapshot so orchestrators learn about the new
	// replication configuration (e.g., cleared primary_conninfo) without waiting
	// for the next 30-second heartbeat.
	pm.broadcastHealth()
	return nil
}

// setPrimaryConnInfoLocked sets the primary connection info for a standby server.
// This function assumes the action lock is already held by the caller.
func (pm *MultiPoolerManager) setPrimaryConnInfoLocked(ctx context.Context, host string, port int32, stopReplicationBefore, startReplicationAfter bool) error {
	if err := AssertActionLockHeld(ctx); err != nil {
		return err
	}

	if err := pm.checkReady(); err != nil {
		return err
	}

	// Guardrail: Check if the PostgreSQL instance is in recovery (standby mode)
	isPrimary, err := pm.isPrimary(ctx)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to check if instance is in recovery", "error", err)
		return mterrors.Wrap(err, "failed to check recovery status")
	}

	if isPrimary {
		pm.logger.ErrorContext(ctx, "SetPrimaryConnInfo called on non-standby instance", "service_id", pm.serviceID.String())
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
	// Format: host=<host> port=<port> user=<user> application_name=<name>
	// The heartbeat_interval is converted to keepalives_interval/keepalives_idle
	user := constants.DefaultPostgresUser
	if pm.connPoolMgr != nil {
		user = pm.connPoolMgr.PgUser()
	}
	connInfo := fmt.Sprintf("host=%s port=%d user=%s application_name=%s",
		host, port, user, appName.appName)

	// Set primary_conninfo using ALTER SYSTEM
	if err = pm.setPrimaryConnInfo(ctx, connInfo); err != nil {
		return err
	}

	// Reload PostgreSQL configuration to apply changes
	if err = pm.reloadPostgresConfig(ctx); err != nil {
		return err
	}

	// Optionally start replication after making changes.
	// Note: If replication was already running when calling SetPrimaryConnInfo,
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

	pm.logger.InfoContext(ctx, "SetPrimaryConnInfo completed successfully",
		"host", host,
		"port", port,
		"stop_replication_before", stopReplicationBefore,
		"start_replication_after", startReplicationAfter)

	return nil
}

// StartReplication starts WAL replay on standby (calls pg_wal_replay_resume)
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

	// Check REPLICA guardrails (pooler type and recovery mode)
	if err = pm.checkReplicaGuardrails(ctx); err != nil {
		return err
	}

	// Resume WAL replay on the standby
	if err := pm.resumeWALReplay(ctx); err != nil {
		return err
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
func (pm *MultiPoolerManager) Status(ctx context.Context) (*multipoolermanagerdatapb.Status, error) {
	// Build status with initialization fields (always available)
	poolerStatus := &multipoolermanagerdatapb.Status{
		PoolerType:       pm.getPoolerType(),
		IsInitialized:    pm.isInitialized(ctx),
		HasDataDirectory: pm.hasDataDirectory(),
		PostgresReady:    pm.isPostgresReady(ctx),
		PostgresRunning:  pm.isPostgresRunning(ctx),
		PostgresRole:     pm.getRole(ctx),
		ShardId:          pm.getShardID(),
	}

	if action, duration := pm.actionLock.ActiveAction(); action != multipoolermanagerdatapb.PostgresAction_POSTGRES_ACTION_UNSPECIFIED {
		poolerStatus.PostgresAction = action
		poolerStatus.PostgresActionDuration = durationpb.New(duration)
	}

	// Get consensus term (use inconsistent read for monitoring)
	if term, err := pm.consensusState.GetInconsistentTerm(); err == nil {
		poolerStatus.ConsensusTerm = term
	}

	// Get WAL position (ignore errors, just return empty string)
	walPosition, _ := pm.getWALPosition(ctx)
	poolerStatus.WalPosition = walPosition

	// Get cohort members from the current rule (best-effort).
	if pos, err := pm.rules.observePosition(ctx); err != nil {
		pm.logger.WarnContext(ctx, "Failed to read current rule for status", "error", err)
	} else if pos != nil && pos.Rule != nil {
		poolerStatus.CohortMembers = pos.Rule.CohortMembers
	}

	// Try to get detailed status based on PostgreSQL role
	isPrimary, err := pm.isPrimary(ctx)
	if err != nil {
		// Can't determine role - return what we have
		pm.logger.WarnContext(ctx, "Failed to check PostgreSQL role, returning partial status", "error", err)
		return poolerStatus, nil
	}

	// Populate role-specific status
	if isPrimary {
		// Acting as primary - get primary status (skip guardrails since we already checked isPrimary)
		primaryStatus, err := pm.getPrimaryStatusInternal(ctx)
		if err != nil {
			pm.logger.WarnContext(ctx, "Failed to get primary status", "error", err)
			// Return partial status instead of error
			return poolerStatus, nil
		}
		poolerStatus.PrimaryStatus = primaryStatus
		return poolerStatus, nil
	}
	// Acting as standby - get replication status (skip guardrails since we already checked isPrimary)
	replStatus, err := pm.getStandbyStatusInternal(ctx)
	if err != nil {
		pm.logger.WarnContext(ctx, "Failed to get standby replication status", "error", err)
		// Return partial status instead of error
		return poolerStatus, nil
	}
	poolerStatus.ReplicationStatus = replStatus
	return poolerStatus, nil
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

// ConfigureSynchronousReplication configures PostgreSQL synchronous replication settings
func (pm *MultiPoolerManager) ConfigureSynchronousReplication(ctx context.Context, synchronousCommit multipoolermanagerdatapb.SynchronousCommitLevel, synchronousMethod multipoolermanagerdatapb.SynchronousMethod, numSync int32, standbyIDs []*clustermetadatapb.ID, reloadConfig bool, force bool) error {
	if err := pm.checkReady(); err != nil {
		return err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	ctx, err := pm.actionLock.Acquire(ctx, "ConfigureSynchronousReplication")
	if err != nil {
		return err
	}
	defer pm.actionLock.Release(ctx)

	return pm.configureSynchronousReplicationLocked(ctx, synchronousCommit, synchronousMethod, numSync, standbyIDs, reloadConfig, force)
}

// configureSynchronousReplicationLocked configures PostgreSQL synchronous replication settings.
// The caller MUST already hold the action lock.
func (pm *MultiPoolerManager) configureSynchronousReplicationLocked(ctx context.Context, synchronousCommit multipoolermanagerdatapb.SynchronousCommitLevel, synchronousMethod multipoolermanagerdatapb.SynchronousMethod, numSync int32, standbyIDs []*clustermetadatapb.ID, reloadConfig bool, force bool) error {
	// Validate input parameters
	standbyNames, err := validateSyncReplicationParams(numSync, standbyIDs)
	if err != nil {
		return err
	}

	// Check PRIMARY guardrails (pooler type and non-recovery mode)
	if err := pm.checkPrimaryGuardrails(ctx); err != nil {
		return err
	}

	// Insert history before applying GUCs.
	// Rationale: we want to ensure that a new cohort is advertised
	// before this primary can accept ACKs from it.
	// This is for safe replica joining of the cluster.
	// It will ensure multiorch can discover the new cohort during a failure.
	term, err := pm.consensusState.GetTerm(ctx)
	if err != nil {
		return mterrors.Wrap(err, "failed to get consensus term")
	}
	update := newRuleUpdate(
		term.GetTermNumber(),
		pm.serviceID,
		"replication_config",
		"ConfigureSynchronousReplication called",
		time.Now()).
		withCohort(standbyIDs).
		withOperation("configure")
	if force {
		update.withForce()
	}
	if _, err := pm.rules.updateRule(ctx, update); err != nil {
		return mterrors.Wrap(err, "failed to record replication config history")
	}

	// Set synchronous_commit level
	if err := pm.setSynchronousCommit(ctx, synchronousCommit); err != nil {
		return err
	}

	// Build and set synchronous_standby_names
	if err := pm.setSynchronousStandbyNames(ctx, synchronousMethod, numSync, standbyNames); err != nil {
		return err
	}

	// Reload configuration if requested
	if reloadConfig {
		if err := pm.reloadPostgresConfig(ctx); err != nil {
			return err
		}
	}

	pm.logger.InfoContext(ctx, "ConfigureSynchronousReplication completed successfully",
		"synchronous_commit", synchronousCommit,
		"synchronous_method", synchronousMethod,
		"num_sync", numSync,
		"standby_ids", standbyIDs,
		"reload_config", reloadConfig)

	return nil
}

// UpdateSynchronousStandbyList updates PostgreSQL synchronous_standby_names by adding,
// removing, or replacing members. It is idempotent and only valid when synchronous
// replication is already configured.
func (pm *MultiPoolerManager) UpdateSynchronousStandbyList(ctx context.Context, operation multipoolermanagerdatapb.StandbyUpdateOperation, standbyIDs []*clustermetadatapb.ID, reloadConfig bool, consensusTerm int64, force bool, coordinatorID *clustermetadatapb.ID) error {
	if err := pm.checkReady(); err != nil {
		return err
	}

	// Validate operation
	if operation == multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_UNSPECIFIED {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "operation must be specified")
	}

	// Validate standby IDs using the shared validation function
	requestedApplicationNames, err := validateStandbyIDs(standbyIDs)
	if err != nil {
		return err
	}

	// Pre-compute history fields before acquiring the lock.
	leaderID := pm.servicePoolerID

	ctx, err = pm.actionLock.Acquire(ctx, "UpdateSynchronousStandbyList")
	if err != nil {
		return err
	}
	defer pm.actionLock.Release(ctx)

	// === Validation ===
	// TODO: We need to validate consensus term here.
	// We should check if the request is a valid term.
	// If it's a newer term and probably we need to demote
	// ourself. But details yet to be implemented

	// Check PRIMARY guardrails (pooler type and non-recovery mode)
	if err = pm.checkPrimaryGuardrails(ctx); err != nil {
		return err
	}

	// === Parse Current Configuration ===

	// Get current synchronous replication configuration
	syncConfig, err := pm.getSynchronousReplicationConfig(ctx)
	if err != nil {
		return err
	}

	// Check if synchronous replication is configured
	if len(syncConfig.StandbyIds) == 0 {
		pm.logger.ErrorContext(ctx, "UpdateSynchronousStandbyList requires synchronous replication to be configured")
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			"synchronous replication is not configured - use ConfigureSynchronousReplication first")
	}

	// Convert current config IDs to application names for set operations.
	// These IDs were previously validated and written by us, so this cannot fail in practice.
	currentApplicationNames, err := toPoolerIDs(syncConfig.StandbyIds)
	if err != nil {
		return err
	}

	// Build the current value string for comparison
	currentValue, err := buildSynchronousStandbyNamesValue(syncConfig.SynchronousMethod, syncConfig.NumSync, currentApplicationNames)
	if err != nil {
		return err
	}

	// === Apply Operation ===

	var updatedStandbys []poolerID
	switch operation {
	case multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_ADD:
		updatedStandbys = applyAddOperation(currentApplicationNames, requestedApplicationNames)

	case multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_REMOVE:
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

	// === Build and Apply New Configuration ===

	// Build new synchronous_standby_names value using shared helper
	newValue, err := buildSynchronousStandbyNamesValue(syncConfig.SynchronousMethod, syncConfig.NumSync, updatedStandbys)
	if err != nil {
		return err
	}

	// Check if there are any changes (idempotent)
	if currentValue == newValue {
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
	standbyUpdate := newRuleUpdate(
		consensusTerm,
		coordID,
		"replication_config",
		"UpdateSynchronousStandbyList: "+operationName,
		time.Now()).
		withLeader(leaderID.id).
		withCohort(updatedStandbyIDs).
		withOperation(operationName)
	if force {
		standbyUpdate.withForce()
	}
	if _, err := pm.rules.updateRule(ctx, standbyUpdate); err != nil {
		return mterrors.Wrap(err, "failed to record replication config history")
	}

	// Apply the setting
	if err = pm.applySynchronousStandbyNames(ctx, newValue); err != nil {
		return err
	}

	// Reload configuration if requested
	if reloadConfig {
		if err := pm.reloadPostgresConfig(ctx); err != nil {
			return err
		}
	}

	pm.logger.InfoContext(ctx, "UpdateSynchronousStandbyList completed successfully",
		"operation", operation,
		"old_value", currentValue,
		"new_value", newValue,
		"reload_config", reloadConfig,
		"consensus_term", consensusTerm,
		"force", force)

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

	// Use the serving state manager to transition components and update the multipooler record.
	// The serving status stays SERVING during type changes (the node remains available).
	if err := pm.servingState.SetState(ctx, poolerType, clustermetadatapb.PoolerServingStatus_SERVING); err != nil {
		return mterrors.Wrap(err, "failed to set serving state")
	}

	// Notify the topology publisher of the new state. The write to etcd happens
	// asynchronously so that a temporarily unreachable etcd does not block type changes.
	if err := pm.topoPublisher.Notify(ctx, pm.multipooler); err != nil {
		pm.logger.ErrorContext(ctx, "topoPublisher.Notify called without action lock", "error", err)
	}

	pm.logger.InfoContext(ctx, "Pooler type updated successfully", "new_type", poolerType.String(), "service_id", pm.serviceID.String())
	return nil
}

// ChangeType changes the pooler type (PRIMARY/REPLICA)
func (pm *MultiPoolerManager) ChangeType(ctx context.Context, poolerType string) error {
	if err := pm.checkReady(); err != nil {
		return err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	ctx, err := pm.actionLock.Acquire(ctx, "ChangeType")
	if err != nil {
		return err
	}
	defer pm.actionLock.Release(ctx)

	// Validate pooler type
	var newType clustermetadatapb.PoolerType
	// TODO: For now allow to change type to PRIMARY, this is to make it easier
	// to perform tests while we are still developing HA. Once, we have multiorch
	// fully implemented, we shouldn't allow to change the type to Primary.
	// This would happen organically as part of Promote workflow.
	switch poolerType {
	case "PRIMARY":
		newType = clustermetadatapb.PoolerType_PRIMARY
	case "REPLICA":
		newType = clustermetadatapb.PoolerType_REPLICA
	default:
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			fmt.Sprintf("invalid pooler type: %s, must be PRIMARY or REPLICA", poolerType))
	}

	// Call the locked version
	return pm.changeTypeLocked(ctx, newType)
}

// EmergencyDemote demotes the current primary server
// This can be called for any of the following use cases:
// - By orchestrator when fixing a broken shard.
// - When performing a Planned demotion.
// - When receiving a SIGTERM and the pooler needs to shutdown.
func (pm *MultiPoolerManager) EmergencyDemote(ctx context.Context, consensusTerm int64, drainTimeout time.Duration, force bool) (_ *multipoolermanagerdatapb.EmergencyDemoteResponse, retErr error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	ctx, err := pm.actionLock.Acquire(ctx, "Demote")
	if err != nil {
		return nil, err
	}
	defer pm.actionLock.Release(ctx)

	// Validate the term but DON'T update yet. We only update the term AFTER
	// successful demotion to avoid a race where a failed demote (e.g., postgres
	// not ready) updates the term, causing subsequent detection to see equal
	// terms and skip demotion.
	if err := pm.validateTerm(ctx, consensusTerm, force); err != nil {
		return nil, err
	}

	nodeName := pm.serviceID.GetName()
	eventlog.Emit(ctx, pm.logger, eventlog.Started, eventlog.PrimaryDemotion{NodeName: nodeName, Reason: "emergency"})
	defer func() {
		if retErr == nil {
			eventlog.Emit(ctx, pm.logger, eventlog.Success, eventlog.PrimaryDemotion{NodeName: nodeName, Reason: "emergency"})
		} else {
			eventlog.Emit(ctx, pm.logger, eventlog.Failed, eventlog.PrimaryDemotion{NodeName: nodeName, Reason: "emergency"}, "error", retErr)
		}
	}()

	// Perform the actual demotion
	resp, err := pm.emergencyDemoteLocked(ctx, consensusTerm, drainTimeout)
	if err != nil {
		return nil, err
	}

	// Only update term AFTER successful demotion
	// This ensures the stale primary keeps its lower term until it's actually demoted,
	// allowing subsequent detection to continue flagging it as stale.
	if err := pm.updateTermIfNewer(ctx, consensusTerm); err != nil {
		// Log but don't fail - demotion succeeded, term update is secondary
		pm.logger.WarnContext(ctx, "Failed to update term after demotion",
			"error", err,
			"consensus_term", consensusTerm)
	}

	return resp, nil
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
func (pm *MultiPoolerManager) emergencyDemoteLocked(ctx context.Context, consensusTerm int64, drainTimeout time.Duration) (*multipoolermanagerdatapb.EmergencyDemoteResponse, error) {
	// Verify action lock is held
	if err := AssertActionLockHeld(ctx); err != nil {
		return nil, err
	}

	// === Validation & State Check ===

	// Guard rail: Demote can only be called on a PRIMARY
	if err := pm.checkPrimaryGuardrails(ctx); err != nil {
		return nil, err
	}

	// Check current demotion state
	state, err := pm.checkDemotionState(ctx)
	if err != nil {
		return nil, err
	}

	// If everything is already complete, return early (fully idempotent)
	if state.isNotServing && state.isReplicaInTopology && state.isReadOnly {
		return &multipoolermanagerdatapb.EmergencyDemoteResponse{
			WasAlreadyDemoted:     true,
			ConsensusTerm:         consensusTerm,
			LsnPosition:           state.finalLSN,
			ConnectionsTerminated: 0,
		}, nil
	}

	// Transition to NOT_SERVING — rejects all queries and stops heartbeat.
	// This ensures no new writes arrive while we drain existing connections.
	if err := pm.setNotServing(ctx, state); err != nil {
		return nil, err
	}

	// Drain write connections

	if err := pm.drainWriteActivity(ctx, drainTimeout); err != nil {
		return nil, err
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
		return nil, err
	}

	// Signal voluntary resignation so the coordinator can trigger an immediate
	// election without waiting for a heartbeat timeout. Use this node's own
	// primary_term (not the incoming consensusTerm) so the coordinator can
	// correlate the signal with the term at which this node was elected.
	if primaryTerm, err := pm.primaryTermLocked(ctx); err == nil && primaryTerm != 0 {
		if err := pm.setResignedPrimaryAtTerm(ctx, primaryTerm); err != nil {
			return nil, mterrors.Wrap(err, "failed to set resigned primary term")
		}
	}

	// Restart PostgreSQL as standby. Unlike the old stop-only path, this keeps
	// the node in the cluster as a replication target, avoiding timeline divergence
	// in most cases. The coordinator still uses pg_rewind for nodes that diverged.
	if err := pm.restartPostgresAsStandby(ctx, state); err != nil {
		return nil, err
	}

	pm.healthStreamer.UpdatePrimaryObservation(nil)

	// Suppress the postgres monitor until a rewind completes; the monitor would
	// otherwise restart postgres on this demoted node.
	pm.rewindPending.Store(true)

	pm.logger.InfoContext(ctx, "Demote completed successfully",
		"final_lsn", finalLSN,
		"consensus_term", consensusTerm,
		"connections_terminated", connectionsTerminated)

	return &multipoolermanagerdatapb.EmergencyDemoteResponse{
		WasAlreadyDemoted:     false,
		ConsensusTerm:         consensusTerm,
		LsnPosition:           finalLSN,
		ConnectionsTerminated: connectionsTerminated,
	}, nil
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

// DemoteStalePrimary demotes a stale primary that came back online after failover.
// This is a complete operation that:
// 1. Stops postgres if running
// 2. Runs pg_rewind to sync with the correct primary
// 3. Clears sync replication config
// 4. Restarts as standby
// 5. Updates topology to REPLICA
func (pm *MultiPoolerManager) DemoteStalePrimary(
	ctx context.Context,
	source *clustermetadatapb.MultiPooler,
	consensusTerm int64,
	force bool,
) (*multipoolermanagerdatapb.DemoteStalePrimaryResponse, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}

	// Validate source pooler
	if source == nil || source.PortMap == nil {
		return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "source pooler or port_map is nil")
	}
	if source.Hostname == "" {
		return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "source hostname is required")
	}

	pm.logger.InfoContext(ctx, "DemoteStalePrimary RPC called",
		"source", source.Id.Name,
		"consensus_term", consensusTerm)

	// Acquire the action lock to ensure only one mutation runs at a time
	ctx, err := pm.actionLock.Acquire(ctx, "DemoteStalePrimary")
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to acquire action lock")
	}
	defer pm.actionLock.Release(ctx)

	// Check if already demoted by consulting the current rule. Only short-circuit
	// when we can positively confirm this node is no longer the primary (err ==
	// nil AND term == 0). On any error — e.g. postgres is down after a crash —
	// we fall through and run the idempotent demotion flow, because we cannot
	// tell the difference between "already demoted" and "primary with unreachable
	// postgres" from local state alone.
	if primaryTerm, err := pm.primaryTermLocked(ctx); err == nil && primaryTerm == 0 {
		pm.logger.InfoContext(ctx, "Pooler already demoted, skipping DemoteStalePrimary")
		// Return success with rewind_performed=false since node is already in correct state
		finalLSN := ""
		if lsn, err := pm.getStandbyReplayLSN(ctx); err == nil {
			finalLSN = lsn
		}
		return &multipoolermanagerdatapb.DemoteStalePrimaryResponse{
			RewindPerformed: false,
			LsnPosition:     finalLSN,
		}, nil
	}

	// Validate the term
	if err := pm.validateTerm(ctx, consensusTerm, force); err != nil {
		return nil, err
	}

	if err := pm.stopPostgresIfRunning(ctx); err != nil {
		return nil, mterrors.Wrap(err, "failed to stop postgres")
	}

	port, ok := source.PortMap["postgres"]
	if !ok {
		return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "postgres port not found in source pooler's port map")
	}

	rewindPerformed, err := pm.runPgRewind(ctx, source.Hostname, port)
	if err != nil {
		return nil, mterrors.Wrap(err, "pg_rewind failed")
	}

	// Fix pgbackrest paths in postgresql.auto.conf after pg_rewind
	// The config may have wrong paths copied from another pooler during initial setup
	if err := pm.fixPgBackRestPaths(ctx); err != nil {
		pm.logger.WarnContext(ctx, "Failed to fix pgbackrest paths, continuing anyway", "error", err)
	}

	if err := pm.restartAsStandbyAfterRewind(ctx); err != nil {
		return nil, mterrors.Wrap(err, "failed to restart as standby")
	}

	if err := pm.resetSynchronousReplication(ctx); err != nil {
		pm.logger.WarnContext(ctx, "Failed to reset synchronous replication", "error", err)
	}

	pm.logger.InfoContext(ctx, "Configuring replication to source primary",
		"source", source.Id.Name,
		"source_host", source.Hostname,
		"source_port", port)

	// Store primary pooler ID for tracking
	pm.mu.Lock()
	pm.primaryPoolerID = source.Id
	pm.primaryHost = source.Hostname
	pm.primaryPort = port
	pm.mu.Unlock()

	// Call the locked version directly since we already hold the action lock
	// (calling SetPrimaryConnInfo would deadlock trying to acquire the same lock)
	if err := pm.setPrimaryConnInfoLocked(ctx, source.Hostname, port, false, false); err != nil {
		return nil, mterrors.Wrap(err, "failed to configure replication to source primary")
	}

	// Report the new primary (source) so the gateway can use this observation.
	pm.healthStreamer.UpdatePrimaryObservation(&poolerserver.PrimaryObservation{
		PrimaryID:   source.Id,
		PrimaryTerm: consensusTerm,
	})

	// Update consensus term to match the correct primary's term after successful demotion
	if err := pm.updateTermIfNewer(ctx, consensusTerm); err != nil {
		return nil, mterrors.Wrap(err, "failed to update consensus term")
	}

	// Get final LSN
	finalLSN := ""
	if lsn, err := pm.getStandbyReplayLSN(ctx); err == nil {
		finalLSN = lsn
	}

	// Update topology to REPLICA
	if err := pm.changeTypeLocked(ctx, clustermetadatapb.PoolerType_REPLICA); err != nil {
		return nil, mterrors.Wrap(err, "failed to update topology")
	}

	pm.logger.InfoContext(ctx, "DemoteStalePrimary completed successfully",
		"rewind_performed", rewindPerformed,
		"lsn_position", finalLSN)

	return &multipoolermanagerdatapb.DemoteStalePrimaryResponse{
		Success:         true,
		RewindPerformed: rewindPerformed,
		LsnPosition:     finalLSN,
	}, nil
}

// Promote promotes a standby to primary
// This is called during the Propagate stage of generalized consensus to safely
// transition a standby to primary and reconfigure replication.
// This operation is fully idempotent - it checks what steps are already complete
// and only executes the missing steps.
func (pm *MultiPoolerManager) Promote(ctx context.Context, consensusTerm int64, expectedLSN string, syncReplicationConfig *multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest, force bool, reason string, coordinatorID *clustermetadatapb.ID, cohortMemberIDs, acceptedMemberIDs []*clustermetadatapb.ID) (*multipoolermanagerdatapb.PromoteResponse, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	ctx, err := pm.actionLock.Acquire(ctx, "Promote")
	if err != nil {
		return nil, err
	}
	defer pm.actionLock.Release(ctx)

	// Validation & Readiness

	// Validate term - strict equality, no automatic updates
	if err := pm.validateTermExactMatch(ctx, consensusTerm, force); err != nil {
		return nil, err
	}

	// Check current promotion state to determine what needs to be done
	state, err := pm.checkPromotionState(ctx, syncReplicationConfig)
	if err != nil {
		return nil, err
	}

	// Guard rail: Check topology type and validate state consistency
	// If topology is PRIMARY, verify everything is in expected state (idempotency check)
	// If topology is REPLICA, proceed with promotion
	if state.isPrimaryInTopology {
		// Topology shows PRIMARY - validate that everything is consistent
		pm.logger.InfoContext(ctx, "Promote called but topology already shows PRIMARY - validating state consistency")

		// Check if everything is in expected state
		if state.isPrimaryInPostgres && state.syncReplicationMatches {
			// Everything is consistent and complete - idempotent success
			pm.logger.InfoContext(ctx, "Promotion already complete and consistent (idempotent)",
				"lsn", state.currentLSN)
			return &multipoolermanagerdatapb.PromoteResponse{
				LsnPosition:       state.currentLSN,
				WasAlreadyPrimary: true,
				ConsensusTerm:     consensusTerm,
			}, nil
		}

		// Inconsistent state detected
		pm.logger.ErrorContext(ctx, "Inconsistent state detected - topology is PRIMARY but state is incomplete",
			"is_primary_in_postgres", state.isPrimaryInPostgres,
			"sync_replication_matches", state.syncReplicationMatches,
			"force", force)

		if !force {
			// Without force flag, require manual intervention
			return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
				fmt.Sprintf("inconsistent state: topology is PRIMARY but PostgreSQL state doesn't match (pg_primary=%v, sync_matches=%v). Manual intervention required or use force=true.",
					state.isPrimaryInPostgres, state.syncReplicationMatches))
		}
	}

	// If PostgreSQL is not promoted yet, validate expected LSN before promotion
	if !state.isPrimaryInPostgres {
		if err := pm.validateExpectedLSN(ctx, expectedLSN); err != nil {
			return nil, err
		}
	}

	// Execute missing steps

	// Promote PostgreSQL if needed
	if err := pm.promoteStandbyToPrimary(ctx, state); err != nil {
		return nil, err
	}

	// Configure sync replication if needed
	if err := pm.configureReplicationAfterPromotion(ctx, state, syncReplicationConfig); err != nil {
		return nil, err
	}

	// Get final LSN position
	finalLSN, err := pm.getPrimaryLSN(ctx)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to get final LSN", "error", err)
		return nil, err
	}

	// Clear any outstanding resignation signal now that the coordinator has
	// explicitly re-promoted us at a new term. A higher primary_term implicitly
	// invalidates the old signal, but clearing eagerly avoids a window where
	// a stale REQUESTING_DEMOTION is still published in StatusResponse.
	if err := pm.clearResignedPrimaryAtTerm(ctx); err != nil {
		return nil, mterrors.Wrap(err, "failed to clear resigned primary term")
	}

	pm.healthStreamer.UpdatePrimaryObservation(&poolerserver.PrimaryObservation{
		PrimaryID:   pm.serviceID,
		PrimaryTerm: consensusTerm,
	})

	// Write rule history record - this validates that sync replication is working.
	// If this fails (typically due to timeout waiting for standby acknowledgment), we fail
	// the promotion. It's better to have no primary than one that can't satisfy durability.
	if reason == "" {
		reason = "unknown"
	}
	promoteCoordID := coordinatorID
	if promoteCoordID == nil {
		promoteCoordID = pm.serviceID
	}
	promoteUpdate := newRuleUpdate(
		consensusTerm,
		promoteCoordID,
		"promotion",
		reason,
		time.Now()).
		withLeader(pm.serviceID).
		withCohort(cohortMemberIDs).
		withAcceptedMembers(acceptedMemberIDs).
		withWALPosition(finalLSN)
	if force {
		promoteUpdate.withForce()
	}
	if _, err = pm.rules.updateRule(ctx, promoteUpdate); err != nil {
		pm.logger.ErrorContext(ctx, "Failed to write rule history - promotion failed",
			"term", consensusTerm,
			"error", err)
		return nil, mterrors.Wrap(err, "promotion failed: could not write rule history (sync replication may not be functioning)")
	}

	// Update topology and notify all components (best-effort, don't fail promotion)
	if err := pm.updateTopologyAfterPromotion(ctx, state); err != nil {
		pm.logger.WarnContext(ctx, "Failed to update topology after promotion", "error", err)
	}

	pm.logger.InfoContext(ctx, "Promote completed successfully",
		"final_lsn", finalLSN,
		"consensus_term", consensusTerm,
		"was_already_primary", state.isPrimaryInPostgres)

	return &multipoolermanagerdatapb.PromoteResponse{
		LsnPosition:       finalLSN,
		WasAlreadyPrimary: state.isPrimaryInPostgres && state.isPrimaryInTopology && state.syncReplicationMatches,
		ConsensusTerm:     consensusTerm,
	}, nil
}

// RewindToSource performs pg_rewind to synchronize this server with a source.
// This operation:
// 1. Stops PostgreSQL
// 2. Runs pg_rewind --dry-run to check if rewind is needed
// 3. If needed, runs actual pg_rewind
// 4. Starts PostgreSQL
func (pm *MultiPoolerManager) RewindToSource(ctx context.Context, source *clustermetadatapb.MultiPooler) (*multipoolermanagerdatapb.RewindToSourceResponse, error) {
	// Check if multipooler is ready
	if err := pm.checkReady(); err != nil {
		return nil, mterrors.Wrap(err, "multipooler not ready")
	}

	// Validate source pooler
	if source == nil || source.PortMap == nil {
		return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "source pooler or port_map is nil")
	}

	if source.Hostname == "" {
		return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "source hostname is required")
	}

	pm.logger.InfoContext(ctx, "RewindToSource RPC called", "source", source.Id.Name)

	port, ok := source.PortMap["postgres"]
	if !ok {
		return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "postgres port not found in source pooler's port map")
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	ctx, err := pm.actionLock.Acquire(ctx, "RewindToSource")
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to acquire action lock")
	}
	defer pm.actionLock.Release(ctx)

	// Check if pgctld client is available
	if pm.pgctldClient == nil {
		return nil, mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE, "pgctld client not available")
	}

	// Pause manager and stop PostgreSQL for pg_rewind
	// resume() is called explicitly after PostgreSQL restart, and also via defer for cleanup
	pm.logger.InfoContext(ctx, "Pausing manager and stopping PostgreSQL for pg_rewind")
	resume := pm.Pause()
	defer resume() // Safety net: ensure manager is resumed even if errors occur

	stopReq := &pgctldpb.StopRequest{
		Mode: "fast",
	}
	if _, err := pm.pgctldClient.Stop(ctx, stopReq); err != nil {
		pm.logger.ErrorContext(ctx, "Failed to stop PostgreSQL", "error", err)
		return nil, mterrors.Wrap(err, "failed to stop PostgreSQL before pg_rewind")
	}

	// Run pg_rewind --dry-run to check if rewind is needed
	pm.logger.InfoContext(ctx, "Running pg_rewind dry-run to check for divergence",
		"source_host", source.Hostname,
		"source_port", port)
	dryRunReq := &pgctldpb.PgRewindRequest{
		SourceHost: source.Hostname,
		SourcePort: port,
		DryRun:     true,
	}
	dryRunResp, err := pm.pgctldClient.PgRewind(ctx, dryRunReq)
	if err != nil {
		pm.logger.ErrorContext(ctx, "pg_rewind dry-run failed, leaving postgres stopped", "error", err)
		if dryRunResp != nil {
			pm.logger.ErrorContext(ctx, "pg_rewind dry-run output", "output", dryRunResp.Output)
		}
		return nil, mterrors.Wrap(err, "pg_rewind dry-run failed")
	}

	// Check if rewind is needed by parsing output
	rewindPerformed := false
	if dryRunResp.Output != "" && strings.Contains(dryRunResp.Output, "servers diverged at") {
		// Servers have diverged - run actual pg_rewind
		pm.logger.InfoContext(ctx, "Servers diverged, running actual pg_rewind",
			"source_host", source.Hostname,
			"source_port", port)

		rewindReq := &pgctldpb.PgRewindRequest{
			SourceHost: source.Hostname,
			SourcePort: port,
			DryRun:     false,
		}
		rewindResp, err := pm.pgctldClient.PgRewind(ctx, rewindReq)
		if err != nil {
			pm.logger.ErrorContext(ctx, "pg_rewind failed, leaving postgres stopped", "error", err)
			if rewindResp != nil {
				pm.logger.ErrorContext(ctx, "pg_rewind output", "output", rewindResp.Output)
			}
			return nil, mterrors.Wrap(err, "pg_rewind failed")
		}

		pm.logger.InfoContext(ctx, "pg_rewind completed successfully", "message", rewindResp.Message)
		rewindPerformed = true
	} else {
		pm.logger.InfoContext(ctx, "No timeline divergence detected, skipping rewind")
	}

	// Step 4: Start PostgreSQL as standby
	// Use Restart with as_standby=true to create standby.signal and start postgres
	// Note: postgres is already stopped, so the stop phase will be a no-op
	pm.logger.InfoContext(ctx, "Starting PostgreSQL as standby after pg_rewind")
	restartReq := &pgctldpb.RestartRequest{
		Mode:      "fast",
		AsStandby: true,
	}
	if _, err := pm.pgctldClient.Restart(ctx, restartReq); err != nil {
		pm.logger.ErrorContext(ctx, "Failed to restart PostgreSQL as standby", "error", err)
		return nil, mterrors.Wrap(err, "failed to start PostgreSQL as standby after pg_rewind")
	}

	// Resume manager now that PostgreSQL is running
	resume()

	// Wait for database connection
	if err := pm.waitForDatabaseConnection(ctx); err != nil {
		pm.logger.ErrorContext(ctx, "Failed to reconnect to database", "error", err)
		return nil, mterrors.Wrap(err, "failed to reconnect to database after pg_rewind")
	}

	// Rewind succeeded: allow the monitor to resume normal operation.
	pm.rewindPending.Store(false)

	pm.logger.InfoContext(ctx, "RewindToSource completed successfully",
		"rewind_performed", rewindPerformed)
	return &multipoolermanagerdatapb.RewindToSourceResponse{
		Success:         true,
		ErrorMessage:    "",
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
// Helper methods for DemoteStalePrimary
// ====================================================================================

// stopPostgresIfRunning stops postgres if it's currently running.
func (pm *MultiPoolerManager) stopPostgresIfRunning(ctx context.Context) error {
	if pm.pgctldClient == nil {
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "pgctld client not initialized")
	}

	pm.logger.InfoContext(ctx, "Stopping postgres if running")

	// Close ONLY connection pools to release database connections.
	// This allows postgres to stop cleanly without waiting for connections,
	// but keeps the manager operational for subsequent operations.
	pm.mu.Lock()
	pm.closeConnectionsLocked()
	pm.mu.Unlock()

	// Stop postgres (no-op if already stopped)
	stopReq := &pgctldpb.StopRequest{Mode: "fast"}
	if _, err := pm.pgctldClient.Stop(ctx, stopReq); err != nil {
		// Treat "already stopped" errors as success to make this truly idempotent.
		// This handles race conditions where postgres was stopped between our check and stop call.
		errMsg := err.Error()
		if strings.Contains(errMsg, "not running") ||
			strings.Contains(errMsg, "no child processes") ||
			strings.Contains(errMsg, "no such process") {
			pm.logger.InfoContext(ctx, "Postgres already stopped, continuing", "error", errMsg)
			return nil
		}
		return mterrors.Wrap(err, "failed to stop postgres")
	}

	return nil
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
		pm.rewindPending.Store(false)
		return true, nil
	}

	// No divergence: the node is already in sync with the source. The rewind is
	// effectively complete; clear the flag so the monitor resumes.
	pm.rewindPending.Store(false)
	pm.logger.InfoContext(ctx, "No divergence, skipping rewind")
	return false, nil
}

// fixPgBackRestPaths fixes the pgbackrest paths in postgresql.auto.conf
// After pg_rewind, the restore_command and archive_command may have paths from another pooler
// This function updates them to point to the current pooler's directories
func (pm *MultiPoolerManager) fixPgBackRestPaths(ctx context.Context) error {
	pm.mu.Lock()
	poolerDir := pm.multipooler.PoolerDir
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

// restartAsStandbyAfterRewind restarts postgres as standby after rewind.
func (pm *MultiPoolerManager) restartAsStandbyAfterRewind(ctx context.Context) error {
	// Use existing restartPostgresAsStandby with a state that indicates postgres is not running
	state := &demotionState{
		isReadOnly: false, // Postgres was stopped, not in standby mode yet
	}
	return pm.restartPostgresAsStandby(ctx, state)
}
