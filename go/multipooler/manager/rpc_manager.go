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
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

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
func (pm *MultiPoolerManager) SetPrimaryConnInfo(ctx context.Context, host string, port int32, stopReplicationBefore, startReplicationAfter bool, currentTerm int64, force bool) error {
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

	// Call the locked version that assumes action lock is already held
	return pm.setPrimaryConnInfoLocked(ctx, host, port, stopReplicationBefore, startReplicationAfter)
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
	pm.mu.Lock()
	database := pm.multipooler.Database
	pm.mu.Unlock()

	// Generate application name using the shared helper
	appName := generateApplicationName(pm.serviceID)
	connInfo := fmt.Sprintf("host=%s port=%d user=%s application_name=%s",
		host, port, database, appName)

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
		// Reconnect to database after restart
		if err := pm.connectDB(); err != nil {
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
		PostgresRunning:  pm.isPostgresRunning(ctx),
		PostgresRole:     pm.getRole(ctx),
		ShardId:          pm.getShardID(),
	}

	// Get consensus term if available (use inconsistent read for monitoring)
	if pm.consensusState != nil {
		term, err := pm.consensusState.GetInconsistentCurrentTermNumber()
		if err == nil {
			poolerStatus.ConsensusTerm = term
		}
	}

	// If database is not connected, return early with just initialization status
	if pm.db == nil {
		return poolerStatus, nil
	}

	// Get WAL position (ignore errors, just return empty string)
	walPosition, _ := pm.getWALPosition(ctx)
	poolerStatus.WalPosition = walPosition

	// Database is connected - try to get detailed status based on PostgreSQL role
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
func (pm *MultiPoolerManager) ConfigureSynchronousReplication(ctx context.Context, synchronousCommit multipoolermanagerdatapb.SynchronousCommitLevel, synchronousMethod multipoolermanagerdatapb.SynchronousMethod, numSync int32, standbyIDs []*clustermetadatapb.ID, reloadConfig bool) error {
	if err := pm.checkReady(); err != nil {
		return err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	ctx, err := pm.actionLock.Acquire(ctx, "ConfigureSynchronousReplication")
	if err != nil {
		return err
	}
	defer pm.actionLock.Release(ctx)

	// Validate input parameters
	if err = validateSyncReplicationParams(numSync, standbyIDs); err != nil {
		return err
	}

	// Check PRIMARY guardrails (pooler type and non-recovery mode)
	if err = pm.checkPrimaryGuardrails(ctx); err != nil {
		return err
	}

	// Set synchronous_commit level
	if err := pm.setSynchronousCommit(ctx, synchronousCommit); err != nil {
		return err
	}

	// Build and set synchronous_standby_names
	if err := pm.setSynchronousStandbyNames(ctx, synchronousMethod, numSync, standbyIDs); err != nil {
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
func (pm *MultiPoolerManager) UpdateSynchronousStandbyList(ctx context.Context, operation multipoolermanagerdatapb.StandbyUpdateOperation, standbyIDs []*clustermetadatapb.ID, reloadConfig bool, consensusTerm int64, force bool) error {
	if err := pm.checkReady(); err != nil {
		return err
	}

	ctx, err := pm.actionLock.Acquire(ctx, "UpdateSynchronousStandbyList")
	if err != nil {
		return err
	}
	defer pm.actionLock.Release(ctx)

	// === Validation ===
	// TODO: We need to validate consensus term here.
	// We should check if the request is a valid term.
	// If it's a newer term and probably we need to demote
	// ourself. But details yet to be implemented

	// Validate operation
	if operation == multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_UNSPECIFIED {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "operation must be specified")
	}

	// Validate standby IDs using the shared validation function
	if err = validateStandbyIDs(standbyIDs); err != nil {
		return err
	}

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

	// Build the current value string for comparison
	currentValue, err := buildSynchronousStandbyNamesValue(syncConfig.SynchronousMethod, syncConfig.NumSync, syncConfig.StandbyIds)
	if err != nil {
		return err
	}

	// === Apply Operation ===

	// Apply the requested operation using the current standby list
	var updatedStandbys []*clustermetadatapb.ID
	switch operation {
	case multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_ADD:
		updatedStandbys = applyAddOperation(syncConfig.StandbyIds, standbyIDs)

	case multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_REMOVE:
		updatedStandbys = applyRemoveOperation(syncConfig.StandbyIds, standbyIDs)

	case multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_REPLACE:
		updatedStandbys = applyReplaceOperation(standbyIDs)

	default:
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT,
			fmt.Sprintf("unsupported operation: %s", operation.String()))
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

	// Apply the setting using shared helper
	if err = applySynchronousStandbyNames(ctx, pm.db, pm.logger, newValue); err != nil {
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

	// Update the multipooler record in topology
	updatedMultipooler, err := pm.topoClient.UpdateMultiPoolerFields(ctx, pm.serviceID, func(mp *clustermetadatapb.MultiPooler) error {
		mp.Type = poolerType
		return nil
	})
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to update pooler type in topology", "error", err, "service_id", pm.serviceID.String())
		return mterrors.Wrap(err, "failed to update pooler type in topology")
	}

	pm.mu.Lock()
	pm.multipooler.MultiPooler = updatedMultipooler
	pm.updateCachedMultipooler()
	pm.mu.Unlock()

	// Update heartbeat tracker based on new type
	if pm.replTracker != nil {
		if poolerType == clustermetadatapb.PoolerType_PRIMARY {
			pm.logger.InfoContext(ctx, "Starting heartbeat writer for new primary")
			pm.replTracker.MakePrimary()
		} else {
			pm.logger.InfoContext(ctx, "Stopping heartbeat writer for replica")
			pm.replTracker.MakeNonPrimary()
		}
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

// State returns the current manager status and error information
func (pm *MultiPoolerManager) State(ctx context.Context) (*multipoolermanagerdatapb.StateResponse, error) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	state := string(pm.state)
	var errorMessage string
	if pm.stateError != nil {
		errorMessage = pm.stateError.Error()
	}

	return &multipoolermanagerdatapb.StateResponse{
		State:        state,
		ErrorMessage: errorMessage,
	}, nil
}

// GetFollowers gets the list of follower servers with detailed replication status
func (pm *MultiPoolerManager) GetFollowers(ctx context.Context) (*multipoolermanagerdatapb.GetFollowersResponse, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}

	// Check PRIMARY guardrails (only primary can have followers)
	if err := pm.checkPrimaryGuardrails(ctx); err != nil {
		return nil, err
	}

	// Get current synchronous replication configuration
	syncConfig, err := pm.getSynchronousReplicationConfig(ctx)
	if err != nil {
		return nil, err
	}

	// Query pg_stat_replication for all connected followers with full details
	connectedMap, err := pm.queryFollowerReplicationStats(ctx)
	if err != nil {
		return nil, err
	}

	// Build the response with all configured standbys
	followers := make([]*multipoolermanagerdatapb.FollowerInfo, 0, len(syncConfig.StandbyIds))
	for _, standbyID := range syncConfig.StandbyIds {
		appName := generateApplicationName(standbyID)

		followerInfo := &multipoolermanagerdatapb.FollowerInfo{
			FollowerId:      standbyID,
			ApplicationName: appName,
		}

		// Check if this standby is currently connected
		if stats, connected := connectedMap[appName]; connected {
			followerInfo.IsConnected = true
			followerInfo.ReplicationStats = stats
		} else {
			followerInfo.IsConnected = false
			// ReplicationStats remains nil for disconnected followers
		}

		followers = append(followers, followerInfo)
	}

	return &multipoolermanagerdatapb.GetFollowersResponse{
		Followers:  followers,
		SyncConfig: syncConfig,
	}, nil
}

// Demote demotes the current primary server
// This can be called for any of the following use cases:
// - By orchestrator when fixing a broken shard.
// - When performing a Planned demotion.
// - When receiving a SIGTERM and the pooler needs to shutdown.
func (pm *MultiPoolerManager) Demote(ctx context.Context, consensusTerm int64, drainTimeout time.Duration, force bool) (*multipoolermanagerdatapb.DemoteResponse, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	ctx, err := pm.actionLock.Acquire(ctx, "Demote")
	if err != nil {
		return nil, err
	}
	defer pm.actionLock.Release(ctx)
	// Demote is an operational cleanup, not a leadership change.
	// Accept if term >= currentTerm to ensure the request isn't stale.
	// Equal or higher terms are safe.
	// Note: we still update the term, as this may arrive after a leader
	// appointment that this (now old) primary missed due to a network partition.
	if err := pm.validateAndUpdateTerm(ctx, consensusTerm, force); err != nil {
		return nil, err
	}

	return pm.demoteLocked(ctx, consensusTerm, drainTimeout)
}

// demoteLocked performs the core demotion logic.
// REQUIRES: action lock must already be held by the caller.
// This is used by BeginTerm and Demote to demote inline without re-acquiring the lock.
func (pm *MultiPoolerManager) demoteLocked(ctx context.Context, consensusTerm int64, drainTimeout time.Duration) (*multipoolermanagerdatapb.DemoteResponse, error) {
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
	if state.isServingReadOnly && state.isReplicaInTopology && state.isReadOnly {
		return &multipoolermanagerdatapb.DemoteResponse{
			WasAlreadyDemoted:     true,
			ConsensusTerm:         consensusTerm,
			LsnPosition:           state.finalLSN,
			ConnectionsTerminated: 0,
		}, nil
	}

	// Transition to Read-Only Serving
	// For now, this is not that useful as we have to restart
	// the server anyways to make it a standby.
	// However, we are setting the hooks to make sure that
	// we can make the primary readonly first,
	// drain write connections and then transition it
	// as a replica without restarting postgres
	if err := pm.setServingReadOnly(ctx, state); err != nil {
		return nil, err
	}

	// Drain & Checkpoint (Parallel)

	if err := pm.drainAndCheckpoint(ctx, drainTimeout); err != nil {
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

	if err := pm.restartPostgresAsStandby(ctx, state); err != nil {
		return nil, err
	}

	// Reset Synchronous Replication Configuration
	// Now that the server is read-only, it's safe to clear sync replication settings
	// This ensures we don't have a window where writes could be accepted with incorrect replication config
	if err := pm.resetSynchronousReplication(ctx); err != nil {
		// Log but don't fail - this is cleanup
		pm.logger.WarnContext(ctx, "Failed to reset synchronous replication configuration", "error", err)
	}

	// Update Topology

	if err := pm.updateTopologyAfterDemotion(ctx, state); err != nil {
		return nil, err
	}

	pm.logger.InfoContext(ctx, "Demote completed successfully",
		"final_lsn", finalLSN,
		"consensus_term", consensusTerm,
		"connections_terminated", connectionsTerminated)

	return &multipoolermanagerdatapb.DemoteResponse{
		WasAlreadyDemoted:     false,
		ConsensusTerm:         consensusTerm,
		LsnPosition:           finalLSN,
		ConnectionsTerminated: connectionsTerminated,
	}, nil
}

// UndoDemote undoes a demotion by restarting PostgreSQL as a primary.
// This is only safe when no other node has been promoted (timeline unchanged).
//
// Invariants for safe undo (no data loss):
//  1. No other node has been promoted (all standbys in recovery mode)
//  2. No other node accepted writes (follows from #1)
//  3. The old primary's data directory hasn't been modified as a standby
//
// The timeline ID validation ensures these invariants hold:
// - If the timeline changed, a standby was promoted, creating divergent history
// - We abort if the timeline doesn't match the expected value
func (pm *MultiPoolerManager) UndoDemote(ctx context.Context) (*multipoolermanagerdatapb.UndoDemoteResponse, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	ctx, err := pm.actionLock.Acquire(ctx, "UndoDemote")
	if err != nil {
		return nil, err
	}
	defer pm.actionLock.Release(ctx)

	pm.logger.InfoContext(ctx, "UndoDemote called")

	return pm.undoDemoteLocked(ctx)
}

// undoDemoteLocked performs the core undo demotion logic.
// REQUIRES: action lock must already be held by the caller.
func (pm *MultiPoolerManager) undoDemoteLocked(ctx context.Context) (*multipoolermanagerdatapb.UndoDemoteResponse, error) {
	// Verify action lock is held
	if err := AssertActionLockHeld(ctx); err != nil {
		return nil, err
	}

	pm.logger.InfoContext(ctx, "undoDemoteLocked called")

	// Ensure database connection
	if err := pm.connectDB(); err != nil {
		pm.logger.ErrorContext(ctx, "Failed to connect to database", "error", err)
		return nil, mterrors.Wrap(err, "database connection failed")
	}

	// First check if already primary (idempotent case)
	isPrimary, err := pm.isPrimary(ctx)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to check if primary")
	}

	if isPrimary {
		// Already primary - get LSN for response
		lsn, err := pm.getPrimaryLSN(ctx)
		if err != nil {
			return nil, err
		}
		pm.logger.InfoContext(ctx, "Already running as primary, undo demotion not needed (idempotent)")
		return &multipoolermanagerdatapb.UndoDemoteResponse{
			WasAlreadyPrimary: true,
			LsnPosition:       lsn,
		}, nil
	}

	// TODO: Evaluate adding timeline validation as a safety check.
	// Currently skipped because querying timeline on a demoted primary (standby not connected
	// to any upstream) requires reading from pg_controldata via pgctld, which is not yet implemented.
	// Timeline validation would prevent undo if another node was promoted (timeline incremented).

	// === Restart PostgreSQL as primary ===
	// restartPostgresAsPrimary handles Close/Open internally to re-establish database connections

	pm.logger.InfoContext(ctx, "Restarting PostgreSQL as primary to undo demotion")

	if err := pm.restartPostgresAsPrimary(ctx); err != nil {
		pm.logger.ErrorContext(ctx, "Failed to restart as primary", "error", err)
		return nil, err
	}

	// === Update topology and heartbeat (consistent with updateTopologyAfterPromotion) ===

	pm.logger.InfoContext(ctx, "Updating topology to PRIMARY")
	updatedMultipooler, err := pm.topoClient.UpdateMultiPoolerFields(ctx, pm.serviceID, func(mp *clustermetadatapb.MultiPooler) error {
		mp.Type = clustermetadatapb.PoolerType_PRIMARY
		return nil
	})
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to update topology to PRIMARY", "error", err)
		return nil, mterrors.Wrap(err, "failed to update topology after undo demotion")
	}

	pm.mu.Lock()
	pm.multipooler.MultiPooler = updatedMultipooler
	pm.updateCachedMultipooler()
	pm.mu.Unlock()

	// Update heartbeat tracker to primary mode (consistent with updateTopologyAfterPromotion)
	if pm.replTracker != nil {
		pm.logger.InfoContext(ctx, "Updating heartbeat tracker to primary mode")
		pm.replTracker.MakePrimary()
	}

	// === Get final state ===

	lsn, err := pm.getPrimaryLSN(ctx)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to query final LSN")
	}

	pm.logger.InfoContext(ctx, "UndoDemote completed successfully", "lsn_position", lsn)

	return &multipoolermanagerdatapb.UndoDemoteResponse{
		WasAlreadyPrimary: false,
		LsnPosition:       lsn,
	}, nil
}

// Promote promotes a standby to primary
// This is called during the Propagate stage of generalized consensus to safely
// transition a standby to primary and reconfigure replication.
// This operation is fully idempotent - it checks what steps are already complete
// and only executes the missing steps.
func (pm *MultiPoolerManager) Promote(ctx context.Context, consensusTerm int64, expectedLSN string, syncReplicationConfig *multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest, force bool) (*multipoolermanagerdatapb.PromoteResponse, error) {
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
	if err = pm.validateTermExactMatch(ctx, consensusTerm, force); err != nil {
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

	// Update topology if needed
	if err := pm.updateTopologyAfterPromotion(ctx, state); err != nil {
		return nil, err
	}

	// Configure sync replication if needed
	if err := pm.configureReplicationAfterPromotion(ctx, state, syncReplicationConfig); err != nil {
		return nil, err
	}

	// TODO: Populate consensus metadata tables.

	// Get final LSN position
	finalLSN, err := pm.getPrimaryLSN(ctx)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to get final LSN", "error", err)
		return nil, err
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

// SetTerm sets the consensus term information to local disk
func (pm *MultiPoolerManager) SetTerm(ctx context.Context, term *multipoolermanagerdatapb.ConsensusTerm) error {
	if err := pm.checkReady(); err != nil {
		return err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	ctx, err := pm.actionLock.Acquire(ctx, "SetTerm")
	if err != nil {
		return err
	}
	defer pm.actionLock.Release(ctx)

	// Initialize consensus state if needed
	pm.mu.Lock()
	if pm.consensusState == nil {
		pm.consensusState = NewConsensusState(pm.config.PoolerDir, pm.serviceID)
	}
	cs := pm.consensusState
	pm.mu.Unlock()

	// Save to disk and update memory atomically
	if err := cs.SetTermDirectly(ctx, term); err != nil {
		pm.logger.ErrorContext(ctx, "Failed to save consensus term", "error", err)
		return mterrors.Wrap(err, "failed to set consensus term")
	}

	return nil
}

// createDurabilityPolicyLocked creates a durability policy without acquiring the action lock.
// The caller MUST already hold the action lock.
func (pm *MultiPoolerManager) createDurabilityPolicyLocked(ctx context.Context, policyName string, quorumRule *clustermetadatapb.QuorumRule) error {
	if err := AssertActionLockHeld(ctx); err != nil {
		return err
	}

	pm.logger.InfoContext(ctx, "createDurabilityPolicyLocked called", "policy_name", policyName)

	// Validate inputs
	if policyName == "" {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "policy_name is required")
	}

	if quorumRule == nil {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "quorum_rule is required")
	}

	// Check that we have a database connection
	if pm.db == nil {
		return mterrors.New(mtrpcpb.Code_UNAVAILABLE, "database connection not available")
	}

	// Marshal the quorum rule to JSON using protojson
	marshaler := protojson.MarshalOptions{
		UseEnumNumbers: true,
	}
	quorumRuleJSON, err := marshaler.Marshal(quorumRule)
	if err != nil {
		return mterrors.Wrapf(err, "failed to marshal quorum rule")
	}

	// Reuse the existing insertDurabilityPolicy function which has the correct
	// table schema and ON CONFLICT clause.
	if err := pm.insertDurabilityPolicy(ctx, policyName, quorumRuleJSON); err != nil {
		return err
	}

	return nil
}

// CreateDurabilityPolicy creates a new durability policy in the local database
// Used by MultiOrch to initialize policies via gRPC instead of direct database connection
func (pm *MultiPoolerManager) CreateDurabilityPolicy(ctx context.Context, req *multipoolermanagerdatapb.CreateDurabilityPolicyRequest) (*multipoolermanagerdatapb.CreateDurabilityPolicyResponse, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}

	pm.logger.InfoContext(ctx, "CreateDurabilityPolicy called", "policy_name", req.PolicyName)

	// Validate inputs
	if req.PolicyName == "" {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "policy_name is required")
	}

	if req.QuorumRule == nil {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "quorum_rule is required")
	}

	// Check that we have a database connection
	if pm.db == nil {
		return nil, mterrors.New(mtrpcpb.Code_UNAVAILABLE, "database connection not available")
	}

	// Marshal the quorum rule to JSON using protojson
	marshaler := protojson.MarshalOptions{
		UseEnumNumbers: true, // Encode enums as numbers, not strings
	}
	quorumRuleJSON, err := marshaler.Marshal(req.QuorumRule)
	if err != nil {
		return nil, mterrors.Wrapf(err, "failed to marshal quorum rule")
	}

	// Insert the policy into the durability_policy table
	if err := pm.insertDurabilityPolicy(ctx, req.PolicyName, quorumRuleJSON); err != nil {
		pm.logger.ErrorContext(ctx, "Failed to insert durability policy", "error", err)
		return nil, err
	}

	return &multipoolermanagerdatapb.CreateDurabilityPolicyResponse{}, nil
}
