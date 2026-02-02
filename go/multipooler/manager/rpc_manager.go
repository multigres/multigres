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
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/multipooler/executor"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
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
		PostgresRunning:  pm.isPostgresRunning(ctx),
		PostgresRole:     pm.getRole(ctx),
		ShardId:          pm.getShardID(),
	}

	// Get consensus term if available (use inconsistent read for monitoring)
	if pm.consensusState != nil {
		term, err := pm.consensusState.GetInconsistentTerm()
		if err == nil {
			poolerStatus.ConsensusTerm = term
		}
	}

	// Get WAL position (ignore errors, just return empty string)
	walPosition, _ := pm.getWALPosition(ctx)
	poolerStatus.WalPosition = walPosition

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

	return pm.configureSynchronousReplicationLocked(ctx, synchronousCommit, synchronousMethod, numSync, standbyIDs, reloadConfig)
}

// configureSynchronousReplicationLocked configures PostgreSQL synchronous replication settings.
// The caller MUST already hold the action lock.
func (pm *MultiPoolerManager) configureSynchronousReplicationLocked(ctx context.Context, synchronousCommit multipoolermanagerdatapb.SynchronousCommitLevel, synchronousMethod multipoolermanagerdatapb.SynchronousMethod, numSync int32, standbyIDs []*clustermetadatapb.ID, reloadConfig bool) error {
	// Validate input parameters
	if err := validateSyncReplicationParams(numSync, standbyIDs); err != nil {
		return err
	}

	// Check PRIMARY guardrails (pooler type and non-recovery mode)
	if err := pm.checkPrimaryGuardrails(ctx); err != nil {
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
			"unsupported operation: "+operation.String())
	}

	pm.logger.InfoContext(ctx, "UpdateSynchronousStandbyList completed successfully",
		"operation", operation,
		"old_value", currentValue,
		"new_value", updatedStandbys,
		"reload_config", reloadConfig,
		"consensus_term", consensusTerm,
		"force", force)

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

	// Update local state first
	pm.mu.Lock()
	pm.multipooler.Type = poolerType
	multiPoolerToSync := proto.Clone(pm.multipooler).(*clustermetadatapb.MultiPooler)
	pm.mu.Unlock()

	// Sync to topology
	if err := pm.topoClient.RegisterMultiPooler(ctx, multiPoolerToSync, true); err != nil {
		pm.logger.ErrorContext(ctx, "Failed to update pooler type in topology", "error", err, "service_id", pm.serviceID.String())
		return mterrors.Wrap(err, "failed to update pooler type in topology")
	}

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

	// Pause monitoring during this operation to prevent interference
	resumeMonitor, err := pm.PausePostgresMonitor(ctx)
	if err != nil {
		return nil, err
	}
	defer resumeMonitor(ctx)

	// Validate the term but DON'T update yet. We only update the term AFTER
	// successful demotion to avoid a race where a failed demote (e.g., postgres
	// not ready) updates the term, causing subsequent detection to see equal
	// terms and skip demotion.
	if err := pm.validateTerm(ctx, consensusTerm, force); err != nil {
		return nil, err
	}

	// Perform the actual demotion
	resp, err := pm.demoteLocked(ctx, consensusTerm, drainTimeout)
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

	// Wait for postgres to accept connections after restart
	// restartPostgresAsStandby uses skip_wait=true, so postgres may not be ready immediately
	pm.logger.InfoContext(ctx, "Waiting for PostgreSQL to accept connections after demotion")
	maxRetries := 30
	var lastErr error
	for i := range maxRetries {
		// Check if postgres is ready by querying pg_is_in_recovery
		queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		result, err := pm.query(queryCtx, "SELECT pg_is_in_recovery()")
		cancel()

		if err == nil {
			var inRecovery bool
			if scanErr := executor.ScanSingleRow(result, &inRecovery); scanErr == nil {
				pm.logger.InfoContext(ctx, "PostgreSQL is now accepting connections after demotion",
					"in_recovery", inRecovery,
					"attempts", i+1)
				lastErr = nil
				break
			} else {
				lastErr = scanErr
			}
		} else {
			lastErr = err
		}

		if i < maxRetries-1 {
			pm.logger.InfoContext(ctx, "Waiting for postgres to accept connections",
				"attempt", i+1,
				"max_retries", maxRetries,
				"error", lastErr)
			time.Sleep(1 * time.Second)
		}
	}
	if lastErr != nil {
		pm.logger.WarnContext(ctx, "Postgres did not accept connections within timeout, continuing anyway", "error", lastErr)
		// Don't fail the demotion - postgres will eventually be ready
		// The cleanup or subsequent operations will retry
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

	// Pause monitoring during this operation to prevent interference
	resumeMonitor, err := pm.PausePostgresMonitor(ctx)
	if err != nil {
		return nil, err
	}
	defer resumeMonitor(ctx)

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

	// Configure primary_conninfo BEFORE restarting postgres
	// After pg_rewind, postgres needs to stream WAL from the primary to reach consistent
	// recovery state. If we try to configure primary_conninfo AFTER starting postgres,
	// we hit a chicken-and-egg problem: postgres won't accept connections until it
	// reaches consistent state, but it can't reach consistent state without streaming
	// WAL, which requires primary_conninfo to be configured.
	//
	// Solution: Write primary_conninfo directly to postgresql.auto.conf before starting postgres.
	pm.logger.InfoContext(ctx, "Configuring replication to source primary before restart",
		"source", source.Id.Name,
		"source_host", source.Hostname,
		"source_port", port)

	pm.mu.Lock()
	database := pm.multipooler.Database
	poolerDir := pm.multipooler.PoolerDir
	pm.primaryPoolerID = source.Id
	pm.primaryHost = source.Hostname
	pm.primaryPort = port
	pm.mu.Unlock()

	appName := generateApplicationName(pm.serviceID)
	connInfo := fmt.Sprintf("host=%s port=%d user=%s application_name=%s",
		source.Hostname, port, database, appName)

	// Write primary_conninfo directly to postgresql.auto.conf
	if err := pm.writePrimaryConnInfoToFile(ctx, poolerDir, connInfo); err != nil {
		return nil, mterrors.Wrap(err, "failed to write primary_conninfo to file")
	}

	// Now restart postgres - it will read primary_conninfo from postgresql.auto.conf
	// and immediately start streaming WAL from the primary
	if err := pm.restartAsStandbyAfterRewind(ctx); err != nil {
		return nil, mterrors.Wrap(err, "failed to restart as standby")
	}

	if err := pm.resetSynchronousReplication(ctx); err != nil {
		pm.logger.WarnContext(ctx, "Failed to reset synchronous replication", "error", err)
	}

	// Wait for postgres to accept connections and verify replication is active
	pm.logger.InfoContext(ctx, "Waiting for PostgreSQL to accept connections")
	maxRetries := 30
	var lastErr error
	for i := range maxRetries {
		// Check if postgres is ready by querying pg_is_in_recovery
		queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		result, err := pm.query(queryCtx, "SELECT pg_is_in_recovery()")
		cancel()

		if err == nil {
			var inRecovery bool
			if scanErr := executor.ScanSingleRow(result, &inRecovery); scanErr == nil {
				pm.logger.InfoContext(ctx, "PostgreSQL is now accepting connections",
					"in_recovery", inRecovery,
					"attempts", i+1)
				lastErr = nil
				break
			} else {
				lastErr = scanErr
			}
		} else {
			lastErr = err
		}

		if i < maxRetries-1 {
			pm.logger.InfoContext(ctx, "Waiting for postgres to accept connections",
				"attempt", i+1,
				"max_retries", maxRetries,
				"error", lastErr)
			time.Sleep(1 * time.Second)
		}
	}
	if lastErr != nil {
		return nil, mterrors.Wrap(lastErr, "postgres did not accept connections after restart")
	}

	// Update topology to REPLICA
	if err := pm.changeTypeLocked(ctx, clustermetadatapb.PoolerType_REPLICA); err != nil {
		return nil, mterrors.Wrap(err, "failed to update topology")
	}

	// Get final LSN
	finalLSN := ""
	if lsn, err := pm.getStandbyReplayLSN(ctx); err == nil {
		finalLSN = lsn
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
func (pm *MultiPoolerManager) Promote(ctx context.Context, consensusTerm int64, expectedLSN string, syncReplicationConfig *multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest, force bool, reason string, coordinatorID string, cohortMembers []string, acceptedMembers []string) (*multipoolermanagerdatapb.PromoteResponse, error) {
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

	// Get final LSN position
	finalLSN, err := pm.getPrimaryLSN(ctx)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to get final LSN", "error", err)
		return nil, err
	}

	// Write leadership history record - this validates that sync replication is working.
	// If this fails (typically due to timeout waiting for standby acknowledgment), we fail
	// the promotion. It's better to have no primary than one that can't satisfy durability.
	leaderID := generateApplicationName(pm.serviceID)
	if reason == "" {
		reason = "unknown"
	}
	if coordinatorID == "" {
		coordinatorID = "unknown"
	}
	if err := pm.insertLeadershipHistory(ctx, consensusTerm, leaderID, coordinatorID, finalLSN, reason, cohortMembers, acceptedMembers); err != nil {
		pm.logger.ErrorContext(ctx, "Failed to insert leadership history - promotion failed",
			"term", consensusTerm,
			"error", err)
		return nil, mterrors.Wrap(err, "promotion failed: could not write leadership history (sync replication may not be functioning)")
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

	// Pause monitoring during this operation to prevent interference
	resumeMonitor, err := pm.PausePostgresMonitor(ctx)
	if err != nil {
		return nil, err
	}
	defer resumeMonitor(ctx)

	// Check if pgctld client is available
	if pm.pgctldClient == nil {
		return nil, mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE, "pgctld client not available")
	}

	// Close manager and stop PostgreSQL
	pm.logger.InfoContext(ctx, "Closing manager and stopping PostgreSQL for pg_rewind")

	// Close the manager to release database connections
	if err := pm.Close(); err != nil {
		pm.logger.WarnContext(ctx, "Failed to close manager before pg_rewind", "error", err)
		// Continue - we'll try to stop postgres anyway
	}

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

	// Reopen the manager
	if err := pm.Open(); err != nil {
		pm.logger.ErrorContext(ctx, "Failed to reopen query service controller after restart", "error", err)
		return nil, mterrors.Wrap(err, "failed to reopen query service controller")
	}

	// Wait for database connection
	if err := pm.waitForDatabaseConnection(ctx); err != nil {
		pm.logger.ErrorContext(ctx, "Failed to reconnect to database", "error", err)
		return nil, mterrors.Wrap(err, "failed to reconnect to database after pg_rewind")
	}

	pm.logger.InfoContext(ctx, "RewindToSource completed successfully",
		"rewind_performed", rewindPerformed)
	return &multipoolermanagerdatapb.RewindToSourceResponse{
		Success:         true,
		ErrorMessage:    "",
		RewindPerformed: rewindPerformed,
	}, nil
}

// SetMonitor enables or disables the PostgreSQL monitoring goroutine (RPC handler)
func (pm *MultiPoolerManager) SetMonitor(
	ctx context.Context,
	req *multipoolermanagerdatapb.SetMonitorRequest,
) (*multipoolermanagerdatapb.SetMonitorResponse, error) {
	if req.Enabled {
		if err := pm.enableMonitorInternal(); err != nil {
			return nil, mterrors.Wrap(err, "failed to enable PostgreSQL monitoring")
		}
		pm.logger.InfoContext(ctx, "SetMonitor RPC completed successfully", "enabled", true)
		return &multipoolermanagerdatapb.SetMonitorResponse{}, nil
	}

	pm.disableMonitorInternal()
	pm.logger.InfoContext(ctx, "SetMonitor RPC completed successfully", "enabled", false)
	return &multipoolermanagerdatapb.SetMonitorResponse{}, nil
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
	if pm.replTracker != nil {
		pm.replTracker.Close()
		pm.replTracker = nil
	}
	if pm.connPoolMgr != nil {
		pm.connPoolMgr.Close()
	}
	pm.mu.Unlock()

	// Stop postgres (no-op if already stopped)
	stopReq := &pgctldpb.StopRequest{Mode: "fast"}
	if _, err := pm.pgctldClient.Stop(ctx, stopReq); err != nil {
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

	// Run crash recovery if needed before pg_rewind
	pm.logger.InfoContext(ctx, "Checking if crash recovery needed before pg_rewind")
	crashRecoveryResp, err := pm.pgctldClient.CrashRecovery(ctx, &pgctldpb.CrashRecoveryRequest{})
	if err != nil {
		pm.logger.ErrorContext(ctx, "Crash recovery check failed", "error", err)
		return false, mterrors.Wrap(err, "crash recovery check failed")
	}

	if crashRecoveryResp.RecoveryPerformed {
		pm.logger.InfoContext(ctx, "Crash recovery performed successfully",
			"state_before", crashRecoveryResp.StateBefore,
			"state_after", crashRecoveryResp.StateAfter)
	} else {
		pm.logger.InfoContext(ctx, "No crash recovery needed, database already clean",
			"state", crashRecoveryResp.StateBefore)
	}

	pm.logger.InfoContext(ctx, "Running pg_rewind dry-run", "source_host", sourceHost, "source_port", sourcePort)

	// Dry-run to check if rewind is needed
	dryRunReq := &pgctldpb.PgRewindRequest{
		SourceHost: sourceHost,
		SourcePort: sourcePort,
		DryRun:     true,
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
		pm.logger.InfoContext(ctx, "Servers diverged, running pg_rewind")

		rewindReq := &pgctldpb.PgRewindRequest{
			SourceHost: sourceHost,
			SourcePort: sourcePort,
			DryRun:     false,
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

// restartAsStandbyAfterRewind restarts postgres as standby after rewind.
func (pm *MultiPoolerManager) restartAsStandbyAfterRewind(ctx context.Context) error {
	// Use existing restartPostgresAsStandby with a state that indicates postgres is not running
	state := &demotionState{
		isReadOnly: false, // Postgres was stopped, not in standby mode yet
	}
	return pm.restartPostgresAsStandby(ctx, state)
}
