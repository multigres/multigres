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
	"errors"
	"fmt"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/tools/executil"
)

// createFirstBackupAndInitialize attempts to create the first pgBackRest backup for this shard.
//
// If no backup exists, this pooler races via the backup lease to run initdb, create the
// multigres schema, write a 0-member cohort record (the signal for ShardNeedsInitialCohort),
// create the first pgBackRest backup, and delete the local data directory. All poolers
// then restore from that backup and end up in the same state: hot standby.
//
// Returns (busy=true, backupFound=false, nil) if the backup lease is held by another pooler —
// the monitor should back off and retry. Returns (false, true, nil) if a backup was found inside
// the lease (created by another pooler just before we acquired it) — the caller should restore
// immediately. Returns (false, false, err) on any other failure.
func (pm *MultiPoolerManager) createFirstBackupAndInitialize(ctx context.Context) (busy bool, backupFound bool, retErr error) {
	// Try to acquire the backup lease without blocking. If another pooler already
	// holds it, we back off and let the monitor retry later.
	lockCtx, unlock, err := pm.topoClient.TryLockBackup(ctx, pm.shardKey(), "create-first-backup")
	if err != nil {
		if errors.Is(err, &topoclient.TopoError{Code: topoclient.NodeExists}) {
			return true, false, nil
		}
		return false, false, mterrors.Wrap(err, "failed to acquire backup lease")
	}
	defer unlock(&retErr)

	// Check again inside the lease — another pooler may have created the backup
	// between our check and acquiring the lease.
	if pm.hasCompleteBackups(lockCtx) {
		pm.logger.InfoContext(lockCtx, "First backup already exists (created by another pooler), proceeding to restore")
		return false, true, nil
	}

	return false, false, pm.createFirstBackupLocked(lockCtx)
}

// createFirstBackupLocked runs the full first-backup sequence. Caller must hold
// both the action lock and the backup lease.
func (pm *MultiPoolerManager) createFirstBackupLocked(ctx context.Context) error {
	pm.logger.InfoContext(ctx, "Creating first backup for shard", "shard", pm.getShardID())

	if pm.pgctldClient == nil {
		return mterrors.New(mtrpcpb.Code_UNAVAILABLE, "pgctld client not available")
	}

	// Refuse to run on an already-initialized data directory — initdb would
	// destroy existing data and this node should be restoring from backup instead.
	if pm.hasDataDirectory() {
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"data directory already exists; cannot create first backup on an initialized node")
	}

	// Read the durability policy from topology before doing any expensive work.
	// A misconfigured database (missing durability_policy) should fail fast,
	// before initdb, starting postgres, or schema creation.
	// TODO: Once pooler health status carries the durability policy, include it in the
	// initial leadership_history record so replicas can verify quorum requirements on startup.
	if _, err := pm.loadDurabilityPolicy(ctx); err != nil {
		return mterrors.Wrap(err, "failed to load durability policy")
	}

	// Initialize a fresh data directory.
	if _, err := pm.pgctldClient.InitDataDir(ctx, &pgctldpb.InitDataDirRequest{}); err != nil {
		return mterrors.Wrap(err, "failed to initialize data directory")
	}

	// On any failure after initdb, remove the data directory so the next monitor
	// iteration sees a clean slate and retries from the beginning.
	//
	// For the success path, the data directory is explicitly removed below (before
	// this defer runs). The skipDeferCleanup flag prevents a redundant remove.
	skipDeferCleanup := false
	defer func() {
		if skipDeferCleanup {
			return
		}
		// Stop postgres before removing the data directory (ignore errors — postgres
		// may already be stopped if the failure occurred before Start).
		if _, err := pm.pgctldClient.Stop(ctx, &pgctldpb.StopRequest{Mode: "fast"}); err != nil {
			pm.logger.WarnContext(ctx, "Failed to stop PostgreSQL during first backup cleanup", "error", err)
		}
		if err := pm.removeDataDirectory(); err != nil {
			pm.logger.WarnContext(ctx, "Failed to remove data directory during first backup cleanup", "error", err)
		}
	}()

	// Configure archive_mode before starting PostgreSQL.
	if err := pm.configureArchiveMode(ctx); err != nil {
		return mterrors.Wrap(err, "failed to configure archive mode")
	}

	// Start PostgreSQL.
	if _, err := pm.pgctldClient.Start(ctx, &pgctldpb.StartRequest{}); err != nil {
		return mterrors.Wrap(err, "failed to start PostgreSQL")
	}

	if err := pm.waitForDatabaseConnection(ctx); err != nil {
		return mterrors.Wrap(err, "failed to connect to database")
	}

	if err := pm.createSidecarSchema(ctx); err != nil {
		return mterrors.Wrap(err, "failed to create multigres schema")
	}

	if err := pm.initializeMultischemaData(ctx); err != nil {
		return mterrors.Wrap(err, "failed to initialize multischema data")
	}

	// Get WAL position for the history record.
	walPosition, err := pm.getPrimaryLSN(ctx)
	if err != nil {
		return mterrors.Wrap(err, "failed to get WAL position")
	}

	// Write a 0-member cohort record. This is the canonical signal that the shard
	// has been initialized but not yet had its initial cohort established by multiorch.
	if err := pm.insertHistoryRecord(ctx,
		0,           // term 0 — no consensus yet
		"promotion", // event type
		poolerID{},  // no leader yet (inserts NULL via NULLIF)
		nil,         // no coordinator yet
		walPosition,
		"bootstrap",             // operation
		"BootstrapInitialShard", // reason
		nil,                     // 0-member cohort
		nil,                     // 0 accepted members
		false,                   // not forced
	); err != nil {
		return mterrors.Wrap(err, "failed to insert initial history record")
	}

	// Run stanza-create within the already-held backup lease (no second acquisition).
	if err := pm.runStanzaCreate(ctx); err != nil {
		return mterrors.Wrap(err, "failed to create pgbackrest stanza")
	}

	// Create the initial backup.
	if _, err := pm.backupLocked(ctx, true, "full", "", nil); err != nil {
		return mterrors.Wrap(err, "failed to create initial backup")
	}

	// Stop PostgreSQL before removing the data directory.
	// Suppress the cleanup defer — we do both steps explicitly here on the success path.
	skipDeferCleanup = true
	if _, err := pm.pgctldClient.Stop(ctx, &pgctldpb.StopRequest{Mode: "fast"}); err != nil {
		pm.logger.WarnContext(ctx, "Failed to stop PostgreSQL cleanly before removing data directory; proceeding anyway — postgres will stop once the data directory is gone", "error", err)
	}

	// Delete the local data directory. Every pooler (including this one) will
	// restore from the backup, ending up in the same hot-standby state.
	if err := pm.removeDataDirectory(); err != nil {
		return mterrors.Wrap(err, "failed to remove data directory after first backup")
	}

	pm.logger.InfoContext(ctx, "First backup created; data directory removed; ready for restore", "shard", pm.getShardID())
	return nil
}

// runStanzaCreate runs pgbackrest stanza-create. Caller must hold the backup lease.
// stanza-create is idempotent: if a previous lease holder ran it and then crashed
// before completing the backup, the next holder can safely re-run it.
func (pm *MultiPoolerManager) runStanzaCreate(ctx context.Context) error {
	if err := topoclient.AssertBackupLockHeld(ctx, pm.shardKey()); err != nil {
		return mterrors.Wrap(err, "backup lease not held")
	}

	configPath, err := pm.pgBackRestConfig()
	if err != nil {
		return mterrors.Wrap(err, "failed to get pgbackrest config")
	}

	stanzaCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	cmd := executil.Command(stanzaCtx, "pgbackrest",
		"--stanza="+pm.stanzaName(),
		"--config="+configPath,
		"stanza-create")

	output, err := safeCombinedOutput(cmd)
	if err != nil {
		return mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("pgbackrest stanza-create failed for stanza %s: %v\nOutput: %s",
				pm.stanzaName(), err, output))
	}

	pm.logger.InfoContext(ctx, "pgbackrest stanza initialized", "stanza", pm.stanzaName())
	return nil
}

// loadDurabilityPolicy reads the bootstrap durability policy from the topology database record.
func (pm *MultiPoolerManager) loadDurabilityPolicy(ctx context.Context) (*clustermetadatapb.DurabilityPolicy, error) {
	db, err := pm.topoClient.GetDatabase(ctx, pm.multipooler.Database)
	if err != nil {
		return nil, mterrors.Wrapf(err, "failed to get database %s from topology", pm.multipooler.Database)
	}

	if db.BootstrapDurabilityPolicy == nil {
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"database %s has no durability_policy configured", pm.multipooler.Database)
	}

	return db.BootstrapDurabilityPolicy, nil
}
