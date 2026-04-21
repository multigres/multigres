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
	"os"
	"path/filepath"
	"time"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/tools/executil"
)

// createFirstBackupAndInitializeLocked attempts to create the first pgBackRest backup for this shard.
//
// Each pooler independently runs initdb, starts postgres, creates the multigres schema,
// and initializes the pgBackRest stanza locally. Then it races via the backup lease to
// create the actual backup. If another pooler wins the race, a backup will already exist
// when we acquire the lease and we skip to restore. After the backup exists, the local
// data directory is removed so all poolers restore from the shared backup.
//
// Returns (busy=true, backupFound=false, nil) if the backup lease is held by another pooler —
// the monitor should back off and retry. Returns (false, true, nil) if a backup was found
// (created by another pooler) — the caller should restore immediately.
func (pm *MultiPoolerManager) createFirstBackupAndInitializeLocked(ctx context.Context) (busy bool, backupFound bool, _ error) {
	pm.logger.InfoContext(ctx, "Creating first backup for shard", "shard", pm.getShardID())

	if pm.pgctldClient == nil {
		return false, false, mterrors.New(mtrpcpb.Code_UNAVAILABLE, "pgctld client not available")
	}

	// A sentinel from a prior attempt means we crashed between initdb and the
	// final data-directory cleanup. The data directory (if any) is stale and
	// safe to remove so we can retry from scratch. The sentinel is recreated
	// below before initdb, so we do not remove it here.
	if pm.hasBootstrapSentinel() {
		pm.logger.WarnContext(ctx, "Bootstrap sentinel from prior attempt detected; removing stale data directory before retry")
		if err := pm.removeDataDirectory(); err != nil {
			return false, false, mterrors.Wrap(err, "failed to remove stale data directory from prior bootstrap attempt")
		}
	}

	// Refuse to run on an already-initialized data directory. This node is already initialized
	if pm.hasDataDirectory() {
		return false, false, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"data directory already exists; cannot create first backup on an initialized node")
	}

	// Read the durability policy from topology before doing any expensive work.
	// A misconfigured database (missing durability_policy) should fail fast.
	if _, err := pm.loadDurabilityPolicy(ctx); err != nil {
		return false, false, mterrors.Wrap(err, "failed to load durability policy")
	}

	// Write the sentinel before initdb so a process crash between here and the
	// final cleanup leaves a detectable marker. The sentinel lives in pooler_dir
	// (not PGDATA), so it is not captured by pgBackRest backups.
	if err := pm.writeBootstrapSentinel(); err != nil {
		return false, false, mterrors.Wrap(err, "failed to write bootstrap sentinel")
	}

	// Schedule cleanup before InitDataDir so that a partial failure (e.g. initdb
	// creates the directory but crashes) is cleaned up on the next iteration.
	skipCleanup := false
	defer func() {
		if skipCleanup {
			return
		}
		if _, err := pm.pgctldClient.Stop(ctx, &pgctldpb.StopRequest{Mode: "fast"}); err != nil {
			pm.logger.WarnContext(ctx, "Failed to stop PostgreSQL during first backup cleanup", "error", err)
		}
		if err := pm.removeDataDirectory(); err != nil {
			// Leave the sentinel in place so the next attempt knows to re-clean.
			pm.logger.WarnContext(ctx, "Failed to remove data directory during first backup cleanup", "error", err)
			return
		}
		if err := pm.removeBootstrapSentinel(); err != nil {
			pm.logger.WarnContext(ctx, "Failed to remove bootstrap sentinel during first backup cleanup", "error", err)
		}
	}()

	// Initialize a fresh data directory.
	if _, err := pm.pgctldClient.InitDataDir(ctx, &pgctldpb.InitDataDirRequest{}); err != nil {
		return false, false, mterrors.Wrap(err, "failed to initialize data directory")
	}

	// Configure archive_mode before starting PostgreSQL.
	if err := pm.configureArchiveMode(ctx); err != nil {
		return false, false, mterrors.Wrap(err, "failed to configure archive mode")
	}

	// Start PostgreSQL.
	if _, err := pm.pgctldClient.Start(ctx, &pgctldpb.StartRequest{}); err != nil {
		return false, false, mterrors.Wrap(err, "failed to start PostgreSQL")
	}

	if err := pm.waitForDatabaseConnection(ctx); err != nil {
		return false, false, mterrors.Wrap(err, "failed to connect to database")
	}

	if err := pm.createSidecarSchema(ctx); err != nil {
		return false, false, mterrors.Wrap(err, "failed to create multigres schema")
	}

	if err := pm.initializeMultischemaData(ctx); err != nil {
		return false, false, mterrors.Wrap(err, "failed to initialize multischema data")
	}

	// The zero-state sentinel row (term=0, empty cohort) was already inserted by
	// createRuleTables via createSidecarSchema above. This signals to multiorch
	// that the shard has been initialized but not yet had its cohort established.

	// Race for the backup lease. Only the lease holder creates the backup;
	// everyone else will find an existing backup and skip to restore.
	// Stanza-create runs inside the lease because it is part of the pgBackRest
	// setup work that must complete before the backup.
	err := pm.topoClient.WithBackupLease(ctx, pm.shardKey(), pm.multipooler.Id.Name, "create-first-backup", pm.logger, func(leaseCtx context.Context) error {
		// Re-check inside the lease — another pooler may have created the backup
		// between our outer check and acquiring the lease.
		if pm.hasCompleteBackups(leaseCtx) {
			pm.logger.InfoContext(leaseCtx, "First backup already exists (created by another pooler)")
			backupFound = true
			return nil
		}

		if err := pm.runStanzaCreate(leaseCtx); err != nil {
			return mterrors.Wrap(err, "failed to create pgbackrest stanza")
		}

		if _, err := pm.backupLocked(leaseCtx, true, "full", "", nil); err != nil {
			return mterrors.Wrap(err, "failed to create initial backup")
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, &topoclient.TopoError{Code: topoclient.NodeExists}) {
			return true, false, nil // lease held by another pooler, back off
		}
		return false, false, mterrors.Wrap(err, "failed during backup lease")
	}

	// Stop PostgreSQL and remove the data directory. Every pooler (including
	// this one) will restore from the backup, ending up in the same state.
	skipCleanup = true
	if _, err := pm.pgctldClient.Stop(ctx, &pgctldpb.StopRequest{Mode: "fast"}); err != nil {
		pm.logger.WarnContext(ctx, "Failed to stop PostgreSQL cleanly before removing data directory", "error", err)
	}
	if err := pm.removeDataDirectory(); err != nil {
		return false, false, mterrors.Wrap(err, "failed to remove data directory after first backup")
	}
	// Ordering matters: only clear the sentinel after the data directory is gone,
	// so the invariant "data dir present ⇒ sentinel present" holds across retries.
	if err := pm.removeBootstrapSentinel(); err != nil {
		pm.logger.WarnContext(ctx, "Failed to remove bootstrap sentinel after successful first backup", "error", err)
	}

	pm.logger.InfoContext(ctx, "First backup created; data directory removed; ready for restore", "shard", pm.getShardID())
	return false, backupFound, nil
}

func (pm *MultiPoolerManager) bootstrapSentinelPath() string {
	return filepath.Join(pm.multipooler.PoolerDir, constants.BootstrapSentinelFile)
}

func (pm *MultiPoolerManager) hasBootstrapSentinel() bool {
	_, err := os.Stat(pm.bootstrapSentinelPath())
	return err == nil
}

func (pm *MultiPoolerManager) writeBootstrapSentinel() error {
	path := pm.bootstrapSentinelPath()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("failed to ensure pooler directory exists: %w", err)
	}
	return os.WriteFile(path, []byte("first-backup bootstrap in progress\n"), 0o644)
}

// removeBootstrapSentinel deletes the sentinel; a missing file is not an error.
func (pm *MultiPoolerManager) removeBootstrapSentinel() error {
	if err := os.Remove(pm.bootstrapSentinelPath()); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// runStanzaCreate runs pgbackrest stanza-create. The operation is idempotent
// and safe to run concurrently — no backup lease required.
func (pm *MultiPoolerManager) runStanzaCreate(ctx context.Context) error {
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
