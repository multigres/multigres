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
	"os"
	"path/filepath"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
	backupengine "github.com/multigres/multigres/go/services/multipooler/internal/manager/backup"
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
func (pm *MultipoolerManager) createFirstBackupAndInitializeLocked(ctx context.Context) (busy bool, backupFound bool, retErr error) {
	if err := actionlock.AssertActionLockHeld(ctx); err != nil {
		return false, false, err
	}

	pm.logger.InfoContext(ctx, "creating first backup for shard", "shard", pm.getShardID())

	if pm.pgctldClient == nil {
		return false, false, mterrors.New(mtrpcpb.Code_UNAVAILABLE, "pgctld client not available")
	}

	// A sentinel from a prior attempt means we crashed between initdb and the
	// final data-directory cleanup. The data directory (if any) is stale and
	// safe to remove so we can retry from scratch. The sentinel is recreated
	// below before initdb, so we do not remove it here.
	sentinelPresent, err := pm.bootstrapSentinel().present()
	if err != nil {
		return false, false, mterrors.Wrap(err, "failed to check bootstrap sentinel")
	}
	if sentinelPresent {
		pm.logger.WarnContext(ctx, "bootstrap sentinel from prior attempt detected; removing stale data directory before retry")
		// os.RemoveAll (inside removeDataDirectory) is idempotent: a prior crash
		// between removeDataDirectory and the sentinel removal returns nil
		// here. Any non-nil error is a real failure worth surfacing.
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
	// The policy is also written into the initial row of current_rule so all
	// subsequent rule reads carry a non-nil DurabilityPolicy.
	policy, err := pm.loadDurabilityPolicy(ctx)
	if err != nil {
		return false, false, mterrors.Wrap(err, "failed to load durability policy")
	}

	// Re-check the encryption requirement at the point where it becomes
	// permanent: the startup check in loadShardConfigFromGlobalTopo read
	// topology once, and stanza-create below fixes the repo cipher forever —
	// a requirement added to topology after startup must still block an
	// unencrypted bootstrap.
	db, err := pm.topoClient.GetDatabase(ctx, pm.record.ShardRef().GetDatabase())
	if err != nil {
		return false, false, mterrors.Wrap(err, "failed to load database record")
	}
	if err := requireInitialRepoEncryptionError(db.BackupLocation, pm.config.BackupCipherKeys); err != nil {
		return false, false, mterrors.Wrap(err, "refusing to bootstrap")
	}

	// Write the sentinel before initdb so a process crash between here and the
	// final cleanup leaves a detectable marker. The sentinel lives in pooler_dir
	// (not PGDATA), so it is not captured by pgBackRest backups.
	if err := pm.bootstrapSentinel().write(); err != nil {
		return false, false, mterrors.Wrap(err, "failed to write bootstrap sentinel")
	}

	// A restore sentinel left by an interrupted restore would make pgctld refuse
	// the start below. Bootstrap rebuilds the data directory from scratch, and the
	// bootstrap sentinel written above now guards whatever is on disk, so the
	// restore sentinel can go.
	if err := pm.restoreSentinel().remove(); err != nil {
		return false, false, mterrors.Wrap(err, "failed to remove restore sentinel")
	}

	// Cleanup runs on every exit path. On success it tears down the local
	// postgres so every pooler restores from the shared backup. On failure it
	// removes whatever was partially created. If data-directory removal fails
	// we leave the sentinel in place so the next attempt knows to re-clean;
	// on an otherwise-successful run we also promote the failure to retErr so
	// the caller retries, since leaving stale pg_data would block the next
	// create-first-backup tick at the hasDataDirectory() guard.
	// Ordering matters: only clear the sentinel after the data directory is
	// gone, to preserve the "data dir present ⇒ sentinel present" invariant.
	defer func() {
		// Detached (actionlock.Detach) so an already-expired ctx (e.g. the
		// monitor tick's timeout) doesn't fail Stop for an unrelated reason,
		// while still satisfying protectedPgctldClient.Stop's action-lock
		// requirement. Mode "immediate" (not the escalation ladder used
		// elsewhere): this instance has no real client traffic to drain and
		// its data is about to be deleted regardless, so there's nothing a
		// graceful stop buys here that's worth waiting for.
		const stopTimeout = 10 * time.Second
		stopCtx, stopCancel := context.WithTimeout(actionlock.Detach(ctx), stopTimeout)
		defer stopCancel()
		if _, err := pm.pgctldClient.Stop(stopCtx, &pgctldpb.StopRequest{Mode: "immediate", Timeout: durationpb.New(stopTimeout)}); err != nil {
			pm.logger.WarnContext(ctx, "failed to stop Postgres during first backup cleanup", "error", err)
		}
		// Removing the data directory regardless of Stop's outcome is safe:
		// Recruit/Promote (the only ways this pooler could become a real
		// leader) need this same action lock, held until this defer returns.
		if err := pm.removeDataDirectory(); err != nil {
			pm.logger.WarnContext(ctx, "failed to remove data directory during first backup cleanup", "error", err)
			if retErr == nil {
				retErr = mterrors.Wrap(err, "failed to remove data directory during first backup cleanup")
			}
			return
		}
		if err := pm.bootstrapSentinel().remove(); err != nil {
			pm.logger.WarnContext(ctx, "failed to remove bootstrap sentinel during first backup cleanup", "error", err)
			if retErr == nil {
				retErr = mterrors.Wrap(err, "failed to remove bootstrap sentinel during first backup cleanup")
			}
		}
	}()

	if err := clearArchiveSpool(pm.record.PoolerDir()); err != nil {
		return false, false, mterrors.Wrap(err, "failed to clear stale archive spool")
	}

	// Initialize a fresh data directory.
	if _, err := pm.pgctldClient.InitDataDir(ctx, &pgctldpb.InitDataDirRequest{}); err != nil {
		return false, false, mterrors.Wrap(err, "failed to initialize data directory")
	}

	// Configure archive_mode before starting PostgreSQL.
	if err := pm.backup.ConfigureArchiveMode(ctx); err != nil {
		return false, false, mterrors.Wrap(err, "failed to configure archive mode")
	}

	// A stop file left by an interrupted stanza reset would block stanza-create
	// and archive-push.
	if err := pm.backup.StanzaStart(ctx); err != nil {
		return false, false, mterrors.Wrap(err, "failed to start the pgbackrest stanza")
	}

	// Start PostgreSQL as a writable primary: this one-time bootstrap of a
	// brand-new shard (term 0, before any other primary can exist, torn down
	// immediately after the first backup) needs to run DDL/DML, so it opts out of
	// the default standby start.
	if _, err := pm.pgctldClient.Start(ctx, &pgctldpb.StartRequest{AsPrimary: true}); err != nil {
		return false, false, mterrors.Wrap(err, "failed to start PostgreSQL")
	}

	if err := pm.waitForDatabaseConnection(ctx); err != nil {
		return false, false, mterrors.Wrap(err, "failed to connect to database")
	}

	if err := pm.createSidecarSchema(ctx, policy); err != nil {
		return false, false, mterrors.Wrap(err, "failed to create multigres schema")
	}

	if err := pm.insertInitialPgBackRestRepo(ctx); err != nil {
		return false, false, err
	}

	// The initial row (term=0, empty cohort) was already inserted by
	// CreateRuleTables via createSidecarSchema above. This signals to multiorch
	// that the shard has been initialized but not yet had its cohort established.

	// Race for the backup lease. Only the lease holder creates the backup;
	// everyone else will find an existing backup and skip to restore.
	// Stanza-create runs inside the lease because it is part of the pgBackRest
	// setup work that must complete before the backup.
	var fnStarted bool
	err = pm.withBackupLease(ctx, false /* steal */, "create-first-backup", func(leaseCtx context.Context) error {
		fnStarted = true
		found, err := pm.prepareStanzaLocked(leaseCtx)
		if err != nil {
			return err
		}
		if found {
			backupFound = true
			return nil
		}

		// Prove the archive pipeline works end-to-end before declaring the
		// shard bootstrapped. ConfigureArchiveMode wrote archive_command
		// above; `check` forces a WAL switch and confirms the segment reaches
		// the repo, so a misconfigured archive_command (bad creds, wrong
		// path) fails cluster creation now rather than silently surviving
		// until a restore needs WAL that was never archived.
		if err := pm.backup.Check(leaseCtx); err != nil {
			return mterrors.Wrap(err, "pgbackrest archive check failed")
		}

		if _, err := pm.backupLocked(leaseCtx, true, "full", "", nil); err != nil {
			return mterrors.Wrap(err, "failed to create initial backup")
		}
		return pm.topoClient.ClearBootstrapMarker(leaseCtx, pm.shardRef())
	})
	if err != nil {
		if errors.Is(err, &topoclient.TopoError{Code: topoclient.NodeExists}) {
			return true, false, nil // lease held by another pooler, back off
		}
		if !fnStarted {
			return false, false, mterrors.Wrap(err, "failed during backup lease")
		}
		return false, false, err
	}

	pm.logger.InfoContext(ctx, "first backup created; deferred cleanup will tear down local data directory", "shard", pm.getShardID())
	return false, backupFound, nil
}

var (
	errArchiveInUse = mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
		"stanza has no backups but its archive holds more WAL than a failed bootstrap, refusing to reset it")
	errUnmarkedStanza = mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
		"stanza has no backups and no unfinished bootstrap is recorded for the shard, refusing to reset it")
)

// prepareStanzaLocked creates this cluster's stanza, first removing a stale one
// left by an unfinished bootstrap, which the shard's bootstrap marker records. It
// returns true when a complete backup already exists. The repository is read
// inside the lease because another pooler may have created the backup since the
// monitor looked. Caller must hold the backup lease.
func (pm *MultipoolerManager) prepareStanzaLocked(ctx context.Context) (backupFound bool, err error) {
	repoState, err := pm.repositoryBackupState(ctx)
	if err != nil {
		return false, mterrors.Wrap(err, "failed to check for existing backups")
	}

	switch repoState {
	case completeBackupAvailable:
		pm.logger.InfoContext(ctx, "first backup already exists (created by another pooler)")
		if err := pm.topoClient.ClearBootstrapMarker(ctx, pm.shardRef()); err != nil {
			return false, err
		}
		return true, nil
	case noUsableBackup:
		return false, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			"repository holds backups but none is a complete backup of this shard, refusing to reset the stanza")
	case archiveInUse:
		return false, errArchiveInUse
	case unmarkedStanza:
		return false, errUnmarkedStanza
	}

	unfinished := repoState == emptyStanza
	if repoState == noStanza {
		unfinished, err = pm.topoClient.BootstrapInProgress(ctx, pm.shardRef())
		if err != nil {
			return false, mterrors.Wrap(err, "failed to read bootstrap marker")
		}
	}
	if err := pm.topoClient.MarkBootstrapInProgress(ctx, pm.shardRef(), pm.record.Id().Name); err != nil {
		return false, mterrors.Wrap(err, "failed to record bootstrap in progress")
	}

	deletedStale := false
	if repoState == emptyStanza {
		// A stanza without backups is bound to an earlier attempt's system id,
		// which this initdb cannot match, so it must go before stanza-create. A
		// repository with no stanza skips this, so a first bootstrap never stops
		// archiving.
		if err := pm.backup.StanzaDelete(ctx); err != nil {
			return false, mterrors.Wrap(err, "failed to remove stale pgbackrest stanza")
		}
		deletedStale = true
	}

	createErr := pm.backup.StanzaCreate(ctx)
	if createErr != nil && !deletedStale && backupengine.IsStaleStanzaError(createErr) {
		// info reports a half-created stanza, such as an archive.info without a
		// backup.info, as never created.
		pm.logger.WarnContext(ctx, "stanza-create found an existing stanza", "error", createErr)
		if !unfinished {
			// No earlier bootstrap left it, so it is not ours to reset, and the
			// marker written above must not make it look as if one did.
			if err := pm.topoClient.ClearBootstrapMarker(ctx, pm.shardRef()); err != nil {
				return false, err
			}
			return false, errUnmarkedStanza
		}
		inUse, err := pm.backup.ArchiveInUse(ctx)
		if err != nil {
			return false, mterrors.Wrap(err, "failed to inspect the pgbackrest archive")
		}
		if inUse {
			return false, errArchiveInUse
		}
		if err := pm.backup.StanzaDelete(ctx); err != nil {
			return false, mterrors.Wrap(err, "failed to remove stale pgbackrest stanza")
		}
		createErr = pm.backup.StanzaCreate(ctx)
	}
	if createErr != nil {
		return false, mterrors.Wrap(createErr, "failed to create pgbackrest stanza")
	}
	return false, nil
}

// clearArchiveSpool removes async archive-push state from earlier attempts. A new
// initdb restarts WAL numbering, so a stale acknowledgement would make
// archive-push skip the new cluster's first segment and the archive check time out.
func clearArchiveSpool(poolerDir string) error {
	return os.RemoveAll(filepath.Join(poolerDir, "pgbackrest", "spool", "archive"))
}

// bootstrapSentinel returns the first-backup bootstrap sentinel; see
// sentinel.go.
func (pm *MultipoolerManager) bootstrapSentinel() sentinel {
	return pm.newSentinel(constants.BootstrapSentinelFile, "first-backup bootstrap in progress\n")
}

// loadDurabilityPolicy reads the bootstrap durability policy from the topology database record.
func (pm *MultipoolerManager) loadDurabilityPolicy(ctx context.Context) (*clustermetadatapb.DurabilityPolicy, error) {
	db, err := pm.topoClient.GetDatabase(ctx, pm.record.ShardRef().GetDatabase())
	if err != nil {
		return nil, mterrors.Wrapf(err, "failed to get database %s from topology", pm.record.ShardRef().GetDatabase())
	}

	if db.BootstrapDurabilityPolicy == nil {
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"database %s has no durability_policy configured", pm.record.ShardRef().GetDatabase())
	}

	return db.BootstrapDurabilityPolicy, nil
}
