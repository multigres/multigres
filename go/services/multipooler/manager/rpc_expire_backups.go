// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/tools/executil"
	"github.com/multigres/multigres/go/tools/telemetry"
)

// ExpireBackups runs pgbackrest expire to remove backups that exceed the
// configured retention policy. This is safe to call at any time.
func (pm *MultiPoolerManager) ExpireBackups(ctx context.Context, overrides map[string]string) error {
	if err := pm.checkReady(); err != nil {
		return err
	}

	// Acquire the action lock to ensure only one mutation runs at a time
	var err error
	ctx, err = pm.actionLock.Acquire(ctx, "ExpireBackups")
	if err != nil {
		return err
	}
	defer pm.actionLock.Release(ctx)

	// Acquire the distributed backup lease (non-stealing).
	// Expire must not run concurrently with backup/stanza-create on any node.
	// Uses WithBackupLease (not WithStolenBackupLease) because expire should
	// not preempt an in-progress backup — it can wait or fail fast.
	return pm.topoClient.WithBackupLease(ctx, pm.shardKey(), pm.multipooler.Id.Name, "expire", pm.logger, func(ctx context.Context) error {
		return pm.expireBackupsLocked(ctx, overrides)
	})
}

// expireBackupsLocked runs pgbackrest expire. Caller must hold the action lock.
func (pm *MultiPoolerManager) expireBackupsLocked(ctx context.Context, overrides map[string]string) error {
	if err := AssertActionLockHeld(ctx); err != nil {
		return err
	}

	configPath, err := pm.pgBackRestConfig()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, backup.ExpireTimeout)
	defer cancel()

	args := []string{
		"--stanza=" + pm.stanzaName(),
		"--config=" + configPath,
		"expire",
	}

	args = backup.ApplyPgBackRestOverrides(args, overrides)

	cmd := executil.Command(ctx, "pgbackrest", args...)

	var output []byte
	err = telemetry.WithSpan(ctx, "expire-backups", func(ctx context.Context) error {
		var runErr error
		output, runErr = pm.runLongCommand(ctx, cmd, "pgbackrest expire")
		return runErr
	})
	if err != nil {
		return fmt.Errorf("pgbackrest expire failed: %w\nOutput: %s", err, string(output))
	}

	pm.logger.InfoContext(ctx, "Backup expiration completed")
	return nil
}
