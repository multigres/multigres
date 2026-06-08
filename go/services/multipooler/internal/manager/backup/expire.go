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

package backup

import (
	"context"
	"fmt"

	commonbackup "github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
	"github.com/multigres/multigres/go/tools/telemetry"
)

// Expire runs pgbackrest expire. Caller must hold the action lock.
// Returns the IDs of backups that were removed.
func (e *Engine) Expire(ctx context.Context, overrides map[string]string) ([]string, error) {
	if err := actionlock.AssertActionLockHeld(ctx); err != nil {
		return nil, err
	}

	configPath, err := e.requireConfigPath()
	if err != nil {
		return nil, err
	}

	// List backups before expiration to detect what gets removed
	beforeBackups, err := e.ListBackups(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list backups before expire: %w", err)
	}
	beforeIDs := make(map[string]struct{}, len(beforeBackups))
	for _, b := range beforeBackups {
		beforeIDs[b.BackupId] = struct{}{}
	}

	ctx, cancel := context.WithTimeout(ctx, commonbackup.ExpireTimeout)
	defer cancel()

	args := []string{
		"--stanza=" + stanzaName,
		"--config=" + configPath,
		"expire",
	}

	args = commonbackup.ApplyPgBackRestOverrides(args, overrides)

	cmd := e.pgbackrestCmd(ctx, args...)

	var output []byte
	err = telemetry.WithSpan(ctx, "expire-backups", func(ctx context.Context) error {
		var runErr error
		output, runErr = e.run(ctx, cmd, "pgbackrest expire")
		return runErr
	})
	if err != nil {
		return nil, fmt.Errorf("pgbackrest expire failed: %w\nOutput: %s", err, string(output))
	}

	// List backups after expiration to determine what was removed
	afterBackups, err := e.ListBackups(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list backups after expire: %w", err)
	}
	afterIDs := make(map[string]struct{}, len(afterBackups))
	for _, b := range afterBackups {
		afterIDs[b.BackupId] = struct{}{}
	}

	// Compute expired IDs: in before but not in after
	var expiredIDs []string
	for id := range beforeIDs {
		if _, ok := afterIDs[id]; !ok {
			expiredIDs = append(expiredIDs, id)
		}
	}

	e.logger.InfoContext(ctx, "Backup expiration completed",
		"expired_backup_ids", expiredIDs,
		"expired_count", len(expiredIDs))
	return expiredIDs, nil
}
