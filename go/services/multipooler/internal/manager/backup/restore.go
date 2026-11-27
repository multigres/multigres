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
	"strings"

	commonbackup "github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// RestoreCommand returns the archive restore_command, or an error if the
// pgbackrest config has not been generated.
func (e *Engine) RestoreCommand(wrappers bool, dir string) (string, error) {
	config, err := e.requireConfigPath()
	if err != nil {
		return "", err
	}
	_, restore := commonbackup.WALCommands(wrappers, config, dir)
	return restore, nil
}

// Restore runs pgbackrest restore to recreate PGDATA from the given backup.
// The manager orchestrates the surrounding PG lifecycle (archive config,
// starting postgres, reopening the pooler).
func (e *Engine) Restore(ctx context.Context, backupID string, wrappers bool, dir string) error {
	defer e.listCache.Invalidate()
	configPath, err := e.requireConfigPath()
	if err != nil {
		return mterrors.Wrap(err, "pgbackrest config not found")
	}

	restoreCtx, cancel := context.WithTimeout(ctx, commonbackup.RestoreTimeout)
	defer cancel()

	_, restore := commonbackup.WALCommands(wrappers, configPath, dir)
	args := []string{
		"--stanza=" + stanzaName,
		"--config=" + configPath,
		"--type=standby",
		"--recovery-option=restore_command=" + strings.ReplaceAll(restore, "'", "''"),
	}

	if backupID != "" {
		args = append(args, "--set="+backupID)
	}

	args = append(args, "restore")

	cmd := e.pgbackrestCmd(restoreCtx, args...)
	e.health.RestoreStarted()
	defer e.health.RestoreFinished()
	output, err := safeCombinedOutput(cmd)
	if err != nil {
		return mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("pgbackrest restore failed: %v\nOutput: %s", err, output))
	}

	return nil
}
