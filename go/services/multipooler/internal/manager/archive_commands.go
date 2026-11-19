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
	"errors"
	"fmt"
	"os"
	"strings"

	commonbackup "github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/sandbox"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/backup"
)

// DefaultDataVolumeRoot is the mount root of the pooler data volume in a pod.
const DefaultDataVolumeRoot = "/var/lib/pooler"

func (pm *MultipoolerManager) dataVolumeAnchor() sandbox.AnchorMode {
	if pm.config == nil {
		return sandbox.AnchorMount
	}
	return pm.config.DataVolumeAnchor
}

// dataVolumeRoot is the mount root that postgresql.auto.conf is reached
// through: the volume is shared with the sidecar.
func (pm *MultipoolerManager) dataVolumeRoot() string {
	if pm.config != nil && pm.config.DataVolumeRoot != "" {
		return pm.config.DataVolumeRoot
	}
	return DefaultDataVolumeRoot
}

// backupEngineSettings builds the engine settings.
func (pm *MultipoolerManager) backupEngineSettings() backup.Settings {
	walSocket := pm.config.PgBackRestWALSocket
	return backup.Settings{
		PgDataDir:      postgresDataDir(),
		DataVolumeRoot: pm.dataVolumeRoot(),
		// A static leader is its shard's only pooler, so its action lock
		// already excludes every other backup.
		NoBackupLease:    pm.config.StaticLeader,
		DataVolumeAnchor: pm.dataVolumeAnchor(),
		ArchiveCommand: func() string {
			return commonbackup.SidecarArchiveCommand(walSocket)
		},
		RestoreCommand: func(pidFile string) string {
			return commonbackup.SidecarRestoreCommand(walSocket, pidFile)
		},
	}
}

// isOurArchiveCommand reports whether an auto.conf archive_command line is one
// this package renders, including the in-container form a data directory
// carries from before the sidecar. A command an operator wrote by hand is left
// alone.
func isOurArchiveCommand(line string) bool {
	return strings.Contains(line, "pgbackrest") &&
		(strings.Contains(line, "archive-push") || strings.Contains(line, "http://pgbackrest/wal/"))
}

func isOurRestoreCommand(line string) bool {
	return strings.Contains(line, "pgctld restore-wrapper")
}

// normalizeArchiveCommands rewrites archive_command and restore_command in
// postgresql.auto.conf into the sidecar form for this pooler. pg_rewind and
// restore leave a copy written for another pooler, and a data directory written
// by a pod that ran pgBackRest in-process carries the in-container commands.
//
// It only rewrites a setting that is present, and never adds one: demote,
// resign and the cohort paths remove restore_command on purpose, so a
// normalising pass that re-added it would let a cohort member replay from the
// archive. Commands it did not render are left as they are.
func (pm *MultipoolerManager) normalizeArchiveCommands(ctx context.Context) error {
	if pm.backup == nil {
		return nil
	}
	pm.mu.Lock()
	poolerDir := pm.record.PoolerDir()
	pm.mu.Unlock()

	render := func(name string) string {
		if name == "archive_command" {
			return pm.backup.ArchiveCommand()
		}
		return pm.backup.RestoreCommand(poolerDir)
	}

	var renderErr error
	err := pm.editAutoConf(ctx, func(content string) (string, bool) {
		lines := strings.Split(content, "\n")
		changed := false
		for i, line := range lines {
			var name string
			switch {
			case autoConfLineSets(line, "archive_command") && isOurArchiveCommand(line):
				name = "archive_command"
			case autoConfLineSets(line, "restore_command") && isOurRestoreCommand(line):
				name = "restore_command"
			default:
				continue
			}
			quoted, qerr := ast.QuoteConfValue(render(name))
			if qerr != nil {
				renderErr = fmt.Errorf("rendering %s: %w", name, qerr)
				return content, false
			}
			entry := name + " = " + quoted
			if strings.TrimSpace(line) != entry {
				lines[i] = entry
				changed = true
			}
		}
		return strings.Join(lines, "\n"), changed
	})
	if renderErr != nil {
		return renderErr
	}
	if errors.Is(err, os.ErrNotExist) {
		// No auto.conf yet, so no command of ours to rewrite.
		return nil
	}
	return err
}
