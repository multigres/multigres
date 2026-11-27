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
	"os"
	"path/filepath"
	"strings"

	commonbackup "github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/tools/fileutil"
)

// autoConfPath returns the path to postgresql.auto.conf in the PostgreSQL data
// directory, or an error if that directory was not configured at construction.
// Guarding here avoids filepath.Join("", ...) silently producing a relative
// path that would operate on the wrong file.
func (e *Engine) autoConfPath() (string, error) {
	if e.settings.PgDataDir == "" {
		return "", mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "postgres data directory not configured")
	}
	return filepath.Join(e.settings.PgDataDir, "postgresql.auto.conf"), nil
}

const archiveConfigHeader = "# Archive mode for pgbackrest backups"

// ConfigureArchiveMode configures archiving after InitDataDir and before
// starting PostgreSQL. It leaves an already-configured archive_mode alone.
func (e *Engine) ConfigureArchiveMode(ctx context.Context, wrappers bool) error {
	return e.configureWALCommands(ctx, wrappers, "", false)
}

// FinalizeRestore replaces the restored archive and recovery settings with
// commands for this pooler, before PostgreSQL can start. Both commands use the
// same freshly selected ABI and are made durable together.
func (e *Engine) FinalizeRestore(ctx context.Context, wrappers bool, poolerDir string) error {
	return e.configureWALCommands(ctx, wrappers, poolerDir, true)
}

func (e *Engine) configureWALCommands(ctx context.Context, wrappers bool, poolerDir string, afterRestore bool) error {
	configPath, err := e.requireConfigPath()
	if err != nil {
		return mterrors.Wrap(err, "failed to initialize pgbackrest")
	}
	cfg, err := e.requireBackupConfig()
	if err != nil {
		return err
	}
	configInfo, err := os.Stat(configPath)
	if err != nil {
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			fmt.Sprintf("failed to stat pgbackrest config file at %s: %v", configPath, err))
	}
	if !configInfo.Mode().IsRegular() {
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			"pgbackrest config is not a regular file: "+configPath)
	}

	path, err := e.autoConfPath()
	if err != nil {
		return err
	}
	var content []byte
	info, err := os.Lstat(path)
	switch {
	case err == nil:
		if !info.Mode().IsRegular() {
			return fmt.Errorf("cannot edit non-regular file %s", path)
		}
		content, err = os.ReadFile(path)
		if err != nil {
			return mterrors.Wrap(err, "failed to read postgresql.auto.conf")
		}
	case !os.IsNotExist(err) || afterRestore:
		// A standby restore writes its recovery settings here. If the file is
		// missing, fail rather than creating one without the rest of its config.
		return mterrors.Wrap(err, "failed to stat postgresql.auto.conf")
	}

	var updated strings.Builder
	for line := range strings.SplitAfterSeq(string(content), "\n") {
		trimmed := strings.TrimSpace(line)
		name := trimmed
		if end := strings.IndexAny(name, " \t="); end != -1 {
			name = name[:end]
		}
		switch strings.ToLower(name) {
		case "archive_mode":
			if !afterRestore {
				e.logger.InfoContext(ctx, "archive_mode already configured, skipping", "auto_conf", path)
				return nil
			}
			continue
		case "archive_command", "restore_command":
			if afterRestore {
				continue
			}
		}
		if afterRestore && trimmed == archiveConfigHeader {
			continue
		}
		if line != "" {
			updated.WriteString(line)
			if !strings.HasSuffix(line, "\n") {
				updated.WriteByte('\n')
			}
		}
	}

	archive, restore := commonbackup.WALCommands(wrappers, configPath, poolerDir)
	archiveValue, err := ast.QuoteConfValue(archive)
	if err != nil {
		return err
	}
	fmt.Fprintf(&updated, "%s\narchive_mode = 'on'\narchive_command = %s\n", archiveConfigHeader, archiveValue)
	if afterRestore {
		restoreValue, err := ast.QuoteConfValue(restore)
		if err != nil {
			return err
		}
		fmt.Fprintf(&updated, "restore_command = %s\n", restoreValue)
	}

	data := []byte(updated.String())
	if info != nil {
		err = fileutil.AtomicReplaceFile(path, data)
	} else {
		// Initdb normally provides auto.conf. If absent, use a private file
		// as initdb does by default.
		err = fileutil.AtomicWriteFile(path, data, 0o600)
	}
	if err != nil {
		return mterrors.Wrap(err, "failed to write postgresql.auto.conf")
	}
	e.logger.InfoContext(ctx, "configured WAL commands in postgresql.auto.conf", "config_path", configPath, "stanza", stanzaName, "backup_type", cfg.Type())
	return nil
}
