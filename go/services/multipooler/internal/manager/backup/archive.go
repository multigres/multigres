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
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
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

// RemoveArchiveConfig removes archive configuration lines from postgresql.auto.conf
// This is used after restore to remove the primary's archive config before applying the standby's config
func (e *Engine) RemoveArchiveConfig() error {
	autoConfPath, err := e.autoConfPath()
	if err != nil {
		return err
	}

	content, err := os.ReadFile(autoConfPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // File doesn't exist, nothing to remove
		}
		return fmt.Errorf("failed to read postgresql.auto.conf: %w", err)
	}

	var filtered []string
	for line := range strings.SplitSeq(string(content), "\n") {
		trimmed := strings.TrimSpace(line)
		// Skip archive-related lines
		if strings.HasPrefix(trimmed, "archive_mode") ||
			strings.HasPrefix(trimmed, "archive_command") ||
			trimmed == "# Archive mode for pgbackrest backups" {
			continue
		}
		filtered = append(filtered, line)
	}

	return os.WriteFile(autoConfPath, []byte(strings.Join(filtered, "\n")), 0o644)
}

// ConfigureArchiveMode configures archive_mode in postgresql.auto.conf for pgbackrest
// This must be called after InitDataDir but BEFORE starting PostgreSQL
func (e *Engine) ConfigureArchiveMode(ctx context.Context) error {
	configPath, err := e.requireConfigPath()
	if err != nil {
		return mterrors.Wrap(err, "failed to initialize pgbackrest")
	}

	cfg, err := e.requireBackupConfig()
	if err != nil {
		return err
	}

	// Check if pgbackrest config file exists before configuring archive mode
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			fmt.Sprintf("pgbackrest config file not found at %s - ensure pgctld generated the config successfully", configPath))
	}

	autoConfPath, err := e.autoConfPath()
	if err != nil {
		return err
	}

	// Check if archive_mode is already configured to avoid duplicates
	if _, err := os.Stat(autoConfPath); err == nil {
		content, err := os.ReadFile(autoConfPath)
		if err == nil && bytes.Contains(content, []byte("archive_mode")) {
			e.logger.InfoContext(ctx, "archive_mode already configured, skipping", "auto_conf", autoConfPath)
			return nil
		}
	}

	// Configure archive_mode in postgresql.auto.conf
	// Following the pattern from test/endtoend/multipooler/setup_test.go:479-498
	archiveConfig := fmt.Sprintf(`
# Archive mode for pgbackrest backups
archive_mode = 'on'
archive_command = 'pgbackrest --stanza=%s --config=%s archive-push %%p'
`, stanzaName, configPath)

	f, err := os.OpenFile(autoConfPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return mterrors.Wrap(err, "failed to open postgresql.auto.conf")
	}
	defer f.Close()

	if _, err := f.WriteString(archiveConfig); err != nil {
		return mterrors.Wrap(err, "failed to write archive config")
	}

	e.logger.InfoContext(ctx, "Configured archive_mode in postgresql.auto.conf", "config_path", configPath, "stanza", stanzaName, "backup_type", cfg.Type())
	return nil
}
