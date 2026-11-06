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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/safefs"
	"github.com/multigres/multigres/go/common/sandbox"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// autoConfName is the file this package edits under PGDATA.
const autoConfName = "postgresql.auto.conf"

// OpenAutoConf opens a safefs.Root over the data volume and returns it with the
// path of postgresql.auto.conf beneath it. postgres can plant symlinks anywhere
// under PGDATA, so every read and write of the file goes through the Root.
//
// With volumeRoot set (the volume is shared with another container) it must be
// a mount root, and the Root is opened there with PGDATA reached as a relative
// path: safefs resolves the components below the root without following links,
// where opening PGDATA itself by name would follow a link or rename planted
// above it. With volumeRoot empty nothing is shared, so the Root is PGDATA.
func OpenAutoConf(pgDataDir, volumeRoot string, mode sandbox.AnchorMode) (*safefs.Root, string, error) {
	if pgDataDir == "" {
		return nil, "", mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "postgres data directory not configured")
	}
	if volumeRoot == "" {
		root, err := safefs.OpenRoot(pgDataDir)
		if err != nil {
			return nil, "", err
		}
		return root, autoConfName, nil
	}
	rel, err := filepath.Rel(volumeRoot, pgDataDir)
	if err != nil || rel == ".." || strings.HasPrefix(rel, "../") {
		return nil, "", mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			fmt.Sprintf("postgres data directory %q is not under the data volume %q", pgDataDir, volumeRoot))
	}
	if mode == sandbox.AnchorMount {
		anchor, err := sandbox.OpenAnchor(volumeRoot, mode)
		if err != nil {
			return nil, "", err
		}
		// The anchor only proves volumeRoot is a mount root at this instant. A
		// mount point cannot be renamed or replaced, so opening it by name next is
		// the same directory.
		_ = anchor.Close()
	}
	// AnchorDir asks only for a directory, which OpenRoot already requires, so
	// it skips the anchor: OpenAnchor is unsupported off Linux, where the local
	// provisioner and the tests run.
	root, err := safefs.OpenRoot(volumeRoot)
	if err != nil {
		return nil, "", err
	}
	return root, filepath.Join(rel, autoConfName), nil
}

func (e *Engine) openAutoConf() (*safefs.Root, string, error) {
	return OpenAutoConf(e.settings.PgDataDir, e.settings.DataVolumeRoot, e.settings.DataVolumeAnchor)
}

// readAutoConf returns the file's content, or exists=false when it is absent.
func readAutoConf(root *safefs.Root, rel string) (content []byte, exists bool, err error) {
	content, err = root.ReadFile(rel)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return content, true, nil
}

// ArchiveCommand returns the archive_command this pooler should have.
func (e *Engine) ArchiveCommand() string {
	return e.settings.ArchiveCommand()
}

// RemoveArchiveConfig removes archive configuration lines from postgresql.auto.conf
// This is used after restore to remove the primary's archive config before applying the standby's config
func (e *Engine) RemoveArchiveConfig() error {
	root, rel, err := e.openAutoConf()
	if err != nil {
		return err
	}
	defer root.Close()

	content, exists, err := readAutoConf(root, rel)
	if err != nil {
		return fmt.Errorf("failed to read postgresql.auto.conf: %w", err)
	}
	if !exists {
		return nil
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

	perm, err := AutoConfPerm(root, rel, 0o644)
	if err != nil {
		return err
	}
	return root.WriteFile(rel, []byte(strings.Join(filtered, "\n")), perm)
}

// ConfigureArchiveMode configures archive_mode in postgresql.auto.conf for pgbackrest
// This must be called after InitDataDir but BEFORE starting PostgreSQL
func (e *Engine) ConfigureArchiveMode(ctx context.Context) error {
	cfg, err := e.requireBackupConfig()
	if err != nil {
		return err
	}

	root, rel, err := e.openAutoConf()
	if err != nil {
		return err
	}
	defer root.Close()

	// Check if archive_mode is already configured to avoid duplicates
	existing, _, err := readAutoConf(root, rel)
	if err == nil && bytes.Contains(existing, []byte("archive_mode")) {
		e.logger.InfoContext(ctx, "archive_mode already configured, skipping", "auto_conf", rel)
		return nil
	}
	if err != nil {
		return mterrors.Wrap(err, "failed to read postgresql.auto.conf")
	}

	// Quote exactly as the normalisation pass does, and refuse a value that
	// would end the line: an operator-supplied path must not inject a setting.
	command := e.ArchiveCommand()
	if strings.ContainsRune(command, 0) {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "archive_command must not contain a NUL byte")
	}
	quotedCommand, err := ast.QuoteConfValue(command)
	if err != nil {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, err.Error())
	}

	// Configure archive_mode in postgresql.auto.conf
	// Following the pattern from test/endtoend/multipooler/setup_test.go:479-498
	archiveConfig := fmt.Sprintf(`
# Archive mode for pgbackrest backups
archive_mode = 'on'
archive_command = %s
`, quotedCommand)

	perm, err := AutoConfPerm(root, rel, 0o644)
	if err != nil {
		return mterrors.Wrap(err, "failed to write archive config")
	}
	if err := root.WriteFile(rel, append(existing, archiveConfig...), perm); err != nil {
		return mterrors.Wrap(err, "failed to write archive config")
	}

	e.logger.InfoContext(ctx, "configured archive_mode in postgresql.auto.conf", "stanza", stanzaName, "backup_type", cfg.Type())
	return nil
}

// AutoConfPerm keeps the mode of an existing postgresql.auto.conf across the
// temp-file-and-rename write, which would otherwise reset it.
//
// safefs offers no stat on the descriptor that was read, so the mode comes from
// a second lookup that could race a planted symlink (which would read as 0777).
// A non-regular result therefore fails the write instead of lending its mode.
func AutoConfPerm(root *safefs.Root, rel string, fallback os.FileMode) (os.FileMode, error) {
	info, err := root.Stat(rel)
	if errors.Is(err, os.ErrNotExist) {
		return fallback, nil
	}
	if err != nil {
		return 0, err
	}
	if !info.Mode().IsRegular() {
		return 0, fmt.Errorf("postgresql.auto.conf is not a regular file (mode %v)", info.Mode())
	}
	return info.Mode().Perm(), nil
}
