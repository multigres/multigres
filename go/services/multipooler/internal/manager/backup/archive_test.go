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
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonbackup "github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

func TestAutoConfPath(t *testing.T) {
	t.Run("errors when PgDataDir is not configured", func(t *testing.T) {
		e := NewEngine(slog.Default(), nil, &fakeIdentity{}, Settings{})
		_, err := e.autoConfPath()
		require.Error(t, err)
		assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
	})

	t.Run("joins PgDataDir when configured", func(t *testing.T) {
		e := NewEngine(slog.Default(), nil, &fakeIdentity{}, Settings{PgDataDir: "/var/lib/pg"})
		path, err := e.autoConfPath()
		require.NoError(t, err)
		assert.Equal(t, "/var/lib/pg/postgresql.auto.conf", path)
	})
}

func TestConfigureArchiveMode(t *testing.T) {
	t.Run("writes archive config and is idempotent", func(t *testing.T) {
		poolerDir := t.TempDir()
		configPath := setupMockPgBackRestConfig(t, poolerDir)
		e, _ := newTestEngine(t, poolerDir, "test-tg", "0", "/tmp/backups")
		e.SetConfigPath(configPath)

		require.NoError(t, e.ConfigureArchiveMode(t.Context(), true))

		autoConf := filepath.Join(poolerDir, "postgresql.auto.conf")
		content, err := os.ReadFile(autoConf)
		require.NoError(t, err)
		assert.Contains(t, string(content), "archive_mode = 'on'")
		archiveValue, err := ast.QuoteConfValue(commonbackup.ArchiveWALCommand)
		require.NoError(t, err)
		assert.Contains(t, string(content), "archive_command = "+archiveValue)

		// A second call detects the existing archive_mode and skips, so the
		// config is not appended twice.
		require.NoError(t, e.ConfigureArchiveMode(t.Context(), true))
		content, err = os.ReadFile(autoConf)
		require.NoError(t, err)
		assert.Equal(t, 1, strings.Count(string(content), "archive_mode = 'on'"))
	})

	t.Run("legacy when wrapper capability is absent", func(t *testing.T) {
		dir := t.TempDir()
		config := setupMockPgBackRestConfig(t, dir)
		e, _ := newTestEngine(t, dir, "tg", "0", "/tmp/backups")
		e.SetConfigPath(config)
		require.NoError(t, e.ConfigureArchiveMode(t.Context(), false))
		got, err := os.ReadFile(filepath.Join(dir, "postgresql.auto.conf"))
		require.NoError(t, err)
		require.Contains(t, string(got), "archive_command = 'pgbackrest --stanza=multigres --config="+config+" archive-push %p'")
	})
	t.Run("errors when config path is not set", func(t *testing.T) {
		poolerDir := t.TempDir()
		e, _ := newTestEngine(t, poolerDir, "test-tg", "0", "/tmp/backups")
		err := e.ConfigureArchiveMode(t.Context(), true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not yet generated")
	})

	t.Run("errors when backup config is not set", func(t *testing.T) {
		poolerDir := t.TempDir()
		configPath := setupMockPgBackRestConfig(t, poolerDir)
		e, _ := newTestEngine(t, poolerDir, "test-tg", "0", "") // no backup location → no repo config
		e.SetConfigPath(configPath)
		err := e.ConfigureArchiveMode(t.Context(), true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "backup config not set")
	})
}

func TestFinalizeRestore(t *testing.T) {
	const original = "archive_mode = 'off'\n" + archiveConfigHeader + "\n" +
		"archive_command = 'wrong pooler'\nARCHIVE_COMMAND 'another pooler'\n" +
		"restore_command = 'wrong restore command'\nrestore_command = 'old wrapper'\n" +
		"# restore_command = 'comment'\narchive_timeout = '5min'\nshared_buffers = '128MB'"
	const kept = "# restore_command = 'comment'\narchive_timeout = '5min'\nshared_buffers = '128MB'\n"

	for _, tc := range []struct {
		name     string
		wrappers bool
	}{{"standalone wrappers", true}, {"legacy commands", false}} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			configPath := setupMockPgBackRestConfig(t, dir)
			e, _ := newTestEngine(t, dir, "tg", "0", "/tmp/backups")
			e.SetConfigPath(configPath)
			path := filepath.Join(dir, "postgresql.auto.conf")
			require.NoError(t, os.WriteFile(path, []byte(original), 0o640))
			before, err := os.Stat(path)
			require.NoError(t, err)

			require.NoError(t, e.FinalizeRestore(t.Context(), tc.wrappers, dir))
			got, err := os.ReadFile(path)
			require.NoError(t, err)
			archive, restore := commonbackup.WALCommands(tc.wrappers, configPath, dir)
			archiveValue, err := ast.QuoteConfValue(archive)
			require.NoError(t, err)
			restoreValue, err := ast.QuoteConfValue(restore)
			require.NoError(t, err)
			assert.Equal(t, kept+archiveConfigHeader+"\narchive_mode = 'on'\narchive_command = "+archiveValue+"\nrestore_command = "+restoreValue+"\n", string(got))
			after, err := os.Stat(path)
			require.NoError(t, err)
			assert.False(t, os.SameFile(before, after), "must replace atomically, not truncate in place")
			assert.Equal(t, before.Mode(), after.Mode())
			assert.Equal(t, before.Sys().(*syscall.Stat_t).Uid, after.Sys().(*syscall.Stat_t).Uid)
			assert.Equal(t, before.Sys().(*syscall.Stat_t).Gid, after.Sys().(*syscall.Stat_t).Gid)
		})
	}

	t.Run("invalid replacement keeps original file", func(t *testing.T) {
		dir := t.TempDir()
		configPath := setupMockPgBackRestConfig(t, dir)
		e, _ := newTestEngine(t, dir, "tg", "0", "/tmp/backups")
		e.SetConfigPath(configPath)
		path := filepath.Join(dir, "postgresql.auto.conf")
		require.NoError(t, os.WriteFile(path, []byte(original), 0o600))
		require.ErrorContains(t, e.FinalizeRestore(t.Context(), false, "invalid\npath"), "must not contain a line break")
		got, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Equal(t, original, string(got))
	})
}
