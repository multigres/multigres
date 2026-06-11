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
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
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
	ctx := context.Background()

	t.Run("writes archive config and is idempotent", func(t *testing.T) {
		poolerDir := t.TempDir()
		configPath := setupMockPgBackRestConfig(t, poolerDir)
		e, _ := newTestEngine(t, poolerDir, "test-tg", "0", "/tmp/backups")
		e.SetConfigPath(configPath)

		require.NoError(t, e.ConfigureArchiveMode(ctx))

		autoConf := filepath.Join(poolerDir, "postgresql.auto.conf")
		content, err := os.ReadFile(autoConf)
		require.NoError(t, err)
		assert.Contains(t, string(content), "archive_mode = 'on'")
		assert.Contains(t, string(content), "archive_command = 'pgbackrest --stanza="+stanzaName+" --config="+configPath)

		// A second call detects the existing archive_mode and skips, so the
		// config is not appended twice.
		require.NoError(t, e.ConfigureArchiveMode(ctx))
		content, err = os.ReadFile(autoConf)
		require.NoError(t, err)
		assert.Equal(t, 1, strings.Count(string(content), "archive_mode = 'on'"))
	})

	t.Run("errors when config path is not set", func(t *testing.T) {
		poolerDir := t.TempDir()
		e, _ := newTestEngine(t, poolerDir, "test-tg", "0", "/tmp/backups")
		err := e.ConfigureArchiveMode(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not yet generated")
	})

	t.Run("errors when backup config is not set", func(t *testing.T) {
		poolerDir := t.TempDir()
		configPath := setupMockPgBackRestConfig(t, poolerDir)
		e, _ := newTestEngine(t, poolerDir, "test-tg", "0", "") // no backup location → no repo config
		e.SetConfigPath(configPath)
		err := e.ConfigureArchiveMode(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "backup config not set")
	})
}

func TestRemoveArchiveConfig(t *testing.T) {
	t.Run("removes only the archive lines", func(t *testing.T) {
		poolerDir := t.TempDir()
		e, _ := newTestEngine(t, poolerDir, "test-tg", "0", "/tmp/backups")

		autoConf := filepath.Join(poolerDir, "postgresql.auto.conf")
		original := strings.Join([]string{
			"shared_buffers = '128MB'",
			"# Archive mode for pgbackrest backups",
			"archive_mode = 'on'",
			"archive_command = 'pgbackrest --stanza=multigres --config=/x archive-push %p'",
			"max_connections = 100",
			"",
		}, "\n")
		require.NoError(t, os.WriteFile(autoConf, []byte(original), 0o644))

		require.NoError(t, e.RemoveArchiveConfig())

		content, err := os.ReadFile(autoConf)
		require.NoError(t, err)
		s := string(content)
		assert.NotContains(t, s, "archive_mode")
		assert.NotContains(t, s, "archive_command")
		assert.NotContains(t, s, "# Archive mode for pgbackrest backups")
		assert.Contains(t, s, "shared_buffers = '128MB'")
		assert.Contains(t, s, "max_connections = 100")
	})

	t.Run("no-op when the file is missing", func(t *testing.T) {
		poolerDir := t.TempDir()
		e, _ := newTestEngine(t, poolerDir, "test-tg", "0", "/tmp/backups")
		require.NoError(t, e.RemoveArchiveConfig())
	})

	t.Run("errors when PgDataDir is not configured", func(t *testing.T) {
		e := NewEngine(slog.Default(), nil, &fakeIdentity{}, Settings{})
		err := e.RemoveArchiveConfig()
		require.Error(t, err)
		assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
	})
}
