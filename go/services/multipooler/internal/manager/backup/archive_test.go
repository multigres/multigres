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

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/sandbox"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

func TestOpenAutoConf(t *testing.T) {
	t.Run("errors when PgDataDir is not configured", func(t *testing.T) {
		_, _, err := OpenAutoConf("", "", sandbox.AnchorMount)
		require.Error(t, err)
		assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
	})

	t.Run("rejects a data dir outside the volume root", func(t *testing.T) {
		_, _, err := OpenAutoConf("/elsewhere/pg", t.TempDir(), sandbox.AnchorDir)
		require.Error(t, err)
		assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
	})

	t.Run("rejects a volume root that is not a mount point", func(t *testing.T) {
		vol := t.TempDir()
		require.NoError(t, os.Mkdir(filepath.Join(vol, "pg"), 0o700))
		_, _, err := OpenAutoConf(filepath.Join(vol, "pg"), vol, sandbox.AnchorMount)
		require.Error(t, err)
	})

	t.Run("reaches the file by a path relative to the volume root", func(t *testing.T) {
		vol := t.TempDir()
		require.NoError(t, os.Mkdir(filepath.Join(vol, "pg"), 0o700))
		root, rel, err := OpenAutoConf(filepath.Join(vol, "pg"), vol, sandbox.AnchorDir)
		require.NoError(t, err)
		defer root.Close()
		assert.Equal(t, filepath.Join("pg", "postgresql.auto.conf"), rel)
	})
}

// The relative walk from the volume root is what refuses a symlinked
// directory above PGDATA; rooting at PGDATA by name would follow it.
func TestOpenAutoConf_RefusesSymlinkedParentOfPgData(t *testing.T) {
	vol := t.TempDir()
	decoy := filepath.Join(t.TempDir(), "decoy")
	require.NoError(t, os.MkdirAll(filepath.Join(decoy, "pg"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(decoy, "pg", "postgresql.auto.conf"), []byte("planted\n"), 0o600))
	require.NoError(t, os.Symlink(decoy, filepath.Join(vol, "sub")))

	root, rel, err := OpenAutoConf(filepath.Join(vol, "sub", "pg"), vol, sandbox.AnchorDir)
	require.NoError(t, err)
	defer root.Close()
	_, err = root.ReadFile(rel)
	require.Error(t, err, "a symlinked component below the volume root must be refused")
	require.Error(t, root.WriteFile(rel, []byte("x"), 0o600))
	b, err := os.ReadFile(filepath.Join(decoy, "pg", "postgresql.auto.conf"))
	require.NoError(t, err)
	assert.Equal(t, "planted\n", string(b))
}

func TestAutoConfPerm_RejectsNonRegular(t *testing.T) {
	dir := t.TempDir()
	root, rel, err := OpenAutoConf(dir, "", sandbox.AnchorDir)
	require.NoError(t, err)
	defer root.Close()
	perm, err := AutoConfPerm(root, rel, 0o644)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o644), perm, "absent file takes the fallback")
	require.NoError(t, os.Symlink("/etc/hosts", filepath.Join(dir, "postgresql.auto.conf")))
	_, err = AutoConfPerm(root, rel, 0o644)
	require.Error(t, err)
}

// A line break in an operator-supplied path must not end the archive_command
// line and start a setting of the attacker's choosing.
func TestConfigureArchiveMode_RejectsLineBreakInCommand(t *testing.T) {
	for _, bad := range []string{"\n", "\r"} {
		poolerDir := t.TempDir()
		e, _ := newTestEngine(t, poolerDir, "test-tg", "0", "/tmp/backups")
		e.settings.ArchiveCommand = func() string { return "pg" + bad + "archive_command = 'evil'" }

		require.Error(t, e.ConfigureArchiveMode(context.Background()), "%q", bad)
		_, err := os.Stat(filepath.Join(poolerDir, "postgresql.auto.conf"))
		assert.True(t, os.IsNotExist(err), "nothing is written")
	}
}

// A NUL cannot be in a file name, so it can only arrive through a renderer.
func TestConfigureArchiveMode_RejectsNULInCommand(t *testing.T) {
	poolerDir := t.TempDir()
	e, _ := newTestEngine(t, poolerDir, "test-tg", "0", "/tmp/backups")
	e.settings.ArchiveCommand = func() string { return "pgbackrest\x00x" }
	require.Error(t, e.ConfigureArchiveMode(context.Background()))
	_, err := os.Stat(filepath.Join(poolerDir, "postgresql.auto.conf"))
	assert.True(t, os.IsNotExist(err))
}

func TestConfigureArchiveMode_QuotesBackslash(t *testing.T) {
	poolerDir := t.TempDir()
	e, _ := newTestEngine(t, poolerDir, "test-tg", "0", "/tmp/backups")
	e.settings.ArchiveCommand = func() string { return `a\b'c.conf` }
	require.NoError(t, e.ConfigureArchiveMode(context.Background()))
	b, err := os.ReadFile(filepath.Join(poolerDir, "postgresql.auto.conf"))
	require.NoError(t, err)
	assert.Contains(t, string(b), `a\\b''c.conf`)
}

// postgres can plant a symlink at postgresql.auto.conf, which resolves to the
// pooler's own files; the writers must fail rather than follow it.
func TestAutoConfWriters_RefuseSymlink(t *testing.T) {
	ctx := context.Background()
	poolerDir := t.TempDir()
	e, _ := newTestEngine(t, poolerDir, "test-tg", "0", "/tmp/backups")

	outside := filepath.Join(t.TempDir(), "secret")
	const secret = "-----secret-----\n"
	require.NoError(t, os.WriteFile(outside, []byte(secret), 0o600))
	require.NoError(t, os.Symlink(outside, filepath.Join(poolerDir, "postgresql.auto.conf")))

	require.Error(t, e.ConfigureArchiveMode(ctx))
	require.Error(t, e.RemoveArchiveConfig())

	b, err := os.ReadFile(outside)
	require.NoError(t, err)
	assert.Equal(t, secret, string(b))
}

func TestEngineCommandRenderers(t *testing.T) {
	poolerDir := t.TempDir()
	e := NewEngine(slog.Default(), &fakeIdentity{}, Settings{
		PgDataDir:      poolerDir,
		ArchiveCommand: func() string { return "pgbackrest-sidecar-archive 'x'" },
		RestoreCommand: func(pid string) string { return "restore " + pid },
	})
	assert.Equal(t, "pgbackrest-sidecar-archive 'x'", e.ArchiveCommand())
	assert.Equal(t, "restore /pool/"+constants.RestoreCommandPIDFile, e.RestoreCommand("/pool"))
}

func TestConfigureArchiveMode(t *testing.T) {
	ctx := context.Background()

	t.Run("writes archive config and is idempotent", func(t *testing.T) {
		poolerDir := t.TempDir()
		e, _ := newTestEngine(t, poolerDir, "test-tg", "0", "/tmp/backups")

		require.NoError(t, e.ConfigureArchiveMode(ctx))

		autoConf := filepath.Join(poolerDir, "postgresql.auto.conf")
		content, err := os.ReadFile(autoConf)
		require.NoError(t, err)
		assert.Contains(t, string(content), "archive_mode = 'on'")
		assert.Contains(t, string(content), "archive_command = 'archive-command'")

		// A second call detects the existing archive_mode and skips, so the
		// config is not appended twice.
		require.NoError(t, e.ConfigureArchiveMode(ctx))
		content, err = os.ReadFile(autoConf)
		require.NoError(t, err)
		assert.Equal(t, 1, strings.Count(string(content), "archive_mode = 'on'"))
	})

	t.Run("errors when backup config is not set", func(t *testing.T) {
		poolerDir := t.TempDir()
		e, _ := newTestEngine(t, poolerDir, "test-tg", "0", "") // no backup location → no repo config
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
		e := NewEngine(slog.Default(), &fakeIdentity{}, Settings{})
		err := e.RemoveArchiveConfig()
		require.Error(t, err)
		assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
	})
}
