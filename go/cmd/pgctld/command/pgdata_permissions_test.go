// Copyright 2025 Supabase, Inc.
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

package command

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnsurePGDATAPermissions(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	t.Run("0700 is a no-op", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.Chmod(dir, 0o700))

		err := ensurePGDATAPermissions(logger, dir)
		require.NoError(t, err)

		info, err := os.Stat(dir)
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0o700), info.Mode().Perm())
	})

	t.Run("setgid 2700 becomes 0700", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.Chmod(dir, 0o2700))

		err := ensurePGDATAPermissions(logger, dir)
		require.NoError(t, err)

		info, err := os.Stat(dir)
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0o700), info.Mode().Perm())
		assert.Zero(t, info.Mode()&os.ModeSetgid, "setgid bit should be cleared")
	})

	t.Run("0750 becomes 0700", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.Chmod(dir, 0o750))

		err := ensurePGDATAPermissions(logger, dir)
		require.NoError(t, err)

		info, err := os.Stat(dir)
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0o700), info.Mode().Perm())
	})

	t.Run("0777 becomes 0700", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.Chmod(dir, 0o777))

		err := ensurePGDATAPermissions(logger, dir)
		require.NoError(t, err)

		info, err := os.Stat(dir)
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0o700), info.Mode().Perm())
	})

	t.Run("UID mismatch returns error", func(t *testing.T) {
		if os.Geteuid() != 0 {
			t.Skip("UID mismatch test requires root to create dirs owned by another user")
		}
	})

	t.Run("path does not exist returns error", func(t *testing.T) {
		err := ensurePGDATAPermissions(logger, "/nonexistent/path/pgdata")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to stat PGDATA")
	})

	t.Run("path is a file not directory returns error", func(t *testing.T) {
		f := filepath.Join(t.TempDir(), "not-a-dir")
		require.NoError(t, os.WriteFile(f, []byte("data"), 0o700))

		err := ensurePGDATAPermissions(logger, f)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "is not a directory")
	})
}
