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

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/tools/executil"
)

func TestMain(m *testing.M) {
	switch filepath.Base(os.Args[0]) {
	case backup.ArchiveWALExecutable, backup.RestoreWALExecutable:
		main()
		return
	}
	os.Exit(m.Run()) //nolint:forbidigo // TestMain returns the suite status.
}

func TestInstalledArchiveSurvivesPgctldRollback(t *testing.T) {
	dir := t.TempDir()
	ctl := filepath.Join(dir, "pgctld")
	self, err := os.Executable()
	require.NoError(t, err)
	require.NoError(t, executil.Command(t.Context(), "cp", "-p", self, ctl).Run())
	for _, name := range []string{backup.ArchiveWALExecutable, backup.RestoreWALExecutable} {
		require.NoError(t, os.Symlink("pgctld", filepath.Join(dir, name)))
	}
	require.NoError(t, executil.Command(t.Context(), "../../../tools/install-wal-commands.sh", dir).Run())
	// Our installer uses independent copies, so even an in-place overwrite is safe.
	require.NoError(t, os.WriteFile(ctl, []byte("#!/bin/sh\nexit 0\n"), 0o755))
	t.Setenv(backup.WALPoolerDirEnv, dir)
	t.Setenv(backup.WALBinDirEnv, dir)
	t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
	file := filepath.Join(dir, "segment")
	require.NoError(t, os.WriteFile(file, []byte("WAL contents"), 0o600))
	dest := filepath.Join(dir, "archived")
	t.Setenv("ARCHIVED", dest)
	t.Setenv("WAL_EXIT", "37")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "pgbackrest"), []byte(`#!/bin/sh
[ "$3" = --reset-archive-push-queue-max ] && [ "$4" = archive-push ] || exit 2
[ "$WAL_EXIT" = 0 ] || exit "$WAL_EXIT"
cp "$5" "$ARCHIVED"
`), 0o755))
	push := func() error {
		return executil.Command(t.Context(), "sh", "-c", strings.ReplaceAll(backup.ArchiveWALCommand, "%p", "$1"), "sh", file).Run()
	}
	var exit *exec.ExitError
	require.ErrorAs(t, push(), &exit)
	require.Equal(t, 37, exit.ExitCode())
	require.NoFileExists(t, dest)
	t.Setenv("WAL_EXIT", "0")
	require.NoError(t, push())
	got, err := os.ReadFile(dest)
	require.NoError(t, err)
	require.Equal(t, "WAL contents", string(got))
}
