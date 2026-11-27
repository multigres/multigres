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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/tools/executil"
)

// A failed pgctld build must not copy a previous installation over working WAL commands.
func TestMakeDoesNotPublishFailedPgctldBuild(t *testing.T) {
	tools := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(tools, "go"), []byte(`#!/bin/sh
case "$*" in
  'env GOBIN') echo "$GOBIN";;
  *./go/cmd/pgctld*) exit 1;;
esac
`), 0o700))
	t.Setenv("PATH", tools+string(os.PathListSeparator)+os.Getenv("PATH"))
	for _, target := range []string{"build", "build-coverage", "build-release", "install"} {
		t.Run(target, func(t *testing.T) {
			bin := t.TempDir()
			t.Setenv("GOBIN", bin)
			dest := bin
			if target == "build-coverage" {
				dest = filepath.Join(bin, "cov")
				require.NoError(t, os.Mkdir(dest, 0o700))
			}
			require.NoError(t, os.WriteFile(filepath.Join(dest, "pgctld"), []byte("old"), 0o700))
			for _, name := range []string{backup.ArchiveWALExecutable, backup.RestoreWALExecutable} {
				require.NoError(t, os.WriteFile(filepath.Join(dest, name), []byte("working"), 0o700))
			}
			out, err := executil.Command(t.Context(), "make", "-C", "../../..", target, "CMDS=pgctld minigres", "BIN_DIR="+bin).CombinedOutput()
			require.Error(t, err, "%s", out)
			for _, name := range []string{backup.ArchiveWALExecutable, backup.RestoreWALExecutable} {
				got, err := os.ReadFile(filepath.Join(dest, name))
				require.NoError(t, err)
				require.Equal(t, "working", string(got))
			}
		})
	}
}
