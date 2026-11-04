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

package safefs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// lockedDir makes dir unwritable for the rest of the test. A process that may
// write anywhere (root) cannot show the refusal, so the test is skipped for it.
func lockedDir(t *testing.T, dir string) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	require.NoError(t, os.Chmod(dir, 0o500))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
}

// A removal the kernel refuses is reported, not swallowed: a caller that wipes
// a directory before a restore must learn that the wipe did not happen.
func TestRemoveAll_ReportsARefusedUnlink(t *testing.T) {
	r, dir := newRoot(t)
	sub := filepath.Join(dir, "sub")
	require.NoError(t, os.Mkdir(sub, 0o700))
	file := filepath.Join(sub, "keep")
	require.NoError(t, os.WriteFile(file, []byte("x"), 0o600))
	lockedDir(t, sub)

	err := r.RemoveAll("sub")
	require.Error(t, err)
	assert.FileExists(t, file)
}

func TestRemoveAll_ReportsARefusedDirectoryRemoval(t *testing.T) {
	r, dir := newRoot(t)
	parent := filepath.Join(dir, "parent")
	child := filepath.Join(parent, "empty")
	require.NoError(t, os.MkdirAll(child, 0o700))
	lockedDir(t, parent)

	require.Error(t, r.RemoveAll("parent/empty"))
	assert.DirExists(t, child)
}

func TestWriteFile_ReportsAnUnwritableDirectoryAndLeavesNothing(t *testing.T) {
	r, dir := newRoot(t)
	sub := filepath.Join(dir, "sub")
	require.NoError(t, os.Mkdir(sub, 0o700))
	lockedDir(t, sub)

	require.Error(t, r.WriteFile("sub/f", []byte("x"), 0o600))
	entries, err := os.ReadDir(sub)
	require.NoError(t, err)
	assert.Empty(t, entries, "no temp file is left behind")
}
