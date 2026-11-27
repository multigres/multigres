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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteFileAtomic_WritesTheWholeFileWithTheRequestedMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cert.pem")
	require.NoError(t, writeFileAtomic(path, []byte("pem"), 0o644))

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, "pem", string(got))
	fi, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o644), fi.Mode().Perm())
}

// A reader never sees a partial file, and a failed write leaves no temp file
// behind.
func TestWriteFileAtomic_FailureLeavesNoTempFile(t *testing.T) {
	dir := t.TempDir()

	t.Run("a destination directory that does not exist", func(t *testing.T) {
		require.Error(t, writeFileAtomic(filepath.Join(dir, "absent", "cert.pem"), []byte("pem"), 0o644))
	})

	t.Run("a destination that is a non-empty directory", func(t *testing.T) {
		dest := filepath.Join(dir, "dest")
		require.NoError(t, os.MkdirAll(filepath.Join(dest, "inner"), 0o755))
		require.Error(t, writeFileAtomic(dest, []byte("pem"), 0o644))

		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		for _, e := range entries {
			assert.NotContains(t, e.Name(), ".tmp-", "the temp file was removed")
		}
	})
}
