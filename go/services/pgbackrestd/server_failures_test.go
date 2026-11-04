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

package pgbackrestd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stoppedConfig returns the configuration of a sidecar that ran once and
// stopped, so its state directory exists and holds a minimal conf.
func stoppedConfig(t *testing.T) Config {
	t.Helper()
	first := newTestServer(t, nil)
	cfg := first.cfg
	require.NoError(t, first.Close())
	return cfg
}

func lockDir(t *testing.T, dir string) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	require.NoError(t, os.Chmod(dir, 0o500))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
}

// Every start-up failure returns an error and leaves nothing running: the
// sidecar gates postgres, so it must not come up half prepared.
func TestNewServer_FailsClosedOnAnUnusableStart(t *testing.T) {
	t.Run("an invalid configuration", func(t *testing.T) {
		s, err := newForTest(Config{})
		require.Error(t, err)
		assert.Nil(t, s)
	})

	t.Run("a state directory that cannot be created", func(t *testing.T) {
		cfg := testConfig(t)
		cfg.LocationType, cfg.FSRepoPath, cfg.S3CredSource, cfg.CipherKeyFile = LocationFilesystem, t.TempDir(), "", ""
		blocker := filepath.Join(t.TempDir(), "file")
		require.NoError(t, os.WriteFile(blocker, nil, 0o600))
		cfg.StateDir = filepath.Join(blocker, "state")
		s, err := newForTest(cfg)
		require.Error(t, err)
		assert.Nil(t, s)
	})

	t.Run("a restore fence that cannot be read", func(t *testing.T) {
		cfg := stoppedConfig(t)
		require.NoError(t, os.Mkdir(cfg.stateSub(restoreRecordFile), 0o700))
		s, err := newForTest(cfg)
		require.Error(t, err)
		assert.Nil(t, s)
	})

	t.Run("a state layout that cannot be completed", func(t *testing.T) {
		cfg := stoppedConfig(t)
		require.NoError(t, os.Remove(cfg.stateSub(restoreSpoolRel)))
		lockDir(t, cfg.StateDir)
		s, err := newForTest(cfg)
		require.Error(t, err)
		assert.Nil(t, s)
	})
}

func TestWriteMinimalConf_KeepsARenderedConf(t *testing.T) {
	cfg := stoppedConfig(t)
	rendered := []byte("[global]\nrepo1-path=/repo\n")
	require.NoError(t, os.WriteFile(cfg.stateSub(fileCommandConf), rendered, 0o600))

	require.NoError(t, writeMinimalConf(cfg))
	got, err := os.ReadFile(cfg.stateSub(fileCommandConf))
	require.NoError(t, err)
	assert.Equal(t, rendered, got, "a conf from an earlier Configure is not replaced")
}

func TestWriteMinimalConf_ReportsAConfDirectoryItCannotWriteTo(t *testing.T) {
	cfg := stoppedConfig(t)
	cfg.StateDir = filepath.Join(t.TempDir(), "no-such-state-dir")
	err := writeMinimalConf(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "minimal conf")
}
