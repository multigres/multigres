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

package manager

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/safefs"
	"github.com/multigres/multigres/go/common/sandbox"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func volumeRootTestManager(t *testing.T, volume, pooler string) *MultipoolerManager {
	t.Helper()
	pm := newTestManager(t, withRecord(newRecordFromProto(&clustermetadatapb.Multipooler{PoolerDir: pooler})))
	pm.config = &Config{DataVolumeRoot: volume, DataVolumeAnchor: sandbox.AnchorDir}
	pm.volumeRoot = pm.openSidecarVolumeRoot
	return pm
}

func TestSidecarVolumeRoot_Layouts(t *testing.T) {
	for _, rel := range []string{".", "nested/pooler"} {
		t.Run(rel, func(t *testing.T) {
			volume := t.TempDir()
			pooler := filepath.Join(volume, rel)
			require.NoError(t, os.MkdirAll(pooler, 0o700))
			pm := volumeRootTestManager(t, volume, pooler)
			root, err := pm.openSidecarVolumeRoot()
			require.NoError(t, err)
			// The intermediate volume root has already been closed by the opener.
			require.NoError(t, root.WriteFile("held", []byte("x"), 0o600))
			require.NoError(t, root.Sync())
			require.NoError(t, root.Close())
			require.ErrorIs(t, root.Sync(), os.ErrClosed)
			require.NoError(t, pm.writeGenerationsCopy(1, []RepoGeneration{{Generation: 1, RepoNumber: 1}}))
			s := pm.restoreSentinel()
			require.NoError(t, s.write())
			require.NoError(t, s.remove())
			require.NoError(t, s.remove())
		})
	}
}

func TestSidecarVolumeRoot_RejectsSymlinkTraversal(t *testing.T) {
	for _, rel := range []string{"link", "link/pooler"} {
		t.Run(rel, func(t *testing.T) {
			volume, outside := t.TempDir(), t.TempDir()
			require.NoError(t, os.Mkdir(filepath.Join(outside, "pooler"), 0o700))
			require.NoError(t, os.Symlink(outside, filepath.Join(volume, "link")))
			pm := volumeRootTestManager(t, volume, filepath.Join(volume, rel))
			root, err := pm.openSidecarVolumeRoot()
			require.Error(t, err)
			assert.Nil(t, root)
			victimDir := outside
			if rel != "link" {
				victimDir = filepath.Join(outside, "pooler")
			}
			s := pm.restoreSentinel()
			for _, name := range []string{s.name, generationsCopyFile} {
				require.NoError(t, os.WriteFile(filepath.Join(victimDir, name), []byte("untouched"), 0o600))
			}
			require.Error(t, pm.writeGenerationsCopy(1, []RepoGeneration{{Generation: 1, RepoNumber: 1}}))
			require.Error(t, s.write())
			require.Error(t, s.remove())
			for _, name := range []string{s.name, generationsCopyFile} {
				data, err := os.ReadFile(filepath.Join(victimDir, name))
				require.NoError(t, err)
				assert.Equal(t, "untouched", string(data))
			}
		})
	}
}

func TestSidecarVolumeRoot_RejectsInvalidDirectories(t *testing.T) {
	volume := t.TempDir()
	for _, pooler := range []string{"", t.TempDir(), "relative", filepath.Join(volume, "missing")} {
		pm := volumeRootTestManager(t, volume, pooler)
		root, err := pm.openSidecarVolumeRoot()
		require.Error(t, err, pooler)
		assert.Nil(t, root)
	}
	pm := volumeRootTestManager(t, volume, volume)
	pm.config.DataVolumeAnchor = sandbox.AnchorMount
	_, err := pm.openSidecarVolumeRoot()
	require.Error(t, err, "an ordinary directory is not a production mount anchor")
}

func TestSentinelRemove_RootSyncSurvivesParentReplacement(t *testing.T) {
	for _, present := range []bool{true, false} {
		volume := t.TempDir()
		parent := filepath.Join(volume, "parent")
		pooler := filepath.Join(parent, "pooler")
		require.NoError(t, os.MkdirAll(pooler, 0o700))
		pm := volumeRootTestManager(t, volume, pooler)
		s := pm.restoreSentinel()
		if present {
			require.NoError(t, s.write())
		}
		moved := parent + ".moved"
		var held *safefs.Root
		testHookSentinelRootSync = func(root *safefs.Root) {
			held = root
			_, err := root.Stat(s.name)
			require.ErrorIs(t, err, os.ErrNotExist, "unlink precedes sync")
			require.NoError(t, os.Rename(parent, moved))
			// A file in place of the parent makes any absolute-path reopen fail.
			// A skipped Sync would pass here, but is killed by the closed-fd test.
			require.NoError(t, os.WriteFile(parent, []byte("replacement"), 0o600))
		}
		t.Cleanup(func() { testHookSentinelRootSync = nil })
		require.NoError(t, s.remove(), "sync must use the directory actually unlinked")
		require.ErrorIs(t, held.Sync(), os.ErrClosed)
		assert.NoFileExists(t, filepath.Join(moved, "pooler", s.name))
		testHookSentinelRootSync = nil
	}
}
