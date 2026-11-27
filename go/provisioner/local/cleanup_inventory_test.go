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

package local

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func inventoryProvisioner(t *testing.T, poolerDirs ...string) *localProvisioner {
	t.Helper()
	cells := map[string]CellServicesConfig{}
	for i, dir := range poolerDirs {
		cells[string(rune('a'+i))] = CellServicesConfig{Pgctld: PgctldConfig{PoolerDir: dir}}
	}
	return &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: t.TempDir(), Cells: cells}}
}

func TestInstallationAffected_ByOverlapWithTheDestructiveRoots(t *testing.T) {
	root := t.TempDir()
	assert.True(t, installationAffected(filepath.Join(root, "data", "pooler"), []string{root}), "a pooler below a root")
	assert.True(t, installationAffected(root, []string{filepath.Join(root, "data")}), "a pooler that contains a root")
	assert.False(t, installationAffected(filepath.Join(t.TempDir(), "pooler"), []string{root}), "an unrelated pooler")
}

// The inventory decides which sidecar installations a destructive cleanup must
// prove quiescent. Anything it cannot verify stops the cleanup.
func TestSidecarCleanupInventory_FailsClosedOnUnverifiableInstallations(t *testing.T) {
	t.Run("a sidecar directory that is not a directory", func(t *testing.T) {
		pooler := filepath.Join(t.TempDir(), "pooler")
		p := inventoryProvisioner(t, pooler)
		require.NoError(t, os.WriteFile(NewPgBackRestdPaths(pooler).Dir, nil, 0o600))

		_, err := p.sidecarCleanupInventoryForRoots(nil, p.cleanupRoots(), true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unverified sidecar directory")
	})

	t.Run("a pgctld record without its pooler directory", func(t *testing.T) {
		p := inventoryProvisioner(t)
		_, err := p.sidecarCleanupInventoryForRoots([]*LocalProvisionedService{{Service: "pgctld"}}, p.cleanupRoots(), true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "lacks pooler directory")
	})

	t.Run("a multipooler record without a data directory has no sidecar to prove", func(t *testing.T) {
		p := inventoryProvisioner(t)
		got, err := p.sidecarCleanupInventoryForRoots([]*LocalProvisionedService{{Service: "multipooler"}}, p.cleanupRoots(), true)
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("only affected installations are inventoried when not every configured one is", func(t *testing.T) {
		inside := filepath.Join(t.TempDir(), "inside")
		outside := filepath.Join(t.TempDir(), "outside")
		p := inventoryProvisioner(t, inside, outside)
		roots := []string{filepath.Dir(inside)}

		got, err := p.sidecarCleanupInventoryForRoots(nil, roots, false)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, NewPgBackRestdPaths(inside).Dir, got[0].Dir)

		all, err := p.sidecarCleanupInventoryForRoots(nil, roots, true)
		require.NoError(t, err)
		assert.Len(t, all, 2)
	})

	t.Run("a declared data directory is affected even if its state is missing", func(t *testing.T) {
		p := inventoryProvisioner(t)
		pooler := filepath.Join(p.getRootWorkingDir(), "data", "pooler")
		got, err := p.sidecarCleanupInventoryForRoots([]*LocalProvisionedService{{Service: "minigres", DataDir: pooler}}, p.cleanupRoots(), false)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, NewPgBackRestdPaths(pooler).Dir, got[0].Dir)
	})
}
