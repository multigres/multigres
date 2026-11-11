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

package manager

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestCheckBackupRepositoryScope(t *testing.T) {
	newManager := func(poolerDir string) *MultipoolerManager {
		return &MultipoolerManager{logger: slog.New(slog.DiscardHandler), record: newRecordFromProto(&clustermetadatapb.Multipooler{PoolerDir: poolerDir})}
	}
	scopeFile := func(poolerDir string) string { return filepath.Join(poolerDir, "pgbackrest", backupScopeFile) }

	t.Run("the first start records the scope, and the same scope passes after", func(t *testing.T) {
		dir := t.TempDir()
		pm := newManager(dir)
		require.NoError(t, pm.checkBackupRepositoryScope("tablegroups/commerce/7/0-80"))
		data, err := os.ReadFile(scopeFile(dir))
		require.NoError(t, err)
		assert.Equal(t, "tablegroups/commerce/7/0-80", string(data))
		require.NoError(t, pm.checkBackupRepositoryScope("tablegroups/commerce/7/0-80"))
	})
	t.Run("a changed tablegroup id is refused", func(t *testing.T) {
		dir := t.TempDir()
		pm := newManager(dir)
		require.NoError(t, pm.checkBackupRepositoryScope("tablegroups/commerce/7/0-80"))
		err := pm.checkBackupRepositoryScope("tablegroups/commerce/8/0-80")
		require.Error(t, err)
		assert.Contains(t, err.Error(), `"tablegroups/commerce/7/0-80"`)
		assert.Contains(t, err.Error(), `"tablegroups/commerce/8/0-80"`)
		assert.Contains(t, err.Error(), scopeFile(dir), "the error says how to accept the change")
	})
	t.Run("an id appearing where there was none is refused", func(t *testing.T) {
		dir := t.TempDir()
		pm := newManager(dir)
		require.NoError(t, pm.checkBackupRepositoryScope("tablegroups/commerce/0-80"))
		require.Error(t, pm.checkBackupRepositoryScope("tablegroups/commerce/7/0-80"))
	})
	t.Run("the catalog shard records the base path", func(t *testing.T) {
		dir := t.TempDir()
		pm := newManager(dir)
		require.NoError(t, pm.checkBackupRepositoryScope(""))
		require.NoError(t, pm.checkBackupRepositoryScope(""))
		err := pm.checkBackupRepositoryScope("tablegroups/default/1/0-inf")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "base path")
	})
	t.Run("an existing directory without a scope is adopted", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "pgbackrest"), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "pgbackrest", "pgbackrest.conf"), []byte("[global]\n"), 0o600))
		pm := newManager(dir)
		require.NoError(t, pm.checkBackupRepositoryScope("tablegroups/commerce/7/0-80"))
		data, err := os.ReadFile(scopeFile(dir))
		require.NoError(t, err)
		assert.Equal(t, "tablegroups/commerce/7/0-80", string(data))
	})
	t.Run("removing the file accepts the new scope", func(t *testing.T) {
		dir := t.TempDir()
		pm := newManager(dir)
		require.NoError(t, pm.checkBackupRepositoryScope("tablegroups/commerce/7/0-80"))
		require.NoError(t, os.Remove(scopeFile(dir)))
		require.NoError(t, pm.checkBackupRepositoryScope("tablegroups/commerce/8/0-80"))
	})
}
