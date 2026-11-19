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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/utils"
)

const commerceScope = "tablegroups/commerce/7/0-80"

func configureCalls(f *fakeSidecarServer) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.configures)
}

func TestSetupSidecar_RefusesASidecarOfAnotherShard(t *testing.T) {
	loc := utils.FilesystemBackupLocation("/backups")

	t.Run("the same scope", func(t *testing.T) {
		s := newSidecarStartup(t, loc, loc)
		s.fake.status.RepositoryScope = commerceScope
		require.NoError(t, s.pm.setupSidecar(t.Context(), loc, commerceScope))
	})

	for name, tc := range map[string]struct{ sidecar, pooler string }{
		"the sidecar serves another shard":     {"tablegroups/commerce/7/80-inf", commerceScope},
		"the sidecar serves another id":        {"tablegroups/commerce/8/0-80", commerceScope},
		"the sidecar serves the catalog shard": {"", commerceScope},
		"the pooler is the catalog shard":      {commerceScope, ""},
	} {
		t.Run(name, func(t *testing.T) {
			s := newSidecarStartup(t, loc, loc)
			s.fake.status.RepositoryScope = tc.sidecar
			require.NoError(t, os.MkdirAll(filepath.Join(s.poolerDir, "pgbackrest"), 0o700))

			err := s.pm.setupSidecar(t.Context(), loc, tc.pooler)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "--table-group")
			assert.DirExists(t, filepath.Join(s.poolerDir, "pgbackrest"), "nothing is removed for a sidecar of another shard")
			assert.Equal(t, 0, configureCalls(s.fake), "and nothing is configured")
		})
	}
}

// A pooler directory pinned by an in-process pgBackRest keeps that guarantee
// across the cutover: the file that records it is only removed once the
// sidecar is known to use the repository it names.
func TestSetupSidecar_CarriesTheLegacyRepositoryPin(t *testing.T) {
	loc := utils.FilesystemBackupLocation("/backups")
	pin := func(t *testing.T, s *sidecarStartup, scope string) {
		t.Helper()
		dir := filepath.Join(s.poolerDir, "pgbackrest")
		require.NoError(t, os.MkdirAll(dir, 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "repository_scope"), []byte(scope), 0o600))
	}

	t.Run("the pin matches the sidecar", func(t *testing.T) {
		s := newSidecarStartup(t, loc, loc)
		s.fake.status.RepositoryScope = commerceScope
		pin(t, s, commerceScope)
		require.NoError(t, s.pm.setupSidecar(t.Context(), loc, commerceScope))
		assert.NoDirExists(t, filepath.Join(s.poolerDir, "pgbackrest"))
	})

	t.Run("the pin names another repository", func(t *testing.T) {
		s := newSidecarStartup(t, loc, loc)
		s.fake.status.RepositoryScope = commerceScope
		pin(t, s, "tablegroups/commerce/6/0-80")
		err := s.pm.setupSidecar(t.Context(), loc, commerceScope)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "repository_scope")
		assert.FileExists(t, filepath.Join(s.poolerDir, "pgbackrest", "repository_scope"), "the evidence stays")
		assert.Equal(t, 0, configureCalls(s.fake), "and the sidecar is never configured")
	})

	t.Run("a catalog shard pin against a scoped sidecar", func(t *testing.T) {
		s := newSidecarStartup(t, loc, loc)
		s.fake.status.RepositoryScope = commerceScope
		pin(t, s, "")
		require.Error(t, s.pm.setupSidecar(t.Context(), loc, commerceScope))
	})

	t.Run("no pin leaves nothing to carry", func(t *testing.T) {
		s := newSidecarStartup(t, loc, loc)
		s.fake.status.RepositoryScope = commerceScope
		require.NoError(t, os.MkdirAll(filepath.Join(s.poolerDir, "pgbackrest"), 0o700))
		require.NoError(t, s.pm.setupSidecar(t.Context(), loc, commerceScope))
	})
}
