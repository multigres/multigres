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
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/safefs"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func newGenerationsTestManager(t *testing.T) (*MultipoolerManager, *mock.QueryService, string) {
	t.Helper()
	pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)
	dir := t.TempDir()
	pm.record = newRecordFromProto(&clustermetadatapb.Multipooler{
		ShardRef:  &clustermetadatapb.ShardRef{TableGroup: constants.DefaultTableGroup, Shard: constants.DefaultShard},
		PoolerDir: dir,
	})
	return pm, qs, dir
}

func TestGenerationsCopy_RejectsOlderRevision(t *testing.T) {
	pm, _, dir := newGenerationsTestManager(t)

	require.NoError(t, pm.writeGenerationsCopy(5, []RepoGeneration{{Generation: 2, RepoNumber: 2}, {Generation: 1, RepoNumber: 1}}))
	before, err := os.ReadFile(filepath.Join(dir, generationsCopyFile))
	require.NoError(t, err)
	assert.JSONEq(t, `{"revision":5,"repos":[{"generation":1,"repo_number":1},{"generation":2,"repo_number":2}]}`, string(before), "repos are sorted")

	for _, rev := range []int64{4, 5, 0} {
		require.NoError(t, pm.writeGenerationsCopy(rev, []RepoGeneration{{Generation: 9, RepoNumber: 9}}))
		after, err := os.ReadFile(filepath.Join(dir, generationsCopyFile))
		require.NoError(t, err)
		assert.Equal(t, string(before), string(after), "revision %d must not replace revision 5", rev)
	}

	require.NoError(t, pm.writeGenerationsCopy(6, []RepoGeneration{{Generation: 3, RepoNumber: 3}}))
	rev, repos, ok := pm.readGenerationsCopy()
	require.True(t, ok)
	assert.Equal(t, int64(6), rev)
	assert.Equal(t, []RepoGeneration{{Generation: 3, RepoNumber: 3}}, repos)
}

func TestGenerationsCopy_MissingFallsBack(t *testing.T) {
	pm, _, dir := newGenerationsTestManager(t)

	rev, repos, ok := pm.readGenerationsCopy()
	assert.False(t, ok)
	assert.Zero(t, rev)
	assert.Empty(t, repos)

	// A corrupt copy is as good as none: the table supplies the list.
	require.NoError(t, os.WriteFile(filepath.Join(dir, generationsCopyFile), []byte("{not json"), 0o600))
	_, _, ok = pm.readGenerationsCopy()
	assert.False(t, ok)

	// And a first write after a bad file repairs it, whatever its revision.
	require.NoError(t, pm.writeGenerationsCopy(1, []RepoGeneration{{Generation: 1, RepoNumber: 1}}))
	rev, _, ok = pm.readGenerationsCopy()
	require.True(t, ok)
	assert.Equal(t, int64(1), rev)
}

func TestGenerationsCopy_RefusesSymlink(t *testing.T) {
	pm, _, dir := newGenerationsTestManager(t)

	target := filepath.Join(t.TempDir(), "elsewhere.json")
	require.NoError(t, os.WriteFile(target, []byte(`{"revision":99,"repos":[{"generation":7,"repo_number":7}]}`), 0o600))
	require.NoError(t, os.Symlink(target, filepath.Join(dir, generationsCopyFile)))

	_, _, ok := pm.readGenerationsCopy()
	assert.False(t, ok, "a symlinked copy must not be honoured")

	require.NoError(t, pm.writeGenerationsCopy(1, []RepoGeneration{{Generation: 1, RepoNumber: 1}}))
	got, err := os.ReadFile(target)
	require.NoError(t, err)
	assert.Contains(t, string(got), `"revision":99`, "the write must not follow the symlink")
	rev, _, ok := pm.readGenerationsCopy()
	require.True(t, ok)
	assert.Equal(t, int64(1), rev)
}

func TestGenerationsCopy_RefreshFollowsTable(t *testing.T) {
	pm, qs, _ := newGenerationsTestManager(t)
	cols := []string{"revision", "generation", "repo_number"}

	qs.AddQueryPatternOnce("pgbackrest_repos_meta m", mock.MakeQueryResult(cols, [][]any{{3, 1, 1}, {3, 2, 2}}))
	require.NoError(t, pm.refreshGenerationsCopy(context.Background()))
	rev, repos, ok := pm.readGenerationsCopy()
	require.True(t, ok)
	assert.Equal(t, int64(3), rev)
	assert.Equal(t, []RepoGeneration{{Generation: 1, RepoNumber: 1}, {Generation: 2, RepoNumber: 2}}, repos)

	// A table behind the copy (a lagging standby) never rewinds it.
	qs.AddQueryPatternOnce("pgbackrest_repos_meta m", mock.MakeQueryResult(cols, [][]any{{2, 1, 1}}))
	require.NoError(t, pm.refreshGenerationsCopy(context.Background()))
	rev, repos, _ = pm.readGenerationsCopy()
	assert.Equal(t, int64(3), rev)
	assert.Len(t, repos, 2)
	assert.NoError(t, qs.ExpectationsWereMet())
}

func TestGenerationsCopy_ReadGenerationsNoRepos(t *testing.T) {
	pm, qs, _ := newGenerationsTestManager(t)
	qs.AddQueryPatternOnce("pgbackrest_repos_meta m", mock.MakeQueryResult([]string{"revision", "generation", "repo_number"}, [][]any{{7, nil, nil}}))

	rev, repos, err := pm.readGenerations(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(7), rev)
	assert.Empty(t, repos)
}

func TestGenerationsCopy_NeverStoresEmptyList(t *testing.T) {
	pm, qs, dir := newGenerationsTestManager(t)

	require.ErrorIs(t, pm.writeGenerationsCopy(3, nil), errEmptyGenerations)
	_, err := os.Stat(filepath.Join(dir, generationsCopyFile))
	assert.ErrorIs(t, err, os.ErrNotExist)

	// A table between schema creation and the seed has a revision and no repos.
	qs.AddQueryPatternOnce("pgbackrest_repos_meta m", mock.MakeQueryResult([]string{"revision", "generation", "repo_number"}, [][]any{{1, nil, nil}}))
	require.NoError(t, pm.refreshGenerationsCopy(context.Background()))
	_, _, ok := pm.readGenerationsCopy()
	assert.False(t, ok)

	// An empty list already on disk is treated as absent.
	require.NoError(t, os.WriteFile(filepath.Join(dir, generationsCopyFile), []byte(`{"revision":9,"repos":[]}`), 0o600))
	_, _, ok = pm.readGenerationsCopy()
	assert.False(t, ok)
	require.NoError(t, pm.writeGenerationsCopy(1, []RepoGeneration{{Generation: 1, RepoNumber: 1}}))
	rev, _, ok := pm.readGenerationsCopy()
	require.True(t, ok)
	assert.Equal(t, int64(1), rev)
}

func TestGenerationsCopy_UsesInjectedVolumeRoot(t *testing.T) {
	pm, _, dir := newGenerationsTestManager(t)
	other := t.TempDir()
	opened := 0
	pm.volumeRoot = func() (*safefs.Root, error) {
		opened++
		return safefs.OpenRoot(other)
	}

	require.NoError(t, pm.writeGenerationsCopy(1, []RepoGeneration{{Generation: 1, RepoNumber: 1}}))
	_, _, ok := pm.readGenerationsCopy()
	require.True(t, ok)
	assert.Positive(t, opened)
	_, err := os.Stat(filepath.Join(other, generationsCopyFile))
	assert.NoError(t, err)
	_, err = os.Stat(filepath.Join(dir, generationsCopyFile))
	assert.ErrorIs(t, err, os.ErrNotExist, "the poolerDir path must not be opened when an opener is set")

	pm.volumeRoot = func() (*safefs.Root, error) { return nil, errors.New("anchor failed") }
	require.Error(t, pm.writeGenerationsCopy(2, []RepoGeneration{{Generation: 1, RepoNumber: 1}}))
}
