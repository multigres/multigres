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

package local

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// refreshFixture starts the sidecar process fixture for a pooler directory and
// returns its paths and the pooler directory.
func refreshFixture(t *testing.T) (PgBackRestdPaths, string) {
	t.Helper()
	paths := ownerTestPaths(t)
	binary, err := os.Executable()
	require.NoError(t, err)
	lock, err := paths.lockOwner()
	require.NoError(t, err)
	_, err = paths.ensureSidecar(context.Background(), binary, fixtureArgs(paths))
	require.NoError(t, lock.Close())
	require.NoError(t, err)
	t.Cleanup(func() { _ = paths.stopOwned(context.Background()) })
	return paths, filepath.Join(filepath.Dir(paths.Dir), "pooler")
}

func refreshEnv(t *testing.T, key string) {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", key)
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret-of-"+key)
	t.Setenv("AWS_SESSION_TOKEN", "token-of-"+key)
}

func fixtureFile(t *testing.T, paths PgBackRestdPaths, name string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(paths.Dir, name))
	if os.IsNotExist(err) {
		return ""
	}
	require.NoError(t, err)
	return string(raw)
}

func TestRefreshSidecarS3Keys_ConfiguredSidecarReloads(t *testing.T) {
	paths, pooler := refreshFixture(t)
	require.NoError(t, os.WriteFile(filepath.Join(paths.Dir, "fixture-revision"), []byte("7"), 0o600))
	refreshEnv(t, "OLDKEY")
	_, err := paths.writeS3KeysFromEnv()
	require.NoError(t, err)

	refreshEnv(t, "NEWKEY")
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	got, err := RefreshSidecarS3Keys(ctx, pooler)
	require.NoError(t, err)
	assert.Equal(t, S3KeysReloaded, got)

	// One Configure, with the revision and repositories the sidecar held, sent
	// after the new bundle was on disk.
	configures := fixtureFile(t, paths, "fixture-configures")
	assert.Equal(t,
		`revision=7 repos=1 wal=0 keys={"access_key_id":"NEWKEY","secret_access_key":"secret-of-NEWKEY","session_token":"token-of-NEWKEY"}`+"\n",
		configures)
}

func TestRefreshSidecarS3Keys_NothingToReload(t *testing.T) {
	t.Run("no sidecar directory", func(t *testing.T) {
		refreshEnv(t, "NEWKEY")
		_, err := RefreshSidecarS3Keys(t.Context(), filepath.Join(t.TempDir(), "pooler"))
		require.ErrorIs(t, err, ErrNoSidecar)
	})
	t.Run("stopped sidecar", func(t *testing.T) {
		paths := ownerTestPaths(t)
		refreshEnv(t, "NEWKEY")
		got, err := RefreshSidecarS3Keys(t.Context(), filepath.Join(filepath.Dir(paths.Dir), "pooler"))
		require.NoError(t, err)
		assert.Equal(t, S3KeysStaged, got)
		assert.Contains(t, fixtureFile(t, paths, filepath.Join("s3-keys", "credentials.json")), "NEWKEY",
			"the next start reads the staged keys")
	})
	t.Run("unconfigured sidecar", func(t *testing.T) {
		paths, pooler := refreshFixture(t)
		refreshEnv(t, "NEWKEY")
		got, err := RefreshSidecarS3Keys(t.Context(), pooler)
		require.NoError(t, err)
		assert.Equal(t, S3KeysStaged, got)
		assert.Empty(t, fixtureFile(t, paths, "fixture-configures"), "an unconfigured sidecar has no Configure to repeat")
		assert.Contains(t, fixtureFile(t, paths, "fixture-statuses"), "after_configure",
			"\"unconfigured\" is only trusted once no first Configure is in progress, which could still install the old keys")
		assert.Contains(t, fixtureFile(t, paths, filepath.Join("s3-keys", "credentials.json")), "NEWKEY")
	})
}

func TestRefreshSidecarS3Keys_ReportsASidecarThatDidNotReload(t *testing.T) {
	for mode, want := range map[string]string{
		"configure-fail":    "did not reload its S3 keys",
		"configure-refused": "refused revision 7 while holding revision 7",
	} {
		t.Run(mode, func(t *testing.T) {
			paths, pooler := refreshFixture(t)
			require.NoError(t, os.WriteFile(filepath.Join(paths.Dir, "fixture-revision"), []byte("7"), 0o600))
			require.NoError(t, os.WriteFile(filepath.Join(paths.Dir, "fixture-mode"), []byte(mode), 0o600))
			refreshEnv(t, "NEWKEY")
			_, err := RefreshSidecarS3Keys(t.Context(), pooler)
			require.ErrorContains(t, err, want)
		})
	}

	// A Configure with a newer revision is applied between the refresher's
	// Status and its own Configure. That newer one may have read the key files
	// before they were replaced, so the refusal it causes proves nothing: the
	// refresher asks again and sends the newer list, and reports a reload only
	// once a Configure of its own was accepted.
	t.Run("a newer Configure is not taken for a reload", func(t *testing.T) {
		paths, pooler := refreshFixture(t)
		require.NoError(t, os.WriteFile(filepath.Join(paths.Dir, "fixture-revision"), []byte("7"), 0o600))
		require.NoError(t, os.WriteFile(filepath.Join(paths.Dir, "fixture-mode"), []byte("configure-superseded-once"), 0o600))
		refreshEnv(t, "NEWKEY")
		got, err := RefreshSidecarS3Keys(t.Context(), pooler)
		require.NoError(t, err)
		assert.Equal(t, S3KeysReloaded, got)
		assert.Equal(t,
			`revision=8 repos=1 wal=0 keys={"access_key_id":"NEWKEY","secret_access_key":"secret-of-NEWKEY","session_token":"token-of-NEWKEY"}`+"\n",
			fixtureFile(t, paths, "fixture-configures"),
			"the reload must be a Configure the sidecar accepted, at the revision it holds now")
	})

	t.Run("a sidecar that keeps moving ahead is an error", func(t *testing.T) {
		paths, pooler := refreshFixture(t)
		require.NoError(t, os.WriteFile(filepath.Join(paths.Dir, "fixture-revision"), []byte("7"), 0o600))
		require.NoError(t, os.WriteFile(filepath.Join(paths.Dir, "fixture-mode"), []byte("configure-superseded"), 0o600))
		refreshEnv(t, "NEWKEY")
		_, err := RefreshSidecarS3Keys(t.Context(), pooler)
		require.ErrorContains(t, err, "did not confirm that it read the new S3 keys")
		assert.Empty(t, fixtureFile(t, paths, "fixture-configures"))
	})

	t.Run("missing keys change nothing", func(t *testing.T) {
		paths, pooler := refreshFixture(t)
		require.NoError(t, os.WriteFile(filepath.Join(paths.Dir, "fixture-revision"), []byte("7"), 0o600))
		t.Setenv("AWS_ACCESS_KEY_ID", "")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "")
		_, err := RefreshSidecarS3Keys(t.Context(), pooler)
		require.Error(t, err)
		assert.Empty(t, fixtureFile(t, paths, "fixture-configures"))
	})
}
