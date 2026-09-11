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

package command

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	"github.com/multigres/multigres/go/services/pgctld"
)

func TestVersionCachePostmasterIdentityAndRetry(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	require.NoError(t, os.Mkdir(bin, 0o755))
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))
	counter := filepath.Join(dir, "calls")
	mock := func(value string) {
		testutil.MockBinary(t, bin, "psql", fmt.Sprintf("printf x >> %q\nprintf '%%s' %q", counter, value))
	}
	pidfile := filepath.Join(dir, "postmaster.pid")
	identity := func(start int) {
		require.NoError(t, os.WriteFile(pidfile, fmt.Appendf(nil, "123\n%s\n%d\n", dir, start), 0o600))
	}
	config := &pgctld.PostgresCtlConfig{PostgresDataDir: dir, PoolerDir: dir, Port: 5432, User: "postgres", Database: "postgres"}
	identity(100)
	mock("version-one")
	require.Equal(t, "version-one", cachedServerVersion(t.Context(), config))
	mock("version-two")
	require.Equal(t, "version-one", cachedServerVersion(t.Context(), config))
	identity(101) // Same PID, different server start: cannot reuse the cached version.
	require.Equal(t, "version-two", cachedServerVersion(t.Context(), config))
	b, e := os.ReadFile(counter)
	require.NoError(t, e)
	require.Equal(t, "xx", string(b))
	identity(102)
	testutil.MockBinary(t, bin, "psql", "exit 1")
	require.Empty(t, cachedServerVersion(t.Context(), config))
	mock("recovered")
	require.Equal(t, "recovered", cachedServerVersion(t.Context(), config))
	require.NoError(t, os.WriteFile(pidfile, []byte("123\n"), 0o600))
	mock("uncached")
	require.Equal(t, "uncached", cachedServerVersion(t.Context(), config))
	mock("uncached-two")
	require.Equal(t, "uncached-two", cachedServerVersion(t.Context(), config))
}

func TestVersionCacheDiscardsQueryAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	require.NoError(t, os.Mkdir(bin, 0o755))
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))
	pidfile := filepath.Join(dir, "postmaster.pid")
	require.NoError(t, os.WriteFile(pidfile, []byte("123\n/data\n100\n"), 0o600))
	config := &pgctld.PostgresCtlConfig{PostgresDataDir: dir, PoolerDir: dir, Port: 5432, User: "postgres", Database: "postgres"}
	testutil.MockBinary(t, bin, "psql", fmt.Sprintf("printf '123\\n/data\\n101\\n' > %q\nprintf old-version", pidfile))
	require.Empty(t, cachedServerVersion(t.Context(), config))
	testutil.MockBinary(t, bin, "psql", "printf new-version")
	require.Equal(t, "new-version", cachedServerVersion(t.Context(), config))
}
