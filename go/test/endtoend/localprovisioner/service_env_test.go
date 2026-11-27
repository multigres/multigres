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

package localprovisioner

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/utils"
)

// launcherCredentialEnv is in the environment of every `multigres cluster
// start` these tests run. The values are not real keys.
var launcherCredentialEnv = []string{
	"AWS_ACCESS_KEY_ID=launcher-test-access-key",
	"AWS_SECRET_ACCESS_KEY=launcher-test-secret-key",
	"AWS_SESSION_TOKEN=launcher-test-session-token",
	"PGBACKREST_REPO1_S3_KEY_SECRET=launcher-test-pgbackrest-secret",
}

// TestServicesDoNotInheritRepositoryCredentials starts a cluster from a
// launcher whose environment holds repository credentials and reads the
// environment of every process that came from it. Only the pgbackrestd
// sidecar may reach the repository, and it gets its keys from a key
// directory, so no process may hold one: postgres in particular runs
// extension code and archive commands.
//
// The processes are found through MULTIGRES_TESTDATA_DIR, which the launcher
// passes on to its services with the rest of its environment. Linux only: the
// environment of another process is read from /proc.
func TestServicesDoNotInheritRepositoryCredentials(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping service environment test in short mode")
	}
	if runtime.GOOS != "linux" {
		t.Skip("reads other processes' environments from /proc")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}
	if _, err := exec.LookPath("etcd"); err != nil {
		t.Skip("etcd binary not found in PATH")
	}

	clusterSetup := setupTestCluster(t)
	marker := []byte("MULTIGRES_TESTDATA_DIR=" + clusterSetup.TempDir)

	entries, err := os.ReadDir("/proc")
	require.NoError(t, err)
	var seen []string
	for _, e := range entries {
		if _, err := strconv.Atoi(e.Name()); err != nil {
			continue
		}
		environ, err := os.ReadFile(filepath.Join("/proc", e.Name(), "environ"))
		if err != nil || !slices.ContainsFunc(bytes.Split(environ, []byte{0}), func(kv []byte) bool { return bytes.Equal(kv, marker) }) {
			continue
		}
		cmdline, _ := os.ReadFile(filepath.Join("/proc", e.Name(), "cmdline"))
		name := filepath.Base(strings.SplitN(string(cmdline), "\x00", 2)[0])
		// postgres rewrites its title: "postgres: checkpointer" and the like.
		name, _, _ = strings.Cut(name, ":")
		seen = append(seen, name)
		for _, kv := range launcherCredentialEnv {
			key, value, _ := strings.Cut(kv, "=")
			assert.NotContains(t, string(environ), key+"=", "%s (pid %s) inherited %s", name, e.Name(), key)
			assert.NotContains(t, string(environ), value, "%s (pid %s) holds the value of %s", name, e.Name(), key)
		}
	}
	// The check means nothing unless it saw the services it is about.
	for _, service := range []string{"pgctld", "postgres", "multipooler", "multigateway", "multiorch", "multiadmin", "etcd"} {
		assert.Contains(t, seen, service, "no %s process of this cluster was inspected (saw %v)", service, seen)
	}
}
