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

	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestCleanupInventoryRejectsAmbiguousInstallations(t *testing.T) {
	for _, obstacle := range []string{"sidecar file", "empty sidecar name", "configured symlink"} {
		t.Run(obstacle, func(t *testing.T) {
			root := t.TempDir()
			p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: root}}
			path := filepath.Join(root, "pgbackrestd-pooler")
			var services []*LocalProvisionedService
			switch obstacle {
			case "sidecar file":
				require.NoError(t, os.WriteFile(path, []byte("evidence"), 0o600))
			case "empty sidecar name":
				require.NoError(t, os.Mkdir(filepath.Join(root, "pgbackrestd-"), 0o700))
			case "configured symlink":
				require.NoError(t, os.Symlink(t.TempDir(), path))
				services = []*LocalProvisionedService{{Service: "pgctld", DataDir: filepath.Join(root, "pooler")}}
			}
			inventory, err := p.sidecarCleanupInventoryForRoots(services, []string{root}, true)
			require.Error(t, err)
			require.Nil(t, inventory)
		})
	}
	p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: t.TempDir()}}
	_, err := p.sidecarCleanupInventoryForRoots([]*LocalProvisionedService{{Service: "pgctld"}}, nil, false)
	require.ErrorContains(t, err, "lacks pooler directory")
	inventory, err := p.sidecarCleanupInventoryForRoots([]*LocalProvisionedService{{Service: "multipooler"}, {Service: "minigres", DataDir: t.TempDir()}}, []string{t.TempDir()}, false)
	require.NoError(t, err)
	require.Empty(t, inventory, "unaffected installations are not acquired or removed")
}

func TestCleanupLockAcquisitionReleasesPartialSet(t *testing.T) {
	first, second := ownerTestPaths(t), ownerTestPaths(t)
	lock, err := second.lockFile("lifecycle.lock")
	require.NoError(t, err)
	defer lock.Close()
	held, err := lockSidecarInstallations([]PgBackRestdPaths{first, second})
	require.Error(t, err)
	require.Nil(t, held)
	reacquired, err := first.lockFile("lifecycle.lock")
	require.NoError(t, err, "failure on the second installation must not leak the first lease")
	require.NoError(t, reacquired.Close())
}

func TestCleanupRejectsFileAncestorOfProtectedLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(path, []byte("evidence"), 0o600))
	keep := []string{filepath.Join(path, "instance.lock")}
	require.ErrorContains(t, cleanupPreservingLocks(path, keep), "non-directory lock ancestor")
	require.ErrorContains(t, removeFilePreservingLocks(path, keep), "lock ancestor")
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "evidence", string(raw))
	require.NoError(t, removeFilePreservingLocks(path, []string{path}), "the protected lock itself is never unlinked")
	require.NoError(t, cleanupPreservingLocks(filepath.Join(t.TempDir(), "missing"), []string{filepath.Join(path, "lock")}))
}

func TestDirectCleanupFailureRetainsServiceRecords(t *testing.T) {
	for _, obstacle := range []string{"log directory", "record directory", "cancelled", "inventory", "installation lease"} {
		t.Run(obstacle, func(t *testing.T) {
			p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: t.TempDir()}}
			data := filepath.Join(t.TempDir(), "data")
			require.NoError(t, os.Mkdir(data, 0o700))
			service := &LocalProvisionedService{Service: "multiadmin", ID: "fixture", DataDir: data, Metadata: map[string]any{"shutdown_attempted": true, "shutdown_complete": true}}
			require.NoError(t, p.saveServiceState(service, "db"))
			record := filepath.Join(p.getStateDir(), "dbs", "db", "multiadmin_fixture.json")
			service.statePath = record
			ctx := t.Context()
			switch obstacle {
			case "log directory":
				service.LogFile = filepath.Join(t.TempDir(), "log")
				require.NoError(t, os.Mkdir(service.LogFile, 0o700))
				require.NoError(t, os.WriteFile(filepath.Join(service.LogFile, "keep"), []byte("evidence"), 0o600))
			case "record directory":
				require.NoError(t, os.Remove(record))
				require.NoError(t, os.Mkdir(record, 0o700))
				require.NoError(t, os.WriteFile(filepath.Join(record, "keep"), []byte("evidence"), 0o600))
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "inventory":
				require.NoError(t, os.WriteFile(filepath.Join(data, "pgbackrestd-unknown"), []byte("evidence"), 0o600))
			case "installation lease":
				paths := NewPgBackRestdPaths(data)
				lock, err := paths.lockFile("lifecycle.lock")
				require.NoError(t, err)
				defer lock.Close()
			}
			require.Error(t, p.deprovisionSelected(ctx, []*LocalProvisionedService{service}, []*LocalProvisionedService{service}, true))
			_, err := os.Stat(record)
			require.NoError(t, err, "failed cleanup must not erase service recovery evidence")
		})
	}
}

func TestDirectCleanupEmptyFootprintAndMissingRecord(t *testing.T) {
	p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: t.TempDir()}}
	require.NoError(t, p.deprovisionSelected(t.Context(), nil, nil, true))
	service := &LocalProvisionedService{Service: "multiadmin", Metadata: map[string]any{"shutdown_attempted": true, "shutdown_complete": true}, statePath: filepath.Join(t.TempDir(), "missing", "record.json")}
	require.NoError(t, p.deprovisionSelected(t.Context(), []*LocalProvisionedService{service}, nil, true))
}

func TestSidecarLayoutAndCredentialWriteFailures(t *testing.T) {
	paths := ownerTestPaths(t)
	require.NoError(t, os.RemoveAll(paths.StateDir))
	require.NoError(t, os.WriteFile(paths.StateDir, []byte("evidence"), 0o600))
	require.ErrorContains(t, paths.Prepare("fixture"), "create")
	paths = ownerTestPaths(t)
	require.NoError(t, os.Remove(paths.PasswordFile))
	require.NoError(t, os.Mkdir(paths.PasswordFile, 0o700))
	require.ErrorContains(t, paths.Prepare("fixture"), "write postgres password copy")
	t.Setenv("AWS_ACCESS_KEY_ID", "fixture")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "fixture")
	require.NoError(t, os.WriteFile(paths.s3KeyDir(), []byte("evidence"), 0o600))
	_, err := paths.writeS3KeysFromEnv()
	require.ErrorContains(t, err, "create")
	cfg := sidecarTestConfig(t, &clustermetadatapb.BackupLocation{Location: &clustermetadatapb.BackupLocation_S3{S3: &clustermetadatapb.S3Backup{Bucket: "b", Region: "r"}}})
	args, err := cfg.BuildArgs()
	require.NoError(t, err)
	require.Contains(t, args, "none", "no ambient credential fallback is invented")
	cfg.PoolerDir = ""
	_, err = cfg.BuildArgs()
	require.ErrorContains(t, err, "needs a pooler directory")
}
