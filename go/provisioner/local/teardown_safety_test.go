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

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/tools/executil"
)

func TestRepeatedTeardownNeverSignalsCompletedOrUncertainPIDs(t *testing.T) {
	p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: t.TempDir()}}
	cmd := executil.Command(context.Background(), "/bin/sleep", "60")
	require.NoError(t, cmd.Start())
	go func() { _ = cmd.Wait() }()
	t.Cleanup(func() { _, _ = cmd.Stop(context.Background()) })
	service := &LocalProvisionedService{ID: "test", Service: "etcd", PID: cmd.Process.Pid}
	require.NoError(t, p.saveServiceState(service, ""))
	require.NoError(t, p.Teardown(t.Context(), false))
	services, err := p.loadShutdownServices()
	require.NoError(t, err)
	require.Len(t, services, 1)
	require.Equal(t, true, services[0].Metadata["shutdown_complete"])
	// Model that numeric PID now naming an unrelated live process. Repeated
	// teardown must use the durable completed state, never send another signal.
	services[0].PID = os.Getpid()
	require.NoError(t, persistShutdownState(services[0]))
	require.NoError(t, p.Teardown(t.Context(), false))
	delete(services[0].Metadata, "shutdown_complete")
	require.NoError(t, persistShutdownState(services[0]))
	require.ErrorContains(t, p.Teardown(t.Context(), false), "previous shutdown outcome is unknown")
}

func TestTeardownFailurePreservesAllFiles(t *testing.T) {
	p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: t.TempDir(), DefaultDbName: "db"}}
	pooler := filepath.Join(p.getDataDir(), "pooler")
	service := &LocalProvisionedService{ID: "dead", Service: "pgctld", PID: os.Getpid(), DataDir: pooler, Ports: map[string]int{"grpc_port": 0}, Metadata: map[string]any{"cell": "zone"}}
	// RPC shutdown fails before anything can signal this test's PID.
	require.NoError(t, p.saveServiceState(service, "db"))
	paths := []string{
		filepath.Join(pooler, "pg_data", "keep"),
		filepath.Join(NewPgBackRestdPaths(pooler).StateDir, "keep"),
		filepath.Join(p.getLogsDir(), "keep"),
		filepath.Join(p.getStateDir(), "dbs", "db", "pgctld_dead.json"),
	}
	for _, path := range paths[:3] {
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
		require.NoError(t, os.WriteFile(path, []byte("keep"), 0o600))
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.ErrorContains(t, p.Teardown(ctx, true), "failed to confirm PostgreSQL shutdown")
	for _, path := range paths {
		_, err := os.Stat(path)
		require.NoError(t, err, path)
	}
}

func TestShutdownDiscoveryRootScopeAndErrors(t *testing.T) {
	p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: t.TempDir()}}
	services, err := p.loadShutdownServices()
	require.NoError(t, err)
	require.Empty(t, services, "absent state still means no saved records")

	// Failure to open an existing state path must not become an empty inventory.
	require.NoError(t, os.WriteFile(p.getStateDir(), []byte("not a directory"), 0o600))
	_, err = p.loadShutdownServices()
	require.Error(t, err)
	require.NoError(t, os.Remove(p.getStateDir()))
	require.NoError(t, os.MkdirAll(p.getStateDir(), 0o700))
	external := filepath.Join(t.TempDir(), "outside.json")
	require.NoError(t, os.WriteFile(external, []byte("not service state"), 0o600))
	link := filepath.Join(p.getStateDir(), "etcd_outside.json")
	require.NoError(t, os.Symlink(external, link))
	_, err = p.loadShutdownServices()
	require.ErrorContains(t, err, "refusing symlinked service state")
	require.NoError(t, os.Remove(link))

	require.NoError(t, p.saveServiceState(&LocalProvisionedService{ID: "global", Service: "etcd", PID: 1}, ""))
	require.NoError(t, p.saveServiceState(&LocalProvisionedService{ID: "database", Service: "pgctld", PID: 2}, "db"))
	services, err = p.loadShutdownServices()
	require.NoError(t, err)
	require.Len(t, services, 2)
	for _, service := range services {
		_, err := os.Stat(service.statePath)
		require.NoError(t, err, "root-relative reads retain the real persistence path")
	}
}

func TestTeardownRejectsUnknownAndUnreadableState(t *testing.T) {
	for _, content := range []string{`{"id":"unknown","service":"unknown","pid":1}`, `{broken`, `{"id":"stale","service":"etcd","pid":0}`} {
		t.Run(content, func(t *testing.T) {
			p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: t.TempDir(), DefaultDbName: "db"}}
			require.NoError(t, os.MkdirAll(p.getStateDir(), 0o700))
			state := filepath.Join(p.getStateDir(), "etcd_stale.json")
			require.NoError(t, os.WriteFile(state, []byte(content), 0o600))
			require.Error(t, p.Teardown(t.Context(), true))
			_, err := os.Stat(state)
			require.NoError(t, err)
		})
	}
}

func TestTeardownRefusesUnrecordedSidecarCleanup(t *testing.T) {
	p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: t.TempDir(), DefaultDbName: "db"}}
	sidecar := NewPgBackRestdPaths(filepath.Join(p.getDataDir(), "pooler"))
	require.NoError(t, os.MkdirAll(sidecar.StateDir, 0o700))
	sentinel := filepath.Join(sidecar.StateDir, "opaque-safety-state")
	require.NoError(t, os.WriteFile(sentinel, []byte("do not erase"), 0o600))
	require.ErrorContains(t, p.Teardown(t.Context(), true), "PostgreSQL shutdown evidence missing")
	raw, err := os.ReadFile(sentinel)
	require.NoError(t, err)
	require.Equal(t, "do not erase", string(raw))
}

func TestTeardownCleanWithoutSidecars(t *testing.T) {
	p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: t.TempDir(), DefaultDbName: "db"}}
	require.NoError(t, os.MkdirAll(p.getDataDir(), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(p.getDataDir(), "old"), []byte("old"), 0o600))
	require.NoError(t, p.Teardown(t.Context(), true))
	_, err := os.Stat(p.getDataDir())
	require.True(t, os.IsNotExist(err))
}

func TestDeprovisionDatabaseRetainsFailedServiceState(t *testing.T) {
	p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: t.TempDir(), DefaultDbName: "db"}}
	require.NoError(t, p.saveServiceState(&LocalProvisionedService{ID: "bad", Service: "unknown", PID: 1}, "db"))
	require.Error(t, p.DeprovisionDatabase(t.Context(), "db", ""))
	_, err := os.Stat(filepath.Join(p.getStateDir(), "dbs", "db", "unknown_bad.json"))
	require.NoError(t, err)
}
