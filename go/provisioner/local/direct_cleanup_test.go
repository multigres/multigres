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
	"golang.org/x/sys/unix"

	"github.com/multigres/multigres/go/common/pgbackreststate"
	"github.com/multigres/multigres/go/provisioner"
	"github.com/multigres/multigres/go/tools/executil"
)

func seedCleanupMultipooler(t *testing.T, p *localProvisioner, pooler string) {
	t.Helper()
	child := executil.Command(context.Background(), "/bin/sleep", "60")
	require.NoError(t, child.Start())
	go func() { _ = child.Wait() }()
	t.Cleanup(func() { _, _ = child.Stop(context.Background()) })
	require.NoError(t, p.saveServiceState(&LocalProvisionedService{ID: "mp", Service: "multipooler", PID: child.Process.Pid, DataDir: pooler}, "db"))
}

func TestDirectCleanWaitsForSupervisorGuardAndRetainsRecords(t *testing.T) {
	for _, entrypoint := range []string{"database", "pgctld"} {
		t.Run(entrypoint, func(t *testing.T) {
			p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: t.TempDir()}}
			pooler := filepath.Join(p.getDataDir(), "pooler")
			paths := NewPgBackRestdPaths(pooler)
			freshSupervisedState(t, paths)
			pgctld := seedCleanupPgctld(t, p, paths)
			if entrypoint == "database" {
				seedCleanupMultipooler(t, p, pooler)
			}
			payload := filepath.Join(pooler, "pg_data", "keep")
			require.NoError(t, os.MkdirAll(filepath.Dir(payload), 0o700))
			require.NoError(t, os.WriteFile(payload, []byte("live-worker-data"), 0o600))
			instance := filepath.Join(paths.StateDir, pgbackreststate.InstanceLockName)
			before, err := os.Stat(instance)
			require.NoError(t, err)
			// Models an inherited supervisor lock surviving the daemon/pgctld PID.
			supervisor, err := os.OpenFile(instance, os.O_RDWR, 0)
			require.NoError(t, err)
			defer supervisor.Close()
			require.NoError(t, unix.Flock(int(supervisor.Fd()), unix.LOCK_EX|unix.LOCK_NB))
			clean := func() error {
				if entrypoint == "database" {
					return p.DeprovisionDatabase(t.Context(), "db", "")
				}
				return p.Deprovision(t.Context(), &provisioner.DeprovisionRequest{Service: "pgctld", ServiceID: "cleanup-pooler", DatabaseName: "db", Clean: true})
			}
			require.ErrorIs(t, clean(), pgbackreststate.ErrStateInUse)
			require.Equal(t, int32(1), pgctld.stopCalls.Load(), "PostgreSQL stopped, but that is not supervisor quiescence")
			raw, err := os.ReadFile(payload)
			require.NoError(t, err)
			require.Equal(t, "live-worker-data", string(raw))
			record := filepath.Join(p.getStateDir(), "dbs", "db", "pgctld_cleanup-pooler.json")
			_, err = os.Stat(record)
			require.NoError(t, err)
			if entrypoint == "database" {
				_, err = os.Stat(filepath.Join(p.getStateDir(), "dbs", "db", "multipooler_mp.json"))
				require.NoError(t, err)
			}
			after, err := os.Stat(instance)
			require.NoError(t, err)
			require.True(t, os.SameFile(before, after))
			require.NoError(t, supervisor.Close())
			// Retry uses retained completion evidence, not another PID signal/RPC.
			require.NoError(t, clean())
			require.Equal(t, int32(1), pgctld.stopCalls.Load())
			_, err = os.Stat(payload)
			require.True(t, os.IsNotExist(err))
			_, err = os.Stat(record)
			require.True(t, os.IsNotExist(err))
			after, err = os.Stat(instance)
			require.NoError(t, err)
			require.True(t, os.SameFile(before, after))
		})
	}
}

func TestDirectGenericCleanCannotDeleteLivePostgresData(t *testing.T) {
	p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: t.TempDir()}}
	pooler := filepath.Join(p.getDataDir(), "pooler")
	paths := NewPgBackRestdPaths(pooler)
	freshSupervisedState(t, paths)
	pgctld := seedCleanupPgctld(t, p, paths)
	seedCleanupMultipooler(t, p, pooler)
	payload := filepath.Join(pooler, "pg_data", "keep")
	require.NoError(t, os.MkdirAll(filepath.Dir(payload), 0o700))
	require.NoError(t, os.WriteFile(payload, []byte("postgres-data"), 0o600))
	require.ErrorContains(t, p.deprovisionService(t.Context(), &provisioner.DeprovisionRequest{Service: "multipooler", ServiceID: "mp", DatabaseName: "db", Clean: true}), "PostgreSQL shutdown evidence missing")
	require.Zero(t, pgctld.stopCalls.Load(), "single-service cleanup does not silently stop another service")
	raw, err := os.ReadFile(payload)
	require.NoError(t, err)
	require.Equal(t, "postgres-data", string(raw))
	_, err = os.Stat(filepath.Join(p.getStateDir(), "dbs", "db", "multipooler_mp.json"))
	require.NoError(t, err)
}

func TestDirectCleanRejectsControlPlaneOverlap(t *testing.T) {
	for _, entrypoint := range []string{"service", "database"} {
		for _, overlap := range []string{"root", "lock-log", "state", "record-log", "alias-root", "alias-state", "alias-missing"} {
			t.Run(entrypoint+"/"+overlap, func(t *testing.T) {
				p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: t.TempDir()}}
				ctx, release, err := p.enterSidecarLifecycle(t.Context())
				require.NoError(t, err)
				defer func() { require.NoError(t, release()) }()
				lock := filepath.Join(p.getRootWorkingDir(), ".sidecar-lifecycle.lock")
				before, err := os.Stat(lock)
				require.NoError(t, err)
				data := filepath.Join(p.getRootWorkingDir(), "custom")
				require.NoError(t, os.MkdirAll(data, 0o700))
				payload := filepath.Join(data, "payload")
				require.NoError(t, os.WriteFile(payload, []byte("untouched"), 0o600))
				record := filepath.Join(p.getStateDir(), "dbs", "db", "multiadmin_overlap.json")
				service := &LocalProvisionedService{ID: "overlap", Service: "multiadmin", PID: 1, DataDir: data, Metadata: map[string]any{"shutdown_attempted": true, "shutdown_complete": true}}
				switch overlap {
				case "root":
					service.DataDir = p.getRootWorkingDir()
				case "lock-log":
					service.LogFile = lock
				case "state":
					service.DataDir = p.getStateDir()
				case "record-log":
					service.LogFile = record
				case "alias-root", "alias-state", "alias-missing":
					alias := filepath.Join(t.TempDir(), "alias")
					require.NoError(t, os.Symlink(p.getRootWorkingDir(), alias))
					service.DataDir = filepath.Join(alias, "state")
					if overlap == "alias-root" {
						service.DataDir = alias
					}
					if overlap == "alias-missing" {
						service.DataDir = filepath.Join(service.DataDir, "missing", "child")
					}
				}
				require.NoError(t, p.saveServiceState(service, "db"))
				recordBefore, err := os.Stat(record)
				require.NoError(t, err)
				saved, err := os.ReadFile(record)
				require.NoError(t, err)
				if entrypoint == "database" {
					err = p.DeprovisionDatabase(ctx, "db", "")
				} else {
					err = p.Deprovision(ctx, &provisioner.DeprovisionRequest{Service: service.Service, ServiceID: service.ID, DatabaseName: "db", Clean: true})
				}
				require.ErrorContains(t, err, "unsafe direct cleanup footprint")
				after, err := os.Stat(lock)
				require.NoError(t, err)
				require.True(t, os.SameFile(before, after))
				other, err := p.lockSidecarLifecycle()
				if other != nil {
					_ = other.Close()
				}
				require.Error(t, err, "a competing launcher must remain excluded")
				raw, err := os.ReadFile(record)
				require.NoError(t, err)
				require.Equal(t, saved, raw)
				recordAfter, err := os.Stat(record)
				require.NoError(t, err)
				require.True(t, os.SameFile(recordBefore, recordAfter))
				raw, err = os.ReadFile(payload)
				require.NoError(t, err)
				require.Equal(t, "untouched", string(raw))
			})
		}
	}
}

func TestDirectCleanAllowsNonoverlappingCustomDirectory(t *testing.T) {
	for _, entrypoint := range []string{"service", "database"} {
		t.Run(entrypoint, func(t *testing.T) {
			p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: t.TempDir()}}
			data := filepath.Join(t.TempDir(), "custom")
			require.NoError(t, os.MkdirAll(data, 0o700))
			require.NoError(t, os.WriteFile(filepath.Join(data, "payload"), []byte("done"), 0o600))
			service := &LocalProvisionedService{ID: "custom", Service: "multiadmin", PID: 1, DataDir: data, Metadata: map[string]any{"shutdown_attempted": true, "shutdown_complete": true}}
			require.NoError(t, p.saveServiceState(service, "db"))
			if entrypoint == "database" {
				require.NoError(t, p.DeprovisionDatabase(t.Context(), "db", ""))
			} else {
				require.NoError(t, p.Deprovision(t.Context(), &provisioner.DeprovisionRequest{Service: service.Service, ServiceID: service.ID, DatabaseName: "db", Clean: true}))
			}
			_, err := os.Stat(data)
			require.ErrorIs(t, err, os.ErrNotExist)
			_, err = os.Stat(filepath.Join(p.getStateDir(), "dbs", "db", "multiadmin_custom.json"))
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}

func TestFileCleanupIsNotRecursive(t *testing.T) {
	dir := t.TempDir()
	payload := filepath.Join(dir, "keep")
	require.NoError(t, os.WriteFile(payload, []byte("keep"), 0o600))
	require.Error(t, removeFilePreservingLocks(dir, nil))
	_, err := os.Stat(payload)
	require.NoError(t, err)
}

func TestDirectPoolerCleanMissingStateIsNotNeverStartedProof(t *testing.T) {
	p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: t.TempDir()}}
	pooler := filepath.Join(p.getDataDir(), "pooler")
	seedCleanupMultipooler(t, p, pooler)
	require.NoError(t, os.MkdirAll(pooler, 0o700))
	payload := filepath.Join(pooler, "keep")
	require.NoError(t, os.WriteFile(payload, []byte("unverified"), 0o600))
	require.ErrorContains(t, p.deprovisionService(t.Context(), &provisioner.DeprovisionRequest{Service: "multipooler", ServiceID: "mp", DatabaseName: "db", Clean: true}), "PostgreSQL shutdown evidence missing")
	_, err := os.Stat(payload)
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(p.getStateDir(), "dbs", "db", "multipooler_mp.json"))
	require.NoError(t, err)
}
