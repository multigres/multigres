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
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/common/pgbackreststate"
	pb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/tools/executil"
)

// freshSupervisedState models startup of a fresh, private test instance with
// no workers. Only the authentic shared API writes the protocol version.
func freshSupervisedState(t *testing.T, paths PgBackRestdPaths) {
	t.Helper()
	require.NoError(t, os.MkdirAll(paths.StateDir, 0o700))
	lock, err := os.OpenFile(filepath.Join(paths.StateDir, pgbackreststate.InstanceLockName), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	require.NoError(t, err)
	t.Cleanup(func() { _ = lock.Close() })
	require.NoError(t, unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB))
	require.NoError(t, pgbackreststate.RecordSupervision(paths.StateDir, lock))
	require.NoError(t, lock.Close())
}

type cleanupPgctld struct {
	statusOverride *pb.ServerStatus
	pb.UnimplementedPgCtldServer
	stopped   atomic.Bool
	stopCalls atomic.Int32
}

func (s *cleanupPgctld) Status(context.Context, *pb.StatusRequest) (*pb.StatusResponse, error) {
	if s.statusOverride != nil {
		return &pb.StatusResponse{Status: *s.statusOverride}, nil
	}
	state := pb.ServerStatus_RUNNING
	if s.stopped.Load() {
		state = pb.ServerStatus_STOPPED
	}
	return &pb.StatusResponse{Status: state}, nil
}

func (s *cleanupPgctld) Stop(context.Context, *pb.StopRequest) (*pb.StopResponse, error) {
	s.stopCalls.Add(1)
	s.stopped.Store(true)
	return &pb.StopResponse{}, nil
}

// A real recorded process and Status/Stop RPC fixture require Teardown to
// establish PostgreSQL shutdown separately from the worker-only sidecar stamp.
func seedCleanupPgctld(t *testing.T, p *localProvisioner, paths PgBackRestdPaths) *cleanupPgctld {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	mock := &cleanupPgctld{}
	server := grpc.NewServer()
	pb.RegisterPgCtldServer(server, mock)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	child := executil.Command(context.Background(), "/bin/sleep", "60")
	require.NoError(t, child.Start())
	go func() { _ = child.Wait() }()
	t.Cleanup(func() { _, _ = child.Stop(context.Background()) })
	pooler := filepath.Join(filepath.Dir(paths.Dir), strings.TrimPrefix(filepath.Base(paths.Dir), "pgbackrestd-"))
	service := &LocalProvisionedService{ID: "cleanup-" + filepath.Base(pooler), Service: "pgctld", PID: child.Process.Pid, DataDir: pooler, Ports: map[string]int{"grpc_port": listener.Addr().(*net.TCPAddr).Port}, Metadata: map[string]any{"cell": "zone"}}
	require.NoError(t, p.saveServiceState(service, "db"))
	return mock
}

// This tests selective deletion only; its plain files do not claim to be a
// versioned server lock or substitute for an authoritative cleanup guard.
func TestCleanupPreservesLockInodesAndSkeleton(t *testing.T) {
	root := t.TempDir()
	paths := NewPgBackRestdPaths(filepath.Join(root, "data", "pooler"))
	require.NoError(t, os.MkdirAll(paths.StateDir, 0o700))
	held := &sidecarCleanupExclusion{paths: []PgBackRestdPaths{paths}}
	keep := held.preservedPaths()
	before := map[string]os.FileInfo{}
	for _, path := range keep {
		require.NoError(t, os.WriteFile(path, []byte("unit-test-inode"), 0o600))
		info, err := os.Stat(path)
		require.NoError(t, err)
		before[path] = info
	}
	for _, path := range []string{filepath.Join(paths.StateDir, "disposable"), filepath.Join(root, "data", "pooler", "pg_data", "disposable")} {
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
		require.NoError(t, os.WriteFile(path, []byte("old"), 0o600))
	}
	require.NoError(t, cleanupPreservingLocks(filepath.Join(root, "data"), keep))
	for _, path := range keep {
		after, err := os.Stat(path)
		require.NoError(t, err)
		require.True(t, os.SameFile(before[path], after), path)
	}
	_, err := os.Stat(filepath.Join(paths.StateDir, "disposable"))
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(filepath.Join(root, "data", "pooler"))
	require.True(t, os.IsNotExist(err))
}

func TestOuterLifecycleLockSurvivesCleanedSubtrees(t *testing.T) {
	p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: t.TempDir()}}
	lock, err := p.lockSidecarLifecycle()
	require.NoError(t, err)
	defer lock.Close()
	before, err := lock.Stat()
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(p.getDataDir(), 0o700))
	require.NoError(t, cleanupPreservingLocks(p.getDataDir(), nil))
	_, err = p.lockSidecarLifecycle()
	require.Error(t, err, "concurrent provisioning must remain excluded")
	after, err := os.Stat(filepath.Join(p.getRootWorkingDir(), ".sidecar-lifecycle.lock"))
	require.NoError(t, err)
	require.True(t, os.SameFile(before, after))
}

func TestCleanupFailureReleasesLifecycleLocks(t *testing.T) {
	p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: t.TempDir()}}
	paths := NewPgBackRestdPaths(filepath.Join(p.getDataDir(), "pooler"))
	require.NoError(t, os.MkdirAll(paths.StateDir, 0o700))
	seedCleanupPgctld(t, p, paths)
	require.ErrorIs(t, p.Teardown(t.Context(), true), pgbackreststate.ErrUnverifiedState, "missing versioned lock is not proof of never started")
	outer, err := p.lockSidecarLifecycle()
	require.NoError(t, err)
	require.NoError(t, outer.Close())
	installation, err := paths.lockFile("lifecycle.lock")
	require.NoError(t, err)
	require.NoError(t, installation.Close())
	_, err = os.Stat(paths.StateDir)
	require.NoError(t, err)
}

func TestCleanupInventoryIncludesExternalAndUnrecordedInstallations(t *testing.T) {
	root, external := t.TempDir(), t.TempDir()
	p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: root, Cells: map[string]CellServicesConfig{"outside": {Pgctld: PgctldConfig{PoolerDir: filepath.Join(external, "pooler")}}}}}
	unknown := NewPgBackRestdPaths(filepath.Join(p.getDataDir(), "lost-pooler"))
	require.NoError(t, os.MkdirAll(unknown.StateDir, 0o700))
	inventory, err := p.sidecarCleanupInventory(nil)
	require.NoError(t, err)
	require.Len(t, inventory, 2)
	dirs := []string{inventory[0].Dir, inventory[1].Dir}
	require.Contains(t, dirs, unknown.Dir)
	require.Contains(t, dirs, NewPgBackRestdPaths(filepath.Join(external, "pooler")).Dir)
}

func TestTeardownCleanPreservesVersionedLockInodes(t *testing.T) {
	root := t.TempDir()
	pooler := filepath.Join(root, "data", "pooler")
	paths := NewPgBackRestdPaths(pooler)
	freshSupervisedState(t, paths)
	p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: root, Cells: map[string]CellServicesConfig{"zone": {Pgctld: PgctldConfig{PoolerDir: pooler}}}}}
	pgctld := seedCleanupPgctld(t, p, paths)
	ownerLock, err := paths.lockOwner()
	require.NoError(t, err)
	require.NoError(t, ownerLock.Close())
	instance := filepath.Join(paths.StateDir, pgbackreststate.InstanceLockName)
	before, err := os.Stat(instance)
	require.NoError(t, err)
	disposable := filepath.Join(pooler, "pg_data", "payload")
	require.NoError(t, os.MkdirAll(filepath.Dir(disposable), 0o700))
	require.NoError(t, os.WriteFile(disposable, []byte("data"), 0o600))
	require.NoError(t, os.WriteFile(paths.PasswordFile, []byte("secret"), 0o600))
	require.NoError(t, p.Teardown(t.Context(), true))
	require.Equal(t, int32(1), pgctld.stopCalls.Load())
	after, err := os.Stat(instance)
	require.NoError(t, err)
	require.True(t, os.SameFile(before, after))
	_, err = os.Stat(disposable)
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(paths.PasswordFile)
	require.True(t, os.IsNotExist(err))
	// Both a direct daemon and subsequent clean see the same versioned inode.
	guard, err := pgbackreststate.AcquireCleanupGuard(paths.StateDir)
	require.NoError(t, err)
	require.NoError(t, guard.Close())
	require.ErrorContains(t, p.Teardown(t.Context(), true), "PostgreSQL shutdown evidence missing", "a subsequent clean without the removed pgctld record must refuse")
	after, err = os.Stat(instance)
	require.NoError(t, err)
	require.True(t, os.SameFile(before, after))
}

func TestLaterGuardFailurePreventsEveryDeletion(t *testing.T) {
	root := t.TempDir()
	p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: root}}
	a := NewPgBackRestdPaths(filepath.Join(p.getDataDir(), "a"))
	b := NewPgBackRestdPaths(filepath.Join(p.getDataDir(), "b"))
	freshSupervisedState(t, a)
	freshSupervisedState(t, b)
	seedCleanupPgctld(t, p, a)
	seedCleanupPgctld(t, p, b)
	payloads := []string{filepath.Join(a.StateDir, "keep"), filepath.Join(b.StateDir, "keep"), filepath.Join(p.getDataDir(), "keep")}
	for _, path := range payloads {
		require.NoError(t, os.WriteFile(path, []byte("keep"), 0o600))
	}
	busy, err := os.OpenFile(filepath.Join(b.StateDir, pgbackreststate.InstanceLockName), os.O_RDWR, 0)
	require.NoError(t, err)
	defer busy.Close()
	require.NoError(t, unix.Flock(int(busy.Fd()), unix.LOCK_EX|unix.LOCK_NB))
	require.ErrorIs(t, p.Teardown(t.Context(), true), pgbackreststate.ErrStateInUse)
	for _, path := range payloads {
		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, "keep", string(raw))
	}
	// The earlier successful guard and local lifecycle locks were released.
	guard, err := pgbackreststate.AcquireCleanupGuard(a.StateDir)
	require.NoError(t, err)
	require.NoError(t, guard.Close())
	outer, err := p.lockSidecarLifecycle()
	require.NoError(t, err)
	require.NoError(t, outer.Close())
	for _, paths := range []PgBackRestdPaths{a, b} {
		lock, err := paths.lockFile("lifecycle.lock")
		require.NoError(t, err)
		require.NoError(t, lock.Close())
	}
}

func TestCleanupExclusionHeldThroughDeletionAndGuardRelease(t *testing.T) {
	root := t.TempDir()
	pooler := filepath.Join(root, "data", "pooler")
	paths := NewPgBackRestdPaths(pooler)
	freshSupervisedState(t, paths)
	p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: root, Cells: map[string]CellServicesConfig{"zone": {Pgctld: PgctldConfig{PoolerDir: pooler, PgPassword: "pw"}}}}}
	seedCleanupPgctld(t, p, paths)
	outer, err := p.lockSidecarLifecycle()
	require.NoError(t, err)
	defer outer.Close()
	held, err := lockSidecarInstallations([]PgBackRestdPaths{paths})
	require.NoError(t, err)
	defer held.Close()
	services, err := p.loadShutdownServices()
	require.NoError(t, err)
	for _, service := range services {
		require.NoError(t, p.shutdownService(t.Context(), service))
	}
	require.NoError(t, held.requirePostgresShutdown(services))
	require.NoError(t, held.acquireCleanupGuards())
	for _, afterDeletion := range []bool{false, true} {
		if afterDeletion {
			require.NoError(t, cleanupPreservingLocks(p.getDataDir(), held.preservedPaths()))
		}
		_, err := pgbackreststate.AcquireCleanupGuard(paths.StateDir)
		require.ErrorIs(t, err, pgbackreststate.ErrStateInUse, "direct daemon must remain excluded")
		_, err = p.provisionPgctld(t.Context(), "db", "default", "pooler", "zone")
		require.ErrorContains(t, err, "ownership is busy", "local reprovision must remain excluded")
		_, err = paths.lockFile("lifecycle.lock")
		require.Error(t, err)
	}
	require.NoError(t, held.Close())
	_, err = p.lockSidecarLifecycle()
	require.Error(t, err, "outer exclusion remains until after guard release")
	require.NoError(t, outer.Close())
	guard, err := pgbackreststate.AcquireCleanupGuard(paths.StateDir)
	require.NoError(t, err)
	require.NoError(t, guard.Close())
}

func TestOuterLifecycleLeaseNestsWithoutAllowingStaleContext(t *testing.T) {
	p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: t.TempDir()}}
	ctx, release, err := p.enterSidecarLifecycle(t.Context())
	require.NoError(t, err)
	_, innerRelease, err := p.enterSidecarLifecycle(ctx)
	require.NoError(t, err)
	require.NoError(t, innerRelease())
	_, err = p.lockSidecarLifecycle()
	require.Error(t, err)
	require.NoError(t, release())
	competing, err := p.lockSidecarLifecycle()
	require.NoError(t, err)
	defer competing.Close()
	_, _, err = p.enterSidecarLifecycle(ctx)
	require.Error(t, err, "expired context must not bypass a new owner")
	_, err = p.Bootstrap(t.Context())
	require.ErrorContains(t, err, "ownership is busy")
	_, err = p.ProvisionDatabase(t.Context(), "db", "")
	require.ErrorContains(t, err, "ownership is busy")
}

func TestCleanupInventoriesEveryDeletionRootAndSiblingBoundary(t *testing.T) {
	for _, rootName := range []string{"data", "sockets", "spool", "logs", "state"} {
		for _, layout := range []string{"nested", "boundary"} {
			t.Run(rootName+"/"+layout, func(t *testing.T) {
				root := t.TempDir()
				p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: root}}
				pooler := filepath.Join(root, rootName)
				if layout == "nested" {
					pooler = filepath.Join(pooler, "pooler")
				}
				paths := NewPgBackRestdPaths(pooler)
				freshSupervisedState(t, paths)
				payload := filepath.Join(pooler, "pg_data", "keep")
				require.NoError(t, os.MkdirAll(filepath.Dir(payload), 0o700))
				require.NoError(t, os.WriteFile(payload, []byte("live-data"), 0o600))
				statePayload := filepath.Join(paths.StateDir, "keep")
				require.NoError(t, os.WriteFile(statePayload, []byte("live-state"), 0o600))
				instance := filepath.Join(paths.StateDir, pgbackreststate.InstanceLockName)
				busy, err := os.OpenFile(instance, os.O_RDWR, 0)
				require.NoError(t, err)
				defer busy.Close()
				require.NoError(t, unix.Flock(int(busy.Fd()), unix.LOCK_EX|unix.LOCK_NB))
				before, err := os.Stat(instance)
				require.NoError(t, err)
				inventory, err := p.sidecarCleanupInventory(nil)
				require.NoError(t, err)
				found := false
				for _, entry := range inventory {
					found = found || entry.Dir == paths.Dir
				}
				require.True(t, found, "destructive footprint must include this installation")
				require.ErrorContains(t, p.Teardown(t.Context(), true), "PostgreSQL shutdown evidence missing")
				for path, want := range map[string]string{payload: "live-data", statePayload: "live-state"} {
					raw, err := os.ReadFile(path)
					require.NoError(t, err)
					require.Equal(t, want, string(raw))
				}
				after, err := os.Stat(instance)
				require.NoError(t, err)
				require.True(t, os.SameFile(before, after))
			})
		}
	}
}

func TestQuiescentSidecarCannotAuthorizeUnrecordedPostgresDeletion(t *testing.T) {
	root := t.TempDir()
	p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: root}}
	pooler := filepath.Join(p.getDataDir(), "pooler")
	paths := NewPgBackRestdPaths(pooler)
	freshSupervisedState(t, paths)
	// A separate live process models PostgreSQL, which is not a supervised
	// pgBackRest child. The no-worker sidecar stamp says nothing about it.
	postgres := executil.Command(context.Background(), "/bin/sleep", "60")
	require.NoError(t, postgres.Start())
	go func() { _ = postgres.Wait() }()
	t.Cleanup(func() { _, _ = postgres.Stop(context.Background()) })
	payload := filepath.Join(pooler, "pg_data", "keep")
	require.NoError(t, os.MkdirAll(filepath.Dir(payload), 0o700))
	require.NoError(t, os.WriteFile(payload, []byte("live-postgres-data"), 0o600))
	guard, err := pgbackreststate.AcquireCleanupGuard(paths.StateDir)
	require.NoError(t, err)
	require.NoError(t, guard.Close())
	require.ErrorContains(t, p.Teardown(t.Context(), true), "PostgreSQL shutdown evidence missing")
	raw, err := os.ReadFile(payload)
	require.NoError(t, err)
	require.Equal(t, "live-postgres-data", string(raw))
	_, err = readSidecarIdentity(postgres.Process.Pid)
	require.NoError(t, err)
}

func TestCleanupNeedsEvidenceForEachInstallation(t *testing.T) {
	p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: t.TempDir()}}
	a := NewPgBackRestdPaths(filepath.Join(p.getDataDir(), "a"))
	b := NewPgBackRestdPaths(filepath.Join(p.getDataDir(), "b"))
	freshSupervisedState(t, a)
	freshSupervisedState(t, b)
	pgctld := seedCleanupPgctld(t, p, a)
	payload := filepath.Join(a.StateDir, "keep")
	require.NoError(t, os.WriteFile(payload, []byte("must-remain"), 0o600))
	require.ErrorContains(t, p.Teardown(t.Context(), true), "PostgreSQL shutdown evidence missing for installation "+filepath.Join(p.getDataDir(), "b"))
	require.Equal(t, int32(1), pgctld.stopCalls.Load())
	raw, err := os.ReadFile(payload)
	require.NoError(t, err)
	require.Equal(t, "must-remain", string(raw))
}

func TestPostgresShutdownRejectsAmbiguousStatus(t *testing.T) {
	for _, state := range []pb.ServerStatus{pb.ServerStatus_UNKNOWN, pb.ServerStatus_STARTING, pb.ServerStatus_STOPPING} {
		t.Run(state.String(), func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			mock := &cleanupPgctld{statusOverride: &state}
			server := grpc.NewServer()
			pb.RegisterPgCtldServer(server, mock)
			go func() { _ = server.Serve(listener) }()
			t.Cleanup(server.Stop)
			p := &localProvisioner{}
			require.ErrorContains(t, p.stopPostgreSQLViaPgctld(t.Context(), listener.Addr().String()), "not confirmed stopped")
			require.Zero(t, mock.stopCalls.Load())
		})
	}
}
