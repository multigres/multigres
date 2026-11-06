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
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/multigres/multigres/go/common/pgbackreststate"
	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
	"github.com/multigres/multigres/go/pb/pgbackrestdservice/pgbackrestdserviceconnect"
	"github.com/multigres/multigres/go/tools/executil"
)

type ownerFixture struct {
	pgbackrestdserviceconnect.UnimplementedPgBackRestdHandler
	paths    PgBackRestdPaths
	instance string
	stop     chan struct{}
	once     sync.Once
}

func (f *ownerFixture) mode() string {
	raw, _ := os.ReadFile(filepath.Join(f.paths.Dir, "fixture-mode"))
	return string(raw)
}

func (f *ownerFixture) Status(context.Context, *connect.Request[pb.StatusRequest]) (*connect.Response[pb.StatusResponse], error) {
	status := &pb.StatusResponse{InstanceId: f.instance, LifecycleProtocolVersion: 1}
	switch f.mode() {
	case "old-protocol":
		status.LifecycleProtocolVersion = 0
	case "missing-instance":
		status.InstanceId = ""
	}
	return connect.NewResponse(status), nil
}

func (f *ownerFixture) Shutdown(ctx context.Context, req *connect.Request[pb.ShutdownRequest]) (*connect.Response[pb.ShutdownResponse], error) {
	if req.Msg.GetInstanceId() == "" || req.Msg.GetInstanceId() != f.instance {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("instance identity mismatch"))
	}
	if f.mode() == "old-protocol" {
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("legacy protocol"))
	}
	switch f.mode() {
	case "request-timeout":
		<-ctx.Done()
		return nil, ctx.Err()
	case "accepted-no-exit":
		return connect.NewResponse(&pb.ShutdownResponse{Accepted: true}), nil
	case "reject":
		return connect.NewResponse(&pb.ShutdownResponse{}), nil
	case "disconnect":
		panic(http.ErrAbortHandler)
	}
	f.once.Do(func() { close(f.stop) })
	return connect.NewResponse(&pb.ShutdownResponse{Accepted: true}), nil
}

// Fresh authoritative no-worker fixture: initialize under the exclusive real
// lifetime lock and hold it until listeners drain. This is not a worker mock.
func serveOwnerFixture(paths PgBackRestdPaths) error {
	if err := os.MkdirAll(paths.StateDir, 0o700); err != nil {
		return err
	}
	lock, err := os.OpenFile(filepath.Join(paths.StateDir, pgbackreststate.InstanceLockName), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return err
	}
	if err := pgbackreststate.RecordSupervision(paths.StateDir, lock); err != nil {
		return err
	}
	wal, err := net.Listen("unix", paths.WALSocket)
	if err != nil {
		return err
	}
	defer wal.Close()
	control, err := net.Listen("unix", paths.ControlSocket)
	if err != nil {
		return err
	}
	defer control.Close()
	f := &ownerFixture{paths: paths, instance: uuid.NewString(), stop: make(chan struct{})}
	path, handler := pgbackrestdserviceconnect.NewPgBackRestdHandler(f)
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	server := &http.Server{Handler: mux, ReadHeaderTimeout: time.Second}
	drained := make(chan struct{})
	go func() {
		<-f.stop
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
		close(drained)
	}()
	err = server.Serve(control)
	if errors.Is(err, http.ErrServerClosed) {
		<-drained
		return nil
	}
	return err
}

// This subprocess has real argv/birth-time identity and serves the real Status
// protocol. It never invokes PostgreSQL or pgBackRest.
func TestSidecarProcessFixture(t *testing.T) {
	for i, arg := range os.Args {
		if arg != "--sidecar-fixture" {
			continue
		}
		dir := os.Args[i+1]
		paths := NewPgBackRestdPaths(filepath.Join(dir, "pooler"))
		require.NoError(t, serveOwnerFixture(paths))
		return
	}
}

func ownerTestPaths(t *testing.T) PgBackRestdPaths {
	t.Helper()
	// macOS's per-test temp directory is too long for AF_UNIX. This is a
	// process fixture, not testkit: use a short root and clean it explicitly.
	dir, err := os.MkdirTemp("/tmp", "sco-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	paths := NewPgBackRestdPaths(filepath.Join(dir, "pooler"))
	require.NoError(t, paths.Prepare("pw"))
	return paths
}

func fixtureArgs(paths PgBackRestdPaths) []string {
	return []string{"-test.run=^TestSidecarProcessFixture$", "--", "--sidecar-fixture", filepath.Dir(paths.Dir)}
}

// runSidecarOwnerFixture consumes the actual serve identity flags, so the real
// local launch path can be exercised without PostgreSQL or a cluster.
func runSidecarOwnerFixture(args []string) int {
	flags := map[string]string{}
	for i := 0; i < len(args); i++ {
		if strings.Contains(args[i], "=") {
			continue
		}
		if i+1 >= len(args) {
			return 2
		}
		flags[args[i]] = args[i+1]
		i++
	}
	paths := NewPgBackRestdPaths(flags["--data-volume"])
	if flags["--state-dir"] != paths.StateDir || flags["--wal-socket"] != paths.WALSocket || flags["--control-socket"] != paths.ControlSocket {
		return 3
	}
	if err := serveOwnerFixture(paths); err != nil {
		return 4
	}
	return 0
}

func TestSidecarLaunchReprovisionsAfterPgctldDeath(t *testing.T) {
	paths := ownerTestPaths(t)
	pooler := filepath.Join(filepath.Dir(paths.Dir), "pooler")
	executable, err := os.Executable()
	require.NoError(t, err)
	binary := filepath.Join(filepath.Dir(paths.Dir), "pgbackrestd")
	require.NoError(t, os.Symlink(executable, binary))
	t.Setenv("MULTIGRES_PGBACKREST_SANDBOX", "off")
	p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: filepath.Dir(paths.Dir)}}
	ctx := t.Context()
	launch := func() (int, error) {
		return p.startPgBackRestd(ctx, binary, pooler, "postgres", "pw", "", 5432, 8432, 0)
	}
	pgctld := executil.Command(ctx, "/bin/sleep", "60")
	require.NoError(t, pgctld.Start())
	go func() { _ = pgctld.Wait() }()
	t.Cleanup(func() { _, _ = pgctld.Stop(context.Background()) })
	pid, err := launch()
	require.NoError(t, err)
	t.Cleanup(func() { _ = paths.stopOwned(context.Background()) })
	_, stopped := pgctld.Kill(context.Background())
	require.True(t, stopped)
	adopted, err := launch()
	require.NoError(t, err)
	require.Equal(t, pid, adopted)
	owner, err := paths.loadOwner()
	require.NoError(t, err)
	// Legacy installations only recorded sidecar ownership in pgctld state.
	require.NoError(t, os.Remove(paths.ownerFile()))
	legacy := &LocalProvisionedService{
		BinaryPath: filepath.Join(filepath.Dir(binary), "pgctld"), DataDir: pooler,
		Metadata: map[string]any{"pgbackrestd_pid": float64(pid)},
	}
	require.ErrorContains(t, paths.stopOwned(context.Background(), legacy), "legacy sidecar ownership is unsupported")
	alive, err := owner.alive()
	require.NoError(t, err)
	require.True(t, alive, "a legacy record must never authorize termination")
	// Restore the exact record retained by this test's launcher for cleanup.
	require.NoError(t, paths.saveOwner(owner))
	require.NoError(t, paths.stopOwned(context.Background()))
	require.Eventually(t, func() bool { alive, err := owner.alive(); return !alive && err == nil }, 3*time.Second, 10*time.Millisecond)
}

func TestSidecarOwnershipSurvivesPgctldDeath(t *testing.T) {
	paths := ownerTestPaths(t)
	binary, err := os.Executable()
	require.NoError(t, err)
	// Simulate the independent pgctld process dying, leaving sidecar owner state.
	pgctld := executil.Command(context.Background(), "/bin/sleep", "60")
	require.NoError(t, pgctld.Start())
	go func() { _ = pgctld.Wait() }()
	t.Cleanup(func() { _, _ = pgctld.Stop(context.Background()) })
	lock, err := paths.lockOwner()
	require.NoError(t, err)
	pid, err := paths.ensureSidecar(context.Background(), binary, fixtureArgs(paths))
	require.NoError(t, err)
	require.NoError(t, lock.Close())
	t.Cleanup(func() { _ = paths.stopOwned(context.Background()) })
	_, stopped := pgctld.Kill(context.Background())
	require.True(t, stopped)

	// A fresh provisioner consults the durable record, not pgctld liveness.
	lock, err = paths.lockOwner()
	require.NoError(t, err)
	adopted, err := paths.ensureSidecar(context.Background(), binary, fixtureArgs(paths))
	require.NoError(t, err)
	require.Equal(t, pid, adopted, "must not launch a second sidecar")
	require.NoError(t, lock.Close())
	owner, err := paths.loadOwner()
	require.NoError(t, err)
	require.NoError(t, paths.stopOwned(context.Background()))
	require.Eventually(t, func() bool { alive, err := owner.alive(); return !alive && err == nil }, 3*time.Second, 10*time.Millisecond)
}

func TestSidecarOwnershipRefusesReusedPIDAndConcurrentLaunch(t *testing.T) {
	paths := ownerTestPaths(t)
	lock, err := paths.lockOwner()
	require.NoError(t, err)
	_, err = paths.lockOwner()
	require.Error(t, err)
	require.NoError(t, lock.Close())
	identity, err := readSidecarIdentity(os.Getpid())
	require.NoError(t, err)
	identity.Created--
	require.NoError(t, paths.saveOwner(identity))
	_, err = paths.ensureSidecar(t.Context(), os.Args[0], nil)
	require.ErrorContains(t, err, "identity changed")
	require.ErrorContains(t, paths.stopOwned(t.Context()), "identity changed")
}

func TestSidecarRefusesUnhealthyOrUnrecordedOwner(t *testing.T) {
	paths := ownerTestPaths(t)
	listener, err := net.Listen("unix", paths.WALSocket)
	require.NoError(t, err)
	defer listener.Close()
	_, err = paths.ensureSidecar(t.Context(), "/must-not-start", nil)
	require.ErrorContains(t, err, "unrecorded sidecar")
	require.True(t, socketAccepts(paths.WALSocket), "must not replace existing sockets")
	conn, err := dialOwnedSidecar(t.Context(), paths.WALSocket, os.Getpid())
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	_, err = dialOwnedSidecar(t.Context(), paths.WALSocket, os.Getpid()+1)
	require.ErrorContains(t, err, "peer identity mismatch")

	identity, err := readSidecarIdentity(os.Getpid())
	require.NoError(t, err)
	require.NoError(t, paths.saveOwner(identity))
	_, err = paths.ensureSidecar(t.Context(), identity.Argv[0], identity.Argv[1:])
	require.ErrorContains(t, err, "existing sidecar unhealthy")
	_, err = paths.ensureSidecar(t.Context(), identity.Argv[0], []string{"different-config"})
	require.ErrorContains(t, err, "configuration differs")
}

func TestLegacyMetadataCannotAuthorizeRecovery(t *testing.T) {
	paths := ownerTestPaths(t)
	for _, pid := range []int{os.Getpid(), 2147483647} {
		require.ErrorContains(t, paths.rejectLegacyOwnership(pid), "legacy sidecar ownership is unsupported")
		_, err := paths.legacyOwnerForStop(&LocalProvisionedService{Metadata: map[string]any{"pgbackrestd_pid": float64(pid)}})
		require.ErrorContains(t, err, "legacy sidecar ownership is unsupported")
	}
	owner, err := paths.loadOwner()
	require.NoError(t, err)
	require.Nil(t, owner, "never synthesize ownership from legacy PID metadata")
}

func TestSidecarFailedLaunchDoesNotLeaveAProcess(t *testing.T) {
	paths := ownerTestPaths(t)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := paths.ensureSidecar(ctx, "/bin/sleep", []string{"60"})
	require.Error(t, err)
	owner, err := paths.loadOwner()
	require.NoError(t, err)
	require.NotNil(t, owner)
	require.Eventually(t, func() bool { alive, err := owner.alive(); return !alive && err == nil }, 3*time.Second, 10*time.Millisecond)
}

// These filesystem tests exercise the real write/rename/directory-sync path.
// They cannot simulate power loss; no production I/O hooks are introduced just
// to count Sync calls.
func TestSidecarOwnerPersistenceTransitions(t *testing.T) {
	paths := ownerTestPaths(t)
	for _, pid := range []int{101, 202} {
		owner := &sidecarOwner{PID: pid, Created: 12345, Argv: []string{"pgbackrestd", "serve"}}
		require.NoError(t, paths.saveOwner(owner))
		got, err := paths.loadOwner()
		require.NoError(t, err)
		require.Equal(t, owner, got)
		info, err := os.Stat(paths.ownerFile())
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	}
	temps, err := filepath.Glob(filepath.Join(paths.Dir, ".tmp-*"))
	require.NoError(t, err)
	require.Empty(t, temps)
	require.NoError(t, paths.removeOwner())
	got, err := paths.loadOwner()
	require.NoError(t, err)
	require.Nil(t, got)
	require.NoError(t, paths.removeOwner(), "idempotent unlink still syncs the directory")

	missing := PgBackRestdPaths{Dir: filepath.Join(paths.Dir, "missing")}
	require.ErrorContains(t, missing.syncOwnerDirectory(), "open sidecar ownership directory")
	require.Error(t, missing.removeOwner(), "an absent owner must not hide directory sync errors")
}

func TestS3KeysPreserveRotateAndRemoveSessionToken(t *testing.T) {
	paths := NewPgBackRestdPaths(filepath.Join(t.TempDir(), "pooler"))
	require.NoError(t, os.MkdirAll(paths.s3KeyDir(), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(paths.s3KeyDir(), "AWS_SESSION_TOKEN"), []byte("stale-legacy-token"), 0o600))
	t.Setenv("AWS_ACCESS_KEY_ID", "key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	for _, token := range []string{"first-token", "rotated-token", ""} {
		t.Setenv("AWS_SESSION_TOKEN", token)
		dir, err := paths.writeS3KeysFromEnv()
		require.NoError(t, err)
		raw, err := os.ReadFile(filepath.Join(dir, "credentials.json"))
		require.NoError(t, err)
		var keys map[string]string
		require.NoError(t, json.Unmarshal(raw, &keys))
		require.Equal(t, map[string]string{"access_key_id": "key", "secret_access_key": "secret", "session_token": token}, keys)
		_, err = os.Stat(filepath.Join(dir, "AWS_SESSION_TOKEN"))
		require.True(t, os.IsNotExist(err))
		info, err := os.Stat(filepath.Join(dir, "credentials.json"))
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	}
	before, err := os.ReadFile(filepath.Join(paths.s3KeyDir(), "credentials.json"))
	require.NoError(t, err)
	t.Setenv("AWS_ACCESS_KEY_ID", "new-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	_, err = paths.writeS3KeysFromEnv()
	require.Error(t, err)
	after, err := os.ReadFile(filepath.Join(paths.s3KeyDir(), "credentials.json"))
	require.NoError(t, err)
	require.Equal(t, before, after, "failed generation leaves previous bundle intact")
}

func TestS3BundleRenameFailureDoesNotRewriteLegacyKeys(t *testing.T) {
	paths := NewPgBackRestdPaths(filepath.Join(t.TempDir(), "pooler"))
	require.NoError(t, os.MkdirAll(filepath.Join(paths.s3KeyDir(), "credentials.json"), 0o700))
	legacy := filepath.Join(paths.s3KeyDir(), "AWS_SESSION_TOKEN")
	require.NoError(t, os.WriteFile(legacy, []byte("old-token"), 0o600))
	t.Setenv("AWS_ACCESS_KEY_ID", "new-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "new-secret")
	t.Setenv("AWS_SESSION_TOKEN", "new-token")
	_, err := paths.writeS3KeysFromEnv()
	require.Error(t, err)
	raw, err := os.ReadFile(legacy)
	require.NoError(t, err)
	require.Equal(t, "old-token", string(raw))
}
