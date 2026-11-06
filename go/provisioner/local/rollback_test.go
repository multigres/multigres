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
	"runtime"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/tools/executil"
)

type rollbackPgctld struct {
	pb.UnimplementedPgCtldServer
	statusCalls atomic.Int32
	stopCalls   atomic.Int32
}

func (s *rollbackPgctld) Status(context.Context, *pb.StatusRequest) (*pb.StatusResponse, error) {
	s.statusCalls.Add(1)
	return &pb.StatusResponse{Status: pb.ServerStatus_RUNNING}, nil
}

func (s *rollbackPgctld) Stop(context.Context, *pb.StopRequest) (*pb.StopResponse, error) {
	s.stopCalls.Add(1)
	return nil, status.Error(codes.Unavailable, "cannot establish shutdown")
}

func TestRollbackRetainsAdoptedSidecarOnUnknownPostgresShutdown(t *testing.T) {
	paths := ownerTestPaths(t)
	binary, err := os.Executable()
	require.NoError(t, err)
	pid, err := paths.ensureSidecar(t.Context(), binary, fixtureArgs(paths))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, paths.stopOwned(context.Background())) })
	before, err := os.ReadFile(paths.ownerFile())
	require.NoError(t, err)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	pb.RegisterPgCtldServer(server, &rollbackPgctld{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	cmd := executil.Command(context.Background(), "/bin/sleep", "60")
	require.NoError(t, cmd.Start())
	go func() { _ = cmd.Wait() }()
	t.Cleanup(func() { _, _ = cmd.Stop(context.Background()) })
	p := &localProvisioner{}
	require.ErrorContains(t, p.rollbackPgctld(t.Context(), cmd, paths, true, listener.Addr().String()), "shutdown unknown")
	owner, err := readSidecarIdentity(pid)
	require.NoError(t, err)
	alive, err := owner.alive()
	require.NoError(t, err)
	require.True(t, alive)
	after, err := os.ReadFile(paths.ownerFile())
	require.NoError(t, err)
	require.Equal(t, before, after)
	_, err = readSidecarIdentity(cmd.Process.Pid)
	require.NoError(t, err, "pgctld must also be retained")
}

func TestProvisionRollbackJoinsFailureAndPreservesAdoptedWAL(t *testing.T) {
	paths := ownerTestPaths(t)
	root := filepath.Dir(paths.Dir)
	t.Setenv("PATH", root+string(os.PathListSeparator)+os.Getenv("PATH"))
	binary, err := os.Executable()
	require.NoError(t, err)
	require.NoError(t, os.Symlink(binary, filepath.Join(root, "pgbackrestd")))
	pgctldBinary := filepath.Join(root, "pgctld")
	require.NoError(t, os.WriteFile(pgctldBinary, []byte("#!/bin/sh\nexit 1\n"), 0o700))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	pb.RegisterPgCtldServer(server, &rollbackPgctld{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	t.Setenv("MULTIGRES_PGBACKREST_SANDBOX", "off")
	pooler := filepath.Join(root, "pooler")
	p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: root, Cells: map[string]CellServicesConfig{
		"zone": {Pgctld: PgctldConfig{Path: pgctldBinary, PoolerDir: pooler, PgPassword: "pw", GrpcPort: listener.Addr().(*net.TCPAddr).Port}},
	}}}
	cfg, err := p.getCellServiceConfig("zone", "pgctld")
	require.NoError(t, err)
	pid, adopted, err := p.startSidecarForPooler(t.Context(), cfg, pooler, "pw", "pooler", "db")
	require.NoError(t, err)
	require.False(t, adopted)
	t.Cleanup(func() { require.NoError(t, paths.stopOwned(context.Background())) })
	before, err := os.ReadFile(paths.ownerFile())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = p.provisionPgctld(ctx, "db", "default", "pooler", "zone")
	require.Error(t, err)
	require.Contains(t, err.Error(), "pgctld")
	require.Contains(t, err.Error(), "rollback: PostgreSQL shutdown unknown")
	owner, err := readSidecarIdentity(pid)
	require.NoError(t, err)
	alive, err := owner.alive()
	require.NoError(t, err)
	require.True(t, alive)
	after, err := os.ReadFile(paths.ownerFile())
	require.NoError(t, err)
	require.Equal(t, before, after)
	_, err = os.Stat(filepath.Join(p.getStateDir(), "dbs", "db", "pgctld_pgctld-pooler.json"))
	require.NoError(t, err, "retain recovery record after rollback refusal")
}

func TestRecoveredSignalPolicyFailsClosed(t *testing.T) {
	_, _, err := openRecoveredSidecar(-1)
	require.Error(t, err)
	if runtime.GOOS != "linux" {
		owner, err := readSidecarIdentity(os.Getpid())
		require.NoError(t, err)
		require.ErrorContains(t, owner.stop(t.Context()), "refusing numeric PID signals")
	} else {
		// No local-sidecar registry entry: exercise the recovered pidfd path.
		cmd := executil.Command(context.Background(), "/bin/sleep", "60")
		require.NoError(t, cmd.Start())
		go func() { _ = cmd.Wait() }()
		t.Cleanup(func() { _, _ = cmd.Stop(context.Background()) })
		// Wait for this fixture's exact exec identity, not the parent's
		// transient pre-exec argv. Runtime recovery remains strict.
		var owner *sidecarOwner
		require.Eventually(t, func() bool {
			var err error
			owner, err = readSidecarIdentity(cmd.Process.Pid)
			return err == nil && slices.Equal(owner.Argv, []string{"/bin/sleep", "60"})
		}, 5*time.Second, time.Millisecond)
		require.NoError(t, owner.stop(t.Context()))
	}
}

func TestRejectedSidecarConfigDoesNotMutateLiveFiles(t *testing.T) {
	paths := ownerTestPaths(t)
	binary, err := os.Executable()
	require.NoError(t, err)
	t.Setenv("MULTIGRES_PGBACKREST_SANDBOX", "off")
	t.Setenv("AWS_ACCESS_KEY_ID", "old-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "old-secret")
	t.Setenv("AWS_SESSION_TOKEN", "old-token")
	p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: filepath.Dir(paths.Dir), Backup: BackupConfig{Type: "s3", S3: &S3Backup{Bucket: "bucket", Region: "region", UseEnvCredentials: true}}}}
	pooler := filepath.Join(filepath.Dir(paths.Dir), "pooler")
	_, err = p.startPgBackRestd(t.Context(), binary, pooler, "postgres", "pw", "", 5432, 8432, 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, paths.stopOwned(context.Background())) })
	before, err := os.ReadFile(filepath.Join(paths.s3KeyDir(), "credentials.json"))
	require.NoError(t, err)
	t.Setenv("AWS_SESSION_TOKEN", "new-token")
	_, err = p.startPgBackRestd(t.Context(), binary, pooler, "postgres", "different-password", "", 5432, 9999, 0)
	require.ErrorContains(t, err, "configuration differs")
	after, err := os.ReadFile(filepath.Join(paths.s3KeyDir(), "credentials.json"))
	require.NoError(t, err)
	require.Equal(t, before, after)
	password, err := os.ReadFile(paths.PasswordFile)
	require.NoError(t, err)
	require.Equal(t, "pw", string(password))
}

func TestPrelaunchFailureNeverContactsConfiguredPgctld(t *testing.T) {
	for _, failure := range []string{"exec-format", "password-write"} {
		t.Run(failure, func(t *testing.T) {
			paths := ownerTestPaths(t)
			root := filepath.Dir(paths.Dir)
			t.Setenv("PATH", root+string(os.PathListSeparator)+os.Getenv("PATH"))
			binary, err := os.Executable()
			require.NoError(t, err)
			require.NoError(t, os.Symlink(binary, filepath.Join(root, "pgbackrestd")))
			pgctldBinary := filepath.Join(root, "pgctld")
			require.NoError(t, os.WriteFile(pgctldBinary, []byte("invalid executable format\n"), 0o700))
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			server := grpc.NewServer()
			mock := &rollbackPgctld{}
			pb.RegisterPgCtldServer(server, mock)
			go func() { _ = server.Serve(listener) }()
			t.Cleanup(server.Stop)
			t.Setenv("MULTIGRES_PGBACKREST_SANDBOX", "off")
			pooler := filepath.Join(root, "pooler")
			p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: root, Cells: map[string]CellServicesConfig{
				"zone": {Pgctld: PgctldConfig{Path: pgctldBinary, PoolerDir: pooler, PgPassword: "pw", GrpcPort: listener.Addr().(*net.TCPAddr).Port}},
			}}}
			cfg, err := p.getCellServiceConfig("zone", "pgctld")
			require.NoError(t, err)
			pid, _, err := p.startSidecarForPooler(t.Context(), cfg, pooler, "pw", "pooler", "db")
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, paths.stopOwned(context.Background())) })
			if failure == "password-write" {
				require.NoError(t, os.Mkdir(filepath.Join(pooler, "postgres-password"), 0o700))
			}
			_, err = p.provisionPgctld(t.Context(), "db", "default", "pooler", "zone")
			require.Error(t, err)
			require.Zero(t, mock.statusCalls.Load(), "this attempt did not start pgctld")
			require.Zero(t, mock.stopCalls.Load(), "must not mutate another listener")
			owner, err := paths.loadOwner()
			require.NoError(t, err)
			require.Equal(t, pid, owner.PID)
			alive, err := owner.alive()
			require.NoError(t, err)
			require.True(t, alive)
		})
	}
}

func TestCapabilityNegativeSidecarPreservesLegacyCredentialFiles(t *testing.T) {
	for _, failure := range []string{"old-protocol", "missing-instance", "stale-owner"} {
		t.Run(failure, func(t *testing.T) {
			paths := ownerTestPaths(t)
			binary, err := os.Executable()
			require.NoError(t, err)
			t.Setenv("MULTIGRES_PGBACKREST_SANDBOX", "off")
			t.Setenv("AWS_ACCESS_KEY_ID", "initial-key")
			t.Setenv("AWS_SECRET_ACCESS_KEY", "initial-secret")
			t.Setenv("AWS_SESSION_TOKEN", "initial-token")
			p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: filepath.Dir(paths.Dir), Backup: BackupConfig{Type: "s3", S3: &S3Backup{Bucket: "b", Region: "r", UseEnvCredentials: true}}}}
			pooler := filepath.Join(filepath.Dir(paths.Dir), "pooler")
			launch := func() (int, error) {
				return p.startPgBackRestd(t.Context(), binary, pooler, "postgres", "pw", "", 5432, 8432, 0)
			}
			pid, err := launch()
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, paths.stopOwned(context.Background())) })
			mode := filepath.Join(paths.Dir, "fixture-mode")
			require.NoError(t, os.WriteFile(mode, []byte(failure), 0o600))
			t.Cleanup(func() { require.NoError(t, os.Remove(mode)) })
			if failure == "stale-owner" {
				original, err := paths.loadOwner()
				require.NoError(t, err)
				stale := *original
				stale.InstanceID = "stale"
				require.NoError(t, paths.saveOwner(&stale))
				t.Cleanup(func() { require.NoError(t, paths.saveOwner(original)) })
			}
			// The fixture answers Status but advertises no bundle/instance capability.
			// Model the on-disk inputs its older credential reader still consumes.
			require.NoError(t, os.Remove(filepath.Join(paths.s3KeyDir(), "credentials.json")))
			for _, name := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"} {
				require.NoError(t, os.WriteFile(filepath.Join(paths.s3KeyDir(), name), []byte("old-"+name), 0o600))
			}
			t.Setenv("AWS_SESSION_TOKEN", "rotated")
			_, err = launch()
			if failure == "stale-owner" {
				require.ErrorContains(t, err, "instance identity mismatch")
			} else {
				require.ErrorContains(t, err, "credential-bundle capability is unverified")
			}
			for _, name := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"} {
				raw, err := os.ReadFile(filepath.Join(paths.s3KeyDir(), name))
				require.NoError(t, err)
				require.Equal(t, "old-"+name, string(raw))
			}
			_, err = os.Stat(filepath.Join(paths.s3KeyDir(), "credentials.json"))
			require.True(t, os.IsNotExist(err))
			owner, err := readSidecarIdentity(pid)
			require.NoError(t, err)
			alive, err := owner.alive()
			require.NoError(t, err)
			require.True(t, alive)
		})
	}
}
