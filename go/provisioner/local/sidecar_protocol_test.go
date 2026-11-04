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
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgbackreststate"
	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
	"github.com/multigres/multigres/go/pb/pgbackrestdservice/pgbackrestdserviceconnect"
	"github.com/multigres/multigres/go/provisioner"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/executil"
)

func recoveredFixture(t *testing.T) (PgBackRestdPaths, *sidecarOwner) {
	t.Helper()
	paths := ownerTestPaths(t)
	binary, err := os.Executable()
	require.NoError(t, err)
	lock, err := paths.lockOwner()
	require.NoError(t, err)
	_, err = paths.ensureSidecar(t.Context(), binary, fixtureArgs(paths))
	require.NoError(t, err)
	require.NoError(t, lock.Close())
	// Forget only this launcher's handle, forcing the real recovery branch.
	// Keep the original handle privately for unconditional negative-test cleanup.
	child, ok := localSidecarChildren.LoadAndDelete(paths.ownerFile())
	require.True(t, ok)
	t.Cleanup(func() { _, _ = child.(*executil.Cmd).Stop(context.Background()) })
	owner, err := paths.loadOwner()
	require.NoError(t, err)
	require.NotEmpty(t, owner.InstanceID)
	require.False(t, owner.InstancePending)
	return paths, owner
}

func fixtureClient(paths PgBackRestdPaths, pid int) (pgbackrestdserviceconnect.PgBackRestdClient, func()) {
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return dialOwnedSidecar(ctx, paths.ControlSocket, pid)
	}, DisableKeepAlives: true}
	return pgbackrestdserviceconnect.NewPgBackRestdClient(&http.Client{Transport: tr}, "http://pgbackrestd"), tr.CloseIdleConnections
}

func TestRecoveredSidecarSelfShutdown(t *testing.T) {
	paths, owner := recoveredFixture(t)
	lock := filepath.Join(paths.StateDir, pgbackreststate.InstanceLockName)
	before, err := os.Stat(lock)
	require.NoError(t, err)
	client, closeClient := fixtureClient(paths, owner.PID)
	defer closeClient()
	for _, id := range []string{"", "stale-instance"} {
		_, err := client.Shutdown(t.Context(), connect.NewRequest(&pb.ShutdownRequest{InstanceId: id}))
		require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
		alive, err := owner.alive()
		require.NoError(t, err)
		require.True(t, alive)
	}
	require.NoError(t, paths.stopOwned(t.Context()))
	alive, err := owner.alive()
	require.NoError(t, err)
	require.False(t, alive)
	retired, err := paths.loadOwner()
	require.NoError(t, err)
	require.Equal(t, owner.InstanceID, retired.InstanceID)
	require.NotEmpty(t, retired.RetiredStateDir)
	after, err := os.Stat(lock)
	require.NoError(t, err)
	require.True(t, os.SameFile(before, after))
	guard, err := pgbackreststate.AcquireCleanupGuard(paths.StateDir)
	require.NoError(t, err)
	require.NoError(t, guard.Close())
}

func TestRecoveredSidecarGuardedCleanupEntrypoints(t *testing.T) {
	for _, entrypoint := range []string{"service", "database", "teardown"} {
		t.Run(entrypoint, func(t *testing.T) {
			paths, owner := recoveredFixture(t)
			p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: filepath.Dir(paths.Dir)}}
			pgctld := seedCleanupPgctld(t, p, paths)
			pooler := filepath.Join(filepath.Dir(paths.Dir), "pooler")
			require.NoError(t, os.MkdirAll(pooler, 0o700))
			payload := filepath.Join(pooler, "payload")
			require.NoError(t, os.WriteFile(payload, []byte("database payload"), 0o600))
			lock := filepath.Join(paths.StateDir, pgbackreststate.InstanceLockName)
			before, err := os.Stat(lock)
			require.NoError(t, err)
			ctx, release, err := p.enterSidecarLifecycle(t.Context())
			require.NoError(t, err)
			if entrypoint == "teardown" {
				// Teardown is a top-level operation, not a nested lease consumer.
				require.NoError(t, release())
			} else {
				defer func() { require.NoError(t, release()) }()
			}
			switch entrypoint {
			case "service":
				err = p.Deprovision(ctx, &provisioner.DeprovisionRequest{Service: "pgctld", ServiceID: "cleanup-pooler", DatabaseName: "db", Clean: true})
			case "database":
				err = p.DeprovisionDatabase(ctx, "db", "")
			case "teardown":
				err = p.Teardown(ctx, true)
			}
			require.NoError(t, err)
			require.Equal(t, int32(1), pgctld.stopCalls.Load())
			alive, err := owner.alive()
			require.NoError(t, err)
			require.False(t, alive)
			after, err := os.Stat(lock)
			require.NoError(t, err)
			require.True(t, os.SameFile(before, after))
			competitor, err := p.lockSidecarLifecycle()
			if competitor != nil {
				_ = competitor.Close()
			}
			if entrypoint == "teardown" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			_, err = os.Stat(filepath.Join(p.getStateDir(), "dbs", "db", "pgctld_cleanup-pooler.json"))
			require.ErrorIs(t, err, os.ErrNotExist)
			_, err = os.Stat(payload)
			if entrypoint == "teardown" {
				require.NoError(t, err, "teardown must not broaden deletion outside its standard roots")
			} else {
				require.ErrorIs(t, err, os.ErrNotExist)
			}
		})
	}
}

func TestRecoveredSidecarShutdownRefusesUncertainty(t *testing.T) {
	for _, mode := range []string{"old-protocol", "missing-instance", "stale-owner", "missing-owner-instance", "disconnect", "reject", "request-timeout", "accepted-no-exit"} {
		t.Run(mode, func(t *testing.T) {
			paths, owner := recoveredFixture(t)
			switch mode {
			case "stale-owner":
				owner.InstanceID = "not-this-instance"
				require.NoError(t, paths.saveOwner(owner))
			case "missing-owner-instance":
				owner.InstanceID = ""
				require.NoError(t, paths.saveOwner(owner))
			default:
				require.NoError(t, os.WriteFile(filepath.Join(paths.Dir, "fixture-mode"), []byte(mode), 0o600))
			}
			before, err := os.ReadFile(paths.ownerFile())
			require.NoError(t, err)
			payload := filepath.Join(paths.Dir, "payload")
			require.NoError(t, os.WriteFile(payload, []byte("keep"), 0o600))
			ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
			defer cancel()
			require.Error(t, paths.stopOwned(ctx))
			alive, err := owner.alive()
			require.NoError(t, err)
			require.True(t, alive, "no recovered signal fallback after a failed RPC or identity check")
			after, err := os.ReadFile(paths.ownerFile())
			require.NoError(t, err)
			require.Equal(t, before, after)
			raw, err := os.ReadFile(payload)
			require.NoError(t, err)
			require.Equal(t, "keep", string(raw))
		})
	}
}

func TestRecoveredSidecarShutdownKeepsUnsettledOwnership(t *testing.T) {
	paths, owner := recoveredFixture(t)
	// Even malformed pending evidence is a veto by the public shared contract.
	marker := filepath.Join(paths.StateDir, "worker-test.pending")
	require.NoError(t, os.WriteFile(marker, []byte("unsettled"), 0o600))
	require.ErrorIs(t, paths.stopOwned(t.Context()), pgbackreststate.ErrWorkersUnsettled)
	alive, err := owner.alive()
	require.NoError(t, err)
	require.False(t, alive, "accepted self-shutdown is insufficient to authorize cleanup")
	_, err = os.Stat(paths.ownerFile())
	require.NoError(t, err)
	raw, err := os.ReadFile(marker)
	require.NoError(t, err)
	require.Equal(t, "unsettled", string(raw))
}

func TestSidecarPendingIdentityCanCompleteAfterLauncherRestart(t *testing.T) {
	paths, owner := recoveredFixture(t)
	id := owner.InstanceID
	owner.InstanceID = ""
	owner.InstancePending = true
	require.NoError(t, paths.saveOwner(owner))
	lock, err := paths.lockOwner()
	require.NoError(t, err)
	pid, err := paths.ensureSidecar(t.Context(), owner.Argv[0], owner.Argv[1:])
	require.NoError(t, err)
	require.NoError(t, lock.Close())
	require.Equal(t, owner.PID, pid)
	completed, err := paths.loadOwner()
	require.NoError(t, err)
	require.Equal(t, id, completed.InstanceID)
	require.False(t, completed.InstancePending)
	require.NoError(t, paths.stopOwned(t.Context()))
}

// Opt-in integration smoke: a real built daemon and pgBackRest TLS worker,
// but no PostgreSQL cluster, repository calls, or real cloud credentials.
func TestRealSidecarProtocolIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("real daemon integration smoke")
	}
	binary := os.Getenv("MULTIGRES_TEST_SIDECAR_BINARY")
	if binary == "" {
		t.Skip("set MULTIGRES_TEST_SIDECAR_BINARY after make build")
	}
	paths := ownerTestPaths(t)
	t.Setenv("MULTIGRES_PGBACKREST_SANDBOX", "off")
	t.Setenv("AWS_ACCESS_KEY_ID", "test-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
	t.Setenv("AWS_SESSION_TOKEN", "test-token")
	p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: filepath.Dir(paths.Dir), Backup: BackupConfig{Type: "s3", S3: &S3Backup{Bucket: "test", Region: "us-east-1", Endpoint: "127.0.0.1:1", UseEnvCredentials: true}}}}
	_, err := GeneratePgBackRestCerts(p.certDir())
	require.NoError(t, err)
	port := utils.GetFreePort(t)
	launch := func() (int, error) {
		return p.startPgBackRestd(t.Context(), binary, filepath.Join(filepath.Dir(paths.Dir), "pooler"), "postgres", "pw", filepath.Join(paths.Dir, "daemon.log"), 5432, port, 0)
	}
	pid, err := launch()
	require.NoError(t, err)
	child, ok := localSidecarChildren.LoadAndDelete(paths.ownerFile())
	require.True(t, ok)
	t.Cleanup(func() { _, _ = child.(*executil.Cmd).Stop(context.Background()) })
	owner, err := paths.loadOwner()
	require.NoError(t, err)
	client, closeClient := fixtureClient(paths, pid)
	defer closeClient()
	require.Eventually(t, func() bool {
		status, err := client.Status(t.Context(), connect.NewRequest(&pb.StatusRequest{}))
		return err == nil && status.Msg.GetServerUp()
	}, 15*time.Second, 100*time.Millisecond, "real pgBackRest TLS worker must run")
	_, err = client.Shutdown(t.Context(), connect.NewRequest(&pb.ShutdownRequest{InstanceId: "stale"}))
	require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
	for revision, token := range []string{"rotated-token", ""} {
		t.Setenv("AWS_SESSION_TOKEN", token)
		adopted, err := launch()
		require.NoError(t, err)
		require.Equal(t, pid, adopted)
		configured, err := client.Configure(t.Context(), connect.NewRequest(&pb.ConfigureRequest{Revision: int64(revision + 1), Repos: []*pb.RepoGeneration{{Generation: 1, RepoNumber: 1}}}))
		require.NoError(t, err)
		require.True(t, configured.Msg.GetAccepted())
		status, err := client.Status(t.Context(), connect.NewRequest(&pb.StatusRequest{}))
		require.NoError(t, err)
		require.Equal(t, owner.InstanceID, status.Msg.GetInstanceId())
		require.Equal(t, uint32(1), status.Msg.GetLifecycleProtocolVersion())
		require.True(t, status.Msg.GetConfigured())
		require.Equal(t, int64(revision+1), status.Msg.GetRevision())
		require.Len(t, status.Msg.GetRepos(), 1)
	}
	require.NoError(t, paths.stopOwned(t.Context()), "real recovered shutdown must drain owned pgBackRest worker")
	guard, err := pgbackreststate.AcquireCleanupGuard(paths.StateDir)
	require.NoError(t, err)
	require.NoError(t, guard.Close())
}

func TestLiveS3AdoptionRotatesAtomicBundle(t *testing.T) {
	paths := ownerTestPaths(t)
	binary, err := os.Executable()
	require.NoError(t, err)
	t.Setenv("MULTIGRES_PGBACKREST_SANDBOX", "off")
	t.Setenv("AWS_ACCESS_KEY_ID", "initial-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "initial-secret")
	t.Setenv("AWS_SESSION_TOKEN", "initial-token")
	p := &localProvisioner{config: &LocalProvisionerConfig{RootWorkingDir: filepath.Dir(paths.Dir), Backup: BackupConfig{Type: "s3", S3: &S3Backup{Bucket: "b", Region: "r", UseEnvCredentials: true}}}}
	launch := func() (int, error) {
		return p.startPgBackRestd(t.Context(), binary, filepath.Join(filepath.Dir(paths.Dir), "pooler"), "postgres", "pw", "", 5432, 8432, 0)
	}
	pid, err := launch()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, paths.stopOwned(context.Background())) })
	owner, err := paths.loadOwner()
	require.NoError(t, err)
	require.NotEmpty(t, owner.InstanceID)
	// A supported reader may migrate an existing legacy credential directory.
	require.NoError(t, os.Remove(filepath.Join(paths.s3KeyDir(), "credentials.json")))
	for _, name := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"} {
		require.NoError(t, os.WriteFile(filepath.Join(paths.s3KeyDir(), name), []byte("legacy"), 0o600))
	}
	for _, token := range []string{"rotated-token", ""} {
		t.Setenv("AWS_ACCESS_KEY_ID", "rotated-key")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "rotated-secret")
		t.Setenv("AWS_SESSION_TOKEN", token)
		adopted, err := launch()
		require.NoError(t, err)
		require.Equal(t, pid, adopted)
		raw, err := os.ReadFile(filepath.Join(paths.s3KeyDir(), "credentials.json"))
		require.NoError(t, err)
		var bundle map[string]string
		require.NoError(t, json.Unmarshal(raw, &bundle))
		require.Equal(t, map[string]string{"access_key_id": "rotated-key", "secret_access_key": "rotated-secret", "session_token": token}, bundle)
		current, err := paths.loadOwner()
		require.NoError(t, err)
		require.Equal(t, owner.InstanceID, current.InstanceID)
	}
	for _, name := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"} {
		_, err := os.Stat(filepath.Join(paths.s3KeyDir(), name))
		require.ErrorIs(t, err, os.ErrNotExist)
	}
}
