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

package manager

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/sandbox"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
	"github.com/multigres/multigres/go/pb/pgbackrestdservice/pgbackrestdserviceconnect"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
	backupengine "github.com/multigres/multigres/go/services/multipooler/internal/manager/backup"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/telemetry"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/protobuf/proto"
)

// fakeSidecarServer is an in-process pgbackrestd control API.
type fakeSidecarServer struct {
	pgbackrestdserviceconnect.UnimplementedPgBackRestdHandler

	mu         sync.Mutex
	status     *pb.StatusResponse
	statusErr  error
	configures []*pb.ConfigureRequest
	// configureReply decides each Configure answer; nil accepts everything.
	configureReply func(req *pb.ConfigureRequest) *pb.ConfigureResponse
	onConfigure    func()
	// statusHook and configureHook run inside the handler, before it answers.
	statusHook    func()
	configureHook func()
	run           func(ctx context.Context, req *pb.RunRequest, stream *connect.ServerStream[pb.RunOutput]) error
}

func (f *fakeSidecarServer) Configure(_ context.Context, req *connect.Request[pb.ConfigureRequest]) (*connect.Response[pb.ConfigureResponse], error) {
	if f.configureHook != nil {
		f.configureHook()
	}
	f.mu.Lock()
	f.configures = append(f.configures, req.Msg)
	reply, hook := f.configureReply, f.onConfigure
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	resp := &pb.ConfigureResponse{Accepted: true, HeldRevision: req.Msg.GetRevision()}
	if reply != nil {
		resp = reply(req.Msg)
	}
	if resp.GetAccepted() {
		f.mu.Lock()
		f.status.Configured = true
		f.status.Revision = req.Msg.GetRevision()
		f.status.Repos = req.Msg.GetRepos()
		f.mu.Unlock()
	}
	return connect.NewResponse(resp), nil
}

func (f *fakeSidecarServer) Status(context.Context, *connect.Request[pb.StatusRequest]) (*connect.Response[pb.StatusResponse], error) {
	if f.statusHook != nil {
		f.statusHook()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.statusErr != nil {
		return nil, f.statusErr
	}
	return connect.NewResponse(proto.Clone(f.status).(*pb.StatusResponse)), nil
}

func (f *fakeSidecarServer) Run(ctx context.Context, req *connect.Request[pb.RunRequest], stream *connect.ServerStream[pb.RunOutput]) error {
	if f.run == nil {
		return f.UnimplementedPgBackRestdHandler.Run(ctx, req, stream)
	}
	return f.run(ctx, req.Msg, stream)
}

func (f *fakeSidecarServer) configureCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.configures)
}

func (f *fakeSidecarServer) lastConfigure(t *testing.T) *pb.ConfigureRequest {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	require.NotEmpty(t, f.configures, "Configure was never sent")
	return f.configures[len(f.configures)-1]
}

// serveFakeSidecar listens on a short temp socket (unix socket paths are
// capped near 100 bytes, which t.TempDir often exceeds).
func serveFakeSidecar(t *testing.T, f *fakeSidecarServer) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "sc")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "c.sock")
	l, err := net.Listen("unix", sock)
	require.NoError(t, err)
	mux := http.NewServeMux()
	path, h := pgbackrestdserviceconnect.NewPgBackRestdHandler(f)
	mux.Handle(path, h)
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: time.Second}
	go func() { _ = srv.Serve(l) }()
	t.Cleanup(func() { _ = srv.Close() })
	return sock
}

func TestSidecarClient_RunStreamsAndReturnsExit(t *testing.T) {
	f := &fakeSidecarServer{status: &pb.StatusResponse{}}
	f.run = func(_ context.Context, req *pb.RunRequest, stream *connect.ServerStream[pb.RunOutput]) error {
		assert.Equal(t, pb.Command_COMMAND_CHECK, req.GetCommand())
		assert.Equal(t, []string{"--repo=1"}, req.GetArgs())
		for _, m := range []*pb.RunOutput{
			{Chunk: &pb.RunOutput_Stdout{Stdout: []byte("out-1 ")}},
			{Chunk: &pb.RunOutput_Stderr{Stderr: []byte("err-1 ")}},
			{Chunk: &pb.RunOutput_Stdout{Stdout: []byte("out-2")}},
			{Chunk: &pb.RunOutput_ExitCode{ExitCode: 103}},
		} {
			if err := stream.Send(m); err != nil {
				return err
			}
		}
		return nil
	}
	c := newSidecarClient(serveFakeSidecar(t, f))
	t.Cleanup(c.Close)

	var stdout, stderr bytes.Buffer
	exit, err := c.Run(t.Context(), pb.Command_COMMAND_CHECK, []string{"--repo=1"}, &stdout, &stderr)
	require.NoError(t, err)
	assert.Equal(t, 103, exit)
	assert.Equal(t, "out-1 out-2", stdout.String())
	assert.Equal(t, "err-1 ", stderr.String())

	t.Run("a stream with no exit code is an error", func(t *testing.T) {
		f.run = func(_ context.Context, _ *pb.RunRequest, stream *connect.ServerStream[pb.RunOutput]) error {
			return stream.Send(&pb.RunOutput{Chunk: &pb.RunOutput_Stdout{Stdout: []byte("partial")}})
		}
		_, err := c.Run(t.Context(), pb.Command_COMMAND_CHECK, nil, &stdout, &stderr)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "without an exit code")
	})

	t.Run("a rejected command is an error, not an exit code", func(t *testing.T) {
		f.run = func(context.Context, *pb.RunRequest, *connect.ServerStream[pb.RunOutput]) error {
			return connect.NewError(connect.CodeInvalidArgument, errors.New("option not allowed"))
		}
		_, err := c.Run(t.Context(), pb.Command_COMMAND_CHECK, nil, &stdout, &stderr)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "option not allowed")
	})

	t.Run("a canceled context ends a stalled call", func(t *testing.T) {
		started := make(chan struct{})
		f.run = func(ctx context.Context, _ *pb.RunRequest, stream *connect.ServerStream[pb.RunOutput]) error {
			_ = stream.Send(&pb.RunOutput{Chunk: &pb.RunOutput_Stdout{Stdout: []byte("x")}})
			close(started)
			<-ctx.Done()
			return ctx.Err()
		}
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() {
			_, err := c.Run(ctx, pb.Command_COMMAND_CHECK, nil, &stdout, &stderr)
			done <- err
		}()
		<-started
		cancel()
		select {
		case err := <-done:
			require.Error(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("Run did not return after its context was canceled")
		}
	})
}

func TestSidecarClient_ConfigureAndStatus(t *testing.T) {
	f := &fakeSidecarServer{status: &pb.StatusResponse{ServerPort: 8432, GenerationsWithKeys: []uint32{1}}}
	f.configureReply = func(req *pb.ConfigureRequest) *pb.ConfigureResponse {
		if req.GetRevision() < 5 {
			return &pb.ConfigureResponse{Accepted: false, HeldRevision: 5}
		}
		return &pb.ConfigureResponse{Accepted: true, HeldRevision: req.GetRevision()}
	}
	c := newSidecarClient(serveFakeSidecar(t, f))
	t.Cleanup(c.Close)

	resp, err := c.Configure(t.Context(), 2, []RepoGeneration{{Generation: 1, RepoNumber: 1}}, 0)
	require.NoError(t, err)
	assert.False(t, resp.GetAccepted())
	assert.Equal(t, int64(5), resp.GetHeldRevision())

	resp, err = c.Configure(t.Context(), 6, []RepoGeneration{{Generation: 1, RepoNumber: 1}, {Generation: 2, RepoNumber: 2}}, 16<<20)
	require.NoError(t, err)
	assert.True(t, resp.GetAccepted())
	got := f.lastConfigure(t)
	assert.Equal(t, uint32(16<<20), got.GetWalSegmentSizeBytes())
	assert.Len(t, got.GetRepos(), 2)

	st, err := c.Status(t.Context())
	require.NoError(t, err)
	assert.True(t, st.GetConfigured())
	assert.Equal(t, int64(6), st.GetRevision())

	port, err := SidecarServerPort(t.Context(), serveFakeSidecar(t, f))
	require.NoError(t, err)
	assert.Equal(t, 8432, port)
}

func TestBackupLocationFingerprint(t *testing.T) {
	fs := utils.FilesystemBackupLocation("/data/backups")
	s3 := &clustermetadatapb.BackupLocation{Location: &clustermetadatapb.BackupLocation_S3{S3: &clustermetadatapb.S3Backup{
		Bucket: "b", Region: "r", Endpoint: "https://e", KeyPrefix: "p/",
	}}}
	assert.NotEqual(t, backupLocationFingerprint(fs), backupLocationFingerprint(s3))
	assert.Equal(t, backupLocationFingerprint(nil), backupLocationFingerprint(utils.FilesystemBackupLocation("/backups")),
		"a nil location is filesystem /backups")
	assert.Len(t, backupLocationFingerprint(fs), 64)
	// Pinned: the sidecar computes the same hash over the same JSON.
	assert.Equal(t, "ac9d5d4edc31242dd8ebdb27151e528dc39568c30066b8104248bea9cf6d1a19", backupLocationFingerprint(fs))
	// Every S3 field set: dropping any one of them from the hash changes this.
	// pgbackrestd pins the same value for the same inputs.
	assert.Equal(t, "3fd332ada31a0d7a11c515a65ea8edd6e030aad106be71742c70cc82dd777c58", backupLocationFingerprint(s3))
	other := &clustermetadatapb.BackupLocation{Location: &clustermetadatapb.BackupLocation_S3{S3: &clustermetadatapb.S3Backup{
		Bucket: "b", Region: "r", Endpoint: "https://e", KeyPrefix: "q/",
	}}}
	assert.NotEqual(t, backupLocationFingerprint(s3), backupLocationFingerprint(other), "the key prefix is part of the location")
}

// sidecarStartup is a manager wired to a fake sidecar,
// a memory topology holding the database record, and a mocked query service.
type sidecarStartup struct {
	pm        *MultipoolerManager
	fake      *fakeSidecarServer
	qs        *mock.QueryService
	poolerDir string
}

const startupDatabase = "startupdb"

func newSidecarStartup(t *testing.T, loc *clustermetadatapb.BackupLocation, fingerprintLoc *clustermetadatapb.BackupLocation) *sidecarStartup {
	t.Helper()
	pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, constants.DefaultShard)

	poolerDir := t.TempDir()
	id := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "test-cell", Name: "test-pooler"}
	pm.record = newRecordFromProto(&clustermetadatapb.Multipooler{
		Id:        id,
		ShardRef:  &clustermetadatapb.ShardRef{Database: startupDatabase, TableGroup: constants.DefaultTableGroup, Shard: constants.DefaultShard},
		PoolerDir: poolerDir,
	})
	require.NoError(t, pm.topoClient.CreateDatabase(t.Context(), startupDatabase, &clustermetadatapb.Database{
		Name: startupDatabase, BackupLocation: loc,
	}))

	fake := &fakeSidecarServer{status: &pb.StatusResponse{
		LocationFingerprint: backupLocationFingerprint(fingerprintLoc),
		GenerationsWithKeys: nil,
		ServerPort:          8432,
	}}
	sock := serveFakeSidecar(t, fake)

	pm.config = &Config{
		PgBackRestControlSocket: sock,
		PgBackRestWALSocket:     "/run/pgbackrest-wal/wal.sock",
		DataVolumeRoot:          poolerDir,
		DataVolumeAnchor:        sandbox.AnchorDir,
	}
	pm.sidecar = newSidecarClient(sock)
	t.Cleanup(pm.sidecar.Close)
	pm.volumeRoot = pm.openSidecarVolumeRoot
	settings := pm.backupEngineSettings()
	settings.Sidecar = pm.sidecar
	pm.backup = newStubEngine(pm.logger, pm.record, settings)

	pm.serviceID = id
	pm.state = ManagerStateStarting
	pm.readyChan = make(chan struct{})
	pm.loadTimeout = 5 * time.Second
	pm.ctx = t.Context()
	var cancel context.CancelFunc
	pm.shutdownCtx, cancel = context.WithCancel(t.Context())
	t.Cleanup(cancel)
	return &sidecarStartup{pm: pm, fake: fake, qs: qs, poolerDir: poolerDir}
}

func TestStartup_ConfiguresFromCopyBeforePostgres(t *testing.T) {
	loc := utils.FilesystemBackupLocation("/backups")

	t.Run("from the PVC copy", func(t *testing.T) {
		s := newSidecarStartup(t, loc, loc)
		require.NoError(t, s.pm.writeGenerationsCopy(7, []RepoGeneration{{Generation: 1, RepoNumber: 1}, {Generation: 2, RepoNumber: 2}}))

		// The manager is not ready until topology and sidecar setup finish, and
		// postgres is only started once it is, so a Configure that lands while
		// the state is still Starting precedes any postgres start.
		stateAtConfigure := make(chan ManagerState, 4)
		s.fake.onConfigure = func() { stateAtConfigure <- s.pm.GetState() }

		s.pm.loadShardConfigFromGlobalTopo()

		require.Equal(t, ManagerStateReady, s.pm.GetState())
		require.Equal(t, ManagerStateStarting, <-stateAtConfigure)
		got := s.fake.lastConfigure(t)
		assert.Equal(t, int64(7), got.GetRevision())
		require.Len(t, got.GetRepos(), 2)
		assert.Equal(t, uint32(2), got.GetRepos()[1].GetGeneration())
		assert.Equal(t, uint32(2), got.GetRepos()[1].GetRepoNumber())
		assert.Nil(t, s.pm.pgctldClient, "nothing here starts postgres")
	})

	t.Run("no copy means revision 0, generation 1 at repo 1", func(t *testing.T) {
		s := newSidecarStartup(t, loc, loc)
		s.pm.loadShardConfigFromGlobalTopo()

		require.Equal(t, ManagerStateReady, s.pm.GetState())
		got := s.fake.lastConfigure(t)
		assert.Equal(t, int64(0), got.GetRevision())
		require.Len(t, got.GetRepos(), 1)
		assert.Equal(t, uint32(1), got.GetRepos()[0].GetGeneration())
		assert.Equal(t, uint32(1), got.GetRepos()[0].GetRepoNumber())
	})

	t.Run("a sidecar that holds a newer list keeps it", func(t *testing.T) {
		s := newSidecarStartup(t, loc, loc)
		require.NoError(t, s.pm.writeGenerationsCopy(2, []RepoGeneration{{Generation: 1, RepoNumber: 1}}))
		s.fake.configureReply = func(*pb.ConfigureRequest) *pb.ConfigureResponse {
			return &pb.ConfigureResponse{Accepted: false, HeldRevision: 9}
		}
		s.pm.loadShardConfigFromGlobalTopo()
		assert.Equal(t, ManagerStateReady, s.pm.GetState(), "a held higher revision is not a startup error")
	})

	t.Run("an unreachable sidecar is a startup error", func(t *testing.T) {
		s := newSidecarStartup(t, loc, loc)
		s.pm.sidecar = newSidecarClient(filepath.Join(t.TempDir(), "nobody-listens"))
		old := sidecarConfigureTimeout
		oldRetry := sidecarConfigureRetry
		sidecarConfigureTimeout, sidecarConfigureRetry = 50*time.Millisecond, 10*time.Millisecond
		t.Cleanup(func() { sidecarConfigureTimeout, sidecarConfigureRetry = old, oldRetry })

		s.pm.loadShardConfigFromGlobalTopo()
		// With no sidecar to read Status from, the location cannot be checked,
		// and starting without that check would defeat it.
		state, err := s.pm.GetStateAndError()
		assert.Equal(t, ManagerStateError, state)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "sidecar unreachable")
	})
}

func TestStartup_DeletesLegacyDir(t *testing.T) {
	loc := utils.FilesystemBackupLocation("/backups")

	t.Run("removes the directory with the conf and pgpass", func(t *testing.T) {
		s := newSidecarStartup(t, loc, loc)
		legacy := filepath.Join(s.poolerDir, "pgbackrest")
		require.NoError(t, os.MkdirAll(filepath.Join(legacy, "logs"), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(legacy, "pgbackrest.conf"), []byte("repo1-cipher-pass=hunter2\n"), 0o600))
		require.NoError(t, os.WriteFile(filepath.Join(legacy, "pgbackrest.pgpass"), []byte("*:*:*:u:pw\n"), 0o600))
		require.NoError(t, os.WriteFile(filepath.Join(legacy, "logs", "x.log"), nil, 0o600))
		keep := filepath.Join(s.poolerDir, "consensus.json")
		require.NoError(t, os.WriteFile(keep, []byte("{}"), 0o600))

		s.pm.loadShardConfigFromGlobalTopo()

		require.Equal(t, ManagerStateReady, s.pm.GetState())
		assert.NoFileExists(t, filepath.Join(legacy, "pgbackrest.conf"))
		assert.NoDirExists(t, legacy)
		assert.FileExists(t, keep, "only the pgbackrest directory goes")
		assert.Equal(t, filepath.Join(s.poolerDir, "replication.pgpass"), s.pm.pgpassFilePath(), "only the replication passfile, outside the deleted dir")
		assert.NoDirExists(t, legacy, "and renders no conf")
	})

	t.Run("a symlink in its place is removed, not followed", func(t *testing.T) {
		s := newSidecarStartup(t, loc, loc)
		outside := t.TempDir()
		victim := filepath.Join(outside, "victim")
		require.NoError(t, os.WriteFile(victim, []byte("keep me"), 0o600))
		require.NoError(t, os.Symlink(outside, filepath.Join(s.poolerDir, "pgbackrest")))

		s.pm.loadShardConfigFromGlobalTopo()

		require.Equal(t, ManagerStateReady, s.pm.GetState())
		assert.FileExists(t, victim, "a link planted on the data volume must not redirect the delete")
		_, err := os.Lstat(filepath.Join(s.poolerDir, "pgbackrest"))
		assert.True(t, os.IsNotExist(err))
	})

	t.Run("an absent directory is fine", func(t *testing.T) {
		s := newSidecarStartup(t, loc, loc)
		s.pm.loadShardConfigFromGlobalTopo()
		assert.Equal(t, ManagerStateReady, s.pm.GetState())
	})
}

func TestFingerprintMismatchIsError(t *testing.T) {
	topoLoc := utils.FilesystemBackupLocation("/backups/a")
	sidecarLoc := utils.FilesystemBackupLocation("/backups/b")

	s := newSidecarStartup(t, topoLoc, sidecarLoc)
	legacy := filepath.Join(s.poolerDir, "pgbackrest")
	require.NoError(t, os.MkdirAll(legacy, 0o755))

	s.pm.loadShardConfigFromGlobalTopo()

	state, err := s.pm.GetStateAndError()
	assert.Equal(t, ManagerStateError, state)
	require.Error(t, err)
	assert.Equal(t, "pgbackrest sidecar location does not match topology", err.Error())
	assert.Zero(t, s.fake.configureCount(), "a sidecar for another location is never configured")
	assert.DirExists(t, legacy, "nothing is deleted on the way to the refusal")
}

func TestCredentialExposedMetric(t *testing.T) {
	setup := telemetry.SetupTestTelemetry(t)
	require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-multipooler"))

	read := func(t *testing.T) int64 {
		t.Helper()
		m := findMetric(t, setup.MetricReader, "pgbackrest.credential_exposed")
		g, ok := m.Data.(metricdata.Gauge[int64])
		require.True(t, ok)
		require.Len(t, g.DataPoints, 1)
		return g.DataPoints[0].Value
	}
	for _, name := range credentialEnvVars {
		t.Setenv(name, "")
		require.NoError(t, os.Unsetenv(name))
	}

	clean := &MultipoolerManager{logger: slog.New(slog.DiscardHandler)}
	assert.False(t, clean.checkCredentialExposure())
	clean.registerCredentialExposedGauge()
	assert.Equal(t, int64(0), read(t))

	for _, name := range credentialEnvVars {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, "x")
			var logs bytes.Buffer
			pm := &MultipoolerManager{logger: slog.New(slog.NewTextHandler(&logs, nil))}
			assert.True(t, pm.checkCredentialExposure(), "the pooler still starts; this only reports")
			assert.Contains(t, logs.String(), "level=ERROR")
			assert.Contains(t, logs.String(), name)
			assert.Equal(t, int64(1), pm.credentialExposed.Load())
		})
	}
}

// syncFixture is a manager with a mocked database for the sync
// loop's decisions.
func newSyncFixture(t *testing.T, sidecarRev int64, configured bool, copyRev int64) (*sidecarStartup, *[]string) {
	t.Helper()
	loc := utils.FilesystemBackupLocation("/backups")
	s := newSidecarStartup(t, loc, loc)
	s.fake.status.Revision = sidecarRev
	s.fake.status.Configured = configured
	if copyRev > 0 {
		require.NoError(t, s.pm.writeGenerationsCopy(copyRev, []RepoGeneration{{Generation: 1, RepoNumber: 1}}))
	}
	var updates []string
	s.qs.AddQueryPatternWithCallback("UPDATE multigres.pgbackrest_repos_meta", mock.MakeQueryResult([]string{"revision"}, [][]any{{100}}), func(q string) { updates = append(updates, q) })
	return s, &updates
}

func primaryMock(s *sidecarStartup, inRecovery string) {
	s.qs.AddQueryPatternWithCallback("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{inRecovery}}), nil)
}

func tableMock(s *sidecarStartup, rev int) {
	s.qs.AddQueryPatternWithCallback("pgbackrest_repos_meta m", mock.MakeQueryResult([]string{"revision", "generation", "repo_number"}, [][]any{{rev, 1, 1}}), nil)
}

func TestSyncSidecar_Transitions(t *testing.T) {
	t.Run("table ahead of the sidecar is sent", func(t *testing.T) {
		s, updates := newSyncFixture(t, 3, true, 0)
		primaryMock(s, "f")
		tableMock(s, 5)
		require.NoError(t, s.pm.syncSidecar(t.Context()))
		assert.Equal(t, int64(5), s.fake.lastConfigure(t).GetRevision())
		assert.Empty(t, *updates)
		rev, _, ok := s.pm.readGenerationsCopy()
		require.True(t, ok)
		assert.Equal(t, int64(5), rev, "the copy follows the table")
	})

	t.Run("equal revision and list sends nothing", func(t *testing.T) {
		s, _ := newSyncFixture(t, 5, true, 0)
		s.fake.status.Repos = []*pb.RepoGeneration{{Generation: 1, RepoNumber: 1}}
		primaryMock(s, "f")
		tableMock(s, 5)
		require.NoError(t, s.pm.syncSidecar(t.Context()))
		assert.Zero(t, s.fake.configureCount())
	})

	t.Run("an unconfigured sidecar is configured even with no database", func(t *testing.T) {
		s, _ := newSyncFixture(t, 0, false, 4)
		// No pg_is_in_recovery expectation: postgres is not up.
		require.NoError(t, s.pm.syncSidecar(t.Context()))
		assert.Equal(t, int64(4), s.fake.lastConfigure(t).GetRevision(), "the copy repairs a sidecar that came up with state_error")
	})

	t.Run("table behind the sidecar on a primary is raised, then sent", func(t *testing.T) {
		s, updates := newSyncFixture(t, 50, true, 0)
		primaryMock(s, "f")
		tableMock(s, 5)
		require.NoError(t, s.pm.syncSidecar(t.Context()))
		require.Len(t, *updates, 1)
		assert.Equal(t, int64(100), s.fake.lastConfigure(t).GetRevision(), "re-sent with the revision the floor returned")
	})

	t.Run("table behind the PVC copy on a primary is raised, then sent", func(t *testing.T) {
		s, updates := newSyncFixture(t, 10, true, 40)
		primaryMock(s, "f")
		tableMock(s, 20)
		require.NoError(t, s.pm.syncSidecar(t.Context()))
		require.Len(t, *updates, 1)
		assert.Equal(t, int64(100), s.fake.lastConfigure(t).GetRevision())
	})

	t.Run("table behind the sidecar on a standby waits for the primary", func(t *testing.T) {
		s, updates := newSyncFixture(t, 50, true, 0)
		primaryMock(s, "t")
		tableMock(s, 5)
		require.NoError(t, s.pm.syncSidecar(t.Context()))
		assert.Empty(t, *updates, "only a primary may raise the revision")
		assert.Zero(t, s.fake.configureCount(), "and the sidecar is never moved back")
	})

	t.Run("a Configure refused after the read raises the floor and re-sends", func(t *testing.T) {
		s, updates := newSyncFixture(t, 3, true, 0)
		primaryMock(s, "f")
		tableMock(s, 5)
		refused := false
		s.fake.configureReply = func(req *pb.ConfigureRequest) *pb.ConfigureResponse {
			if !refused {
				refused = true
				return &pb.ConfigureResponse{Accepted: false, HeldRevision: 60}
			}
			return &pb.ConfigureResponse{Accepted: true, HeldRevision: req.GetRevision()}
		}
		require.NoError(t, s.pm.syncSidecar(t.Context()))
		require.Len(t, *updates, 1)
		assert.Equal(t, int64(100), s.fake.lastConfigure(t).GetRevision())
		assert.Equal(t, 2, s.fake.configureCount())
	})

	t.Run("an empty table sends nothing", func(t *testing.T) {
		s, _ := newSyncFixture(t, 0, true, 0)
		primaryMock(s, "f")
		s.qs.AddQueryPatternWithCallback("pgbackrest_repos_meta m", mock.MakeQueryResult([]string{"revision", "generation", "repo_number"}, [][]any{{1, nil, nil}}), nil)
		require.NoError(t, s.pm.syncSidecar(t.Context()))
		assert.Zero(t, s.fake.configureCount())
	})
}

func segmentMock(s *sidecarStartup, bytes int) {
	s.qs.AddQueryPatternWithCallback("FROM pg_settings WHERE name = 'wal_segment_size'", mock.MakeQueryResult([]string{"setting"}, [][]any{{bytes}}), nil)
}

// postgres's wal_segment_size reaches the sidecar, and a change (or the first
// read, after a Configure that had none) is sent without any other difference.
func TestSyncSidecar_SendsWalSegmentSize(t *testing.T) {
	t.Run("read once postgres is up and sent with the list", func(t *testing.T) {
		s, _ := newSyncFixture(t, 3, true, 0)
		primaryMock(s, "f")
		tableMock(s, 5)
		segmentMock(s, 64<<20)
		require.NoError(t, s.pm.syncSidecar(t.Context()))
		assert.Equal(t, uint32(64<<20), s.fake.lastConfigure(t).GetWalSegmentSizeBytes())
	})

	t.Run("a segment size the sidecar lacks is sent although the list is current", func(t *testing.T) {
		s, _ := newSyncFixture(t, 5, true, 0)
		s.fake.status.Repos = []*pb.RepoGeneration{{Generation: 1, RepoNumber: 1}}
		primaryMock(s, "f")
		tableMock(s, 5)
		segmentMock(s, 1<<30)
		require.NoError(t, s.pm.syncSidecar(t.Context()))
		require.Equal(t, 1, s.fake.configureCount())
		assert.Equal(t, uint32(1<<30), s.fake.lastConfigure(t).GetWalSegmentSizeBytes())

		require.NoError(t, s.pm.syncSidecar(t.Context()))
		assert.Equal(t, 1, s.fake.configureCount(), "and not again once the sidecar accepted it")
	})

	t.Run("a changed segment size is re-sent", func(t *testing.T) {
		s, _ := newSyncFixture(t, 5, true, 0)
		s.fake.status.Repos = []*pb.RepoGeneration{{Generation: 1, RepoNumber: 1}}
		primaryMock(s, "f")
		tableMock(s, 5)
		s.pm.walSegBytes.Store(16 << 20)
		s.pm.sidecarWalSegBytes.Store(16 << 20)
		require.NoError(t, s.pm.syncSidecar(t.Context()))
		assert.Zero(t, s.fake.configureCount(), "unchanged: nothing to send")

		s.pm.walSegBytes.Store(64 << 20)
		require.NoError(t, s.pm.syncSidecar(t.Context()))
		assert.Equal(t, uint32(64<<20), s.fake.lastConfigure(t).GetWalSegmentSizeBytes())
	})

	t.Run("an unreadable size sends 0 and does not block the sync", func(t *testing.T) {
		s, _ := newSyncFixture(t, 3, true, 0)
		primaryMock(s, "f")
		tableMock(s, 5)
		require.NoError(t, s.pm.syncSidecar(t.Context()))
		assert.Zero(t, s.fake.lastConfigure(t).GetWalSegmentSizeBytes())
	})

	t.Run("the start-up Configure carries a size already known", func(t *testing.T) {
		s, _ := newSyncFixture(t, 0, false, 4)
		s.pm.walSegBytes.Store(32 << 20)
		require.NoError(t, s.pm.syncSidecar(t.Context()))
		assert.Equal(t, uint32(32<<20), s.fake.lastConfigure(t).GetWalSegmentSizeBytes())
	})
}

// TestSyncSidecar_Liveness checks that every input degraded still ends the
// pass, and that a sidecar that comes back is repaired by the next one.
func TestSyncSidecar_Liveness(t *testing.T) {
	t.Run("an unreachable sidecar fails the pass promptly", func(t *testing.T) {
		s, _ := newSyncFixture(t, 0, true, 0)
		s.pm.sidecar = newSidecarClient(filepath.Join(t.TempDir(), "gone"))
		done := make(chan error, 1)
		go func() { done <- s.pm.syncSidecar(t.Context()) }()
		select {
		case err := <-done:
			assert.Error(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("sync hung on an unreachable sidecar")
		}
	})

	t.Run("an unreadable table does not stop the next pass", func(t *testing.T) {
		s, _ := newSyncFixture(t, 3, true, 0)
		primaryMock(s, "f")
		s.qs.AddQueryPatternOnceWithError("pgbackrest_repos_meta m", errors.New("table unreadable"))
		require.Error(t, s.pm.syncSidecar(t.Context()))
		tableMock(s, 5)
		require.NoError(t, s.pm.syncSidecar(t.Context()))
		assert.Equal(t, int64(5), s.fake.lastConfigure(t).GetRevision())
	})

	t.Run("the loop reconfigures a sidecar that restarted unconfigured", func(t *testing.T) {
		old := sidecarSyncInterval
		sidecarSyncInterval = 20 * time.Millisecond
		t.Cleanup(func() { sidecarSyncInterval = old })

		s, _ := newSyncFixture(t, 0, true, 6)
		go s.pm.runSidecarSync()
		require.Eventually(t, func() bool { return s.fake.configureCount() == 0 }, time.Second, 5*time.Millisecond)

		s.fake.mu.Lock()
		s.fake.status.Configured = false
		s.fake.mu.Unlock()
		require.Eventually(t, func() bool { return s.fake.configureCount() >= 1 }, 5*time.Second, 10*time.Millisecond,
			"a sidecar reporting not-configured must be configured again without a multipooler restart")
		assert.Equal(t, int64(6), s.fake.lastConfigure(t).GetRevision())
	})
}

func TestStartup_ReplicationPassfile(t *testing.T) {
	loc := utils.FilesystemBackupLocation("/backups")
	t.Setenv(constants.PgPasswordEnvVar, "s3cret")
	s := newSidecarStartup(t, loc, loc)
	legacy := filepath.Join(s.poolerDir, "pgbackrest")
	require.NoError(t, os.MkdirAll(legacy, 0o755))
	// A pod that ran pgBackRest in-process kept the passfile primary_conninfo
	// names inside the pgbackrest directory.
	require.NoError(t, os.WriteFile(filepath.Join(legacy, "pgbackrest.pgpass"), []byte("*:*:*:postgres:s3cret\n"), 0o600))

	s.pm.loadShardConfigFromGlobalTopo()
	require.Equal(t, ManagerStateReady, s.pm.GetState())

	want := filepath.Join(s.poolerDir, "replication.pgpass")
	assert.Equal(t, want, s.pm.pgpassFilePath())
	assert.NotContains(t, want, string(filepath.Separator)+"pgbackrest"+string(filepath.Separator), "not under the deleted directory")
	assert.NoDirExists(t, legacy, "the legacy directory is gone")
	info, err := os.Stat(want)
	require.NoError(t, err, "removing the legacy directory leaves the replication passfile")
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	got, err := os.ReadFile(want)
	require.NoError(t, err)
	assert.Equal(t, "*:*:*:"+constants.DefaultPostgresUser+":s3cret\n", string(got), "only the postgres user entry")

	t.Run("a standby's primary_conninfo carries the path and does not drift", func(t *testing.T) {
		expected := s.pm.expectedPrimaryConnInfoAt("primary.example.com", 5432)
		assert.Equal(t, want, expected.GetPassfile())
		rendered := buildPrimaryConnInfo(expected)
		assert.Contains(t, rendered, "passfile="+want)
		for range 3 {
			parsed, err := parseAndRedactPrimaryConnInfo(rendered)
			require.NoError(t, err)
			assert.False(t, connInfoDrifted(parsed, s.pm.expectedPrimaryConnInfoAt("primary.example.com", 5432)))
		}
	})

	t.Run("a second start rewrites it in place", func(t *testing.T) {
		s.pm.loadShardConfigFromGlobalTopo()
		_, err := os.Stat(want)
		require.NoError(t, err)
	})
}

func TestInitialRepo_SidecarSeedsRealFingerprint(t *testing.T) {
	loc := utils.FilesystemBackupLocation("/backups")

	t.Run("encrypted", func(t *testing.T) {
		s := newSidecarStartup(t, loc, loc)
		fp := backup.CipherKeyFingerprint("hunter2")
		s.fake.status.GenerationsWithKeys = []uint32{1}
		s.fake.status.GenerationFingerprints = map[uint32]string{1: fp}

		repo, err := s.pm.initialRepo(t.Context())
		require.NoError(t, err)
		assert.True(t, repo.Encrypted)
		assert.Equal(t, fp, repo.KeyFingerprint)
		assert.Equal(t, backup.InitialPgBackRestRepo(backup.CipherKeys{1: "hunter2"}), repo, "identical to the row seeded for the same key")
	})

	t.Run("unencrypted", func(t *testing.T) {
		s := newSidecarStartup(t, loc, loc)
		repo, err := s.pm.initialRepo(t.Context())
		require.NoError(t, err)
		assert.Equal(t, backup.InitialPgBackRestRepo(nil), repo)
	})

	t.Run("a key without a fingerprint is refused", func(t *testing.T) {
		s := newSidecarStartup(t, loc, loc)
		s.fake.status.GenerationsWithKeys = []uint32{1}
		_, err := s.pm.initialRepo(t.Context())
		require.Error(t, err)
	})
}

func TestSidecarCalls_AreBounded(t *testing.T) {
	loc := utils.FilesystemBackupLocation("/backups")
	oldCall, oldCfg, oldRetry := sidecarCallTimeout, sidecarConfigureTimeout, sidecarConfigureRetry
	sidecarCallTimeout, sidecarConfigureTimeout, sidecarConfigureRetry = 100*time.Millisecond, 300*time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() { sidecarCallTimeout, sidecarConfigureTimeout, sidecarConfigureRetry = oldCall, oldCfg, oldRetry })
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })

	t.Run("a Status that never answers fails setup within the bound", func(t *testing.T) {
		s := newSidecarStartup(t, loc, loc)
		s.fake.statusHook = func() { <-block }
		start := time.Now()
		err := s.pm.setupSidecar(t.Context(), loc)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "sidecar unreachable")
		assert.Less(t, time.Since(start), 3*time.Second)
	})

	t.Run("a Configure that never answers does not hold setup", func(t *testing.T) {
		s := newSidecarStartup(t, loc, loc)
		s.fake.configureHook = func() { <-block }
		start := time.Now()
		require.NoError(t, s.pm.setupSidecar(t.Context(), loc), "a Configure failure is logged and retried by the sync loop")
		assert.Less(t, time.Since(start), 3*time.Second)
	})

	t.Run("the sync pass is bounded too", func(t *testing.T) {
		s, _ := newSyncFixture(t, 0, true, 0)
		s.fake.statusHook = func() { <-block }
		start := time.Now()
		require.Error(t, s.pm.syncSidecar(t.Context()))
		assert.Less(t, time.Since(start), 3*time.Second)
	})
}

func TestSidecarSocketsValidation(t *testing.T) {
	assert.Error(t, validateSidecarSockets(&Config{}), "the sockets are required")
	assert.Error(t, validateSidecarSockets(nil))
	assert.NoError(t, validateSidecarSockets(&Config{PgBackRestControlSocket: "/run/c.sock", PgBackRestWALSocket: "/run/w.sock"}))
	err := validateSidecarSockets(&Config{PgBackRestControlSocket: "/run/c.sock"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--pgbackrest-wal-socket")
	assert.Error(t, validateSidecarSockets(&Config{PgBackRestControlSocket: "run/c.sock", PgBackRestWALSocket: "/run/w.sock"}), "relative")
	assert.Error(t, validateSidecarSockets(&Config{PgBackRestControlSocket: "/run/c.sock", PgBackRestWALSocket: "w.sock"}), "relative")

	// And the constructor refuses it, so the pooler fails at startup rather than
	// rendering --unix-socket '' into archive_command.
	_, err = NewMultipoolerManager(slog.New(slog.DiscardHandler), &clustermetadatapb.Multipooler{
		Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "z", Name: "n"},
		ShardRef: &clustermetadatapb.ShardRef{TableGroup: constants.DefaultTableGroup, Shard: constants.DefaultShard},
	}, &Config{PgBackRestControlSocket: "/run/c.sock"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--pgbackrest-wal-socket")
}

func TestSyncSidecar_TableBehindWarnsOncePerPair(t *testing.T) {
	s, updates := newSyncFixture(t, 50, true, 0)
	var logs bytes.Buffer
	s.pm.logger = slog.New(slog.NewTextHandler(&logs, nil))
	primaryMock(s, "t") // a standby: it can only wait
	tableMock(s, 5)

	for range 4 {
		require.NoError(t, s.pm.syncSidecar(t.Context()))
	}
	assert.Equal(t, 1, strings.Count(logs.String(), "trails the sidecar"), "the same (revision, floor) pair is logged once")
	assert.Equal(t, int64(1), s.pm.tableBehind.Load(), "and stays visible as a gauge")
	assert.Empty(t, *updates)

	s.fake.mu.Lock()
	s.fake.status.Revision = 60
	s.fake.mu.Unlock()
	require.NoError(t, s.pm.syncSidecar(t.Context()))
	assert.Equal(t, 2, strings.Count(logs.String(), "trails the sidecar"), "a new floor is a new line")

	s.fake.mu.Lock()
	s.fake.status.Revision = 3
	s.fake.mu.Unlock()
	require.NoError(t, s.pm.syncSidecar(t.Context()))
	assert.Equal(t, int64(0), s.pm.tableBehind.Load(), "the gauge clears once the table has caught up")
}

func TestLogSyncFailure_WarnsOncePerDistinctError(t *testing.T) {
	var logs bytes.Buffer
	pm := &MultipoolerManager{logger: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo}))}
	notBootstrapped := errors.New(`relation "multigres.pgbackrest_repos_meta" does not exist`)
	for range 5 {
		pm.logSyncFailure(notBootstrapped)
	}
	assert.Equal(t, 1, strings.Count(logs.String(), "level=WARN"))
	pm.logSyncFailure(errors.New("another"))
	assert.Equal(t, 2, strings.Count(logs.String(), "level=WARN"), "a different failure is worth a line")
	pm.logSyncFailure(nil)
	pm.logSyncFailure(notBootstrapped)
	assert.Equal(t, 3, strings.Count(logs.String(), "level=WARN"), "and a failure after recovery is new again")
}

// The replication passfile exists before the pgbackrest directory a pod that
// ran pgBackRest in-process left (and may have kept the passfile
// primary_conninfo names in) is touched: if the removal fails, the replication
// passfile is already written.
func TestStartup_SidecarWritesTheReplicationPassfileBeforeRemovingLegacy(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("a read-only directory does not stop root from removing its contents")
	}
	t.Setenv(constants.PgPasswordEnvVar, "pw")
	loc := utils.FilesystemBackupLocation("/backups")
	s := newSidecarStartup(t, loc, loc)
	legacy := filepath.Join(s.poolerDir, "pgbackrest")
	require.NoError(t, os.MkdirAll(legacy, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(legacy, "pgbackrest.pgpass"), []byte("*:*:*:u:pw\n"), 0o600))
	require.NoError(t, os.Chmod(legacy, 0o500))
	t.Cleanup(func() { _ = os.Chmod(legacy, 0o700) })

	s.pm.loadShardConfigFromGlobalTopo()

	assert.NotEqual(t, ManagerStateReady, s.pm.GetState(), "removing the legacy directory failed")
	assert.FileExists(t, filepath.Join(s.poolerDir, "replication.pgpass"), "but the replication passfile was already written")
	assert.FileExists(t, filepath.Join(legacy, "pgbackrest.pgpass"), "and the one in the directory is still there")
}

func TestSidecarClient_DialFailureIsNotDelivered(t *testing.T) {
	c := newSidecarClient(filepath.Join(t.TempDir(), "gone"))
	t.Cleanup(c.Close)
	var out bytes.Buffer
	_, err := c.Run(t.Context(), pb.Command_COMMAND_RESTORE, nil, &out, &out)
	require.ErrorIs(t, err, backupengine.ErrRunNotDelivered)

	// A sidecar that answers and then fails is delivered: it may have started.
	f := &fakeSidecarServer{status: &pb.StatusResponse{}}
	f.run = func(context.Context, *pb.RunRequest, *connect.ServerStream[pb.RunOutput]) error {
		return connect.NewError(connect.CodeInternal, errors.New("boom"))
	}
	c2 := newSidecarClient(serveFakeSidecar(t, f))
	t.Cleanup(c2.Close)
	_, err = c2.Run(t.Context(), pb.Command_COMMAND_RESTORE, nil, &out, &out)
	require.Error(t, err)
	assert.NotErrorIs(t, err, backupengine.ErrRunNotDelivered)
}

func TestSidecarSocketsValidation_ErrorIsDeterministic(t *testing.T) {
	cfg := &Config{PgBackRestControlSocket: "relative-control", PgBackRestWALSocket: "relative-wal"}
	for range 30 {
		err := validateSidecarSockets(cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "--pgbackrest-control-socket", "the first flag in a fixed order is the one reported")
	}
}

func TestSyncSidecar_GaugeResetsWhenAPassCannotCompare(t *testing.T) {
	s, _ := newSyncFixture(t, 50, true, 0)
	primaryMock(s, "t")
	tableMock(s, 5)
	require.NoError(t, s.pm.syncSidecar(t.Context()))
	require.Equal(t, int64(1), s.pm.tableBehind.Load())

	// Next pass: the sidecar cannot be read, so nothing was compared.
	s.fake.mu.Lock()
	s.fake.statusErr = errors.New("down")
	s.fake.mu.Unlock()
	require.Error(t, s.pm.syncSidecar(t.Context()))
	assert.Equal(t, int64(0), s.pm.tableBehind.Load(), "the last verdict must not stand when a pass could not compare")
}
