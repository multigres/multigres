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
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
	backupengine "github.com/multigres/multigres/go/services/multipooler/internal/manager/backup"
	"github.com/multigres/multigres/go/test/utils"
)

func TestSidecarCopyAndArchiveRenderingRefuseInvalidState(t *testing.T) {
	pm, _, dir := newGenerationsTestManager(t)
	require.NoError(t, os.WriteFile(filepath.Join(dir, generationsCopyFile), []byte(`{"revision":-1,"repos":[{"generation":1,"repo_number":1}]}`), 0o600))
	_, err := pm.loadGenerationsCopy()
	require.ErrorContains(t, err, "negative revision")
	pm.record = newRecordFromProto(&clustermetadatapb.Multipooler{})
	_, err = pm.openGenerationsRoot()
	require.ErrorContains(t, err, "pooler dir is not set")
	for _, name := range []string{"archive_command", "restore_command"} {
		t.Run(name, func(t *testing.T) {
			seed := foreignLegacySeed("/other")
			pm, path := newNormalizeManager(t, sidecarMode, seed)
			settings := pm.backupEngineSettings()
			settings.ArchiveCommand = func() string { return "invalid\ncommand" }
			settings.RestoreCommand = func(string) string { return "invalid\ncommand" }
			// Keep only the setting under test, so each refusal is exercised.
			if name == "restore_command" {
				require.NoError(t, os.WriteFile(path, []byte("restore_command = "+quoteConf(legacyRestoreCommand("/other/conf", "/other/pid"))+"\n"), 0o600))
			}
			before, err := os.ReadFile(path)
			require.NoError(t, err)
			pm.backup = newStubEngine(pm.logger, pm.record, settings)
			require.ErrorContains(t, pm.normalizeArchiveCommands(t.Context()), "rendering "+name)
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, before, after, "failed rendering must not partially rewrite auto.conf")
		})
	}
}

func TestSidecarGenerationMetadataRejectsMalformedRows(t *testing.T) {
	for _, tc := range []struct {
		name string
		rows [][]any
		want string
	}{
		{"missing revision", nil, "has no revision row"},
		{"short row", [][]any{{1, 1}}, "has 2 columns"},
		{"invalid revision", [][]any{{"bad", 1, 1}}, "invalid pgbackrest_repos_meta revision"},
		{"overflow generation", [][]any{{1, "4294967296", 1}}, "invalid pgbackrest_repos generation"},
		{"negative repository", [][]any{{1, 1, "-1"}}, "invalid pgbackrest_repos repo_number"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := newSyncFixture(t, 3, true, 0)
			s.qs.AddQueryPatternOnce("pgbackrest_repos_meta m", mock.MakeQueryResult([]string{"revision", "generation", "repo_number"}, tc.rows))
			rev, repos, err := s.pm.readGenerations(t.Context())
			require.ErrorContains(t, err, tc.want)
			require.Zero(t, rev)
			require.Nil(t, repos)
			require.Zero(t, s.fake.configureCount(), "invalid metadata must not reach Configure")
		})
	}
	for _, tc := range []struct {
		name string
		rows [][]any
		want string
	}{
		{"missing", nil, "has no revision row"},
		{"malformed", [][]any{{"not-a-number"}}, "invalid pgbackrest_repos_meta revision"},
	} {
		t.Run("floor/"+tc.name, func(t *testing.T) {
			loc := utils.FilesystemBackupLocation("/backups")
			s := newSidecarStartup(t, loc, loc)
			s.qs.AddQueryPatternOnce("UPDATE multigres.pgbackrest_repos_meta", mock.MakeQueryResult([]string{"revision"}, tc.rows))
			rev, err := s.pm.reconcileRevisionFloor(t.Context(), 9)
			require.ErrorContains(t, err, tc.want)
			require.Zero(t, rev)
		})
	}
	t.Run("floor query failure", func(t *testing.T) {
		loc := utils.FilesystemBackupLocation("/backups")
		s := newSidecarStartup(t, loc, loc)
		s.qs.AddQueryPatternOnceWithError("UPDATE multigres.pgbackrest_repos_meta", errors.New("fixture unavailable"))
		_, err := s.pm.reconcileRevisionFloor(t.Context(), 9)
		require.ErrorContains(t, err, "failed to raise pgbackrest revision floor")
	})
}

func TestSidecarInvalidWalSegmentMetadataIsNotCached(t *testing.T) {
	for _, rows := range [][][]any{nil, {{"0"}}, {{"invalid"}}, {{"4294967296"}}} {
		s, _ := newSyncFixture(t, 3, true, 0)
		s.qs.AddQueryPatternOnce("pg_settings", mock.MakeQueryResult([]string{"setting"}, rows))
		require.Zero(t, s.pm.walSegmentSize(t.Context()))
		require.Zero(t, s.pm.walSegBytes.Load())
	}
}

type rejectingStreamWriter struct{ err error }

func (w rejectingStreamWriter) Write([]byte) (int, error) { return 0, w.err }

func TestSidecarStreamPropagatesConsumerAndTruncationErrors(t *testing.T) {
	for _, stderr := range []bool{false, true} {
		f := &fakeSidecarServer{status: &pb.StatusResponse{}}
		f.run = func(_ context.Context, _ *pb.RunRequest, stream *connect.ServerStream[pb.RunOutput]) error {
			output := &pb.RunOutput{Chunk: &pb.RunOutput_Stdout{Stdout: []byte("fixture")}}
			if stderr {
				output.Chunk = &pb.RunOutput_Stderr{Stderr: []byte("fixture")}
			}
			return stream.Send(output)
		}
		c := newSidecarClient(serveFakeSidecar(t, f))
		t.Cleanup(c.Close)
		sentinel := errors.New("consumer stopped")
		var stdout, errout io.Writer = rejectingStreamWriter{sentinel}, io.Discard
		if stderr {
			stdout, errout = io.Discard, rejectingStreamWriter{sentinel}
		}
		_, err := c.run(t.Context(), pb.Command_COMMAND_INFO, nil, stdout, errout)
		require.ErrorIs(t, err, sentinel)
	}
	f := &fakeSidecarServer{status: &pb.StatusResponse{}}
	f.run = func(_ context.Context, _ *pb.RunRequest, stream *connect.ServerStream[pb.RunOutput]) error {
		return stream.Send(&pb.RunOutput{Chunk: &pb.RunOutput_ExitCode{ExitCode: 0}, OutputTruncated: true})
	}
	c := newSidecarClient(serveFakeSidecar(t, f))
	t.Cleanup(c.Close)
	_, err := c.run(t.Context(), pb.Command_COMMAND_INFO, nil, io.Discard, io.Discard)
	require.ErrorIs(t, err, backupengine.ErrOutputTruncated, "exit zero must not turn incomplete info output into success")
}

func TestSidecarUnavailableMetadataCannotAuthorizeStartup(t *testing.T) {
	for _, tc := range []string{"noninitial keys", "unsupported authoritative generation", "encryption required"} {
		t.Run(tc, func(t *testing.T) {
			loc := utils.FilesystemBackupLocation("/backups")
			s := newSidecarStartup(t, loc, loc)
			want := "holds no key for generation 1"
			switch tc {
			case "noninitial keys":
				s.fake.status.GenerationsWithKeys = []uint32{2}
			case "unsupported authoritative generation":
				loc = utils.FilesystemBackupLocation("/backups")
				loc.AuthoritativeGeneration = 2
				want = "authoritative_generation 2 is not supported"
			case "encryption required":
				loc = utils.FilesystemBackupLocation("/backups")
				loc.RequireInitialRepoEncryption = true
				want = "backup encryption is required"
			}
			legacy := filepath.Join(s.poolerDir, "pgbackrest")
			require.NoError(t, os.Mkdir(legacy, 0o700))
			require.ErrorContains(t, s.pm.setupSidecar(t.Context(), loc), want)
			require.DirExists(t, legacy, "refusal must precede deleting legacy credentials")
			require.Zero(t, s.fake.configureCount())
		})
	}
	s := newSidecarStartup(t, utils.FilesystemBackupLocation("/backups"), utils.FilesystemBackupLocation("/backups"))
	s.fake.statusErr = connect.NewError(connect.CodeUnavailable, errors.New("fixture unavailable"))
	_, err := s.pm.cipherKeys(t.Context())
	require.Error(t, err)
	_, err = s.pm.initialRepo(t.Context())
	require.Error(t, err)
	require.Error(t, s.pm.insertInitialPgBackRestRepo(t.Context()))
	socket := serveFakeSidecar(t, &fakeSidecarServer{status: &pb.StatusResponse{}})
	_, err = SidecarServerPort(t.Context(), socket)
	require.ErrorContains(t, err, "reports no server port")
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	_, err = SidecarServerPort(ctx, filepath.Join(t.TempDir(), "missing.sock"))
	require.Error(t, err)
	require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
}

type failureBoundaryMeter struct {
	metric.Meter
	failCallback bool
}

func (m failureBoundaryMeter) Int64ObservableGauge(name string, opts ...metric.Int64ObservableGaugeOption) (metric.Int64ObservableGauge, error) {
	if !m.failCallback {
		return nil, errors.New("fixture instrument failure")
	}
	return m.Meter.Int64ObservableGauge(name, opts...)
}

func (m failureBoundaryMeter) RegisterCallback(metric.Callback, ...metric.Observable) (metric.Registration, error) {
	return nil, errors.New("fixture callback failure")
}

type failureBoundaryProvider struct {
	metric.MeterProvider
	meter metric.Meter
}

func (p failureBoundaryProvider) Meter(string, ...metric.MeterOption) metric.Meter { return p.meter }

func TestSidecarManagerMetricsFailuresDoNotChangeState(t *testing.T) {
	for _, callback := range []bool{false, true} {
		provider := noop.NewMeterProvider()
		original := otel.GetMeterProvider()
		otel.SetMeterProvider(failureBoundaryProvider{MeterProvider: provider, meter: failureBoundaryMeter{Meter: provider.Meter("fixture"), failCallback: callback}})
		var logs bytes.Buffer
		pm := &MultipoolerManager{logger: slog.New(slog.NewTextHandler(&logs, nil))}
		pm.tableBehind.Store(1)
		pm.markerAgeSec.Store(42)
		pm.credentialExposed.Store(1)
		pm.registerTableBehindGauge()
		pm.registerRestoreMarkerAgeGauge()
		pm.registerCredentialExposedGauge()
		otel.SetMeterProvider(original)
		require.Contains(t, logs.String(), "generation_table_behind")
		require.Contains(t, logs.String(), "restore_marker_age")
		require.Contains(t, logs.String(), "credential_exposed")
		require.Equal(t, int64(1), pm.tableBehind.Load())
		require.Equal(t, int64(42), pm.markerAgeSec.Load())
		require.Equal(t, int64(1), pm.credentialExposed.Load())
	}
}
