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
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	commonbackup "github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/safefs"
	"github.com/multigres/multigres/go/common/sandbox"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
	backupengine "github.com/multigres/multigres/go/services/multipooler/internal/manager/backup"
	"github.com/multigres/multigres/go/tools/executil"
)

const (
	testWALSocket     = "/run/pgbackrest-wal/wal.sock"
	testControlSocket = "/run/pgbackrest-control/control.sock"
)

// sidecarSubcommands is the pgBackRest subcommand the sidecar runs for each
// control API command.
var sidecarSubcommands = map[pb.Command]string{
	pb.Command_COMMAND_BACKUP:        "backup",
	pb.Command_COMMAND_VERIFY:        "verify",
	pb.Command_COMMAND_INFO:          "info",
	pb.Command_COMMAND_CHECK:         "check",
	pb.Command_COMMAND_EXPIRE:        "expire",
	pb.Command_COMMAND_STANZA_CREATE: "stanza-create",
	pb.Command_COMMAND_RESTORE:       "restore",
}

// runStub runs the `pgbackrest` stub on PATH with the argv the real sidecar
// renders (the stanza first, the subcommand last) and returns its exit code.
func runStub(ctx context.Context, cmd pb.Command, args []string, stdout, stderr io.Writer) (int, error) {
	argv := append([]string{"--stanza=multigres"}, args...)
	argv = append(argv, sidecarSubcommands[cmd])
	c := executil.Command(ctx, "pgbackrest", argv...)
	c.Stdout, c.Stderr = stdout, stderr
	if err := c.Start(); err != nil {
		return 127, nil //nolint:nilerr // a stub that cannot start reads as a command-not-found exit, like a shell
	}
	if err := c.Wait(); err != nil {
		if ee, ok := errors.AsType[*exec.ExitError](err); ok {
			return ee.ExitCode(), nil
		}
		return 127, nil
	}
	return 0, nil
}

// execingRun is a fakeSidecarServer.run that executes the stub pgbackrest on
// PATH, so a test scripts pgBackRest's output and exit code with a shell
// script (see stubPgbackrest in the backup package's tests).
func execingRun(ctx context.Context, req *pb.RunRequest, stream *connect.ServerStream[pb.RunOutput]) error {
	var out bytes.Buffer
	exit, _ := runStub(ctx, req.GetCommand(), req.GetArgs(), &out, &out)
	if out.Len() > 0 {
		if err := stream.Send(&pb.RunOutput{Chunk: &pb.RunOutput_Stdout{Stdout: out.Bytes()}}); err != nil {
			return err
		}
	}
	return stream.Send(&pb.RunOutput{Chunk: &pb.RunOutput_ExitCode{ExitCode: int32(exit)}})
}

// applyTestSidecar makes cfg name a sidecar the way production config must:
// both sockets set and the data volume at poolerDir. A config that already
// names its own sockets is left alone.
func applyTestSidecar(cfg *Config, poolerDir string) {
	if cfg.PgBackRestControlSocket == "" {
		cfg.PgBackRestControlSocket = testControlSocket
	}
	if cfg.PgBackRestWALSocket == "" {
		cfg.PgBackRestWALSocket = testWALSocket
	}
	if cfg.DataVolumeRoot == "" {
		cfg.DataVolumeRoot = poolerDir
		// A temp dir is not a mount point. A config that already named its
		// root keeps the anchor it chose.
		cfg.DataVolumeAnchor = sandbox.AnchorDir
	}
}

// withTestSidecarServer serves a fake sidecar and points cfg at it. The fake
// serves whatever backup location the topology holds for the pooler's database,
// so the manager can reach Ready, and its Run execs the stub pgbackrest.
func withTestSidecarServer(t *testing.T, cfg *Config, mp *clustermetadatapb.Multipooler) {
	t.Helper()
	status := &pb.StatusResponse{ServerPort: 8432}
	if cfg.TopoClient != nil {
		if db, err := cfg.TopoClient.GetDatabase(t.Context(), mp.GetShardRef().GetDatabase()); err == nil {
			status.LocationFingerprint = backupLocationFingerprint(db.GetBackupLocation())
		}
	}
	cfg.PgBackRestControlSocket = serveFakeSidecar(t, &fakeSidecarServer{status: status, run: execingRun})
	applyTestSidecar(cfg, mp.GetPoolerDir())
}

// execRunner is a backupengine.SidecarRunner that execs the stub pgbackrest on
// PATH, for tests that build an engine by hand and script pgBackRest with a
// shell script.
type execRunner struct{}

func (execRunner) Run(ctx context.Context, cmd pb.Command, args []string, stdout, stderr io.Writer) (int, error) {
	return runStub(ctx, cmd, args, stdout, stderr)
}

func (execRunner) Status(context.Context) (*pb.StatusResponse, error) {
	return &pb.StatusResponse{}, nil
}

// newStubEngine builds a backup engine on the stub runner, with the sidecar's
// command renderers unless settings brings its own.
func newStubEngine(logger *slog.Logger, id backupengine.Identity, settings backupengine.Settings) *backupengine.Engine {
	if settings.Sidecar == nil {
		settings.Sidecar = execRunner{}
	}
	if settings.ArchiveCommand == nil {
		settings.ArchiveCommand = func() string { return commonbackup.SidecarArchiveCommand(testWALSocket) }
	}
	if settings.RestoreCommand == nil {
		settings.RestoreCommand = func(pidFile string) string { return commonbackup.SidecarRestoreCommand(testWALSocket, pidFile) }
	}
	return backupengine.NewEngine(logger, id, settings)
}

// useTestSidecar gives a hand-built manager what production always has: a
// sidecar client and the data volume root the restore marker is read through,
// for tests whose record names no pooler directory. The client dials a socket
// nobody listens on: it starts no goroutine, so it is safe inside a synctest
// bubble, and any call that needs the sidecar fails the way an unreachable
// sidecar does. A test that needs a sidecar to answer starts a fake of its own.
func useTestSidecar(t *testing.T, pm *MultipoolerManager) {
	t.Helper()
	if pm.sidecar == nil {
		pm.sidecar = newSidecarClient(filepath.Join(os.TempDir(), "multigres-no-sidecar.sock"))
		t.Cleanup(pm.sidecar.Close)
	}
	if pm.volumeRoot != nil || (pm.record != nil && pm.record.PoolerDir() != "") {
		return
	}
	dir := t.TempDir()
	pm.volumeRoot = func() (*safefs.Root, error) { return safefs.OpenRoot(dir) }
}

// newManagerForTest builds a manager through the production constructor
// against a fake sidecar (see withTestSidecarServer). A config that names its
// own sockets keeps them.
func newManagerForTest(t *testing.T, logger *slog.Logger, mp *clustermetadatapb.Multipooler, cfg *Config, timeout time.Duration) (*MultipoolerManager, error) {
	t.Helper()
	if cfg.PgBackRestControlSocket == "" {
		withTestSidecarServer(t, cfg, mp)
	} else {
		applyTestSidecar(cfg, mp.GetPoolerDir())
	}
	if timeout == 0 {
		return NewMultipoolerManager(logger, mp, cfg)
	}
	return NewMultipoolerManagerWithTimeout(logger, mp, cfg, timeout)
}

// useAnsweringSidecar points a hand-built manager at a fake sidecar that
// answers Status with no restore running, for tests that exercise the restore
// flow. It is not for synctest bubbles (the fake serves on a real socket).
func useAnsweringSidecar(t *testing.T, pm *MultipoolerManager) {
	t.Helper()
	if pm.sidecar != nil {
		pm.sidecar.Close()
	}
	pm.sidecar = newSidecarClient(serveFakeSidecar(t, &fakeSidecarServer{status: &pb.StatusResponse{}}))
	t.Cleanup(pm.sidecar.Close)
}

// stubPgbackrest puts a `pgbackrest` stub first on PATH for the test, so the
// commands the fake sidecar execs never reach a pgBackRest installed on the
// host.
func stubPgbackrest(t *testing.T, script string) {
	t.Helper()
	bin := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(bin, "pgbackrest"), []byte(script), 0o755))
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))
}
