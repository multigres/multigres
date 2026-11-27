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

package clustersetup

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/pgbackreststate"
	"github.com/multigres/multigres/go/common/sandbox"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/provisioner/local"
	"github.com/multigres/multigres/go/tools/executil"
)

// sandboxMode is the --sandbox mode every sidecar in this process gets,
// $MULTIGRES_PGBACKREST_SANDBOX or required.
func sandboxMode(t *testing.T) string {
	t.Helper()
	mode, err := local.SandboxMode()
	if err != nil {
		t.Fatal(err)
	}
	return mode
}

// RequireSidecarHost fails or skips a test that needs the sidecar on a host
// where it cannot run. The mode is $MULTIGRES_PGBACKREST_SANDBOX (required by
// default):
//   - required on a host that is not Linux (a macOS dev machine): skipped, and
//     the reason names the variable; run the test inside a Linux VM such as
//     OrbStack, through bin/sidecar-linux-e2e.sh, or set the variable to auto
//     or off to run natively;
//   - required on Linux without Landlock: a failure, because the sidecar
//     refuses to start;
//   - auto or off: runs, confined only where auto finds Landlock. A host
//     without the pgbackrest binary skips (required fails loudly instead);
//   - root: skipped on every OS, because pgBackRest refuses to run as root.
func RequireSidecarHost(t *testing.T) {
	t.Helper()
	mode := sandboxMode(t)
	switch {
	case mode == constants.SandboxRequired && runtime.GOOS != "linux":
		t.Skipf("%s; set %s=auto or off to run natively (this host is %s)", sandbox.RequirementMessage, constants.PgBackRestSandboxEnvVar, runtime.GOOS)
	case mode == constants.SandboxRequired:
		if err := checkLandlock(); err != nil {
			t.Fatalf("the pgbackrestd sidecar needs Landlock under %s=required and this kernel lacks it: %v", constants.PgBackRestSandboxEnvVar, err)
		}
	}
	if mode != constants.SandboxRequired {
		if _, err := exec.LookPath("pgbackrest"); err != nil {
			t.Skipf("pgbackrest is not installed: %v", err)
		}
	}
	if os.Geteuid() == 0 {
		t.Skip("pgBackRest refuses to run as root, and the sidecar must run as the test user")
	}
}

// RequireEnforcedSandbox is RequireSidecarHost for a test whose point is
// confinement: it skips when the sidecar would run its children unconfined,
// which is always so in off, and in auto on a host without Landlock.
func RequireEnforcedSandbox(t *testing.T) {
	t.Helper()
	RequireSidecarHost(t)
	mode := sandboxMode(t)
	if mode == constants.SandboxOff {
		t.Skipf("confinement is not exercised under %s=off", constants.PgBackRestSandboxEnvVar)
	}
	if err := checkLandlock(); err != nil {
		t.Skipf("children run unconfined under %s=%s on this host: %v", constants.PgBackRestSandboxEnvVar, mode, err)
	}
}

// checkLandlock is a variable so staticcheck does not conclude, on a host where
// the check always fails, that the code after it is dead.
var checkLandlock = sandbox.CheckAvailable

// SidecarSessionTTL is longer than five minutes because pgbackrestd rejects a
// credential source that returns that little life.
const SidecarSessionTTL = "10m"

// SidecarSettings is the part of the pgbackrestd configuration shared by every
// pooler in a setup.
type SidecarSettings struct {
	// S3CredEndpoint is the s3mock session URL the sidecar mints credentials
	// from. Empty when the repository is not on an S3 endpoint.
	S3CredEndpoint string
	// S3CredCAFile verifies S3CredEndpoint. The CA that verifies the S3
	// endpoint itself is in the backup location (S3Backup.ca_file).
	S3CredCAFile string
	// S3StaticKey, when set, makes the sidecar read its S3 keys from a key
	// directory holding these, the way the local provisioner runs it, instead
	// of minting sessions from S3CredEndpoint.
	S3StaticKey   *S3StaticKey
	CipherKeyFile string
	// TLSCertDir holds the certificates of the sidecar's pgBackRest TLS server.
	TLSCertDir string
	// ServerPort is the sidecar's pgBackRest TLS server port.
	ServerPort int
}

// S3StaticKey is one set of S3 keys for a sidecar's static key directory.
type S3StaticKey struct {
	AccessKeyID, SecretAccessKey, SessionToken string
}

// PgBackRestdInstance is the pgbackrestd process serving one pooler.
//
// Cleanup layers: t.Cleanup terminates it, and on Linux the kernel kills it if
// the test binary dies first (go test -timeout, SIGKILL, CI cancel). On a clean
// shutdown pgbackrestd kills the pgBackRest children it tracks. pgBackRest's
// asynchronous archive workers call setsid, so they leave the process group and
// neither a group kill nor the parent-death signal reaches them; they exit on
// their own once their queue drains, which only takes long while the
// repository is unreachable.
//
// The harness owns it directly rather than through ProcessInstance because it has
// no gRPC port to wait on: it is ready when its two unix sockets accept.
type PgBackRestdInstance struct {
	Name    string
	Paths   local.PgBackRestdPaths
	Config  local.PgBackRestdConfig
	LogFile string

	Process *executil.Cmd
	exited  chan struct{}
}

// AttachPgBackRestd wires one pooler to its own sidecar and returns the
// instance. Every pooler has one: it adds the control and WAL sockets to the
// multipooler's (or minigres's) flags and returns the command line for the
// sidecar itself. loc is the database's backup location.
func AttachPgBackRestd(t *testing.T, name, tempDir string, pgctld, multipooler *ProcessInstance, loc *clustermetadatapb.BackupLocation, settings SidecarSettings) *PgBackRestdInstance {
	t.Helper()

	// In a pod the data volume exists before any container starts; multipooler
	// opens it at startup, before pgctld would create it.
	if err := os.MkdirAll(pgctld.PoolerDir, 0o755); err != nil {
		t.Fatalf("create pooler directory %s: %v", pgctld.PoolerDir, err)
	}
	paths := local.NewPgBackRestdPaths(pgctld.PoolerDir)
	if err := paths.Prepare(TestPostgresPassword); err != nil {
		t.Fatalf("prepare pgbackrestd directories for %s: %v", name, err)
	}
	if fs := loc.GetFilesystem(); fs != nil {
		// A sandbox rule on a missing path is an error, so the repository
		// directory has to exist before the sidecar starts.
		if err := os.MkdirAll(fs.GetPath(), 0o755); err != nil {
			t.Fatalf("create backup repository %s: %v", fs.GetPath(), err)
		}
	}

	logFile := filepath.Join(tempDir, name, "pgbackrestd.log")
	inst := &PgBackRestdInstance{
		Name:    name,
		Paths:   paths,
		LogFile: logFile,
		Config: local.PgBackRestdConfig{
			PoolerDir:      pgctld.PoolerDir,
			Paths:          paths,
			PgPort:         pgctld.PgPort,
			PgUser:         constants.DefaultPostgresUser,
			Sandbox:        sandboxMode(t),
			TableGroup:     constants.DefaultTableGroup,
			Shard:          constants.DefaultShard,
			Location:       loc,
			ServerPort:     settings.ServerPort,
			TLSCertDir:     settings.TLSCertDir,
			CipherKeyFile:  settings.CipherKeyFile,
			S3CredEndpoint: settings.S3CredEndpoint,
			S3CredCAFile:   settings.S3CredCAFile,
			LogFile:        logFile,
			LogLevel:       "info",
		},
	}

	if key := settings.S3StaticKey; key != nil {
		inst.Config.S3CredEndpoint = ""
		inst.Config.S3StaticKeyDir = filepath.Join(paths.Dir, "s3-keys")
		if err := local.WriteS3KeyBundle(inst.Config.S3StaticKeyDir, key.AccessKeyID, key.SecretAccessKey, key.SessionToken); err != nil {
			t.Fatalf("write the S3 keys of %s: %v", name, err)
		}
	}

	// The sidecar owns the pgBackRest server, its certificates and the cipher
	// keys, so multipooler is given the sockets and none of them.
	multipooler.ExtraArgs = append(multipooler.ExtraArgs,
		"--pgbackrest-control-socket", paths.ControlSocket,
		"--pgbackrest-wal-socket", paths.WALSocket,
		// The pooler directory is not a mount point here.
		"--data-volume-root", pgctld.PoolerDir,
		"--data-volume-anchor", "dir",
	)
	return inst
}

// Start launches pgbackrestd and waits until both API sockets accept. It must
// run before pgctld starts postgres, whose archive_command dials the WAL
// socket.
func (i *PgBackRestdInstance) Start(ctx context.Context, t *testing.T) error {
	t.Helper()
	if i.IsRunning() {
		return nil
	}

	args, err := i.Config.BuildArgs()
	if err != nil {
		return fmt.Errorf("build pgbackrestd args for %s: %w", i.Name, err)
	}

	// Anything printed before logging is set up (flag validation) goes to
	// stderr, so keep it beside the log.
	stderr, err := os.OpenFile(i.LogFile+".stderr", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open pgbackrestd stderr file: %w", err)
	}
	defer stderr.Close()

	i.Process = executil.Command(ctx, "pgbackrestd", args...).WithProcessGroup()
	i.Process.Cmd.Stderr = stderr
	setParentDeathSignal(i.Process)
	i.exited = make(chan struct{})

	// Pdeathsig fires when the thread that forked the child exits, not the
	// process, so the goroutine that starts it keeps its thread for as long as
	// the child lives: it is also the one that waits.
	started := make(chan error, 1)
	go func() {
		runtime.LockOSThread()
		defer close(i.exited)
		if err := i.Process.Start(); err != nil {
			started <- err
			return
		}
		started <- nil
		_ = i.Process.Wait()
	}()
	if err := <-started; err != nil {
		return fmt.Errorf("start pgbackrestd for %s: %w", i.Name, err)
	}
	t.Logf("Started pgbackrestd for %s (PID %d): %v", i.Name, i.Process.Process.Pid, args)
	// No t.Cleanup here: a shared setup is built inside whichever test runs
	// first, and that test's cleanup would kill sidecars every later test still
	// uses. ShardSetup.Cleanup terminates it, ctx cancellation ends it if the
	// setup is torn down another way, and on Linux the kernel kills it if the
	// test binary dies.

	if err := local.WaitForPgBackRestdSockets(ctx, i.Paths, i.exited, 30*time.Second); err != nil {
		for _, f := range []string{i.LogFile, i.LogFile + ".stderr"} {
			if b, rerr := os.ReadFile(f); rerr == nil {
				t.Logf("%s:\n%s", f, b)
			}
		}
		return fmt.Errorf("pgbackrestd for %s: %w", i.Name, err)
	}
	return nil
}

// Rescope restarts the sidecar for another shard's repository. A pooler of
// another tablegroup or shard runs with its own --table-group, --shard and
// --tablegroup-id, and its sidecar must be started with the same values: the
// pooler refuses a sidecar that serves another repository. It is only for a
// sidecar that has not configured a repository yet, since a configured one is
// pinned to the repository it used.
func (i *PgBackRestdInstance) Rescope(ctx context.Context, t *testing.T, tableGroup, shard string, tableGroupID int64) error {
	t.Helper()
	if err := i.Stop(5 * time.Second); err != nil {
		return err
	}
	i.Config.TableGroup, i.Config.Shard, i.Config.TableGroupID = tableGroup, shard, tableGroupID
	return i.Start(ctx, t)
}

// IsRunning reports whether the process was started and has not exited.
func (i *PgBackRestdInstance) IsRunning() bool {
	if i == nil || i.exited == nil {
		return false
	}
	select {
	case <-i.exited:
		return false
	default:
		return true
	}
}

// Stop stops the sidecar with SIGTERM, then SIGKILL after timeout, and returns
// an error unless the process was seen to exit. A caller that is about to
// delete the sidecar's state, PGDATA or the repository must check it: a
// sidecar that is still running would go on writing into what is being wiped.
// After a nil return IsRunning is false, so a following Start launches a new
// process.
func (i *PgBackRestdInstance) Stop(timeout time.Duration) error {
	if !i.IsRunning() {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	// SIGTERM goes to the sidecar alone first, the way the kubelet and the
	// local provisioner stop one, so that it shuts its workers down itself.
	// The group is signalled only if the sidecar does not exit.
	executil.TerminatePID(ctx, i.Process.Process.Pid)
	select {
	case <-i.exited:
		return nil
	case <-ctx.Done():
	case <-time.After(time.Second):
	}
	if _, stopped := i.Process.Stop(ctx); !stopped {
		return fmt.Errorf("pgbackrestd for %s (PID %d) did not exit within %v of SIGTERM or after SIGKILL", i.Name, i.Process.Process.Pid, timeout)
	}
	// The goroutine that started the process closes exited once its own Wait
	// returns, which is what IsRunning reads.
	select {
	case <-i.exited:
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("pgbackrestd for %s exited but its wait did not return", i.Name)
	}
}

// StopAndRemoveState stops the sidecar and deletes its directory, for a test
// that starts the pooler over on an empty repository.
//
// That the sidecar process exited does not show that its pgBackRest workers
// did: it exits non-zero and keeps a marker when it cannot prove they stopped,
// and a worker runs outside its process group. Deleting the directory would
// delete that marker, and the next sidecar would start beside a worker that is
// still writing. So the directory is deleted only under the cleanup guard,
// which is granted when no sidecar or supervisor holds the instance lock and
// no worker marker is left. Otherwise the directory is kept as it is and the
// error says why.
func (i *PgBackRestdInstance) StopAndRemoveState(timeout time.Duration) error {
	if err := i.Stop(timeout); err != nil {
		return err
	}
	if _, err := os.Stat(i.Paths.StateDir); os.IsNotExist(err) {
		// Never started: there is no state and nothing that could own it.
		return os.RemoveAll(i.Paths.Dir)
	}
	guard, err := pgbackreststate.AcquireCleanupGuard(i.Paths.StateDir)
	if err != nil {
		return fmt.Errorf("pgbackrestd for %s stopped, but its workers are not shown to have stopped; %s is kept: %w", i.Name, i.Paths.Dir, err)
	}
	removeErr := os.RemoveAll(i.Paths.Dir)
	if err := guard.Close(); err != nil && removeErr == nil {
		removeErr = err
	}
	if removeErr != nil {
		return fmt.Errorf("remove %s: %w", i.Paths.Dir, removeErr)
	}
	return nil
}

// Terminate is Stop for teardown paths that have nothing left to protect: a
// sidecar that does not stop is logged, not reported.
func (i *PgBackRestdInstance) Terminate(logf func(string, ...any), timeout time.Duration) {
	if !i.IsRunning() {
		return
	}
	if err := i.Stop(timeout); err != nil {
		logf("WARNING: %v", err)
		return
	}
	logf("pgbackrestd for %s terminated", i.Name)
}
